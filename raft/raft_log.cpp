#include "raft/raft_log.h"
#include "common/log.h"
#include "raft/util.h"

namespace kv {

RaftLog::RaftLog(StoragePtr storage, uint64_t max_next_ents_size)
    : storage_(std::move(storage)),
      committed_(0),
      applied_(0),
      max_next_ents_size_(max_next_ents_size) {
  assert(storage_);
  uint64_t first;
  auto status = storage_->first_index(first);
  assert(status.is_ok());

  uint64_t last;
  status = storage_->last_index(last);
  assert(status.is_ok());

  unstable_ = std::make_shared<Unstable>(last + 1);

  // Initialize our committed and applied pointers to the time of the last compaction.
  applied_ = committed_ = first - 1;
}
RaftLog::~RaftLog() {

}
//尝试将给定的日志条目追加到本地日志中。如果日志条目与本地日志匹配（即索引和任期匹配），则处理冲突并更新提交索引。
void RaftLog::maybe_append(uint64_t index,
                           uint64_t log_term,
                           uint64_t committed,
                           std::vector<proto::EntryPtr> entries,
                           uint64_t& last_new_index,
                           bool& ok) {
  if (match_term(index, log_term)) {
    uint64_t lastnewi = index + entries.size();
    uint64_t ci = find_conflict(entries);
    if (ci == 0) {
      //no conflict
    } else if (ci <= committed_) {
      LOG_FATAL("entry %lu conflict with committed entry [committed(%lu)]", ci, committed_);
    } else {
      assert(ci > 0);
      uint64_t offset = index + 1;
      uint64_t n = ci - offset;
      entries.erase(entries.begin(), entries.begin() + n);
      append(std::move(entries));
    }

    commit_to(std::min(committed, lastnewi));

    last_new_index = lastnewi;
    ok = true;
    return;
  } else {
    last_new_index = 0;
    ok = false;
  }
}
// 将给定的日志条目追加到本地日志中。
uint64_t RaftLog::append(std::vector<proto::EntryPtr> entries) {
    //如果日志条目为空，则返回最后一个日志条目的索引。
  if (entries.empty()) {
    return last_index();
  }
//计算最后一个日志的前一个索引
  uint64_t after = entries[0]->index - 1;
  if (after < committed_) {
    LOG_FATAL("after(%lu) is out of range [committed(%lu)]\", after, committed_", after, committed_);
  }

  unstable_->truncate_and_append(std::move(entries));
  return last_index();
}
// 在给定的日志条目中查找与本地日志冲突的条目。如果找到冲突，返回冲突条目的索引。
uint64_t RaftLog::find_conflict(const std::vector<proto::EntryPtr>& entries) {
  for (const proto::EntryPtr& entry : entries) {
    if (!match_term(entry->index, entry->term)) {
      if (entry->index < last_index()) {
        uint64_t t;
        Status status = this->term(entry->index, t);
        LOG_INFO("found conflict at index %lu [existing term: %lu, conflicting term: %lu], %s",
                 entry->index,
                 t,
                 entry->term,
                 status.to_string().c_str());
      }
      return entry->index;
    }
  }
  return 0;
}
// 获取从 applied_ 之后的未应用日志条目。
void RaftLog::next_entries(std::vector<proto::EntryPtr>& entries) const {
  uint64_t off = std::max(applied_ + 1, first_index());
  if (committed_ + 1 > off) {
    Status status = slice(off, committed_ + 1, max_next_ents_size_, entries);
    if (!status.is_ok()) {
      LOG_FATAL("unexpected error when getting unapplied entries");
    }
  }
}
// 检查是否存在未应用的日志条目。
bool RaftLog::has_next_entries() const {
  uint64_t off = std::max(applied_ + 1, first_index());
  return committed_ + 1 > off;
}
// 尝试将提交索引更新到给定的最大索引 max_index，如果该索引的任期与当前任期匹配。
bool RaftLog::maybe_commit(uint64_t max_index, uint64_t term) {
  if (max_index > committed_) {
    uint64_t t;
    this->term(max_index, t);
    if (t == term) {
      commit_to(max_index);
      return true;
    }
  }
  return false;
}
// 从给定的快照恢复日志状态
void RaftLog::restore(proto::SnapshotPtr snapshot) {
  LOG_INFO("log starts to restore snapshot [index: %lu, term: %lu]",
           snapshot->metadata.index,
           snapshot->metadata.term);
  committed_ = snapshot->metadata.index;
  unstable_->restore(std::move(snapshot));
}
// 获取当前日志的快照
Status RaftLog::snapshot(proto::SnapshotPtr& snap) const {
  if (unstable_->snapshot_) {
    snap = unstable_->snapshot_;
    return Status::ok();
  }

  proto::SnapshotPtr s;
  Status status = storage_->snapshot(s);
  if (s) {
    snap = s;
  }
  return status;
}
// 更新应用索引到指定的索引。
void RaftLog::applied_to(uint64_t index) {
  if (index == 0) {
    return;
  }
  if (committed_ < index || index < applied_) {
    LOG_ERROR("applied(%lu) is out of range [prevApplied(%lu), committed(%lu)]", index, applied_, committed_);
  }
  applied_ = index;
}
// 从日志中截取一段日志条目，从 low 到 high
Status RaftLog::slice(uint64_t low, uint64_t high, uint64_t max_size, std::vector<proto::EntryPtr>& entries) const {
  Status status = must_check_out_of_bounds(low, high);
  if (!status.is_ok()) {
    return status;
  }
  if (low == high) {
    return Status::ok();
  }

  //slice from storage_
  if (low < unstable_->offset_) {
    status = storage_->entries(low, std::min(high, unstable_->offset_), max_size, entries);
    if (!status.is_ok()) {
      return status;
    }

    // check if ents has reached the size limitation
    if (entries.size() < std::min(high, unstable_->offset_) - low) {
      return Status::ok();
    }

  }

  //slice unstable
  if (high > unstable_->offset_) {
    std::vector<proto::EntryPtr> unstable;
    unstable_->slice(std::max(low, unstable_->offset_), high, entries);
    entries.insert(entries.end(), unstable.begin(), unstable.end());
  }
  entry_limit_size(max_size, entries);
  return Status::ok();
}
// 更新提交索引到指定的索引。
void RaftLog::commit_to(uint64_t to_commit) {
  // never decrease commit
  if (committed_ < to_commit) {
    if (last_index() < to_commit) {
      LOG_FATAL("to_commit(%lu) is out of range [lastIndex(%lu)]. Was the raft log corrupted, truncated, or lost?",
                to_commit,
                last_index());
    }
    committed_ = to_commit;
  } else {
    //ignore to_commit < committed_
  }
}
// 检查给定索引的任期是否与本地日志中的任期匹配
bool RaftLog::match_term(uint64_t index, uint64_t t) {
  uint64_t term_out;
  Status status = this->term(index, term_out);
  if (!status.is_ok()) {
    return false;
  }
  return t == term_out;
}
// 获取最后一个日志条目的任期。
uint64_t RaftLog::last_term() const {
  uint64_t t;
  Status status = term(last_index(), t);
  assert(status.is_ok());
  return t;
}
// 获取指定索引的任期。
Status RaftLog::term(uint64_t index, uint64_t& t) const {
  uint64_t dummy_index = first_index() - 1;
  if (index < dummy_index || index > last_index()) {
    // TODO: return an error instead?
    t = 0;
    return Status::ok();
  }

  uint64_t term_index;
  bool ok;

  unstable_->maybe_term(index, term_index, ok);
  if (ok) {
    t = term_index;
    return Status::ok();
  }

  Status status = storage_->term(index, term_index);
  if (status.is_ok()) {
    t = term_index;
  }
  return status;
}
// 获取第一个日志条目的索引。
uint64_t RaftLog::first_index() const {
  uint64_t index;
  bool ok;
  unstable_->maybe_first_index(index, ok);
  if (ok) {
    return index;
  }

  Status status = storage_->first_index(index);
  assert(status.is_ok());

  return index;
}
// 获取最后一个日志条目的索引
//首先尝试从未持久化的日志条目中获取最后一个索引，如果未成功，则从持久化的日志条目中获取。
// 这样可以确保无论日志条目是未持久化还是已持久化，都能正确获取到最后一个日志条目的索引。
uint64_t RaftLog::last_index() const {
  uint64_t index;
  bool ok;
  unstable_->maybe_last_index(index, ok);
  if (ok) {
    return index;
  }

  Status status = storage_->last_index(index);
  assert(status.is_ok());

  return index;
}
// 获取所有日志条目
void RaftLog::all_entries(std::vector<proto::EntryPtr>& entries) {
  entries.clear();
  Status status = this->entries(first_index(), RaftLog::unlimited(), entries);
  if (status.is_ok()) {
    return;
  }

  // try again if there was a racing compaction
  if (status.to_string()
      == Status::invalid_argument("requested index is unavailable due to compaction").to_string()) {
    this->all_entries(entries);
  }
  LOG_FATAL("%s", status.to_string().c_str());
}
// 检查给定的日志条目范围是否超出日志的有效范围
Status RaftLog::must_check_out_of_bounds(uint64_t low, uint64_t high) const {
  assert(high >= low);

  uint64_t first = first_index();

  if (low < first) {
    return Status::invalid_argument("requested index is unavailable due to compaction");
  }

  uint64_t length = last_index() + 1 - first;
  if (low < first || high > first + length) {
    LOG_FATAL("slice[%lu,%lu) out of bound [%lu,%lu]", low, high, first, last_index());
  }
  return Status::ok();

}

}

