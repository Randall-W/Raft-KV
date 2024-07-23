#include "raft/unstable.h"
#include "common/log.h"

namespace kv {

void Unstable::maybe_first_index(uint64_t& index, bool& ok) {
  if (snapshot_) {
    ok = true;
    index = snapshot_->metadata.index + 1;
  } else {
    ok = false;
    index = 0;
  }
}

void Unstable::maybe_last_index(uint64_t& index, bool& ok) {
  if (!entries_.empty()) {
    ok = true;
    index = offset_ + entries_.size() - 1;
    return;
  }
  if (snapshot_) {
    ok = true;
    index = snapshot_->metadata.index;
    return;
  }
  index = 0;
  ok = false;
}

void Unstable::maybe_term(uint64_t index, uint64_t& term, bool& ok) {
  term = 0;
  ok = false;

  if (index < offset_) {
    if (!snapshot_) {
      return;
    }
    if (snapshot_->metadata.index == index) {
      term = snapshot_->metadata.term;
      ok = true;
      return;
    }
    return;
  }

  uint64_t last = 0;
  bool last_ok = false;
  maybe_last_index(last, last_ok);
  if (!last_ok) {
    return;
  }
  if (index > last) {
    return;

  }
  ok = true;
  term = entries_[index - offset_]->term;
}

void Unstable::stable_to(uint64_t index, uint64_t term) {
  uint64_t gt = 0;
  bool ok = false;
  maybe_term(index, gt, ok);

  if (!ok) {
    return;
  }
  // if index < offset, term is matched with the snapshot
  // only update the unstable entries if term is matched with
  // an unstable entry.
  if (gt == term && index >= offset_) {
    uint64_t n = index + 1 - offset_;
    entries_.erase(entries_.begin(), entries_.begin() + n);
    offset_ = index + 1;
  }
}

void Unstable::stable_snap_to(uint64_t index) {
  if (snapshot_ && snapshot_->metadata.index == index) {
    snapshot_ = nullptr;
  }
}

void Unstable::restore(proto::SnapshotPtr snapshot) {
  offset_ = snapshot->metadata.index + 1;
  entries_.clear();
  snapshot_ = snapshot;
}

void Unstable::truncate_and_append(std::vector<proto::EntryPtr> entries) {
  if (entries.empty()) {
    return;
  }
  //获取传入的日志条目集合中第一个日志条目的索引 after
  uint64_t after = entries[0]->index;
  //如果 after 等于当前未持久化日志条目的偏移量 offset_ 加上当前未持久化日志条目的数量 entries_.size()，
  // 说明传入的日志条目集合可以直接追加到当前未持久化日志条目的末尾。
  if (after == offset_ + entries_.size()) {
    // directly append
    entries_.insert(entries_.end(), entries.begin(), entries.end());
  }
//  如果 after 小于或等于当前未持久化日志条目的偏移量 offset_，
//  说明传入的日志条目集合需要替换当前未持久化日志条目的一部分或全部。
//  此时，更新 offset_ 并替换 entries_。
  else if (after <= offset_) {
    // The log is being truncated to before our current offset
    // portion, so set the offset and replace the entries
    LOG_INFO("replace the unstable entries from index %lu", after);
    offset_ = after;
    entries_ = std::move(entries);
  }
  //如果 after 大于当前未持久化日志条目的偏移量 offset_，
  // 说明传入的日志条目集合需要截断当前未持久化日志条目的一部分并追加。
  // 此时，调用 slice 方法截取从 offset_ 到 after 的日志条目，
  // 并将传入的日志条目集合追加到截取的日志条目集合中，最后替换 entries_。
  else {
    // truncate to after and copy entries_
    // then append
    LOG_INFO("truncate the unstable entries before index %lu", after);
    std::vector<proto::EntryPtr> entries_slice;
    this->slice(offset_, after, entries_slice);

    entries_slice.insert(entries_slice.end(), entries.begin(), entries.end());
    entries_ = std::move(entries_slice);
  }
}

void Unstable::slice(uint64_t low, uint64_t high, std::vector<proto::EntryPtr>& entries) {
  assert(high > low);
  uint64_t upper = offset_ + entries_.size();
  if (low < offset_ || high > upper) {
    LOG_FATAL("unstable.slice[%lu,%lu) out of bound [%lu,%lu]", low, high, offset_, upper);
  }

  entries.insert(entries.end(), entries_.begin() + low - offset_, entries_.begin() + high - offset_);
}

}
