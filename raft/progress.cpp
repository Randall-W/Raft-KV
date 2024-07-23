#include "raft/progress.h"
#include "common/log.h"

namespace kv {
// 将 ProgressState 枚举类型转换为对应的字符串表示，用于日志记录和调试。
const char* progress_state_to_string(ProgressState state) {
  switch (state) {
    case ProgressStateProbe: {
      return "ProgressStateProbe";
    }
    case ProgressStateReplicate: {
      return "ProgressStateReplicate";
    }
    case ProgressStateSnapshot: {
      return "ProgressStateSnapshot";
    }
    default: {
      LOG_FATAL("unknown state %d", state);
    }
  }
}
//  add 方法用于添加一个新的正在传输的日志条目索引。如果缓冲区已满，则抛出致命错误。
void InFlights::add(uint64_t inflight) {
  if (is_full()) {
    LOG_FATAL("cannot add into a full inflights");
  }

  uint64_t next = start + count;

  if (next >= size) {
    next -= size;
  }
  if (next >= buffer.size()) {
    uint32_t new_size = buffer.size() * 2;
    if (new_size == 0) {
      new_size = 1;
    } else if (new_size > size) {
      new_size = size;
    }
    buffer.resize(new_size);
  }
  buffer[next] = inflight;
  count++;
}
//    free_to 方法用于释放到指定索引的日志条目。
void InFlights::free_to(uint64_t to) {
  if (count == 0 || to < buffer[start]) {
    // out of the left side of the window
    return;
  }

  uint32_t idx = start;
  size_t i;
  for (i = 0; i < count; i++) {
    if (to < buffer[idx]) { // found the first large inflight
      break;
    }

    // increase index and maybe rotate
    idx++;

    if (idx >= size) {
      idx -= size;
    }
  }
  // free i inflights and set new start index
  count -= i;
  start = idx;
  if (count == 0) {
    // inflights is empty, reset the start index so that we don't grow the
    // buffer unnecessarily.
    start = 0;
  }
}
//    free_first_one 方法用于释放第一个正在传输的日志条目。
void InFlights::free_first_one() {
  free_to(buffer[start]);
}
//    Progress 类用于跟踪每个跟随者的复制进度。
//become_replicate 方法将状态切换为 ProgressStateReplicate，并更新 next 索引。
void Progress::become_replicate() {
  reset_state(ProgressStateReplicate);
  next = match + 1;
}
//become_probe 方法将状态切换为 ProgressStateProbe，并更新 next 索引。
void Progress::become_probe() {
  // If the original state is ProgressStateSnapshot, progress knows that
  // the pending snapshot has been sent to this peer successfully, then
  // probes from pendingSnapshot + 1.
  if (state == ProgressStateSnapshot) {
    uint64_t pending = pending_snapshot;
    reset_state(ProgressStateProbe);
    next = std::max(match + 1, pending + 1);
  } else {
    reset_state(ProgressStateProbe);
    next = match + 1;
  }
}
//become_snapshot 方法将状态切换为 ProgressStateSnapshot，并设置 pending_snapshot。
void Progress::become_snapshot(uint64_t snapshoti) {
  reset_state(ProgressStateSnapshot);
  pending_snapshot = snapshoti;
}
//reset_state 方法用于重置 Progress 对象的状态。
void Progress::reset_state(ProgressState st) {
  paused = false;
  pending_snapshot = 0;
  this->state = st;
  this->inflights->reset();
}
//string 方法返回 Progress 对象的字符串表示，用于日志记录和调试。
std::string Progress::string() const {
  char buffer[256];
  int n = snprintf(buffer,
                   sizeof(buffer),
                   "next = %lu, match = %lu, state = %s, waiting = %d, pendingSnapshot = %lu",
                   next,
                   match,
                   progress_state_to_string(state),
                   is_paused(),
                   pending_snapshot);
  return std::string(buffer, n);
}
//is_paused 方法用于判断 Progress 对象是否处于暂停状态。
bool Progress::is_paused() const {
  switch (state) {
    case ProgressStateProbe: {
      return paused;
    }
    case ProgressStateReplicate: {
      return inflights->is_full();
    }
    case ProgressStateSnapshot: {
      return true;
    }
    default: {
      LOG_FATAL("unexpected state");
    }
  }
}
//maybe_update 方法用于更新 match 和 next 索引。
bool Progress::maybe_update(uint64_t n) {
  bool updated = false;
  if (match < n) {
    match = n;
    updated = true;
    resume();
  }
  if (next < n + 1) {
    next = n + 1;
  }
  return updated;
}
//maybe_decreases_to 方法用于减少 next 索引。
bool Progress::maybe_decreases_to(uint64_t rejected, uint64_t last) {
  if (state == ProgressStateReplicate) {
    // the rejection must be stale if the progress has matched and "rejected"
    // is smaller than "match".
    if (rejected <= match) {
      return false;
    }
    // directly decrease next to match + 1
    next = match + 1;
    return true;
  }

  // the rejection must be stale if "rejected" does not match next - 1
  if (next - 1 != rejected) {
    return false;
  }

  next = std::min(rejected, last + 1);
  if (next < 1) {
    next = 1;
  }
  resume();
  return true;
}
//need_snapshot_abort 方法用于判断是否需要中止快照传输。
bool Progress::need_snapshot_abort() const {
  return state == ProgressStateSnapshot && match >= pending_snapshot;
}

}
