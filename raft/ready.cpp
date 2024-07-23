#include "raft/ready.h"
#include "raft/raft.h"
#include "raft/util.h"

namespace kv {

Ready::Ready(std::shared_ptr<Raft> raft, SoftStatePtr pre_soft_state, const proto::HardState& pre_hard_state)
    : entries(raft->raft_log_->unstable_entries()) {
  std::swap(this->messages, raft->msgs_);

  raft->raft_log_->next_entries(committed_entries);

  SoftStatePtr st = raft->soft_state();
  if (!st->equal(*pre_soft_state)) {
    this->soft_state = st;
  }

  proto::HardState hs = raft->hard_state();
  if (!hs.equal(pre_hard_state)) {
    this->hard_state = hs;
  }

  proto::SnapshotPtr snapshot = raft->raft_log_->unstable_->snapshot_;
  if (snapshot) {
    //copy
    this->snapshot = *snapshot;
  }
  if (!raft->read_states_.empty()) {
    this->read_states = raft->read_states_;
  }

  this->must_sync = is_must_sync(hs, hard_state, entries.size());
}

bool Ready::contains_updates() const {
  return soft_state != nullptr || !hard_state.is_empty_state() ||
      !snapshot.is_empty() || !entries.empty() ||
      !committed_entries.empty() || !messages.empty() || read_states.empty();
}

uint64_t Ready::applied_cursor() const {
  if (!committed_entries.empty()) {
    return committed_entries.back()->index;
  }
  uint64_t index = snapshot.metadata.index;
  if (index > 0) {
    return index;
  }
  return 0;
}

bool Ready::equal(const Ready& rd) const {
  if ((soft_state && !rd.soft_state) || (!soft_state && rd.soft_state)) {
    return false;
  }
  if (soft_state && rd.soft_state && !soft_state->equal(*rd.soft_state)) {
    return false;
  }
  if (!hard_state.equal(rd.hard_state)) {
    return false;
  }

  if (read_states.size() != read_states.size()) {
    return false;
  }

  for (size_t i = 0; i < read_states.size(); ++i) {
    if (!read_states[i].equal(rd.read_states[i])) {
      return false;
    }
  }

  if (entries.size() != rd.entries.size()) {
    return false;
  }

  for (size_t i = 0; i < entries.size(); ++i) {
    if (*entries[i] != *rd.entries[i]) {
      return false;
    }
  }

  if (!snapshot.equal(rd.snapshot)) {
    return false;
  }

  if (committed_entries.size() != rd.committed_entries.size()) {
    return false;
  }

  for (size_t i = 0; i < committed_entries.size(); ++i) {
    if (*committed_entries[i] != *rd.committed_entries[i]) {
      return false;
    }
  }
  return must_sync == rd.must_sync;
}

}

