#pragma once
#include <vector>
#include "raft/proto.h"
#include "raft/config.h"

namespace kv {

// ReadState provides state for read only query.
// It's caller's responsibility to call read_index first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// request_ctx
struct ReadState {
  bool equal(const ReadState& rs) const {
    if (index != rs.index) {
      return false;
    }
    return request_ctx == rs.request_ctx;
  }
  uint64_t index;
  std::vector<uint8_t> request_ctx;
};

struct ReadIndexStatus {
  proto::Message req;
  uint64_t index;
  std::unordered_set<uint64_t> acks;

};
typedef std::shared_ptr<ReadIndexStatus> ReadIndexStatusPtr;

struct ReadOnly {
  explicit ReadOnly(ReadOnlyOption option)
      : option(option) {}

  // last_pending_request_ctx returns the context of the last pending read only
  // request in readonly struct.
  void last_pending_request_ctx(std::vector<uint8_t>& ctx);

  uint32_t recv_ack(const proto::Message& msg);

  std::vector<ReadIndexStatusPtr> advance(const proto::Message& msg);

  void add_request(uint64_t index, proto::MessagePtr msg);

  ReadOnlyOption option;
  std::unordered_map<std::string, ReadIndexStatusPtr> pending_read_index;
  std::vector<std::string> read_index_queue;
};
typedef std::shared_ptr<ReadOnly> ReadOnlyPtr;

}
