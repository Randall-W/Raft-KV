#include "raft/util.h"
#include "common/log.h"
#include <boost/crc.hpp>

namespace kv {

void entry_limit_size(uint64_t max_size, std::vector<proto::EntryPtr>& entries) {
  if (entries.empty()) {
    return;
  }

  uint64_t size = entries[0]->serialize_size();
  for (size_t limit = 1; limit < entries.size(); ++limit) {
    size += entries[limit]->serialize_size();
    if (size > max_size) {
      entries.resize(limit);
      break;
    }
  }
}

proto::MessageType vote_resp_msg_type(proto::MessageType type) {
  switch (type) {
    case proto::MsgVote: {
      return proto::MsgVoteResp;
    }
    case proto::MsgPreVote: {
      return proto::MsgPreVoteResp;
    }
    default: {
      LOG_FATAL("not a vote message: %s", proto::msg_type_to_string(type));
    }
  }
}

bool is_local_msg(proto::MessageType type) {
  return type == proto::MsgHup || type == proto::MsgBeat || type == proto::MsgUnreachable ||
      type == proto::MsgSnapStatus || type == proto::MsgCheckQuorum;
}

uint32_t compute_crc32(const char* data, size_t len) {
  boost::crc_32_type crc32;
  crc32.process_bytes(data, len);
  //调用 crc32 对象的 operator()，它会返回当前计算的 CRC32 校验和值
  return crc32();
}

// must_sync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
bool is_must_sync(const proto::HardState& st, const proto::HardState& prevst, size_t entsnum) {
  // Persistent state on all servers:
  // (Updated on stable storage before responding to RPCs)
  // currentTerm
  // votedFor
  // log entries[]
  return entsnum != 0 || st.vote != prevst.vote || st.term != prevst.term;
}


}

