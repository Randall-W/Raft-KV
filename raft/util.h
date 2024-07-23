#pragma once
#include "raft/proto.h"

namespace kv {

void entry_limit_size(uint64_t max_size, std::vector<proto::EntryPtr>& entries);

// vote_resp_msg_type maps vote and prevote message types to their corresponding responses.
proto::MessageType vote_resp_msg_type(proto::MessageType type);

bool is_local_msg(proto::MessageType type);

uint32_t compute_crc32(const char* data, size_t len);

bool is_must_sync(const proto::HardState& st, const proto::HardState& prevst, size_t entsnum);

}
