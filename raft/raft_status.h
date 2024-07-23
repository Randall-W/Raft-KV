#pragma once
#include "raft/proto.h"

namespace kv {

struct RaftStatus {
  uint64_t id;

};
typedef std::shared_ptr<RaftStatus> RaftStatusPtr;

}
