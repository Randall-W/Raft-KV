#pragma once
#include <stdint.h>
#include <vector>
#include <msgpack.hpp>

namespace kv {

namespace proto {

typedef uint8_t MessageType;

const MessageType MsgHup = 0;
const MessageType MsgBeat = 1;
const MessageType MsgProp = 2;
const MessageType MsgApp = 3;
const MessageType MsgAppResp = 4;
const MessageType MsgVote = 5;
const MessageType MsgVoteResp = 6;
const MessageType MsgSnap = 7;
const MessageType MsgHeartbeat = 8;
const MessageType MsgHeartbeatResp = 9;
const MessageType MsgUnreachable = 10;
const MessageType MsgSnapStatus = 11;
const MessageType MsgCheckQuorum = 12;
const MessageType MsgTransferLeader = 13;
const MessageType MsgTimeoutNow = 14;
const MessageType MsgReadIndex = 15;
const MessageType MsgReadIndexResp = 16;
const MessageType MsgPreVote = 17;
const MessageType MsgPreVoteResp = 18;

const MessageType MsgTypeSize = 19;

const char* msg_type_to_string(MessageType type);

typedef uint8_t EntryType;

const EntryType EntryNormal = 0;
const EntryType EntryConfChange = 1;

const char* entry_type_to_string(EntryType type);

struct Entry {
  Entry()
      : type(EntryNormal),
        term(0),
        index(0) {}

  explicit Entry(Entry&& entry)
      : type(entry.type),
        term(entry.term),
        index(entry.index),
        data(std::move(entry.data)) {

  }

  kv::proto::Entry& operator=(const kv::proto::Entry& entry) = default;
  Entry(const Entry& entry) = default;

  explicit Entry(EntryType type, uint64_t term, uint64_t index, std::vector<uint8_t> data)
      : type(type),
        term(term),
        index(index),
        data(std::move(data)) {}

  uint32_t serialize_size() const;

  uint32_t payload_size() const {
    return static_cast<uint32_t>(data.size());
  }

  bool operator==(const Entry& entry) const {
    return type == entry.type && term == entry.term && index == entry.index && data == entry.data;
  }
  bool operator!=(const Entry& entry) const {
    return !(*this == entry);
  }

  EntryType type;
  uint64_t term;
  uint64_t index;
  std::vector<uint8_t> data;
  // 定义类序列化的方式
  MSGPACK_DEFINE (type, term, index, data);
};
typedef std::shared_ptr<Entry> EntryPtr;

struct ConfState {
  bool operator==(const ConfState& cs) const {
    return nodes == cs.nodes && learners == cs.learners;
  }

  std::vector<uint64_t> nodes;
  std::vector<uint64_t> learners;
  MSGPACK_DEFINE (nodes, learners);
};
typedef std::shared_ptr<ConfState> ConfStatePtr;

struct SnapshotMetadata {
  SnapshotMetadata()
      : index(0),
        term(0) {
  }

  bool operator==(const SnapshotMetadata& meta) const {
    return conf_state == meta.conf_state && index == meta.index && term == meta.term;
  }

  ConfState conf_state;
  uint64_t index;
  uint64_t term;
  MSGPACK_DEFINE (conf_state, index, term);
};

struct Snapshot {
  Snapshot() = default;

  explicit Snapshot(const std::vector<uint8_t>& data)
      : data(data) {
  }

  bool equal(const Snapshot& snap) const;

  bool is_empty() const {
    return metadata.index == 0;
  }
  std::vector<uint8_t> data;
  SnapshotMetadata metadata;
  MSGPACK_DEFINE (data, metadata);
};
typedef std::shared_ptr<Snapshot> SnapshotPtr;

struct Message {
  Message()
      : type(MsgHup),
        to(0),
        from(0),
        term(0),
        log_term(0),
        index(0),
        commit(0),
        reject(false),
        reject_hint(0) {

  }

  bool operator==(const Message& msg) const {
    return type == msg.type && to == msg.to && from == msg.from && term == msg.term
        && log_term == msg.log_term && index == msg.index
        && entries == msg.entries && commit == msg.commit
        && snapshot.equal(msg.snapshot) && reject == msg.reject
        && reject_hint == msg.reject_hint && context == msg.context;
  }

  bool is_local_msg() const;

  bool is_response_msg() const;

  MessageType type;
  uint64_t to;
  uint64_t from;
  uint64_t term;
  uint64_t log_term;
  uint64_t index;
  std::vector<Entry> entries;
  uint64_t commit;
  Snapshot snapshot;
  bool reject;
  uint64_t reject_hint;
  std::vector<uint8_t> context;
  MSGPACK_DEFINE (type, to, from, term, log_term, index, entries, commit, snapshot, reject, reject_hint, context);
};
typedef std::shared_ptr<Message> MessagePtr;

struct HardState {
  HardState()
      : term(0),
        vote(0),
        commit(0) {
  }

  bool is_empty_state() const {
    return term == 0 && vote == 0 && commit == 0;
  }

  bool equal(const HardState& hs) const {
    return term == hs.term && vote == hs.vote && commit == hs.commit;
  }

  uint64_t term;
  uint64_t vote;
  uint64_t commit;
  MSGPACK_DEFINE (term, vote, commit);
};

const uint8_t ConfChangeAddNode = 0;
const uint8_t ConfChangeRemoveNode = 1;
const uint8_t ConfChangeUpdateNode = 2;
const uint8_t ConfChangeAddLearnerNode = 3;

struct ConfChange {
  static void from_data(const std::vector<uint8_t>& data, ConfChange& cc);
  uint64_t id;
  uint8_t conf_change_type;
  uint64_t node_id;
  std::vector<uint8_t> context;
  MSGPACK_DEFINE (id, conf_change_type, node_id, context);
  std::vector<uint8_t> serialize() const;
};
typedef std::shared_ptr<ConfChange> ConfChangePtr;

}
}