#include <msgpack.hpp>
#include "server/redis_store.h"
#include "server/raft_node.h"
#include "common/log.h"
#include "server/redis_session.h"

namespace kv {
// 这段代码实现了一个分布式Redis存储服务，它通过Raft协议来保证数据的一致性。
// 服务能够处理Redis命令，将这些命令转换为Raft日志条目，并在Raft集群中复制这些日志条目。
// 此外，服务还支持快照功能，用于数据恢复和状态保存。通过使用异步I/O和多线程，服务能够高效地处理多个并发请求。

// see redis keys command
// 这个函数实现了类似于Redis的 KEYS 命令中的模式匹配功能，用于在存储中查找匹配特定模式的键。
int string_match_len(const char* pattern, int patternLen,
                     const char* string, int stringLen, int nocase) {
  while (patternLen && stringLen) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (string_match_len(pattern + 1, patternLen - 1,
                               string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      case '[': {
        int not_match, match;

        pattern++;
        patternLen--;
        not_match = pattern[0] == '^';
        if (not_match) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\' && patternLen >= 2) {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0])
              match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (pattern[1] == '-' && patternLen >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end)
              match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0])
                match = 1;
            } else {
              if (tolower((int) pattern[0]) == tolower((int) string[0]))
                match = 1;
            }
          }
          pattern++;
          patternLen--;
        }
        if (not_match)
          match = !match;
        if (!match)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower((int) pattern[0]) != tolower((int) string[0]))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}
// 构造函数初始化Redis存储服务，包括设置Raft节点、网络接受器、请求ID等。如果提供了快照数据，它会尝试从快照中恢复键值对。
RedisStore::RedisStore(RaftNode* server, std::vector<uint8_t> snap, uint16_t port)
    : server_(server),
      acceptor_(io_service_),
      next_request_id_(0) {

  if (!snap.empty()) {
    std::unordered_map<std::string, std::string> kv;
    msgpack::object_handle oh = msgpack::unpack((const char*) snap.data(), snap.size());
    try {
      oh.get().convert(kv);
    } catch (std::exception& e) {
      LOG_WARN("invalid snapshot");
    }
    std::swap(kv, key_values_);
  }

  auto address = boost::asio::ip::address::from_string("0.0.0.0");
  auto endpoint = boost::asio::ip::tcp::endpoint(address, port);

  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
  acceptor_.bind(endpoint);
  acceptor_.listen();
}

RedisStore::~RedisStore() {
  if (worker_.joinable()) {
    worker_.join();
  }
}
// start 函数启动一个新线程来运行I/O服务，并开始接受新的客户端连接。
void RedisStore::start(std::promise<pthread_t>& promise) {
  start_accept();

  worker_ = std::thread([this, &promise]() {
    promise.set_value(pthread_self());
    this->io_service_.run();
  });
}
// start_accept 函数异步接受新的客户端连接，并为每个新连接创建一个新的Redis会话。
void RedisStore::start_accept() {
  RedisSessionPtr session(new RedisSession(this, io_service_));

  acceptor_.async_accept(session->socket_, [this, session](const boost::system::error_code& error) {
    if (error) {
      LOG_DEBUG("accept error %s", error.message().c_str());
      return;
    }
    this->start_accept();
    session->start();
  });
}
// 这些函数将Redis的 SET 和 DEL 命令转换为Raft协议的日志条目，并提交给Raft节点。
// 每个命令都有一个唯一的请求ID，用于跟踪命令的执行状态。
void RedisStore::set(std::string key, std::string value, const StatusCallback& callback) {
  uint32_t commit_id = next_request_id_++;

  RaftCommit commit;
  commit.node_id = static_cast<uint32_t>(server_->node_id());
  commit.commit_id = commit_id;
  commit.redis_data.type = RedisCommitData::kCommitSet;
  commit.redis_data.strs.push_back(std::move(key));
  commit.redis_data.strs.push_back(std::move(value));

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, commit);
  std::shared_ptr<std::vector<uint8_t>> data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));

  pending_requests_[commit_id] = callback;

  server_->propose(std::move(data), [this, commit_id](const Status& status) {
    io_service_.post([this, status, commit_id]() {
      if (status.is_ok()) {
        return;
      }
      auto it = pending_requests_.find(commit_id);
      if (it != pending_requests_.end()) {
        it->second(status);
        pending_requests_.erase(it);
      }
    });
  });
}

void RedisStore::del(std::vector<std::string> keys, const StatusCallback& callback) {
  uint32_t commit_id = next_request_id_++;

  RaftCommit commit;
  commit.node_id = static_cast<uint32_t>(server_->node_id());
  commit.commit_id = commit_id;
  commit.redis_data.type = RedisCommitData::kCommitDel;
  commit.redis_data.strs = std::move(keys);
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, commit);
  std::shared_ptr<std::vector<uint8_t>> data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));

  pending_requests_[commit_id] = callback;

  server_->propose(std::move(data), [this, commit_id](const Status& status) {
    io_service_.post([commit_id, status, this]() {
      auto it = pending_requests_.find(commit_id);
      if (it != pending_requests_.end()) {
        it->second(status);
        pending_requests_.erase(it);
      }
    });
  });
}
// get_snapshot 函数生成当前键值存储的快照
void RedisStore::get_snapshot(const GetSnapshotCallback& callback) {
  io_service_.post([this, callback] {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, this->key_values_);
    SnapshotDataPtr data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));
    callback(data);
  });
}
// recover_from_snapshot 函数从提供的快照数据中恢复键值存储。
void RedisStore::recover_from_snapshot(SnapshotDataPtr snap, const StatusCallback& callback) {
  io_service_.post([this, snap, callback] {
    std::unordered_map<std::string, std::string> kv;
    msgpack::object_handle oh = msgpack::unpack((const char*) snap->data(), snap->size());
    try {
      oh.get().convert(kv);
    } catch (std::exception& e) {
      callback(Status::io_error("invalid snapshot"));
      return;
    }
    std::swap(kv, key_values_);
    callback(Status::ok());
  });
}
// 这个函数根据提供的模式查找匹配的键，并返回这些键的列表
void RedisStore::keys(const char* pattern, int len, std::vector<std::string>& keys) {
  for (auto it = key_values_.begin(); it != key_values_.end(); ++it) {
    if (string_match_len(pattern, len, it->first.c_str(), it->first.size(), 0)) {
      keys.push_back(it->first);
    }
  }
}
// read_commit 函数处理Raft节点提交的日志条目，根据日志条目的内容更新键值存储，并处理相关的回调。
void RedisStore::read_commit(proto::EntryPtr entry) {
  auto cb = [this, entry] {
    RaftCommit commit;
    try {
      msgpack::object_handle oh = msgpack::unpack((const char*) entry->data.data(), entry->data.size());
      oh.get().convert(commit);

    }
    catch (std::exception& e) {
      LOG_ERROR("bad entry %s", e.what());
      return;
    }
    RedisCommitData& data = commit.redis_data;

    switch (data.type) {
      case RedisCommitData::kCommitSet: {
        assert(data.strs.size() == 2);
        this->key_values_[std::move(data.strs[0])] = std::move(data.strs[1]);
        break;
      }
      case RedisCommitData::kCommitDel: {
        for (const std::string& key : data.strs) {
          this->key_values_.erase(key);
        }
        break;
      }
      default: {
        LOG_ERROR("not supported type %d", data.type);
      }
    }

    if (commit.node_id == server_->node_id()) {
      auto it = pending_requests_.find(commit.commit_id);
      if (it != pending_requests_.end()) {
        it->second(Status::ok());
        pending_requests_.erase(it);
      }
    }
  };

  io_service_.post(std::move(cb));
}

}

