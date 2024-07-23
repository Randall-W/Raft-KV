#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <future>
#include "server/raft_node.h"
#include "common/log.h"

namespace kv {
// 定义了快照的默认计数和补充日志条目的数量。
static uint64_t defaultSnapCount = 100000;
static uint64_t snapshotCatchUpEntriesN = 100000;

RaftNode::RaftNode(uint64_t id, const std::string& cluster, uint16_t port)
    : port_(port),
      pthread_id_(0),
      //timer 对象被绑定到 io_service 对象。这意味着当定时器到期时，它将使用 io_service 来调用相应的回调函数
      timer_(io_service_),
      id_(id),
      last_index_(0),
      conf_state_(new proto::ConfState()),
      snapshot_index_(0),
      applied_index_(0),
      storage_(new MemoryStorage()),
      snap_count_(defaultSnapCount) {
// 解析集群配置：
// 使用 boost::split 函数将 cluster 字符串分割成 peers_ 向量，每个元素是集群中一个节点的地址。
// 检查 peers_ 是否为空，如果为空则记录致命错误日志并退出。
  boost::split(peers_, cluster, boost::is_any_of(","));
  if (peers_.empty()) {
    LOG_FATAL("invalid args %s", cluster.c_str());
  }

  std::string work_dir = "node_" + std::to_string(id);
  snap_dir_ = work_dir + "/snap";
  wal_dir_ = work_dir + "/wal";

  if (!boost::filesystem::exists(snap_dir_)) {
    boost::filesystem::create_directories(snap_dir_);
  }

  snapshotter_.reset(new Snapshotter(snap_dir_));

  bool wal_exists = boost::filesystem::exists(wal_dir_);

  replay_WAL();

  Config c;
  //id：本地 Raft 节点的唯一标识符。
  c.id = id;
  // election_tick 和 heartbeat_tick：分别定义了选举超时和心跳间隔的时间单位。
  //election_tick 必须大于 heartbeat_tick。
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  //storage：Raft 使用的存储接口，用于持久化日志条目和状态。
  c.storage = storage_;
  //applied：最后应用的日志索引。仅在重启 Raft 时设置。
  c.applied = 0;
  // max_size_per_msg : 限制了消息的最大大小
  c.max_size_per_msg = 1024 * 1024;
  // max_committed_size_per_ready : 准备应用的已提交日志的最大大小
  c.max_committed_size_per_ready = 0;
  // max_uncommitted_entries_size : 未提交日志的最大总大小
  c.max_uncommitted_entries_size = 1 << 30;
  // max_inflight_msgs：飞行中的消息的最大数量。
  c.max_inflight_msgs = 256;
  // check_quorum：指定领导者是否应检查法定人数的活动。如果法定人数在选举超时内不活跃，领导者将下台。
  c.check_quorum = true;
  // pre_vote：启用预投票算法，以防止在分区节点重新加入集群时造成干扰。
  c.pre_vote = true;
  // read_only_option：指定如何处理只读请求。可以选择 ReadOnlySafe 或 ReadOnlyLeaseBased。
  c.read_only_option = ReadOnlySafe;
  // disable_proposal_forwarding：如果设置为 true，则跟随者将丢弃提案而不是转发给领导者。
  c.disable_proposal_forwarding = false;

  Status status = c.validate();

  if (!status.is_ok()) {
    LOG_FATAL("invalid configure %s", status.to_string().c_str());
  }

  if (wal_exists) 
  {
    node_.reset(Node::restart_node(c));
  } 
  else 
  {
    //PeerContext 是一个结构体
    std::vector<PeerContext> peers;
    for (size_t i = 0; i < peers_.size(); ++i) 
    {
      peers.push_back(PeerContext{.id = i + 1});
    }
    node_.reset(Node::start_node(c, peers));
  }
}

RaftNode::~RaftNode() {
  LOG_DEBUG("stopped");
  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
}
// start_timer 函数启动一个定时器，
// 用于定期触发事件处理函数 pull_ready_events。
void RaftNode::start_timer() {
//async_wait 方法会将定时器到期后的处理任务提交到 I/O 服务队列中，然后立即返回，允许当前线程继续执行后续代码。
// 当定时器到期时，I/O 服务会在后台线程中调用回调函数，执行定时器到期后的操作。
//因此，this->start_timer(); 调用后，当前线程会继续执行 this->node_->tick();
//和 this->pull_ready_events();，而不会被阻塞。
  timer_.expires_from_now(boost::posix_time::millisec(100));
  timer_.async_wait([this](const boost::system::error_code& err) {
    if (err) {
      LOG_ERROR("timer waiter error %s", err.message().c_str());
      return;
    }

    this->start_timer();
    this->node_->tick();
    this->pull_ready_events();
  });
}
// pull_ready_events 函数处理节点的各种事件，包括保存日志条目、处理快照、发布日志条目、发送消息等。
void RaftNode::pull_ready_events() {
  assert(pthread_id_ == pthread_self());
  while (node_->has_ready()) {
    auto rd = node_->ready();
    if (!rd->contains_updates()) {
      LOG_WARN("ready not contains updates");
      return;
    }
//将硬状态和日志条目保存到预写日志（WAL）中。
    wal_->save(rd->hard_state, rd->entries);
//如果准备好的事件包含快照，则保存快照，应用快照到存储中，并发布快照。
    if (!rd->snapshot.is_empty()) {
      Status status = save_snap(rd->snapshot);
      if (!status.is_ok()) {
        LOG_FATAL("save snapshot error %s", status.to_string().c_str());
      }
      storage_->apply_snapshot(rd->snapshot);
      publish_snapshot(rd->snapshot);
    }
//如果准备好的事件包含日志条目，则将日志条目追加到存储中。
    if (!rd->entries.empty()) {
      storage_->append(rd->entries);
    }
    //如果准备好的事件包含消息，则发送消息。
    if (!rd->messages.empty()) {
      transport_->send(rd->messages);
    }
//如果准备好的事件包含已提交的日志条目，则将这些条目转换为应用的条目，并发布这些条目。
    if (!rd->committed_entries.empty()) {
      std::vector<proto::EntryPtr> ents;
      entries_to_apply(rd->committed_entries, ents);
      if (!ents.empty()) {
        publish_entries(ents);
      }
    }
    //可能触发快照操作。
    maybe_trigger_snapshot();
    //推进 node_ 对象的状态，以便处理下一个准备好的事件。
    node_->advance(rd);
  }
}

// 这些函数负责快照的保存、发布和触发
// save_snap 将快照保存到WAL和快照存储中。
Status RaftNode::save_snap(const proto::Snapshot& snap) {
  // must save the snapshot index to the WAL before saving the
  // snapshot to maintain the invariant that we only Open the
  // wal at previously-saved snapshot indexes.
  Status status;

  WAL_Snapshot wal_snapshot;
  wal_snapshot.index = snap.metadata.index;
  wal_snapshot.term = snap.metadata.term;

  status = wal_->save_snapshot(wal_snapshot);
  if (!status.is_ok()) {
    return status;
  }

  status = snapshotter_->save_snap(snap);
  if (!status.is_ok()) {
    LOG_FATAL("save snapshot error %s", status.to_string().c_str());
  }
  return status;

  return wal_->release_to(snap.metadata.index);
}
// publish_snapshot 发布快照并更新系统状态。
void RaftNode::publish_snapshot(const proto::Snapshot& snap) {
  if (snap.is_empty()) {
    return;
  }

  LOG_DEBUG("publishing snapshot at index %lu", snapshot_index_);

  if (snap.metadata.index <= applied_index_) {
    LOG_FATAL("snapshot index [%lu] should > progress.appliedIndex [%lu] + 1", snap.metadata.index, applied_index_);
  }

  //trigger to load snapshot
  proto::SnapshotPtr snapshot(new proto::Snapshot());
  snapshot->metadata = snap.metadata;
  SnapshotDataPtr data(new std::vector<uint8_t>(snap.data));

  *(this->conf_state_) = snapshot->metadata.conf_state;
  snapshot_index_ = snapshot->metadata.index;
  applied_index_ = snapshot->metadata.index;

  redis_server_->recover_from_snapshot(data, [snapshot, this](const Status& status) {
    //由redis线程回调
    if (!status.is_ok()) {
      LOG_FATAL("recover from snapshot error %s", status.to_string().c_str());
    }

    LOG_DEBUG("finished publishing snapshot at index %lu", snapshot_index_);
  });

}

void RaftNode::open_WAL(const proto::Snapshot& snap) {
  if (!boost::filesystem::exists(wal_dir_)) {
    boost::filesystem::create_directories(wal_dir_);
    WAL::create(wal_dir_);
  }

  WAL_Snapshot walsnap;
  walsnap.index = snap.metadata.index;
  walsnap.term = snap.metadata.term;
  LOG_INFO("loading WAL at term %lu and index %lu", walsnap.term, walsnap.index);

  wal_ = WAL::open(wal_dir_, walsnap);
}
// replay_WAL 函数用于重放WAL日志，以恢复节点的状态。
// 它会加载快照并应用到存储，然后读取WAL日志条目并追加到存储。
void RaftNode::replay_WAL() {
  LOG_DEBUG("replaying WAL of member %lu", id_);

  proto::Snapshot snapshot;
  Status status = snapshotter_->load(snapshot);
  if (!status.is_ok()) {
    if (status.is_not_found()) {
      LOG_INFO("snapshot not found for node %lu", id_);
    } else {
      LOG_FATAL("error loading snapshot %s", status.to_string().c_str());
    }
  } else {
    storage_->apply_snapshot(snapshot);
  }

  open_WAL(snapshot);
  assert(wal_ != nullptr);

  proto::HardState hs;
  std::vector<proto::EntryPtr> ents;
  status = wal_->read_all(hs, ents);
  if (!status.is_ok()) {
    LOG_FATAL("failed to read WAL %s", status.to_string().c_str());
  }

  storage_->set_hard_state(hs);

  // append to storage so raft starts at the right place in log
  storage_->append(ents);

  // send nil once lastIndex is published so client knows commit channel is current
  if (!ents.empty()) {
    last_index_ = ents.back()->index;
  } else {
    snap_data_ = std::move(snapshot.data);
  }
}
// publish_entries 发布日志条目并处理配置变化。
bool RaftNode::publish_entries(const std::vector<proto::EntryPtr>& entries) {
  for (const proto::EntryPtr& entry : entries) {
    switch (entry->type) {
      case proto::EntryNormal: {
        if (entry->data.empty()) {
          // ignore empty messages
          break;
        }
        redis_server_->read_commit(entry);
        break;
      }

      case proto::EntryConfChange: {
        proto::ConfChange cc;
        try {
          msgpack::object_handle oh = msgpack::unpack((const char*) entry->data.data(), entry->data.size());
          oh.get().convert(cc);
        }
        catch (std::exception& e) {
          LOG_ERROR("invalid EntryConfChange msg %s", e.what());
          continue;
        }
        conf_state_ = node_->apply_conf_change(cc);

        switch (cc.conf_change_type) {
          case proto::ConfChangeAddNode:
            if (!cc.context.empty()) {
              std::string str((const char*) cc.context.data(), cc.context.size());
              transport_->add_peer(cc.node_id, str);
            }
            break;
          case proto::ConfChangeRemoveNode:
            if (cc.node_id == id_) {
              LOG_INFO("I've been removed from the cluster! Shutting down.");
              return false;
            }
            transport_->remove_peer(cc.node_id);
          default: {
            LOG_INFO("configure change %d", cc.conf_change_type);
          }
        }
        break;
      }
      default: {
        LOG_FATAL("unknown type %d", entry->type);
        return false;
      }
    }

    // after commit, update appliedIndex
    applied_index_ = entry->index;

    // replay has finished
    if (entry->index == this->last_index_) {
      LOG_DEBUG("replay has finished");
    }
  }
  return true;
}
// entries_to_apply 选择需要应用的日志条目。
void RaftNode::entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents) {
  if (entries.empty()) {
    return;
  }

  uint64_t first = entries[0]->index;
  if (first > applied_index_ + 1) {
    LOG_FATAL("first index of committed entry[%lu] should <= progress.appliedIndex[%lu]+1", first, applied_index_);
  }
  if (applied_index_ - first + 1 < entries.size()) {
    ents.insert(ents.end(), entries.begin() + applied_index_ - first + 1, entries.end());
  }
}

// maybe_trigger_snapshot 根据条件决定是否创建新的快照。
void RaftNode::maybe_trigger_snapshot() {
  if (applied_index_ - snapshot_index_ <= snap_count_) {
    return;
  }

  LOG_DEBUG("start snapshot [applied index: %lu | last snapshot index: %lu], snapshot count[%lu]",
            applied_index_,
            snapshot_index_,
            snap_count_);

  std::promise<SnapshotDataPtr> promise;
  std::future<SnapshotDataPtr> future = promise.get_future();
  redis_server_->get_snapshot(std::move([&promise](const SnapshotDataPtr& data) {
    promise.set_value(data);
  }));

  future.wait();
  SnapshotDataPtr snapshot_data = future.get();

  proto::SnapshotPtr snap;
  Status status = storage_->create_snapshot(applied_index_, conf_state_, *snapshot_data, snap);
  if (!status.is_ok()) {
    LOG_FATAL("create snapshot error %s", status.to_string().c_str());
  }

  status = save_snap(*snap);
  if (!status.is_ok()) {
    LOG_FATAL("save snapshot error %s", status.to_string().c_str());
  }

  uint64_t compactIndex = 1;
  if (applied_index_ > snapshotCatchUpEntriesN) {
    compactIndex = applied_index_ - snapshotCatchUpEntriesN;
  }
  status = storage_->compact(compactIndex);
  if (!status.is_ok()) {
    LOG_FATAL("compact error %s", status.to_string().c_str());
  }
  LOG_INFO("compacted log at index %lu", compactIndex);
  snapshot_index_ = applied_index_;
}
// 功能：初始化节点并启动定时器和事件循环。
void RaftNode::schedule() {
  // 设置当前线程的 ID。
  // pthread_self是一个 POSIX 线程库函数，它返回调用它的线程的线程 ID。这个 ID 是线程的唯一标识符，用于在程序中识别和管理线程
  pthread_id_ = pthread_self();
  // 从存储中获取快照。
  proto::SnapshotPtr snap;
  Status status = storage_->snapshot(snap);
  if (!status.is_ok()) {
    LOG_FATAL("get snapshot failed %s", status.to_string().c_str());
  }
  // 更新配置状态、快照索引和应用索引。
  *conf_state_ = snap->metadata.conf_state;
  snapshot_index_ = snap->metadata.index;
  applied_index_ = snap->metadata.index;

  // 启动 Redis 服务器，并等待其启动完成。
  redis_server_ = std::make_shared<RedisStore>(this, std::move(snap_data_), port_);
  std::promise<pthread_t> promise;
  std::future<pthread_t> future = promise.get_future();
  redis_server_->start(promise);
  future.wait();
  pthread_t id = future.get();
  LOG_DEBUG("server start [%lu]", id);
  // 启动定时器，用于定期触发事件处理。
  // 定时器里会再次开启定时器保证io_service_.run不会退出
  start_timer();
  // 运行 I/O 服务，处理异步事件。
  io_service_.run();
}

void RaftNode::propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback) {
//   如果当前线程不是节点的主线程，则通过 I/O 服务异步提交提案。
  if (pthread_id_ != pthread_self()) {
    io_service_.post([this, data, callback]() {
      Status status = node_->propose(std::move(*data));
      callback(status);
      pull_ready_events();
    });
  } 
  // 否则，直接提交提案。
  else {
    Status status = node_->propose(std::move(*data));
    callback(status);
    pull_ready_events();
  }
  // 提案提交后，调用回调函数并处理准备好的事件。
}

// 功能：处理来自其他节点的消息。
void RaftNode::process(proto::MessagePtr msg, const StatusCallback& callback) {
  // 如果当前线程不是节点的主线程，通过 io_service_ 的 post 方法将处理任务提交到 I/O 服务队列中异步执行。
  if (pthread_id_ != pthread_self()) {
    io_service_.post([this, msg, callback]() {
      Status status = this->node_->step(msg);
      callback(status);
      pull_ready_events();
    });
  } 
  // 否则，直接处理消息。
  else {
    Status status = this->node_->step(msg);
    callback(status);
    pull_ready_events();
  }
  // 处理后，调用回调函数并处理准备好的事件。
}

void RaftNode::is_id_removed(uint64_t id, const std::function<void(bool)>& callback) {
  LOG_DEBUG("no impl yet");
  callback(false);
}

void RaftNode::report_unreachable(uint64_t id) {
  LOG_DEBUG("no impl yet");
}

void RaftNode::report_snapshot(uint64_t id, SnapshotStatus status) {
  LOG_DEBUG("no impl yet");
}

static RaftNodePtr g_node = nullptr;
// 全局变量 g_node 用于存储当前运行的 Raft 节点实例。
// on_signal() 函数用于处理系统信号，如 SIGINT 和 SIGHUP，以便优雅地停止节点。
void on_signal(int) {
  LOG_INFO("catch signal");
  if (g_node) {
    g_node->stop();
  }
}

// 功能：启动 Raft 节点。
void RaftNode::main(uint64_t id, const std::string& cluster, uint16_t port) {
  // 设置信号处理函数。
  ::signal(SIGINT, on_signal);
  ::signal(SIGHUP, on_signal);
  // 创建并配置 Raft 节点。
  g_node = std::make_shared<RaftNode>(id, cluster, port);
  // 启动传输服务。
  g_node->transport_ = Transport::create(g_node.get(), g_node->id_);
  std::string& host = g_node->peers_[id - 1];
  g_node->transport_->start(host);
  // 添加对等节点。
  for (uint64_t i = 0; i < g_node->peers_.size(); ++i) {
    uint64_t peer = i + 1;
    // 如果是本身就不重复增加
    if (peer == g_node->id_) {
      continue;
    }
    g_node->transport_->add_peer(peer, g_node->peers_[i]);
  }
  // 调度节点以开始处理事件。
  g_node->schedule();
}

// 功能：停止 Raft 节点。
void RaftNode::stop() {
  LOG_DEBUG("stopping");
  // 停止 Redis 服务器。
  redis_server_->stop();
  // 停止传输服务。
  if (transport_) {
    transport_->stop();
    transport_ = nullptr;
  }
  // 停止 I/O 服务。
  io_service_.stop();
}

}
