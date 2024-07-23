#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include "transport/transport.h"
#include "common/log.h"
// boost/algorithm/string.hpp：用于字符串处理。
// boost/asio.hpp：用于网络编程。
// raft-kv/transport/transport.h：定义了 Transport 接口。
// raft-kv/common/log.h：用于日志记录。
namespace kv {

// TransportImpl 类是 Transport 类的具体实现
// TransportImpl 类主要用于管理与 Raft 协议相关的网络通信，包括启动服务器、添加和移除对等节点、发送消息等操作
// 实现了一个基于 Raft 协议的网络通信层，负责处理节点间的通信和消息传递。
class TransportImpl : public Transport {

 public:
  explicit TransportImpl(RaftServer* raft, uint64_t id)
      : raft_(raft),
        id_(id) {
  }

// 停止并加入 io_thread_，记录日志。
  ~TransportImpl() final {
    // io_thread_.joinable() 是 C++ 标准库中 std::thread 类的一个成员函数，用于检查线程是否可以被 join。
    // 如果一个线程已经被 join 过，或者从未启动，那么它就不能再次被 join。
    if (io_thread_.joinable()) {
      io_thread_.join();
      LOG_DEBUG("transport stopped");
    }
  }
  // 启动网络服务，创建并启动 IoServer，启动 io_service_ 的线程。
  void start(const std::string& host) final {
    server_ = IoServer::create((void*) &io_service_, host, raft_);
    server_->start();

    io_thread_ = std::thread([this]() {
      this->io_service_.run();
    });
  }
  // 添加对等节点，创建 Peer 实例并启动，将其添加到 peers_ 映射中。
  void add_peer(uint64_t id, const std::string& peer) final {
    LOG_DEBUG("node:%lu, peer:%lu, addr:%s", id_, id, peer.c_str());
    //TODO:不加锁会发生什么？？
    std::lock_guard<std::mutex> guard(mutex_);
    // 检查是否已经创建过对等节点 改peers_和raftnode里的peers_不是同一个含义
    auto it = peers_.find(id);
    if (it != peers_.end()) {
      LOG_DEBUG("peer already exists %lu", id);
      return;
    }

    PeerPtr p = Peer::creat(id, peer, (void*) &io_service_);
    p->start();
    peers_[id] = p;
  }
  // 移除对等节点的占位符实现，目前仅记录警告日志。
  void remove_peer(uint64_t id) final {
    LOG_WARN("no impl yet");
  }
  // 发送消息，遍历消息列表，将消息发送给相应的对等节点
  void send(std::vector<proto::MessagePtr> msgs) final {
    auto callback = [this](std::vector<proto::MessagePtr> msgs) {
      for (proto::MessagePtr& msg : msgs) {
        if (msg->to == 0) {
          // ignore intentionally dropped message
          continue;
        }

        auto it = peers_.find(msg->to);
        if (it != peers_.end()) {
            // 为什么要选择将要发送到的节点的send？
            // 因为调用该节点的send,就能调用该节点的某些成员变量
          it->second->send(msg);
          continue;
        }
        LOG_DEBUG("ignored message %d (sent to unknown peer %lu)", msg->type, msg->to);
      }
    };
    io_service_.post(std::bind(callback, std::move(msgs)));
  }
  // 停止网络服务，停止 io_service_。 
  void stop() final {
    io_service_.stop();
  }

 private:
  // 指向 Raft 服务器的指针。
  RaftServer* raft_;
  // 节点的唯一标识。
  uint64_t id_;
  // 运行 io_service_ 的线程。
  std::thread io_thread_;
  // 用于异步 I/O 操作的服务。
  boost::asio::io_service io_service_;
  // 用于保护对等节点列表的互斥锁。
  std::mutex mutex_;
  // 存储对等节点的映射。
  std::unordered_map<uint64_t, PeerPtr> peers_;
  // 指向网络服务器的智能指针。
  IoServerPtr server_;
};
// 工厂函数
std::shared_ptr<Transport> Transport::create(RaftServer* raft, uint64_t id) {
  std::shared_ptr<TransportImpl> impl(new TransportImpl(raft, id));
  return impl;
}

}
