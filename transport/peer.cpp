#include <boost/algorithm/string.hpp>
#include "transport/peer.h"
#include "common/log.h"
#include "common/bytebuffer.h"
#include "transport/proto.h"
#include <boost/asio.hpp>

namespace kv {

class PeerImpl;
// ClientSession 类负责与远程节点的单个连接。它提供了发送数据、关闭会话、启动连接和写入数据的方法。
class ClientSession {
 public:
  explicit ClientSession(boost::asio::io_service& io_service, PeerImpl* peer);

  ~ClientSession() {

  }

  void send(uint8_t transport_type, const uint8_t* data, uint32_t len) {
    uint32_t remaining = buffer_.readable_bytes();

    TransportMeta meta;
    meta.type = transport_type;
    meta.len = htonl(len);
    assert(sizeof(TransportMeta) == 5);
    buffer_.put((const uint8_t*) &meta, sizeof(TransportMeta));
    buffer_.put(data, len);
    assert(remaining + sizeof(TransportMeta) + len == buffer_.readable_bytes());
//如果没有连接 则不会进行写入操作 只准备好缓冲区的数据
    if (connected_ && remaining == 0) {
      start_write();
    }
  }

  void close_session();

  void start_connect() {
    socket_.async_connect(endpoint_, [this](const boost::system::error_code& err) {
      if (err) {
        LOG_DEBUG("connect [%lu] error %s", this->peer_id_, err.message().c_str());
        this->close_session();
        return;
      }
      this->connected_ = true;
      LOG_INFO("connected to [%lu]", this->peer_id_);
//连好之后会尝试进行一次数据写入
      if (this->buffer_.readable()) {
        this->start_write();
      }
    });
  }

  void start_write() {
    if (!buffer_.readable()) {
      return;
    }

    uint32_t remaining = buffer_.readable_bytes();
    auto buffer = boost::asio::buffer(buffer_.reader(), remaining);
    auto handler = [this](const boost::system::error_code& error, std::size_t bytes) {
      if (error || bytes == 0) {
        LOG_DEBUG("send [%lu] error %s", this->peer_id_, error.message().c_str());
        this->close_session();
        return;
      }
      this->buffer_.read_bytes(bytes);
      // 循环写
      this->start_write();
    };
    // 写入完成之后执行回调
    boost::asio::async_write(socket_, buffer, handler);
  }

 private:
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::endpoint endpoint_;
  PeerImpl* peer_;
  uint64_t peer_id_;
  ByteBuffer buffer_;
  bool connected_;
};
// PeerImpl 类实现了 Peer 接口，代表一个远程节点。它负责启动与远程节点的通信、发送消息和快照、更新节点信息、报告活动时间以及停止通信。
class PeerImpl : public Peer {
 public:
  explicit PeerImpl(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
      : peer_(peer),
        io_service_(io_service),
        timer_(io_service) {
    std::vector<std::string> strs;
    boost::split(strs, peer_str, boost::is_any_of(":"));
    if (strs.size() != 2) {
      LOG_DEBUG("invalid host %s", peer_str.c_str());
      exit(0);
    }
    auto address = boost::asio::ip::address::from_string(strs[0]);
    int port = std::atoi(strs[1].c_str());
    endpoint_ = boost::asio::ip::tcp::endpoint(address, port);
  }

  ~PeerImpl() final {
  }

  void start() final {
    start_timer();
  };

  void send(proto::MessagePtr msg) final {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, *msg);

    do_send_data(TransportTypeStream, (const uint8_t*) sbuf.data(), (uint32_t) sbuf.size());
  }

  void send_snap(proto::SnapshotPtr snap) final {
    LOG_DEBUG("no impl yet");
  }

  void update(const std::string& peer) final {
    LOG_DEBUG("no impl yet");
  }

  uint64_t active_since() final {
    LOG_DEBUG("no impl yet");
    return 0;
  }

  void stop() final {

  }

 private:
  void do_send_data(uint8_t type, const uint8_t* data, uint32_t len) {
    //如果 session_ 为空（即没有活动的 ClientSession 实例），
    // 则创建一个新的 ClientSession 实例。
    if (!session_) {
      session_ = std::make_shared<ClientSession>(io_service_, this);
      // 将数据发送到远程服务器。这里 type 是数据类型，data 是数据指针，len 是数据长度。
      session_->send(type, data, len);
      // 启动连接过程，这通常意味着开始尝试连接到远程服务器。
      session_->start_connect();
    }
    else {
      session_->send(type, data, len);
    }
  }

  void start_timer() {
    //设置定时器在当前时间后的 3 秒触发。
    timer_.expires_from_now(boost::posix_time::seconds(3));
    //启动一个异步等待操作，当定时器触发时，将调用提供的回调函数。
    timer_.async_wait([this](const boost::system::error_code& err) {
      // 如果定时器触发时出现错误（例如被取消），则通过 LOG_ERROR 记录错误信息。
      // 如果没有错误，则递归调用 start_timer 函数，重新启动定时器，实现定时器的循环触发。
      if (err) {
        LOG_ERROR("timer waiter error %s", err.message().c_str());
        return;
      }
      this->start_timer();
    });
    //定义了一个静态的原子变量 tick，用于生成唯一的计数器值。
    static std::atomic<uint32_t> tick;
    //定义了一个结构体实例 dbg，用于存储调试信息。
    DebugMessage dbg;
    //dbg.a 和 dbg.b 分别被赋值为 tick 的递增值，用于生成调试消息的内容
    dbg.a = tick++;
    dbg.b = tick++;
    //将 dbg 结构体的内容作为数据发送出去，使用 TransportTypeDebug 作为传输类型。
//   定时发送也能保证连接不断
    do_send_data(TransportTypeDebug, (const uint8_t*) &dbg, sizeof(dbg));
  }

  uint64_t peer_;
  boost::asio::io_service& io_service_;
  friend class ClientSession;
  std::shared_ptr<ClientSession> session_;
  boost::asio::ip::tcp::endpoint endpoint_;
  boost::asio::deadline_timer timer_;
};

ClientSession::ClientSession(boost::asio::io_service& io_service, PeerImpl* peer)
    : socket_(io_service),
      endpoint_(peer->endpoint_),
      peer_(peer),
      peer_id_(peer_->peer_),
      connected_(false) {

}

void ClientSession::close_session() {
    // 由于session_是智能指针管理的 指向nullptr引用计数为0 就自动释放了
  peer_->session_ = nullptr;
}

std::shared_ptr<Peer> Peer::creat(uint64_t peer, const std::string& peer_str, void* io_service) {
    //将 void* 类型的 io_service 指针转换为 boost::asio::io_service* 并解引用，以传递给 PeerImpl 构造函数。
  std::shared_ptr<PeerImpl> peer_ptr(new PeerImpl(*(boost::asio::io_service*) io_service, peer, peer_str));
  return peer_ptr;
}

}
