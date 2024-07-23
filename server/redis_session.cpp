#include "server/redis_session.h"
#include "common/log.h"
#include <unordered_map>
#include "server/redis_store.h"
#include <glib.h>
// 这段代码实现了一个简单的Redis协议处理会话 RedisSession，它负责处理和响应客户端的Redis命令请求
namespace kv {

#define RECEIVE_BUFFER_SIZE (1024 * 512)

namespace shared {
// 这些常量定义了Redis协议的标准响应。
static const char* ok = "+OK\r\n";
static const char* err = "-ERR %s\r\n";
static const char* wrong_type = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
static const char* unknown_command = "-ERR unknown command `%s`\r\n";
static const char* wrong_number_arguments = "-ERR wrong number of arguments for '%s' command\r\n";
static const char* pong = "+PONG\r\n";
static const char* null = "$-1\r\n";
// CommandCallback 是一个函数类型，用于处理不同的Redis命令。
typedef std::function<void(RedisSessionPtr, struct redisReply* reply)> CommandCallback;
// command_table 是一个命令表，用于将命令字符串映射到对应的处理函数。
static std::unordered_map<std::string, CommandCallback> command_table = {
    {"ping", RedisSession::ping_command},
    {"PING", RedisSession::ping_command},
    {"get", RedisSession::get_command},
    {"GET", RedisSession::get_command},
    {"set", RedisSession::set_command},
    {"SET", RedisSession::set_command},
    {"del", RedisSession::del_command},
    {"DEL", RedisSession::del_command},
    {"keys", RedisSession::keys_command},
    {"KEYS", RedisSession::keys_command},
};

}

static void build_redis_string_array_reply(const std::vector<std::string>& strs, std::string& reply) {
  //*2\r\n$4\r\nkey1\r\n$4key2\r\n

  char buffer[64];
  snprintf(buffer, sizeof(buffer), "*%lu\r\n", strs.size());
  reply.append(buffer);

  for (const std::string& str : strs) {
//  snprintf 函数在写入数据时，会在生成的字符串末尾添加一个空字符（\0）
    snprintf(buffer, sizeof(buffer), "$%lu\r\n", str.size());
    reply.append(buffer);

    if (!str.empty()) {
      reply.append(str);
      reply.append("\r\n");
    }
  }
}

// 构造函数初始化会话，包括指向Redis服务器的指针、网络套接字、读缓冲区和Redis协议读取器。
RedisSession::RedisSession(RedisStore* server, boost::asio::io_service& io_service)
    : quit_(false),
      server_(server),
      socket_(io_service),
      read_buffer_(RECEIVE_BUFFER_SIZE),
      reader_(redisReaderCreate()) {
}
// start 函数启动异步读取操作，等待客户端发送的数据。
void RedisSession::start() {
  if (quit_) {
    return;
  }
  auto self = shared_from_this();
  auto buffer = boost::asio::buffer(read_buffer_.data(), read_buffer_.size());
  auto handler = [self](const boost::system::error_code& error, size_t bytes) {
    if (bytes == 0) {
      return;
    }
    if (error) {
      LOG_DEBUG("read error %s", error.message().c_str());
      return;
    }
    self->handle_read(bytes);
  };
  // 异步操作 不会阻塞 也不会循环进入导致栈溢出
  socket_.async_read_some(buffer, std::move(handler));
}
// handle_read 函数处理读取到的数据，解析Redis协议消息并生成回复。对于每个解析的回复，调用 on_redis_reply 函数进行处理
void RedisSession::handle_read(size_t bytes) {
  uint8_t* start = read_buffer_.data();
  uint8_t* end = read_buffer_.data() + bytes;
  int err = REDIS_OK;
  std::vector<struct redisReply*> replies;

  while (!quit_ && start < end) {
    //memchr 函数用于查找换行符 \n，如果没有找到，则重新开始异步读取
    uint8_t* p = (uint8_t*) memchr(start, '\n', bytes);
    if (!p) {
      this->start();
      break;
    }
//这里使用 redisReaderFeed 函数将数据喂给 Redis 解析器。如果解析失败，则记录错误并退出。
    size_t n = p + 1 - start;
    err = redisReaderFeed(reader_, (const char*) start, n);
    if (err != REDIS_OK) {
      LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
      quit_ = true;
      break;
    }
//使用 redisReaderGetReply 函数获取解析结果，并将其存储在 replies 向量中。
    struct redisReply* reply = NULL;
    err = redisReaderGetReply(reader_, (void**) &reply);
    if (err != REDIS_OK) {
      LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
      quit_ = true;
      break;
    }
    if (reply) {
      replies.push_back(reply);
    }

    start += n;
    bytes -= n;
  }
  if (err == REDIS_OK) {
//  调用 on_redis_reply 方法处理每个解析结果，并重新开始异步读取。
    for (struct redisReply* reply : replies) {
      on_redis_reply(reply);
    }
    this->start();
  }

  for (struct redisReply* reply : replies) {
    freeReplyObject(reply);
  }
}
// on_redis_reply 函数根据解析的Redis命令进行处理。如果命令有效，调用命令对应的处理函数
void RedisSession::on_redis_reply(struct redisReply* reply) {
  char buffer[256];
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_WARN("wrong type %d", reply->type);
    send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  if (reply->elements < 1) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "");
    send_reply(buffer, n);
    return;
  }

  if (reply->element[0]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[0]->type);
    send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  std::string command(reply->element[0]->str, reply->element[0]->len);
  auto it = shared::command_table.find(command);
  if (it == shared::command_table.end()) {
    int n = snprintf(buffer, sizeof(buffer), shared::unknown_command, command.c_str());
    send_reply(buffer, n);
    return;
  }
  shared::CommandCallback& cb = it->second;
  cb(shared_from_this(), reply);
}
// send_reply 函数将回复数据添加到发送缓冲区
void RedisSession::send_reply(const char* data, uint32_t len) {
  uint32_t bytes = send_buffer_.readable_bytes();
  send_buffer_.put((uint8_t*) data, len);
  if (bytes == 0) {
    start_send();
  }
}
// 调用 start_send 函数启动异步发送操作。
void RedisSession::start_send() {
  if (!send_buffer_.readable()) {
    return;
  }
  auto self = shared_from_this();
  uint32_t remaining = send_buffer_.readable_bytes();
  auto buffer = boost::asio::buffer(send_buffer_.reader(), remaining);
  auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
    if (bytes == 0) {
      return;
    }
    if (error) {
      LOG_DEBUG("send error %s", error.message().c_str());
      return;
    }
    std::string str((const char*) self->send_buffer_.reader(), bytes);
    self->send_buffer_.read_bytes(bytes);
    self->start_send();
  };
  boost::asio::async_write(socket_, buffer, std::move(handler));
}
// 这些函数实现了各种Redis命令的处理逻辑，包括 PING、GET、SET、DEL 和 KEYS 命令。
// 每个命令函数都会验证请求的格式和类型，并调用相应的服务器功能来处理请求。
void RedisSession::ping_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  self->send_reply(shared::pong, strlen(shared::pong));
}

void RedisSession::get_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements != 2) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "get");
    self->send_reply(buffer, n);
    return;
  }

  if (reply->element[1]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[1]->type);
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  std::string value;
  std::string key(reply->element[1]->str, reply->element[1]->len);
  bool get = self->server_->get(key, value);
  if (!get) {
    self->send_reply(shared::null, strlen(shared::null));
  } else {
    char* str = g_strdup_printf("$%lu\r\n%s\r\n", value.size(), value.c_str());
    self->send_reply(str, strlen(str));
    g_free(str);
  }
}

void RedisSession::set_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements != 3) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "set");
    self->send_reply(buffer, n);
    return;
  }

  if (reply->element[1]->type != REDIS_REPLY_STRING || reply->element[2]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[1]->type);
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }
  std::string key(reply->element[1]->str, reply->element[1]->len);
  std::string value(reply->element[2]->str, reply->element[2]->len);
  self->server_->set(std::move(key), std::move(value), [self](const Status& status) {
    if (status.is_ok()) {
      self->send_reply(shared::ok, strlen(shared::ok));
    }
    else {
      char buff[256];
      int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
      self->send_reply(buff, n);
    }
  });
}

void RedisSession::del_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements <= 1) { ;
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "del");
    self->send_reply(buffer, n);
    return;
  }

  std::vector<std::string> keys;
  for (size_t i = 1; i < reply->elements; ++i) {
    redisReply* element = reply->element[i];
    if (element->type != REDIS_REPLY_STRING) {
      self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
      return;
    }

    keys.emplace_back(element->str, element->len);
  }

  self->server_->del(std::move(keys), [self](const Status& status) {
    if (status.is_ok()) {
      self->send_reply(shared::ok, strlen(shared::ok));
    } else {
      char buff[256];
      int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
      self->send_reply(buff, n);
    }
  });
}

void RedisSession::keys_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements != 2) { ;
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "keys");
    self->send_reply(buffer, n);
    return;
  }

  redisReply* element = reply->element[1];

  if (element->type != REDIS_REPLY_STRING) {
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  std::vector<std::string> keys;
  self->server_->keys(element->str, element->len, keys);
  std::string str;
  build_redis_string_array_reply(keys, str);
  self->send_reply(str.data(), str.size());
}

}
