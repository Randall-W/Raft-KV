#pragma once
#include <vector>
#include <stdint.h>
#include <string.h>
#include "common/slice.h"

namespace kv {

class ByteBuffer {
 public:
  explicit ByteBuffer();
//将数据写入缓冲区。
  void put(const uint8_t* data, uint32_t len);
//从缓冲区读取指定字节数的数据。
  void read_bytes(uint32_t bytes);
//检查缓冲区是否有可读数据。
  bool readable() const {
    return writer_ > reader_;
  }
//返回可读字节数。
  uint32_t readable_bytes() const;
//返回缓冲区的容量。
  uint32_t capacity() const {
    return static_cast<uint32_t>(buff_.capacity());
  }
//返回当前读取位置的指针。
  const uint8_t* reader() const {
    return buff_.data() + reader_;
  }
//返回当前可读数据的 Slice 对象。
  Slice slice() const {
    return Slice((const char*) reader(), readable_bytes());
  }
//重置缓冲区。
  void reset();
 private:
    //可能调整缓冲区大小以适应数据
  void may_shrink_to_fit();
    //读取位置的索引
  uint32_t reader_;
  //写入位置的索引
  uint32_t writer_;
  //存储数据的缓冲区
  std::vector<uint8_t> buff_;
};

}
