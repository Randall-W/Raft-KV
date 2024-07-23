#include "snap/snapshotter.h"
#include <boost/filesystem.hpp>
#include "common/log.h"
#include <msgpack.hpp>
#include "raft/util.h"
#include <inttypes.h>

namespace kv {
//SnapshotRecord 结构体用于保存快照数据的元数据和数据本身。
// data_len 表示数据的长度，crc32 是数据的CRC32校验值，data 是一个柔性数组成员，用于存储实际的快照数据。
struct SnapshotRecord {
  uint32_t data_len;
  uint32_t crc32;
  char data[0];
};
// 加载最新的快照：



Status Snapshotter::load(proto::Snapshot& snapshot) {
  std::vector<std::string> names;
  // 调用 get_snap_names 获取所有快照文件名。
  get_snap_names(names);
  // 遍历文件名，尝试加载每个快照。
  for (std::string& filename : names) {
    Status status = load_snap(filename, snapshot);
    // 如果成功加载，返回成功状态。
    // 如果所有尝试都失败，返回未找到快照的状态。
    if (status.is_ok()) {
      return Status::ok();
    }
  }

  return Status::not_found("snap not found");
}
// 根据给定的任期（term）和索引（index）生成快照文件名。
std::string Snapshotter::snap_name(uint64_t term, uint64_t index) {
  char buffer[64];
//  格式化字符串 "%016" PRIx64 "-%016" PRIx64 ".snap" 包含两个十六进制整数，
//  每个整数占 16 位，并用 - 分隔，最后加上 .snap 后缀。PRIx64 是一个宏，用于指定 64 位无符号整数的十六进制格式。
  snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".snap", term, index);
  return buffer;
}

// 保存快照到文件系统：
Status Snapshotter::save_snap(const proto::Snapshot& snapshot) {
  Status status;
  // 将快照数据序列化为MsgPack格式。
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, snapshot);
  // 分配内存并填充 SnapshotRecord 结构体。
  SnapshotRecord* record = (SnapshotRecord*) malloc(sbuf.size() + sizeof(SnapshotRecord));
  record->data_len = sbuf.size();
  record->crc32 = compute_crc32(sbuf.data(), sbuf.size());
  memcpy(record->data, sbuf.data(), sbuf.size());
  // 打开文件，写入快照数据和元数据。
  char save_path[128];
  snprintf(save_path,
           sizeof(save_path),
           "%s/%s",
           dir_.c_str(),
           snap_name(snapshot.metadata.term, snapshot.metadata.index).c_str());
  // 关闭文件，释放内存。
  FILE* fp = fopen(save_path, "w");
  if (!fp) {
    free(record);
    return Status::io_error(strerror(errno));
  }

  size_t bytes = sizeof(SnapshotRecord) + record->data_len;
  if (fwrite((void*) record, 1, bytes, fp) != bytes) {
    status = Status::io_error(strerror(errno));
  }
  free(record);
  fclose(fp);

  return status;
}
// 这个函数用于获取快照目录下的所有快照文件名，并按降序排序。
void Snapshotter::get_snap_names(std::vector<std::string>& names) {
  using namespace boost;

  filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(dir_); it != end; it++) {
    filesystem::path filename = (*it).path().filename();
    filesystem::path extension = filename.extension();
    if (extension != ".snap") {
      continue;
    }
    names.push_back(filename.string());
  }
  std::sort(names.begin(), names.end(), std::greater<std::string>());
}
// 这个函数用于从文件系统加载单个快照文件：




Status Snapshotter::load_snap(const std::string& filename, proto::Snapshot& snapshot) {
  using namespace boost;
  SnapshotRecord snap_hdr;
  std::vector<char> data;
//  方法通过将文件名附加到目录 (dir_) 来构建快照文件的完整路径。
//  这里的 / 操作符是 Boost.Filesystem 库中定义的一个重载操作符，用于连接两个路径部分
  filesystem::path path = filesystem::path(dir_) / filename;
  // 打开文件，读取快照的元数据和数据。
  FILE* fp = fopen(path.c_str(), "r");

  if (!fp) {
    goto invalid_snap;
  }
//从文件中读取快照头 (SnapshotRecord)
  if (fread(&snap_hdr, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord)) {
    goto invalid_snap;
  }

  if (snap_hdr.data_len == 0 || snap_hdr.crc32 == 0) {
    goto invalid_snap;
  }

  data.resize(snap_hdr.data_len);
  if (fread(data.data(), 1, snap_hdr.data_len, fp) != snap_hdr.data_len) {
    goto invalid_snap;
  }

  fclose(fp);
  // 计算数据的CRC32校验值，并与存储的校验值比较。
  fp = NULL;
  if (compute_crc32(data.data(), data.size()) != snap_hdr.crc32) {
    goto invalid_snap;
  }
 
  try {
 // 如果校验通过，反序列化快照数据。
    msgpack::object_handle oh = msgpack::unpack((const char*) data.data(), data.size());
    oh.get().convert(snapshot);
    return Status::ok();

  } catch (std::exception& e) {
    goto invalid_snap;
  }
// 如果校验失败或反序列化失败，标记文件为损坏并返回错误状态。
invalid_snap:
  if (fp) {
    fclose(fp);
  }
  LOG_INFO("broken snapshot %s", path.string().c_str());
  filesystem::rename(path, path.string() + ".broken");
  return Status::io_error("unexpected empty snapshot");
}

}
