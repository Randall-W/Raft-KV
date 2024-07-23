#include "store_engine/RocksDB.h"
#include <boost/filesystem.hpp>
namespace  kv{
RocksDB::RocksDB(uint64_t node_id) {
    // 确保在数据库不存在时创建一个新的数据库
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db_ptr;
    std::string db_path = "./RocksDB_tmp/testdb" + std::to_string(node_id);
    rocksdb::Status status;
    if (boost::filesystem::exists(db_path))
    {
        status = rocksdb::DestroyDB(db_path, options);
        if (!status.ok()) {
            LOG_ERROR("Failed to destroy old database: %s", status.ToString().c_str());
        }
    }
    else{
        boost::filesystem::create_directories(db_path);
    }

    status = rocksdb::DB::Open(options, db_path, &db_ptr);
    db.reset(db_ptr);
    if (!status.ok()) {
        LOG_ERROR("unable to open data library: %s", status.ToString().c_str());
        return;
    }
}

void RocksDB::input_kv(std::unordered_map<std::string, std::string>& umap)
{
    rocksdb::Status status;
    // 创建一个写批处理
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        batch.Delete(it->key());
    }
    // 检查迭代器是否有效
    if (!it->status().ok()) {
        std::cerr << "Iterator error: " << it->status().ToString() << std::endl;
    }
    // 执行写批处理
    status = db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        std::cerr << "Write batch error: " << status.ToString() << std::endl;
    }
    // 写入键值对
    for (const auto& kv : umap) {
        status = db->Put(rocksdb::WriteOptions(), kv.first, kv.second);
        if (!status.ok()) {
            LOG_ERROR("Failed to write key: %s, value: %s. Error: %s", kv.first.c_str(), kv.second.c_str(), status.ToString().c_str());
        }
    }
    // 读取并验证数据
    for (const auto& kv : umap) {
        std::string value;
        status = db->Get(rocksdb::ReadOptions(), kv.first, &value);
        if (!status.ok()) {
            LOG_ERROR("Failed to read key: %s. Error: %s", kv.first.c_str(), status.ToString().c_str());
        }
    }
}

void RocksDB::output_kv(std::unordered_map<std::string, std::string>& umap){
    umap.clear();
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        umap[it->key().ToString()] = it->value().ToString();
    }
//    if (!it->status().ok()) {
//        LOG_ERROR("An error occurred during iteration: %s", it->status().ToString().c_str());
//        throw std::runtime_error("Failed to iterate over RocksDB");
//    }
}
bool RocksDB::get(const std::string& key, std::string& value)
{
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok()) {
        LOG_ERROR("Get error : %s", status.ToString().c_str());
        return false;
    }
    else if (status.IsNotFound()) {
    // Key does not exist
        LOG_ERROR("Key not found : %s", key.c_str());
        return false;
    }
    return true;
}

void RocksDB::set(const std::string& key, const std::string& value)
{
    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        std::cerr << "Put error: " << status.ToString() << std::endl;
    }
//    LOG_INFO("Set Command");
}
void RocksDB::del(const std::string& key)
{
    rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), key);
    if (!status.ok()) {
        std::cerr << "Delete error: " << status.ToString() << std::endl;
    }
//    LOG_INFO("Del Command");
}
}