#pragma once
#include <unordered_map>
#include <string>
#include <iostream>
#include <memory>
#include "common/log.h"
#include <sqlite3.h>
#include "store_engine.h"
namespace kv {
class SQlite : public StoreEngine{
public:
    explicit SQlite(uint64_t node_id);
    ~SQlite();
    //把snapshot中的键值对导入数据库
    void input_kv(std::unordered_map<std::string, std::string>& umap) final;
    // 把数据库中的键值对导出用来生成snapshot
    void output_kv(std::unordered_map<std::string, std::string>& umap) final;
    bool get(const std::string& key, std::string& value) final;
    void del(const std::string& key) final;
    void set(const std::string& key, const std::string& value) final;
private:
    sqlite3 *db;
};
typedef std::shared_ptr<SQlite> SQlitePtr;
}


