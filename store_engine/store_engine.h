#pragma once
#include <unordered_map>
#include <string>
#include <iostream>
#include <memory>
#include "common/log.h"
namespace kv {
class StoreEngine {
public:
    virtual ~StoreEngine() = default;
    //把snapshot中的键值对导入数据库
    virtual void input_kv(std::unordered_map<std::string, std::string>& umap) = 0;
    // 把数据库中的键值对导出用来生成snapshot
    virtual void output_kv(std::unordered_map<std::string, std::string>& umap) = 0;
    virtual bool get(const std::string& key, std::string& value) = 0;
    virtual void del(const std::string& key) = 0;
    virtual void set(const std::string& key, const std::string& value) = 0;
private:
};

typedef std::shared_ptr<StoreEngine> StoreEnginePtr;
}


