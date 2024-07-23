#pragma once
#include <unordered_map>
#include <string>
#include <iostream>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "common/log.h"
#include "store_engine.h"
namespace kv {
    class HashMap : public StoreEngine{
    public:
        HashMap() = default;
        ~HashMap() = default;
        //把snapshot中的键值对导入数据库
        void input_kv(std::unordered_map<std::string, std::string>& umap) final;
        // 把数据库中的键值对导出用来生成snapshot
        void output_kv(std::unordered_map<std::string, std::string>& umap) final;
        bool get(const std::string& key, std::string& value) final;
        void del(const std::string& key) final;
        void set(const std::string& key, const std::string& value) final;
    private:
        std::unordered_map<std::string, std::string> key_value;
    };

    typedef std::shared_ptr<HashMap> HashMapPtr;
}


