#include "hashmap.h"
#include "store_engine/RocksDB.h"
namespace  kv{
    void HashMap::input_kv(std::unordered_map<std::string, std::string>& umap)
    {
        key_value = std::move(umap);
    }

    void HashMap::output_kv(std::unordered_map<std::string, std::string>& umap){
        umap.clear();
        umap = key_value;
    }

    bool HashMap::get(const std::string& key, std::string& value)
    {
        auto it = key_value.find(key);
        if (it != key_value.end())
        {
            value = it->second;
            return true;
        }
        else
        {
            return false;
        }
    }

    void HashMap::set(const std::string& key, const std::string& value)
    {
        key_value[key] = value;
    }

    void HashMap::del(const std::string& key)
    {
        key_value.erase(key);
    }
}