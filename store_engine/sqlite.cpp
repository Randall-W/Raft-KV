#include "sqlite.h"
#include <boost/filesystem.hpp>
namespace  kv{
SQlite::SQlite(uint64_t node_id) {
    int ret;
    std::string db_path = "./sqlite";
    if (!boost::filesystem::exists(db_path))
    {
        boost::filesystem::create_directories(db_path);
    }
    // 打开数据库连接
    ret = sqlite3_open(std::string ("./sqlite/testsqlite"+std::to_string(node_id)+".db").c_str(), &db);
    if (ret) {
        LOG_ERROR("Can't open database : %s", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }
    // 创建表（如果不存在）
    const char *createTableSQL = "CREATE TABLE IF NOT EXISTS KeyValueTable("
                                 "Key TEXT PRIMARY KEY NOT NULL, "
                                 "Value TEXT NOT NULL);";
    char *zErrMsg = nullptr;
    ret = sqlite3_exec(db, createTableSQL, nullptr, 0, &zErrMsg);
    if (ret != SQLITE_OK) {
        LOG_ERROR("SQL error : %s", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    // 清空数据库
    const char *clearTableSQL = "DELETE FROM KeyValueTable;";
    ret = sqlite3_exec(db, clearTableSQL, nullptr, 0, &zErrMsg);
    if (ret != SQLITE_OK) {
        LOG_ERROR("SQL error: %s", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else {
        LOG_INFO("Database cleared successfully.");
    }
}
SQlite::~SQlite()
{
    sqlite3_close(db);
}
void SQlite::input_kv(std::unordered_map<std::string, std::string>& umap)
{
    // 首先删除数据库中的所有数据
    const char *sqlDelete = "DELETE FROM KeyValueTable;";
    char *zErrMsg = nullptr;
    int ret = sqlite3_exec(db, sqlDelete, nullptr, 0, &zErrMsg);
    if (ret != SQLITE_OK) {
        std::cerr << "SQL error: " << zErrMsg << std::endl;
        sqlite3_free(zErrMsg);
        return;
    }

    // 然后将输入的键值对全部导入数据库里
    const char *sqlInsert = "INSERT INTO KeyValueTable (Key, Value) VALUES (?, ?);";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sqlInsert, -1, &stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR("Failed to prepare statement : %s", sqlite3_errmsg(db));
        return;
    }

    for (const auto& kv : umap) {
        sqlite3_bind_text(stmt, 1, kv.first.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, kv.second.c_str(), -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            LOG_ERROR("Execution failed : %s", sqlite3_errmsg(db));
        }
        sqlite3_reset(stmt); // 重置语句，以便下一次绑定
    }

    sqlite3_finalize(stmt);
}

void SQlite::output_kv(std::unordered_map<std::string, std::string>& umap){
    // 清空传入的 umap
    umap.clear();

    // 查询数据库中的所有键值对
    const char *sqlSelect = "SELECT Key, Value FROM KeyValueTable;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sqlSelect, -1, &stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR("Failed to prepare statement : %s", sqlite3_errmsg(db));
        return;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        std::string key = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
        std::string value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
        umap[key] = value;
    }

    sqlite3_finalize(stmt);
}
bool SQlite::get(const std::string& key, std::string& value)
{
    const char *sqlSelect = "SELECT Value FROM KeyValueTable WHERE Key = ?;";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(db, sqlSelect, -1, &stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR("Failed to prepare statement : %s", sqlite3_errmsg(db));
        return false;
    }

    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
        return true;
    }
    else {
        LOG_ERROR("The key <%s> not found", key.c_str());
        sqlite3_finalize(stmt);
        return false;
    }
}

void SQlite::set(const std::string& key, const std::string& value)
{
    const char *sqlInsert = "INSERT OR REPLACE INTO KeyValueTable (Key, Value) VALUES (?, ?);";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(db, sqlInsert, -1, &stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR("Failed to prepare statement : %s", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        LOG_ERROR("Execution failed : %s", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
}
void SQlite::del(const std::string& key)
{
    const char *sqlDelete = "DELETE FROM KeyValueTable WHERE Key = ?;";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(db, sqlDelete, -1, &stmt, nullptr) != SQLITE_OK) {
        LOG_ERROR("Failed to prepare statement : %s", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        LOG_ERROR("Execution failed : %s", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
}
}