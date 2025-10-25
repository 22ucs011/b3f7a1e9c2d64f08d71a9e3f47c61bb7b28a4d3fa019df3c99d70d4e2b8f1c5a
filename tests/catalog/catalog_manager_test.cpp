#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include "storage/index/index_manager.h"
#include "catalog/catalog_manager.h"
#include "engine/ddl_executor.h"
#include "engine/dml_executor.h"
#include "common/config.h"
#include "common/exception.h"
#include "sql/dml_parser.h"

using namespace kizuna;

namespace fs = std::filesystem;

namespace fs = std::filesystem;

bool catalog_manager_ddl_tests()
{
    const std::string db_path = (config::temp_dir() / "catalog_manager_test.kz").string();
    fs::create_directories(fs::path(db_path).parent_path());
    if (fs::exists(db_path))
        fs::remove(db_path);

    FileManager fm(db_path, true);
    fm.open();
    PageManager pm(fm, 32);
    catalog::CatalogManager catalog(pm, fm);
    index::IndexManager index_manager;
    engine::DDLExecutor executor(catalog, pm, fm, index_manager);

    auto entry = executor.create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(32) NOT NULL, age INTEGER DEFAULT 0);");
    assert(entry.name == "users");
    assert(entry.table_id != 0);

    auto tables = catalog.list_tables();
    assert(tables.size() == 1);
    assert(tables.front().name == "users");

    auto columns = catalog.get_columns(entry.table_id);
    assert(columns.size() == 3);
    assert(columns[0].column.constraint.primary_key);
    assert(columns[1].column.constraint.not_null);
    assert(columns[2].column.constraint.has_default);
    auto existing_indexes = catalog.get_indexes(entry.table_id);
    assert(existing_indexes.size() == 1);
    assert(existing_indexes.front().is_primary);
    executor.execute("CREATE UNIQUE INDEX idx_users_name ON users(name);");
    assert(catalog.index_exists("idx_users_name"));
    auto fetched_index = catalog.get_index("idx_users_name");
    assert(fetched_index.has_value());
    assert(fetched_index->is_unique);

    bool duplicate_index_threw = false;
    try
    {
        executor.execute("CREATE UNIQUE INDEX idx_users_name ON users(name);");
    }
    catch (const DBException &ex)
    {
        duplicate_index_threw = (ex.code() == StatusCode::INVALID_CONSTRAINT || ex.code() == StatusCode::DUPLICATE_KEY);
    }
    assert(duplicate_index_threw);

    executor.execute("DROP INDEX idx_users_name;");
    assert(!catalog.index_exists("idx_users_name"));

    executor.execute("CREATE UNIQUE INDEX idx_users_name ON users(name);");

    auto table_meta_before = catalog.get_table(entry.table_id);
    assert(table_meta_before.has_value());
    uint32_t schema_version_before = table_meta_before->schema_version;
    column_id_t next_column_id_before = table_meta_before->next_column_id;

    engine::DMLExecutor dml_executor(catalog, pm, fm, index_manager);
    auto insert_result = dml_executor.insert_into(sql::parse_insert("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 40);"));
    assert(insert_result.rows_inserted == 2);

    executor.execute("ALTER TABLE users ADD COLUMN status BOOLEAN DEFAULT TRUE;");

    auto columns_after_add = catalog.get_columns(entry.table_id);
    assert(columns_after_add.size() == 4);
    assert(columns_after_add.back().column.name == "status");

    auto status_meta = catalog.get_column(entry.table_id, "status", true);
    assert(status_meta.has_value());
    assert(!status_meta->is_dropped);

    auto status_rows = dml_executor.select(sql::parse_select("SELECT id, status FROM users ORDER BY id;"));
    const std::vector<std::string> expected_status_header{"id", "status"};
    assert(status_rows.column_names == expected_status_header);
    assert(status_rows.rows.size() == 2);
    for (const auto &row : status_rows.rows)
    {
        assert(row.size() == 2);
        assert(row[1] == "TRUE");
    }

    auto table_meta_after_add = catalog.get_table(entry.table_id);
    assert(table_meta_after_add.has_value());
    assert(table_meta_after_add->schema_version == schema_version_before + 1);
    assert(table_meta_after_add->next_column_id == next_column_id_before + 1);

    executor.execute("CREATE INDEX idx_users_age ON users(age);");
    assert(catalog.index_exists("idx_users_age"));

    executor.execute("ALTER TABLE users DROP COLUMN age;");

    assert(!catalog.index_exists("idx_users_age"));

    auto columns_after_drop = catalog.get_columns(entry.table_id);
    assert(columns_after_drop.size() == 3);
    for (const auto &col : columns_after_drop)
    {
        assert(col.column.name != "age");
    }

    auto dropped_meta = catalog.get_column(entry.table_id, "age", true);
    assert(dropped_meta.has_value() && dropped_meta->is_dropped);

    auto final_rows = dml_executor.select(sql::parse_select("SELECT id, name, status FROM users ORDER BY id;"));
    const std::vector<std::string> expected_final_header{"id", "name", "status"};
    assert(final_rows.column_names == expected_final_header);
    assert(final_rows.rows.size() == 2);
    for (const auto &row : final_rows.rows)
    {
        assert(row.size() == 3);
        assert(row[2] == "TRUE");
    }

    auto table_meta_after_drop = catalog.get_table(entry.table_id);
    assert(table_meta_after_drop.has_value());
    assert(table_meta_after_drop->schema_version == table_meta_after_add->schema_version + 1);
    assert(table_meta_after_drop->next_column_id == table_meta_after_add->next_column_id);

    bool duplicate_threw = false;
    try
    {
        executor.create_table("CREATE TABLE users (id INTEGER);");
    }
    catch (const DBException &ex)
    {
        duplicate_threw = (ex.code() == StatusCode::TABLE_EXISTS);
    }
    assert(duplicate_threw);

    executor.drop_table("DROP TABLE users;");
    tables = catalog.list_tables();
    assert(tables.empty());
    assert(catalog.list_indexes().empty());

    auto table_file = FileManager::table_path(entry.table_id);
    assert(!fs::exists(table_file));

    executor.drop_table("DROP TABLE IF EXISTS users;");

    pm.flush_all();
    fm.close();
    if (fs::exists(db_path))
        fs::remove(db_path);

    return true;
}
