#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <cassert>

#include "common/exception.h"
#include "engine/ddl_executor.h"
#include "engine/dml_executor.h"
#include "sql/dml_parser.h"
#include "storage/file_manager.h"
#include "storage/page_manager.h"
#include "storage/index/index_manager.h"
#include "storage/record.h"

using namespace kizuna;
namespace fs = std::filesystem;

namespace
{
    struct TestContext
    {
        std::string db_path;
        FileManager fm;
        std::unique_ptr<PageManager> pm;
        std::unique_ptr<catalog::CatalogManager> catalog;
        std::unique_ptr<index::IndexManager> index_manager;

        explicit TestContext(const std::string &name)
            : db_path((config::temp_dir() / (name + config::DB_FILE_EXTENSION)).string()),
              fm(db_path, /*create_if_missing*/ true)
        {
            std::error_code ec;
            fs::create_directories(config::temp_dir(), ec);
            fs::remove(db_path, ec);
            fm.open();
            pm = std::make_unique<PageManager>(fm, /*capacity*/ 32);
            catalog = std::make_unique<catalog::CatalogManager>(*pm, fm);
            index_manager = std::make_unique<index::IndexManager>();
        }

        ~TestContext()
        {
            catalog.reset();
            pm.reset();
            index_manager.reset();
            fm.close();
            std::error_code ec;
            fs::remove(db_path, ec);
        }
    };

    constexpr const char *kCreateEmployeesSql = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name VARCHAR(32), active BOOLEAN, age INTEGER, joined DATE, nickname VARCHAR(32));";
    constexpr const char *kSeedEmployeesSql = "INSERT INTO employees (id, name, active, age, joined, nickname) VALUES (1, 'amy', TRUE, 25, '2023-05-01', 'ace'), (2, 'beth', TRUE, 34, '2022-04-15', NULL), (3, 'cora', FALSE, 31, '2020-01-01', 'cee'), (4, 'dina', TRUE, 41, '2019-12-12', NULL);";

    bool basic_flow()
    {
        TestContext ctx("dml_exec_v04");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(32) NOT NULL, active BOOLEAN);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);

        auto insert_stmt = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE), (3, 'cara', TRUE);");
        auto insert_result = dml.insert_into(insert_stmt);
        if (insert_result.rows_inserted != 3) return false;

        auto select_limit = dml.select(sql::parse_select("SELECT name FROM users WHERE active LIMIT 1;"));
        if (select_limit.column_names != std::vector<std::string>{"name"}) return false;
        if (select_limit.rows.size() != 1 || select_limit.rows[0][0] != "alice") return false;

        auto delete_result = dml.delete_all(sql::parse_delete("DELETE FROM users WHERE active = FALSE;"));
        if (delete_result.rows_deleted != 1) return false;

        auto update_result = dml.update_all(sql::parse_update("UPDATE users SET name = 'ally', active = FALSE WHERE id = 1;"));
        if (update_result.rows_updated != 1) return false;

        auto update_long = dml.update_all(sql::parse_update("UPDATE users SET name = 'this string is definitely longer' WHERE id = 3;"));
        if (update_long.rows_updated != 1) return false;

        auto check_rows = dml.select(sql::parse_select("SELECT id, name, active FROM users;"));
        if (check_rows.column_names != std::vector<std::string>{"id", "name", "active"}) return false;
        if (check_rows.rows.size() != 2) return false;

        bool found_ally = false;
        bool found_cara = false;
        for (const auto &row : check_rows.rows)
        {
            if (row.size() != 3) return false;
            if (row[0] == "1" && row[1] == "ally" && row[2] == "FALSE")
                found_ally = true;
            if (row[0] == "3" && row[1] == "this string is definitely longer" && row[2] == "TRUE")
                found_cara = true;
        }
        if (!found_ally || !found_cara) return false;

        bool caught_not_null = false;
        try
        {
            dml.update_all(sql::parse_update("UPDATE users SET name = NULL WHERE id = 1;"));
        }
        catch (const QueryException &)
        {
            caught_not_null = true;
        }
        if (!caught_not_null) return false;

        auto limit_zero = dml.select(sql::parse_select("SELECT name FROM users LIMIT 0;"));
        if (limit_zero.rows.size() != 0 || limit_zero.column_names != std::vector<std::string>{"name"}) return false;

        auto delete_all = dml.delete_all(sql::parse_delete("DELETE FROM users;"));
        if (delete_all.rows_deleted != 2) return false;

        auto empty = dml.select(sql::parse_select("SELECT * FROM users;"));
        if (empty.rows.size() != 0) return false;
        if (empty.column_names != std::vector<std::string>{"id", "name", "active"}) return false;

        dml.truncate(sql::parse_truncate("TRUNCATE TABLE users;"));
        auto after_truncate = dml.select(sql::parse_select("SELECT * FROM users;"));
        if (!after_truncate.rows.empty()) return false;

        return true;
    }

    bool projection_limit_tests()
    {
        TestContext ctx("dml_exec_projection");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto projection = dml.select(sql::parse_select("SELECT id, name, active, age, joined, nickname FROM employees WHERE active AND age >= 30 LIMIT 5;"));
        const std::vector<std::string> expected_columns = {"id", "name", "active", "age", "joined", "nickname"};
        if (projection.column_names != expected_columns) return false;

        const std::vector<std::vector<std::string>> expected_rows = {
            {"2", "beth", "TRUE", "34", "2022-04-15", "NULL"},
            {"4", "dina", "TRUE", "41", "2019-12-12", "NULL"}
        };
        if (projection.rows != expected_rows) return false;

        auto star_projection = dml.select(sql::parse_select("SELECT * FROM employees LIMIT 1;"));
        if (star_projection.column_names != expected_columns) return false;

        auto limit_two = dml.select(sql::parse_select("SELECT name FROM employees WHERE active LIMIT 2;"));
        if (limit_two.rows != std::vector<std::vector<std::string>>{{"amy"}, {"beth"}}) return false;

        auto limit_all = dml.select(sql::parse_select("SELECT name FROM employees WHERE active LIMIT 10;"));
        if (limit_all.rows.size() != 3) return false;
        if (limit_all.rows[2][0] != "dina") return false;

        return true;
    }

    bool predicate_null_tests()
    {
        TestContext ctx("dml_exec_predicates");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto null_ids = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL;"));
        if (null_ids.rows != std::vector<std::vector<std::string>>{{"2"}, {"4"}}) return false;

        auto not_null = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NOT NULL;"));
        if (not_null.rows != std::vector<std::vector<std::string>>{{"1"}, {"3"}}) return false;

        auto not_active = dml.select(sql::parse_select("SELECT id FROM employees WHERE NOT active;"));
        if (not_active.rows != std::vector<std::vector<std::string>>{{"3"}}) return false;

        auto or_pred = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname = 'ace' OR NOT active;"));
        if (or_pred.rows != std::vector<std::vector<std::string>>{{"1"}, {"3"}}) return false;

        auto delete_none = dml.delete_all(sql::parse_delete("DELETE FROM employees WHERE nickname = 'zzz';"));
        if (delete_none.rows_deleted != 0) return false;

        auto update_null = dml.update_all(sql::parse_update("UPDATE employees SET nickname = NULL WHERE id = 3;"));
        if (update_null.rows_updated != 1) return false;

        auto null_after = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL;"));
        if (null_after.rows != std::vector<std::vector<std::string>>{{"2"}, {"3"}, {"4"}}) return false;

        auto delete_inactive = dml.delete_all(sql::parse_delete("DELETE FROM employees WHERE NOT active;"));
        if (delete_inactive.rows_deleted != 1) return false;

        auto remaining = dml.select(sql::parse_select("SELECT id FROM employees;"));
        if (remaining.rows.size() != 3) return false;
        if (remaining.rows[0][0] != "1" || remaining.rows[1][0] != "2" || remaining.rows[2][0] != "4") return false;

        return true;
    }

    bool order_by_tests()
    {
        TestContext ctx("dml_exec_order_by");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.execute("CREATE INDEX idx_employees_age ON employees(age);");
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto asc = dml.select(sql::parse_select("SELECT age FROM employees ORDER BY age;"));
        const std::vector<std::vector<std::string>> expected_asc = {{"25"}, {"31"}, {"34"}, {"41"}};
        assert(asc.rows == expected_asc);

        auto desc = dml.select(sql::parse_select("SELECT name FROM employees ORDER BY name DESC;"));
        const std::vector<std::vector<std::string>> expected_desc = {{"dina"}, {"cora"}, {"beth"}, {"amy"}};
        assert(desc.rows == expected_desc);

        auto filtered = dml.select(sql::parse_select("SELECT name FROM employees WHERE active ORDER BY age DESC LIMIT 2;"));
        const std::vector<std::vector<std::string>> expected_filtered = {{"dina"}, {"beth"}};
        assert(filtered.rows == expected_filtered);

        auto multi = dml.select(sql::parse_select("SELECT name, active, age FROM employees ORDER BY active DESC, age ASC;"));
        const std::vector<std::vector<std::string>> expected_multi = {
            {"amy", "TRUE", "25"},
            {"beth", "TRUE", "34"},
            {"dina", "TRUE", "41"},
            {"cora", "FALSE", "31"}
        };
        assert(multi.rows == expected_multi);

        return true;
    }

    bool distinct_tests()
    {
        TestContext ctx("dml_exec_distinct");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto distinct_active = dml.select(sql::parse_select("SELECT DISTINCT active FROM employees ORDER BY active;"));
        const std::vector<std::vector<std::string>> expected_active = {{"FALSE"}, {"TRUE"}};
        assert(distinct_active.rows == expected_active);

        auto distinct_nickname = dml.select(sql::parse_select("SELECT DISTINCT nickname FROM employees ORDER BY nickname;"));
        const std::vector<std::vector<std::string>> expected_nickname = {{"ace"}, {"cee"}, {"NULL"}};
        assert(distinct_nickname.rows == expected_nickname);

        auto distinct_limit = dml.select(sql::parse_select("SELECT DISTINCT name FROM employees WHERE active ORDER BY name DESC LIMIT 1;"));
        assert(distinct_limit.rows.size() == 1);
        assert(distinct_limit.rows[0][0] == "dina");

        return true;
    }

    bool aggregate_tests()
    {
        TestContext ctx("dml_exec_aggregate");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto aggregates = dml.select(sql::parse_select(
            "SELECT COUNT(*), COUNT(nickname), COUNT(DISTINCT active), SUM(age), AVG(age), MIN(name), MAX(joined) FROM employees;"));
        assert(aggregates.rows.size() == 1);
        const std::vector<std::string> expected_row = {"4", "2", "2", "131", "32.75", "amy", "2023-05-01"};
        assert(aggregates.rows[0] == expected_row);

        auto empty_sum = dml.select(sql::parse_select("SELECT SUM(age), AVG(age) FROM employees WHERE age > 100;"));
        assert(empty_sum.rows.size() == 1);
        assert(empty_sum.rows[0][0] == "NULL");
        assert(empty_sum.rows[0][1] == "NULL");

        auto count_limit = dml.select(sql::parse_select("SELECT COUNT(*) FROM employees LIMIT 0;"));
        assert(count_limit.rows.empty());

        return true;
    }

    bool join_tests()
    {
        TestContext ctx("dml_exec_join");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table(kCreateEmployeesSql);
        ddl.create_table("CREATE TABLE badges (employee_id INTEGER, badge VARCHAR(16));");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));
        dml.insert_into(sql::parse_insert("INSERT INTO badges (employee_id, badge) VALUES "
                                          "(1, 'mentor'), (2, 'lead'), (4, 'mentor'), (1, 'coach');"));

        auto join_basic = dml.select(sql::parse_select(
            "SELECT e.name, b.badge FROM employees AS e INNER JOIN badges AS b ON e.id = b.employee_id ORDER BY e.id;"));
        const std::vector<std::vector<std::string>> expected_basic = {
            {"amy", "mentor"},
            {"amy", "coach"},
            {"beth", "lead"},
            {"dina", "mentor"}
        };
        assert(join_basic.rows == expected_basic);
        const std::vector<std::string> expected_columns = {"e.name", "b.badge"};
        assert(join_basic.column_names == expected_columns);

        auto join_filtered = dml.select(sql::parse_select(
            "SELECT e.name FROM employees e INNER JOIN badges b ON e.id = b.employee_id WHERE b.badge = 'mentor' ORDER BY e.name;"));
        const std::vector<std::vector<std::string>> expected_filtered = {{"amy"}, {"dina"}};
        assert(join_filtered.rows == expected_filtered);

        return true;
    }

    bool index_single_column_test()
    {
        TestContext ctx("dml_exec_index_single");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table("CREATE TABLE items (id INTEGER PRIMARY KEY, sku VARCHAR(16), price INTEGER);");
        ddl.execute("CREATE INDEX idx_items_sku ON items(sku);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert("INSERT INTO items (id, sku, price) VALUES (1, 'sku1', 100), (2, 'sku2', 200);"));

        auto index_entry = ctx.catalog->get_index("idx_items_sku");
        assert(index_entry.has_value());

        auto handle = ctx.index_manager->OpenIndex(*index_entry);
        std::vector<record::Field> fields;
        fields.push_back(record::from_string("sku1"));
        auto key = record::encode(fields);
        auto lookup = handle->tree().Search(key);
        assert(lookup.found);

        dml.delete_all(sql::parse_delete("DELETE FROM items WHERE id = 1;"));

        index_entry = ctx.catalog->get_index("idx_items_sku");
        assert(index_entry.has_value());
        handle = ctx.index_manager->OpenIndex(*index_entry);
        lookup = handle->tree().Search(key);
        assert(!lookup.found);

        auto remaining = dml.select(sql::parse_select("SELECT sku FROM items;"));
        assert(remaining.rows.size() == 1);
        assert(remaining.rows[0][0] == "sku2");

        return true;
    }

    bool index_multi_column_test()
    {
        TestContext ctx("dml_exec_index_multi");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table("CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku VARCHAR(16), vendor VARCHAR(16), batch INTEGER);");
        ddl.execute("CREATE INDEX idx_inventory_sku_vendor ON inventory(sku, vendor);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert("INSERT INTO inventory (id, sku, vendor, batch) VALUES "
                                          "(1, 'skuA', 'north', 10), "
                                          "(2, 'skuA', 'south', 11), "
                                          "(3, 'skuB', 'north', 12);"));

        auto index_entry = ctx.catalog->get_index("idx_inventory_sku_vendor");
        assert(index_entry.has_value());
        auto handle = ctx.index_manager->OpenIndex(*index_entry);

        std::vector<record::Field> composite;
        composite.push_back(record::from_string("skuA"));
        composite.push_back(record::from_string("north"));
        auto composite_key = record::encode(composite);
        auto found = handle->tree().Search(composite_key);
        assert(found.found);

        std::vector<record::Field> alt_key_fields;
        alt_key_fields.push_back(record::from_string("skuB"));
        alt_key_fields.push_back(record::from_string("north"));
        auto alt_key = record::encode(alt_key_fields);
        auto alt_lookup = handle->tree().Search(alt_key);
        assert(alt_lookup.found);

        dml.delete_all(sql::parse_delete("DELETE FROM inventory WHERE id = 1;"));

        index_entry = ctx.catalog->get_index("idx_inventory_sku_vendor");
        assert(index_entry.has_value());
        handle = ctx.index_manager->OpenIndex(*index_entry);

        found = handle->tree().Search(composite_key);
        assert(!found.found);

        alt_lookup = handle->tree().Search(alt_key);
        assert(alt_lookup.found);

        auto remaining = dml.select(sql::parse_select("SELECT sku, vendor FROM inventory;"));
        assert(remaining.rows.size() == 2);
        bool saw_south = false;
        bool saw_north = false;
        for (const auto &row : remaining.rows)
        {
            assert(row.size() == 2);
            if (row[0] == "skuA" && row[1] == "south")
                saw_south = true;
            if (row[0] == "skuB" && row[1] == "north")
                saw_north = true;
        }
        assert(saw_south && saw_north);

        return true;
    }

    bool index_update_test()
    {
        TestContext ctx("dml_exec_index_update");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table("CREATE TABLE items (id INTEGER PRIMARY KEY, sku VARCHAR(16), price INTEGER);");
        ddl.execute("CREATE INDEX idx_items_sku ON items(sku);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert("INSERT INTO items (id, sku, price) VALUES (1, 'sku1', 100), (2, 'sku2', 200);"));

        auto update_result = dml.update_all(sql::parse_update("UPDATE items SET sku = 'sku9' WHERE id = 1;"));
        assert(update_result.rows_updated == 1);

        auto index_entry = ctx.catalog->get_index("idx_items_sku");
        assert(index_entry.has_value());
        auto handle = ctx.index_manager->OpenIndex(*index_entry);

        std::vector<record::Field> fields;
        fields.push_back(record::from_string("sku1"));
        auto old_key = record::encode(fields);
        auto lookup_old = handle->tree().ScanEqual(old_key);
        assert(lookup_old.empty());

        fields.clear();
        fields.push_back(record::from_string("sku9"));
        auto new_key = record::encode(fields);
        auto lookup_new = handle->tree().ScanEqual(new_key);
        assert(lookup_new.size() == 1);

        auto rows = dml.select(sql::parse_select("SELECT sku FROM items WHERE id = 1;"));
        assert(rows.rows.size() == 1);
        assert(rows.rows[0][0] == "sku9");

        return true;
    }

    bool index_usage_select_test()
    {
        TestContext ctx("dml_exec_index_usage");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        ddl.create_table("CREATE TABLE items (id INTEGER PRIMARY KEY, sku VARCHAR(16), price INTEGER);");
        ddl.execute("CREATE INDEX idx_items_sku ON items(sku);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        dml.insert_into(sql::parse_insert("INSERT INTO items (id, sku, price) VALUES (1, 'sku1', 90), (2, 'sku2', 120);"));

        bool index_used = false;
        dml.set_index_usage_observer([&](const catalog::IndexCatalogEntry &entry,
                                         const std::vector<record_id_t> &)
                                     {
                                         if (entry.name == "idx_items_sku")
                                             index_used = true;
                                     });

        auto rows = dml.select(sql::parse_select("SELECT id FROM items WHERE sku = 'sku1';"));
        assert(index_used);
        assert(rows.rows.size() == 1);
        assert(rows.rows[0][0] == "1");

        return true;
    }
}

bool index_maintenance_tests()
{
    return index_single_column_test() && index_multi_column_test() && index_update_test();
}

bool dml_executor_tests()
{
    return basic_flow() && projection_limit_tests() && predicate_null_tests() && order_by_tests() && distinct_tests() &&
           aggregate_tests() && join_tests() && index_usage_select_test() && index_maintenance_tests();
}
