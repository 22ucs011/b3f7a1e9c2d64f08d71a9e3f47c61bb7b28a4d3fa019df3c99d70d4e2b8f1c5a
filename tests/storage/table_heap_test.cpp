#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <string_view>
#include <cstring>

#include "storage/table_heap.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"
#include "storage/record.h"
#include "catalog/schema.h"
#include "common/value.h"

using namespace kizuna;
namespace fs = std::filesystem;

namespace
{
    bool is_valid_page_id(page_id_t id)
    {
        return id >= config::FIRST_PAGE_ID;
    }

    struct TestContext
    {
        std::string db_path;
        FileManager fm;
        std::unique_ptr<PageManager> pm;

        TestContext(const std::string &name)
            : db_path((config::temp_dir() / (name + config::DB_FILE_EXTENSION)).string()),
              fm(db_path, true)
        {
            std::error_code ec;
            fs::create_directories(config::temp_dir(), ec);
            fs::remove(db_path, ec);
            fm.open();
            pm = std::make_unique<PageManager>(fm, 8);
        }

        ~TestContext()
        {
            pm.reset();
            fm.close();
            std::error_code ec;
            fs::remove(db_path, ec);
        }
    };

    std::vector<uint8_t> make_payload_with_label(int value, std::string_view label)
    {
        std::vector<record::Field> fields;
        fields.push_back(record::from_int32(value));
        fields.push_back(record::from_string(label));
        return record::encode(fields);
    }


    std::vector<uint8_t> make_payload(int value)
    {
        return make_payload_with_label(value, "val" + std::to_string(value));
    }

    bool decode_payload(const std::vector<uint8_t> &payload, int &out_int, std::string &out_str)
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
            return false;
        if (fields.size() < 2) return false;
        if (fields[0].payload.size() != 4) return false;
        std::memcpy(&out_int, fields[0].payload.data(), 4);
        out_str.assign(reinterpret_cast<const char *>(fields[1].payload.data()), fields[1].payload.size());
        return true;
    }

    bool decode_int(const std::vector<uint8_t> &payload, int &out)
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
            return false;
        if (fields.empty() || fields[0].payload.size() != 4)
            return false;
        std::memcpy(&out, fields[0].payload.data(), 4);
        return true;
    }

    bool basic_insert_scan()
    {
        TestContext ctx("table_heap_basic");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        const auto p1 = heap.insert(make_payload(1));
        const auto p2 = heap.insert(make_payload(2));
        const auto p3 = heap.insert(make_payload(3));

        std::vector<int> seen;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            int value = 0;
            if (!decode_int(*it, value)) return false;
            seen.push_back(value);
        }
        if (seen != std::vector<int>{1, 2, 3}) return false;
        if (p1.page_id != root || p2.page_id != root || p3.page_id != root) return false;
        return true;
    }

    bool overflow_insert()
    {
        TestContext ctx("table_heap_overflow");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        std::vector<uint8_t> big_payload(1500, 0xAB);
        std::vector<TableHeap::RowLocation> locs;
        for (int i = 0; i < 8; ++i)
        {
            locs.push_back(heap.insert(big_payload));
        }
        bool moved_to_new_page = false;
        for (const auto &loc : locs)
        {
            if (loc.page_id != root)
            {
                moved_to_new_page = true;
                break;
            }
        }
        if (!moved_to_new_page) return false;

        auto &root_page = ctx.pm->fetch(root, true);
        page_id_t next = root_page.next_page_id();
        ctx.pm->unpin(root, false);
        if (!is_valid_page_id(next)) return false;

        auto &tail = ctx.pm->fetch(locs.back().page_id, true);
        if (tail.prev_page_id() == config::INVALID_PAGE_ID) return false;
        ctx.pm->unpin(locs.back().page_id, false);

        size_t count = 0;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            ++count;
        }
        if (count != locs.size()) return false;
        return true;
    }

    bool erase_skip()
    {
        TestContext ctx("table_heap_erase");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        auto loc1 = heap.insert(make_payload(10));
        auto loc2 = heap.insert(make_payload(20));
        auto loc3 = heap.insert(make_payload(30));

        if (!heap.erase(loc2)) return false;
        std::vector<uint8_t> scratch;
        if (heap.read(loc2, scratch)) return false;

        std::vector<int> seen;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            int value = 0;
            if (!decode_int(*it, value)) return false;
            seen.push_back(value);
        }
        if (seen != std::vector<int>{10, 30}) return false;

        std::vector<uint8_t> out;
        if (!heap.read(loc1, out)) return false;
        if (!heap.read(loc3, out)) return false;
        return true;
    }

    bool truncate_resets()
    {
        TestContext ctx("table_heap_truncate");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        for (int i = 0; i < 12; ++i)
        {
            heap.insert(make_payload(i));
        }
        heap.truncate();

        if (heap.begin() != heap.end()) return false;

        auto &root_page = ctx.pm->fetch(root, true);
        if (root_page.header().slot_count != 0) return false;
        if (root_page.next_page_id() != config::INVALID_PAGE_ID) return false;
        ctx.pm->unpin(root, false);

        auto loc = heap.insert(make_payload(99));
        if (loc.page_id != root) return false;
        return true;
    }

    bool update_in_place_success()
    {
        TestContext ctx("table_heap_update_same");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        auto original = heap.insert(make_payload_with_label(10, "same"));

        auto updated = heap.update(original, make_payload_with_label(42, "diff"));
        if (!(updated == original)) return false;

        std::vector<uint8_t> out;
        if (!heap.read(updated, out)) return false;
        int value = 0;
        std::string text;
        if (!decode_payload(out, value, text)) return false;
        if (value != 42 || text != "diff") return false;
        return true;
    }

    bool update_relocates_to_new_slot()
    {
        TestContext ctx("table_heap_update_grow");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        auto original = heap.insert(make_payload_with_label(5, "tiny"));

        auto updated = heap.update(original, make_payload_with_label(6, "this string is definitely longer"));
        if (updated == original) return false;

        std::vector<uint8_t> out;
        if (!heap.read(updated, out)) return false;
        int value = 0;
        std::string text;
        if (!decode_payload(out, value, text)) return false;
        if (value != 6 || text != "this string is definitely longer") return false;

        std::vector<uint8_t> old_payload;
        if (heap.read(original, old_payload)) return false;
        return true;
    }

    bool scan_helper_visits_all_rows()
    {
        TestContext ctx("table_heap_scan_helper");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        heap.insert(make_payload(1));
        heap.insert(make_payload(2));
        heap.insert(make_payload(3));

        std::vector<int> seen;
        heap.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
            int v = 0;
            if (decode_int(payload, v))
            {
                seen.push_back(v);
            }
        });

        return seen == std::vector<int>{1, 2, 3};
    }

    catalog::ColumnCatalogEntry make_column_entry(table_id_t table_id,
                                                  column_id_t column_id,
                                                  uint32_t ordinal,
                                                  std::string name,
                                                  DataType type,
                                                  bool not_null = false,
                                                  uint32_t length = 0)
    {
        catalog::ColumnCatalogEntry entry;
        entry.table_id = table_id;
        entry.column_id = column_id;
        entry.ordinal_position = ordinal;
        entry.schema_version = 1;
        entry.is_dropped = false;
        entry.column.id = column_id;
        entry.column.name = std::move(name);
        entry.column.type = type;
        entry.column.length = length;
        entry.column.constraint.not_null = not_null;
        return entry;
    }

    bool migration_add_drop_column()
    {
        TestContext ctx("table_heap_migration");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);

        auto col_id = make_column_entry(1, 1, 0, "id", DataType::INTEGER, true);
        auto col_name = make_column_entry(1, 2, 1, "name", DataType::VARCHAR, false, 32);
        std::vector<catalog::ColumnCatalogEntry> schema{col_id, col_name};

        for (int i = 1; i <= 3; ++i)
        {
            std::vector<record::Field> fields;
            fields.push_back(record::from_int32(i));
            fields.push_back(record::from_string("user" + std::to_string(i)));
            heap.insert(record::encode(fields));
        }

        catalog::ColumnCatalogEntry col_active = make_column_entry(1, 3, 2, "active", DataType::BOOLEAN, false);
        auto new_root = TableHeapMigration::add_column(*ctx.pm, root, schema, col_active, Value::boolean(true));
        TableHeapMigration::free_chain(*ctx.pm, root);
        root = new_root;

        col_active.ordinal_position = 2;
        std::vector<catalog::ColumnCatalogEntry> schema_with_active = schema;
        schema_with_active[0].ordinal_position = 0;
        schema_with_active[1].ordinal_position = 1;
        schema_with_active.push_back(col_active);

        TableHeap heap_after_add(*ctx.pm, root);
        int expected = 1;
        bool add_ok = true;
        heap_after_add.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
            std::vector<record::Field> fields;
            if (!record::decode(payload.data(), payload.size(), fields))
            {
                add_ok = false;
                return;
            }
            if (fields.size() != 3 || fields[0].payload.size() != sizeof(int32_t) || fields[2].payload.size() != 1)
            {
                add_ok = false;
                return;
            }
            int32_t id_value = 0;
            std::memcpy(&id_value, fields[0].payload.data(), sizeof(int32_t));
            std::string name(reinterpret_cast<const char *>(fields[1].payload.data()), fields[1].payload.size());
            bool active = fields[2].payload[0] != 0;
            if (id_value != expected || name != ("user" + std::to_string(expected)) || !active)
            {
                add_ok = false;
                return;
            }
            ++expected;
        });
        if (!add_ok || expected != 4)
            return false;

        auto drop_root = TableHeapMigration::drop_column(*ctx.pm, root, schema_with_active, col_name.column_id);
        TableHeapMigration::free_chain(*ctx.pm, root);
        root = drop_root;

        TableHeap heap_after_drop(*ctx.pm, root);
        int check_id = 1;
        bool drop_ok = true;
        heap_after_drop.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
            std::vector<record::Field> fields;
            if (!record::decode(payload.data(), payload.size(), fields))
            {
                drop_ok = false;
                return;
            }
            if (fields.size() != 2 || fields[0].payload.size() != sizeof(int32_t) || fields[1].payload.size() != 1)
            {
                drop_ok = false;
                return;
            }
            int32_t id_value = 0;
            std::memcpy(&id_value, fields[0].payload.data(), sizeof(int32_t));
            bool active = fields[1].payload[0] != 0;
            if (id_value != check_id || !active)
            {
                drop_ok = false;
                return;
            }
            ++check_id;
        });
        TableHeapMigration::free_chain(*ctx.pm, root);
        return drop_ok && (check_id == 4);
    }
}

bool table_heap_tests()
{
    return basic_insert_scan() && overflow_insert() && erase_skip() && truncate_resets() &&
           update_in_place_success() && update_relocates_to_new_slot() && scan_helper_visits_all_rows() &&
           migration_add_drop_column();
}
