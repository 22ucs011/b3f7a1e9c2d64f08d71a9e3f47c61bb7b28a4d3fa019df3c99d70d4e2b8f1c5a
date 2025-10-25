#include <cassert>
#include <filesystem>
#include <string>

#include "storage/index/index_manager.h"
#include "catalog/schema.h"

using namespace kizuna;
using namespace kizuna::index;

namespace
{
    catalog::IndexCatalogEntry make_entry(index_id_t id, table_id_t table_id, bool unique)
    {
        catalog::IndexCatalogEntry entry;
        entry.index_id = id;
        entry.table_id = table_id;
        entry.name = "idx_test_" + std::to_string(id);
        entry.root_page_id = config::INVALID_PAGE_ID;
        entry.is_unique = unique;
        entry.column_ids = {1};
        entry.create_sql = "CREATE INDEX";
        return entry;
    }
}

bool index_manager_tests()
{
    const auto base_dir = config::default_index_dir() / "test_suite";
    std::error_code ec;
    std::filesystem::remove_all(base_dir, ec);

    IndexManager manager(base_dir);

    auto entry = make_entry(1, 10, true);
    {
        auto handle = manager.CreateIndex(entry);
        assert(handle);
        assert(handle->tree().is_unique());
        entry.root_page_id = handle->tree().root_page_id();
    }

    {
        auto handle = manager.OpenIndex(entry);
        assert(handle);
        auto result = handle->tree().Search(std::vector<uint8_t>{});
        assert(!result.found);
        handle->tree().Insert(std::vector<uint8_t>{'a'}, 111);
        auto lookup = handle->tree().Search(std::vector<uint8_t>{'a'});
        assert(lookup.found);
        assert(lookup.value == 111);
    }

    manager.DropIndex(entry);
    assert(!FileManager::exists(FileManager::index_path(entry.index_id, base_dir)));

    std::filesystem::remove_all(base_dir, ec);
    return true;
}

