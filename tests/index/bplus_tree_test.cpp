#include <cassert>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "storage/index/bplus_tree.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"

using namespace kizuna;
using namespace kizuna::index;

namespace
{
    std::vector<uint8_t> to_key(const std::string &text)
    {
        return std::vector<uint8_t>(text.begin(), text.end());
    }

    bool basic_insert_search_unique()
    {
        const std::string path = (config::temp_dir() / "bplus_tree_basic.kzi").string();
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);

        FileManager fm(path, true);
        fm.open();
        PageManager pm(fm, 64);

        BPlusTree tree(pm, fm, config::INVALID_PAGE_ID, true);

        const size_t insert_count = 80; // enough to trigger splits under default max keys
        for (size_t i = 0; i < insert_count; ++i)
        {
            std::string key = "key_" + std::to_string(i);
            tree.Insert(to_key(key), static_cast<record_id_t>(i + 1));
        }

        for (size_t i = 0; i < insert_count; ++i)
        {
            std::string key = "key_" + std::to_string(i);
            auto res = tree.Search(to_key(key));
            assert(res.found);
            assert(res.value == static_cast<record_id_t>(i + 1));
        }

        auto missing = tree.Search(to_key("missing"));
        assert(!missing.found);

        bool threw_duplicate = false;
        try
        {
            tree.Insert(to_key("key_10"), 111);
        }
        catch (const DBException &ex)
        {
            threw_duplicate = (ex.code() == StatusCode::DUPLICATE_KEY);
        }
        assert(threw_duplicate);

        pm.flush_all();
        fm.close();
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        return true;
    }

    bool duplicate_allowed_when_not_unique()
    {
        const std::string path = (config::temp_dir() / "bplus_tree_dupe.kzi").string();
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);

        FileManager fm(path, true);
        fm.open();
        PageManager pm(fm, 32);

        BPlusTree tree(pm, fm, config::INVALID_PAGE_ID, false);
        tree.Insert(to_key("same"), 100);
        tree.Insert(to_key("same"), 200);

        auto res = tree.Search(to_key("same"));
        assert(res.found);
        assert(res.value == 200);

        pm.flush_all();
        fm.close();
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        return true;
    }

    bool range_query_tests()
    {
        const std::string path = (config::temp_dir() / "bplus_tree_range.kzi").string();
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);

        FileManager fm(path, true);
        fm.open();
        PageManager pm(fm, 32);

        BPlusTree tree(pm, fm, config::INVALID_PAGE_ID, false);
        tree.Insert(to_key("k1"), 10);
        tree.Insert(to_key("k2"), 20);
        tree.Insert(to_key("k2"), 21);
        tree.Insert(to_key("k3"), 30);
        tree.Insert(to_key("k4"), 40);

        auto equal = tree.ScanEqual(to_key("k2"));
        assert(equal.size() == 1);
        assert(equal[0] == 21);

        auto lower = std::optional<std::vector<uint8_t>>(to_key("k2"));
        auto upper = std::optional<std::vector<uint8_t>>(to_key("k4"));
        auto inclusive_range = tree.ScanRange(lower, true, upper, false);
        assert(inclusive_range.size() == 2);
        assert(inclusive_range[0] == 21);
        assert(inclusive_range[1] == 30);

        auto unbounded = tree.ScanRange(std::nullopt, false, std::optional<std::vector<uint8_t>>(to_key("k2")), true);
        assert(unbounded.size() == 2);
        assert(unbounded[0] == 10);
        assert(unbounded[1] == 21);

        pm.flush_all();
        fm.close();
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        return true;
    }
}

bool bplus_tree_tests()
{
    bool ok = true;
    ok &= basic_insert_search_unique();
    ok &= duplicate_allowed_when_not_unique();
    ok &= range_query_tests();
    return ok;
}
