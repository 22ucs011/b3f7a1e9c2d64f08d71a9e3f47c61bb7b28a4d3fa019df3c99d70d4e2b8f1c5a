#include <string>
#include <vector>
#include <cstdint>
#include <filesystem>

#include "storage/file_manager.h"
#include "common/exception.h"

using namespace kizuna;
namespace fs = std::filesystem;

bool file_manager_edge_tests()
{
    const std::string db = (config::temp_dir() / (std::string("fm_edge") + config::DB_FILE_EXTENSION)).string();
    std::error_code ec; fs::remove(db, ec);
    FileManager fm(db, true);
    try
    {
        fm.open();
        // Invalid read: page 0
        std::vector<std::uint8_t> buf(config::PAGE_SIZE);
        bool threw = false;
        try { fm.read_page(0, buf.data()); } catch (const DBException &) { threw = true; }
        if (!threw) return false;

        // Invalid write length
        threw = false;
        try { fm.write_page(config::FIRST_PAGE_ID, buf.data(), config::PAGE_SIZE - 1); } catch (const DBException &) { threw = true; }
        if (!threw) return false;

        // Allocate increases page_count by 1 each time
        auto before = fm.page_count();
        auto id = fm.allocate_page();
        if (fm.page_count() != before + 1) return false;
        (void)id;
    }
    catch (...)
    {
        return false;
    }
    return true;
}

