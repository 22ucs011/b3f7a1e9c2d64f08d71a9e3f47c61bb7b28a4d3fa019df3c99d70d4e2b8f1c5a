#include <array>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "storage/file_manager.h"
#include "common/logger.h"

using namespace kizuna;
namespace fs = std::filesystem;

bool file_manager_tests()
{
    const std::string db_path = (config::temp_dir() / (std::string("fm_test") + config::DB_FILE_EXTENSION)).string();

    // Ensure clean
    std::error_code ec;
    fs::remove(db_path, ec);

    FileManager fm(db_path, /*create_if_missing*/ true);
    try
    {
        fm.open();
        if (!fm.is_open()) return false;

        // Allocate a page
        const page_id_t pid = fm.allocate_page();
        if (pid < config::FIRST_PAGE_ID) return false;

        // Write a pattern
        std::vector<std::uint8_t> wbuf(config::PAGE_SIZE);
        for (size_t i = 0; i < wbuf.size(); ++i) wbuf[i] = static_cast<std::uint8_t>(i & 0xFF);
        fm.write_page(pid, wbuf.data());

        // Read back
        std::vector<std::uint8_t> rbuf(config::PAGE_SIZE);
        fm.read_page(pid, rbuf.data());
        if (rbuf != wbuf) return false;

        // Try invalid page read — expect exception
        bool threw = false;
        try
        {
            fm.read_page(0, rbuf.data());
        }
        catch (const DBException &)
        {
            threw = true;
        }
        if (!threw) return false;
    }
    catch (...)
    {
        return false;
    }
    return true;
}
