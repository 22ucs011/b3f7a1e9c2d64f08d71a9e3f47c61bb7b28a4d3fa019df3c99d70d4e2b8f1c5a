#include <algorithm>
#include <vector>
#include <string>
#include <filesystem>

#include "storage/file_manager.h"
#include "storage/page_manager.h"

using namespace kizuna;
namespace fs = std::filesystem;

bool page_manager_freelist_tests()
{
    const std::string db = (config::temp_dir() / (std::string("pm_freelist") + config::DB_FILE_EXTENSION)).string();
    std::error_code ec; fs::remove(db, ec);

    try
    {
        // Phase 1: allocate many, then free them
        FileManager fm(db, true);
        fm.open();
        PageManager pm(fm, 4);
        std::vector<page_id_t> ids;
        const int N = 64;
        for (int i = 0; i < N; ++i)
        {
            auto id = pm.new_page(PageType::DATA);
            pm.unpin(id, false);
            ids.push_back(id);
        }
        for (auto id : ids)
        {
            pm.free_page(id);
        }
        pm.flush_all();

        // Phase 2: reopen and ensure we reuse freed ids
        FileManager fm2(db, true);
        fm2.open();
        PageManager pm2(fm2, 4);
        // Allocate a bunch, they should come from freelist
        std::vector<page_id_t> reused;
        for (int i = 0; i < N / 2; ++i)
        {
            auto id = pm2.new_page(PageType::DATA);
            pm2.unpin(id, false);
            reused.push_back(id);
        }
        // Check that all reused ids are from the original set
        for (auto id : reused)
        {
            if (std::find(ids.begin(), ids.end(), id) == ids.end())
                return false;
        }
    }
    catch (...)
    {
        return false;
    }
    return true;
}

