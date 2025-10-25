#include "storage/index/index_manager.h"

#include <system_error>

namespace kizuna::index
{
    IndexManager::IndexManager(std::filesystem::path base_dir)
        : base_dir_(std::move(base_dir))
    {
        if (base_dir_.empty())
        {
            base_dir_ = config::default_index_dir();
        }
        std::error_code ec;
        std::filesystem::create_directories(base_dir_, ec);
    }

    std::unique_ptr<IndexHandle> IndexManager::CreateIndex(const catalog::IndexCatalogEntry &entry)
    {
        auto handle = MakeHandle(entry, /*create_if_missing=*/true);

        BPlusTree &tree = handle->tree();
        if (tree.root_page_id() == config::INVALID_PAGE_ID)
        {
            KIZUNA_THROW_INDEX(StatusCode::INTERNAL_ERROR, "Failed to initialize index root", entry.name);
        }
        return handle;
    }

    std::unique_ptr<IndexHandle> IndexManager::OpenIndex(const catalog::IndexCatalogEntry &entry) const
    {
        return MakeHandle(entry, /*create_if_missing=*/false);
    }

    void IndexManager::DropIndex(const catalog::IndexCatalogEntry &entry) const
    {
        const auto path = FileManager::index_path(entry.index_id, base_dir_);
        if (FileManager::exists(path))
        {
            FileManager::remove_file(path);
        }
    }

    std::unique_ptr<IndexHandle> IndexManager::MakeHandle(const catalog::IndexCatalogEntry &entry, bool create_if_missing) const
    {
        const auto path = FileManager::index_path(entry.index_id, base_dir_);
        if (!create_if_missing && !FileManager::exists(path))
        {
            KIZUNA_THROW_INDEX(StatusCode::INDEX_NOT_FOUND, "Index file not found", entry.name);
        }

        auto fm = std::make_unique<FileManager>(path.string(), create_if_missing);
        fm->open();

        auto pm = std::make_unique<PageManager>(*fm, config::DEFAULT_CACHE_SIZE);

        page_id_t root_page = entry.root_page_id;
        auto tree = std::make_unique<BPlusTree>(*pm, *fm, root_page, entry.is_unique);
        return std::make_unique<IndexHandle>(std::move(fm), std::move(pm), std::move(tree));
    }
}

