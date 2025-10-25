#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <unordered_map>

#include "catalog/schema.h"
#include "storage/index/bplus_tree.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"

namespace kizuna::index
{
    class IndexHandle
    {
    public:
        IndexHandle(std::unique_ptr<FileManager> file_manager,
                    std::unique_ptr<PageManager> page_manager,
                    std::unique_ptr<BPlusTree> tree)
            : file_manager_(std::move(file_manager)),
              page_manager_(std::move(page_manager)),
              tree_(std::move(tree))
        {
        }

        FileManager &file_manager() { return *file_manager_; }
        PageManager &page_manager() { return *page_manager_; }
        BPlusTree &tree() { return *tree_; }

    private:
        std::unique_ptr<FileManager> file_manager_;
        std::unique_ptr<PageManager> page_manager_;
        std::unique_ptr<BPlusTree> tree_;
    };

    class IndexManager
    {
    public:
        IndexManager(std::filesystem::path base_dir = config::default_index_dir());

        std::unique_ptr<IndexHandle> CreateIndex(const catalog::IndexCatalogEntry &entry);
        std::unique_ptr<IndexHandle> OpenIndex(const catalog::IndexCatalogEntry &entry) const;
        void DropIndex(const catalog::IndexCatalogEntry &entry) const;

    private:
        std::filesystem::path base_dir_;

        std::unique_ptr<IndexHandle> MakeHandle(const catalog::IndexCatalogEntry &entry, bool create_if_missing) const;
    };
}

