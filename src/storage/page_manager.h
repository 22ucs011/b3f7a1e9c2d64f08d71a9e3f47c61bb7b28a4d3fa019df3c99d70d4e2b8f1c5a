#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <list>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/file_manager.h"
#include "storage/page.h"

namespace kizuna
{
    // Minimal page cache with LRU eviction and pin/unpin
    class PageManager
    {
    public:
        explicit PageManager(FileManager &fm, std::size_t capacity = config::DEFAULT_CACHE_SIZE);
        ~PageManager();

        // Non-copyable
        PageManager(const PageManager &) = delete;
        PageManager &operator=(const PageManager &) = delete;

        // Create a new page on disk and cache it pinned. Returns new page id.
        page_id_t new_page(PageType type);

        // Fetch page into cache (pins by default). Throws if not present on disk.
        Page &fetch(page_id_t id, bool pin = true);
        // Shortcut to load the catalog metadata page (page 1).
        Page &fetch_catalog_root(bool pin = true);

        uint32_t catalog_version() const noexcept { return catalog_version_; }
        page_id_t catalog_tables_root() const noexcept { return catalog_tables_root_; }
        page_id_t catalog_columns_root() const noexcept { return catalog_columns_root_; }
        page_id_t catalog_indexes_root() const noexcept { return catalog_indexes_root_; }
        index_id_t next_index_id() const noexcept { return next_index_id_; }
        table_id_t next_table_id() const noexcept { return next_table_id_; }
        void set_catalog_tables_root(page_id_t id);
        void set_catalog_columns_root(page_id_t id);
        void set_catalog_indexes_root(page_id_t id);
        void set_next_index_id(index_id_t id);
        void set_next_table_id(table_id_t id);

        // Unpin a page; if dirty=true, marks for flush.
        void unpin(page_id_t id, bool dirty = false);

        // Deallocate a page: marks it FREE and adds to free list for reuse.
        void free_page(page_id_t id);

        // Mark a page as dirty (to be flushed on eviction/flush_all)
        void mark_dirty(page_id_t id);

        // Flush a single page or all pages
        void flush(page_id_t id);
        void flush_all();

        std::size_t capacity() const noexcept { return capacity_; }
        uint32_t free_count() const noexcept { return free_count_; }

    private:
        struct Frame
        {
            page_id_t id{0};
            Page page{};
            bool dirty{false};
            std::size_t pin_count{0};
            std::list<page_id_t>::iterator lru_it{};
            bool in_lru{false};
        };

        FileManager &fm_;
        std::size_t capacity_;
        std::vector<Frame> frames_;
        std::unordered_map<page_id_t, std::size_t> page_table_; // id -> frame index
        std::list<page_id_t> lru_;                               // unpinned pages, front = most recent
        // SQLite-like freelist using trunk pages
        // Metadata (page 1) stores: magic, version, first_trunk_id, free_count
        uint32_t first_trunk_id_{0};
        uint32_t free_count_{0};
        uint32_t catalog_version_{config::CATALOG_SCHEMA_VERSION};
        page_id_t catalog_tables_root_{0};
        page_id_t catalog_columns_root_{0};
        page_id_t catalog_indexes_root_{0};
        index_id_t next_index_id_{1};
        table_id_t next_table_id_{1};


        // Metadata helpers
        void init_metadata_if_needed();
        void load_metadata();
        void save_metadata();

        // Trunk helpers
        static constexpr size_t trunk_header_size() { return 8; } // next_trunk(4) + leaf_count(4)
        static constexpr size_t trunk_capacity()
        {
            return (config::PAGE_SIZE - sizeof(PageHeader) - trunk_header_size()) / sizeof(uint32_t);
        }
        void trunk_write_new(page_id_t trunk_id, uint32_t next_trunk, uint32_t leaf_count);
        void trunk_append_leaf(page_id_t trunk_id, page_id_t leaf_id);
        bool trunk_pop_leaf(page_id_t trunk_id, page_id_t &out_leaf);
        uint32_t trunk_next(page_id_t trunk_id);

        std::size_t obtain_frame_for(page_id_t id, bool pin);
        std::size_t evict_frame();
        std::size_t find_free_frame() const;
    };
}





