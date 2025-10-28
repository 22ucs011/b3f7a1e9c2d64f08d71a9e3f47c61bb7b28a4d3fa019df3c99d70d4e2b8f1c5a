#include "storage/page_manager.h"

#include <algorithm>

namespace kizuna
{
    PageManager::PageManager(FileManager &fm, std::size_t capacity)
        : fm_(fm), capacity_(capacity ? capacity : 1), frames_(capacity_)
    {
        init_metadata_if_needed();
        load_metadata();
    }

    PageManager::~PageManager()
    {
        try { flush_all(); } catch (...) { /* best-effort */ }
    }

    page_id_t PageManager::new_page(PageType type)
    {
        page_id_t id = 0;
        if (first_trunk_id_ != 0 && free_count_ > 0)
        {
            // Try to pop from head trunk
            page_id_t leaf = 0;
            if (trunk_pop_leaf(first_trunk_id_, leaf))
            {
                // Got a leaf page id
                id = leaf;
                free_count_--;
                save_metadata();
            }
            else
            {
                // Head trunk had no leaves; use trunk page itself
                uint32_t next = trunk_next(first_trunk_id_);
                id = first_trunk_id_;
                first_trunk_id_ = next;
                free_count_--;
                save_metadata();
            }
        }
        else
        {
            // Append new page at file end
            id = fm_.allocate_page();
        }

        // Get a frame (pinned). Initialize header
        const std::size_t idx = obtain_frame_for(id, /*pin*/ true);
        auto &fr = frames_[idx];
        std::memset(fr.page.data(), 0, config::PAGE_SIZE);
        fr.page.init(type, id);
        fr.dirty = true;
        flush(id);
        fr.dirty = false;
        unpin(id, /*dirty*/ false);
        return id;
    }

    Page &PageManager::fetch(page_id_t id, bool pin)
    {
        if (id < config::FIRST_PAGE_ID)
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_NOT_FOUND, "Invalid page id", std::to_string(id));
        }

        auto it = page_table_.find(id);
        if (it != page_table_.end())
        {
            auto &fr = frames_[it->second];
            if (pin)
            {
                if (fr.pin_count == 0 && fr.in_lru)
                {
                    lru_.erase(fr.lru_it);
                    fr.in_lru = false;
                }
                fr.pin_count++;
            }
            else
            {
                // touch LRU if unpinned
                if (fr.pin_count == 0)
                {
                    if (fr.in_lru)
                    {
                        lru_.erase(fr.lru_it);
                    }
                    lru_.push_front(id);
                    fr.lru_it = lru_.begin();
                    fr.in_lru = true;
                }
            }
            return fr.page;
        }

        // Load from disk into a frame
        const std::size_t idx = obtain_frame_for(id, pin);
        auto &fr = frames_[idx];
        try
        {
            fm_.read_page(id, fr.page.data());
        }
        catch (const DBException &)
        {
            // release the frame since load failed
            page_table_.erase(id);
            fr.id = 0;
            if (fr.pin_count == 0 && fr.in_lru)
            {
                lru_.erase(fr.lru_it);
                fr.in_lru = false;
            }
            throw;
        }
        return fr.page;
    }

    Page &PageManager::fetch_catalog_root(bool pin)
    {
        auto &meta = fetch(config::FIRST_PAGE_ID, pin);
        const auto type = static_cast<PageType>(meta.header().page_type);
        if (type != PageType::METADATA)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE, "Catalog root is not metadata", std::to_string(meta.header().page_id));
        }
        return meta;
    }
    void PageManager::unpin(page_id_t id, bool dirty)
    {
        auto it = page_table_.find(id);
        if (it == page_table_.end())
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_NOT_FOUND, "Unpin unknown page", std::to_string(id));
        }
        auto &fr = frames_[it->second];
        if (fr.pin_count == 0)
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_LOCKED, "Unpin already unpinned", std::to_string(id));
        }
        fr.pin_count--;
        if (dirty) fr.dirty = true;
        if (fr.pin_count == 0)
        {
            // move to LRU front
            if (fr.in_lru)
            {
                lru_.erase(fr.lru_it);
            }
            lru_.push_front(id);
            fr.lru_it = lru_.begin();
            fr.in_lru = true;
        }
    }

    void PageManager::mark_dirty(page_id_t id)
    {
        auto it = page_table_.find(id);
        if (it == page_table_.end())
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_NOT_FOUND, "Mark dirty unknown page", std::to_string(id));
        }
        frames_[it->second].dirty = true;
    }

    void PageManager::free_page(page_id_t id)
    {
        if (id < config::FIRST_PAGE_ID + 1) // do not free metadata page (1)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Cannot free reserved page", std::to_string(id));
        }
        // Mark page as FREE on disk
        {
            Page &pg = fetch(id, /*pin*/ true);
            std::memset(pg.data(), 0, config::PAGE_SIZE);
            pg.init(PageType::FREE, id);
            unpin(id, /*dirty*/ true);
        }
        // Add to freelist: either append to head trunk leaves or create a new head trunk
        if (first_trunk_id_ != 0)
        {
            // Try appending; if full, make this page the new trunk head
            size_t cap = trunk_capacity();
            // Read current leaf_count
            Page trunk;
            fm_.read_page(first_trunk_id_, trunk.data());
            uint8_t *tb = trunk.data();
            size_t off = sizeof(PageHeader);
            uint32_t next = 0, leaf_count = 0;
            std::memcpy(&next, tb + off + 0, 4);
            std::memcpy(&leaf_count, tb + off + 4, 4);
            if (leaf_count < cap)
            {
                std::memcpy(tb + off + trunk_header_size() + leaf_count * 4, &id, 4);
                leaf_count++;
                std::memcpy(tb + off + 4, &leaf_count, 4);
                fm_.write_page(first_trunk_id_, trunk.data());
            }
            else
            {
                // Convert freed page to a trunk head
                trunk_write_new(id, first_trunk_id_, 0);
                first_trunk_id_ = id;
                save_metadata();
            }
        }
        else
        {
            // No trunk exists: make this page the first trunk
            trunk_write_new(id, 0, 0);
            first_trunk_id_ = id;
            save_metadata();
        }
        free_count_++;
        save_metadata();
    }

    void PageManager::flush(page_id_t id)
    {
        auto it = page_table_.find(id);
        if (it == page_table_.end())
        {
            // Not cached, nothing to do
            return;
        }
        auto &fr = frames_[it->second];
        if (fr.dirty)
        {
            fm_.write_page(fr.id, fr.page.data());
            fr.dirty = false;
        }
    }

    void PageManager::flush_all()
    {
        for (auto &kv : page_table_)
        {
            auto &fr = frames_[kv.second];
            if (fr.dirty)
            {
                fm_.write_page(fr.id, fr.page.data());
                fr.dirty = false;
            }
        }
    }

    std::size_t PageManager::find_free_frame() const
    {
        for (std::size_t i = 0; i < frames_.size(); ++i)
        {
            if (frames_[i].id == 0)
            {
                return i;
            }
        }
        return static_cast<std::size_t>(-1);
    }

    std::size_t PageManager::evict_frame()
    {
        // Choose LRU tail
        if (lru_.empty())
        {
            KIZUNA_THROW_STORAGE(StatusCode::CACHE_FULL, "No unpinned pages to evict", "");
        }
        const page_id_t victim_id = lru_.back();
        lru_.pop_back();

        auto it = page_table_.find(victim_id);
        if (it == page_table_.end())
        {
            KIZUNA_THROW_STORAGE(StatusCode::INTERNAL_ERROR, "LRU victim not in page table", std::to_string(victim_id));
        }
        std::size_t idx = it->second;
        auto &fr = frames_[idx];
        if (fr.pin_count != 0)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INTERNAL_ERROR, "Evicting pinned page", std::to_string(victim_id));
        }
        if (fr.dirty)
        {
            fm_.write_page(fr.id, fr.page.data());
            fr.dirty = false;
        }
        page_table_.erase(it);
        fr.id = 0;
        fr.in_lru = false;
        return idx;
    }

    std::size_t PageManager::obtain_frame_for(page_id_t id, bool pin)
    {
        auto free_idx = find_free_frame();
        std::size_t idx = (free_idx == static_cast<std::size_t>(-1)) ? evict_frame() : free_idx;
        auto &fr = frames_[idx];
        fr.id = id;
        fr.dirty = false;
        fr.pin_count = pin ? 1 : 0;
        if (!pin)
        {
            lru_.push_front(id);
            fr.lru_it = lru_.begin();
            fr.in_lru = true;
        }
        else
        {
            fr.in_lru = false;
        }
        page_table_[id] = idx;
        return idx;
    }

    // --- metadata + free list persistence ---
    void PageManager::init_metadata_if_needed()
    {
        if (fm_.page_count() == 0)
        {
            // Create metadata page at id 1
            const page_id_t meta_id = fm_.allocate_page(); // should be 1
            (void)meta_id;

            // Allocate root pages for catalog tables/columns
            Page tables;
            catalog_tables_root_ = fm_.allocate_page();
            tables.init(PageType::DATA, catalog_tables_root_);
            fm_.write_page(catalog_tables_root_, tables.data());

            Page columns;
            catalog_columns_root_ = fm_.allocate_page();
            columns.init(PageType::DATA, catalog_columns_root_);
            fm_.write_page(catalog_columns_root_, columns.data());

            Page indexes;
            catalog_indexes_root_ = fm_.allocate_page();
            indexes.init(PageType::DATA, catalog_indexes_root_);
            fm_.write_page(catalog_indexes_root_, indexes.data());


            first_trunk_id_ = 0;
            free_count_ = 0;
            next_index_id_ = 1;
            next_table_id_ = 1;
            catalog_version_ = config::CATALOG_SCHEMA_VERSION;
            save_metadata();
        }
    }

    void PageManager::load_metadata()
    {
        Page meta;
        fm_.read_page(config::FIRST_PAGE_ID, meta.data());
        const uint8_t *b = meta.data();
        const size_t off = sizeof(PageHeader);
        uint32_t magic = 0;
        uint32_t version = 0;
        std::memcpy(&magic, b + off + 0, 4);
        std::memcpy(&version, b + off + 4, 4);
        if (magic != 0x4B5A464Du /* 'KZFM' */)
        {
            first_trunk_id_ = 0;
            free_count_ = 0;
            catalog_tables_root_ = 0;
            catalog_columns_root_ = 0;
            next_table_id_ = 1;
            catalog_version_ = config::CATALOG_SCHEMA_VERSION;
            save_metadata();
            return;
        }
        catalog_version_ = version;
        bool metadata_dirty = false;
        std::memcpy(&first_trunk_id_, b + off + 8, 4);
        std::memcpy(&free_count_, b + off + 12, 4);
        if (catalog_version_ >= 3)
        {
            std::memcpy(&catalog_tables_root_, b + off + 16, 4);
            std::memcpy(&catalog_columns_root_, b + off + 20, 4);
            std::memcpy(&catalog_indexes_root_, b + off + 24, 4);
            uint32_t next_table_raw = 0;
            std::memcpy(&next_table_raw, b + off + 28, 4);
            uint32_t next_index_raw = 1;
            std::memcpy(&next_index_raw, b + off + 32, 4);
            next_table_id_ = static_cast<table_id_t>(next_table_raw);
            next_index_id_ = static_cast<index_id_t>(next_index_raw);
        }
        else if (catalog_version_ >= 2)
        {
            std::memcpy(&catalog_tables_root_, b + off + 16, 4);
            std::memcpy(&catalog_columns_root_, b + off + 20, 4);
            uint32_t next_id_raw = 0;
            std::memcpy(&next_id_raw, b + off + 24, 4);
            next_table_id_ = static_cast<table_id_t>(next_id_raw);
            catalog_indexes_root_ = 0;
            next_index_id_ = 1;
            metadata_dirty = true;
        }
        else
        {
            catalog_tables_root_ = 0;
            catalog_columns_root_ = 0;
            catalog_indexes_root_ = 0;
            next_table_id_ = 1;
            next_index_id_ = 1;
            metadata_dirty = true;
        }
        if (catalog_version_ < config::CATALOG_SCHEMA_VERSION)
        {
            catalog_version_ = config::CATALOG_SCHEMA_VERSION;
            metadata_dirty = true;
        }

        if (catalog_tables_root_ == 0)
        {
            Page tables;
            catalog_tables_root_ = fm_.allocate_page();
            tables.init(PageType::DATA, catalog_tables_root_);
            fm_.write_page(catalog_tables_root_, tables.data());
            metadata_dirty = true;
        }
        if (catalog_columns_root_ == 0)
        {
            Page columns;
            catalog_columns_root_ = fm_.allocate_page();
            columns.init(PageType::DATA, catalog_columns_root_);
            fm_.write_page(catalog_columns_root_, columns.data());
            metadata_dirty = true;
        }
        if (catalog_indexes_root_ == 0)
        {
            Page indexes;
            catalog_indexes_root_ = fm_.allocate_page();
            indexes.init(PageType::DATA, catalog_indexes_root_);
            fm_.write_page(catalog_indexes_root_, indexes.data());
            metadata_dirty = true;
        }
        if (next_table_id_ == 0)
        {
            next_table_id_ = 1;
            metadata_dirty = true;
        }
        if (next_index_id_ == 0)
        {
            next_index_id_ = 1;
            metadata_dirty = true;
        }

        if (metadata_dirty || catalog_version_ != config::CATALOG_SCHEMA_VERSION)
        {
            catalog_version_ = config::CATALOG_SCHEMA_VERSION;
            save_metadata();
        }
    }

    void PageManager::save_metadata()
    {
        Page meta;
        fm_.read_page(config::FIRST_PAGE_ID, meta.data());
        uint8_t *b = meta.data();
        const size_t off = sizeof(PageHeader);
        const uint32_t magic = 0x4B5A464D; // 'KZFM'
        const uint32_t version = catalog_version_;
        std::memcpy(b + off + 0, &magic, 4);
        std::memcpy(b + off + 4, &version, 4);
        std::memcpy(b + off + 8, &first_trunk_id_, 4);
        std::memcpy(b + off + 12, &free_count_, 4);
        std::memcpy(b + off + 16, &catalog_tables_root_, 4);
        std::memcpy(b + off + 20, &catalog_columns_root_, 4);
        std::memcpy(b + off + 24, &catalog_indexes_root_, 4);
        uint32_t next_table_raw = static_cast<uint32_t>(next_table_id_);
        std::memcpy(b + off + 28, &next_table_raw, 4);
        uint32_t next_index_raw = static_cast<uint32_t>(next_index_id_);
        std::memcpy(b + off + 32, &next_index_raw, 4);
        fm_.write_page(config::FIRST_PAGE_ID, meta.data());
    }
    void PageManager::set_catalog_tables_root(page_id_t id)
    {
        catalog_tables_root_ = id;
        save_metadata();
    }

    void PageManager::set_catalog_columns_root(page_id_t id)
    {
        catalog_columns_root_ = id;
        save_metadata();
    }

    void PageManager::set_catalog_indexes_root(page_id_t id)
    {
        catalog_indexes_root_ = id;
        save_metadata();
    }

    void PageManager::set_next_index_id(index_id_t id)
    {
        next_index_id_ = id;
        save_metadata();
    }

    void PageManager::set_next_table_id(table_id_t id)
    {
        next_table_id_ = id;
        save_metadata();
    }
    void PageManager::trunk_write_new(page_id_t trunk_id, uint32_t next_trunk, uint32_t leaf_count)
    {
        Page pg;
        fm_.read_page(trunk_id, pg.data());
        uint8_t *b = pg.data();
        const size_t off = sizeof(PageHeader);
        std::memcpy(b + off + 0, &next_trunk, 4);
        std::memcpy(b + off + 4, &leaf_count, 4);
        fm_.write_page(trunk_id, pg.data());
    }

    void PageManager::trunk_append_leaf(page_id_t trunk_id, page_id_t leaf_id)
    {
        Page pg;
        fm_.read_page(trunk_id, pg.data());
        uint8_t *b = pg.data();
        const size_t off = sizeof(PageHeader);
        uint32_t leaf_count = 0;
        std::memcpy(&leaf_count, b + off + 4, 4);
        if (leaf_count >= trunk_capacity())
        {
            KIZUNA_THROW_STORAGE(StatusCode::CACHE_FULL, "Trunk full", std::to_string(trunk_id));
        }
        std::memcpy(b + off + trunk_header_size() + leaf_count * 4, &leaf_id, 4);
        leaf_count++;
        std::memcpy(b + off + 4, &leaf_count, 4);
        fm_.write_page(trunk_id, pg.data());
    }

    bool PageManager::trunk_pop_leaf(page_id_t trunk_id, page_id_t &out_leaf)
    {
        Page pg;
        fm_.read_page(trunk_id, pg.data());
        uint8_t *b = pg.data();
        const size_t off = sizeof(PageHeader);
        uint32_t leaf_count = 0;
        std::memcpy(&leaf_count, b + off + 4, 4);
        if (leaf_count == 0)
            return false;
        leaf_count--;
        std::memcpy(&out_leaf, b + off + trunk_header_size() + leaf_count * 4, 4);
        std::memcpy(b + off + 4, &leaf_count, 4);
        fm_.write_page(trunk_id, pg.data());
        return true;
    }

    uint32_t PageManager::trunk_next(page_id_t trunk_id)
    {
        Page pg;
        fm_.read_page(trunk_id, pg.data());
        const uint8_t *b = pg.data();
        const size_t off = sizeof(PageHeader);
        uint32_t next = 0;
        std::memcpy(&next, b + off + 0, 4);
        return next;
    }
}
















