#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/types.h"
#include "common/value.h"
#include "catalog/schema.h"
#include "storage/page_manager.h"

namespace kizuna
{
    class TableHeap
    {
    public:
        struct RowLocation
        {
            page_id_t page_id{config::INVALID_PAGE_ID};
            slot_id_t slot{0};

            bool operator==(const RowLocation &other) const noexcept
            {
                return page_id == other.page_id && slot == other.slot;
            }
        };

        class Iterator;

        TableHeap(PageManager &pm, page_id_t root_page_id);

        page_id_t root_page_id() const noexcept { return root_page_id_; }

        RowLocation insert(const std::vector<uint8_t> &payload);
        RowLocation update(const RowLocation &loc, const std::vector<uint8_t> &payload);
        bool erase(const RowLocation &loc);
        bool read(const RowLocation &loc, std::vector<uint8_t> &out) const;
        void truncate();

        template <typename Fn>
        void scan(Fn &&fn);

        Iterator begin();
        Iterator end();

    private:
        PageManager &pm_;
        page_id_t root_page_id_;
        page_id_t tail_page_id_;

        page_id_t find_tail(page_id_t start) const;
        RowLocation append_new_page(page_id_t previous_tail, const std::vector<uint8_t> &payload);

    public:
        class Iterator
        {
        public:
            using difference_type = std::ptrdiff_t;
            using value_type = std::vector<uint8_t>;
            using pointer = const std::vector<uint8_t> *;
            using reference = const std::vector<uint8_t> &;
            using iterator_category = std::input_iterator_tag;

            Iterator() = default;

            reference operator*() const { return payload_; }
            pointer operator->() const { return &payload_; }

            Iterator &operator++();
            Iterator operator++(int);

            bool operator==(const Iterator &other) const;
            bool operator!=(const Iterator &other) const { return !(*this == other); }

            RowLocation location() const noexcept { return loc_; }
            const std::vector<uint8_t> &payload() const noexcept { return payload_; }

        private:
            Iterator(TableHeap *heap, page_id_t page, slot_id_t slot, bool end);
            void advance();

            TableHeap *heap_{nullptr};
            page_id_t page_{config::INVALID_PAGE_ID};
            slot_id_t slot_{0};
            RowLocation loc_{};
            std::vector<uint8_t> payload_{};
            bool end_{true};

            friend class TableHeap;
        };
    };

    class TableHeapMigration
    {
    public:
        static page_id_t rewrite(PageManager &pm,
                                 page_id_t source_root,
                                 const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                 const std::vector<catalog::ColumnCatalogEntry> &new_schema,
                                 const std::unordered_map<column_id_t, Value> &defaults);

        static page_id_t add_column(PageManager &pm,
                                    page_id_t source_root,
                                    const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                    const catalog::ColumnCatalogEntry &new_column,
                                    const std::optional<Value> &default_value);

        static page_id_t drop_column(PageManager &pm,
                                     page_id_t source_root,
                                     const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                     column_id_t drop_column_id);

        static void free_chain(PageManager &pm, page_id_t root_page_id);
    };

    template <typename Fn>
    inline void TableHeap::scan(Fn &&fn)
    {
        for (auto it = begin(); it != end(); ++it)
        {
            fn(it.location(), it.payload());
        }
    }
}
