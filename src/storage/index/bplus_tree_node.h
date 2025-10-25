#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/page.h"

namespace kizuna::index
{
    /**
     * Lightweight view of a B+ tree node encoded inside a Page.
     * Supports both internal (page-id children) and leaf (record-id values)
     * nodes with variable length keys packed into the page body.
     */
    class BPlusTreeNode
    {
    public:
        enum class NodeType : uint8_t
        {
            INTERNAL = 0,
            LEAF = 1,
        };

        struct LeafEntry
        {
            std::vector<uint8_t> key;      ///< serialized key bytes
            record_id_t value{0};          ///< payload: points into table heap
        };

        struct InternalEntry
        {
            std::vector<uint8_t> key;      ///< separator key
            page_id_t child{config::INVALID_PAGE_ID}; ///< child page to the right of key
        };

        static constexpr uint32_t kNodeMagic = 0x4B5A4958; // 'KZIX'

        static BPlusTreeNode MakeLeaf(page_id_t page_id);
        static BPlusTreeNode MakeInternal(page_id_t page_id);

        static BPlusTreeNode Deserialize(const Page &page);

        void Serialize(Page &page) const;

        NodeType node_type() const noexcept { return type_; }
        page_id_t page_id() const noexcept { return page_id_; }
        page_id_t parent_page_id() const noexcept { return parent_page_id_; }
        page_id_t next_leaf() const noexcept { return next_leaf_page_id_; }
        page_id_t prev_leaf() const noexcept { return prev_leaf_page_id_; }

        void set_parent(page_id_t parent) noexcept { parent_page_id_ = parent; }
        void set_next_leaf(page_id_t next) noexcept { next_leaf_page_id_ = next; }
        void set_prev_leaf(page_id_t prev) noexcept { prev_leaf_page_id_ = prev; }

        const std::vector<LeafEntry> &leaf_entries() const noexcept { return leaf_entries_; }
        const std::vector<InternalEntry> &internal_entries() const noexcept { return internal_entries_; }
        const std::vector<page_id_t> &children() const noexcept { return children_; }

        std::vector<LeafEntry> &leaf_entries() noexcept { return leaf_entries_; }
        std::vector<InternalEntry> &internal_entries() noexcept { return internal_entries_; }
        std::vector<page_id_t> &children() noexcept { return children_; }

        void set_page_id(page_id_t id) noexcept { page_id_ = id; }
        void set_type(NodeType type) noexcept { type_ = type; }

        size_t key_count() const noexcept;

    private:
        static constexpr size_t HeaderSize();

        BPlusTreeNode(NodeType type, page_id_t page_id) noexcept;

        struct RawHeader
        {
            uint32_t magic;
            uint8_t node_type;
            uint8_t reserved;
            uint16_t key_count;
            page_id_t parent_page_id;
            page_id_t next_leaf_page_id;
            page_id_t prev_leaf_page_id;
            uint16_t key_data_offset;
        };

        static RawHeader ReadHeader(const Page &page);
        static void WriteHeader(Page &page, const RawHeader &header);
        NodeType type_{NodeType::LEAF};
        page_id_t page_id_{config::INVALID_PAGE_ID};
        page_id_t parent_page_id_{config::INVALID_PAGE_ID};
        page_id_t next_leaf_page_id_{config::INVALID_PAGE_ID};
        page_id_t prev_leaf_page_id_{config::INVALID_PAGE_ID};

        std::vector<LeafEntry> leaf_entries_;
        std::vector<InternalEntry> internal_entries_;
        std::vector<page_id_t> children_; // size = key_count + 1 for internal nodes
    };

} // namespace kizuna::index


