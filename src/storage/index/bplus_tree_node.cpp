#include "storage/index/bplus_tree_node.h"

#include <algorithm>
#include <cstring>

namespace kizuna::index
{
    namespace
    {
        constexpr size_t kPageHeaderSize = Page::kHeaderSize;
        constexpr size_t kPageSize = Page::page_size();

        uint8_t *node_base(Page &page)
        {
            return page.data() + kPageHeaderSize;
        }

        const uint8_t *node_base(const Page &page)
        {
            return page.data() + kPageHeaderSize;
        }
    } // namespace

    constexpr size_t BPlusTreeNode::HeaderSize()
    {
        return sizeof(RawHeader);
    }

    BPlusTreeNode::BPlusTreeNode(NodeType type, page_id_t page_id) noexcept
        : type_(type), page_id_(page_id)
    {
    }

    BPlusTreeNode BPlusTreeNode::MakeLeaf(page_id_t page_id)
    {
        return BPlusTreeNode(NodeType::LEAF, page_id);
    }

    BPlusTreeNode BPlusTreeNode::MakeInternal(page_id_t page_id)
    {
        return BPlusTreeNode(NodeType::INTERNAL, page_id);
    }

    size_t BPlusTreeNode::key_count() const noexcept
    {
        return (type_ == NodeType::LEAF) ? leaf_entries_.size() : internal_entries_.size();
    }

    bool BPlusTreeNode::requires_split() const noexcept
    {
        const size_t keys = key_count();
        if (keys == 0)
            return false;
        if (keys > config::BTREE_MAX_KEYS)
            return true;

        size_t key_bytes = 0;
        if (type_ == NodeType::LEAF)
        {
            for (const auto &entry : leaf_entries_)
                key_bytes += entry.key.size();
        }
        else
        {
            for (const auto &entry : internal_entries_)
                key_bytes += entry.key.size();
        }

        size_t pos = kPageHeaderSize + HeaderSize();
        if (type_ == NodeType::LEAF)
        {
            pos += keys * sizeof(record_id_t);
        }
        else
        {
            pos += children_.size() * sizeof(page_id_t);
        }

        if (pos > Page::page_size())
            return true;

        pos += keys * sizeof(uint16_t);
        if (pos > Page::page_size())
            return true;

        const size_t available = Page::page_size() - pos;
        const size_t required = key_bytes + keys * sizeof(uint16_t);
        return required > available;
    }

    BPlusTreeNode::RawHeader BPlusTreeNode::ReadHeader(const Page &page)
    {
        if (static_cast<PageType>(page.header().page_type) != PageType::INDEX)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE, "Expected INDEX page", std::to_string(page.header().page_id));
        }

        RawHeader header{};
        std::memcpy(&header, node_base(page), sizeof(RawHeader));

        if (header.magic != kNodeMagic)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "B+ tree node magic mismatch", std::to_string(page.header().page_id));
        }
        if (header.node_type > static_cast<uint8_t>(NodeType::LEAF))
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "Unknown B+ tree node type", std::to_string(header.node_type));
        }
        if (header.key_count > config::BTREE_MAX_KEYS)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "B+ tree node key_count out of range", std::to_string(header.key_count));
        }
        const uint16_t min_offset = static_cast<uint16_t>(HeaderSize());
        const uint16_t max_offset = static_cast<uint16_t>(kPageSize - kPageHeaderSize);
        if (header.key_data_offset < min_offset || header.key_data_offset > max_offset)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "B+ tree node key-data offset invalid", std::to_string(header.key_data_offset));
        }
        return header;
    }

    void BPlusTreeNode::WriteHeader(Page &page, const RawHeader &header)
    {
        std::memcpy(node_base(page), &header, sizeof(RawHeader));
    }

    void BPlusTreeNode::Serialize(Page &page) const
    {
        if (page_id_ == config::INVALID_PAGE_ID)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "B+ tree node missing page id", "");
        }
        page.init(PageType::INDEX, page_id_);
        std::memset(node_base(page), 0, kPageSize - kPageHeaderSize);

        const size_t keys = key_count();
        if (keys > config::BTREE_MAX_KEYS)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Too many keys for B+ tree node", std::to_string(keys));
        }

        RawHeader header{};
        header.magic = kNodeMagic;
        header.node_type = static_cast<uint8_t>(type_);
        header.key_count = static_cast<uint16_t>(keys);
        header.parent_page_id = parent_page_id_;
        header.next_leaf_page_id = (type_ == NodeType::LEAF) ? next_leaf_page_id_ : config::INVALID_PAGE_ID;
        header.prev_leaf_page_id = (type_ == NodeType::LEAF) ? prev_leaf_page_id_ : config::INVALID_PAGE_ID;

        uint8_t *base = page.data();
        size_t pos = kPageHeaderSize + HeaderSize();

        std::vector<std::vector<uint8_t>> key_bytes;
        key_bytes.reserve(keys);

        if (type_ == NodeType::LEAF)
        {
            if (leaf_entries_.size() != keys)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Leaf entry count mismatch", std::to_string(leaf_entries_.size()));
            }
            record_id_t *value_ptr = reinterpret_cast<record_id_t *>(base + pos);
            for (size_t i = 0; i < keys; ++i)
            {
                const auto &entry = leaf_entries_[i];
                if (entry.key.size() > config::MAX_KEY_LENGTH)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Leaf key length exceeds limit", std::to_string(entry.key.size()));
                }
                key_bytes.push_back(entry.key);
                value_ptr[i] = entry.value;
            }
            pos += keys * sizeof(record_id_t);
        }
        else
        {
            if (internal_entries_.size() != keys)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Internal entry count mismatch", std::to_string(internal_entries_.size()));
            }
            if (children_.size() != keys + 1)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Internal node child count mismatch", std::to_string(children_.size()));
            }
            page_id_t *child_ptr = reinterpret_cast<page_id_t *>(base + pos);
            for (size_t i = 0; i < children_.size(); ++i)
            {
                child_ptr[i] = children_[i];
            }
            pos += children_.size() * sizeof(page_id_t);

            for (size_t i = 0; i < keys; ++i)
            {
                const auto &entry = internal_entries_[i];
                if (entry.key.size() > config::MAX_KEY_LENGTH)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Internal key length exceeds limit", std::to_string(entry.key.size()));
                }
                key_bytes.push_back(entry.key);
            }
        }

        uint16_t *offsets = reinterpret_cast<uint16_t *>(base + pos);
        pos += keys * sizeof(uint16_t);

        size_t key_data_ptr = kPageSize;
        for (size_t i = 0; i < keys; ++i)
        {
            const auto &key = key_bytes[i];
            const uint16_t len = static_cast<uint16_t>(key.size());
            key_data_ptr -= len;
            if (key_data_ptr < pos)
            {
                KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE, "B+ tree node out of space while writing keys", std::to_string(page_id_));
            }
            std::memcpy(base + key_data_ptr, key.data(), len);
            key_data_ptr -= sizeof(uint16_t);
            if (key_data_ptr < pos)
            {
                KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE, "B+ tree node out of space while writing key length", std::to_string(page_id_));
            }
            std::memcpy(base + key_data_ptr, &len, sizeof(uint16_t));
            offsets[i] = static_cast<uint16_t>(key_data_ptr - kPageHeaderSize);
        }

        header.key_data_offset = static_cast<uint16_t>(key_data_ptr - kPageHeaderSize);

        WriteHeader(page, header);
    }

    BPlusTreeNode BPlusTreeNode::Deserialize(const Page &page)
    {
        RawHeader header = ReadHeader(page);
        const NodeType type = static_cast<NodeType>(header.node_type);

        BPlusTreeNode node(type, page.header().page_id);
        node.parent_page_id_ = header.parent_page_id;
        node.next_leaf_page_id_ = header.next_leaf_page_id;
        node.prev_leaf_page_id_ = header.prev_leaf_page_id;

        const size_t keys = header.key_count;
        if (keys == 0)
        {
            return node;
        }

        const uint8_t *base = page.data();
        size_t pos = kPageHeaderSize + HeaderSize();

        if (type == NodeType::LEAF)
        {
            node.leaf_entries_.reserve(keys);
            const record_id_t *values = reinterpret_cast<const record_id_t *>(base + pos);
            pos += keys * sizeof(record_id_t);

            const uint16_t *offsets = reinterpret_cast<const uint16_t *>(base + pos);
            pos += keys * sizeof(uint16_t);

            for (size_t i = 0; i < keys; ++i)
            {
                const uint16_t key_offset = offsets[i];
                if (key_offset < HeaderSize() || key_offset >= kPageSize - kPageHeaderSize)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "Leaf key offset out of range", std::to_string(key_offset));
                }
                const uint8_t *key_ptr = base + kPageHeaderSize + key_offset;
                uint16_t len = 0;
                std::memcpy(&len, key_ptr, sizeof(uint16_t));
                key_ptr += sizeof(uint16_t);
                if (len > config::MAX_KEY_LENGTH || (key_ptr + len) > base + kPageSize)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "Leaf key length invalid", std::to_string(len));
                }
                std::vector<uint8_t> key(len);
                if (len > 0)
                {
                    std::memcpy(key.data(), key_ptr, len);
                }
                node.leaf_entries_.push_back(LeafEntry{std::move(key), values[i]});
            }
        }
        else
        {
            node.internal_entries_.reserve(keys);
            node.children_.resize(keys + 1);
            const size_t child_bytes = node.children_.size() * sizeof(page_id_t);
            const page_id_t *children = reinterpret_cast<const page_id_t *>(base + pos);
            std::memcpy(node.children_.data(), children, child_bytes);
            pos += child_bytes;

            const uint16_t *offsets = reinterpret_cast<const uint16_t *>(base + pos);
            pos += keys * sizeof(uint16_t);

            for (size_t i = 0; i < keys; ++i)
            {
                const uint16_t key_offset = offsets[i];
                if (key_offset < HeaderSize() || key_offset >= kPageSize - kPageHeaderSize)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "Internal key offset out of range", std::to_string(key_offset));
                }
                const uint8_t *key_ptr = base + kPageHeaderSize + key_offset;
                uint16_t len = 0;
                std::memcpy(&len, key_ptr, sizeof(uint16_t));
                key_ptr += sizeof(uint16_t);
                if (len > config::MAX_KEY_LENGTH || (key_ptr + len) > base + kPageSize)
                {
                    KIZUNA_THROW_STORAGE(StatusCode::INVALID_RECORD_FORMAT, "Internal key length invalid", std::to_string(len));
                }
                std::vector<uint8_t> key(len);
                if (len > 0)
                {
                    std::memcpy(key.data(), key_ptr, len);
                }
                page_id_t right_child = node.children_.at(i + 1);
                node.internal_entries_.push_back(InternalEntry{std::move(key), right_child});
            }
        }

        return node;
    }

} // namespace kizuna::index
