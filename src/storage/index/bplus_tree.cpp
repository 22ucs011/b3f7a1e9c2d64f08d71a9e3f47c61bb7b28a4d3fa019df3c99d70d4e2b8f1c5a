#include "storage/index/bplus_tree.h"

#include <algorithm>
#include <cstring>

#include "common/exception.h"

namespace kizuna::index
{
    BPlusTree::BPlusTree(PageManager &pm, FileManager &fm, page_id_t root_page_id, bool unique)
        : pm_(pm), fm_(fm), root_page_id_(root_page_id), unique_(unique)
    {
        if (root_page_id_ == config::INVALID_PAGE_ID)
        {
            page_id_t new_page = pm_.new_page(PageType::INDEX);
            BPlusTreeNode root = BPlusTreeNode::MakeLeaf(new_page);
            root.set_parent(config::INVALID_PAGE_ID);
            StoreNode(root);
            root_page_id_ = new_page;
        }
    }

    BPlusTree::SearchResult BPlusTree::Search(const std::vector<uint8_t> &key)
    {
        page_id_t current = root_page_id_;
        while (true)
        {
            BPlusTreeNode node = LoadNode(current);
            if (node.node_type() == BPlusTreeNode::NodeType::LEAF)
            {
                size_t idx = FindLeafIndex(node, key);
                if (idx < node.leaf_entries().size() && CompareKeys(node.leaf_entries()[idx].key, key) == 0)
                {
                    return {true, node.leaf_entries()[idx].value};
                }
                return {false, 0};
            }
            size_t child_index = FindInternalChild(node, key);
            current = node.children().at(child_index);
        }
    }

    void BPlusTree::Insert(const std::vector<uint8_t> &key, record_id_t value)
    {
        std::optional<std::vector<uint8_t>> promoted_key;
        std::optional<page_id_t> promoted_child;
        InsertRecursive(root_page_id_, key, value, promoted_key, promoted_child);
        if (promoted_key.has_value())
        {
            page_id_t new_root_page = pm_.new_page(PageType::INDEX);
            BPlusTreeNode new_root = BPlusTreeNode::MakeInternal(new_root_page);
            new_root.set_parent(config::INVALID_PAGE_ID);
            new_root.children().push_back(root_page_id_);
            new_root.children().push_back(*promoted_child);
            new_root.internal_entries().push_back(BPlusTreeNode::InternalEntry{*promoted_key, *promoted_child});

            BPlusTreeNode left_child = LoadNode(root_page_id_);
            left_child.set_parent(new_root_page);
            StoreNode(left_child);

            BPlusTreeNode right_child = LoadNode(*promoted_child);
            right_child.set_parent(new_root_page);
            StoreNode(right_child);

            StoreNode(new_root);
            root_page_id_ = new_root_page;
        }
    }

    void BPlusTree::Remove(const std::vector<uint8_t> &key, record_id_t value)
    {
        page_id_t current = root_page_id_;
        while (true)
        {
            BPlusTreeNode node = LoadNode(current);
            if (node.node_type() == BPlusTreeNode::NodeType::LEAF)
            {
                size_t idx = FindLeafIndex(node, key);
                while (idx < node.leaf_entries().size() && CompareKeys(node.leaf_entries()[idx].key, key) == 0)
                {
                    if (node.leaf_entries()[idx].value == value)
                    {
                        node.leaf_entries().erase(node.leaf_entries().begin() + idx);
                        StoreNode(node);
                        return;
                    }
                    ++idx;
                }
                return;
            }
            size_t child_index = FindInternalChild(node, key);
            if (child_index >= node.children().size())
                child_index = node.children().size() - 1;
            current = node.children().at(child_index);
        }
    }

    std::vector<record_id_t> BPlusTree::ScanEqual(const std::vector<uint8_t> &key) const
    {
        return ScanRange(key, true, key, true);
    }

    std::vector<record_id_t> BPlusTree::ScanRange(const std::optional<std::vector<uint8_t>> &lower_key,
                                                  bool lower_inclusive,
                                                  const std::optional<std::vector<uint8_t>> &upper_key,
                                                  bool upper_inclusive) const
    {
        std::vector<record_id_t> results;

        page_id_t current = config::INVALID_PAGE_ID;
        size_t start_index = 0;

        if (lower_key.has_value())
        {
            auto [leaf, index] = FindLeafPosition(*lower_key);
            current = leaf;
            start_index = index;
        }
        else
        {
            current = FindLeftmostLeaf();
            start_index = 0;
        }

        while (current != config::INVALID_PAGE_ID)
        {
            BPlusTreeNode node = LoadNode(current);
            if (node.leaf_entries().empty() || start_index >= node.leaf_entries().size())
            {
                current = node.next_leaf();
                start_index = 0;
                continue;
            }

            for (size_t idx = start_index; idx < node.leaf_entries().size(); ++idx)
            {
                const auto &entry = node.leaf_entries()[idx];

                if (lower_key.has_value())
                {
                    int cmp = CompareKeys(entry.key, *lower_key);
                    if (cmp < 0 || (cmp == 0 && !lower_inclusive))
                        continue;
                }

                if (upper_key.has_value())
                {
                    int cmp = CompareKeys(entry.key, *upper_key);
                    if (cmp > 0 || (cmp == 0 && !upper_inclusive))
                        return results;
                }

                results.push_back(entry.value);
            }

            current = node.next_leaf();
            start_index = 0;
        }

        return results;
    }

    BPlusTreeNode BPlusTree::LoadNode(page_id_t page_id) const
    {
        Page &page = pm_.fetch(page_id, true);
        BPlusTreeNode node = BPlusTreeNode::Deserialize(page);
        pm_.unpin(page_id, false);
        return node;
    }

    void BPlusTree::StoreNode(const BPlusTreeNode &node)
    {
        Page &page = pm_.fetch(node.page_id(), true);
        node.Serialize(page);
        pm_.unpin(node.page_id(), true);
    }

    size_t BPlusTree::FindLeafIndex(const BPlusTreeNode &leaf, const std::vector<uint8_t> &key) const
    {
        const auto &entries = leaf.leaf_entries();
        size_t idx = 0;
        while (idx < entries.size() && CompareKeys(entries[idx].key, key) < 0)
        {
            ++idx;
        }
        return idx;
    }

    size_t BPlusTree::FindInternalChild(const BPlusTreeNode &node, const std::vector<uint8_t> &key) const
    {
        const auto &entries = node.internal_entries();
        size_t idx = 0;
        while (idx < entries.size() && CompareKeys(entries[idx].key, key) <= 0)
        {
            ++idx;
        }
        return idx;
    }

    std::pair<page_id_t, size_t> BPlusTree::FindLeafPosition(const std::vector<uint8_t> &key) const
    {
        page_id_t current = root_page_id_;
        while (true)
        {
            BPlusTreeNode node = LoadNode(current);
            if (node.node_type() == BPlusTreeNode::NodeType::LEAF)
            {
                size_t idx = FindLeafIndex(node, key);
                return {current, idx};
            }
            size_t child_index = FindInternalChild(node, key);
            if (child_index >= node.children().size())
                child_index = node.children().size() - 1;
            current = node.children().at(child_index);
        }
    }

    page_id_t BPlusTree::FindLeftmostLeaf() const
    {
        page_id_t current = root_page_id_;
        while (true)
        {
            BPlusTreeNode node = LoadNode(current);
            if (node.node_type() == BPlusTreeNode::NodeType::LEAF)
                return current;
            if (node.children().empty())
                return config::INVALID_PAGE_ID;
            current = node.children().front();
        }
    }

    void BPlusTree::InsertRecursive(page_id_t page_id,
                                    const std::vector<uint8_t> &key,
                                    record_id_t value,
                                    std::optional<std::vector<uint8_t>> &out_promoted_key,
                                    std::optional<page_id_t> &out_new_child)
    {
        BPlusTreeNode node = LoadNode(page_id);
        if (node.node_type() == BPlusTreeNode::NodeType::LEAF)
        {
            size_t idx = FindLeafIndex(node, key);
            if (idx < node.leaf_entries().size() && CompareKeys(node.leaf_entries()[idx].key, key) == 0)
            {
                if (unique_)
                {
                    KIZUNA_THROW_INDEX(StatusCode::DUPLICATE_KEY, "Duplicate key insertion", "");
                }
                node.leaf_entries()[idx].value = value;
                StoreNode(node);
                return;
            }
            node.leaf_entries().insert(node.leaf_entries().begin() + idx,
                                       BPlusTreeNode::LeafEntry{key, value});

            if (node.leaf_entries().size() > config::BTREE_MAX_KEYS)
            {
                BPlusTreeNode new_leaf = BPlusTreeNode::MakeLeaf(pm_.new_page(PageType::INDEX));
                new_leaf.set_parent(node.parent_page_id());
                SplitLeaf(node, new_leaf, out_promoted_key);

                new_leaf.set_next_leaf(node.next_leaf());
                new_leaf.set_prev_leaf(node.page_id());
                node.set_next_leaf(new_leaf.page_id());

                if (new_leaf.next_leaf() != config::INVALID_PAGE_ID)
                {
                    BPlusTreeNode next = LoadNode(new_leaf.next_leaf());
                    next.set_prev_leaf(new_leaf.page_id());
                    StoreNode(next);
                }

                out_new_child = new_leaf.page_id();
                StoreNode(node);
                StoreNode(new_leaf);
            }
            else
            {
                StoreNode(node);
            }
            return;
        }

        size_t child_index = FindInternalChild(node, key);
        page_id_t child_page = node.children().at(child_index);

        std::optional<std::vector<uint8_t>> promoted_key;
        std::optional<page_id_t> promoted_child;
        InsertRecursive(child_page, key, value, promoted_key, promoted_child);
        if (!promoted_key.has_value())
        {
            StoreNode(node);
            out_promoted_key.reset();
            out_new_child.reset();
            return;
        }

        node.internal_entries().insert(node.internal_entries().begin() + child_index,
                                        BPlusTreeNode::InternalEntry{*promoted_key, *promoted_child});
        node.children().insert(node.children().begin() + child_index + 1, *promoted_child);

        if (node.internal_entries().size() > config::BTREE_MAX_KEYS)
        {
            BPlusTreeNode new_internal = BPlusTreeNode::MakeInternal(pm_.new_page(PageType::INDEX));
            new_internal.set_parent(node.parent_page_id());
            SplitInternal(node, new_internal, out_promoted_key);
            out_new_child = new_internal.page_id();

            for (auto child_id : new_internal.children())
            {
                BPlusTreeNode child = LoadNode(child_id);
                child.set_parent(new_internal.page_id());
                StoreNode(child);
            }

            StoreNode(node);
            StoreNode(new_internal);
        }
        else
        {
            StoreNode(node);
            out_new_child = std::nullopt;
            out_promoted_key = std::nullopt;
        }
    }

    void BPlusTree::SplitLeaf(BPlusTreeNode &node,
                              BPlusTreeNode &new_node,
                              std::optional<std::vector<uint8_t>> &promoted_key)
    {
        size_t total = node.leaf_entries().size();
        size_t split_point = total / 2;
        new_node.leaf_entries().assign(node.leaf_entries().begin() + split_point, node.leaf_entries().end());
        node.leaf_entries().erase(node.leaf_entries().begin() + split_point, node.leaf_entries().end());
        promoted_key = new_node.leaf_entries().front().key;
    }

    void BPlusTree::SplitInternal(BPlusTreeNode &node,
                                  BPlusTreeNode &new_node,
                                  std::optional<std::vector<uint8_t>> &promoted_key)
    {
        size_t total = node.internal_entries().size();
        size_t split_point = total / 2;

        auto pivot_it = node.internal_entries().begin() + split_point;
        promoted_key = pivot_it->key;

        new_node.internal_entries().assign(pivot_it + 1, node.internal_entries().end());
        node.internal_entries().erase(pivot_it, node.internal_entries().end());

        new_node.children().assign(node.children().begin() + split_point + 1, node.children().end());
        node.children().erase(node.children().begin() + split_point + 1, node.children().end());

        for (size_t i = 0; i < new_node.internal_entries().size(); ++i)
        {
            new_node.internal_entries()[i].child = new_node.children().at(i + 1);
        }
    }

    int BPlusTree::CompareKeys(const std::vector<uint8_t> &lhs, const std::vector<uint8_t> &rhs)
    {
        const size_t min_len = std::min(lhs.size(), rhs.size());
        int cmp = 0;
        if (min_len > 0)
        {
            cmp = std::memcmp(lhs.data(), rhs.data(), min_len);
            if (cmp != 0)
                return cmp;
        }
        if (lhs.size() < rhs.size())
            return -1;
        if (lhs.size() > rhs.size())
            return 1;
        return 0;
    }
}
