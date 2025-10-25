#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "storage/index/bplus_tree_node.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"

namespace kizuna::index
{
    class BPlusTree
    {
    public:
        struct SearchResult
        {
            bool found{false};
            record_id_t value{0};
        };

        BPlusTree(PageManager &pm, FileManager &fm, page_id_t root_page_id, bool unique);

        SearchResult Search(const std::vector<uint8_t> &key);
        void Insert(const std::vector<uint8_t> &key, record_id_t value);
        void Remove(const std::vector<uint8_t> &key, record_id_t value);

        std::vector<record_id_t> ScanEqual(const std::vector<uint8_t> &key) const;
        std::vector<record_id_t> ScanRange(const std::optional<std::vector<uint8_t>> &lower_key,
                                           bool lower_inclusive,
                                           const std::optional<std::vector<uint8_t>> &upper_key,
                                           bool upper_inclusive) const;

        page_id_t root_page_id() const noexcept { return root_page_id_; }
        bool is_unique() const noexcept { return unique_; }

    private:
        struct NavPath
        {
            page_id_t page_id{config::INVALID_PAGE_ID};
            size_t index{0};
        };

        PageManager &pm_;
        FileManager &fm_;
        page_id_t root_page_id_{config::INVALID_PAGE_ID};
        bool unique_{false};

        BPlusTreeNode LoadNode(page_id_t page_id) const;
        void StoreNode(const BPlusTreeNode &node);
        void InsertRecursive(page_id_t page_id, const std::vector<uint8_t> &key, record_id_t value,
                             std::optional<std::vector<uint8_t>> &out_promoted_key,
                             std::optional<page_id_t> &out_new_child);

        void SplitLeaf(BPlusTreeNode &node, BPlusTreeNode &new_node, std::optional<std::vector<uint8_t>> &promoted_key);
        void SplitInternal(BPlusTreeNode &node, BPlusTreeNode &new_node, std::optional<std::vector<uint8_t>> &promoted_key);

        size_t FindLeafIndex(const BPlusTreeNode &leaf, const std::vector<uint8_t> &key) const;
        size_t FindInternalChild(const BPlusTreeNode &node, const std::vector<uint8_t> &key) const;
        std::pair<page_id_t, size_t> FindLeafPosition(const std::vector<uint8_t> &key) const;
        page_id_t FindLeftmostLeaf() const;

        static int CompareKeys(const std::vector<uint8_t> &lhs, const std::vector<uint8_t> &rhs);
    };
}
