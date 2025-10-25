#include <cassert>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/bplus_tree_node.h"
#include "storage/page.h"

using namespace kizuna;
using namespace kizuna::index;

namespace
{
    std::vector<uint8_t> make_key(std::string_view text)
    {
        return std::vector<uint8_t>(text.begin(), text.end());
    }

    bool leaf_roundtrip()
    {
        Page page;
        page.init(PageType::INDEX, 42);

        auto node = BPlusTreeNode::MakeLeaf(42);
        node.set_parent(7);
        node.set_prev_leaf(41);
        node.set_next_leaf(43);

        auto &entries = node.leaf_entries();
        entries.push_back(BPlusTreeNode::LeafEntry{make_key("alpha"), 101});
        entries.push_back(BPlusTreeNode::LeafEntry{make_key("bravo"), 202});
        entries.push_back(BPlusTreeNode::LeafEntry{{}, 303}); // empty key support

        node.Serialize(page);
        auto decoded = BPlusTreeNode::Deserialize(page);

        assert(decoded.node_type() == BPlusTreeNode::NodeType::LEAF);
        assert(decoded.page_id() == 42);
        assert(decoded.parent_page_id() == 7);
        assert(decoded.prev_leaf() == 41);
        assert(decoded.next_leaf() == 43);
        assert(decoded.leaf_entries().size() == 3);
        assert(decoded.leaf_entries()[0].value == 101);
        assert(std::string(decoded.leaf_entries()[0].key.begin(), decoded.leaf_entries()[0].key.end()) == "alpha");
        assert(decoded.leaf_entries()[1].value == 202);
        assert(std::string(decoded.leaf_entries()[1].key.begin(), decoded.leaf_entries()[1].key.end()) == "bravo");
        assert(decoded.leaf_entries()[2].value == 303);
        assert(decoded.leaf_entries()[2].key.empty());

        return true;
    }

    bool internal_roundtrip()
    {
        Page page;
        page.init(PageType::INDEX, 128);

        auto node = BPlusTreeNode::MakeInternal(128);
        node.set_parent(7);

        auto &children = node.children();
        children = {500, 600, 700};

        auto &entries = node.internal_entries();
        entries.push_back(BPlusTreeNode::InternalEntry{make_key("k1"), 600});
        entries.push_back(BPlusTreeNode::InternalEntry{make_key("k2"), 700});

        node.Serialize(page);
        auto decoded = BPlusTreeNode::Deserialize(page);

        assert(decoded.node_type() == BPlusTreeNode::NodeType::INTERNAL);
        assert(decoded.children().size() == 3);
        assert(decoded.children()[0] == 500);
        assert(decoded.children()[1] == 600);
        assert(decoded.children()[2] == 700);
        assert(decoded.internal_entries().size() == 2);
        assert(decoded.internal_entries()[0].child == 600);
        assert(std::string(decoded.internal_entries()[0].key.begin(), decoded.internal_entries()[0].key.end()) == "k1");
        assert(decoded.internal_entries()[1].child == 700);
        assert(std::string(decoded.internal_entries()[1].key.begin(), decoded.internal_entries()[1].key.end()) == "k2");

        return true;
    }

    bool oversized_key_rejected()
    {
        Page page;
        page.init(PageType::INDEX, 900);

        auto node = BPlusTreeNode::MakeLeaf(900);
        auto &entries = node.leaf_entries();
        std::vector<uint8_t> huge(config::MAX_KEY_LENGTH + 1, 'x');
        entries.push_back(BPlusTreeNode::LeafEntry{huge, 11});

        bool threw = false;
        try
        {
            node.Serialize(page);
        }
        catch (const DBException &ex)
        {
            threw = (ex.code() == StatusCode::INVALID_ARGUMENT);
        }
        return threw;
    }

    bool invalid_magic_detection()
    {
        Page page;
        page.init(PageType::INDEX, 77);
        // Write bogus header
        auto *raw = reinterpret_cast<uint32_t *>(page.data() + Page::kHeaderSize);
        *raw = 0xDEADBEEF;
        bool threw = false;
        try
        {
            (void)BPlusTreeNode::Deserialize(page);
        }
        catch (const DBException &ex)
        {
            threw = (ex.code() == StatusCode::INVALID_RECORD_FORMAT);
        }
        return threw;
    }
}

bool bplus_tree_node_tests()
{
    bool ok = true;
    ok &= leaf_roundtrip();
    ok &= internal_roundtrip();
    ok &= oversized_key_rejected();
    ok &= invalid_magic_detection();
    return ok;
}
