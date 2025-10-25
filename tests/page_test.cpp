#include <cassert>
#include <string>
#include <vector>

#include "storage/page.h"
#include "storage/record.h"

using namespace kizuna;

static bool check_insert_read()
{
    Page p;
    p.init(PageType::DATA, 1);

    std::vector<record::Field> fields;
    fields.push_back(record::from_int32(42));
    fields.push_back(record::from_string("hello"));
    auto payload = record::encode(fields);

    slot_id_t slot = 0;
    if (!p.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot))
        return false;

    std::vector<uint8_t> out;
    if (!p.read(slot, out))
        return false;

    std::vector<record::Field> decoded;
    if (!record::decode(out.data(), out.size(), decoded))
        return false;

    if (decoded.size() != 2) return false;
    if (decoded[0].type != DataType::INTEGER || decoded[0].is_null) return false;
    if (decoded[1].type != DataType::VARCHAR || decoded[1].is_null) return false;
    return true;
}

static bool check_erase()
{
    Page p;
    p.init(PageType::DATA, 2);

    std::vector<uint8_t> data = {1, 2, 3, 4};
    slot_id_t s1 = 0, s2 = 0;
    if (!p.insert(data.data(), static_cast<uint16_t>(data.size()), s1)) return false;
    if (!p.insert(data.data(), static_cast<uint16_t>(data.size()), s2)) return false;

    if (!p.erase(s1)) return false;

    std::vector<uint8_t> out;
    if (p.read(s1, out)) return false;
    if (!p.read(s2, out)) return false;
    return true;
}

bool page_tests()
{
    auto check_fill_many = []() -> bool {
        Page p;
        p.init(PageType::DATA, 3);
        std::vector<record::Field> fields;
        fields.push_back(record::from_int32(42));
        fields.push_back(record::from_string("hello world"));
        auto payload = record::encode(fields);
        slot_id_t last_slot = 0;
        size_t inserts = 0;
        while (true)
        {
            slot_id_t s{};
            if (!p.insert(payload.data(), static_cast<uint16_t>(payload.size()), s)) break;
            last_slot = s;
            ++inserts;
            if (inserts > 10000) return false;
        }
        if (inserts == 0) return false;

        std::vector<uint8_t> out;
        if (!p.read(0, out)) return false;
        std::vector<record::Field> decoded;
        if (!record::decode(out.data(), out.size(), decoded)) return false;
        if (decoded.size() != 2) return false;
        if (decoded[0].type != DataType::INTEGER || decoded[0].is_null) return false;
        if (decoded[1].type != DataType::VARCHAR || decoded[1].is_null) return false;

        if (!p.read(last_slot, out)) return false;
        decoded.clear();
        if (!record::decode(out.data(), out.size(), decoded)) return false;
        if (decoded.size() != 2) return false;
        if (decoded[0].type != DataType::INTEGER || decoded[0].is_null) return false;
        if (decoded[1].type != DataType::VARCHAR || decoded[1].is_null) return false;

        if (last_slot > 4)
        {
            for (slot_id_t s : {static_cast<slot_id_t>(1), static_cast<slot_id_t>(last_slot / 2), static_cast<slot_id_t>(last_slot - 1)})
            {
                if (!p.read(s, out)) return false;
                decoded.clear();
                if (!record::decode(out.data(), out.size(), decoded)) return false;
                if (decoded.size() != 2) return false;
            }
        }
        return true;
    };

    auto check_invalid_slot = []() -> bool {
        Page p;
        p.init(PageType::DATA, 4);
        std::vector<uint8_t> out;
        if (p.read(0, out)) return false;
        return true;
    };

    return check_insert_read() && check_erase() && check_fill_many() && check_invalid_slot();
}
