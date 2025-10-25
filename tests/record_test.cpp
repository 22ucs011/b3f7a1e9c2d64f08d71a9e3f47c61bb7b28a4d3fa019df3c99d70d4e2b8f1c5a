#include <cstdint>
#include <string>
#include <vector>
#include <cstring>

#include "common/exception.h"
#include "storage/record.h"

using namespace kizuna;

static bool check_encode_decode_basic()
{
    std::vector<record::Field> fields;
    fields.push_back(record::from_bool(true));
    fields.push_back(record::from_int32(123));
    fields.push_back(record::from_int64(4567890123LL));
    fields.push_back(record::from_double(3.14159));
    fields.push_back(record::from_string("alpha"));
    fields.push_back(record::from_null(DataType::VARCHAR));

    auto payload = record::encode(fields);

    std::vector<record::Field> out;
    if (!record::decode(payload.data(), payload.size(), out)) return false;
    if (out.size() != fields.size()) return false;
    if (out[4].type != DataType::VARCHAR || out[4].is_null) return false;
    if (!out[5].is_null) return false;
    return true;
}

static bool check_date_roundtrip()
{
    std::vector<record::Field> fields;
    const std::int64_t expected_days = 19'123;
    fields.push_back(record::from_date(expected_days));

    auto payload = record::encode(fields);
    std::vector<record::Field> out;
    if (!record::decode(payload.data(), payload.size(), out)) return false;
    if (out.size() != 1) return false;
    if (out[0].type != DataType::DATE) return false;
    if (out[0].payload.size() != sizeof(std::int64_t)) return false;
    std::int64_t actual = 0;
    std::memcpy(&actual, out[0].payload.data(), sizeof(std::int64_t));
    return actual == expected_days;
}

static bool check_encode_too_large()
{
    std::vector<record::Field> fields;
    std::string big(config::MAX_RECORD_SIZE + 100, 'x');
    auto blob = std::vector<std::uint8_t>(big.begin(), big.end());
    fields.push_back(record::from_blob(blob));
    try
    {
        auto payload = record::encode(fields);
        (void)payload;
    }
    catch (const DBException &e)
    {
        return e.code() == StatusCode::RECORD_TOO_LARGE;
    }
    return false;
}

bool record_tests()
{
    return check_encode_decode_basic() && check_encode_too_large() && check_date_roundtrip();
}
