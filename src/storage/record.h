#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"

namespace kizuna::record
{
    struct Field
    {
        DataType type{DataType::NULL_TYPE};
        bool is_null{false};
        std::vector<std::uint8_t> payload{};
    };

    Field from_null(DataType declared_type);
    Field from_bool(bool v);
    Field from_int32(std::int32_t v);
    Field from_int64(std::int64_t v);
    Field from_double(double v);
    Field from_string(std::string_view s);
    Field from_date(std::int64_t days_since_epoch);
    Field from_blob(const std::vector<std::uint8_t> &b);

    std::vector<std::uint8_t> encode(const std::vector<Field> &fields);
    bool decode(const std::uint8_t *data, std::size_t len, std::vector<Field> &out_fields);
}
