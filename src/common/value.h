#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "common/types.h"

namespace kizuna
{
    enum class TriBool : uint8_t
    {
        False = 0,
        True = 1,
        Unknown = 2
    };

    enum class CompareResult : int8_t
    {
        Less = -1,
        Equal = 0,
        Greater = 1,
        Unknown = 2
    };

    class Value
    {
    public:
        Value();

        static Value null(DataType type = DataType::NULL_TYPE);
        static Value boolean(bool v);
        static Value int32(std::int32_t v);
        static Value int64(std::int64_t v);
        static Value floating(double v);
        static Value string(std::string v, DataType type = DataType::VARCHAR);
        static Value date(std::int64_t days_since_epoch);

        DataType type() const noexcept { return type_; }
        bool is_null() const noexcept { return is_null_; }
        bool is_numeric() const noexcept;

        bool as_bool() const;
        std::int32_t as_int32() const;
        std::int64_t as_int64() const;
        double as_double() const;
        const std::string &as_string() const;

        std::string to_string() const;

    private:
        DataType type_{DataType::NULL_TYPE};
        bool is_null_{true};
        std::variant<std::monostate, bool, std::int32_t, std::int64_t, double, std::string> data_;

        explicit Value(DataType type, bool is_null, std::variant<std::monostate, bool, std::int32_t, std::int64_t, double, std::string> payload);
    };

    CompareResult compare(const Value &lhs, const Value &rhs);

    TriBool logical_and(TriBool lhs, TriBool rhs) noexcept;
    TriBool logical_or(TriBool lhs, TriBool rhs) noexcept;
    TriBool logical_not(TriBool value) noexcept;

    std::optional<std::int64_t> parse_date(std::string_view text);
    std::string format_date(std::int64_t days_since_epoch);
    std::string data_type_to_string(DataType type);
}
