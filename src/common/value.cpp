#include "common/value.h"

#include <charconv>
#include <chrono>
#include <iomanip>
#include <optional>
#include <sstream>
#include <stdexcept>

#include "common/exception.h"

namespace kizuna
{
    namespace
    {
        bool is_fixed_numeric(DataType type) noexcept
        {
            switch (type)
            {
            case DataType::INTEGER:
            case DataType::BIGINT:
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return true;
            default:
                return false;
            }
        }

        long double to_long_double(const Value &value)
        {
            switch (value.type())
            {
            case DataType::INTEGER:
                return static_cast<long double>(value.as_int32());
            case DataType::BIGINT:
            case DataType::DATE:
            case DataType::TIMESTAMP:
                return static_cast<long double>(value.as_int64());
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return static_cast<long double>(value.as_double());
            default:
                throw QueryException::type_error("numeric comparison", data_type_to_string(value.type()), data_type_to_string(value.type()));
            }
        }

        std::string pad_two(unsigned value)
        {
            char buf[3] = {0};
            buf[0] = static_cast<char>('0' + (value / 10));
            buf[1] = static_cast<char>('0' + (value % 10));
            return std::string(buf, 2);
        }

        std::string pad_four(int value)
        {
            std::ostringstream oss;
            oss << std::setw(4) << std::setfill('0') << value;
            return oss.str();
        }
    } // namespace

    Value::Value() = default;

    Value::Value(DataType type, bool is_null, std::variant<std::monostate, bool, std::int32_t, std::int64_t, double, std::string> payload)
        : type_(type), is_null_(is_null), data_(std::move(payload))
    {
    }

    Value Value::null(DataType type)
    {
        return Value(type, true, std::monostate{});
    }

    Value Value::boolean(bool v)
    {
        return Value(DataType::BOOLEAN, false, v);
    }

    Value Value::int32(std::int32_t v)
    {
        return Value(DataType::INTEGER, false, v);
    }

    Value Value::int64(std::int64_t v)
    {
        return Value(DataType::BIGINT, false, v);
    }

    Value Value::floating(double v)
    {
        return Value(DataType::DOUBLE, false, v);
    }

    Value Value::string(std::string v, DataType type)
    {
        return Value(type, false, std::move(v));
    }

    Value Value::date(std::int64_t days_since_epoch)
    {
        return Value(DataType::DATE, false, days_since_epoch);
    }

    bool Value::is_numeric() const noexcept
    {
        return is_fixed_numeric(type_);
    }

    bool Value::as_bool() const
    {
        if (type_ != DataType::BOOLEAN || is_null_)
            throw QueryException::type_error("boolean access", "BOOLEAN", data_type_to_string(type_));
        return std::get<bool>(data_);
    }

    std::int32_t Value::as_int32() const
    {
        if (type_ != DataType::INTEGER || is_null_)
            throw QueryException::type_error("int32 access", "INTEGER", data_type_to_string(type_));
        return std::get<std::int32_t>(data_);
    }

    std::int64_t Value::as_int64() const
    {
        if ((type_ != DataType::BIGINT && type_ != DataType::DATE && type_ != DataType::TIMESTAMP) || is_null_)
            throw QueryException::type_error("int64 access", "BIGINT", data_type_to_string(type_));
        return std::get<std::int64_t>(data_);
    }

    double Value::as_double() const
    {
        if ((type_ != DataType::FLOAT && type_ != DataType::DOUBLE) || is_null_)
            throw QueryException::type_error("double access", "DOUBLE", data_type_to_string(type_));
        return std::get<double>(data_);
    }

    const std::string &Value::as_string() const
    {
        if ((type_ != DataType::VARCHAR && type_ != DataType::TEXT) || is_null_)
            throw QueryException::type_error("string access", "TEXT", data_type_to_string(type_));
        return std::get<std::string>(data_);
    }

    std::string Value::to_string() const
    {
        if (is_null_)
            return "NULL";
        switch (type_)
        {
        case DataType::BOOLEAN:
            return as_bool() ? "TRUE" : "FALSE";
        case DataType::INTEGER:
            return std::to_string(as_int32());
        case DataType::BIGINT:
            return std::to_string(as_int64());
        case DataType::FLOAT:
        case DataType::DOUBLE:
        {
            std::ostringstream oss;
            oss << as_double();
            return oss.str();
        }
        case DataType::VARCHAR:
        case DataType::TEXT:
            return as_string();
        case DataType::DATE:
            return format_date(as_int64());
        case DataType::TIMESTAMP:
            return std::to_string(as_int64());
        default:
            return "<unsupported>";
        }
    }

    CompareResult compare(const Value &lhs, const Value &rhs)
    {
        if (lhs.is_null() || rhs.is_null())
        {
            return CompareResult::Unknown;
        }

        if (lhs.type() == rhs.type())
        {
            switch (lhs.type())
            {
            case DataType::BOOLEAN:
            {
                const bool l = lhs.as_bool();
                const bool r = rhs.as_bool();
                if (l == r) return CompareResult::Equal;
                return l ? CompareResult::Greater : CompareResult::Less;
            }
            case DataType::INTEGER:
            {
                const auto l = lhs.as_int32();
                const auto r = rhs.as_int32();
                if (l == r) return CompareResult::Equal;
                return l < r ? CompareResult::Less : CompareResult::Greater;
            }
            case DataType::BIGINT:
            case DataType::DATE:
            case DataType::TIMESTAMP:
            {
                const auto l = lhs.as_int64();
                const auto r = rhs.as_int64();
                if (l == r) return CompareResult::Equal;
                return l < r ? CompareResult::Less : CompareResult::Greater;
            }
            case DataType::FLOAT:
            case DataType::DOUBLE:
            {
                const auto l = lhs.as_double();
                const auto r = rhs.as_double();
                if (l == r) return CompareResult::Equal;
                return l < r ? CompareResult::Less : CompareResult::Greater;
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                const auto &l = lhs.as_string();
                const auto &r = rhs.as_string();
                if (l == r) return CompareResult::Equal;
                return l < r ? CompareResult::Less : CompareResult::Greater;
            }
            default:
                throw QueryException::unsupported_type(data_type_to_string(lhs.type()));
            }
        }

        if (lhs.is_numeric() && rhs.is_numeric())
        {
            const long double l = to_long_double(lhs);
            const long double r = to_long_double(rhs);
            if (l == r) return CompareResult::Equal;
            return l < r ? CompareResult::Less : CompareResult::Greater;
        }

        throw QueryException::type_error("comparison", data_type_to_string(lhs.type()), data_type_to_string(rhs.type()));
    }

    TriBool logical_and(TriBool lhs, TriBool rhs) noexcept
    {
        if (lhs == TriBool::False || rhs == TriBool::False)
            return TriBool::False;
        if (lhs == TriBool::Unknown || rhs == TriBool::Unknown)
            return TriBool::Unknown;
        return TriBool::True;
    }

    TriBool logical_or(TriBool lhs, TriBool rhs) noexcept
    {
        if (lhs == TriBool::True || rhs == TriBool::True)
            return TriBool::True;
        if (lhs == TriBool::Unknown || rhs == TriBool::Unknown)
            return TriBool::Unknown;
        return TriBool::False;
    }

    TriBool logical_not(TriBool value) noexcept
    {
        if (value == TriBool::Unknown) return TriBool::Unknown;
        return (value == TriBool::True) ? TriBool::False : TriBool::True;
    }

    std::optional<std::int64_t> parse_date(std::string_view text)
    {
        if (text.size() != 10 || text[4] != '-' || text[7] != '-')
            return std::nullopt;

        auto parse_int = [](std::string_view slice) -> std::optional<int>
        {
            int value = 0;
            if (slice.empty()) return std::nullopt;
            auto first = slice.data();
            auto last = slice.data() + slice.size();
            auto result = std::from_chars(first, last, value);
            if (result.ec != std::errc{} || result.ptr != last)
                return std::nullopt;
            return value;
        };

        auto year_opt = parse_int(text.substr(0, 4));
        auto month_opt = parse_int(text.substr(5, 2));
        auto day_opt = parse_int(text.substr(8, 2));
        if (!year_opt || !month_opt || !day_opt)
            return std::nullopt;

        const int year = *year_opt;
        const unsigned month = static_cast<unsigned>(*month_opt);
        const unsigned day = static_cast<unsigned>(*day_opt);

        using namespace std::chrono;
        const std::chrono::year y{year};
        const std::chrono::month m{month};
        const std::chrono::day d{day};
        std::chrono::year_month_day ymd{y, m, d};
        if (!ymd.ok())
            return std::nullopt;

        const std::chrono::sys_days days{ymd};
        return days.time_since_epoch().count();
    }

    std::string format_date(std::int64_t days_since_epoch)
    {
        using namespace std::chrono;
        const sys_days days{std::chrono::days{days_since_epoch}};
        const year_month_day ymd{days};
        const int year = static_cast<int>(ymd.year());
        const unsigned month = static_cast<unsigned>(ymd.month());
        const unsigned day = static_cast<unsigned>(ymd.day());
        return pad_four(year) + '-' + pad_two(month) + '-' + pad_two(day);
    }

    std::string data_type_to_string(DataType type)
    {
        switch (type)
        {
        case DataType::NULL_TYPE: return "NULL";
        case DataType::BOOLEAN: return "BOOLEAN";
        case DataType::INTEGER: return "INTEGER";
        case DataType::BIGINT: return "BIGINT";
        case DataType::FLOAT: return "FLOAT";
        case DataType::DOUBLE: return "DOUBLE";
        case DataType::VARCHAR: return "VARCHAR";
        case DataType::TEXT: return "TEXT";
        case DataType::DATE: return "DATE";
        case DataType::TIMESTAMP: return "TIMESTAMP";
        case DataType::BLOB: return "BLOB";
        default: return "UNKNOWN";
        }
    }
}


