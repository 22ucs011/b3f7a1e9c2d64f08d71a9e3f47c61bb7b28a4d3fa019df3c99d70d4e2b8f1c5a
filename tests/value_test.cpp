#include "common/value.h"

using namespace kizuna;

bool value_tests()
{
    auto v_int = Value::int32(42);
    auto v_big = Value::int64(42);
    if (compare(v_int, v_big) != CompareResult::Equal) return false;

    auto v_double = Value::floating(41.5);
    if (compare(v_big, v_double) != CompareResult::Greater) return false;

    auto v_null = Value::null(DataType::INTEGER);
    if (compare(v_null, v_big) != CompareResult::Unknown) return false;

    auto parsed = parse_date("2024-01-15");
    if (!parsed) return false;
    if (format_date(*parsed) != "2024-01-15") return false;

    auto invalid = parse_date("2024-13-15");
    if (invalid.has_value()) return false;

    auto d1 = Value::date(*parsed);
    auto d2 = Value::date(*parsed + 4);
    if (compare(d1, d2) != CompareResult::Less) return false;

    if (!v_int.is_numeric() || !Value::floating(0.0).is_numeric() || Value::string("literal").is_numeric()) return false;
    if (Value::boolean(true).is_numeric()) return false;
    if (Value::boolean(false).to_string() != "FALSE") return false;
    if (Value::null(DataType::INTEGER).to_string() != "NULL") return false;
    auto s1 = Value::string("abc");
    auto s2 = Value::string("abd");
    if (compare(s1, s2) != CompareResult::Less) return false;
    if (parse_date("20240115").has_value()) return false;
    if (logical_and(TriBool::True, TriBool::Unknown) != TriBool::Unknown) return false;
    if (logical_or(TriBool::False, TriBool::Unknown) != TriBool::Unknown) return false;
    if (logical_not(TriBool::Unknown) != TriBool::Unknown) return false;

    if (data_type_to_string(DataType::DATE) != "DATE") return false;

    return true;
}
