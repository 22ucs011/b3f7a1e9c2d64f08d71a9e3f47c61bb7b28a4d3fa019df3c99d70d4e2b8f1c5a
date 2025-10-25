#pragma once

#include <string>
#include <string_view>

#include "sql/ast.h"

namespace kizuna::sql
{
    InsertStatement parse_insert(std::string_view sql);
    SelectStatement parse_select(std::string_view sql);
    DeleteStatement parse_delete(std::string_view sql);
    UpdateStatement parse_update(std::string_view sql);
    TruncateStatement parse_truncate(std::string_view sql);
    ParsedDML parse_dml(std::string_view sql);
}
