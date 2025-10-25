#pragma once

#include <string>
#include <string_view>

#include "common/exception.h"
#include "sql/ast.h"

namespace kizuna::sql
{
    struct ParsedDDL
    {
        StatementKind kind{StatementKind::CREATE_TABLE};
        CreateTableStatement create;
        DropTableStatement drop;
        CreateIndexStatement create_index;
        DropIndexStatement drop_index;
        AlterTableStatement alter;
    };

    CreateTableStatement parse_create_table(std::string_view sql);
    DropTableStatement parse_drop_table(std::string_view sql);
    CreateIndexStatement parse_create_index(std::string_view sql);
    DropIndexStatement parse_drop_index(std::string_view sql);
    AlterTableStatement parse_alter_table(std::string_view sql);
    ParsedDDL parse_ddl(std::string_view sql);
}
