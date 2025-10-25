#include <cassert>
#include <string>

#include "sql/ddl_parser.h"
#include "common/exception.h"

using namespace kizuna;

bool sql_ddl_parser_tests()
{
    try
    {
        auto stmt = sql::parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(40) NOT NULL, age INTEGER);");
        assert(stmt.table_name == "users");
        assert(stmt.columns.size() == 3);
        assert(stmt.columns[0].constraint.primary_key);
        assert(stmt.columns[1].type == DataType::VARCHAR);
        assert(stmt.columns[1].length == 40);
        assert(stmt.columns[1].constraint.not_null);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto stmt = sql::parse_drop_table("DROP TABLE IF EXISTS users CASCADE;");
        assert(stmt.table_name == "users");
        assert(stmt.if_exists);
        assert(stmt.cascade);
    }
    catch (...)
    {
        return false;
    }

    bool caught = false;
    try
    {
        (void)sql::parse_create_table("CREATE TABLE broken ();");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR || ex.code() == StatusCode::INVALID_ARGUMENT);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);

    try
    {
        auto stmt = sql::parse_create_index("CREATE UNIQUE INDEX idx_users_name ON users(name, email);");
        assert(stmt.index_name == "idx_users_name");
        assert(stmt.unique);
        assert(stmt.table_name == "users");
        assert(stmt.column_names.size() == 2);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto stmt = sql::parse_drop_index("DROP INDEX IF EXISTS idx_users_name;");
        assert(stmt.index_name == "idx_users_name");
        assert(stmt.if_exists);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto ddl = sql::parse_ddl("CREATE INDEX idx_users_age ON users(age);");
        assert(ddl.kind == sql::StatementKind::CREATE_INDEX);
        assert(ddl.create_index.index_name == "idx_users_age");
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto stmt = sql::parse_alter_table("ALTER TABLE users ADD COLUMN status BOOLEAN NOT NULL;");
        assert(stmt.table_name == "users");
        assert(stmt.action == sql::AlterTableAction::ADD_COLUMN);
        assert(stmt.add_column.has_value());
        assert(stmt.add_column->name == "status");
        assert(stmt.add_column->type == DataType::BOOLEAN);
        assert(stmt.add_column->constraint.not_null);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto stmt = sql::parse_alter_table("ALTER TABLE users DROP COLUMN status;");
        assert(stmt.table_name == "users");
        assert(stmt.action == sql::AlterTableAction::DROP_COLUMN);
        assert(stmt.drop_column_name.has_value());
        assert(*stmt.drop_column_name == "status");
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto ddl = sql::parse_ddl("ALTER TABLE users DROP COLUMN email;");
        assert(ddl.kind == sql::StatementKind::ALTER_TABLE);
        assert(ddl.alter.action == sql::AlterTableAction::DROP_COLUMN);
        assert(ddl.alter.drop_column_name.has_value());
        assert(*ddl.alter.drop_column_name == "email");
    }
    catch (...)
    {
        return false;
    }

    return true;
}
