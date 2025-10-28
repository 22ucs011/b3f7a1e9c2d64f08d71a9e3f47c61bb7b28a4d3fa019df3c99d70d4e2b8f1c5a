#include <cassert>
#include <string>

#include "common/exception.h"
#include "sql/dml_parser.h"

using namespace kizuna;

static void check_select_with_where_limit()
{
    auto select = sql::parse_select("SELECT id, name FROM users WHERE age >= 18 AND NOT active LIMIT 5;");
    assert(select.table_name == "users");
    assert(select.from.table_name == "users");
    assert(!select.from.has_alias());
    assert(select.columns.size() == 2);
    assert(select.columns[0].kind == sql::SelectItemKind::COLUMN);
    assert(select.columns[0].column.column == "id");
    assert(select.columns[1].kind == sql::SelectItemKind::COLUMN);
    assert(select.columns[1].column.column == "name");
    assert(select.where != nullptr);
    assert(select.limit.has_value());
    assert(*select.limit == 5);
    assert(select.where->kind == sql::ExpressionKind::BINARY);
    assert(select.where->binary_op == sql::BinaryOperator::AND);
    assert(select.where->left != nullptr && select.where->right != nullptr);
}

static void check_select_star()
{
    auto select = sql::parse_select("SELECT * FROM logs;");
    assert(select.columns.size() == 1);
    assert(select.columns[0].kind == sql::SelectItemKind::STAR);
    assert(!select.where);
    assert(!select.limit.has_value());
}

static void check_select_predicate_or()
{
    auto select = sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL OR NOT active;");
    assert(select.columns.size() == 1);
    assert(select.columns[0].kind == sql::SelectItemKind::COLUMN);
    assert(select.columns[0].column.column == "id");
    assert(select.where != nullptr);
    assert(select.where->kind == sql::ExpressionKind::BINARY);
    assert(select.where->binary_op == sql::BinaryOperator::OR);
}

static void check_null_tests()
{
    auto select = sql::parse_select("SELECT id FROM employees WHERE nickname IS NOT NULL;");
    assert(select.where != nullptr);
    assert(select.where->kind == sql::ExpressionKind::NULL_TEST);
    assert(select.where->is_not_null);

    auto update = sql::parse_update("UPDATE employees SET nickname = NULL WHERE nickname IS NULL;");
    assert(update.where != nullptr);
    assert(update.where->kind == sql::ExpressionKind::NULL_TEST);
    assert(!update.where->is_not_null);
}

static void check_delete_where()
{
    auto del = sql::parse_delete("DELETE FROM users WHERE id = 10;");
    assert(del.table_name == "users");
    assert(del.where != nullptr);
    assert(del.where->kind == sql::ExpressionKind::BINARY);
    assert(del.where->binary_op == sql::BinaryOperator::EQUAL);
}

static void check_update_parse()
{
    auto update = sql::parse_update("UPDATE users SET name = 'bob', age = 30 WHERE id = 1;");
    assert(update.table_name == "users");
    assert(update.assignments.size() == 2);
    assert(update.assignments[0].column_name == "name");
    assert(update.assignments[0].value->kind == sql::ExpressionKind::LITERAL);
    assert(update.assignments[1].column_name == "age");
    assert(update.assignments[1].value->kind == sql::ExpressionKind::LITERAL);
    assert(update.where != nullptr);
    assert(update.where->kind == sql::ExpressionKind::BINARY);
}

static void check_select_order_by_single()
{
    auto select = sql::parse_select("SELECT id FROM users ORDER BY name DESC;");
    assert(select.order_by.size() == 1);
    assert(select.order_by[0].column.column == "name");
    assert(!select.order_by[0].ascending);
    assert(!select.limit.has_value());
}

static void check_select_multi_order_by()
{
    auto select = sql::parse_select("SELECT id FROM users ORDER BY name DESC, created ASC;");
    assert(select.order_by.size() == 2);
    assert(select.order_by[0].column.column == "name");
    assert(!select.order_by[0].ascending);
    assert(select.order_by[1].column.column == "created");
    assert(select.order_by[1].ascending);
}

static void check_select_distinct()
{
    auto select = sql::parse_select("SELECT DISTINCT name FROM users;");
    assert(select.distinct);
    assert(select.columns.size() == 1);
    assert(select.columns[0].kind == sql::SelectItemKind::COLUMN);
    assert(select.columns[0].column.column == "name");
}

static void check_select_aggregates()
{
    auto select = sql::parse_select("SELECT COUNT(*), SUM(DISTINCT balance), AVG(balance) FROM accounts;");
    assert(select.columns.size() == 3);
    assert(select.columns[0].kind == sql::SelectItemKind::AGGREGATE);
    assert(select.columns[0].aggregate.function == sql::AggregateFunction::COUNT);
    assert(select.columns[0].aggregate.is_star);
    assert(!select.columns[0].aggregate.is_distinct);
    assert(!select.columns[0].aggregate.column.has_value());

    assert(select.columns[1].kind == sql::SelectItemKind::AGGREGATE);
    assert(select.columns[1].aggregate.function == sql::AggregateFunction::SUM);
    assert(select.columns[1].aggregate.is_distinct);
    assert(select.columns[1].aggregate.column.has_value());
    assert(select.columns[1].aggregate.column->column == "balance");

    assert(select.columns[2].kind == sql::SelectItemKind::AGGREGATE);
    assert(select.columns[2].aggregate.function == sql::AggregateFunction::AVG);
    assert(!select.columns[2].aggregate.is_distinct);
    assert(select.columns[2].aggregate.column.has_value());
    assert(select.columns[2].aggregate.column->column == "balance");
}

static void check_join_parse()
{
    auto select = sql::parse_select("SELECT u.id FROM users AS u INNER JOIN accounts a ON u.id = a.user_id WHERE a.active;");
    assert(select.table_name == "users");
    assert(select.from.table_name == "users");
    assert(select.from.alias == "u");
    assert(select.joins.size() == 1);
    assert(select.joins[0].table.table_name == "accounts");
    assert(select.joins[0].table.alias == "a");
    assert(select.joins[0].condition != nullptr);
    assert(select.where != nullptr);
}

static void check_join_without_inner_keyword()
{
    auto select = sql::parse_select("SELECT a.id FROM accounts a JOIN ledger l ON a.id = l.account_id;");
    assert(select.from.alias == "a");
    assert(select.joins.size() == 1);
    assert(select.joins[0].table.alias == "l");
    assert(select.joins[0].condition != nullptr);
}

static void check_select_complex_where()
{
    auto select = sql::parse_select("SELECT id FROM users WHERE (active AND (age > 30 OR dept = 'r&d')) AND NOT archived;");
    assert(select.where != nullptr);
    assert(select.where->kind == sql::ExpressionKind::BINARY);
    assert(select.where->binary_op == sql::BinaryOperator::AND);
    assert(select.where->left->kind == sql::ExpressionKind::BINARY);
    assert(select.where->right->kind == sql::ExpressionKind::UNARY);
}

static void check_multi_join_chain()
{
    auto select = sql::parse_select(
        "SELECT u.id FROM users u JOIN accounts a ON u.id = a.user_id JOIN ledger l ON a.id = l.account_id WHERE l.balance > 0;");
    assert(select.joins.size() == 2);
    assert(select.joins[0].table.alias == "a");
    assert(select.joins[1].table.alias == "l");
    assert(select.joins[1].condition != nullptr);
    assert(select.where != nullptr);
}

static void check_nested_select_error()
{
    bool caught = false;
    try
    {
        (void)sql::parse_select("SELECT (SELECT id FROM inner_table) FROM outer_table;");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);
}

static void check_insert_variants()
{
    auto insert = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE);");
    assert(insert.table_name == "users");
    assert(insert.column_names.size() == 3);
    assert(insert.rows.size() == 2);
    assert(insert.rows[0].values[2].kind == sql::LiteralKind::BOOLEAN);

    auto insert2 = sql::parse_insert("INSERT INTO logs VALUES (-10, 3.14, NULL);");
    assert(insert2.rows.size() == 1);
    assert(insert2.rows[0].values[1].kind == sql::LiteralKind::DOUBLE);
    assert(insert2.rows[0].values[2].kind == sql::LiteralKind::NULL_LITERAL);
}

static void check_truncate()
{
    auto trunc = sql::parse_truncate("TRUNCATE TABLE users;");
    assert(trunc.table_name == "users");
}

static void check_parse_dml_switch()
{
    auto parsed = sql::parse_dml("UPDATE accounts SET balance = 100;");
    assert(parsed.kind == sql::DMLStatementKind::UPDATE);
    assert(parsed.update.assignments.size() == 1);
}

static void check_invalid_count_distinct_star()
{
    bool caught = false;
    try
    {
        (void)sql::parse_select("SELECT COUNT(DISTINCT * ) FROM accounts;");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);
}

bool sql_dml_parser_tests()
{
    check_insert_variants();
    check_select_with_where_limit();
    check_select_star();
    check_select_predicate_or();
    check_null_tests();
    check_select_order_by_single();
    check_select_multi_order_by();
    check_select_distinct();
    check_select_aggregates();
    check_join_parse();
    check_join_without_inner_keyword();
    check_select_complex_where();
    check_multi_join_chain();
    check_delete_where();
    check_update_parse();
    check_truncate();
    check_parse_dml_switch();
    check_invalid_count_distinct_star();
    check_nested_select_error();

    bool caught = false;
    try
    {
        (void)sql::parse_select("SELECT users;");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);

    return true;
}
