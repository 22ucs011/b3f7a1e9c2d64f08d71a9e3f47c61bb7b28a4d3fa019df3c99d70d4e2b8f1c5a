#include "sql/ast.h"

#include <utility>

namespace kizuna::sql
{
    bool CreateTableStatement::has_primary_key() const
    {
        for (const auto &col : columns)
        {
            if (col.constraint.primary_key)
                return true;
        }
        return false;
    }

    LiteralValue LiteralValue::null()
    {
        LiteralValue lit;
        lit.kind = LiteralKind::NULL_LITERAL;
        lit.text = "NULL";
        lit.bool_value = false;
        return lit;
    }

    LiteralValue LiteralValue::boolean(bool value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::BOOLEAN;
        lit.bool_value = value;
        lit.text = value ? "TRUE" : "FALSE";
        return lit;
    }

    LiteralValue LiteralValue::integer(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::INTEGER;
        lit.text = std::move(value);
        return lit;
    }

    LiteralValue LiteralValue::floating(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::DOUBLE;
        lit.text = std::move(value);
        return lit;
    }

    LiteralValue LiteralValue::string(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::STRING;
        lit.text = std::move(value);
        return lit;
    }

    std::unique_ptr<Expression> Expression::make_literal(LiteralValue literal)
    {
        auto expr = std::make_unique<Expression>();
        expr->kind = ExpressionKind::LITERAL;
        expr->literal = std::move(literal);
        return expr;
    }

    std::unique_ptr<Expression> Expression::make_column(ColumnRef column)
    {
        auto expr = std::make_unique<Expression>();
        expr->kind = ExpressionKind::COLUMN_REF;
        expr->column = std::move(column);
        return expr;
    }

    std::unique_ptr<Expression> Expression::make_unary(UnaryOperator op, std::unique_ptr<Expression> operand)

    {
        auto expr = std::make_unique<Expression>();
        expr->kind = ExpressionKind::UNARY;
        expr->unary_op = op;
        expr->left = std::move(operand);
        return expr;
    }

    std::unique_ptr<Expression> Expression::make_binary(BinaryOperator op,
                                                       std::unique_ptr<Expression> left,
                                                       std::unique_ptr<Expression> right)
    {
        auto expr = std::make_unique<Expression>();
        expr->kind = ExpressionKind::BINARY;
        expr->binary_op = op;
        expr->left = std::move(left);
        expr->right = std::move(right);
        return expr;
    }

    std::unique_ptr<Expression> Expression::make_null_check(std::unique_ptr<Expression> operand, bool is_not)
    {
        auto expr = std::make_unique<Expression>();
        expr->kind = ExpressionKind::NULL_TEST;
        expr->is_not_null = is_not;
        expr->left = std::move(operand);
        return expr;
    }

    SelectItem SelectItem::star()
    {
        SelectItem item;
        item.kind = SelectItemKind::STAR;
        return item;
    }

    SelectItem SelectItem::column_item(ColumnRef column)
    {
        SelectItem item;
        item.kind = SelectItemKind::COLUMN;
        item.column = std::move(column);
        return item;
    }

    SelectItem SelectItem::aggregate_item(AggregateCall aggregate)
    {
        SelectItem item;
        item.kind = SelectItemKind::AGGREGATE;
        item.aggregate = std::move(aggregate);
        return item;
    }
}
