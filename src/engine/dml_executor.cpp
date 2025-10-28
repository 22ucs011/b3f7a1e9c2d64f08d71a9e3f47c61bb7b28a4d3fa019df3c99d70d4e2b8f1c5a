#include "engine/dml_executor.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <cstring>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <numeric>

#include "common/exception.h"
#include "common/logger.h"
#include "engine/expression_evaluator.h"
#include "storage/record.h"

namespace kizuna::engine
{

    namespace
    {
        constexpr std::string_view kClauseSelectList = "SELECT list";
        constexpr std::string_view kClauseAggregate = "SELECT aggregate";
        constexpr std::string_view kClauseWhere = "WHERE clause";
        constexpr std::string_view kClauseOrderBy = "ORDER BY clause";
        constexpr std::string_view kClauseFrom = "FROM clause";
        constexpr std::string_view kClauseJoin = "JOIN clause";
        constexpr std::string_view kClauseJoinCondition = "JOIN condition";
        constexpr std::string_view kClauseInsertTarget = "INSERT target";
        constexpr std::string_view kClauseInsertColumns = "INSERT column list";
        constexpr std::string_view kClauseUpdateTarget = "UPDATE target";
        constexpr std::string_view kClauseUpdateSet = "SET clause";
        constexpr std::string_view kClauseDeleteTarget = "DELETE target";
        constexpr std::string_view kClauseTruncateTarget = "TRUNCATE target";

        std::string join_strings(const std::vector<std::string> &items, std::string_view delimiter)
        {
            std::ostringstream oss;
            for (std::size_t i = 0; i < items.size(); ++i)
            {
                if (i != 0)
                    oss << delimiter;
                oss << items[i];
            }
            return oss.str();
        }

        std::string column_ref_to_string(const sql::ColumnRef &ref)
        {
            if (!ref.table.empty())
                return ref.table + "." + ref.column;
            return ref.column;
        }

        std::string literal_to_string(const sql::LiteralValue &literal)
        {
            switch (literal.kind)
            {
            case sql::LiteralKind::NULL_LITERAL:
                return "NULL";
            case sql::LiteralKind::BOOLEAN:
                return literal.bool_value ? "TRUE" : "FALSE";
            case sql::LiteralKind::STRING:
            case sql::LiteralKind::INTEGER:
            case sql::LiteralKind::DOUBLE:
                return literal.text;
            default:
                return "<literal>";
            }
        }

        std::string binary_operator_to_string(sql::BinaryOperator op)
        {
            switch (op)
            {
            case sql::BinaryOperator::EQUAL:
                return "=";
            case sql::BinaryOperator::NOT_EQUAL:
                return "!=";
            case sql::BinaryOperator::LESS:
                return "<";
            case sql::BinaryOperator::LESS_EQUAL:
                return "<=";
            case sql::BinaryOperator::GREATER:
                return ">";
            case sql::BinaryOperator::GREATER_EQUAL:
                return ">=";
            case sql::BinaryOperator::AND:
                return "AND";
            case sql::BinaryOperator::OR:
                return "OR";
            }
            return "?";
        }

        std::string describe_expression(const sql::Expression *expr)
        {
            if (expr == nullptr)
                return "<null>";

            switch (expr->kind)
            {
            case sql::ExpressionKind::LITERAL:
                return literal_to_string(expr->literal);
            case sql::ExpressionKind::COLUMN_REF:
                return column_ref_to_string(expr->column);
            case sql::ExpressionKind::UNARY:
                return "NOT (" + describe_expression(expr->left.get()) + ")";
            case sql::ExpressionKind::BINARY:
                return "(" + describe_expression(expr->left.get()) + " " +
                       binary_operator_to_string(expr->binary_op) + " " +
                       describe_expression(expr->right.get()) + ")";
            case sql::ExpressionKind::NULL_TEST:
                return describe_expression(expr->left.get()) +
                       (expr->is_not_null ? " IS NOT NULL" : " IS NULL");
            }
            return "<expr>";
        }

        std::string describe_assignments(const std::vector<sql::UpdateAssignment> &assignments)
        {
            std::vector<std::string> parts;
            parts.reserve(assignments.size());
            for (const auto &assign : assignments)
            {
                parts.push_back(assign.column_name + "=" + describe_expression(assign.value.get()));
            }
            return join_strings(parts, ", ");
        }

        std::string aggregate_function_to_string(sql::AggregateFunction fn)
        {
            switch (fn)
            {
            case sql::AggregateFunction::COUNT:
                return "COUNT";
            case sql::AggregateFunction::SUM:
                return "SUM";
            case sql::AggregateFunction::AVG:
                return "AVG";
            case sql::AggregateFunction::MIN:
                return "MIN";
            case sql::AggregateFunction::MAX:
                return "MAX";
            }
            return "AGG";
        }

        std::string describe_aggregate(const sql::AggregateCall &call)
        {
            std::string text = aggregate_function_to_string(call.function);
            text.push_back('(');
            if (call.is_distinct)
            {
                text.append("DISTINCT ");
            }
            if (call.is_star)
            {
                text.push_back('*');
            }
            else if (call.column.has_value())
            {
                text.append(column_ref_to_string(*call.column));
            }
            text.push_back(')');
            return text;
        }

        bool is_true(TriBool value) noexcept
        {
            return value == TriBool::True;
        }
    }

    struct DMLExecutor::ColumnPredicate
    {
        std::optional<Value> equality;
        std::optional<Value> lower;
        bool lower_inclusive{false};
        std::optional<Value> upper;
        bool upper_inclusive{false};
        bool contradiction{false};

        bool apply_equality(const Value &value);
        bool apply_lower(const Value &value, bool inclusive);
        bool apply_upper(const Value &value, bool inclusive);
        bool bounds_compatible() const;
    };

    struct DMLExecutor::PredicateExtraction
    {
        std::unordered_map<column_id_t, ColumnPredicate> predicates;
        bool contradiction{false};
    };

    struct DMLExecutor::IndexScanSpec
    {
        enum class Kind
        {
            Equality,
            Range
        };

        std::size_t context_index{0};
        Kind kind{Kind::Equality};
        std::vector<Value> equality_values;
        std::optional<Value> lower_value;
        bool lower_inclusive{true};
        std::optional<Value> upper_value;
        bool upper_inclusive{true};
    };

    DMLExecutor::DMLExecutor(catalog::CatalogManager &catalog,
                             PageManager &pm,
                             FileManager &fm,
                             index::IndexManager &index_manager)
        : catalog_(catalog), pm_(pm), fm_(fm), index_manager_(index_manager)
    {
    }

    std::string DMLExecutor::execute(std::string_view sql)
    {
        auto parsed = sql::parse_dml(sql);
        switch (parsed.kind)
        {
        case sql::DMLStatementKind::INSERT:
        {
            auto result = insert_into(parsed.insert);
            return "Rows inserted: " + std::to_string(result.rows_inserted);
        }
        case sql::DMLStatementKind::SELECT:
        {
            auto result = select(parsed.select);
            return "Rows returned: " + std::to_string(result.rows.size());
        }
        case sql::DMLStatementKind::DELETE:
        {
            auto result = delete_all(parsed.del);
            return "Rows deleted: " + std::to_string(result.rows_deleted);
        }
        case sql::DMLStatementKind::UPDATE:
        {
            auto result = update_all(parsed.update);
            return "Rows updated: " + std::to_string(result.rows_updated);
        }
        case sql::DMLStatementKind::TRUNCATE:
        {
            truncate(parsed.truncate);
            return "Table truncated";
        }
        }
        throw DBException(StatusCode::NOT_IMPLEMENTED, "Unsupported DML statement", std::string(sql));
    }

    void DMLExecutor::set_index_usage_observer(std::function<void(const catalog::IndexCatalogEntry &,
                                                                  const std::vector<record_id_t> &)> observer)
    {
        index_usage_observer_ = std::move(observer);
    }

    InsertResult DMLExecutor::insert_into(const sql::InsertStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name, kClauseInsertTarget);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        if (columns.empty())
            throw QueryException::invalid_constraint("table has no columns");

        std::vector<std::string> column_names = stmt.column_names;
        if (column_names.empty())
        {
            column_names.reserve(columns.size());
            for (const auto &c : columns)
                column_names.push_back(c.column.name);
        }
        if (column_names.size() != columns.size())
            throw QueryException::invalid_constraint("column count mismatch");

        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto column_lookup = build_column_lookup(columns);

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t inserted = 0;
        for (const auto &row : stmt.rows)
        {
            if (row.values.size() != column_names.size())
                throw QueryException::invalid_constraint("row value count mismatch");
            auto payload = encode_row(columns, row, column_names, table_entry.name);
            auto row_values = decode_row_values(columns, payload);
            auto location = heap.insert(payload);
            record_id_t record_id = make_record_id(location);

            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto key = build_index_key(index_contexts[i], columns, row_values, column_lookup);
                auto &tree = index_handles[i]->tree();
                tree.Insert(key, record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }

            ++inserted;
        }
        return InsertResult{inserted};
    }

    SelectResult DMLExecutor::select(const sql::SelectStatement &stmt)
    {
        SelectResult result;

        const sql::TableRef base_ref = !stmt.from.table_name.empty() ? stmt.from : sql::TableRef{stmt.table_name, {}};
        auto bind_table = [&](const sql::TableRef &ref, std::string_view clause) -> BoundTable
        {
            auto table_opt = catalog_.get_table(ref.table_name);
            if (!table_opt)
                throw QueryException::table_not_found(ref.table_name, clause);
            BoundTable bound;
            bound.table = *table_opt;
            bound.columns = catalog_.get_columns(bound.table.table_id);
            bound.alias = ref.alias;
            if (bound.columns.empty())
                throw QueryException::invalid_constraint("Table has no columns");
            return bound;
        };

        std::vector<BoundTable> tables;
        tables.push_back(bind_table(base_ref, kClauseFrom));
        for (const auto &join : stmt.joins)
        {
            tables.push_back(bind_table(join.table, kClauseJoin));
        }

        std::vector<BoundColumn> bound_columns;
        std::size_t total_columns = 0;
        for (const auto &tbl : tables)
            total_columns += tbl.columns.size();
        bound_columns.reserve(total_columns);
        for (const auto &tbl : tables)
        {
            for (const auto &col : tbl.columns)
            {
                bound_columns.push_back(BoundColumn{col, tbl.table.name, tbl.alias});
            }
        }

        const std::size_t limit = stmt.limit.has_value() && *stmt.limit >= 0
                                      ? static_cast<std::size_t>(*stmt.limit)
                                      : std::numeric_limits<std::size_t>::max();

        bool has_aggregates = false;
        bool has_scalar_items = false;
        for (const auto &item : stmt.columns)
        {
            if (item.kind == sql::SelectItemKind::AGGREGATE)
                has_aggregates = true;
            else
                has_scalar_items = true;
        }
        if (has_aggregates && has_scalar_items)
        {
            throw QueryException::invalid_constraint("Cannot mix aggregate and scalar select items without GROUP BY");
        }

        std::vector<ExpressionEvaluator::BindingEntry> binding_entries;
        binding_entries.reserve(bound_columns.size());
        for (std::size_t i = 0; i < bound_columns.size(); ++i)
        {
            ExpressionEvaluator::BindingEntry entry;
            entry.column_name = bound_columns[i].column.column.name;
            entry.index = i;
            entry.type = bound_columns[i].column.column.type;
            entry.qualifiers.push_back(bound_columns[i].table_name);
            if (!bound_columns[i].table_alias.empty())
                entry.qualifiers.push_back(bound_columns[i].table_alias);
            binding_entries.push_back(std::move(entry));
        }
        ExpressionEvaluator full_evaluator(binding_entries);

        struct OrderTerm
        {
            std::size_t value_index{0};
            bool ascending{true};
            column_id_t column_id{0};
        };

        std::vector<OrderTerm> order_terms;
        order_terms.reserve(stmt.order_by.size());
        bool all_order_descending = true;
        bool mixed_order_direction = false;
        bool first_term = true;
        bool last_direction = true;
        for (const auto &term : stmt.order_by)
        {
            auto resolved = full_evaluator.resolve_column(term.column, kClauseOrderBy);
            const auto &bound = bound_columns[resolved.index];
            order_terms.push_back(OrderTerm{resolved.index, term.ascending, bound.column.column_id});
            if (term.ascending)
                all_order_descending = false;
            if (first_term)
            {
                last_direction = term.ascending;
                first_term = false;
            }
            else if (term.ascending != last_direction)
            {
                mixed_order_direction = true;
            }
            last_direction = term.ascending;
        }
        const bool has_order = !order_terms.empty();

        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;
        std::vector<std::vector<Value>> filtered_rows;
        bool rows_already_sorted = false;

        if (tables.size() == 1)
        {
            const auto &tbl = tables.front();
            const auto &columns = tbl.columns;
            auto index_contexts = load_table_indexes(tbl.table.table_id);
            auto column_lookup = build_column_lookup(columns);

            std::optional<PredicateExtraction> predicate_info;
            if (predicate)
                predicate_info = extract_column_predicates(predicate, columns, tbl.table.name);
            if (predicate_info && predicate_info->contradiction)
            {
                filtered_rows.clear();
            }
            else
            {
                std::optional<std::size_t> order_index_context;
                if (has_order && !mixed_order_direction)
                {
                    for (std::size_t i = 0; i < index_contexts.size(); ++i)
                    {
                        const auto &ctx = index_contexts[i];
                        if (ctx.catalog_entry.column_ids.size() < order_terms.size())
                            continue;
                        bool matches = true;
                        for (std::size_t j = 0; j < order_terms.size(); ++j)
                        {
                            if (ctx.catalog_entry.column_ids[j] != order_terms[j].column_id)
                            {
                                matches = false;
                                break;
                            }
                        }
                        if (matches)
                        {
                            order_index_context = i;
                            break;
                        }
                    }
                }

                std::vector<record_id_t> candidate_ids;
                bool candidate_ids_populated = false;
                bool candidate_ids_in_final_order = false;

                if (predicate && predicate_info && !index_contexts.empty())
                {
                    auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
                    if (spec_opt.has_value())
                    {
                        auto handle = index_manager_.OpenIndex(index_contexts[spec_opt->context_index].catalog_entry);
                        candidate_ids = run_index_scan(*spec_opt, index_contexts, *handle, columns, column_lookup);
                        candidate_ids_populated = true;
                        if (has_order && !mixed_order_direction && order_index_context.has_value() &&
                            spec_opt->context_index == *order_index_context)
                        {
                            candidate_ids_in_final_order = true;
                            if (all_order_descending)
                                std::reverse(candidate_ids.begin(), candidate_ids.end());
                        }
                    }
                }

                if (!candidate_ids_populated && has_order && order_index_context.has_value())
                {
                    const auto &ctx = index_contexts[*order_index_context];
                    auto handle = index_manager_.OpenIndex(ctx.catalog_entry);
                    std::optional<std::vector<uint8_t>> lower_key;
                    std::optional<std::vector<uint8_t>> upper_key;
                    bool lower_inclusive = true;
                    bool upper_inclusive = true;

                    if (predicate_info)
                    {
                        if (!ctx.catalog_entry.column_ids.empty())
                        {
                            column_id_t first_column = ctx.catalog_entry.column_ids.front();
                            auto pred_it = predicate_info->predicates.find(first_column);
                            if (pred_it != predicate_info->predicates.end())
                            {
                                const auto &col_pred = pred_it->second;
                                auto lookup_it = column_lookup.find(first_column);
                                if (lookup_it != column_lookup.end())
                                {
                                    std::vector<catalog::ColumnCatalogEntry> key_columns{columns[lookup_it->second]};
                                    if (col_pred.equality.has_value())
                                    {
                                        std::vector<Value> key_values{col_pred.equality.value()};
                                        auto key = encode_values(key_columns, key_values);
                                        lower_key = key;
                                        upper_key = key;
                                        lower_inclusive = true;
                                        upper_inclusive = true;
                                    }
                                    else
                                    {
                                        if (col_pred.lower.has_value())
                                        {
                                            std::vector<Value> key_values{col_pred.lower.value()};
                                            lower_key = encode_values(key_columns, key_values);
                                            lower_inclusive = col_pred.lower_inclusive;
                                        }
                                        if (col_pred.upper.has_value())
                                        {
                                            std::vector<Value> key_values{col_pred.upper.value()};
                                            upper_key = encode_values(key_columns, key_values);
                                            upper_inclusive = col_pred.upper_inclusive;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    candidate_ids = handle->tree().ScanRange(lower_key, lower_inclusive, upper_key, upper_inclusive);
                    candidate_ids_populated = true;
                    candidate_ids_in_final_order = true;
                    if (all_order_descending)
                        std::reverse(candidate_ids.begin(), candidate_ids.end());
                }

                TableHeap heap(pm_, tbl.table.root_page_id);
                auto process_row = [&](std::vector<Value> values)
                {
                    if (predicate && !is_true(full_evaluator.evaluate_predicate(*predicate, values, kClauseWhere)))
                        return;
                    filtered_rows.push_back(std::move(values));
                };

                if (candidate_ids_populated)
                {
                    for (record_id_t rid : candidate_ids)
                    {
                        std::vector<uint8_t> payload;
                        auto location = decode_record_id(rid);
                        if (!heap.read(location, payload))
                            continue;
                        auto values = decode_row_values(columns, payload);
                        process_row(std::move(values));
                    }
                }
                else
                {
                    heap.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload)
                              {
                        auto values = decode_row_values(columns, payload);
                        process_row(std::move(values)); });
                }

                rows_already_sorted = candidate_ids_in_final_order;
            }
        }
        else
        {
            auto build_prefix_evaluator = [&](std::size_t table_count)
            {
                std::vector<ExpressionEvaluator::BindingEntry> prefix;
                std::size_t prefix_columns = 0;
                for (std::size_t t = 0; t < table_count; ++t)
                    prefix_columns += tables[t].columns.size();
                prefix.reserve(prefix_columns);
                std::size_t index = 0;
                for (std::size_t t = 0; t < table_count; ++t)
                {
                    const auto &tbl = tables[t];
                    for (const auto &col : tbl.columns)
                    {
                        ExpressionEvaluator::BindingEntry entry;
                        entry.column_name = col.column.name;
                        entry.index = index++;
                        entry.type = col.column.type;
                        entry.qualifiers.push_back(tbl.table.name);
                        if (!tbl.alias.empty())
                            entry.qualifiers.push_back(tbl.alias);
                        prefix.push_back(std::move(entry));
                    }
                }
                return ExpressionEvaluator(prefix);
            };

            std::vector<std::vector<std::vector<Value>>> table_rows;
            table_rows.reserve(tables.size());
            for (const auto &tbl : tables)
            {
                std::vector<std::vector<Value>> rows;
                TableHeap heap(pm_, tbl.table.root_page_id);
                heap.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload)
                          { rows.push_back(decode_row_values(tbl.columns, payload)); });
                table_rows.push_back(std::move(rows));
            }

            std::vector<std::vector<Value>> combined_rows;
            if (!table_rows.empty())
            {
                combined_rows = std::move(table_rows.front());
            }

            for (std::size_t join_idx = 0; join_idx < stmt.joins.size(); ++join_idx)
            {
                std::vector<std::vector<Value>> next_rows;
                next_rows.reserve(combined_rows.size() * table_rows[join_idx + 1].size());
                auto join_evaluator = build_prefix_evaluator(join_idx + 2);
                const auto *condition = stmt.joins[join_idx].condition.get();
                for (const auto &left : combined_rows)
                {
                    for (const auto &right : table_rows[join_idx + 1])
                    {
                        std::vector<Value> merged;
                        merged.reserve(left.size() + right.size());
                        merged.insert(merged.end(), left.begin(), left.end());
                        merged.insert(merged.end(), right.begin(), right.end());
                        if (!condition || is_true(join_evaluator.evaluate_predicate(*condition, merged, kClauseJoinCondition)))
                        {
                            next_rows.push_back(std::move(merged));
                        }
                    }
                }
                combined_rows = std::move(next_rows);
                if (combined_rows.empty())
                    break;
            }

            filtered_rows.reserve(combined_rows.size());
            if (predicate)
            {
                for (auto &row : combined_rows)
                {
                    if (is_true(full_evaluator.evaluate_predicate(*predicate, row, kClauseWhere)))
                        filtered_rows.push_back(std::move(row));
                }
            }
            else
            {
                filtered_rows = std::move(combined_rows);
            }
        }

        if (has_aggregates)
        {
            std::vector<std::string> column_names;
            std::vector<Value> aggregate_values;
            column_names.reserve(stmt.columns.size());
            aggregate_values.reserve(stmt.columns.size());
            for (const auto &item : stmt.columns)
            {
                if (item.kind != sql::SelectItemKind::AGGREGATE)
                    continue;
                column_names.push_back(describe_aggregate(item.aggregate));
                aggregate_values.push_back(evaluate_aggregate(item.aggregate, full_evaluator, filtered_rows));
            }
            result.column_names = std::move(column_names);
            if (limit == 0)
                return result;
            std::vector<std::string> out_row;
            out_row.reserve(aggregate_values.size());
            for (const auto &value : aggregate_values)
                out_row.push_back(value.to_string());
            if (!out_row.empty())
                result.rows.push_back(std::move(out_row));
            return result;
        }

        std::vector<std::string> projection_names;
        auto projection = build_projection(stmt, bound_columns, full_evaluator, tables.size() > 1, projection_names);
        if (projection.empty())
        {
            projection.resize(bound_columns.size());
            projection_names.clear();
            projection_names.reserve(bound_columns.size());
            for (std::size_t i = 0; i < bound_columns.size(); ++i)
            {
                projection[i] = i;
                const auto &col = bound_columns[i];
                if (tables.size() > 1)
                {
                    const std::string qualifier = col.table_alias.empty() ? col.table_name : col.table_alias;
                    projection_names.push_back(qualifier + "." + col.column.column.name);
                }
                else
                {
                    projection_names.push_back(col.column.column.name);
                }
            }
        }
        result.column_names = projection_names;
        if (limit == 0)
            return result;

        std::vector<std::size_t> row_indices(filtered_rows.size());
        std::iota(row_indices.begin(), row_indices.end(), 0);

        if (!order_terms.empty() && !rows_already_sorted)
        {
            auto comparator = [&](std::size_t lhs_idx, std::size_t rhs_idx)
            {
                const auto &lhs = filtered_rows[lhs_idx];
                const auto &rhs = filtered_rows[rhs_idx];
                for (const auto &term : order_terms)
                {
                    const Value &lv = lhs[term.value_index];
                    const Value &rv = rhs[term.value_index];
                    const bool lhs_null = lv.is_null();
                    const bool rhs_null = rv.is_null();
                    if (lhs_null != rhs_null)
                        return !lhs_null;
                    auto cmp = compare(lv, rv);
                    if (cmp == CompareResult::Less)
                        return term.ascending;
                    if (cmp == CompareResult::Greater)
                        return !term.ascending;
                }
                return false;
            };
            std::stable_sort(row_indices.begin(), row_indices.end(), comparator);
        }

        if (stmt.distinct)
        {
            std::unordered_set<std::string> seen;
            seen.reserve(row_indices.size());
            std::vector<std::size_t> deduped;
            deduped.reserve(row_indices.size());
            for (auto idx : row_indices)
            {
                const auto &row = filtered_rows[idx];
                std::string key = row_signature(row, projection);
                if (seen.insert(key).second)
                    deduped.push_back(idx);
            }
            row_indices = std::move(deduped);
        }

        if (row_indices.size() > limit)
            row_indices.resize(limit);

        for (auto idx : row_indices)
        {
            const auto &row = filtered_rows[idx];
            std::vector<std::string> out_row;
            out_row.reserve(projection.size());
            for (auto proj_idx : projection)
                out_row.push_back(row[proj_idx].to_string());
            result.rows.push_back(std::move(out_row));
        }

        return result;
    }
    DeleteResult DMLExecutor::delete_all(const sql::DeleteStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name, kClauseDeleteTarget);
        const auto table_entry = *table_opt;
        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto columns = catalog_.get_columns(table_entry.table_id);
        auto column_lookup = build_column_lookup(columns);

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[DELETE] table=", table_entry.name, " predicate=", predicate_desc);

        std::optional<PredicateExtraction> predicate_info;
        if (predicate)
            predicate_info = extract_column_predicates(predicate, columns, table_entry.name);
        if (predicate_info && predicate_info->contradiction)
        {
            return DeleteResult{0};
        }

        std::optional<IndexScanSpec> index_spec;
        std::vector<record_id_t> candidate_ids;
        if (predicate && predicate_info && !index_contexts.empty())
        {
            auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
            if (spec_opt.has_value())
            {
                index_spec = std::move(*spec_opt);
                candidate_ids = run_index_scan(*index_spec, index_contexts, *index_handles[index_spec->context_index], columns, column_lookup);
            }
        }

        std::size_t deleted = 0;
        auto remove_row = [&](const TableHeap::RowLocation &loc, const std::vector<Value> &values)
        {
            if (!heap.erase(loc))
                return;
            record_id_t record_id = make_record_id(loc);
            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto key = build_index_key(index_contexts[i], columns, values, column_lookup);
                auto &tree = index_handles[i]->tree();
                tree.Remove(key, record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }
            ++deleted;
        };

        if (index_spec.has_value())
        {
            for (record_id_t rid : candidate_ids)
            {
                auto loc = decode_record_id(rid);
                std::vector<uint8_t> payload;
                if (!heap.read(loc, payload))
                    continue;
                auto values = decode_row_values(columns, payload);
                if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values, kClauseWhere)))
                    continue;
                remove_row(loc, values);
            }
        }
        else
        {
            heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload)
                      {
                auto values = decode_row_values(columns, payload);
                if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values, kClauseWhere)))
                    return;
                remove_row(loc, values); });
        }

        return DeleteResult{deleted};
    }

    UpdateResult DMLExecutor::update_all(const sql::UpdateStatement &stmt)
    {
        if (stmt.assignments.empty())
            throw QueryException::invalid_constraint("UPDATE requires at least one assignment");

        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name, kClauseUpdateTarget);
        const auto table_entry = *table_opt;
        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto columns = catalog_.get_columns(table_entry.table_id);
        auto column_lookup = build_column_lookup(columns);

        std::unordered_map<std::string, std::size_t> column_index;
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            column_index.emplace(columns[i].column.name, i);
        }

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string assignments_desc = describe_assignments(stmt.assignments);
        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[UPDATE] table=", table_entry.name, " assignments=", (assignments_desc.empty() ? std::string("<none>") : assignments_desc), " predicate=", predicate_desc);

        struct UpdateTarget
        {
            TableHeap::RowLocation location;
            std::vector<Value> current_values;
        };

        std::vector<UpdateTarget> targets;
        std::optional<PredicateExtraction> predicate_info;
        if (predicate)
            predicate_info = extract_column_predicates(predicate, columns, table_entry.name);
        if (predicate_info && predicate_info->contradiction)
        {
            return UpdateResult{0};
        }

        std::optional<IndexScanSpec> index_spec;
        std::vector<record_id_t> candidate_ids;
        if (predicate && predicate_info && !index_contexts.empty())
        {
            auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
            if (spec_opt.has_value())
            {
                index_spec = std::move(*spec_opt);
                candidate_ids = run_index_scan(*index_spec, index_contexts, *index_handles[index_spec->context_index], columns, column_lookup);
            }
        }

        auto collect_target = [&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload)
        {
            auto current_values = decode_row_values(columns, payload);
            if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, current_values, kClauseWhere)))
                return;
            targets.push_back(UpdateTarget{loc, std::move(current_values)});
        };

        if (index_spec.has_value())
        {
            for (record_id_t rid : candidate_ids)
            {
                auto loc = decode_record_id(rid);
                std::vector<uint8_t> payload;
                if (!heap.read(loc, payload))
                    continue;
                collect_target(loc, payload);
            }
        }
        else
        {
            heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload)
                      { collect_target(loc, payload); });
        }

        std::size_t updated = 0;
        for (auto &target : targets)
        {
            auto &current_values = target.current_values;
            std::vector<Value> new_values = current_values;
            for (const auto &assignment : stmt.assignments)
            {
                auto it = column_index.find(assignment.column_name);
                if (it == column_index.end())
                    throw QueryException::column_not_found(assignment.column_name, stmt.table_name, kClauseUpdateSet);

                std::size_t idx = it->second;
                Value evaluated = evaluator.evaluate_scalar(*assignment.value, current_values, kClauseUpdateSet);
                Value coerced = coerce_value_for_column(columns[idx], evaluated);
                new_values[idx] = coerced;
            }

            auto new_payload = encode_values(columns, new_values);
            record_id_t old_record_id = make_record_id(target.location);
            auto new_location = heap.update(target.location, new_payload);
            record_id_t new_record_id = make_record_id(new_location);

            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto old_key = build_index_key(index_contexts[i], columns, current_values, column_lookup);
                auto new_key = build_index_key(index_contexts[i], columns, new_values, column_lookup);
                if (old_record_id == new_record_id && old_key == new_key)
                    continue;
                auto &tree = index_handles[i]->tree();
                tree.Remove(old_key, old_record_id);
                tree.Insert(new_key, new_record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }

            target.location = new_location;
            current_values = std::move(new_values);
            ++updated;
        }

        return UpdateResult{updated};
    }

    void DMLExecutor::truncate(const sql::TruncateStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name, kClauseTruncateTarget);
        const auto table_entry = *table_opt;

        TableHeap heap(pm_, table_entry.root_page_id);
        heap.truncate();
    }

    std::vector<Value> DMLExecutor::decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::vector<uint8_t> &payload) const
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
        {
            throw DBException(StatusCode::INVALID_RECORD_FORMAT, "Failed to decode row", "table row");
        }
        if (fields.size() != columns.size())
        {
            throw DBException(StatusCode::INVALID_ARGUMENT, "Decoded field count mismatch", "table row");
        }

        std::vector<Value> values;
        values.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &col = columns[i].column;
            const auto &field = fields[i];
            if (field.is_null)
            {
                values.push_back(Value::null(col.type));
                continue;
            }

            switch (col.type)
            {
            case DataType::BOOLEAN:
            {
                bool v = !field.payload.empty() && field.payload[0] != 0;
                values.push_back(Value::boolean(v));
                break;
            }
            case DataType::INTEGER:
            {
                int32_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int32_t));
                values.push_back(Value::int32(v));
                break;
            }
            case DataType::BIGINT:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::DATE:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::date(v));
                break;
            }
            case DataType::TIMESTAMP:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::FLOAT:
            {
                float v = 0.0f;
                std::memcpy(&v, field.payload.data(), sizeof(float));
                values.push_back(Value::floating(static_cast<double>(v)));
                break;
            }
            case DataType::DOUBLE:
            {
                double v = 0.0;
                std::memcpy(&v, field.payload.data(), sizeof(double));
                values.push_back(Value::floating(v));
                break;
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                std::string text(reinterpret_cast<const char *>(field.payload.data()), field.payload.size());
                values.push_back(Value::string(std::move(text), col.type));
                break;
            }
            default:
                values.push_back(Value::string("<unsupported>"));
                break;
            }
        }
        return values;
    }

    std::vector<DMLExecutor::TableIndexContext> DMLExecutor::load_table_indexes(table_id_t table_id) const
    {
        std::vector<TableIndexContext> contexts;
        auto indexes = catalog_.get_indexes(table_id);
        contexts.reserve(indexes.size());
        for (auto &entry : indexes)
        {
            contexts.push_back(TableIndexContext{entry});
        }
        return contexts;
    }

    std::unordered_map<column_id_t, std::size_t> DMLExecutor::build_column_lookup(const std::vector<catalog::ColumnCatalogEntry> &columns) const
    {
        std::unordered_map<column_id_t, std::size_t> lookup;
        lookup.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            lookup.emplace(columns[i].column_id, i);
        }
        return lookup;
    }

    std::vector<uint8_t> DMLExecutor::build_index_key(const TableIndexContext &ctx,
                                                      const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::vector<Value> &row_values,
                                                      const std::unordered_map<column_id_t, std::size_t> &lookup) const
    {
        std::vector<catalog::ColumnCatalogEntry> key_columns;
        std::vector<Value> key_values;
        key_columns.reserve(ctx.catalog_entry.column_ids.size());
        key_values.reserve(ctx.catalog_entry.column_ids.size());
        for (auto column_id : ctx.catalog_entry.column_ids)
        {
            auto it = lookup.find(column_id);
            if (it == lookup.end())
            {
                KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Index column metadata missing", std::to_string(column_id));
            }
            key_columns.push_back(columns[it->second]);
            key_values.push_back(row_values[it->second]);
        }
        return encode_values(key_columns, key_values);
    }

    bool DMLExecutor::ColumnPredicate::bounds_compatible() const
    {
        if (contradiction)
            return false;
        if (lower && upper)
        {
            auto cmp = compare(*lower, *upper);
            if (cmp == CompareResult::Greater)
                return false;
            if (cmp == CompareResult::Equal && (!lower_inclusive || !upper_inclusive))
                return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_lower(const Value &value, bool inclusive)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (!lower.has_value())
        {
            lower = value;
            lower_inclusive = inclusive;
        }
        else
        {
            auto cmp = compare(value, *lower);
            if (cmp == CompareResult::Greater)
            {
                lower = value;
                lower_inclusive = inclusive;
            }
            else if (cmp == CompareResult::Equal)
            {
                lower_inclusive = lower_inclusive && inclusive;
            }
            else if (cmp == CompareResult::Unknown)
            {
                contradiction = true;
                return false;
            }
        }
        if (!bounds_compatible())
        {
            contradiction = true;
            return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_upper(const Value &value, bool inclusive)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (!upper.has_value())
        {
            upper = value;
            upper_inclusive = inclusive;
        }
        else
        {
            auto cmp = compare(value, *upper);
            if (cmp == CompareResult::Less)
            {
                upper = value;
                upper_inclusive = inclusive;
            }
            else if (cmp == CompareResult::Equal)
            {
                upper_inclusive = upper_inclusive && inclusive;
            }
            else if (cmp == CompareResult::Unknown)
            {
                contradiction = true;
                return false;
            }
        }
        if (!bounds_compatible())
        {
            contradiction = true;
            return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_equality(const Value &value)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (equality.has_value())
        {
            if (compare(*equality, value) != CompareResult::Equal)
            {
                contradiction = true;
                return false;
            }
        }
        equality = value;
        if (!apply_lower(value, true))
            return false;
        if (!apply_upper(value, true))
            return false;
        return true;
    }

    Value DMLExecutor::literal_to_value_for_column(const catalog::ColumnCatalogEntry &column,
                                                   const sql::LiteralValue &literal) const
    {
        const auto &col = column.column;
        switch (literal.kind)
        {
        case sql::LiteralKind::NULL_LITERAL:
            return Value::null(col.type);
        case sql::LiteralKind::BOOLEAN:
            if (col.type == DataType::BOOLEAN)
                return Value::boolean(literal.bool_value);
            if (col.type == DataType::INTEGER)
                return Value::int32(literal.bool_value ? 1 : 0);
            if (col.type == DataType::BIGINT)
                return Value::int64(literal.bool_value ? 1 : 0);
            break;
        case sql::LiteralKind::INTEGER:
        {
            long long parsed = 0;
            try
            {
                parsed = std::stoll(literal.text);
            }
            catch (const std::exception &)
            {
                throw QueryException::type_error("literal", "INTEGER", literal.text);
            }
            switch (col.type)
            {
            case DataType::BOOLEAN:
                return Value::boolean(parsed != 0);
            case DataType::INTEGER:
                if (parsed < std::numeric_limits<int32_t>::min() || parsed > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("literal", "INTEGER", literal.text);
                return Value::int32(static_cast<int32_t>(parsed));
            case DataType::BIGINT:
                return Value::int64(static_cast<int64_t>(parsed));
            case DataType::DOUBLE:
            case DataType::FLOAT:
                return Value::floating(static_cast<double>(parsed));
            default:
                break;
            }
            break;
        }
        case sql::LiteralKind::DOUBLE:
        {
            double parsed = 0.0;
            try
            {
                parsed = std::stod(literal.text);
            }
            catch (const std::exception &)
            {
                throw QueryException::type_error("literal", "DOUBLE", literal.text);
            }
            switch (col.type)
            {
            case DataType::DOUBLE:
            case DataType::FLOAT:
                return Value::floating(parsed);
            case DataType::INTEGER:
                if (parsed < std::numeric_limits<int32_t>::min() || parsed > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("literal", "INTEGER", literal.text);
                return Value::int32(static_cast<int32_t>(parsed));
            case DataType::BIGINT:
                return Value::int64(static_cast<int64_t>(parsed));
            default:
                break;
            }
            break;
        }
        case sql::LiteralKind::STRING:
        {
            switch (col.type)
            {
            case DataType::DATE:
            {
                auto parsed = parse_date(literal.text);
                if (!parsed)
                    throw QueryException::type_error("literal", "DATE", literal.text);
                return Value::date(*parsed);
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                if (col.type == DataType::VARCHAR && col.length > 0 && literal.text.size() > col.length)
                    throw QueryException::invalid_constraint("value too long for column '" + col.name + "'");
                return Value::string(literal.text, col.type);
            }
            default:
                break;
            }
            break;
        }
        }

        throw QueryException::type_error("literal comparison", data_type_to_string(col.type), literal.text);
    }

    std::optional<DMLExecutor::PredicateExtraction> DMLExecutor::extract_column_predicates(
        const sql::Expression *predicate,
        const std::vector<catalog::ColumnCatalogEntry> &columns,
        const std::string &table_name) const
    {
        PredicateExtraction extraction;
        if (!predicate)
            return extraction;

        std::function<bool(const sql::Expression *)> visit = [&](const sql::Expression *expr) -> bool
        {
            if (expr == nullptr)
                return true;

            if (expr->kind == sql::ExpressionKind::BINARY && expr->binary_op == sql::BinaryOperator::AND)
            {
                return visit(expr->left.get()) && visit(expr->right.get());
            }

            if (expr->kind == sql::ExpressionKind::BINARY)
            {
                sql::BinaryOperator op = expr->binary_op;
                switch (op)
                {
                case sql::BinaryOperator::EQUAL:
                case sql::BinaryOperator::LESS:
                case sql::BinaryOperator::LESS_EQUAL:
                case sql::BinaryOperator::GREATER:
                case sql::BinaryOperator::GREATER_EQUAL:
                    break;
                default:
                    return false;
                }

                const sql::Expression *column_expr = nullptr;
                const sql::Expression *literal_expr = nullptr;
                bool column_on_left = true;

                if (expr->left && expr->left->kind == sql::ExpressionKind::COLUMN_REF &&
                    expr->right && expr->right->kind == sql::ExpressionKind::LITERAL)
                {
                    column_expr = expr->left.get();
                    literal_expr = expr->right.get();
                    column_on_left = true;
                }
                else if (expr->right && expr->right->kind == sql::ExpressionKind::COLUMN_REF &&
                         expr->left && expr->left->kind == sql::ExpressionKind::LITERAL)
                {
                    column_expr = expr->right.get();
                    literal_expr = expr->left.get();
                    column_on_left = false;
                }
                else
                {
                    return false;
                }

                std::size_t column_index = find_column_index(columns, table_name, column_expr->column, kClauseWhere);
                const auto &column_entry = columns[column_index];
                Value literal_value = literal_to_value_for_column(column_entry, literal_expr->literal);
                if (literal_value.is_null())
                {
                    return false;
                }

                auto &column_predicate = extraction.predicates[column_entry.column_id];

                sql::BinaryOperator effective_op = op;
                if (!column_on_left)
                {
                    switch (op)
                    {
                    case sql::BinaryOperator::LESS:
                        effective_op = sql::BinaryOperator::GREATER;
                        break;
                    case sql::BinaryOperator::LESS_EQUAL:
                        effective_op = sql::BinaryOperator::GREATER_EQUAL;
                        break;
                    case sql::BinaryOperator::GREATER:
                        effective_op = sql::BinaryOperator::LESS;
                        break;
                    case sql::BinaryOperator::GREATER_EQUAL:
                        effective_op = sql::BinaryOperator::LESS_EQUAL;
                        break;
                    default:
                        break;
                    }
                }

                bool ok = true;
                switch (effective_op)
                {
                case sql::BinaryOperator::EQUAL:
                    ok = column_predicate.apply_equality(literal_value);
                    break;
                case sql::BinaryOperator::GREATER:
                    ok = column_predicate.apply_lower(literal_value, false);
                    break;
                case sql::BinaryOperator::GREATER_EQUAL:
                    ok = column_predicate.apply_lower(literal_value, true);
                    break;
                case sql::BinaryOperator::LESS:
                    ok = column_predicate.apply_upper(literal_value, false);
                    break;
                case sql::BinaryOperator::LESS_EQUAL:
                    ok = column_predicate.apply_upper(literal_value, true);
                    break;
                default:
                    ok = false;
                    break;
                }

                if (!ok)
                {
                    extraction.contradiction = column_predicate.contradiction;
                    if (!extraction.contradiction)
                        return false;
                }

                return true;
            }

            return false;
        };

        if (!visit(predicate))
            return std::nullopt;

        for (auto &entry : extraction.predicates)
        {
            if (entry.second.contradiction || !entry.second.bounds_compatible())
            {
                extraction.contradiction = true;
                break;
            }
        }

        return extraction;
    }

    std::optional<DMLExecutor::IndexScanSpec> DMLExecutor::choose_index_scan(
        const std::vector<TableIndexContext> &index_contexts,
        const PredicateExtraction &predicates) const
    {
        if (predicates.contradiction || predicates.predicates.empty())
            return std::nullopt;

        std::optional<IndexScanSpec> best_spec;
        std::size_t best_width = 0;

        for (std::size_t i = 0; i < index_contexts.size(); ++i)
        {
            const auto &ctx = index_contexts[i];
            if (ctx.catalog_entry.column_ids.empty())
                continue;

            bool matches_all = true;
            std::vector<Value> equality_values;
            equality_values.reserve(ctx.catalog_entry.column_ids.size());

            for (auto column_id : ctx.catalog_entry.column_ids)
            {
                auto pred_it = predicates.predicates.find(column_id);
                if (pred_it == predicates.predicates.end() || !pred_it->second.equality.has_value())
                {
                    matches_all = false;
                    break;
                }
                equality_values.push_back(pred_it->second.equality.value());
            }

            if (matches_all)
            {
                if (!best_spec.has_value() || ctx.catalog_entry.column_ids.size() > best_width)
                {
                    IndexScanSpec spec;
                    spec.context_index = i;
                    spec.kind = IndexScanSpec::Kind::Equality;
                    spec.equality_values = equality_values;
                    best_width = ctx.catalog_entry.column_ids.size();
                    best_spec = spec;
                }
            }
        }

        if (best_spec.has_value())
            return best_spec;

        for (std::size_t i = 0; i < index_contexts.size(); ++i)
        {
            const auto &ctx = index_contexts[i];
            if (ctx.catalog_entry.column_ids.size() != 1)
                continue;

            auto pred_it = predicates.predicates.find(ctx.catalog_entry.column_ids.front());
            if (pred_it == predicates.predicates.end())
                continue;

            const auto &column_pred = pred_it->second;
            if (column_pred.contradiction)
                return std::nullopt;

            if (column_pred.equality.has_value())
            {
                IndexScanSpec spec;
                spec.context_index = i;
                spec.kind = IndexScanSpec::Kind::Equality;
                spec.equality_values = {column_pred.equality.value()};
                return spec;
            }

            if (column_pred.lower.has_value() || column_pred.upper.has_value())
            {
                IndexScanSpec spec;
                spec.context_index = i;
                spec.kind = IndexScanSpec::Kind::Range;
                if (column_pred.lower.has_value())
                {
                    spec.lower_value = column_pred.lower;
                    spec.lower_inclusive = column_pred.lower_inclusive;
                }
                if (column_pred.upper.has_value())
                {
                    spec.upper_value = column_pred.upper;
                    spec.upper_inclusive = column_pred.upper_inclusive;
                }
                return spec;
            }
        }

        return std::nullopt;
    }

    std::vector<record_id_t> DMLExecutor::run_index_scan(
        const IndexScanSpec &spec,
        const std::vector<TableIndexContext> &index_contexts,
        index::IndexHandle &handle,
        const std::vector<catalog::ColumnCatalogEntry> &columns,
        const std::unordered_map<column_id_t, std::size_t> &column_lookup) const
    {
        const auto &ctx = index_contexts[spec.context_index];
        std::vector<catalog::ColumnCatalogEntry> key_columns;
        key_columns.reserve(ctx.catalog_entry.column_ids.size());
        for (auto column_id : ctx.catalog_entry.column_ids)
        {
            auto it = column_lookup.find(column_id);
            if (it == column_lookup.end())
            {
                KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Index column metadata missing", std::to_string(column_id));
            }
            key_columns.push_back(columns[it->second]);
        }

        auto &tree = handle.tree();
        std::vector<record_id_t> result;
        switch (spec.kind)
        {
        case IndexScanSpec::Kind::Equality:
        {
            if (spec.equality_values.size() != key_columns.size())
                break;
            auto key = encode_values(key_columns, spec.equality_values);
            result = tree.ScanEqual(key);
            break;
        }
        case IndexScanSpec::Kind::Range:
        {
            std::optional<std::vector<uint8_t>> lower;
            std::optional<std::vector<uint8_t>> upper;
            if (spec.lower_value.has_value())
            {
                std::vector<Value> tmp{spec.lower_value.value()};
                lower = encode_values(key_columns, tmp);
            }
            if (spec.upper_value.has_value())
            {
                std::vector<Value> tmp{spec.upper_value.value()};
                upper = encode_values(key_columns, tmp);
            }
            result = tree.ScanRange(lower, spec.lower_inclusive, upper, spec.upper_inclusive);
            break;
        }
        }
        if (index_usage_observer_)
            index_usage_observer_(ctx.catalog_entry, result);
        return result;
    }

    TableHeap::RowLocation DMLExecutor::decode_record_id(record_id_t id)
    {
        TableHeap::RowLocation loc;
        loc.page_id = static_cast<page_id_t>(id >> 32);
        loc.slot = static_cast<slot_id_t>(id & 0xFFFFFFFFu);
        return loc;
    }
    record_id_t DMLExecutor::make_record_id(const TableHeap::RowLocation &loc)
    {
        return (static_cast<record_id_t>(loc.page_id) << 32) | static_cast<record_id_t>(loc.slot);
    }

    std::vector<uint8_t> DMLExecutor::encode_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                    const std::vector<Value> &values) const
    {
        std::vector<record::Field> fields;
        fields.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &column = columns[i].column;
            const Value &value = values[i];
            if (value.is_null())
            {
                if (column.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + column.name + "' is NOT NULL");
                fields.push_back(record::from_null(column.type));
                continue;
            }

            switch (column.type)
            {
            case DataType::BOOLEAN:
                fields.push_back(record::from_bool(value.as_bool()));
                break;
            case DataType::INTEGER:
                fields.push_back(record::from_int32(value.as_int32()));
                break;
            case DataType::BIGINT:
                fields.push_back(record::from_int64(value.as_int64()));
                break;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                fields.push_back(record::from_double(value.as_double()));
                break;
            case DataType::DATE:
                fields.push_back(record::from_date(value.as_int64()));
                break;
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                const std::string &text = value.as_string();
                if (column.type == DataType::VARCHAR && column.length > 0 && text.size() > column.length)
                    throw QueryException::invalid_constraint("value too long for column '" + column.name + "'");
                fields.push_back(record::from_string(text));
                break;
            }
            default:
                throw QueryException::unsupported_type("unsupported column type");
            }
        }
        return record::encode(fields);
    }

    Value DMLExecutor::coerce_value_for_column(const catalog::ColumnCatalogEntry &column,
                                               const Value &value) const
    {
        if (value.is_null())
        {
            if (column.column.constraint.not_null)
                throw QueryException::invalid_constraint("column '" + column.column.name + "' is NOT NULL");
            return Value::null(column.column.type);
        }

        switch (column.column.type)
        {
        case DataType::BOOLEAN:
            if (value.type() == DataType::BOOLEAN)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::boolean(value.as_int32() != 0);
            if (value.type() == DataType::BIGINT)
                return Value::boolean(value.as_int64() != 0);
            throw QueryException::type_error("UPDATE", "BOOLEAN", value.to_string());
        case DataType::INTEGER:
            if (value.type() == DataType::INTEGER)
                return value;
            if (value.type() == DataType::BIGINT)
            {
                auto v = value.as_int64();
                if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("UPDATE", "INTEGER", std::to_string(v));
                return Value::int32(static_cast<int32_t>(v));
            }
            throw QueryException::type_error("UPDATE", "INTEGER", value.to_string());
        case DataType::BIGINT:
            if (value.type() == DataType::BIGINT)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::int64(static_cast<int64_t>(value.as_int32()));
            throw QueryException::type_error("UPDATE", "BIGINT", value.to_string());
        case DataType::FLOAT:
        case DataType::DOUBLE:
            if (value.type() == DataType::DOUBLE || value.type() == DataType::FLOAT)
                return Value::floating(value.as_double());
            if (value.type() == DataType::INTEGER)
                return Value::floating(static_cast<double>(value.as_int32()));
            if (value.type() == DataType::BIGINT)
                return Value::floating(static_cast<double>(value.as_int64()));
            throw QueryException::type_error("UPDATE", "DOUBLE", value.to_string());
        case DataType::DATE:
            if (value.type() == DataType::DATE)
                return value;
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
            {
                auto parsed = parse_date(value.as_string());
                if (!parsed)
                    throw QueryException::type_error("UPDATE", "DATE", value.as_string());
                return Value::date(*parsed);
            }
            throw QueryException::type_error("UPDATE", "DATE", value.to_string());
        case DataType::VARCHAR:
        case DataType::TEXT:
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
                return Value::string(value.as_string(), column.column.type);
            throw QueryException::type_error("UPDATE", "STRING", value.to_string());
        default:
            throw QueryException::unsupported_type("unsupported column type");
        }
    }

    std::vector<size_t> DMLExecutor::build_projection(const sql::SelectStatement &stmt,
                                                      const std::vector<BoundColumn> &columns,
                                                      const ExpressionEvaluator &resolver,
                                                      bool qualify_names,
                                                      std::vector<std::string> &out_names) const
    {
        std::vector<size_t> projection;
        out_names.clear();

        bool expanded_star = false;
        for (const auto &item : stmt.columns)
        {
            switch (item.kind)
            {
            case sql::SelectItemKind::STAR:
                if (!expanded_star)
                {
                    for (std::size_t i = 0; i < columns.size(); ++i)
                    {
                        projection.push_back(i);
                        if (qualify_names)
                        {
                            const auto &col = columns[i];
                            const std::string qualifier = col.table_alias.empty() ? col.table_name : col.table_alias;
                            out_names.push_back(qualifier + "." + col.column.column.name);
                        }
                        else
                        {
                            out_names.push_back(columns[i].column.column.name);
                        }
                    }
                    expanded_star = true;
                }
                break;
            case sql::SelectItemKind::COLUMN:
            {
                auto resolved = resolver.resolve_column(item.column, kClauseSelectList);
                projection.push_back(resolved.index);
                if (qualify_names)
                {
                    const auto &col = columns[resolved.index];
                    const std::string qualifier = col.table_alias.empty() ? col.table_name : col.table_alias;
                    out_names.push_back(qualifier + "." + col.column.column.name);
                }
                else
                {
                    out_names.push_back(columns[resolved.index].column.column.name);
                }
                break;
            }
            case sql::SelectItemKind::AGGREGATE:
                break;
            }
        }

        return projection;
    }

    Value DMLExecutor::evaluate_aggregate(const sql::AggregateCall &call,
                                          const ExpressionEvaluator &resolver,
                                          const std::vector<std::vector<Value>> &rows) const
    {
        auto ensure_column = [&](const char *operation) -> ExpressionEvaluator::ResolvedColumn
        {
            if (!call.column.has_value())
                throw QueryException::invalid_constraint(std::string(operation) + " requires a column reference");
            std::string clause = std::string(kClauseAggregate) + " (" + operation + ")";
            return resolver.resolve_column(*call.column, clause);
        };

        switch (call.function)
        {
        case sql::AggregateFunction::COUNT:
            if (call.is_star)
                return Value::int64(static_cast<std::int64_t>(rows.size()));
            else
            {
                auto resolved = ensure_column("COUNT");
                std::unordered_set<std::string> seen;
                std::int64_t count = 0;
                for (const auto &row : rows)
                {
                    const Value &value = row[resolved.index];
                    if (value.is_null())
                        continue;
                    if (call.is_distinct)
                    {
                        if (!seen.insert(value_signature(value)).second)
                            continue;
                    }
                    ++count;
                }
                return Value::int64(count);
            }
        case sql::AggregateFunction::SUM:
        {
            auto resolved = ensure_column("SUM");
            std::unordered_set<std::string> seen;
            bool any = false;
            long double total = 0.0;
            const DataType type = resolved.type;
            auto add_value = [&](const Value &value)
            {
                switch (type)
                {
                case DataType::INTEGER:
                    total += static_cast<long double>(value.as_int32());
                    break;
                case DataType::BIGINT:
                    total += static_cast<long double>(value.as_int64());
                    break;
                case DataType::FLOAT:
                case DataType::DOUBLE:
                    total += static_cast<long double>(value.as_double());
                    break;
                default:
                    throw QueryException::type_error("SUM", "numeric", data_type_to_string(type));
                }
            };

            for (const auto &row : rows)
            {
                const Value &value = row[resolved.index];
                if (value.is_null())
                    continue;
                if (call.is_distinct)
                {
                    if (!seen.insert(value_signature(value)).second)
                        continue;
                }
                add_value(value);
                any = true;
            }
            if (!any)
            {
                return Value::null(type == DataType::DOUBLE || type == DataType::FLOAT ? DataType::DOUBLE : DataType::BIGINT);
            }
            if (type == DataType::DOUBLE || type == DataType::FLOAT)
                return Value::floating(static_cast<double>(total));
            return Value::int64(static_cast<std::int64_t>(total));
        }
        case sql::AggregateFunction::AVG:
        {
            auto resolved = ensure_column("AVG");
            std::unordered_set<std::string> seen;
            long double total = 0.0;
            std::int64_t count = 0;
            for (const auto &row : rows)
            {
                const Value &value = row[resolved.index];
                if (value.is_null())
                    continue;
                if (call.is_distinct)
                {
                    if (!seen.insert(value_signature(value)).second)
                        continue;
                }
                switch (resolved.type)
                {
                case DataType::INTEGER:
                    total += static_cast<long double>(value.as_int32());
                    break;
                case DataType::BIGINT:
                    total += static_cast<long double>(value.as_int64());
                    break;
                case DataType::FLOAT:
                case DataType::DOUBLE:
                    total += static_cast<long double>(value.as_double());
                    break;
                default:
                    throw QueryException::type_error("AVG", "numeric", data_type_to_string(resolved.type));
                }
                ++count;
            }
            if (count == 0)
                return Value::null(DataType::DOUBLE);
            double avg = static_cast<double>(total / static_cast<long double>(count));
            return Value::floating(avg);
        }
        case sql::AggregateFunction::MIN:
        case sql::AggregateFunction::MAX:
        {
            auto resolved = ensure_column(call.function == sql::AggregateFunction::MIN ? "MIN" : "MAX");
            std::unordered_set<std::string> seen;
            bool has_value = false;
            Value best;
            for (const auto &row : rows)
            {
                const Value &value = row[resolved.index];
                if (value.is_null())
                    continue;
                if (call.is_distinct)
                {
                    if (!seen.insert(value_signature(value)).second)
                        continue;
                }
                if (!has_value)
                {
                    best = value;
                    has_value = true;
                    continue;
                }
                auto cmp = compare(value, best);
                if (call.function == sql::AggregateFunction::MIN && cmp == CompareResult::Less)
                    best = value;
                else if (call.function == sql::AggregateFunction::MAX && cmp == CompareResult::Greater)
                    best = value;
            }
            if (!has_value)
                return Value::null(resolved.type);
            return best;
        }
        }
        throw QueryException::invalid_constraint("Unsupported aggregate function");
    }

    std::string DMLExecutor::value_signature(const Value &value) const
    {
        std::string signature = std::to_string(static_cast<int>(value.type()));
        signature.push_back('|');
        if (value.is_null())
        {
            signature.append("NULL");
        }
        else
        {
            signature.append(value.to_string());
        }
        return signature;
    }

    std::string DMLExecutor::row_signature(const std::vector<Value> &row,
                                           const std::vector<size_t> &projection) const
    {
        std::string key;
        bool first = true;
        for (auto index : projection)
        {
            if (!first)
                key.push_back('\x1f');
            first = false;
            key.append(value_signature(row[index]));
        }
        return key;
    }

    std::size_t DMLExecutor::find_column_index(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                               const std::string &table_name,
                                               const sql::ColumnRef &ref,
                                               std::string_view clause) const
    {
        if (!ref.table.empty() && ref.table != table_name)
            throw QueryException::column_not_found(ref.column, ref.table, clause);

        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            if (columns[i].column.name == ref.column)
                return i;
        }
        throw QueryException::column_not_found(ref.column, table_name, clause);
    }

    std::vector<uint8_t> DMLExecutor::encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                 const sql::InsertRow &row,
                                                 const std::vector<std::string> &column_names,
                                                 std::string_view table_name)
    {
        std::unordered_map<std::string, const sql::LiteralValue *> value_lookup;
        value_lookup.reserve(column_names.size());
        for (std::size_t i = 0; i < column_names.size(); ++i)
        {
            value_lookup.emplace(column_names[i], &row.values[i]);
        }

        std::vector<record::Field> fields;
        fields.reserve(columns.size());

        for (const auto &entry : columns)
        {
            const auto &col = entry.column;
            auto it = value_lookup.find(col.name);
            if (it == value_lookup.end())
                throw QueryException::column_not_found(col.name, table_name, kClauseInsertColumns);

            const sql::LiteralValue &literal = *(it->second);
            record::Field field;
            field.type = col.type;

            if (literal.kind == sql::LiteralKind::NULL_LITERAL)
            {
                if (col.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + col.name + "' is NOT NULL");
                field.is_null = true;
            }
            else
            {
                field.is_null = false;
                switch (col.type)
                {
                case DataType::BOOLEAN:
                    if (literal.kind != sql::LiteralKind::BOOLEAN)
                        throw QueryException::type_error("INSERT", "BOOLEAN", literal.text);
                    field = record::from_bool(literal.bool_value);
                    break;
                case DataType::INTEGER:
                case DataType::BIGINT:
                {
                    if (literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    long long value = 0;
                    try
                    {
                        value = std::stoll(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    }
                    if (col.type == DataType::INTEGER)
                    {
                        if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())
                            throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                        field = record::from_int32(static_cast<int32_t>(value));
                    }
                    else
                    {
                        field = record::from_int64(static_cast<int64_t>(value));
                    }
                    break;
                }
                case DataType::DOUBLE:
                {
                    if (literal.kind != sql::LiteralKind::DOUBLE && literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    double value = 0.0;
                    try
                    {
                        value = std::stod(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    }
                    field = record::from_double(value);
                    break;
                }
                case DataType::DATE:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    auto parsed = parse_date(literal.text);
                    if (!parsed)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    field = record::from_date(*parsed);
                    break;
                }
                case DataType::VARCHAR:
                case DataType::TEXT:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "STRING", literal.text);
                    if (col.type == DataType::VARCHAR && col.length > 0 && literal.text.size() > col.length)
                        throw QueryException::invalid_constraint("value too long for column '" + col.name + "'");
                    field = record::from_string(literal.text);
                    break;
                }
                default:
                    throw QueryException::type_error("INSERT", "supported type", literal.text);
                }
            }

            fields.push_back(std::move(field));
        }

        return record::encode(fields);
    }

} // namespace kizuna::engine
