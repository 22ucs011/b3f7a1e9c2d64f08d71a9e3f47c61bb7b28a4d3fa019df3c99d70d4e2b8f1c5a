#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog_manager.h"
#include "engine/expression_evaluator.h"
#include "common/value.h"
#include "sql/ast.h"
#include "sql/dml_parser.h"
#include "storage/index/index_manager.h"
#include "storage/table_heap.h"

namespace kizuna::engine
{
    struct InsertResult
    {
        std::size_t rows_inserted{0};
    };

    struct DeleteResult
    {
        std::size_t rows_deleted{0};
    };

    struct UpdateResult
    {
        std::size_t rows_updated{0};
    };

    struct SelectResult
    {
        std::vector<std::string> column_names;
        std::vector<std::vector<std::string>> rows;
    };

    class DMLExecutor
    {
    public:
        DMLExecutor(catalog::CatalogManager &catalog,
                    PageManager &pm,
                    FileManager &fm,
                    index::IndexManager &index_manager);

        InsertResult insert_into(const sql::InsertStatement &stmt);
        SelectResult select(const sql::SelectStatement &stmt);
        DeleteResult delete_all(const sql::DeleteStatement &stmt);
        UpdateResult update_all(const sql::UpdateStatement &stmt);
        void truncate(const sql::TruncateStatement &stmt);

        std::string execute(std::string_view sql);

        void set_index_usage_observer(std::function<void(const catalog::IndexCatalogEntry &,
                                                         const std::vector<record_id_t> &)> observer);

    private:
        catalog::CatalogManager &catalog_;
        PageManager &pm_;
        FileManager &fm_;
        index::IndexManager &index_manager_;

        std::vector<Value> decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             const std::vector<uint8_t> &payload) const;
        struct TableIndexContext
        {
            catalog::IndexCatalogEntry catalog_entry;
        };
        std::vector<TableIndexContext> load_table_indexes(table_id_t table_id) const;
        std::unordered_map<column_id_t, std::size_t> build_column_lookup(const std::vector<catalog::ColumnCatalogEntry> &columns) const;
        std::vector<uint8_t> build_index_key(const TableIndexContext &ctx,
                                             const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             const std::vector<Value> &row_values,
                                             const std::unordered_map<column_id_t, std::size_t> &lookup) const;
        static record_id_t make_record_id(const TableHeap::RowLocation &loc);
        std::vector<uint8_t> encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                        const sql::InsertRow &row,
                                        const std::vector<std::string> &column_names);
        std::vector<uint8_t> encode_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                           const std::vector<Value> &values) const;
        Value coerce_value_for_column(const catalog::ColumnCatalogEntry &column,
                                      const Value &value) const;
        struct BoundTable
        {
            catalog::TableCatalogEntry table;
            std::vector<catalog::ColumnCatalogEntry> columns;
            std::string alias;
        };

        struct BoundColumn
        {
            catalog::ColumnCatalogEntry column;
            std::string table_name;
            std::string table_alias;
        };

        std::vector<size_t> build_projection(const sql::SelectStatement &stmt,
                                             const std::vector<BoundColumn> &columns,
                                             const ExpressionEvaluator &resolver,
                                             bool qualify_names,
                                             std::vector<std::string> &out_names) const;
        std::size_t find_column_index(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                      const std::string &table_name,
                                      const sql::ColumnRef &ref) const;

        Value evaluate_aggregate(const sql::AggregateCall &call,
                                 const ExpressionEvaluator &resolver,
                                 const std::vector<std::vector<Value>> &rows) const;

        std::string value_signature(const Value &value) const;
        std::string row_signature(const std::vector<Value> &row,
                                  const std::vector<size_t> &projection) const;

        struct ColumnPredicate;
        struct PredicateExtraction;
        struct IndexScanSpec;
        Value literal_to_value_for_column(const catalog::ColumnCatalogEntry &column,
                                          const sql::LiteralValue &literal) const;
        std::optional<PredicateExtraction> extract_column_predicates(
            const sql::Expression *predicate,
            const std::vector<catalog::ColumnCatalogEntry> &columns,
            const std::string &table_name) const;
        std::optional<IndexScanSpec> choose_index_scan(const std::vector<TableIndexContext> &index_contexts,
                                                       const PredicateExtraction &predicates) const;
        std::vector<record_id_t> run_index_scan(const IndexScanSpec &spec,
                                                const std::vector<TableIndexContext> &index_contexts,
                                                index::IndexHandle &handle,
                                                const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                const std::unordered_map<column_id_t, std::size_t> &column_lookup) const;
        static TableHeap::RowLocation decode_record_id(record_id_t id);

        mutable std::function<void(const catalog::IndexCatalogEntry &,
                                   const std::vector<record_id_t> &)>
            index_usage_observer_;
    };
}
