#pragma once

#include <optional>
#include <unordered_map>
#include <string>
#include <string_view>

#include "catalog/catalog_manager.h"
#include "sql/ddl_parser.h"
#include "storage/index/index_manager.h"
#include "storage/table_heap.h"
#include "common/value.h"

namespace kizuna::engine
{
    class DDLExecutor
    {
    public:
        DDLExecutor(catalog::CatalogManager &catalog,
                    PageManager &pm,
                    FileManager &fm,
                    index::IndexManager &index_manager);

        catalog::TableCatalogEntry create_table(std::string_view sql);
        void drop_table(std::string_view sql);
        std::string execute(std::string_view sql);

    private:
        catalog::CatalogManager &catalog_;
        PageManager &pm_;
        FileManager &fm_;
        index::IndexManager &index_manager_;

        catalog::TableCatalogEntry create_from_ast(const sql::CreateTableStatement &stmt,
                                                   std::string_view original_sql);
        bool drop_from_ast(const sql::DropTableStatement &stmt);
        std::string create_index_from_ast(const sql::CreateIndexStatement &stmt,
                                          std::string_view original_sql,
                                          bool is_primary = false);
        bool drop_index_from_ast(const sql::DropIndexStatement &stmt);
        std::string alter_table_from_ast(const sql::AlterTableStatement &stmt,
                                         std::string_view original_sql);

        std::optional<Value> build_default_value(const ColumnDef &column) const;
        static std::optional<Value> parse_default_literal(const ColumnDef &column);

        void rebuild_table_indexes(const catalog::TableCatalogEntry &table_entry);
        std::vector<Value> decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             const std::vector<uint8_t> &payload) const;
        std::unordered_map<column_id_t, std::size_t> build_column_lookup(const std::vector<catalog::ColumnCatalogEntry> &columns) const;
        std::vector<uint8_t> encode_index_key(const std::vector<catalog::ColumnCatalogEntry> &key_columns,
                                              const std::vector<Value> &values) const;
        static record_id_t make_record_id(const TableHeap::RowLocation &loc);

        static ColumnConstraint map_constraint(const sql::ColumnConstraintAST &constraint);
        static ColumnDef map_column(std::size_t index, const sql::ColumnDefAST &column_ast);
    };
}

