#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/schema.h"
#include "storage/page.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"

namespace kizuna::catalog
{
    class CatalogManager
    {
    public:
        CatalogManager(PageManager &pm, FileManager &fm);

        bool table_exists(std::string_view name) const;
        std::optional<TableCatalogEntry> get_table(std::string_view name) const;
        std::optional<TableCatalogEntry> get_table(table_id_t id) const;
        std::vector<TableCatalogEntry> list_tables() const;

        std::vector<ColumnCatalogEntry> get_columns(table_id_t table_id) const;
        std::optional<ColumnCatalogEntry> get_column(table_id_t table_id, std::string_view column_name, bool include_dropped = false) const;
        ColumnCatalogEntry add_column(table_id_t table_id, ColumnDef column, std::optional<uint32_t> position = std::nullopt);
        ColumnCatalogEntry drop_column(table_id_t table_id, std::string_view column_name);
        bool index_exists(std::string_view name) const;
        std::optional<IndexCatalogEntry> get_index(std::string_view name) const;
        std::vector<IndexCatalogEntry> get_indexes(table_id_t table_id) const;
        std::vector<IndexCatalogEntry> list_indexes() const;

        IndexCatalogEntry create_index(IndexCatalogEntry entry);
        void set_index_root(index_id_t index_id, page_id_t root_page_id);
        bool drop_index(std::string_view name);
        void set_table_root(table_id_t table_id, page_id_t root_page_id);


        TableCatalogEntry create_table(TableDef def,
                                       page_id_t root_page_id,
                                       const std::string &create_sql);

        bool drop_table(std::string_view name, bool cascade);

    private:
        PageManager &pm_;
        FileManager &fm_;
        page_id_t tables_root_;
        page_id_t columns_root_;
        page_id_t indexes_root_;

        mutable bool tables_loaded_{false};
        mutable bool indexes_loaded_{false};
        mutable std::vector<TableCatalogEntry> tables_cache_;
        mutable std::vector<IndexCatalogEntry> indexes_cache_;

        void ensure_catalog_pages();
        void load_tables_cache() const;
        void reload_tables_cache() const;
        void load_indexes_cache() const;
        void reload_indexes_cache() const;

        void persist_table_entry(const TableCatalogEntry &entry);
        void persist_column_entry(const ColumnCatalogEntry &entry);
        void persist_index_entry(const IndexCatalogEntry &entry);

        std::vector<TableCatalogEntry> read_all_tables() const;
        std::vector<ColumnCatalogEntry> read_all_columns() const;
        std::vector<IndexCatalogEntry> read_all_indexes() const;
        std::vector<ColumnCatalogEntry> read_all_columns(table_id_t table_id) const;

        void rewrite_tables_page(const std::vector<TableCatalogEntry> &entries);
        void rewrite_columns_page(const std::vector<ColumnCatalogEntry> &entries);
        void rewrite_indexes_page(const std::vector<IndexCatalogEntry> &entries);
    };
}
