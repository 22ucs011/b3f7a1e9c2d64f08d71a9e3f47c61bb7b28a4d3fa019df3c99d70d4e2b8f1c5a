#include "catalog/catalog_manager.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
namespace kizuna::catalog
{
    namespace
    {
        template <typename F>
        void for_each_slot(PageManager &pm, page_id_t page_id, F &&fn)
        {
            auto &page = pm.fetch(page_id, true);
            try
            {
                const auto slot_count = page.header().slot_count;
                for (slot_id_t slot = 0; slot < slot_count; ++slot)
                {
                    std::vector<uint8_t> payload;
                    if (!page.read(slot, payload))
                        continue;
                    if (payload.empty())
                        continue;
                    fn(payload);
                }
                pm.unpin(page_id, false);
            }
            catch (...)
            {
                pm.unpin(page_id, false);
                throw;
            }
        }

        void refresh_cached_page(PageManager &pm, page_id_t page_id)
        {
            try
            {
                pm.fetch(page_id, true);
                pm.unpin(page_id, false);
            }
            catch (const DBException &)
            {
                // best-effort; ignore failures during refresh
            }
        }

        bool column_entry_less(const ColumnCatalogEntry &a, const ColumnCatalogEntry &b)
        {
            if (a.table_id == b.table_id)
            {
                if (a.is_dropped != b.is_dropped)
                    return !a.is_dropped && b.is_dropped;
                if (a.ordinal_position == b.ordinal_position)
                    return a.column_id < b.column_id;
                return a.ordinal_position < b.ordinal_position;
            }
            return a.table_id < b.table_id;
        }
    } // namespace

    CatalogManager::CatalogManager(PageManager &pm, FileManager &fm)
        : pm_(pm), fm_(fm)
    {
        ensure_catalog_pages();
    }

    void CatalogManager::ensure_catalog_pages()
    {
        tables_root_ = pm_.catalog_tables_root();
        columns_root_ = pm_.catalog_columns_root();
        indexes_root_ = pm_.catalog_indexes_root();

        if (tables_root_ < config::FIRST_PAGE_ID)
        {
            tables_root_ = pm_.new_page(PageType::DATA);
            pm_.set_catalog_tables_root(tables_root_);
            pm_.unpin(tables_root_, false);
        }
        if (columns_root_ < config::FIRST_PAGE_ID)
        {
            columns_root_ = pm_.new_page(PageType::DATA);
            pm_.set_catalog_columns_root(columns_root_);
            pm_.unpin(columns_root_, false);
        }
        if (indexes_root_ < config::FIRST_PAGE_ID)
        {
            indexes_root_ = pm_.new_page(PageType::DATA);
            pm_.set_catalog_indexes_root(indexes_root_);
            pm_.unpin(indexes_root_, false);
        }
    }

    void CatalogManager::load_tables_cache() const
    {
        if (tables_loaded_)
            return;

        tables_cache_.clear();
        for_each_slot(pm_, tables_root_, [this](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            TableCatalogEntry entry = TableCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            tables_cache_.push_back(std::move(entry));
        });
        tables_loaded_ = true;
    }

    void CatalogManager::load_indexes_cache() const
    {
        if (indexes_loaded_)
            return;

        indexes_cache_.clear();
        for_each_slot(pm_, indexes_root_, [this](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            IndexCatalogEntry entry = IndexCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            indexes_cache_.push_back(std::move(entry));
        });
        std::sort(indexes_cache_.begin(), indexes_cache_.end(), [](const IndexCatalogEntry &a, const IndexCatalogEntry &b) {
            if (a.table_id == b.table_id)
                return a.name < b.name;
            return a.table_id < b.table_id;
        });
        indexes_loaded_ = true;
    }

    void CatalogManager::reload_indexes_cache() const
    {
        indexes_loaded_ = false;
        load_indexes_cache();
    }


    void CatalogManager::reload_tables_cache() const
    {
        tables_loaded_ = false;
        load_tables_cache();
    }

    std::vector<TableCatalogEntry> CatalogManager::read_all_tables() const
    {
        load_tables_cache();
        return tables_cache_;
    }

    std::vector<ColumnCatalogEntry> CatalogManager::read_all_columns() const
    {
        std::vector<ColumnCatalogEntry> result;
        for_each_slot(pm_, columns_root_, [&result](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            ColumnCatalogEntry entry = ColumnCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            result.push_back(std::move(entry));
        });
        std::sort(result.begin(), result.end(), column_entry_less);
        return result;
    }

    std::vector<IndexCatalogEntry> CatalogManager::read_all_indexes() const
    {
        std::vector<IndexCatalogEntry> result;
        for_each_slot(pm_, indexes_root_, [&result](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            IndexCatalogEntry entry = IndexCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            result.push_back(std::move(entry));
        });
        std::sort(result.begin(), result.end(), [](const IndexCatalogEntry &a, const IndexCatalogEntry &b) {
            if (a.table_id == b.table_id)
                return a.name < b.name;
            return a.table_id < b.table_id;
        });
        return result;
    }


    std::vector<ColumnCatalogEntry> CatalogManager::read_all_columns(table_id_t table_id) const
    {
        std::vector<ColumnCatalogEntry> result;
        for_each_slot(pm_, columns_root_, [&result, table_id](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            ColumnCatalogEntry entry = ColumnCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            if (entry.table_id == table_id)
            {
                result.push_back(std::move(entry));
            }
        });
        std::sort(result.begin(), result.end(), column_entry_less);
        return result;
    }

    bool CatalogManager::table_exists(std::string_view name) const
    {
        load_tables_cache();
        return std::any_of(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
    }

    std::optional<TableCatalogEntry> CatalogManager::get_table(std::string_view name) const
    {
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == tables_cache_.end())
            return std::nullopt;
        return *it;
    }

    std::optional<TableCatalogEntry> CatalogManager::get_table(table_id_t id) const
    {
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.table_id == id;
        });
        if (it == tables_cache_.end())
            return std::nullopt;
        return *it;
    }

    std::vector<TableCatalogEntry> CatalogManager::list_tables() const
    {
        return read_all_tables();
    }

    std::vector<ColumnCatalogEntry> CatalogManager::get_columns(table_id_t table_id) const
    {
        auto columns = read_all_columns(table_id);
        columns.erase(std::remove_if(columns.begin(), columns.end(), [](const ColumnCatalogEntry &entry) {
            return entry.is_dropped;
        }), columns.end());
        return columns;
    }

    std::optional<ColumnCatalogEntry> CatalogManager::get_column(table_id_t table_id, std::string_view column_name, bool include_dropped) const
    {
        auto columns = read_all_columns(table_id);
        for (const auto &entry : columns)
        {
            if (!include_dropped && entry.is_dropped)
                continue;
            if (entry.column.name == column_name)
                return entry;
        }
        return std::nullopt;
    }

    ColumnCatalogEntry CatalogManager::add_column(table_id_t table_id, ColumnDef column, std::optional<uint32_t> position)
    {
        ensure_catalog_pages();
        load_tables_cache();

        auto table_it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.table_id == table_id;
        });
        if (table_it == tables_cache_.end())
            throw QueryException::table_not_found(std::to_string(table_id));

        if (column.constraint.primary_key)
            throw QueryException::invalid_constraint("ALTER TABLE ADD COLUMN does not support PRIMARY KEY");

        auto all_columns = read_all_columns();
        std::vector<std::size_t> table_indices;
        table_indices.reserve(all_columns.size());
        uint32_t active_count = 0;
        for (std::size_t i = 0; i < all_columns.size(); ++i)
        {
            auto &entry = all_columns[i];
            if (entry.table_id != table_id)
                continue;
            table_indices.push_back(i);
            if (!entry.is_dropped)
            {
                ++active_count;
                if (entry.column.name == column.name)
                    throw QueryException::duplicate_column(column.name);
            }
        }

        if (active_count >= config::MAX_COLUMNS_PER_TABLE)
            throw QueryException::invalid_constraint("too many columns");

        const uint32_t insert_pos = position.value_or(active_count);
        if (insert_pos > active_count)
            throw QueryException::invalid_constraint("invalid column position");

        const uint32_t new_schema_version = table_it->schema_version + 1;

        ColumnCatalogEntry new_entry;
        new_entry.table_id = table_id;
        new_entry.column_id = table_it->next_column_id;
        new_entry.ordinal_position = insert_pos;
        new_entry.schema_version = new_schema_version;
        new_entry.is_dropped = false;
        column.id = new_entry.column_id;
        new_entry.column = std::move(column);

        for (std::size_t idx : table_indices)
        {
            auto &entry = all_columns[idx];
            if (!entry.is_dropped && entry.ordinal_position >= insert_pos)
            {
                entry.ordinal_position += 1;
            }
            entry.schema_version = new_schema_version;
        }

        all_columns.push_back(new_entry);
        std::sort(all_columns.begin(), all_columns.end(), column_entry_less);
        rewrite_columns_page(all_columns);

        table_it->schema_version = new_schema_version;
        table_it->next_column_id = new_entry.column_id + 1;
        rewrite_tables_page(tables_cache_);

        return new_entry;
    }

    ColumnCatalogEntry CatalogManager::drop_column(table_id_t table_id, std::string_view column_name)
    {
        ensure_catalog_pages();
        load_tables_cache();

        auto table_it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.table_id == table_id;
        });
        if (table_it == tables_cache_.end())
            throw QueryException::table_not_found(std::to_string(table_id));

        auto all_columns = read_all_columns();
        std::vector<std::size_t> table_indices;
        table_indices.reserve(all_columns.size());

        std::size_t target_index = std::numeric_limits<std::size_t>::max();
        for (std::size_t i = 0; i < all_columns.size(); ++i)
        {
            auto &entry = all_columns[i];
            if (entry.table_id != table_id)
                continue;
            table_indices.push_back(i);
            if (!entry.is_dropped && entry.column.name == column_name)
                target_index = i;
        }

        if (target_index == std::numeric_limits<std::size_t>::max())
            throw QueryException::column_not_found(std::string(column_name), table_it->name);
        auto &target = all_columns[target_index];
        if (target.column.constraint.primary_key)
            throw QueryException::invalid_constraint("cannot drop PRIMARY KEY column '" + target.column.name + "'");

        std::size_t remaining = 0;
        for (std::size_t idx : table_indices)
        {
            if (idx == target_index)
                continue;
            const auto &entry = all_columns[idx];
            if (!entry.is_dropped)
                ++remaining;
        }
        if (remaining == 0)
            throw QueryException::invalid_constraint("cannot drop the last column");

        const uint32_t new_schema_version = table_it->schema_version + 1;

        target.is_dropped = true;
        target.schema_version = new_schema_version;
        target.ordinal_position = std::numeric_limits<uint32_t>::max();
        ColumnCatalogEntry dropped_copy = target;

        for (std::size_t idx : table_indices)
        {
            all_columns[idx].schema_version = new_schema_version;
        }
        uint32_t ordinal = 0;
        for (std::size_t idx : table_indices)
        {
            if (idx == target_index)
                continue;
            auto &entry = all_columns[idx];
            if (entry.is_dropped)
                continue;
            entry.ordinal_position = ordinal++;
        }

        std::sort(all_columns.begin(), all_columns.end(), column_entry_less);
        rewrite_columns_page(all_columns);

        table_it->schema_version = new_schema_version;
        rewrite_tables_page(tables_cache_);

        return dropped_copy;
    }

    bool CatalogManager::index_exists(std::string_view name) const
    {
        return get_index(name).has_value();
    }

    std::optional<IndexCatalogEntry> CatalogManager::get_index(std::string_view name) const
    {
        load_indexes_cache();
        auto it = std::find_if(indexes_cache_.begin(), indexes_cache_.end(), [name](const IndexCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == indexes_cache_.end())
        {
            return std::nullopt;
        }
        return *it;
    }

    std::vector<IndexCatalogEntry> CatalogManager::get_indexes(table_id_t table_id) const
    {
        load_indexes_cache();
        std::vector<IndexCatalogEntry> result;
        for (const auto &entry : indexes_cache_)
        {
            if (entry.table_id == table_id)
            {
                result.push_back(entry);
            }
        }
        return result;
    }

    std::vector<IndexCatalogEntry> CatalogManager::list_indexes() const
    {
        load_indexes_cache();
        return indexes_cache_;
    }


    void CatalogManager::persist_table_entry(const TableCatalogEntry &entry)
    {
        auto data = entry.serialize();
        auto &page = pm_.fetch(tables_root_, true);
        try
        {
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(tables_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog table page full", std::to_string(tables_root_));
            }
            pm_.unpin(tables_root_, true);
        }
        catch (...)
        {
            pm_.unpin(tables_root_, false);
            throw;
        }
    }

    void CatalogManager::persist_column_entry(const ColumnCatalogEntry &entry)
    {
        auto data = entry.serialize();
        auto &page = pm_.fetch(columns_root_, true);
        try
        {
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(columns_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog column page full", std::to_string(columns_root_));
            }
            pm_.unpin(columns_root_, true);
        }
        catch (...)
        {
            pm_.unpin(columns_root_, false);
            throw;
        }
    }

    void CatalogManager::persist_index_entry(const IndexCatalogEntry &entry)
    {
        auto data = entry.serialize();
        auto &page = pm_.fetch(indexes_root_, true);
        try
        {
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(indexes_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog index page full", std::to_string(indexes_root_));
            }
            pm_.unpin(indexes_root_, true);
        }
        catch (...)
        {
            pm_.unpin(indexes_root_, false);
            throw;
        }
    }

    void CatalogManager::rewrite_tables_page(const std::vector<TableCatalogEntry> &entries)
    {
        auto &page = pm_.fetch(tables_root_, true);
        page.init(PageType::DATA, tables_root_);
        for (const auto &entry : entries)
        {
            auto data = entry.serialize();
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(tables_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog table page full", std::to_string(tables_root_));
            }
        }
        pm_.unpin(tables_root_, true);
    }

    void CatalogManager::rewrite_columns_page(const std::vector<ColumnCatalogEntry> &entries)
    {
        auto &page = pm_.fetch(columns_root_, true);
        page.init(PageType::DATA, columns_root_);
        for (const auto &entry : entries)
        {
            auto data = entry.serialize();
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(columns_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog column page full", std::to_string(columns_root_));
            }
        }
        pm_.unpin(columns_root_, true);
    }

    void CatalogManager::rewrite_indexes_page(const std::vector<IndexCatalogEntry> &entries)
    {
        auto &page = pm_.fetch(indexes_root_, true);
        page.init(PageType::DATA, indexes_root_);
        for (const auto &entry : entries)
        {
            auto data = entry.serialize();
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(indexes_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog index page full", std::to_string(indexes_root_));
            }
        }
        pm_.unpin(indexes_root_, true);
    }


    void CatalogManager::set_table_root(table_id_t table_id, page_id_t root_page_id)
    {
        ensure_catalog_pages();
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [table_id](const TableCatalogEntry &entry) {
            return entry.table_id == table_id;
        });
        if (it == tables_cache_.end())
        {
            throw QueryException::table_not_found(std::to_string(table_id));
        }
        it->root_page_id = root_page_id;
        rewrite_tables_page(tables_cache_);
    }

    IndexCatalogEntry CatalogManager::create_index(IndexCatalogEntry entry)
    {
        ensure_catalog_pages();
        load_indexes_cache();

        if (entry.name.empty())
        {
            KIZUNA_THROW_QUERY(StatusCode::INVALID_ARGUMENT, "index name cannot be empty", "");
        }
        if (index_exists(entry.name))
        {
            KIZUNA_THROW_QUERY(StatusCode::DUPLICATE_KEY, "index already exists", std::string(entry.name));
        }
        auto owning_table = get_table(entry.table_id);
        if (!owning_table.has_value())
        {
            KIZUNA_THROW_QUERY(StatusCode::TABLE_NOT_FOUND, "table not found for index", std::to_string(entry.table_id));
        }
        if (entry.column_ids.empty())
        {
            KIZUNA_THROW_QUERY(StatusCode::INVALID_ARGUMENT, "index requires at least one column", entry.name);
        }

        index_id_t new_id = pm_.next_index_id();
        pm_.set_next_index_id(new_id + 1);
        entry.index_id = new_id;

        persist_index_entry(entry);
        indexes_cache_.push_back(entry);
        std::sort(indexes_cache_.begin(), indexes_cache_.end(), [](const IndexCatalogEntry &a, const IndexCatalogEntry &b) {
            if (a.table_id == b.table_id)
                return a.name < b.name;
            return a.table_id < b.table_id;
        });
        indexes_loaded_ = true;
        return entry;
    }

    void CatalogManager::set_index_root(index_id_t index_id, page_id_t root_page_id)
    {
        load_indexes_cache();
        bool updated = false;
        for (auto &entry : indexes_cache_)
        {
            if (entry.index_id == index_id)
            {
                entry.root_page_id = root_page_id;
                updated = true;
                break;
            }
        }
        if (!updated)
        {
            KIZUNA_THROW_INDEX(StatusCode::INDEX_NOT_FOUND, "Index not found", std::to_string(index_id));
        }
        rewrite_indexes_page(indexes_cache_);
    }

    bool CatalogManager::drop_index(std::string_view name)
    {
        load_indexes_cache();
        auto it = std::find_if(indexes_cache_.begin(), indexes_cache_.end(), [name](const IndexCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == indexes_cache_.end())
        {
            return false;
        }
        indexes_cache_.erase(it);
        rewrite_indexes_page(indexes_cache_);
        indexes_loaded_ = true;
        return true;
    }

    TableCatalogEntry CatalogManager::create_table(TableDef def, page_id_t root_page_id, const std::string &create_sql)
    {
        ensure_catalog_pages();
        load_tables_cache();

        if (table_exists(def.name))
        {
            throw QueryException::table_exists(def.name);
        }

        table_id_t new_id = pm_.next_table_id();
        pm_.set_next_table_id(new_id + 1);

        def.id = new_id;
        if (def.schema_version == 0)
            def.schema_version = 1;
        if (def.next_column_id <= def.columns.size())
            def.next_column_id = static_cast<column_id_t>(def.columns.size() + 1);
        TableCatalogEntry table_entry = TableCatalogEntry::from_table_def(def, root_page_id, create_sql);
        persist_table_entry(table_entry);
        if (tables_loaded_)
        {
            tables_cache_.push_back(table_entry);
        }

        for (std::size_t i = 0; i < def.columns.size(); ++i)
        {
            ColumnCatalogEntry col_entry;
            col_entry.table_id = new_id;
            col_entry.column_id = static_cast<column_id_t>(i + 1);
            col_entry.ordinal_position = static_cast<uint32_t>(i);
            col_entry.schema_version = table_entry.schema_version;
            col_entry.is_dropped = false;
            col_entry.column = def.columns[i];
            col_entry.column.id = col_entry.column_id;
            persist_column_entry(col_entry);
        }

        return table_entry;
    }

    bool CatalogManager::drop_table(std::string_view name, bool cascade)
    {
        (void)cascade; // no dependent objects yet
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == tables_cache_.end())
        {
            return false;
        }

        TableCatalogEntry removed = *it;
        tables_cache_.erase(it);
        rewrite_tables_page(tables_cache_);
        tables_loaded_ = true;

        auto all_columns = read_all_columns();
        std::vector<ColumnCatalogEntry> filtered;
        filtered.reserve(all_columns.size());
        for (auto &entry : all_columns)
        {
            if (entry.table_id != removed.table_id)
            {
                filtered.push_back(std::move(entry));
            }
        }
        rewrite_columns_page(filtered);
        auto all_indexes = read_all_indexes();
        std::vector<IndexCatalogEntry> index_filtered;
        index_filtered.reserve(all_indexes.size());
        for (auto &entry : all_indexes)
        {
            if (entry.table_id != removed.table_id)
            {
                index_filtered.push_back(std::move(entry));
            }
        }
        rewrite_indexes_page(index_filtered);
        indexes_cache_ = index_filtered;
        indexes_loaded_ = true;

        return true;
    }
}








