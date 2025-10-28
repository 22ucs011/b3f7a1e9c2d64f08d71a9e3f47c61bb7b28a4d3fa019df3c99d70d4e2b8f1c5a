#include "engine/ddl_executor.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <limits>
#include <cstring>

#include "storage/table_heap.h"
#include "storage/record.h"

#include "common/exception.h"
#include "common/config.h"

namespace kizuna::engine
{
    namespace
    {
        std::string normalize_identifier(std::string_view name)
        {
            std::string upper(name);
            std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c)
                           { return static_cast<char>(std::toupper(c)); });
            return upper;
        }
    }

    DDLExecutor::DDLExecutor(catalog::CatalogManager &catalog,
                             PageManager &pm,
                             FileManager &fm,
                             index::IndexManager &index_manager)
        : catalog_(catalog), pm_(pm), fm_(fm), index_manager_(index_manager)
    {
    }

    catalog::TableCatalogEntry DDLExecutor::create_table(std::string_view sql)
    {
        auto stmt = sql::parse_create_table(sql);
        return create_from_ast(stmt, sql);
    }

    void DDLExecutor::drop_table(std::string_view sql)
    {
        auto stmt = sql::parse_drop_table(sql);
        drop_from_ast(stmt);
    }

    std::string DDLExecutor::execute(std::string_view sql)
    {
        auto ddl = sql::parse_ddl(sql);
        switch (ddl.kind)
        {
        case sql::StatementKind::CREATE_TABLE:
        {
            auto entry = create_from_ast(ddl.create, sql);
            return "Table created: " + entry.name;
        }
        case sql::StatementKind::DROP_TABLE:
        {
            bool dropped = drop_from_ast(ddl.drop);
            if (dropped)
            {
                return "Table dropped: " + ddl.drop.table_name;
            }
            return "Table not found (no-op): " + ddl.drop.table_name;
        }
        case sql::StatementKind::CREATE_INDEX:
        {
            return create_index_from_ast(ddl.create_index, sql);
        }
        case sql::StatementKind::DROP_INDEX:
        {
            bool dropped = drop_index_from_ast(ddl.drop_index);
            if (dropped)
            {
                return "Index dropped: " + ddl.drop_index.index_name;
            }
            return "Index not found (no-op): " + ddl.drop_index.index_name;
        }
        case sql::StatementKind::ALTER_TABLE:
        {
            return alter_table_from_ast(ddl.alter, sql);
        }
        default:
            break;
        }
        throw DBException(StatusCode::NOT_IMPLEMENTED, "DDL not supported yet", std::string(sql));
    }
    catalog::TableCatalogEntry DDLExecutor::create_from_ast(const sql::CreateTableStatement &stmt,
                                                            std::string_view original_sql)
    {
        TableDef def;
        def.name = stmt.table_name;
        if (def.name.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "table name");
        if (stmt.columns.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "column list");
        if (stmt.columns.size() > config::MAX_COLUMNS_PER_TABLE)
            throw QueryException::invalid_constraint("too many columns");

        std::unordered_set<std::string> seen_names;
        bool primary_key_seen = false;
        std::string primary_key_name;
        def.columns.reserve(stmt.columns.size());
        for (std::size_t i = 0; i < stmt.columns.size(); ++i)
        {
            const auto &col_ast = stmt.columns[i];
            if (col_ast.name.empty())
                throw QueryException::syntax_error(std::string(original_sql), 0, "column name");
            std::string normalized = normalize_identifier(col_ast.name);
            if (!seen_names.insert(normalized).second)
                throw QueryException::duplicate_column(col_ast.name);

            ColumnDef column = map_column(i, col_ast);
            if (column.constraint.primary_key)
            {
                if (primary_key_seen)
                    throw QueryException::invalid_constraint("multiple PRIMARY KEY columns");
                primary_key_seen = true;
                primary_key_name = column.name;
            }
            def.columns.push_back(std::move(column));
        }
        def.schema_version = 1;
        def.next_column_id = static_cast<column_id_t>(def.columns.size() + 1);

        page_id_t root_page_id = pm_.new_page(PageType::DATA);

        catalog::TableCatalogEntry entry = catalog_.create_table(def, root_page_id, std::string(original_sql));

        if (!primary_key_name.empty())
        {
            sql::CreateIndexStatement pk_stmt;
            pk_stmt.index_name = entry.name + "_pk";
            pk_stmt.unique = true;
            pk_stmt.table_name = entry.name;
            pk_stmt.column_names.push_back(primary_key_name);
            pk_stmt.if_not_exists = true;
            (void)create_index_from_ast(pk_stmt, "AUTO PRIMARY KEY INDEX", true);
        }

        auto table_file = FileManager::table_path(entry.table_id);
        std::error_code ec;
        auto parent = table_file.parent_path();
        if (!parent.empty())
            std::filesystem::create_directories(parent, ec);
        std::ofstream ofs(table_file, std::ios::binary | std::ios::trunc);
        if (!ofs)
        {
            catalog_.drop_table(entry.name, true);
            pm_.free_page(entry.root_page_id);
            throw IOException::write_error(table_file.string(), 0);
        }
        ofs.close();
        return entry;
    }

    bool DDLExecutor::drop_from_ast(const sql::DropTableStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
        {
            if (stmt.if_exists)
            {
                return false;
            }
            throw QueryException::table_not_found(stmt.table_name);
        }
        const auto table_entry = table_opt.value();
        auto indexes = catalog_.get_indexes(table_entry.table_id);
        for (const auto &idx_entry : indexes)
        {
            index_manager_.DropIndex(idx_entry);
        }
        bool removed = catalog_.drop_table(stmt.table_name, stmt.cascade);
        if (!removed)
        {
            if (stmt.if_exists)
            {
                return false;
            }
            throw QueryException::table_not_found(stmt.table_name);
        }
        pm_.free_page(table_entry.root_page_id);
        auto table_file = FileManager::table_path(table_entry.table_id);
        if (FileManager::exists(table_file))
        {
            FileManager::remove_file(table_file);
        }
        return true;
    }

    std::string DDLExecutor::create_index_from_ast(const sql::CreateIndexStatement &stmt,
                                                   std::string_view original_sql,
                                                   bool is_primary)
    {
        if (stmt.index_name.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "index name");

        if (catalog_.index_exists(stmt.index_name))
        {
            if (stmt.if_not_exists)
            {
                return "Index already exists (no-op): " + stmt.index_name;
            }
            throw QueryException::invalid_constraint("index already exists: " + stmt.index_name);
        }

        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
        {
            throw QueryException::table_not_found(stmt.table_name);
        }
        const auto table_entry = table_opt.value();
        auto columns = catalog_.get_columns(table_entry.table_id);

        if (stmt.column_names.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "column list");

        std::vector<column_id_t> column_ids;
        column_ids.reserve(stmt.column_names.size());
        for (const auto &name : stmt.column_names)
        {
            std::string normalized = normalize_identifier(name);
            auto it = std::find_if(columns.begin(), columns.end(), [&](const catalog::ColumnCatalogEntry &entry)
                                   { return normalize_identifier(entry.column.name) == normalized; });
            if (it == columns.end())
            {
                throw QueryException::column_not_found(name, stmt.table_name);
            }
            column_ids.push_back(it->column_id);
        }

        catalog::IndexCatalogEntry entry;
        entry.table_id = table_entry.table_id;
        entry.name = stmt.index_name;
        entry.is_unique = stmt.unique;
        entry.is_primary = is_primary;
        entry.column_ids = column_ids;
        entry.root_page_id = config::INVALID_PAGE_ID;
        entry.create_sql = std::string(original_sql);

        auto created = catalog_.create_index(entry);
        auto handle = index_manager_.CreateIndex(created);
        created.root_page_id = handle->tree().root_page_id();
        catalog_.set_index_root(created.index_id, created.root_page_id);
        return "Index created: " + created.name;
    }

    bool DDLExecutor::drop_index_from_ast(const sql::DropIndexStatement &stmt)
    {
        auto index_opt = catalog_.get_index(stmt.index_name);
        if (!index_opt)
        {
            if (stmt.if_exists)
            {
                return false;
            }
            throw IndexException::key_not_found(stmt.index_name, stmt.index_name);
        }

        const auto entry = index_opt.value();
        index_manager_.DropIndex(entry);
        return catalog_.drop_index(stmt.index_name);
    }

    std::string DDLExecutor::alter_table_from_ast(const sql::AlterTableStatement &stmt,
                                                  std::string_view original_sql)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
        {
            throw QueryException::table_not_found(stmt.table_name);
        }
        const auto table_entry = table_opt.value();
        auto old_columns = catalog_.get_columns(table_entry.table_id);

        switch (stmt.action)
        {
        case sql::AlterTableAction::ADD_COLUMN:
        {
            if (!stmt.add_column.has_value())
            {
                throw QueryException::syntax_error(std::string(original_sql), 0, "column definition");
            }

            ColumnDef column = map_column(old_columns.size(), *stmt.add_column);
            auto default_value = build_default_value(column);

            catalog::ColumnCatalogEntry migration_entry;
            migration_entry.table_id = table_entry.table_id;
            migration_entry.column_id = table_entry.next_column_id;
            migration_entry.ordinal_position = static_cast<uint32_t>(old_columns.size());
            migration_entry.schema_version = table_entry.schema_version + 1;
            migration_entry.is_dropped = false;
            column.id = migration_entry.column_id;
            migration_entry.column = column;

            page_id_t new_root = TableHeapMigration::add_column(pm_,
                                                                table_entry.root_page_id,
                                                                old_columns,
                                                                migration_entry,
                                                                default_value);

            auto added_entry = catalog_.add_column(table_entry.table_id, column);
            catalog_.set_table_root(table_entry.table_id, new_root);
            TableHeapMigration::free_chain(pm_, table_entry.root_page_id);
            if (auto updated_entry = catalog_.get_table(table_entry.table_id))
            {
                rebuild_table_indexes(*updated_entry);
            }

            return "Column added: " + added_entry.column.name;
        }
        case sql::AlterTableAction::DROP_COLUMN:
        {
            if (!stmt.drop_column_name.has_value() || stmt.drop_column_name->empty())
            {
                throw QueryException::syntax_error(std::string(original_sql), 0, "column name");
            }
            if (old_columns.size() <= 1)
            {
                throw QueryException::invalid_constraint("cannot drop the last column");
            }
            auto it = std::find_if(old_columns.begin(), old_columns.end(), [&](const catalog::ColumnCatalogEntry &entry)
                                   { return entry.column.name == *stmt.drop_column_name; });
            if (it == old_columns.end())
            {
                throw QueryException::column_not_found(*stmt.drop_column_name, stmt.table_name);
            }
            if (it->column.constraint.primary_key)
            {
                throw QueryException::invalid_constraint("cannot drop PRIMARY KEY column '" + it->column.name + "'");
            }

            page_id_t new_root = TableHeapMigration::drop_column(pm_,
                                                                 table_entry.root_page_id,
                                                                 old_columns,
                                                                 it->column_id);

            auto dropped_entry = catalog_.drop_column(table_entry.table_id, *stmt.drop_column_name);
            (void)dropped_entry;
            catalog_.set_table_root(table_entry.table_id, new_root);
            TableHeapMigration::free_chain(pm_, table_entry.root_page_id);
            if (auto updated_entry = catalog_.get_table(table_entry.table_id))
            {
                rebuild_table_indexes(*updated_entry);
            }

            return "Column dropped: " + *stmt.drop_column_name;
        }
        default:
            break;
        }

        throw DBException(StatusCode::NOT_IMPLEMENTED, "ALTER TABLE action not supported", std::string(original_sql));
    }

    std::optional<Value> DDLExecutor::build_default_value(const ColumnDef &column) const
    {
        if (!column.constraint.has_default)
        {
            if (column.constraint.not_null)
            {
                throw QueryException::invalid_constraint("ALTER TABLE ADD COLUMN requires DEFAULT for NOT NULL column '" + column.name + "'");
            }
            if (!config::ALTER_TABLE_ALLOW_IMPLICIT_NULL_FILL)
            {
                throw QueryException::invalid_constraint("ALTER TABLE ADD COLUMN requires DEFAULT value");
            }
            return std::nullopt;
        }
        return parse_default_literal(column);
    }

    std::optional<Value> DDLExecutor::parse_default_literal(const ColumnDef &column)
    {
        if (!column.constraint.has_default)
            return std::nullopt;

        const std::string &literal = column.constraint.default_value;
        auto is_null_literal = [](std::string_view text)
        {
            std::string upper(text);
            std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c)
                           { return static_cast<char>(std::toupper(c)); });
            return upper == "NULL";
        };
        if (is_null_literal(literal))
        {
            return Value::null(column.type);
        }

        try
        {
            switch (column.type)
            {
            case DataType::BOOLEAN:
            {
                std::string upper(literal);
                std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c)
                               { return static_cast<char>(std::toupper(c)); });
                if (upper == "TRUE" || literal == "1")
                    return Value::boolean(true);
                if (upper == "FALSE" || literal == "0")
                    return Value::boolean(false);
                throw QueryException::invalid_constraint("invalid BOOLEAN default: " + literal);
            }
            case DataType::INTEGER:
            {
                long long value = std::stoll(literal);
                if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())
                    throw QueryException::invalid_constraint("INTEGER default out of range");
                return Value::int32(static_cast<int32_t>(value));
            }
            case DataType::BIGINT:
                return Value::int64(std::stoll(literal));
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return Value::floating(std::stod(literal));
            case DataType::DATE:
            {
                auto parsed = parse_date(literal);
                if (!parsed.has_value())
                    throw QueryException::invalid_constraint("invalid DATE default: " + literal);
                return Value::date(*parsed);
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
                return Value::string(literal, column.type);
            default:
                throw QueryException::unsupported_type("default values for this column type are not supported");
            }
        }
        catch (const std::exception &)
        {
            throw QueryException::invalid_constraint("invalid default literal '" + literal + "' for column '" + column.name + "'");
        }
    }

    void DDLExecutor::rebuild_table_indexes(const catalog::TableCatalogEntry &table_entry)
    {
        auto indexes = catalog_.get_indexes(table_entry.table_id);
        if (indexes.empty())
            return;

        auto columns = catalog_.get_columns(table_entry.table_id);
        auto lookup = build_column_lookup(columns);

        std::vector<catalog::IndexCatalogEntry> active_indexes;
        active_indexes.reserve(indexes.size());
        for (const auto &idx : indexes)
        {
            bool missing_column = false;
            for (auto column_id : idx.column_ids)
            {
                if (lookup.find(column_id) == lookup.end())
                {
                    missing_column = true;
                    break;
                }
            }
            if (missing_column)
            {
                index_manager_.DropIndex(idx);
                catalog_.drop_index(idx.name);
                continue;
            }
            active_indexes.push_back(idx);
        }

        indexes = std::move(active_indexes);
        if (indexes.empty())
            return;

        struct RowSnapshot
        {
            record_id_t record_id;
            std::vector<Value> values;
        };
        std::vector<RowSnapshot> rows;
        TableHeap heap(pm_, table_entry.root_page_id);
        heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload)
                  {
            RowSnapshot snap;
            snap.record_id = make_record_id(loc);
            snap.values = decode_row_values(columns, payload);
            rows.push_back(std::move(snap)); });

        for (auto &idx : indexes)
        {
            index_manager_.DropIndex(idx);
            catalog::IndexCatalogEntry temp_entry = idx;
            temp_entry.root_page_id = config::INVALID_PAGE_ID;
            auto handle = index_manager_.CreateIndex(temp_entry);
            auto &tree = handle->tree();
            catalog_.set_index_root(idx.index_id, tree.root_page_id());

            std::vector<catalog::ColumnCatalogEntry> key_columns;
            key_columns.reserve(idx.column_ids.size());
            std::vector<std::size_t> key_positions;
            key_positions.reserve(idx.column_ids.size());
            for (auto column_id : idx.column_ids)
            {
                auto it = lookup.find(column_id);
                if (it == lookup.end())
                {
                    KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Index column metadata missing", std::to_string(column_id));
                }
                key_positions.push_back(it->second);
                key_columns.push_back(columns[it->second]);
            }

            for (const auto &row : rows)
            {
                std::vector<Value> key_values;
                key_values.reserve(key_positions.size());
                for (auto pos : key_positions)
                {
                    key_values.push_back(row.values[pos]);
                }
                auto key = encode_index_key(key_columns, key_values);
                tree.Insert(key, row.record_id);
            }
            catalog_.set_index_root(idx.index_id, tree.root_page_id());
        }
    }

    std::vector<Value> DDLExecutor::decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
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
                throw QueryException::unsupported_type("Unsupported column type for index rebuild");
            }
        }
        return values;
    }

    std::unordered_map<column_id_t, std::size_t> DDLExecutor::build_column_lookup(const std::vector<catalog::ColumnCatalogEntry> &columns) const
    {
        std::unordered_map<column_id_t, std::size_t> lookup;
        lookup.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            lookup.emplace(columns[i].column_id, i);
        }
        return lookup;
    }

    std::vector<uint8_t> DDLExecutor::encode_index_key(const std::vector<catalog::ColumnCatalogEntry> &key_columns,
                                                       const std::vector<Value> &values) const
    {
        std::vector<record::Field> fields;
        fields.reserve(key_columns.size());
        for (std::size_t i = 0; i < key_columns.size(); ++i)
        {
            const auto &column = key_columns[i].column;
            const auto &value = values[i];
            if (value.is_null())
            {
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
            case DataType::DATE:
            case DataType::TIMESTAMP:
                fields.push_back(record::from_int64(value.as_int64()));
                break;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                fields.push_back(record::from_double(value.as_double()));
                break;
            case DataType::VARCHAR:
            case DataType::TEXT:
                fields.push_back(record::from_string(value.as_string()));
                break;
            default:
                throw QueryException::unsupported_type("Unsupported index column type");
            }
        }
        return record::encode(fields);
    }

    record_id_t DDLExecutor::make_record_id(const TableHeap::RowLocation &loc)
    {
        return (static_cast<record_id_t>(loc.page_id) << 32) | static_cast<record_id_t>(loc.slot);
    }

    ColumnConstraint DDLExecutor::map_constraint(const sql::ColumnConstraintAST &constraint)
    {
        ColumnConstraint result;
        result.not_null = constraint.not_null || constraint.primary_key;
        result.primary_key = constraint.primary_key;
        result.unique = constraint.unique || constraint.primary_key;
        if (constraint.default_literal.has_value())
        {
            result.has_default = true;
            result.default_value = constraint.default_literal.value();
        }
        return result;
    }

    ColumnDef DDLExecutor::map_column(std::size_t index, const sql::ColumnDefAST &column_ast)
    {
        ColumnDef column;
        column.id = static_cast<column_id_t>(index + 1);
        column.name = column_ast.name;
        column.type = column_ast.type;
        column.length = column_ast.length;
        column.constraint = map_constraint(column_ast.constraint);
        return column;
    }
}
