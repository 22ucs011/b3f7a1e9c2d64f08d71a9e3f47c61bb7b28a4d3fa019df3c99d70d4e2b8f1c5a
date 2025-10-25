#include "storage/table_heap.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "storage/record.h"

namespace kizuna
{
    namespace
    {
        constexpr bool is_valid_page(page_id_t id)
        {
            return id >= config::FIRST_PAGE_ID;
        }

        record::Field field_from_value(const catalog::ColumnCatalogEntry &column, const Value &value)
        {
            const auto &meta = column.column;
            if (value.is_null())
            {
                if (meta.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + meta.name + "' is NOT NULL");
                return record::from_null(meta.type);
            }

            switch (meta.type)
            {
            case DataType::BOOLEAN:
                if (value.type() == DataType::BOOLEAN)
                    return record::from_bool(value.as_bool());
                if (value.type() == DataType::INTEGER)
                    return record::from_bool(value.as_int32() != 0);
                if (value.type() == DataType::BIGINT)
                    return record::from_bool(value.as_int64() != 0);
                break;
            case DataType::INTEGER:
                if (value.type() == DataType::INTEGER)
                    return record::from_int32(value.as_int32());
                if (value.type() == DataType::BIGINT)
                {
                    auto v = value.as_int64();
                    if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max())
                        throw QueryException::type_error("ALTER TABLE", "INTEGER", std::to_string(v));
                    return record::from_int32(static_cast<int32_t>(v));
                }
                break;
            case DataType::BIGINT:
                if (value.type() == DataType::BIGINT)
                    return record::from_int64(value.as_int64());
                if (value.type() == DataType::INTEGER)
                    return record::from_int64(value.as_int32());
                break;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                if (value.is_numeric())
                    return record::from_double(value.as_double());
                break;
            case DataType::DATE:
                return record::from_date(value.as_int64());
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                const std::string &text = value.as_string();
                if (meta.type == DataType::VARCHAR && meta.length > 0 && text.size() > meta.length)
                    throw QueryException::invalid_constraint("value too long for column '" + meta.name + "'");
                return record::from_string(text);
            }
            default:
                break;
            }

            throw QueryException::type_error("ALTER TABLE", data_type_to_string(meta.type), value.to_string());
        }
    }

    TableHeap::TableHeap(PageManager &pm, page_id_t root_page_id)
        : pm_(pm), root_page_id_(root_page_id), tail_page_id_(root_page_id)
    {
        if (!is_valid_page(root_page_id_))
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Invalid table root", std::to_string(root_page_id_));
        }
        auto &root = pm_.fetch(root_page_id_, true);
        const auto type = static_cast<PageType>(root.header().page_type);
        if (type != PageType::DATA)
        {
            pm_.unpin(root_page_id_, false);
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE, "Table root is not DATA", std::to_string(root_page_id_));
        }
        tail_page_id_ = find_tail(root_page_id_);
        pm_.unpin(root_page_id_, false);
    }

    TableHeap::RowLocation TableHeap::insert(const std::vector<uint8_t> &payload)
    {
        if (payload.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE,
                                 "Record payload too large",
                                 std::to_string(payload.size()));
        }

        page_id_t current = tail_page_id_;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            slot_id_t slot{};
            if (page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot))
            {
                pm_.unpin(current, true);
                tail_page_id_ = current;
                return RowLocation{current, slot};
            }
            page_id_t next = page.next_page_id();
            pm_.unpin(current, false);
            if (is_valid_page(next))
            {
                current = next;
                continue;
            }
            return append_new_page(current, payload);
        }
        return append_new_page(root_page_id_, payload);
    }

    TableHeap::RowLocation TableHeap::update(const RowLocation &loc, const std::vector<uint8_t> &payload)
    {
        if (payload.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE,
                                 "Record payload too large",
                                 std::to_string(payload.size()));
        }
        if (!is_valid_page(loc.page_id))
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_NOT_FOUND, "Invalid page for update", std::to_string(loc.page_id));
        }

        auto &page = pm_.fetch(loc.page_id, true);
        bool updated = page.update(loc.slot, payload.data(), static_cast<uint16_t>(payload.size()));
        pm_.unpin(loc.page_id, updated);
        if (updated)
        {
            return loc;
        }

        if (!erase(loc))
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_NOT_FOUND, "Update erase failed", std::to_string(loc.page_id));
        }

        return insert(payload);
    }

    bool TableHeap::erase(const RowLocation &loc)
    {
        if (!is_valid_page(loc.page_id))
            return false;
        auto &page = pm_.fetch(loc.page_id, true);
        bool ok = page.erase(loc.slot);
        pm_.unpin(loc.page_id, ok);
        return ok;
    }

    bool TableHeap::read(const RowLocation &loc, std::vector<uint8_t> &out) const
    {
        if (!is_valid_page(loc.page_id))
            return false;
        auto &page = pm_.fetch(loc.page_id, true);
        bool ok = page.read(loc.slot, out);
        pm_.unpin(loc.page_id, false);
        return ok;
    }

    void TableHeap::truncate()
    {
        auto &root = pm_.fetch(root_page_id_, true);
        page_id_t next = root.next_page_id();
        root.set_next_page_id(config::INVALID_PAGE_ID);
        root.set_prev_page_id(config::INVALID_PAGE_ID);
        root.header().record_count = 0;
        root.header().slot_count = 0;
        root.header().free_space_offset = static_cast<uint16_t>(Page::kHeaderSize);
        std::memset(root.data() + Page::kHeaderSize, 0, Page::page_size() - Page::kHeaderSize);
        pm_.unpin(root_page_id_, true);

        page_id_t current = next;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            page_id_t nxt = page.next_page_id();
            pm_.unpin(current, false);
            pm_.free_page(current);
            current = nxt;
        }
        tail_page_id_ = root_page_id_;
    }

    TableHeap::Iterator TableHeap::begin()
    {
        return Iterator(this, root_page_id_, 0, false);
    }

    TableHeap::Iterator TableHeap::end()
    {
        return Iterator();
    }

    page_id_t TableHeap::find_tail(page_id_t start) const
    {
        page_id_t current = start;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            page_id_t next = page.next_page_id();
            pm_.unpin(current, false);
            if (!is_valid_page(next))
            {
                return current;
            }
            current = next;
        }
        return start;
    }

    TableHeap::RowLocation TableHeap::append_new_page(page_id_t previous_tail, const std::vector<uint8_t> &payload)
    {
        page_id_t new_page_id = pm_.new_page(PageType::DATA);
        auto &new_page = pm_.fetch(new_page_id, true);
        new_page.set_prev_page_id(previous_tail);
        new_page.set_next_page_id(config::INVALID_PAGE_ID);
        slot_id_t slot{};
        bool ok = new_page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot);
        if (!ok)
        {
            pm_.unpin(new_page_id, false);
            pm_.free_page(new_page_id);
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Record does not fit in empty page", std::to_string(payload.size()));
        }
        pm_.unpin(new_page_id, true);

        auto &prev_page = pm_.fetch(previous_tail, true);
        prev_page.set_next_page_id(new_page_id);
        pm_.unpin(previous_tail, true);

        tail_page_id_ = new_page_id;
        return RowLocation{new_page_id, slot};
    }

    TableHeap::Iterator::Iterator(TableHeap *heap, page_id_t page, slot_id_t slot, bool end)
        : heap_(heap), page_(page), slot_(slot), end_(end)
    {
        if (end_ || heap_ == nullptr)
        {
            heap_ = nullptr;
            page_ = config::INVALID_PAGE_ID;
            slot_ = 0;
            loc_ = RowLocation{};
            payload_.clear();
            end_ = true;
        }
        else
        {
            advance();
        }
    }

    TableHeap::Iterator &TableHeap::Iterator::operator++()
    {
        advance();
        return *this;
    }

    TableHeap::Iterator TableHeap::Iterator::operator++(int)
    {
        Iterator tmp = *this;
        advance();
        return tmp;
    }

    bool TableHeap::Iterator::operator==(const Iterator &other) const
    {
        if (end_ && other.end_)
            return true;
        return heap_ == other.heap_ && end_ == other.end_ && loc_ == other.loc_;
    }

    void TableHeap::Iterator::advance()
    {
        if (heap_ == nullptr)
        {
            end_ = true;
            return;
        }

        while (is_valid_page(page_))
        {
            auto &page = heap_->pm_.fetch(page_, true);
            const auto slot_count = page.header().slot_count;
            while (slot_ < slot_count)
            {
                std::vector<uint8_t> data;
                if (page.read(slot_, data))
                {
                    loc_ = RowLocation{page_, slot_};
                    payload_ = std::move(data);
                    ++slot_;
                    heap_->pm_.unpin(page_, false);
                    end_ = false;
                    return;
                }
                ++slot_;
            }
            page_id_t next = page.next_page_id();
            heap_->pm_.unpin(page_, false);
            page_ = next;
            slot_ = 0;
        }

        heap_ = nullptr;
        page_ = config::INVALID_PAGE_ID;
        slot_ = 0;
        loc_ = RowLocation{};
        payload_.clear();
        end_ = true;
    }

    page_id_t TableHeapMigration::rewrite(PageManager &pm,
                                          page_id_t source_root,
                                          const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                          const std::vector<catalog::ColumnCatalogEntry> &new_schema,
                                          const std::unordered_map<column_id_t, Value> &defaults)
    {
        if (!is_valid_page(source_root))
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Invalid table root", std::to_string(source_root));
        }

        std::vector<catalog::ColumnCatalogEntry> old_sorted = old_schema;
        std::sort(old_sorted.begin(), old_sorted.end(), [](const auto &lhs, const auto &rhs) {
            return lhs.ordinal_position < rhs.ordinal_position;
        });
        std::vector<catalog::ColumnCatalogEntry> new_sorted = new_schema;
        std::sort(new_sorted.begin(), new_sorted.end(), [](const auto &lhs, const auto &rhs) {
            return lhs.ordinal_position < rhs.ordinal_position;
        });

        std::unordered_map<column_id_t, std::size_t> old_index;
        old_index.reserve(old_sorted.size());
        for (std::size_t i = 0; i < old_sorted.size(); ++i)
        {
            old_index.emplace(old_sorted[i].column_id, i);
        }

        page_id_t new_root = pm.new_page(PageType::DATA);
        pm.unpin(new_root, false);
        TableHeap source(pm, source_root);
        TableHeap dest(pm, new_root);

        source.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
            std::vector<record::Field> decoded;
            if (!record::decode(payload.data(), payload.size(), decoded))
            {
                KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "Row decode failed during migration", "");
            }
            if (decoded.size() != old_sorted.size())
            {
                KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "Row schema mismatch during migration", "");
            }

            std::vector<record::Field> new_fields;
            new_fields.reserve(new_sorted.size());

            for (const auto &column : new_sorted)
            {
                auto it = old_index.find(column.column_id);
                if (it != old_index.end())
                {
                    new_fields.push_back(decoded[it->second]);
                    continue;
                }

                auto def_it = defaults.find(column.column_id);
                if (def_it != defaults.end())
                {
                    new_fields.push_back(field_from_value(column, def_it->second));
                    continue;
                }

                if (column.column.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + column.column.name + "' requires DEFAULT value for existing rows");

                if (!config::ALTER_TABLE_ALLOW_IMPLICIT_NULL_FILL)
                    throw QueryException::invalid_constraint("ALTER TABLE ADD COLUMN requires DEFAULT value for existing rows");

                new_fields.push_back(record::from_null(column.column.type));
            }

            auto encoded = record::encode(new_fields);
            dest.insert(encoded);
        });

        return new_root;
    }

    page_id_t TableHeapMigration::add_column(PageManager &pm,
                                             page_id_t source_root,
                                             const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                             const catalog::ColumnCatalogEntry &new_column,
                                             const std::optional<Value> &default_value)
    {
        std::vector<catalog::ColumnCatalogEntry> new_schema;
        new_schema.reserve(old_schema.size() + 1);
        bool inserted = false;
        for (const auto &col : old_schema)
        {
            if (!inserted && col.ordinal_position >= new_column.ordinal_position)
            {
                auto copy = new_column;
                copy.ordinal_position = static_cast<uint32_t>(new_schema.size());
                new_schema.push_back(copy);
                inserted = true;
            }
            auto copy_existing = col;
            copy_existing.ordinal_position = static_cast<uint32_t>(new_schema.size());
            new_schema.push_back(copy_existing);
        }
        if (!inserted)
        {
            auto copy = new_column;
            copy.ordinal_position = static_cast<uint32_t>(new_schema.size());
            new_schema.push_back(copy);
        }

        std::unordered_map<column_id_t, Value> defaults;
        if (default_value.has_value())
        {
            defaults.emplace(new_column.column_id, *default_value);
        }

        return rewrite(pm, source_root, old_schema, new_schema, defaults);
    }

    page_id_t TableHeapMigration::drop_column(PageManager &pm,
                                              page_id_t source_root,
                                              const std::vector<catalog::ColumnCatalogEntry> &old_schema,
                                              column_id_t drop_column_id)
    {
        std::vector<catalog::ColumnCatalogEntry> new_schema;
        new_schema.reserve(old_schema.size());
        bool removed = false;
        for (const auto &col : old_schema)
        {
            if (col.column_id == drop_column_id)
            {
                removed = true;
                continue;
            }
            auto copy = col;
            copy.ordinal_position = static_cast<uint32_t>(new_schema.size());
            new_schema.push_back(copy);
        }
        if (!removed)
            KIZUNA_THROW_QUERY(StatusCode::COLUMN_NOT_FOUND, "Column not found during migration", std::to_string(drop_column_id));
        if (new_schema.empty())
            KIZUNA_THROW_QUERY(StatusCode::INVALID_CONSTRAINT, "cannot drop all columns", "");

        std::unordered_map<column_id_t, Value> defaults;
        return rewrite(pm, source_root, old_schema, new_schema, defaults);
    }

    void TableHeapMigration::free_chain(PageManager &pm, page_id_t root_page_id)
    {
        page_id_t current = root_page_id;
        while (is_valid_page(current))
        {
            auto &page = pm.fetch(current, true);
            page_id_t next = page.next_page_id();
            pm.unpin(current, false);
            pm.free_page(current);
            current = next;
        }
    }
}
