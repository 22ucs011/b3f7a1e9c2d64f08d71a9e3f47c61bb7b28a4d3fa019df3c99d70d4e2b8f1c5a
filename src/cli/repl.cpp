#include "cli/repl.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string_view>

#include "sql/dml_parser.h"

namespace fs = std::filesystem;

namespace
{
    constexpr std::size_t kMaxSelectColumnWidth = 40;

    std::string trim_copy(std::string_view text)
    {
        auto begin = text.begin();
        auto end = text.end();
        while (begin != end && std::isspace(static_cast<unsigned char>(*begin)))
            ++begin;
        while (end != begin && std::isspace(static_cast<unsigned char>(*(end - 1))))
            --end;
        return std::string(begin, end);
    }

    std::string to_upper(std::string_view text)
    {
        std::string out;
        out.reserve(text.size());
        for (unsigned char c : text)
            out.push_back(static_cast<char>(std::toupper(c)));
        return out;
    }

    std::string sanitize_cell_text(std::string_view text)
    {
        std::string out;
        out.reserve(text.size());
        for (unsigned char c : text)
        {
            switch (c)
            {
            case '\n':
            case '\r':
                out.push_back(' ');
                break;
            case '\t':
                out.append("    ");
                break;
            default:
                out.push_back(static_cast<char>(c));
                break;
            }
        }
        return out;
    }

    enum class CellAlign
    {
        Left,
        Center,
        Right
    };

    std::string pad_text(const std::string &text, std::size_t width, CellAlign align)
    {
        if (width == 0)
            return text;

        std::string clipped = text;
        if (clipped.size() > width)
            clipped = clipped.substr(0, width);

        std::size_t padding = width > clipped.size() ? width - clipped.size() : 0;
        switch (align)
        {
        case CellAlign::Right:
            return std::string(padding, ' ') + clipped;
        case CellAlign::Center:
        {
            std::size_t left = padding / 2;
            std::size_t right = padding - left;
            return std::string(left, ' ') + clipped + std::string(right, ' ');
        }
        case CellAlign::Left:
        default:
            return clipped + std::string(padding, ' ');
        }
    }

    std::vector<std::string> wrap_text(const std::string &text, std::size_t width)
    {
        std::vector<std::string> lines;
        if (width == 0)
        {
            lines.emplace_back(text);
            return lines;
        }

        if (text.empty())
        {
            lines.emplace_back("");
            return lines;
        }

        std::size_t pos = 0;
        while (pos < text.size())
        {
            std::size_t remaining = text.size() - pos;
            if (remaining <= width)
            {
                lines.push_back(text.substr(pos));
                break;
            }

            std::size_t end = pos + width;
            std::size_t break_pos = text.find_last_of(' ', end - 1);
            if (break_pos != std::string::npos && break_pos >= pos)
            {
                end = break_pos + 1;
            }

            std::string slice = text.substr(pos, end - pos);
            while (!slice.empty() && std::isspace(static_cast<unsigned char>(slice.back())))
                slice.pop_back();

            if (slice.empty())
            {
                // No reasonable break point; hard break at width.
                slice = text.substr(pos, width);
                end = pos + width;
            }

            lines.push_back(slice);
            pos = end;
            while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos])))
                ++pos;
        }

        if (lines.empty())
            lines.emplace_back("");

        return lines;
    }
}

namespace kizuna
{
    Repl::Repl()
    {
        init_handlers();
        db_path_ = (config::default_db_dir() / (std::string("demo") + config::DB_FILE_EXTENSION)).string();
    }

    void Repl::init_handlers()
    {
        handlers_["help"] = [this](auto const &[[maybe_unused]] args)
        { print_help(); };
        handlers_["status"] = [this](auto const &args)
        { cmd_status(args); };
        handlers_["show"] = [this](auto const &args)
        { cmd_show_tables(args); };
        handlers_["schema"] = [this](auto const &args)
        { cmd_schema(args); };
        handlers_["open"] = [this](auto const &args)
        { cmd_open(args); };
        handlers_["newpage"] = [this](auto const &args)
        { cmd_newpage(args); };
        handlers_["write_demo"] = [this](auto const &args)
        { cmd_write_demo(args); };
        handlers_["read_demo"] = [this](auto const &args)
        { cmd_read_demo(args); };
        handlers_["loglevel"] = [this](auto const &args)
        { cmd_loglevel(args); };
        handlers_["freepage"] = [this](auto const &args)
        { cmd_freepage(args); };
    }

    void Repl::print_help() const
    {
        const auto default_demo = (config::default_db_dir() / (std::string("demo") + config::DB_FILE_EXTENSION)).string();
        std::cout << "Commands:\n"
                  << "  help                      - show this help\n"
                  << "  open [path]               - open/create database file (default: " << default_demo << ")\n"
                  << "  status                    - show current status\n"
                  << "  show tables               - list tables in the current database\n"
                  << "  schema <table>            - show catalog info for a table\n"
                  << "  newpage [type]            - allocate new page (types: DATA, INDEX, METADATA)\n"
                  << "  write_demo <page_id>      - write a demo record to page\n"
                  << "  read_demo <page_id> <slot>- read and display a demo record\n"
                  << "  freepage <page_id>        - free a page (adds to free list)\n"
                  << "  loglevel <DEBUG|INFO|...> - set log verbosity\n"
                  << "  exit/quit                 - leave\n"
                  << "\nSQL DDL (V0.2):\n"
                  << "  CREATE TABLE <name>(...) [;]     - add a table to the catalog (INT, FLOAT, VARCHAR(n))\n"
                  << "  DROP TABLE [IF EXISTS] <name> [;]- drop table metadata and storage\n"
                  << "\nSQL DDL (V0.6 additions):\n"
                  << "  ALTER TABLE <name> ADD COLUMN col TYPE [DEFAULT expr]; - append column with optional default\n"
                  << "  ALTER TABLE <name> DROP COLUMN col;                   - remove column (migrates table data)\n"
                  << "\nSQL DML (V0.3 baseline):\n"
                  << "  INSERT INTO <table> VALUES (...);                 - append rows\n"
                  << "  SELECT * FROM <table>;                            - scan entire table\n"
                  << "  DELETE FROM <table>;                              - delete all rows\n"
                  << "  TRUNCATE TABLE <table>;                            - wipe the table fast\n"
                  << "\nSQL DML (V0.4 additions):\n"
                  << "  INSERT INTO <table> [(col,...)] VALUES (...);      - column-targeted inserts\n"
                  << "  SELECT col[, ...] FROM <table> [WHERE ...] [LIMIT n]; - projection + filtering\n"
                  << "  UPDATE <table> SET col = expr[, ...] [WHERE ...];    - edit rows in place\n"
                  << "  DELETE FROM <table> [WHERE ...];                   - remove matching rows\n"
                  << "\nSQL DML (V0.5 additions):\n"
                  << "  SELECT ... ORDER BY <col> [ASC|DESC] [LIMIT n];    - ordered results via indexes or in-memory sort\n"
                  << "\nSQL DML (V0.6 additions):\n"
                  << "  SELECT ... ORDER BY col1 [ASC|DESC], col2 ...;    - multi-column ordering with mixed directions\n"
                  << "  SELECT DISTINCT col[, ...] FROM ...;              - remove duplicate result rows\n"
                  << "  SELECT COUNT|SUM|AVG|MIN|MAX(expr) FROM ...;      - aggregation (including DISTINCT variants)\n"
                  << "  SELECT ... FROM a INNER JOIN b ON predicate;      - combine rows across tables\n";
    }

    std::vector<std::string> Repl::tokenize(const std::string &line)
    {
        std::vector<std::string> tokens;
        std::istringstream iss(line);
        std::string tok;
        while (iss >> tok)
            tokens.push_back(tok);
        return tokens;
    }

    bool Repl::ensure_db_open() const
    {
        if (!fm_)
        {
            std::cout << "Open a DB first (use 'open')\n";
            return false;
        }
        return true;
    }

    bool Repl::ensure_valid_data_page(page_id_t id, bool must_exist) const
    {
        if (!ensure_db_open())
            return false;
        if (id == config::FIRST_PAGE_ID)
        {
            std::cout << "Page 1 is reserved for metadata; use a page >= 2\n";
            return false;
        }
        if (must_exist)
        {
            const auto count = fm_->page_count();
            if (id > count)
            {
                std::cout << "Page " << id << " does not exist (page count = " << count << "). Use 'newpage'.\n";
                return false;
            }
        }
        return true;
    }

    int Repl::run()
    {
        std::cout << "Kizuna REPL (V0.6) - type 'help'\n";
        Logger::instance().enable_console(false);
        Logger::instance().info("Starting REPL");

        try
        {
            fs::create_directories(config::database_root_dir());
            fs::create_directories(config::catalog_dir());
            fs::create_directories(config::default_db_dir());
            fs::create_directories(config::default_index_dir());
            fs::create_directories(config::temp_dir());
            fs::create_directories(config::backup_dir());
            fs::create_directories(config::logs_dir());
        }
        catch (...)
        {
        }

        std::string line;
        while (true)
        {
            std::cout << "> " << std::flush;
            if (!std::getline(std::cin, line))
                break;
            auto tokens = tokenize(line);
            if (tokens.empty())
                continue;

            const std::string cmd = tokens[0];
            if (cmd == "exit" || cmd == "quit")
                break;

            auto it = handlers_.find(cmd);
            try
            {
                if (it != handlers_.end())
                {
                    it->second(tokens);
                }
                else if (looks_like_sql(line))
                {
                    dispatch_sql(line);
                }
                else
                {
                    std::cout << "Unknown command: " << cmd << " (try 'help')\n";
                }
            }
            catch (const DBException &e)
            {
                Logger::instance().error("Exception: ", e.what());
                std::cout << "Error: " << e.what() << "\n";
            }
            catch (const std::exception &e)
            {
                Logger::instance().error("Exception: ", e.what());
                std::cout << "Error: " << e.what() << "\n";
            }
            catch (...)
            {
                Logger::instance().error("Unknown exception");
                std::cout << "Unknown error\n";
            }
        }

        Logger::instance().info("Exiting REPL");
        return 0;
    }

    void Repl::cmd_open(const std::vector<std::string> &args)
    {
        if (args.size() >= 3)
        {
            std::cout << "Usage: open [path]\n";
            return;
        }

        dml_executor_.reset();
        ddl_executor_.reset();
        index_manager_.reset();
        catalog_.reset();
        pm_.reset();
        fm_.reset();

        std::filesystem::path target_path;
        if (args.size() == 2)
        {
            std::filesystem::path provided(args[1]);
            if (!provided.has_parent_path())
                target_path = config::default_db_dir() / provided;
            else
                target_path = provided;
        }
        else
        {
            target_path = config::default_db_dir() / std::string("demo");
        }

        if (!target_path.has_extension())
            target_path += config::DB_FILE_EXTENSION;
        else if (target_path.extension() != config::DB_FILE_EXTENSION)
            target_path.replace_extension(config::DB_FILE_EXTENSION);

        target_path = target_path.lexically_normal();
        db_path_ = target_path.string();
        std::cout << "Opening: " << db_path_ << "\n";

        fm_ = std::make_unique<FileManager>(db_path_, /*create_if_missing*/ true);
        fm_->open();
        pm_ = std::make_unique<PageManager>(*fm_, /*capacity*/ 64);
        catalog_ = std::make_unique<catalog::CatalogManager>(*pm_, *fm_);
        index_manager_ = std::make_unique<index::IndexManager>();
        ddl_executor_ = std::make_unique<engine::DDLExecutor>(*catalog_, *pm_, *fm_, *index_manager_);
        dml_executor_ = std::make_unique<engine::DMLExecutor>(*catalog_, *pm_, *fm_, *index_manager_);
        Logger::instance().info("Opened DB ", db_path_);
    }

    void Repl::cmd_status(const std::vector<std::string> &[[maybe_unused]] args)
    {
        std::cout << "DB: " << (fm_ ? db_path_ : std::string("<not open>")) << "\n";
        if (!fm_)
            return;

        std::cout << "  size: " << fm_->size_bytes() << " bytes, pages: " << fm_->page_count();
        if (pm_)
        {
            std::cout << ", free pages: " << pm_->free_count();
        }
        if (catalog_)
        {
            auto tables = catalog_->list_tables();
            std::cout << ", tables: " << tables.size();
        }
        std::cout << "\n";
    }

    void Repl::cmd_schema(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: schema <table>\n";
            return;
        }
        if (!ensure_db_open() || !catalog_)
            return;

        const std::string &table_name = args[1];
        auto table_opt = catalog_->get_table(table_name);
        if (!table_opt)
        {
            std::cout << "No table named '" << table_name << "'.\n";
            return;
        }

        const auto &table = *table_opt;
        auto columns = catalog_->get_columns(table.table_id);
        std::cout << "Table: " << table.name
                  << " (id=" << table.table_id
                  << ", root_page=" << table.root_page_id << ")\n";

        if (columns.empty())
        {
            std::cout << "  No columns recorded for this table.\n";
            if (!table.create_sql.empty())
                std::cout << "  CREATE SQL: " << table.create_sql << "\n";
            return;
        }

        std::cout << std::left;
        std::cout << "  #  " << std::setw(18) << "Name"
                  << std::setw(16) << "Type"
                  << "Constraints\n";
        std::cout << "  ------------------------------------------------------------\n";

        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &col_entry = columns[i];
            const auto &col = col_entry.column;

            std::string type_label;
            switch (col.type)
            {
            case DataType::INTEGER:
                type_label = "INTEGER";
                break;
            case DataType::BIGINT:
                type_label = "BIGINT";
                break;
            case DataType::FLOAT:
                type_label = "FLOAT";
                break;
            case DataType::DOUBLE:
                type_label = "DOUBLE";
                break;
            case DataType::BOOLEAN:
                type_label = "BOOLEAN";
                break;
            case DataType::VARCHAR:
                type_label = "VARCHAR(" + std::to_string(col.length) + ")";
                break;
            default:
                type_label = "TYPE#" + std::to_string(static_cast<int>(col.type));
                break;
            }

            std::string constraints = "";
            const auto &c = col.constraint;
            if (c.primary_key)
            {
                constraints = "PRIMARY KEY";
            }
            else
            {
                if (c.not_null)
                    constraints += constraints.empty() ? "NOT NULL" : ", NOT NULL";
                if (c.unique)
                    constraints += constraints.empty() ? "UNIQUE" : ", UNIQUE";
            }
            if (c.has_default)
            {
                constraints += constraints.empty() ? "DEFAULT " : ", DEFAULT ";
                constraints += c.default_value;
            }
            if (constraints.empty())
                constraints = "-";

            std::cout << "  " << std::setw(3) << (i + 1)
                      << std::setw(18) << col.name
                      << std::setw(16) << type_label
                      << constraints << "\n";
        }

        if (!table.create_sql.empty())
            std::cout << "  CREATE SQL: " << table.create_sql << "\n";
    }

    void Repl::cmd_show_tables(const std::vector<std::string> &args)
    {
        if (args.size() != 2 || to_upper(args[1]) != "TABLES")
        {
            std::cout << "Usage: show tables\n";
            return;
        }
        if (!ensure_db_open() || !catalog_)
            return;

        auto tables = catalog_->list_tables();
        if (tables.empty())
        {
            std::cout << "(no tables yet)\n";
            return;
        }

        std::cout << "Tables (" << tables.size() << "):\n";
        std::cout << "  #  " << std::setw(18) << "Name"
                  << std::setw(10) << "Table ID"
                  << std::setw(12) << "Root Page"
                  << "Columns\n";
        std::cout << "  -----------------------------------------------------------\n";

        for (std::size_t i = 0; i < tables.size(); ++i)
        {
            const auto &table = tables[i];
            auto cols = catalog_->get_columns(table.table_id);
            std::cout << "  " << std::setw(3) << (i + 1)
                      << std::setw(18) << table.name
                      << std::setw(10) << table.table_id
                      << std::setw(12) << table.root_page_id
                      << cols.size() << "\n";
        }
    }

    void Repl::cmd_newpage(const std::vector<std::string> &args)
    {
        if (!ensure_db_open())
            return;

        PageType t = PageType::DATA;
        if (args.size() == 2)
        {
            std::string type = to_upper(args[1]);
            if (type == "DATA")
                t = PageType::DATA;
            else if (type == "INDEX")
                t = PageType::INDEX;
            else if (type == "METADATA")
                t = PageType::METADATA;
            else
            {
                std::cout << "Unknown page type '" << args[1] << "' (use DATA/INDEX/METADATA)\n";
                return;
            }
        }

        page_id_t id = pm_->new_page(t);
        std::cout << "Allocated page " << id << " of type " << static_cast<int>(t) << "\n";
    }

    void Repl::cmd_write_demo(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: write_demo <page_id>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt != PageType::DATA)
        {
            std::cout << "Page " << id << " is not a DATA page.\n";
            pm_->unpin(id, false);
            return;
        }
        std::vector<record::Field> fields;
        fields.push_back(record::from_int32(42));
        fields.push_back(record::from_string("hello world"));
        auto payload = record::encode(fields);
        slot_id_t slot{};
        if (!page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot))
        {
            std::cout << "Page full or not enough space (free=" << page.free_bytes()
                      << " bytes, need=" << (payload.size() + 2 + sizeof(uint16_t)) << ")\n";
        }
        else
        {
            std::cout << "Wrote record at slot " << slot << "\n";
        }
        pm_->unpin(id, /*dirty*/ true);
    }

    void Repl::cmd_read_demo(const std::vector<std::string> &args)
    {
        if (args.size() != 3)
        {
            std::cout << "Usage: read_demo <page_id> <slot>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        slot_id_t slot = static_cast<slot_id_t>(std::stoul(args[2]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt != PageType::DATA)
        {
            std::cout << "Page " << id << " is not a DATA page.\n";
            pm_->unpin(id, false);
            return;
        }
        std::vector<uint8_t> out;
        if (!page.read(slot, out))
        {
            if (slot >= page.header().slot_count)
                std::cout << "No such slot (slot_count=" << page.header().slot_count << ")\n";
            else
                std::cout << "Empty/tombstoned or invalid record at that slot\n";
            pm_->unpin(id, false);
            return;
        }

        std::vector<record::Field> fields;
        if (!record::decode(out.data(), out.size(), fields))
        {
            std::cout << "Failed to decode record\n";
            pm_->unpin(id, false);
            return;
        }
        std::cout << "Record fields (" << fields.size() << "):\n";
        for (size_t i = 0; i < fields.size(); ++i)
        {
            std::cout << "  [" << i << "] ";
            switch (fields[i].type)
            {
            case DataType::INTEGER:
                if (fields[i].payload.size() == 4)
                {
                    int32_t v{};
                    std::memcpy(&v, fields[i].payload.data(), 4);
                    std::cout << "INTEGER=" << v;
                }
                break;
            case DataType::BIGINT:
                if (fields[i].payload.size() == 8)
                {
                    int64_t v{};
                    std::memcpy(&v, fields[i].payload.data(), 8);
                    std::cout << "BIGINT=" << v;
                }
                break;
            case DataType::DOUBLE:
                if (fields[i].payload.size() == 8)
                {
                    double v{};
                    std::memcpy(&v, fields[i].payload.data(), 8);
                    std::cout << "DOUBLE=" << v;
                }
                break;
            case DataType::BOOLEAN:
                if (!fields[i].payload.empty())
                    std::cout << "BOOLEAN=" << (fields[i].payload[0] ? "true" : "false");
                break;
            case DataType::VARCHAR:
                std::cout << "VARCHAR='" << std::string(fields[i].payload.begin(), fields[i].payload.end()) << "'";
                break;
            default:
                std::cout << "type=" << static_cast<int>(fields[i].type) << ", bytes=" << fields[i].payload.size();
                break;
            }
            std::cout << "\n";
        }
        pm_->unpin(id, false);
    }

    void Repl::cmd_loglevel(const std::vector<std::string> &args)
    {
        if (args.size() < 2)
        {
            std::cout << "Usage: loglevel <DEBUG|INFO|WARN|ERROR|FATAL>\n";
            return;
        }
        const std::string lv = to_upper(args[1]);
        LogLevel level = LogLevel::INFO;
        if (lv == "DEBUG")
            level = LogLevel::DEBUG;
        else if (lv == "INFO")
            level = LogLevel::INFO;
        else if (lv == "WARN")
            level = LogLevel::WARN;
        else if (lv == "ERROR")
            level = LogLevel::ERROR;
        else if (lv == "FATAL")
            level = LogLevel::FATAL;
        Logger::instance().set_level(level);
        std::cout << "Log level set to " << lv << "\n";
    }

    void Repl::cmd_freepage(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: freepage <page_id>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        pm_->free_page(id);
        std::cout << "Freed page " << id << " (added to free list)\n";
    }

    void Repl::print_select_result(const engine::SelectResult &result) const
    {
        const std::size_t column_count = result.column_names.size();
        if (column_count == 0)
        {
            std::cout << "(no columns)\n";
            if (result.rows.empty())
                std::cout << "(no rows)\n";
            std::cout << "[rows=" << result.rows.size() << "]\n";
            return;
        }

        std::vector<std::vector<std::string>> sanitized_rows;
        std::vector<std::vector<bool>> is_null;
        sanitized_rows.reserve(result.rows.size());
        is_null.reserve(result.rows.size());

        for (const auto &row : result.rows)
        {
            sanitized_rows.emplace_back(column_count);
            is_null.emplace_back(column_count, false);
            for (std::size_t col = 0; col < column_count; ++col)
            {
                if (col < row.size())
                {
                    is_null.back()[col] = (row[col] == "NULL");
                    sanitized_rows.back()[col] = sanitize_cell_text(row[col]);
                }
                else
                {
                    sanitized_rows.back()[col].clear();
                }
            }
        }

        std::vector<std::string> headers(column_count);
        std::vector<std::size_t> widths(column_count, 1);
        for (std::size_t col = 0; col < column_count; ++col)
        {
            headers[col] = sanitize_cell_text(result.column_names[col]);
            std::size_t header_len = headers[col].size();
            widths[col] = std::min<std::size_t>(kMaxSelectColumnWidth, std::max<std::size_t>(widths[col], header_len));
        }

        for (std::size_t row_idx = 0; row_idx < sanitized_rows.size(); ++row_idx)
        {
            for (std::size_t col = 0; col < column_count; ++col)
            {
                std::size_t cell_len = is_null[row_idx][col] ? 4 : sanitized_rows[row_idx][col].size();
                widths[col] = std::min<std::size_t>(kMaxSelectColumnWidth, std::max<std::size_t>(widths[col], cell_len));
            }
        }

        for (auto &width : widths)
            width = std::max<std::size_t>(1, width);

        const std::string indent = "  ";
        const std::string gap = "  ";
        std::size_t separator_width = 0;
        for (std::size_t col = 0; col < column_count; ++col)
        {
            separator_width += widths[col];
        }
        if (column_count > 1)
            separator_width += gap.size() * (column_count - 1);

        std::cout << indent;
        for (std::size_t col = 0; col < column_count; ++col)
        {
            std::cout << pad_text(headers[col], widths[col], CellAlign::Left);
            if (col + 1 < column_count)
                std::cout << gap;
        }
        std::cout << "\n"
                  << indent << std::string(separator_width, '-') << "\n";

        for (std::size_t row_idx = 0; row_idx < sanitized_rows.size(); ++row_idx)
        {
            std::size_t max_lines = 1;
            std::vector<std::vector<std::string>> wrapped(column_count);
            for (std::size_t col = 0; col < column_count; ++col)
            {
                if (is_null[row_idx][col])
                {
                    wrapped[col] = {"NULL"};
                }
                else
                {
                    const auto &cell = sanitized_rows[row_idx][col];
                    wrapped[col] = cell.empty() ? std::vector<std::string>{""} : wrap_text(cell, widths[col]);
                }
                max_lines = std::max<std::size_t>(max_lines, wrapped[col].size());
            }

            for (std::size_t line = 0; line < max_lines; ++line)
            {
                std::cout << indent;
                for (std::size_t col = 0; col < column_count; ++col)
                {
                    std::string text;
                    CellAlign align = CellAlign::Left;
                    if (is_null[row_idx][col])
                    {
                        if (line == 0)
                        {
                            text = "NULL";
                            align = CellAlign::Center;
                        }
                    }
                    else if (line < wrapped[col].size())
                    {
                        text = wrapped[col][line];
                    }
                    std::cout << pad_text(text, widths[col], align);
                    if (col + 1 < column_count)
                        std::cout << gap;
                }
                std::cout << "\n";
            }
        }

        if (result.rows.empty())
            std::cout << "(no rows)\n";

        std::cout << "[rows=" << result.rows.size() << "]\n";
    }

    bool Repl::looks_like_sql(const std::string &line) const
    {
        auto trimmed = trim_copy(line);
        if (trimmed.empty())
            return false;
        if (trimmed.find(';') != std::string::npos)
            return true;

        std::istringstream iss(trimmed);
        std::string keyword;
        if (!(iss >> keyword))
            return false;
        std::string upper = to_upper(keyword);
        static const std::array<std::string, 7> sql_keywords = {"CREATE", "DROP", "ALTER", "TRUNCATE", "INSERT", "SELECT", "DELETE"};
        return std::find(sql_keywords.begin(), sql_keywords.end(), upper) != sql_keywords.end();
    }

    namespace
    {
        std::string format_duration_ms(double ms)
        {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(3) << ms;
            return oss.str();
        }
    }

    void Repl::dispatch_sql(const std::string &line)
    {
        if (!ensure_db_open() || !catalog_)
            return;
        auto trimmed = trim_copy(line);
        if (trimmed.empty())
            return;

        std::istringstream iss(trimmed);
        std::string keyword;
        if (!(iss >> keyword))
            return;
        std::string upper = to_upper(keyword);

        auto print_query_error = [&](const QueryException &e)
        {
            const char *code = status_code_to_string(e.code());
            std::cout << "SQL error [" << code << "] " << e.message();
            if (!e.context().empty())
                std::cout << " (" << e.context() << ")";
            std::cout << "\n";
        };
        auto print_engine_error = [&](const DBException &e)
        {
            const char *code = status_code_to_string(e.code());
            std::cout << "Engine error [" << code << "] " << e.message();
            if (!e.context().empty())
                std::cout << " (" << e.context() << ")";
            std::cout << "\n";
        };

        auto is_dml_keyword = [&](const std::string &kw)
        {
            return kw == "INSERT" || kw == "SELECT" || kw == "DELETE" || kw == "UPDATE" || kw == "TRUNCATE";
        };

        try
        {
            if (is_dml_keyword(upper))
            {
                if (!dml_executor_)
                {
                    std::cout << "DML executor not initialized (open a database first)\n";
                    return;
                }

                using Clock = std::chrono::steady_clock;
                if (upper == "SELECT")
                {
                    auto start = Clock::now();
                    auto stmt = sql::parse_select(trimmed);
                    auto result = dml_executor_->select(stmt);
                    double elapsed_ms =
                        std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                    print_select_result(result);
                    std::cout << "[time=" << format_duration_ms(elapsed_ms) << " ms]\n";
                }
                else if (upper == "DELETE")
                {
                    auto start = Clock::now();
                    auto stmt = sql::parse_delete(trimmed);
                    auto result = dml_executor_->delete_all(stmt);
                    double elapsed_ms =
                        std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                    std::cout << "[rows=" << result.rows_deleted << "] deleted [time="
                              << format_duration_ms(elapsed_ms) << " ms]\n";
                }
                else if (upper == "UPDATE")
                {
                    auto start = Clock::now();
                    auto stmt = sql::parse_update(trimmed);
                    auto result = dml_executor_->update_all(stmt);
                    double elapsed_ms =
                        std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                    std::cout << "[rows=" << result.rows_updated << "] updated [time="
                              << format_duration_ms(elapsed_ms) << " ms]\n";
                }
                else
                {
                    auto start = Clock::now();
                    std::string message = dml_executor_->execute(trimmed);
                    double elapsed_ms =
                        std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                    std::cout << message << " [time=" << format_duration_ms(elapsed_ms) << " ms]\n";
                }
                return;
            }

            if (!ddl_executor_)
            {
                std::cout << "DDL executor not initialized (open a database first)\n";
                return;
            }

            if (upper == "CREATE" || upper == "DROP" || upper == "ALTER")
            {
                auto start = std::chrono::steady_clock::now();
                std::string message = ddl_executor_->execute(trimmed);
                double elapsed_ms =
                    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() -
                                                              start)
                        .count();
                std::cout << message << " [time=" << format_duration_ms(elapsed_ms) << " ms]\n";
                return;
            }

            std::cout << "Unknown SQL command (try 'help')\n";
        }
        catch (const QueryException &e)
        {
            print_query_error(e);
        }
        catch (const DBException &e)
        {
            print_engine_error(e);
        }
        catch (const std::exception &e)
        {
            std::cout << "Unhandled std::exception: " << e.what() << "\n";
        }
    }
}
