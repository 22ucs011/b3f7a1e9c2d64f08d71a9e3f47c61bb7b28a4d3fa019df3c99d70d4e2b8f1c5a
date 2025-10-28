// Implementation for Kizuna exceptions and status helpers

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>

#include "common/exception.h"

namespace kizuna
{

    // --------------------- status_code_to_string ---------------------
    const char *status_code_to_string(StatusCode code) noexcept
    {
        switch (code)
        {
        // Success
        case StatusCode::OK: return "OK";

        // General
        case StatusCode::UNKNOWN_ERROR: return "UNKNOWN_ERROR";
        case StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case StatusCode::OUT_OF_MEMORY: return "OUT_OF_MEMORY";
        case StatusCode::NOT_IMPLEMENTED: return "NOT_IMPLEMENTED";
        case StatusCode::INTERNAL_ERROR: return "INTERNAL_ERROR";

        // I/O
        case StatusCode::IO_ERROR: return "IO_ERROR";
        case StatusCode::FILE_NOT_FOUND: return "FILE_NOT_FOUND";
        case StatusCode::FILE_ALREADY_EXISTS: return "FILE_ALREADY_EXISTS";
        case StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
        case StatusCode::DISK_FULL: return "DISK_FULL";
        case StatusCode::READ_ERROR: return "READ_ERROR";
        case StatusCode::WRITE_ERROR: return "WRITE_ERROR";
        case StatusCode::SEEK_ERROR: return "SEEK_ERROR";
        case StatusCode::SYNC_ERROR: return "SYNC_ERROR";
        case StatusCode::FILE_CORRUPTED: return "FILE_CORRUPTED";

        // Storage
        case StatusCode::PAGE_NOT_FOUND: return "PAGE_NOT_FOUND";
        case StatusCode::PAGE_CORRUPTED: return "PAGE_CORRUPTED";
        case StatusCode::PAGE_FULL: return "PAGE_FULL";
        case StatusCode::INVALID_PAGE_TYPE: return "INVALID_PAGE_TYPE";
        case StatusCode::CACHE_FULL: return "CACHE_FULL";
        case StatusCode::BUFFER_OVERFLOW: return "BUFFER_OVERFLOW";
        case StatusCode::INVALID_OFFSET: return "INVALID_OFFSET";
        case StatusCode::PAGE_LOCKED: return "PAGE_LOCKED";

        // Record
        case StatusCode::RECORD_NOT_FOUND: return "RECORD_NOT_FOUND";
        case StatusCode::RECORD_TOO_LARGE: return "RECORD_TOO_LARGE";
        case StatusCode::INVALID_RECORD_FORMAT: return "INVALID_RECORD_FORMAT";
        case StatusCode::RECORD_CORRUPTED: return "RECORD_CORRUPTED";
        case StatusCode::DUPLICATE_RECORD: return "DUPLICATE_RECORD";
        case StatusCode::SCHEMA_MISMATCH: return "SCHEMA_MISMATCH";

        // Index
        case StatusCode::INDEX_NOT_FOUND: return "INDEX_NOT_FOUND";
        case StatusCode::INDEX_CORRUPTED: return "INDEX_CORRUPTED";
        case StatusCode::KEY_NOT_FOUND: return "KEY_NOT_FOUND";
        case StatusCode::DUPLICATE_KEY: return "DUPLICATE_KEY";
        case StatusCode::INDEX_FULL: return "INDEX_FULL";
        case StatusCode::INVALID_INDEX_TYPE: return "INVALID_INDEX_TYPE";

        // Transaction
        case StatusCode::TRANSACTION_ABORTED: return "TRANSACTION_ABORTED";
        case StatusCode::DEADLOCK_DETECTED: return "DEADLOCK_DETECTED";
        case StatusCode::LOCK_TIMEOUT: return "LOCK_TIMEOUT";
        case StatusCode::ISOLATION_VIOLATION: return "ISOLATION_VIOLATION";
        case StatusCode::WRITE_CONFLICT: return "WRITE_CONFLICT";

        // Query
        case StatusCode::SYNTAX_ERROR: return "SYNTAX_ERROR";
        case StatusCode::SEMANTIC_ERROR: return "SEMANTIC_ERROR";
        case StatusCode::TYPE_ERROR: return "TYPE_ERROR";
        case StatusCode::TABLE_NOT_FOUND: return "TABLE_NOT_FOUND";
        case StatusCode::COLUMN_NOT_FOUND: return "COLUMN_NOT_FOUND";
        case StatusCode::CONSTRAINT_VIOLATION: return "CONSTRAINT_VIOLATION";
        case StatusCode::DIVISION_BY_ZERO: return "DIVISION_BY_ZERO";
        case StatusCode::TABLE_EXISTS: return "TABLE_EXISTS";
        case StatusCode::DUPLICATE_COLUMN: return "DUPLICATE_COLUMN";
        case StatusCode::INVALID_CONSTRAINT: return "INVALID_CONSTRAINT";
        case StatusCode::UNSUPPORTED_DATA_TYPE: return "UNSUPPORTED_DATA_TYPE";

        // Network
        case StatusCode::CONNECTION_FAILED: return "CONNECTION_FAILED";
        case StatusCode::CONNECTION_LOST: return "CONNECTION_LOST";
        case StatusCode::TIMEOUT: return "TIMEOUT";
        case StatusCode::PROTOCOL_ERROR: return "PROTOCOL_ERROR";

        default: return "UNKNOWN_STATUS";
        }
    }

    // --------------------------- DBException ---------------------------
    DBException::DBException(
        StatusCode code,
        std::string_view message,
        std::string_view context,
        const std::source_location &location) noexcept
        : code_(code), message_(message), context_(context), full_message_(), location_(location)
    {
        build_full_message();
    }

    void DBException::build_full_message() noexcept
    {
        std::ostringstream oss;
        oss << '[' << status_code_to_string(code_) << "] ";
        if (!message_.empty())
        {
            oss << message_;
        }
        if (!context_.empty())
        {
            if (!message_.empty()) oss << ' ';
            oss << '(' << context_ << ')';
        }
        // Append source location
        oss << " at " << location_.file_name() << ':' << location_.line() << ':' << location_.function_name();
        full_message_ = oss.str();
    }

    bool DBException::is_recoverable() const noexcept
    {
        switch (code_)
        {
        case StatusCode::TIMEOUT:
        case StatusCode::LOCK_TIMEOUT:
        case StatusCode::DEADLOCK_DETECTED:
        case StatusCode::CACHE_FULL:
        case StatusCode::PAGE_FULL:
        case StatusCode::FILE_ALREADY_EXISTS:
            return true;
        default:
            return false;
        }
    }

    static inline bool in_range(StatusCode c, uint32_t lo, uint32_t hi)
    {
        const auto v = static_cast<uint32_t>(c);
        return v >= lo && v <= hi;
    }

    bool DBException::is_io_error() const noexcept
    {
        return in_range(code_, 100, 199);
    }

    bool DBException::is_storage_error() const noexcept
    {
        return in_range(code_, 200, 299);
    }

    bool DBException::is_transaction_error() const noexcept
    {
        return in_range(code_, 500, 599);
    }

    bool DBException::is_query_error() const noexcept
    {
        return in_range(code_, 600, 699);
    }

    // --------------------------- IOException ---------------------------
    IOException IOException::file_not_found(std::string_view filename, const std::source_location &location) noexcept
    {
        return IOException(StatusCode::FILE_NOT_FOUND, "File not found", filename, location);
    }

    IOException IOException::permission_denied(std::string_view filename, const std::source_location &location) noexcept
    {
        return IOException(StatusCode::PERMISSION_DENIED, "Permission denied", filename, location);
    }

    IOException IOException::disk_full(std::string_view path, const std::source_location &location) noexcept
    {
        return IOException(StatusCode::DISK_FULL, "Disk full", path, location);
    }

    IOException IOException::read_error(std::string_view filename, size_t attempted_bytes, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << filename << ": attempted " << attempted_bytes << " bytes";
        return IOException(StatusCode::READ_ERROR, "Read error", ctx.str(), location);
    }

    IOException IOException::write_error(std::string_view filename, size_t attempted_bytes, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << filename << ": attempted " << attempted_bytes << " bytes";
        return IOException(StatusCode::WRITE_ERROR, "Write error", ctx.str(), location);
    }

    // ------------------------- StorageException ------------------------
    StorageException StorageException::page_not_found(uint32_t page_id, const std::source_location &location) noexcept
    {
        return StorageException(StatusCode::PAGE_NOT_FOUND, "Page not found", std::to_string(page_id), location);
    }

    StorageException StorageException::page_corrupted(uint32_t page_id, std::string_view details, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << "page " << page_id;
        if (!details.empty()) ctx << ": " << details;
        return StorageException(StatusCode::PAGE_CORRUPTED, "Page corrupted", ctx.str(), location);
    }

    StorageException StorageException::cache_full(const std::source_location &location) noexcept
    {
        return StorageException(StatusCode::CACHE_FULL, "Cache full", "", location);
    }

    StorageException StorageException::invalid_page_type(uint32_t page_id, uint8_t expected_type, uint8_t actual_type, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << "page " << page_id << ": expected " << static_cast<int>(expected_type) << ", actual " << static_cast<int>(actual_type);
        return StorageException(StatusCode::INVALID_PAGE_TYPE, "Invalid page type", ctx.str(), location);
    }

    // ------------------------- RecordException -------------------------
    RecordException RecordException::too_large(size_t record_size, size_t max_size, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << record_size << "/" << max_size;
        return RecordException(StatusCode::RECORD_TOO_LARGE, "Record too large", ctx.str(), location);
    }

    RecordException RecordException::invalid_format(std::string_view details, const std::source_location &location) noexcept
    {
        return RecordException(StatusCode::INVALID_RECORD_FORMAT, "Invalid record format", details, location);
    }

    RecordException RecordException::schema_mismatch(std::string_view expected, std::string_view actual, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << "expected " << expected << ", actual " << actual;
        return RecordException(StatusCode::SCHEMA_MISMATCH, "Schema mismatch", ctx.str(), location);
    }

    // ---------------------- TransactionException ----------------------
    TransactionException TransactionException::deadlock_detected(const std::source_location &location) noexcept
    {
        return TransactionException(StatusCode::DEADLOCK_DETECTED, "Deadlock detected", "", location);
    }

    TransactionException TransactionException::lock_timeout(std::string_view resource, const std::source_location &location) noexcept
    {
        return TransactionException(StatusCode::LOCK_TIMEOUT, "Lock timeout", resource, location);
    }

    TransactionException TransactionException::write_conflict(std::string_view resource, const std::source_location &location) noexcept
    {
        return TransactionException(StatusCode::WRITE_CONFLICT, "Write conflict", resource, location);
    }

    // --------------------------- QueryException ------------------------
    QueryException QueryException::syntax_error(std::string_view query, size_t position, std::string_view expected, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << "pos " << position;
        if (!expected.empty()) ctx << ", expected " << expected;
        if (!query.empty())
        {
            constexpr size_t context_window = 24;
            const size_t clamped_pos = std::min(position, query.size());
            const size_t start = (clamped_pos > context_window) ? clamped_pos - context_window : 0;
            const size_t end = std::min(query.size(), clamped_pos + context_window);
            std::string snippet(query.substr(start, end - start));
            for (char &ch : snippet)
            {
                if (ch == '\n' || ch == '\r') ch = ' ';
            }
            if (!snippet.empty())
            {
                ctx << ", near \"" << snippet << '\"';
            }
        }
        return QueryException(StatusCode::SYNTAX_ERROR, "Syntax error", ctx.str(), location);
    }

    QueryException QueryException::table_not_found(std::string_view table_name,
                                                   std::string_view clause,
                                                   const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        if (!clause.empty())
            ctx << clause << ": ";
        ctx << table_name;
        return QueryException(StatusCode::TABLE_NOT_FOUND, "Table not found", ctx.str(), location);
    }

    QueryException QueryException::column_not_found(std::string_view column_name,
                                                    std::string_view table_name,
                                                    std::string_view clause,
                                                    const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        if (!clause.empty())
            ctx << clause << ": ";
        if (!table_name.empty())
            ctx << table_name << '.';
        ctx << column_name;
        return QueryException(StatusCode::COLUMN_NOT_FOUND, "Column not found", ctx.str(), location);
    }

    QueryException QueryException::ambiguous_column(std::string_view column_name,
                                                    std::string_view clause,
                                                    const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        if (!clause.empty())
            ctx << clause << ": ";
        ctx << column_name;
        return QueryException(StatusCode::SEMANTIC_ERROR, "Ambiguous column reference", ctx.str(), location);
    }

    QueryException QueryException::type_error(std::string_view operation, std::string_view expected_type, std::string_view actual_type, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << operation << ": expected " << expected_type << ", actual " << actual_type;
        return QueryException(StatusCode::TYPE_ERROR, "Type error", ctx.str(), location);
    }
    QueryException QueryException::table_exists(std::string_view table_name, const std::source_location &location) noexcept
    {
        return QueryException(StatusCode::TABLE_EXISTS, "Table already exists", table_name, location);
    }

    QueryException QueryException::duplicate_column(std::string_view column_name, const std::source_location &location) noexcept
    {
        return QueryException(StatusCode::DUPLICATE_COLUMN, "Duplicate column", column_name, location);
    }

    QueryException QueryException::invalid_constraint(std::string_view details, const std::source_location &location) noexcept
    {
        return QueryException(StatusCode::INVALID_CONSTRAINT, "Invalid constraint", details, location);
    }

    QueryException QueryException::unsupported_type(std::string_view type_name, const std::source_location &location) noexcept
    {
        return QueryException(StatusCode::UNSUPPORTED_DATA_TYPE, "Unsupported data type", type_name, location);
    }

    // --------------------------- IndexException ------------------------
    IndexException IndexException::duplicate_key(std::string_view key, std::string_view index_name, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << index_name << ": key=" << key;
        return IndexException(StatusCode::DUPLICATE_KEY, "Duplicate key", ctx.str(), location);
    }

    IndexException IndexException::key_not_found(std::string_view key, std::string_view index_name, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << index_name << ": key=" << key;
        return IndexException(StatusCode::KEY_NOT_FOUND, "Key not found", ctx.str(), location);
    }

    IndexException IndexException::corrupted(std::string_view index_name, std::string_view details, const std::source_location &location) noexcept
    {
        std::ostringstream ctx;
        ctx << index_name;
        if (!details.empty()) ctx << ": " << details;
        return IndexException(StatusCode::INDEX_CORRUPTED, "Index corrupted", ctx.str(), location);
    }

} // namespace kizuna





