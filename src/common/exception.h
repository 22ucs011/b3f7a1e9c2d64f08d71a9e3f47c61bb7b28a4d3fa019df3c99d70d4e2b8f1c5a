#ifndef KIZUNA_COMMON_EXCEPTION_H
#define KIZUNA_COMMON_EXCEPTION_H

#include <stdexcept>
#include <string>
#include <string_view>
#include <source_location>
#include <cstdint>
#include <cstddef>

namespace kizuna
{

    /**
     * @brief Status codes for database operations
     *
     * These codes provide specific error categorization for better
     * error handling and debugging. Each code maps to specific
     * exception types and recovery strategies.
     */
    enum class StatusCode : uint32_t
    {
        // Success
        OK = 0,

        // General errors (1-99)
        UNKNOWN_ERROR = 1,
        INVALID_ARGUMENT = 2,
        OUT_OF_MEMORY = 3,
        NOT_IMPLEMENTED = 4,
        INTERNAL_ERROR = 5,

        // I/O errors (100-199)
        IO_ERROR = 100,
        FILE_NOT_FOUND = 101,
        FILE_ALREADY_EXISTS = 102,
        PERMISSION_DENIED = 103,
        DISK_FULL = 104,
        READ_ERROR = 105,
        WRITE_ERROR = 106,
        SEEK_ERROR = 107,
        SYNC_ERROR = 108,
        FILE_CORRUPTED = 109,

        // Storage errors (200-299)
        PAGE_NOT_FOUND = 200,
        PAGE_CORRUPTED = 201,
        PAGE_FULL = 202,
        INVALID_PAGE_TYPE = 203,
        CACHE_FULL = 204,
        BUFFER_OVERFLOW = 205,
        INVALID_OFFSET = 206,
        PAGE_LOCKED = 207,

        // Record errors (300-399)
        RECORD_NOT_FOUND = 300,
        RECORD_TOO_LARGE = 301,
        INVALID_RECORD_FORMAT = 302,
        RECORD_CORRUPTED = 303,
        DUPLICATE_RECORD = 304,
        SCHEMA_MISMATCH = 305,

        // Index errors (400-499)
        INDEX_NOT_FOUND = 400,
        INDEX_CORRUPTED = 401,
        KEY_NOT_FOUND = 402,
        DUPLICATE_KEY = 403,
        INDEX_FULL = 404,
        INVALID_INDEX_TYPE = 405,

        // Transaction errors (500-599)
        TRANSACTION_ABORTED = 500,
        DEADLOCK_DETECTED = 501,
        LOCK_TIMEOUT = 502,
        ISOLATION_VIOLATION = 503,
        WRITE_CONFLICT = 504,

        // Query errors (600-699)
        SYNTAX_ERROR = 600,
        SEMANTIC_ERROR = 601,
        TYPE_ERROR = 602,
        TABLE_NOT_FOUND = 603,
        COLUMN_NOT_FOUND = 604,
        CONSTRAINT_VIOLATION = 605,
        DIVISION_BY_ZERO = 606,
        TABLE_EXISTS = 607,
        DUPLICATE_COLUMN = 608,
        INVALID_CONSTRAINT = 609,
        UNSUPPORTED_DATA_TYPE = 610,

        // Network/Connection errors (700-799)
        CONNECTION_FAILED = 700,
        CONNECTION_LOST = 701,
        TIMEOUT = 702,
        PROTOCOL_ERROR = 703
    };

    /**
     * @brief Convert status code to human-readable string
     */
    const char *status_code_to_string(StatusCode code) noexcept;

    /**
     * @brief Base exception class for all Kizuna database errors
     *
     * Provides structured error information including:
     * - Specific status codes for programmatic handling
     * - Context information for debugging
     * - Source location tracking (C++20 feature)
     * - Error categorization for recovery strategies
     */
    class DBException : public std::exception
    {
    public:
        /**
         * @brief Construct exception with status code and message
         *
         * @param code Status code indicating specific error type
         * @param message Human-readable error description
         * @param context Additional context (e.g., "in table 'users'")
         * @param location Source location where error occurred
         */
        explicit DBException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        /**
         * @brief Get the complete error message
         *
         * Format: "[STATUS_CODE] message (context) at file:line:function"
         */
        const char *what() const noexcept override
        {
            return full_message_.c_str();
        }

        /**
         * @brief Get the status code
         */
        StatusCode code() const noexcept { return code_; }

        /**
         * @brief Get the basic error message
         */
        const std::string &message() const noexcept { return message_; }

        /**
         * @brief Get additional context information
         */
        const std::string &context() const noexcept { return context_; }

        /**
         * @brief Get source location where error occurred
         */
        const std::source_location &location() const noexcept { return location_; }

        /**
         * @brief Check if this is a recoverable error
         *
         * Recoverable errors might succeed on retry or with different parameters.
         * Non-recoverable errors indicate bugs or permanent conditions.
         */
        bool is_recoverable() const noexcept;

        /**
         * @brief Check if this error category matches
         */
        bool is_io_error() const noexcept;
        bool is_storage_error() const noexcept;
        bool is_transaction_error() const noexcept;
        bool is_query_error() const noexcept;

    private:
        StatusCode code_;
        std::string message_;
        std::string context_;
        std::string full_message_;
        std::source_location location_;

        void build_full_message() noexcept;
    };

    /**
     * @brief I/O related exceptions
     *
     * Thrown for file system operations, disk I/O errors,
     * and other storage-related failures.
     */
    class IOException : public DBException
    {
    public:
        explicit IOException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common I/O errors
        static IOException file_not_found(
            std::string_view filename,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IOException permission_denied(
            std::string_view filename,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IOException disk_full(
            std::string_view path,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IOException read_error(
            std::string_view filename,
            size_t attempted_bytes,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IOException write_error(
            std::string_view filename,
            size_t attempted_bytes,
            const std::source_location &location = std::source_location::current()) noexcept;
    };

    /**
     * @brief Storage engine exceptions
     *
     * Thrown for page management errors, cache issues,
     * and storage layer problems.
     */
    class StorageException : public DBException
    {
    public:
        explicit StorageException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common storage errors
        static StorageException page_not_found(
            uint32_t page_id,
            const std::source_location &location = std::source_location::current()) noexcept;

        static StorageException page_corrupted(
            uint32_t page_id,
            std::string_view details = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        static StorageException cache_full(
            const std::source_location &location = std::source_location::current()) noexcept;

        static StorageException invalid_page_type(
            uint32_t page_id,
            uint8_t expected_type,
            uint8_t actual_type,
            const std::source_location &location = std::source_location::current()) noexcept;
    };

    /**
     * @brief Record format and data exceptions
     *
     * Thrown for record parsing errors, schema violations,
     * and data format issues.
     */
    class RecordException : public DBException
    {
    public:
        explicit RecordException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common record errors
        static RecordException too_large(
            size_t record_size,
            size_t max_size,
            const std::source_location &location = std::source_location::current()) noexcept;

        static RecordException invalid_format(
            std::string_view details,
            const std::source_location &location = std::source_location::current()) noexcept;

        static RecordException schema_mismatch(
            std::string_view expected,
            std::string_view actual,
            const std::source_location &location = std::source_location::current()) noexcept;
    };

    /**
     * @brief Transaction and concurrency exceptions
     *
     * Thrown for locking conflicts, deadlocks,
     * and transaction isolation violations.
     */
    class TransactionException : public DBException
    {
    public:
        explicit TransactionException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common transaction errors
        static TransactionException deadlock_detected(
            const std::source_location &location = std::source_location::current()) noexcept;

        static TransactionException lock_timeout(
            std::string_view resource,
            const std::source_location &location = std::source_location::current()) noexcept;

        static TransactionException write_conflict(
            std::string_view resource,
            const std::source_location &location = std::source_location::current()) noexcept;
    };

    /**
     * @brief Query parsing and execution exceptions
     *
     * Thrown for SQL syntax errors, semantic errors,
     * and query execution failures.
     */
    class QueryException : public DBException
    {
    public:
        explicit QueryException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common query errors
        static QueryException syntax_error(
            std::string_view query,
            size_t position,
            std::string_view expected = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException table_not_found(
            std::string_view table_name,
            std::string_view clause = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException column_not_found(
            std::string_view column_name,
            std::string_view table_name = "",
            std::string_view clause = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException ambiguous_column(
            std::string_view column_name,
            std::string_view clause = "",
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException type_error(
            std::string_view operation,
            std::string_view expected_type,
            std::string_view actual_type,
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException table_exists(
            std::string_view table_name,
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException duplicate_column(
            std::string_view column_name,
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException invalid_constraint(
            std::string_view details,
            const std::source_location &location = std::source_location::current()) noexcept;

        static QueryException unsupported_type(
            std::string_view type_name,
            const std::source_location &location = std::source_location::current()) noexcept;
    };

    /**
     * @brief Index management exceptions
     *
     * Thrown for B+ tree operations, index corruption,
     * and key constraint violations.
     */
    class IndexException : public DBException
    {
    public:
        explicit IndexException(
            StatusCode code,
            std::string_view message = "",
            std::string_view context = "",
            const std::source_location &location = std::source_location::current()) noexcept : DBException(code, message, context, location) {}

        // Convenience constructors for common index errors
        static IndexException duplicate_key(
            std::string_view key,
            std::string_view index_name,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IndexException key_not_found(
            std::string_view key,
            std::string_view index_name,
            const std::source_location &location = std::source_location::current()) noexcept;

        static IndexException corrupted(
            std::string_view index_name,
            std::string_view details = "",
            const std::source_location &location = std::source_location::current()) noexcept;
    };

/**
 * @brief Utility macros for exception throwing
 *
 * These macros automatically capture source location
 * and provide consistent error reporting.
 */
#define KIZUNA_THROW(exception_type, code, message, context) \
    throw exception_type(code, message, context)

#define KIZUNA_THROW_IO(code, message, context) \
    KIZUNA_THROW(IOException, code, message, context)

#define KIZUNA_THROW_STORAGE(code, message, context) \
    KIZUNA_THROW(StorageException, code, message, context)

#define KIZUNA_THROW_RECORD(code, message, context) \
    KIZUNA_THROW(RecordException, code, message, context)

#define KIZUNA_THROW_TRANSACTION(code, message, context) \
    KIZUNA_THROW(TransactionException, code, message, context)

#define KIZUNA_THROW_QUERY(code, message, context) \
    KIZUNA_THROW(QueryException, code, message, context)

#define KIZUNA_THROW_INDEX(code, message, context) \
    KIZUNA_THROW(IndexException, code, message, context)

    /**
     * @brief Result type for operations that can fail
     *
     * Provides a way to return either a value or an error
     * without using exceptions for expected failures.
     *
     * @tparam T The success value type
     */
    template <typename T>
    class Result
    {
    public:
        /**
         * @brief Construct successful result
         */
        explicit Result(T &&value) noexcept
            : has_value_(true), value_(std::forward<T>(value)) {}

        explicit Result(const T &value) noexcept
            : has_value_(true), value_(value) {}

        /**
         * @brief Construct error result
         */
        explicit Result(StatusCode error) noexcept
            : has_value_(false), error_(error) {}

        explicit Result(const DBException &exception) noexcept
            : has_value_(false), error_(exception.code()) {}

        /**
         * @brief Check if result contains a value
         */
        bool has_value() const noexcept { return has_value_; }
        bool is_ok() const noexcept { return has_value_; }
        bool is_error() const noexcept { return !has_value_; }

        /**
         * @brief Get the value (throws if error)
         */
        const T &value() const &
        {
            if (!has_value_)
            {
                throw DBException(error_, "Attempted to access error result");
            }
            return value_;
        }

        T &value() &
        {
            if (!has_value_)
            {
                throw DBException(error_, "Attempted to access error result");
            }
            return value_;
        }

        T &&value() &&
        {
            if (!has_value_)
            {
                throw DBException(error_, "Attempted to access error result");
            }
            return std::move(value_);
        }

        /**
         * @brief Get the value or default
         */
        T value_or(T &&default_value) const &
        {
            return has_value_ ? value_ : std::forward<T>(default_value);
        }

        T value_or(T &&default_value) &&
        {
            return has_value_ ? std::move(value_) : std::forward<T>(default_value);
        }

        /**
         * @brief Get the error code (only valid if is_error())
         */
        StatusCode error() const noexcept
        {
            return has_value_ ? StatusCode::OK : error_;
        }

        /**
         * @brief Implicit conversion to bool (true if has value)
         */
        explicit operator bool() const noexcept { return has_value_; }

    private:
        bool has_value_;
        union
        {
            T value_;
            StatusCode error_;
        };
    };

    /**
     * @brief Specialization for void results
     */
    template <>
    class Result<void>
    {
    public:
        /**
         * @brief Construct successful result
         */
        Result() noexcept : has_value_(true), error_(StatusCode::OK) {}

        /**
         * @brief Construct error result
         */
        explicit Result(StatusCode error) noexcept
            : has_value_(false), error_(error) {}

        explicit Result(const DBException &exception) noexcept
            : has_value_(false), error_(exception.code()) {}

        bool has_value() const noexcept { return has_value_; }
        bool is_ok() const noexcept { return has_value_; }
        bool is_error() const noexcept { return !has_value_; }

        StatusCode error() const noexcept
        {
            return has_value_ ? StatusCode::OK : error_;
        }

        explicit operator bool() const noexcept { return has_value_; }

    private:
        bool has_value_;
        StatusCode error_;
    };

} // namespace kizuna

#endif // KIZUNA_COMMON_EXCEPTION_H


