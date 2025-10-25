#pragma once

// Standard library headers
#include <cstdint>
#include <cstddef>
#include <string>
#include <memory>
#include <type_traits>
#include <vector>

namespace kizuna
{

    // TYPE ALIASES
    using page_id_t = uint32_t;   // ~4B pages x 4KB = 16 TB max database size
    using record_id_t = uint64_t; // high 32 page_id, low 32 slot_id
    using slot_id_t = uint16_t;   // 65k records per page
    using offset_t = uint16_t;    // offset within page
    using txn_id_t = uint32_t;    // id for MVCC
    using timestamp_t = uint64_t; // transaction timestamp

    using catalog_id_t = uint32_t; // catalog tables stay tiny, 32-bit ids work great
    using table_id_t = uint32_t;   // mirrors SQLite root page ids for tables
    using column_id_t = uint32_t;  // one entry per column in __columns__
    using index_id_t = uint32_t;   // future-proofing for __indexes__ catalog

    // ENUMS
    enum class PageType : uint8_t
    {
        INVALID = 0,    // Uninitialized page
        DATA = 1,       // Contains table records
        INDEX = 2,      // B+ tree index page
        OVERFLOW_PAGE = 3, // For records too large for one page
        METADATA = 4,   // Database metadata (catalog)
        FREE = 5        // Page is in the free list
    };

    enum class RecordType : uint8_t
    {
        FIXED_LENGTH = 0,    // All fields have fixed size
        VARIABLE_LENGTH = 1, // Has VARCHAR or TEXT fields
        DELETED = 2,         // Tombstone for deleted record
        OVERFLOW_PAGE = 3    // Points to overflow page
    };

    enum class LogLevel : uint8_t
    {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3,
        FATAL = 4
    };

    enum class DataType : uint8_t
    {
        NULL_TYPE = 0,
        BOOLEAN = 1,   // 1 byte: 0 or 1
        INTEGER = 2,   // 4 bytes: -2^31 to 2^31-1
        BIGINT = 3,    // 8 bytes: -2^63 to 2^63-1
        FLOAT = 4,     // 4 bytes: IEEE 754 single precision
        DOUBLE = 5,    // 8 bytes: IEEE 754 double precision
        VARCHAR = 6,   // Variable length string
        TEXT = 7,      // Large text (stored in overflow)
        DATE = 8,      // 8 bytes: date without time
        TIMESTAMP = 9, // 8 bytes: date with time
        BLOB = 10      // Binary large object
    };

    enum class ConstraintType : uint8_t
    {
        NONE = 0,
        NOT_NULL = 1,
        PRIMARY_KEY = 2,
        UNIQUE = 3,
        DEFAULT_VALUE = 4
    };

    struct ColumnConstraint
    {
        bool not_null{false};
        bool primary_key{false};
        bool unique{false};
        bool has_default{false};
        std::string default_value; // literal text, same as SQLite catalog keeps it
    };

    struct ColumnDef
    {
        column_id_t id{0};
        std::string name;           // user-provided identifier
        DataType type{DataType::NULL_TYPE};
        uint32_t length{0};         // VARCHAR(n) -> n, ignored for fixed-size types
        ColumnConstraint constraint{};
    };

    struct TableDef
    {
        table_id_t id{0};
        uint32_t schema_version{1};
        column_id_t next_column_id{1};
        std::string name;                 // table name in CREATE TABLE
        std::vector<ColumnDef> columns;   // column order defines row layout
    };

    enum class TransactionState : uint8_t
    {
        ACTIVE = 0,    // Transaction running
        COMMITTED = 1, // Transaction committed
        ABORTED = 2    // Transaction aborted - rolled back
    };

    enum class LockType : uint8_t
    {
        SHARED = 0,   // Shared/read lock      - multi
        EXCLUSIVE = 1 // Exclusive/write lock  - single
    };

    // UTILITY STRUCTS
    template <typename T>
    struct Optional
    {
        bool has_value;
        T value;

        Optional() : has_value(false), value() {}
        Optional(const T &val) : has_value(true), value(val) {}
    };

    // FORWARD DECLARATIONS
    class Page;
    class PageManager;
    class FileManager;
    class Record;
    class Table;
    class Database;
    class Transaction;
    class BTreeNode;

    // TYPE TRAITS / HELPERS
    template <typename T>
    constexpr bool is_numeric_type()
    {
        return std::is_arithmetic_v<T> && !std::is_same_v<T, bool>;
    }

    constexpr size_t get_type_size(DataType type)
    {
        switch (type)
        {
        case DataType::NULL_TYPE:
            return 0;
        case DataType::BOOLEAN:
            return 1;
        case DataType::INTEGER:
            return 4;
        case DataType::BIGINT:
            return 8;
        case DataType::FLOAT:
            return 4;
        case DataType::DOUBLE:
            return 8;
        case DataType::DATE:
            return 8;
        case DataType::TIMESTAMP:
            return 8;
        default:
            return 0; // Variable length types
        }
    }

} // namespace kizuna

