#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "common/path_utils.h"

namespace kizuna
{
    namespace config
    {

        // ==================== STORAGE CONFIGURATION ====================

        /// Size of each database page in bytes (4KB - standard page size)
        constexpr size_t PAGE_SIZE = 4096;

        /// Size of page header in bytes
        constexpr size_t PAGE_HEADER_SIZE = 24;

        /// Maximum size of a single record (page size - header - safety margin)
        constexpr size_t MAX_RECORD_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - 16;

        /// Maximum number of records per page (theoretical limit)
        constexpr size_t MAX_RECORDS_PER_PAGE = 65535; // uint16_t limit

        /// Default page cache size (number of pages to keep in memory)
        constexpr size_t DEFAULT_CACHE_SIZE = 100;

        /// Maximum page cache size
        constexpr size_t MAX_CACHE_SIZE = 10000;

        /// Page alignment for direct I/O (must be power of 2)
        constexpr size_t PAGE_ALIGNMENT = 4096;

        // ==================== DATABASE LIMITS ====================

        /// Maximum database size in pages (16TB with 4KB pages)
        constexpr uint32_t MAX_PAGES = UINT32_MAX;

        /// First valid page ID (0 is reserved as invalid)
        constexpr uint32_t FIRST_PAGE_ID = 1;

        /// Invalid page ID marker
        constexpr uint32_t INVALID_PAGE_ID = 0;

        /// Maximum database name length
        constexpr size_t MAX_DB_NAME_LENGTH = 255;

        /// Maximum table name length
        constexpr size_t MAX_TABLE_NAME_LENGTH = 255;

        /// Maximum column name length
        constexpr size_t MAX_COLUMN_NAME_LENGTH = 255;

        /// Maximum number of columns per table
        constexpr size_t MAX_COLUMNS_PER_TABLE = 1024;
        /// Maximum number of indexes per table
        constexpr size_t MAX_INDEXES_PER_TABLE = 64;
        /// Maximum number of columns per index key (initial scope keeps this modest)
        constexpr size_t MAX_COLUMNS_PER_INDEX = 4;

        /// Number of rows to process per batch when migrating table data for ALTER TABLE
        constexpr size_t ALTER_TABLE_MIGRATION_BATCH_SIZE = 256;

        /// Allow ALTER TABLE ADD COLUMN without explicit DEFAULT to fill existing rows with NULL
        constexpr bool ALTER_TABLE_ALLOW_IMPLICIT_NULL_FILL = true;

        // ==================== CATALOG CONFIGURATION ====================

        /// Catalog schema version (increment when layout changes)
        constexpr uint32_t CATALOG_SCHEMA_VERSION = 3;

        /// Internal catalog table names (modeled after SQLite's sqlite_master)
        constexpr const char *CATALOG_TABLES_NAME = "__tables__";
        constexpr const char *CATALOG_COLUMNS_NAME = "__columns__";
        constexpr const char *CATALOG_INDEXES_NAME = "__indexes__";

        /// Default on-disk naming for table storage files
        constexpr const char *TABLE_FILE_PREFIX = "table_";
        constexpr const char *TABLE_FILE_EXTENSION = ".db";
        constexpr const char *INDEX_FILE_PREFIX = "index_";
        constexpr const char *INDEX_FILE_EXTENSION = ".idx";

        // ==================== TRANSACTION CONFIGURATION ====================

        /// Maximum number of concurrent transactions
        constexpr uint32_t MAX_CONCURRENT_TRANSACTIONS = 1000;

        /// Transaction timeout in milliseconds
        constexpr uint32_t TRANSACTION_TIMEOUT_MS = 30000; // 30 seconds

        /// Lock timeout in milliseconds
        constexpr uint32_t LOCK_TIMEOUT_MS = 5000; // 5 seconds

        /// Maximum transaction log size in MB
        constexpr size_t MAX_WAL_SIZE_MB = 100;

        // ==================== LOGGING CONFIGURATION ====================

        /// Default log file name
        inline const std::filesystem::path &database_root_dir()
        {
            static const std::filesystem::path path = path_utils::executable_dir() / "database";
            return path;
        }

        inline const std::filesystem::path &catalog_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "catalog";
            return path;
        }

        inline const std::filesystem::path &default_db_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "data";
            return path;
        }

        inline const std::filesystem::path &default_index_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "indexes";
            return path;
        }

        inline const std::filesystem::path &logs_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "logs";
            return path;
        }

        inline const std::filesystem::path &temp_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "temp";
            return path;
        }

        inline const std::filesystem::path &backup_dir()
        {
            static const std::filesystem::path path = database_root_dir() / "backup";
            return path;
        }

        inline std::filesystem::path default_log_file()
        {
            return logs_dir() / "kizuna.log";
        }

        /// Maximum log file size in MB before rotation
        constexpr size_t MAX_LOG_FILE_SIZE_MB = 10;

        /// Number of log files to keep in rotation
        constexpr size_t MAX_LOG_FILES = 5;

        /// Log buffer size in bytes
        constexpr size_t LOG_BUFFER_SIZE = 8192;

        // ==================== I/O CONFIGURATION ====================

        /// Read buffer size for file operations
        constexpr size_t READ_BUFFER_SIZE = 64 * 1024; // 64KB

        /// Write buffer size for file operations
        constexpr size_t WRITE_BUFFER_SIZE = 64 * 1024; // 64KB

        /// Number of I/O threads for async operations
        constexpr size_t IO_THREAD_COUNT = 4;

        /// Sync frequency - pages written before forcing sync
        constexpr size_t SYNC_FREQUENCY = 100;

        // ==================== B+ TREE CONFIGURATION ====================

        /// Default B+ tree node size (should fit in one page)
        constexpr size_t BTREE_NODE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

        /// Minimum number of keys per B+ tree node (except root)
        constexpr size_t BTREE_MIN_KEYS = 64;

        /// Maximum number of keys per B+ tree node
        constexpr size_t BTREE_MAX_KEYS = 256;

        /// Maximum key length for B+ tree indexes
        constexpr size_t MAX_KEY_LENGTH = 255;

        // ==================== STRING CONFIGURATION ====================

        /// Maximum VARCHAR length
        constexpr size_t MAX_VARCHAR_LENGTH = 65535;

        /// Maximum TEXT field length (stored in overflow pages)
        constexpr size_t MAX_TEXT_LENGTH = 1024 * 1024; // 1MB

        /// Default string encoding (UTF-8)
        constexpr const char *DEFAULT_ENCODING = "UTF-8";

        // ==================== PERFORMANCE TUNING ====================

        /// Enable direct I/O for better performance (bypasses OS cache)
        constexpr bool ENABLE_DIRECT_IO = false; // Disabled by default for portability

        /// Enable memory-mapped files for read operations
        constexpr bool ENABLE_MMAP = true;

        /// Prefetch window size for sequential scans (pages to read ahead)
        constexpr size_t PREFETCH_WINDOW_SIZE = 8;

        /// Checkpoint frequency - transactions between checkpoints
        constexpr uint32_t CHECKPOINT_FREQUENCY = 1000;

// ==================== DEBUGGING CONFIGURATION ====================

/// Enable debug mode (extra validation, slower performance)
#ifdef NDEBUG
        constexpr bool DEBUG_MODE = false;
#else
        constexpr bool DEBUG_MODE = true;
#endif

        /// Enable page checksum validation
        constexpr bool ENABLE_PAGE_CHECKSUMS = true;

        /// Enable memory debugging (track allocations)
        constexpr bool ENABLE_MEMORY_DEBUG = DEBUG_MODE;

        /// Enable detailed query tracing
        constexpr bool ENABLE_QUERY_TRACING = DEBUG_MODE;

        // ==================== FILE PATHS ====================

        /// Default database file extension
        constexpr const char *DB_FILE_EXTENSION = ".kz";

        /// Default database directory
        /// Lock file extension
        constexpr const char *LOCK_FILE_EXTENSION = ".lock";

        // ==================== VALIDATION HELPERS ====================

        /// Validate that a page size is reasonable
        constexpr bool is_valid_page_size(size_t size)
        {
            return size >= 512 && size <= 65536 && (size & (size - 1)) == 0; // Power of 2
        }

        /// Validate that a cache size is reasonable
        constexpr bool is_valid_cache_size(size_t size)
        {
            return size > 0 && size <= MAX_CACHE_SIZE;
        }

        /// Calculate number of records that can fit in a page
        constexpr size_t calculate_max_records_per_page(size_t record_size)
        {
            if (record_size == 0)
                return 0;
            size_t available_space = PAGE_SIZE - PAGE_HEADER_SIZE;
            return available_space / (record_size + sizeof(uint16_t)); // +2 for slot directory
        }

        /// Calculate optimal cache size based on available memory
        constexpr size_t calculate_optimal_cache_size(size_t available_memory_mb)
        {
            size_t cache_memory = available_memory_mb * 1024 * 1024 / 4; // Use 1/4 of available memory
            size_t optimal_size = cache_memory / PAGE_SIZE;
            return optimal_size > MAX_CACHE_SIZE ? MAX_CACHE_SIZE : optimal_size < DEFAULT_CACHE_SIZE ? DEFAULT_CACHE_SIZE
                                                                                                      : optimal_size;
        }

        // ==================== COMPILE-TIME ASSERTIONS ====================

        // Ensure our configuration makes sense
        static_assert(PAGE_SIZE >= 512, "Page size too small");
        static_assert(PAGE_SIZE <= 65536, "Page size too large");
        static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0, "Page size must be power of 2");
        static_assert(PAGE_HEADER_SIZE < PAGE_SIZE / 2, "Page header too large");
        static_assert(MAX_RECORD_SIZE > 0, "Max record size must be positive");
        static_assert(DEFAULT_CACHE_SIZE > 0, "Cache size must be positive");
        static_assert(FIRST_PAGE_ID > INVALID_PAGE_ID, "First page ID must be greater than invalid");

    } // namespace config
} // namespace kizuna


