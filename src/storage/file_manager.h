#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"

namespace kizuna
{
    // Minimal file manager for fixed-size page I/O.
    class FileManager
    {
    public:
        explicit FileManager(std::string path, bool create_if_missing = true);
        ~FileManager();

        // Non-copyable, non-movable for simplicity in v0.1
        FileManager(const FileManager &) = delete;
        FileManager &operator=(const FileManager &) = delete;

        void open();
        void close() noexcept;
        bool is_open() const noexcept { return file_.is_open(); }

        const std::string &path() const noexcept { return path_; }

        // File stats
        std::uint64_t size_bytes() const;                // total file size
        std::uint64_t page_count() const { return size_bytes() / config::PAGE_SIZE; }

        // Table file helpers for catalog-managed storage
        static std::string table_filename(table_id_t table_id);
        static std::filesystem::path table_path(table_id_t table_id, std::filesystem::path directory = config::default_db_dir());

        static std::string index_filename(index_id_t index_id);
        static std::filesystem::path index_path(index_id_t index_id, std::filesystem::path directory = config::default_index_dir());
        static bool exists(const std::filesystem::path &path) noexcept;
        static bool remove_file(const std::filesystem::path &path);


        // Page I/O
        void read_page(page_id_t page_id, std::uint8_t *out_buffer, std::size_t len = config::PAGE_SIZE);
        void write_page(page_id_t page_id, const std::uint8_t *buffer, std::size_t len = config::PAGE_SIZE);

        // Allocate a new page (zero-filled) and return its page id
        page_id_t allocate_page();

    private:
        std::string path_;
        bool create_if_missing_;
        mutable std::fstream file_;

        static std::uint64_t page_offset(page_id_t page_id)
        {
            // Pages are 1-based externally; on disk, page 1 starts at offset 0.
            return static_cast<std::uint64_t>(page_id - 1) * static_cast<std::uint64_t>(config::PAGE_SIZE);
        }

        void ensure_open_for_rw();
    };
}

