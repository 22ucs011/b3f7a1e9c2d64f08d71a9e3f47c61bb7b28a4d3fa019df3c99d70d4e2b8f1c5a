#include "storage/file_manager.h"

#include <algorithm>
#include <cstring>

namespace fs = std::filesystem;

namespace kizuna
{
    FileManager::FileManager(std::string path, bool create_if_missing)
        : path_(std::move(path)), create_if_missing_(create_if_missing)
    {
    }

    FileManager::~FileManager()
    {
        close();
    }

    void FileManager::open()
    {
        // Create parent dir if requested and not present
        fs::path p(path_);
        if (create_if_missing_)
        {
            std::error_code ec;
            if (p.has_parent_path() && !p.parent_path().empty())
            {
                fs::create_directories(p.parent_path(), ec);
            }
        }

        // Try opening read/write, create if needed
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open())
        {
            if (!create_if_missing_)
            {
                KIZUNA_THROW_IO(StatusCode::FILE_NOT_FOUND, "Failed to open database file", path_);
            }

            // Create new file
            std::ofstream create(path_, std::ios::out | std::ios::binary | std::ios::trunc);
            if (!create)
            {
                KIZUNA_THROW_IO(StatusCode::IO_ERROR, "Failed to create database file", path_);
            }
            create.close();

            // Reopen RW
            file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
            if (!file_.is_open())
            {
                KIZUNA_THROW_IO(StatusCode::IO_ERROR, "Failed to reopen created database file", path_);
            }
        }
    }

    void FileManager::close() noexcept
    {
        if (file_.is_open())
        {
            file_.flush();
            file_.close();
        }
    }

    void FileManager::ensure_open_for_rw()
    {
        if (!file_.is_open())
        {
            KIZUNA_THROW_IO(StatusCode::IO_ERROR, "File is not open", path_);
        }
    }

    std::uint64_t FileManager::size_bytes() const
    {
        std::error_code ec;
        const auto sz = fs::file_size(path_, ec);
        if (ec)
        {
            KIZUNA_THROW_IO(StatusCode::IO_ERROR, "Failed to get file size", path_);
        }
        return sz;
    }

    void FileManager::read_page(page_id_t page_id, std::uint8_t *out_buffer, std::size_t len)
    {
        if (out_buffer == nullptr)
        {
            KIZUNA_THROW_IO(StatusCode::INVALID_ARGUMENT, "Null output buffer", "read_page");
        }
        if (len != config::PAGE_SIZE)
        {
            KIZUNA_THROW_IO(StatusCode::INVALID_ARGUMENT, "Invalid read length (must be PAGE_SIZE)", std::to_string(len));
        }
        if (page_id < config::FIRST_PAGE_ID)
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_NOT_FOUND, "Invalid page id", std::to_string(page_id));
        }

        ensure_open_for_rw();

        const std::uint64_t off = page_offset(page_id);
        const auto file_sz = size_bytes();
        if (off + len > file_sz)
        {
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_NOT_FOUND, "Page beyond EOF", std::to_string(page_id));
        }

        file_.seekg(static_cast<std::streamoff>(off), std::ios::beg);
        if (!file_)
        {
            KIZUNA_THROW_IO(StatusCode::SEEK_ERROR, "Failed to seek for read", std::to_string(off));
        }
        file_.read(reinterpret_cast<char *>(out_buffer), static_cast<std::streamsize>(len));
        if (file_.gcount() != static_cast<std::streamsize>(len))
        {
            KIZUNA_THROW_IO(StatusCode::READ_ERROR, "Short read", std::to_string(len));
        }
    }

    void FileManager::write_page(page_id_t page_id, const std::uint8_t *buffer, std::size_t len)
    {
        if (buffer == nullptr)
        {
            KIZUNA_THROW_IO(StatusCode::INVALID_ARGUMENT, "Null input buffer", "write_page");
        }
        if (len != config::PAGE_SIZE)
        {
            KIZUNA_THROW_IO(StatusCode::INVALID_ARGUMENT, "Invalid write length (must be PAGE_SIZE)", std::to_string(len));
        }
        if (page_id < config::FIRST_PAGE_ID)
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_OFFSET, "Invalid page id for write", std::to_string(page_id));
        }

        ensure_open_for_rw();

        const std::uint64_t off = page_offset(page_id);

        file_.seekp(static_cast<std::streamoff>(off), std::ios::beg);
        if (!file_)
        {
            KIZUNA_THROW_IO(StatusCode::SEEK_ERROR, "Failed to seek for write", std::to_string(off));
        }

        file_.write(reinterpret_cast<const char *>(buffer), static_cast<std::streamsize>(len));
        if (!file_)
        {
            KIZUNA_THROW_IO(StatusCode::WRITE_ERROR, "Failed to write page", std::to_string(page_id));
        }
        file_.flush();
    }

    page_id_t FileManager::allocate_page()
    {
        ensure_open_for_rw();

        const std::uint64_t next_id = page_count() + 1; // append after last
        std::vector<std::uint8_t> zeros(config::PAGE_SIZE, 0);
        write_page(static_cast<page_id_t>(next_id), zeros.data(), zeros.size());
        return static_cast<page_id_t>(next_id);
    }

    std::string FileManager::table_filename(table_id_t table_id)
    {
        return std::string(config::TABLE_FILE_PREFIX) + std::to_string(table_id) + config::TABLE_FILE_EXTENSION;
    }

    std::filesystem::path FileManager::table_path(table_id_t table_id, std::filesystem::path directory)
    {
        if (directory.empty())
        {
            return config::default_db_dir() / table_filename(table_id);
        }
        return directory / table_filename(table_id);
    }

    std::string FileManager::index_filename(index_id_t index_id)
    {
        return std::string(config::INDEX_FILE_PREFIX) + std::to_string(index_id) + config::INDEX_FILE_EXTENSION;
    }

    std::filesystem::path FileManager::index_path(index_id_t index_id, std::filesystem::path directory)
    {
        if (directory.empty())
        {
            return config::default_index_dir() / index_filename(index_id);
        }
        return directory / index_filename(index_id);
    }

    bool FileManager::exists(const std::filesystem::path &path) noexcept
    {
        std::error_code ec;
        return fs::exists(path, ec);
    }

    bool FileManager::remove_file(const std::filesystem::path &path)
    {
        std::error_code ec;
        const bool removed = fs::remove(path, ec);
        if (ec)
        {
            KIZUNA_THROW_IO(StatusCode::IO_ERROR, "Failed to remove file", path.string());
        }
        return removed;
    }

}

