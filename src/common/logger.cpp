#include "common/logger.h"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <ctime>

namespace fs = std::filesystem;

namespace kizuna
{

    // -------- static helpers --------
    const char *Logger::level_to_string(LogLevel level) noexcept
    {
        switch (level)
        {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        }
        return "UNK";
    }

    // -------- lifecycle --------
    Logger &Logger::instance()
    {
        static Logger inst;
        return inst;
    }

    Logger::Logger()
        : file_path_(config::default_log_file().string()),
          level_(LogLevel::INFO),
          console_enabled_(true)
    {
        buffer_.resize(config::LOG_BUFFER_SIZE);
        open_file_if_needed();
    }

    Logger::~Logger()
    {
        std::lock_guard<std::mutex> lg(mutex_);
        if (file_.is_open())
        {
            file_.flush();
            file_.close();
        }
    }

    // -------- configuration --------
    void Logger::set_level(LogLevel level) noexcept { level_.store(level); }
    void Logger::enable_console(bool enabled) noexcept { console_enabled_.store(enabled); }

    void Logger::set_log_file(const std::string &path)
    {
        std::lock_guard<std::mutex> lg(mutex_);
        if (file_.is_open())
        {
            file_.flush();
            file_.close();
        }
        file_path_ = path;
        open_file_if_needed();
    }

    // -------- internals --------
    void Logger::open_file_if_needed()
    {
        if (!file_.is_open())
        {
            // Ensure directory exists if path has a directory
            try
            {
                fs::path p(file_path_);
                if (p.has_parent_path() && !p.parent_path().empty())
                {
                    fs::create_directories(p.parent_path());
                }
            }
            catch (...)
            {
                // Ignore directory creation errors; we'll try open anyway
            }

            file_.open(file_path_, std::ios::out | std::ios::app);
            if (file_.rdbuf())
            {
                // Hint a buffer; not guaranteed, but fine
                file_.rdbuf()->pubsetbuf(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
            }
        }
    }

    void Logger::rotate_if_needed_unlocked()
    {
        try
        {
            auto max_bytes = static_cast<std::uintmax_t>(config::MAX_LOG_FILE_SIZE_MB) * 1024u * 1024u;
            if (max_bytes == 0) return;

            std::error_code ec;
            const auto sz = fs::file_size(file_path_, ec);
            if (ec || sz <= max_bytes) return;

            // Close current file before rotating
            if (file_.is_open())
            {
                file_.flush();
                file_.close();
            }

            // Shift older logs: log.(N-1) -> log.N
            const std::size_t keep = config::MAX_LOG_FILES;
            fs::path base(file_path_);
            for (std::size_t i = keep; i-- > 1;)
            {
                fs::path from = base;
                from += "." + std::to_string(i - 1);
                fs::path to = base;
                to += "." + std::to_string(i);
                std::error_code ec_from;
                if (fs::exists(from, ec_from))
                {
                    std::error_code ec_rm;
                    fs::remove(to, ec_rm); // remove if exists
                    std::error_code ec_mv;
                    fs::rename(from, to, ec_mv);
                }
            }

            // Move current -> .1
            {
                fs::path from = base;
                fs::path to = base;
                to += ".1";
                std::error_code ec_rm;
                fs::remove(to, ec_rm);
                std::error_code ec_mv;
                fs::rename(from, to, ec_mv);
            }

            // Reopen fresh file
            file_.open(file_path_, std::ios::out | std::ios::trunc);
            if (file_.rdbuf())
            {
                file_.rdbuf()->pubsetbuf(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
            }
        }
        catch (...)
        {
            // If rotation fails, we silently continue logging to the same file
            if (!file_.is_open())
            {
                file_.open(file_path_, std::ios::out | std::ios::app);
            }
        }
    }

    std::string Logger::format_prefix(LogLevel level, const std::source_location &loc)
    {
        using namespace std::chrono;
        const auto now = system_clock::now();
        const auto t = system_clock::to_time_t(now);
        const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

        std::tm tm_snapshot{};
#if defined(_WIN32)
        localtime_s(&tm_snapshot, &t);
#else
        localtime_r(&t, &tm_snapshot);
#endif

        std::ostringstream oss;
        oss << '[' << std::put_time(&tm_snapshot, "%Y-%m-%d %H:%M:%S")
            << '.' << std::setw(3) << std::setfill('0') << ms.count() << ']'
            << '[' << level_to_string(level) << ']'
            << '[' << std::this_thread::get_id() << ']'
            << ' ';

        if (config::DEBUG_MODE)
        {
            oss << '(' << loc.file_name() << ':' << loc.line() << ':' << loc.function_name() << ") ";
        }
        return oss.str();
    }

    // -------- logging --------
    void Logger::log(LogLevel level, std::string_view message, const std::source_location &loc)
    {
        if (level < level_.load()) return;

        std::lock_guard<std::mutex> lg(mutex_);
        open_file_if_needed();
        rotate_if_needed_unlocked();

        const std::string prefix = format_prefix(level, loc);
        const std::string line = prefix + std::string(message) + '\n';

        if (console_enabled_.load())
        {
            // Console: stdout for info and below, stderr for warn and above
            if (level <= LogLevel::INFO)
                std::fwrite(line.data(), 1, line.size(), stdout);
            else
                std::fwrite(line.data(), 1, line.size(), stderr);
        }

        if (file_.is_open())
        {
            file_ << line;
            if (level >= LogLevel::ERROR) file_.flush();
        }
    }

} // namespace kizuna

