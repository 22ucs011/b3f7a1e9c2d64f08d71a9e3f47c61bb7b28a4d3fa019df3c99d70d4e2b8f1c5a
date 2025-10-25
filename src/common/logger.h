#pragma once

#include <atomic>
#include <fstream>
#include <mutex>
#include <optional>
#include <vector>
#include <sstream>
#include <string>
#include <string_view>
#include <source_location>
#include <thread>

#include "common/types.h"      // LogLevel
#include "common/config.h"     // logging config constants

namespace kizuna
{

    class Logger
    {
    public:
        static Logger &instance();

        // Configuration
        void set_level(LogLevel level) noexcept;
        LogLevel level() const noexcept { return level_.load(); }

        void enable_console(bool enabled) noexcept;
        bool console_enabled() const noexcept { return console_enabled_.load(); }

        void set_log_file(const std::string &path);

        // Logging APIs
        void log(LogLevel level,
                 std::string_view message,
                 const std::source_location &loc = std::source_location::current());

        template <typename... Args>
        void logf_loc(LogLevel level,
                      const std::source_location &loc,
                      Args &&...args)
        {
            if (level < level_.load()) return;
            std::ostringstream oss;
            (oss << ... << std::forward<Args>(args));
            log(level, oss.str(), loc);
        }

        template <typename... Args>
        void logf(LogLevel level, Args &&...args)
        {
            logf_loc(level, std::source_location::current(), std::forward<Args>(args)...);
        }

        // Convenience wrappers
        template <typename... Args> void debug(Args &&...args) { logf(LogLevel::DEBUG, std::forward<Args>(args)...); }
        template <typename... Args> void info(Args &&...args) { logf(LogLevel::INFO, std::forward<Args>(args)...); }
        template <typename... Args> void warn(Args &&...args) { logf(LogLevel::WARN, std::forward<Args>(args)...); }
        template <typename... Args> void error(Args &&...args) { logf(LogLevel::ERROR, std::forward<Args>(args)...); }
        template <typename... Args> void fatal(Args &&...args) { logf(LogLevel::FATAL, std::forward<Args>(args)...); }

    private:
        Logger();
        ~Logger();
        Logger(const Logger &) = delete;
        Logger &operator=(const Logger &) = delete;

        void open_file_if_needed();
        void rotate_if_needed_unlocked();
        std::string format_prefix(LogLevel level,
                                  const std::source_location &loc);

        static const char *level_to_string(LogLevel level) noexcept;

    private:
        std::mutex mutex_;
        std::ofstream file_;
        std::string file_path_;
        std::atomic<LogLevel> level_;
        std::atomic<bool> console_enabled_;
        std::vector<char> buffer_;
    };

    // Macros for convenient logging with source location
    #define KIZUNA_LOG_DEBUG(...) ::kizuna::Logger::instance().debug(__VA_ARGS__)
    #define KIZUNA_LOG_INFO(...)  ::kizuna::Logger::instance().info(__VA_ARGS__)
    #define KIZUNA_LOG_WARN(...)  ::kizuna::Logger::instance().warn(__VA_ARGS__)
    #define KIZUNA_LOG_ERROR(...) ::kizuna::Logger::instance().error(__VA_ARGS__)
    #define KIZUNA_LOG_FATAL(...) ::kizuna::Logger::instance().fatal(__VA_ARGS__)

} // namespace kizuna
