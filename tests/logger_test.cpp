#include <cstdio>
#include <filesystem>
#include <string>
#include "common/logger.h"

using namespace kizuna;
namespace fs = std::filesystem;

bool logger_tests()
{
    try
    {
        const std::string log_path = (config::temp_dir() / "logger_test.log").string();
        Logger::instance().set_log_file(log_path);
        Logger::instance().set_level(LogLevel::DEBUG);
        Logger::instance().enable_console(false); // quiet tests

        KIZUNA_LOG_INFO("hello ", 123);
        KIZUNA_LOG_DEBUG("debug line");
        KIZUNA_LOG_WARN("warn line");
        KIZUNA_LOG_ERROR("error line");

        std::error_code ec;
        const auto sz = fs::file_size(log_path, ec);
        if (ec || sz == 0) return false;
    }
    catch (...)
    {
        return false;
    }
    return true;
}

