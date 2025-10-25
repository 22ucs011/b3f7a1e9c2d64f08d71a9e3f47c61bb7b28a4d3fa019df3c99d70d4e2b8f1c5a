#include "common/path_utils.h"

#include <filesystem>

#if defined(_WIN32)
#include <windows.h>
#elif defined(__APPLE__)
#include <mach-o/dyld.h>
#include <limits.h>
#else
#include <limits.h>
#include <unistd.h>
#endif

namespace kizuna::path_utils
{
    std::filesystem::path executable_dir()
    {
        static const std::filesystem::path cached = [] {
            std::filesystem::path exe_path;

#if defined(_WIN32)
            wchar_t buffer[MAX_PATH];
            DWORD len = GetModuleFileNameW(nullptr, buffer, static_cast<DWORD>(std::size(buffer)));
            if (len != 0)
                exe_path = std::filesystem::path(buffer);
#elif defined(__APPLE__)
            char buffer[PATH_MAX];
            uint32_t size = static_cast<uint32_t>(sizeof(buffer));
            if (_NSGetExecutablePath(buffer, &size) == 0)
            {
                exe_path = std::filesystem::path(buffer);
            }
            else
            {
                std::string tmp;
                tmp.resize(size);
                if (_NSGetExecutablePath(tmp.data(), &size) == 0)
                    exe_path = std::filesystem::path(tmp.c_str());
            }
#else
            char buffer[PATH_MAX];
            ssize_t len = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
            if (len != -1)
            {
                buffer[len] = '\0';
                exe_path = std::filesystem::path(buffer);
            }
#endif

            if (exe_path.empty())
                return std::filesystem::current_path();
            return exe_path.parent_path();
        }();

        return cached;
    }
}

