#include <string>
#include <cstring>
#include "common/exception.h"

using namespace kizuna;

static bool contains(const std::string &hay, const char *needle)
{
    return hay.find(needle) != std::string::npos;
}

bool exception_tests()
{
    // status_code_to_string basic check
    if (std::strcmp(status_code_to_string(StatusCode::OK), "OK") != 0) return false;

    // DBException formatting and classification
    DBException ex(StatusCode::FILE_NOT_FOUND, "Missing file", "tests");
    if (!ex.is_io_error()) return false;
    if (ex.is_storage_error()) return false;
    if (!contains(ex.what(), "FILE_NOT_FOUND")) return false;

    // Convenience factory
    auto io = IOException::file_not_found("/tmp/nope");
    if (io.code() != StatusCode::FILE_NOT_FOUND) return false;

    return true;
}

