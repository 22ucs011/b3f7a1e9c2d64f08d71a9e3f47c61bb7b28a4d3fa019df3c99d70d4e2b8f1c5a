cmake_minimum_required(VERSION 3.10)

# Project definition
project(kizuna CXX)

# Use C++20 for std::source_location and other features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type flags (compiler-specific)
if (MSVC)
    # MSVC flags
    # /Zi: debug info, /W4: warnings, /EHsc: exceptions semantics
    add_compile_options(/EHsc)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Zi /W4 /DDEBUG_BUILD")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /DNDEBUG")
else()
    # GCC/Clang flags
    add_compile_options(-Wall -Wextra)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -DDEBUG_BUILD")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# Source directory
set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests)

# Create a static library with the sources that exist now
add_library(kizuna_common STATIC
    ${SOURCE_DIR}/common/exception.cpp
    ${SOURCE_DIR}/common/logger.cpp
    ${SOURCE_DIR}/common/path_utils.cpp
    ${SOURCE_DIR}/common/value.cpp
    ${SOURCE_DIR}/storage/file_manager.cpp
    ${SOURCE_DIR}/storage/page_manager.cpp
    ${SOURCE_DIR}/storage/record.cpp
    ${SOURCE_DIR}/storage/table_heap.cpp
    ${SOURCE_DIR}/storage/index/bplus_tree_node.cpp
    ${SOURCE_DIR}/storage/index/bplus_tree.cpp
    ${SOURCE_DIR}/storage/index/index_manager.cpp
    ${SOURCE_DIR}/catalog/schema.cpp
    ${SOURCE_DIR}/catalog/catalog_manager.cpp
    ${SOURCE_DIR}/sql/ast.cpp
    ${SOURCE_DIR}/sql/ddl_parser.cpp
    ${SOURCE_DIR}/sql/dml_parser.cpp
    ${SOURCE_DIR}/engine/ddl_executor.cpp
    ${SOURCE_DIR}/engine/dml_executor.cpp
    ${SOURCE_DIR}/engine/expression_evaluator.cpp
)

# Public headers live under src
target_include_directories(kizuna_common PUBLIC ${SOURCE_DIR})

# --------- CLI executable ---------
add_executable(kizuna
    ${SOURCE_DIR}/main.cpp
    ${SOURCE_DIR}/cli/repl.cpp
)
target_link_libraries(kizuna PRIVATE kizuna_common)

# Status output
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Source directory: ${SOURCE_DIR}")

# ---------------- Tests ----------------
add_executable(run_tests
    ${TEST_DIR}/test_main.cpp
    ${TEST_DIR}/exception_test.cpp
    ${TEST_DIR}/logger_test.cpp
    ${TEST_DIR}/value_test.cpp
    ${TEST_DIR}/file_manager_test.cpp
    ${TEST_DIR}/file_manager_edge.cpp
    ${TEST_DIR}/page_test.cpp
    ${TEST_DIR}/page_manager_test.cpp
    ${TEST_DIR}/record_test.cpp
    ${TEST_DIR}/page_manager_freelist_test.cpp
    ${TEST_DIR}/storage/table_heap_test.cpp
    ${TEST_DIR}/sql/dml_parser_test.cpp
    ${TEST_DIR}/engine/dml_executor_test.cpp
    ${TEST_DIR}/engine/expression_evaluator_test.cpp
    ${TEST_DIR}/sql/ddl_parser_test.cpp
    ${TEST_DIR}/catalog/catalog_manager_test.cpp
    ${TEST_DIR}/storage/bplus_tree_node_test.cpp
    ${TEST_DIR}/index/bplus_tree_test.cpp
    ${TEST_DIR}/index/index_manager_test.cpp
)
target_link_libraries(run_tests PRIVATE kizuna_common)
target_include_directories(run_tests PRIVATE ${SOURCE_DIR})

add_custom_target(test
    COMMAND run_tests
    DEPENDS run_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)








