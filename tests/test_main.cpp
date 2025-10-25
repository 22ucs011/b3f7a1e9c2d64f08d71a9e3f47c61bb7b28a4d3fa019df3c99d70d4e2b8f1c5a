#include <iostream>
#include <vector>
#include <string>

using TestFunc = bool(*)();

struct TestCase { const char* name; TestFunc fn; };

// Declarations for individual test files
bool exception_tests();
bool logger_tests();
bool file_manager_tests();
bool file_manager_edge_tests();
bool page_tests();
bool page_manager_tests();
bool record_tests();
bool value_tests();
bool page_manager_freelist_tests();
bool table_heap_tests();
bool bplus_tree_tests();
bool bplus_tree_node_tests();
bool index_manager_tests();
bool sql_dml_parser_tests();
bool sql_ddl_parser_tests();
bool catalog_manager_ddl_tests();
bool dml_executor_tests();
bool expression_evaluator_tests();

int main()
{
    std::vector<TestCase> tests = {
        {"exception_tests", &exception_tests},
        {"logger_tests", &logger_tests},
        {"file_manager_tests", &file_manager_tests},
        {"file_manager_edge_tests", &file_manager_edge_tests},
        {"page_tests", &page_tests},
        {"record_tests", &record_tests},
        {"value_tests", &value_tests},
        {"page_manager_tests", &page_manager_tests},
        {"page_manager_freelist_tests", &page_manager_freelist_tests},
        {"table_heap_tests", &table_heap_tests},
        {"bplus_tree_tests", &bplus_tree_tests},
        {"index_manager_tests", &index_manager_tests},
        {"bplus_tree_node_tests", &bplus_tree_node_tests},
        {"sql_dml_parser_tests", &sql_dml_parser_tests},
        {"sql_ddl_parser_tests", &sql_ddl_parser_tests},
        {"expression_evaluator_tests", &expression_evaluator_tests},
        {"dml_executor_tests", &dml_executor_tests},
        {"catalog_manager_ddl_tests", &catalog_manager_ddl_tests},
    };

    int failures = 0;
    for (const auto &t : tests)
    {
        try
        {
            const bool ok = t.fn();
            std::cout << (ok ? "[PASS] " : "[FAIL] ") << t.name << "\n";
            if (!ok) ++failures;
        }
        catch (const std::exception &e)
        {
            std::cout << "[EXCEPTION] " << t.name << ": " << e.what() << "\n";
            ++failures;
        }
        catch (...)
        {
            std::cout << "[EXCEPTION] " << t.name << ": unknown\n";
            ++failures;
        }
    }

    std::cout << (failures == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED") << "\n";
    return failures == 0 ? 0 : 1;
}





