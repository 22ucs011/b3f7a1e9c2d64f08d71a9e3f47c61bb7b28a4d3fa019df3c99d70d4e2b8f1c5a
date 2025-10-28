#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/catalog_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "engine/ddl_executor.h"
#include "engine/dml_executor.h"
#include "sql/dml_parser.h"
#include "storage/file_manager.h"
#include "storage/index/index_manager.h"
#include "storage/page_manager.h"

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace
{
    struct Options
    {
        std::vector<int> rows{1'000, 10'000};
        int chunk_size{500};
        int lookup_samples{5};
        unsigned int seed{42};
    };

    struct BenchmarkResult
    {
        int rows{};
        double create_table_ms{0.0};
        double insert_total_ms{0.0};
        double create_index_ms{0.0};
        std::vector<double> lookup_samples_ms;

        double lookup_average() const
        {
            if (lookup_samples_ms.empty())
                return 0.0;
            double sum = std::accumulate(lookup_samples_ms.begin(), lookup_samples_ms.end(), 0.0);
            return sum / static_cast<double>(lookup_samples_ms.size());
        }
    };

    [[noreturn]] void print_usage_and_exit(std::ostream &out, int code)
    {
        out << "Usage: kizuna_index_benchmark [options]\n"
            << "Options:\n"
            << "  --rows N [N ...]         Row counts to benchmark (default: 1000 10000)\n"
            << "  --chunk-size N           Number of VALUES per INSERT (default: 500)\n"
            << "  --lookup-samples N       Number of lookup probes (default: 5)\n"
            << "  --seed N                 Random seed (default: 42)\n"
            << "  -h, --help               Show this message\n";
        std::exit(code);
    }

    int parse_positive_int(const std::string &value, std::string_view flag)
    {
        try
        {
            std::size_t pos = 0;
            int parsed = std::stoi(value, &pos);
            if (pos != value.size() || parsed <= 0)
            {
                throw std::invalid_argument("non-positive");
            }
            return parsed;
        }
        catch (const std::exception &)
        {
            std::ostringstream oss;
            oss << "Invalid numeric value for " << flag << ": " << value;
            throw std::runtime_error(oss.str());
        }
    }

    unsigned int parse_unsigned_int(const std::string &value, std::string_view flag)
    {
        try
        {
            std::size_t pos = 0;
            unsigned long parsed = std::stoul(value, &pos);
            if (pos != value.size() || parsed > std::numeric_limits<unsigned int>::max())
            {
                throw std::invalid_argument("out of range");
            }
            return static_cast<unsigned int>(parsed);
        }
        catch (const std::exception &)
        {
            std::ostringstream oss;
            oss << "Invalid numeric value for " << flag << ": " << value;
            throw std::runtime_error(oss.str());
        }
    }

    Options parse_arguments(int argc, char **argv)
    {
        Options opts;
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            if (arg == "--help" || arg == "-h")
            {
                print_usage_and_exit(std::cout, 0);
            }
            else if (arg == "--rows")
            {
                opts.rows.clear();
                if (i + 1 >= argc)
                {
                    throw std::runtime_error("Expected one or more values after --rows");
                }
                while (i + 1 < argc)
                {
                    const std::string next = argv[i + 1];
                    if (next.rfind("--", 0) == 0)
                        break;
                    ++i;
                    opts.rows.push_back(parse_positive_int(next, "--rows"));
                }
                if (opts.rows.empty())
                {
                    throw std::runtime_error("Expected at least one numeric value after --rows");
                }
            }
            else if (arg == "--chunk-size")
            {
                if (i + 1 >= argc)
                    throw std::runtime_error("Expected value after --chunk-size");
                opts.chunk_size = parse_positive_int(argv[++i], "--chunk-size");
            }
            else if (arg == "--lookup-samples")
            {
                if (i + 1 >= argc)
                    throw std::runtime_error("Expected value after --lookup-samples");
                opts.lookup_samples = parse_positive_int(argv[++i], "--lookup-samples");
            }
            else if (arg == "--seed")
            {
                if (i + 1 >= argc)
                    throw std::runtime_error("Expected value after --seed");
                opts.seed = parse_unsigned_int(argv[++i], "--seed");
            }
            else
            {
                std::ostringstream oss;
                oss << "Unknown option: " << arg;
                throw std::runtime_error(oss.str());
            }
        }
        return opts;
    }

    std::string make_key(int value)
    {
        std::ostringstream oss;
        oss << "key" << std::setw(6) << std::setfill('0') << value;
        return oss.str();
    }

    std::string make_payload(int value)
    {
        std::ostringstream oss;
        oss << "payload_" << std::setw(6) << std::setfill('0') << value;
        return oss.str();
    }

    template <typename Fn>
    double measure_ms(Fn &&fn)
    {
        const auto start = Clock::now();
        fn();
        const auto end = Clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }

    struct BenchmarkContext
    {
        fs::path db_path;
        kizuna::FileManager fm;
        std::unique_ptr<kizuna::PageManager> pm;
        std::unique_ptr<kizuna::catalog::CatalogManager> catalog;
        std::unique_ptr<kizuna::index::IndexManager> index_manager;

        explicit BenchmarkContext(fs::path path)
            : db_path(std::move(path)),
              fm(db_path.string(), /*create_if_missing=*/true)
        {
            std::error_code ec;
            fs::create_directories(db_path.parent_path(), ec);
            fs::remove(db_path, ec);
            fm.open();
            pm = std::make_unique<kizuna::PageManager>(fm, kizuna::config::DEFAULT_CACHE_SIZE);
            catalog = std::make_unique<kizuna::catalog::CatalogManager>(*pm, fm);
            fs::path indexes_dir = db_path.parent_path() / "indexes";
            index_manager = std::make_unique<kizuna::index::IndexManager>(indexes_dir);
        }

        ~BenchmarkContext()
        {
            if (pm)
            {
                try
                {
                    pm->flush_all();
                }
                catch (...)
                {
                    // Suppress destructor exceptions.
                }
            }
            catalog.reset();
            index_manager.reset();
            pm.reset();
            fm.close();
            std::error_code ec;
            const fs::path run_dir = db_path.parent_path();
            fs::remove(db_path, ec);
            fs::remove_all(run_dir / "indexes", ec);
            fs::remove_all(run_dir, ec);
        }
    };

    fs::path make_database_path()
    {
        auto base = kizuna::config::temp_dir();
        std::error_code ec;
        fs::create_directories(base, ec);
        const auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        std::ostringstream oss;
        oss << "kizuna_perf_" << now;
        const std::string unique_folder = oss.str();
        fs::path run_dir = base / unique_folder;
        fs::create_directories(run_dir, ec);
        std::string filename = "benchmark";
        filename += kizuna::config::DB_FILE_EXTENSION;
        return run_dir / filename;
    }

    std::string build_insert_sql(int begin_id, int end_id_exclusive)
    {
        std::ostringstream oss;
        oss << "INSERT INTO bench (id, lookup_key, payload) VALUES ";
        bool first = true;
        for (int value = begin_id; value < end_id_exclusive; ++value)
        {
            if (!first)
                oss << ", ";
            first = false;
            oss << "(" << value << ", '" << make_key(value) << "', '" << make_payload(value) << "')";
        }
        oss << ";";
        return oss.str();
    }

    BenchmarkResult run_single_benchmark(int rows,
                                         int chunk_size,
                                         int lookup_samples,
                                         std::mt19937 &rng)
    {
        const fs::path db_path = make_database_path();
        BenchmarkContext ctx(db_path);

        kizuna::engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);
        kizuna::engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm, *ctx.index_manager);

        BenchmarkResult result;
        result.rows = rows;

        result.create_table_ms = measure_ms([&]()
                                            { ddl.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, lookup_key VARCHAR(32), payload VARCHAR(64));"); });

        double insert_total = 0.0;
        for (int start = 1; start <= rows; start += chunk_size)
        {
            const int end = std::min(rows + 1, start + chunk_size);
            const std::string sql = build_insert_sql(start, end);
            insert_total += measure_ms([&]()
                                       {
                                           auto stmt = kizuna::sql::parse_insert(sql);
                                           dml.insert_into(stmt);
                                       });
        }
        result.insert_total_ms = insert_total;

        result.create_index_ms = measure_ms([&]()
                                            { ddl.execute("CREATE INDEX idx_bench_lookup ON bench(lookup_key);"); });

        const int sample_count = std::min(std::max(1, lookup_samples), rows);
        std::vector<int> candidates(rows);
        std::iota(candidates.begin(), candidates.end(), 1);
        std::vector<int> selected;
        selected.reserve(sample_count);
        std::sample(candidates.begin(), candidates.end(), std::back_inserter(selected), sample_count, rng);

        result.lookup_samples_ms.reserve(selected.size());
        for (int value : selected)
        {
            const std::string sql = "SELECT id FROM bench WHERE lookup_key = '" + make_key(value) + "';";
            const double elapsed = measure_ms([&]()
                                              {
                                                  auto stmt = kizuna::sql::parse_select(sql);
                                                  dml.select(stmt);
                                              });
            result.lookup_samples_ms.push_back(elapsed);
        }

        return result;
    }
} // namespace

int main(int argc, char **argv)
{
    try
    {
        const Options options = parse_arguments(argc, argv);
        std::mt19937 rng(options.seed);

        std::cout << "Kizuna index benchmark (C++ driver)\n";
        std::cout << "Row counts     : ";
        for (std::size_t i = 0; i < options.rows.size(); ++i)
        {
            if (i != 0)
                std::cout << ", ";
            std::cout << options.rows[i];
        }
        std::cout << "\n";
        std::cout << "Chunk size     : " << options.chunk_size << "\n";
        std::cout << "Lookup samples : " << options.lookup_samples << "\n";
        std::cout << "Seed           : " << options.seed << "\n\n";

        std::cout.setf(std::ios::fixed);
        std::cout << std::setprecision(3);

        for (int rows : options.rows)
        {
            try
            {
                BenchmarkResult result = run_single_benchmark(rows,
                                                              options.chunk_size,
                                                              options.lookup_samples,
                                                              rng);
                std::cout << "=== " << result.rows << " rows ===\n";
                std::cout << "  Create table : " << result.create_table_ms << " ms\n";
                std::cout << "  Insert total : " << result.insert_total_ms << " ms\n";
                std::cout << "  Create index : " << result.create_index_ms << " ms\n";
                for (std::size_t idx = 0; idx < result.lookup_samples_ms.size(); ++idx)
                {
                    std::cout << "  Lookup #" << std::setw(2) << std::setfill('0') << (idx + 1)
                              << "  : " << std::setw(0) << result.lookup_samples_ms[idx] << " ms\n";
                    std::cout << std::setfill(' ');
                }
                std::cout << "  Lookup avg   : " << result.lookup_average() << " ms\n\n";
            }
            catch (const kizuna::DBException &ex)
            {
                std::cout << "=== " << rows << " rows ===\n";
                std::cout << "  FAILED: " << ex.what() << "\n\n";
            }
            catch (const std::exception &ex)
            {
                std::cout << "=== " << rows << " rows ===\n";
                std::cout << "  FAILED: " << ex.what() << "\n\n";
            }
        }

        return 0;
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
}
