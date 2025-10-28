# Kizuna (V0.6)

Kizuna is a lightweight, teaching-focused DBMS written in modern C++ (C++20). V0.6 extends the V0.5 indexed storage engine with advanced SELECT execution (multi-column ORDER BY, DISTINCT, aggregates, INNER JOIN) and schema evolution via ALTER TABLE, while preserving the education-friendly architecture and test coverage from earlier milestones.

## Feature Highlights

- **Storage Core (V0.1)**: fixed-size paged file manager, slotted data pages, buffer pool with LRU, SQLite-style freelist trunks, typed record encode/decode, structured exceptions, logger, and REPL scaffolding.
- **Catalog & DDL (V0.2)**: persistent table/column catalog, SQL lexer/parser for CREATE/DROP TABLE, DDL executor wiring, REPL schema inspection and DROP IF EXISTS UX.
- **SQL DML (V0.3)**:
  - Null-bitmap record format with overflow-aware heap pages and BOOLEAN end-to-end support.
  - Table heap abstraction handling inserts, slot iteration, tombstone deletes, and fast truncate flows.
  - Baseline DML parser/AST/executor for `INSERT`, `SELECT *`, `DELETE` (without WHERE), and `TRUNCATE`, wired through the catalog.
  - REPL integration that routes DML commands, prints tabular SELECT output, and surfaces result counts/status.
  - Storage, parser, and executor unit tests covering FR-012..FR-016 scenarios.
- **SQL DML (V0.4)**:
  - Expression-aware parser for projection lists, WHERE comparisons/logicals, UPDATE assignments, and LIMIT clauses.
  - DML executor with predicate pushdown, projection materialisation, typed UPDATE/DELETE, and LIMIT enforcement.
  - Table heap update API that reuses slots when possible and relocates safely when payloads grow.
  - REPL SELECT output that mirrors projection headers and reports row counts for SELECT/UPDATE/DELETE.
  - Expanded unit tests covering expression evaluation, predicate semantics (NULL/tri-state), LIMIT edge cases, and update relocation.
- **Indexing & ORDER BY (V0.5)**:
  - B+ Tree implementation with leaf chaining and range/equality lookup helpers, plus on-disk index manager.
  - Catalog metadata for secondary indexes and automatic primary-key index creation during CREATE TABLE.
  - CREATE INDEX / DROP INDEX grammar + executor support with duplicate enforcement and persistence.
  - DML executor maintains index entries on INSERT/DELETE/UPDATE and can plan equality/range scans via indexes.
  - SELECT ... ORDER BY <column> [ASC|DESC] reuses a matching single-column index when available or performs an in-memory stable sort.
  - Additional parser/engine tests covering multi-column indexes, update maintenance, and ORDER BY result ordering.
- **Advanced Queries & Schema (V0.6)**:
  - Multi-column ORDER BY with mixed ASC/DESC terms, including stable in-memory sort when no composite index exists.
  - SELECT DISTINCT, COUNT/SUM/AVG/MIN/MAX (with DISTINCT variants), and LIMIT interaction aligned with SQL semantics.
  - INNER JOIN execution with table aliasing and ON predicates evaluated by the expression engine.
  - ALTER TABLE ADD/DROP COLUMN with table-heap migrations, catalog updates, and index rebuilds when schema changes.
  - Expanded parser, executor, catalog, and table-heap tests covering joins, aggregates, DISTINCT, and schema migration flows.

## Build

Prereqs: CMake 3.10+, a C++20 compiler (MSVC 2022, GCC 11+, Clang 13+).

**Windows (Developer PowerShell for VS 2022)**

- `cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64`
- `cmake --build build-msvc --config Debug -- /m`

**Linux/WSL/macOS**

- `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
- `cmake --build build --config Debug -- -j`

## Test & Run

- Tests: `build-msvc\Debug\run_tests.exe` (Windows) or `./build/run_tests` (POSIX).
- REPL: `build-msvc\Debug\kizuna.exe` (Windows) or `./build/kizuna` (POSIX).

See `docs/DEMO.md` for an end-to-end walkthrough that exercises the SQL pipeline.

## Benchmark Snapshot (Ao5)

- **Heap scans (no index)**: 1k rows `22.58 ms`, 10k rows `224.28 ms`, 100k rows `2531.09 ms`.
- **Indexed equality lookups**: 1k rows `4.80 ms` average (median `0.793 ms`), 10k rows `10.72 ms` average (median `7.27 ms`), 100k rows `64.92 ms` average (median `49.83 ms`).
- Measurements captured with `kizuna_index_benchmark` using average-of-five runs after discarding the cold-start outlier.

## SQL Quick Reference

**DDL**

- `CREATE TABLE employees ( employee_id INT PRIMARY KEY, full_name VARCHAR(64) NOT NULL, email VARCHAR(64) UNIQUE NOT NULL, department VARCHAR(32), is_active BOOLEAN DEFAULT TRUE );`
- `DROP TABLE [IF EXISTS] employees;`

**DML**

- `INSERT INTO employees (employee_id, full_name, email, department, is_active) VALUES (1001, 'Alice Johnson', 'alice.johnson@acme.com', 'Engineering', TRUE);`
- `SELECT full_name, is_active FROM employees WHERE is_active = TRUE LIMIT 5;`
- `UPDATE employees SET department = 'Research & Development' WHERE employee_id = 1001;`
- `DELETE FROM employees WHERE is_active = FALSE;`
- `TRUNCATE TABLE employees;`

**Indexing & Advanced Queries**

- `CREATE INDEX idx_users_name ON users(name);`
- `DROP INDEX IF EXISTS idx_users_name;`
- `SELECT id, name FROM users WHERE name = 'Alice Johnson' ORDER BY id DESC LIMIT 3;`
- `SELECT DISTINCT department FROM employees ORDER BY department;`
- `SELECT COUNT(*), AVG(age) FROM employees WHERE active;`
- `SELECT e.name, b.badge FROM employees e INNER JOIN badges b ON e.id = b.employee_id ORDER BY e.id, b.badge DESC;`

**Schema Evolution**

- `ALTER TABLE employees ADD COLUMN nickname VARCHAR(16);`
- `ALTER TABLE employees DROP COLUMN nickname;`

Additional REPL helpers: `show tables`, `schema <table>`, `loglevel <level>`

## Project Layout

- `src/common/` configuration, types, exceptions, logger
- `src/storage/` file manager, page/page_manager, record helpers, table heap
- `src/catalog/` table/column catalog persistence
- `src/sql/` AST definitions plus DDL/DML parsers
- `src/engine/` DDL/DML executors and expression evaluation
- `src/storage/index/` B+ tree implementation and index manager
- `src/cli/` REPL and command wiring
- `tests/` storage, catalog, parser, engine suites

## Notes

- All runtime assets are created beside the executable under `database/` (catalog, data, indexes, logs, temp, backup).
- Page 1 is reserved for catalog metadata; data pages start at ID 2 and the freelist uses SQLite-style trunks.
- Indexes currently support equality and range scans on single-column keys; ORDER BY reuses matching indexes or falls back to a stable in-memory sort. Multi-column ordering, DISTINCT, aggregates, and INNER JOIN operate via the SELECT executor.
