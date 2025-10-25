Kizuna - Developer Notes (V0.6)

Overview

- Storage-first teaching database covering catalogued SQL DDL (V0.2), richer SQL DML (V0.4 predicates/UPDATE/DELETE/LIMIT), V0.5 secondary indexing + ORDER BY, and V0.6 advanced SELECT features (DISTINCT, aggregates, joins) plus ALTER TABLE schema evolution.
- Pipeline: SQL text -> lexer/parser -> AST -> engine executor -> catalog + storage (TableHeap + PageManager).
- Focus areas this cycle: expression evaluation, predicate-aware DML executor, REPL ergonomics, and storage update mechanics.

Modules

- common/config.h: Tunables (page size, directories, version toggles) plus helper funcs.
- common/types.h: Shared enums, ids, and SQL value helpers (see LiteralValue for DML binding).
- common/exception.h/.cpp: StatusCode taxonomy + DBException/QueryException wrappers that carry source location.
- common/logger.h/.cpp: Lightweight singleton logger with level control and rotating file sink.
- common/value.h/.cpp: Variant Value abstraction with tri-state logic, numeric/date coercions, and formatting helpers.
- storage/file_manager.h/.cpp: Backing file I/O and page allocation (1-based ids, freelist trunk layout).
- storage/page.h: Slotted page header, null-aware slots, tombstone flags, next-page pointer for heap chaining.
- storage/record.h/.cpp: Encode/decode routines that honor column metadata, null bitmap, and boolean literals.
- storage/page_manager.h/.cpp: Buffer manager + freelist allocator; now exposes helpers for heap growth and trunk maintenance.
- storage/table_heap.h/.cpp: Table-level helper that appends, iterates, tombstones, and truncates across chained DATA pages.
- catalog/schema.h/.cpp: Serializable structs for tables/columns; versions stay in sync with SRS schema spec.
- catalog/catalog_manager.h/.cpp: CRUD on catalog records + table root tracking + file lifecycle.
- sql/ast.h: AST nodes for DDL/DML including CREATE/DROP INDEX metadata, SELECT ORDER BY clauses, aggregates, DISTINCT, and JOIN descriptors.
- sql/ddl_parser.h/.cpp & sql/dml_parser.h/.cpp: Hand-written recursive-descent parsers spanning CREATE/DROP INDEX, ALTER TABLE, JOIN grammar, DISTINCT, and aggregate calls with friendlier error text.
- engine/ddl_executor.h/.cpp: Bind DDL/INDEX ASTs into catalog mutations and storage allocations, with constraint enforcement and ALTER TABLE ADD/DROP COLUMN migrations.
- engine/dml_executor.h/.cpp: Execute INSERT/SELECT/DELETE/UPDATE/TRUNCATE with projection, predicate pushdown, LIMIT/ORDER BY enforcement, INNER JOINs, DISTINCT, aggregates, table heap updates, and index maintenance.
- engine/expression_evaluator.h/.cpp: Evaluate expression AST nodes with tri-valued logic, type coercion, and column bindings for WHERE/SET/ORDER BY/JOIN/aggregate clauses.
- cli/repl.h/.cpp: Command handlers (status/show/schema) plus SQL dispatcher that routes DDL/DML, prints ordered SELECT results, and manages DB lifecycle.

Testing

- Configure: cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
- Build: cmake --build build --config Debug -- /m
- Run: build\Debug\run_tests.exe (or ./build/run_tests on POSIX)
- Suites cover: record encode/decode, page manager freelist, table heap insert/delete/update, SQL parser + expression evaluator cases, predicate-aware DML executor flows, join/distinct/aggregate scenarios, and ALTER TABLE migrations.
- Tests write under ./temp/; safe to purge between runs.

Change Log (append new bullets as we iterate)

- Added: exceptions implementation (src/common/exception.cpp).
- Added: logger (src/common/logger.\*) with console + file sinks.
- Added: file manager (src/storage/file_manager.\*) for paged I/O.
- Added: page layout (src/storage/page.\*) with slot directory and guard rails.
- Added: page cache (src/storage/page_manager.\*) with freelist trunking.
- Added: record helpers (src/storage/record.\*) covering INT/BIGINT/DOUBLE/BOOLEAN/VARCHAR.
- Added: catalog schema + manager (src/catalog/\*) persisting **tables**/**columns**.
- Added: SQL AST + DDL executor path (src/sql/_, src/engine/ddl_executor._) for CREATE/DROP TABLE.
- Added: REPL schema/show tables commands and nicer DROP IF EXISTS UX.
- Added: Record format retrofit with null bitmap + heap page linkage (V0.3 Step 1).
- Added: TableHeap abstraction that handles append, tombstone delete, and truncate with iterator (V0.3 Step 2).
- Added: SQL DML parser for INSERT/SELECT/DELETE/TRUNCATE (V0.3 Step 3).
- Added: DML executor + REPL integration + row printing (V0.3 Step 4 & 5).
- Added: Storage, SQL, and engine unit tests for V0.3 features; hooked into run_tests target (V0.3 Step 6).
- Added: Value/date extensions with tri-valued logic and record encoding updates (V0.4 Step 1).
- Added: Expression AST + parser support for WHERE/LIMIT/UPDATE assignments (V0.4 Step 2).
- Added: Expression evaluator engine with typed coercions and tri-bool logic (V0.4 Step 3).
- Added: TableHeap update path with relocate-or-reuse semantics and iterator scan helper (V0.4 Step 4).
- Added: DML executor predicate pushdown, projections, LIMIT, and typed UPDATE flow (V0.4 Step 5).
- Added: REPL DML UX refresh with projection-aware printing and row-count summaries (V0.4 Step 6).
- Added: Extended parser/expression/dml tests plus README/DEMO updates (V0.4 Step 7).
- Added: B+ tree index storage layer, index manager, and catalog metadata wiring (V0.5 Step 1-4).
- Added: CREATE/DROP INDEX grammar + auto primary-key indexes in DDL executor (V0.5 Step 5-6).
- Added: Index-maintained DML executor with equality/range scans and UPDATE support (V0.5 Step 7).
- Added: SELECT ORDER BY parsing/execution with index-aware planning and fallback sorting plus updated REPL UX/docs/tests (V0.5 Step 8-9).
- Added: Multi-column ORDER BY, DISTINCT, aggregate evaluation, and INNER JOIN execution in the DML executor (V0.6 Steps 3-6).
- Added: ALTER TABLE ADD/DROP COLUMN migrations with catalog/table-heap/index rebuild support and tests (V0.6 Step 2 & 5).
- Added: Parser/REPL/docs refresh for V0.6 SQL surface alongside new engine/catalog/unit tests (V0.6 Step 1 & 7-8).

Troubleshooting Log (Issues & Fixes)

- IntelliSense C++20 mismatch: set IDE standard to C++20 to match CMake flags.
- MSVC /Wextra invalid: switch to /W4 on MSVC via generator expressions.
- Slotted page overlap: fix by reserving slot space before writes and bounding by free_space_offset.
- Disk offset bug (1-based ids): use (page_id - 1) \* PAGE_SIZE.
- Metadata misuse: guard REPL commands from writing to page 1.
- Freelist scaling: trunk/leaf freelist like SQLite, stored in metadata + dedicated pages.
- Post-free access: enforce page type checks before record ops.
- Null bitmap regression: align slot payload to decode helper and zero unused bitmap bits (tests cover it).
- Heap chain corruption: always set next_page_id and flush parent before allocating another page.

Test Enhancements (Edge Cases)

- Page capacity: fill until full; verify sum of slot lengths matches page usage.
- File I/O edges: guard invalid reads/writes; ensure allocate increments count once.
- Freelist persistence: trunk reuse validated after reopen.
- Record roundtrip: mix types, nulls, and bool literal text.
- TableHeap: insert across multiple pages, delete some rows, reinsert to reuse tombstones.
- SQL parsers: cover happy path + malformed tokens for DDL and DML.
- Expression evaluator: tri-valued AND/OR/NOT, NULL comparisons, and DATE predicates.
- DML executor: projection + WHERE/LIMIT paths for SELECT/DELETE/UPDATE, including VARCHAR growth cases.

Demo Script

- Walkthrough lives in docs/DEMO.md; highlights V0.6 joins, DISTINCT, aggregates, ALTER TABLE flows, and legacy storage ops.
