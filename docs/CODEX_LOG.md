# Codex Session Log

## 2025-09-14 Session
- Reviewed kizuna-SRS.md and docs/DEV_NOTES.md for scope and current V0.1 coverage.
- Catalogued repository structure (src, docs, tests) to prepare for V0.2 implementation.
- Outlined V0.2 deltas and incremental change plan (see docs/V0_2_PLAN.md).

- Established V0.2 implementation plan in docs/V0_2_PLAN.md mapping FR-007..FR-011 to code changes.

- Updated src/common/config.h with catalog constants and table file naming for V0.2 groundwork.

- Extended src/common/types.h with catalog ids and schema structs (ColumnDef/TableDef) ready for V0.2 catalog work.

- Extended exception codes/helpers for catalog DDL errors and prepped FileManager/PageManager/REPL for SQL V0.2 (see src/common/exception.*, src/storage/file_manager.*, src/storage/page_manager.*, src/cli/repl.*).

- Added catalog schema module (src/catalog/schema.*) with serialization helpers for table/column metadata and wired into build.

- Implemented catalog manager scaffolding with metadata persistence and slotted-page storage for __tables__/__columns__ (src/catalog/catalog_manager.*).

- Implemented SQL AST + DDL parser for CREATE/DROP TABLE with basic constraint handling (src/sql/*).

- Completed V0.2: DDL executor + REPL wiring, parser tweaks, catalog drop path, and added tests (sql/catalog). All tests passing.

- Updated REPL help output to call out V0.2 SQL DDL support and version bump.
- Added REPL schema dump command plus friendlier SQL error/output for DROP IF EXISTS.

- Restored REPL schema command wiring so the handler registers and help lists it.

- Added REPL "show tables" command to list catalog tables from the current database.
- Documented the new command in docs/DEMO.md walkthrough.

- Drafted V0.3 implementation plan aligned with SRS FR-012..FR-016 (see docs/V0_3_PLAN.md).

- Retrofitted record/page layout for V0.3: null-bitmaps in row encoding, page header now tracks heap neighbors, tests adjusted.

- Added table heap layer (sequential heap pages + iterator) and wired storage tests for V0.3 Step 2.

- Added DML AST + parser for INSERT/SELECT/DELETE/TRUNCATE with parser tests to prep V0.3 execution.


- Implemented DML executor bridging TableHeap with catalog lookups and constraint checks (src/engine/dml_executor.*).
- Wired REPL DML commands (INSERT/SELECT/DELETE/TRUNCATE) with nicer SELECT output and error surfacing.
- Added storage + engine regression tests for DML flows and hooked them into test main.
- Docs and help text still pending full V0.3 refresh (next session).


- Updated DEV_NOTES.md and DEMO.md to V0.3 so the DML flow is documented for next session.
- Taught the DDL parser about BOOLEAN/BOOL tokens and reran the full test suite (all green).


- Patched legacy catalog pages by auto-upgrading INVALID page types to DATA in Page::insert/read/erase so CREATE TABLE works on older .kz files (tests green).
- Refreshed docs/DEMO.md with the V0.3 demo script covering DDL/DML flow and legacy storage tricks.
- Authored docs/V0_4_PLAN.md outlining expression/WHERE roadmap for FR-017..FR-023 plus suggested commit cadence.
- V0.4 Step 1: added common Value utilities with tri-state comparison, DATE parsing/formatting, record DATE encoding, DML DATE handling, plus tests/build wiring (value/record).
- V0.4 Step 2: expanded AST with expression tree/projections/assignments, overhauled DML parser for WHERE/LIMIT/UPDATE, and added parser tests covering new grammar (all tests passing).
- V0.4 Step 3: built ExpressionEvaluator over the new AST (tri-valued AND/OR/NOT, comparison coercions, scalar pulls) and validated with dedicated engine tests.
- V0.4 Step 4: taught Page/TableHeap to update in-place or relocate rows, exposed scan helper, and expanded table_heap tests for update + iteration.
- V0.4 Step 5: Reworked DML executor for projection-aware SELECT, predicate-driven DELETE/UPDATE, LIMIT enforcement, and added post-relocation update batching (src/engine/dml_executor.* plus tests).
- V0.4 Step 6: Updated REPL DML handling and help copy for WHERE/LIMIT/UPDATE support with row-count summaries (src/cli/repl.cpp), reran full test suite.
## 2025-10-14 Session
- Landed V0.6 advanced SELECT features: multi-column ORDER BY, DISTINCT, aggregate evaluation (COUNT/SUM/AVG/MIN/MAX), and INNER JOIN execution in `DMLExecutor` with matching engine tests.
- Extended expression binding to resolve qualified columns across joined tables and aggregate inputs.
- Added ALTER TABLE ADD/DROP COLUMN pipeline covering catalog updates, table-heap migrations, and index rebuild triggers with storage/catalog regression coverage.
- Refreshed README, DEV_NOTES, DEMO walkthrough, and V0_6 plan checklist to document the new SQL surface and workflow.
- Reran the unified `run_tests` target to verify parser, engine, catalog, and storage suites after V0.6 changes.
- V0.4 Step 7: Expanded value/expression/DML tests for projections, LIMIT, null semantics, plus README/DEMO refresh highlighting the new SQL surface.\n## 2025-10-11 Session\n- Delivered V0.5 indexing + ORDER BY updates: README/DEMO/DEV_NOTES refreshed for CREATE/DROP INDEX, auto PK indexes, index-aware DML, and SELECT ORDER BY.\n- Added ORDER BY clause support to DML AST/parser, executor sorting/index planning, and engine tests exercising ASC/DESC plus ORDER BY LIMIT cases.\n- Extended REPL help with V0.5 syntax blurb and reran full un_tests (all green).\n

