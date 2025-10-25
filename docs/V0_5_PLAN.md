# Kizuna V0.5 Implementation Plan

## Current Baseline (V0.4)

- Predicate-aware DML executor supports SELECT projections, WHERE (logical/comparison/NULL ops), UPDATE/DELETE filtering, and LIMIT.
- Table heap can update rows in place or relocate them, with iterator helpers that surface row handles for DML operations.
- Expression evaluator handles tri-valued logic, type coercions, and DATE comparisons; Value subsystem stores DATE as 64-bit epoch.
- REPL exposes richer SQL UX with [rows=...] summaries, projection-aware SELECT printing, and DEBUG logging hooks.
- Tests cover value coercions, expression evaluation, predicate DML flows, and table heap update mechanics; docs synced to V0.4 surface.

## V0.5 Functional Targets (SRS Section 2.1 V0.5)

| FR     | Description                           | Key Considerations                                                              |
| ------ | ------------------------------------- | ------------------------------------------------------------------------------- |
| FR-024 | B+ Tree indexing                      | Node layout, split/merge correctness, durable on-disk format                    |
| FR-025 | Automatic primary-key index           | Triggered during CREATE TABLE, catalog metadata stored                          |
| FR-026 | CREATE INDEX / DROP INDEX             | Parser grammar, metadata persistence, file lifecycle                            |
| FR-027 | Single-column ORDER BY                | Sorting via indexes when possible, fallback sort                                |
| FR-028 | Index-based lookups and range queries | Executor chooses index scan, supports equality and BETWEEN or inequality ranges |

**SRS Notes to Honor**

- B+ tree node size should be configurable (default near 256 entries) via config constants.
- Leaf nodes must chain via next_leaf pointers for range scans and ORDER BY.
- Primary keys auto-indexed; ensure unique enforcement and catalog entry visibility.
- ORDER BY required for a single column with ASC or DESC; leverage index ordering when available.

## Change Map (anticipated)

- src/common/config.h: expose index directory paths, B+ tree fanout defaults, and tunables for ORDER BY memory limits.
- src/catalog/schema.\*: extend structs and serialization for index metadata (name, table id, column ids, unique flag, root page).
- src/catalog/catalog_manager.\*: CRUD for index catalog entries and lookup helpers used by executor.
- src/storage/index/bplus_tree_node.h/.cpp (new): define node structs, layout helpers, and serialization routines.
- src/storage/index/bplus_tree.h/.cpp (new): implement search, insert, split, and merge plus iterators.
- src/storage/index/index_manager.h/.cpp (new): manage index lifecycle (create, drop, open) and bridge to storage layer.
- src/sql/ast.h and src/sql/dml_parser._ / src/sql/ddl_parser._: add AST nodes for CREATE INDEX, DROP INDEX, and ORDER BY clause on SELECT.
- src/engine/ddl_executor.\*: handle index DDL execution, including primary-key auto index creation during table creation.
- src/engine/dml_executor.\*: plan and index selection for predicates, range scan support, ORDER BY integration, uniqueness enforcement.
- src/cli/repl.\*: teach REPL help text about index commands and ORDER BY usage.
- Tests: add suites under tests/storage for B+ tree behavior, tests/catalog for index metadata, and extend SQL or engine tests for index-enabled queries.
- Docs: update README.md, docs/DEMO.md, docs/DEV_NOTES.md, docs/CODEX_LOG.md with indexing instructions and ORDER BY examples.

## Execution Plan

1. **Catalog and Config Prep (FR-025/FR-026)**

   - Extend catalog schema (structures, serialization) for index entries and expose new constants in config.
   - Update catalog manager to persist and load index metadata; add unit tests covering round-trip storage.

2. **B+ Tree Page Layout (FR-024)**

   - Define node formats (internal versus leaf), key storage strategy, and child or record pointer layout; wire serialization helpers and constants.
   - Write low-level tests for encoding and decoding nodes and leaf linkage.

3. **B+ Tree Core Operations (FR-024)**

   - Implement search, insert, split, and merge logic, including root elevation and redistribution.
   - Cover with unit tests for point lookups, sequential inserts, and delete or rebalance scenarios.

4. **Index Manager Integration (FR-024/FR-026)**

   - Create index manager responsible for index file creation, opening, and closing; expose APIs for insert, erase, and scan.
   - Add tests simulating multi-page trees and verifying persistence across reopen.

5. **SQL Parser and Executor Hooks for Index DDL (FR-026)**

   - Update tokenizer and parser to recognize CREATE INDEX or DROP INDEX syntax and add AST nodes.
   - Extend DDL executor to call index manager, update catalog, and enforce duplicate name checks; add parser and executor tests.

6. **Primary-Key Auto Index (FR-025)**

   - During CREATE TABLE, detect PRIMARY KEY definitions, seed an index catalog entry, and build the corresponding B+ tree.
   - Validate uniqueness constraints during INSERT or UPDATE via executor tests.

7. **Index-Aware DML Execution (FR-028)**

   - Enhance DML executor to detect equality or range predicates, choose index scans, and support BETWEEN or inequality filters.
   - Ensure DELETE or UPDATE leverage index scans and maintain index consistency; cover with engine tests.

8. **ORDER BY Support (FR-027)**

   - Extend parser or executor for single-column ORDER BY (ASC or DESC); prefer using existing index order else perform spill-safe in-memory sort.
   - Add SQL tests validating ordering with and without indexes.

9. **REPL, Docs, and Final QA**
   - Refresh REPL help or banner for index commands and ORDER BY usage; ensure [rows=...] messaging fits new flows.
   - Update documentation artifacts and rerun full test suite plus targeted index stress tests.

(extra mini steps for context):
Index Metadata Prep in DML Layer – Expose needed index catalog info inside DMLExecutor (lookup active indexes per table, basic helpers) without yet mutating trees.
Index Maintenance on INSERT/DELETE – On table modifications, update associated indexes (insert/delete of keys) and add regression tests covering multi-column tables.
UPDATE + Range Predicate Index Support – Teach DMLExecutor to pick index scans for equality/range WHERE clauses, keep indexes consistent on UPDATE, and verify via focused tests.
SQL Grammar + AST for ORDER BY – Extend AST and DML parser to capture single-column ORDER BY with ASC/DESC flags; add parser unit tests.
Executor ORDER BY Handling – Implement ORDER BY evaluation (prefer index order else in-memory sort) and surface results in REPL output; expand SELECT tests for ordering.
Documentation & Help Refresh – Update REPL help, README, DEMO, DEV_NOTES, CODEX_LOG for V0.5 features and rerun the full test suite for release confidence.

## Risks and Mitigations

- **Node split bugs:** Use exhaustive unit tests with deterministic seed data; log debug traces for splits and merges.
- **Catalog drift:** Centralize index metadata updates in catalog manager; add integration tests that recreate indexes across reopen.
- **Executor plan selection errors:** Start with simple heuristics (single predicate, single index) and assert fallback to table scan when ambiguous; document limitations.
- **ORDER BY memory pressure:** Provide config-tunable cap and fallback to chunked sorting; note constraints in docs for large result sets.

## Verification Checklist (pre-release)

- [ ] Catalog lists indexes (show tables or new index listing) and survives restart.
- [ ] B+ tree tests cover insert, search, delete across multiple levels without corruption.
- [ ] CREATE INDEX or DROP INDEX commands succeed or fail with appropriate messaging and catalog updates.
- [ ] Primary-key index auto-creates during CREATE TABLE and enforces uniqueness on INSERT or UPDATE.
- [ ] SELECT with WHERE uses index scans for equality and range predicates (verified via DEBUG logs or tests).
- [ ] ORDER BY returns correctly sorted results for ASC or DESC, with index-backed optimization when possible.
- [ ] Full test suite plus new index stress tests pass (for example 10000 row insert or select).

## Suggested Commit Cadence

1. After catalog and config groundwork with associated tests (Step 1).
2. After B+ tree layout plus core operations (Steps 2-3) once storage tests pass.
3. After index manager plus index DDL plumbing (Steps 4-5).
4. After primary-key auto index plus executor uniqueness enforcement (Step 6).
5. After index-aware DML and ORDER BY implementation with tests (Steps 7-8).
6. Final documentation and QA sweep (Step 9) before tagging V0.5.

Aim to keep commits small and narrated in CODEX_LOG or DEV_NOTES so future sessions can pick up the indexing narrative quickly.
