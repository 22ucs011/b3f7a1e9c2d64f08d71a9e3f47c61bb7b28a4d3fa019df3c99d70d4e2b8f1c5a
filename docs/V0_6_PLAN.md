# Kizuna V0.6 Implementation Plan

## Current Baseline (V0.5)

- Secondary indexing is backed by B+ trees with catalog metadata, index manager lifecycle, and automatic primary-key indexes.
- DML executor maintains indexes across INSERT/UPDATE/DELETE, performs index-driven equality/range scans, and supports single-column ORDER BY with index reuse or stable in-memory sorting.
- Catalog schema persists tables/columns/indexes; REPL manages databases under a `database/` runtime tree and exposes index-aware help.
- Tests cover B+ tree behaviour, index catalog flows, index-maintained DML paths, ORDER BY sorting, and documentation has been refreshed for the V0.5 surface.

## V0.6 Functional Targets (SRS Section 2.1 – Advanced Queries & Schema Modifications)

| FR     | Description                                      | Key Considerations                                                                 |
|--------|--------------------------------------------------|-------------------------------------------------------------------------------------|
| FR-029 | ORDER BY multiple columns                        | Stable sort or index reuse across multi-key ordering; ASC/DESC mix support         |
| FR-030 | ALTER TABLE ADD/DROP COLUMN                      | Catalog mutation, table heap storage evolution, default handling                   |
| FR-031 | INNER JOIN support                               | Nested-loop join baseline; row materialization; future hash join hook              |
| FR-032 | Aggregates COUNT/SUM/AVG/MIN/MAX                 | Type-aware accumulation, NULL semantics, integration with GROUP-less SELECT        |
| FR-033 | DISTINCT keyword                                 | Deduplicate result sets (projection columns) using sort + unique or hash set       |

**SRS Notes to Honor**

- Multi-column ORDER BY must accept mixed direction (ASC/DESC) lists.
- ALTER TABLE should update catalog metadata, rebuild table files if needed, and honour existing data (NULL/default for new columns).
- JOIN implementation starts with nested-loop joins; hash join is a stretch goal. JOIN output must materialize combined rows.
- Aggregates must respect NULL handling (COUNT vs others) and numeric precision (use 64-bit accumulators/double).
- DISTINCT can be implemented via sort/unique; document performance considerations.

## Change Map (anticipated)

- `src/common/config.h`: expose tunables for ALTER TABLE (e.g., default column fill policy) and sort/aggregation limits.
- `src/sql/ast.h`: extend SELECT AST with ORDER BY list (vector), DISTINCT flag, aggregate function nodes, JOIN clauses, and ALTER TABLE AST nodes.
- `src/sql/dml_parser.cpp`: parse multi-column ORDER BY, DISTINCT, aggregate calls, simple INNER JOIN syntax, and aggregate-only SELECT validation.
- `src/sql/ddl_parser.cpp`: add ALTER TABLE ADD/DROP COLUMN grammar.
- `src/catalog/schema.*`: add column metadata versioning, default value persistence, tracking for dropped columns.
- `src/catalog/catalog_manager.*`: implement ALTER TABLE orchestration (catalog mutation + data migration) and JOIN metadata utilities.
- `src/storage/table_heap.*`: support column addition/removal (data migration helpers) and row materialization for joins.
- `src/engine/dml_executor.*`: 
  - extend ORDER BY planning for multiple keys and mixed directions.
  - evaluate DISTINCT using sort/unique or hash set.
  - compute aggregates (COUNT/SUM/AVG/MIN/MAX) including without GROUP BY.
  - implement nested-loop join executor node and integrate into SELECT pipeline.
- `src/engine/ddl_executor.*`: handle ALTER TABLE ADD/DROP COLUMN (catalog update + data rewrite).
- `src/cli/repl.*`: update help text for ALTER TABLE, DISTINCT, aggregates, JOIN, and multi-column ORDER BY examples.
- Tests: 
  - new SQL parser cases (ALTER TABLE, JOIN, DISTINCT, ORDER BY multi-columns, aggregates).
  - engine tests for ALTER TABLE migration, aggregate correctness, DISTINCT semantics, join results.
  - table heap migration tests and catalog persistence tests.
- Docs: update README, DEMO, DEV_NOTES, CODEX_LOG with V0.6 features.

## Execution Plan

1. **AST & Parser Foundations (FR-029/FR-030/FR-031/FR-032/FR-033)**
   - Extend AST structures for ORDER BY list, DISTINCT flag, aggregate nodes, join clauses, and ALTER TABLE statements.
   - Update DML/DDL parser to build new AST nodes; add unit tests for parsing syntax (multi-column ORDER BY, DISTINCT, aggregate calls, INNER JOIN, ALTER TABLE).

2. **Catalog & Storage Migration Support (FR-030)**
   - Enhance catalog manager to add/drop column metadata with versioning.
   - Implement table heap migration utilities for ADD COLUMN (append default/NULLs) and DROP COLUMN (rewrite rows without column).
   - Write catalog + storage tests that alter tables and validate data integrity.

3. **Multi-column ORDER BY & DISTINCT (FR-029, FR-033)**
   - Update DML executor sort planner: re-use indexes when ORDER BY prefix matches index columns, otherwise perform multi-key stable sort with ASC/DESC handling.
   - Implement DISTINCT (sort & unique on projection columns or hash-based) and integrate into SELECT pipeline.
   - Add engine tests covering multi-column ordering (with/without indexes) and DISTINCT correctness.

4. **Aggregate Functions (FR-032)**
   - Implement COUNT, SUM, AVG, MIN, MAX evaluation with type coercions and NULL semantics.
   - Add aggregate result formatting (single row) and ensure compatibility with DISTINCT and ORDER BY.
   - Provide tests for aggregates over int/double/date fields and mixed NULLs.

5. **ALTER TABLE Execution (FR-030)**
   - Implement ALTER TABLE ADD COLUMN and DROP COLUMN in DDL executor (catalog update + data migration).
   - Handle default value injection (NULL if none specified).
  - Write integration tests verifying schema changes persist and DML after migration behaves as expected.

6. **JOIN Implementation (FR-031)**
   - Build nested loop join executor: plan join order, evaluate ON predicates, produce combined rows.
   - Extend SELECT pipeline to handle joins (table alias resolution, column binding).
   - Add engine tests for INNER JOIN cases (1:N, N:N) and ensure join output columns align with SELECT list.

7. **REPL & Documentation Updates**
   - Update REPL help banner and documentation artifacts (README, DEMO, DEV_NOTES, CODEX_LOG) with ALTER TABLE, JOIN, aggregates, DISTINCT, and multi-column ORDER BY usage.
   - Provide demo script updates to exercise new features.

8. **QA & Performance Pass**
   - Rerun the full test suite plus new stress tests (table alteration, join scenarios, aggregate workloads).
   - Document current limitations (e.g., only INNER JOIN, no GROUP BY yet).

## Risks and Mitigations

- **Schema migration corruption (ALTER TABLE):** Mitigate with snapshot/migration helpers, thorough tests on ADD/DROP column, and backup before rewrite.
- **Join performance:** Nested-loop joins may be slow on large tables; document expected limits and gate with config guard.
- **Aggregate precision:** Ensure 64-bit accumulators, handle overflow; add tests with large numeric inputs.
- **DISTINCT memory usage:** Sorting large result sets may consume memory; add config cap and warn in docs.
- **Parser complexity:** Keep grammar changes incremental; rely on unit tests for new syntax.

## Verification Checklist (pre-release)

- [x] Parser & AST handle ALTER TABLE, DISTINCT, aggregates, multi-column ORDER BY, and INNER JOIN syntax.
- [x] Catalog updates for ADD/DROP COLUMN persist and data migration retains existing rows.
- [x] DML executor returns correctly sorted results for multi-column ORDER BY (ASC/DESC mix) with and without indexes.
- [x] DISTINCT removes duplicates across projection columns and interacts correctly with LIMIT/ORDER BY.
- [x] Aggregates (COUNT/SUM/AVG/MIN/MAX) produce correct results, respecting NULL semantics.
- [x] INNER JOIN returns expected row combinations and column binding works with aliases.
- [x] REPL help/documentation updated; demo script covers new features.
- [x] Full test suite passes plus targeted join/aggregate/alter-table stress tests.

## Suggested Commit Cadence

1. Parser & AST extensions with unit tests.
2. Catalog/table heap migration support for ALTER TABLE.
3. ORDER BY (multi-column) + DISTINCT execution updates.
4. Aggregate function evaluation and tests.
5. ALTER TABLE executor integration and end-to-end tests.
6. JOIN executor implementation and tests.
7. Documentation/REPL updates and final QA pass.

Keep commits small, reference SRS FR identifiers, and log progress in CODEX_LOG/DEV_NOTES for continuity.
