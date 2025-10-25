# Kizuna V0.3 Implementation Plan

## Current Baseline (V0.2)
- SQL DDL path complete: parser -> AST -> DDL executor -> catalog/table storage.
- Catalog persists table/column metadata in `__tables__` / `__columns__` and mints table files plus root pages.
- REPL routes CREATE/DROP TABLE and exposes catalog inspection (`schema`, `show tables`).
- Storage layer supports slotted pages with freelist management; record encode/decode handles core scalar types (INT, BIGINT, DOUBLE, BOOLEAN, VARCHAR).

## V0.3 Functional Targets (SRS section 2.1, V0.3)
| FR | Description | Implementation Focus |
|----|-------------|----------------------|
| FR-012 | `INSERT INTO` single and multi-row | SQL DML parsing, value binding -> record builder, sequential page insert with overflow |
| FR-013 | `SELECT *` (full table scan) | Table scan iterator, row materialization, REPL output |
| FR-014 | `DELETE` without WHERE | Page-level tombstone marking, free-space reuse, executor plumbing |
| FR-015 | `TRUNCATE TABLE` | Fast table reset: catalog counters, page deallocation, table file compaction |
| FR-016 | Record iteration/retrieval | Null bitmap-aware record format, iterator API across heap pages |

SRS technical notes to honor:
- Record format must include a null bitmap (see V0.3 technical specifications).
- Sequential inserts with overflow pages; maintain page linkage or catalog-managed heap.
- Deleted rows should become tombstones rather than immediate compaction.
- BOOLEAN type is flagged as a V0.3 addition; confirm support end-to-end (parser literals, record encoding, presentation).

## Change Map (anticipated)
- `src/common/types.h`: extend value representation for DML literals, add iterator/result row structs.
- `src/common/exception.{h,cpp}`: add status codes for DML errors (for example `COLUMN_COUNT_MISMATCH`, `TABLE_EMPTY`).
- `src/storage/record.{h,cpp}`: redesign encoding with null bitmap plus typed writers/readers; support boolean literals consistently.
- `src/storage/page.{h}`: add APIs for append/erase with tombstone flags, track continuation/next page identifier.
- `src/storage/page_manager.{h,cpp}`: helpers for table heap growth (allocate chain, maintain metadata) and page-iteration utilities.
- New `src/storage/table_heap.{h,cpp}` (or similar): encapsulate table-level record insertion, scanning, deletion, truncate.
- `src/sql/ast.h`: add DML statement nodes (InsertStmt, SelectStmt, DeleteStmt, TruncateStmt).
- New or extended parser (`src/sql/dml_parser.{h,cpp}` or merged module) to tokenize/parse INSERT/SELECT/DELETE/TRUNCATE per SRS grammar.
- `src/engine`: add `dml_executor.{h,cpp}` translating DML AST into storage operations; integrate with existing DDL executor.
- `src/cli/repl.{h,cpp}`: route DML statements, add output formatting for SELECT, REPL-friendly messaging for INSERT/DELETE/TRUNCATE.
- Tests: new suites under `tests/sql/`, `tests/engine/`, `tests/storage/` covering DML parsing, heap operations, tombstones, truncate semantics.
- Docs: update `docs/DEV_NOTES.md`, `docs/DEMO.md`, `docs/CODEX_LOG.md`, and consider adding a `docs/SQL_REFERENCE.md` focused on DML commands.

## Incremental Execution Plan
1. **Record format and storage prep**
   - Introduce null bitmap and column metadata-driven encoding/decoding in record utilities.
   - Extend `Page` to expose slot tombstone flag and next-page pointer for overflow chains.
   - Validate BOOLEAN handling across encode/decode and ensure catalog metadata stores required type information.

2. **Table heap abstraction**
   - Create a table heap manager that owns root page, sequential allocation, and scan cursors (covers FR-012 and FR-016).
   - Implement insert path with automatic overflow allocation via `PageManager`.
   - Add delete (tombstone) and truncate (bulk free plus metadata reset) actions.

3. **SQL layer enhancements**
   - Extend AST and parser to cover INSERT (with/without column list, multi-row), SELECT * (no WHERE), DELETE (no WHERE), TRUNCATE.
   - Reuse lexer tokens; ensure error messaging mirrors SRS syntax expectations.

4. **Execution pipeline**
   - Implement a DML executor bridging AST to the table heap.
   - Manage value binding using catalog column order, enforce column count/type checks (raising SRS-aligned errors).
   - Provide SELECT materialization as a vector of rows or streaming iterator for REPL consumption.

5. **REPL and UX**
   - Integrate DML executor invocation, pretty-print SELECT results, and user guidance for destructive operations.
   - Add helper commands if needed (for example `rows <table>`) while staying inside V0.3 scope.

6. **Testing and validation**
   - Unit tests: record encoding null cases, table heap insert/delete/truncate, tombstone reuse.
   - Parser tests: various INSERT layouts, malformed statements, multi-row batches.
   - Executor tests: end-to-end insert/select/delete/truncate using temporary catalogs.
   - Integration smoke: REPL script verifying FR-012 through FR-015 scenarios (automate if feasible).

7. **Documentation and demo refresh**
   - Update developer notes with new modules and data flow.
   - Extend the demo script to showcase INSERT/SELECT/DELETE/TRUNCATE cycle.
   - Record outcomes and commands in `docs/CODEX_LOG.md` for session continuity.

## Risk and Mitigation Snapshot
- **Complexity of null bitmap retrofit:** mitigate by introducing transitional encode/decode layer and a migration step for existing catalog records (or version bump requiring table recreation).
- **Page overflow logic regression:** cover with stress tests inserting until multiple pages link; instrument free-space tracking.
- **SELECT output volume:** start with small interactive print; consider pagination later (deferred beyond V0.3).

## Verification Checklist (pre-release)
- [ ] All FR-012 through FR-016 scenarios demonstrated via automated tests.
- [ ] BOOLEAN values insert and select correctly (true/false literals, integer coercion).
- [ ] DELETE marks tombstones and subsequent inserts reuse freed space.
- [ ] TRUNCATE clears data pages and resets catalog row counts without recreating the table entry.
- [ ] REPL `help` updated to enumerate DML capabilities.
- [ ] Demo document validated end-to-end against a fresh database.
