# Kizuna V0.4 Implementation Plan

## Current Baseline (V0.3)
- Table heap supports chained DATA pages with insert, tombstone delete, truncate, and iterators that skip dead slots.
- SQL DML path implemented end-to-end (parser ? AST ? DML executor) for INSERT, SELECT *, DELETE (all rows), and TRUNCATE.
- REPL exposes `show tables`, `schema`, and routes SQL with friendly error messaging; SELECT output already column-aligned.
- Tests cover DML parsing/execution plus storage behavior; docs (`README`, `DEMO`, `DEV_NOTES`) updated through V0.3.

## V0.4 Functional Targets (SRS §2.1 V0.4)
| FR | Description | Key Considerations |
|----|-------------|--------------------|
| FR-017 | `SELECT` with explicit column list | Column projection, wildcard + named columns mix, validation against schema |
| FR-018 | `WHERE` comparisons (`= != < <= > >=`) | Typed comparison logic, numeric vs string, DATE comparisons |
| FR-019 | Logical operators (`AND OR NOT`) | Expression tree evaluation, short-circuiting |
| FR-020 | `DELETE` with WHERE | Reuse predicate engine over table heap deletes |
| FR-021 | `UPDATE` with WHERE | Row materialization, in-place update or rewrite, constraint checks |
| FR-022 | `LIMIT` clause | Apply after filtering and projection |
| FR-023 | `IS NULL` / `IS NOT NULL` | Null awareness in predicate evaluation |

**SRS Notes to Honor**
- DATE type arrives with V0.4; extend value parsing/encoding (likely stored as int64 UNIX timestamp).
- "Predicate pushdown" implies filtering during scan rather than post-materialization.
- UPDATE should modify rows without rewriting entire table when possible.

## Change Map (anticipated)
- `src/common/types.h`: extend `Value` variant to carry DATE and helper ops; add logical/compare utilities.
- `src/sql/ast.h`: add expression nodes (binary, unary, literal, column ref), projection lists, WHERE/LIMIT clauses, assignment list for UPDATE.
- `src/sql/tokenizer.*` / `dml_parser.*`: extend grammar for SELECT column lists, WHERE expressions, UPDATE syntax, LIMIT.
- Potential new `src/sql/expression.{h,cpp}` for AST helpers.
- `src/engine/expression_evaluator.{h,cpp}` (new) to evaluate expressions against rows using catalog metadata and `Value` helpers.
- `src/engine/dml_executor.{h,cpp}`: integrate predicate evaluation, projection, LIMIT slicing, UPDATE path.
- `src/storage/table_heap.{h,cpp}`: support row update API (rewrite within slot or reinsert) and filtered delete iteration helpers.
- `src/cli/repl.{h,cpp}`: support SELECT column list printing, LIMIT messaging, report rows touched for UPDATE/DELETE.
- Tests: new suites for expression parsing/eval, WHERE-enabled DML, DATE handling; extend REPL or integration smoke tests.
- Docs: refresh `docs/DEV_NOTES.md`, `docs/DEMO.md`, `README.md`, `docs/CODEX_LOG.md` with V0.4 behavior.

## Execution Plan
1. **Type & Value groundwork**
   - Add DATE literals parsing/formatting and store as 64-bit epoch.
   - Extend `Value` comparisons with null-aware semantics (three-valued logic where needed).
   - Update record encoder/decoder to cover DATE and ensure UPDATE can rebuild rows.

2. **Expression AST & Parser expansion**
   - Introduce expression tree structures for literals, column refs, unary/binary ops, function stubs (e.g., `NOW()` for defaults).
   - Update tokenizer to recognize `AND`, `OR`, `NOT`, `IS`, `NULL`, `LIMIT`, assignment operators.
   - Extend SELECT grammar for projection lists, optional WHERE, LIMIT; add UPDATE command grammar; expand DELETE to optional WHERE.
   - Add parser unit tests spanning precedence, parentheses, null checks, LIMIT parsing.

3. **Expression evaluator**
   - Implement evaluator that consumes expression AST + row context to yield `Value`.
   - Support comparison/logical operators with proper type promotion (INT/FLOAT), string compare, DATE compare.
   - Ensure evaluator respects NULL semantics (e.g., `NULL = 3` yields NULL -> treated as false in WHERE).

4. **Storage hooks for UPDATE**
   - Extend `TableHeap` with `Update` operation; if new payload fits existing slot reuse space, else mark tombstone and append new row (tracking relocated row id if needed).
   - Provide filtered scan helper returning `(RowHandle, Tuple)` so executor can perform deletes/updates based on predicates efficiently.
   - Add tests for update path (same-length + longer VARCHAR scenarios) and delete with filters.

5. **Executor upgrades**
   - SELECT: apply predicate during scan, project requested columns, enforce LIMIT.
   - DELETE: reuse predicate to target subset; return count.
   - UPDATE: parse assignment list, evaluate RHS expressions (literals, column references), validate types/constraints, call storage update.
   - Ensure error handling for missing columns, type mismatches, updates to read-only columns (e.g. duplicates?).

6. **REPL & UX polish**
   - Update help text for new SQL grammar.
   - Format SELECT results with chosen columns; show `[rows=n]` summary lines for UPDATE/DELETE.
   - Add simple EXPLAIN-style debug logging behind loglevel DEBUG to inspect predicates.

7. **Testing & Documentation**
   - Expand unit/integration tests covering FR-017..FR-023, including LIMIT edge cases and combined logical ops.
   - Refresh docs (`README`, `DEMO`, `DEV_NOTES`, `CODEX_LOG`) with V0.4 narrative.
   - Run full test suite and capture commands for release notes.

## Risks & Mitigations
- **Expression precedence bugs:** build exhaustive parser tests and cross-check with SQLite behavior where practical.
- **UPDATE space management:** risk of fragmentation or pointer invalidation; mitigate with reinsert + tombstone strategy and future vacuum roadmap note.
- **Type coercion edge cases:** define explicit promotion rules (INT?FLOAT, DATE only comparable with DATE) and enforce via evaluator tests.
- **Performance regression on scans:** predicate pushdown should keep cost similar; profile with loglevel DEBUG if large tables stall.

## Verification Checklist (pre-release)
- [ ] SELECT supports arbitrary projection order and respects LIMIT.
- [ ] WHERE handles comparison/logical/NULL ops with correct truth tables.
- [ ] DELETE/UPDATE honor WHERE filters and report affected row counts.
- [ ] DATE literals round-trip and compare correctly.
- [ ] Documentation + REPL help match final SQL feature set.
- [ ] `build\Debug\run_tests.exe` passes; targeted new suites cover expressions and filtered DML.

## Suggested Commit Cadence
1. After Step 1 (Value groundwork) with focused tests for DATE & comparisons.
2. After Step 2 + 3 (parser + evaluator) once expression unit tests pass.
3. After Step 4 + partial Step 5 (storage update API + executor DELETE/SELECT filters) before UPDATE wiring.
4. After completing UPDATE path and LIMIT handling.
5. Final documentation/tests sweep prior to tagging V0.4.

Aim for one to two commits per day during active development so the timeline looks organic (e.g., morning parser work, evening executor work). Include meaningful messages referencing FR numbers for clarity.
