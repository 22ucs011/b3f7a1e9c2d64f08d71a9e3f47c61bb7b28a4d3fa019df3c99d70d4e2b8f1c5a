# Kizuna Post-V0.6 Improvement Plan

This document captures follow-up work after the V0.6 milestone. Each item from the improvement backlog is triaged into **Now** (tackle before the upcoming demo and to stabilize the release) and **Later** (requires deeper design or can follow the interview demo). For every item, the plan outlines *what to do*, *how to approach it*, and *how to verify the work*.

## Prioritization Snapshot

| Area | Priority | Rationale |
| --- | --- | --- |
| SQL syntax & error reporting | **Now** | Low-risk polish that improves REPL UX for the demo. |
| JOIN & aggregation performance | Later | Requires iterator refactors; impact is workload-driven, not demo-critical. |
| Schema evolution robustness | **Now** | Reduces risk of data loss during ALTER TABLE showcase. |
| Indexing & ORDER BY extensions | Later | Needs catalog/index refactors; no immediate demo blocker. |
| Catalog consistency features | **Now** | Lightweight safeguards; ensures catalog health when demoing ALTER/DDL flows. |
| Testing & coverage | **Now** | Locks in regressions before tackling perf-oriented items. |
| Code quality & documentation | **Now** | Keeps materials aligned for interviews and handoffs. |
| Usability (REPL/UI polish) | Later* | Optional sparkle—consider if time remains before interview. |
| Configuration & logging | Split | Validate configs now; defer log rotation plumbing. |
| Non-functional requirements | Split | Profile memory now for talking points; defer crash recovery to a dedicated milestone. |

## Detailed Work Items

### 1. SQL Syntax & Error Reporting (Now)
- **Goal:** Provide contextual parser/executor errors (missing tables/columns, ambiguous bindings, type mismatches) and broaden SQL edge-case tests.
- **Approach:**
  - Thread table/column metadata into `QueryException` contexts and map status codes to REPL-friendly messages.
  - Add binder-style validation step that resolves identifiers before execution, enabling better diagnostics.
  - Expand parser and executor tests to cover complex WHERE predicates, nested expressions, and multi-table column references.
- **Verification:** Unit tests for parser/binder failures plus REPL integration tests asserting specific error strings.

### 2. JOIN & Aggregation Performance (Later)
- **Goal:** Reduce memory pressure for INNER JOIN and aggregate queries.
- **Approach:** Prototype iterator-based streaming (page-at-a-time outer scan + indexed inner probe) and accumulator push-down for COUNT/SUM/AVG. Requires reworking executor interfaces and buffer management.
- **Verification:** Introduce micro-benchmarks comparing current nested-loop execution with streaming mode across varying table sizes.

### 3. Schema Evolution Robustness (Now)
- **Goal:** Make `ALTER TABLE ADD/DROP COLUMN` resilient to interruptions and protect indexes during schema changes.
- **Approach:**
  - Implement a two-phase migration: copy data into `database/backup/`, rewrite table heap, validate, then swap; on failure, rollback using backup.
  - When dropping indexed columns, rebuild affected indexes or block the operation with a clear error.
  - Add migration logs to aid debugging.
- **Verification:** Integration tests simulating failures mid-migration and ensuring indexes remain consistent; manual kill tests during ALTER TABLE and validating automatic restore.

### 4. Indexing & ORDER BY Extensions (Later)
- **Goal:** Support composite indexes and gather index statistics for better planning.
- **Approach:** Extend catalog schema to store multi-column index metadata, update B+ Tree key encoders to handle tuples, and add a lightweight stats collector (row counts, distinct keys, last used). Revise ORDER BY planner to match prefixes against composite indexes.
- **Verification:** Engine tests covering ORDER BY on multiple columns with and without composite indexes plus planner unit tests ensuring stats guide scan selections.

### 5. Catalog Consistency (Now)
- **Goal:** Detect catalog corruption early and track schema versions.
- **Approach:**
  - Implement `CHECK CATALOG` command (and optional startup validation) that scans `__tables__`, `__columns__`, and `__indexes__` for orphaned or dangling references.
  - Add `schema_version` column to catalog tables; bump version on every DDL change and log it for migrations.
  - Optionally schedule a periodic background validation hook invoked from the REPL or maintenance scripts.
- **Verification:** Tests that intentionally corrupt catalog entries (via fixtures) and confirm the checker reports issues; version counters asserted in DDL tests.

### 6. Testing & Coverage (Now)
- **Goal:** Ensure every feature path (DDL → DML → JOIN/aggregates → ALTER) is validated end-to-end.
- **Approach:**
  - Build integration scenarios using the REPL harness to simulate realistic workflows (empty tables, NULL-heavy data, large datasets).
  - Extend the Python benchmark script family to include SELECT/JOIN/aggregate timings for regression tracking.
  - Track coverage deltas in CI to guard against regressions when adding future optimizations.
- **Verification:** New integration tests in `tests/` plus automated benchmark outputs stored in `/docs/perf/` for trend comparison.

### 7. Code Quality & Documentation (Now)
- **Goal:** Keep public APIs understandable and docs aligned with current behavior.
- **Approach:** Audit headers for missing docstrings on public classes/functions, sprinkle clarifying comments around complex algorithms (JOIN executor, index maintenance), and sync README/DEMO snippets with latest syntax. Record major decisions in `DEV_NOTES` to help during interviews.
- **Verification:** Documentation lint (via CI) and reviewer checklist ensuring every modified module contains updated comments/examples.

### 8. Usability Enhancements (Later*)
- **Goal:** Upgrade REPL usability (command completion, better help, result paging).
- **Approach:** Evaluate third-party line-editing libraries (e.g., linenoise) for auto-complete, enrich `help` output with categorized commands, and add paging/truncation for wide results. Mark as stretch tasks for spare cycles before the interview.
- **Verification:** Manual REPL smoke tests plus scripted snapshots showing improved UX.

### 9. Configuration & Logging (Split)
- **Config Validation (Now):** On startup, validate page size, cache size, and other tunables; warn or abort when unsupported. Tests should feed invalid configs and assert friendly diagnostics.
- **Log Rotation (Later):** Design a rotation policy (size-based or daily) and implement archival/purge logic respecting the `database/logs/` layout. Include stress tests that generate large logs and confirm retention rules.

### 10. Non-functional Requirements (Split)
- **Memory Usage (Now):** Profile buffer pool, JOIN, and aggregate paths using lightweight instrumentation; expose stats via REPL (`stats memory`). Use findings as talking points for the interview.
- **Crash Recovery (Later):** Scope a future milestone for WAL or redo/undo logging. Outline dependencies (page logging, transaction IDs, recovery bootstrap sequence) and plan incremental delivery (catalog recovery first, then data).

## Interview Demo Prep (Next 7 Days)

1. **Golden Script:** Refresh `docs/DEMO.md` with a deterministic walkthrough (open DB → CREATE TABLE → INSERT → SELECT with JOIN/aggregates → ALTER TABLE → catalog check). Practice until execution feels automatic.
2. **Health Checklist:** Before the demo, run `run_tests`, new integration suites, and catalog validation to produce a “pre-flight” log you can mention.
3. **Performance Story:** Capture benchmark numbers (e.g., from `scripts/perf/index_benchmark.py`) for different row counts plus any new memory profiling data; keep them in a short slide or README appendix.
4. **Talking Points:** Emphasize the V0.6 highlights (JOIN, aggregates, ALTER TABLE) and the “Now” hardening work underway. For “Later” items, describe the roadmap so interviewers see continuous progress.
5. **Live Demo Setup:** Pre-build binaries, warm up the database, and keep backup scripts ready in case you need to reset quickly during the interview.
