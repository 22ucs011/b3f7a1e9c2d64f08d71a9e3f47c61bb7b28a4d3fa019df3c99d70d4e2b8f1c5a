# Kizuna V0.2 Implementation Plan

## Current Baseline (V0.1)
- Storage layer in place: FileManager, Page, and PageManager handle slotted pages with freelist support.
- Common utilities: configuration, typed exceptions with StatusCode, logging infrastructure, and basic record encoding/decoding.
- CLI REPL offers manual page operations only; no SQL awareness yet.

## V0.2 Functional Targets
| FR | Description | Required Capabilities |
|----|-------------|-----------------------|
| FR-007 | CREATE TABLE with supported data types | Definition of TableSchema, column metadata persistence, table id allocator, table file creation |
| FR-008 | DROP TABLE (cascade) | Catalog updates, physical table file removal, page freelist integration |
| FR-009 | Table metadata storage/retrieval | Catalog pages (__tables__, __columns__, __indexes__), serialization + caching |
| FR-010 | Basic SQL parser for DDL | Tokenizer + recursive-descent (or Pratt) parser for CREATE/DROP TABLE |
| FR-011 | Schema validation & constraints | Column constraints (NOT NULL, PRIMARY KEY, UNIQUE, DEFAULT literal) validation pipeline |

## File-Level Change Map
- src/common/config.h: add catalog identifiers, filesystem paths, and default table file prefix.
- src/common/types.h: add catalog_id_t, table_id_t, column constraint enums, and lightweight ColumnDef/TableDef structs.
- src/common/exception.{h,cpp}: extend StatusCode mappings for catalog/query errors (e.g., TABLE_EXISTS, INVALID_CONSTRAINT).
- src/storage/file_manager.{h,cpp}: support dedicated data files per table (file handle registry) and helper to delete files.
- src/storage/page_manager.{h,cpp}: ensure metadata/catalog pages are initialized and expose helper to load catalog root.
- New src/catalog/ module:
  - schema.h/.cpp: Column, TableSchema, serialization to/from bytes.
  - catalog_manager.h/.cpp: manages catalog tables, table id allocation, schema CRUD.
- New src/sql/ddl_parser.h/.cpp: tokenizer + parser returning AST for CREATE/DROP TABLE.
- src/sql/ast.h: define AST node structs for DDL.
- New src/engine/ddl_executor.h/.cpp: orchestrate catalog and storage actions for DDL commands.
- src/cli/repl.{h,cpp}: route SQL commands to parser/executor; keep legacy debug commands.
- Tests:
  - tests/catalog/catalog_manager_test.cpp
  - tests/sql/ddl_parser_test.cpp
  - tests/engine/ddl_executor_test.cpp
  - Extend existing record/schema tests as needed.
- Documentation: append V0.2 progress in docs/DEV_NOTES.md, update docs/CODEX_LOG.md, and add DDL usage examples to docs/DEMO.md.

## Incremental Implementation Outline
1. **Schema groundwork**: update configs/types/exceptions; create catalog schema structs.
2. **Catalog storage**: build CatalogManager with metadata pages, ensure persistence + reload.
3. **DDL parser & AST**: implement tokenizer and grammar for CREATE/DROP TABLE with constraints.
4. **DDL executor**: translate AST into catalog + storage updates, create physical table files.
5. **REPL integration**: add SQL command path, simple input loop for multi-word statements, pretty-print catalog state.
6. **Testing & docs**: unit tests per module, document workflows, update changelog/log.

## Testing Strategy
- Unit tests for schema serialization/deserialization and constraint validation.
- Parser tests covering happy-path DDL as well as syntax errors.
- Executor tests verifying catalog entries, file creation, and cascade drop behaviour (using temp/ sandbox).
- End-to-end smoke via REPL integration test (if feasible) or documentation-guided manual steps.
