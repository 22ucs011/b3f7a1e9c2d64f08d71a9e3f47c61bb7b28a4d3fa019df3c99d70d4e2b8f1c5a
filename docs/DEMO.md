# Kizuna V0.6 – Demo Script

This walkthrough highlights the current SQL surface (DDL, indexed DML, advanced SELECT features, and schema evolution) plus the legacy storage tooling that still ships in the REPL. Use it as a lab script or talking points when walking someone through the project.

## 0. Prep

- Configure once: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
- Build & test: `cmake --build build --config Debug` then `build\Debug\run_tests.exe`
- Launch the REPL: `build\Debug\kizuna.exe`

On first run the REPL creates `build\Debug\database\...` (catalog, data, indexes, logs, temp) beside the executable.

## 1. Quick tour

```
help
```

Call out:

- `show tables` lists catalog entries (name, ids, root pages, column counts)
- `schema <table>` prints column metadata plus the original CREATE statement
- DDL covers `CREATE TABLE`, `DROP TABLE [IF EXISTS]`, and `ALTER TABLE ADD/DROP COLUMN`
- DML covers INSERT, SELECT with WHERE/LIMIT/ORDER BY, UPDATE with WHERE, DELETE with WHERE, TRUNCATE, DISTINCT, aggregates, and JOINs

Show the fresh catalog:

```
open
show tables
```

New databases report `Tables (0)`.

## 2. CREATE + schema

```
CREATE TABLE ook (id INT PRIMARY KEY,name VARCHAR(25), active BOOLEAN, joined DATE, nickname VARCHAR(16));
show tables
schema ook
```

Explain catalog (page 1) vs. table heap (root page shown in `show tables`).

## 3. INSERT and SELECT (projection, predicates, LIMIT, ORDER BY)

```
INSERT INTO ook VALUES (1, 'nice', TRUE,  '2023-06-01', 'ace');
INSERT INTO ook VALUES (2, 'not nice', FALSE, '2022-05-05', NULL);
INSERT INTO ook VALUES (3, 'still nice', TRUE, '2021-02-14', NULL);
SELECT name, active FROM ook WHERE active LIMIT 2;
```

Projection order matches the SELECT list; LIMIT stops scanning once satisfied.

```
SELECT id, joined FROM ook ORDER BY joined DESC;
SELECT id, name, active FROM ook ORDER BY active DESC, id ASC;
```

ORDER BY reuses an index when available; otherwise performs a stable multi-key in-memory sort.

```
SELECT DISTINCT active FROM ook ORDER BY active;
SELECT COUNT(*), COUNT(nickname), AVG(id) FROM ook;
```

DISTINCT and aggregates are evaluated inside the executor with NULL-aware semantics.

## 4. UPDATE with filters

```
UPDATE ook SET name = 'ally', nickname = NULL WHERE id = 1;
SELECT id, name, nickname FROM ook WHERE id = 1;
```

Updates are type-checked; short payload changes reuse space, longer rows relocate safely.

## 5. JOINs

```
CREATE TABLE badges (employee_id INT, badge VARCHAR(16));
INSERT INTO badges VALUES (1, 'mentor'), (1, 'coach'), (3, 'host');
SELECT o.name, b.badge FROM ook AS o INNER JOIN badges AS b ON o.id = b.employee_id ORDER BY o.id, b.badge DESC;
```

INNER JOIN materialises combined rows and honours aliases/ORDER BY expressions.

## 6. ALTER TABLE

```
ALTER TABLE ook ADD COLUMN title VARCHAR(32) DEFAULT 'n/a';
SELECT id, name, title FROM ook ORDER BY id;
ALTER TABLE ook DROP COLUMN nickname;
SELECT * FROM ook;
```

Schema migration rewrites the table heap, updates catalog metadata, and rebuilds indexes if needed.

## 7. Secondary indexes & indexed scans

```
CREATE INDEX idx_ook_name ON ook(name);
show tables
```

Point out the index entry in the catalog and the new file under `database\indexes\`.

```
SELECT id, name FROM ook WHERE name = 'ally';
SELECT id, name FROM ook WHERE name = 'ally' ORDER BY id DESC;
```

Highlight that the executor plans an index lookup (DEBUG log shows index usage) and integrates ORDER BY with index scans when possible.

```
DROP INDEX idx_ook_name;
```

Demonstrate catalog/index manager lifecycle.

## 8. DELETE vs TRUNCATE

```
DELETE FROM ook WHERE active = FALSE;
SELECT id, name, active FROM ook ORDER BY id;
TRUNCATE TABLE ook;
SELECT * FROM ook;
```

DELETE tombstones matching rows; TRUNCATE resets the heap and freelist pointers.

## 9. DROP TABLE cleanup

```
DROP TABLE IF EXISTS nope;
DROP TABLE badges;
DROP TABLE ook;
show tables
```

`DROP TABLE` without the clause raises an error; `IF EXISTS` is a no-op with a message.

## 10. Legacy storage commands (optional)

```
newpage DATA
write_demo 5
read_demo 5 0
status
```

Useful for discussing the V0.1 page layout: catalog and freelist share the same page manager API as user data.

## 11. Error showcase

- Syntax: `CREATE TABLE broken id INT);` → `[SYNTAX_ERROR]`
- Duplicate column: `CREATE TABLE dup (c INT, c INT);`
- Missing table: `DROP TABLE ghosts;` → `[TABLE_NOT_FOUND]`
- Arity: `INSERT INTO ook VALUES (1);`
- Constraint: `UPDATE ook SET name = NULL WHERE id = 1;`
- Duplicate index: `CREATE INDEX dup ON ook(name);` → `[DUPLICATE_KEY]`
- Aggregate misuse: `SELECT name, COUNT(*) FROM ook;` → `[INVALID_CONSTRAINT]`

## 12. Logging tips

```
loglevel DEBUG
```

Debug level prints every AST, executor call, and storage mutation. Drop back to INFO to reduce noise.
