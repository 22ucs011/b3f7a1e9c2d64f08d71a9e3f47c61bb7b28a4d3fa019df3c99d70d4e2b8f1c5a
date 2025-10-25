# Kizuna - Database Management System
## Software Requirements Specification (SRS)

**Version:** 1.0  
**Project Duration:** 30 Days  
**Target:** Resume-worthy DBMS implementation for freshers  
**Technology Stack:** C++17, Web Interface (HTML/CSS/JS), Assembly optimizations  

---

## 1. PROJECT OVERVIEW

### 1.1 Purpose
Kizuna is a lightweight, educational database management system inspired by SQLite architecture. It demonstrates core database concepts including storage management, indexing, query processing, and transaction handling.

### 1.2 Scope
- File-based relational database system
- SQL-like query language support
- B+ Tree indexing implementation
- Basic transaction management
- Web-based demonstration interface
- Performance optimization with assembly code

### 1.3 Success Criteria
- Handle tables with 10,000+ records efficiently
- Support concurrent read operations
- Demonstrate ACID properties
- Web deployable for interview presentations
- Extensible architecture for future enhancements

---

## 2. FUNCTIONAL REQUIREMENTS

### 2.1 Version Roadmap

#### **V0.1 - Foundation Infrastructure (Days 1-3)**
**Priority:** Critical  
**Dependencies:** None  

**Requirements:**
- **FR-001:** File-based storage system with configurable page size (default 4KB)
- **FR-002:** Page manager for allocation, deallocation, and caching
- **FR-003:** Basic record format supporting fixed and variable length data
- **FR-004:** Error handling system with custom exception classes
- **FR-005:** Logging system with configurable log levels (DEBUG, INFO, WARN, ERROR)
- **FR-006:** Command-line interface with basic REPL functionality

**Technical Specifications:**
```cpp
// Page Structure
struct Page {
    uint32_t page_id;
    uint16_t record_count;
    uint16_t free_space_offset;
    char data[PAGE_SIZE - 8];
};

// Error Handling
class DBException : public std::exception {
    std::string message;
    ErrorCode code;
};
```

**Deliverables:**
- Basic file I/O operations
- Memory page management
- CLI skeleton with command parsing
- Unit tests for core components

---

#### **V0.2 - Basic Table Operations (Days 4-6)**
**Priority:** Critical  
**Dependencies:** V0.1  

**Requirements:**
- **FR-007:** CREATE TABLE with supported data types
- **FR-008:** DROP TABLE with cascade option
- **FR-009:** Table metadata storage and retrieval
- **FR-010:** Basic SQL parser for DDL commands
- **FR-011:** Schema validation and constraint checking

**Supported Data Types:**
```sql
-- Basic Types (V0.2)
INTEGER     -- 4 bytes, signed
FLOAT       -- 4 bytes, IEEE 754
VARCHAR(n)  -- Variable length string, max n chars
BOOLEAN     -- 1 byte, 0/1 (V0.3 addition)
DATE        -- 8 bytes, stored as timestamp (V0.4 addition)
```

**DDL Syntax:**
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INTEGER,
    created_date DATE DEFAULT NOW()
);

DROP TABLE users;
```

**Technical Specifications:**
- Catalog tables: `__tables__`, `__columns__`, `__indexes__`
- Schema stored in dedicated catalog pages
- Table files named as `table_<id>.db`

---

#### **V0.3 - Basic CRUD Operations (Days 7-10)**
**Priority:** Critical  
**Dependencies:** V0.2  

**Requirements:**
- **FR-012:** INSERT INTO single and multiple row support
- **FR-013:** SELECT * with full table scan
- **FR-014:** DELETE without WHERE clause
- **FR-015:** TRUNCATE TABLE for efficient table clearing
- **FR-016:** Basic record iteration and retrieval

**DML Syntax:**
```sql
INSERT INTO users VALUES (1, 'John Doe', 'john@email.com', 25, '2024-01-15');
INSERT INTO users (name, email) VALUES ('Jane Doe', 'jane@email.com');

SELECT * FROM users;

DELETE FROM users;
TRUNCATE TABLE users;
```

**Technical Specifications:**
- Record format with null bitmap
- Sequential record insertion
- Page overflow handling
- Tombstone marking for deleted records

---

#### **V0.4 - Query Filtering & Updates (Days 11-14)**
**Priority:** High  
**Dependencies:** V0.3  

**Requirements:**
- **FR-017:** SELECT with column specification
- **FR-018:** WHERE clause with comparison operators (=, !=, <, <=, >, >=)
- **FR-019:** WHERE clause with logical operators (AND, OR, NOT)
- **FR-020:** DELETE with WHERE clause
- **FR-021:** UPDATE with WHERE clause
- **FR-022:** LIMIT clause for result limiting
- **FR-023:** IS NULL / IS NOT NULL operators

**Query Syntax:**
```sql
SELECT id, name, email FROM users WHERE age > 18 AND name IS NOT NULL;
UPDATE users SET age = 26 WHERE id = 1;
DELETE FROM users WHERE age < 18;
SELECT * FROM users LIMIT 10;
```

**Technical Specifications:**
- Expression evaluation engine
- Predicate pushdown optimization
- Record filtering during scan
- Update-in-place when possible

---

#### **V0.5 - Indexing & Sorting (Days 15-18)**
**Priority:** High  
**Dependencies:** V0.4  

**Requirements:**
- **FR-024:** B+ Tree implementation for indexing
- **FR-025:** Primary key automatic indexing
- **FR-026:** CREATE INDEX / DROP INDEX support
- **FR-027:** ORDER BY single column (ASC/DESC)
- **FR-028:** Index-based lookups and range queries

**Index Syntax:**
```sql
CREATE INDEX idx_users_age ON users(age);
CREATE UNIQUE INDEX idx_users_email ON users(email);
DROP INDEX idx_users_age;

SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users WHERE age BETWEEN 18 AND 65;
```

**B+ Tree Specifications:**
- Node size: Configurable (default 256 entries)
- Leaf nodes: Store record pointers
- Internal nodes: Store keys and child pointers
- Split/merge algorithms for balance maintenance

---

#### **V0.6 - Advanced Queries & Schema Modifications (Days 19-22)**
**Priority:** Medium  
**Dependencies:** V0.5  

**Requirements:**
- **FR-029:** ORDER BY multiple columns
- **FR-030:** ALTER TABLE ADD COLUMN / DROP COLUMN
- **FR-031:** Simple JOIN operations (INNER JOIN initially)
- **FR-032:** Basic aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- **FR-033:** DISTINCT keyword support

**Advanced Query Syntax:**
```sql
SELECT COUNT(*) FROM users WHERE age > 18;
SELECT AVG(age), MIN(age), MAX(age) FROM users;
SELECT DISTINCT age FROM users;

ALTER TABLE users ADD COLUMN phone VARCHAR(15);
ALTER TABLE users DROP COLUMN phone;

SELECT u.name, p.title 
FROM users u INNER JOIN posts p ON u.id = p.user_id;
```

**Join Implementation:**
- Nested loop join (initial implementation)
- Hash join (if time permits)
- Join result materialization

---

#### **V0.7 - Transaction Management (Days 23-25)**
**Priority:** Medium  
**Dependencies:** V0.6  

**Requirements:**
- **FR-034:** BEGIN TRANSACTION support
- **FR-035:** COMMIT and ROLLBACK operations
- **FR-036:** Write-ahead logging (WAL)
- **FR-037:** Basic lock management
- **FR-038:** Isolation level support (READ UNCOMMITTED, READ COMMITTED)

**Transaction Syntax:**
```sql
BEGIN TRANSACTION;
UPDATE users SET age = age + 1 WHERE id = 1;
INSERT INTO audit_log VALUES (1, 'Updated user age', NOW());
COMMIT;

BEGIN TRANSACTION;
DELETE FROM users WHERE id = 999;
ROLLBACK;
```

**ACID Implementation:**
- **Atomicity:** WAL-based rollback
- **Consistency:** Constraint validation
- **Isolation:** Basic locking
- **Durability:** Force-write on commit

---

#### **V0.8 - Advanced Features (Days 26-28)**
**Priority:** Low  
**Dependencies:** V0.7  

**Requirements:**
- **FR-039:** GROUP BY and HAVING clauses
- **FR-040:** Multiple join types (LEFT, RIGHT, FULL OUTER)
- **FR-041:** Subqueries (WHERE EXISTS, IN clauses)
- **FR-042:** Basic query optimization
- **FR-043:** EXPLAIN QUERY PLAN functionality

**Advanced SQL:**
```sql
SELECT age, COUNT(*) as count 
FROM users 
GROUP BY age 
HAVING COUNT(*) > 1;

EXPLAIN SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users);
```

---

#### **V0.9 - Performance & Web Interface (Days 29-30)**
**Priority:** Medium  
**Dependencies:** V0.8  

**Requirements:**
- **FR-044:** Assembly optimizations for critical sections
- **FR-045:** Performance benchmarking suite
- **FR-046:** Web-based query interface
- **FR-047:** Query result visualization
- **FR-048:** Database statistics and monitoring

**Web Interface Features:**
- SQL query editor with syntax highlighting
- Result table display
- Query execution time tracking
- Database schema browser
- Performance metrics dashboard

---

## 3. NON-FUNCTIONAL REQUIREMENTS

### 3.1 Performance Requirements
- **NFR-001:** Support tables with up to 100,000 records
- **NFR-002:** Query response time < 100ms for indexed lookups
- **NFR-003:** Transaction commit time < 10ms
- **NFR-004:** Memory usage < 100MB for typical workloads
- **NFR-005:** Disk I/O operations batched for efficiency

### 3.2 Reliability Requirements
- **NFR-006:** 99.9% uptime for read operations
- **NFR-007:** Data consistency maintained across crashes
- **NFR-008:** Transaction rollback success rate 100%
- **NFR-009:** Recovery time < 5 seconds after crash

### 3.3 Usability Requirements
- **NFR-010:** SQL syntax compatible with SQLite subset
- **NFR-011:** Clear error messages with context
- **NFR-012:** Web interface responsive on modern browsers
- **NFR-013:** Command completion and syntax highlighting

### 3.4 Security Requirements
- **NFR-014:** Input validation against SQL injection
- **NFR-015:** File system access controls
- **NFR-016:** Basic authentication for web interface

---

## 4. SYSTEM ARCHITECTURE

### 4.1 Component Architecture
```
┌─────────────────────────────────────────────┐
│                 Web Interface                │
│              (HTTP Server)                  │
└─────────────────────┬───────────────────────┘
                     │
┌─────────────────────┴───────────────────────┐
│              Query Interface                │
│         (SQL Parser & Executor)            │
└─────────────────────┬───────────────────────┘
                     │
┌─────────────────────┼───────────────────────┐
│  Transaction Mgr    │    Query Optimizer    │
└─────────────────────┼───────────────────────┘
                     │
┌─────────────────────┼───────────────────────┐
│    Index Manager    │     Catalog Manager   │
└─────────────────────┼───────────────────────┘
                     │
┌─────────────────────┴───────────────────────┐
│              Storage Engine                 │
│        (Page Manager, File Manager)        │
└─────────────────────────────────────────────┘
```

### 4.2 Data Flow
1. **Query Input:** Web/CLI → SQL Parser
2. **Query Planning:** Parser → Query Optimizer
3. **Execution:** Optimizer → Execution Engine
4. **Data Access:** Execution Engine → Storage Engine
5. **Result Return:** Storage Engine → Web/CLI

### 4.3 File Structure
```
database/
├── catalog/
│   ├── tables.cat      # Table metadata
│   ├── columns.cat     # Column definitions
│   └── indexes.cat     # Index metadata
├── data/
│   ├── table_1.db      # Table data files
│   ├── table_2.db
│   └── ...
├── indexes/
│   ├── idx_1.ndx       # Index files
│   └── ...
├── logs/
│   ├── wal.log         # Write-ahead log
│   └── error.log       # Error log
└── temp/
    └── sort_*.tmp      # Temporary sort files
```

## **Detailed JOIN Implementation Guide**

### **V0.6 - Basic JOIN Support (Days 19-22)**

**Implementation Strategy:**
1. **Start Simple:** INNER JOIN with nested loop algorithm
2. **Table Aliasing:** Support for table aliases in FROM clause  
3. **ON Clause Parser:** Parse and validate join conditions
4. **Result Materialization:** Combine records from both tables

**Key Classes:**
```cpp
class JoinParser {
public:
    struct ParsedJoin {
        std::string left_table;
        std::string right_table;
        std::string left_alias;
        std::string right_alias;
        JoinCondition condition;
        JoinType type;
    };
    
    ParsedJoin parse_join_clause(const std::string& sql);
};

class JoinExecutor {
private:
    TableManager* table_manager_;
    
public:
    ResultSet execute_join(const ParsedJoin& join_spec) {
        Table left = table_manager_->get_table(join_spec.left_table);
        Table right = table_manager_->get_table(join_spec.right_table);
        
        return nested_loop_join(left, right, join_spec.condition);
    }
    
    ResultSet nested_loop_join(const Table& left, const Table& right, 
                              const JoinCondition& condition) {
        ResultSet result;
        
        // Outer loop: iterate through left table
        for (const auto& left_record : left.scan()) {
            // Inner loop: iterate through right table
            for (const auto& right_record : right.scan()) {
                if (evaluate_join_condition(left_record, right_record, condition)) {
                    Record combined = combine_records(left_record, right_record);
                    result.add_record(combined);
                }
            }
        }
        return result;
    }
};
```

**Memory Considerations:**
- **Streaming:** Don't load entire tables into memory
- **Buffering:** Use page-based iteration
- **Result Size:** Estimate and warn for large cross-products

### **V0.8 - Advanced JOIN Algorithms (Days 26-28)**

**Hash Join Implementation:**
```cpp
class HashJoinExecutor {
public:
    ResultSet hash_join(const Table& build_table, const Table& probe_table,
                       const JoinCondition& condition) {
        // Phase 1: Build hash table from smaller relation
        std::unordered_multimap<Value, Record> hash_table;
        
        for (const auto& record : build_table.scan()) {
            Value join_key = extract_join_key(record, condition.left_column);
            hash_table.emplace(join_key, record);
        }
        
        // Phase 2: Probe with larger relation
        ResultSet result;
        for (const auto& probe_record : probe_table.scan()) {
            Value probe_key = extract_join_key(probe_record, condition.right_column);
            
            auto range = hash_table.equal_range(probe_key);
            for (auto it = range.first; it != range.second; ++it) {
                Record combined = combine_records(it->second, probe_record);
                result.add_record(combined);
            }
        }
        return result;
    }
};
```

**JOIN Algorithm Selection Logic:**
```cpp
JoinAlgorithm select_join_algorithm(const Table& left, const Table& right,
                                   const JoinCondition& condition) {
    size_t left_size = left.estimated_row_count();
    size_t right_size = right.estimated_row_count();
    
    // Use hash join for large tables with equi-join
    if ((left_size > 1000 || right_size > 1000) && condition.op == EQUALS) {
        return HASH_JOIN;
    }
    
    // Use merge join if both sides are sorted on join key
    if (left.is_sorted_on(condition.left_column) && 
        right.is_sorted_on(condition.right_column)) {
        return MERGE_JOIN;
    }
    
    // Default to nested loop for small tables or non-equi joins
    return NESTED_LOOP_JOIN;
}
```

### **JOIN Testing Strategy**

**Test Cases for V0.6:**
```sql
-- Basic functionality
CREATE TABLE users (id INTEGER, name VARCHAR(50));
CREATE TABLE posts (id INTEGER, user_id INTEGER, title VARCHAR(100));

INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO posts VALUES (1, 1, 'Post 1'), (2, 1, 'Post 2'), (3, 2, 'Post 3');

-- Test cases
SELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id;
-- Expected: 3 rows (Alice-Post1, Alice-Post2, Bob-Post3)

SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;
-- Expected: Projected columns only

-- Edge cases
SELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.name = 'Alice';
-- Expected: 2 rows (Alice's posts only)
```

**Performance Benchmarks:**
- **Small tables:** < 100ms for 1K x 1K join
- **Medium tables:** < 1s for 10K x 10K join  
- **Memory usage:** < 50MB for hash join build phase
- **Algorithm comparison:** Hash join should be 10x faster than nested loop for large equi-joins

### 5.1 Storage Engine

**Page Manager:**
```cpp
class PageManager {
public:
    Page* get_page(uint32_t page_id);
    uint32_t allocate_page();
    void deallocate_page(uint32_t page_id);
    void flush_page(uint32_t page_id);
    
private:
    LRUCache<uint32_t, Page> page_cache_;
    std::unordered_map<uint32_t, bool> dirty_pages_;
};
```

**Record Format:**
```cpp
struct Record {
    uint16_t size;
    uint8_t null_bitmap[];  // Variable size based on column count
    char data[];           // Variable size based on actual data
};
```

### 5.2 Index Manager

**B+ Tree Node:**
```cpp
struct BTreeNode {
    bool is_leaf;
    uint16_t key_count;
    uint32_t parent;
    union {
        struct {  // Internal node
            uint32_t children[MAX_KEYS + 1];
            char keys[MAX_KEYS][MAX_KEY_SIZE];
        } internal;
        struct {  // Leaf node
            uint32_t next_leaf;
            struct {
                char key[MAX_KEY_SIZE];
                uint32_t record_id;
            } entries[MAX_KEYS];
        } leaf;
    };
};
```

### 5.3 Query Processor

**Expression Evaluator:**
```cpp
class Expression {
public:
    virtual Value evaluate(const Record& record) = 0;
    virtual ~Expression() = default;
};

class ComparisonExpression : public Expression {
    std::unique_ptr<Expression> left_, right_;
    ComparisonOp op_;
public:
    Value evaluate(const Record& record) override;
};
```

### 5.4 Transaction Manager

**Transaction Structure:**
```cpp
struct Transaction {
    uint32_t txn_id;
    TransactionState state;
    std::vector<LogRecord> undo_records;
    std::unordered_set<uint32_t> locked_pages;
    timestamp_t start_time;
};
```

---

## 6. TESTING REQUIREMENTS

### 6.1 Unit Testing
- **Test Coverage:** Minimum 80% code coverage
- **Test Framework:** Custom lightweight testing framework
- **Test Categories:**
  - Storage engine operations
  - B+ tree operations
  - Query parsing and execution
  - Transaction management
  - Index management

### 6.2 Integration Testing
- **End-to-end SQL operations**
- **Concurrent transaction testing**
- **Recovery testing**
- **Performance benchmarking**

### 6.3 Test Data
- **Small dataset:** 1,000 records
- **Medium dataset:** 10,000 records
- **Large dataset:** 100,000 records
- **Schema variations:** Different table structures

---

## 7. DEPLOYMENT REQUIREMENTS

### 7.1 Build System
```cmake
# CMakeLists.txt structure
project(kizuna)
set(CMAKE_CXX_STANDARD 17)

# Core library
add_library(litedb_core ${CORE_SOURCES})

# CLI executable
add_executable(litedb_cli ${CLI_SOURCES})

# Web server
add_executable(litedb_web ${WEB_SOURCES})

# Tests
add_executable(tests ${TEST_SOURCES})
```

### 7.2 Web Deployment
- **Static files:** HTML, CSS, JavaScript
- **REST API:** JSON-based query interface
- **WebSocket:** Real-time query execution
- **Docker containerization** (optional)

---

## 8. PERFORMANCE OPTIMIZATION TARGETS

### 8.1 Assembly Optimizations (V0.9)
**Critical Sections for Assembly:**
1. **Memory copying for large records**
   ```cpp
   // Target: Vectorized memory operations
   void fast_memcpy(void* dest, const void* src, size_t n);
   ```

2. **B+ tree key comparison**
   ```cpp
   // Target: SIMD string comparison
   int fast_key_compare(const char* key1, const char* key2, size_t len);
   ```

3. **Hash functions for indexing**
   ```cpp
   // Target: Optimized hash computation
   uint64_t fast_hash(const char* data, size_t len);
   ```

4. **Sorting algorithms**
   ```cpp
   // Target: Cache-efficient sorting
   void fast_sort(Record* records, size_t count, int column_idx);
   ```

### 8.2 Performance Benchmarks
- **Insert performance:** Records per second
- **Query performance:** Queries per second
- **Index lookup time:** Microseconds per lookup
- **Transaction throughput:** Transactions per second

---

## 9. DOCUMENTATION REQUIREMENTS

### 9.1 Code Documentation
- **Header comments:** Every class and function
- **Inline comments:** Complex algorithms
- **API documentation:** Public interface descriptions

### 9.2 User Documentation
- **README.md:** Project setup and basic usage
- **SQL Reference:** Supported SQL syntax
- **Architecture Guide:** System design explanation
- **Performance Guide:** Optimization tips

### 9.3 Interview Documentation
- **Feature Demo Script:** Step-by-step demonstration
- **Technical Deep Dive:** Architecture explanation
- **Code Walkthrough:** Key implementation highlights

---

## 10. RISK MANAGEMENT

### 10.1 Technical Risks
- **Risk:** B+ tree implementation complexity
  - **Mitigation:** Start with simpler binary tree, evolve to B+ tree
- **Risk:** Concurrent access bugs
  - **Mitigation:** Extensive testing, simplified locking initially
- **Risk:** Performance bottlenecks
  - **Mitigation:** Profiling and incremental optimization

### 10.2 Timeline Risks
- **Risk:** Feature creep
  - **Mitigation:** Strict version boundaries, MVP focus
- **Risk:** Debugging time overruns
  - **Mitigation:** Comprehensive unit testing from V0.1

---

## 11. SUCCESS METRICS

### 11.1 Functional Metrics
- [ ] All SQL operations work correctly
- [ ] Handles 10,000+ record tables
- [ ] Transaction ACID properties maintained
- [ ] Web interface fully functional

### 11.2 Quality Metrics
- [ ] 80% code coverage achieved
- [ ] Zero critical bugs in core features
- [ ] Performance targets met
- [ ] Clean, maintainable code structure

### 11.3 Interview Readiness
- [ ] Live demo capabilities
- [ ] Clear architecture explanation
- [ ] Code quality demonstration
- [ ] Problem-solving showcase

---

## APPENDIX A: SQL SYNTAX REFERENCE

### Supported DDL
```sql
-- Table operations
CREATE TABLE table_name (column_definitions);
DROP TABLE table_name;
ALTER TABLE table_name ADD COLUMN column_definition;
ALTER TABLE table_name DROP COLUMN column_name;
TRUNCATE TABLE table_name;

-- Index operations
CREATE [UNIQUE] INDEX index_name ON table_name(column_list);
DROP INDEX index_name;
```

### Supported DML
```sql
-- Data manipulation
INSERT INTO table_name VALUES (value_list);
INSERT INTO table_name (column_list) VALUES (value_list);
UPDATE table_name SET column = value WHERE condition;
DELETE FROM table_name WHERE condition;

-- Queries
SELECT column_list FROM table_name 
  [WHERE condition] 
  [ORDER BY column_list [ASC|DESC]]
  [GROUP BY column_list [HAVING condition]]
  [LIMIT number];

-- Transactions
BEGIN [TRANSACTION];
COMMIT;
ROLLBACK;
```

### Supported Functions
```sql
-- Aggregate functions
COUNT(*), COUNT(column)
SUM(column), AVG(column)
MIN(column), MAX(column)

-- Utility functions
NOW()           -- Current timestamp
LENGTH(string)  -- String length
UPPER(string)   -- Uppercase conversion
LOWER(string)   -- Lowercase conversion
```

---

*This SRS serves as the definitive guide for Kizuna development. All implementation decisions should reference this document.*
