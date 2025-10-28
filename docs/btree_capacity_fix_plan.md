# B+ Tree Capacity Fix Plan

## Overview
- **Problem:** Index benchmarks fail with `RECORD_TOO_LARGE` when inserting ~200+ unique keys because leaf nodes only split after `config::BTREE_MAX_KEYS` (256) entries, ignoring the actual bytes consumed by encoded keys. With current key encoding, 4 KB pages overflow long before hitting 256 entries.
- **Impact:** Violates SRS success criteria (“Handle tables with 10,000+ records efficiently”) and blocks performance validation via `kizuna_index_benchmark`.

## Root Cause Highlights
1. `BPlusTree::InsertRecursive` decides to split solely on `BTREE_MAX_KEYS`.
2. Each key is stored via `record::encode`, adding headers/bitmap/type codes (~17 B per single-column key).
3. Leaf serialization packs key/value pairs tightly; once the byte budget is exceeded, `Serialize` aborts with `StatusCode::RECORD_TOO_LARGE`.

## Remediation Strategy
1. **Adaptive Split Threshold**
   - Add a helper (e.g., `BPlusTreeNode::serialized_byte_size()` or `WouldOverflow(new_key)`) to estimate required bytes _before_ storing.
   - Trigger a split when the required bytes exceed `(Page::page_size() - Page::kHeaderSize)`, regardless of key count.
   - Retain `BTREE_MAX_KEYS` as an absolute ceiling but no longer rely on it for deciding splits.
2. **Key Encoding Audit**
   - Confirm `record::encode` is the minimal representation for index keys; if not, consider a specialized encoding (skip null bitmap/type tags for fixed schema).
   - Defer specialised encoding if adaptive split alone resolves overflow with healthy fan-out (goal: ≥80 keys/leaf for VARCHAR(32)).
3. **Config Safety Net**
   - Derive a conservative `BTREE_MAX_KEYS` at startup using column widths and page size, or lower the constant (e.g., 128) to avoid unbounded growth.
4. **Testing & Benchmarks**
   - Extend unit tests to insert >10 k rows with 4 KB pages, verifying no overflow and that leaf occupancy remains within page limits.
   - Re-run `kizuna_index_benchmark` to confirm 10 k and 100 k row cases succeed.

## Implementation Steps
1. **B+ Tree Node Utilities**
   - Introduce byte accounting helpers in `BPlusTreeNode`.
   - Refactor `Serialize` to reuse the helper, ensuring consistency.
2. **Insertion Logic Update**
   - Modify leaf/internal insert paths to check `NeedsSplit()` using the new byte-based heuristic.
   - Adjust split methods if necessary to rebalance based on bytes instead of pure counts.
3. **Configuration Tuning**
   - Update `config::BTREE_MAX_KEYS` (if still needed) and document the new behaviour.
4. **Regression Tests**
   - Add tests under `tests/index/bplus_tree_test.cpp` covering large key counts and verifying tree height.
   - Add integration test via DML executor inserting 50 k keys with index.
5. **Benchmark Validation**
   - Build and run `kizuna_index_benchmark` for 1 k, 10 k, and stress values (e.g., 50 k) to capture timing and confirm stability.

## Risks & Mitigations
- **Risk:** Byte-estimation diverges from actual serialization → handle by sharing logic with `Serialize` or reusing a dry-run mode.
- **Risk:** Tree fan-out drops too low, increasing depth → monitor via tests; if needed, compress key metadata or adjust page size.
- **Risk:** Changes ripple into index rebuild/DDL paths → ensure catalog rebuild code uses updated helpers.

## Interview Storyboard
**Context:** Performance testing for V0.6 required building an automated index benchmark (`kizuna_index_benchmark`).  
**Issue Encountered:** Running the benchmark immediately failed with `RECORD_TOO_LARGE` while writing B+ tree keys.  
**Investigation Steps:**
1. Reproduced with both Python and new C++ benchmark drivers; failure happened at ~200 inserts.
2. Inspected the exception trace → `BPlusTreeNode::Serialize` complaining about lack of space.
3. Reviewed `config::BTREE_MAX_KEYS` and key encoding (`record::encode`) to gauge per-key size.
4. Calculated page budget vs. key footprint and confirmed nodes overflowed before reaching the hard max count.
**Resolution Plan:**
1. Introduce byte-aware capacity checks so leaf/internal nodes split when pages near exhaustion, not only when key counts exceed a constant.
2. Optionally optimize key serialization and recalibrate `BTREE_MAX_KEYS`.
3. Add regression tests and re-run the benchmark to verify 10 k+ row handling.

This narrative demonstrates end-to-end debugging: from reproducing the benchmark failure, isolating the serialization bottleneck, analysing configuration limits, to defining concrete code and validation steps that ensure Kizuna meets its 10 k record requirement.
