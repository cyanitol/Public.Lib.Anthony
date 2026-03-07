# WITHOUT ROWID Implementation Plan

Current state:
- Synthetic rowid derived from PK is used for inserts/updates (hash-based).
- PK uniqueness check added via row scan for WITHOUT ROWID tables.
- Order-preserving composite key encoder added at `internal/withoutrowid`.

Next steps (must complete for correctness):
1) **B-tree key model**
   - Add support for byte-composite keys (collation-aware) alongside int64 rowids.
   - Ensure comparator uses SQLite ordering (binary for now).
   - Update cursor seek/Next/Prev to operate on composite keys for WITHOUT ROWID tables.

2) **Planner/VDBE integration**
   - Change DML codegen for WITHOUT ROWID to encode PK via `withoutrowid.EncodeCompositeKey` and pass to btree ops.
   - Adjust opcodes (OpenWrite/Insert/Delete/Seek) to accept composite keys and enforce PK uniqueness via key comparison (no scans).
   - Ensure range scans, ORDER BY, and index usage honor PK ordering.

3) **DDL/schema**
   - Persist WITHOUT ROWID table metadata and PK column order for key encoding.
   - VACUUM/ANALYZE must rebuild and collect stats with composite keys.

4) **Collations**
   - Integrate collation-aware encoding for text PK columns; binary-only until collation hooks added.

5) **Testing**
   - CRUD and range tests on WITHOUT ROWID tables (single/composite PK, collation cases).
   - PK update scenarios, uniqueness violations, FK interactions.
   - VACUUM/ANALYZE/EXPLAIN plans and performance sanity.

Notes:
- Avoid hash-derived int64 keys in final implementation; must preserve PK ordering.
- Use existing encoder as the canonical key; extend with collation-aware transforms before encoding.
