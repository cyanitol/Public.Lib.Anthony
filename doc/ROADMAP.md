# Anthony Roadmap (Production Readiness)

## Scope and Current Snapshot
- Feature parity: ~58% vs reference SQLite; 655 tests passing, 476 skipped.
- Major gaps to close: window functions, foreign key runtime, WITHOUT ROWID tables, FTS5, R-Tree.
- Targets: no CGO, maintain cyclomatic complexity ≤ 10, push critical package coverage to ≥80%.

## Priority Workstream Overview
1) Foreign Key Runtime
- Enforce immediate/deferred constraints at statement/transaction boundaries.
- Implement ON DELETE/UPDATE CASCADE/SET NULL/SET DEFAULT/RESTRICT; integrate pragma coverage.
- Add cascaded delete/update tests, cycle detection, self-referential validation.

2) WITHOUT ROWID Tables
- Implement alternate btree layout using PRIMARY KEY as the key.
- Route DML/DDL paths without rowid generation; ensure index selection and VACUUM support.
- Test composite PKs, covering indexes, and EXPLAIN/ANALYZE stability.

3) Window Functions
- Initial set: ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX windows with PARTITION BY/ORDER BY and ROWS/RANGE frames.
- Extended set: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE; handle NULL/tie semantics and frame edges.
- Add executor tests for large partitions, peer groups, and mixed aggregates.

4) FTS5 Virtual Table
- Tokenizer pipeline, segment storage/merge, MATCH/NEAR/prefix queries.
- Ranking (bm25), highlight/offsets, content/contentless tables.
- Tests for DDL/DML, concurrent writes, integrity, and rebuild/optimize paths.

5) R-Tree Virtual Table
- R*-tree insertion/split/reinsertion, delete/repack; 2D/3D configurations.
- Bounding box queries (overlap/contains/within) with constraint pushdown.
- Tests for point/box queries, numeric edge cases, and vacuum/pack behavior.

## Hardening and Compatibility
- Run sqllogictest subsets and reference SQLite diffs to catch drift.
- Fuzz parser and record decoder; add crash-replay tests for WAL/journal durability.
- Expand pragma coverage (journal_mode, synchronous, locking) and ensure unsupported pragmas error cleanly.
- Benchmark hot paths (btree cursor, pager I/O, VDBE ops) and set regression budgets.

## Documentation and Release
- Keep README and TODO.txt aligned with current priorities and unsupported features.
- Update CHANGELOG and examples for new capabilities (window functions, FK runtime, WITHOUT ROWID, FTS5, R-Tree) as they land.
- Publish tagged releases once the above workstreams are completed and conformance/coverage thresholds are met.
