# Anthony Roadmap (Production Readiness)

## Scope and Current Snapshot
- Feature parity: ~75% vs reference SQLite; 25 packages all passing, comprehensive test coverage.
- Targets: no CGO, maintain cyclomatic complexity ≤ 11, push critical package coverage to ≥80%.

## Recently Completed
- **Trigger Runtime** - Full BEFORE/AFTER execution, WHEN clause, RAISE, UPDATE OF, cascading
- **ALTER TABLE** - RENAME TABLE, RENAME COLUMN, DROP COLUMN
- **ATTACH/DETACH** - Cross-database queries, PRAGMA database_list
- **Table-Valued Functions** - json_each, json_tree wired to SQL FROM clauses
- **Window Functions** - NTH_VALUE, named WINDOW clauses, PARTITION BY
- **Date/Time** - strftime %w/%u/%W/%j format codes
- **AUTOINCREMENT** - sqlite_sequence tracking
- **FTS5/R-Tree Persistence** - Shadow table management
- **Foreign Key Runtime** - 83/83 tests passing with deferred/immediate enforcement
- **WITHOUT ROWID Tables** - 45 passing, JOINs working

## Active Workstreams

1) Window Functions (In Progress)
- LAG, LEAD, FIRST_VALUE, LAST_VALUE being implemented now.
- DENSE_RANK, NTILE still need compiler wiring.
- Frame specification (ROWS/RANGE) needs edge case testing.

2) Recursive CTE (In Progress)
- Cursor architecture being reworked for correct anchor/recursive member interaction.
- Three-ephemeral-table approach (result, queue, next) implemented but cursor scope broken.

3) Trigger Completion
- OLD row extraction from cursors for DELETE/UPDATE triggers.
- INSTEAD OF trigger execution on views.

## Future Workstreams

4) VACUUM Robustness
- Schema persistence across VACUUM.
- Table/index rebuild fidelity.

5) Concurrent WAL Operations
- Multiple reader/single writer with WAL mode.
- Checkpoint operations.

6) FTS5/R-Tree SQL Integration
- Wire virtual table modules to SQL execution through xBestIndex/xFilter.
- MATCH/NEAR/prefix queries for FTS5.
- Spatial queries for R-Tree.

## Hardening and Compatibility
- Run sqllogictest subsets and reference SQLite diffs to catch drift.
- Fuzz parser and record decoder; add crash-replay tests for WAL/journal durability.
- Expand pragma coverage (journal_mode, synchronous, locking) and ensure unsupported pragmas error cleanly.
- Benchmark hot paths (btree cursor, pager I/O, VDBE ops) and set regression budgets.

## Documentation and Release
- Keep README and CHANGELOG aligned with current priorities and unsupported features.
- Publish tagged releases once active workstreams are completed and conformance/coverage thresholds are met.

*Last updated: 2026-03-15*
