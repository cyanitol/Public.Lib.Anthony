# Anthony Roadmap (Production Readiness)

## Scope and Current Snapshot
- Feature parity: 100% trinity (1,257/1,257); 24 packages all passing, 0 failures.
- Targets: no CGO, maintain cyclomatic complexity ≤ 11.

## Recently Completed (v0.3.1)
- **100% Trinity Parity** - All 1,257 DO-178C trace tests passing, 0 skipped
- **Correlated TVF Cross-Joins** - `FROM table, json_each(table.col)` pattern
- **Derived Table Materialization** - JOIN subqueries as B-tree temp tables
- **Window State Isolation** - Multiple OVER clauses with separate state
- **IPK-Aware Table Reading** - OpRowid for INTEGER PRIMARY KEY columns

## Previously Completed (v0.2.0–v0.2.1)
- **Cyclomatic Complexity ≤9** - All functions across entire codebase
- **Race Condition Fixes** - PlanCache.Get() (RLock→Lock+atomics), VDBE test isolation
- **Test Isolation** - 181 hardcoded DB paths converted to t.TempDir()
- **alltests Build Tag** - Unified main + trinity test execution
- **Go 1.26.1** - Toolchain upgrade
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

1) Recursive CTE (In Progress)
- Cursor architecture being reworked for correct anchor/recursive member interaction.

2) PERCENT_RANK / CUME_DIST Window Functions
- Not yet implemented.

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

## Reference & Testing Integration (Merged Planning Notes)
- Maintain contrib/sqlite as a read-only reference source with version tracking.
- Keep trinity generator + traceability matrix; target decision + MC/DC coverage in critical paths.
- Expand fault injection (I/O, OOM, corruption), fuzzing, and regression suites.
- Continue TCL -> Go test conversion in phased batches for high-value SQLite suites.

## Documentation and Release
- Keep README and CHANGELOG aligned with current priorities and unsupported features.
- Publish tagged releases once active workstreams are completed and conformance/coverage thresholds are met.

*Last updated: 2026-03-16*
