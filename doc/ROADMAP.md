# Anthony Roadmap (Production Readiness)

## Scope and Current Snapshot
- Feature parity: ~89% trinity (1,122/1,257); 24 packages all passing, 14,072 tests passing (alltests tag), 0 failures.
- Targets: no CGO, maintain cyclomatic complexity ≤ 9, push trinity parity to ≥95%.

## Recently Completed (v0.2.2)
- **JSON Aggregates** - json_group_array, json_group_object as Step/Final aggregates
- **JOIN+Aggregate Pipeline** - compile_join_agg.go for SELECT with JOINs and GROUP BY
- **NULL-Safe GROUP BY** - OpIsNull checks for correct NULL grouping
- **Trigger Expression Substitution** - CAST, BETWEEN, IN, CASE in trigger bodies
- **View WHERE Filtering** - Outer WHERE applied after view materialization
- **VTab Aggregate Routing** - TVF selects with aggregates
- **22 Trinity Tests Unskipped** - 1,122 passing (was 1,073)

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

1) Unskip Already-Implemented Tests
- GROUP BY NULL (2 tests), triggers (2 tests), json_group_array/object (6 tests), JOIN+aggregate (5 tests).
- These features are implemented; need verification and skip removal.

2) Join Edge Cases (7 tests)
- LEFT JOIN unmatched rows, NULL handling in ON clause, empty table joins.
- IS operator in JOIN ON clause (NULL IS NULL = true).

3) HAVING, Views, DELETE Subquery (7 tests)
- HAVING with complex aggregates.
- Views with DISTINCT, HAVING.
- DELETE FROM WHERE IN (SELECT ...).

4) Window Functions (42 tests - largest gap)
- Need OpWindowAggregate opcode for SUM/COUNT/AVG/MIN/MAX over frame.
- Column mapping fix in sorter population (rowid alias).
- PERCENT_RANK, CUME_DIST not implemented.
- Outer ORDER BY/LIMIT after window computation.

5) TVF Multi-Table FROM (5 tests)
- Correlated TVF evaluation (per-row args from outer table).
- New compilation path needed.

6) Recursive CTE (In Progress)
- Cursor architecture being reworked for correct anchor/recursive member interaction.

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

*Last updated: 2026-03-16*
