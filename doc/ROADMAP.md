# Anthony Roadmap (Production Readiness)

## Current State (v0.6.0)
- **20,000+ tests passing**, 0 skipped, 0 failures
- **94.8% statement coverage** (MC/DC coverage across all packages)
- **1,257 Trinity (DO-178C trace) tests** at 100% parity
- **Primary target: WASM (browser-embedded DBMS)**; also embedded, VPS, scalable

## Next Milestone: Algorithm Improvements (v0.6.0)
- Replace O(n²) insertion sort with pdqsort (`vdbe/vdbe.go`)
- Lazy streaming external sort merge (`vdbe/sorter_spill.go`)
- Allocation-free NOCASE collation (`utf/collation.go`)
- B-tree interior page merge (`btree/merge.go`)
- B-tree right-edge split bias (`btree/split.go`)
- WASM packaging (IndexedDB, npm bundle)
- MC/DC coverage ≥99% all packages

## Baseline (v0.3.3)
- **17,443 tests passing**, 0 skipped, 0 failures
- **1,257 Trinity (DO-178C trace) tests** at 100% parity
- **Race detector clean** across all 27 packages
- **Cyclomatic complexity ≤11** for all functions
- No external Go dependencies (zero-dependency project)
- Go 1.26.1, pure Go (no CGO)

## Recently Completed (v0.3.3)
- **Zero Skipped Tests** — Eliminated all 787+ `t.Skip()` calls across 147 test files
- **B-tree Split Fix** — Non-sequential inserts no longer produce phantom rows
- **Pager Reference Counting** — Shared connection state properly managed
- **Cache Eviction** — Page ref leak fixed, dirty page flush under pressure
- **Stress Tests** — All concurrent/large-blob tests passing
- **Cyclomatic Complexity** — All functions (production + test) ≤11
- **GitHub Actions** — checkout@v6, setup-go@v6, gh CLI (Node.js 24)

## Completed (v0.3.2)
- RETURNING clause, UPDATE...FROM, ANALYZE
- IS DISTINCT FROM / IS NOT DISTINCT FROM
- generate_series TVF, PERCENT_RANK/CUME_DIST
- trunc(), custom collations, BACKUP API
- RIGHT/FULL OUTER JOIN, generated columns
- EXPLAIN QUERY PLAN improvements
- Recursive CTE improvements (iterative VDBE loop, dedup, cycle detection)
- FTS5 + R-Tree SQL integration (MATCH queries, spatial range, INSERT/DELETE)

## Completed (v0.3.1)
- 100% Trinity test parity (1,257 tests)
- Correlated TVF cross-joins, derived table materialization
- Window function state isolation, IPK-aware table reading

## Completed (v0.2.x)
- Core SQL engine, pager, btree, VDBE
- All 11 window functions
- Triggers (BEFORE/AFTER, WHEN, RAISE, cascading)
- ALTER TABLE, ATTACH/DETACH, TVFs
- Foreign keys (83/83 tests), WITHOUT ROWID (68 tests)
- JSON functions + aggregates, WAL mode, online backup
- Security audit (23 vulnerabilities resolved)

## Future Workstreams

### P1 — Hardening
- Run sqllogictest subsets vs reference SQLite
- Fuzz parser and record decoder
- Crash-replay tests for WAL/journal durability
- Target ≥80% coverage in pager/vdbe/btree/parser/engine

### P2 — Performance
- Cost-based index selection
- Join reordering
- Subquery flattening
- Incremental vacuum (auto_vacuum=INCREMENTAL)
- Page cache tuning, overflow page read-ahead

### P3 — Feature Gaps
- Partial indexes (CREATE INDEX ... WHERE)
- Expression indexes
- ALTER TABLE ADD CONSTRAINT
- Native composite key B-tree for WITHOUT ROWID

### Testing & Compatibility
- Expand fault injection (I/O, OOM, corruption)
- Continue TCL → Go test conversion for high-value SQLite suites
- Benchmark hot paths (btree cursor, pager I/O, VDBE ops)

*Last updated: 2026-03-19*
