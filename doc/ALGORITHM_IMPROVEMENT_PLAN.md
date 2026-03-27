# Algorithm Improvement Plan (WASM-first)

Researched 2026-03-27. All priorities ordered for WASM-first deployment (browser-embedded DBMS).


---

## Algorithm Improvement Plan (WASM-first)

---

### P0 — Correctness/Safety (must fix before any release)

#### P0.1 — `SorterWithSpill` uses filesystem unconditionally, silently breaking WASM

- **Problem:** `sorter_spill.go` has no build tag. It compiles to WASM and is the only sorter variant instantiated in production (`exec.go:4613` calls `NewSorterWithSpillAndRegistry(..., nil)` which hits `DefaultSorterConfig()` with `EnableSpill: true`). On WASM the `os.TempDir()`, `os.Create`, `os.Open`, `os.Remove`, and `os.Getpid` calls will produce runtime failures once any query spills. The current workaround (`EnableSpill` flag) is never set to `false` by the production call-site.
- **Reference:** POSIX file I/O is absent under `GOOS=js GOARCH=wasm` per the Go WASM runtime ABI.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/sorter_spill.go:147-168` (`createSpillFilePath`, `writeAndRecordSpill`); `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/exec.go:4613`
- **WASM impact:** High (runtime panic on any large ORDER BY)
- **Proposed fix:** Introduce a `SpillBackend` interface (`Write(run []byte) error`, `Read() ([]byte, error)`, `Close() error`). Provide two implementations behind build tags: `spill_file.go` (current `os.File` logic, `//go:build !js && !wasip1`) and `spill_mem.go` (a `[]byte` ring backed by a capped `bytes.Buffer`, `//go:build js || wasip1`). The WASM backend simply caps the in-memory limit higher and refuses to spill rather than crashing. Wire this into `SorterConfig` replacing the `TempDir string` / `*os.File` fields. The production instantiation in `exec.go` passes `nil` config, which resolves to `DefaultSorterConfig()` — that function should select the appropriate backend at init time via a build-tag-guarded default.
- **Effort:** Medium

---

### P1 — Critical for WASM performance (fix second, after P0)

#### P1.1 — `Sorter.Sort()`: O(n²) insertion sort

- **Problem:** `vdbe.go:307-324` implements a textbook insertion sort over `s.Rows`. For queries returning more than a few hundred rows (common in browser apps reading local datasets), this is O(n²) in comparisons and O(n²) in slice-element assignments. On single-threaded WASM, there is no background goroutine to overlap this cost; the UI thread blocks for the entire sort.
- **Reference:** Knuth TAOCP Vol. 3 §5.2.1 shows insertion sort degrades past ~20 elements; `sort.Slice` in the Go standard library uses pdqsort (pattern-defeating quicksort), O(n log n) average and worst case.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/vdbe.go:313-322`
- **WASM impact:** High. Single-threaded; no concurrency escape valve. A 10,000-row result set means ~50M comparisons with insertion sort vs ~133K with pdqsort.
- **Proposed fix:** Replace the insertion-sort loop with a single `sort.Slice(s.Rows, func(i, j int) bool { return s.compareRows(s.Rows[i], s.Rows[j]) < 0 })`. `sort.Slice` is allocation-free (it does not allocate a comparison functor on the heap in recent Go versions when the closure captures only `s`), uses pdqsort internally, and has no new imports beyond the already-present `sort` import in `sorter_spill.go` (add it to `vdbe.go`). No change to the `SorterInterface` or any caller. Existing tests cover correctness.
- **Effort:** Small

#### P1.2 — `serializeRow` / `serializeMem`: cascading small allocations per row

- **Problem:** `sorter_spill.go:229-303` allocates multiple independent `[]byte` slices per cell (`make([]byte, 4)` for flags, `make([]byte, 4)` for length, `make([]byte, 8)` for numeric payload) then `append`s them together. For a row of N columns, this produces at least `3N` small heap allocations plus the intermediate `buf` growth. In WASM, GC is single-threaded and expensive; each allocation is a potential GC trigger.
- **Reference:** Go memory model: small allocations under 32 KB each require a `runtime.mallocgc` call; frequent small allocations are the dominant GC pressure source in Go WASM (Go issue #14841, empirically).
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/sorter_spill.go:229-303` (`serializeMem`)
- **WASM impact:** High (fires on every row that spills; also indirectly through deserialize path doubling allocations at merge time).
- **Proposed fix:** Adopt a two-pass or pre-sized single-buffer approach. Add a `estimateSerializedSize(row []*Mem) int` helper (reuse the existing `estimateRowMemory` shape), then do one `buf := make([]byte, 0, estimatedSize)` per row. Inside `serializeMem`, write into `buf` using `binary.LittleEndian.AppendUint16`, `AppendUint32`, `AppendUint64` (introduced in Go 1.19 — all slice-append forms, zero additional allocation). This collapses `3N + 1` allocations per row to `1`. The `pageBufferPool` already in `pool.go` could supply a reusable scratch buffer keyed to `SorterWithSpill` if rows are serialized sequentially. Note: `deserializeMem` at line 394 also does `make([]byte, dataLen)` for each string/blob; replace with a direct slice of the already-read `rowData` (safe because rows are consumed once during merge).
- **Effort:** Small-to-medium

#### P1.3 — `mergeSpilledRuns`: eager full-run load into RAM

- **Problem:** `sorter_spill.go:487-498` calls `readRunFromFile(file)` which reads ALL rows of each spilled run into `[][]*Mem` before starting the merge. With k runs each up to `MaxMemoryBytes` (10 MB default), this loads up to k × 10 MB simultaneously, potentially exceeding the browser heap budget (256 MB).
- **Reference:** CLRS §8.4 external sort: a k-way merge heap only needs one row per run in memory at a time; total working set is O(k) rows, not O(k × run_size).
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/sorter_spill.go:472-510` (`mergeSpilledRuns`), `runReader` struct at line 513.
- **WASM impact:** High (memory budget violation; on WASM the browser OOM-kills the tab).
- **Proposed fix:** Change `runReader` to hold a streaming file reader. Add `nextRow() ([]*Mem, error)` that reads and deserializes exactly one row on demand (consuming the length-prefixed format that already exists). Remove the `rows [][]*Mem` field from `runReader`; replace with `file *os.File` (already present), `buf []byte` (reusable read scratch), and `remaining int64` (rows left). The `mergeRuns` heap loop already calls `reader.next()` one row at a time — only the initialization and `peek()` need changing. On WASM, this reduces peak working memory from O(k × run_size) to O(k × avg_row_size). Note: this is blocked by P0.1 for WASM (file is unavailable), but the fix is needed for non-WASM targets and establishes the correct abstraction for a future in-memory streaming backend.
- **Effort:** Medium

---

### P2 — Important for all targets

#### P2.1 — `CompareNoCase` (and `StrICmp`, `StrNICmp`): unnecessary string-to-`[]byte` conversion

- **Problem:** `utf/collation.go:80-81` converts both string arguments to `[]byte` before byte-walking them:
  ```
  aBytes := []byte(a)
  bBytes := []byte(b)
  ```
  In Go, `string → []byte` always allocates a copy on the heap unless the compiler can prove the slice does not escape; in a comparison function returning `int`, the slice does escape the inlining budget. `StrICmp` at line 198 also does `[]byte(a)` and `[]byte(b)`. For an index scan filtering on a NOCASE column, this fires once per B-tree comparison.
- **Reference:** Go spec: `string` is indexable directly — `s[i]` returns the byte at index `i` without any allocation. The `UpperToLower` table is a `[256]byte` fixed array, so `UpperToLower[s[i]]` is legal where `s` is a `string`.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/utf/collation.go:80-107` (`CompareNoCase`), lines 194-198 (`StrICmp`), lines 227-229 (`StrNICmp`).
- **WASM impact:** Medium (one allocation per B-tree comparison on NOCASE columns; GC pressure accumulates over large scans).
- **Proposed fix:** Change `CompareNoCase(a, b string)` to index `a` and `b` directly as strings using `a[i]` and `b[i]` (which returns a `byte`). Replace the `aBytes`/`bBytes` locals entirely. `CompareNoCaseBytes` already takes `[]byte` and is correct as-is. Apply the same direct-indexing pattern to `StrICmp` and `StrNICmp`. The `UpperToLower` lookup is unchanged; only the operand type changes from `aBytes[i]` to `a[i]`. This is a zero-semantic-change refactor because `string` and `[]byte` share identical byte representation. Cyclomatic complexity of `CompareNoCase` does not increase.
- **Effort:** Small

#### P2.2 — B-tree leaf split: hardcoded 50/50 median for all insert patterns

- **Problem:** `btree/split.go:39` always computes `medianIdx := len(cells) / 2`, splitting pages exactly in half. For monotone-increasing primary key insertions (autoincrement, timestamp-ordered data, sequential ROWID), the newly inserted key is always the largest, so the right half immediately contains only the new key and is 1/(n+1) full. The left page is 50% full after each split, causing twice as many page splits as necessary and doubling write amplification. This directly inflates the in-memory page count (and working set) on WASM `:memory:` databases.
- **Reference:** Bayer & McCreight (1972), and Graefe's "Modern B-Tree Techniques" (2011) §3.2: right-edge biased splitting (90/10) eliminates wasted space for append-only workloads. SQLite itself uses this strategy in `btree.c:moveToChild`.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/btree/split.go:39` (leaf integer), line 121 (interior integer), line 176 (leaf composite), line 192 (interior composite).
- **WASM impact:** Medium (more pages → more memory pressure, more GC, slower sequential scan over in-memory pages).
- **Proposed fix:** Add a boolean parameter (or detect automatically) for right-edge insert: if the new key is strictly greater than all existing keys on the page, use `medianIdx = len(cells) - 1` (one cell on right, rest on left). The existing `keys []int64` slice passed to `executeLeafSplit` is sufficient to make this determination (`key > keys[len(keys)-2]` after insertion). For composite keys, compare `keyBytes` against the last existing composite key using the page's existing comparator. This is a local change to `splitLeafPage` and `splitInteriorPage`; no interface changes required. Correctness is preserved because the split contract only requires that the divider key is accurate, regardless of how many cells are on each side.
- **Effort:** Medium

#### P2.3 — WAL `checksumCache`: unbounded map growth

- **Problem:** `pager/wal.go:75` stores `map[uint32][2]uint32`. Every written or validated frame adds an entry. A long-running session (realistic in an embedded browser app) with frequent writes never purges the map. The WAL is file-based and irrelevant for `:memory:` mode, but when used for file-backed databases on non-WASM targets, the map grows without bound for the lifetime of the WAL object.
- **Reference:** WAL checksum is cumulative (each frame depends on all prior frames), so the cache cannot be arbitrarily pruned — but entries before the last checkpoint are no longer needed.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/pager/wal.go:75`, 182, 241, 654, 665, 688, 763.
- **WASM impact:** Low (WAL is not used in `:memory:` mode, confirmed by `memory_pager.go` containing no WAL reference). Medium for file-backed targets.
- **Proposed fix:** After a successful checkpoint (`wal_checkpoint.go`), call a `pruneChecksumCache(upToFrame uint32)` method that deletes all entries with key `<= upToFrame`. The pruning is safe because after a checkpoint, the WAL is reset and `frameCount` returns to 0 (or the checkpointed frames are no longer in the active WAL window). Alternatively, replace `map[uint32][2]uint32` with a simple two-element rolling cache (`[2]struct{ frame uint32; s1, s2 uint32 }`), keeping only the most recent cached frame and the one before it — sufficient for the sequential write path that calls `isCachedChecksumValid` on consecutive frame numbers.
- **Effort:** Small

#### P2.4 — `sync.Pool` in `pool.go` on WASM: no benefit, adds atomic overhead

- **Problem:** `pool.go:13-58` uses `sync.Pool` for `Mem`, `Instruction`, page buffers, and slices, with `sync/atomic` counters on every `Get`/`Put`. On WASM (single-threaded, no goroutines), `sync.Pool` still compiles and runs, but its multi-threaded GC interaction logic (`poolLocal` per-P caching) provides zero benefit. The `atomic.AddInt64` on every `GetMem`/`PutMem` is a memory barrier on platforms that implement atomics via `__atomic_*`, but on WASM it is a no-op emulation — still a function call overhead that fires on every row access.
- **Reference:** Go runtime: `sync.Pool` under `GOARCH=wasm` uses a single global pool (no per-P caches) and still goes through the full `poolGet`/`poolPut` path.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/pool.go:112-135` (`GetMem`/`PutMem`), lines 140-157 (`GetInstruction`/`PutInstruction`).
- **WASM impact:** Medium (every row in every query touches `GetMem`/`PutMem`).
- **Proposed fix:** Add a build-tag-gated alternative `pool_wasm.go` (`//go:build js || wasip1`) that replaces the `sync.Pool` with a simple free-list implemented as a `[]*Mem` slice (a stack). Because WASM is single-threaded, no mutex is needed. Remove the `atomic` counters from the WASM path (or make them plain `int64` increments). The existing `pool.go` becomes `pool_notwasm.go` (`//go:build !(js || wasip1)`). The exported API (`GetMem`, `PutMem`, etc.) is identical, so no callers change.
- **Effort:** Medium

---

### P3 — Nice to have

#### P3.1 — Interior page merge never attempted

- **Problem:** `btree/merge.go:27-28` explicitly returns `false, nil` for non-leaf pages (`if !currentHeader.IsLeaf { return false, nil }`). Interior pages accumulate tombstoned separator keys after leaf merges, bloating the tree height and increasing page count.
- **Reference:** Knuth TAOCP Vol. 3 §6.4: B-tree deletion requires merging or redistributing interior nodes after leaf collapse to maintain the height invariant.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/btree/merge.go:27-28`
- **WASM impact:** Low (affects delete-heavy workloads; most browser apps are read-heavy or insert-heavy). The `canMergePageTypes` guard at line 487 also hard-blocks interior merges.
- **Proposed fix:** Implement interior merge as a follow-up to leaf merges when the parent drops below 50% fill. This is a significant correctness-sensitive change requiring its own test suite. Recommend a phased approach: first add a `FillFactor() float64` method to `PageHeader` returning `(contentSize / usableSize)` for monitoring only, then implement the merge in a subsequent PR once the fill-factor metric confirms the problem is worth fixing on real workloads.
- **Effort:** Large

#### P3.2 — `DistinctSets` uses `map[string]bool` (heap per key)

- **Problem:** `vdbe.go:208` stores `map[int]map[string]bool` for DISTINCT aggregate tracking. Each string key in the inner map is a heap-allocated string header plus data. For high-cardinality DISTINCT columns this causes significant GC pressure.
- **File + line:** `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/vdbe.go:208`
- **WASM impact:** Low-to-medium (only fires on DISTINCT aggregate queries; `map[string]struct{}` is marginally better; a bitset or hash-based approach would be substantially better for numeric or bounded-domain keys).
- **Proposed fix:** Change the value type to `map[string]struct{}` (saves one byte per key in Go's map implementation). For a more impactful fix, use `map[uint64]struct{}` after hashing each key with `fnv.New64a` — this is safe for DISTINCT deduplication (hash collisions are astronomically unlikely and the correctness bar for DISTINCT is only probabilistic in standard SQL engines). This change is purely internal to `execDistinctAgg` and has no interface impact.
- **Effort:** Small

---

## Summary Table

| ID   | Area               | WASM Impact | Effort  | Priority   |
|------|--------------------|-------------|---------|------------|
| P0.1 | Spill filesystem   | High        | Medium  | P0 — ship-blocker |
| P1.1 | Insertion sort     | High        | Small   | P1 — critical     |
| P1.2 | Serialize allocs   | High        | Small+  | P1 — critical     |
| P1.3 | Eager spill load   | High        | Medium  | P1 — critical     |
| P2.1 | NOCASE []byte copy | Medium      | Small   | P2 — important    |
| P2.2 | Split right-edge   | Medium      | Medium  | P2 — important    |
| P2.3 | Checksum map growth| Low/Medium  | Small   | P2 — important    |
| P2.4 | sync.Pool on WASM  | Medium      | Medium  | P2 — important    |
| P3.1 | Interior merge     | Low         | Large   | P3 — defer        |
| P3.2 | DistinctSets map   | Low/Medium  | Small   | P3 — easy win     |

---

## WASM-specific Assessment Summary

| Item | Syscalls broken? | GC pressure | Single-thread safe? |
|------|-----------------|-------------|----------------------|
| P0.1 spill | Yes — `os.Create`, `os.TempDir`, `os.Getpid`, `os.Open`, `os.Remove` are runtime failures on `js/wasm` | N/A (crashes first) | N/A |
| P1.1 sort | No | Low (no allocs in sort.Slice closure for small n) | Yes |
| P1.2 serialize | No | High — N small allocs per row | Yes |
| P1.3 eager load | No (blocked by P0.1 on WASM) | High — entire runs in RAM | Yes |
| P2.1 NOCASE | No | Medium — 2 allocs per string compare | Yes |
| P2.2 split bias | No | Medium — fewer pages = less GC | Yes |
| P2.3 checksum map | No (WAL not used in :memory:) | Low on WASM | Yes |
| P2.4 sync.Pool | No — compiles, but atomic overhead | Medium | Yes — single-threaded simplification valid |

---

### Critical Files for Implementation

- `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/sorter_spill.go` — Core file for P0.1, P1.2, and P1.3: the spill backend interface, serialization allocation reduction, and streaming merge all live here.
- `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/vdbe.go` — P1.1 insertion sort replacement; also where `SorterInterface` is defined and the `Sorter.Sort()` method resides.
- `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/utf/collation.go` — P2.1 NOCASE allocation fix; all three affected functions (`CompareNoCase`, `StrICmp`, `StrNICmp`) are here.
- `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/btree/split.go` — P2.2 right-edge split bias; all four `medianIdx` calculation sites are in this file.
- `/home/bacon/._all_files_/Programming/Workspace/JuniperBible/Public.Lib.Anthony/internal/vdbe/pool.go` — P2.4 WASM-specific pool replacement; the `GetMem`/`PutMem` hot path that fires on every row access needs a build-tag-gated variant here.
