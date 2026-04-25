# Anthony Concurrency Modes Proposal (`v0.8.x`)

## Goal

Increase Anthony's concurrency without giving up:

- SQLite-compatible file format
- SQLite-compatible SQL behavior
- good portability

At the same time, provide a strict SQLite-style operational mode for users who
need conservative locking and transaction guarantees.

## Modes

### `hard-compat`

Use conservative SQLite-style operational semantics.

Properties:
- serialized write behavior
- conservative read/write coordination
- predictable parity with SQLite-style transaction expectations
- best choice for migrations, debugging, and safety-first deployments

### `extended`

Preserve SQLite file and SQL compatibility while allowing Anthony-specific
concurrency improvements.

Properties:
- concurrent readers where safe
- staged writer preparation with serialized durable commit
- retryable write conflicts rather than universal preemptive serialization
- best choice for Anthony-only workloads that need more throughput

## Non-Negotiable Invariants

For both modes:
- on-disk state stays SQLite-compatible
- SQL behavior stays SQLite-compatible
- durable commit ordering remains serialized
- WAL/journal durability rules remain explicit and testable

For `hard-compat`:
- operational semantics stay conservative
- no Anthony-specific relaxed concurrency behavior

For `extended`:
- higher concurrency is allowed only when correctness is preserved
- conflicts must be surfaced explicitly rather than hidden

## Architecture Direction

Anthony should move toward:

1. snapshot-based reads
2. isolated writer work before commit
3. serialized validate-and-commit
4. explicit schema-generation tracking
5. mode-driven behavior at the driver and pager boundaries

## `v0.8.x` Implementation Phases

### Phase 1

- add compatibility mode config plumbing
- add DSN support via `compat_mode`
- add CLI switch via `cmd/anthony -compat-mode`
- formalize `hard-compat` as the default
- document the roadmap in repo docs and `TODO.txt`

### Phase 2

- route all transaction/statement serialization through mode-aware helpers
- preserve current conservative behavior for `hard-compat`
- keep `extended` functionally equivalent where behavior is not yet proven safe

### Phase 3

- remove unnecessary read-path serialization in `extended`
- add mode-specific concurrency tests
- validate read/read and read/write overlap correctness

### Phase 4

- add explicit snapshot/version policy for data and schema
- return retryable write conflicts on stale extended-mode writers
- serialize DDL strictly in both modes initially

### Phase 5

- broaden `extended` mode concurrency once pager/btree visibility rules are
  fully covered by tests and durability checks

## CLI

The Anthony CLI should expose:

```text
-compat-mode hard-compat|extended
```

Default:

```text
hard-compat
```

## DSN

The driver should expose:

```text
compat_mode=hard-compat|extended
```

Example:

```text
file.db?compat_mode=extended&journal_mode=wal
```

## Initial Safety Notes

`extended` should not claim broad concurrency wins until the relevant pager,
btree, trigger, foreign-key, savepoint, and schema-cache behaviors are covered
by dedicated tests. Early `v0.8.x` work is therefore intentionally
infrastructure-first.
