# Window Functions - SQL Language Reference

> See [WINDOW_FUNCTIONS.md](WINDOW_FUNCTIONS.md) for the complete SQLite window functions reference.

Window functions (also known as analytic functions) perform calculations across a set of
table rows related to the current row. Unlike aggregate functions, window functions do not
collapse the result set -- each row retains its identity.

## Syntax

A window function call looks like:

    function-name(args) OVER (
        [PARTITION BY expr, ...]
        [ORDER BY expr [ASC|DESC], ...]
        [frame-spec]
    )

Or using a named window:

    function-name(args) OVER window-name

## Frame Specification

    {ROWS | RANGE | GROUPS} BETWEEN frame-start AND frame-end

Where frame-start / frame-end is one of:
- `UNBOUNDED PRECEDING`
- `expr PRECEDING`
- `CURRENT ROW`
- `expr FOLLOWING`
- `UNBOUNDED FOLLOWING`

## Built-in Window Functions

| Function | Description |
|---|---|
| `row_number()` | Sequential row number within partition |
| `rank()` | Rank with gaps |
| `dense_rank()` | Rank without gaps |
| `percent_rank()` | Relative rank: (rank-1)/(rows-1) |
| `cume_dist()` | Cumulative distribution |
| `ntile(N)` | Divide rows into N buckets |
| `lag(expr, offset, default)` | Value from previous row |
| `lead(expr, offset, default)` | Value from following row |
| `first_value(expr)` | First value in frame |
| `last_value(expr)` | Last value in frame |
| `nth_value(expr, N)` | Nth value in frame |

All aggregate functions (`sum()`, `avg()`, `count()`, etc.) can also be used as window functions.

## See Also

- [Window Functions Reference](WINDOW_FUNCTIONS.md) -- complete reference from SQLite docs
- [SELECT Reference](SELECT.md) -- full SELECT syntax
- Source: https://www.sqlite.org/windowfunctions.html
