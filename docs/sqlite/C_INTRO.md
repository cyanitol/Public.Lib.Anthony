# SQLite C Interface Introduction

> See [C_API_INTRO.md](C_API_INTRO.md) for the complete SQLite C API introduction,
> and [C_API_REFERENCE.md](C_API_REFERENCE.md) for the full reference.

This is a brief guide to the SQLite C interface. Since this project is a pure Go
implementation, the C API is provided for reference to understand the original SQLite
interface design.

## Key Objects

- **sqlite3** -- database connection handle
- **sqlite3_stmt** -- prepared statement handle
- **sqlite3_value** -- generic value container
- **sqlite3_context** -- context for user-defined functions

## Basic Pattern

```
sqlite3_open()     -> open/create database
sqlite3_prepare()  -> compile SQL to bytecode
sqlite3_step()     -> execute one step
sqlite3_column_*() -> read result columns
sqlite3_finalize() -> free the prepared statement
sqlite3_close()    -> close the database
```

Source: https://www.sqlite.org/capi3.html
