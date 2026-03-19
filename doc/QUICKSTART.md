# Quick Start

This is a minimal, copy/paste-friendly start for using the Anthony driver.
For a full tutorial and examples, see [Getting Started](GETTING_STARTED.md).

## Install

```bash
go get github.com/cyanitol/Public.Lib.Anthony
```

## Minimal Example

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Next Steps

- [Getting Started](GETTING_STARTED.md)
- [User Guide](USER_GUIDE.md)
- [API Reference](API.md)
- [Documentation Index](INDEX.md)
