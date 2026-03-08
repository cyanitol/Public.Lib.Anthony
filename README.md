# Anthony - Pure Go SQLite Implementation

A pure Go SQLite implementation for the cyanitol project. No CGO dependencies.

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

db, _ := sql.Open("sqlite_internal", "mydb.sqlite")
db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
db.Exec(`INSERT INTO users (name) VALUES (?)`, "Alice")
```

## Documentation

Full documentation is in the [doc/](doc/) directory:

- **[Full README](doc/README.md)** - Complete documentation with examples
- **[Architecture](doc/ARCHITECTURE.md)** - System design and internals
- **[Feature Status](doc/AREWETHEREYET.md)** - Feature parity tracker
- **[TODO List](doc/TODO.txt)** - Detailed task breakdown and roadmap
- **[Security](doc/SECURITY.md)** - Security model and best practices
- **[Contributing](doc/CONTRIBUTING.md)** - Contribution guidelines

## Development

```bash
nix-shell           # Enter development environment
make commit         # Run pre-commit checks
go test ./...       # Run tests
```

## License

**Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0**

See `LICENSE` files for details.
