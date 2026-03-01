# Anthony SQLite - Quick Start Guide

This guide will get you up and running with Anthony, a pure Go implementation of SQLite.

## Installation

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

## Basic Usage

### Opening a Database

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open database (creates if it doesn't exist)
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Verify connection
    err = db.Ping()
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Connected to database successfully!")
}
```

### Creating Tables

```go
// Create a table
_, err = db.Exec(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER CHECK(age >= 0),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
`)
if err != nil {
    log.Fatal(err)
}
```

### Inserting Data

```go
// Insert a single row
result, err := db.Exec(`
    INSERT INTO users (name, email, age)
    VALUES (?, ?, ?)
`, "Alice", "alice@example.com", 30)
if err != nil {
    log.Fatal(err)
}

// Get the inserted row ID
id, err := result.LastInsertId()
fmt.Printf("Inserted row with ID: %d\n", id)

// Insert multiple rows
users := []struct {
    Name  string
    Email string
    Age   int
}{
    {"Bob", "bob@example.com", 25},
    {"Charlie", "charlie@example.com", 35},
    {"Diana", "diana@example.com", 28},
}

for _, user := range users {
    _, err := db.Exec(`
        INSERT INTO users (name, email, age)
        VALUES (?, ?, ?)
    `, user.Name, user.Email, user.Age)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Querying Data

```go
// Query all users
rows, err := db.Query(`
    SELECT id, name, email, age
    FROM users
    ORDER BY name
`)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Iterate through results
for rows.Next() {
    var id, age int
    var name, email string

    err := rows.Scan(&id, &name, &email, &age)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("ID: %d, Name: %s, Email: %s, Age: %d\n",
        id, name, email, age)
}

// Check for errors during iteration
if err = rows.Err(); err != nil {
    log.Fatal(err)
}
```

### Query Single Row

```go
// Query a single row
var name string
var age int

err := db.QueryRow(`
    SELECT name, age
    FROM users
    WHERE email = ?
`, "alice@example.com").Scan(&name, &age)

if err != nil {
    if err == sql.ErrNoRows {
        log.Println("User not found")
    } else {
        log.Fatal(err)
    }
} else {
    fmt.Printf("Found user: %s, age %d\n", name, age)
}
```

### Updating Data

```go
// Update a single row
result, err := db.Exec(`
    UPDATE users
    SET age = ?
    WHERE email = ?
`, 31, "alice@example.com")
if err != nil {
    log.Fatal(err)
}

// Check how many rows were affected
rowsAffected, err := result.RowsAffected()
fmt.Printf("Updated %d row(s)\n", rowsAffected)
```

### Deleting Data

```go
// Delete rows
result, err := db.Exec(`
    DELETE FROM users
    WHERE age < ?
`, 25)
if err != nil {
    log.Fatal(err)
}

rowsAffected, err := result.RowsAffected()
fmt.Printf("Deleted %d row(s)\n", rowsAffected)
```

## Transactions

```go
// Begin a transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute statements within transaction
_, err = tx.Exec(`INSERT INTO users (name, email, age) VALUES (?, ?, ?)`,
    "Eve", "eve@example.com", 27)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec(`UPDATE users SET age = age + 1 WHERE name = ?`, "Eve")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

## Prepared Statements

```go
// Prepare a statement
stmt, err := db.Prepare(`
    INSERT INTO users (name, email, age)
    VALUES (?, ?, ?)
`)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute the statement multiple times
users := [][]interface{}{
    {"Frank", "frank@example.com", 32},
    {"Grace", "grace@example.com", 29},
    {"Henry", "henry@example.com", 34},
}

for _, user := range users {
    _, err := stmt.Exec(user...)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Advanced Queries

### JOIN Operations

```go
// Create related tables
db.Exec(`
    CREATE TABLE departments (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
`)

db.Exec(`
    CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        department_id INTEGER,
        FOREIGN KEY (department_id) REFERENCES departments(id)
    )
`)

// Query with JOIN
rows, err := db.Query(`
    SELECT e.name, d.name as department
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    ORDER BY e.name
`)
```

### Aggregate Functions

```go
// Count users
var count int
err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
fmt.Printf("Total users: %d\n", count)

// Average age
var avgAge float64
err = db.QueryRow(`SELECT AVG(age) FROM users`).Scan(&avgAge)
fmt.Printf("Average age: %.2f\n", avgAge)

// Group by
rows, err := db.Query(`
    SELECT age, COUNT(*) as count
    FROM users
    GROUP BY age
    ORDER BY age
`)
```

### Subqueries

```go
// Find users older than the average age
rows, err := db.Query(`
    SELECT name, age
    FROM users
    WHERE age > (SELECT AVG(age) FROM users)
    ORDER BY age DESC
`)
```

### Common Table Expressions (CTEs)

```go
// Use WITH clause
rows, err := db.Query(`
    WITH adult_users AS (
        SELECT * FROM users WHERE age >= 18
    )
    SELECT name, email FROM adult_users
    WHERE age < 30
    ORDER BY age
`)
```

## Indexes

```go
// Create an index for better query performance
_, err = db.Exec(`
    CREATE INDEX idx_users_email ON users(email)
`)

// Create a unique index
_, err = db.Exec(`
    CREATE UNIQUE INDEX idx_users_name ON users(name)
`)

// Create a composite index
_, err = db.Exec(`
    CREATE INDEX idx_users_age_name ON users(age, name)
`)
```

## PRAGMA Statements

```go
// Get database configuration
var pageSize int
err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)
fmt.Printf("Page size: %d\n", pageSize)

// Set configuration (must be done before any other operations)
_, err = db.Exec(`PRAGMA journal_mode=WAL`)

// Get table info
rows, err := db.Query(`PRAGMA table_info(users)`)
defer rows.Close()

for rows.Next() {
    var cid int
    var name, colType string
    var notNull, pk int
    var dfltValue sql.NullString

    rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk)
    fmt.Printf("Column: %s, Type: %s, NotNull: %t, PK: %t\n",
        name, colType, notNull == 1, pk == 1)
}
```

## Error Handling Best Practices

```go
// Always check errors
result, err := db.Exec(`INSERT INTO users ...`)
if err != nil {
    // Handle constraint violations
    if strings.Contains(err.Error(), "UNIQUE constraint failed") {
        log.Println("Email already exists")
        return
    }
    log.Fatal(err)
}

// Check rows affected
rowsAffected, err := result.RowsAffected()
if err != nil {
    log.Fatal(err)
}
if rowsAffected == 0 {
    log.Println("No rows were updated")
}
```

## Connection Pooling

```go
// Configure connection pool
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)

// Check pool stats
stats := db.Stats()
fmt.Printf("Open connections: %d\n", stats.OpenConnections)
fmt.Printf("In use: %d\n", stats.InUse)
fmt.Printf("Idle: %d\n", stats.Idle)
```

## Complete Example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open database
    db, err := sql.Open("sqlite_internal", "example.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            completed BOOLEAN DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert tasks
    tasks := []string{"Buy groceries", "Write code", "Exercise"}
    for _, task := range tasks {
        _, err := db.Exec(`INSERT INTO tasks (title) VALUES (?)`, task)
        if err != nil {
            log.Fatal(err)
        }
    }

    // Query tasks
    rows, err := db.Query(`SELECT id, title, completed FROM tasks`)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    fmt.Println("Tasks:")
    for rows.Next() {
        var id int
        var title string
        var completed bool

        err := rows.Scan(&id, &title, &completed)
        if err != nil {
            log.Fatal(err)
        }

        status := "[ ]"
        if completed {
            status = "[[x]]"
        }

        fmt.Printf("%s %d. %s\n", status, id, title)
    }

    // Mark task as completed
    _, err = db.Exec(`UPDATE tasks SET completed = 1 WHERE id = ?`, 1)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("\nTask 1 marked as completed!")
}
```

## Next Steps

- Read the [Architecture Overview](ARCHITECTURE.md) to understand the internals
- Browse [Package Documentation](INDEX.md) for detailed API references
- Check out [Feature Documentation](INDEX.md#feature-documentation) for advanced SQL features
- Run tests: `go test ./...`

## Common Issues

### Database Locked
If you get "database is locked" errors, ensure you're properly closing transactions and connections.

### Constraint Violations
Anthony enforces all SQL constraints. Make sure your data satisfies CHECK, UNIQUE, and FOREIGN KEY constraints.

### Performance
- Create indexes on frequently queried columns
- Use prepared statements for repeated queries
- Enable WAL mode for better concurrency: `PRAGMA journal_mode=WAL`

## Getting Help

- Documentation: [docs/INDEX.md](INDEX.md)
- SQLite Reference: https://www.sqlite.org/docs.html
- Go database/sql: https://pkg.go.dev/database/sql

## License

Anthony is in the public domain (SQLite License). Use freely!
