# SQL Language Reference - Go Implementation

This document describes the SQL language supported by Anthony, a pure Go SQLite implementation. It focuses on Go-specific implementation details and supported features.

## Table of Contents

- [Overview](#overview)
- [Data Definition Language (DDL)](#data-definition-language-ddl)
- [Data Manipulation Language (DML)](#data-manipulation-language-dml)
- [Query Language](#query-language)
- [Expressions](#expressions)
- [Functions](#functions)
- [Transactions](#transactions)
- [Go Implementation Notes](#go-implementation-notes)

## Overview

Anthony implements the SQL dialect used by SQLite 3.x, focusing on compatibility while providing a pure Go implementation.

**Package Location:** `internal/parser`, `internal/sql`, `internal/planner`

### SQL Statements

Anthony supports these statement categories:

- **DDL**: CREATE, DROP, ALTER
- **DML**: INSERT, UPDATE, DELETE, REPLACE
- **Query**: SELECT, VALUES, WITH
- **Transaction**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- **Database**: ATTACH, DETACH, VACUUM
- **Utility**: PRAGMA, EXPLAIN, ANALYZE

## Data Definition Language (DDL)

### CREATE TABLE

Create a new table in the database.

**Syntax:**
```sql
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS]
    [schema_name.]table_name (
    column_def [, column_def]*
    [, table_constraint]*
) [table_options];
```

**Column Definition:**
```sql
column_name type_name [column_constraint]*
```

**Example:**
```sql
CREATE TABLE users (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    email    TEXT NOT NULL,
    age      INTEGER CHECK(age >= 0),
    created  INTEGER DEFAULT (strftime('%s', 'now')),
    UNIQUE(username, email)
);
```

**Go Implementation:**
```go
// Package: internal/parser
type CreateTableStmt struct {
    Temporary   bool
    IfNotExists bool
    Schema      string
    Table       string
    Columns     []*ColumnDef
    Constraints []*TableConstraint
    Options     *TableOptions
}

type ColumnDef struct {
    Name        string
    Type        *TypeName
    Constraints []*ColumnConstraint
}
```

**Table Options:**

```sql
-- WITHOUT ROWID (more compact storage)
CREATE TABLE coordinates (
    x INTEGER,
    y INTEGER,
    PRIMARY KEY (x, y)
) WITHOUT ROWID;

-- STRICT (enforce type checking)
CREATE TABLE strict_example (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
) STRICT;

-- Both options
CREATE TABLE compact (
    key TEXT PRIMARY KEY,
    val TEXT
) WITHOUT ROWID, STRICT;
```

### Column Constraints

**PRIMARY KEY:**
```sql
-- Single column
id INTEGER PRIMARY KEY

-- Autoincrement
id INTEGER PRIMARY KEY AUTOINCREMENT

-- Ordering (ASC/DESC)
id INTEGER PRIMARY KEY DESC
```

**NOT NULL:**
```sql
name TEXT NOT NULL
```

**UNIQUE:**
```sql
email TEXT UNIQUE
username TEXT UNIQUE ON CONFLICT REPLACE
```

**CHECK:**
```sql
age INTEGER CHECK(age >= 0 AND age <= 150)
price REAL CHECK(price > 0)
```

**DEFAULT:**
```sql
-- Literal value
status TEXT DEFAULT 'active'

-- Expression (must be in parentheses)
created INTEGER DEFAULT (strftime('%s', 'now'))
timestamp REAL DEFAULT (julianday('now'))

-- Special values
id TEXT DEFAULT (hex(randomblob(16)))
```

**FOREIGN KEY:**
```sql
-- Column constraint
user_id INTEGER REFERENCES users(id)

user_id INTEGER REFERENCES users(id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT

-- Deferred checking
parent_id INTEGER REFERENCES nodes(id)
    DEFERRABLE INITIALLY DEFERRED
```

### Table Constraints

**PRIMARY KEY:**
```sql
CREATE TABLE t (
    a INTEGER,
    b TEXT,
    PRIMARY KEY (a, b)
);
```

**UNIQUE:**
```sql
CREATE TABLE t (
    email TEXT,
    phone TEXT,
    UNIQUE (email, phone)
);
```

**CHECK:**
```sql
CREATE TABLE t (
    start_date INTEGER,
    end_date INTEGER,
    CHECK (end_date > start_date)
);
```

**FOREIGN KEY:**
```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
        ON DELETE CASCADE
);
```

### CREATE INDEX

Create an index on table columns.

**Syntax:**
```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS]
    [schema_name.]index_name
    ON table_name (column_expr [ASC|DESC] [, ...])
    [WHERE expr];
```

**Examples:**
```sql
-- Simple index
CREATE INDEX idx_users_email ON users(email);

-- Unique index
CREATE UNIQUE INDEX idx_username ON users(username);

-- Composite index
CREATE INDEX idx_name_age ON users(last_name, first_name, age);

-- Expression index
CREATE INDEX idx_lower_email ON users(lower(email));

-- Partial index (with WHERE clause)
CREATE INDEX idx_active_users
    ON users(username)
    WHERE active = 1;

-- Descending index
CREATE INDEX idx_created_desc
    ON posts(created DESC);
```

**Go Implementation:**
```go
type CreateIndexStmt struct {
    Unique      bool
    IfNotExists bool
    Schema      string
    Index       string
    Table       string
    Columns     []*IndexedColumn
    Where       Expression
}

type IndexedColumn struct {
    Expr      Expression  // Column name or expression
    Collation string      // Optional collation
    Order     Order       // ASC or DESC
}
```

### CREATE VIEW

Create a virtual table based on a SELECT query.

**Syntax:**
```sql
CREATE [TEMP] VIEW [IF NOT EXISTS]
    [schema_name.]view_name [(column_list)]
    AS select_statement;
```

**Examples:**
```sql
-- Simple view
CREATE VIEW active_users AS
    SELECT * FROM users WHERE active = 1;

-- View with column list
CREATE VIEW user_summary (uid, uname, post_count) AS
    SELECT u.id, u.username, COUNT(p.id)
    FROM users u
    LEFT JOIN posts p ON u.id = p.user_id
    GROUP BY u.id, u.username;

-- Temporary view
CREATE TEMP VIEW session_data AS
    SELECT * FROM current_session();
```

### CREATE TRIGGER

Create a trigger that executes SQL statements when certain events occur.

**Syntax:**
```sql
CREATE [TEMP] TRIGGER [IF NOT EXISTS]
    [schema_name.]trigger_name
    [BEFORE | AFTER | INSTEAD OF]
    [DELETE | INSERT | UPDATE [OF column_list]]
    ON table_name
    [FOR EACH ROW]
    [WHEN expression]
BEGIN
    trigger_statements;
END;
```

**Examples:**
```sql
-- Update timestamp on modification
CREATE TRIGGER update_timestamp
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    UPDATE users SET modified = strftime('%s', 'now')
    WHERE id = NEW.id;
END;

-- Audit trail
CREATE TRIGGER audit_delete
AFTER DELETE ON sensitive_data
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (action, table_name, row_id, timestamp)
    VALUES ('DELETE', 'sensitive_data', OLD.id, strftime('%s', 'now'));
END;

-- Conditional trigger
CREATE TRIGGER validate_email
BEFORE INSERT ON users
FOR EACH ROW
WHEN NEW.email NOT LIKE '%@%'
BEGIN
    SELECT RAISE(ABORT, 'Invalid email address');
END;
```

### ALTER TABLE

Modify an existing table structure.

**Syntax:**
```sql
-- Rename table
ALTER TABLE table_name RENAME TO new_table_name;

-- Rename column
ALTER TABLE table_name RENAME [COLUMN] old_name TO new_name;

-- Add column
ALTER TABLE table_name ADD [COLUMN] column_def;

-- Drop column
ALTER TABLE table_name DROP [COLUMN] column_name;
```

**Examples:**
```sql
-- Rename table
ALTER TABLE users RENAME TO accounts;

-- Add column
ALTER TABLE users ADD COLUMN phone TEXT;

-- Add column with default
ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';

-- Rename column
ALTER TABLE users RENAME COLUMN username TO user_name;

-- Drop column
ALTER TABLE users DROP COLUMN temp_field;
```

**Go Implementation:**
```go
type AlterTableStmt struct {
    Table      string
    Action     AlterAction
    // Action-specific fields
    NewTable   string          // For RENAME TO
    OldColumn  string          // For RENAME COLUMN
    NewColumn  string          // For RENAME COLUMN
    Column     *ColumnDef      // For ADD COLUMN
    DropColumn string          // For DROP COLUMN
}

type AlterAction int

const (
    AlterRenameTable AlterAction = iota
    AlterRenameColumn
    AlterAddColumn
    AlterDropColumn
)
```

### DROP Statements

**DROP TABLE:**
```sql
DROP TABLE [IF EXISTS] table_name;
```

**DROP INDEX:**
```sql
DROP INDEX [IF EXISTS] index_name;
```

**DROP VIEW:**
```sql
DROP VIEW [IF EXISTS] view_name;
```

**DROP TRIGGER:**
```sql
DROP TRIGGER [IF EXISTS] trigger_name;
```

**Examples:**
```sql
DROP TABLE IF EXISTS temp_data;
DROP INDEX IF EXISTS idx_old;
DROP VIEW IF EXISTS legacy_view;
DROP TRIGGER IF EXISTS obsolete_trigger;
```

## Data Manipulation Language (DML)

### INSERT

Insert new rows into a table.

**Syntax:**
```sql
INSERT INTO table_name [(column_list)]
    VALUES (value_list) [, (value_list)]*
    [ON CONFLICT resolution];

INSERT INTO table_name [(column_list)]
    select_statement
    [ON CONFLICT resolution];

INSERT INTO table_name
    DEFAULT VALUES;
```

**Examples:**
```sql
-- Insert single row
INSERT INTO users (username, email)
VALUES ('alice', 'alice@example.com');

-- Insert multiple rows
INSERT INTO users (username, email) VALUES
    ('bob', 'bob@example.com'),
    ('charlie', 'charlie@example.com'),
    ('diana', 'diana@example.com');

-- Insert from SELECT
INSERT INTO users_backup
SELECT * FROM users WHERE created < strftime('%s', 'now', '-1 year');

-- Insert with default values
INSERT INTO users DEFAULT VALUES;

-- Insert or replace on conflict
INSERT INTO users (id, username)
VALUES (1, 'admin')
ON CONFLICT(id) REPLACE;
```

**Go Implementation:**
```go
type InsertStmt struct {
    Table       string
    Columns     []string
    Values      [][]Expression  // For VALUES clause
    Select      *SelectStmt     // For SELECT clause
    Default     bool            // For DEFAULT VALUES
    OnConflict  *OnConflict
}

type OnConflict struct {
    Resolution ConflictResolution
    Target     *ConflictTarget
    Action     ConflictAction
}
```

### UPDATE

Modify existing rows in a table.

**Syntax:**
```sql
UPDATE [OR resolution] table_name
    SET column = expr [, column = expr]*
    [WHERE expr]
    [ORDER BY expr]
    [LIMIT expr];
```

**Examples:**
```sql
-- Update all rows
UPDATE users SET active = 1;

-- Update with WHERE clause
UPDATE users
SET email = 'new@example.com'
WHERE id = 42;

-- Update multiple columns
UPDATE users
SET username = 'newname',
    email = 'newemail@example.com',
    modified = strftime('%s', 'now')
WHERE id = 42;

-- Update with expressions
UPDATE products
SET price = price * 1.1
WHERE category = 'electronics';

-- Update with subquery
UPDATE users
SET post_count = (
    SELECT COUNT(*) FROM posts WHERE user_id = users.id
);

-- Update with LIMIT
UPDATE users
SET processed = 1
WHERE processed = 0
ORDER BY created ASC
LIMIT 100;
```

**Conflict Resolution:**
```sql
UPDATE OR REPLACE users SET username = 'admin' WHERE id = 1;
UPDATE OR IGNORE users SET email = 'duplicate@example.com' WHERE id = 2;
UPDATE OR ROLLBACK users SET status = 'invalid' WHERE check_failed;
```

### DELETE

Remove rows from a table.

**Syntax:**
```sql
DELETE FROM table_name
    [WHERE expr]
    [ORDER BY expr]
    [LIMIT expr];
```

**Examples:**
```sql
-- Delete all rows
DELETE FROM temp_data;

-- Delete specific rows
DELETE FROM users WHERE id = 42;

-- Delete with complex condition
DELETE FROM posts
WHERE created < strftime('%s', 'now', '-1 year')
  AND views < 10;

-- Delete with LIMIT
DELETE FROM logs
WHERE level = 'DEBUG'
ORDER BY timestamp ASC
LIMIT 1000;

-- Delete with subquery
DELETE FROM orphaned_posts
WHERE user_id NOT IN (SELECT id FROM users);
```

### REPLACE

Insert a new row or replace an existing row (shorthand for INSERT OR REPLACE).

**Syntax:**
```sql
REPLACE INTO table_name [(column_list)]
    VALUES (value_list);

REPLACE INTO table_name [(column_list)]
    select_statement;
```

**Examples:**
```sql
-- Replace single row
REPLACE INTO cache (key, value)
VALUES ('config', '{"setting": "value"}');

-- Replace from SELECT
REPLACE INTO current_prices
SELECT * FROM latest_prices;
```

### UPSERT

Insert a row, or update it if it already exists (INSERT ... ON CONFLICT).

**Syntax:**
```sql
INSERT INTO table_name [(column_list)]
    VALUES (value_list)
    ON CONFLICT [(conflict_target)] DO UPDATE SET
        column = expr [, column = expr]*
        [WHERE expr];

INSERT INTO table_name [(column_list)]
    VALUES (value_list)
    ON CONFLICT DO NOTHING;
```

**Examples:**
```sql
-- Update on conflict
INSERT INTO user_stats (user_id, login_count)
VALUES (42, 1)
ON CONFLICT(user_id) DO UPDATE
SET login_count = login_count + 1;

-- Update with WHERE clause
INSERT INTO products (id, name, stock)
VALUES (1, 'Widget', 100)
ON CONFLICT(id) DO UPDATE
SET stock = excluded.stock
WHERE excluded.stock > products.stock;

-- Do nothing on conflict
INSERT INTO unique_events (id, event)
VALUES (1, 'startup')
ON CONFLICT DO NOTHING;

-- Update multiple columns
INSERT INTO cache (key, value, hits)
VALUES ('page1', '<html>...</html>', 1)
ON CONFLICT(key) DO UPDATE
SET value = excluded.value,
    hits = hits + 1,
    updated = strftime('%s', 'now');
```

## Query Language

### SELECT Statement

Retrieve data from one or more tables.

**Basic Syntax:**
```sql
SELECT [DISTINCT | ALL] result_column [, ...]
FROM table_or_subquery [, ...]
[WHERE expr]
[GROUP BY expr [, ...] [HAVING expr]]
[WINDOW window_name AS window_defn [, ...]]
[ORDER BY ordering_term [, ...]]
[LIMIT expr [OFFSET expr]];
```

**Result Columns:**
```sql
-- All columns
SELECT * FROM users;

-- Specific columns
SELECT id, username, email FROM users;

-- Expressions
SELECT id, upper(username) AS name, age * 2 FROM users;

-- Table-qualified
SELECT u.id, u.username, p.title
FROM users u, posts p;
```

**DISTINCT:**
```sql
-- Remove duplicates
SELECT DISTINCT category FROM products;

-- All rows (default)
SELECT ALL username FROM users;
```

### WHERE Clause

Filter rows based on conditions.

**Examples:**
```sql
-- Simple conditions
SELECT * FROM users WHERE active = 1;
SELECT * FROM users WHERE age >= 18;

-- Logical operators
SELECT * FROM users
WHERE active = 1 AND age >= 18;

SELECT * FROM users
WHERE role = 'admin' OR role = 'moderator';

SELECT * FROM users
WHERE NOT deleted;

-- Pattern matching
SELECT * FROM users
WHERE username LIKE 'a%';

SELECT * FROM products
WHERE name GLOB '*[0-9]*';

-- NULL checks
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;

-- Range checks
SELECT * FROM products
WHERE price BETWEEN 10 AND 100;

SELECT * FROM users
WHERE id IN (1, 2, 3, 4, 5);

SELECT * FROM users
WHERE username IN (SELECT username FROM admins);
```

### JOIN Operations

Combine rows from multiple tables.

**INNER JOIN:**
```sql
SELECT u.username, p.title
FROM users u
INNER JOIN posts p ON u.id = p.user_id;
```

**LEFT JOIN:**
```sql
SELECT u.username, COUNT(p.id) AS post_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
GROUP BY u.id, u.username;
```

**CROSS JOIN:**
```sql
SELECT * FROM colors CROSS JOIN sizes;
```

**Join Types:**
```sql
-- INNER JOIN (only matching rows)
SELECT * FROM a JOIN b ON a.id = b.id;

-- LEFT JOIN (all from left, matching from right)
SELECT * FROM a LEFT JOIN b ON a.id = b.id;

-- RIGHT JOIN (not supported in SQLite/Anthony)
-- Use LEFT JOIN with swapped tables instead

-- FULL OUTER JOIN (not supported in SQLite/Anthony)
-- Use UNION of LEFT JOIN and filtered LEFT JOIN instead
```

### GROUP BY and HAVING

Aggregate rows into groups.

**Examples:**
```sql
-- Simple grouping
SELECT category, COUNT(*) AS count
FROM products
GROUP BY category;

-- Multiple columns
SELECT category, brand, AVG(price) AS avg_price
FROM products
GROUP BY category, brand;

-- With HAVING filter
SELECT category, COUNT(*) AS count
FROM products
GROUP BY category
HAVING COUNT(*) > 10;

-- Complex aggregation
SELECT
    strftime('%Y-%m', created) AS month,
    COUNT(*) AS posts,
    COUNT(DISTINCT user_id) AS unique_users
FROM posts
GROUP BY strftime('%Y-%m', created)
HAVING COUNT(*) > 100
ORDER BY month DESC;
```

### ORDER BY

Sort result rows.

**Examples:**
```sql
-- Ascending order (default)
SELECT * FROM users ORDER BY username;

-- Descending order
SELECT * FROM posts ORDER BY created DESC;

-- Multiple columns
SELECT * FROM users
ORDER BY last_name ASC, first_name ASC;

-- Expression ordering
SELECT * FROM products
ORDER BY price * quantity DESC;

-- NULL handling
SELECT * FROM users
ORDER BY email NULLS FIRST;

SELECT * FROM users
ORDER BY email NULLS LAST;
```

### LIMIT and OFFSET

Restrict the number of rows returned.

**Examples:**
```sql
-- First 10 rows
SELECT * FROM posts ORDER BY created DESC LIMIT 10;

-- Rows 11-20 (pagination)
SELECT * FROM posts ORDER BY created DESC LIMIT 10 OFFSET 10;

-- Alternative syntax
SELECT * FROM posts ORDER BY created DESC LIMIT 10, 10;
```

### Subqueries

Use SELECT statement as an expression or table source.

**Scalar Subquery:**
```sql
SELECT username,
       (SELECT COUNT(*) FROM posts WHERE user_id = users.id) AS post_count
FROM users;
```

**IN Subquery:**
```sql
SELECT * FROM products
WHERE category_id IN (
    SELECT id FROM categories WHERE active = 1
);
```

**EXISTS Subquery:**
```sql
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM posts p WHERE p.user_id = u.id
);
```

**Table Subquery:**
```sql
SELECT * FROM (
    SELECT user_id, COUNT(*) AS count
    FROM posts
    GROUP BY user_id
) WHERE count > 10;
```

### Common Table Expressions (WITH Clause)

Define temporary named result sets.

**Basic CTE:**
```sql
WITH active_users AS (
    SELECT * FROM users WHERE active = 1
)
SELECT * FROM active_users WHERE age >= 18;
```

**Multiple CTEs:**
```sql
WITH
    recent_posts AS (
        SELECT * FROM posts
        WHERE created > strftime('%s', 'now', '-7 days')
    ),
    active_users AS (
        SELECT * FROM users WHERE active = 1
    )
SELECT u.username, p.title
FROM active_users u
JOIN recent_posts p ON u.id = p.user_id;
```

**Recursive CTE:**
```sql
WITH RECURSIVE cnt(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM cnt WHERE x < 10
)
SELECT x FROM cnt;

-- Tree traversal
WITH RECURSIVE tree(id, parent_id, path) AS (
    SELECT id, parent_id, name AS path
    FROM nodes
    WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.parent_id, tree.path || '/' || n.name
    FROM nodes n
    JOIN tree ON n.parent_id = tree.id
)
SELECT * FROM tree;
```

**Go Implementation:**
```go
type SelectStmt struct {
    With        *WithClause
    Distinct    bool
    Columns     []*ResultColumn
    From        *FromClause
    Where       Expression
    GroupBy     []Expression
    Having      Expression
    Window      []*WindowDef
    OrderBy     []*OrderingTerm
    Limit       Expression
    Offset      Expression
}

type WithClause struct {
    Recursive bool
    CTEs      []*CTE
}

type CTE struct {
    Name    string
    Columns []string
    Select  *SelectStmt
}
```

### UNION, INTERSECT, EXCEPT

Combine results from multiple SELECT statements.

**UNION:**
```sql
-- Remove duplicates (default)
SELECT id FROM users
UNION
SELECT id FROM deleted_users;

-- Keep duplicates
SELECT id FROM users
UNION ALL
SELECT id FROM deleted_users;
```

**INTERSECT:**
```sql
-- Common rows
SELECT email FROM subscribers
INTERSECT
SELECT email FROM active_users;
```

**EXCEPT:**
```sql
-- Rows in first but not second
SELECT email FROM all_users
EXCEPT
SELECT email FROM bounced_emails;
```

### Window Functions

Perform calculations across sets of rows related to the current row.

**Syntax:**
```sql
function_name(...) OVER (
    [PARTITION BY expr [, ...]]
    [ORDER BY expr [, ...]]
    [frame_spec]
)
```

**Examples:**
```sql
-- Row number
SELECT username,
       ROW_NUMBER() OVER (ORDER BY created) AS row_num
FROM users;

-- Rank
SELECT username, score,
       RANK() OVER (ORDER BY score DESC) AS rank
FROM users;

-- Partition by category
SELECT category, product, price,
       AVG(price) OVER (PARTITION BY category) AS avg_category_price
FROM products;

-- Running total
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total
FROM transactions;

-- Named window
SELECT username, created,
       RANK() OVER w AS rank,
       DENSE_RANK() OVER w AS dense_rank
FROM users
WINDOW w AS (ORDER BY created);
```

## Expressions

### Operators

**Arithmetic:**
```sql
SELECT 10 + 5;   -- Addition: 15
SELECT 10 - 5;   -- Subtraction: 5
SELECT 10 * 5;   -- Multiplication: 50
SELECT 10 / 5;   -- Division: 2
SELECT 10 % 3;   -- Modulo: 1
```

**Comparison:**
```sql
SELECT 5 = 5;    -- Equality: 1 (true)
SELECT 5 != 3;   -- Inequality: 1 (true)
SELECT 5 <> 3;   -- Inequality: 1 (true)
SELECT 5 < 10;   -- Less than: 1 (true)
SELECT 5 <= 5;   -- Less or equal: 1 (true)
SELECT 5 > 3;    -- Greater than: 1 (true)
SELECT 5 >= 5;   -- Greater or equal: 1 (true)
```

**Logical:**
```sql
SELECT 1 AND 1;  -- Logical AND: 1
SELECT 1 OR 0;   -- Logical OR: 1
SELECT NOT 0;    -- Logical NOT: 1
```

**String:**
```sql
SELECT 'Hello' || ' ' || 'World';  -- Concatenation: 'Hello World'
SELECT 'abc' LIKE 'a%';            -- Pattern match: 1
SELECT 'abc' GLOB 'a*';            -- Glob match: 1
```

**Bitwise:**
```sql
SELECT 5 & 3;    -- Bitwise AND: 1
SELECT 5 | 3;    -- Bitwise OR: 7
SELECT ~5;       -- Bitwise NOT: -6
SELECT 5 << 1;   -- Left shift: 10
SELECT 5 >> 1;   -- Right shift: 2
```

### CASE Expression

Conditional logic in expressions.

**Simple CASE:**
```sql
SELECT username,
       CASE role
           WHEN 'admin' THEN 'Administrator'
           WHEN 'mod' THEN 'Moderator'
           ELSE 'User'
       END AS role_name
FROM users;
```

**Searched CASE:**
```sql
SELECT username,
       CASE
           WHEN age < 18 THEN 'Minor'
           WHEN age < 65 THEN 'Adult'
           ELSE 'Senior'
       END AS age_group
FROM users;
```

### CAST Expression

Convert values between types.

**Syntax:**
```sql
CAST(expr AS type_name)
```

**Examples:**
```sql
SELECT CAST('42' AS INTEGER);     -- 42
SELECT CAST(3.14 AS INTEGER);     -- 3
SELECT CAST(42 AS TEXT);          -- '42'
SELECT CAST('2024-01-15' AS TEXT);  -- '2024-01-15'
```

### Collation

Specify text comparison rules.

**Syntax:**
```sql
expr COLLATE collation_name
```

**Built-in Collations:**
- `BINARY`: Byte-by-byte comparison
- `NOCASE`: Case-insensitive ASCII
- `RTRIM`: Trailing spaces ignored

**Examples:**
```sql
SELECT * FROM users
ORDER BY username COLLATE NOCASE;

SELECT * FROM products
WHERE name = 'widget' COLLATE NOCASE;

CREATE TABLE t (
    name TEXT COLLATE NOCASE
);
```

## Functions

Anthony implements SQLite's built-in SQL functions.

### Aggregate Functions

**COUNT:**
```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT category) FROM products;
```

**SUM:**
```sql
SELECT SUM(price) FROM products;
SELECT SUM(price * quantity) FROM order_items;
```

**AVG:**
```sql
SELECT AVG(age) FROM users;
SELECT AVG(price) FROM products WHERE category = 'electronics';
```

**MIN/MAX:**
```sql
SELECT MIN(price), MAX(price) FROM products;
```

**GROUP_CONCAT:**
```sql
SELECT GROUP_CONCAT(username, ', ') FROM users;
SELECT GROUP_CONCAT(DISTINCT category) FROM products;
```

### String Functions

**length:**
```sql
SELECT length('hello');  -- 5
```

**substr:**
```sql
SELECT substr('hello', 2, 3);  -- 'ell'
```

**upper/lower:**
```sql
SELECT upper('hello');  -- 'HELLO'
SELECT lower('HELLO');  -- 'hello'
```

**trim/ltrim/rtrim:**
```sql
SELECT trim('  hello  ');   -- 'hello'
SELECT ltrim('  hello');    -- 'hello'
SELECT rtrim('hello  ');    -- 'hello'
```

**replace:**
```sql
SELECT replace('hello world', 'world', 'there');  -- 'hello there'
```

### Numeric Functions

**abs:**
```sql
SELECT abs(-42);  -- 42
```

**round:**
```sql
SELECT round(3.14159, 2);  -- 3.14
```

**min/max:**
```sql
SELECT min(5, 10, 3);  -- 3
SELECT max(5, 10, 3);  -- 10
```

**random:**
```sql
SELECT random();  -- Random integer
```

### Date/Time Functions

**datetime:**
```sql
SELECT datetime('now');
SELECT datetime('2024-01-15 10:30:00', '+7 days');
```

**date:**
```sql
SELECT date('now');
SELECT date('2024-01-15', 'start of month');
```

**time:**
```sql
SELECT time('now');
```

**strftime:**
```sql
SELECT strftime('%Y-%m-%d', 'now');
SELECT strftime('%H:%M:%S', 'now');
```

## Transactions

### BEGIN TRANSACTION

Start a new transaction.

**Syntax:**
```sql
BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE] [TRANSACTION];
```

**Examples:**
```sql
BEGIN;
BEGIN TRANSACTION;
BEGIN IMMEDIATE;
BEGIN EXCLUSIVE;
```

### COMMIT

Save all changes made in the current transaction.

**Syntax:**
```sql
COMMIT [TRANSACTION];
END [TRANSACTION];
```

### ROLLBACK

Discard all changes made in the current transaction.

**Syntax:**
```sql
ROLLBACK [TRANSACTION];
```

**Example:**
```sql
BEGIN;
INSERT INTO users (username) VALUES ('test');
-- Oops, wrong data
ROLLBACK;  -- Discards the INSERT
```

### SAVEPOINT

Create a point within a transaction to which you can later rollback.

**Syntax:**
```sql
SAVEPOINT savepoint_name;
RELEASE [SAVEPOINT] savepoint_name;
ROLLBACK [TRANSACTION] TO [SAVEPOINT] savepoint_name;
```

**Example:**
```sql
BEGIN;
INSERT INTO users (username) VALUES ('alice');
SAVEPOINT sp1;
INSERT INTO users (username) VALUES ('bob');
ROLLBACK TO sp1;  -- Keeps 'alice', discards 'bob'
COMMIT;
```

## Go Implementation Notes

### Parser API

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/parser"

// Parse a SQL statement
p := parser.NewParser(sql)
stmt, err := p.Parse()
if err != nil {
    log.Fatal(err)
}

// Type assertion to specific statement type
switch s := stmt.(type) {
case *parser.SelectStmt:
    // Handle SELECT
    fmt.Println("Columns:", s.Columns)

case *parser.InsertStmt:
    // Handle INSERT
    fmt.Println("Table:", s.Table)

case *parser.CreateTableStmt:
    // Handle CREATE TABLE
    fmt.Println("Columns:", len(s.Columns))
}
```

### Statement Execution

```go
import "database/sql"

db, _ := sql.Open("anthony", "mydb.db")

// Execute DML
result, err := db.Exec(`
    INSERT INTO users (username, email)
    VALUES (?, ?)
`, "alice", "alice@example.com")

// Query data
rows, err := db.Query(`
    SELECT id, username FROM users WHERE active = ?
`, 1)
defer rows.Close()

for rows.Next() {
    var id int64
    var username string
    rows.Scan(&id, &username)
    fmt.Println(id, username)
}
```

### Prepared Statements

```go
// Prepare statement
stmt, err := db.Prepare(`
    SELECT * FROM users WHERE id = ?
`)
defer stmt.Close()

// Execute multiple times
for _, id := range userIDs {
    row := stmt.QueryRow(id)
    // Process row...
}
```

## References

- **Package:** `internal/parser` - SQL parsing
- **Package:** `internal/planner` - Query planning
- **Package:** `internal/sql` - SQL compilation
- **Package:** `internal/vdbe` - Bytecode execution

## See Also

- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type affinity and conversions
- [FILE_FORMAT.md](FILE_FORMAT.md) - Database file format
- [PRAGMAS.md](PRAGMAS.md) - PRAGMA commands
- [API.md](API.md) - Go API documentation

## SQLite SQL Reference (local)

Official SQLite SQL language documentation, available locally:

- [SQL Language Overview](sqlite/SQL_LANGUAGE_OVERVIEW.md)
- [SELECT](sqlite/SELECT.md) * [INSERT](sqlite/INSERT.md) * [UPDATE](sqlite/UPDATE.md) * [DELETE](sqlite/DELETE.md)
- [WITH (CTE)](sqlite/WITH_CTE.md) * [UPSERT](sqlite/UPSERT.md) * [RETURNING](sqlite/RETURNING.md)
- [Window Functions](sqlite/WINDOW_FUNCTIONS.md) * [Savepoints](sqlite/SAVEPOINTS.md) * [Transactions](sqlite/TRANSACTIONS.md)
- [Expressions](sqlite/EXPRESSIONS.md) * [Core Functions](sqlite/CORE_FUNCTIONS.md)
- [Full index ->](sqlite/README.md)
