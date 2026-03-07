# VACUUM Usage Guide

## Quick Reference

The VACUUM command rebuilds your database file, removing unused space and defragmenting data.

## When to Use VACUUM

Use VACUUM when you want to:
- Reclaim disk space after deleting many rows
- Defragment a heavily modified database
- Create a clean, compacted copy of your database
- Optimize database file layout for better performance

## Syntax

### Basic VACUUM
```sql
VACUUM;
```
Rebuilds the main database, removing all free pages.

### VACUUM INTO (Backup)
```sql
VACUUM INTO 'backup.db';
```
Creates a compacted copy in a new file without modifying the original.

### VACUUM Attached Database
```sql
VACUUM mydb;
```
Vacuums a specific attached database schema.

### Combined
```sql
VACUUM mydb INTO 'mydb_backup.db';
```
Vacuum a specific schema into a new file.

## Examples

### Example 1: Reclaim Space After Deletes

```sql
-- Create a table and populate it
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    message TEXT,
    timestamp DATETIME
);

-- Insert 1 million rows
INSERT INTO logs (message, timestamp) VALUES ('Log entry', datetime('now'));
-- ... (repeat for many rows)

-- Delete 90% of the rows
DELETE FROM logs WHERE id % 10 != 0;

-- Database file is still large because of free pages
-- VACUUM to reclaim space
VACUUM;

-- Database file is now much smaller
```

### Example 2: Create a Compacted Backup

```sql
-- Backup and compact in one operation
VACUUM INTO 'mydb_backup_2024.db';

-- Original database is unchanged
-- Backup contains all data, fully compacted
```

### Example 3: Regular Maintenance

```go
package main

import (
    "database/sql"
    _ "github.com/cyanitol/Public.Lib.Anthony"
)

func main() {
    db, _ := sql.Open("anthony", "myapp.db")
    defer db.Close()

    // Perform regular operations
    // ...

    // Periodic maintenance (e.g., weekly)
    _, err := db.Exec("VACUUM")
    if err != nil {
        log.Printf("VACUUM failed: %v", err)
    }
}
```

## Best Practices

### DO:
- Run VACUUM during off-peak hours (it locks the database)
- Ensure you have enough disk space (needs ~2x database size temporarily)
- Use VACUUM INTO for backups instead of file copying
- Run VACUUM after bulk DELETE operations
- Monitor database file size to determine when VACUUM is needed

### DON'T:
- Run VACUUM during active transactions
- Run VACUUM on very large databases during peak hours
- Rely on VACUUM for regular operation (it's maintenance, not a requirement)
- Run VACUUM too frequently (it's I/O intensive)
- Run VACUUM on read-only databases

## Performance Impact

### Time Complexity
- O(n) where n = number of pages in database
- Approximately: `(database_size / disk_speed) * 2`
- Example: 1GB database on SSD ~= 2-5 seconds

### Space Requirements
- Temporary disk space needed: ~same as database size
- Final database size: sum of all live pages
- Space saved: all free pages are removed

### Lock Duration
- Exclusive lock held for entire operation
- No concurrent reads or writes possible
- Consider using VACUUM INTO for production systems

## Common Use Cases

### 1. Post-Migration Cleanup
```sql
-- After migrating data from old tables
DROP TABLE old_users;
DROP TABLE old_orders;
VACUUM;  -- Reclaim space from dropped tables
```

### 2. Nightly Maintenance
```sql
-- Cron job or scheduled task
VACUUM;
```

### 3. Before Backup
```sql
-- Create optimized backup
VACUUM INTO 'backup_$(date +%Y%m%d).db';
```

### 4. Development Database Reset
```sql
-- Clean up development database
DELETE FROM test_data WHERE created_at < date('now', '-7 days');
VACUUM;
```

## Monitoring VACUUM

### Check Free Pages
```sql
-- Query database header (if supported)
PRAGMA freelist_count;
```

### Check File Size
```bash
# Before VACUUM
ls -lh mydb.db

# After VACUUM
ls -lh mydb.db
# File should be smaller if there were free pages
```

### Measure Impact
```go
// Get file size before and after
beforeStat, _ := os.Stat("mydb.db")
beforeSize := beforeStat.Size()

db.Exec("VACUUM")

afterStat, _ := os.Stat("mydb.db")
afterSize := afterStat.Size()

saved := beforeSize - afterSize
fmt.Printf("VACUUM saved %d bytes (%.1f%%)\n",
    saved, float64(saved)/float64(beforeSize)*100)
```

## Error Handling

### Common Errors

#### Database is Locked
```
Error: database is locked (SQLITE_BUSY)
```
**Solution**: Close all transactions and retry

#### Not Enough Disk Space
```
Error: disk full (SQLITE_FULL)
```
**Solution**: Free up disk space or use VACUUM INTO to a different disk

#### Database is Read-Only
```
Error: pager is read-only
```
**Solution**: Open database in read-write mode

#### Active Transaction
```
Error: transaction already open
```
**Solution**: Commit or rollback current transaction first

### Proper Error Handling

```go
_, err := db.Exec("VACUUM")
if err != nil {
    switch {
    case strings.Contains(err.Error(), "read-only"):
        log.Println("Cannot VACUUM: database is read-only")
    case strings.Contains(err.Error(), "transaction"):
        log.Println("Cannot VACUUM: close current transaction first")
    case strings.Contains(err.Error(), "locked"):
        log.Println("Cannot VACUUM: database is locked, retry later")
    default:
        log.Printf("VACUUM error: %v", err)
    }
}
```

## Integration with Applications

### Web Application Example
```go
// Scheduled maintenance endpoint (admin only)
http.HandleFunc("/admin/vacuum", func(w http.ResponseWriter, r *http.Request) {
    // Verify admin authentication
    // ...

    start := time.Now()
    _, err := db.Exec("VACUUM")
    duration := time.Since(start)

    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    fmt.Fprintf(w, "VACUUM completed in %v", duration)
})
```

### Automated Maintenance
```go
// Run VACUUM weekly
func scheduleVacuum(db *sql.DB) {
    ticker := time.NewTicker(7 * 24 * time.Hour)
    defer ticker.Stop()

    for range ticker.C {
        log.Println("Starting scheduled VACUUM")
        if _, err := db.Exec("VACUUM"); err != nil {
            log.Printf("Scheduled VACUUM failed: %v", err)
        } else {
            log.Println("Scheduled VACUUM completed")
        }
    }
}
```

## Comparison: VACUUM vs VACUUM INTO

| Aspect | VACUUM | VACUUM INTO |
|--------|--------|-------------|
| Original DB | Modified | Unchanged |
| Disk Space | Needs 2x temporarily | Needs 1x for target |
| Downtime | Yes (locked) | No (for original) |
| Use Case | Maintenance | Backup + compact |
| Speed | Fast | Same speed |
| Safety | Safe (atomic) | Very safe (copy) |

## Conclusion

VACUUM is a powerful maintenance tool for optimizing your Anthony SQLite database. Use it wisely:
- After bulk deletes
- During scheduled maintenance
- For creating compacted backups
- To optimize file layout

Remember: VACUUM is maintenance, not magic. It won't speed up slow queries, but it will help keep your database file compact and efficient.
