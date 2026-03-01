# SQLite JSON Functions

This document describes the JSON functions implemented in the Anthony SQLite library.

## Overview

The JSON functions provide support for storing, querying, and manipulating JSON data within SQLite. All functions are implemented in `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/functions/json.go` and registered in `RegisterJSONFunctions()`.

## Core JSON Functions

### 1. json(X)

Validates and minifies JSON input.

**Syntax:** `json(X)`

**Returns:** Minified JSON string or NULL if invalid

**Examples:**
```sql
SELECT json('{"a": 1, "b": 2}');  -- Returns: {"a":1,"b":2}
SELECT json('[1, 2, 3]');          -- Returns: [1,2,3]
SELECT json('{invalid}');          -- Returns: NULL
```

### 2. json_valid(X)

Checks if X is valid JSON.

**Syntax:** `json_valid(X)`

**Returns:** 1 if valid JSON, 0 otherwise

**Examples:**
```sql
SELECT json_valid('{"a":1}');  -- Returns: 1
SELECT json_valid('[1,2,3]');  -- Returns: 1
SELECT json_valid('{invalid}'); -- Returns: 0
```

### 3. json_type(X [, path])

Returns the type of the JSON value at the specified path.

**Syntax:** `json_type(X [, path])`

**Returns:** One of: "null", "true", "false", "integer", "real", "text", "array", "object"

**Examples:**
```sql
SELECT json_type('123');              -- Returns: integer
SELECT json_type('{"a":1}');          -- Returns: object
SELECT json_type('[1,2,3]');          -- Returns: array
SELECT json_type('"hello"');          -- Returns: text
SELECT json_type('null');             -- Returns: null
SELECT json_type('{"x":[1,2]}', '$.x'); -- Returns: array
```

### 4. json_extract(X, path [, path2, ...])

Extracts values from JSON at the specified path(s).

**Syntax:** `json_extract(X, path [, path2, ...])`

**Returns:** The extracted value(s)

**Path Syntax:**
- `$` - Root element
- `$.key` - Object key
- `$[n]` - Array element at index n
- `$.key1.key2` - Nested object keys
- `$.key[n]` - Array element within object

**Examples:**
```sql
SELECT json_extract('{"a":1}', '$.a');                    -- Returns: 1
SELECT json_extract('{"name":"John"}', '$.name');         -- Returns: John
SELECT json_extract('{"a":{"b":{"c":42}}}', '$.a.b.c');  -- Returns: 42
SELECT json_extract('[1,2,3]', '$[1]');                   -- Returns: 2
SELECT json_extract('{"x":1,"y":2}', '$.x', '$.y');       -- Returns: [1,2]
```

### 5. json_array([value1, value2, ...])

Creates a JSON array from the given values.

**Syntax:** `json_array([value1, value2, ...])`

**Returns:** JSON array string

**Examples:**
```sql
SELECT json_array();                    -- Returns: []
SELECT json_array(1, 2, 3);             -- Returns: [1,2,3]
SELECT json_array(1, 'hello', 3.14);    -- Returns: [1,"hello",3.14]
SELECT json_array(1, NULL, 3);          -- Returns: [1,null,3]
```

### 6. json_object(key1, value1 [, key2, value2, ...])

Creates a JSON object from key-value pairs.

**Syntax:** `json_object(key1, value1 [, key2, value2, ...])`

**Returns:** JSON object string

**Note:** Keys cannot be NULL. Number of arguments must be even.

**Examples:**
```sql
SELECT json_object();                              -- Returns: {}
SELECT json_object('x', 1);                        -- Returns: {"x":1}
SELECT json_object('name', 'Alice', 'age', 30);    -- Returns: {"age":30,"name":"Alice"}
SELECT json_object('x', NULL);                     -- Returns: {"x":null}
```

## Additional JSON Functions

The implementation also includes these advanced functions:

### json_array_length(X [, path])

Returns the length of a JSON array.

```sql
SELECT json_array_length('[1,2,3]');              -- Returns: 3
SELECT json_array_length('{"a":[1,2,3]}', '$.a'); -- Returns: 3
```

### json_insert(X, path1, value1 [, path2, value2, ...])

Inserts values into JSON only if the path doesn't exist.

```sql
SELECT json_insert('{"x":1}', '$.y', 2);  -- Returns: {"x":1,"y":2}
SELECT json_insert('{"x":1}', '$.x', 2);  -- Returns: {"x":1} (unchanged)
```

### json_replace(X, path1, value1 [, path2, value2, ...])

Replaces values in JSON only if the path exists.

```sql
SELECT json_replace('{"x":1}', '$.x', 2);  -- Returns: {"x":2}
SELECT json_replace('{"x":1}', '$.y', 2);  -- Returns: {"x":1} (unchanged)
```

### json_set(X, path1, value1 [, path2, value2, ...])

Sets values in JSON (creates or replaces).

```sql
SELECT json_set('{"x":1}', '$.x', 2);      -- Returns: {"x":2}
SELECT json_set('{"x":1}', '$.y', 2);      -- Returns: {"x":1,"y":2}
SELECT json_set('{}', '$.a.b.c', 1);       -- Returns: {"a":{"b":{"c":1}}}
```

### json_remove(X, path1 [, path2, ...])

Removes values from JSON at the specified paths.

```sql
SELECT json_remove('{"x":1,"y":2}', '$.y');           -- Returns: {"x":1}
SELECT json_remove('[1,2,3]', '$[1]');                -- Returns: [1,3]
SELECT json_remove('{"x":1,"y":2,"z":3}', '$.x', '$.z'); -- Returns: {"y":2}
```

### json_patch(X, Y)

Applies RFC 7396 JSON Merge Patch.

```sql
SELECT json_patch('{"x":1}', '{"y":2}');     -- Returns: {"x":1,"y":2}
SELECT json_patch('{"x":1}', '{"x":2}');     -- Returns: {"x":2}
SELECT json_patch('{"x":1,"y":2}', '{"y":null}'); -- Returns: {"x":1}
```

### json_quote(X)

Quotes a value as a JSON string.

```sql
SELECT json_quote('hello');   -- Returns: "hello"
SELECT json_quote(42);        -- Returns: 42
SELECT json_quote(NULL);      -- Returns: null
```

## Real-World Usage Example

```sql
-- Create a table with JSON data
CREATE TABLE users (id INTEGER, profile TEXT);

-- Insert users with JSON profiles
INSERT INTO users VALUES (1, '{"name":"Alice","age":30,"city":"NYC"}');
INSERT INTO users VALUES (2, '{"name":"Bob","age":25,"city":"LA"}');
INSERT INTO users VALUES (3, '{"name":"Charlie","age":35,"city":"SF"}');

-- Extract all names
SELECT id, json_extract(profile, '$.name') AS name FROM users;

-- Filter by age > 28
SELECT id,
       json_extract(profile, '$.name') AS name,
       json_extract(profile, '$.age') AS age
FROM users
WHERE json_extract(profile, '$.age') > 28;

-- Count valid JSON profiles
SELECT COUNT(*) FROM users WHERE json_valid(profile) = 1;
```

## Implementation Details

- **Location:** `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/functions/json.go`
- **Registration:** `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/functions/functions.go` in `RegisterJSONFunctions()`
- **Parser:** Uses Go's standard library `encoding/json` package
- **Path Support:** Simplified JSONPath syntax supporting `$`, `.key`, and `[index]` notation
- **NULL Handling:** Invalid JSON operations return NULL
- **Testing:** Comprehensive unit tests in `json_test.go` and integration tests in `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/json_integration_test.go`

## Running Tests

### Unit Tests
```bash
nix-shell --run "go test -v ./internal/functions -run JSON"
```

### Integration Tests
```bash
nix-shell --run "go test -v ./internal/driver -run TestJSONFunctions"
```

### Demo Program
```bash
nix-shell --run "go run ./examples/json_demo.go"
```

## Notes

- All JSON functions are case-insensitive (can be called as `JSON_VALID`, `json_valid`, etc.)
- Invalid JSON input typically returns NULL rather than an error
- JSON numbers are automatically converted to SQLite INTEGER or REAL types
- Complex JSON structures (arrays, objects) are returned as JSON strings when extracted
- The implementation follows SQLite's JSON1 extension behavior

## See Also

- [SQLite JSON1 Extension Reference (local)](sqlite/JSON1.md) -- complete official JSON1 docs
