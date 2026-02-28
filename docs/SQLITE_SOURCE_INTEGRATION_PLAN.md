# SQLite Source Integration Plan

This document describes the integration of official SQLite source code into the Anthony project for reference, test generation, and compatibility verification.

## Purpose and Scope

The SQLite source code integration serves several critical purposes:

1. **Reference Implementation** - Consult SQLite's C implementation for complex features
2. **Test Generation** - Adapt SQLite's comprehensive TCL test suite for Anthony
3. **Compatibility Verification** - Ensure bytecode, file format, and behavior compatibility
4. **Documentation** - Reference SQLite's inline documentation and design notes
5. **Regression Testing** - Compare Anthony's output against official SQLite

**Important**: This is a read-only reference integration. We do NOT compile or link SQLite source into Anthony. Anthony remains a pure Go implementation with zero CGO dependencies.

## 1. Source Acquisition

### 1.1 Download Location

Official SQLite source is available from:
- **Primary**: https://sqlite.org/src/
- **Mirror**: https://github.com/sqlite/sqlite (read-only mirror)
- **Tarball**: https://sqlite.org/download.html

### 1.2 Download Procedure

Choose one of the following methods:

#### Method A: Fossil Repository (Recommended)

```bash
# Install fossil if not already installed
# On Ubuntu/Debian: sudo apt-get install fossil
# On macOS: brew install fossil

# Clone the SQLite fossil repository
cd /home/justin/Programming/Workspace/Public.Lib.Anthony/contrib/sqlite/
fossil clone https://sqlite.org/src/sqlite.fossil sqlite.fossil

# Open the repository
fossil open sqlite.fossil

# This creates a checkout of the latest trunk in the current directory
```

#### Method B: GitHub Mirror

```bash
cd /home/justin/Programming/Workspace/Public.Lib.Anthony/contrib/sqlite/
git clone https://github.com/sqlite/sqlite.git official-source
cd official-source
```

#### Method C: Tarball Download

```bash
cd /home/justin/Programming/Workspace/Public.Lib.Anthony/contrib/sqlite/
# Download the amalgamation (for quick reference)
wget https://sqlite.org/2025/sqlite-amalgamation-3470000.zip
unzip sqlite-amalgamation-3470000.zip

# Download the full source (for tests and docs)
wget https://sqlite.org/2025/sqlite-src-3470000.zip
unzip sqlite-src-3470000.zip
```

### 1.3 Version Selection

For compatibility testing, we should track multiple SQLite versions:

- **Latest Stable** - Currently 3.47.0 (as of January 2025)
- **Common Production** - 3.43.x (widely deployed)
- **Historic Reference** - 3.35.x (for backward compatibility)

Store version-specific sources in separate directories.

## 2. Directory Structure

### 2.1 Recommended Layout

```
contrib/
└── sqlite/
    ├── README.md                    # Integration documentation
    ├── VERSION.txt                  # Current version tracking
    ├── .gitignore                   # Ignore build artifacts
    │
    ├── official-source/             # Primary reference (fossil or git)
    │   ├── src/                     # Core C source files
    │   ├── test/                    # TCL test suite
    │   ├── doc/                     # Documentation
    │   ├── ext/                     # Extensions
    │   ├── tool/                    # Build tools
    │   └── manifest.uuid            # Fossil version identifier
    │
    ├── versions/                    # Version-specific snapshots
    │   ├── 3.47.0/
    │   │   ├── src/
    │   │   └── test/
    │   ├── 3.43.2/
    │   │   ├── src/
    │   │   └── test/
    │   └── 3.35.5/
    │       ├── src/
    │       └── test/
    │
    ├── extracted-tests/             # TCL tests adapted for Anthony
    │   ├── select/
    │   ├── join/
    │   ├── cte/
    │   ├── trigger/
    │   └── README.md                # Test extraction guidelines
    │
    ├── reference-builds/            # Optional: compiled SQLite for testing
    │   ├── build-3.47.0/
    │   │   └── sqlite3              # Compiled binary
    │   └── build-3.43.2/
    │       └── sqlite3
    │
    └── scripts/                     # Integration utilities
        ├── update-source.sh         # Update to latest SQLite
        ├── extract-tests.sh         # Extract and convert tests
        ├── build-reference.sh       # Build reference binaries
        └── compare-output.sh        # Compare Anthony vs SQLite
```

### 2.2 .gitignore Configuration

Create `contrib/sqlite/.gitignore`:

```
# Fossil repository files
*.fossil
_FOSSIL_

# Build artifacts
*.o
*.so
*.dylib
*.dll
*.exe
sqlite3
reference-builds/*/

# Temporary files
*.tmp
*.log
*.out

# Editor files
.vscode/
.idea/
*.swp
*~

# But keep version tracking
!VERSION.txt
!README.md
```

### 2.3 Version Tracking

Create `contrib/sqlite/VERSION.txt`:

```
Current SQLite Version: 3.47.0
Last Updated: 2025-01-15
Fossil Hash: [hash from manifest.uuid]
Source Method: fossil
Update Frequency: Quarterly

Tracked Versions:
- 3.47.0 (latest stable)
- 3.43.2 (common production)
- 3.35.5 (backward compatibility)

Next Review: 2025-04-15
```

## 3. Key Source Files Reference

### 3.1 Core Source Files (src/)

Essential files for implementation reference:

| File | Purpose | Reference Use |
|------|---------|---------------|
| `src/vdbe.c` | Virtual Database Engine | Bytecode execution, opcode implementation |
| `src/vdbeaux.c` | VDBE auxiliary functions | Cursor management, register operations |
| `src/vdbeInt.h` | VDBE internal structures | Data structures, opcode definitions |
| `src/vdbemem.c` | VDBE memory management | Value storage, type affinity |
| `src/btree.c` | B-tree implementation | Page layout, tree navigation |
| `src/pager.c` | Page cache and journal | Transaction management, WAL |
| `src/btreeInt.h` | B-tree internals | Page structure, cell format |
| `src/select.c` | SELECT compilation | Query planning, subqueries |
| `src/expr.c` | Expression compilation | Expression trees, type coercion |
| `src/where.c` | WHERE clause optimization | Index selection, join ordering |
| `src/parse.y` | SQL grammar (yacc) | SQL syntax rules |
| `src/insert.c` | INSERT compilation | Row insertion, constraint checks |
| `src/update.c` | UPDATE compilation | Update logic, triggers |
| `src/delete.c` | DELETE compilation | Deletion logic |
| `src/build.c` | DDL compilation | CREATE/ALTER/DROP tables |
| `src/pragma.c` | PRAGMA implementation | Database configuration |
| `src/trigger.c` | Trigger compilation | Trigger execution |
| `src/attach.c` | ATTACH/DETACH | Multi-database support |
| `src/func.c` | Built-in functions | Scalar functions |
| `src/date.c` | Date/time functions | Temporal operations |
| `src/utf.c` | UTF encoding | String handling |
| `src/printf.c` | printf implementation | String formatting |
| `src/random.c` | Random number generation | RANDOM() function |
| `src/vdbesort.c` | Sorting engine | ORDER BY implementation |
| `src/walker.c` | AST tree walker | Expression traversal |
| `src/resolve.c` | Name resolution | Column/table lookup |

### 3.2 Test Files (test/)

Critical test files for Anthony compatibility:

| File | Coverage | Priority |
|------|----------|----------|
| `test/select1.test` | Basic SELECT | High |
| `test/select2.test` | Complex SELECT | High |
| `test/select3.test` | Subqueries | High |
| `test/select4.test` | UNION/INTERSECT | High |
| `test/select5.test` | ORDER BY/GROUP BY | High |
| `test/join.test` | JOIN operations | High |
| `test/join2.test` | Complex joins | High |
| `test/where.test` | WHERE clauses | High |
| `test/where2.test` | WHERE optimization | Medium |
| `test/expr.test` | Expressions | High |
| `test/types.test` | Type affinity | High |
| `test/types2.test` | Type coercion | Medium |
| `test/insert.test` | INSERT operations | High |
| `test/insert2.test` | INSERT edge cases | Medium |
| `test/update.test` | UPDATE operations | High |
| `test/delete.test` | DELETE operations | High |
| `test/with1.test` | CTEs | High |
| `test/trigger1.test` | Triggers | Medium |
| `test/trigger2.test` | Complex triggers | Low |
| `test/btree.test` | B-tree operations | Medium |
| `test/pager1.test` | Pager operations | Medium |
| `test/trans.test` | Transactions | High |
| `test/lock.test` | Locking | Medium |
| `test/corrupt.test` | Corruption handling | Low |
| `test/boundary1.test` | Boundary conditions | Medium |
| `test/fuzz.test` | Fuzzing tests | Low |
| `test/constraint.test` | Constraints | High |
| `test/fkey1.test` | Foreign keys | Medium |
| `test/check.test` | CHECK constraints | Medium |
| `test/unique.test` | UNIQUE constraints | High |
| `test/index.test` | Index operations | High |
| `test/analyze.test` | ANALYZE | Low |
| `test/vacuum.test` | VACUUM | Medium |
| `test/pragma.test` | PRAGMA statements | Medium |
| `test/attach.test` | ATTACH/DETACH | Low |
| `test/collate.test` | Collation | Medium |
| `test/func.test` | Built-in functions | High |
| `test/date.test` | Date/time functions | Medium |
| `test/cast.test` | CAST operations | Medium |
| `test/alter.test` | ALTER TABLE | Medium |

### 3.3 Documentation Files (doc/)

| File | Content | Use Case |
|------|---------|----------|
| `doc/fileformat2.wiki` | Database file format | File structure verification |
| `doc/opcode.wiki` | VDBE opcodes | Bytecode implementation |
| `doc/vdbe.wiki` | VDBE design | Execution engine design |
| `doc/btree.wiki` | B-tree design | Storage layer design |
| `doc/locking.wiki` | Locking protocol | Concurrency control |
| `doc/wal.wiki` | Write-Ahead Logging | WAL implementation |
| `doc/queryplanner.wiki` | Query planner | Optimization strategies |
| `doc/datatype3.wiki` | Type affinity | Type system |

### 3.4 Extension Files (ext/)

Useful for future virtual table development:

- `ext/fts5/` - Full-text search (FTS5)
- `ext/rtree/` - R-tree spatial index
- `ext/json1/` - JSON functions
- `ext/misc/` - Miscellaneous extensions

## 4. Version Management

### 4.1 Tracking SQLite Versions

Maintain a mapping between Anthony versions and tested SQLite versions:

| Anthony Version | SQLite Version | Compatibility Level |
|----------------|----------------|---------------------|
| 0.1.x | 3.35.5 | Basic file format |
| 0.2.x | 3.43.2 | Core SQL features |
| 0.3.x | 3.47.0 | Advanced features |
| 1.0.0 | 3.47.0+ | Production ready |

### 4.2 Version Update Schedule

- **Quarterly Reviews** - Check for new SQLite releases
- **Major Releases** - Update when SQLite has significant changes
- **Security Updates** - Immediate review of security fixes
- **Breaking Changes** - Document any file format changes

### 4.3 Version Documentation

Create `contrib/sqlite/versions/VERSION-NOTES.md`:

```markdown
# SQLite Version Notes

## 3.47.0 (2024-12-15)

### New Features
- Enhanced JSON support
- Improved query planner
- New STRICT tables option

### Relevant Changes for Anthony
- New opcodes: OpXYZ, OpABC
- File format unchanged (remains version 1)
- New test cases in select*.test

### Integration Status
- [ ] Review new opcodes
- [ ] Port new test cases
- [ ] Update compatibility matrix
- [ ] Test file format compatibility

## 3.43.2 (2023-08-24)

...
```

## 5. Build Integration (Optional)

Building reference SQLite binaries enables comparative testing.

### 5.1 Building SQLite from Source

Create `contrib/sqlite/scripts/build-reference.sh`:

```bash
#!/bin/bash
# Build reference SQLite binaries for comparison testing

set -e

SQLITE_VERSION="${1:-3.47.0}"
SOURCE_DIR="contrib/sqlite/official-source"
BUILD_DIR="contrib/sqlite/reference-builds/build-${SQLITE_VERSION}"

echo "Building SQLite ${SQLITE_VERSION}..."

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Configure and build
# Enable debugging symbols for analysis
CFLAGS="-g -O0 -DSQLITE_DEBUG" \
../../official-source/configure \
    --enable-debug \
    --enable-explain-comments

make sqlite3

echo "Built: ${BUILD_DIR}/sqlite3"
echo "Test with: ${BUILD_DIR}/sqlite3 test.db"
```

### 5.2 Build Configuration

For compatibility testing, build with specific options:

```bash
# Minimal build (matches Anthony's feature set)
./configure --disable-amalgamation --enable-debug

# Full build (all features)
./configure --enable-all --enable-debug

# Specific features
./configure \
    --enable-fts5 \
    --enable-rtree \
    --enable-json1 \
    --enable-debug
```

### 5.3 Using Reference Builds

```bash
# Compare output between Anthony and SQLite
cd contrib/sqlite/scripts/

# Run the same query on both
echo "SELECT * FROM test;" | \
    /path/to/anthony-cli test.db > anthony-output.txt

echo "SELECT * FROM test;" | \
    ../reference-builds/build-3.47.0/sqlite3 test.db > sqlite-output.txt

# Compare
diff anthony-output.txt sqlite-output.txt
```

## 6. License and Attribution

### 6.1 SQLite License

SQLite is in the **public domain**. From the SQLite project:

> **Public Domain Notice**
>
> The original author and copyright holder of all SQLite software disclaims all copyright.
>
> All contributors to the SQLite software have disclaimed all copyright to their contributions.
>
> SQLite is thus completely free to use for any purpose, commercial or private.

### 6.2 Attribution Requirements

While not legally required, we should acknowledge SQLite:

1. **In Documentation**: Credit SQLite for inspiration and reference
2. **In Source Comments**: When directly referencing SQLite implementation
3. **In README**: Acknowledge SQLite's public domain status

### 6.3 Attribution Template

Add to `contrib/sqlite/README.md`:

```markdown
# SQLite Source Reference

This directory contains reference copies of the official SQLite source code
for development and testing purposes.

## Attribution

This directory contains source code from the SQLite project (https://sqlite.org/).

SQLite is in the public domain. The authors disclaim copyright to this source code.
In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.

We are grateful to the SQLite team for their excellent work and for placing
their software in the public domain.

## Purpose

The SQLite source is used in the Anthony project for:

1. Reference implementation consultation
2. Test case generation and adaptation
3. Compatibility verification
4. Documentation reference

**Note**: The Anthony project is a from-scratch pure Go implementation.
We do NOT compile or link any SQLite C code. This is strictly for reference.
```

### 6.4 Source File Headers

When adapting SQLite tests or algorithms, add attribution:

```go
// This implementation is based on SQLite's [feature] as documented in [file].
// Reference: https://sqlite.org/src/file?name=src/[filename]
// SQLite is in the public domain.
```

## 7. Update Procedures

### 7.1 Regular Update Workflow

Create `contrib/sqlite/scripts/update-source.sh`:

```bash
#!/bin/bash
# Update SQLite source to latest version

set -e

cd contrib/sqlite/

echo "Updating SQLite source..."

# Method 1: Fossil update
if [ -f "sqlite.fossil" ]; then
    fossil update trunk
    FOSSIL_HASH=$(fossil info | grep checkout | awk '{print $2}')
    echo "Updated to fossil hash: ${FOSSIL_HASH}"
fi

# Method 2: Git update
if [ -d "official-source/.git" ]; then
    cd official-source
    git pull origin master
    GIT_HASH=$(git rev-parse HEAD)
    echo "Updated to git hash: ${GIT_HASH}"
    cd ..
fi

# Update VERSION.txt
echo "Current SQLite Version: $(grep VERSION official-source/VERSION)" > VERSION.txt
echo "Last Updated: $(date +%Y-%m-%d)" >> VERSION.txt

echo "Update complete. Review changes and update documentation."
```

### 7.2 Update Checklist

When updating to a new SQLite version:

- [ ] Download new source (fossil update / git pull)
- [ ] Update VERSION.txt with new version number
- [ ] Review SQLite CHANGELOG for breaking changes
- [ ] Scan for new opcodes in src/vdbe.c
- [ ] Check for file format changes in src/btree.c
- [ ] Review new test files in test/
- [ ] Build reference binary for new version
- [ ] Run Anthony test suite against new reference
- [ ] Document compatibility issues
- [ ] Update version mapping in docs
- [ ] Create snapshot in versions/ if major release

### 7.3 Change Analysis Process

After updating SQLite source:

```bash
# 1. Check for new opcodes
cd contrib/sqlite/official-source/src/
grep "case OP_" vdbe.c | sort > /tmp/opcodes-new.txt
# Compare with previous version
diff /tmp/opcodes-old.txt /tmp/opcodes-new.txt

# 2. Check for file format changes
git log --all --grep="file format" --oneline

# 3. Check for new test files
cd ../test/
ls -t | head -20

# 4. Review breaking changes
git log --since="3 months ago" --grep="breaking\|incompatible" --oneline
```

### 7.4 Integration Testing

After each SQLite update:

```bash
# 1. Build reference binary
contrib/sqlite/scripts/build-reference.sh 3.47.0

# 2. Run Anthony's test suite
go test ./... -v

# 3. Run compatibility tests
go test ./test/compatibility -sqlite-path=contrib/sqlite/reference-builds/build-3.47.0/sqlite3

# 4. Compare file formats
go test ./internal/format -compare-with-sqlite

# 5. Check bytecode compatibility
go test ./internal/vdbe -explain-compare
```

### 7.5 Quarterly Review Process

Every 3 months:

1. **Check SQLite Releases**
   - Visit https://sqlite.org/changes.html
   - Review release notes for 3 most recent versions

2. **Evaluate Update Necessity**
   - Security fixes → Immediate review
   - File format changes → High priority
   - New features → Medium priority
   - Bug fixes → Low priority

3. **Perform Update**
   - Run update script
   - Complete update checklist
   - Run integration tests

4. **Document Changes**
   - Update VERSION-NOTES.md
   - Update compatibility matrix
   - Add notes to Anthony CHANGELOG.md

5. **Communicate**
   - Notify team of changes
   - Update project documentation
   - Consider blog post for major updates

## 8. Test Extraction Strategy

### 8.1 TCL Test Conversion

SQLite tests are written in TCL. Converting them to Go requires:

1. **Manual Review** - Understand test intent
2. **SQL Extraction** - Extract SQL statements
3. **Go Translation** - Convert to Go test functions
4. **Assertion Mapping** - Map TCL assertions to Go

### 8.2 Extraction Script

Create `contrib/sqlite/scripts/extract-tests.sh`:

```bash
#!/bin/bash
# Extract SQL statements from SQLite TCL tests

SQLITE_TEST_DIR="contrib/sqlite/official-source/test"
OUTPUT_DIR="contrib/sqlite/extracted-tests"

# Extract all SQL from a test file
extract_sql() {
    local test_file="$1"
    local category="$2"

    grep -E "execsql|catchsql" "$test_file" | \
        sed 's/.*{//' | \
        sed 's/}.*//' > "${OUTPUT_DIR}/${category}/$(basename $test_file .test).sql"
}

# Process select tests
mkdir -p "${OUTPUT_DIR}/select"
for test in ${SQLITE_TEST_DIR}/select*.test; do
    extract_sql "$test" "select"
done

echo "Extraction complete. Review ${OUTPUT_DIR}/"
```

### 8.3 Manual Adaptation Guidelines

For each extracted test:

1. **Review Original** - Read the TCL test to understand intent
2. **Extract SQL** - Pull out SQL statements
3. **Create Go Test** - Write equivalent Go test
4. **Add Context** - Include comments explaining the test
5. **Mark Source** - Reference original SQLite test file

Example:

```go
// TestSelectSubquery tests scalar subqueries in SELECT
// Adapted from SQLite test/select3.test lines 100-150
// Reference: https://sqlite.org/src/file?name=test/select3.test
func TestSelectSubquery(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    // Create test tables
    _, err := db.Exec(`CREATE TABLE t1 (a, b)`)
    require.NoError(t, err)

    // Original SQLite test: select3-1.1
    rows, err := db.Query(`SELECT (SELECT COUNT(*) FROM t1)`)
    require.NoError(t, err)
    // ... assertions ...
}
```

## 9. Practical Workflows

### 9.1 Implementing New Feature

When implementing a new SQL feature (e.g., WINDOW functions):

1. **Study SQLite Source**
   ```bash
   cd contrib/sqlite/official-source/src/
   vim window.c  # Read implementation
   vim select.c  # See integration points
   ```

2. **Review Tests**
   ```bash
   cd ../test/
   vim window1.test  # See test cases
   ```

3. **Extract Test Cases**
   ```bash
   ../scripts/extract-tests.sh window1.test window
   ```

4. **Implement in Anthony**
   - Reference SQLite's approach
   - Write Go implementation
   - Port relevant tests

5. **Verify Compatibility**
   ```bash
   # Compare output
   go test ./internal/sql/window -compare-with-sqlite
   ```

### 9.2 Debugging Opcode Issues

When debugging bytecode execution:

1. **Generate Explain Output**
   ```bash
   # SQLite explain
   contrib/sqlite/reference-builds/build-3.47.0/sqlite3 test.db \
       "EXPLAIN SELECT ..." > sqlite-explain.txt

   # Anthony explain
   anthony-cli test.db "EXPLAIN SELECT ..." > anthony-explain.txt

   # Compare
   diff -u sqlite-explain.txt anthony-explain.txt
   ```

2. **Study Opcode Implementation**
   ```bash
   cd contrib/sqlite/official-source/src/
   # Search for opcode implementation
   grep -n "case OP_Column:" vdbe.c
   ```

3. **Cross-Reference Documentation**
   ```bash
   # Read opcode docs
   vim ../doc/opcode.wiki
   ```

### 9.3 File Format Investigation

When investigating file format issues:

1. **Read Format Documentation**
   ```bash
   vim contrib/sqlite/official-source/doc/fileformat2.wiki
   ```

2. **Compare Database Files**
   ```bash
   # Create test database with SQLite
   sqlite3 sqlite-test.db "CREATE TABLE t(x); INSERT INTO t VALUES(1);"

   # Create test database with Anthony
   anthony-cli anthony-test.db "CREATE TABLE t(x); INSERT INTO t VALUES(1);"

   # Compare binary format
   hexdump -C sqlite-test.db > sqlite-hex.txt
   hexdump -C anthony-test.db > anthony-hex.txt
   diff sqlite-hex.txt anthony-hex.txt
   ```

3. **Trace Pager Operations**
   - Enable SQLite debug mode
   - Compare pager behavior

## 10. Best Practices

### 10.1 Reference Usage Guidelines

**DO:**
- Consult SQLite source for design patterns
- Study SQLite tests for edge cases
- Reference SQLite documentation for specifications
- Compare output for compatibility testing
- Learn from SQLite's algorithms

**DON'T:**
- Copy-paste C code directly
- Port without understanding
- Ignore license attribution
- Assume implementation details match
- Skip writing your own tests

### 10.2 Maintaining Independence

Anthony is a **from-scratch implementation**, not a port:

- Understand the algorithm, then implement in idiomatic Go
- Write Go-style code, not C-style code
- Use Go's standard library where appropriate
- Add proper error handling (SQLite uses return codes)
- Leverage Go's type system and memory safety

### 10.3 Documentation Discipline

When referencing SQLite:

```go
// GOOD: Clear attribution with context
// This implements the bytecode executor based on SQLite's VDBE design.
// See contrib/sqlite/official-source/src/vdbe.c for reference implementation.
// Key differences: Go uses stack-based error handling instead of return codes.

// BAD: Vague or missing attribution
// This is the bytecode thing
```

### 10.4 Test Coverage Strategy

Prioritize test adaptation:

1. **High Priority** - Core SQL (SELECT, INSERT, UPDATE, DELETE)
2. **Medium Priority** - Advanced features (CTEs, triggers, constraints)
3. **Low Priority** - Edge cases and obscure features
4. **As-Needed** - Performance and stress tests

## 11. Maintenance and Ownership

### 11.1 Responsibilities

| Task | Owner | Frequency |
|------|-------|-----------|
| SQLite version updates | Maintainer | Quarterly |
| Test extraction | Contributors | As needed |
| Reference builds | CI/CD | On update |
| Documentation sync | Maintainer | On update |
| Compatibility testing | CI/CD | Every commit |

### 11.2 CI/CD Integration

Add to CI pipeline:

```yaml
# .github/workflows/sqlite-compatibility.yml
name: SQLite Compatibility

on: [push, pull_request]

jobs:
  compatibility:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
        with:
          submodules: true

      - name: Build SQLite reference
        run: contrib/sqlite/scripts/build-reference.sh

      - name: Run compatibility tests
        run: go test ./test/compatibility -v

      - name: Compare file formats
        run: go test ./internal/format -compare-with-sqlite
```

### 11.3 Review Process

When adding new SQLite reference material:

1. **Pull Request Required** - No direct commits to contrib/sqlite/
2. **Document Purpose** - Explain why the reference is needed
3. **Size Limits** - Don't commit large binaries or build artifacts
4. **License Check** - Verify public domain status
5. **Attribution** - Include proper attribution

## 12. Troubleshooting

### 12.1 Common Issues

**Issue**: Fossil repository clone fails
**Solution**: Use GitHub mirror or download tarball

**Issue**: Build reference fails on macOS
**Solution**: Install Xcode command line tools: `xcode-select --install`

**Issue**: Test extraction script gets malformed SQL
**Solution**: Manual review required - TCL tests have complex syntax

**Issue**: Diff shows many differences despite correct behavior
**Solution**: Output formatting differences are normal - compare semantics

### 12.2 Getting Help

1. **SQLite Documentation**: https://sqlite.org/docs.html
2. **SQLite Forum**: https://sqlite.org/forum/forum
3. **Source Code**: https://sqlite.org/src/doc/trunk/README.md
4. **Mailing List**: sqlite-users@mailinglists.sqlite.org

## 13. Future Enhancements

### 13.1 Planned Improvements

- **Automated Test Conversion** - Tool to convert TCL tests to Go
- **Bytecode Comparator** - Automated EXPLAIN output comparison
- **File Format Validator** - Automated binary format verification
- **Performance Baseline** - Benchmark against reference SQLite
- **Fuzzing Integration** - Adapt SQLite's fuzzing infrastructure

### 13.2 Integration Roadmap

| Quarter | Goal | Status |
|---------|------|--------|
| Q1 2025 | Initial source integration | In Progress |
| Q2 2025 | Test extraction automation | Planned |
| Q3 2025 | CI/CD compatibility tests | Planned |
| Q4 2025 | Automated bytecode comparison | Planned |

## Conclusion

This integration plan provides a structured approach to incorporating SQLite source code as a reference for the Anthony project. By maintaining a clean separation between reference material and implementation, we can leverage SQLite's excellent codebase while preserving Anthony's independence as a pure Go implementation.

The key to successful integration is:

1. **Respect the source** - SQLite is battle-tested and well-designed
2. **Maintain independence** - Anthony is not a port, it's a reimplementation
3. **Test thoroughly** - Use SQLite tests to ensure compatibility
4. **Document clearly** - Track versions and attribute sources
5. **Update regularly** - Stay current with SQLite development

For questions or suggestions about this integration plan, please open an issue or submit a pull request.

---

**Last Updated**: 2025-02-28
**SQLite Version**: 3.47.0
**Next Review**: 2025-05-28
