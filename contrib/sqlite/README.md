# SQLite Source Code Archive

This directory contains the official SQLite source code, amalgamation, and documentation downloaded from [sqlite.org](https://sqlite.org/).

## Version Information

- **Version**: 3.51.2 (Build 3510200)
- **Downloaded**: February 28, 2026
- **Source URL**: https://sqlite.org/download.html

## Directory Structure

```
contrib/sqlite/
├── sqlite-src-3510200/           # Complete SQLite source code
│   ├── src/                      # Core SQLite C source files (125 files)
│   ├── test/                     # TCL test suite (1174+ test files)
│   ├── ext/                      # Extensions (FTS3, FTS5, RTree, etc.)
│   ├── tool/                     # Build and development tools
│   ├── doc/                      # Source documentation
│   ├── autoconf/                 # Autoconf build configuration
│   ├── art/                      # Artwork and icons
│   ├── compat/                   # Compatibility layers
│   ├── contrib/                  # Contributed code
│   ├── mptest/                   # Multi-process test harness
│   ├── configure                 # Configure script
│   ├── Makefile.in              # Makefile template
│   ├── Makefile.msc             # Microsoft Visual C++ makefile
│   ├── main.mk                  # Main makefile rules
│   ├── README.md                # Upstream README
│   ├── LICENSE.md               # SQLite license (public domain)
│   └── VERSION                  # Version number file
│
├── sqlite-amalgamation-3510200/  # Amalgamation (single-file distribution)
│   ├── sqlite3.c                 # Complete SQLite library (9.4 MB, all code in one file)
│   ├── sqlite3.h                 # Public API header (671 KB)
│   ├── sqlite3ext.h              # Extension API header (38 KB)
│   └── shell.c                   # Command-line shell (1.0 MB)
│
├── sqlite-doc-3510200/           # Complete HTML documentation
│   ├── index.html                # Documentation home page
│   ├── c3ref/                    # C API reference
│   ├── releaselog/               # Release history
│   ├── session/                  # Session extension docs
│   ├── syntax/                   # SQL syntax diagrams
│   └── [350+ HTML documentation pages]
│
└── README.md                     # This file
```

## Contents Overview

### Complete Source Code (sqlite-src-3510200/)

The complete source distribution includes:

- **Core Engine**: 125 C source files implementing the SQLite database engine
  - B-tree implementation (btree.c, btreeInt.h)
  - Virtual Database Engine (vdbe*.c)
  - SQL parser and compiler (parse.y, build.c, where.c)
  - Pager and cache (pager.c, pcache*.c)
  - Platform abstraction (os*.c for Unix, Windows, etc.)

- **Test Suite**: 1174+ TCL test files providing comprehensive test coverage
  - Tests are written in TCL and require tclsh to run
  - Located in test/ directory
  - Covers all major features and edge cases

- **Extensions**:
  - FTS3/FTS5: Full-text search
  - RTree: Spatial indexing
  - JSON1: JSON functions
  - Session: Change tracking
  - Expert: Query optimization advice
  - Recovery: Database recovery tools
  - JNI: Java Native Interface bindings
  - WASM: WebAssembly port

- **Build System**:
  - Autoconf-based configuration
  - Makefiles for Unix/Linux (Makefile.in)
  - MSVC makefile (Makefile.msc)
  - Custom build tool (autosetup)

### Amalgamation (sqlite-amalgamation-3510200/)

The amalgamation is a convenient single-file distribution:

- **sqlite3.c**: All SQLite source code combined into one file (9.4 MB)
  - Easier to integrate into projects
  - Faster compilation (better optimization potential)
  - Self-contained, no dependencies

- **sqlite3.h**: Public API header defining all SQLite functions and types

- **shell.c**: Full-featured command-line interface
  - Can be compiled with: `gcc shell.c sqlite3.c -o sqlite3 -lpthread -ldl`

### Documentation (sqlite-doc-3510200/)

Complete offline HTML documentation including:

- SQL language reference (lang_*.html)
- C API documentation (c3ref/)
- Architecture and internals (arch.html, opcode.html)
- Features and limits (features.html, limits.html)
- File format specification (fileformat2.html)
- Query planner documentation (queryplanner.html)
- Extension development guides
- Release notes and change history

## Building SQLite

### Quick Build (Amalgamation)

The simplest way to build SQLite:

```bash
cd sqlite-amalgamation-3510200
gcc -O2 -o sqlite3 shell.c sqlite3.c -lpthread -ldl -lm
./sqlite3
```

### Full Build (Source Tree)

For development or customization:

```bash
cd sqlite-src-3510200
./configure
make
make test  # Requires TCL
sudo make install
```

### Using Nix (Recommended for NixOS)

```bash
nix-shell -p gcc gnumake tcl --run "cd sqlite-src-3510200 && ./configure && make"
```

## License

SQLite is in the **public domain**. You can do anything you want with this code.

See LICENSE.md in the source directory for the full dedication to the public domain.

## Version Control

The official SQLite source code is managed using [Fossil](https://fossil-scm.org/):

- **Official Repository**: https://sqlite.org/src
- **Repository URL**: https://sqlite.org/src
- **Clone Command**: `fossil clone https://sqlite.org/src sqlite.fossil`

## Key Features of SQLite 3.51.2

- Self-contained, serverless SQL database engine
- Zero-configuration (no setup or administration)
- ACID transactions
- Full-text search (FTS5)
- JSON support (JSON1 extension)
- Common Table Expressions (WITH clause)
- Window functions
- Recursive queries
- Partial indexes
- Generated columns
- STRICT tables
- And much more...

## Documentation

- **Online**: https://sqlite.org/docs.html
- **Offline**: Open `sqlite-doc-3510200/index.html` in a web browser
- **C API Reference**: `sqlite-doc-3510200/c3ref/intro.html`
- **SQL Reference**: `sqlite-doc-3510200/lang.html`

## Testing

The test suite requires TCL:

```bash
cd sqlite-src-3510200
make test           # Run standard tests
make fulltest       # Run comprehensive tests (takes hours)
make fuzztest       # Run fuzz tests
```

## Source Files Organization

### Core Source Files (src/)

- **Parser/Compiler**: parse.y, build.c, expr.c, select.c, where.c
- **Virtual Machine**: vdbe.c, vdbeapi.c, vdbeaux.c, vdbemem.c, vdbesort.c
- **B-tree**: btree.c, btmutex.c
- **Pager**: pager.c, wal.c (Write-Ahead Log)
- **Cache**: pcache.c, pcache1.c
- **OS Interface**: os.c, os_unix.c, os_win.c
- **Main API**: main.c, legacy.c, prepare.c
- **SQL Functions**: func.c, date.c, json.c
- **Utilities**: util.c, printf.c, random.c, utf.c
- **Memory**: malloc.c, mem*.c
- **Schema**: analyze.c, attach.c, pragma.c, table.c
- **Features**: backup.c, fkey.c (foreign keys), trigger.c, vtab.c (virtual tables)

### Extensions (ext/)

Each extension is in its own subdirectory with README and source files.

## Useful Files

- `VERSION`: Version number (3.51.2)
- `manifest.uuid`: Fossil check-in hash
- `LICENSE.md`: Public domain dedication
- `sqlite3.1`: Man page for sqlite3 command
- `sqlite3.pc.in`: pkg-config template

## Notes

1. SQLite uses the Fossil version control system, not Git
2. The source tree includes extensive test coverage (1174+ test files)
3. Tests are written in TCL and require tclsh to run
4. The amalgamation is recommended for most users (simpler to build)
5. The complete source is recommended for development and debugging
6. All documentation is self-contained in the sqlite-doc-3510200/ directory

## References

- Official Website: https://sqlite.org/
- Documentation: https://sqlite.org/docs.html
- Download Page: https://sqlite.org/download.html
- Source Repository: https://sqlite.org/src
- Forum: https://sqlite.org/forum
- Bug Tracker: https://sqlite.org/src/reportlist

---

Downloaded and organized for reference and study purposes.
