- 
ATTACH DATABASE

[

](index.html)

Small. Fast. Reliable.
Choose any three.

- [Home](index.html)
- [Menu](javascript:void(0))
- About
- [Documentation](docs.html)
- [Download](download.html)
- License
- [Support](support.html)
- [Purchase](prosupport.html)
- 
[Search](javascript:void(0))

- About
- Documentation
- Download
- Support
- Purchase

Search Documentation
Search Changelog

ATTACH DATABASE

# 1. Overview

**[attach-stmt:](syntax/attach-stmt.html)**
hide

 
 

**[expr:](syntax/expr.html)**
show

 
 

**[filter-clause:](syntax/filter-clause.html)**
show

 
 

**[function-arguments:](syntax/function-arguments.html)**
show

 
 

**[ordering-term:](syntax/ordering-term.html)**
show

 
 

**[literal-value:](syntax/literal-value.html)**
show

 
 

**[over-clause:](syntax/over-clause.html)**
show

 
 

**[frame-spec:](syntax/frame-spec.html)**
show

 
 

**[ordering-term:](syntax/ordering-term.html)**
show

 
 

**[raise-function:](syntax/raise-function.html)**
show

 
 

**[select-stmt:](syntax/select-stmt.html)**
show

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[compound-operator:](syntax/compound-operator.html)**
show

 
 

**[join-clause:](syntax/join-clause.html)**
show

 
 

**[join-constraint:](syntax/join-constraint.html)**
show

 
 

**[join-operator:](syntax/join-operator.html)**
show

 
 

**[ordering-term:](syntax/ordering-term.html)**
show

 
 

**[result-column:](syntax/result-column.html)**
show

 
 

**[table-or-subquery:](syntax/table-or-subquery.html)**
show

 
 

**[window-defn:](syntax/window-defn.html)**
show

 
 

**[frame-spec:](syntax/frame-spec.html)**
show

 
 

**[type-name:](syntax/type-name.html)**
show

 
 

**[signed-number:](syntax/signed-number.html)**
show

 
 

 The ATTACH DATABASE statement adds another database 
file to the current [database connection](c3ref/sqlite3.html). 
Database files that were previously attached can be removed using
the [DETACH DATABASE](lang_detach.html) command.

# 2. Details

The filename for the database to be attached is the value of
the expression that occurs before the AS keyword.
The filename of the database follows the same semantics as the
filename argument to [sqlite3_open()](c3ref/open.html) and [sqlite3_open_v2()](c3ref/open.html); the
special name "[:memory:](inmemorydb.html)" results in an [in-memory database](inmemorydb.html) and an
empty string results in a new temporary database.
The filename argument can be a [URI filename](uri.html) if URI filename processing
is enabled on the database connection.  The default behavior is for
URI filenames to be disabled, however that might change in a future release
of SQLite, so application developers are advised to plan accordingly.

The name that occurs after the AS keyword is the name of the database
used internally by SQLite.
The schema-names 'main' and 
'temp' refer to the main database and the database used for 
temporary tables.  The main and temp databases cannot be attached or
detached.

Attached databases use the same VFS as the main database unless
another VFS is specified using the `vfs=NAME` URI flag.

 Tables in an attached database can be referred to using the syntax 
*schema-name.table-name*.  If the name of the table is unique
across all attached databases and the main and temp databases, then the
*schema-name* prefix is not required.  If two or more tables in
different databases have the same name and the 
*schema-name* prefix is not used on a table reference, then the
table chosen is the one in the database that was least recently attached.

Transactions involving multiple attached databases are atomic,
assuming that the main database is not "[:memory:](inmemorydb.html)" and the 
[journal_mode](pragma.html#pragma_journal_mode) is not [WAL](wal.html).  If the main
database is ":memory:" or if the journal_mode is WAL, then 
transactions continue to be atomic within each individual
database file. But if the host computer crashes in the middle
of a [COMMIT](lang_transaction.html) where two or more database files are updated,
some of those files might get the changes where others
might not.

 There is a limit, set using [sqlite3_limit()](c3ref/limit.html) and 
[SQLITE_LIMIT_ATTACHED](c3ref/c_limit_attached.html#sqlitelimitattached), to the number of databases that can be
simultaneously attached to a single database connection.

*This page was last updated on 2025-02-06 23:19:09Z *