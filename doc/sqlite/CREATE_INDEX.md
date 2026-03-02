- 
CREATE INDEX

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

CREATE INDEX

# 1. Syntax

**[create-index-stmt:](syntax/create-index-stmt.html)**
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

 
 

**[indexed-column:](syntax/indexed-column.html)**
show

 
 

The CREATE INDEX command consists of the keywords "CREATE INDEX" followed
by the name of the new index, the keyword "ON", the name of a previously
created table that is to be indexed, and a parenthesized list of table column
names and/or expressions that are used for the index key.
If the optional WHERE clause is included, then the index is a "[partial index](partialindex.html)".

If the optional IF NOT EXISTS clause is present and another index
with the same name already exists, then this command becomes a no-op.

There are no arbitrary limits on the number of indices that can be
attached to a single table.  The number of columns in an index is 
limited to the value set by
[sqlite3_limit](c3ref/limit.html)([SQLITE_LIMIT_COLUMN](c3ref/c_limit_attached.html#sqlitelimitcolumn),...).

Indexes are removed with the [DROP INDEX](lang_dropindex.html) command.

## 1.1. Unique Indexes

If the UNIQUE keyword appears between CREATE and INDEX then duplicate
index entries are not allowed.  Any attempt to insert a duplicate entry
will result in an error.

For the purposes of unique indices, all NULL values
are considered different from all other NULL values and are thus unique.
This is one of the two possible interpretations of the SQL-92 standard
(the language in the standard is ambiguous).  The interpretation used
by SQLite is the same and is the interpretation
followed by PostgreSQL, MySQL, Firebird, and Oracle.  Informix and
Microsoft SQL Server follow the other interpretation of the standard, which
is that all NULL values are equal to one another.

## 1.2. Indexes on Expressions

Expressions in an index may not reference other tables
and may not use subqueries nor functions whose result might
change (ex: [random()](lang_corefunc.html#random) or [sqlite_version()](lang_corefunc.html#sqlite_version)).
Expressions in an index may only refer to columns in the table
that is being indexed.
Indexes on expression will not work with versions of SQLite prior
to [version 3.9.0](releaselog/3_9_0.html) (2015-10-14).
See the [Indexes On Expressions](expridx.html) document for additional information
about using general expressions in CREATE INDEX statements.

## 1.3. Descending Indexes

Each column name or expression can be followed by one
of the "ASC" or "DESC" keywords to indicate sort order.
The sort order may or may not be ignored depending
on the database file format, and in particular the [schema format number](fileformat2.html#schemaformat).
The "legacy" schema format (1) ignores index
sort order.  The descending index schema format (4) takes index sort order
into account.  Only versions of SQLite 3.3.0 (2006-01-11)
and later are able to understand
the descending index format. For compatibility, version of SQLite between 3.3.0
and 3.7.9 use the legacy schema format by default.  The newer schema format is
used by default in version 3.7.10 (2012-01-16) and later.
The [legacy_file_format pragma](pragma.html#pragma_legacy_file_format) can be used to change set the specific
behavior for any version of SQLite.

## 1.4. NULLS FIRST and NULLS LAST

The NULLS FIRST and NULLS LAST predicates are not supported
for indexes.  For [sorting purposes](datatype3.html#sortorder), SQLite considers NULL values 
to be smaller than all other values.  Hence NULL values always appear at
the beginning of an ASC index and at the end of a DESC index.

## 1.5. Collations

The COLLATE clause optionally following each column name
or expression defines a
collating sequence used for text entries in that column.
The default collating
sequence is the collating sequence defined for that column in the
[CREATE TABLE](lang_createtable.html) statement.  Or if no collating sequence is otherwise defined,
the built-in BINARY collating sequence is used.

*This page was last updated on 2022-04-18 02:55:50Z *