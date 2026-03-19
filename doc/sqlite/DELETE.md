- 
DELETE

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

DELETE

Table Of Contents
[1. Overview](#1-overview)
[2. Restrictions on DELETE Statements Within CREATE TRIGGER](#2-restrictions-on-delete-statements-within-create-trigger)
[3. Optional LIMIT and ORDER BY clauses](#3-optional-limit-and-order-by-clauses)
[4. The Truncate Optimization](#4-the-truncate-optimization)

# 1. Overview

**[delete-stmt:](syntax/delete-stmt.html)**
hide

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[select-stmt:](syntax/select-stmt.html)**
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

 
 

**[qualified-table-name:](syntax/qualified-table-name.html)**
show

 
 

**[returning-clause:](syntax/returning-clause.html)**
show

 
 

The DELETE command removes records from the table identified by the
   [qualified-table-name](syntax/qualified-table-name.html).

If the WHERE clause is not present, all records in the table are deleted.
   If a WHERE clause is supplied, then only those rows for which the
   WHERE clause [boolean expression](lang_expr.html#booleanexpr) is true are deleted.
   Rows for which the expression is false or NULL are retained.

# 2. Restrictions on DELETE Statements Within CREATE TRIGGER

The following restrictions apply to DELETE statements that occur within the
   body of a [CREATE TRIGGER](lang_createtrigger.html) statement:

  - 
The table-name specified as part of a 
    DELETE statement within
    a trigger body must be unqualified.  In other words, the
    *schema-name***.** prefix on the table name is not allowed 
    within triggers. If the table to which the trigger is attached is
    not in the temp database, then DELETE statements within the trigger
    body must operate on tables within the same database as it. If the table
    to which the trigger is attached is in the TEMP database, then the
    unqualified name of the table being deleted is resolved in the same way as
    it is for a top-level statement (by searching first the TEMP database, then
    the main database, then any other databases in the order they were
    attached).
    
  

- 
The INDEXED BY and NOT INDEXED clauses are not allowed on DELETE
    statements within triggers.

  
- 
The LIMIT and ORDER BY clauses (described below) are unsupported for
    DELETE statements within triggers.

  
- 
The RETURNING clause is not supported for triggers.

# 3. Optional LIMIT and ORDER BY clauses

If SQLite is compiled with the [SQLITE_ENABLE_UPDATE_DELETE_LIMIT](compile.html#enable_update_delete_limit)
compile-time option, then the syntax of the DELETE statement is
extended by the addition of optional ORDER BY and LIMIT clauses:

**[delete-stmt-limited:](syntax/delete-stmt-limited.html)**

 

 

If a DELETE statement has a LIMIT clause, the maximum number of rows that
will be deleted is found by evaluating the accompanying expression and casting
it to an integer value. If the result of evaluating the LIMIT clause
cannot be losslessly converted to an integer value, it is an error. A 
negative LIMIT value is interpreted as "no limit". If the DELETE statement 
also has an OFFSET clause, then it is similarly evaluated and cast to an
integer value. Again, it is an error if the value cannot be losslessly
converted to an integer. If there is no OFFSET clause, or the calculated
integer value is negative, the effective OFFSET value is zero.

If the DELETE statement has an ORDER BY clause, then all rows that would 
be deleted in the absence of the LIMIT clause are sorted according to the 
ORDER BY. The first *M* rows, where *M* is the value found by
evaluating the OFFSET clause expression, are skipped, and the following 
*N*, where *N* is the value of the LIMIT expression, are deleted.
If there are less than *N* rows remaining after taking the OFFSET clause
into account, or if the LIMIT clause evaluated to a negative value, then all
remaining rows are deleted.

If the DELETE statement has no ORDER BY clause, then all rows that
would be deleted in the absence of the LIMIT clause are assembled in an
arbitrary order before applying the LIMIT and OFFSET clauses to determine 
the subset that are actually deleted.

The ORDER BY clause on a DELETE statement is used only to determine which
rows fall within the LIMIT. The order in which rows are deleted is arbitrary
and is not influenced by the ORDER BY clause.
This means that if there is a [RETURNING clause](lang_returning.html), the rows returned by
the statement probably will not be in the order specified by the
ORDER BY clause.

# 4. The Truncate Optimization

When the WHERE clause and RETURNING clause are both  omitted
from a DELETE statement and the table being deleted has no triggers,
SQLite uses an optimization to erase the entire table content
without having to visit each row of the table individually.
This "truncate" optimization makes the delete run much faster.
Prior to SQLite [version 3.6.5](releaselog/3_6_5.html) (2008-11-12), the truncate optimization
also meant that the [sqlite3_changes()](c3ref/changes.html) and
[sqlite3_total_changes()](c3ref/total_changes.html) interfaces
and the [count_changes pragma](pragma.html#pragma_count_changes)
will not actually return the number of deleted rows.  
That problem has been fixed as of [version 3.6.5](releaselog/3_6_5.html) (2008-11-12).

The truncate optimization can be permanently disabled for all queries
by recompiling
SQLite with the [SQLITE_OMIT_TRUNCATE_OPTIMIZATION](compile.html#omit_truncate_optimization) compile-time switch.

The truncate optimization can also be disabled at runtime using
the [sqlite3_set_authorizer()](c3ref/set_authorizer.html) interface.  If an authorizer callback
returns [SQLITE_IGNORE](c3ref/c_deny.html) for an [SQLITE_DELETE](c3ref/c_alter_table.html) action code, then
the DELETE operation will proceed but the truncate optimization will
be bypassed and rows will be deleted one by one.

*This page was last updated on 2025-05-31 13:08:22Z *