- 
CREATE VIEW

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

CREATE VIEW

# 1. Syntax

**[create-view-stmt:](syntax/create-view-stmt.html)**
hide

 
 

**[select-stmt:](syntax/select-stmt.html)**
show

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[compound-operator:](syntax/compound-operator.html)**
show

 
 

**[expr:](syntax/expr.html)**
show

 
 

**[filter-clause:](syntax/filter-clause.html)**
show

 
 

**[function-arguments:](syntax/function-arguments.html)**
show

 
 

**[literal-value:](syntax/literal-value.html)**
show

 
 

**[over-clause:](syntax/over-clause.html)**
show

 
 

**[frame-spec:](syntax/frame-spec.html)**
show

 
 

**[raise-function:](syntax/raise-function.html)**
show

 
 

**[type-name:](syntax/type-name.html)**
show

 
 

**[signed-number:](syntax/signed-number.html)**
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

 
 

# 2. Description

The CREATE VIEW command assigns a name to a pre-packaged 
[SELECT](lang_select.html) statement. 
Once the view is created, it can be used in the FROM clause
of another [SELECT](lang_select.html) in place of a table name.

If the "TEMP" or "TEMPORARY" keyword occurs in between "CREATE"
and "VIEW" then the view that is created is only visible to the
[database connection](c3ref/sqlite3.html) that created it and is automatically deleted when
the database connection is closed.

 If a schema-name is specified, then the view 
is created in the specified database.
It is an error to specify both a schema-name
and the TEMP keyword on a VIEW, unless the schema-name 
is "temp".
If no schema name is specified, and the TEMP keyword is not present,
the VIEW is created in the main database.

You cannot [DELETE](lang_delete.html), [INSERT](lang_insert.html), or [UPDATE](lang_update.html) a view.  Views are read-only 
in SQLite.  However, in many cases you can use an
[INSTEAD OF trigger](lang_createtrigger.html#instead_of_trigger) on the view to accomplish 
the same thing.  Views are removed 
with the [DROP VIEW](lang_dropview.html) command.

If a column-name list follows 
the view-name, then that list determines
the names of the columns for the view.  If the column-name
list is omitted, then the names of the columns in the view are derived
from the names of the result-set columns in the [select-stmt](syntax/select-stmt.html).
The use of column-name list is recommended.  Or, if
column-name list is omitted, then the result
columns in the [SELECT](lang_select.html) statement that defines the view should have
well-defined names using the 
"[AS column-alias](syntax/result-column.html)" syntax.
SQLite allows you to create views that depend on automatically 
generated column names, but you should avoid using them since the 
rules used to generate column names are not a defined part of the
interface and might change in future releases of SQLite.

The column-name list syntax was added in
SQLite versions 3.9.0 (2015-10-14).