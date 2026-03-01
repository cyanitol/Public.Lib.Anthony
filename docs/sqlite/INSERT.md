- 
INSERT

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

INSERT

# 1. Overview

**[insert-stmt:](syntax/insert-stmt.html)**
hide

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
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

 
 

**[type-name:](syntax/type-name.html)**
show

 
 

**[signed-number:](syntax/signed-number.html)**
show

 
 

**[returning-clause:](syntax/returning-clause.html)**
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

 
 

**[upsert-clause:](syntax/upsert-clause.html)**
show

 
 

**[column-name-list:](syntax/column-name-list.html)**
show

 
 

**[indexed-column:](syntax/indexed-column.html)**
show

 
 

The INSERT statement comes in three basic forms.  

- 
**INSERT INTO ***table*** VALUES(...);**

The first form (with the "VALUES" keyword) creates one or more
new rows in
an existing table. If the column-name list after
table-name is omitted then the number
of values inserted into each row
must be the same as the number of columns in the table. In this case
the result of evaluating the left-most expression from each term of
the VALUES list is inserted into the left-most column of each new row,
and so forth for each subsequent expression. If a column-name
list is specified, then the number of values in each term of the
VALUE list must match the number of
specified columns. Each of the named columns of the new row is populated
with the results of evaluating the corresponding VALUES expression. Table
columns that do not appear in the column list are populated with the 
[default column value](lang_createtable.html#dfltval) (specified as part of the [CREATE TABLE](lang_createtable.html) statement), or
with NULL if no [default value](lang_createtable.html#dfltval) is specified.

- 
**INSERT INTO ***table*** SELECT ...;**

The second form of the INSERT statement contains a [SELECT](lang_select.html) statement
instead of a VALUES clause. A new entry is inserted into the table for each
row of data returned by executing the SELECT statement. If a column-list is
specified, the number of columns in the result of the SELECT must be the same
as the number of items in the column-list. Otherwise, if no column-list is
specified, the number of columns in the result of the SELECT must be the same
as the number of columns in the table. Any SELECT statement, including
[compound SELECTs](lang_select.html#compound) and SELECT statements with [ORDER BY](lang_select.html#orderby) and/or [LIMIT](lang_select.html#limitoffset) clauses, 
may be used in an INSERT statement of this form.

To avoid a parsing ambiguity, the SELECT statement should always
contain a WHERE clause, even if that clause is simply "WHERE true",
if the [upsert-clause](syntax/upsert-clause.html) is present.  Without the WHERE clause, the
parser does not know if the token "ON" is part of a join constraint
on the SELECT, or the beginning of the [upsert-clause](syntax/upsert-clause.html).

- 
**INSERT INTO ***table*** DEFAULT VALUES;**

The third form of an INSERT statement is with DEFAULT VALUES.
The INSERT ... DEFAULT VALUES statement inserts a single new row into the
named table. Each column of the new row is populated with its 
[default value](lang_createtable.html#dfltval), or with a NULL if no default value is specified 
as part of the column definition in the [CREATE TABLE](lang_createtable.html) statement.
The [upsert-clause](syntax/upsert-clause.html) is not supported after DEFAULT VALUES.

The initial "INSERT" keyword can be replaced by
"REPLACE" or "INSERT OR *action*" to specify an alternative
constraint [conflict resolution algorithm](lang_conflict.html) to use during 
that one INSERT command.
For compatibility with MySQL, the parser allows the use of the
single keyword [REPLACE](lang_replace.html) as an 
alias for "INSERT OR REPLACE".

The optional "*schema-name***.**" prefix on the 
table-name
is supported for top-level INSERT statements only.  The table name must be
unqualified for INSERT statements that occur within [CREATE TRIGGER](lang_createtrigger.html) statements.
Similarly, the "DEFAULT VALUES" form of the INSERT statement is supported for
top-level INSERT statements only and not for INSERT statements within
triggers.

The optional "AS alias" phrase provides an alternative
name for the table into which content is being inserted.  The alias name
can be used within WHERE and SET clauses of the [UPSERT](lang_upsert.html).  If there is no
[upsert-clause](syntax/upsert-clause.html), then the alias is pointless, but also
harmless.

See the separate [UPSERT](lang_upsert.html) documentation for the additional trailing
syntax that can cause an INSERT to behave as an UPDATE if the INSERT would
otherwise violate a uniqueness constraint.  The [upsert clause](lang_upsert.html) is not
allowed on an "INSERT ... DEFAULT VALUES".