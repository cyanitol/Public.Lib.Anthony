- 
CREATE TRIGGER

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

CREATE TRIGGER

Table Of Contents
[1. Syntax](#syntax)
[2. Description](#description)
[2.1. Syntax Restrictions On UPDATE, DELETE, and INSERT Statements Within
Triggers](#syntax_restrictions_on_update_delete_and_insert_statements_within_triggers)
[3. INSTEAD OF triggers](#instead_of_triggers)
[4. Some Example Triggers](#some_example_triggers)
[5. Cautions On The Use Of BEFORE triggers](#cautions_on_the_use_of_before_triggers)
[6. The RAISE() function](#the_raise_function)
[7. TEMP Triggers on Non-TEMP Tables](#temp_triggers_on_non_temp_tables)

# 1. Syntax

**[create-trigger-stmt:](syntax/create-trigger-stmt.html)**
hide

 
 

**[delete-stmt:](syntax/delete-stmt.html)**
show

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[qualified-table-name:](syntax/qualified-table-name.html)**
show

 
 

**[returning-clause:](syntax/returning-clause.html)**
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

 
 

**[insert-stmt:](syntax/insert-stmt.html)**
show

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[returning-clause:](syntax/returning-clause.html)**
show

 
 

**[upsert-clause:](syntax/upsert-clause.html)**
show

 
 

**[column-name-list:](syntax/column-name-list.html)**
show

 
 

**[indexed-column:](syntax/indexed-column.html)**
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

 
 

**[update-stmt:](syntax/update-stmt.html)**
show

 
 

**[column-name-list:](syntax/column-name-list.html)**
show

 
 

**[common-table-expression:](syntax/common-table-expression.html)**
show

 
 

**[join-clause:](syntax/join-clause.html)**
show

 
 

**[join-constraint:](syntax/join-constraint.html)**
show

 
 

**[join-operator:](syntax/join-operator.html)**
show

 
 

**[qualified-table-name:](syntax/qualified-table-name.html)**
show

 
 

**[returning-clause:](syntax/returning-clause.html)**
show

 
 

**[table-or-subquery:](syntax/table-or-subquery.html)**
show

 
 

# 2. Description

The CREATE TRIGGER statement is used to add triggers to the 
database schema. Triggers are database operations 
that are automatically performed when a specified database event
occurs.  

Each trigger must specify that it will fire for one of
the following operations: [DELETE](lang_delete.html), [INSERT](lang_insert.html), [UPDATE](lang_update.html).
The trigger fires once for each row that is deleted, inserted,
or updated.  If the "UPDATE OF column-name"
syntax is used, then the trigger will only fire if
column-name appears on the left-hand side of
one of the terms in the SET clause of the [UPDATE](lang_update.html) statement.

Due to an historical oversight, columns named in the "UPDATE OF"
clause do not actually have to exist in the table being updated.
Unrecognized column names are silently ignored.
It would be more helpful if SQLite would fail the CREATE TRIGGER
statement if any of the names in the "UPDATE OF" clause are not
columns in the table.  However, as this problem was discovered
many years after SQLite was widely deployed, we have resisted
fixing the problem for fear of breaking legacy applications.

At this time SQLite supports only FOR EACH ROW triggers, not FOR EACH
STATEMENT triggers. Hence explicitly specifying FOR EACH ROW is optional.
FOR EACH ROW implies that the SQL statements specified in the trigger
may be executed (depending on the WHEN clause) for each database row being
inserted, updated or deleted by the statement causing the trigger to fire.

Both the WHEN clause and the trigger actions may access elements of 
the row being inserted, deleted or updated using references of the form 
"NEW.*column-name*" and "OLD.*column-name*", where
*column-name* is the name of a column from the table that the trigger
is associated with. OLD and NEW references may only be used in triggers on
events for which they are relevant, as follows:

| *INSERT* 
| NEW references are valid 

| *UPDATE* 
| NEW and OLD references are valid 

| *DELETE* 
| OLD references are valid 

If a WHEN clause is supplied, the SQL statements specified
are only executed if the WHEN clause is true.
If no WHEN clause is supplied, the SQL statements
are executed every time the trigger fires.

The BEFORE or AFTER keyword determines when the trigger actions
will be executed relative to the insertion, modification or removal of the
associated row.  BEFORE is the default when neither keyword is present.

An [ON CONFLICT](lang_conflict.html) clause may be specified as part of an [UPDATE](lang_update.html) or [INSERT](lang_insert.html)
action within the body of the trigger.
However if an [ON CONFLICT](lang_conflict.html) clause is specified as part of 
the statement causing the trigger to fire, then conflict handling
policy of the outer statement is used instead.

Triggers are automatically [dropped](lang_droptrigger.html)
when the table that they are 
associated with (the *table-name* table) is 
[dropped](lang_droptable.html).  However if the trigger actions reference
other tables, the trigger is not dropped or modified if those other
tables are [dropped](lang_droptable.html) or [modified](lang_altertable.html).

Triggers are removed using the [DROP TRIGGER](lang_droptrigger.html) statement.

## 2.1. Syntax Restrictions On UPDATE, DELETE, and INSERT Statements Within
    Triggers

The [UPDATE](lang_update.html), [DELETE](lang_delete.html), and [INSERT](lang_insert.html)
statements within triggers do not support
the full syntax for [UPDATE](lang_update.html), [DELETE](lang_delete.html), and [INSERT](lang_insert.html) statements.  The following
restrictions apply:

- 

  The name of the table to be modified in an [UPDATE](lang_update.html), [DELETE](lang_delete.html), or [INSERT](lang_insert.html)
  statement must be an unqualified table name.  In other words, one must
  use just "*tablename*" not "*database***.***tablename*"
  when specifying the table. 

- 

 For non-TEMP triggers,
  the table to be modified or queried must exist in the
  same database as the table or view to which the trigger is attached.
  TEMP triggers are not subject to the same-database rule.  A TEMP
  trigger is allowed to query or modify any table in any [ATTACH](lang_attach.html)-ed database.
  

- 

  The "INSERT INTO *table* DEFAULT VALUES" form of the [INSERT](lang_insert.html) statement
  is not supported.
  

- 

  The INDEXED BY and NOT INDEXED clauses are not supported for [UPDATE](lang_update.html) and
  [DELETE](lang_delete.html) statements.
  

- 

  The ORDER BY and LIMIT clauses on [UPDATE](lang_update.html) and [DELETE](lang_delete.html) statements are not
  supported.  ORDER BY and LIMIT are not normally supported for [UPDATE](lang_update.html) or
  [DELETE](lang_delete.html) in any context but can be enabled for top-level statements
  using the [SQLITE_ENABLE_UPDATE_DELETE_LIMIT](compile.html#enable_update_delete_limit) compile-time option.  However,
  that compile-time option only applies to top-level [UPDATE](lang_update.html) and [DELETE](lang_delete.html)
  statements, not [UPDATE](lang_update.html) and [DELETE](lang_delete.html) statements within triggers.
  

- 

  [Common table expressions](syntax/common-table-expression.html) are not supported
  directly by statements inside of triggers but may be embedded within
  sub-selects used by statements inside of triggers.
  

# 3. INSTEAD OF triggers

BEFORE and AFTER triggers work only on ordinary tables.
INSTEAD OF triggers work only on views.

If an INSTEAD OF INSERT trigger exists on a view, then it is
possible to execute an INSERT statement against that view.  No actual
insert occurs.  Instead, the statements contained within the trigger
are run.  INSTEAD OF DELETE and
INSTEAD OF UPDATE triggers work the same way for DELETE and UPDATE statements
against views.

Note that the [sqlite3_changes()](c3ref/changes.html) and [sqlite3_total_changes()](c3ref/total_changes.html) interfaces
do not count INSTEAD OF trigger firings, but the
[count_changes pragma](pragma.html#pragma_count_changes) does count INSTEAD OF trigger firing.

# 4. Some Example Triggers

Assuming that customer records are stored in the "customers" table, and
that order records are stored in the "orders" table, the following
UPDATE trigger
ensures that all associated orders are redirected when a customer changes
his or her address:

```sql

CREATE TRIGGER update_customer_address UPDATE OF address ON customers 
  BEGIN
    UPDATE orders SET address = new.address WHERE customer_name = old.name;
  END;

```

With this trigger installed, executing the statement:

```sql

UPDATE customers SET address = '1 Main St.' WHERE name = 'Jack Jones';

```

causes the following to be automatically executed:

```sql

UPDATE orders SET address = '1 Main St.' WHERE customer_name = 'Jack Jones';

```

For an example of an INSTEAD OF trigger, consider the following schema:

```sql

CREATE TABLE customer(
  cust_id INTEGER PRIMARY KEY,
  cust_name TEXT,
  cust_addr TEXT
);
CREATE VIEW customer_address AS
   SELECT cust_id, cust_addr FROM customer;
CREATE TRIGGER cust_addr_chng
INSTEAD OF UPDATE OF cust_addr ON customer_address
BEGIN
  UPDATE customer SET cust_addr=NEW.cust_addr
   WHERE cust_id=NEW.cust_id;
END;

```

With the schema above, a statement of the form:

```sql

UPDATE customer_address SET cust_addr=$new_address WHERE cust_id=$cust_id;

```

causes the customer.cust_addr field to be updated for a specific
customer entry that has customer.cust_id equal to the $cust_id parameter.
Note how the values assigned to the view are made available as fields
in the special "NEW" table within the trigger body.

# 5. Cautions On The Use Of BEFORE triggers

If a BEFORE UPDATE or BEFORE DELETE trigger modifies or deletes a row
that was to have been updated or deleted, then the result of the subsequent
update or delete operation is undefined.  Furthermore, if a BEFORE trigger
modifies or deletes a row, then it is undefined whether or not AFTER triggers
that would have otherwise run on those rows will in fact run.

The value of NEW.rowid is undefined in a BEFORE INSERT trigger in which
the rowid is not explicitly set to an integer.

Because of the behaviors described above, programmers are encouraged to
prefer AFTER triggers over BEFORE triggers.

# 6. The RAISE() function

A special SQL function RAISE() may be used within a trigger-program,
with the following syntax
 

**[raise-function:](syntax/raise-function.html)**

 

 

When one of RAISE(ROLLBACK,...), RAISE(ABORT,...) or RAISE(FAIL,...)
is called during trigger-program
execution, the specified [ON CONFLICT](lang_conflict.html) processing is performed and
the current query terminates.
An error code of [SQLITE_CONSTRAINT](rescode.html#constraint) is returned to the application,
along with the specified error message.

When RAISE(IGNORE) is called, the remainder of the current trigger program,
the statement that caused the trigger program to execute and any subsequent
trigger programs that would have been executed are abandoned. No database
changes are rolled back.  If the statement that caused the trigger program
to execute is itself part of a trigger program, then that trigger program
resumes execution at the beginning of the next step.

Beginning with version 3.47.0 (2024-10-21), the
error-message can be an SQL expression.
In older versions of SQLite, the error-message
was required to be a string literal.

# 7. TEMP Triggers on Non-TEMP Tables

A trigger normally exists in the same database as the table named
after the "ON" keyword in the CREATE TRIGGER statement.  Except, it is
possible to create a TEMP TRIGGER on a table in another database.  
Such a trigger will only fire when changes
are made to the target table by the application that defined the trigger.
Other applications that modify the database will not be able to see the
TEMP trigger and hence cannot run the trigger.

When defining a TEMP trigger on a non-TEMP table, it is important to
specify the database holding the non-TEMP table.  For example,
in the following statement, it is important to say "main.tab1" instead
of just "tab1":

```sql

CREATE TEMP TRIGGER ex1 AFTER INSERT ON **main.**tab1 BEGIN ...

```

Failure to specify the schema name on the target table could result
in the TEMP trigger being reattached to a table with the same name in
another database whenever any schema change occurs.

*This page was last updated on 2025-05-31 13:08:22Z *