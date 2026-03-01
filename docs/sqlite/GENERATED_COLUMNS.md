Generated Columns
Small. Fast. Reliable.Choose any three.
Home
Menu
About
Documentation
Download
License
Support
Purchase
Search
About
Documentation
Download
Support
Purchase
Search Documentation
Search Changelog
function toggle_div(nm) {
var w = document.getElementById(nm);
if( w.style.display=="block" ){
w.style.display = "none";
}else{
w.style.display = "block";
}
}
function toggle_search() {
var w = document.getElementById("searchmenu");
if( w.style.display=="block" ){
w.style.display = "none";
} else {
w.style.display = "block";
setTimeout(function(){
document.getElementById("searchbox").focus()
}, 30);
}
}
function div_off(nm){document.getElementById(nm).style.display="none";}
window.onbeforeunload = function(e){div_off("submenu");}
/* Disable the Search feature if we are not operating from CGI, since */
/* Search is accomplished using CGI and will not work without it. */
if( !location.origin || !location.origin.match || !location.origin.match(/http/) ){
document.getElementById("search_menubutton").style.display = "none";
}
/* Used by the Hide/Show button beside syntax diagrams, to toggle the */
function hideorshow(btn,obj){
var x = document.getElementById(obj);
var b = document.getElementById(btn);
if( x.style.display!='none' ){
x.style.display = 'none';
b.innerHTML='show';
}else{
x.style.display = '';
b.innerHTML='hide';
}
return false;
}
var antiRobot = 0;
function antiRobotGo(){
if( antiRobot!=3 ) return;
antiRobot = 7;
var j = document.getElementById("mtimelink");
if(j && j.hasAttribute("data-href")) j.href=j.getAttribute("data-href");
}
function antiRobotDefense(){
document.body.onmousedown=function(){
antiRobot |= 2;
antiRobotGo();
document.body.onmousedown=null;
}
document.body.onmousemove=function(){
antiRobot |= 2;
antiRobotGo();
document.body.onmousemove=null;
}
setTimeout(function(){
antiRobot |= 1;
antiRobotGo();
}, 100)
antiRobotGo();
}
antiRobotDefense();
Generated Columns
Table Of Contents
1. Introduction
2. Syntax
2.1. VIRTUAL versus STORED columns
2.2. Capabilities
2.3. Limitations
3. Compatibility
1. Introduction
Generated columns (also sometimes called "computed columns")
are columns of a table whose values are a function of other columns
in the same row.
Generated columns can be read, but their values can not be directly
written.  The only way to change the value of a generated column is to
modify the values of the other columns used to calculate
the generated column.
2. Syntax
Syntactically, generated columns are designated using a
"GENERATED ALWAYS" column-constraint.  For example:
CREATE TABLE t1(
   a INTEGER PRIMARY KEY,
   b INT,
   c TEXT,
   d INT GENERATED ALWAYS AS (a*abs(b)) VIRTUAL,
   e TEXT GENERATED ALWAYS AS (substr(c,b,b+1)) STORED
);
The statement above has three ordinary columns, "a" (the PRIMARY KEY),
"b", and "c", and two generated columns "d" and "e".
The "GENERATED ALWAYS" keywords at the beginning of the constraint
and the "VIRTUAL" or "STORED" keyword at the end are all optional.
Only the "AS" keyword and the parenthesized expression are required.
If the trailing "VIRTUAL" or "STORED" keyword is omitted, then
VIRTUAL is the default.  Hence, the example statement above could
be simplified to just:
CREATE TABLE t1(
   a INTEGER PRIMARY KEY,
   b INT,
   c TEXT,
   d INT AS (a*abs(b)),
   e TEXT AS (substr(c,b,b+1)) STORED
);
2.1. VIRTUAL versus STORED columns
Generated columns can be either VIRTUAL or STORED.  The value of
a VIRTUAL column is computed when read, whereas the value of a STORED
column is computed when the row is written.  STORED columns take up space
in the database file, whereas VIRTUAL columns use more CPU cycles when
being read.
From the point of view of SQL, STORED and VIRTUAL columns are almost
exactly the same.  Queries against either class of generated column
produce the same results.  The only functional difference is that
one cannot add new STORED columns using the
ALTER TABLE ADD COLUMN command.  Only VIRTUAL columns can be added
using ALTER TABLE.
2.2. Capabilities
Generated columns can have a datatype.  SQLite attempts to transform
the result of the generating expression into that datatype using the
same affinity rules as for ordinary columns.
Generated columns may have NOT NULL, CHECK, and UNIQUE constraints,
and foreign key constraints, just like ordinary columns.
Generated columns can participate in indexes, just like ordinary
columns.
The expression of a generated column can refer to any of the
other declared columns in the table, including other generated columns,
as long as the expression does not directly or indirectly refer back
to itself.
Generated columns can occur anywhere in the table definition.  Generated
columns can be interspersed among ordinary columns.  It is not necessary
to put generated columns at the end of the list of columns in the
table definition, as is shown in the examples above.
2.3. Limitations
Generated columns may not have a default value (they may not use the
"DEFAULT" clause).  The value of a generated column is always the value
specified by the expression that follows the "AS" keyword.
Generated columns may not be used as part of the PRIMARY KEY.
(Future versions of SQLite might relax this constraint for STORED columns.)
The expression of a generated column may only reference
constant literals and columns within the same row, and may only use
scalar deterministic functions.  The expression may not use subqueries,
aggregate functions, window functions, or table-valued functions.
The expression of a generated column may refer to other generated columns
in the same row, but no generated column can depend upon itself, either
directly or indirectly.  
The expression of a generated column may not directly reference
the ROWID, though it can reference the INTEGER PRIMARY KEY column,
which is often the same thing.
Every table must have at least one non-generated column.
It is not possible to ALTER TABLE ADD COLUMN a STORED column.
One can add a VIRTUAL column, however.
The datatype and collating sequence of the generated column are determined
only by the datatype and COLLATE clause on the column definition.
The datatype and collating sequence of the GENERATED ALWAYS AS expression
have no effect on the datatype and collating sequence of the column itself.
Generated columns are not included in the list of columns provided by
the PRAGMA table_info statement.  But they are included in the output of
the newer PRAGMA table_xinfo statement.
3. Compatibility
Generated column support was added with SQLite version 3.31.0
(2020-01-22).  If an earlier version of SQLite attempts to read
a database file that contains a generated column in its schema, then
that earlier version will perceive the generated column syntax as an
error and will report that the database schema is corrupt.
To clarify:  SQLite version 3.31.0 can read and write any database
created by any prior version of SQLite going back to 
SQLite 3.0.0 (2004-06-18).  And, earlier versions of SQLite,
prior to 3.31.0, can read and write databases created by SQLite
version 3.31.0 and later as long
as the database schema does not contain features, such as
generated columns, that are not understood by the earlier version.
Problems only arise if you create a new database that contains
generated columns, using SQLite version 3.31.0 or later, and then
try to read or write that database file using an earlier version of
SQLite that does not understand generated columns.
This page was last updated on 2025-05-31 13:08:22Z 
