The Bytecode() And Tables_Used() Table-Valued Functions
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
The Bytecode() And Tables_Used() Table-Valued Functions
Table Of Contents
1. Overview
2. Usage
2.1. Schema For bytecode
2.2. Schema For tables_used
1. Overview
Bytecode and tables_used are virtual tables built into SQLite that
access information about prepared statements.
Both bytecode and tables_used operate as table-valued functions.
They take a single required argument which is either the text of
an SQL statement, or a pointer to an existing prepared statement.
The bytecode function returns one row of result for each bytecode
operation in the prepared statement.  The tables_used function returns
one row for each persistent btree (either a table or an index) accessed
by the prepared statement.
2. Usage
The bytecode and tables_used tables are only available if SQLite has
been compiled with the -DSQLITE_ENABLE_BYTECODE_VTAB compile-time option.
The CLI has been compiled that way, and so you can use the standard
CLI as a test platform to experiment.
Both virtual tables are read-only eponymous-only virtual tables.  You use them
by mentioning them directly in the FROM clause of a SELECT statement.
They both require a single argument which is the SQL statement to be
analyzed.  For example:
SELECT * FROM bytecode('SELECT * FROM bytecode(?1)');
The argument can be either the text of an SQL statement, in which case
the bytecode (or tables_used) for that statement is returned, or the
argument can be a parameter such as ?1 or $stmt that is later bound
to a prepared statement object using the
sqlite3_bind_pointer() interface.  Use a pointer type of
"stmt-pointer" for the sqlite3_bind_pointer() interface.
2.1. Schema For bytecode
The schema of the bytecode virtual table is:
CREATE TABLE bytecode(
  addr INT,
  opcode TEXT,
  p1 INT,
  p2 INT,
  p3 INT,
  p4 TEXT,
  p5 INT,
  comment TEXT,
  subprog TEXT,
  nexec INT,
  ncycle INT,
  stmt HIDDEN
);
The first eight columns are the address, opcode, and operands for a
single bytecode in the virtual machine that implements the statement.
These columns are the same columns output when using EXPLAIN.
 The bytecode virtual tables shows all operations in the prepared
statement, both the main body of the prepared statement and in
subprograms used to implement triggers or foreign key actions.
The "subprog" field is NULL for the main body of the prepared statement,
or is the trigger name or the string "(FK)" for triggers and foreign
key actions, respectively.
 The "nexec" and "ncycle" columns show the number of times
each opcode has been executed, and the total number of CPU cycles used
by that opcode, respectively.  These fields always have a value of 0
unless SQLite has been compiled with SQLITE_ENABLE_STMT_SCANSTATUS
compile-time options and scan-status statistics are enabled using the
sqlite3_db_config(db,SQLITE_DBCONFIG_STMT_SCANSTATUS,...)
interface.  The values for nexec and ncycle are cumulative, until
reset using sqlite3_stmt_scanstatus_reset().
The "ncycle" column currently always returns 0 unless SQLite is compiled
for X86_64 or AARCH64 processors using GCC, Clang, or MSVC, or
compiled for PPC processors using GCC or Clang.
2.2. Schema For tables_used
The schema for the tables_used table is:
CREATE TABLE tables_used(
  type TEXT,
  schema TEXT,
  name TEXT,
  wr INT,
  subprog TEXT,
  stmt HIDDEN
);
The tables_used table is intended to show which btrees of the database file
are read or written by a prepared statement, both by the main statement
itself but also by related triggers and foreign key actions.  The columns
are as follows:
type &rarr; Either "table" or "index", depending on what role
the btree is serving.
schema &rarr; Which database file the btree is located in.
This will be "main" for the main database (the usual case), or "temp" for
TEMP tables and indexes, or the name assigned to attached databases by
the ATTACH statement.
name &rarr; The name of the table or index
wr &rarr; 0 if the object is read, 1 if the object is written
subprog &rarr;  The sub-program in which the object is
accessed.  NULL means the main body of the prepared statement.  Otherwise
this field is the name of a trigger or "(FK)" for a foreign key action.
This page was last updated on 2025-11-13 07:12:58Z 
