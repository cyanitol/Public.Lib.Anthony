Determine If An SQL Statement Writes The Database
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
SQLite C Interface
Determine If An SQL Statement Writes The Database
int sqlite3_stmt_readonly(sqlite3_stmt *pStmt);
The sqlite3_stmt_readonly(X) interface returns true (non-zero) if
and only if the prepared statement X makes no direct changes to
the content of the database file.
Note that application-defined SQL functions or
virtual tables might change the database indirectly as a side effect.
For example, if an application defines a function "eval()" that
calls sqlite3_exec(), then the following SQL statement would
change the database file through side-effects:
SELECT eval('DELETE FROM t1') FROM t2;
But because the SELECT statement does not change the database file
directly, sqlite3_stmt_readonly() would still return true.
Transaction control statements such as BEGIN, COMMIT, ROLLBACK,
SAVEPOINT, and RELEASE cause sqlite3_stmt_readonly() to return true,
since the statements themselves do not actually modify the database but
rather they control the timing of when other statements modify the
database.  The ATTACH and DETACH statements also cause
sqlite3_stmt_readonly() to return true since, while those statements
change the configuration of a database connection, they do not make
changes to the content of the database files on disk.
The sqlite3_stmt_readonly() interface returns true for BEGIN since
BEGIN merely sets internal flags, but the BEGIN IMMEDIATE and
BEGIN EXCLUSIVE commands do touch the database and so
sqlite3_stmt_readonly() returns false for those commands.
This routine returns false if there is any possibility that the
statement might change the database file.  A false return does
not guarantee that the statement will change the database file.
For example, an UPDATE statement might have a WHERE clause that
makes it a no-op, but the sqlite3_stmt_readonly() result would still
be false.  Similarly, a CREATE TABLE IF NOT EXISTS statement is a
read-only no-op if the table already exists, but
sqlite3_stmt_readonly() still returns false for such a statement.
If prepared statement X is an EXPLAIN or EXPLAIN QUERY PLAN
statement, then sqlite3_stmt_readonly(X) returns the same value as
if the EXPLAIN or EXPLAIN QUERY PLAN prefix were omitted.
See also lists of
  Objects,
  Constants, and
  Functions.
