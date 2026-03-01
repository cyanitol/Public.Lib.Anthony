One-Step Query Execution Interface
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
One-Step Query Execution Interface
int sqlite3_exec(
  sqlite3*,                                  /* An open database */
  const char *sql,                           /* SQL to be evaluated */
  int (*callback)(void*,int,char**,char**),  /* Callback function */
  void *,                                    /* 1st argument to callback */
  char **errmsg                              /* Error msg written here */
);
The sqlite3_exec() interface is a convenience wrapper around
sqlite3_prepare_v2(), sqlite3_step(), and sqlite3_finalize(),
that allows an application to run multiple statements of SQL
without having to use a lot of C code.
The sqlite3_exec() interface runs zero or more UTF-8 encoded,
semicolon-separated SQL statements passed into its 2nd argument,
in the context of the database connection passed in as its 1st
argument.  If the callback function of the 3rd argument to
sqlite3_exec() is not NULL, then it is invoked for each result row
coming out of the evaluated SQL statements.  The 4th argument to
sqlite3_exec() is relayed through to the 1st argument of each
callback invocation.  If the callback pointer to sqlite3_exec()
is NULL, then no callback is ever invoked and result rows are
ignored.
If an error occurs while evaluating the SQL statements passed into
sqlite3_exec(), then execution of the current statement stops and
subsequent statements are skipped.  If the 5th parameter to sqlite3_exec()
is not NULL then any error message is written into memory obtained
from sqlite3_malloc() and passed back through the 5th parameter.
To avoid memory leaks, the application should invoke sqlite3_free()
on error message strings returned through the 5th parameter of
sqlite3_exec() after the error message string is no longer needed.
If the 5th parameter to sqlite3_exec() is not NULL and no errors
occur, then sqlite3_exec() sets the pointer in its 5th parameter to
NULL before returning.
If an sqlite3_exec() callback returns non-zero, the sqlite3_exec()
routine returns SQLITE_ABORT without invoking the callback again and
without running any subsequent SQL statements.
The 2nd argument to the sqlite3_exec() callback function is the
number of columns in the result.  The 3rd argument to the sqlite3_exec()
callback is an array of pointers to strings obtained as if from
sqlite3_column_text(), one for each column.  If an element of a
result row is NULL then the corresponding string pointer for the
sqlite3_exec() callback is a NULL pointer.  The 4th argument to the
sqlite3_exec() callback is an array of pointers to strings where each
entry represents the name of a corresponding result column as obtained
from sqlite3_column_name().
If the 2nd parameter to sqlite3_exec() is a NULL pointer, a pointer
to an empty string, or a pointer that contains only whitespace and/or
SQL comments, then no SQL statements are evaluated and the database
is not changed.
Restrictions:
 The application must ensure that the 1st parameter to sqlite3_exec()
is a valid and open database connection.
 The application must not close the database connection specified by
the 1st parameter to sqlite3_exec() while sqlite3_exec() is running.
 The application must not modify the SQL statement text passed into
the 2nd parameter of sqlite3_exec() while sqlite3_exec() is running.
 The application must not dereference the arrays or string pointers
passed as the 3rd and 4th callback parameters after it returns.
See also lists of
  Objects,
  Constants, and
  Functions.
