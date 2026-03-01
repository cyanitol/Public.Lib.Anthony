Retrieving Statement SQL
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
Retrieving Statement SQL
const char *sqlite3_sql(sqlite3_stmt *pStmt);
char *sqlite3_expanded_sql(sqlite3_stmt *pStmt);
#ifdef SQLITE_ENABLE_NORMALIZE
const char *sqlite3_normalized_sql(sqlite3_stmt *pStmt);
#endif
The sqlite3_sql(P) interface returns a pointer to a copy of the UTF-8
SQL text used to create prepared statement P if P was
created by sqlite3_prepare_v2(), sqlite3_prepare_v3(),
sqlite3_prepare16_v2(), or sqlite3_prepare16_v3().
The sqlite3_expanded_sql(P) interface returns a pointer to a UTF-8
string containing the SQL text of prepared statement P with
bound parameters expanded.
The sqlite3_normalized_sql(P) interface returns a pointer to a UTF-8
string containing the normalized SQL text of prepared statement P.  The
semantics used to normalize a SQL statement are unspecified and subject
to change.  At a minimum, literal values will be replaced with suitable
placeholders.
For example, if a prepared statement is created using the SQL
text "SELECT $abc,:xyz" and if parameter $abc is bound to integer 2345
and parameter :xyz is unbound, then sqlite3_sql() will return
the original string, "SELECT $abc,:xyz" but sqlite3_expanded_sql()
will return "SELECT 2345,NULL".
The sqlite3_expanded_sql() interface returns NULL if insufficient memory
is available to hold the result, or if the result would exceed the
maximum string length determined by the SQLITE_LIMIT_LENGTH.
The SQLITE_TRACE_SIZE_LIMIT compile-time option limits the size of
bound parameter expansions.  The SQLITE_OMIT_TRACE compile-time
option causes sqlite3_expanded_sql() to always return NULL.
The strings returned by sqlite3_sql(P) and sqlite3_normalized_sql(P)
are managed by SQLite and are automatically freed when the prepared
statement is finalized.
The string returned by sqlite3_expanded_sql(P), on the other hand,
is obtained from sqlite3_malloc() and must be freed by the application
by passing it to sqlite3_free().
The sqlite3_normalized_sql() interface is only available if
the SQLITE_ENABLE_NORMALIZE compile-time option is defined.
See also lists of
  Objects,
  Constants, and
  Functions.
