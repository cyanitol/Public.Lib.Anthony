Column Names In A Result Set
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
Column Names In A Result Set
const char *sqlite3_column_name(sqlite3_stmt*, int N);
const void *sqlite3_column_name16(sqlite3_stmt*, int N);
These routines return the name assigned to a particular column
in the result set of a SELECT statement.  The sqlite3_column_name()
interface returns a pointer to a zero-terminated UTF-8 string
and sqlite3_column_name16() returns a pointer to a zero-terminated
UTF-16 string.  The first parameter is the prepared statement
that implements the SELECT statement. The second parameter is the
column number.  The leftmost column is number 0.
The returned string pointer is valid until either the prepared statement
is destroyed by sqlite3_finalize() or until the statement is automatically
reprepared by the first call to sqlite3_step() for a particular run
or until the next call to
sqlite3_column_name() or sqlite3_column_name16() on the same column.
If sqlite3_malloc() fails during the processing of either routine
(for example during a conversion from UTF-8 to UTF-16) then a
NULL pointer is returned.
The name of a result column is the value of the "AS" clause for
that column, if there is an AS clause.  If there is no AS clause
then the name of the column is unspecified and may change from
one release of SQLite to the next.
See also lists of
  Objects,
  Constants, and
  Functions.
