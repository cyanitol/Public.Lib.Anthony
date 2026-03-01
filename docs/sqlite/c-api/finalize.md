Destroy A Prepared Statement Object
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
Destroy A Prepared Statement Object
int sqlite3_finalize(sqlite3_stmt *pStmt);
The sqlite3_finalize() function is called to delete a prepared statement.
If the most recent evaluation of the statement encountered no errors
or if the statement has never been evaluated, then sqlite3_finalize() returns
SQLITE_OK.  If the most recent evaluation of statement S failed, then
sqlite3_finalize(S) returns the appropriate error code or
extended error code.
The sqlite3_finalize(S) routine can be called at any point during
the life cycle of prepared statement S:
before statement S is ever evaluated, after
one or more calls to sqlite3_reset(), or after any call
to sqlite3_step() regardless of whether or not the statement has
completed execution.
Invoking sqlite3_finalize() on a NULL pointer is a harmless no-op.
The application must finalize every prepared statement in order to avoid
resource leaks.  It is a grievous error for the application to try to use
a prepared statement after it has been finalized.  Any use of a prepared
statement after it has been finalized can result in undefined and
undesirable behavior such as segfaults and heap corruption.
See also lists of
  Objects,
  Constants, and
  Functions.
