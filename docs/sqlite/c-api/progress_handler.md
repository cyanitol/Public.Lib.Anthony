Query Progress Callbacks
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
Query Progress Callbacks
void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);
The sqlite3_progress_handler(D,N,X,P) interface causes the callback
function X to be invoked periodically during long running calls to
sqlite3_step() and sqlite3_prepare() and similar for
database connection D.  An example use for this
interface is to keep a GUI updated during a large query.
The parameter P is passed through as the only parameter to the
callback function X.  The parameter N is the approximate number of
virtual machine instructions that are evaluated between successive
invocations of the callback X.  If N is less than one then the progress
handler is disabled.
Only a single progress handler may be defined at one time per
database connection; setting a new progress handler cancels the
old one.  Setting parameter X to NULL disables the progress handler.
The progress handler is also disabled by setting N to a value less
than 1.
If the progress callback returns non-zero, the operation is
interrupted.  This feature can be used to implement a
"Cancel" button on a GUI progress dialog box.
The progress handler callback must not do anything that will modify
the database connection that invoked the progress handler.
Note that sqlite3_prepare_v2() and sqlite3_step() both modify their
database connections for the meaning of "modify" in this paragraph.
The progress handler callback would originally only be invoked from the
bytecode engine.  It still might be invoked during sqlite3_prepare()
and similar because those routines might force a reparse of the schema
which involves running the bytecode engine.  However, beginning with
SQLite version 3.41.0, the progress handler callback might also be
invoked directly from sqlite3_prepare() while analyzing and generating
code for complex queries.
See also lists of
  Objects,
  Constants, and
  Functions.
