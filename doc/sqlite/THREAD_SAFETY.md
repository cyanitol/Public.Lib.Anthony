Using SQLite In Multi-Threaded Applications
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
Using SQLite In Multi-Threaded Applications
1. Overview
SQLite supports three different threading modes:
Single-thread.
In this mode, all mutexes are disabled and SQLite is unsafe to use in
more than a single thread at once.
Multi-thread.
In this mode, SQLite can be safely used by multiple threads provided that
no single database connection nor any object derived from database connection,
such as a prepared statement,
is used in two or more threads at the same time.
Serialized.
In serialized mode, API calls to affect or use any SQLite database connection
or any object derived from such a database connection
can be made safely from multiple threads.
The effect on an individual object is the same as if the API calls had all
been made in the same order from a single thread.  The name "serialized"
arises from the fact that SQLite uses mutexes to serialize access to each
object.
The threading mode can be selected at compile-time (when the SQLite
library is being compiled from source code) or at start-time (when the
application that intends to use SQLite is initializing) or at
run-time (when a new SQLite database connection is being created).
Generally speaking, run-time overrides start-time and start-time
overrides compile-time.  Except, single-thread mode cannot be
overridden once selected.
The default mode is serialized.
2. Compile-time selection of threading mode
Use the SQLITE_THREADSAFE compile-time parameter to select the
threading mode.  If no SQLITE_THREADSAFE compile-time parameter is
present, then serialized mode is used.
This can be made explicit with 
-DSQLITE_THREADSAFE=1.
With
-DSQLITE_THREADSAFE=0 the threading mode is
single-thread.  With
-DSQLITE_THREADSAFE=2 the threading mode is
multi-thread.
The return value of the sqlite3_threadsafe() interface is the value
of SQLITE_THREADSAFE set at compile-time. It does not reflect changes
to the threading mode made at runtime via the sqlite3_config()
interface or by flags given as the third argument to sqlite3_open_v2().
If single-thread mode is selected at compile-time, then critical
mutexing logic is omitted from the build and it is impossible to
enable either multi-thread or serialized modes at start-time or
run-time.
3. Start-time selection of threading mode
Assuming that the compile-time threading mode is not single-thread, then
the threading mode can be changed during initialization using the
sqlite3_config() interface.  The SQLITE_CONFIG_SINGLETHREAD verb
puts SQLite into single-thread mode, the SQLITE_CONFIG_MULTITHREAD
verb sets multi-thread mode, and the SQLITE_CONFIG_SERIALIZED verb
sets serialized mode.
4. Run-time selection of threading mode
If single-thread mode has not been selected at compile-time or start-time,
then individual database connections can be created as either multi-thread
or serialized.  It is not possible to downgrade an individual database
connection to single-thread mode.  Nor is it possible to escalate an
individual database connection if the compile-time or start-time mode
is single-thread.
The threading mode for an individual database connection is determined
by flags given as the third argument to sqlite3_open_v2().  The
SQLITE_OPEN_NOMUTEX flag causes the database connection to be in the
multi-thread mode and the SQLITE_OPEN_FULLMUTEX flag causes the connection
to be in serialized mode.  If neither flag is specified or if
sqlite3_open() or sqlite3_open16() are used instead of 
sqlite3_open_v2(), then the default
mode determined by the compile-time and start-time settings is used.
This page was last updated on 2023-12-05 14:43:20Z 
