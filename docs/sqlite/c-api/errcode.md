Error Codes And Messages
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
Error Codes And Messages
int sqlite3_errcode(sqlite3 *db);
int sqlite3_extended_errcode(sqlite3 *db);
const char *sqlite3_errmsg(sqlite3*);
const void *sqlite3_errmsg16(sqlite3*);
const char *sqlite3_errstr(int);
int sqlite3_error_offset(sqlite3 *db);
If the most recent sqlite3_* API call associated with
database connection D failed, then the sqlite3_errcode(D) interface
returns the numeric result code or extended result code for that
API call.
The sqlite3_extended_errcode()
interface is the same except that it always returns the
extended result code even when extended result codes are
disabled.
The values returned by sqlite3_errcode() and/or
sqlite3_extended_errcode() might change with each API call.
Except, there are some interfaces that are guaranteed to never
change the value of the error code.  The error-code preserving
interfaces include the following:
 sqlite3_errcode()
 sqlite3_extended_errcode()
 sqlite3_errmsg()
 sqlite3_errmsg16()
 sqlite3_error_offset()
The sqlite3_errmsg() and sqlite3_errmsg16() return English-language
text that describes the error, as either UTF-8 or UTF-16 respectively,
or NULL if no error message is available.
(See how SQLite handles invalid UTF for exceptions to this rule.)
Memory to hold the error message string is managed internally.
The application does not need to worry about freeing the result.
However, the error string might be overwritten or deallocated by
subsequent calls to other SQLite interface functions.
The sqlite3_errstr(E) interface returns the English-language text
that describes the result code E, as UTF-8, or NULL if E is not a
result code for which a text error message is available.
Memory to hold the error message string is managed internally
and must not be freed by the application.
If the most recent error references a specific token in the input
SQL, the sqlite3_error_offset() interface returns the byte offset
of the start of that token.  The byte offset returned by
sqlite3_error_offset() assumes that the input SQL is UTF-8.
If the most recent error does not reference a specific token in the input
SQL, then the sqlite3_error_offset() function returns -1.
When the serialized threading mode is in use, it might be the
case that a second error occurs on a separate thread in between
the time of the first error and the call to these interfaces.
When that happens, the second error will be reported since these
interfaces always report the most recent result.  To avoid
this, each thread can obtain exclusive use of the database connection D
by invoking sqlite3_mutex_enter(sqlite3_db_mutex(D)) before beginning
to use D and invoking sqlite3_mutex_leave(sqlite3_db_mutex(D)) after
all calls to the interfaces listed here are completed.
If an interface fails with SQLITE_MISUSE, that means the interface
was invoked incorrectly by the application.  In that case, the
error code and message may or may not be set.
See also lists of
  Objects,
  Constants, and
  Functions.
