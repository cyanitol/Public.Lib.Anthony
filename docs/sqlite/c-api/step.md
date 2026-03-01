Evaluate An SQL Statement
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
Evaluate An SQL Statement
int sqlite3_step(sqlite3_stmt*);
After a prepared statement has been prepared using any of
sqlite3_prepare_v2(), sqlite3_prepare_v3(), sqlite3_prepare16_v2(),
or sqlite3_prepare16_v3() or one of the legacy
interfaces sqlite3_prepare() or sqlite3_prepare16(), this function
must be called one or more times to evaluate the statement.
The details of the behavior of the sqlite3_step() interface depend
on whether the statement was prepared using the newer "vX" interfaces
sqlite3_prepare_v3(), sqlite3_prepare_v2(), sqlite3_prepare16_v3(),
sqlite3_prepare16_v2() or the older legacy
interfaces sqlite3_prepare() and sqlite3_prepare16().  The use of the
new "vX" interface is recommended for new applications but the legacy
interface will continue to be supported.
In the legacy interface, the return value will be either SQLITE_BUSY,
SQLITE_DONE, SQLITE_ROW, SQLITE_ERROR, or SQLITE_MISUSE.
With the "v2" interface, any of the other result codes or
extended result codes might be returned as well.
SQLITE_BUSY means that the database engine was unable to acquire the
database locks it needs to do its job.  If the statement is a COMMIT
or occurs outside of an explicit transaction, then you can retry the
statement.  If the statement is not a COMMIT and occurs within an
explicit transaction then you should rollback the transaction before
continuing.
SQLITE_DONE means that the statement has finished executing
successfully.  sqlite3_step() should not be called again on this virtual
machine without first calling sqlite3_reset() to reset the virtual
machine back to its initial state.
If the SQL statement being executed returns any data, then SQLITE_ROW
is returned each time a new row of data is ready for processing by the
caller. The values may be accessed using the column access functions.
sqlite3_step() is called again to retrieve the next row of data.
SQLITE_ERROR means that a run-time error (such as a constraint
violation) has occurred.  sqlite3_step() should not be called again on
the VM. More information may be found by calling sqlite3_errmsg().
With the legacy interface, a more specific error code (for example,
SQLITE_INTERRUPT, SQLITE_SCHEMA, SQLITE_CORRUPT, and so forth)
can be obtained by calling sqlite3_reset() on the
prepared statement.  In the "v2" interface,
the more specific error code is returned directly by sqlite3_step().
SQLITE_MISUSE means that the this routine was called inappropriately.
Perhaps it was called on a prepared statement that has
already been finalized or on one that had
previously returned SQLITE_ERROR or SQLITE_DONE.  Or it could
be the case that the same database connection is being used by two or
more threads at the same moment in time.
For all versions of SQLite up to and including 3.6.23.1, a call to
sqlite3_reset() was required after sqlite3_step() returned anything
other than SQLITE_ROW before any subsequent invocation of
sqlite3_step().  Failure to reset the prepared statement using
sqlite3_reset() would result in an SQLITE_MISUSE return from
sqlite3_step().  But after version 3.6.23.1 (2010-03-26),
sqlite3_step() began
calling sqlite3_reset() automatically in this circumstance rather
than returning SQLITE_MISUSE.  This is not considered a compatibility
break because any application that ever receives an SQLITE_MISUSE error
is broken by definition.  The SQLITE_OMIT_AUTORESET compile-time option
can be used to restore the legacy behavior.
Goofy Interface Alert: In the legacy interface, the sqlite3_step()
API always returns a generic error code, SQLITE_ERROR, following any
error other than SQLITE_BUSY and SQLITE_MISUSE.  You must call
sqlite3_reset() or sqlite3_finalize() in order to find one of the
specific error codes that better describes the error.
We admit that this is a goofy design.  The problem has been fixed
with the "v2" interface.  If you prepare all of your SQL statements
using sqlite3_prepare_v3() or sqlite3_prepare_v2()
or sqlite3_prepare16_v2() or sqlite3_prepare16_v3() instead
of the legacy sqlite3_prepare() and sqlite3_prepare16() interfaces,
then the more specific error codes are returned directly
by sqlite3_step().  The use of the "vX" interfaces is recommended.
See also lists of
  Objects,
  Constants, and
  Functions.
