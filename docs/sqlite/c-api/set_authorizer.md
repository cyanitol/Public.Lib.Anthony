Compile-Time Authorization Callbacks
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
Compile-Time Authorization Callbacks
int sqlite3_set_authorizer(
  sqlite3*,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void *pUserData
);
This routine registers an authorizer callback with a particular
database connection, supplied in the first argument.
The authorizer callback is invoked as SQL statements are being compiled
by sqlite3_prepare() or its variants sqlite3_prepare_v2(),
sqlite3_prepare_v3(), sqlite3_prepare16(), sqlite3_prepare16_v2(),
and sqlite3_prepare16_v3().  At various
points during the compilation process, as logic is being created
to perform various actions, the authorizer callback is invoked to
see if those actions are allowed.  The authorizer callback should
return SQLITE_OK to allow the action, SQLITE_IGNORE to disallow the
specific action but allow the SQL statement to continue to be
compiled, or SQLITE_DENY to cause the entire SQL statement to be
rejected with an error.  If the authorizer callback returns
any value other than SQLITE_IGNORE, SQLITE_OK, or SQLITE_DENY
then the sqlite3_prepare_v2() or equivalent call that triggered
the authorizer will fail with an error message.
When the callback returns SQLITE_OK, that means the operation
requested is ok.  When the callback returns SQLITE_DENY, the
sqlite3_prepare_v2() or equivalent call that triggered the
authorizer will fail with an error message explaining that
access is denied.
The first parameter to the authorizer callback is a copy of the third
parameter to the sqlite3_set_authorizer() interface. The second parameter
to the callback is an integer action code that specifies
the particular action to be authorized. The third through sixth parameters
to the callback are either NULL pointers or zero-terminated strings
that contain additional details about the action to be authorized.
Applications must always be prepared to encounter a NULL pointer in any
of the third through the sixth parameters of the authorization callback.
If the action code is SQLITE_READ
and the callback returns SQLITE_IGNORE then the
prepared statement statement is constructed to substitute
a NULL value in place of the table column that would have
been read if SQLITE_OK had been returned.  The SQLITE_IGNORE
return can be used to deny an untrusted user access to individual
columns of a table.
When a table is referenced by a SELECT but no column values are
extracted from that table (for example in a query like
"SELECT count(*) FROM tab") then the SQLITE_READ authorizer callback
is invoked once for that table with a column name that is an empty string.
If the action code is SQLITE_DELETE and the callback returns
SQLITE_IGNORE then the DELETE operation proceeds but the
truncate optimization is disabled and all rows are deleted individually.
An authorizer is used when preparing
SQL statements from an untrusted source, to ensure that the SQL statements
do not try to access data they are not allowed to see, or that they do not
try to execute malicious statements that damage the database.  For
example, an application may allow a user to enter arbitrary
SQL queries for evaluation by a database.  But the application does
not want the user to be able to make arbitrary changes to the
database.  An authorizer could then be put in place while the
user-entered SQL is being prepared that
disallows everything except SELECT statements.
Applications that need to process SQL from untrusted sources
might also consider lowering resource limits using sqlite3_limit()
and limiting database size using the max_page_count PRAGMA
in addition to using an authorizer.
Only a single authorizer can be in place on a database connection
at a time.  Each call to sqlite3_set_authorizer overrides the
previous call.  Disable the authorizer by installing a NULL callback.
The authorizer is disabled by default.
The authorizer callback must not do anything that will modify
the database connection that invoked the authorizer callback.
Note that sqlite3_prepare_v2() and sqlite3_step() both modify their
database connections for the meaning of "modify" in this paragraph.
When sqlite3_prepare_v2() is used to prepare a statement, the
statement might be re-prepared during sqlite3_step() due to a
schema change.  Hence, the application should ensure that the
correct authorizer callback remains in place during the sqlite3_step().
Note that the authorizer callback is invoked only during
sqlite3_prepare() or its variants.  Authorization is not
performed during statement evaluation in sqlite3_step(), unless
as stated in the previous paragraph, sqlite3_step() invokes
sqlite3_prepare_v2() to reprepare a statement after a schema change.
See also lists of
  Objects,
  Constants, and
  Functions.
