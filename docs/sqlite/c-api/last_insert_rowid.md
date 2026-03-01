Last Insert Rowid
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
Last Insert Rowid
sqlite3_int64 sqlite3_last_insert_rowid(sqlite3*);
Each entry in most SQLite tables (except for WITHOUT ROWID tables)
has a unique 64-bit signed
integer key called the "rowid". The rowid is always available
as an undeclared column named ROWID, OID, or _ROWID_ as long as those
names are not also used by explicitly declared columns. If
the table has a column of type INTEGER PRIMARY KEY then that column
is another alias for the rowid.
The sqlite3_last_insert_rowid(D) interface usually returns the rowid of
the most recent successful INSERT into a rowid table or virtual table
on database connection D. Inserts into WITHOUT ROWID tables are not
recorded. If no successful INSERTs into rowid tables have ever occurred
on the database connection D, then sqlite3_last_insert_rowid(D) returns
zero.
As well as being set automatically as rows are inserted into database
tables, the value returned by this function may be set explicitly by
sqlite3_set_last_insert_rowid()
Some virtual table implementations may INSERT rows into rowid tables as
part of committing a transaction (e.g. to flush data accumulated in memory
to disk). In this case subsequent calls to this function return the rowid
associated with these internal INSERT operations, which leads to
unintuitive results. Virtual table implementations that do write to rowid
tables in this way can avoid this problem by restoring the original
rowid value using sqlite3_set_last_insert_rowid() before returning
control to the user.
If an INSERT occurs within a trigger then this routine will
return the rowid of the inserted row as long as the trigger is
running. Once the trigger program ends, the value returned
by this routine reverts to what it was before the trigger was fired.
An INSERT that fails due to a constraint violation is not a
successful INSERT and does not change the value returned by this
routine.  Thus INSERT OR FAIL, INSERT OR IGNORE, INSERT OR ROLLBACK,
and INSERT OR ABORT make no changes to the return value of this
routine when their insertion fails.  When INSERT OR REPLACE
encounters a constraint violation, it does not fail.  The
INSERT continues to completion after deleting rows that caused
the constraint problem so INSERT OR REPLACE will always change
the return value of this interface.
For the purposes of this routine, an INSERT is considered to
be successful even if it is subsequently rolled back.
This function is accessible to SQL statements via the
last_insert_rowid() SQL function.
If a separate thread performs a new INSERT on the same
database connection while the sqlite3_last_insert_rowid()
function is running and thus changes the last insert rowid,
then the value returned by sqlite3_last_insert_rowid() is
unpredictable and might not equal either the old or the new
last insert rowid.
See also lists of
  Objects,
  Constants, and
  Functions.
