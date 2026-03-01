Attach A Table To A Session Object
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
Session Module C InterfaceAttach A Table To A Session Objectint sqlite3session_attach(
  sqlite3_session *pSession,      /* Session object */
  const char *zTab                /* Table name */
);
If argument zTab is not NULL, then it is the name of a table to attach
to the session object passed as the first argument. All subsequent changes 
made to the table while the session object is enabled will be recorded. See 
documentation for sqlite3session_changeset() for further details.
Or, if argument zTab is NULL, then changes are recorded for all tables
in the database. If additional tables are added to the database (by 
executing "CREATE TABLE" statements) after this call is made, changes for 
the new tables are also recorded.
Changes can only be recorded for tables that have a PRIMARY KEY explicitly
defined as part of their CREATE TABLE statement. It does not matter if the 
PRIMARY KEY is an "INTEGER PRIMARY KEY" (rowid alias) or not. The PRIMARY
KEY may consist of a single column, or may be a composite key.
It is not an error if the named table does not exist in the database. Nor
is it an error if the named table does not have a PRIMARY KEY. However,
no changes will be recorded in either of these scenarios.
Changes are not recorded for individual rows that have NULL values stored
in one or more of their PRIMARY KEY columns.
SQLITE_OK is returned if the call completes without error. Or, if an error 
occurs, an SQLite error code (e.g. SQLITE_NOMEM) is returned.
Special sqlite_stat1 Handling
As of SQLite version 3.22.0, the "sqlite_stat1" table is an exception to 
some of the rules above. In SQLite, the schema of sqlite_stat1 is:
       CREATE TABLE sqlite_stat1(tbl,idx,stat)  
Even though sqlite_stat1 does not have a PRIMARY KEY, changes are 
recorded for it as if the PRIMARY KEY is (tbl,idx). Additionally, changes 
are recorded for rows for which (idx IS NULL) is true. However, for such
rows a zero-length blob (SQL value X'') is stored in the changeset or
patchset instead of a NULL value. This allows such changesets to be
manipulated by legacy implementations of sqlite3changeset_invert(),
concat() and similar.
The sqlite3changeset_apply() function automatically converts the 
zero-length blob back to a NULL value when updating the sqlite_stat1
table. However, if the application calls sqlite3changeset_new(),
sqlite3changeset_old() or sqlite3changeset_conflict on a changeset 
iterator directly (including on a changeset iterator passed to a
conflict-handler callback) then the X'' value is returned. The application
must translate X'' to NULL itself if required.
Legacy (older than 3.22.0) versions of the sessions module cannot capture
changes made to the sqlite_stat1 table. Legacy versions of the
sqlite3changeset_apply() function silently ignore any modifications to the
sqlite_stat1 table that are part of a changeset or patchset.
See also lists of
  Objects,
  Constants, and
  Functions.
