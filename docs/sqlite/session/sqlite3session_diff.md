Load The Difference Between Tables Into A Session
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
Session Module C InterfaceLoad The Difference Between Tables Into A Sessionint sqlite3session_diff(
  sqlite3_session *pSession,
  const char *zFromDb,
  const char *zTbl,
  char **pzErrMsg
);
If it is not already attached to the session object passed as the first
argument, this function attaches table zTbl in the same manner as the
sqlite3session_attach() function. If zTbl does not exist, or if it
does not have a primary key, this function is a no-op (but does not return
an error).
Argument zFromDb must be the name of a database ("main", "temp" etc.)
attached to the same database handle as the session object that contains 
a table compatible with the table attached to the session by this function.
A table is considered compatible if it:
   Has the same name,
   Has the same set of columns declared in the same order, and
   Has the same PRIMARY KEY definition.
If the tables are not compatible, SQLITE_SCHEMA is returned. If the tables
are compatible but do not have any PRIMARY KEY columns, it is not an error
but no changes are added to the session object. As with other session
APIs, tables without PRIMARY KEYs are simply ignored.
This function adds a set of changes to the session object that could be
used to update the table in database zFrom (call this the "from-table") 
so that its content is the same as the table attached to the session 
object (call this the "to-table"). Specifically:
   For each row (primary key) that exists in the to-table but not in 
    the from-table, an INSERT record is added to the session object.
   For each row (primary key) that exists in the to-table but not in 
    the from-table, a DELETE record is added to the session object.
   For each row (primary key) that exists in both tables, but features 
    different non-PK values in each, an UPDATE record is added to the
    session.  
To clarify, if this function is called and then a changeset constructed
using sqlite3session_changeset(), then after applying that changeset to 
database zFrom the contents of the two compatible tables would be 
identical.
Unless the call to this function is a no-op as described above, it is an
error if database zFrom does not exist or does not contain the required 
compatible table.
If the operation is successful, SQLITE_OK is returned. Otherwise, an SQLite
error code. In this case, if argument pzErrMsg is not NULL, *pzErrMsg
may be set to point to a buffer containing an English language error 
message. It is the responsibility of the caller to free this buffer using
sqlite3_free().
See also lists of
  Objects,
  Constants, and
  Functions.
