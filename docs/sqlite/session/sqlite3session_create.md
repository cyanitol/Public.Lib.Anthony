Create A New Session Object
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
Session Module C InterfaceCreate A New Session Objectint sqlite3session_create(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Name of db (e.g. "main") */
  sqlite3_session **ppSession     /* OUT: New session object */
);
Create a new session object attached to database handle db. If successful,
a pointer to the new object is written to *ppSession and SQLITE_OK is
returned. If an error occurs, *ppSession is set to NULL and an SQLite
error code (e.g. SQLITE_NOMEM) is returned.
It is possible to create multiple session objects attached to a single
database handle.
Session objects created using this function should be deleted using the
sqlite3session_delete() function before the database handle that they
are attached to is itself closed. If the database handle is closed before
the session object is deleted, then the results of calling any session
module function, including sqlite3session_delete() on the session object
are undefined.
Because the session module uses the sqlite3_preupdate_hook() API, it
is not possible for an application to register a pre-update hook on a
database handle that has one or more session objects attached. Nor is
it possible to create a session object attached to a database handle for
which a pre-update hook is already defined. The results of attempting 
either of these things are undefined.
The session object will be used to create changesets for tables in
database zDb, where zDb is either "main", or "temp", or the name of an
attached database. It is not an error if database zDb is not attached
to the database when the session object is created.
See also lists of
  Objects,
  Constants, and
  Functions.
