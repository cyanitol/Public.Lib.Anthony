Create An Iterator To Traverse A Changeset 
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
Session Module C InterfaceCreate An Iterator To Traverse A Changeset int sqlite3changeset_start(
  sqlite3_changeset_iter **pp,    /* OUT: New changeset iterator handle */
  int nChangeset,                 /* Size of changeset blob in bytes */
  void *pChangeset                /* Pointer to blob containing changeset */
);
int sqlite3changeset_start_v2(
  sqlite3_changeset_iter **pp,    /* OUT: New changeset iterator handle */
  int nChangeset,                 /* Size of changeset blob in bytes */
  void *pChangeset,               /* Pointer to blob containing changeset */
  int flags                       /* SESSION_CHANGESETSTART_* flags */
);
Create an iterator used to iterate through the contents of a changeset.
If successful, *pp is set to point to the iterator handle and SQLITE_OK
is returned. Otherwise, if an error occurs, *pp is set to zero and an
SQLite error code is returned.
The following functions can be used to advance and query a changeset 
iterator created by this function:
   sqlite3changeset_next()
   sqlite3changeset_op()
   sqlite3changeset_new()
   sqlite3changeset_old()
It is the responsibility of the caller to eventually destroy the iterator
by passing it to sqlite3changeset_finalize(). The buffer containing the
changeset (pChangeset) must remain valid until after the iterator is
destroyed.
Assuming the changeset blob was created by one of the
sqlite3session_changeset(), sqlite3changeset_concat() or
sqlite3changeset_invert() functions, all changes within the changeset 
that apply to a single table are grouped together. This means that when 
an application iterates through a changeset using an iterator created by 
this function, all changes that relate to a single table are visited 
consecutively. There is no chance that the iterator will visit a change 
the applies to table X, then one for table Y, and then later on visit 
another change for table X.
The behavior of sqlite3changeset_start_v2() and its streaming equivalent
may be modified by passing a combination of
supported flags as the 4th parameter.
Note that the sqlite3changeset_start_v2() API is still experimental
and therefore subject to change.
See also lists of
  Objects,
  Constants, and
  Functions.
