Constants Returned By The Conflict Handler
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
Session Module C InterfaceConstants Returned By The Conflict Handler#define SQLITE_CHANGESET_OMIT       0
#define SQLITE_CHANGESET_REPLACE    1
#define SQLITE_CHANGESET_ABORT      2
A conflict handler callback must return one of the following three values.
SQLITE_CHANGESET_OMIT
  If a conflict handler returns this value no special action is taken. The
  change that caused the conflict is not applied. The session module 
  continues to the next change in the changeset.
SQLITE_CHANGESET_REPLACE
  This value may only be returned if the second argument to the conflict
  handler was SQLITE_CHANGESET_DATA or SQLITE_CHANGESET_CONFLICT. If this
  is not the case, any changes applied so far are rolled back and the 
  call to sqlite3changeset_apply() returns SQLITE_MISUSE.
  If CHANGESET_REPLACE is returned by an SQLITE_CHANGESET_DATA conflict
  handler, then the conflicting row is either updated or deleted, depending
  on the type of change.
  If CHANGESET_REPLACE is returned by an SQLITE_CHANGESET_CONFLICT conflict
  handler, then the conflicting row is removed from the database and a
  second attempt to apply the change is made. If this second attempt fails,
  the original row is restored to the database before continuing.
SQLITE_CHANGESET_ABORT
  If this value is returned, any changes applied so far are rolled back 
  and the call to sqlite3changeset_apply() returns SQLITE_ABORT.
See also lists of
  Objects,
  Constants, and
  Functions.
