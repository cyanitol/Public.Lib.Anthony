Options for sqlite3session_object_config
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
Session Module C InterfaceOptions for sqlite3session_object_config#define SQLITE_SESSION_OBJCONFIG_SIZE  1
#define SQLITE_SESSION_OBJCONFIG_ROWID 2
The following values may passed as the the 2nd parameter to
sqlite3session_object_config().
SQLITE_SESSION_OBJCONFIG_SIZE 
  This option is used to set, clear or query the flag that enables
  the sqlite3session_changeset_size() API. Because it imposes some
  computational overhead, this API is disabled by default. Argument
  pArg must point to a value of type (int). If the value is initially
  0, then the sqlite3session_changeset_size() API is disabled. If it
  is greater than 0, then the same API is enabled. Or, if the initial
  value is less than zero, no change is made. In all cases the (int)
  variable is set to 1 if the sqlite3session_changeset_size() API is
  enabled following the current call, or 0 otherwise.
  It is an error (SQLITE_MISUSE) to attempt to modify this setting after 
  the first table has been attached to the session object.
SQLITE_SESSION_OBJCONFIG_ROWID 
  This option is used to set, clear or query the flag that enables
  collection of data for tables with no explicit PRIMARY KEY.
  Normally, tables with no explicit PRIMARY KEY are simply ignored
  by the sessions module. However, if this flag is set, it behaves
  as if such tables have a column "_rowid_ INTEGER PRIMARY KEY" inserted
  as their leftmost columns.
  It is an error (SQLITE_MISUSE) to attempt to modify this setting after 
  the first table has been attached to the session object.
See also lists of
  Objects,
  Constants, and
  Functions.
