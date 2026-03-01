Register A Virtual Table Implementation
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
Register A Virtual Table Implementation
int sqlite3_create_module(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *p,   /* Methods for the module */
  void *pClientData          /* Client data for xCreate/xConnect */
);
int sqlite3_create_module_v2(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *p,   /* Methods for the module */
  void *pClientData,         /* Client data for xCreate/xConnect */
  void(*xDestroy)(void*)     /* Module destructor function */
);
These routines are used to register a new virtual table module name.
Module names must be registered before
creating a new virtual table using the module and before using a
preexisting virtual table for the module.
The module name is registered on the database connection specified
by the first parameter.  The name of the module is given by the
second parameter.  The third parameter is a pointer to
the implementation of the virtual table module.   The fourth
parameter is an arbitrary client data pointer that is passed through
into the xCreate and xConnect methods of the virtual table module
when a new virtual table is being created or reinitialized.
The sqlite3_create_module_v2() interface has a fifth parameter which
is a pointer to a destructor for the pClientData.  SQLite will
invoke the destructor function (if it is not NULL) when SQLite
no longer needs the pClientData pointer.  The destructor will also
be invoked if the call to sqlite3_create_module_v2() fails.
The sqlite3_create_module()
interface is equivalent to sqlite3_create_module_v2() with a NULL
destructor.
If the third parameter (the pointer to the sqlite3_module object) is
NULL then no new module is created and any existing modules with the
same name are dropped.
See also: sqlite3_drop_modules()
See also lists of
  Objects,
  Constants, and
  Functions.
