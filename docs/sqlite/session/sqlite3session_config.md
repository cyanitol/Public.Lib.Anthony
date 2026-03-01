Configure global parameters
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
Session Module C InterfaceConfigure global parametersint sqlite3session_config(int op, void *pArg);
The sqlite3session_config() interface is used to make global configuration
changes to the sessions module in order to tune it to the specific needs 
of the application.
The sqlite3session_config() interface is not threadsafe. If it is invoked
while any other thread is inside any other sessions method then the
results are undefined. Furthermore, if it is invoked after any sessions
related objects have been created, the results are also undefined. 
The first argument to the sqlite3session_config() function must be one
of the SQLITE_SESSION_CONFIG_XXX constants defined below. The 
interpretation of the (void*) value passed as the second parameter and
the effect of calling this function depends on the value of the first
parameter.
SQLITE_SESSION_CONFIG_STRMSIZE
   By default, the sessions module streaming interfaces attempt to input
   and output data in approximately 1 KiB chunks. This operand may be used
   to set and query the value of this configuration setting. The pointer
   passed as the second argument must point to a value of type (int).
   If this value is greater than 0, it is used as the new streaming data
   chunk size for both input and output. Before returning, the (int) value
   pointed to by pArg is set to the final value of the streaming interface
   chunk size.
This function returns SQLITE_OK if successful, or an SQLite error code
otherwise.
See also lists of
  Objects,
  Constants, and
  Functions.
