Finalize A Changeset Iterator
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
Session Module C InterfaceFinalize A Changeset Iteratorint sqlite3changeset_finalize(sqlite3_changeset_iter *pIter);
This function is used to finalize an iterator allocated with
sqlite3changeset_start().
This function should only be called on iterators created using the
sqlite3changeset_start() function. If an application calls this
function with an iterator passed to a conflict-handler by
sqlite3changeset_apply(), SQLITE_MISUSE is immediately returned and the
call has no effect.
If an error was encountered within a call to an sqlite3changeset_xxx()
function (for example an SQLITE_CORRUPT in sqlite3changeset_next() or an 
SQLITE_NOMEM in sqlite3changeset_new()) then an error code corresponding
to that error is returned by this function. Otherwise, SQLITE_OK is
returned. This is to allow the following pattern (pseudo-code):
  sqlite3changeset_start();
  while( SQLITE_ROW==sqlite3changeset_next() ){
    // Do something with change.
  }
  rc = sqlite3changeset_finalize();
  if( rc!=SQLITE_OK ){
    // An error has occurred 
  }
See also lists of
  Objects,
  Constants, and
  Functions.
