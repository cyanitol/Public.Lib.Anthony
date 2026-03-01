Obtain old.* Values From A Changeset Iterator
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
Session Module C InterfaceObtain old.* Values From A Changeset Iteratorint sqlite3changeset_old(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Column number */
  sqlite3_value **ppValue         /* OUT: Old value (or NULL pointer) */
);
The pIter argument passed to this function may either be an iterator
passed to a conflict-handler by sqlite3changeset_apply(), or an iterator
created by sqlite3changeset_start(). In the latter case, the most recent
call to sqlite3changeset_next() must have returned SQLITE_ROW. 
Furthermore, it may only be called if the type of change that the iterator
currently points to is either SQLITE_DELETE or SQLITE_UPDATE. Otherwise,
this function returns SQLITE_MISUSE and sets *ppValue to NULL.
Argument iVal must be greater than or equal to 0, and less than the number
of columns in the table affected by the current change. Otherwise,
SQLITE_RANGE is returned and *ppValue is set to NULL.
If successful, this function sets *ppValue to point to a protected
sqlite3_value object containing the iVal'th value from the vector of 
original row values stored as part of the UPDATE or DELETE change and
returns SQLITE_OK. The name of the function comes from the fact that this 
is similar to the "old.*" columns available to update or delete triggers.
If some other error occurs (e.g. an OOM condition), an SQLite error code
is returned and *ppValue is set to NULL.
See also lists of
  Objects,
  Constants, and
  Functions.
