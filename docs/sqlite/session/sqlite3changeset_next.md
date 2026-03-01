Advance A Changeset Iterator
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
Session Module C InterfaceAdvance A Changeset Iteratorint sqlite3changeset_next(sqlite3_changeset_iter *pIter);
This function may only be used with iterators created by the function
sqlite3changeset_start(). If it is called on an iterator passed to
a conflict-handler callback by sqlite3changeset_apply(), SQLITE_MISUSE
is returned and the call has no effect.
Immediately after an iterator is created by sqlite3changeset_start(), it
does not point to any change in the changeset. Assuming the changeset
is not empty, the first call to this function advances the iterator to
point to the first change in the changeset. Each subsequent call advances
the iterator to point to the next change in the changeset (if any). If
no error occurs and the iterator points to a valid change after a call
to sqlite3changeset_next() has advanced it, SQLITE_ROW is returned. 
Otherwise, if all changes in the changeset have already been visited,
SQLITE_DONE is returned.
If an error occurs, an SQLite error code is returned. Possible error 
codes include SQLITE_CORRUPT (if the changeset buffer is corrupt) or 
SQLITE_NOMEM.
See also lists of
  Objects,
  Constants, and
  Functions.
