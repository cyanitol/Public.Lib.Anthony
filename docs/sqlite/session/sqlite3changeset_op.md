Obtain The Current Operation From A Changeset Iterator
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
Session Module C InterfaceObtain The Current Operation From A Changeset Iteratorint sqlite3changeset_op(
  sqlite3_changeset_iter *pIter,  /* Iterator object */
  const char **pzTab,             /* OUT: Pointer to table name */
  int *pnCol,                     /* OUT: Number of columns in table */
  int *pOp,                       /* OUT: SQLITE_INSERT, DELETE or UPDATE */
  int *pbIndirect                 /* OUT: True for an 'indirect' change */
);
The pIter argument passed to this function may either be an iterator
passed to a conflict-handler by sqlite3changeset_apply(), or an iterator
created by sqlite3changeset_start(). In the latter case, the most recent
call to sqlite3changeset_next() must have returned SQLITE_ROW. If this
is not the case, this function returns SQLITE_MISUSE.
Arguments pOp, pnCol and pzTab may not be NULL. Upon return, three
outputs are set through these pointers: 
*pOp is set to one of SQLITE_INSERT, SQLITE_DELETE or SQLITE_UPDATE,
depending on the type of change that the iterator currently points to;
*pnCol is set to the number of columns in the table affected by the change; and
*pzTab is set to point to a nul-terminated utf-8 encoded string containing
the name of the table affected by the current change. The buffer remains
valid until either sqlite3changeset_next() is called on the iterator
or until the conflict-handler function returns.
If pbIndirect is not NULL, then *pbIndirect is set to true (1) if the change
is an indirect change, or false (0) otherwise. See the documentation for
sqlite3session_indirect() for a description of direct and indirect
changes.
If no error occurs, SQLITE_OK is returned. If an error does occur, an
SQLite error code is returned. The values of the output variables may not
be trusted in this case.
See also lists of
  Objects,
  Constants, and
  Functions.
