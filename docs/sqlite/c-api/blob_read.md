Read Data From A BLOB Incrementally
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
Read Data From A BLOB Incrementally
int sqlite3_blob_read(sqlite3_blob *, void *Z, int N, int iOffset);
This function is used to read data from an open BLOB handle into a
caller-supplied buffer. N bytes of data are copied into buffer Z
from the open BLOB, starting at offset iOffset.
If offset iOffset is less than N bytes from the end of the BLOB,
SQLITE_ERROR is returned and no data is read.  If N or iOffset is
less than zero, SQLITE_ERROR is returned and no data is read.
The size of the blob (and hence the maximum value of N+iOffset)
can be determined using the sqlite3_blob_bytes() interface.
An attempt to read from an expired BLOB handle fails with an
error code of SQLITE_ABORT.
On success, sqlite3_blob_read() returns SQLITE_OK.
Otherwise, an error code or an extended error code is returned.
This routine only works on a BLOB handle which has been created
by a prior successful call to sqlite3_blob_open() and which has not
been closed by sqlite3_blob_close().  Passing any other pointer in
to this routine results in undefined and probably undesirable behavior.
See also: sqlite3_blob_write().
See also lists of
  Objects,
  Constants, and
  Functions.
