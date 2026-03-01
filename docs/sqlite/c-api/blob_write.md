Write Data Into A BLOB Incrementally
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
Write Data Into A BLOB Incrementally
int sqlite3_blob_write(sqlite3_blob *, const void *z, int n, int iOffset);
This function is used to write data into an open BLOB handle from a
caller-supplied buffer. N bytes of data are copied from the buffer Z
into the open BLOB, starting at offset iOffset.
On success, sqlite3_blob_write() returns SQLITE_OK.
Otherwise, an  error code or an extended error code is returned.
Unless SQLITE_MISUSE is returned, this function sets the
database connection error code and message accessible via
sqlite3_errcode() and sqlite3_errmsg() and related functions.
If the BLOB handle passed as the first argument was not opened for
writing (the flags parameter to sqlite3_blob_open() was zero),
this function returns SQLITE_READONLY.
This function may only modify the contents of the BLOB; it is
not possible to increase the size of a BLOB using this API.
If offset iOffset is less than N bytes from the end of the BLOB,
SQLITE_ERROR is returned and no data is written. The size of the
BLOB (and hence the maximum value of N+iOffset) can be determined
using the sqlite3_blob_bytes() interface. If N or iOffset are less
than zero SQLITE_ERROR is returned and no data is written.
An attempt to write to an expired BLOB handle fails with an
error code of SQLITE_ABORT.  Writes to the BLOB that occurred
before the BLOB handle expired are not rolled back by the
expiration of the handle, though of course those changes might
have been overwritten by the statement that expired the BLOB handle
or by other independent statements.
This routine only works on a BLOB handle which has been created
by a prior successful call to sqlite3_blob_open() and which has not
been closed by sqlite3_blob_close().  Passing any other pointer in
to this routine results in undefined and probably undesirable behavior.
See also: sqlite3_blob_read().
See also lists of
  Objects,
  Constants, and
  Functions.
