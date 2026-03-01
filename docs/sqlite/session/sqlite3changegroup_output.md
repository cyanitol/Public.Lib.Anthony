Obtain A Composite Changeset From A Changegroup
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
Session Module C InterfaceObtain A Composite Changeset From A Changegroupint sqlite3changegroup_output(
  sqlite3_changegroup*,
  int *pnData,                    /* OUT: Size of output buffer in bytes */
  void **ppData                   /* OUT: Pointer to output buffer */
);
Obtain a buffer containing a changeset (or patchset) representing the
current contents of the changegroup. If the inputs to the changegroup
were themselves changesets, the output is a changeset. Or, if the
inputs were patchsets, the output is also a patchset.
As with the output of the sqlite3session_changeset() and
sqlite3session_patchset() functions, all changes related to a single
table are grouped together in the output of this function. Tables appear
in the same order as for the very first changeset added to the changegroup.
If the second or subsequent changesets added to the changegroup contain
changes for tables that do not appear in the first changeset, they are
appended onto the end of the output changeset, again in the order in
which they are first encountered.
If an error occurs, an SQLite error code is returned and the output
variables (*pnData) and (*ppData) are set to 0. Otherwise, SQLITE_OK
is returned and the output variables are set to the size of and a 
pointer to the output buffer, respectively. In this case it is the
responsibility of the caller to eventually free the buffer using a
call to sqlite3_free().
See also lists of
  Objects,
  Constants, and
  Functions.
