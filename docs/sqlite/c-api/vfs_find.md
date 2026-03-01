Virtual File System Objects
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
Virtual File System Objects
sqlite3_vfs *sqlite3_vfs_find(const char *zVfsName);
int sqlite3_vfs_register(sqlite3_vfs*, int makeDflt);
int sqlite3_vfs_unregister(sqlite3_vfs*);
A virtual filesystem (VFS) is an sqlite3_vfs object
that SQLite uses to interact
with the underlying operating system.  Most SQLite builds come with a
single default VFS that is appropriate for the host computer.
New VFSes can be registered and existing VFSes can be unregistered.
The following interfaces are provided.
The sqlite3_vfs_find() interface returns a pointer to a VFS given its name.
Names are case sensitive.
Names are zero-terminated UTF-8 strings.
If there is no match, a NULL pointer is returned.
If zVfsName is NULL then the default VFS is returned.
New VFSes are registered with sqlite3_vfs_register().
Each new VFS becomes the default VFS if the makeDflt flag is set.
The same VFS can be registered multiple times without injury.
To make an existing VFS into the default VFS, register it again
with the makeDflt flag set.  If two different VFSes with the
same name are registered, the behavior is undefined.  If a
VFS is registered with a name that is NULL or an empty string,
then the behavior is undefined.
Unregister a VFS with the sqlite3_vfs_unregister() interface.
If the default VFS is unregistered, another VFS is chosen as
the default.  The choice for the new VFS is arbitrary.
See also lists of
  Objects,
  Constants, and
  Functions.
