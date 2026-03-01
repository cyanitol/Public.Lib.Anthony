File Format Changes in SQLite
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
File Format Changes in SQLite
The underlying file format for SQLite databases does not
change in incompatible ways.  There are literally trillions of
SQLite database files in circulation and the SQLite developers are
committing to supporting those files for decades into the future.
Prior to SQLite version 3.0.0 (2004-06-18), the file format did
sometimes change from one release to the next.  But since that time,
the file format has been fully backwards compatible.
By "backwards compatible" we mean that
newer versions of SQLite can always read and write database files created
by older versions of SQLite.
It is often also the case that SQLite is "forwards compatible", that
older versions of SQLite can read and write database files created by
newer versions of SQLite.  But there are sometimes forward compatibility
breaks.  Sometimes new features are added to the file format.  For
example, WAL mode was added in version 3.7.0 (2010-07-21).  
SQLite 3.7.0 and later can read and write all database files created
by earlier versions of SQLite.  And earlier versions of SQLite can
read and write database files created by SQLite 3.7.0 and later
as long as the database does not use WAL mode.  But versions of
SQLite prior to version 3.7.0 cannot read nor write SQLite database files
that make use of WAL mode.
Summary
Newer versions of SQLite can always read and/or write database files
created by older versions of SQLite, back to version 3.0.0 (2004-06-18).
Older versions of SQLite back to version 3.0.0 can read and write
database files created by newer versions of SQLite as long as the
database does not make use of newer features that are unknown to that
older version.
This page was last updated on 2022-09-13 14:04:46Z 
