Version Numbers in SQLite
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
Version Numbers in SQLite
1. SQLite Version Numbers
Beginning with version 3.9.0 (2015-10-14) SQLite uses 
semantic versioning.
Prior to that time, SQLite employed a version identifier that
contained between two and four numbers.
1.1. The New Version Numbering System (After 2015-10-14)
All SQLite releases starting with 3.9.0 use a three-number
"semantic version" of the form X.Y.Z.
The first number X is only increased when there is a change that
breaks backward compatibility.  The
current value for X is 3, and the SQLite developers plan to support
the current SQLite database file format, SQL syntax, and C interface
through at least the year 2050.  Hence, one
can expect that all future versions of SQLite for the next several
decades will begin with "3.".
The second number Y is incremented for any change that breaks forward
compatibility by adding new features.
Most future SQLite releases are expected
to increment the second number Y.  The Z is reset to zero whenever Y
is increased.
The third number Z is incremented for releases consisting of only
small changes that implement performance enhancements and/or bug fixes.
The rate of enhancement for SQLite over the previous five years
(2010-2015) is approximately 6 increments of Y per year.  The
numbering format used by for SQLITE_VERSION_NUMBER and
sqlite3_libversion_number() allows versions up to 3.999.999, which is
more than enough for the planned end-of-support date for SQLite
in 2050.  However, the current tarball naming conventions only
reserve two digits for the Y and so the naming format for downloads
will need to be revised in about 2030.
1.2. The Historical Numbering System (Before 2015-10-14)
This historical version numbering system used a two-, three-,
or four-number version:  W.X, W.X.Y, or W.X.Y.Z.
W was the file format: 1 or 2 or 3.
X was the major version.
Y was the minor version.
Z was used only for patch releases to fix bugs.
There have been three historical file formats for SQLite.
SQLite 1.0 through 1.0.32 used the
gdbm library as its storage
engine.
SQLite 2.0.0 through 2.8.17 used a custom b-tree storage engine that
supported only text keys and data.
All modern versions of SQLite (3.0.0 to present) use a b-tree storage
engine that has full support for binary data and Unicode.
This major version number X was historically incremented only for
large and important changes to the code.  What constituted "large
and important" was subjective.  The 3.6.23 to 3.7.0 change
was a result of adding support for WAL mode.
The 3.7.17 to 3.8.0 change was a result of rewrite known as the
next generation query planner.
The minor version number Y was historically incremented for new
features and/or new interfaces that did not significantly change
the structure of the code.  The addition of common table expressions,
partial indexes, and indexes on expressions are all examples of
"minor" changes.  Again, the distinction between "major" and "minor"
is subjective.
The patch level Z was historically only used for bug-fix releases
that changed only a small number of code lines.
1.3. Version History
Chronology
Change log
