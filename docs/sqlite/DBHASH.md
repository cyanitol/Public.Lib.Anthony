The dbhash.exe Utility Program
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
The dbhash.exe Utility Program
1. Overview
The dbhash (or dbhash.exe on Windows) utility is a
command-line program that computes the SHA1 hash of the schema and content 
for an SQLite database.
Dbhash ignores extraneous formatting details and hashes only the database
schema and content.  Hence the hash is constant even if the database file
is modified by:
 VACUUM
 PRAGMA page_size
 PRAGMA journal_mode
 REINDEX
 ANALYZE
 copied via the backup API
 ... and so forth
The operations above can potentially cause vast changes in the raw database
file, and hence cause very different SHA1 hashes at the file level.
But since the content represented in the database file is unchanged by these
operations, the hash computed by dbhash is also unchanged.
Dbhash can be used to compare two databases to confirm that they
are equivalent, even though their representation on disk is quite different.
Dbhash might also be used to verify the content of a remote database without having
to transmit the entire content of the remote database over a slow link.
2. Usage
Dbhash is a command-line utility.
To run it, type "dbhash" on a command-line prompt followed by the names of
one or more SQLite database files that are to be hashed.
The database hashes will be displayed on standard output.
For example:
drh@bella:~/sqlite/bld$ dbhash ~/Fossils/sqlite.fossil
8d3da9ff87196312aaa33076627ccb7943ef79e3 /home/drh/Fossils/sqlite.fossil
Dbhash supports command-line options that can restrict the tables of the
database file that are hashed, or restrict the hash to only content or only
the schema.  Run "dbhash --help" for further information.
3. Building
To build a copy of the dbhash utility program on unix, get a copy of the
canonical SQLite source code and enter:
./configure
make dbhash
On Windows, enter:
nmake /f makefile.msc dbhash.exe
The dbhash program is implemented by a single file of C-code
called dbhash.c.
To build the dbhash program manually, simply compile the dbhash.c source file
and link it against the SQLite library.
This page was last updated on 2025-05-31 13:08:22Z 
