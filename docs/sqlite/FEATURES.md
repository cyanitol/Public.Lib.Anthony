Features Of SQLite
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
Features Of SQLite
Transactions
    are atomic, consistent, isolated, and durable (ACID)
    even after system crashes and power failures.
Zero-configuration
     - no setup or administration needed.
Full-featured SQL implementation
    with advanced capabilities like partial indexes,
    indexes on expressions, JSON,
    common table expressions, and window functions.
    (Omitted features)
A complete database is stored in a 
    single cross-platform disk file.
    Great for use as an application file format.
Supports terabyte-sized databases and gigabyte-sized strings
    and blobs.  (See limits.html.)
Small code footprint: 
    less than 900KiB fully configured or much less
    with optional features omitted.
Simple, easy to use API.
Fast:  In some cases, SQLite is 
    faster than direct filesystem I/O
Written in ANSI-C.  TCL bindings included.
    Bindings for dozens of other languages available separately.
Well-commented source code with
    100% branch test coverage.
Available as a 
    single ANSI-C source-code file 
    that is easy to compile and hence is easy
    to add into a larger project.
Self-contained:
    no external dependencies.
Cross-platform: Android, *BSD, iOS, Linux, Mac, Solaris, VxWorks, 
    and Windows (Win32, WinCE, WinRT)
    are supported out of the box.  Easy to port to other systems.
Sources are in the public domain.
    Use for any purpose.
Comes with a standalone command-line interface
    (CLI) client that can be used to administer SQLite databases.
Suggested Uses For SQLite:
Database For The Internet Of Things.
SQLite is a popular choice for the database engine in cellphones,
PDAs, MP3 players, set-top boxes, and other electronic gadgets.
SQLite has a small code footprint, makes efficient use of memory,
disk space, and disk bandwidth, is highly reliable, and requires
no maintenance from a Database Administrator.
Application File Format.
Rather than using fopen() to write XML, JSON, CSV,
or some proprietary format into
disk files used by your application, use an SQLite database.
You'll avoid having to write and troubleshoot a parser, your data
will be more easily accessible and cross-platform, and your updates
will be transactional.  (more...)
Website Database.
Because it requires no configuration and stores information in ordinary
disk files, SQLite is a popular choice as the database to back small
to medium-sized websites.
Stand-in For An Enterprise RDBMS.
SQLite is often used as a surrogate for an enterprise RDBMS for
demonstration purposes or for testing.  SQLite is fast and requires
no setup, which takes a lot of the hassle out of testing and which
makes demos perky and easy to launch.
More suggestions...
This page was last updated on 2025-11-13 07:12:58Z 
