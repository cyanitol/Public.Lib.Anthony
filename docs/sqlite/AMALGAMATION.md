The SQLite Amalgamation
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
The SQLite Amalgamation
1. Executive Summary
Over 100 separate source files are concatenated into a
single large file of C-code named "sqlite3.c" and
referred to as "the amalgamation". The amalgamation
contains everything an application needs to embed SQLite.
Combining all the code for SQLite into one big file makes SQLite
easier to deploy &mdash; there is just one file to keep track of.
And because all code is in
a single translation unit, compilers can do
better inter-procedure and inlining optimization
resulting in machine code that is between 5% and 10% faster.
2. The SQLite Amalgamation
The SQLite library consists of 111 files of C code
(as of Version 3.37.0 - 2021-11-27)
in the core with 22 additional files that
implement certain commonly used extensions.
Of the 133
main source files, about 75% are C code and about 25% are C header files.
Most of these are "source" files in the sense that they are stored
in the SQLite version control system
and are edited manually in an ordinary text editor.
But some of the C-language files are generated using scripts
or auxiliary programs.  For example, the
parse.y
file contains an LALR(1) grammar of the SQL language which is compiled,
by the Lemon parser generator, to produce a parser contained in the file
"parse.c" accompanied by token identifiers in "parse.h".
The makefiles for SQLite have an "sqlite3.c" target for building the
amalgamation, to contain all C code for the core SQLite library and the
FTS3, FTS5, RTREE, DBSTAT, JSON1,
RBU and SESSION
extensions.
This file contains about 238K lines of code
(or 145K if you omit blank lines and comments) and is over 8.4 megabytes
in size (as of 2021-12-29).
Though the various extensions are included in the
"sqlite3.c" amalgamation file, they are disabled using #ifdef statements.
Activate the extensions using compile-time options like:
 -DSQLITE_ENABLE_FTS3
 -DSQLITE_ENABLE_FTS5
 -DSQLITE_ENABLE_RTREE
 -DSQLITE_ENABLE_DBSTAT_VTAB
 -DSQLITE_ENABLE_RBU
 -DSQLITE_ENABLE_SESSION
The amalgamation contains everything you need to integrate SQLite
into a larger project.  Just copy the amalgamation into your source
directory and compile it along with the other C code files in your project.
(A more detailed discussion of the compilation process is
available.)
You may also want to make use of the "sqlite3.h" header file that
defines the programming API for SQLite.
The sqlite3.h header file is available separately.
The sqlite3.h file is also contained within the amalgamation, in
the first few thousand lines. So if you have a copy of
sqlite3.c but cannot seem to locate sqlite3.h, you can always
regenerate the sqlite3.h by copying and pasting from the amalgamation.
In addition to making SQLite easier to incorporate into other
projects, the amalgamation also makes it run faster. Many
compilers are able to do additional optimizations on code when
it is contained with in a single translation unit such as it
is in the amalgamation. We have measured performance improvements
of between 5 and 10% when we use the amalgamation to compile
SQLite rather than individual source files.  The downside of this
is that the additional optimizations often take the form of
function inlining which tends to make the size of the resulting
binary image larger.
3. The Split Amalgamation
Developers sometimes experience trouble debugging the
quarter-million line amalgamation source file because some debuggers
are only able to handle source code line numbers less than 32,768.
The amalgamation source code runs fine.  One just cannot single-step
through it in a debugger.
To circumvent this limitation, the amalgamation is also available in
a split form, consisting of files "sqlite3-1.c", "sqlite3-2.c", and
so forth, where each file is less than 32,768 lines in length and
where the concatenation of the files contain all of the code for the
complete amalgamation.  Then there is a separate source file named
"sqlite3-all.c" which basically consists of code like this:
#include "sqlite3-1.c"
#include "sqlite3-2.c"
#include "sqlite3-3.c"
#include "sqlite3-4.c"
#include "sqlite3-5.c"
#include "sqlite3-6.c"
#include "sqlite3-7.c"
Applications using the split amalgamation simply compile against
"sqlite3-all.c" instead of "sqlite3.c".  The two files work exactly
the same.  But with "sqlite3-all.c", no single source file contains more
than 32,767 lines of code, and so it is more convenient to use some
debuggers.  The downside of the split amalgamation is that it consists
of 6 C source code files instead of just 1.
4. Download Copies Of The Precompiled Amalgamation
The amalgamation and
the sqlite3.h header file are available on
the download page as a file
named sqlite-amalgamation-X.zip
where the X is replaced by the appropriate version number.
The download page also usually
contains a tarball named "sqlite-autoconf-X.tar.gz" that contains
both the amalgamation source files and a ./configure script and
Makefile sufficient to build the SQLite library and the CLI.
5. Building The Amalgamation From Canonical Source Code
To build the amalgamation (either the full amalgamation or the
split amalgamation), first
get the canonical source code from one of the three servers.
Then, on unix-like systems run:
./configure && make sqlite3.c
To build on Windows using Microsoft Visual C++, run this command:
nmake /f makefile.msc sqlite3.c
In both cases, the split amalgamation can be obtained by
substituting "sqlite3-all.c" for "sqlite3.c" as the make target.
5.1. Dependencies
The build process makes extensive use of the
Tcl scripting language.  You will need to have a
copy of TCL installed in order for the make targets above to work.
Installing TCL is not difficult and does not even require admin privilege.
Step-by-step instructions on how to build SQLite from canonical sources,
including instructions on how to install TCL,
are provided in the source tree in the following documents:
 https://sqlite.org/src/doc/trunk/doc/compile-for-unix.md
 https://sqlite.org/src/doc/trunk/doc/compile-for-windows.md
In the links above, you can replace the "trunk" path component with
a different symbol or a date to find older versions of the document.
For example, to find the compile-for-unix.md document that was valid
for SQLite version-3.47.0, use a URL like
"https://sqlite.org/src/doc/version-3.47.0/doc/compile-for-unix.md".
Or to get the version of compile-for-windows.md that was valid on
2024-10-18, use a URL like this:
"https://sqlite.org/src/doc/20241018/doc/compile-for-windows.md".
5.2. See Also
Additional notes on compiling SQLite can be found on the
How To Compile SQLite page.
This page was last updated on 2025-04-16 13:13:29Z 
