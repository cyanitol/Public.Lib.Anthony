How To Download Canonical SQLite Source Code
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
How To Download Canonical SQLite Source Code
1. Introduction
Most programmers compile SQLite into their applications using
the amalgamation.  The amalgamation is C-code but it is not
"source code".  The amalgamation is generated from source code
by scripts.
This document describes how to obtain the canonical source code
for SQLite - the raw source files from which the amalgamation is
built.  See the How To Compile SQLite page for additional information
on what to do with the canonical source code once it is obtained.
2. Direct Downloads
Snapshots of official releases of SQLite source code can often
be obtained directly from the download page of the SQLite website.
Even if the specific version desired is not listed on the download page,
the naming conventions are fairly clear and so programmers can often
guess the name of an historical release and download it that way.
3. Obtaining Code Directly From the Version Control System
For any historical version of SQLite, the source tree can be obtained
from the Fossil version control system,
either downloading a tarball or ZIP archive for a specific version, or
by cloning the entire project history.
SQLite sources are maintained on three geographically dispersed
servers:
https://sqlite.org/src (Dallas)
https://www2.sqlite.org/src (Newark)
https://www3.sqlite.org/src (San Francisco)
The documentation is maintained in separate source repositories on
those same servers:
https://sqlite.org/docsrc (Dallas)
https://www2.sqlite.org/docsrc (Newark)
https://www3.sqlite.org/docsrc (San Francisco)
To download a specific historical version, first locate the specific
version desired by visiting the timeline page on one of these servers
(for example: https://sqlite.org/src/timeline).  If
you know the approximate date of the version you want to download, you
can add a query parameter like "c=YYYY-MM-DD" to the "timeline" URL to
see a timeline centered on that date.  For example, to see all the check-ins
that occurred around August 26, 2013, visit
https://sqlite.org/src/timeline?c=2013-08-26.
If you are looking for an official release, visit the
chronology page, click on the date to the left of the release
you are looking for, and that will take you immediately to the
check-in corresponding to the release.
Once you locate a specific version, click on the hyperlink for that
version to see the "Check-in Information Page".
Then click on either the "Tarball" link or the
"ZIP archive" link to download the complete source tree.
4. Verifying That The Code Is Unmodified
The "manifest" file at the root directory of the source tree
contains either a SHA3-256 hash or a SHA1 hash for every source file
in the repository. The name of the version of the entire source tree
is just the SHA3-256 hash of the "manifest" file itself, possibly with
the last line of that file omitted if the last line begins with 
"# Remove this line". The "manifest.uuid" file should contain
the SHA3-256 hash of the "manifest" file. If all of the above hash 
comparisons are correct, then you can be confident that your source
tree is authentic and unadulterated. Details on the format of
manifest files are available on the 
Fossil website.
The process of checking source code authenticity is automated by the makefile:
make verify-source
Or on windows:
nmake /f Makefile.msc verify-source
Using the makefile to verify source integrity is good for detecting
accidental changes to the source tree, but malicious changes could
be hidden by also modifying the makefiles.
5. Cloning The Complete Development History
To clone the entire history of SQLite, first go to the
https://www.fossil-scm.org/download.html page and grab a precompiled binary
for the Fossil version control program.  Or get the source code on the
same page and compile it yourself.
As of 2017-03-12, you must use Fossil version
2.0 or later for the following instructions to work.  
The SQLite repository started using
artifacts named using SHA3 hashes instead of SHA1 hashes on that date,
and Fossil 2.0 or later is needed in order to understand the new SHA3
hashes.  To find out what version of Fossil you are running, 
type "fossil -v".
Fossil is a completely stand-alone
program, so install it simply by putting the "fossil" or "fossil.exe"
executable someplace on your $PATH or %PATH%.  After you have Fossil
installed, do this:
fossil clone https://sqlite.org/src sqlite.fossil
The command above
will make a copy of the complete development history of
SQLite into the "sqlite.fossil" file on your computer.  Making this copy
takes about a minute and uses about 32 megabytes of transfer.  After
making the copy, "open" the repository by typing:
fossil open sqlite.fossil
This second command will "checkout" the latest check-in from the SQLite
source tree into your current directory.  Subsequently, you can easily switch
to a different version by typing:
fossil update VERSION
Where VERSION can be a branch name (like "trunk" or "session") to get the
latest check-in on a specific branch, or VERSION can be a SHA1 hash or a
prefix of a SHA1 hash for a specific check-in, or VERSION can be a tag
such as "version-3.8.8".  Every time you run "fossil update" it will
automatically reach out to the original repository at
https://sqlite.org/src to obtain new check-ins that might have been
made by others since your previous update.
This page was last updated on 2025-04-16 13:13:29Z 
