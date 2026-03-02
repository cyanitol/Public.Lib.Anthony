Invalid UTF Policy
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
Invalid UTF Policy
1. Garbage In, Garbage Out
With regard to invalid UTF, SQLite follows a policy of
Garbage-In, Garbage-Out (GIGO). If you insert invalid UTF
into an SQLite database, then try to query that data, what you get back out
might not be exactly what you put in.  If you put garbage in, then you
may not complain if you get different garbage back out.
For the purposes of this discussion, "invalid UTF" can mean any of
the following circumstances:
Invalid surrogate pairs in UTF-16.
Invalid multi-byte sequences in UTF-8.
Using more bytes of UTF-8 than necessary to represent a single
code point.  (Example:  encoding 'A' as the two-byte sequence
0xc1, 0x01 instead of just a single 0x41 byte.)
NUL characters (U+0000) embedded in strings.
Invalid sequences of combining characters.
UTF-8 or UTF-16 bytes sequences that encode numbers that are not
defined Unicode characters.
1.1. Invalid UTF will never cause memory errors
If you insert invalid UTF into an SQLite database, then SQLite makes
no guarantees about what text you might get back out.  But it does
promise that invalid UTF will never cause memory errors
(array overruns, reads or writes of uninitialized memory, etc), at
least for the built-in processing of SQLite.
In other words, invalid UTF will not cause SQLite to crash.
This promise only applies to the core SQLite components, not
application-provided extensions, of course.
If an application adds new application-defined SQL functions or
virtual tables or collating sequences or other extensions, and a
database contains invalid UTF, then invalid UTF might get passed
into those extensions.  If the invalid UTF causes one of those
extensions to crash, then that is a problem with the extension,
not with SQLite.
2. No enforcement of text formatting rules
SQLite does not try to enforce UTF formatting rules.  You can
insert invalid UTF into a TEXT field and SQLite will not complain
about this.  It stores the invalid TEXT as best it can.  SQLite
sees its role in the world as a storage engine, not a text format
validation engine.
3. Best effort to preserve text
SQLite does not promise to always preserve invalid UTF, but it does
make an effort.  Generally speaking, if you insert invalid UTF into
SQLite, you will get the exact same byte sequence back out, as long
as you do not ask SQLite to transform the text in any way.
For example, if you insert some UTF-16LE with invalid surrogates into
a TEXT column of a table of a database that has PRAGMA encoding=UTF16LE,
then later query that column using sqlite3_column_text16(), you will 
probably get back the same exact invalid UTF-16.  But if you insert the
same invalid UTF-16LE content in a PRAGMA encoding=UTF8 database,
the content must be converted into UTF8 when it is stored, which could
cause irreversible changes to the content.  Or if you insert that
same invalid UTF-16LE content into a PRAGMA encoding=UTF16LE database
but then read it out using sqlite3_column_text(), then a UTF16 to
UTF8 conversion must occur during the read-out and that conversion might
introduce irreversible changes.
Or, suppose you are doing everything using UTF-8 (the most common case).
Invalid UTF-8 will normally pass through the database without any change
in its byte sequence.  However, if you try to transform the invalid
UTF-8 with SQL functions like substr() or replace()
or if you try to do string matching with the LIKE operator, then
you might get unexpected results.
So, in other words, SQLite does not actively try to subvert your
invalid text.  But when you ask SQLite to make transformations of invalid
UTF, there are no guarantees that those transformations will be reversible
or even sensible.
4. Invalid UTF in the database schema
If a database schema contains names (table names, column names, index
names, and so forth) that are invalid UTF, SQLite will continue to
operate normally.  As far as SQLite is concerned, those names are just
byte sequences.  SQLite does not care whether they are valid UTF or not.
When generating error messages (using, for example, sqlite3_errmsg()),
sometimes SQLite will embed parts of the database schema into the
error message.  If those embedded schema elements
are invalid UTF, then the resulting error message might also be
invalid UTF.
Similarly, the output from the PRAGMA integrity_check and similar
statements will sometimes embed the names of schema elements.  If those
schema element names are invalid UTF, then the output of the command
will also be invalid UTF.
This page was last updated on 2025-05-31 13:08:22Z 
