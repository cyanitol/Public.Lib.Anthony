The Base85() SQL Function
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
The Base85() SQL Function
1. Overview
 The base85() function is an SQL function implemented as 
a loadable extension for SQLite.  The function converts a binary
BLOB into an equivalent base-85 text encoding
or converts that same base-85 text encoding back into a BLOB.
 The base85() function is not a standard part of SQLite.
It must be loaded as a separate extension.  The source code to
base85() is in the 
base85.c source file
in the ext/misc/ folder of the
SQLite source tree.
 The base85() function is not included in standard builds of
the SQLite library, but it is loaded by default in the CLI.  This
is typical of the CLI which loads various extensions above and beyond
what are available in the standard SQLite library.
Like base64() representations, the base85() function can be used
to format binary content through any
sane ASCII channel unmolested. It also plays nicely in CSV or
written as TCL brace-enclosed literals or SQL string literals.
It is not suited for unmodified use in XML-like documents.
2. Encoding
The encoding used resembles Ascii85, but was devised by the author
(Larry Brasfield) before Mozilla, Adobe, ZMODEM or other Ascii85
variant sources existed, in the 1984 timeframe on a VAX mainframe.
Further, this is an independent implementation of a base85 system.
Hence, the author has rightfully put this into the public domain.
Base85 numerals are taken from the set of 7-bit USASCII codes,
excluding control characters and Space ! " ' ( ) { | } ~ Del
in code order representing digit values 0 to 84 (base 10.)
Groups of 4 bytes, interpreted as big-endian 32-bit values,
are represented as 5-digit base85 numbers with MS to LS digit
order. Groups of 1-3 bytes are represented with 2-4 digits,
still big-endian but 8-24 bit values. (Using big-endian yields
the simplest transition to byte groups smaller than 4 bytes.
These byte groups can also be considered base-256 numbers.)
Groups of 0 bytes are represented with 0 digits and vice-versa.
No pad characters are used; encoded base85 numeral sequence
(aka "group") length maps 1-to-1 to the decoded binary length.
Any character not in the base85 numeral set delimits groups.
When base85 is streamed or stored in containers of indefinite
size, newline is used to separate it into sub-sequences of no
more than 80 digits so that fgets() can be used to read it.
3. Features
The base85() function always takes a single argument that must be TEXT
(for base85-to-binary conversion) or a BLOB (for binary-to-base85 conversion).
If the argument to base85() is a BLOB, then the return value is TEXT
according to the encoding described above.
If the argument to base85() is TEXT as described above then the return value is
a BLOB that is the binary data corresponding to that base85 text.
An error is raised if the argument to base85() is something other than
TEXT or BLOB.
4. See Also
Build-in SQL function hex().
Extension function base64().
This page was last updated on 2025-07-12 15:11:36Z 
