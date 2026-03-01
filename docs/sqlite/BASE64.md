The Base64() SQL Function
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
The Base64() SQL Function
1. Overview
 The base64() function is an SQL function implemented as 
a loadable extension for SQLite.  The function converts a binary
BLOB into equivalent RFC 4648
text or converts RFC 4648 text into the equivalent BLOB.
 The base64() function is not a standard part of SQLite.
It must be loaded as a separate extension.  The source code to
base64() is in the 
base64.c source file
in the ext/misc/ folder of the
SQLite source tree.
 The base64() function is not included in standard builds of
the SQLite library, but it is loaded by default in the CLI.  This
is typical of the CLI which loads various extensions above and beyond
what are available in the standard SQLite library.
2. Features
The base64() function always takes a single argument.
If the argument to base64() is a BLOB, then the return value is TEXT
that is the RFC 4648 encoding
of that BLOB.
If the argument to base64() is base64 TEXT then the return value is
a BLOB that is the binary data corresponding to that base64 TEXT.
If the argument to base64() is NULL, then NULL is returned.
An error is raised if the argument to base64() is something other than
TEXT, BLOB, or NULL.
If the argument is TEXT, leading and trailing whitespace is ignored.
If the argument is TEXT that has a prefix that looks like base64 but contains
non-base64 characters, then as much of the input as possible is translated into
a BLOB and that BLOB is returned.
The base64() function uses the standard
RFC 4648 alphabet:
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".
3. See Also
Build-in SQL functions hex() and unhex().
Extension function base85().
