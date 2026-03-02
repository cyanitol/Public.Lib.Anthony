SQLite Library Footprint
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
Size Of The SQLite Library
As of 2023-07-04, the size of SQLite library is generally less than
1 megabyte.  The size varies by compiler, operating system,
CPU architecture, compile-time options, and other factors.  When
compiling using -Os (optimize for size) and with no other compile
time-options specified, here are a few examples from commonly used
platforms:
 gcc 10.2.1 on Raspberry PI 4 64-bit ARM:  590 KB.
 clang 14.0.0 on MacOS M1: 750 KB.
 gcc 5.4.0 on Ubuntu 16.04.7 x64: 650 KB
 gcc 9.4.0 on Ubuntu 20.04.5 x64: 650 KB
Your mileage may vary.
Library size will likely be larger
when including optional features such as full-text search or r-tree indexes,
or when using more aggressive compiler options such as -O3.
This document is intended only as a general guideline to the
compiled size of the SQLite library.  If you need exact numbers, please
make your own measurements using your specific combination of SQLite
source code version, compiler, target platform, and compile-time options.
This page was last updated on 2023-07-30 10:50:18Z 
