Define New Collating Sequences
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
SQLite C Interface
Define New Collating Sequences
int sqlite3_create_collation(
  sqlite3*,
  const char *zName,
  int eTextRep,
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*)
);
int sqlite3_create_collation_v2(
  sqlite3*,
  const char *zName,
  int eTextRep,
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*),
  void(*xDestroy)(void*)
);
int sqlite3_create_collation16(
  sqlite3*,
  const void *zName,
  int eTextRep,
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*)
);
These functions add, remove, or modify a collation associated
with the database connection specified as the first argument.
The name of the collation is a UTF-8 string
for sqlite3_create_collation() and sqlite3_create_collation_v2()
and a UTF-16 string in native byte order for sqlite3_create_collation16().
Collation names that compare equal according to sqlite3_strnicmp() are
considered to be the same name.
The third argument (eTextRep) must be one of the constants:
 SQLITE_UTF8,
 SQLITE_UTF16LE,
 SQLITE_UTF16BE,
 SQLITE_UTF16, or
 SQLITE_UTF16_ALIGNED.
The eTextRep argument determines the encoding of strings passed
to the collating function callback, xCompare.
The SQLITE_UTF16 and SQLITE_UTF16_ALIGNED values for eTextRep
force strings to be UTF16 with native byte order.
The SQLITE_UTF16_ALIGNED value for eTextRep forces strings to begin
on an even byte address.
The fourth argument, pArg, is an application data pointer that is passed
through as the first argument to the collating function callback.
The fifth argument, xCompare, is a pointer to the collating function.
Multiple collating functions can be registered using the same name but
with different eTextRep parameters and SQLite will use whichever
function requires the least amount of data transformation.
If the xCompare argument is NULL then the collating function is
deleted.  When all collating functions having the same name are deleted,
that collation is no longer usable.
The collating function callback is invoked with a copy of the pArg
application data pointer and with two strings in the encoding specified
by the eTextRep argument.  The two integer parameters to the collating
function callback are the length of the two strings, in bytes. The collating
function must return an integer that is negative, zero, or positive
if the first string is less than, equal to, or greater than the second,
respectively.  A collating function must always return the same answer
given the same inputs.  If two or more collating functions are registered
to the same collation name (using different eTextRep values) then all
must give an equivalent answer when invoked with equivalent strings.
The collating function must obey the following properties for all
strings A, B, and C:
 If A==B then B==A.
 If A==B and B==C then A==C.
 If A<B THEN B>A.
 If A<B and B<C then A<C.
If a collating function fails any of the above constraints and that
collating function is registered and used, then the behavior of SQLite
is undefined.
The sqlite3_create_collation_v2() works like sqlite3_create_collation()
with the addition that the xDestroy callback is invoked on pArg when
the collating function is deleted.
Collating functions are deleted when they are overridden by later
calls to the collation creation functions or when the
database connection is closed using sqlite3_close().
The xDestroy callback is not called if the
sqlite3_create_collation_v2() function fails.  Applications that invoke
sqlite3_create_collation_v2() with a non-NULL xDestroy argument should
check the return code and dispose of the application data pointer
themselves rather than expecting SQLite to deal with it for them.
This is different from every other SQLite interface.  The inconsistency
is unfortunate but cannot be changed without breaking backwards
compatibility.
See also:  sqlite3_collation_needed() and sqlite3_collation_needed16().
See also lists of
  Objects,
  Constants, and
  Functions.
