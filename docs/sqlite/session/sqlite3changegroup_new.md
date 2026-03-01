Create A New Changegroup Object
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
Session Module C InterfaceCreate A New Changegroup Objectint sqlite3changegroup_new(sqlite3_changegroup **pp);
An sqlite3_changegroup object is used to combine two or more changesets
(or patchsets) into a single changeset (or patchset). A single changegroup
object may combine changesets or patchsets, but not both. The output is
always in the same format as the input.
If successful, this function returns SQLITE_OK and populates (*pp) with
a pointer to a new sqlite3_changegroup object before returning. The caller
should eventually free the returned object using a call to 
sqlite3changegroup_delete(). If an error occurs, an SQLite error code
(i.e. SQLITE_NOMEM) is returned and *pp is set to NULL.
The usual usage pattern for an sqlite3_changegroup object is as follows:
   It is created using a call to sqlite3changegroup_new().
   Zero or more changesets (or patchsets) are added to the object
       by calling sqlite3changegroup_add().
   The result of combining all input changesets together is obtained 
       by the application via a call to sqlite3changegroup_output().
   The object is deleted using a call to sqlite3changegroup_delete().
Any number of calls to add() and output() may be made between the calls to
new() and delete(), and in any order.
As well as the regular sqlite3changegroup_add() and 
sqlite3changegroup_output() functions, also available are the streaming
versions sqlite3changegroup_add_strm() and sqlite3changegroup_output_strm().
See also lists of
  Objects,
  Constants, and
  Functions.
