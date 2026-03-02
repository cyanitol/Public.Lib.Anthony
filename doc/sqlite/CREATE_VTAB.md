CREATE VIRTUAL TABLE
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
CREATE VIRTUAL TABLE
create-virtual-table-stmt:
hide
CREATE
VIRTUAL
TABLE
IF
NOT
EXISTS
schema-name
.
table-name
USING
module-name
(
module-argument
)
,
A virtual table is an interface to an external storage or computation
engine that appears to be a table but does not actually store information
in the database file.
In general, you can do anything with a virtual table that can be done
with an ordinary table, except that you cannot create indices or triggers on a
virtual table.  Some virtual table implementations might impose additional
restrictions.  For example, many virtual tables are read-only.
The module-name is the name of an object that implements
the virtual table.  The module-name must be registered with
the SQLite database connection using
sqlite3_create_module() or sqlite3_create_module_v2()
prior to issuing the CREATE VIRTUAL TABLE statement.
The module takes zero or more comma-separated arguments.
The arguments can be just about any text as long as it has balanced
parentheses.  The argument syntax is sufficiently general that the
arguments can be made to appear as column definitions in a traditional
CREATE TABLE statement.  
SQLite passes the module arguments directly
to the xCreate and xConnect methods of the module implementation
without any interpretation.  It is the responsibility
of the module implementation to parse and interpret its own arguments.
A virtual table is destroyed using the ordinary
DROP TABLE statement.  There is no
DROP VIRTUAL TABLE statement.
