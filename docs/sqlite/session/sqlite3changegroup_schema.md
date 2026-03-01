Add a Schema to a Changegroup
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
Session Module C InterfaceAdd a Schema to a Changegroupint sqlite3changegroup_schema(sqlite3_changegroup*, sqlite3*, const char *zDb);
This method may be used to optionally enforce the rule that the changesets
added to the changegroup handle must match the schema of database zDb
("main", "temp", or the name of an attached database). If
sqlite3changegroup_add() is called to add a changeset that is not compatible
with the configured schema, SQLITE_SCHEMA is returned and the changegroup
object is left in an undefined state.
A changeset schema is considered compatible with the database schema in
the same way as for sqlite3changeset_apply(). Specifically, for each
table in the changeset, there exists a database table with:
   The name identified by the changeset, and
   at least as many columns as recorded in the changeset, and
   the primary key columns in the same position as recorded in 
       the changeset.
The output of the changegroup object always has the same schema as the
database nominated using this function. In cases where changesets passed
to sqlite3changegroup_add() have fewer columns than the corresponding table
in the database schema, these are filled in using the default column
values from the database schema. This makes it possible to combined 
changesets that have different numbers of columns for a single table
within a changegroup, provided that they are otherwise compatible.
See also lists of
  Objects,
  Constants, and
  Functions.
