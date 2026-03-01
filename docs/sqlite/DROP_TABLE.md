DROP TABLE
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
DROP TABLE
drop-table-stmt:
hide
DROP
TABLE
IF
EXISTS
schema-name
.
table-name
The DROP TABLE statement removes a table added with the
CREATE TABLE statement.  The name specified is the
table name.  The dropped table is completely removed from the database 
schema and the disk file.  The table can not be recovered.  
All indices and triggers
associated with the table are also deleted.
The optional IF EXISTS clause suppresses the error that would normally
result if the table does not exist.
If foreign key constraints are enabled, a DROP TABLE command performs an
implicit DELETE FROM command before removing the
table from the database schema. Any triggers attached to the table are
dropped from the database schema before the implicit DELETE FROM
is executed, so this cannot cause any triggers to fire. By contrast, an
implicit DELETE FROM does cause any configured
foreign key actions to take place. 
If the implicit DELETE FROM executed
as part of a DROP TABLE command violates any immediate foreign key constraints,
an error is returned and the table is not dropped. If 
the implicit DELETE FROM causes any 
deferred foreign key constraints to be violated, and the violations still
exist when the transaction is committed, an error is returned at the time
of commit.
