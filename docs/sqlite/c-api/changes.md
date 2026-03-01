Count The Number Of Rows Modified
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
Count The Number Of Rows Modified
int sqlite3_changes(sqlite3*);
sqlite3_int64 sqlite3_changes64(sqlite3*);
These functions return the number of rows modified, inserted or
deleted by the most recently completed INSERT, UPDATE or DELETE
statement on the database connection specified by the only parameter.
The two functions are identical except for the type of the return value
and that if the number of rows modified by the most recent INSERT, UPDATE,
or DELETE is greater than the maximum value supported by type "int", then
the return value of sqlite3_changes() is undefined. Executing any other
type of SQL statement does not modify the value returned by these functions.
For the purposes of this interface, a CREATE TABLE AS SELECT statement
does not count as an INSERT, UPDATE or DELETE statement and hence the rows
added to the new table by the CREATE TABLE AS SELECT statement are not
counted.
Only changes made directly by the INSERT, UPDATE or DELETE statement are
considered - auxiliary changes caused by triggers,
foreign key actions or REPLACE constraint resolution are not counted.
Changes to a view that are intercepted by
INSTEAD OF triggers are not counted. The value
returned by sqlite3_changes() immediately after an INSERT, UPDATE or
DELETE statement run on a view is always zero. Only changes made to real
tables are counted.
Things are more complicated if the sqlite3_changes() function is
executed while a trigger program is running. This may happen if the
program uses the changes() SQL function, or if some other callback
function invokes sqlite3_changes() directly. Essentially:
 Before entering a trigger program the value returned by
sqlite3_changes() function is saved. After the trigger program
has finished, the original value is restored.
 Within a trigger program each INSERT, UPDATE and DELETE
statement sets the value returned by sqlite3_changes()
upon completion as normal. Of course, this value will not include
any changes performed by sub-triggers, as the sqlite3_changes()
value will be saved and restored after each sub-trigger has run.
This means that if the changes() SQL function (or similar) is used
by the first INSERT, UPDATE or DELETE statement within a trigger, it
returns the value as set when the calling statement began executing.
If it is used by the second or subsequent such statement within a trigger
program, the value returned reflects the number of rows modified by the
previous INSERT, UPDATE or DELETE statement within the same trigger.
If a separate thread makes changes on the same database connection
while sqlite3_changes() is running then the value returned
is unpredictable and not meaningful.
See also:
 the sqlite3_total_changes() interface
 the count_changes pragma
 the changes() SQL function
 the data_version pragma
See also lists of
  Objects,
  Constants, and
  Functions.
