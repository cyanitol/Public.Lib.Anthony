Total Number Of Rows Modified
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
Total Number Of Rows Modified
int sqlite3_total_changes(sqlite3*);
sqlite3_int64 sqlite3_total_changes64(sqlite3*);
These functions return the total number of rows inserted, modified or
deleted by all INSERT, UPDATE or DELETE statements completed
since the database connection was opened, including those executed as
part of trigger programs. The two functions are identical except for the
type of the return value and that if the number of rows modified by the
connection exceeds the maximum value supported by type "int", then
the return value of sqlite3_total_changes() is undefined. Executing
any other type of SQL statement does not affect the value returned by
sqlite3_total_changes().
Changes made as part of foreign key actions are included in the
count, but those made as part of REPLACE constraint resolution are
not. Changes to a view that are intercepted by INSTEAD OF triggers
are not counted.
The sqlite3_total_changes(D) interface only reports the number
of rows that changed due to SQL statement run against database
connection D.  Any changes by other database connections are ignored.
To detect changes against a database file from other database
connections use the PRAGMA data_version command or the
SQLITE_FCNTL_DATA_VERSION file control.
If a separate thread makes changes on the same database connection
while sqlite3_total_changes() is running then the value
returned is unpredictable and not meaningful.
See also:
 the sqlite3_changes() interface
 the count_changes pragma
 the changes() SQL function
 the data_version pragma
 the SQLITE_FCNTL_DATA_VERSION file control
See also lists of
  Objects,
  Constants, and
  Functions.
