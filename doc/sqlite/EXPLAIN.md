EXPLAIN
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
EXPLAIN
1. Syntax
sql-stmt:
EXPLAIN
QUERY
PLAN
alter-table-stmt
analyze-stmt
attach-stmt
begin-stmt
commit-stmt
create-index-stmt
create-table-stmt
create-trigger-stmt
create-view-stmt
create-virtual-table-stmt
delete-stmt
delete-stmt-limited
detach-stmt
drop-index-stmt
drop-table-stmt
drop-trigger-stmt
drop-view-stmt
insert-stmt
pragma-stmt
reindex-stmt
release-stmt
rollback-stmt
savepoint-stmt
select-stmt
update-stmt
update-stmt-limited
vacuum-stmt
2. Description
An SQL statement can be preceded by the keyword "EXPLAIN" or
by the phrase "EXPLAIN QUERY PLAN".  Either modification causes the
SQL statement to behave as a query and to return information about
how the SQL statement would have operated if the EXPLAIN keyword or
phrase had been omitted.
The output from EXPLAIN and EXPLAIN QUERY PLAN is intended for
interactive analysis and troubleshooting only.  The details of the 
output format are subject to change from one release of SQLite to the next.
Applications should not use EXPLAIN or EXPLAIN QUERY PLAN since
their exact behavior is variable and only partially documented.
When the EXPLAIN keyword appears by itself it causes the statement
to behave as a query that returns the sequence of 
virtual machine instructions it would have used to execute the command had
the EXPLAIN keyword not been present. When the EXPLAIN QUERY PLAN phrase
appears, the statement returns high-level information regarding the query
plan that would have been used.
The EXPLAIN QUERY PLAN command is described in 
more detail here.
2.1. EXPLAIN operates at run-time, not at prepare-time
The EXPLAIN and EXPLAIN QUERY PLAN prefixes affect the behavior of
running a prepared statement using sqlite3_step().  The process of
generating a new prepared statement using sqlite3_prepare() or similar
is (mostly) unaffected by EXPLAIN.  (The exception to the previous sentence
is that some special opcodes used by EXPLAIN QUERY PLAN are omitted when
building an EXPLAIN QUERY PLAN prepared statement, as a performance
optimization.)
This means that actions that occur during sqlite3_prepare() are
unaffected by EXPLAIN.
Some PRAGMA statements do their work during sqlite3_prepare() rather
than during sqlite3_step().  Those PRAGMA statements are unaffected
by EXPLAIN.  They operate the same with or without the EXPLAIN prefix.
The set of PRAGMA statements that are unaffected by EXPLAIN can vary
from one release to the next.  Some PRAGMA statements operate during
sqlite3_prepare() depending on their arguments.  For consistent
results, avoid using EXPLAIN on PRAGMA statements.
The authorizer callback is invoked regardless of the presence of
EXPLAIN or EXPLAIN QUERY PLAN.
