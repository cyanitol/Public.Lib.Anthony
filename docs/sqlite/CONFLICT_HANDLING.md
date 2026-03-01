Constraint Conflict Resolution in SQLite
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
Constraint Conflict Resolution in SQLite
In most SQL databases, if you have a UNIQUE, NOT NULL, or
CHECK constraint on
a table and you try to do an UPDATE or INSERT that violates
the constraint, the database will abort the operation in
progress, back out any prior changes associated with the same
UPDATE or INSERT statement, and return an error.
This is the default behavior of SQLite, though SQLite also allows one to
define alternative ways for dealing with constraint violations.
This article describes those alternatives and how to use them.
Conflict Resolution Algorithms
SQLite defines five constraint conflict resolution algorithms
as follows:
ROLLBACK
When a constraint violation occurs, an immediate ROLLBACK
occurs, thus ending the current transaction, and the command aborts
with a return code of SQLITE_CONSTRAINT.  If no transaction is
active (other than the implied transaction that is created on every
command) then this algorithm works the same as ABORT.
ABORT
When a constraint violation occurs, the command backs out
any prior changes it might have made and aborts with a return code
of SQLITE_CONSTRAINT.  But no ROLLBACK is executed so changes
from prior commands within the same transaction
are preserved.  This is the default behavior for SQLite.
FAIL
When a constraint violation occurs, the command aborts with a
return code SQLITE_CONSTRAINT.  But any changes to the database that
the command made prior to encountering the constraint violation
are preserved and are not backed out.  For example, if an UPDATE
statement encountered a constraint violation on the 100th row that
it attempts to update, then the first 99 row changes are preserved
but change to rows 100 and beyond never occur.
IGNORE
When a constraint violation occurs, the one row that contains
the constraint violation is not inserted or changed.  But the command
continues executing normally.  Other rows before and after the row that
contained the constraint violation continue to be inserted or updated
normally.  No error is returned.
REPLACE
When a UNIQUE constraint violation occurs, the pre-existing row
that caused the constraint violation is removed prior to inserting
or updating the current row.  Thus the insert or update always occurs.
The command continues executing normally.  No error is returned.
