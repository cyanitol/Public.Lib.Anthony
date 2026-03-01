The INDEXED BY Clause
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
The INDEXED BY Clause
1. How INDEXED BY Works
The INDEXED BY phrase forces the SQLite query planner to use a
particular named index on a DELETE, SELECT, or UPDATE statement.
The INDEXED BY phrase is an SQLite extension and
is not portable to other SQL database engines.
qualified-table-name:
hide
schema-name
.
table-name
AS
alias
INDEXED
BY
index-name
NOT
INDEXED
The "INDEXED BY index-name" phrase specifies 
that the named index
must be used in order to look up values on the preceding table.
If index-name does not exist or cannot be used 
for the query, then the preparation of the SQL statement fails.
The "NOT INDEXED" clause specifies that no index shall be used when
accessing the preceding table, including implied indices create by
UNIQUE and PRIMARY KEY constraints.  However, the rowid
can still be used to look up entries even when "NOT INDEXED" is specified.
Some SQL database engines provide non-standard "hint" mechanisms which
can be used to give the query optimizer clues about what indices it should
use for a particular statement.  The INDEXED BY clause of SQLite is 
not a hinting mechanism and it should not be used as such.
The INDEXED BY clause does not give the optimizer hints about which index
to use; it gives the optimizer a requirement of which index to use.
If the query optimizer is unable to use the index specified by the
INDEXED BY clause, then the query will fail with an error.
The INDEXED BY clause is not intended for use in tuning
the performance of a query.  The intent of the INDEXED BY clause is
to raise a run-time error if a schema change, such as dropping or
creating an index, causes the query plan for a time-sensitive query
to change.  The INDEXED BY clause is designed to help detect
undesirable query plan changes during regression testing.
Application 
developers are admonished to omit all use of INDEXED BY during
application design, implementation, testing, and tuning.  If
INDEXED BY is to be used at all, it should be inserted at the very
end of the development process when "locking down" a design.
2. See Also
The query planner checklist describes steps that application
developers should follow to resolve query planner problems.
Notice the that the use of INDEXED BY is a last resort, to be used only
when all other measures fail.
The unary "+" operator
can be used to disqualify terms in the WHERE clause from use by indices.
Careful use of unary + can sometimes help prevent the query planner from
choosing a poor index without restricting it to using one specific index.
Careful placement of unary + operators is a better method for controlling 
which indices are used by a query.
The sqlite3_stmt_status() C/C++ interface together with the
SQLITE_STMTSTATUS_FULLSCAN_STEP and SQLITE_STMTSTATUS_SORT verbs
can be used to detect at run-time when an SQL statement is not
making effective use of indices.  Many applications may prefer to
use the sqlite3_stmt_status() interface to detect index misuse
rather than the INDEXED BY phrase described here.
This page was last updated on 2025-07-12 15:11:36Z 
