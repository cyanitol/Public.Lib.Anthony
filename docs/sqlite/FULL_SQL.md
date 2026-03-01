Full-Featured SQL
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
Full-Featured SQL
Do not be misled by the "Lite" in the name.  SQLite has a full-featured
SQL implementation, including:
Tables, indexes,
    triggers, and views
    in unlimited quantity
Up to 32K columns in a table and unlimited rows
Multi-column indexes
Indexes can use DESC and COLLATE
Partial indexes
Indexes On Expressions
Clustered indexes
Covering indexes
CHECK, UNIQUE, NOT NULL, and FOREIGN KEY constraints.
ACID transactions using BEGIN, COMMIT, and ROLLBACK
Nested transactions using SAVEPOINT, RELEASE, and 
    ROLLBACK TO
Subqueries, including correlated subqueries
Up to 64-way joins
LEFT, RIGHT, and FULL OUTER JOINs
DISTINCT, ORDER BY, GROUP BY, HAVING, LIMIT, and OFFSET
UNION, UNION ALL, INTERSECT, and EXCEPT
A rich library of standard SQL functions
Aggregate functions including DISTINCT aggregates
Window functions
UPDATE, DELETE, and INSERT (of course)
Common table expressions including
    recursive common table expressions
Row values
UPSERT
An advanced query planner
Full-text search
R-tree indexes
JSON support
The IS operator
Table-valued functions
REPLACE INTO
VACUUM
REINDEX
The GLOB operator
Hexadecimal integer literals
The ON CONFLICT clause
The INDEXED BY clause
Virtual tables
Multiple databases on the same database connection using
    ATTACH DATABASE
The ability to add application-defined SQL functions, including
    aggregate and table-valued functions.
Application-defined collating functions
There are many more features not listed above.
SQLite may be small in size and have "Lite" in its name, but it is
not lacking in capability.
