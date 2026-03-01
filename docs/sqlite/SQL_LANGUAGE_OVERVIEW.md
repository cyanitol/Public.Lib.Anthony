Query Language Understood by SQLite
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
SQL As Understood By SQLite
SQLite understands most of the standard SQL
language.  But it does omit some features
while at the same time
adding a few features of its own.  This document attempts to
describe precisely what parts of the SQL language SQLite does
and does not support.  A list of SQL keywords is
also provided.  The SQL language syntax is described by
syntax diagrams.
The following syntax documentation topics are available:
aggregate functions
ALTER TABLE
ANALYZE
ATTACH DATABASE
BEGIN TRANSACTION
comment
COMMIT TRANSACTION
core functions
CREATE INDEX
CREATE TABLE
CREATE TRIGGER
CREATE VIEW
CREATE VIRTUAL TABLE
date and time functions
DELETE
DETACH DATABASE
DROP INDEX
DROP TABLE
DROP TRIGGER
DROP VIEW
END TRANSACTION
EXPLAIN
expression
INDEXED BY
INSERT
JSON functions
keywords
math functions
ON CONFLICT clause
PRAGMA
REINDEX
RELEASE SAVEPOINT
REPLACE
RETURNING clause
ROLLBACK TRANSACTION
SAVEPOINT
SELECT
UPDATE
UPSERT
VACUUM
window functions
WITH clause
The routines sqlite3_prepare_v2(), sqlite3_prepare(),
sqlite3_prepare16(), sqlite3_prepare16_v2(),
sqlite3_exec(), and sqlite3_get_table() accept
an SQL statement list (sql-stmt-list) which is a semicolon-separated
list of statements.
sql-stmt-list:
sql-stmt
;
Each SQL statement in the statement list is an instance of the
following:
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
This page was last updated on 2024-04-01 12:41:31Z 
