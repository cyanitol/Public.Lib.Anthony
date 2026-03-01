CREATE TRIGGER
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
CREATE TRIGGER
Table Of Contents
1. Syntax
2. Description
2.1. Syntax Restrictions On UPDATE, DELETE, and INSERT Statements Within
Triggers
3. INSTEAD OF triggers
4. Some Example Triggers
5. Cautions On The Use Of BEFORE triggers
6. The RAISE() function
7. TEMP Triggers on Non-TEMP Tables
1. Syntax
create-trigger-stmt:
hide
CREATE
TEMP
TEMPORARY
TRIGGER
IF
NOT
EXISTS
schema-name
.
trigger-name
BEFORE
AFTER
INSTEAD
OF
DELETE
INSERT
UPDATE
OF
column-name
,
ON
table-name
FOR
EACH
ROW
WHEN
expr
BEGIN
update-stmt
;
END
insert-stmt
delete-stmt
select-stmt
delete-stmt:
show
WITH
RECURSIVE
common-table-expression
,
DELETE
FROM
qualified-table-name
returning-clause
expr
WHERE
common-table-expression:
show
table-name
(
column-name
)
AS
NOT
MATERIALIZED
(
select-stmt
)
,
qualified-table-name:
show
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
returning-clause:
show
RETURNING
expr
AS
column-alias
*
,
expr:
show
literal-value
bind-parameter
schema-name
.
table-name
.
column-name
unary-operator
expr
expr
binary-operator
expr
function-name
(
function-arguments
)
filter-clause
over-clause
(
expr
)
,
CAST
(
expr
AS
type-name
)
expr
COLLATE
collation-name
expr
NOT
LIKE
GLOB
REGEXP
MATCH
expr
expr
ESCAPE
expr
expr
ISNULL
NOTNULL
NOT
NULL
expr
IS
NOT
DISTINCT
FROM
expr
expr
NOT
BETWEEN
expr
AND
expr
expr
NOT
IN
(
select-stmt
)
expr
,
schema-name
.
table-function
(
expr
)
table-name
,
NOT
EXISTS
(
select-stmt
)
CASE
expr
WHEN
expr
THEN
expr
ELSE
expr
END
raise-function
filter-clause:
show
FILTER
(
WHERE
expr
)
function-arguments:
show
DISTINCT
expr
,
*
ORDER
BY
ordering-term
,
ordering-term:
show
expr
COLLATE
collation-name
DESC
ASC
NULLS
FIRST
NULLS
LAST
literal-value:
show
CURRENT_TIMESTAMP
numeric-literal
string-literal
blob-literal
NULL
TRUE
FALSE
CURRENT_TIME
CURRENT_DATE
over-clause:
show
OVER
window-name
(
base-window-name
PARTITION
BY
expr
,
ORDER
BY
ordering-term
,
frame-spec
)
frame-spec:
show
GROUPS
BETWEEN
UNBOUNDED
PRECEDING
AND
UNBOUNDED
FOLLOWING
RANGE
ROWS
UNBOUNDED
PRECEDING
expr
PRECEDING
CURRENT
ROW
expr
PRECEDING
CURRENT
ROW
expr
FOLLOWING
expr
PRECEDING
CURRENT
ROW
expr
FOLLOWING
EXCLUDE
CURRENT
ROW
EXCLUDE
GROUP
EXCLUDE
TIES
EXCLUDE
NO
OTHERS
ordering-term:
show
expr
COLLATE
collation-name
DESC
ASC
NULLS
FIRST
NULLS
LAST
raise-function:
show
RAISE
(
ROLLBACK
,
expr
)
IGNORE
ABORT
FAIL
type-name:
show
name
(
signed-number
,
signed-number
)
(
signed-number
)
signed-number:
show
+
numeric-literal
-
insert-stmt:
show
WITH
RECURSIVE
common-table-expression
,
REPLACE
INSERT
OR
ROLLBACK
INTO
ABORT
FAIL
IGNORE
REPLACE
schema-name
.
table-name
AS
alias
(
column-name
)
,
VALUES
(
expr
)
,
,
upsert-clause
select-stmt
upsert-clause
DEFAULT
VALUES
returning-clause
common-table-expression:
show
table-name
(
column-name
)
AS
NOT
MATERIALIZED
(
select-stmt
)
,
returning-clause:
show
RETURNING
expr
AS
column-alias
*
,
upsert-clause:
show
ON
CONFLICT
(
indexed-column
)
WHERE
expr
DO
,
conflict target
UPDATE
SET
column-name-list
=
expr
WHERE
expr
NOTHING
,
column-name
column-name-list:
show
(
column-name
)
,
indexed-column:
show
column-name
COLLATE
collation-name
DESC
expr
ASC
select-stmt:
show
WITH
RECURSIVE
common-table-expression
,
SELECT
DISTINCT
result-column
