CREATE TABLE
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
CREATE TABLE
Table Of Contents
1. Syntax
2. The CREATE TABLE command
2.1. CREATE TABLE ... AS SELECT Statements
3. Column Definitions
3.1. Column Data Types
3.2. The DEFAULT clause
3.3. The COLLATE clause
3.4. The GENERATED ALWAYS AS clause
3.5. The PRIMARY KEY
3.6. UNIQUE constraints
3.7. CHECK constraints
3.8. NOT NULL constraints
4. Constraint enforcement
4.1. Response to constraint violations
5. ROWIDs and the INTEGER PRIMARY KEY
1. Syntax
create-table-stmt:
hide
CREATE
TEMP
TEMPORARY
TABLE
IF
NOT
EXISTS
schema-name
.
table-name
(
column-def
table-constraint
,
)
table-options
,
AS
select-stmt
column-def:
show
column-name
type-name
column-constraint
column-constraint:
show
CONSTRAINT
name
PRIMARY
KEY
DESC
conflict-clause
AUTOINCREMENT
ASC
NOT
NULL
conflict-clause
UNIQUE
conflict-clause
CHECK
(
expr
)
DEFAULT
(
expr
)
literal-value
signed-number
COLLATE
collation-name
foreign-key-clause
GENERATED
ALWAYS
AS
(
expr
)
VIRTUAL
STORED
conflict-clause:
show
ON
CONFLICT
ROLLBACK
ABORT
FAIL
IGNORE
REPLACE
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
foreign-key-clause:
show
REFERENCES
foreign-table
(
column-name
)
,
ON
DELETE
SET
NULL
UPDATE
SET
DEFAULT
CASCADE
RESTRICT
NO
ACTION
MATCH
name
NOT
DEFERRABLE
INITIALLY
DEFERRED
INITIALLY
IMMEDIATE
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
signed-number:
show
+
numeric-literal
-
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
select-stmt:
show
WITH
RECURSIVE
common-table-expression
,
SELECT
DISTINCT
result-column
,
ALL
FROM
table-or-subquery
join-clause
,
WHERE
expr
GROUP
BY
expr
HAVING
expr
,
WINDOW
window-name
AS
window-defn
,
VALUES
(
expr
)
,
,
compound-operator
select-core
ORDER
BY
LIMIT
expr
ordering-term
,
OFFSET
expr
,
expr
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
compound-operator:
show
UNION
UNION
INTERSECT
EXCEPT
ALL
expr:
show
literal-value
bind-parameter
schema-name
.
table-name
.
