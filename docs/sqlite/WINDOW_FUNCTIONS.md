Window Functions
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
Window Functions
Table Of Contents
1. Introduction to Window Functions
2. Aggregate Window Functions
2.1. The PARTITION BY Clause
2.2. Frame Specifications
2.2.1. Frame Type
2.2.2. Frame Boundaries
2.2.3. The EXCLUDE Clause
2.3. The FILTER Clause
3. Built-in Window Functions
4. Window Chaining
5. User-Defined Aggregate Window Functions
6. History
1. Introduction to Window Functions
A window function is an SQL function where the input
values are taken from
a "window" of one or more rows in the results set of a SELECT statement.
Window functions are distinguished from scalar functions and
aggregate functions by the
presence of an OVER clause. If a function has an OVER clause,
then it is a window function. If it lacks an OVER clause, then it is an
ordinary aggregate or scalar function. Window functions might also
have a FILTER clause in between the function and the OVER clause.
The syntax for a window function is like this:
window-function-invocation:
hide
window-func
(
expr
)
filter-clause
OVER
window-name
window-defn
,
*
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
hide
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
join-clause:
show
table-or-subquery
join-operator
table-or-subquery
join-constraint
join-constraint:
show
USING
(
column-name
)
,
ON
expr
join-operator:
show
NATURAL
LEFT
OUTER
JOIN
,
RIGHT
FULL
INNER
CROSS
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
result-column:
show
expr
AS
column-alias
*
table-name
.
*
table-or-subquery:
show
schema-name
.
table-name
AS
table-alias
INDEXED
BY
index-name
NOT
INDEXED
table-function-name
(
expr
)
,
AS
table-alias
(
select-stmt
)
(
table-or-subquery
)
,
join-clause
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
filter-clause:
hide
FILTER
(
WHERE
expr
)
window-defn:
hide
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
hide
GROUPS
BETWEEN
UNBOUNDED
