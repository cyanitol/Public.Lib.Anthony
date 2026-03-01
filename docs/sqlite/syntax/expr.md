SQLite Syntax: expr
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
expr
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
Used by:   aggregate-function-invocation   attach-stmt   column-constraint   compound-select-stmt   create-index-stmt   create-trigger-stmt   delete-stmt   delete-stmt-limited   factored-select-stmt   filter-clause   frame-spec   function-arguments   indexed-column   insert-stmt   join-constraint   ordering-term   over-clause   result-column   returning-clause   select-core   select-stmt   simple-function-invocation   simple-select-stmt   table-constraint   table-or-subquery   update-stmt   update-stmt-limited   upsert-clause   window-defn   window-function-invocation
References:   filter-clause   function-arguments   literal-value   over-clause   raise-function   select-stmt   type-name
See also:   lang_aggfunc.html   lang_altertable.html   lang_attach.html   lang_createindex.html   lang_createtable.html   lang_createtrigger.html   lang_createview.html   lang_delete.html   lang_expr.html   lang_insert.html   lang_returning.html   lang_select.html   lang_update.html   lang_upsert.html   lang_with.html   partialindex.html
