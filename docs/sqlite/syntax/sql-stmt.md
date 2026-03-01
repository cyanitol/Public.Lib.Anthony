SQLite Syntax: sql-stmt
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
sql-stmt
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
Used by:   sql-stmt-list
References:   alter-table-stmt   analyze-stmt   attach-stmt   begin-stmt   commit-stmt   create-index-stmt   create-table-stmt   create-trigger-stmt   create-view-stmt   create-virtual-table-stmt   delete-stmt   delete-stmt-limited   detach-stmt   drop-index-stmt   drop-table-stmt   drop-trigger-stmt   drop-view-stmt   insert-stmt   pragma-stmt   reindex-stmt   release-stmt   rollback-stmt   savepoint-stmt   select-stmt   update-stmt   update-stmt-limited   vacuum-stmt
See also:   lang.html   lang_explain.html
