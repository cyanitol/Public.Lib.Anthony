REINDEX
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
REINDEX
reindex-stmt:
hide
REINDEX
schema-name
.
index-name
table-name
collation-name
The REINDEX command is used to delete and recreate indices from scratch.
This is useful when the definition of a collation sequence has changed, or
when there are indexes on expressions involving a function whose definition
has changed.
If the REINDEX keyword is not followed by a collation-sequence or database 
object identifier, then all indices in all attached databases are rebuilt.
If the REINDEX keyword is followed by a collation-sequence name, then
all indices in all attached databases that use the named collation sequences
are recreated. 
Or, if the argument attached to the REINDEX identifies a specific 
database table, then all indices attached to the database table are rebuilt. 
If it identifies a specific database index, then just that index is recreated.
For a command of the form "REINDEX name", a match
against collation-name takes precedence over a match
against index-name or table-name.
This ambiguity in the syntax may be avoided by always specifying a
schema-name when reindexing a specific table or index.
