The COMPLETION() Table-Valued Function
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
The COMPLETION() Table-Valued Function
1. Overview
The COMPLETION extension implements a table-valued function named
"completion" that can be used to suggest completions of partially entered
words during interactive SQL input.  The completion table can be
used to help implement tab-completion, for example.
2. Details
The designed query interface is:
SELECT DISTINCT candidate COLLATE nocase
  FROM completion($prefix, $wholeline)
 ORDER BY 1;
The query above will return suggestions for the whole input word that
begins with $prefix.  The $wholeline parameter is all text from the beginning
of the line up to the insertion point.  The $wholeline parameter is used
for context.
The $prefix parameter may be NULL, in which case the prefix is deduced
from $wholeline.  Or, the $wholeline parameter may be NULL or omitted if 
context information is unavailable or if context-aware completion is not
desired.
The completion table might return the same candidate more than once, and
it will return candidates in an arbitrary order.  The DISTINCT keyword and
the ORDER BY in the sample query above are added to make the answers unique
and in lexicographical order.
2.1. Example Usage
The completion table is used to implement tab-completion in the
command-line shell in conjunction with either the readline or linenoise
input line editing packages for unix.  See the
https://sqlite.org/src/file/src/shell.c.in source file for example
code.  Search for "FROM completion" to find the relevant code sections.
Because the completion table is built into the command-line shell in order
to provide for tab-completions, you can run test queries against the
completion table directly in the command-line shell.  Simply type a
query such as the example shown above, filling in appropriate values
for $prefix and $wholeline, and observe the output.
3. Limitations
The completion table is designed for interactive use.  It will return
answers at a speed appropriate for human typing.  No effort is made to
be unusually efficient, so long as the response time is nearly instantaneous
in a user interface.
As of this writing (2017-07-13), the completion virtual table only
looks for SQL keywords, and schema, table, and column names.  The
context contained in $wholeline is completely ignored.  Future enhancements
will try to return new completions taken from function and pragma names
and other sources, as well as consider more context.  The completion
table should be considered a work-in-progress.
