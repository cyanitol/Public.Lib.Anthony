Obtain Aggregate Function Context
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
SQLite C Interface
Obtain Aggregate Function Context
void *sqlite3_aggregate_context(sqlite3_context*, int nBytes);
Implementations of aggregate SQL functions use this
routine to allocate memory for storing their state.
The first time the sqlite3_aggregate_context(C,N) routine is called
for a particular aggregate function, SQLite allocates
N bytes of memory, zeroes out that memory, and returns a pointer
to the new memory. On second and subsequent calls to
sqlite3_aggregate_context() for the same aggregate function instance,
the same buffer is returned.  Sqlite3_aggregate_context() is normally
called once for each invocation of the xStep callback and then one
last time when the xFinal callback is invoked.  When no rows match
an aggregate query, the xStep() callback of the aggregate function
implementation is never called and xFinal() is called exactly once.
In those cases, sqlite3_aggregate_context() might be called for the
first time from within xFinal().
The sqlite3_aggregate_context(C,N) routine returns a NULL pointer
when first called if N is less than or equal to zero or if a memory
allocation error occurs.
The amount of space allocated by sqlite3_aggregate_context(C,N) is
determined by the N parameter on the first successful call.  Changing the
value of N in any subsequent call to sqlite3_aggregate_context() within
the same aggregate function instance will not resize the memory
allocation.  Within the xFinal callback, it is customary to set
N=0 in calls to sqlite3_aggregate_context(C,N) so that no
pointless memory allocations occur.
SQLite automatically frees the memory allocated by
sqlite3_aggregate_context() when the aggregate query concludes.
The first parameter must be a copy of the
SQL function context that is the first parameter
to the xStep or xFinal callback routine that implements the aggregate
function.
This routine must be called from the same thread in which
the aggregate SQL function is running.
See also lists of
  Objects,
  Constants, and
  Functions.
