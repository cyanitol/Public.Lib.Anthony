Reset A Prepared Statement Object
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
Reset A Prepared Statement Object
int sqlite3_reset(sqlite3_stmt *pStmt);
The sqlite3_reset() function is called to reset a prepared statement
object back to its initial state, ready to be re-executed.
Any SQL statement variables that had values bound to them using
the sqlite3_bind_*() API retain their values.
Use sqlite3_clear_bindings() to reset the bindings.
The sqlite3_reset(S) interface resets the prepared statement S
back to the beginning of its program.
The return code from sqlite3_reset(S) indicates whether or not
the previous evaluation of prepared statement S completed successfully.
If sqlite3_step(S) has never before been called on S or if
sqlite3_step(S) has not been called since the previous call
to sqlite3_reset(S), then sqlite3_reset(S) will return
SQLITE_OK.
If the most recent call to sqlite3_step(S) for the
prepared statement S indicated an error, then
sqlite3_reset(S) returns an appropriate error code.
The sqlite3_reset(S) interface might also return an error code
if there were no prior errors but the process of resetting
the prepared statement caused a new error. For example, if an
INSERT statement with a RETURNING clause is only stepped one time,
that one call to sqlite3_step(S) might return SQLITE_ROW but
the overall statement might still fail and the sqlite3_reset(S) call
might return SQLITE_BUSY if locking constraints prevent the
database change from committing.  Therefore, it is important that
applications check the return code from sqlite3_reset(S) even if
no prior call to sqlite3_step(S) indicated a problem.
The sqlite3_reset(S) interface does not change the values
of any bindings on the prepared statement S.
See also lists of
  Objects,
  Constants, and
  Functions.
