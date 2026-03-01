Set Or Clear the Indirect Change Flag
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
Session Module C InterfaceSet Or Clear the Indirect Change Flagint sqlite3session_indirect(sqlite3_session *pSession, int bIndirect);
Each change recorded by a session object is marked as either direct or
indirect. A change is marked as indirect if either:
   The session object "indirect" flag is set when the change is
       made, or
   The change is made by an SQL trigger or foreign key action 
       instead of directly as a result of a users SQL statement.
If a single row is affected by more than one operation within a session,
then the change is considered indirect if all operations meet the criteria
for an indirect change above, or direct otherwise.
This function is used to set, clear or query the session object indirect
flag.  If the second argument passed to this function is zero, then the
indirect flag is cleared. If it is greater than zero, the indirect flag
is set. Passing a value less than zero does not modify the current value
of the indirect flag, and may be used to query the current state of the 
indirect flag for the specified session object.
The return value indicates the final state of the indirect flag: 0 if 
it is clear, or 1 if it is set.
See also lists of
  Objects,
  Constants, and
  Functions.
