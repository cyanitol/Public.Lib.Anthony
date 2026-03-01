Flags for sqlite3changeset_apply_v2
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
Session Module C InterfaceFlags for sqlite3changeset_apply_v2#define SQLITE_CHANGESETAPPLY_NOSAVEPOINT   0x0001
#define SQLITE_CHANGESETAPPLY_INVERT        0x0002
#define SQLITE_CHANGESETAPPLY_IGNORENOOP    0x0004
#define SQLITE_CHANGESETAPPLY_FKNOACTION    0x0008
The following flags may passed via the 9th parameter to
sqlite3changeset_apply_v2 and sqlite3changeset_apply_v2_strm:
SQLITE_CHANGESETAPPLY_NOSAVEPOINT 
  Usually, the sessions module encloses all operations performed by
  a single call to apply_v2() or apply_v2_strm() in a SAVEPOINT. The
  SAVEPOINT is committed if the changeset or patchset is successfully
  applied, or rolled back if an error occurs. Specifying this flag
  causes the sessions module to omit this savepoint. In this case, if the
  caller has an open transaction or savepoint when apply_v2() is called, 
  it may revert the partially applied changeset by rolling it back.
SQLITE_CHANGESETAPPLY_INVERT 
  Invert the changeset before applying it. This is equivalent to inverting
  a changeset using sqlite3changeset_invert() before applying it. It is
  an error to specify this flag with a patchset.
SQLITE_CHANGESETAPPLY_IGNORENOOP 
  Do not invoke the conflict handler callback for any changes that
  would not actually modify the database even if they were applied.
  Specifically, this means that the conflict handler is not invoked
  for:
   a delete change if the row being deleted cannot be found, 
   an update change if the modified fields are already set to 
       their new values in the conflicting row, or
   an insert change if all fields of the conflicting row match
       the row being inserted.
SQLITE_CHANGESETAPPLY_FKNOACTION 
  If this flag it set, then all foreign key constraints in the target
  database behave as if they were declared with "ON UPDATE NO ACTION ON
  DELETE NO ACTION", even if they are actually CASCADE, RESTRICT, SET NULL
  or SET DEFAULT.
See also lists of
  Objects,
  Constants, and
  Functions.
