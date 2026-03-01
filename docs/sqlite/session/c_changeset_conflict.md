Constants Passed To The Conflict Handler
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
Session Module C InterfaceConstants Passed To The Conflict Handler#define SQLITE_CHANGESET_DATA        1
#define SQLITE_CHANGESET_NOTFOUND    2
#define SQLITE_CHANGESET_CONFLICT    3
#define SQLITE_CHANGESET_CONSTRAINT  4
#define SQLITE_CHANGESET_FOREIGN_KEY 5
Values that may be passed as the second argument to a conflict-handler.
SQLITE_CHANGESET_DATA
  The conflict handler is invoked with CHANGESET_DATA as the second argument
  when processing a DELETE or UPDATE change if a row with the required
  PRIMARY KEY fields is present in the database, but one or more other 
  (non primary-key) fields modified by the update do not contain the 
  expected "before" values.
  The conflicting row, in this case, is the database row with the matching
  primary key.
SQLITE_CHANGESET_NOTFOUND
  The conflict handler is invoked with CHANGESET_NOTFOUND as the second
  argument when processing a DELETE or UPDATE change if a row with the
  required PRIMARY KEY fields is not present in the database.
  There is no conflicting row in this case. The results of invoking the
  sqlite3changeset_conflict() API are undefined.
SQLITE_CHANGESET_CONFLICT
  CHANGESET_CONFLICT is passed as the second argument to the conflict
  handler while processing an INSERT change if the operation would result 
  in duplicate primary key values.
  The conflicting row in this case is the database row with the matching
  primary key.
SQLITE_CHANGESET_FOREIGN_KEY
  If foreign key handling is enabled, and applying a changeset leaves the
  database in a state containing foreign key violations, the conflict 
  handler is invoked with CHANGESET_FOREIGN_KEY as the second argument
  exactly once before the changeset is committed. If the conflict handler
  returns CHANGESET_OMIT, the changes, including those that caused the
  foreign key constraint violation, are committed. Or, if it returns
  CHANGESET_ABORT, the changeset is rolled back.
  No current or conflicting row information is provided. The only function
  it is possible to call on the supplied sqlite3_changeset_iter handle
  is sqlite3changeset_fk_conflicts().
SQLITE_CHANGESET_CONSTRAINT
  If any other constraint violation occurs while applying a change (i.e. 
  a UNIQUE, CHECK or NOT NULL constraint), the conflict handler is 
  invoked with CHANGESET_CONSTRAINT as the second argument.
  There is no conflicting row in this case. The results of invoking the
  sqlite3changeset_conflict() API are undefined.
See also lists of
  Objects,
  Constants, and
  Functions.
