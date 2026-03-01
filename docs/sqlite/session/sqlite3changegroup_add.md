Add A Changeset To A Changegroup
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
Session Module C InterfaceAdd A Changeset To A Changegroupint sqlite3changegroup_add(sqlite3_changegroup*, int nData, void *pData);
Add all changes within the changeset (or patchset) in buffer pData (size
nData bytes) to the changegroup. 
If the buffer contains a patchset, then all prior calls to this function
on the same changegroup object must also have specified patchsets. Or, if
the buffer contains a changeset, so must have the earlier calls to this
function. Otherwise, SQLITE_ERROR is returned and no changes are added
to the changegroup.
Rows within the changeset and changegroup are identified by the values in
their PRIMARY KEY columns. A change in the changeset is considered to
apply to the same row as a change already present in the changegroup if
the two rows have the same primary key.
Changes to rows that do not already appear in the changegroup are
simply copied into it. Or, if both the new changeset and the changegroup
contain changes that apply to a single row, the final contents of the
changegroup depends on the type of each change, as follows:
  Existing Change  
      New Change       
      Output Change
  INSERT INSERT 
      The new change is ignored. This case does not occur if the new
      changeset was recorded immediately after the changesets already
      added to the changegroup.
  INSERT UPDATE 
      The INSERT change remains in the changegroup. The values in the 
      INSERT change are modified as if the row was inserted by the
      existing change and then updated according to the new change.
  INSERT DELETE 
      The existing INSERT is removed from the changegroup. The DELETE is
      not added.
  UPDATE INSERT 
      The new change is ignored. This case does not occur if the new
      changeset was recorded immediately after the changesets already
      added to the changegroup.
  UPDATE UPDATE 
      The existing UPDATE remains within the changegroup. It is amended 
      so that the accompanying values are as if the row was updated once 
      by the existing change and then again by the new change.
  UPDATE DELETE 
      The existing UPDATE is replaced by the new DELETE within the
      changegroup.
  DELETE INSERT 
      If one or more of the column values in the row inserted by the
      new change differ from those in the row deleted by the existing 
      change, the existing DELETE is replaced by an UPDATE within the
      changegroup. Otherwise, if the inserted row is exactly the same 
      as the deleted row, the existing DELETE is simply discarded.
  DELETE UPDATE 
      The new change is ignored. This case does not occur if the new
      changeset was recorded immediately after the changesets already
      added to the changegroup.
  DELETE DELETE 
      The new change is ignored. This case does not occur if the new
      changeset was recorded immediately after the changesets already
      added to the changegroup.
If the new changeset contains changes to a table that is already present
in the changegroup, then the number of columns and the position of the
primary key columns for the table must be consistent. If this is not the
case, this function fails with SQLITE_SCHEMA. Except, if the changegroup
object has been configured with a database schema using the
sqlite3changegroup_schema() API, then it is possible to combine changesets
with different numbers of columns for a single table, provided that
they are otherwise compatible.
If the input changeset appears to be corrupt and the corruption is
detected, SQLITE_CORRUPT is returned. Or, if an out-of-memory condition
occurs during processing, this function returns SQLITE_NOMEM. 
In all cases, if an error occurs the state of the final contents of the
changegroup is undefined. If no error occurs, SQLITE_OK is returned.
See also lists of
  Objects,
  Constants, and
  Functions.
