Invert A Changeset
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
Session Module C InterfaceInvert A Changesetint sqlite3changeset_invert(
  int nIn, const void *pIn,       /* Input changeset */
  int *pnOut, void **ppOut        /* OUT: Inverse of input */
);
This function is used to "invert" a changeset object. Applying an inverted
changeset to a database reverses the effects of applying the uninverted
changeset. Specifically:
   Each DELETE change is changed to an INSERT, and
   Each INSERT change is changed to a DELETE, and
   For each UPDATE change, the old.* and new.* values are exchanged.
This function does not change the order in which changes appear within
the changeset. It merely reverses the sense of each individual change.
If successful, a pointer to a buffer containing the inverted changeset
is stored in *ppOut, the size of the same buffer is stored in *pnOut, and
SQLITE_OK is returned. If an error occurs, both *pnOut and *ppOut are
zeroed and an SQLite error code returned.
It is the responsibility of the caller to eventually call sqlite3_free()
on the *ppOut pointer to free the buffer allocation following a successful 
call to this function.
WARNING/TODO: This function currently assumes that the input is a valid
changeset. If it is not, the results are undefined.
See also lists of
  Objects,
  Constants, and
  Functions.
