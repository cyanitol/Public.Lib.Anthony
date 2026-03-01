List Of SQLite Functions
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
Session Module C Interface
Functions:
sqlite3changegroup_add
sqlite3changegroup_add_change
sqlite3changegroup_add_strm
sqlite3changegroup_delete
sqlite3changegroup_new
sqlite3changegroup_output
sqlite3changegroup_output_strm
sqlite3changegroup_schema
sqlite3changeset_apply
sqlite3changeset_apply_strm
sqlite3changeset_apply_v2
sqlite3changeset_apply_v2_strm
sqlite3changeset_apply_v3
sqlite3changeset_apply_v3_strm
sqlite3changeset_concat
sqlite3changeset_concat_strm
sqlite3changeset_conflict
sqlite3changeset_finalize
sqlite3changeset_fk_conflicts
sqlite3changeset_invert
sqlite3changeset_invert_strm
sqlite3changeset_new
sqlite3changeset_next
sqlite3changeset_old
sqlite3changeset_op
sqlite3changeset_pk
sqlite3changeset_start
sqlite3changeset_start_strm
sqlite3changeset_start_v2
sqlite3changeset_start_v2_strm
sqlite3rebaser_configure(exp)
sqlite3rebaser_create(exp)
sqlite3rebaser_delete(exp)
sqlite3rebaser_rebase(exp)
sqlite3rebaser_rebase_strm
sqlite3session_attach
sqlite3session_changeset
sqlite3session_changeset_size
sqlite3session_changeset_strm
sqlite3session_config
sqlite3session_create
sqlite3session_delete
sqlite3session_diff
sqlite3session_enable
sqlite3session_indirect
sqlite3session_isempty
sqlite3session_memory_used
sqlite3session_object_config
sqlite3session_patchset
sqlite3session_patchset_strm
sqlite3session_table_filter
Other lists:
Constants and
Objects.
