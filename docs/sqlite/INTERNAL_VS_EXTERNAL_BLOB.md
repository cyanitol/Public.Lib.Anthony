Internal Versus External BLOBs
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
Internal Versus External BLOBs in SQLite
If you have a database of large BLOBs, do you get better read performance
when you store the complete
BLOB content directly in the database or is it faster to store each BLOB
in a separate file and store just the corresponding filename in the database?
To try to answer this, we ran 49 test cases with various BLOB sizes and
SQLite page sizes on a Linux workstation (Ubuntu circa 2011 with the
Ext4 filesystem on a fast SATA disk).
For each test case, a database was created containing 100MB of BLOB
content.  The sizes of the BLOBs ranged from 10KB to 1MB.  The number
of BLOBs varied in order to keep the total BLOB content at about 100MB.
(Hence, 100 BLOBs for the 1MB size and 10000 BLOBs for the 10K size and
so forth.)  SQLite version 3.7.8 (2011-09-19) was used.
Update: New measurements for SQLite version 3.19.0
(2017-05-22) show that SQLite is about 
35% faster than direct disk I/O for 
both reads and writes of 10KB blobs.
The matrix below shows the time needed to read BLOBs stored in separate files
divided by the time needed to read BLOBs stored entirely in the database.  
Hence, for numbers larger than 1.0, it is faster to store the BLOBs directly 
in the database.  For numbers smaller than 1.0, it is faster to store the BLOBs
in separate files.
In every case, the pager cache size was adjusted to keep the amount of
cache memory at about 2MB.  
For example, a 2000 page cache was used for 1024 byte pages
and a 31 page cache was used for 65536 byte pages.
The BLOB values were read in a random order.
Database Page SizeBLOB size
10k20k50k100k200k500k1m
10241.5351.0200.6080.4560.3300.2470.233
20482.0041.4370.8700.6360.4830.3720.340
40962.2611.8861.1730.8900.7010.5260.487
81922.2401.8661.3341.0350.8300.6250.720
163842.4391.7571.2921.0230.8290.8200.598
327681.8781.8431.2960.9810.9760.6750.613
655361.2561.2551.3390.9830.7690.6870.609
We deduce the following rules of thumb from the matrix above:
A database page size of 8192 or 16384 gives the best performance
for large BLOB I/O.
For BLOBs smaller than 100KB, reads are faster when
the BLOBs are stored directly in the database file.  For BLOBs larger than
100KB, reads from a separate file are faster.
Of course, your mileage may vary depending on hardware, filesystem,
and operating system.  Double-check these figures on target hardware
before committing to a particular design.
This page was last updated on 2022-04-18 02:55:50Z 
