The DBSTAT Virtual Table
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
The DBSTAT Virtual Table
1. Overview
The DBSTAT virtual table is a read-only eponymous virtual table that returns
information about the amount of disk space used to store the content
of an SQLite database.
Example use cases for the
DBSTAT virtual table include the sqlite3_analyzer.exe
utility program and the
table size pie-chart in
the Fossil-implemented version control system
for SQLite.
The DBSTAT virtual table is available on all 
database connections when SQLite is built using the
SQLITE_ENABLE_DBSTAT_VTAB compile-time option.
The DBSTAT virtual table is an eponymous virtual table, meaning
that is not necessary to run CREATE VIRTUAL TABLE to create an
instance of the dbstat virtual table before using it.  The "dbstat"
module name can be used as if it were a table name to query the
dbstat virtual table directly.  For example:
SELECT * FROM dbstat;
If a named virtual table that uses the dbstat module is desired,
then the recommended way to create an instance of the dbstat
virtual table is as follows:
CREATE VIRTUAL TABLE temp.stat USING dbstat(main);
Note the "temp." qualifier before the virtual table name ("stat").  This
qualifier causes the virtual table to be temporary - to only exist for
the duration of the current database connection.  This is the
recommended approach.
The "main" argument to dbstat is default schema
for which information is to be provided.  The default is "main", and
so the use of "main" in the example above is redundant.  For any
particular query, the schema can be changed by specifying the
alternative schema as a function argument to the virtual table
name in the FROM clause of the query.  (See further discussion of
table-valued functions in the FROM clause for more details.)
The schema for the DBSTAT virtual table looks like this:
CREATE TABLE dbstat(
  name       TEXT,        -- Name of table or index
  path       TEXT,        -- Path to page from root
  pageno     INTEGER,     -- Page number, or page count
  pagetype   TEXT,        -- 'internal', 'leaf', 'overflow', or NULL
  ncell      INTEGER,     -- Cells on page (0 for overflow pages)
  payload    INTEGER,     -- Bytes of payload on this page or btree
  unused     INTEGER,     -- Bytes of unused space on this page or btree
  mx_payload INTEGER,     -- Largest payload size of all cells on this row
  pgoffset   INTEGER,     -- Byte offset of the page in the database file
  pgsize     INTEGER,     -- Size of the page, in bytes
  schema     TEXT HIDDEN, -- Database schema being analyzed
  aggregate  BOOL HIDDEN  -- True to enable aggregate mode
);
The DBSTAT table only reports on the content of btrees within the database file.
Freelist pages, pointer-map pages, and the lock page are omitted from
the analysis.
By default, there is a single row in the DBSTAT table for each
btree page the database file.  Each row provides
information about the space utilization of that one page of the
database.  However, if the hidden column "aggregate" is TRUE, then
results are aggregated and there is a single row in the DBSTAT table
for each btree in the database, providing information about space
utilization across the entire btree.
2. The "path" column of the dbstat virtual table
The "path" column describes the path taken from the 
root node of the btree structure to each page.  The
"path" of the root node itself is '/'.
The "path" is NULL when "aggregate" is TRUE.
The "path" for the left-most child page of the root of
a btree page is '/000/'. (Btrees store content ordered from left to right
so the pages to the left have smaller keys than the pages to the right.)
The next to left-most child of the root page is '/001', and so on,
each sibling page identified by a 3-digit hex  value.
The children of the 451st left-most sibling have paths such
as '/1c2/000/, '/1c2/001/' etc.
Overflow pages are specified by appending a '+' character and a 
six-digit hexadecimal value to the path to the cell they are linked
from. For example, the three overflow pages in a chain linked from 
the left-most cell of the 450th child of the root page are identified
by the paths:
'/1c2/000+000000'         // First page in overflow chain
'/1c2/000+000001'         // Second page in overflow chain
'/1c2/000+000002'         // Third page in overflow chain
If the paths are sorted using the BINARY collation sequence, then
the overflow pages associated with a cell will appear earlier in the
sort-order than its child page:
'/1c2/000/'               // Left-most child of 451st child of root
3. Aggregated Data
Beginning with SQLite version 3.31.0 (2020-01-22), the DBSTAT table
has a new hidden column named "aggregate", which if constrained to be
TRUE will cause DBSTAT to generate one row per btree in the database,
rather than one row per page.  When running in aggregated mode, the 
"path", "pagetype", and "pgoffset" columns are always NULL and the
"pageno" column holds the number of pages in the entire btree, rather
than the number of the page that corresponds to the row.
The following table shows the meanings of the (non-hidden) columns of
DBSTAT in both normal and aggregated mode:
Column
Normal meaning
Aggregate-mode meaning
name
The name of the table or index that is implemented by
the btree of the current row
path
See description above
Always NULL
pageno
The page number of the database page for the current row
The total number of pages in the btree for the current row
pagetype
'leaf' or 'interior'
Always NULL
ncell
Number of cells on the current page or btree
payload
Bytes of useful payload on the current page or btree
unused
Unused bytes of on the current page or btree
mx_payload
The largest payload found anywhere in the current page
or btree.
pgoffset
Byte offset to the start of the page
Always NULL
pgsize
Total storage space used by the current page or btree.
4. Example uses of the dbstat virtual table
To find the total number of pages used to store table "xyz" in schema "aux1",
use either of the following two queries (the first is the traditional way,
and the second shows the use of the aggregated feature):
SELECT count(*) FROM dbstat('aux1') WHERE name='xyz';
SELECT pageno FROM dbstat('aux1',1) WHERE name='xyz';
To see how efficiently the content of a table is stored on disk,
compute the amount of space used to hold actual content divided
by the total amount of disk space used.  The closer this number
is to 100%, the more efficient the packing.  (In this example, the
'xyz' table is assumed to be in the 'main' schema.  Again, there
are two different versions that show the use of DBSTAT both without
and with the new aggregated feature, respectively.)
SELECT sum(pgsize-unused)*100.0/sum(pgsize) FROM dbstat WHERE name='xyz';
SELECT (pgsize-unused)*100.0/pgsize FROM dbstat
 WHERE name='xyz' AND aggregate=TRUE;
To find the average fan-out for a table, run:
SELECT avg(ncell) FROM dbstat WHERE name='xyz' AND pagetype='internal';
Modern filesystems operate faster when disk accesses are sequential.
Hence, SQLite will run faster if the content of the database file
is on sequential pages.  To find out what fraction of the pages in
a database are sequential (and thus obtain a measurement that might
be useful in determining when to VACUUM), run a query like the following:
CREATE TEMP TABLE s(rowid INTEGER PRIMARY KEY, pageno INT);
INSERT INTO s(pageno) SELECT pageno FROM dbstat ORDER BY path;
SELECT sum(s1.pageno+1==s2.pageno)*1.0/count(*)
  FROM s AS s1, s AS s2
 WHERE s1.rowid+1=s2.rowid;
DROP TABLE s;
This page was last updated on 2025-04-16 13:13:29Z 
