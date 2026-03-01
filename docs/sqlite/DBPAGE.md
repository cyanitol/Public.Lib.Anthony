The SQLITE_DBPAGE Virtual Table
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
The SQLITE_DBPAGE Virtual Table
1. Overview
The SQLITE_DBPAGE extension implements an eponymous-only virtual table that
provides direct access to the underlying database file by interacting
with the pager.  SQLITE_DBPAGE is capable of both reading and writing any
page of the database.  Because interaction is through the pager layer, all
changes are transactional.
Warning: writing to the SQLITE_DBPAGE virtual table can very easily
cause unrecoverable database corruption.  Do not allow untrusted components
to access the SQLITE_DBPAGE table.  Use appropriate care while using the
SQLITE_DBPAGE table.  Back up important data prior to experimenting with the
SQLITE_DBPAGE table.  Writes to the SQLITE_DBPAGE virtual table are
disabled when the SQLITE_DBCONFIG_DEFENSIVE flag is set.
The SQLITE_DBPAGE extension is included in the amalgamation though 
it is disabled
by default.  Use the SQLITE_ENABLE_DBPAGE_VTAB compile-time option to enable
the SQLITE_DBPAGE extension.  The SQLITE_DBPAGE extension makes use of
unpublished internal interfaces and is not run-time loadable.  The only way
to add SQLITE_DBPAGE to an application is to compile it in using the
SQLITE_ENABLE_DBPAGE_VTAB compile-time option.
The SQLITE_DBPAGE extension is enabled in default builds
of the command-line shell, but is read-only by default.  Use
the --unsafe-testing command-line option to the CLI to enable writing
to the sqlite_dbpage virtual table.
2. Usage
The SQLITE_DBPAGE virtual table read/write table that provides direct
access to the underlying disk file on a page-by-page basis.  The
virtual table appears to have a schema like this:
CREATE TABLE sqlite_dbpage(
  pgno INTEGER PRIMARY KEY,
  data BLOB
);
An SQLite database file is divided into pages.
The first page is 1, the second page is 2, and so forth.
There is no page 0.
Every page is the same size.
The size of every page is a power of 2 between 512 and 65536.
See the file format documentation for further details.
The SQLITE_DBPAGE table allows an application to view or replace the
raw binary content of each page of the database file.
No attempt is made to interpret the content of the page.
Content is returned byte-for-byte as it appears on disk.
The SQLITE_DBPAGE table has one row for each page in the database file.
SQLITE_DBPAGE allows pages to be read using a SELECT statement or
to be overwritten using an UPDATE or INSERT statement.  When using
an INSERT against sqlite_dbpage, it acts like REPLACE.  In other words,
INSERT into a page that already exists overwrites that page.  To increase
the size of a database, INSERT into a page beyond the end of the database.
2.1. Using SQLITE_DBPAGE On ATTACH-ed Databases
The SQLITE_DBPAGE table schema shown above is incomplete.  There is
a third hidden column named "schema" that determines which
ATTACH-ed database should be read or written.  Because
the "schema" column is hidden, it can be used as a parameter when
SQLITE_DBPAGE is invoked as a table-valued function.
For example, suppose an additional database is attached to the 
database connection using a statement like this:
ATTACH 'auxdata1.db' AS aux1;
Then to read the first page of that database file, one merely runs:
SELECT data FROM sqlite_dbpage('aux1') WHERE pgno=1;
If the "schema" is omitted, it defaults to the primary database
(usually called 'main', unless renamed using SQLITE_DBCONFIG_MAINDBNAME).
Hence, the following two queries are normally equivalent:
SELECT data FROM sqlite_dbpage('main') WHERE pgno=1;
SELECT data FROM sqlite_dbpage WHERE pgno=1;
The SQLITE_DBPAGE table can participate in a join just like any other
table.  Hence, to see the content of the first page to all connected
database files, one might run a statement like this:
SELECT dbpage.data, dblist.name
  FROM pragma_database_list AS dblist
  JOIN sqlite_dbpage(dblist.name) AS dbpage
 WHERE dbpage.pgno=1;
2.2. Truncating A Database
Normally the data for UPDATE and INSERT must be a blob which is the exact
same size as a database page.  However, as a special case, doing an INSERT
of a NULL into page 2 or greater as the last operation prior to COMMIT
causes the page being inserted into and all subsequent pages to be deleted,
truncating the database.  For example, a command like
"INSERT INTO sqlite_dbpage(pgno,data) VALUES(52,NULL)" causes
the database to be truncated to 51 pages in length. Pages are numbered
beginning with 1, so the total number of pages remaining is the same as
the last page number or 51 in this case. 
Call an INSERT into sqlite_dbpage with a NULL page content
a "null-INSERT".  The null-INSERT behavior is a special case and does
not necessarily follow the usual rules.  The following are some specific
limitations of null-INSERT:
The sqlite_dbpage table does not allow a null-INSERT into page 1.
Hence, it is not possible to truncate a database to zero bytes or
zero pages using sqlite_dbpage, as doing so would omit the database
header and the file would cease to be an SQLite database.
A null-INSERT only truncates the database if it is the last write
operation to occur before the transaction commit.  If the null-INSERT is
within a multi-statement transaction and is followed by one or more UPDATE
or INSERT operations within that same transaction, then the null-INSERT
becomes a no-op and the database is not truncated.
Only a single database file can be truncated per transaction.
The sqlite_dbpage virtual table is able to write to multiple ATTACH-ed
databases during the same transaction, but because a null-INSERT must
be the last write statement to occur before COMMIT, sqlite_dbpage can
only truncate one of the ATTACH-ed databases.  If multiple ATTACH-ed
databases need to be truncated, each must be truncated in a separate
transaction.
Any ROLLBACK TO will cancel the null-INSERT,
even a ROLLBACK TO that does not involve the null-INSERT itself.  The
special behavior of null-INSERT is only guaranteed if it occurs
as its own transaction or immediately before COMMIT.
3. Compatibility
The ability to do an INSERT into sqlite_dbpage, and to increase
or decrease the size of the database using INSERT was added in
SQLite version 3.47.0 (2024-10-21).  Older versions do not allow
INSERT against sqlite_dbpage.
This page was last updated on 2024-10-22 19:45:21Z 
