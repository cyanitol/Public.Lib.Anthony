ANALYZE
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
ANALYZE
Table Of Contents
1. Overview
2. Recommended usage patterns
2.1. Periodically run "PRAGMA optimize"
2.2. Fixed results of ANALYZE
3. Details
4. Automatically Running ANALYZE
5. Approximate ANALYZE For Large Databases
5.1. Limitations of approximate ANALYZE
1. Overview
analyze-stmt:
hide
ANALYZE
schema-name
.
table-or-index-name
schema-name
index-or-table-name
 The ANALYZE command gathers statistics about tables and
indices and stores the collected information
in internal tables of the database where the query optimizer can
access the information and use it to help make better query planning choices.
If no arguments are given, the main database and all attached databases are
analyzed.  If a schema name is given as the argument, then all tables
and indices in that one database are analyzed.  
If the argument is a table name, then only that table and the
indices associated with that table are analyzed.  If the argument
is an index name, then only that one index is analyzed.
2. Recommended usage patterns
 The use of ANALYZE is never required.  However, if an application
makes complex queries that have many possible query plans, the query
planner will be better able to pick the best plan if ANALYZE has
been run.  This can result in significant performance improvements for
some queries.
 Two recommended approaches for when and how to run ANALYZE are
described in the next subsections, in order of preference.
2.1. Periodically run "PRAGMA optimize"
The PRAGMA optimize command will automatically run ANALYZE when
needed.  Suggested use:
Applications with short-lived database connections should run
"PRAGMA optimize;" once, just prior to closing each database connection.
Applications that use long-lived database connections should run
"PRAGMA optimize=0x10002;" when the connection is first opened, and then
also run "PRAGMA optimize;" periodically, perhaps once per day, or more if
the database is evolving rapidly.
All applications should run "PRAGMA optimize;" after a schema change,
especially after one or more CREATE INDEX statements.
The PRAGMA optimize command is usually a no-op but it will occasionally
run one or more ANALYZE subcommands on individual tables of the database
if doing so will be useful to the query planner.
Since SQLite version 3.46.0 (2024-05-23), the "PRAGMA optimize" command
automatically limits the scope of ANALYZE subcommands so that
the overall "PRAGMA optimize" command completes quickly even on enormous
databases.  There is no need to use PRAGMA analysis_limit.  This is the
recommended way of running ANALYZE moving forward.
The PRAGMA optimize command will normally only consider running ANALYZE on
tables that have been previously queried by the same database connection or
that do not have entries in the sqlite_stat1 table.
However, if the 0x10000 bit is added to the argument, PRAGMA optimize will
examine all tables to see if they can benefit from ANALYZE, not just those
that have been recently queried.
There is no query history when a database connection first opens, and
that is why adding the 0x10000 bit is recommended when running PRAGMA optimize
on a fresh database connection.
See the Automatically Running ANALYZE and
Approximate ANALYZE For Large Databases sections below for additional
information.
2.2. Fixed results of ANALYZE
Running ANALYZE can cause SQLite to choose different query plans
for subsequent queries.  This is almost always a positive thing, as the
query plans chosen after ANALYZE will in nearly every case be better than
the query plans picked before ANALYZE.  That is the whole point of ANALYZE.
But there can be no proof of running ANALYZE will always be beneficial.
One can construct pathological cases where running
ANALYZE could make some subsequent queries run slower.
Some developers prefer that once the design of an application is frozen,
SQLite will always pick the same query plans as it did during
development and testing.
Then if a millions of copies of the application are shipped to customers,
the developers are assured that all of those millions of copies are running
the same query plans regardless of what data the individual customers insert
into their particular databases.  This can help in reproducing complaints
of performance problems coming back from the field.
To achieve this objection, never run a full ANALYZE nor the
"PRAGMA optimize" command in the application.
Rather, only run ANALYZE during development, manually using the
command-line interface or similar, on a test database 
that is similar in size and content to live databases.  Then capture
the result of this one-time ANALYZE using a script like the
following:
.mode list
SELECT 
  'ANALYZE sqlite_schema;' ||
  'DELETE FROM sqlite_stat1;' ||
  'INSERT INTO sqlite_stat1(tbl,idx,stat)VALUES' ||
  (SELECT group_concat(format('(%Q,%Q,%Q)',tbl,idx,stat),',')
    FROM sqlite_stat1) ||
  ';ANALYZE sqlite_schema;';
When creating a new instance of the database in deployed instances of
the application, or perhaps every time the application is started up in
the case of long-running applications, run the commands generated by
script above.  This will populate the sqlite_stat1 table exactly as
it was during development and testing and ensure that the query plans
selected in the field are same as those selected during testing in the
lab.  Maybe copy/paste the string generated by the script above into
a static string constant named "zStat1Init" and then invoke:
sqlite3_exec(db, zStat1Init, 0, 0, 0);
Perhaps also add "BEGIN;" at the start of the string constant and
"COMMIT;" at the end, depending on the context in which the script is run.
See the query planner stability guarantee for addition information.
3. Details
 The default implementation stores all statistics in a single
table named "sqlite_stat1".  
 If SQLite is compiled with the
SQLITE_ENABLE_STAT4 option, then additional histogram data is
collected and stored in sqlite_stat4.
Older versions of SQLite would make use of the sqlite_stat2 table
or sqlite_stat3 table
when compiled with SQLITE_ENABLE_STAT2 or SQLITE_ENABLE_STAT3,
but all recent versions of
SQLite ignore the sqlite_stat2 and sqlite_stat3 tables.
Future enhancements may create
additional internal tables with the same name pattern except with
final digit larger than "4".
All of these tables are collectively referred to as "statistics tables".
 The content of the statistics tables can be queried using SELECT
and can be changed using the DELETE, INSERT, and UPDATE commands.
The DROP TABLE command works on statistics tables
as of SQLite version 3.7.9. (2011-11-01)
The ALTER TABLE command does not work on statistics tables.
Appropriate care should be used when changing the content of the statistics
tables as invalid content can cause SQLite to select inefficient
query plans.  Generally speaking, one should not modify the content of
the statistics tables by any mechanism other than invoking the
ANALYZE command.  
See "Manual Control Of Query Plans Using SQLITE_STAT Tables" for
further information.
 Statistics gathered by ANALYZE are not updated as
the content of the database changes.  If the content of the database
changes significantly, or if the database schema changes, then one should
consider rerunning the ANALYZE command in order to update the statistics.
 The query planner loads the content of the statistics tables
into memory when the schema is read.  Hence, when an application
changes the statistics tables directly, SQLite will not immediately
notice the changes. An application
can force the query planner to reread the statistics tables by running
ANALYZE sqlite_schema. 
4. Automatically Running ANALYZE
The PRAGMA optimize command will automatically run ANALYZE on individual
tables on an as-needed basis.  The recommended practice is for applications
to invoke the PRAGMA optimize statement just before closing each database
connection.  Or, if the application keeps a single database connection open
for a long time, then it should run "PRAGMA optimize=0x10002" when the
connection is first opened and run "PRAGMA optimize;" periodically thereafter,
perhaps once per day or even once per hour.
Each SQLite database connection records cases when the query planner would
benefit from having accurate results of ANALYZE at hand.  These records
are held in memory and accumulate over the life of a database connection.
The PRAGMA optimize command looks at those records and runs ANALYZE on only
those tables for which new or updated ANALYZE data seems likely to be useful.
In most cases PRAGMA optimize will not run ANALYZE, but it will occasionally
do so either for tables that have never before been analyzed, or for tables
that have grown significantly since they were last analyzed.
Since the actions of PRAGMA optimize are determined to some extent by
prior queries that have been evaluated on the same database connection, it
is recommended that PRAGMA optimize be deferred until the database connection
is closing and has thus had an opportunity to accumulate as much usage information
as possible.  It is also reasonable to set a timer to run PRAGMA optimize
every few hours, or every few days, for database connections that stay open
for a long time.  When running PRAGMA optimize immediately after a
database connection is opened, one can add the 0x10000 bit to the bitmask
argument (thus making the command read "PRAGMA optimize=0x10002") which
causes all tables to be examined, even tables that have not been
queried during the current connection.
The PRAGMA optimize command was first introduced with 
SQLite 3.18.0 (2017-03-28) and is a no-op for all prior releases
of SQLite.  The PRAGMA optimize command was significantly enhanced
in SQLite 3.46.0 (2024-05-23) and the advice given in this
documentation is based on those enhancements.  Applications that
use earlier versions of SQLite should consult the corresponding
documentation for better advice on the best ways to use PRAGMA optimize.
5. Approximate ANALYZE For Large Databases
By default, ANALYZE does a full scan of every index.  This can be slow for
large databases.  So beginning with SQLite version 3.32.0 (2020-05-22), the
PRAGMA analysis_limit command can be used to limit the amount of
scanning performed by ANALYZE, and thus help ANALYZE to run faster,
even on very large database files.  We call this running an
"approximate ANALYZE".
The recommended usage pattern for the analysis_limit pragma is
like this:
PRAGMA analysis_limit=1000;
This pragma tells the ANALYZE command to start a full scan
of the index as it normally would.  But when the number of rows visited
reaches 1000 (or whatever other limit is specified by the pragma), the
ANALYZE command will begin taking actions to stop the scan.  If
the left-most column of the index has changed at least once during the
previous 1000 steps, then the analysis stops immediately.  But if the
left-most column has always been the same, then ANALYZE skips ahead to
the first entry with a different left-most column and reads an additional
1000 rows before terminating.
The details of the effects of the analysis limit described in the previous
paragraph are subject to change in future versions of SQLite.  But the
core idea will remain the same.  An analysis limit of N will strive to
limit the number of rows visited in each index to approximately N.
Values of N between 100 and 1000 are recommended.
Or, to disable the analysis limit, causing ANALYZE to do a
complete scan of each index, set the analysis limit to 0.  The default
value for the analysis limit is 0 for backwards compatibility.
The values placed in the sqlite_stat1 table by an approximate ANALYZE
are not exactly the same as what would be computed by an unrestricted 
analysis.  But they are usually close enough.  The index statistics in
the sqlite_stat1 table are approximations in any case, so the fact that
the results of an approximate ANALYZE are slightly different from
a traditional full scan ANALYZE has little practical impact.  It is
possible to construct a pathological case where an approximate ANALYZE
is noticeably inferior to a full-scan ANALYZE, but such cases are rare in
real-world problems.
A good rule of thumb seems to be to always set "PRAGMA analysis_limit=N"
for N between 100 and 1000 prior to running either "ANALYZE".  It used
to be that this was also recommended  prior to running
"PRAGMA optimize", but since version 3.46.0 (2024-05-23) that
happens automatically.  The results are not quite as precise when using
PRAGMA analysis_limit, but they are precise enough, and the fact that
the results are computed so much faster means that developers are more
likely to compute them.  An approximate ANALYZE is better than not
running ANALYZE at all.
5.1. Limitations of approximate ANALYZE
The content in the sqlite_stat4 table cannot be computed with
anything less than a full scan.  Hence, if a non-zero analysis limit
is specified, the sqlite_stat4 table is not computed.
This page was last updated on 2025-05-31 13:08:22Z 
