Online Backup API.
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
SQLite C Interface
Online Backup API.
sqlite3_backup *sqlite3_backup_init(
  sqlite3 *pDest,                        /* Destination database handle */
  const char *zDestName,                 /* Destination database name */
  sqlite3 *pSource,                      /* Source database handle */
  const char *zSourceName                /* Source database name */
);
int sqlite3_backup_step(sqlite3_backup *p, int nPage);
int sqlite3_backup_finish(sqlite3_backup *p);
int sqlite3_backup_remaining(sqlite3_backup *p);
int sqlite3_backup_pagecount(sqlite3_backup *p);
The backup API copies the content of one database into another.
It is useful either for creating backups of databases or
for copying in-memory databases to or from persistent files.
See Also: Using the SQLite Online Backup API
SQLite holds a write transaction open on the destination database file
for the duration of the backup operation.
The source database is read-locked only while it is being read;
it is not locked continuously for the entire backup operation.
Thus, the backup may be performed on a live source database without
preventing other database connections from
reading or writing to the source database while the backup is underway.
To perform a backup operation:
sqlite3_backup_init() is called once to initialize the
backup,
sqlite3_backup_step() is called one or more times to transfer
the data between the two databases, and finally
sqlite3_backup_finish() is called to release all resources
associated with the backup operation.
There should be exactly one call to sqlite3_backup_finish() for each
successful call to sqlite3_backup_init().
 sqlite3_backup_init()
The D and N arguments to sqlite3_backup_init(D,N,S,M) are the
database connection associated with the destination database
and the database name, respectively.
The database name is "main" for the main database, "temp" for the
temporary database, or the name specified after the AS keyword in
an ATTACH statement for an attached database.
The S and M arguments passed to
sqlite3_backup_init(D,N,S,M) identify the database connection
and database name of the source database, respectively.
The source and destination database connections (parameters S and D)
must be different or else sqlite3_backup_init(D,N,S,M) will fail with
an error.
A call to sqlite3_backup_init() will fail, returning NULL, if
there is already a read or read-write transaction open on the
destination database.
If an error occurs within sqlite3_backup_init(D,N,S,M), then NULL is
returned and an error code and error message are stored in the
destination database connection D.
The error code and message for the failed call to sqlite3_backup_init()
can be retrieved using the sqlite3_errcode(), sqlite3_errmsg(), and/or
sqlite3_errmsg16() functions.
A successful call to sqlite3_backup_init() returns a pointer to an
sqlite3_backup object.
The sqlite3_backup object may be used with the sqlite3_backup_step() and
sqlite3_backup_finish() functions to perform the specified backup
operation.
 sqlite3_backup_step()
Function sqlite3_backup_step(B,N) will copy up to N pages between
the source and destination databases specified by sqlite3_backup object B.
If N is negative, all remaining source pages are copied.
If sqlite3_backup_step(B,N) successfully copies N pages and there
are still more pages to be copied, then the function returns SQLITE_OK.
If sqlite3_backup_step(B,N) successfully finishes copying all pages
from source to destination, then it returns SQLITE_DONE.
If an error occurs while running sqlite3_backup_step(B,N),
then an error code is returned. As well as SQLITE_OK and
SQLITE_DONE, a call to sqlite3_backup_step() may return SQLITE_READONLY,
SQLITE_NOMEM, SQLITE_BUSY, SQLITE_LOCKED, or an
SQLITE_IOERR_XXX extended error code.
The sqlite3_backup_step() might return SQLITE_READONLY if
 the destination database was opened read-only, or
 the destination database is using write-ahead-log journaling
and the destination and source page sizes differ, or
 the destination database is an in-memory database and the
destination and source page sizes differ.
If sqlite3_backup_step() cannot obtain a required file-system lock, then
the busy-handler function
is invoked (if one is specified). If the
busy-handler returns non-zero before the lock is available, then
SQLITE_BUSY is returned to the caller. In this case the call to
sqlite3_backup_step() can be retried later. If the source
database connection
is being used to write to the source database when sqlite3_backup_step()
is called, then SQLITE_LOCKED is returned immediately. Again, in this
case the call to sqlite3_backup_step() can be retried later on. If
SQLITE_IOERR_XXX, SQLITE_NOMEM, or
SQLITE_READONLY is returned, then
there is no point in retrying the call to sqlite3_backup_step(). These
errors are considered fatal.  The application must accept
that the backup operation has failed and pass the backup operation handle
to the sqlite3_backup_finish() to release associated resources.
The first call to sqlite3_backup_step() obtains an exclusive lock
on the destination file. The exclusive lock is not released until either
sqlite3_backup_finish() is called or the backup operation is complete
and sqlite3_backup_step() returns SQLITE_DONE.  Every call to
sqlite3_backup_step() obtains a shared lock on the source database that
lasts for the duration of the sqlite3_backup_step() call.
Because the source database is not locked between calls to
sqlite3_backup_step(), the source database may be modified mid-way
through the backup process.  If the source database is modified by an
external process or via a database connection other than the one being
used by the backup operation, then the backup will be automatically
restarted by the next call to sqlite3_backup_step(). If the source
database is modified by using the same database connection as is used
by the backup operation, then the backup database is automatically
updated at the same time.
 sqlite3_backup_finish()
When sqlite3_backup_step() has returned SQLITE_DONE, or when the
application wishes to abandon the backup operation, the application
should destroy the sqlite3_backup by passing it to sqlite3_backup_finish().
The sqlite3_backup_finish() interfaces releases all
resources associated with the sqlite3_backup object.
If sqlite3_backup_step() has not yet returned SQLITE_DONE, then any
active write-transaction on the destination database is rolled back.
The sqlite3_backup object is invalid
and may not be used following a call to sqlite3_backup_finish().
The value returned by sqlite3_backup_finish is SQLITE_OK if no
sqlite3_backup_step() errors occurred, regardless of whether or not
sqlite3_backup_step() completed.
If an out-of-memory condition or IO error occurred during any prior
sqlite3_backup_step() call on the same sqlite3_backup object, then
sqlite3_backup_finish() returns the corresponding error code.
A return of SQLITE_BUSY or SQLITE_LOCKED from sqlite3_backup_step()
is not a permanent error and does not affect the return value of
sqlite3_backup_finish().
sqlite3_backup_remaining() and sqlite3_backup_pagecount()
The sqlite3_backup_remaining() routine returns the number of pages still
to be backed up at the conclusion of the most recent sqlite3_backup_step().
The sqlite3_backup_pagecount() routine returns the total number of pages
in the source database at the conclusion of the most recent
sqlite3_backup_step().
The values returned by these functions are only updated by
sqlite3_backup_step(). If the source database is modified in a way that
changes the size of the source database or the number of pages remaining,
those changes are not reflected in the output of sqlite3_backup_pagecount()
and sqlite3_backup_remaining() until after the next
sqlite3_backup_step().
Concurrent Usage of Database Handles
The source database connection may be used by the application for other
purposes while a backup operation is underway or being initialized.
If SQLite is compiled and configured to support threadsafe database
connections, then the source database connection may be used concurrently
from within other threads.
However, the application must guarantee that the destination
database connection is not passed to any other API (by any thread) after
sqlite3_backup_init() is called and before the corresponding call to
sqlite3_backup_finish().  SQLite does not currently check to see
if the application incorrectly accesses the destination database connection
and so no error code is reported, but the operations may malfunction
nevertheless.  Use of the destination database connection while a
backup is in progress might also cause a mutex deadlock.
If running in shared cache mode, the application must
guarantee that the shared cache used by the destination database
is not accessed while the backup is running. In practice this means
that the application must guarantee that the disk file being
backed up to is not accessed by any connection within the process,
not just the specific connection that was passed to sqlite3_backup_init().
The sqlite3_backup object itself is partially threadsafe. Multiple
threads may safely make multiple concurrent calls to sqlite3_backup_step().
However, the sqlite3_backup_remaining() and sqlite3_backup_pagecount()
APIs are not strictly speaking threadsafe. If they are invoked at the
same time as another thread is invoking sqlite3_backup_step() it is
possible that they return invalid values.
Alternatives To Using The Backup API
Other techniques for safely creating a consistent backup of an SQLite
database include:
 The VACUUM INTO command.
 The sqlite3_rsync utility program.
See also lists of
  Objects,
  Constants, and
  Functions.
