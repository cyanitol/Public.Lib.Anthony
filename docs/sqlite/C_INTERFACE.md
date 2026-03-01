The C language interface to SQLite Version 2
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
Editorial Note:
This document describes SQLite version 2, which was deprecated and
replaced by SQLite3 in 2004.
This document is retained as part of the historical record of SQLite.
Modern programmers should refer to
more up-to-date documentation on SQLite is available elsewhere
on this website.
The C language interface to SQLite Version 2
The SQLite library is designed to be very easy to use from
a C or C++ program.  This document gives an overview of the C/C++
programming interface.
1.0 The Core API
The interface to the SQLite library consists of three core functions,
one opaque data structure, and some constants used as return values.
The core interface is as follows:
typedef struct sqlite sqlite;
#define SQLITE_OK           0   /* Successful result */
sqlite *sqlite_open(const char *dbname, int mode, char **errmsg);
void sqlite_close(sqlite *db);
int sqlite_exec(
  sqlite *db,
  char *sql,
  int (*xCallback)(void*,int,char**,char**),
  void *pArg,
  char **errmsg
);
The above is all you really need to know in order to use SQLite
in your C or C++ programs.  There are other interface functions
available (and described below) but we will begin by describing
the core functions shown above.
1.1 Opening a database
Use the sqlite_open function to open an existing SQLite
database or to create a new SQLite database.  The first argument
is the database name.  The second argument is intended to signal
whether the database is going to be used for reading and writing
or just for reading.  But in the current implementation, the
second argument to sqlite_open is ignored.
The third argument is a pointer to a string pointer.
If the third argument is not NULL and an error occurs
while trying to open the database, then an error message will be
written to memory obtained from malloc() and *errmsg will be made
to point to this error message.  The calling function is responsible
for freeing the memory when it has finished with it.
The name of an SQLite database is the name of a file that will
contain the database.  If the file does not exist, SQLite attempts
to create and initialize it.  If the file is read-only (due to
permission bits or because it is located on read-only media like
a CD-ROM) then SQLite opens the database for reading only.  The
entire SQL database is stored in a single file on the disk.  But
additional temporary files may be created during the execution of
an SQL command in order to store the database rollback journal or
temporary and intermediate results of a query.
The return value of the sqlite_open function is a
pointer to an opaque sqlite structure.  This pointer will
be the first argument to all subsequent SQLite function calls that
deal with the same database.  NULL is returned if the open fails
for any reason.
1.2 Closing the database
To close an SQLite database, call the sqlite_close
function passing it the sqlite structure pointer that was obtained
from a prior call to sqlite_open.
If a transaction is active when the database is closed, the transaction
is rolled back.
1.3 Executing SQL statements
The sqlite_exec function is used to process SQL statements
and queries.  This function requires 5 parameters as follows:
A pointer to the sqlite structure obtained from a prior call
       to sqlite_open.
A zero-terminated string containing the text of one or more
       SQL statements and/or queries to be processed.
A pointer to a callback function which is invoked once for each
       row in the result of a query.  This argument may be NULL, in which
       case no callbacks will ever be invoked.
A pointer that is forwarded to become the first argument
       to the callback function.
A pointer to an error string.  Error messages are written to space
       obtained from malloc() and the error string is made to point to
       the malloced space.  The calling function is responsible for freeing
       this space when it has finished with it.
       This argument may be NULL, in which case error messages are not
       reported back to the calling function.
The callback function is used to receive the results of a query.  A
prototype for the callback function is as follows:
int Callback(void *pArg, int argc, char **argv, char **columnNames){
  return 0;
}
The first argument to the callback is just a copy of the fourth argument
to sqlite_exec  This parameter can be used to pass arbitrary
information through to the callback function from client code.
The second argument is the number of columns in the query result.
The third argument is an array of pointers to strings where each string
is a single column of the result for that record.  Note that the
callback function reports a NULL value in the database as a NULL pointer,
which is very different from an empty string.  If the i-th parameter
is an empty string, we will get:
argv&#91;i]&#91;0] == 0
But if the i-th parameter is NULL we will get:
argv&#91;i] == 0
The names of the columns are contained in the first argc
entries of the fourth argument.
If the SHOW_DATATYPES pragma
is on (it is off by default) then
the second argc entries in the 4th argument are the datatypes
for the corresponding columns.
If the 
EMPTY_RESULT_CALLBACKS pragma is set to ON and the result of
a query is an empty set, then the callback is invoked once with the
third parameter (argv) set to 0.  In other words
argv == 0
The second parameter (argc)
and the fourth parameter (columnNames) are still valid
and can be used to determine the number and names of the result
columns if there had been a result.
The default behavior is not to invoke the callback at all if the
result set is empty.
The callback function should normally return 0.  If the callback
function returns non-zero, the query is immediately aborted and 
sqlite_exec will return SQLITE_ABORT.
1.4 Error Codes
The sqlite_exec function normally returns SQLITE_OK.  But
if something goes wrong it can return a different value to indicate
the type of error.  Here is a complete list of the return codes:
#define SQLITE_OK           0   /* Successful result */
#define SQLITE_ERROR        1   /* SQL error or missing database */
#define SQLITE_INTERNAL     2   /* An internal logic error in SQLite */
#define SQLITE_PERM         3   /* Access permission denied */
#define SQLITE_ABORT        4   /* Callback routine requested an abort */
#define SQLITE_BUSY         5   /* The database file is locked */
#define SQLITE_LOCKED       6   /* A table in the database is locked */
#define SQLITE_NOMEM        7   /* A malloc() failed */
#define SQLITE_READONLY     8   /* Attempt to write a readonly database */
#define SQLITE_INTERRUPT    9   /* Operation terminated by sqlite_interrupt() */
#define SQLITE_IOERR       10   /* Some kind of disk I/O error occurred */
#define SQLITE_CORRUPT     11   /* The database disk image is malformed */
#define SQLITE_NOTFOUND    12   /* (Internal Only) Table or record not found */
#define SQLITE_FULL        13   /* Insertion failed because database is full */
#define SQLITE_CANTOPEN    14   /* Unable to open the database file */
#define SQLITE_PROTOCOL    15   /* Database lock protocol error */
#define SQLITE_EMPTY       16   /* (Internal Only) Database table is empty */
#define SQLITE_SCHEMA      17   /* The database schema changed */
#define SQLITE_TOOBIG      18   /* Too much data for one row of a table */
#define SQLITE_CONSTRAINT  19   /* Abort due to constraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_ROW         100  /* sqlite_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite_step() has finished executing */
The meanings of these various return values are as follows:
SQLITE_OK
This value is returned if everything worked and there were no errors.
SQLITE_INTERNAL
This value indicates that an internal consistency check within
the SQLite library failed.  This can only happen if there is a bug in
the SQLite library.  If you ever get an SQLITE_INTERNAL reply from
an sqlite_exec call, please report the problem on the SQLite
mailing list.
SQLITE_ERROR
This return value indicates that there was an error in the SQL
that was passed into the sqlite_exec.
SQLITE_PERM
This return value says that the access permissions on the database
file are such that the file cannot be opened.
SQLITE_ABORT
This value is returned if the callback function returns non-zero.
SQLITE_BUSY
This return code indicates that another program or thread has
the database locked.  SQLite allows two or more threads to read the
database at the same time, but only one thread can have the database
open for writing at the same time.  Locking in SQLite is on the
entire database.
SQLITE_LOCKED
This return code is similar to SQLITE_BUSY in that it indicates
that the database is locked.  But the source of the lock is a recursive
call to sqlite_exec.  This return can only occur if you attempt
to invoke sqlite_exec from within a callback routine of a query
from a prior invocation of sqlite_exec.  Recursive calls to
sqlite_exec are allowed as long as they do
not attempt to write the same table.
SQLITE_NOMEM
This value is returned if a call to malloc fails.
SQLITE_READONLY
This return code indicates that an attempt was made to write to
a database file that is opened for reading only.
SQLITE_INTERRUPT
This value is returned if a call to sqlite_interrupt
interrupts a database operation in progress.
SQLITE_IOERR
This value is returned if the operating system informs SQLite
that it is unable to perform some disk I/O operation.  This could mean
that there is no more space left on the disk.
SQLITE_CORRUPT
This value is returned if SQLite detects that the database it is
working on has become corrupted.  Corruption might occur due to a rogue
process writing to the database file or it might happen due to a
previously undetected logic error in SQLite. This value is also
returned if a disk I/O error occurs in such a way that SQLite is forced
to leave the database file in a corrupted state.  The latter should only
happen due to a hardware or operating system malfunction.
SQLITE_FULL
This value is returned if an insertion failed because there is
no space left on the disk, or the database is too big to hold any
more information.  The latter case should only occur for databases
that are larger than 2GB in size.
SQLITE_CANTOPEN
This value is returned if the database file could not be opened
for some reason.
SQLITE_PROTOCOL
This value is returned if some other process is messing with
file locks and has violated the file locking protocol that SQLite uses
on its rollback journal files.
SQLITE_SCHEMA
When the database is first opened, SQLite reads the database schema
into memory and uses that schema to parse new SQL statements.  If another
process changes the schema, the command currently being processed will
abort because the virtual machine code generated assumed the old
schema.  This is the return code for such cases.  Retrying the
command usually will clear the problem.
SQLITE_TOOBIG
SQLite will not store more than about 1 megabyte of data in a single
row of a single table.  If you attempt to store more than 1 megabyte
in a single row, this is the return code you get.
SQLITE_CONSTRAINT
This constant is returned if the SQL statement would have violated
a database constraint.
SQLITE_MISMATCH
This error occurs when there is an attempt to insert non-integer
data into a column labeled INTEGER PRIMARY KEY.  For most columns, SQLite
ignores the data type and allows any kind of data to be stored.  But
an INTEGER PRIMARY KEY column is only allowed to store integer data.
SQLITE_MISUSE
This error might occur if one or more of the SQLite API routines
is used incorrectly.  Examples of incorrect usage include calling
sqlite_exec after the database has been closed using
sqlite_close or 
calling sqlite_exec with the same
database pointer simultaneously from two separate threads.
SQLITE_NOLFS
This error means that you have attempted to create or access a
database file that is larger than 2GB on a legacy Unix machine that
lacks large file support.
SQLITE_AUTH
This error indicates that the authorizer callback
has disallowed the SQL you are attempting to execute.
SQLITE_ROW
This is one of the return codes from the
sqlite_step routine which is part of the non-callback API.
It indicates that another row of result data is available.
SQLITE_DONE
This is one of the return codes from the
sqlite_step routine which is part of the non-callback API.
It indicates that the SQL statement has been completely executed and
the sqlite_finalize routine is ready to be called.
2.0 Accessing Data Without Using A Callback Function
The sqlite_exec routine described above used to be the only
way to retrieve data from an SQLite database.  But many programmers found
it inconvenient to use a callback function to obtain results.  So beginning
with SQLite version 2.7.7, a second access interface is available that
does not use callbacks.
The new interface uses three separate functions to replace the single
sqlite_exec function.
typedef struct sqlite_vm sqlite_vm;
int sqlite_compile(
  sqlite *db,              /* The open database */
  const char *zSql,        /* SQL statement to be compiled */
  const char **pzTail,     /* OUT: uncompiled tail of zSql */
  sqlite_vm **ppVm,        /* OUT: the virtual machine to execute zSql */
  char **pzErrmsg          /* OUT: Error message. */
);
int sqlite_step(
  sqlite_vm *pVm,          /* The virtual machine to execute */
  int *pN,                 /* OUT: Number of columns in result */
  const char ***pazValue,  /* OUT: Column data */
  const char ***pazColName /* OUT: Column names and datatypes */
);
int sqlite_finalize(
  sqlite_vm *pVm,          /* The virtual machine to be finalized */
  char **pzErrMsg          /* OUT: Error message */
);
The strategy is to compile a single SQL statement using
sqlite_compile then invoke sqlite_step multiple times,
once for each row of output, and finally call sqlite_finalize
to clean up after the SQL has finished execution.
2.1 Compiling An SQL Statement Into A Virtual Machine
The sqlite_compile "compiles" a single SQL statement (specified
by the second parameter) and generates a virtual machine that is able
to execute that statement.  
As with most interface routines, the first parameter must be a pointer
to an sqlite structure that was obtained from a prior call to
sqlite_open.
A pointer to the virtual machine is stored in a pointer which is passed
in as the 4th parameter.
Space to hold the virtual machine is dynamically allocated.  To avoid
a memory leak, the calling function must invoke
sqlite_finalize on the virtual machine after it has finished
with it.
The 4th parameter may be set to NULL if an error is encountered during
compilation.
If any errors are encountered during compilation, an error message is
written into memory obtained from malloc and the 5th parameter
is made to point to that memory.  If the 5th parameter is NULL, then
no error message is generated.  If the 5th parameter is not NULL, then
the calling function should dispose of the memory containing the error
message by calling sqlite_freemem.
If the 2nd parameter actually contains two or more statements of SQL,
only the first statement is compiled.  (This is different from the
behavior of sqlite_exec which executes all SQL statements
in its input string.)  The 3rd parameter to sqlite_compile
is made to point to the first character beyond the end of the first
statement of SQL in the input.  If the 2nd parameter contains only
a single SQL statement, then the 3rd parameter will be made to point
to the '\000' terminator at the end of the 2nd parameter.
On success, sqlite_compile returns SQLITE_OK.
Otherwise an error code is returned.
2.2 Step-By-Step Execution Of An SQL Statement
After a virtual machine has been generated using sqlite_compile
it is executed by one or more calls to sqlite_step.  Each
invocation of sqlite_step, except the last one,
returns a single row of the result.
The number of columns in  the result is stored in the integer that
the 2nd parameter points to.
The pointer specified by the 3rd parameter is made to point
to an array of pointers to column values.
The pointer in the 4th parameter is made to point to an array
of pointers to column names and datatypes.
The 2nd through 4th parameters to sqlite_step convey the
same information as the 2nd through 4th parameters of the
callback routine when using
the sqlite_exec interface.  Except, with sqlite_step
the column datatype information is always included in the
4th parameter regardless of whether or not the
SHOW_DATATYPES pragma
is on or off.
Each invocation of sqlite_step returns an integer code that
indicates what happened during that step.  This code may be
SQLITE_BUSY, SQLITE_ROW, SQLITE_DONE, SQLITE_ERROR, or
SQLITE_MISUSE.
If the virtual machine is unable to open the database file because
it is locked by another thread or process, sqlite_step
will return SQLITE_BUSY.  The calling function should do some other
activity, or sleep, for a short amount of time to give the lock a
chance to clear, then invoke sqlite_step again.  This can
be repeated as many times as desired.
Whenever another row of result data is available,
sqlite_step will return SQLITE_ROW.  The row data is
stored in an array of pointers to strings and the 2nd parameter
is made to point to this array.
When all processing is complete, sqlite_step will return
either SQLITE_DONE or SQLITE_ERROR.  SQLITE_DONE indicates that the
statement completed successfully and SQLITE_ERROR indicates that there
was a run-time error.  (The details of the error are obtained from
sqlite_finalize.)  It is a misuse of the library to attempt
to call sqlite_step again after it has returned SQLITE_DONE
or SQLITE_ERROR.
When sqlite_step returns SQLITE_DONE or SQLITE_ERROR,
the *pN and *pazColName values are set to the number of columns
in the result set and to the names of the columns, just as they
are for an SQLITE_ROW return.  This allows the calling code to
find the number of result columns and the column names and datatypes
even if the result set is empty.  The *pazValue parameter is always
set to NULL when the return code is SQLITE_DONE or SQLITE_ERROR.
If the SQL being executed is a statement that does not
return a result (such as an INSERT or an UPDATE) then *pN will
be set to zero and *pazColName will be set to NULL.
If you abuse the library by trying to call sqlite_step
inappropriately it will attempt return SQLITE_MISUSE.
This can happen if you call sqlite_step() on the same virtual machine
at the same
time from two or more threads or if you call sqlite_step()
again after it returned SQLITE_DONE or SQLITE_ERROR or if you
pass in an invalid virtual machine pointer to sqlite_step().
You should not depend on the SQLITE_MISUSE return code to indicate
an error.  It is possible that a misuse of the interface will go
undetected and result in a program crash.  The SQLITE_MISUSE is
intended as a debugging aid only - to help you detect incorrect
usage prior to a mishap.  The misuse detection logic is not guaranteed
to work in every case.
2.3 Deleting A Virtual Machine
Every virtual machine that sqlite_compile creates should
eventually be handed to sqlite_finalize.  The sqlite_finalize()
procedure deallocates the memory and other resources that the virtual
machine uses.  Failure to call sqlite_finalize() will result in 
resource leaks in your program.
The sqlite_finalize routine also returns the result code
that indicates success or failure of the SQL operation that the
virtual machine carried out.
The value returned by sqlite_finalize() will be the same as would
have been returned had the same SQL been executed by sqlite_exec.
The error message returned will also be the same.
It is acceptable to call sqlite_finalize on a virtual machine
before sqlite_step has returned SQLITE_DONE.  Doing so has
the effect of interrupting the operation in progress.  Partially completed
changes will be rolled back and the database will be restored to its
original state (unless an alternative recovery algorithm is selected using
an ON CONFLICT clause in the SQL being executed.)  The effect is the
same as if a callback function of sqlite_exec had returned
non-zero.
It is also acceptable to call sqlite_finalize on a virtual machine
that has never been passed to sqlite_step even once.
3.0 The Extended API
Only the three core routines described in section 1.0 are required to use
SQLite.  But there are many other functions that provide 
useful interfaces.  These extended routines are as follows:
int sqlite_last_insert_rowid(sqlite*);
int sqlite_changes(sqlite*);
int sqlite_get_table(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg
);
void sqlite_free_table(char**);
void sqlite_interrupt(sqlite*);
int sqlite_complete(const char *sql);
void sqlite_busy_handler(sqlite*, int (*)(void*,const char*,int), void*);
void sqlite_busy_timeout(sqlite*, int ms);
