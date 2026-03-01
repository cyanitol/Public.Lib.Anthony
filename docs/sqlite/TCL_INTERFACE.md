The Tcl interface to the SQLite library
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
The Tcl interface to the SQLite library
Table Of Contents
1. Introduction
2. Building And Installing The SQLite Extension for Tcl
2.1. How the build process works
2.2. Additional build documentation
3. The Tcl interface to the SQLite library
3.1. The eval method
3.2. The close method
3.3. The transaction method
3.4. The cache method
3.5. The complete method
3.6. The config method
3.7. The copy method
3.8. The timeout method
3.9. The busy method
3.10. The enable_load_extension method
3.11. The exists method
3.12. The last_insert_rowid method
3.13. The function method
3.14. The nullvalue method
3.15. The onecolumn method
3.16. The changes method
3.17. The total_changes method
3.18. The authorizer method
3.19. The bind_fallback method
3.20. The progress method
3.21. The collate method
3.22. The collation_needed method
3.23. The commit_hook method
3.24. The rollback_hook method
3.25. The status method
3.26. The update_hook method
3.27. The preupdate method
3.28. The wal_hook method
3.29. The incrblob method
3.30. The errorcode method
3.31. The trace method
3.32. The trace_v2 method
3.33. The backup method
3.34. The restore method
3.35. The serialize method
3.36. The deserialize method
3.37. The interrupt method
3.38. The version method
3.39. The profile method
3.40. The unlock_notify method
1. Introduction
The SQLite library is designed to be very easy to use from
a Tcl or Tcl/Tk script.  SQLite
began as a Tcl extension
and the primary test suite for SQLite is written in Tcl and uses
the SQLite extension for Tcl described by this document.  SQLite
can be used with any programming language, but its connections to
Tcl run deep.
This document gives an overview of the Tcl
programming interface for SQLite.
2. Building And Installing The SQLite Extension for Tcl
As of SQLite version 3.48.0 (2025-01-14), the preferred method for
building the SQLite Extension for Tcl is to use the 
canonical SQLite source tree and run the makefile
with the "tclextension-install" target.  If you have multiple Tcl versions
installed, you can specify a particular Tcl installation to install the
SQLite extension on by using the --with-tclsh=PATH command-line option
on unix.  For example, if you have a custom Tcl 9.0 installation at
$HOME/tcl you can install the SQLite for that installation by running:
./configure --with-tclsh=$HOME/tcl/bin/tclsh9.0
make tclextension-install
On Windows, use the TCLDIR environment variable to specify the top-level
directory of your Tcl installation.  Again, if you have a custom Tcl installation
at %HOME%/tcl, your command would be:
nmake /f Makefile.msc TCLDIR=%HOME%\tcl tclextension-install
In both cases, there are additional makefile targets that might be
useful:
tclextension &rarr; Build the SQLite extension but do not
move the files into the installation directory.
tclextension-list &rarr;  Show all SQLite extensions available to
the Tcl interpreter specified to ./configure or by TCLDIR.
tclextension-uninstall &rarr;  Uninstall the SQLite extension.
tclextension-verify &rarr; Verify that the installed SQLite extension
uses the same version of SQLite as source tree from which the command is run.
After you have the SQLite extension installed with your Tcl interpreter
using the techniques above,
you can start it up by running the following command from any tclsh or wish:
package require sqlite3
2.1. How the build process works
The SQLite adaptor for Tcl is implemented by a source-code
file name src/tclsqlite.c.
This one file is appended to the amalgamation ("sqlite3.c") that
implements SQLite to create a new source file
"tclsqlite3.c".  Compile tclsqlite3.c into a shared library in order
to make the run-time loadable Tcl extension that implements SQLite.
The "tclextension-install" make target does that and then also
constructs a suitable pkgIndex.tcl script and moves both the shared
library and the pkgIndex.tcl script into the appropriate installation
directory.
There is a makefile target to build tclsqlite3.c:
make tclsqlite3.c
The tclsqlite3.c file does not have to be compiled into a shared
library.  The tclsqlite3.c library can be statically linked into an
application that also statically links the Tcl library, to construct
a stand-alone executable.  The "testfixture" executable used for testing
SQLite works this way.
2.2. Additional build documentation
See the src/tcl-extension-testing.md
document in the SQLite sources for step-by-step instructions on how
the SQLite extension for TCL is tested.  That document might provide further
hints on how to build the SQLite extension for Tcl should you run into trouble.
3. The Tcl interface to the SQLite library
The SQLite extension for Tcl consists of a single new
Tcl command named sqlite3.
Because there is only this
one command, the interface is not placed in a separate
namespace.
The sqlite3 command is mostly used as follows
to open or create a database:
sqlite3  dbcmd  ?database-name?  ?options?
To get information only, the sqlite3 command may be given exactly
one argument, either "-version", "-sourceid" or "-has-codec", which will
return the specified datum with no other effect.
With other arguments, the sqlite3 command opens the database
named in the second non-option argument, or named "" if there is no such.
If the open succeeds, a new Tcl command named by the first argument is created
and "" is returned.
(This approach is similar to the way widgets are created in Tk.)
If the open fails, an error is raised without creating a Tcl command
and an error message string is returned.
If the database does not already exist, the default behavior
is for it to be created automatically (though this can be changed by
using the "-create false" option).
The name of the database is usually just the name of a disk file in which
the database is stored.  If the name of the database is
the special name ":memory:", then a new database is created
in memory.  If the name of the database is an empty string, then
the database is created in an empty file that is automatically deleted
when the database connection closes.  URI filenames can be used if
the "-uri yes" option is supplied on the sqlite3 command.
Options understood by the sqlite3 command include:
-create BOOLEAN
If true, then a new database is created if one does not already exist.
If false, then an attempt to open a database file that does not previously
exist raises an error.  The default behavior is "true".
-nomutex BOOLEAN
If true, then all mutexes for the database connection are disabled.
This provides a small performance boost in single-threaded applications.
-readonly BOOLEAN
If true, then open the database file read-only.  If false, then the
database is opened for both reading and writing if filesystem permissions
allow, or for reading only if filesystem write permission is denied
by the operating system.  The default setting is "false".  Note that
if the previous process to have the database did not exit cleanly
and left behind a hot journal, then the write permission is required
to recover the database after opening, and the database cannot be
opened read-only.
-uri BOOLEAN
If true, then interpret the filename argument as a URI filename.  If
false, then the argument is a literal filename.  The default value is
"false".
-vfs VFSNAME
Use an alternative VFS named by the argument.
-fullmutex BOOLEAN
If true, multiple threads can safely attempt to use the database.
If false, such attempts are unsafe. The default value depends upon
how the extension is built.
-nofollow BOOLEAN
If true, and the database name refers to a symbolic link,
it will not be followed to open the true database file.
If false, symbolic links will be followed.
The default is "false".
Once an SQLite database is open, it can be controlled using
methods of the dbcmd.
There are currently 40 methods
defined.
authorizer
backup
bind_fallback
busy
cache
changes
close
collate
collation_needed
commit_hook
complete
config
copy
deserialize
enable_load_extension
errorcode
eval
exists
function
incrblob
interrupt
last_insert_rowid
nullvalue
onecolumn
preupdate
profile
progress
restore
rollback_hook
serialize
status
timeout
total_changes
trace
trace_v2
transaction
unlock_notify
update_hook
version
wal_hook
The use of each of these methods will be explained in the sequel, though
not in the order shown above.
3.1. The eval method
The most useful dbcmd method is "eval".  The eval method is used
to execute SQL on the database.  The syntax of the eval method looks
like this:
dbcmd  eval  ?-withoutnulls?
?-asdict?  ?  sql
  var-name?  ?script?
The job of the eval method is to execute the SQL statement or statements
given in the second argument.  For example, to create a new table in
a database, you can do this:
sqlite3 db1 ./testdb
db1 eval {CREATE TABLE t1(a int, b text)}
The above code creates a new table named t1 with columns
a and b.  What could be simpler?
Query results are returned as a list of column values.  If a
query requests 2 columns and there are 3 rows matching the query,
then the returned list will contain 6 elements.  For example:
db1 eval {INSERT INTO t1 VALUES(1,'hello')}
db1 eval {INSERT INTO t1 VALUES(2,'goodbye')}
db1 eval {INSERT INTO t1 VALUES(3,'howdy!')}
set x &#91;db1 eval {SELECT * FROM t1 ORDER BY a}&#93;
The variable $x is set by the above code to
1 hello 2 goodbye 3 howdy!
You can also process the results of a query one row at a time by
specifying the name of an array or, with the -asdict
flag, a dict variable and a script following the SQL code.  For each
row of the query result, the values of all columns will be inserted
into the array resp. dict variable and the script will be executed.
For instance:
db1 eval {SELECT * FROM t1 ORDER BY a} values {
    parray values
    puts ""
}
This last code will give the following output:
values(*) = a b
values(a) = 1
values(b) = hello
values(*) = a b
values(a) = 2
values(b) = goodbye
values(*) = a b
values(a) = 3
values(b) = howdy!
For each column in a row of the result, the name of that column
is used as an index into the array and the value of the column is stored
in the corresponding array entry.  (Caution:  If two or more columns
in the result set of a query have the same name, then the last column
with that name will overwrite prior values and earlier columns with the
same name will be inaccessible.) The special array index * is
used to store a list of column names in the order that they appear.
Normally, NULL SQL results are stored in the array using the
nullvalue setting.  However, if
the -withoutnulls option is used, then NULL SQL values
cause the corresponding array element to be unset instead.
If the array variable name is omitted or is the empty string, then the value of
each column is stored in a variable with the same name as the column
itself.  For example:
db1 eval {SELECT * FROM t1 ORDER BY a} {
    puts "a=$a b=$b"
}
From this we get the following output
a=1 b=hello
a=2 b=goodbye
a=3 b=howdy!
Tcl variable names can appear in the SQL statement of the second argument
in any position where it is legal to put a string or number literal.  The
value of the variable is substituted for the variable name.  If the
variable does not exist a NULL values is used.  For example:
db1 eval {INSERT INTO t1 VALUES(5,$bigstring)}
Note that it is not necessary to quote the $bigstring value.  That happens
automatically.  If $bigstring is a large string or binary object, this
technique is not only easier to write, it is also much more efficient
since it avoids making a copy of the content of $bigstring.
If the $bigstring variable has both a string and a "bytearray" representation,
then TCL inserts the value as a string.  If it has only a "bytearray"
representation, then the value is inserted as a BLOB.  To force a
value to be inserted as a BLOB even if it also has a text representation,
use a "@" character to in place of the "$".  Like this:
db1 eval {INSERT INTO t1 VALUES(5,@bigstring)}
If the variable does not have a bytearray representation, then "@" works
just like "$".  Note that ":" works like "$" in all cases so the following
is another way to express the same statement:
db1 eval {INSERT INTO t1 VALUES(5,:bigstring)}
The use of ":" instead of "$" before the name of a variable can
sometimes be useful if the SQL text is enclosed in double-quotes "..."
instead of curly-braces {...}.
When the SQL is contained within double-quotes "..." then TCL will do
the substitution of $-variables, which can lead to SQL injection if
extreme care is not used.  But TCL will never substitute a :-variable
regardless of whether double-quotes "..." or curly-braces {...} are
used to enclose the SQL, so the use of :-variables adds an extra
measure of defense against SQL
injection.
3.2. The close method
As its name suggests, the "close" method to an SQLite database just
closes the database.  This has the side-effect of deleting the
dbcmd Tcl command.  Here is an example of opening and then
immediately closing a database:
sqlite3 db1 ./testdb
db1 close
If you delete the dbcmd directly, that has the same effect
as invoking the "close" method.  So the following code is equivalent
to the previous:
sqlite3 db1 ./testdb
rename db1 {}
3.3. The transaction method
The "transaction" method is used to execute a TCL script inside an SQLite
database transaction.  The transaction is committed when the script completes,
or it rolls back if the script fails.  If the transaction occurs within
another transaction (even one that is started manually using BEGIN) it
is a no-op.
The transaction command can be used to group together several SQLite
commands in a safe way.  You can always start transactions manually using
BEGIN, of
course.  But if an error occurs so that the COMMIT or ROLLBACK are never
run, then the database will remain locked indefinitely.  Also, BEGIN
does not nest, so you have to make sure no other transactions are active
before starting a new one.  The "transaction" method takes care of
all of these details automatically.
The syntax looks like this:
dbcmd  transaction  ?transaction-type?
  script
The transaction-type can be one of deferred,
exclusive or immediate.  The default is deferred.
3.4. The cache method
The "eval" method described above keeps a cache of
prepared statements
for recently evaluated SQL commands.
The "cache" method is used to control this cache.
The first form of this command is:
dbcmd  cache size  N
This sets the maximum number of statements that can be cached.
The upper limit is 100.  The default is 10.  If you set the cache size
to 0, no caching is done.
The second form of the command is this:
dbcmd  cache flush
The cache-flush method
finalizes
all prepared statements currently
in the cache.
3.5. The complete method
The "complete" method takes a string of supposed SQL as its only argument.
It returns TRUE if the string is a complete statement of SQL and FALSE if
there is more to be entered.
The "complete" method is useful when building interactive applications
in order to know when the user has finished entering a line of SQL code.
This is really just an interface to the
sqlite3_complete() C
function.
3.6. The config method
The "config" method queries or changes certain configuration settings for
the database connection using the sqlite3_db_config() interface.
Run this method with no arguments to get a TCL list of available
configuration settings and their current values:
dbcmd  config
The above will return something like this:
defensive 0 dqs_ddl 1 dqs_dml 1 enable_fkey 0 enable_qpsg 0 enable_trigger 1 enable_view 1 fts3_tokenizer 1 legacy_alter_table 0 legacy_file_format 0 load_extension 0 no_ckpt_on_close 0 reset_database 0 trigger_eqp 0 trusted_schema 1 writable_schema 0
Add the name of an individual configuration setting to query the current
value of that setting.  Optionally add a boolean value to change a setting.
The following four configuration changes are recommended for maximum
application security.  Turning off the trust_schema setting prevents
virtual tables and dodgy SQL functions from being used inside of triggers,
views, CHECK constraints, generated columns, and expression indexes.
Turning off the dqs_dml and dqs_ddl settings prevents the use of
double-quoted strings.  Turning on defensive prevents direct writes
to shadow tables.
db config trusted_schema 0
db config defensive 1
db config dqs_dml 0
db config dqs_ddl 0
3.7. The copy method
The "copy" method copies data from a file into a table.
It returns the number of rows processed successfully from the file.
The syntax of the copy method looks like this:
dbcmd  copy  conflict-algorithm
  table-name   file-name 
    ?column-separator?
  ?null-indicator?
Conflict-algorithm must be one of the SQLite conflict algorithms for
the INSERT statement: rollback, abort,
fail, ignore, or replace. See the SQLite Language
section for ON CONFLICT for
more information. The conflict-algorithm must be specified in lower case.
Table-name must already exist as a table.  File-name must exist, and
each row must contain the same number of columns as defined in the table.
If a line in the file contains more or less than the number of columns defined,
the copy method rollbacks any inserts, and returns an error.
Column-separator is an optional column separator string.  The default is
the ASCII tab character \t. 
Null-indicator is an optional string that indicates a column value is null.
The default is an empty string.  Note that column-separator and
null-indicator are optional positional arguments; if null-indicator
is specified, a column-separator argument must be specified and
precede the null-indicator argument.
The copy method implements similar functionality to the .import
SQLite shell command.
3.8. The timeout method
The "timeout" method is used to control how long the SQLite library
will wait for locks to clear before giving up on a database transaction.
The default timeout is 0 millisecond.  (In other words, the default behavior
is not to wait at all.)
The SQLite database allows multiple simultaneous
readers or a single writer but not both.  If any process is writing to
