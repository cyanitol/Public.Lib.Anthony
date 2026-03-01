The Virtual Table Mechanism Of SQLite
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
The Virtual Table Mechanism Of SQLite
Table Of Contents
1. Introduction
1.1. Usage
1.1.1. Temporary virtual tables
1.1.2. Eponymous virtual tables
1.1.3. Eponymous-only virtual tables
1.2. Implementation
1.3. Virtual Tables And Shared Cache
1.4. Creating New Virtual Table Implementations
2. Virtual Table Methods
2.1. The xCreate Method
2.1.1. Hidden columns in virtual tables
2.1.2. Table-valued functions
2.1.3.  WITHOUT ROWID Virtual Tables 
2.2. The xConnect Method
2.3. The xBestIndex Method
2.3.1. Inputs
2.3.1.1. LIKE, GLOB, REGEXP, and MATCH functions
2.3.1.2. LIMIT and OFFSET
2.3.1.3. Right-hand side values of constraints
2.3.2. Outputs
2.3.2.1. Omit constraint checking in bytecode
2.3.2.2. ORDER BY and orderByConsumed
2.3.3. Return Value
2.3.4. Enforcing Required Parameters On Table-Valued Functions
2.4. The xDisconnect Method
2.5. The xDestroy Method
2.6. The xOpen Method
2.7. The xClose Method
2.8. The xEof Method
2.9. The xFilter Method
2.10. The xNext Method
2.11. The xColumn Method
2.12. The xRowid Method
2.13. The xUpdate Method
2.14. The xFindFunction Method
2.15. The xBegin Method
2.16. The xSync Method
2.17. The xCommit Method
2.18. The xRollback Method
2.19. The xRename Method
2.20. The xSavepoint, xRelease, and xRollbackTo Methods
2.21. The xShadowName Method
2.22. The xIntegrity Method
1. Introduction
A virtual table is an object that is registered with an open SQLite
database connection. From the perspective of an SQL statement,
the virtual table object looks like any other table or view. 
But behind the scenes, queries and updates on a virtual table
invoke callback methods of the virtual table object instead of
reading and writing on the database file.
The virtual table mechanism allows an application to publish
interfaces that are accessible from SQL statements as if they were
tables. SQL statements can do almost anything to a
virtual table that they can do to a real table, with the following
exceptions:
 One cannot create a trigger on a virtual table.
 One cannot create additional indices on a virtual table. 
     (Virtual tables can have indices but that must be built into
     the virtual table implementation.  Indices cannot be added
     separately using CREATE INDEX statements.)
 One cannot run ALTER TABLE ... ADD COLUMN
     commands against a virtual table.
Individual virtual table implementations might impose additional
constraints. For example, some virtual implementations might provide
read-only tables. Or some virtual table implementations might allow
INSERT or DELETE but not UPDATE.  Or some virtual table implementations
might limit the kinds of UPDATEs that can be made.
A virtual table might represent an in-memory data structure. 
Or it might represent a view of data on disk that is not in the
SQLite format. Or the application might compute the content of the 
virtual table on demand.
Here are some existing and postulated uses for virtual tables:
 A full-text search interface
 Spatial indices using R-Trees
 Introspect the disk content of an SQLite database file
     (the dbstat virtual table)
 Read and/or write the content of a comma-separated value (CSV)
     file
 Access the filesystem of the host computer as if it were a database table
 Enabling SQL manipulation of data in statistics packages like R
See the list of virtual tables page for a longer list of actual
virtual table implementations.
1.1. Usage
A virtual table is created using a CREATE VIRTUAL TABLE statement.
create-virtual-table-stmt:
hide
CREATE
VIRTUAL
TABLE
IF
NOT
EXISTS
schema-name
.
table-name
USING
module-name
(
module-argument
)
,
The CREATE VIRTUAL TABLE statement creates a new table
called table-name derived from the class
module-name.  The module-name
is the name that is registered for the virtual table by
the sqlite3_create_module() interface.
CREATE VIRTUAL TABLE tablename USING modulename;
One can also provide comma-separated arguments to the module following 
the module name:
CREATE VIRTUAL TABLE tablename USING modulename(arg1, arg2, ...);
The format of the arguments to the module is very general. Each 
module-argument
may contain keywords, string literals, identifiers, numbers, and 
punctuation. Each module-argument is passed as 
written (as text) into the
constructor method of the virtual table implementation 
when the virtual 
table is created and that constructor is responsible for parsing and 
interpreting the arguments. The argument syntax is sufficiently general 
that a virtual table implementation can, if it wants to, interpret its
arguments as column definitions in an ordinary CREATE TABLE statement. 
The implementation could also impose some other interpretation on the 
arguments.
Once a virtual table has been created, it can be used like any other 
table with the exceptions noted above and imposed by specific virtual
table implementations. A virtual table is destroyed using the ordinary
DROP TABLE syntax.
1.1.1. Temporary virtual tables
There is no "CREATE TEMP VIRTUAL TABLE" statement.  To create a
temporary virtual table, add the "temp" schema
before the virtual table name.
CREATE VIRTUAL TABLE temp.tablename USING module(arg1, ...);
1.1.2. Eponymous virtual tables
Some virtual tables exist automatically in the "main" schema of
every database connection in which their
module is registered, even without a CREATE VIRTUAL TABLE statement.
Such virtual tables are called "eponymous virtual tables".
To use an eponymous virtual table, simply use the 
module name as if it were a table.
Eponymous virtual tables exist in the "main" schema only, so they will
not work if prefixed with a different schema name.
An example of an eponymous virtual table is the dbstat virtual table.
To use the dbstat virtual table as an eponymous virtual table, 
simply query against the "dbstat"
module name, as if it were an ordinary table.  (Note that SQLite
must be compiled with the SQLITE_ENABLE_DBSTAT_VTAB option to include
the dbstat virtual table in the build.)
SELECT * FROM dbstat;
A virtual table is eponymous if its xCreate method is the exact same
function as the xConnect method, or if the xCreate method is NULL.
The xCreate method is called when a virtual table is first created
using the CREATE VIRTUAL TABLE statement.  The xConnect method 
is invoked whenever
a database connection attaches to or reparses a schema. When these two methods
are the same, that indicates that the virtual table has no persistent
state that needs to be created and destroyed.
1.1.3. Eponymous-only virtual tables
If the xCreate method is NULL, then
CREATE VIRTUAL TABLE statements are prohibited for that virtual table,
and the virtual table is an "eponymous-only virtual table".
Eponymous-only virtual tables are useful as 
table-valued functions.
Note that prior to version 3.9.0 (2015-10-14), 
SQLite did not check the xCreate method
for NULL before invoking it.  So if an eponymous-only virtual table is
registered with SQLite version 3.8.11.1 (2015-07-29)
or earlier and a CREATE VIRTUAL TABLE
command is attempted against that virtual table module, a jump to a NULL
pointer will occur, resulting in a crash.
1.2. Implementation
Several new C-level objects are used by the virtual table implementation:
typedef struct sqlite3_vtab sqlite3_vtab;
typedef struct sqlite3_index_info sqlite3_index_info;
typedef struct sqlite3_vtab_cursor sqlite3_vtab_cursor;
typedef struct sqlite3_module sqlite3_module;
The sqlite3_module structure defines a module object used to implement
a virtual table. Think of a module as a class from which one can 
construct multiple virtual tables having similar properties. For example,
one might have a module that provides read-only access to 
comma-separated-value (CSV) files on disk. That one module can then be
used to create several virtual tables where each virtual table refers
to a different CSV file.
The module structure contains methods that are invoked by SQLite to
perform various actions on the virtual table such as creating new
instances of a virtual table or destroying old ones, reading and
writing data, searching for and deleting, updating, or inserting rows. 
The module structure is explained in more detail below.
Each virtual table instance is represented by an sqlite3_vtab structure. 
The sqlite3_vtab structure looks like this:
struct sqlite3_vtab {
  const sqlite3_module *pModule;
  int nRef;
  char *zErrMsg;
};
Virtual table implementations will normally subclass this structure 
to add additional private and implementation-specific fields. 
The nRef field is used internally by the SQLite core and should not 
be altered by the virtual table implementation. The virtual table 
implementation may pass error message text to the core by putting 
an error message string in zErrMsg.
Space to hold this error message string must be obtained from an
SQLite memory allocation function such as sqlite3_mprintf() or
sqlite3_malloc().
Prior to assigning a new value to zErrMsg, the virtual table 
implementation must free any preexisting content of zErrMsg using 
sqlite3_free(). Failure to do this will result in a memory leak. 
The SQLite core will free and zero the content of zErrMsg when it 
delivers the error message text to the client application or when 
it destroys the virtual table. The virtual table implementation only 
needs to worry about freeing the zErrMsg content when it overwrites 
the content with a new, different error message.
The sqlite3_vtab_cursor structure represents a pointer to a specific
row of a virtual table. This is what an sqlite3_vtab_cursor looks like:
struct sqlite3_vtab_cursor {
  sqlite3_vtab *pVtab;
};
Once again, practical implementations will likely subclass this 
structure to add additional private fields.
The sqlite3_index_info structure is used to pass information into
and out of the xBestIndex method of the module that implements a 
virtual table.
Before a CREATE VIRTUAL TABLE statement can be run, the module 
specified in that statement must be registered with the database 
connection. This is accomplished using either of the sqlite3_create_module()
or sqlite3_create_module_v2() interfaces:
int sqlite3_create_module(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *,    /* Methods for the module */
  void *                     /* Client data for xCreate/xConnect */
);
int sqlite3_create_module_v2(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *,    /* Methods for the module */
  void *,                    /* Client data for xCreate/xConnect */
  void(*xDestroy)(void*)     /* Client data destructor function */
);
The sqlite3_create_module() and sqlite3_create_module_v2()
routines associate a module name with an sqlite3_module 
structure and a separate client data pointer that is specific 
to each module.  The only difference between the two create_module methods
is that the _v2 method includes an extra parameter that specifies a
destructor for client data pointer.  The module structure is what defines
the behavior of a virtual table.  The module structure looks like this:
struct sqlite3_module {
  int iVersion;
  int (*xCreate)(sqlite3*, void *pAux,
               int argc, char *const*argv,
               sqlite3_vtab **ppVTab,
               char **pzErr);
  int (*xConnect)(sqlite3*, void *pAux,
               int argc, char *const*argv,
               sqlite3_vtab **ppVTab,
               char **pzErr);
  int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*);
  int (*xDisconnect)(sqlite3_vtab *pVTab);
  int (*xDestroy)(sqlite3_vtab *pVTab);
  int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
  int (*xClose)(sqlite3_vtab_cursor*);
  int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                int argc, sqlite3_value **argv);
  int (*xNext)(sqlite3_vtab_cursor*);
  int (*xEof)(sqlite3_vtab_cursor*);
  int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context*, int);
  int (*xRowid)(sqlite3_vtab_cursor*, sqlite_int64 *pRowid);
  int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **, sqlite_int64 *);
  int (*xBegin)(sqlite3_vtab *pVTab);
  int (*xSync)(sqlite3_vtab *pVTab);
  int (*xCommit)(sqlite3_vtab *pVTab);
  int (*xRollback)(sqlite3_vtab *pVTab);
  int (*xFindFunction)(sqlite3_vtab *pVtab, int nArg, const char *zName,
                     void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                     void **ppArg);
  int (*xRename)(sqlite3_vtab *pVtab, const char *zNew);
  /* The methods above are in version 1 of the sqlite_module object. Those 
  ** below are for version 2 and greater. */
  int (*xSavepoint)(sqlite3_vtab *pVTab, int);
  int (*xRelease)(sqlite3_vtab *pVTab, int);
  int (*xRollbackTo)(sqlite3_vtab *pVTab, int);
  /* The methods above are in versions 1 and 2 of the sqlite_module object.
  ** Those below are for version 3 and greater. */
  int (*xShadowName)(const char*);
  /* The methods above are in versions 1 through 3 of the sqlite_module object.
  ** Those below are for version 4 and greater. */
  int (*xIntegrity)(sqlite3_vtab *pVTab, const char *zSchema,
                    const char *zTabName, int mFlags, char **pzErr);
};
The module structure defines all of the methods for each virtual 
table object. The module structure also contains the iVersion field which
defines the particular edition of the module table structure. Currently, 
iVersion is always 4 or less, but in future releases of SQLite the module
structure definition might be extended with additional methods and in 
that case the maximum iVersion value will be increased.
The rest of the module structure consists of methods used to implement
various features of the virtual table. Details on what each of these 
methods do are provided in the sequel.
1.3. Virtual Tables And Shared Cache
Prior to SQLite version 3.6.17 (2009-08-10), 
the virtual table mechanism assumes 
that each database connection kept
its own copy of the database schema. Hence, the virtual table mechanism
could not be used in a database that has shared cache mode enabled. 
The sqlite3_create_module() interface would return an error if 
shared cache mode is enabled.  That restriction was relaxed
beginning with SQLite version 3.6.17.
1.4. Creating New Virtual Table Implementations
Follow these steps to create your own virtual table:
 Write all necessary methods.
 Create an instance of the sqlite3_module structure containing pointers
     to all the methods from step 1.
 Register your sqlite3_module structure using one of the
     sqlite3_create_module() or sqlite3_create_module_v2() interfaces.
 Run a CREATE VIRTUAL TABLE command that specifies the new module in 
     the USING clause. 
The only really hard part is step 1. You might want to start with an 
existing virtual table implementation and modify it to suit your needs.
The SQLite source tree
contains many virtual table implementations that are suitable for copying,
including:
 templatevtab.c
&rarr; A virtual table created specifically to serve as a template for
other custom virtual tables.
 series.c
&rarr; Implementation of the generate_series() table-valued function.
 json.c &rarr;
Contains the sources for the json_each() and json_tree() table-valued
functions.
 csv.c &rarr;
A virtual table that reads CSV files.
There are many other virtual table implementations
in the SQLite source tree that can be used as examples.  Locate 
these other virtual table implementations by searching 
for "sqlite3_create_module".
You might also want to implement your new virtual table as a 
loadable extension.
2. Virtual Table Methods
2.1. The xCreate Method
int (*xCreate)(sqlite3 *db, void *pAux,
             int argc, char *const*argv,
             sqlite3_vtab **ppVTab,
             char **pzErr);
The xCreate method is called to create a new instance of a virtual table 
in response to a CREATE VIRTUAL TABLE statement.
If the xCreate method is the same pointer as the xConnect method, then the
virtual table is an eponymous virtual table.
If the xCreate method is omitted (if it is a NULL pointer) then the virtual 
table is an eponymous-only virtual table.
The db parameter is a pointer to the SQLite database connection that 
is executing the CREATE VIRTUAL TABLE statement. 
The pAux argument is the copy of the client data pointer that was the 
fourth argument to the sqlite3_create_module() or
sqlite3_create_module_v2() call that registered the 
virtual table module. 
The argv parameter is an array of argc pointers to null terminated strings. 
The first string, argv[0], is the name of the module being invoked.   The
module name is the name provided as the second argument to 
sqlite3_create_module() and as the argument to the USING clause of the
CREATE VIRTUAL TABLE statement that is running.
The second, argv[1], is the name of the database in which the new virtual 
table is being created. The database name is "main" for the primary database, or
"temp" for TEMP database, or the name given at the end of the ATTACH
statement for attached databases.  The third element of the array, argv[2], 
is the name of the new virtual table, as specified following the TABLE
keyword in the CREATE VIRTUAL TABLE statement.
If present, the fourth and subsequent strings in the argv[] array report 
the arguments to the module name in the CREATE VIRTUAL TABLE statement.
The job of this method is to construct the new virtual table object
(an sqlite3_vtab object) and return a pointer to it in *ppVTab.
As part of the task of creating a new sqlite3_vtab structure, this 
method must invoke sqlite3_declare_vtab() to tell the SQLite 
core about the columns and datatypes in the virtual table. 
The sqlite3_declare_vtab() API has the following prototype:
int sqlite3_declare_vtab(sqlite3 *db, const char *zCreateTable)
The first argument to sqlite3_declare_vtab() must be the same 
database connection pointer as the first parameter to this method.
The second argument to sqlite3_declare_vtab() must a zero-terminated 
UTF-8 string that contains a well-formed CREATE TABLE statement that 
defines the columns in the virtual table and their data types. 
The name of the table in this CREATE TABLE statement is ignored, 
as are all constraints. Only the column names and datatypes matter.
The CREATE TABLE statement string need not be 
held in persistent memory.  The string can be
deallocated and/or reused as soon as the sqlite3_declare_vtab()
routine returns.
The xConnect method can also optionally request special features
for the virtual table by making one or more calls to
the sqlite3_vtab_config() interface:
int sqlite3_vtab_config(sqlite3 *db, int op, ...);
Calls to sqlite3_vtab_config() are optional.  But for maximum
security, it is recommended that virtual table implementations
invoke "sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY)" if the
virtual table will not be used from inside of triggers or views.
The xCreate method need not initialize the pModule, nRef, and zErrMsg
fields of the sqlite3_vtab object.  The SQLite core will take care of 
that chore.
The xCreate should return SQLITE_OK if it is successful in 
creating the new virtual table, or SQLITE_ERROR if it is not successful.
If not successful, the sqlite3_vtab structure must not be allocated. 
An error message may optionally be returned in *pzErr if unsuccessful.
Space to hold the error message string must be allocated using
an SQLite memory allocation function like 
sqlite3_malloc() or sqlite3_mprintf() as the SQLite core will
attempt to free the space using sqlite3_free() after the error has
been reported up to the application.
If the xCreate method is omitted (left as a NULL pointer) then the
virtual table is an eponymous-only virtual table.  New instances of
the virtual table cannot be created using CREATE VIRTUAL TABLE and the
virtual table can only be used via its module name.
Note that SQLite versions prior to 3.9.0 (2015-10-14) do not understand
eponymous-only virtual tables and will segfault if an attempt is made
to CREATE VIRTUAL TABLE on an eponymous-only virtual table because
the xCreate method was not checked for null.
If the xCreate method is the exact same pointer as the xConnect method,
that indicates that the virtual table does not need to initialize backing
store.  Such a virtual table can be used as an eponymous virtual table
