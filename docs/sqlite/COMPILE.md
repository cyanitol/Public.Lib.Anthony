Compile-time Options
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
Compile-time Options
Table Of Contents
1. Overview
2. Recommended Compile-time Options
3.  Platform Configuration
4.  Options To Set Default Parameter Values
5.  Options To Set Size Limits
6.  Options To Control Operating Characteristics
7.  Options To Enable Features Normally Turned Off
8.  Options To Disable Features Normally Turned On
9.  Options To Omit Features
10.  Analysis and Debugging Options
11.  Windows-Specific Options
12. Compiler Linkage and Calling Convention Control
1. Overview
For most purposes, SQLite can be built just fine using the default
compilation options. However, if required, the compile-time options
documented below can be used to
omit SQLite features (resulting in
a smaller compiled library size) or to change the
default values of some parameters.
Every effort has been made to ensure that the various combinations
of compilation options work harmoniously and produce a working library.
Nevertheless, it is strongly recommended that the SQLite test-suite
be executed to check for errors before using an SQLite library built
with non-standard compilation options.
The compile_options
pragma can be used to determine which of the options listed below
were used in building a given copy of SQLite.
2. Recommended Compile-time Options
The following compile-time options are recommended for applications that
are able to use them, in order to minimized the number of CPU cycles and
the bytes of memory used by SQLite.
Not all of these compile-time options are usable by every application.
For example, the SQLITE_THREADSAFE=0 option is only usable by applications
that never access SQLite from more than one thread at a time.  And the
SQLITE_OMIT_PROGRESS_CALLBACK option is only usable by applications that
do not use the sqlite3_progress_handler() interface.  And so forth.
It is impossible to test every possible combination of compile-time
options for SQLite.  But the following set of compile-time options is
one configuration that is always fully tested.
SQLITE_DQS=0.
This setting disables the double-quoted string literal misfeature.
SQLITE_THREADSAFE=0.
Setting -DSQLITE_THREADSAFE=0 causes all of the mutex and thread-safety logic
in SQLite to be omitted.  This is the single compile-time option causes SQLite
to run about 2% faster and also reduces the size of the library by about 2%.
But the downside is that using the compile-time option means that SQLite can never
be used by more than a single thread at a time, even if each thread has its own
database connection.
SQLITE_DEFAULT_MEMSTATUS=0.
This setting causes the sqlite3_status() interfaces that track memory usage
to be disabled.  This helps the sqlite3_malloc() routines run much faster,
and since SQLite uses sqlite3_malloc() internally, this helps to make the
entire library faster.
SQLITE_DEFAULT_WAL_SYNCHRONOUS=1.
For maximum database safety following a power loss, the setting of
PRAGMA synchronous=FULL is recommended.  However, in WAL mode, complete
database integrity is guaranteed with PRAGMA synchronous=NORMAL.  With
PRAGMA synchronous=NORMAL in WAL mode, recent changes to the database might
be rolled back by a power loss, but the database will not be corrupted.
Furthermore, transaction commit is much faster in WAL mode using
synchronous=NORMAL than with the default synchronous=FULL.  For these
reasons, it is recommended that the synchronous setting be changed from
FULL to NORMAL when switching to WAL mode.  This compile-time option will
accomplish that.
SQLITE_LIKE_DOESNT_MATCH_BLOBS.
Historically, SQLite has allowed BLOB operands to the LIKE and GLOB
operators.  But having a BLOB as an operand of LIKE or GLOB complicates
and slows the LIKE optimization.  When this option is set, it means that
the LIKE and GLOB operators always return FALSE if either operand is a BLOB.
That simplifies the implementation of the LIKE optimization and allows
queries that use the LIKE optimization to run faster.
SQLITE_MAX_EXPR_DEPTH=0.
Setting the maximum expression parse-tree depth to zero disables all checking
of the expression parse-tree depth, which simplifies the code resulting in
faster execution, and helps the parse tree to use less memory.
SQLITE_OMIT_DECLTYPE.
By omitting the (seldom-needed) ability to return the declared type of
columns from the result set of query, prepared statements can be made
to consume less memory.
SQLITE_OMIT_DEPRECATED.
Omitting deprecated interfaces and features will not help SQLite to
run any faster.  It will reduce the library footprint, however.  And
it is the right thing to do.
SQLITE_OMIT_PROGRESS_CALLBACK.
The progress handler callback counter must be checked in the inner loop
of the bytecode engine.  By omitting this interface, a single conditional
is removed from the inner loop of the bytecode engine, helping SQL statements
to run slightly faster.
SQLITE_OMIT_SHARED_CACHE.
Omitting the possibility of using shared cache allows many conditionals
in performance-critical sections of the code to be eliminated.  This can
give a noticeable improvement in performance.
SQLITE_USE_ALLOCA.
Make use of alloca() for dynamically allocating temporary stack space for
use within a single function, on systems that support alloca().  Without
this option, temporary space is allocated from the heap.
SQLITE_OMIT_AUTOINIT.
The SQLite library needs to be initialized using a call to
sqlite3_initialize() before certain interfaces are used.
This initialization normally happens automatically the first time
it is needed.  However, with the SQLITE_OMIT_AUTOINIT option, the automatic
initialization is omitted.  This helps many API calls to run a little faster
(since they do not have to check to see if initialization has already occurred
and then run initialization if it has not previously been invoked) but it
also means that the application must call sqlite3_initialize() manually.
If SQLite is compiled with -DSQLITE_OMIT_AUTOINIT and a routine like
sqlite3_malloc() or sqlite3_vfs_find() or sqlite3_open() is invoked
without first calling sqlite3_initialize(), the likely result will be
a segfault.
SQLITE_STRICT_SUBTYPE=1.
This option causes an error to be raised if an application defined
function that does not have the SQLITE_RESULT_SUBTYPE property
invokes the sqlite3_result_subtype() interface.  The sqlite3_result_subtype()
interface does not work reliably unless the function is registered
with the SQLITE_RESULT_SUBTYPE property.  This compile-time option
is designed to bring this problem to the attention of developers
early.
When all of the recommended compile-time options above are used,
the SQLite library will be approximately 3% smaller and use about 5% fewer
CPU cycles.  So these options do not make a huge difference.  But in
some design situations, every little bit helps.
Library-level configuration options, such as those listed above,
may optionally be defined in a client-side header file. Defining
SQLITE_CUSTOM_INCLUDE=myconfig.h (with no quotes) will cause sqlite3.c
to include myconfig.h early on in the compilation process, enabling
the client to customize the flags without having to explicitly pass
all of them to the compiler.
3.  Platform Configuration
_HAVE_SQLITE_CONFIG_H
  If the _HAVE_SQLITE_CONFIG_H macro is defined
  then the SQLite source code will attempt to #include a file named "sqlite_cfg.h".
  The "sqlite_cfg.h" file usually contains other configuration options, especially
  "HAVE_INTERFACE" type options generated by the configure process. Note that this
  header is intended only for use for platform-level configuration, not library-level
  configuration. To set SQLite-level configuration flags in a custom header, define
  SQLITE_CUSTOM_INCLUDE=myconfig.h, as described in the previous section.
HAVE_FDATASYNC
  If the HAVE_FDATASYNC compile-time option is true, then the default VFS
  for unix systems will attempt to use fdatasync() instead of fsync() where
  appropriate.  If this flag is missing or false, then fsync() is always used.
HAVE_GMTIME_R
  If the HAVE_GMTIME_R option is true and if SQLITE_OMIT_DATETIME_FUNCS is true,
  then the CURRENT_TIME, CURRENT_DATE, and CURRENT_TIMESTAMP keywords will use
  the threadsafe "gmtime_r()" interface rather than "gmtime()".  In the usual case
  where SQLITE_OMIT_DATETIME_FUNCS is not defined or is false, then the
  built-in date and time functions are used to implement the CURRENT_TIME,
  CURRENT_DATE, and CURRENT_TIMESTAMP keywords and neither gmtime_r() nor
  gmtime() is ever called.
HAVE_ISNAN
  If the HAVE_ISNAN option is true, then SQLite invokes the system library isnan()
  function to determine if a double-precision floating point value is a NaN.
  If HAVE_ISNAN is undefined or false, then SQLite substitutes its own home-grown
  implementation of isnan().
HAVE_LOCALTIME_R
  If the HAVE_LOCALTIME_R option is true, then SQLite uses the threadsafe
  localtime_r() library routine instead of localtime()
  to help implement the localtime modifier
  to the built-in date and time functions.
HAVE_LOCALTIME_S
  If the HAVE_LOCALTIME_S option is true, then SQLite uses the threadsafe
  localtime_s() library routine instead of localtime()
  to help implement the localtime modifier
  to the built-in date and time functions.
HAVE_MALLOC_USABLE_SIZE
  If the HAVE_MALLOC_USABLE_SIZE option is true, then SQLite uses the
  malloc_usable_size() interface to find the size of a memory allocation obtained
  from the standard-library malloc() or realloc() routines.  This option is only
  applicable if the standard-library malloc() is used.  On Apple systems,
  "zone malloc" is used instead, and so this option is not applicable.  And, of
  course, if the application supplies its own malloc implementation using
  SQLITE_CONFIG_MALLOC then this option has no effect.
  If the HAVE_MALLOC_USABLE_SIZE option is omitted or is false, then SQLite
  uses a wrapper around system malloc() and realloc() that enlarges each allocation
  by 8 bytes and writes the size of the allocation in the initial 8 bytes, and
  then SQLite also implements its own home-grown version of malloc_usable_size()
  that consults that 8-byte prefix to find the allocation size.  This approach
  works but it is suboptimal.  Applications are encouraged to use
  HAVE_MALLOC_USABLE_SIZE whenever possible.
HAVE_STRCHRNUL
  If the HAVE_STRCHRNUL option is true, then SQLite uses the strchrnul() library
  function.  If this option is missing or false, then SQLite substitutes its own
  home-grown implementation of strchrnul().
HAVE_UTIME
  If the HAVE_UTIME option is true, then the built-in but non-standard
  "unix-dotfile" VFS will use the utime() system call, instead of utimes(),
  to set the last access time on the lock file.
SQLITE_BYTEORDER=(0|1234|4321)
  SQLite needs to know if the native byte order of the target CPU is
  big-endian or little-endian.  The SQLITE_BYTEORDER preprocessor is set
  to 4321 for big-endian machines and 1234 for little-endian machines, or
  it can be 0 to mean that the byte order must be determined at run-time.
  There are #ifdefs in the code that set SQLITE_BYTEORDER automatically
  for all common platforms and compilers.  However, it may be advantageous
  to set SQLITE_BYTEORDER appropriately when compiling SQLite for obscure
  targets.  If the target byte order cannot be determined at compile-time,
  then SQLite falls back to doing run-time checks, which always work, though
  with a small performance penalty.
4.  Options To Set Default Parameter Values
SQLITE_DEFAULT_AUTOMATIC_INDEX=<0 or 1>
  This macro determines the initial setting for PRAGMA automatic_index
  for newly opened database connections.
  For all versions of SQLite through 3.7.17,
  automatic indices are normally enabled for new database connections if
  this compile-time option is omitted.
  However, that might change in future releases of SQLite.
  See also: SQLITE_OMIT_AUTOMATIC_INDEX
SQLITE_DEFAULT_AUTOVACUUM=<0 or 1 or 2>
  This macro determines if SQLite creates databases with the
  auto_vacuum flag set by default to OFF (0), FULL (1), or
  INCREMENTAL (2). The default value is 0 meaning that databases
  are created with auto-vacuum turned off.
  In any case the compile-time default may be overridden by the
  PRAGMA auto_vacuum command.
SQLITE_DEFAULT_CACHE_SIZE=<N>
  This macro sets the default maximum size of the page-cache for each attached
  database.  A positive value means that the limit is N page.  If N is negative
  that means to limit the cache size to -N*1024 bytes.
  The suggested maximum cache size can be overridden by the
  PRAGMA cache_size command. The default value is -2000, which translates
  into a maximum of 2048000 bytes per cache.
SQLITE_DEFAULT_FILE_FORMAT=<1 or 4>
  The default schema format number used by SQLite when creating
  new database files is set by this macro.  The schema formats are all
  very similar.  The difference between formats 1 and 4 is that format
  4 understands descending indices and has a tighter encoding for
  boolean values.
  All versions of SQLite since 3.3.0 (2006-01-10)
  can read and write any schema format
  between 1 and 4.  But older versions of SQLite might not be able to
  read formats greater than 1.  So that older versions of SQLite will
  be able to read and write database files created by newer versions
  of SQLite, the default schema format was set to 1 for SQLite versions
  through 3.7.9 (2011-11-01).  Beginning with
  version 3.7.10 (2012-01-16), the default
  schema format is 4.
  The schema format number for a new database can be set at runtime using
  the PRAGMA legacy_file_format command.
SQLITE_DEFAULT_FILE_PERMISSIONS=N
  The default numeric file permissions for newly created database files
  under unix.  If not specified, the default is 0644 which means that
  the files is globally readable but only writable by the creator.
SQLITE_DEFAULT_FOREIGN_KEYS=<0 or 1>
  This macro determines whether enforcement of
  foreign key constraints is enabled or disabled by default for
  new database connections.  Each database connection can always turn
  enforcement of foreign key constraints on and off at run-time using
  the foreign_keys pragma.  Enforcement of foreign key constraints
  is normally off by default, but if this compile-time parameter is
  set to 1, enforcement of foreign key constraints will be on by default.
SQLITE_DEFAULT_MMAP_SIZE=N
  This macro sets the default limit on the amount of memory that
  will be used for memory-mapped I/O
  for each open database file.  If N
  is zero, then memory mapped I/O is disabled by default.  This
  compile-time limit and the SQLITE_MAX_MMAP_SIZE can be modified
  at start-time using the
  sqlite3_config(SQLITE_CONFIG_MMAP_SIZE) call, or at run-time
  using the mmap_size pragma.
SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT=<bytes>
  This option sets the size limit on rollback journal files in
  persistent journal mode and
  exclusive locking mode or sets the size of the
  write-ahead log file in WAL mode. When this
  compile-time option is omitted there is no upper bound on the
  size of the rollback journals or write-ahead logs.
  The journal file size limit
  can be changed at run-time using the journal_size_limit pragma.
SQLITE_DEFAULT_LOCKING_MODE=<1 or 0>
  If set to 1, then the default locking_mode is set to EXCLUSIVE.
  If omitted or set to 0 then the default locking_mode is NORMAL.
SQLITE_DEFAULT_LOOKASIDE=SZ,N
  Sets the default size of the lookaside memory allocator memory pool
  to N entries of SZ bytes each.  This setting can be modified at
  start-time using sqlite3_config(SQLITE_CONFIG_LOOKASIDE) and/or
  as each database connection is opened using
  sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE).
SQLITE_DEFAULT_MEMSTATUS=<1 or 0>
  This macro is used to determine whether or not the features enabled and
  disabled using the SQLITE_CONFIG_MEMSTATUS argument to sqlite3_config()
  are available by default. The default value is 1 (SQLITE_CONFIG_MEMSTATUS
  related features enabled).
  The sqlite3_memory_used() and sqlite3_memory_highwater() interfaces,
  the sqlite3_status64(SQLITE_STATUS_MEMORY_USED) interface,
  and the SQLITE_MAX_MEMORY compile-time option are all non-functional
  when memory usage tracking is disabled.
SQLITE_DEFAULT_PCACHE_INITSZ=N
  This macro determines the number of pages initially allocated by the
  page cache module when SQLITE_CONFIG_PAGECACHE configuration option is
  not used and memory for the page cache is obtained from sqlite3_malloc()
  instead.  The number of pages set by this macro are allocated in a single
  allocation, which reduces the load on the memory allocator.
SQLITE_DEFAULT_PAGE_SIZE=<bytes>
  This macro is used to set the default page-size used when a
  database is created. The value assigned must be a power of 2. The
  default value is 4096. The compile-time default may be overridden at
  runtime by the PRAGMA page_size command.
SQLITE_DEFAULT_SYNCHRONOUS=<0-3>
  This macro determines the default value of the
  PRAGMA synchronous setting.  If not overridden at compile-time,
  the default setting is 2 (FULL).
SQLITE_DEFAULT_WAL_SYNCHRONOUS=<0-3>
  This macro determines the default value of the
  PRAGMA synchronous setting for database files that open in
  WAL mode.  If not overridden at compile-time, this value is the
  same as SQLITE_DEFAULT_SYNCHRONOUS.
  If SQLITE_DEFAULT_WAL_SYNCHRONOUS differs from SQLITE_DEFAULT_SYNCHRONOUS,
  and if the application has not modified the synchronous setting for
  the database file using the PRAGMA synchronous statement, then
  the synchronous setting is changed to the value defined by
  SQLITE_DEFAULT_WAL_SYNCHRONOUS when the database connection switches
  into WAL mode for the first time.
  If the SQLITE_DEFAULT_WAL_SYNCHRONOUS value is not overridden at
  compile-time, then it will always be the same as
  SQLITE_DEFAULT_SYNCHRONOUS and so no automatic synchronous setting
  changes will ever occur.
SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=<pages>
  This macro sets the default page count for the WAL
  automatic checkpointing feature.  If unspecified,
  the default page count is 1000.
SQLITE_DEFAULT_WORKER_THREADS=N
  This macro sets the default value for
  the maximum number of auxiliary threads that a single
  prepared statement will launch to assist it with a query.  If not specified,
  the default maximum is 0.
  The value set here cannot be more than SQLITE_MAX_WORKER_THREADS.
SQLITE_DQS=N
  This macro determines the default values for
  SQLITE_DBCONFIG_DQS_DDL and SQLITE_DBCONFIG_DQS_DML, which
  in turn determine how SQLite handles each double-quoted string literal.
  The "DQS" name stands for
  "Double-Quoted String".
  The N argument should be an integer 0, 1, 2, or 3.
  SQLITE_DQSDouble-Quoted Strings Allowed
      Remarks
  In DDLIn DML
  3yesyesdefault
  2yesno 
  1noyes 
  0nonorecommended
  The recommended setting is 0, meaning that double-quoted
  strings are disallowed in all contexts.  However, the default
  setting is 3 for maximum compatibility with legacy applications.
SQLITE_EXTRA_DURABLE
  The SQLITE_EXTRA_DURABLE compile-time option that used to cause the default
  PRAGMA synchronous setting to be EXTRA, rather than FULL.  This option
  is no longer supported.  Use
  SQLITE_DEFAULT_SYNCHRONOUS=3 instead.
SQLITE_FTS3_MAX_EXPR_DEPTH=N
  This macro sets the maximum depth of the search tree that corresponds to
  the right-hand side of the MATCH operator in an FTS3 or FTS4 full-text
  index.  The full-text search uses a recursive algorithm, so the depth of
  the tree is limited to prevent using too much stack space.  The default
  limit is 12.  This limit is sufficient for up to 4095 search terms on the
  right-hand side of the MATCH operator and it holds stack space usage to
  less than 2000 bytes.
  For ordinary FTS3/FTS4 queries, the search tree depth is approximately
  the base-2 logarithm of the number of terms in the right-hand side of the
  MATCH operator.  However, for phrase queries and NEAR queries the
  search tree depth is linear in the number of right-hand side terms.
  So the default depth limit of 12 is sufficient for up to 4095 ordinary
  terms on a MATCH, it is only sufficient for 11 or 12 phrase or NEAR
  terms.  Even so, the default is more than enough for most application.
SQLITE_JSON_MAX_DEPTH=N
  This macro sets the maximum nesting depth for JSON objects and arrays.
  The default value is 1000.
  The JSON SQL functions use a
  recursive descent parser.
  This means that deeply nested JSON might require a lot of stack space to
  parse.  On systems with limited stack space, SQLite can be compiled with
  a greatly reduced maximum JSON nesting depth to avoid the possibility of
  a stack overflow, even from hostile inputs.  A value of 10 or 20 is normally
  sufficient even for the most complex real-world JSON.
SQLITE_LIKE_DOESNT_MATCH_BLOBS
  This compile-time option causes the LIKE operator to always return
  False if either operand is a BLOB.  The default behavior of LIKE
  is that BLOB operands are cast to TEXT before the comparison is done.
  This compile-time option makes SQLite run more efficiently when processing
  queries that use the LIKE operator, at the expense of breaking backwards
  compatibility.  However, the backwards compatibility break may be only
  a technicality.  There was a long-standing bug in the LIKE processing logic
  (see https://sqlite.org/src/info/05f43be8fdda9f) that caused it to
  misbehave for BLOB operands and nobody observed that bug in nearly
  10 years of active use.  So for more users, it is probably safe to
  enable this compile-time option and thereby save a little CPU time
  on LIKE queries.
  This compile-time option affects the SQL LIKE operator only and has
  no impact on the sqlite3_strlike() C-language interface.
SQLITE_MAX_MEMORY=N
  This option limits the total amount of memory that SQLite will request
  from malloc() to N bytes.  Any attempt by SQLite to allocate
  new memory that would cause the sum of all allocations held by SQLite to exceed
  N bytes will result in an out-of-memory error.
  This is a hard upper limit.  See also the sqlite3_soft_heap_limit()
  interface.
  This option is a limit on the total amount of memory allocated.
  See the SQLITE_MAX_ALLOCATION_SIZE option for a limitation on the amount
  of memory allowed in any single memory allocation.
  This limit is only functional if memory usage statistics are available via
  the sqlite3_memory_used() and sqlite3_status64(SQLITE_STATUS_MEMORY_USED)
  interfaces.  Without that memory usage information, SQLite has no way of
  knowing when it is about to go over the limit, and thus is unable to prevent
  the excess memory allocation.  Memory usage tracking is turned on by default,
  but can be disabled at compile-time using the SQLITE_DEFAULT_MEMSTATUS option,
  or at start-time using sqlite3_config(SQLITE_CONFIG_MEMSTATUS).
SQLITE_MAX_MMAP_SIZE=N
  This macro sets a hard upper bound on the amount of address space that
  can be used by any single database for memory-mapped I/O.
  Setting this value to 0 completely disables memory-mapped I/O and
  causes logic associated with memory-mapped I/O to be omitted from the
  build.  This option does change the default memory-mapped I/O address
  space size (set by SQLITE_DEFAULT_MMAP_SIZE or
  sqlite3_config(SQLITE_CONFIG_MMAP_SIZE)) or the
  run-time memory-mapped I/O address space size (set by
  sqlite3_file_control(SQLITE_FCNTL_MMAP_SIZE) or
  PRAGMA mmap_size) as long as those other settings are less than the
  maximum value defined here.
