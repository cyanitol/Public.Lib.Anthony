Pragma statements supported by SQLite
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
PRAGMA Statements
The PRAGMA statement is an SQL extension specific to SQLite and used to 
modify the operation of the SQLite library or to query the SQLite library for 
internal (non-table) data. The PRAGMA statement is issued using the same
interface as other SQLite commands (e.g. SELECT, INSERT) but is
different in the following important respects:
The pragma command is specific to SQLite and is
    not compatible with any other SQL database engine.
Specific pragma statements may be removed and others added in future
    releases of SQLite. There is no guarantee of backwards compatibility.
No error messages are generated if an unknown pragma is issued.
    Unknown pragmas are simply ignored. This means if there is a typo in 
    a pragma statement the library does not inform the user of the fact.
Some pragmas take effect during the SQL compilation stage, not the
    execution stage. This means if using the C-language sqlite3_prepare(), 
    sqlite3_step(), sqlite3_finalize() API (or similar in a wrapper 
    interface), the pragma may run during the sqlite3_prepare() call,
    not during the sqlite3_step() call as normal SQL statements do.
    Or the pragma might run during sqlite3_step() just like normal
    SQL statements.  Whether or not the pragma runs during sqlite3_prepare()
    or sqlite3_step() depends on the pragma and on the specific release
    of SQLite.
The EXPLAIN and EXPLAIN QUERY PLAN prefixes to SQL statements
    only affect the behavior of the statement during sqlite3_step().
    That means that PRAGMA statements that take effect during
    sqlite3_prepare() will behave the same way regardless of whether or
    not they are prefaced by "EXPLAIN".
The C-language API for SQLite provides the SQLITE_FCNTL_PRAGMA
file control which gives VFS implementations the
opportunity to add new PRAGMA statements or to override the meaning of
built-in PRAGMA statements.
PRAGMA command syntax
pragma-stmt:
hide
PRAGMA
schema-name
.
pragma-name
(
pragma-value
)
=
pragma-value
pragma-value:
hide
signed-number
name
signed-literal
signed-number:
show
+
numeric-literal
-
A pragma can take either zero or one argument.  The argument may be either
in parentheses or it may be separated from the pragma name by an equal sign.
The two syntaxes yield identical results.
In many pragmas, the argument is a boolean.  The boolean can be one of:
1 yes true on0 no false off
Keyword arguments can optionally appear in quotes.  
(Example:  'yes' &#91;FALSE&#93;.) Some pragmas
take a string literal as their argument.  When a pragma takes a keyword
argument, it will usually also take a numeric equivalent as well.
For example, "0" and "no" mean the same thing, as does "1" and "yes".
When querying the value of a setting, many pragmas return the number
rather than the keyword.
A pragma may have an optional schema-name
before the pragma name.
The schema-name is the name of an ATTACH-ed database 
or "main" or "temp" for the main and the TEMP databases.  If the optional
schema name is omitted, "main" is assumed.  In some pragmas, the schema
name is meaningless and is simply ignored.  In the documentation below,
pragmas for which the schema name is meaningful are shown with a
"schema." prefix.
PRAGMA functions
PRAGMAs that return results and that have no side-effects can be 
accessed from ordinary SELECT statements as table-valued functions.
For each participating PRAGMA, the corresponding table-valued function
has the same name as the PRAGMA with a 7-character "pragma_" prefix.
The PRAGMA argument and schema, if any, are passed as arguments to the
table-valued function, with the schema as an optional, last argument.
For example, information about the columns in an index can be
read using the index_info pragma as follows:
PRAGMA index_info('idx52');
Or, the same content can be read using:
SELECT * FROM pragma_index_info('idx52');
The advantage of the table-valued function format is that the query
can return just a subset of the PRAGMA columns, can include a WHERE clause,
can use aggregate functions, and the table-valued function can be just
one of several data sources in a join.
For example, to get a list of all indexed columns in a schema, one
could query:
SELECT DISTINCT m.name || '.' || ii.name AS 'indexed-columns'
  FROM sqlite_schema AS m,
       pragma_index_list(m.name) AS il,
       pragma_index_info(il.name) AS ii
 WHERE m.type='table'
 ORDER BY 1;
Additional notes:
Table-valued functions exist only for built-in PRAGMAs, not for PRAGMAs
defined using the SQLITE_FCNTL_PRAGMA file control.
Table-valued functions exist only for PRAGMAs that return results and
that have no side-effects.
This feature could be used to implement
information schema
by first creating a separate schema using
ATTACH ':memory:' AS 'information_schema';
Then creating
VIEWs in that schema that implement the official information schema
tables using table-valued PRAGMA functions.
The table-valued functions for PRAGMA feature was added
in SQLite version 3.16.0 (2017-01-02).  Prior versions of SQLite
cannot use this feature.
List Of PRAGMAs
analysis_limit
application_id
auto_vacuum
automatic_index
busy_timeout
cache_size
cache_spill
case_sensitive_like&sup1;
cell_size_check
checkpoint_fullfsync
collation_list
compile_options
count_changes&sup1;
data_store_directory&sup1;
data_version
database_list
default_cache_size&sup1;
defer_foreign_keys
empty_result_callbacks&sup1;
encoding
foreign_key_check
foreign_key_list
foreign_keys
freelist_count
full_column_names&sup1;
fullfsync
function_list
hard_heap_limit
ignore_check_constraints
incremental_vacuum
index_info
index_list
index_xinfo
integrity_check
journal_mode
journal_size_limit
legacy_alter_table
legacy_file_format
locking_mode
max_page_count
mmap_size
module_list
optimize
page_count
page_size
parser_trace&sup2;
pragma_list
query_only
quick_check
read_uncommitted
recursive_triggers
reverse_unordered_selects
schema_version&sup3;
secure_delete
short_column_names&sup1;
shrink_memory
soft_heap_limit
stats&sup3;
synchronous
table_info
table_list
table_xinfo
temp_store
temp_store_directory&sup1;
threads
trusted_schema
user_version
vdbe_addoptrace&sup2;
vdbe_debug&sup2;
vdbe_listing&sup2;
vdbe_trace&sup2;
wal_autocheckpoint
wal_checkpoint
writable_schema&sup3;
Notes:
Pragmas whose names are struck through
are deprecated. Do not use them. They exist
for historical compatibility.
These pragmas are only available in builds using non-standard
compile-time options.
These pragmas are used for testing SQLite and are not recommended
for use in application programs.
 PRAGMA analysis_limit
   PRAGMA analysis_limit;
        PRAGMA analysis_limit = N;
   Query or change a limit on the approximate ANALYZE setting.
      This is the approximate number of
      rows examined in each index by the ANALYZE command.
      If the argument N is omitted, then the analysis limit
      is unchanged.
      If the limit is zero, then the analysis limit is disabled and
      the ANALYZE command will examine all rows of each index.
      If N is greater than zero, then the analysis limit is set to N
      and subsequent ANALYZE commands will stop analyzing
      each index after it has examined approximately N rows.
      If N is a negative number or something other than an integer value,
      then the pragma behaves as if the N argument was omitted.
      In all cases, the value returned is the new analysis limit used
      for subsequent ANALYZE commands.
   This pragma can be used to help the ANALYZE command run faster
      on large databases.  The results of analysis are not as good
      when only part of each index is examined, but the results are
      usually good enough.  Setting N to 100 or 1000 allows the
      ANALYZE command to run quickly, even on enormous
      database files.
   This pragma was added in SQLite version 3.32.0 (2020-05-22).
      The current implementation only uses the lower 31 bits of the
      N value - higher order bits are silently ignored.  Future versions
      of SQLite might begin using higher order bits.
   Beginning with SQLite version 3.46.0 (2024-05-23),
      the recommended way of running ANALYZE is with the
      PRAGMA optimize command.  The PRAGMA optimize will automatically
      set a reasonable, temporary analysis limit that ensures that the
      PRAGMA optimize command will finish quickly even on enormous
      databases.  Applications that use the PRAGMA optimize instead of
      running ANALYZE directly do not need to set an analysis limit.
 PRAGMA application_id
    PRAGMA schema.application_id;
     PRAGMA schema.application_id = integer ;
    The application_id PRAGMA is used to query or set the 32-bit
       signed big-endian "Application ID" integer located at offset
       68 into the database header.  Applications that use SQLite as their
       application file-format should set the Application ID integer to
       a unique integer so that utilities such as 
       file(1) can determine the specific
       file type rather than just reporting "SQLite3 Database".  A list of
       assigned application IDs can be seen by consulting the
       magic.txt file in the SQLite source repository.
   See also the user_version pragma.
 PRAGMA auto_vacuum
    PRAGMA schema.auto_vacuum;
          PRAGMA schema.auto_vacuum = 
           0 | NONE | 1 | FULL | 2 | INCREMENTAL;
    Query or set the auto-vacuum status in the database.
    The default setting for auto-vacuum is 0 or "none",
    unless the SQLITE_DEFAULT_AUTOVACUUM compile-time option is used.
    The "none" setting means that auto-vacuum is disabled.
    When auto-vacuum is disabled and data is deleted from a database,
    the database file remains the same size.  Unused database file 
    pages are added to a "freelist" and reused for subsequent inserts.  So
    no database file space is lost.  However, the database file does not
    shrink.  In this mode the VACUUM
    command can be used to rebuild the entire database file and
    thus reclaim unused disk space.
    When the auto-vacuum mode is 1  or "full", the freelist pages are
    moved to the end of the database file and the database file is truncated
    to remove the freelist pages at every transaction commit.
    Note, however, that auto-vacuum only truncates the freelist pages
    from the file.  Auto-vacuum does not defragment the database nor
    repack individual database pages the way that the
    VACUUM command does.  In fact, because
    it moves pages around within the file, auto-vacuum can actually
    make fragmentation worse.
    Auto-vacuuming is only possible if the database stores some
    additional information that allows each database page to be
    traced backwards to its referrer.  Therefore, auto-vacuuming must
    be turned on before any tables are created.  It is not possible
    to enable or disable auto-vacuum after a table has been created.
    When the value of auto-vacuum is 2 or "incremental" then the additional
    information needed to do auto-vacuuming is stored in the database file
    but auto-vacuuming does not occur automatically at each commit as it
    does with auto_vacuum=full.  In incremental mode, the separate
    incremental_vacuum pragma must
    be invoked to cause the auto-vacuum to occur.
    The database connection can be changed between full and incremental
    autovacuum mode at any time.  However, changing from
    "none" to "full" or "incremental" can only occur when the database 
    is new (no tables
    have yet been created) or by running the VACUUM command.  To
    change auto-vacuum modes, first use the auto_vacuum pragma to set
    the new desired mode, then invoke the VACUUM command to 
    reorganize the entire database file.  To change from "full" or
    "incremental" back to "none" always requires running VACUUM even
    on an empty database.
    When the auto_vacuum pragma is invoked with no arguments, it
    returns the current auto_vacuum mode.
 PRAGMA automatic_index
    PRAGMA automatic_index;
     PRAGMA automatic_index = boolean;
    Query, set, or clear the automatic indexing capability.
    Automatic indexing is enabled by default as of 
    version 3.7.17 (2013-05-20),
    but this might change in future releases of SQLite.
 PRAGMA busy_timeout
    PRAGMA busy_timeout;
         PRAGMA busy_timeout = milliseconds;
    Query or change the setting of the
    busy timeout.
    This pragma is an alternative to the sqlite3_busy_timeout() C-language
    interface which is made available as a pragma for use with language
    bindings that do not provide direct access to sqlite3_busy_timeout().
    Each database connection can only have a single
    busy handler.  This PRAGMA sets the busy handler
    for the process, possibly overwriting any previously set busy handler.
 PRAGMA cache_size
    PRAGMA schema.cache_size;
       PRAGMA schema.cache_size = pages;
       PRAGMA schema.cache_size = -kibibytes;
    Query or change the suggested maximum number of database disk pages
    that SQLite will hold in memory at once per open database file.  Whether
    or not this suggestion is honored is at the discretion of the
    Application Defined Page Cache.
    The default page cache that is built into SQLite honors the request,
    however alternative application-defined page cache implementations
    may choose to interpret the suggested cache size in different ways
    or to ignore it altogether.
    The default suggested cache size is -2000, which means the cache size
    is limited to 2048000 bytes of memory.
    The default suggested cache size can be altered using the
    SQLITE_DEFAULT_CACHE_SIZE compile-time options.
    The TEMP database has a default suggested cache size of 0 pages.
    If the argument N is positive then the suggested cache size is set 
    to N. If the argument N is negative, then the
    number of cache pages is adjusted to be a number of pages that would
    use approximately abs(N*1024) bytes of memory based on the current
    page size.  SQLite remembers the number of pages in the page cache,
    not the amount of memory used.  So if you set the cache size using
    a negative number and subsequently change the page size (using the
    PRAGMA page_size command) then the maximum amount of cache
    memory will go up or down in proportion to the change in page size.
    Backwards compatibility note:
    The behavior of cache_size with a negative N
    was different prior to version 3.7.10 (2012-01-16).  In
    earlier versions, the number of pages in the cache was set
    to the absolute value of N.
    When you change the cache size using the cache_size pragma, the
    change only endures for the current session.  The cache size reverts
    to the default value when the database is closed and reopened.
    The default page cache implemention does not allocate
    the full amount of cache memory all at once.  Cache memory
    is allocated in smaller chunks on an as-needed basis.  The page_cache
    setting is a (suggested) upper bound on the amount of memory that the
    cache can use, not the amount of memory it will use all of the time.
    This is the behavior of the default page cache implementation, but an
    application defined page cache is free
    to behave differently if it wants.
 PRAGMA cache_spill
    PRAGMA cache_spill;
         PRAGMA cache_spill=boolean;
         PRAGMA schema.cache_spill=N;
    The cache_spill pragma enables or disables the ability of the pager
    to spill dirty cache pages to the database file in the middle of a 
    transaction.  Cache_spill is enabled by default and most applications
    should leave it that way as cache spilling is usually advantageous.
    However, a cache spill has the side-effect of acquiring an
    EXCLUSIVE lock on the database file.  Hence, some applications that
    have large long-running transactions may want to disable cache spilling
    in order to prevent the application from acquiring an exclusive lock
    on the database until the moment that the transaction COMMITs.
    The "PRAGMA cache_spill=N" form of this pragma sets a minimum
    cache size threshold required for spilling to occur. The number of pages
    in cache must exceed both the cache_spill threshold and the maximum cache
    size set by the PRAGMA cache_size statement in order for spilling to
    occur.
    The "PRAGMA cache_spill=boolean" form of this pragma applies
    across all databases attached to the database connection.  But the
    "PRAGMA cache_spill=N" form of this statement only applies to
    the "main" schema or whatever other schema is specified as part of the
    statement.
 PRAGMA case_sensitive_like
    PRAGMA case_sensitive_like = boolean;
    The default behavior of the LIKE operator is to ignore case
    for ASCII characters. Hence, by default 'a' LIKE 'A' is
    true.  The case_sensitive_like pragma installs a new application-defined
    LIKE function that is either case sensitive or insensitive depending
    on the value of the case_sensitive_like pragma.
    When case_sensitive_like is disabled, the default LIKE behavior is
    expressed.  When case_sensitive_like is enabled, case becomes
    significant.  So, for example,
    'a' LIKE 'A' is false but 'a' LIKE 'a' is still true.
    This pragma uses sqlite3_create_function() to overload the
    LIKE and GLOB functions, which may override previous implementations
    of LIKE and GLOB registered by the application.  This pragma
    only changes the behavior of the SQL LIKE operator.  It does not
    change the behavior of the sqlite3_strlike() C-language interface,
    which is always case insensitive.
    WARNING: If a database uses the LIKE operator anywhere in
    the schema, such as in a CHECK constraint or in an
    expression index or in the WHERE clause of a partial index, then
    changing the definition of the LIKE operator using this PRAGMA can
    cause the database to appear to be corrupt.  PRAGMA integrity_check
    will report errors.  The database is not really corrupt in that
    changing the behavior of LIKE back to the way
    it was when the schema was defined and the database was populated
    will clear the problem.   If the use of LIKE occurs only in indexes,
    then the problem can be cleared by running REINDEX.  Nevertheless, 
    the use of the case_sensitive_like pragma is discouraged.
    This pragma is deprecated and exists
    for backwards compatibility only.  New applications
    should avoid using this pragma.  Older applications should discontinue
    use of this pragma at the earliest opportunity.  This pragma may be omitted
    from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.
 PRAGMA cell_size_check
    PRAGMA cell_size_check
       PRAGMA cell_size_check = boolean;
    The cell_size_check pragma enables or disables additional sanity
    checking on database b-tree pages as they are initially read from disk.
    With cell size checking enabled, database corruption is detected earlier
    and is less likely to "spread".  However, there is a small performance
    hit for doing the extra checks and so cell size checking is turned off
    by default.
 PRAGMA checkpoint_fullfsync
    PRAGMA checkpoint_fullfsync
       PRAGMA checkpoint_fullfsync = boolean;
    Query or change the fullfsync flag for checkpoint operations.
