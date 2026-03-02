# PRAGMA Statements
 
The PRAGMA statement is an SQL extension specific to SQLite and used to modify the operation of the SQLite library or to query the SQLite library for internal (non-table) data. The PRAGMA statement is issued using the same interface as other SQLite commands (e.g. SELECT, INSERT) but is different in the following important respects: 
 
 
- The pragma command is specific to SQLite and is not compatible with any other SQL database engine. 
- Specific pragma statements may be removed and others added in future releases of SQLite. There is no guarantee of backwards compatibility. 
- No error messages are generated if an unknown pragma is issued. Unknown pragmas are simply ignored. This means if there is a typo in a pragma statement the library does not inform the user of the fact. 
- Some pragmas take effect during the SQL compilation stage, not the execution stage. This means if using the C-language sqlite3_prepare(), sqlite3_step(), sqlite3_finalize() API (or similar in a wrapper interface), the pragma may run during the sqlite3_prepare() call, not during the sqlite3_step() call as normal SQL statements do. Or the pragma might run during sqlite3_step() just like normal SQL statements. Whether or not the pragma runs during sqlite3_prepare() or sqlite3_step() depends on the pragma and on the specific release of SQLite. 
- The EXPLAIN and EXPLAIN QUERY PLAN prefixes to SQL statements only affect the behavior of the statement during sqlite3_step(). That means that PRAGMA statements that take effect during sqlite3_prepare() will behave the same way regardless of whether or not they are prefaced by "EXPLAIN". 
 The C-language API for SQLite provides the SQLITE_FCNTL_PRAGMA file control which gives VFS implementations the opportunity to add new PRAGMA statements or to override the meaning of built-in PRAGMA statements. 

---


<a name="syntax"></a>
 
## PRAGMA command syntax
 **pragma-stmt:**    **pragma-value:**    **signed-number:**        A pragma can take either zero or one argument. The argument may be either in parentheses or it may be separated from the pragma name by an equal sign. The two syntaxes yield identical results. In many pragmas, the argument is a boolean. The boolean can be one of:   **1 yes true on  
0 no false off**  Keyword arguments can optionally appear in quotes. (Example: `'yes' [FALSE]`.) Some pragmas take a string literal as their argument. When a pragma takes a keyword argument, it will usually also take a numeric equivalent as well. For example, "0" and "no" mean the same thing, as does "1" and "yes". When querying the value of a setting, many pragmas return the number rather than the keyword. A pragma may have an optional schema-name before the pragma name. The schema-name is the name of an ATTACH-ed database or "main" or "temp" for the main and the TEMP databases. If the optional schema name is omitted, "main" is assumed. In some pragmas, the schema name is meaningless and is simply ignored. In the documentation below, pragmas for which the schema name is meaningful are shown with a "*schema.*" prefix. 

---


<a name="pragfunc"></a>
 
## PRAGMA functions
  PRAGMAs that return results and that have no side-effects can be accessed from ordinary SELECT statements as table-valued functions. For each participating PRAGMA, the corresponding table-valued function has the same name as the PRAGMA with a 7-character "pragma_" prefix. The PRAGMA argument and schema, if any, are passed as arguments to the table-valued function, with the schema as an optional, last argument. For example, information about the columns in an index can be read using the index_info pragma as follows: 


```sql
PRAGMA index_info('idx52');
```


 Or, the same content can be read using: 


```sql
SELECT * FROM pragma_index_info('idx52');
```


 The advantage of the table-valued function format is that the query can return just a subset of the PRAGMA columns, can include a WHERE clause, can use aggregate functions, and the table-valued function can be just one of several data sources in a join. For example, to get a list of all indexed columns in a schema, one could query: 


```sql
SELECT DISTINCT m.name || '.' || ii.name AS 'indexed-columns'
  FROM sqlite_schema AS m,
       pragma_index_list(m.name) AS il,
       pragma_index_info(il.name) AS ii
 WHERE m.type='table'
 ORDER BY 1;
```


  Additional notes: 
 
-  Table-valued functions exist only for built-in PRAGMAs, not for PRAGMAs defined using the SQLITE_FCNTL_PRAGMA file control. 
-  Table-valued functions exist only for PRAGMAs that return results and that have no side-effects. 
-  This feature could be used to implement information schema by first creating a separate schema using 


```sql
ATTACH ':memory:' AS 'information_schema';
```


 Then creating VIEWs in that schema that implement the official information schema tables using table-valued PRAGMA functions. 
-  The table-valued functions for PRAGMA feature was added in SQLite version 3.16.0 (2017-01-02). Prior versions of SQLite cannot use this feature. 
 

---


<a name="toc"></a>
 
## List Of PRAGMAs
  - [analysis_limit](#pragma_analysis_limit)
- [application_id](#pragma_application_id)
- [auto_vacuum](#pragma_auto_vacuum)
- [automatic_index](#pragma_automatic_index)
- [busy_timeout](#pragma_busy_timeout)
- [cache_size](#pragma_cache_size)
- [cache_spill](#pragma_cache_spill)
- [~~case_sensitive_like(1)~~](#pragma_case_sensitive_like)
- [cell_size_check](#pragma_cell_size_check)
- [checkpoint_fullfsync](#pragma_checkpoint_fullfsync)
- [collation_list](#pragma_collation_list)
- [compile_options](#pragma_compile_options)
- [~~count_changes(1)~~](#pragma_count_changes)
- [~~data_store_directory(1)~~](#pragma_data_store_directory)
- [data_version](#pragma_data_version)
- [database_list](#pragma_database_list)
- [~~default_cache_size(1)~~](#pragma_default_cache_size)
- [defer_foreign_keys](#pragma_defer_foreign_keys)
- [~~empty_result_callbacks(1)~~](#pragma_empty_result_callbacks)
- [encoding](#pragma_encoding)
- [foreign_key_check](#pragma_foreign_key_check)
- [foreign_key_list](#pragma_foreign_key_list)
- [foreign_keys](#pragma_foreign_keys)
- [freelist_count](#pragma_freelist_count)
- [~~full_column_names(1)~~](#pragma_full_column_names)
- [fullfsync](#pragma_fullfsync)
- [function_list](#pragma_function_list)
- [hard_heap_limit](#pragma_hard_heap_limit)
- [ignore_check_constraints](#pragma_ignore_check_constraints)
- [incremental_vacuum](#pragma_incremental_vacuum)
- [index_info](#pragma_index_info)
- [index_list](#pragma_index_list)
- [index_xinfo](#pragma_index_xinfo)
- [integrity_check](#pragma_integrity_check)
- [journal_mode](#pragma_journal_mode)
- [journal_size_limit](#pragma_journal_size_limit)
- [legacy_alter_table](#pragma_legacy_alter_table)
- [legacy_file_format](#pragma_legacy_file_format)
- [locking_mode](#pragma_locking_mode)
- [max_page_count](#pragma_max_page_count)
- [mmap_size](#pragma_mmap_size)
- [module_list](#pragma_module_list)
- [optimize](#pragma_optimize)
- [page_count](#pragma_page_count)
- [page_size](#pragma_page_size)
- [parser_trace(2)](#pragma_parser_trace)
- [pragma_list](#pragma_pragma_list)
- [query_only](#pragma_query_only)
- [quick_check](#pragma_quick_check)
- [read_uncommitted](#pragma_read_uncommitted)
- [recursive_triggers](#pragma_recursive_triggers)
- [reverse_unordered_selects](#pragma_reverse_unordered_selects)
- [schema_version(3)](#pragma_schema_version)
- [secure_delete](#pragma_secure_delete)
- [~~short_column_names(1)~~](#pragma_short_column_names)
- [shrink_memory](#pragma_shrink_memory)
- [soft_heap_limit](#pragma_soft_heap_limit)
- [stats(3)](#pragma_stats)
- [synchronous](#pragma_synchronous)
- [table_info](#pragma_table_info)
- [table_list](#pragma_table_list)
- [table_xinfo](#pragma_table_xinfo)
- [temp_store](#pragma_temp_store)
- [~~temp_store_directory(1)~~](#pragma_temp_store_directory)
- [threads](#pragma_threads)
- [trusted_schema](#pragma_trusted_schema)
- [user_version](#pragma_user_version)
- [vdbe_addoptrace(2)](#pragma_vdbe_addoptrace)
- [vdbe_debug(2)](#pragma_vdbe_debug)
- [vdbe_listing(2)](#pragma_vdbe_listing)
- [vdbe_trace(2)](#pragma_vdbe_trace)
- [wal_autocheckpoint](#pragma_wal_autocheckpoint)
- [wal_checkpoint](#pragma_wal_checkpoint)
- [writable_schema(3)](#pragma_writable_schema)


Notes: 
 
1. Pragmas whose names are ~~struck through~~ are deprecated. Do not use them. They exist for historical compatibility. 
1. These pragmas are only available in builds using non-standard compile-time options. 
1. These pragmas are used for testing SQLite and are not recommended for use in application programs.
 
<a name="pragma_analysis_limit"></a>
 

---

 **PRAGMA analysis_limit;   
PRAGMA analysis_limit = ***N***;** Query or change a limit on the approximate ANALYZE setting. This is the approximate number of rows examined in each index by the ANALYZE command. If the argument *N* is omitted, then the analysis limit is unchanged. If the limit is zero, then the analysis limit is disabled and the ANALYZE command will examine all rows of each index. If N is greater than zero, then the analysis limit is set to N and subsequent ANALYZE commands will stop analyzing each index after it has examined approximately N rows. If N is a negative number or something other than an integer value, then the pragma behaves as if the N argument was omitted. In all cases, the value returned is the new analysis limit used for subsequent ANALYZE commands. This pragma can be used to help the ANALYZE command run faster on large databases. The results of analysis are not as good when only part of each index is examined, but the results are usually good enough. Setting N to 100 or 1000 allows the ANALYZE command to run quickly, even on enormous database files. This pragma was added in SQLite version 3.32.0 (2020-05-22). The current implementation only uses the lower 31 bits of the N value - higher order bits are silently ignored. Future versions of SQLite might begin using higher order bits. Beginning with SQLite version 3.46.0 (2024-05-23), the recommended way of running ANALYZE is with the PRAGMA optimize command. The PRAGMA optimize will automatically set a reasonable, temporary analysis limit that ensures that the PRAGMA optimize command will finish quickly even on enormous databases. Applications that use the PRAGMA optimize instead of running ANALYZE directly do not need to set an analysis limit. 
<a name="pragma_application_id"></a>
 

---

 **PRAGMA ***schema.***application_id;   
PRAGMA ***schema.***application_id = ***integer ***;** The application_id PRAGMA is used to query or set the 32-bit signed big-endian "Application ID" integer located at offset 68 into the database header. Applications that use SQLite as their application file-format should set the Application ID integer to a unique integer so that utilities such as file(1) can determine the specific file type rather than just reporting "SQLite3 Database". A list of assigned application IDs can be seen by consulting the magic.txt file in the SQLite source repository.  See also the user_version pragma. 
<a name="pragma_auto_vacuum"></a>
 

---

 **PRAGMA ***schema.***auto_vacuum;  
 PRAGMA ***schema.***auto_vacuum = ** *0 | NONE | 1 | FULL | 2 | INCREMENTAL***;** Query or set the auto-vacuum status in the database. The default setting for auto-vacuum is 0 or "none", unless the SQLITE_DEFAULT_AUTOVACUUM compile-time option is used. The "none" setting means that auto-vacuum is disabled. When auto-vacuum is disabled and data is deleted from a database, the database file remains the same size. Unused database file pages are added to a "freelist" and reused for subsequent inserts. So no database file space is lost. However, the database file does not shrink. In this mode the VACUUM command can be used to rebuild the entire database file and thus reclaim unused disk space. When the auto-vacuum mode is 1 or "full", the freelist pages are moved to the end of the database file and the database file is truncated to remove the freelist pages at every transaction commit. Note, however, that auto-vacuum only truncates the freelist pages from the file. Auto-vacuum does not defragment the database nor repack individual database pages the way that the VACUUM command does. In fact, because it moves pages around within the file, auto-vacuum can actually make fragmentation worse. Auto-vacuuming is only possible if the database stores some additional information that allows each database page to be traced backwards to its referrer. Therefore, auto-vacuuming must be turned on before any tables are created. It is not possible to enable or disable auto-vacuum after a table has been created. When the value of auto-vacuum is 2 or "incremental" then the additional information needed to do auto-vacuuming is stored in the database file but auto-vacuuming does not occur automatically at each commit as it does with auto_vacuum=full. In incremental mode, the separate incremental_vacuum pragma must be invoked to cause the auto-vacuum to occur. The database connection can be changed between full and incremental autovacuum mode at any time. However, changing from "none" to "full" or "incremental" can only occur when the database is new (no tables have yet been created) or by running the VACUUM command. To change auto-vacuum modes, first use the auto_vacuum pragma to set the new desired mode, then invoke the VACUUM command to reorganize the entire database file. To change from "full" or "incremental" back to "none" always requires running VACUUM even on an empty database.  When the auto_vacuum pragma is invoked with no arguments, it returns the current auto_vacuum mode. 
<a name="pragma_automatic_index"></a>
 

---

 **PRAGMA automatic_index;   
PRAGMA automatic_index = ***boolean***;** Query, set, or clear the automatic indexing capability. Automatic indexing is enabled by default as of version 3.7.17 (2013-05-20), but this might change in future releases of SQLite. 
<a name="pragma_busy_timeout"></a>
 

---

 **PRAGMA busy_timeout;   
PRAGMA busy_timeout = ***milliseconds***;** Query or change the setting of the busy timeout. This pragma is an alternative to the sqlite3_busy_timeout() C-language interface which is made available as a pragma for use with language bindings that do not provide direct access to sqlite3_busy_timeout(). Each database connection can only have a single busy handler. This PRAGMA sets the busy handler for the process, possibly overwriting any previously set busy handler. 
<a name="pragma_cache_size"></a>
 

---

 **PRAGMA ***schema.***cache_size;   
PRAGMA ***schema.***cache_size = ***pages***;   
PRAGMA ***schema.***cache_size = -***kibibytes***;** Query or change the suggested maximum number of database disk pages that SQLite will hold in memory at once per open database file. Whether or not this suggestion is honored is at the discretion of the Application Defined Page Cache. The default page cache that is built into SQLite honors the request, however alternative application-defined page cache implementations may choose to interpret the suggested cache size in different ways or to ignore it altogether. The default suggested cache size is -2000, which means the cache size is limited to 2048000 bytes of memory. The default suggested cache size can be altered using the SQLITE_DEFAULT_CACHE_SIZE compile-time options. The TEMP database has a default suggested cache size of 0 pages. If the argument N is positive then the suggested cache size is set to N. If the argument N is negative, then the number of cache pages is adjusted to be a number of pages that would use approximately abs(N*1024) bytes of memory based on the current page size. SQLite remembers the number of pages in the page cache, not the amount of memory used. So if you set the cache size using a negative number and subsequently change the page size (using the PRAGMA page_size command) then the maximum amount of cache memory will go up or down in proportion to the change in page size. *Backwards compatibility note:* The behavior of cache_size with a negative N was different prior to version 3.7.10 (2012-01-16). In earlier versions, the number of pages in the cache was set to the absolute value of N. When you change the cache size using the cache_size pragma, the change only endures for the current session. The cache size reverts to the default value when the database is closed and reopened. The default page cache implemention does not allocate the full amount of cache memory all at once. Cache memory is allocated in smaller chunks on an as-needed basis. The page_cache setting is a (suggested) upper bound on the amount of memory that the cache can use, not the amount of memory it will use all of the time. This is the behavior of the default page cache implementation, but an application defined page cache is free to behave differently if it wants. 
<a name="pragma_cache_spill"></a>
 

---

 **PRAGMA cache_spill;   
PRAGMA cache_spill=***boolean***;   
PRAGMA ***schema.***cache_spill=*N*;** The cache_spill pragma enables or disables the ability of the pager to spill dirty cache pages to the database file in the middle of a transaction. Cache_spill is enabled by default and most applications should leave it that way as cache spilling is usually advantageous. However, a cache spill has the side-effect of acquiring an EXCLUSIVE lock on the database file. Hence, some applications that have large long-running transactions may want to disable cache spilling in order to prevent the application from acquiring an exclusive lock on the database until the moment that the transaction COMMITs. The "PRAGMA cache_spill=*N*" form of this pragma sets a minimum cache size threshold required for spilling to occur. The number of pages in cache must exceed both the cache_spill threshold and the maximum cache size set by the PRAGMA cache_size statement in order for spilling to occur. The "PRAGMA cache_spill=*boolean*" form of this pragma applies across all databases attached to the database connection. But the "PRAGMA cache_spill=*N*" form of this statement only applies to the "main" schema or whatever other schema is specified as part of the statement. 
<a name="pragma_case_sensitive_like"></a>
 

---

 **PRAGMA case_sensitive_like = ***boolean***;** The default behavior of the LIKE operator is to ignore case for ASCII characters. Hence, by default **'a' LIKE 'A'** is true. The case_sensitive_like pragma installs a new application-defined LIKE function that is either case sensitive or insensitive depending on the value of the case_sensitive_like pragma. When case_sensitive_like is disabled, the default LIKE behavior is expressed. When case_sensitive_like is enabled, case becomes significant. So, for example, **'a' LIKE 'A'** is false but **'a' LIKE 'a'** is still true. This pragma uses sqlite3_create_function() to overload the LIKE and GLOB functions, which may override previous implementations of LIKE and GLOB registered by the application. This pragma only changes the behavior of the SQL LIKE operator. It does not change the behavior of the sqlite3_strlike() C-language interface, which is always case insensitive. **WARNING:** If a database uses the LIKE operator anywhere in the schema, such as in a CHECK constraint or in an expression index or in the WHERE clause of a partial index, then changing the definition of the LIKE operator using this PRAGMA can cause the database to appear to be corrupt. PRAGMA integrity_check will report errors. The database is not really corrupt in that changing the behavior of LIKE back to the way it was when the schema was defined and the database was populated will clear the problem. If the use of LIKE occurs only in indexes, then the problem can be cleared by running REINDEX. Nevertheless, the use of the case_sensitive_like pragma is discouraged.  **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_cell_size_check"></a>
 

---

 **PRAGMA cell_size_check   
PRAGMA cell_size_check = ***boolean***;** The cell_size_check pragma enables or disables additional sanity checking on database b-tree pages as they are initially read from disk. With cell size checking enabled, database corruption is detected earlier and is less likely to "spread". However, there is a small performance hit for doing the extra checks and so cell size checking is turned off by default. 
<a name="pragma_checkpoint_fullfsync"></a>
 

---

 **PRAGMA checkpoint_fullfsync   
PRAGMA checkpoint_fullfsync = ***boolean***;** Query or change the fullfsync flag for checkpoint operations. If this flag is set, then the F_FULLFSYNC syncing method is used during checkpoint operations on systems that support F_FULLFSYNC. The default value of the checkpoint_fullfsync flag is off. Only Mac OS-X supports F_FULLFSYNC. If the fullfsync flag is set, then the F_FULLFSYNC syncing method is used for all sync operations and the checkpoint_fullfsync setting is irrelevant. 
<a name="pragma_collation_list"></a>
 

---

 **PRAGMA collation_list;** Return a list of the collating sequences defined for the current database connection. 
<a name="pragma_compile_options"></a>
 

---

 **PRAGMA compile_options;** This pragma returns the names of compile-time options used when building SQLite, one option per row. The "SQLITE_" prefix is omitted from the returned option names. See also the sqlite3_compileoption_get() C/C++ interface and the sqlite_compileoption_get() SQL functions. 
<a name="pragma_count_changes"></a>
 

---

 **PRAGMA count_changes;   
PRAGMA count_changes = **boolean***;** Query or change the count-changes flag. Normally, when the count-changes flag is not set, INSERT, UPDATE and DELETE statements return no data. When count-changes is set, each of these commands returns a single row of data consisting of one integer value - the number of rows inserted, modified or deleted by the command. The returned change count does not include any insertions, modifications or deletions performed by triggers, any changes made automatically by foreign key actions, or updates caused by an upsert. Another way to get the row change counts is to use the sqlite3_changes() or sqlite3_total_changes() interfaces. There is a subtle difference, though. When an INSERT, UPDATE, or DELETE is run against a view using an INSTEAD OF trigger, the count_changes pragma reports the number of rows in the view that fired the trigger, whereas sqlite3_changes() and sqlite3_total_changes() do not.  **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_data_store_directory"></a>
 

---

 **PRAGMA data_store_directory;   
PRAGMA data_store_directory = '***directory-name***';** Query or change the value of the sqlite3_data_directory global variable, which windows operating-system interface backends use to determine where to store database files specified using a relative pathname. Changing the data_store_directory setting is *not* threadsafe. Never change the data_store_directory setting if another thread within the application is running any SQLite interface at the same time. Doing so results in undefined behavior. Changing the data_store_directory setting writes to the sqlite3_data_directory global variable and that global variable is not protected by a mutex. This facility is provided for WinRT which does not have an OS mechanism for reading or changing the current working directory. The use of this pragma in any other context is discouraged and may be disallowed in future releases.  **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_data_version"></a>
 

---

 **PRAGMA ***schema.***data_version;** The "PRAGMA data_version" command provides an indication that the database file has been modified. Interactive programs that hold database content in memory or that display database content on-screen can use the PRAGMA data_version command to determine if they need to flush and reload their memory or update the screen display. The integer values returned by two invocations of "PRAGMA data_version" from the same connection will be different if changes were committed to the database by any other connection in the interim. The "PRAGMA data_version" value is unchanged for commits made on the same database connection. The behavior of "PRAGMA data_version" is the same for all database connections, including database connections in separate processes and shared cache database connections. The "PRAGMA data_version" value is a local property of each database connection and so values returned by two concurrent invocations of "PRAGMA data_version" on separate database connections are often different even though the underlying database is identical. It is only meaningful to compare the "PRAGMA data_version" values returned by the same database connection at two different points in time. 
<a name="pragma_database_list"></a>
 

---

 **PRAGMA database_list;** This pragma works like a query to return one row for each database attached to the current database connection. The second column is "main" for the main database file, "temp" for the database file used to store TEMP objects, or the name of the ATTACHed database for other database files. The third column is the name of the database file itself, or an empty string if the database is not associated with a file. 
<a name="pragma_default_cache_size"></a>
 

---

 **PRAGMA ***schema.***default_cache_size;   
PRAGMA ***schema.***default_cache_size = ***Number-of-pages***;** This pragma queries or sets the suggested maximum number of pages of disk cache that will be allocated per open database file. The difference between this pragma and cache_size is that the value set here persists across database connections. The value of the default cache size is stored in the 4-byte big-endian integer located at offset 48 in the header of the database file.   **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_defer_foreign_keys"></a>
 

---

 **PRAGMA defer_foreign_keys   
PRAGMA defer_foreign_keys = ***boolean***;** When the defer_foreign_keys PRAGMA is on, enforcement of all foreign key constraints is delayed until the outermost transaction is committed. The defer_foreign_keys pragma defaults to OFF so that foreign key constraints are only deferred if they are created as "DEFERRABLE INITIALLY DEFERRED". The defer_foreign_keys pragma is automatically switched off at each COMMIT or ROLLBACK. Hence, the defer_foreign_keys pragma must be separately enabled for each transaction. This pragma is only meaningful if foreign key constraints are enabled, of course. The sqlite3_db_status(db,SQLITE_DBSTATUS_DEFERRED_FKS,...) C-language interface can be used during a transaction to determine if there are deferred and unresolved foreign key constraints. 
<a name="pragma_empty_result_callbacks"></a>
 

---

 **PRAGMA empty_result_callbacks;   
PRAGMA empty_result_callbacks = ***boolean***;** Query or change the empty-result-callbacks flag. The empty-result-callbacks flag affects the sqlite3_exec() API only. Normally, when the empty-result-callbacks flag is cleared, the callback function supplied to the sqlite3_exec() is not invoked for commands that return zero rows of data. When empty-result-callbacks is set in this situation, the callback function is invoked exactly once, with the third parameter set to 0 (NULL). This is to enable programs that use the sqlite3_exec() API to retrieve column-names even when a query returns no data.  **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_encoding"></a>
 

---

 **PRAGMA encoding;   
PRAGMA encoding = 'UTF-8';   
PRAGMA encoding = 'UTF-16';   
PRAGMA encoding = 'UTF-16le';   
PRAGMA encoding = 'UTF-16be';** In first form, if the main database has already been created, then this pragma returns the text encoding used by the main database, one of 'UTF-8', 'UTF-16le' (little-endian UTF-16 encoding) or 'UTF-16be' (big-endian UTF-16 encoding). If the main database has not already been created, then the value returned is the text encoding that will be used to create the main database, if it is created by this session. The second through fifth forms of this pragma set the encoding that the main database will be created with if it is created by this session. The string 'UTF-16' is interpreted as "UTF-16 encoding using native machine byte-ordering". It is not possible to change the text encoding of a database after it has been created and any attempt to do so will be silently ignored. If no encoding is first set with this pragma, then the encoding with which the main database will be created defaults to one determined by the API used to open the connection. Once an encoding has been set for a database, it cannot be changed. Databases created by the ATTACH command always use the same encoding as the main database. An attempt to ATTACH a database with a different text encoding from the "main" database will fail. 
<a name="pragma_foreign_key_check"></a>
 

---

 **PRAGMA ***schema.***foreign_key_check;   
PRAGMA ***schema.***foreign_key_check(***table-name***);**** The foreign_key_check pragma checks the database, or the table called "*table-name*", for foreign key constraints that are violated. The foreign_key_check pragma returns one row output for each foreign key violation. There are four columns in each result row. The first column is the name of the table that contains the REFERENCES clause. The second column is the rowid of the row that contains the invalid REFERENCES clause, or NULL if the child table is a WITHOUT ROWID table. The third column is the name of the table that is referred to. The fourth column is the index of the specific foreign key constraint that failed. The fourth column in the output of the foreign_key_check pragma is the same integer as the first column in the output of the foreign_key_list pragma. When a "*table-name*" is specified, the only foreign key constraints checked are those created by REFERENCES clauses in the CREATE TABLE statement for *table-name*. 
<a name="pragma_foreign_key_list"></a>
 

---

 **PRAGMA foreign_key_list(***table-name***);** This pragma returns one row for each foreign key constraint created by a REFERENCES clause in the CREATE TABLE statement of table "*table-name*". 
<a name="pragma_foreign_keys"></a>
 

---

 **PRAGMA foreign_keys;   
PRAGMA foreign_keys = ***boolean***;** Query, set, or clear the enforcement of foreign key constraints. This pragma is a no-op within a transaction; foreign key constraint enforcement may only be enabled or disabled when there is no pending BEGIN or SAVEPOINT. Changing the foreign_keys setting affects the execution of all statements prepared using the database connection, including those prepared before the setting was changed. Any existing statements prepared using the legacy sqlite3_prepare() interface may fail with an SQLITE_SCHEMA error after the foreign_keys setting is changed. As of SQLite version 3.6.19, the default setting for foreign key enforcement is OFF. However, that might change in a future release of SQLite. The default setting for foreign key enforcement can be specified at compile-time using the SQLITE_DEFAULT_FOREIGN_KEYS preprocessor macro. To minimize future problems, applications should set the foreign key enforcement flag as required by the application and not depend on the default setting. 
<a name="pragma_freelist_count"></a>
 

---

 **PRAGMA ***schema.***freelist_count;** Return the number of unused pages in the database file. 
<a name="pragma_full_column_names"></a>
 

---

 **PRAGMA full_column_names;   
PRAGMA full_column_names = ***boolean***;** Query or change the full_column_names flag. This flag together with the short_column_names flag determine the way SQLite assigns names to result columns of SELECT statements. Result columns are named by applying the following rules in order: 
 
1. If there is an AS clause on the result, then the name of the column is the right-hand side of the AS clause. 
1. If the result is a general expression, not just the name of a source table column, then the name of the result is a copy of the expression text. 
1. If the short_column_names pragma is ON, then the name of the result is the name of the source table column without the source table name prefix: COLUMN. 
1. If both pragmas short_column_names and full_column_names are OFF then case (2) applies.  
1. The name of the result column is a combination of the source table and source column name: TABLE.COLUMN 
 
 **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED. 
 
<a name="pragma_fullfsync"></a>
 

---

 
**PRAGMA fullfsync   
PRAGMA fullfsync = ***boolean***;**
 
Query or change the fullfsync flag. This flag determines whether or not the F_FULLFSYNC syncing method is used on systems that support it. The default value of the fullfsync flag is off. Only Mac OS X supports F_FULLFSYNC.
 
See also checkpoint_fullfsync.
 
<a name="pragma_function_list"></a>
 

---

 
**PRAGMA function_list;** 
This pragma returns a list of SQL functions known to the database connection. Each row of the result describes a single calling signature for a single SQL function. Some SQL functions will have multiple rows in the result set if they can (for example) be invoked with a varying number of arguments or can accept text in various encodings. 
<a name="pragma_hard_heap_limit"></a>
 

---

 
**PRAGMA hard_heap_limit  
 PRAGMA hard_heap_limit=***N*
 
This pragma invokes the sqlite3_hard_heap_limit64() interface with the argument N, if N is specified and N is a positive integer that is less than the current hard heap limit. The hard_heap_limit pragma always returns the same integer that would be returned by the sqlite3_hard_heap_limit64(-1) C-language function. That is to say, it always returns the value of the hard heap limit that is set after any changes imposed by this PRAGMA. 
 
This pragma can only lower the heap limit, never raise it. The C-language interface sqlite3_hard_heap_limit64() must be used to raise the heap limit.
 
See also the soft_heap_limit pragma. 
<a name="pragma_ignore_check_constraints"></a>
 

---

 
**PRAGMA ignore_check_constraints = ***boolean***;**
 
This pragma enables or disables the enforcement of CHECK constraints. The default setting is off, meaning that CHECK constraints are enforced by default.
 
<a name="pragma_incremental_vacuum"></a>
 

---

 
**PRAGMA ***schema.***incremental_vacuum***(N)***;  
 PRAGMA ***schema.***incremental_vacuum;**
 
The incremental_vacuum pragma causes up to *N* pages to be removed from the freelist. The database file is truncated by the same amount. The incremental_vacuum pragma has no effect if the database is not in auto_vacuum=incremental mode or if there are no pages on the freelist. If there are fewer than *N* pages on the freelist, or if *N* is less than 1, or if the "(*N*)" argument is omitted, then the entire freelist is cleared.
 
<a name="pragma_index_info"></a>
 

---

 
**PRAGMA ***schema.***index_info(***index-name***);**
 
This pragma returns one row for each key column in the named index. A key column is a column that is actually named in the CREATE INDEX index statement or UNIQUE constraint or PRIMARY KEY constraint that created the index. Index entries also usually contain auxiliary columns that point back to the table row being indexed. The auxiliary index-columns are not shown by the index_info pragma, but they are listed by the index_xinfo pragma.
 
Output columns from the index_info pragma are as follows: 
 
1. The rank of the column within the index. (0 means left-most.) 
1. The rank of the column within the table being indexed. A value of -1 means rowid and a value of -2 means that an expression is being used. 
1. The name of the column being indexed. This columns is NULL if the column is the rowid or an expression. 
 If there is no index named *index-name* but there is a WITHOUT ROWID table with that name, then (as of SQLite version 3.30.0 on 2019-10-04) this pragma returns the PRIMARY KEY columns of the WITHOUT ROWID table as they are used in the records of the underlying b-tree, which is to say with duplicate columns removed. 
<a name="pragma_index_list"></a>
 

---

 **PRAGMA ***schema.***index_list(***table-name***);** This pragma returns one row for each index associated with the given table. Output columns from the index_list pragma are as follows: 
 
1. A sequence number assigned to each index for internal tracking purposes. 
1. The name of the index. 
1. "1" if the index is UNIQUE and "0" if not. 
1. "c" if the index was created by a CREATE INDEX statement, "u" if the index was created by a UNIQUE constraint, or "pk" if the index was created by a PRIMARY KEY constraint. 
1. "1" if the index is a partial index and "0" if not. 
  
<a name="pragma_index_xinfo"></a>
 

---

 **PRAGMA ***schema.***index_xinfo(***index-name***);** This pragma returns information about every column in an index. Unlike this index_info pragma, this pragma returns information about every column in the index, not just the key columns. (A key column is a column that is actually named in the CREATE INDEX index statement or UNIQUE constraint or PRIMARY KEY constraint that created the index. Auxiliary columns are additional columns needed to locate the table entry that corresponds to each index entry.) Output columns from the index_xinfo pragma are as follows: 
 
1. The rank of the column within the index. (0 means left-most. Key columns come before auxiliary columns.) 
1. The rank of the column within the table being indexed, or -1 if the index-column is the rowid of the table being indexed and -2 if the index is on an expression. 
1. The name of the column being indexed, or NULL if the index-column is the rowid of the table being indexed or an expression. 
1. 1 if the index-column is sorted in reverse (DESC) order by the index and 0 otherwise. 
1. The name for the collating sequence used to compare values in the index-column. 
1. 1 if the index-column is a key column and 0 if the index-column is an auxiliary column. 
 If there is no index named *index-name* but there is a WITHOUT ROWID table with that name, then (as of SQLite version 3.30.0 on 2019-10-04) this pragma returns the columns of the WITHOUT ROWID table as they are used in the records of the underlying b-tree, which is to say with de-duplicated PRIMARY KEY columns first followed by data columns. 
<a name="pragma_integrity_check"></a>
 

---

 **PRAGMA ***schema.***integrity_check;   
PRAGMA ***schema.***integrity_check(***N***)   
PRAGMA ***schema.***integrity_check(***TABLENAME***)** This pragma does a low-level formatting and consistency check of the database. The integrity_check pragma look for: 
 
-  Table or index entries that are out of sequence 
-  Misformatted records 
-  Missing pages 
-  Missing or surplus index entries 
-  UNIQUE, CHECK, and NOT NULL constraint errors 
-  Integrity of the freelist 
-  Sections of the database that are used more than once, or not at all 
 If the integrity_check pragma finds problems, strings are returned (as multiple rows with a single column per row) which describe the problems. Pragma integrity_check will return at most *N* errors before the analysis quits, with N defaulting to 100. If pragma integrity_check finds no errors, a single row with the value 'ok' is returned. The usual case is that the entire database file is checked. However, if the argument is *TABLENAME*, then checking is only performed for the the table named and its associated indexes. This is called a "partial integrity check". Because only a subset of the database is checked, errors such as unused sections of the file or duplication use of the same section of the file by two or more tables cannot be detected. The freelist is only verified on a partial integrity check if *TABLENAME* is sqlite_schema or one of its aliases. Support for partial integrity checks was added with version 3.33.0 (2020-08-14). PRAGMA integrity_check does not find FOREIGN KEY errors. Use the PRAGMA foreign_key_check command to find errors in FOREIGN KEY constraints. See also the PRAGMA quick_check command which does most of the checking of PRAGMA integrity_check but runs much faster. 
<a name="pragma_journal_mode"></a>
 

---

 **PRAGMA ***schema.***journal_mode;   
PRAGMA ***schema.***journal_mode = *DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF*** This pragma queries or sets the journal mode for databases associated with the current database connection. The first form of this pragma queries the current journaling mode for *database*. When *database* is omitted, the "main" database is queried. The second form changes the journaling mode for "*database*" or for all attached databases if "*database*" is omitted. The new journal mode is returned. If the journal mode could not be changed, the original journal mode is returned. The DELETE journaling mode is the default. In the DELETE mode, the rollback journal is deleted at the conclusion of each transaction. Indeed, the delete operation is the action that causes the transaction to commit. (See the document titled  Atomic Commit In SQLite for additional detail.) The TRUNCATE journaling mode commits transactions by truncating the rollback journal to zero-length instead of deleting it. On many systems, truncating a file is much faster than deleting the file since the containing directory does not need to be changed. The PERSIST journaling mode prevents the rollback journal from being deleted at the end of each transaction. Instead, the header of the journal is overwritten with zeros. This will prevent other database connections from rolling the journal back. The PERSIST journaling mode is useful as an optimization on platforms where deleting or truncating a file is much more expensive than overwriting the first block of a file with zeros. See also: PRAGMA journal_size_limit and SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT. The MEMORY journaling mode stores the rollback journal in volatile RAM. This saves disk I/O but at the expense of database safety and integrity. If the application using SQLite crashes in the middle of a transaction when the MEMORY journaling mode is set, then the database file will very likely go corrupt. The WAL journaling mode uses a write-ahead log instead of a rollback journal to implement transactions. The WAL journaling mode is persistent; after being set it stays in effect across multiple database connections and after closing and reopening the database. A database in WAL journaling mode can only be accessed by SQLite version 3.7.0 (2010-07-21) or later. The OFF journaling mode disables the rollback journal completely. No rollback journal is ever created and hence there is never a rollback journal to delete. The OFF journaling mode disables the atomic commit and rollback capabilities of SQLite. The ROLLBACK command no longer works; it behaves in an undefined way. Applications must avoid using the ROLLBACK command when the journal mode is OFF. If the application crashes in the middle of a transaction when the OFF journaling mode is set, then the database file will very likely go corrupt. Without a journal, there is no way for a statement to unwind partially completed operations following a constraint error. This might also leave the database in a corrupted state. For example, if a duplicate entry causes a CREATE UNIQUE INDEX statement to fail half-way through, it will leave behind a partially created, and hence corrupt, index. Because OFF journaling mode allows the database file to be corrupted using ordinary SQL, it is disabled when SQLITE_DBCONFIG_DEFENSIVE is enabled. Note that the journal_mode for an in-memory database is either MEMORY or OFF and can not be changed to a different value. An attempt to change the journal_mode of an in-memory database to any setting other than MEMORY or OFF is ignored. Note also that the journal_mode cannot be changed while a transaction is active. 
<a name="pragma_journal_size_limit"></a>
 

---

 ** PRAGMA ***schema.***journal_size_limit  
 PRAGMA ***schema.***journal_size_limit = ***N* **;** If a database connection is operating in exclusive locking mode or in persistent journal mode (PRAGMA journal_mode=persist) then after committing a transaction the rollback journal file may remain in the file-system. This increases performance for subsequent transactions since overwriting an existing file is faster than append to a file, but it also consumes file-system space. After a large transaction (e.g. a VACUUM), the rollback journal file may consume a very large amount of space. Similarly, in WAL mode, the write-ahead log file is not truncated following a checkpoint. Instead, SQLite reuses the existing file for subsequent WAL entries since overwriting is faster than appending. The journal_size_limit pragma may be used to limit the size of rollback-journal and WAL files left in the file-system after transactions or checkpoints. Each time a transaction is committed or a WAL file resets, SQLite compares the size of the rollback journal file or WAL file left in the file-system to the size limit set by this pragma and if the journal or WAL file is larger it is truncated to the limit. The second form of the pragma listed above is used to set a new limit in bytes for the specified database. A negative number implies no limit. To always truncate rollback journals and WAL files to their minimum size, set the journal_size_limit to zero. Both the first and second forms of the pragma listed above return a single result row containing a single integer column - the value of the journal size limit in bytes. The default journal size limit is -1 (no limit). The SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT preprocessor macro can be used to change the default journal size limit at compile-time. This pragma only operates on the single database specified prior to the pragma name (or on the "main" database if no database is specified.) There is no way to change the journal size limit on all attached databases using a single PRAGMA statement. The size limit must be set separately for each attached database. 
<a name="pragma_legacy_alter_table"></a>
 

---

 **PRAGMA legacy_alter_table;   
PRAGMA legacy_alter_table = *boolean*** This pragma sets or queries the value of the legacy_alter_table flag. When this flag is on, the ALTER TABLE RENAME command (for changing the name of a table) works as it did in SQLite 3.24.0 (2018-06-04) and earlier. More specifically, when this flag is on the ALTER TABLE RENAME command only rewrites the initial occurrence of the table name in its CREATE TABLE statement and in any associated CREATE INDEX and CREATE TRIGGER statements. Other references to the table are unmodified, including: 
 
-  References to the table within the bodies of triggers and views. 
-  References to the table within CHECK constraints in the original CREATE TABLE statement. 
-  References to the table within the WHERE clauses of partial indexes. 
 The default setting for this pragma is OFF, which means that all references to the table anywhere in the schema are converted to the new name. This pragma is provided as a work-around for older programs that contain code that expect the incomplete behavior of ALTER TABLE RENAME found in older versions of SQLite. New applications should leave this flag turned off. For compatibility with older virtual table implementations, this flag is turned on temporarily while the sqlite3_module.xRename method is being run. The value of this flag is restored after the sqlite3_module.xRename method finishes. The legacy alter table behavior can also be toggled on and off using the SQLITE_DBCONFIG_LEGACY_ALTER_TABLE option to the sqlite3_db_config() interface. The legacy alter table behavior is a per-connection setting. Turning this features on or off affects all attached database files within the database connection. The setting does not persist. Changing this setting in one connection does not affect any other connections. 
<a name="pragma_legacy_file_format"></a>
 

---

 **PRAGMA legacy_file_format;** This pragma no longer functions. It has become a no-op. The capabilities formerly provided by PRAGMA legacy_file_format are now available using the SQLITE_DBCONFIG_LEGACY_FILE_FORMAT option to the sqlite3_db_config() C-language interface.  
<a name="pragma_locking_mode"></a>
 

---

 **PRAGMA ***schema.***locking_mode;   
PRAGMA ***schema.***locking_mode = *NORMAL | EXCLUSIVE*** This pragma sets or queries the database connection locking-mode. The locking-mode is either NORMAL or EXCLUSIVE. In NORMAL locking-mode (the default unless overridden at compile-time using SQLITE_DEFAULT_LOCKING_MODE), a database connection unlocks the database file at the conclusion of each read or write transaction. When the locking-mode is set to EXCLUSIVE, the database connection never releases file-locks. The first time the database is read in EXCLUSIVE mode, a shared lock is obtained and held. The first time the database is written, an exclusive lock is obtained and held. Database locks obtained by a connection in EXCLUSIVE mode may be released either by closing the database connection, or by setting the locking-mode back to NORMAL using this pragma and then accessing the database file (for read or write). Simply setting the locking-mode to NORMAL is not enough - locks are not released until the next time the database file is accessed. There are three reasons to set the locking-mode to EXCLUSIVE. 
 
1. The application wants to prevent other processes from accessing the database file. 
1. The number of system calls for filesystem operations is reduced, possibly resulting in a small performance increase. 
1. WAL databases can be accessed in EXCLUSIVE mode without the use of shared memory. (Additional information) 
  When the locking_mode pragma specifies a particular database, for example: 
> PRAGMA**> main.**> locking_mode=EXCLUSIVE;

 then the locking mode applies only to the named database. If no database name qualifier precedes the "locking_mode" keyword then the locking mode is applied to all databases, including any new databases added by subsequent ATTACH commands. The "temp" database (in which TEMP tables and indices are stored) and in-memory databases always uses exclusive locking mode. The locking mode of temp and in-memory databases cannot be changed. All other databases use the normal locking mode by default and are affected by this pragma. If the locking mode is EXCLUSIVE when first entering WAL journal mode, then the locking mode cannot be changed to NORMAL until after exiting WAL journal mode. If the locking mode is NORMAL when first entering WAL journal mode, then the locking mode can be changed between NORMAL and EXCLUSIVE and back again at any time and without needing to exit WAL journal mode. 
<a name="pragma_max_page_count"></a>
 

---

 **PRAGMA ***schema.***max_page_count;   
PRAGMA ***schema.***max_page_count = ***N***;** Query or set the maximum number of pages in the database file. Both forms of the pragma return the maximum page count. The second form attempts to modify the maximum page count. The maximum page count cannot be reduced below the current database size.  
<a name="pragma_mmap_size"></a>
 

---

   
**PRAGMA ***schema.***mmap_size;   
PRAGMA ***schema.***mmap_size=***N* Query or change the maximum number of bytes that are set aside for memory-mapped I/O on a single database. The first form (without an argument) queries the current limit. The second form (with a numeric argument) sets the limit for the specified database, or for all databases if the optional database name is omitted. In the second form, if the database name is omitted, the limit that is set becomes the default limit for all databases that are added to the database connection by subsequent ATTACH statements. The argument N is the maximum number of bytes of the database file that will be accessed using memory-mapped I/O. If N is zero then memory mapped I/O is disabled. If N is negative, then the limit reverts to the default value determined by the most recent sqlite3_config(SQLITE_CONFIG_MMAP_SIZE), or to the compile time default determined by SQLITE_DEFAULT_MMAP_SIZE if no start-time limit has been set. The PRAGMA mmap_size statement will never increase the amount of address space used for memory-mapped I/O above the hard limit set by the SQLITE_MAX_MMAP_SIZE compile-time option, nor the hard limit set at startup-time by the second argument to sqlite3_config(SQLITE_CONFIG_MMAP_SIZE) The size of the memory-mapped I/O region cannot be changed while the memory-mapped I/O region is in active use, to avoid unmapping memory out from under running SQL statements. For this reason, the mmap_size pragma may be a no-op if the prior mmap_size is non-zero and there are other SQL statements running concurrently on the same database connection. 
<a name="pragma_module_list"></a>
 

---

 **PRAGMA module_list;** This pragma returns a list of virtual table modules registered with the database connection. 
<a name="pragma_optimize"></a>
 

---

 **PRAGMA optimize;   
PRAGMA optimize(***MASK***);   
PRAGMA ***schema***.optimize;   
PRAGMA ***schema***.optimize(***MASK***);** Attempt to optimize the database. All schemas are optimized in the first two forms, and only the specified schema is optimized in the latter two. In most applications, using PRAGMA optimize as follows will help SQLite to achieve the best possible query performance: 
 
1.  Applications with short-lived database connections should run "PRAGMA optimize;" once, just prior to closing each database connection. 
1.  Applications that use long-lived database connections should run "PRAGMA optimize=0x10002;" when the connection is first opened, and then also run "PRAGMA optimize;" periodically, perhaps once per day or once per hour. 
1. All applications should run "PRAGMA optimize;" after a schema change, especially after one or more CREATE INDEX statements. 
 This pragma is usually a no-op or nearly so and is very fast. On the occasions where it does need to run ANALYZE on one or more tables, it sets a temporary analysis limit, valid for the duration of this pragma only, that prevents the ANALYZE invocations from running for too long. Recommended practice is that applications with short-lived database connections should run "PRAGMA optimize" once when the database connection closes. Applications with long-lived database connections should run "PRAGMA optimize=0x10002" when the database connection first opens, then run "PRAGMA optimize" again at periodic intervals - perhaps once per day. All applications should run "PRAGMA optimize" after schema changes, especially CREATE INDEX.  The details of optimizations performed by this pragma are expected to change and improve over time. Applications should anticipate that this pragma will perform new optimizations in future releases. The optional MASK argument is a bitmask of optimizations to perform:  
****

1. 
1. 
1. 
1. 
  1. 
  1. 
  1. 
1. 
  1. 
  1. 

<a name="pragma_page_count"></a>


---

**********
<a name="pragma_page_size"></a>


---

********  
**************
<a name="pragma_parser_trace"></a>


---

**********
<a name="pragma_pragma_list"></a>


---

****
<a name="pragma_query_only"></a>


---

**  
********
<a name="pragma_quick_check"></a>


---

********  
**************  
**************
<a name="pragma_read_uncommitted"></a>


---

**  
********
<a name="pragma_recursive_triggers"></a>


---

**  
********
<a name="pragma_reverse_unordered_selects"></a>


---

**  
********
<a name="pragma_schema_version"></a>


---

********  
**************
<a name="pragma_secure_delete"></a>


---

********  
******************
<a name="pragma_short_column_names"></a>


---

**  
************
<a name="pragma_shrink_memory"></a>


---

****
<a name="pragma_soft_heap_limit"></a>


---

**  
****
<a name="pragma_stats"></a>


---

****
<a name="pragma_synchronous"></a>


---

********  
**************

********


********


********


********


<a name="pragma_table_info"></a>


---

****************
<a name="pragma_table_list"></a>


---

**  
******  
********

1. ****
1. ****
1. ****
1. ****
1. ****
1. ****
1. **
****
<a name="pragma_table_xinfo"></a>


---

****************
<a name="pragma_temp_store"></a>


---

**  
****************
  
  
****

| SQLITE_TEMP_STORE | PRAGMAtemp_store | Storage used forTEMP tables and indices |
| --- | --- | --- |
| 0 | any | file |
| 1 | 0 | file |
| 1 | 1 | file |
| 1 | 2 | memory |
| 2 | 0 | memory |
| 2 | 1 | file |
| 2 | 2 | memory |
| 3 | any | memory |


 
<a name="pragma_temp_store_directory"></a>
 

---

 **PRAGMA temp_store_directory;   
PRAGMA temp_store_directory = '***directory-name***';** Query or change the value of the sqlite3_temp_directory global variable, which many operating-system interface backends use to determine where to store temporary tables and indices. When the temp_store_directory setting is changed, all existing temporary tables, indices, triggers, and viewers in the database connection that issued the pragma are immediately deleted. In practice, temp_store_directory should be set immediately after the first database connection for a process is opened. If the temp_store_directory is changed for one database connection while other database connections are open in the same process, then the behavior is undefined and probably undesirable. Changing the temp_store_directory setting is *not* threadsafe. Never change the temp_store_directory setting if another thread within the application is running any SQLite interface at the same time. Doing so results in undefined behavior. Changing the temp_store_directory setting writes to the sqlite3_temp_directory global variable and that global variable is not protected by a mutex. The value *directory-name* should be enclosed in single quotes. To revert the directory to the default, set the *directory-name* to an empty string, e.g., *PRAGMA temp_store_directory = ''*. An error is raised if *directory-name* is not found or is not writable.  The default directory for temporary files depends on the OS. Some OS interfaces may choose to ignore this variable and place temporary files in some other directory different from the directory specified here. In that sense, this pragma is only advisory.  **This pragma is deprecated** and exists for backwards compatibility only. New applications should avoid using this pragma. Older applications should discontinue use of this pragma at the earliest opportunity. This pragma may be omitted from the build when SQLite is compiled using SQLITE_OMIT_DEPRECATED.  
<a name="pragma_threads"></a>
 

---

 **PRAGMA threads;   
PRAGMA threads = ***N***;** Query or change the value of the sqlite3_limit(db,SQLITE_LIMIT_WORKER_THREADS,...) limit for the current database connection. This limit sets an upper bound on the number of auxiliary threads that a prepared statement is allowed to launch to assist with a query. The default limit is 0 unless it is changed using the SQLITE_DEFAULT_WORKER_THREADS compile-time option. When the limit is zero, that means no auxiliary threads will be launched. This pragma is a thin wrapper around the sqlite3_limit(db,SQLITE_LIMIT_WORKER_THREADS,...) interface.  
<a name="pragma_trusted_schema"></a>
 

---

 **PRAGMA trusted_schema;   
PRAGMA trusted_schema = ***boolean***;** The trusted_schema setting is a per-connection boolean that determines whether or not SQL functions and virtual tables that have not been security audited are allowed to be run by views, triggers, or in expressions of the schema such as CHECK constraints, DEFAULT clauses, generated columns, expression indexes, and/or partial indexes. This setting can also be controlled using the sqlite3_db_config(db,SQLITE_DBCONFIG_TRUSTED_SCHEMA,...) C-language interface. In order to maintain backwards compatibility, this setting is ON by default. There are advantages to turning it off, and most applications will be unaffected if it is turned off. For that reason, all applications are encouraged to switch this setting off on every database connection as soon as that connection is opened. The -DSQLITE_TRUSTED_SCHEMA=0 compile-time option will cause this setting to default to OFF. 
<a name="pragma_user_version"></a>
 

---

 **PRAGMA ***schema.***user_version;   
PRAGMA ***schema.***user_version = ***integer ***;**  The user_version pragma will get or set the value of the user-version integer at offset 60 in the database header. The user-version is an integer that is available to applications to use however they want. SQLite makes no use of the user-version itself.  See also the application_id pragma and schema_version pragma. 
<a name="pragma_vdbe_addoptrace"></a>
 

---

 **PRAGMA vdbe_addoptrace = ***boolean***;** If SQLite has been compiled with the SQLITE_DEBUG compile-time option, then the vdbe_addoptrace pragma can be used to cause complete VDBE opcodes to be displayed as they are created during code generation. This feature is used for debugging SQLite itself. See the VDBE documentation for more information.  This pragma is intended for use when debugging SQLite itself. It is only available when the SQLITE_DEBUG compile-time option is used. 
<a name="pragma_vdbe_debug"></a>
 

---

 **PRAGMA vdbe_debug = ***boolean***;** If SQLite has been compiled with the SQLITE_DEBUG compile-time option, then the vdbe_debug pragma is a shorthand for three other debug-only pragmas: vdbe_addoptrace, vdbe_listing, and vdbe_trace. This feature is used for debugging SQLite itself. See the VDBE documentation for more information.  This pragma is intended for use when debugging SQLite itself. It is only available when the SQLITE_DEBUG compile-time option is used. 
<a name="pragma_vdbe_listing"></a>
 

---

 **PRAGMA vdbe_listing = ***boolean***;** If SQLite has been compiled with the SQLITE_DEBUG compile-time option, then the vdbe_listing pragma can be used to cause a complete listing of the virtual machine opcodes to appear on standard output as each statement is evaluated. When vdbe_listing is on, the entire content of a program is printed just prior to beginning execution. The statement executes normally after the listing is printed. This feature is used for debugging SQLite itself. See the VDBE documentation for more information.  This pragma is intended for use when debugging SQLite itself. It is only available when the SQLITE_DEBUG compile-time option is used. 
<a name="pragma_vdbe_trace"></a>
 

---

 **PRAGMA vdbe_trace = ***boolean***;** If SQLite has been compiled with the SQLITE_DEBUG compile-time option, then the vdbe_trace pragma can be used to cause virtual machine opcodes to be printed on standard output as they are evaluated. This feature is used for debugging SQLite. See the VDBE documentation for more information.  This pragma is intended for use when debugging SQLite itself. It is only available when the SQLITE_DEBUG compile-time option is used. 
<a name="pragma_wal_autocheckpoint"></a>
 

---

 **PRAGMA wal_autocheckpoint;  
 PRAGMA wal_autocheckpoint=***N***;** This pragma queries or sets the write-ahead log auto-checkpoint interval. When the write-ahead log is enabled (via the journal_mode pragma) a checkpoint will be run automatically whenever the write-ahead log equals or exceeds *N* pages in length. Setting the auto-checkpoint size to zero or a negative value turns auto-checkpointing off. This pragma is a wrapper around the sqlite3_wal_autocheckpoint() C interface. All automatic checkpoints are PASSIVE. Autocheckpointing is enabled by default with an interval of 1000 or SQLITE_DEFAULT_WAL_AUTOCHECKPOINT. 
<a name="pragma_wal_checkpoint"></a>
 

---

 **PRAGMA ***schema.***wal_checkpoint;**  
 **PRAGMA ***schema.***wal_checkpoint(PASSIVE);**  
 **PRAGMA ***schema.***wal_checkpoint(FULL);**  
 **PRAGMA ***schema.***wal_checkpoint(RESTART);**  
 **PRAGMA ***schema.***wal_checkpoint(TRUNCATE);**  
 **PRAGMA ***schema.***wal_checkpoint(NOOP);**  If the write-ahead log is enabled (via the journal_mode pragma), this pragma causes a checkpoint operation to run on database *database*, or on all attached databases if *database* is omitted. If write-ahead log mode is disabled, this pragma is a harmless no-op. Invoking this pragma without an argument is equivalent to calling the sqlite3_wal_checkpoint() C interface. Invoking this pragma with an argument is equivalent to calling the sqlite3_wal_checkpoint_v2() C interface with a 3rd parameter corresponding to the argument: 
 
**PASSIVE**

 Checkpoint as many frames as possible without waiting for any database readers or writers to finish. Sync the db file if all frames in the log are checkpointed. This mode is the same as calling the sqlite3_wal_checkpoint() C interface. The busy-handler callback is never invoked in this mode. 
**FULL**

 This mode blocks (invokes the busy-handler callback) until there is no database writer and all readers are reading from the most recent database snapshot. It then checkpoints all frames in the log file and syncs the database file. FULL blocks concurrent writers while it is running, but readers can proceed. 
**RESTART**

 This mode works the same way as FULL with the addition that after checkpointing the log file it blocks (calls the busy-handler callback) until all readers are finished with the log file. This ensures that the next client to write to the database file restarts the log file from the beginning. RESTART blocks concurrent writers while it is running, but allows readers to proceed. 
**TRUNCATE**

 This mode works the same way as RESTART with the addition that the WAL file is truncated to zero bytes upon successful completion. 
**NOOP**

 This mode does not checkpoint any frames. It is used to obtain the returned values only. 
 The wal_checkpoint pragma returns a single row with three integer columns. The first column is usually 0 but will be 1 if a RESTART or FULL or TRUNCATE checkpoint was blocked from completing, for example because another thread or process was actively using the database. In other words, the first column is 0 if the equivalent call to sqlite3_wal_checkpoint_v2() would have returned SQLITE_OK or 1 if the equivalent call would have returned SQLITE_BUSY. The second column is the number of modified pages that have been written to the write-ahead log file. The third column is the number of pages in the write-ahead log file that have been successfully moved back into the database file at the conclusion of the checkpoint. The second and third column are -1 if there is no write-ahead log, for example if this pragma is invoked on a database connection that is not in WAL mode. 
<a name="pragma_writable_schema"></a>
 

---

 **PRAGMA writable_schema = ***boolean***;**  
 **PRAGMA writable_schema = RESET** When this pragma is on, and the SQLITE_DBCONFIG_DEFENSIVE flag is off, then the sqlite_schema table can be changed using ordinary UPDATE, INSERT, and DELETE statements. If the argument is "RESET" then schema writing is disabled (as with "PRAGMA writable_schema=OFF") and, in addition, the schema is reloaded. **Warning:** misuse of this pragma can easily result in a corrupt database file. 

---

 *This page was last updated on 2025-11-13 07:12:58Z *