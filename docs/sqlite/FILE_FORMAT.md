Database File Format
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
Database File Format
Table Of Contents
1. The Database File
1.1. Hot Journals
1.2. Pages
1.3. The Database Header
1.3.1. Magic Header String
1.3.2. Page Size
1.3.3. File format version numbers
1.3.4. Reserved bytes per page
1.3.5. Payload fractions
1.3.6. File change counter
1.3.7. In-header database size
1.3.8. Free page list
1.3.9. Schema cookie
1.3.10. Schema format number
1.3.11. Suggested cache size
1.3.12. Incremental vacuum settings
1.3.13. Text encoding
1.3.14. User version number
1.3.15. Application ID
1.3.16. Write library version number and version-valid-for number
1.3.17. Header space reserved for expansion
1.4. The Lock-Byte Page
1.5. The Freelist
1.6. B-tree Pages
1.7. Cell Payload Overflow Pages
1.8. Pointer Map or Ptrmap Pages
2. Schema Layer
2.1. Record Format
2.2. Record Sort Order
2.3. Representation Of SQL Tables
2.4. Representation of WITHOUT ROWID Tables
2.4.1. Suppression of redundant columns in the PRIMARY KEY
of WITHOUT ROWID tables
2.5. Representation Of SQL Indices
2.5.1. Suppression of redundant columns in WITHOUT ROWID secondary indexes
2.6. Storage Of The SQL Database Schema
2.6.1. Alternative Names For The Schema Table
2.6.2. Internal Schema Objects
2.6.3. The sqlite_sequence table
2.6.4. The sqlite_stat1 table
2.6.5. The sqlite_stat2 table
2.6.6. The sqlite_stat3 table
2.6.7. The sqlite_stat4 table
3. The Rollback Journal
4. The Write-Ahead Log
4.1. WAL File Format
4.2. Checksum Algorithm
4.3. Checkpoint Algorithm
4.4. WAL Reset
4.5. Reader Algorithm
4.6. WAL-Index Format
This document describes and defines the on-disk database file
format used by all releases of SQLite since 
version 3.0.0 (2004-06-18).
1. The Database File
The complete state of an SQLite database is usually
contained in a single file on disk called the "main database file".
During a transaction, SQLite stores additional information 
in a second file called the "rollback journal", or if SQLite is in
WAL mode, a write-ahead log file.
1.1. Hot Journals
If the application or
host computer crashes before the transaction completes, then the rollback
journal or write-ahead log contains information needed 
to restore the main database file to a consistent state.  When a rollback 
journal or write-ahead log contains information necessary for recovering 
the state of the database, they are called a "hot journal" or "hot WAL file".
Hot journals and WAL files are only a factor during error recovery
scenarios and so are uncommon, but they are part of the state of an SQLite
database and so cannot be ignored.  This document defines the format
of a rollback journal and the write-ahead log file, but the focus is
on the main database file.
1.2. Pages
The main database file consists of one or more pages.  The size of a
page is a power of two between 512 and 65536 inclusive.  All pages within
the same database are the same size.  The page size for a database file
is determined by the 2-byte integer located at an offset of
16 bytes from the beginning of the database file.
Pages are numbered beginning with 1.  The maximum page number is
4294967294 (232 - 2).  The minimum size
SQLite database is a single 512-byte page.
The maximum size database would be 4294967294 pages at 65536 bytes per
page or 281,474,976,579,584 bytes (about 281 terabytes).  Usually SQLite will
hit the maximum file size limit of the underlying filesystem or disk
hardware long before it hits its own internal size limit.
In common use, SQLite databases tend to range in size from a few kilobytes
to a few gigabytes, though terabyte-size SQLite databases are known to exist
in production.
At any point in time, every page in the main database has a single
use which is one of the following:
A b-tree page
A table b-tree interior page
A table b-tree leaf page
An index b-tree interior page
An index b-tree leaf page
A freelist page
A freelist trunk page
A freelist leaf page
A payload overflow page
A pointer map page
The lock-byte page
All reads from and writes to the main database file begin at a page
boundary and all writes are an integer number of pages in size.  Reads
are also usually an integer number of pages in size, with the one exception
that when the database is first opened, the first 100 bytes of the
database file (the database file header) are read as a sub-page size unit.
1.3. The Database Header
The first 100 bytes of the database file comprise the database file 
header.  The database file header is divided into fields as shown by
the table below.  All multibyte fields in the database file header are
stored with the most significant byte first (big-endian).
Database Header Format
OffsetSizeDescription
016
The header string: "SQLite format 3\000"
162
The database page size in bytes.  Must be a power of two between 512
and 32768 inclusive, or the value 1 representing a page size of 65536.
181
File format write version.  1 for legacy; 2 for WAL.
191
File format read version.  1 for legacy; 2 for WAL.
201
Bytes of unused "reserved" space at the end of each page.  Usually 0.
211
Maximum embedded payload fraction.  Must be 64.
221
Minimum embedded payload fraction.  Must be 32.
231
Leaf payload fraction.  Must be 32.
244
File change counter.
284
Size of the database file in pages.  The "in-header database size".
324
Page number of the first freelist trunk page.
364
Total number of freelist pages.
404
The schema cookie.
444
The schema format number.  Supported schema formats are 1, 2, 3, and 4.
484
Default page cache size.
524
The page number of the largest root b-tree page when in auto-vacuum or
incremental-vacuum modes, or zero otherwise.
564
The database text encoding.  A value of 1 means UTF-8.  A value of 2
means UTF-16le.  A value of 3 means UTF-16be.
604
The "user version" as read and set by the user_version pragma.
644
True (non-zero) for incremental-vacuum mode.  False (zero) otherwise.
684
The "Application ID" set by PRAGMA application_id.
7220
Reserved for expansion.  Must be zero.
924
The version-valid-for number.
964
SQLITE_VERSION_NUMBER
1.3.1. Magic Header String
Every valid SQLite database file begins with the following 16 bytes 
(in hex): 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00.  This byte sequence
corresponds to the UTF-8 string "SQLite format 3" including the nul
terminator character at the end.
1.3.2. Page Size
The two-byte value beginning at offset 16 determines the page size of 
the database.  For SQLite versions 3.7.0.1 (2010-08-04)
and earlier, this value is 
interpreted as a big-endian integer and must be a power of two between
512 and 32768, inclusive.  Beginning with SQLite version 3.7.1
(2010-08-23), a page
size of 65536 bytes is supported.  The value 65536 will not fit in a
two-byte integer, so to specify a 65536-byte page size, the value
at offset 16 is 0x00 0x01.
This value can be interpreted as a big-endian
1 and thought of as a magic number to represent the 65536 page size.
Or one can view the two-byte field as a little endian number and say
that it represents the page size divided by 256.  These two 
interpretations of the page-size field are equivalent.
1.3.3. File format version numbers
The file format write version and file format read version at offsets
18 and 19 are intended to allow for enhancements of the file format
in future versions of SQLite.  In current versions of SQLite, both of
these values are 1 for rollback journalling modes and 2 for WAL
journalling mode.  If a version of SQLite coded to the current
file format specification encounters a database file where the read
version is 1 or 2 but the write version is greater than 2, then the database
file must be treated as read-only.  If a database file with a read version
greater than 2 is encountered, then that database cannot be read or written.
1.3.4. Reserved bytes per page
SQLite has the ability to set aside a small number of extra bytes at
the end of every page for use by extensions.  These extra bytes are
used, for example, by the SQLite Encryption Extension to store a nonce
and/or cryptographic checksum associated with each page.  The 
"reserved space" size in the 1-byte integer at offset 20 is the number
of bytes of space at the end of each page to reserve for extensions.
This value is usually 0.  The value can be odd.
The "usable size" of a database page is the page size specified by the
2-byte integer at offset 16 in the header less the "reserved" space size
recorded in the 1-byte integer at offset 20 in the header.  The usable
size of a page might be an odd number.  However, the usable size is not
allowed to be less than 480.  In other words, if the page size is 512,
then the reserved space size cannot exceed 32.
1.3.5. Payload fractions
The maximum and minimum embedded payload fractions and the leaf
payload fraction values must be 64, 32, and 32.  These values were
originally intended to be tunable parameters that could be used to
modify the storage format of the b-tree algorithm.  However, that
functionality is not supported and there are no current plans to add
support in the future.  Hence, these three bytes are fixed at the
values specified.
1.3.6. File change counter
The file change counter is a 4-byte big-endian integer at
offset 24 that is incremented whenever the database file is unlocked
after having been modified.
When two or more processes are reading the same database file, each 
process can detect database changes from other processes by monitoring 
the change counter.
A process will normally want to flush its database page cache when
another process modified the database, since the cache has become stale.
The file change counter facilitates this.
In WAL mode, changes to the database are detected using the wal-index
and so the change counter is not needed.  Hence, the change counter might
not be incremented on each transaction in WAL mode.
1.3.7. In-header database size
The 4-byte big-endian integer at offset 28 into the header 
stores the size of the database file in pages.  If this in-header
datasize size is not valid (see the next paragraph), then the database 
size is computed by looking
at the actual size of the database file. Older versions of SQLite
ignored the in-header database size and used the actual file size
exclusively.  Newer versions of SQLite use the in-header database
size if it is available but fall back to the actual file size if
the in-header database size is not valid.
The in-header database size is only considered to be valid if
it is non-zero and if the 4-byte change counter at offset 24
exactly matches the 4-byte version-valid-for number at offset 92.
The in-header database size is always valid 
when the database is only modified using recent versions of SQLite,
versions 3.7.0 (2010-07-21) and later.
If a legacy version of SQLite writes to the database, it will not
know to update the in-header database size and so the in-header
database size could be incorrect.  But legacy versions of SQLite
will also leave the version-valid-for number at offset 92 unchanged
so it will not match the change-counter.  Hence, invalid in-header
database sizes can be detected (and ignored) by observing when
the change-counter does not match the version-valid-for number.
1.3.8. Free page list
Unused pages in the database file are stored on a freelist.  The
4-byte big-endian integer at offset 32 stores the page number of
the first page of the freelist, or zero if the freelist is empty.
The 4-byte big-endian integer at offset 36 stores the total 
number of pages on the freelist.
1.3.9. Schema cookie
The schema cookie is a 4-byte big-endian integer at offset 40
that is incremented whenever the database schema changes.  A 
prepared statement is compiled against a specific version of the
database schema.  When the database schema changes, the statement
must be reprepared.  When a prepared statement runs, it first checks
the schema cookie to ensure the value is the same as when the statement
was prepared and if the schema cookie has changed, the statement either
automatically reprepares and reruns or it aborts with an SQLITE_SCHEMA 
error.
1.3.10. Schema format number
The schema format number is a 4-byte big-endian integer at offset 44.
The schema format number is similar to the file format read and write
version numbers at offsets 18 and 19 except that the schema format number
refers to the high-level SQL formatting rather than the low-level b-tree
formatting.  Four schema format numbers are currently defined:
Format 1 is understood by all versions of SQLite back to
version 3.0.0 (2004-06-18).
Format 2 adds the ability of rows within the same table
to have a varying number of columns, in order to support the
ALTER TABLE ... ADD COLUMN functionality.  Support for
reading and writing format 2 was added in SQLite 
version 3.1.3 on 2005-02-20.
Format 3 adds the ability of extra columns added by
ALTER TABLE ... ADD COLUMN to have non-NULL default
values.  This capability was added in SQLite version 3.1.4
on 2005-03-11.
Format 4 causes SQLite to respect the
DESC keyword on
index declarations.  (The DESC keyword is ignored in indexes for 
formats 1, 2, and 3.)
Format 4 also adds two new boolean record type values (serial types
8 and 9).  Support for format 4 was added in SQLite 3.3.0 on
2006-01-10.
New database files created by SQLite use format 4 by default.
The SQLITE_DBCONFIG_LEGACY_FILE_FORMAT option for the
sqlite3_db_config() C-language interface can be used to cause SQLite
to create new database files using format 1. The format version number
can be made to default to 1 instead of 4 by setting
SQLITE_DEFAULT_FILE_FORMAT=1 at compile-time.
If the database is completely empty, if it has no schema, then the
schema format number can be zero.
1.3.11. Suggested cache size
The 4-byte big-endian signed integer at offset 48 is the suggested
cache size in pages for the database file.  The value is a suggestion
only and SQLite is under no obligation to honor it.  The absolute value
of the integer is used as the suggested size.  The suggested cache size
can be set using the default_cache_size pragma.
1.3.12. Incremental vacuum settings
The two 4-byte big-endian integers at offsets 52 and 64 are used
to manage the auto_vacuum and incremental_vacuum modes.  If
the integer at offset 52 is zero then pointer-map (ptrmap) pages are
omitted from the database file and neither auto_vacuum nor
incremental_vacuum are supported.  If the integer at offset 52 is
non-zero then it is the page number of the largest root page in the
database file, the database file will contain ptrmap pages, and the
mode must be either auto_vacuum or incremental_vacuum.  In this latter
case, the integer at offset 64 is true for incremental_vacuum and
false for auto_vacuum.  If the integer at offset 52 is zero then
the integer at offset 64 must also be zero.
1.3.13. Text encoding
The 4-byte big-endian integer at offset 56 determines the encoding
used for all text strings stored in the database.  
A value of 1 means UTF-8.
A value of 2 means UTF-16le.
A value of 3 means UTF-16be.
No other values are allowed.
The sqlite3.h header file defines C-preprocessor macros SQLITE_UTF8 as 1,
SQLITE_UTF16LE as 2, and SQLITE_UTF16BE as 3, to use in place of
the numeric codes for the text encoding.
1.3.14. User version number
The 4-byte big-endian integer at offset 60 is the user version which
is set and queried by the user_version pragma.  The user version is
not used by SQLite.
1.3.15. Application ID
The 4-byte big-endian integer at offset 68 is an "Application ID" that
can be set by the PRAGMA application_id command in order to identify the
database as belonging to or associated with a particular application.
The application ID is intended for database files used as an
application file-format.  The application ID can be used by utilities 
such as file(1) to determine the specific
file type rather than just reporting "SQLite3 Database".  A list of
assigned application IDs can be seen by consulting the
magic.txt
file in the SQLite source repository.
1.3.16. Write library version number and version-valid-for number
The 4-byte big-endian integer at offset 96 stores the 
SQLITE_VERSION_NUMBER value for the SQLite library that most
recently modified the database file.  The 4-byte big-endian integer at
offset 92 is the value of the change counter when the version number
was stored.  The integer at offset 92 indicates which transaction
the version number is valid for and is sometimes called the
"version-valid-for number".
1.3.17. Header space reserved for expansion
All other bytes of the database file header are reserved for
future expansion and must be set to zero.
1.4. The Lock-Byte Page
The lock-byte page is the single page of the database file
that contains the bytes at offsets between 1073741824 and 1073742335,
inclusive.  A database file that is less than or equal to 1073741824 bytes 
in size contains no lock-byte page.  A database file larger than
1073741824 contains exactly one lock-byte page.
The lock-byte page is set aside for use by the operating-system specific
VFS implementation in implementing the database file locking primitives.
SQLite does not use the lock-byte page.  The SQLite core 
will never read or write the lock-byte page,
though operating-system specific VFS 
implementations may choose to read or write bytes on the lock-byte 
page according to the 
needs and proclivities of the underlying system.  The unix and win32
VFS implementations that come built into SQLite do not write to the
lock-byte page, but third-party VFS implementations for
other operating systems might.
The lock-byte page arose from the need to support Win95 which was the
predominant operating system when this file format was designed and which 
only supported mandatory file locking.  All modern operating systems that
we know of support advisory file locking, and so the lock-byte page is
not really needed any more, but is retained for backwards compatibility.
1.5. The Freelist
A database file might contain one or more pages that are not in
active use.  Unused pages can come about, for example, when information
is deleted from the database.  Unused pages are stored on the freelist
and are reused when additional pages are required.
The freelist is organized as a linked list of freelist trunk pages
with each trunk page containing page numbers for zero or more freelist
leaf pages.
A freelist trunk page consists of an array of 4-byte big-endian integers.
The size of the array is as many integers as will fit in the usable space
of a page.  The minimum usable space is 480 bytes so the array will always
be at least 120 entries in length.  The first integer on a freelist trunk
page is the page number of the next freelist trunk page in the list or zero 
if this is the last freelist trunk page.  The second integer on a freelist
trunk page is the number of leaf page pointers to follow.  
Call the second integer on a freelist trunk page L.
If L is greater than zero then integers with array indexes between 2 and
L+1 inclusive contain page numbers for freelist leaf pages.
Freelist leaf pages contain no information.  SQLite avoids reading or
writing freelist leaf pages in order to reduce disk I/O.
A bug in SQLite versions prior to 3.6.0 (2008-07-16)
caused the database to be
reported as corrupt if any of the last 6 entries in the freelist trunk page 
array contained non-zero values.  Newer versions of SQLite do not have
this problem.  However, newer versions of SQLite still avoid using the 
last six entries in the freelist trunk page array in order that database
files created by newer versions of SQLite can be read by older versions
of SQLite.
The number of freelist pages is stored as a 4-byte big-endian integer
in the database header at an offset of 36 from the beginning of the file.
The database header also stores the page number of the first freelist trunk
page as a 4-byte big-endian integer at an offset of 32 from the beginning
of the file.
1.6. B-tree Pages
The b-tree algorithm provides key/data storage with unique and
ordered keys on page-oriented storage devices.
For background information on b-trees, see
Knuth, The Art Of Computer Programming, Volume 3 "Sorting
and Searching", pages 471-479.  Two variants of b-trees are used by
SQLite.  "Table b-trees" use a 64-bit signed integer key and store
all data in the leaves.  "Index b-trees" use arbitrary keys and store no
data at all.
