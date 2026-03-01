Custom Builds Of SQLite
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
Custom Builds Of SQLite
or
Porting SQLite To New Operating Systems
1.0 Introduction
For most applications, the recommended method for building
SQLite is to use the amalgamation code
file, sqlite3.c, and its corresponding header file
sqlite3.h.  The sqlite3.c code file should compile and
run on any Unix, Windows system without any changes
or special compiler options.  Most applications can simply include
the sqlite3.c file together with the other C code files that make
up the application, compile them all together, and have a working
and well configured version of SQLite.
Most applications work great with SQLite in its
default configuration and with no special compile-time configuration.
Most developers should be able to completely ignore this document
and simply build SQLite from
the amalgamation without any
special knowledge and without taking any special actions.
However, highly tuned and specialized
applications may want or need to replace some of
SQLite's built-in system interfaces with alternative implementations
more suitable for the needs of the application.  SQLite is designed
to be easily reconfigured at compile-time to meet the specific
needs of individual projects.  Among the compile-time configuration
options for SQLite are these:
 Replace the built-in mutex subsystem with an alternative
        implementation.  
 Completely disable all mutexing for use in single-threaded
        applications. 
 Reconfigure the memory allocation subsystem to use a memory
        allocator other the malloc() implementation from the standard
        library. 
 Realign the memory allocation subsystem so that it never calls
        malloc() at all but instead satisfies all memory requests using
        a fixed-size memory buffer assigned to SQLite at startup. 
 Replace the interface to the file system with an alternative
        design.  In other words, override all of the system calls that
        SQLite makes in order to talk to the disk with a completely different
        set of system calls. 
 Override other operating system interfaces such as calls to obtain
        Zulu or local time. 
Generally speaking, there are three separate subsystems within
SQLite that can be modified or overridden at compile-time.  The
mutex subsystem is used to serialize access to SQLite resources that
are shared among threads.  The memory allocation subsystem is used
to allocate memory required by SQLite objects and for the database
cache.  Finally, the Virtual File System subsystem is
used to provide a portable interface between SQLite and the underlying
operating system and especially the file system.  We call these three
subsystems the "interface" subsystems of SQLite.
We emphasis that most applications are well-served by the 
built-in default implementations of the SQLite interface subsystems.
Developers are encouraged to use the
default built-in implementations whenever possible
and to build SQLite without any special compile-time options or parameters.
However, some highly specialized applications may benefit from
substituting or modifying one or more of these built-in SQLite
interface subsystems.
Or, if SQLite is used on an operating system other than
Unix (Linux or Mac OS X), Windows (Win32 or WinCE), or OS/2 then none
of the interface subsystems that come built into SQLite will work
and the application will need to provide alternative implementations
suitable for the target platform.
2.0 Configuring Or Replacing The Mutex Subsystem
In a multithreaded environment, SQLite uses mutexes to serialize
access to shared resources.
The mutex subsystem is only required for applications that access
SQLite from multiple threads.  For single-threaded applications, or
applications which only call SQLite from a single thread, the mutex
subsystem can be completely disabled by recompiling with the following
option:
-DSQLITE_THREADSAFE=0
Mutexes are cheap but they are not free, so performance will be better
when mutexes are completely disabled.  The resulting library footprint
will also be a little smaller.  Disabling the mutexes at compile-time
is a recommended optimization for applications where it makes sense.
When using SQLite as a shared library, an application can test to see
whether or not mutexes have been disabled using the
sqlite3_threadsafe() API.  Applications that link against SQLite at
run-time and use SQLite from multiple threads should probably check this
API to make sure they did not accidentally get linked against a version of
the SQLite library that has its mutexes disabled.  Single-threaded
applications will, of course, work correctly regardless of whether or
not SQLite is configured to be threadsafe, though they will be a little
bit faster when using versions of SQLite with mutexes disabled.
SQLite mutexes can also be disabled at run-time using the
sqlite3_config() interface.  To completely disable all mutexing,
the application can invoke:
sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
Disabling mutexes at run-time is not as effective as disabling them
at compile-time since SQLite still must do a test of a boolean variable
to see if mutexes are enabled or disabled at each point where a mutex
might be required.  But there is still a performance advantage for
disabling mutexes at run-time.
For multi-threaded applications that are careful about how they
manage threads, SQLite supports an alternative run-time configuration
that is half way between not using any mutexes and the default situation
of mutexing everything in sight.  This in-the-middle mutex alignment can
be established as follows:
sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
There are two separate configuration changes here which can
be used either together or separately. The
SQLITE_CONFIG_MULTITHREAD setting disables the mutexes that
serialize access to database connection objects and 
prepared statement objects.  With this setting, the application
is free to use SQLite from multiple threads, but it must make sure
than no two threads try to access the same database connection
or any prepared statements associated with the same 
database connection at the same time.  Two threads can use SQLite
at the same time, but they must use separate database connections.
The second SQLITE_CONFIG_MEMSTATUS setting disables the mechanism
in SQLite that tracks the total size of all outstanding memory
allocation requests.  This omits the need to mutex each call
to sqlite3_malloc() and sqlite3_free(), which saves a huge
number of mutex operations.  But a consequence of disabling the
memory statistics mechanism is that the 
sqlite3_memory_used(), sqlite3_memory_highwater(), and
sqlite3_soft_heap_limit64() interfaces cease to work.
SQLite uses pthreads for its mutex implementation on Unix and
SQLite requires a recursive mutex.  Most modern pthread implementations
support recursive mutexes, but not all do.  For systems that do not
support recursive mutexes, it is recommended that applications operate
in single-threaded mode only.  If this is not possible, SQLite provides
an alternative recursive mutex implementation built on top of the
standard "fast" mutexes of pthreads.  This alternative
implementation should work correctly as long as pthread_equal() is
atomic and the processor has a coherent data cache.  The alternative
recursive mutex implementation is enabled by the following
compiler command-line switch:
-DSQLITE_HOMEGROWN_RECURSIVE_MUTEX=1
When porting SQLite to a new operating system, it is usually necessary
to completely replace the built-in mutex subsystem with an alternative
built around the mutex primitives of the new operating system.  This
is accomplished by compiling SQLite with the following option:
-DSQLITE_MUTEX_APPDEF=1
When SQLite is compiled with the SQLITE_MUTEX_APPDEF=1 option, it
completely omits the implementation of its 
mutex primitive functions.  But the SQLite
library still attempts to call these functions where necessary, so the
application must itself implement the
mutex primitive functions and link them together
with SQLite.
3.0 Configuring Or Replacing The Memory Allocation Subsystem
By default, SQLite obtains the memory it needs for objects and
cache from the malloc()/free() implementation of the standard library.
There is also on-going work with experimental memory allocators that
satisfy all memory requests from a single fixed memory buffer handed
to SQLite at application start.  Additional information on these
experimental memory allocators will be provided in a future revision
of this document.
SQLite supports the ability of an application to specify an alternative
memory allocator at run-time by filling in an instance of the
sqlite3_mem_methods object with pointers to the routines of the
alternative implementation then registering the new alternative
implementation using the sqlite3_config() interface.
For example:
sqlite3_config(SQLITE_CONFIG_MALLOC, &my_malloc_implementation);
SQLite makes a copy of the content of the sqlite3_mem_methods object
so the object can be modified after the sqlite3_config() call returns.
4.0 Adding New Virtual File Systems
Since version 3.5.0 (2007-09-04), 
SQLite has supported an interface called the
virtual file system or "VFS".
This object is somewhat misnamed since it
is really an interface to the whole underlying operating system, not
just the filesystem.
 One of the interesting features
of the VFS interface is that SQLite can support multiple VFSes at the
same time.  Each database connection has to choose a single VFS for its
use when the connection is first opened using sqlite3_open_v2().
But if a process contains multiple database connections each can choose
a different VFS.  VFSes can be added at run-time using the
sqlite3_vfs_register() interface.
The default builds for SQLite on Unix, Windows, and OS/2 include 
a VFS appropriate for the target platform.  SQLite builds for other
operating systems do not contain a VFS by default, but the application
can register one or more at run-time.
5.0 Porting SQLite To A New Operating System
In order to port SQLite to a new operating system - an operating
system not supported by default - the application
must provide...
 a working mutex subsystem (but only if it is multithreaded), 
 a working memory allocation subsystem (assuming it lacks malloc()
in its standard library), and
 a working VFS implementation.
All of these things can be provided in a single auxiliary C code file
and then linked with the stock "sqlite3.c" code file to generate a working
SQLite build for the target operating system.  In addition to the
alternative mutex and memory allocation subsystems and the new VFS,
the auxiliary C code file should contain implementations for the
following two routines:
 sqlite3_os_init() 
 sqlite3_os_end() 
The "sqlite3.c" code file contains default implementations of a VFS
and of the sqlite3_initialize() and sqlite3_shutdown() functions that
are appropriate for Unix, Windows, and OS/2.
To prevent one of these default components from being loaded when sqlite3.c
is compiled, it is necessary to add the following compile-time
option:
-DSQLITE_OS_OTHER=1
The SQLite core will call sqlite3_initialize() early.  The auxiliary
C code file can contain an implementation of sqlite3_initialize() that
registers an appropriate VFS and also perhaps initializes an alternative
mutex system (if mutexes are required) or does any memory allocation
subsystem initialization that is required.
The SQLite core never calls sqlite3_shutdown() but it is part of the
official SQLite API and is not otherwise provided when compiled with
-DSQLITE_OS_OTHER=1, so the auxiliary C code file should probably provide
it for completeness.
This page was last updated on 2025-05-31 13:08:22Z 
