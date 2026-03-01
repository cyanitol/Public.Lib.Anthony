How To Corrupt An SQLite Database File
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
How To Corrupt An SQLite Database File
Table Of Contents
1.  File overwrite by a rogue thread or process
1.1.  Continuing to use a file descriptor after it has been closed
1.2.  Backup or restore while a transaction is active
1.3.  Deleting a hot journal
1.4.  Mispairing database files and hot journals 
2.  File locking problems
2.1.  Filesystems with broken or missing lock implementations
2.2.  Posix advisory locks canceled by a separate thread doing close()
2.3. Multiple copies of SQLite linked into the same application
2.4.  Two processes using different locking protocols
2.5.  Unlinking or renaming a database file while in use
2.6.  Multiple links to the same file
2.7.  Carrying an open database connection across a fork() 
3.  Failure to sync
3.1.  Disk drives that do not honor sync requests
3.2.  Disabling sync using PRAGMAs
4.  Disk Drive and Flash Memory Failures
4.1.  Non-powersafe flash memory controllers
4.2.  Fake capacity USB sticks
5.  Memory corruption
6.  Other operating system problems
6.1.  Linux Threads
6.2.  Failures of mmap() on QNX
6.3.  Filesystem Corruption
7. SQLite Configuration Errors
8.  Bugs in SQLite
8.1.  False corruption reports due to database shrinkage
8.2.  Corruption following switches between rollback and WAL modes
8.3.  I/O error while obtaining a lock leads to corruption
8.4.  Database pages leak from the free page list
8.5.  Corruption following alternating writes from 3.6 and 3.7
8.6.  Race condition in recovery on Windows systems
8.7.  Boundary value error in the secondary journals used by nested transactions
 Overview 
An SQLite database is highly resistant to corruption.
If an application crash, or an operating-system crash, or even
a power failure occurs in the middle of a transaction, the partially
written transaction should be automatically rolled back the next time
the database file is accessed.  The recovery process is fully
automatic and does not require any action on the part of the user
or the application.
Though SQLite is resistant to database corruption, it is not immune.
This document describes the various ways that an SQLite database might
go corrupt.
1.  File overwrite by a rogue thread or process
SQLite database files are ordinary disk files.
That means that any process can open the file and 
overwrite it with garbage.  There is nothing that the SQLite
library can do to defend against this.
1.1.  Continuing to use a file descriptor after it has been closed
We have seen multiple cases where a file descriptor was open on a file,
then that file descriptor was closed and reopened on an SQLite database.
Later, some other thread continued to write into the
old file descriptor, not realizing that the original file had been closed
already.  But because the file descriptor had been reopened by SQLite,
the information that was intended to go into the original file ended up
overwriting parts of the SQLite database, leading to corruption of the
database.
One example of this occurred circa 2013-08-30 on the canonical repository
for the Fossil DVCS.  In that event,
file descriptor 2 (standard error) was being erroneously closed (by
stunnel, we suspect) prior to 
sqlite3_open_v2() so that the file descriptor used for the
repository database file was 2.  Later, an application 
bug caused an assert() statement to emit
an error message by invoking write(2,...).  But since file descriptor 2 was 
now connected to a database file, the error message
overwrote part of the database.  To guard against this kind of problem,
SQLite version 3.8.1 (2013-10-17)
and later refuse to use low-numbered file descriptors
for database files. 
(See SQLITE_MINIMUM_FILE_DESCRIPTOR for additional information.)
Another example of corruption caused by using a closed file
descriptor was 
reported by facebook engineers in a blog post on 2014-08-12.
Another example of this error was reported against
Fossil on 2019-07-11.  A file descriptor would
be opened for debugging output, but then closed and reopened by SQLite.
But the debugging logic continued to write into the original file
descriptor. See the
forum discussion
for the bug report and a link to the fix.
1.2.  Backup or restore while a transaction is active
Systems that run automatic backups in the background might try to
make a backup copy of an SQLite database file while it is in the middle
of a transaction.  The backup copy then might contain some old and some
new content, and thus be corrupt.
There are multiple safe approaches to making backup copies of
SQLite databases - safe in the sense that they are generate a correct,
uncorrupted backup.  In no particular order:
      The sqlite3_rsync utility program (available beginning with
      SQLite 3.47.0 (2024-10-21) and later) will make a copy
      of a live SQLite over SSH using a bandwidth-efficient protocol.
      The VACUUM INTO filename command copies out
      the current state of an SQLite database into a separate file.
      The backup API is a C-language interface that can make a
      consistent copy of an SQLite database.
Any of the above approaches will work even on a live database.
It is also safe to make a copy of an SQLite database file
as long as there are no transactions in progress while the copy
is taking place.
If the previous write transaction failed, then it is important that
any rollback journal (the *-journal file) or write-ahead log
(the *-wal file) be copied together with the database file
itself.
1.3.  Deleting a hot journal
SQLite normally stores all content in a single disk file.  However,
while performing a transaction, information necessary to recover the
database following a crash or power failure is stored in auxiliary
journal files.  Such journal files are described as "hot".
The journal files have the same name as the
original database file with the addition
of -journal or -wal suffix.
SQLite must see the journal files in order to recover from a crash
or power failure.  If the hot journal files are moved, deleted, or renamed
after a crash or power failure, then automatic recovery will not work
and the database may go corrupt.
Another manifestation of this problem is
database corruption caused by inconsistent use of 8+3 filenames.
1.4.  Mispairing database files and hot journals 
 The previous example is a specific case of a more general problem:
The state of an SQLite database is controlled by both the
database file and the journal file.  In a quiescent state, the journal
file does not exist and only the database file matters.
But if the journal file does exist, it must be
kept together with the database to avoid corruption.  The following
actions are all likely to lead to corruption:
 Swapping journal files between two different databases.
 Overwriting a journal file with a different journal file.
 Moving a journal file from one database to another.
 Copying a database file without also copying its journal.
 Overwriting a database file with another without also
     deleting any hot journal associated with the original database.
2.  File locking problems
SQLite uses file locks on the database file, and on the 
write-ahead log or WAL file, to coordinate access between concurrent
processes.  Without coordination, two threads or processes might try
to make incompatible changes to a database file at the same time,
resulting in database corruption.
2.1.  Filesystems with broken or missing lock implementations
SQLite depends on the underlying filesystem to do locking as the
documentation says it will.  But some filesystems contain bugs in their
locking logic such that the locks do not always behave as advertised.
This is especially true of network filesystems and NFS in particular.
If SQLite is used on a filesystem where the locking primitives contain
bugs, and if two or more threads or processes try to access the same
database at the same time, then database corruption might result.
2.2.  Posix advisory locks canceled by a separate thread doing close()
The default locking mechanism used by SQLite on unix platforms is
POSIX advisory locking.  Unfortunately, POSIX advisory locking has design
quirks that make it prone to misuse and failure. In particular, any
thread in the same process with a file descriptor that is holding a POSIX
advisory lock can override that lock using a different file descriptor.
One particularly pernicious problem is that the close() system
call will cancel all POSIX advisory locks on the same file for all
threads and all file descriptors in the process.
So, for example, suppose a multi-thread process has
two or more threads with separate SQLite database connections to the
same database file.  Then a third thread comes along and wants to read
something out of that same database file on its own, without using the 
SQLite library.  Maybe the third thread wants to make a backup copy
of the database.  Or maybe the third thread is just trying to identify
the file type and hences tries to read the first 16 bytes to determine
if it really is an SQLite database.  Regardless of the reason,
the third thread does an open(), a read() and then
a close().
One would think this would be harmless.
But the close() system call caused the
locks held on the database by all the other threads to be dropped.  Those
other threads have no way of knowing that their locks have just been
trashed (POSIX does not provide any mechanism to determine this) and so
they keep on running under the assumption that their locks are still valid.
This can lead to two or more threads or processes trying to write to the
database at the same time, resulting in database corruption.
Note that it is perfectly safe for two or more threads to access the
same SQLite database file using the SQLite library.  The unix drivers for
SQLite know about the POSIX advisory locking quirks and work around them.
This problem only arises when a thread tries to bypass the SQLite library
and read the database file directly.
Beginning with SQLite version 3.51.0 (2025-11-04), SQLite implements
additional defenses to try to avoid problems caused by locks that are
broken by close().  These new defenses help when the database
is in WAL mode and is being accessed from multiple processes.  But
they are not a cure-all.  To avoid corruptions, developers should be
careful to never use close() on an SQLite database file
while one or more database connections are open, even in other threads.
2.3. Multiple copies of SQLite linked into the same application
As pointed out in the previous section, SQLite takes steps to work
around the quirks of POSIX advisory locking.  Part of that work-around involves
keeping a global list (mutex protected) of open SQLite database files.
But, if multiple copies of SQLite are linked into the same application,
then there will be multiple instances of this global list.
Database connections opened using one copy of the SQLite library
will be unaware of database connections opened using the other copy,
and will be unable to work around the POSIX advisory locking quirks.
A close() operation on one connection might unknowingly 
clear the locks on a different database connection, leading to database
corruption.
The scenario above sounds far-fetched.
But the SQLite developers are aware of at 
least one commercial product that was released
with exactly this bug.  The vendor came to the SQLite developers seeking
help in tracking down some infrequent database corruption issues they were
seeing on Linux and Mac.  The problem was eventually traced to the
fact that the application was linking against two separate copies of SQLite.
The solution was to change the application build procedures to link against
just one copy of SQLite instead of two.
2.4.  Two processes using different locking protocols
The default locking mechanism used by SQLite on unix platforms is
POSIX advisory locking, but there are other options.  By selecting an
alternative sqlite3_vfs using the sqlite3_open_v2() interface, an
application can make use of other locking protocols that might be more
appropriate to certain filesystems.  For example, dot-file locking might
be selected for use in an application that has to run on an NFS filesystem
that does not support POSIX advisory locking.
It is important that all connections to the same database file use 
the same locking protocol.
If one application is using POSIX advisory locks and another application
is using dot-file locking, then the two applications will not see each
other's locks and will not be able to coordinate database access, possibly
leading to database corruption.
2.5.  Unlinking or renaming a database file while in use
If two processes have open connections to the same database file and
one process closes its connection, unlinks the file, then creates a new
database file in its place with the same name and reopens the new file,
then the two processes will be talking to different database files with 
the same name.  (Note that this is only possible on Posix and Posix-like
systems that permit a file to be unlinked while it is still open for
reading and writing.  Windows does not allow this to occur.)
Since rollback journals and WAL files are based on the name of the database
file, the two different database files will share the same rollback
journal or WAL file.  A rollback or recovery for one of the databases
might use content from the other database, resulting in corruption.
A similar problem occurs if a database file is renamed while it is
opened and a new file is created with the old name.
In other words, unlinking or renaming an open database file 
results in behavior that is undefined and probably undesirable.
Beginning with SQLite version 3.7.17 (2013-05-20),
the unix OS interface will
send SQLITE_WARNING messages to the error log if a database file is unlinked
while it is still in use.
2.6.  Multiple links to the same file
If a single database file has multiple links (either hard or soft links)
then that is just another way of saying that the file has multiple names.
If two or more processes open the database using different names, then
they will use different rollback journals and WAL files.  That means that
if one process crashes, the other process will be unable to recover the
transaction in progress because it will be looking in the wrong place
for the appropriate journal.
In other words, opening and using a database file that has two or
more names results in behavior that is undefined and probably undesirable.
Beginning with SQLite version 3.7.17 (2013-05-20),
the unix OS interface will
send SQLITE_WARNING messages to the error log if a database file has 
multiple hard links.
Beginning with SQLite version 3.10.0 (2016-01-06), 
the unix OS interface will
attempt to resolve symbolic links and open the database file by its
canonical name.  Prior to version 3.10.0, opening a database file 
through a symbolic link was similar to opening a database file
that had multiple hard links and resulted in undefined behavior.
2.7.  Carrying an open database connection across a fork() 
 Do not open an SQLite database connection, then fork(), then try
to use that database connection in the child process.  All kinds of
locking problems will result and you can easily end up with a corrupt
database.  SQLite is not designed to support that kind of behavior.
Any database connection that is used in a child process must be opened
in the child process, not inherited from the parent.
 Do not even call sqlite3_close() on a database connection from a
child process if the connection was opened in the parent.  It is safe
to close the underlying file descriptor, but the sqlite3_close()
interface might invoke cleanup activities that will delete content out
from under the parent, leading to errors and perhaps even database
corruption.
3.  Failure to sync
In order to guarantee that database files are always consistent, SQLite
will occasionally ask the operating system to flush all pending writes to
persistent storage then wait for that flush to complete.  This is 
accomplished using the fsync() system call under unix and
FlushFileBuffers() under Windows.  We call this flush of
pending writes a "sync".
Actually, if one is only concerned with atomic and consistent writes and
is willing to forego durable writes, the sync operation does not need
to wait until the content is completely stored on persistent media.  Instead,
the sync operation can be thought of as an I/O barrier.  As long as all
writes that occur before the sync are completed before any write that happens
after the sync, no database corruption will occur.  If sync is operating as
an I/O barrier and not as a true sync, then a power failure or system crash
might cause one or more previously committed transactions to roll back
(in violation of the "durable" property of "ACID") but the database will at
least continue to be consistent, and that is what most people care about.
3.1.  Disk drives that do not honor sync requests
Unfortunately, most consumer-grade mass storage devices lie about
syncing.  Disk drives will report that content is safely on persistent
media as soon as it reaches the track buffer and before actually being
written to oxide.  This makes the disk drives seem to operate faster
(which is vitally important to the manufacturer so that they can show
good benchmark numbers in trade magazines).  And in fairness, the lie
normally causes no harm, as long as there is no power loss or hard reset
prior to the track buffer actually being written to oxide.  But if a
power loss or hard reset does occur, and if that results in content that
was written after a sync reaching oxide while content written before
the sync is still in a track buffer, then database corruption can occur.
USB flash memory sticks seem to be especially pernicious liars 
regarding sync requests.  One can easily see this by committing a large
transaction to an SQLite database on a USB memory stick.  The COMMIT
command will return relatively quickly, indicating that the memory stick
has told the operating system and the operating system has told SQLite that
all content is safely in persistent storage, and yet the LED on the end
of the memory stick will continue flashing for several more seconds. 
Pulling out the memory stick while the LED is still flashing will frequently
result in database corruption.
Note that SQLite must believe whatever the operating system and hardware
tell it about the status of sync requests.  There is no way for SQLite to
detect that either is lying and that writes might be occurring out-of-order.
However, SQLite in WAL mode is far more forgiving of
out-of-order writes than in the default rollback journal modes.  In WAL
mode, the only time that a failed sync operation can cause database corruption
is during a checkpoint operation.  A sync failure during a COMMIT might
result in loss of durability but not in a corrupt database file.  Hence,
one line of defense against database corruption due to failed sync operations
is to use SQLite in WAL mode and to checkpoint as infrequently as possible.
3.2.  Disabling sync using PRAGMAs
The sync operations that SQLite performs to help ensure integrity
can be disabled at run-time using the synchronous pragma.  By setting
PRAGMA synchronous=OFF, all sync operations are omitted.  This makes
SQLite seem to run faster, but it also allows the operating system to freely
reorder writes, which could result in database corruption if a power failure
or hard reset occurs prior to all content reaching persistent storage.
For maximum reliability and for robustness against database corruption,
SQLite should always be run with its default synchronous setting of FULL.
4.  Disk Drive and Flash Memory Failures
An SQLite database can become corrupt if the file content changes 
due to a disk drive or flash memory failure.  It is very rare, but disks 
will occasionally flip a bit in the middle of a sector.
4.1.  Non-powersafe flash memory controllers
We are told that in some flash memory controllers the wear-leveling logic
can cause random filesystem damage if power is interrupted during a write.
This can manifest, for example, as random changes in the middle of a file
that was not even open at the time of the power loss.  So, for example,
a device would be writing content into an MP3 file in flash memory when a
power loss occurs, and that could result in an SQLite database being
corrupted even though the database was not even in use at the time of the
power loss.
4.2.  Fake capacity USB sticks
There are many fraudulent USB sticks in circulation that report to have
a high capacity (ex: 8GB) but are really only capable of storing a much
smaller amount (ex: 1GB).   Attempts to write on these devices will
often result in unrelated files being overwritten.  Any use of a fraudulent
flash memory device can easily lead to database corruption, therefore.
Internet searches such as "fake capacity usb" will turn up lots of
disturbing information about this problem.
5.  Memory corruption
SQLite is a C-library that runs in the same address space as the 
application that it serves.  That means that stray pointers, buffer
overruns, heap corruption, or other malfunctions in the application can
corrupt internal SQLite data structure and ultimately result in a
corrupt database file.  Normally these kinds of problems manifest themselves
as segfaults prior to any database corruption occurring, but there have
been instances where application code errors have caused SQLite to
malfunction subtly so as to corrupt the database file rather than
panicking.
The memory corruption problem becomes more acute when
using memory-mapped I/O.
When all or part of the database file is mapped into the application's
address space, then a stray pointer that overwrites any part of that
mapped space will immediately corrupt the database file, without
requiring the application to do a subsequent write() system call.
6.  Other operating system problems
Sometimes operating systems will exhibit non-standard behavior which
can lead to problems.  Sometimes this non-standard behavior is deliberate,
and sometimes it is a mistake in the implementation.  But in any event,
if the operating performs differently from they way SQLite expects it to
perform, the possibility of database corruption exists.
6.1.  Linux Threads
Some older versions of Linux used the LinuxThreads library for thread
support.  LinuxThreads is similar to Pthreads, but is subtly different
with respect to handling of POSIX advisory locks.  SQLite versions
2.2.3 through 3.6.23 recognized that LinuxThreads were being used at
runtime and took appropriate action to work around the non-standard
behavior of LinuxThreads.  But most modern Linux implementations make
use of the newer, and correct, NPTL implementation of Pthreads.  Beginning
with SQLite version 3.7.0 (2010-07-21), the use of NPTL is assumed.
No checks are 
made.  Hence, recent versions of SQLite will subtly malfunction and may 
corrupt database files if used in multi-threaded application that run
on older linux systems that make use of LinuxThreads.
6.2.  Failures of mmap() on QNX
There exists some subtle problem with mmap() on QNX such that making
a second mmap() call against a single file descriptor can cause
the memory obtained from the first mmap() call to be zeroed.  SQLite on
unix uses mmap() to create a shared memory region for transaction 
coordination in WAL mode, and it will call mmap() multiple times
for large transactions.  The QNX mmap() has been demonstrated to corrupt
database file under that scenario.  QNX engineers are aware of this problem
and are working on a solution; the problem may have already been fixed by
the time you read this.
When running on QNX, it is recommended that memory-mapped I/O never
be used.  Furthermore, to use WAL mode, it is recommended that applications
employ the exclusive locking mode in order to 
use WAL without shared memory.
6.3.  Filesystem Corruption
Since SQLite databases are ordinary disk files, any malfunction in the
filesystem can corrupt the database.  Filesystems in modern operating systems
are very reliable, but errors do still occur.  For example, on 2013-10-01
the SQLite database that holds the
Wiki for Tcl/Tk went corrupt a few days
after the host computer was moved to a dodgy build of the (linux) kernel
that had issues in the filesystem layer.  In that event, the filesystem
eventually became so badly corrupted that the machine was unusable, but
the earliest symptom of trouble was the corrupted SQLite database.
7. SQLite Configuration Errors
SQLite has many built-in protections against database corruption.
But many of these protections can be disabled by configuration options.
If protections are disabled, database corruption may occur.
The following are examples of disabling the built-in protection
