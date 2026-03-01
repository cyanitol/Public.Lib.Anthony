Atomic Commit In SQLite
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
Atomic Commit In SQLite
Table Of Contents
1.  Introduction
2.  Hardware Assumptions
3.  Single File Commit
3.1.  Initial State
3.2.  Acquiring A Read Lock
3.3.  Reading Information Out Of The Database
3.4.  Obtaining A Reserved Lock
3.5.  Creating A Rollback Journal File
3.6.  Changing Database Pages In User Space
3.7.  Flushing The Rollback Journal File To Mass Storage
3.8.  Obtaining An Exclusive Lock
3.9.  Writing Changes To The Database File
3.10. 0 Flushing Changes To Mass Storage
3.11. 1 Deleting The Rollback Journal
3.12. 2 Releasing The Lock
4.  Rollback
4.1.  When Something Goes Wrong...
4.2.  Hot Rollback Journals
4.3.  Obtaining An Exclusive Lock On The Database
4.4.  Rolling Back Incomplete Changes
4.5.  Deleting The Hot Journal
4.6.  Continue As If The Uncompleted Writes Had Never Happened
5.  Multi-file Commit
5.1.  Separate Rollback Journals For Each Database
5.2.  The Super-Journal File
5.3.  Updating Rollback Journal Headers
5.4.  Updating The Database Files
5.5.  Delete The Super-Journal File
5.6.  Clean Up The Rollback Journals
6.  Additional Details Of The Commit Process
6.1.  Always Journal Complete Sectors
6.2.  Dealing With Garbage Written Into Journal Files
6.3.  Cache Spill Prior To Commit
7.  Optimizations
7.1.  Cache Retained Between Transactions
7.2.  Exclusive Access Mode
7.3.  Do Not Journal Freelist Pages
7.4.  Single Page Updates And Atomic Sector Writes
7.5.  Filesystems With Safe Append Semantics
7.6.  Persistent Rollback Journals
8.  Testing Atomic Commit Behavior
9.  Things That Can Go Wrong
9.1.  Broken Locking Implementations
9.2.  Incomplete Disk Flushes
9.3.  Partial File Deletions
9.4.  Garbage Written Into Files
9.5.  Deleting Or Renaming A Hot Journal
10.  Future Directions And Conclusion
1.  Introduction
An important feature of transactional databases like SQLite
is "atomic commit".
Atomic commit means that either all database changes within a single
transaction occur or none of them occur.  With atomic commit, it
is as if many different writes to different sections of the database
file occur instantaneously and simultaneously.
Real hardware serializes writes to mass storage, and writing
a single sector takes a finite amount of time.
So it is impossible to truly write many different sectors of a
database file simultaneously and/or instantaneously.
But the atomic commit logic within
SQLite makes it appear as if the changes for a transaction
are all written instantaneously and simultaneously.
SQLite has the important property that transactions appear
to be atomic even if the transaction is interrupted by an
operating system crash or power failure.
This article describes the techniques used by SQLite to create the
illusion of atomic commit.
The information in this article applies only when SQLite is operating
in "rollback mode", or in other words when SQLite is not
using a write-ahead log.  SQLite still supports atomic commit when
write-ahead logging is enabled, but it accomplishes atomic commit by
a different mechanism from the one described in this article.  See
the write-ahead log documentation for additional information on how
SQLite supports atomic commit in that context.
2.  Hardware Assumptions
Throughout this article, we will call the mass storage device "disk"
even though the mass storage device might really be flash memory.
We assume that disk is written in chunks which we call a "sector".
It is not possible to modify any part of the disk smaller than a sector.
To change a part of the disk smaller than a sector, you have to read in
the full sector that contains the part you want to change, make the
change, then write back out the complete sector.
On a traditional spinning disk, a sector is the minimum unit of transfer
in both directions, both reading and writing.  On flash memory, however,
the minimum size of a read is typically much smaller than a minimum write.
SQLite is only concerned with the minimum write amount and so for the
purposes of this article, when we say "sector" we mean the minimum amount
of data that can be written to mass storage in a single go.
  Prior to SQLite version 3.3.14, a sector size of 512 bytes was
  assumed in all cases.  There was a compile-time option to change
  this but the code had never been tested with a larger value.  The
  512 byte sector assumption seemed reasonable since until very recently
  all disk drives used a 512 byte sector internally.  However, there
  has recently been a push to increase the sector size of disks to
  4096 bytes.  Also the sector size
  for flash memory is usually larger than 512 bytes.  For these reasons,
  versions of SQLite beginning with 3.3.14 have a method in the OS
  interface layer that interrogates the underlying filesystem to find
  the true sector size.  As currently implemented (version 3.5.0) this
  method still returns a hard-coded value of 512 bytes, since there
  is no standard way of discovering the true sector size on either
  Unix or Windows.  But the method is available for embedded device
  manufacturers to tweak according to their own needs.  And we have
  left open the possibility of filling in a more meaningful implementation
  on Unix and Windows in the future.
SQLite has traditionally assumed that a sector write is not atomic.
However, SQLite does always assume that a sector write is linear.  By "linear"
we mean that SQLite assumes that when writing a sector, the hardware begins
at one end of the data and writes byte by byte until it gets to
the other end.  The write might go from beginning to end or from
end to beginning.  If a power failure occurs in the middle of a
sector write it might be that part of the sector was modified
and another part was left unchanged.  The key assumption by SQLite
is that if any part of the sector gets changed, then either the
first or the last bytes will be changed.  So the hardware will
never start writing a sector in the middle and work towards the
ends.  We do not know if this assumption is always true but it
seems reasonable.
The previous paragraph states that SQLite does not assume that
sector writes are atomic.  This is true by default.  But as of
SQLite version 3.5.0, there is a new interface called the
Virtual File System (VFS) interface.  The VFS is the only means
by which SQLite communicates to the underlying filesystem.  The
code comes with default VFS implementations for Unix and Windows
and there is a mechanism for creating new custom VFS implementations
at runtime.  In this new VFS interface there is a method called
xDeviceCharacteristics.  This method interrogates the underlying
filesystem to discover various properties and behaviors that the
filesystem may or may not exhibit.  The xDeviceCharacteristics
method might indicate that sector writes are atomic, and if it does
so indicate, SQLite will try to take advantage of that fact.  But
the default xDeviceCharacteristics method for both Unix and Windows
does not indicate atomic sector writes and so these optimizations
are normally omitted.
SQLite assumes that the operating system will buffer writes and
that a write request will return before data has actually been stored
in the mass storage device.
SQLite further assumes that write operations will be reordered by
the operating system.
For this reason, SQLite does a "flush" or "fsync" operation at key
points.  SQLite assumes that the flush or fsync will not return until
all pending write operations for the file that is being flushed have
completed.  We are told that the flush and fsync primitives
are broken on some versions of Windows and Linux.  This is unfortunate.
It opens SQLite up to the possibility of database corruption following
a power loss in the middle of a commit.  However, there is nothing
that SQLite can do to test for or remedy the situation.  SQLite
assumes that the operating system that it is running on works as
advertised.  If that is not quite the case, well then hopefully you
will not lose power too often.
SQLite assumes that when a file grows in length that the new
file space originally contains garbage and then later is filled in
with the data actually written.  In other words, SQLite assumes that
the file size is updated before the file content.  This is a
pessimistic assumption and SQLite has to do some extra work to make
sure that it does not cause database corruption if power is lost
between the time when the file size is increased and when the
new content is written.  The xDeviceCharacteristics method of
the VFS might indicate that the filesystem will always write the
data before updating the file size.  (This is the
SQLITE_IOCAP_SAFE_APPEND property for those readers who are looking
at the code.)  When the xDeviceCharacteristics method indicates
that files content is written before the file size is increased,
SQLite can forego some of its pedantic database protection steps
and thereby decrease the amount of disk I/O needed to perform a
commit.  The current implementation, however, makes no such assumptions
for the default VFSes for Windows and Unix.
SQLite assumes that a file deletion is atomic from the
point of view of a user process.  By this we mean that if SQLite
requests that a file be deleted and the power is lost during the
delete operation, once power is restored either the file will
exist completely with all if its original content unaltered, or
else the file will not be seen in the filesystem at all.  If
after power is restored the file is only partially deleted,
if some of its data has been altered or erased,
or the file has been truncated but not completely removed, then
database corruption will likely result.
SQLite assumes that the detection and/or correction of
bit errors caused by cosmic rays, thermal noise, quantum
fluctuations, device driver bugs, or other mechanisms, is the
responsibility of the underlying hardware and operating system.
SQLite does not add any redundancy to the database file for
the purpose of detecting corruption or I/O errors.
SQLite assumes that the data it reads is exactly the same data
that it previously wrote.
By default, SQLite assumes that an operating system call to write
a range of bytes will not damage or alter any bytes outside of that range
even if a power loss or OS crash occurs during that write.  We
call this the "powersafe overwrite" property.
Prior to version 3.7.9 (2011-11-01),
SQLite did not assume powersafe overwrite.  But with the standard
sector size increasing from 512 to 4096 bytes on most disk drives, it
has become necessary to assume powersafe overwrite in order to maintain
historical performance levels and so powersafe overwrite is assumed by
default in recent versions of SQLite.  The assumption of powersafe
overwrite property can be disabled at compile-time or a run-time if
desired.  See the powersafe overwrite documentation for further
details.
3.  Single File Commit
We begin with an overview of the steps SQLite takes in order to
perform an atomic commit of a transaction against a single database
file.  The details of file formats used to guard against damage from
power failures and techniques for performing an atomic commit across
multiple databases are discussed in later sections.
3.1.  Initial State
The state of the computer when a database connection is
first opened is shown conceptually by the diagram at the
right.
The area of the diagram on the extreme right (labeled "Disk") represents
information stored on the mass storage device.  Each rectangle is
a sector.  The blue color represents that the sectors contain
original data.
The middle area is the operating systems disk cache.  At the
onset of our example, the cache is cold and this is represented
by leaving the rectangles of the disk cache empty.
The left area of the diagram shows the content of memory for
the process that is using SQLite.  The database connection has
just been opened and no information has been read yet, so the
user space is empty.
3.2.  Acquiring A Read Lock
Before SQLite can write to a database, it must first read
the database to see what is there already.  Even if it is just
appending new data, SQLite still has to read in the database
schema from the "sqlite_schema" table so that it can know
how to parse the INSERT statements and discover where in the
database file the new information should be stored.
The first step toward reading from the database file
is obtaining a shared lock on the database file.  A "shared"
lock allows two or more database connections to read from the
database file at the same time.  But a shared lock prevents
another database connection from writing to the database file
while we are reading it.  This is necessary because if another
database connection were writing to the database file at the
same time we are reading from the database file, we might read
some data before the change and other data after the change.
This would make it appear as if the change made by the other
process is not atomic.
Notice that the shared lock is on the operating system
disk cache, not on the disk itself.  File locks
really are just flags within the operating system kernel,
usually.  (The details depend on the specific OS layer
interface.)  Hence, the lock will instantly vanish if the
operating system crashes or if there is a power loss.  It
is usually also the case that the lock will vanish if the
process that created the lock exits.
3.3.  Reading Information Out Of The Database
After the shared lock is acquired, we can begin reading
information from the database file.  In this scenario, we
are assuming a cold cache, so information must first be
read from mass storage into the operating system cache then
transferred from operating system cache into user space.
On subsequent reads, some or all of the information might
already be found in the operating system cache and so only
the transfer to user space would be required.
Usually only a subset of the pages in the database file
are read.  In this example we are showing three
pages out of eight being read.  In a typical application, a
database will have thousands of pages and a query will normally
only touch a small percentage of those pages.
3.4.  Obtaining A Reserved Lock
Before making changes to the database, SQLite first
obtains a "reserved" lock on the database file.  A reserved
lock is similar to a shared lock in that both a reserved lock
and shared lock allow other processes to read from the database
file.  A single reserve lock can coexist with multiple shared
locks from other processes.  However, there can only be a
single reserved lock on the database file.  Hence only a
single process can be attempting to write to the database
at one time.
The idea behind a reserved lock is that it signals that
a process intends to modify the database file in the near
future but has not yet started to make the modifications.
And because the modifications have not yet started, other
processes can continue to read from the database.  However,
no other process should also begin trying to write to the
database.
3.5.  Creating A Rollback Journal File
Prior to making any changes to the database file, SQLite first
creates a separate rollback journal file and writes into the
rollback journal the original
content of the database pages that are to be altered.
The idea behind the rollback journal is that it contains
all information needed to restore the database back to
its original state.
The rollback journal contains a small header (shown in green
in the diagram) that records the original size of the database
file.  So if a change causes the database file to grow, we
will still know the original size of the database.  The page
number is stored together with each database page that is
written into the rollback journal.
  When a new file is created, most desktop operating systems
  (Windows, Linux, Mac OS X) will not actually write anything to
  disk.  The new file is created in the operating systems disk
  cache only.  The file is not created on mass storage until sometime
  later, when the operating system has a spare moment.  This creates
  the impression to users that I/O is happening much faster than
  is possible when doing real disk I/O.  We illustrate this idea in
  the diagram to the right by showing that the new rollback journal
  appears in the operating system disk cache only and not on the
  disk itself.
3.6.  Changing Database Pages In User Space
After the original page content has been saved in the rollback
journal, the pages can be modified in user memory.  Each database
connection has its own private copy of user space, so the changes
that are made in user space are only visible to the database connection
that is making the changes.  Other database connections still see
the information in operating system disk cache buffers which have
not yet been changed.  And so even though one process is busy
modifying the database, other processes can continue to read their
own copies of the original database content.
3.7.  Flushing The Rollback Journal File To Mass Storage
The next step is to flush the content of the rollback journal
file to nonvolatile storage.
As we will see later,
this is a critical step in insuring that the database can survive
an unexpected power loss.
This step also takes a lot of time, since writing to nonvolatile
storage is normally a slow operation.
This step is usually more complicated than simply flushing
the rollback journal to the disk.  On most platforms two separate
flush (or fsync()) operations are required.  The first flush writes
out the base rollback journal content.  Then the header of the
rollback journal is modified to show the number of pages in the
rollback journal.  Then the header is flushed to disk.  The details
on why we do this header modification and extra flush are provided
in a later section of this paper.
3.8.  Obtaining An Exclusive Lock
Prior to making changes to the database file itself, we must
obtain an exclusive lock on the database file.  Obtaining an
exclusive lock is really a two-step process.  First SQLite obtains
a "pending" lock.  Then it escalates the pending lock to an
exclusive lock.
A pending lock allows other processes that already have a
shared lock to continue reading the database file.  But it
prevents new shared locks from being established.  The idea
behind a pending lock is to prevent writer starvation caused
by a large pool of readers.  There might be dozens, even hundreds,
of other processes trying to read the database file.  Each process
acquires a shared lock before it starts reading, reads what it
needs, then releases the shared lock.  If, however, there are
many different processes all reading from the same database, it
might happen that a new process always acquires its shared lock before
the previous process releases its shared lock.  And so there is
never an instant when there are no shared locks on the database
file and hence there is never an opportunity for the writer to
seize the exclusive lock.  A pending lock is designed to prevent
that cycle by allowing existing shared locks to proceed but
blocking new shared locks from being established.  Eventually
all shared locks will clear and the pending lock will then be
able to escalate into an exclusive lock.
3.9.  Writing Changes To The Database File
Once an exclusive lock is held, we know that no other
processes are reading from the database file and it is
safe to write changes into the database file.  Usually
those changes only go as far as the operating systems disk
cache and do not make it all the way to mass storage.
3.10. 0 Flushing Changes To Mass Storage
Another flush must occur to make sure that all the
database changes are written into nonvolatile storage.
This is a critical step to ensure that the database will
survive a power loss without damage.  However, because
of the inherent slowness of writing to disk or flash memory,
this step together with the rollback journal file flush in section
3.7 above takes up most of the time required to complete a
transaction commit in SQLite.
3.11. 1 Deleting The Rollback Journal
After the database changes are all safely on the mass
storage device, the rollback journal file is deleted.
This is the instant where the transaction commits.
If a power failure or system crash occurs prior to this
point, then recovery processes to be described later make
it appear as if no changes were ever made to the database
file.  If a power failure or system crash occurs after
the rollback journal is deleted, then it appears as if
all changes have been written to disk.  Thus, SQLite gives
the appearance of having made no changes to the database
file or having made the complete set of changes to the
database file depending on whether or not the rollback
journal file exists.
Deleting a file is not really an atomic operation, but
it appears to be from the point of view of a user process.
A process is always able to ask the operating system "does
this file exist?" and the process will get back a yes or no
answer.  After a power failure that occurs during a
transaction commit, SQLite will ask the operating system
whether or not the rollback journal file exists.  If the
answer is "yes" then the transaction is incomplete and is
rolled back.  If the answer is "no" then it means the transaction
did commit.
The existence of a transaction depends on whether or
not the rollback journal file exists and the deletion
of a file appears to be an atomic operation from the point of
view of a user-space process.  Therefore,
a transaction appears to be an atomic operation.
The act of deleting a file is expensive on many systems.
As an optimization, SQLite can be configured to truncate
the journal file to zero bytes in length
or overwrite the journal file header with zeros.  In either
case, the resulting journal file is no longer capable of rolling
back and so the transaction still commits.  Truncating a file
to zero length, like deleting a file, is assumed to be an atomic
operation from the point of view of a user process.  Overwriting
the header of the journal with zeros is not atomic, but if any
part of the header is malformed the journal will not roll back.
Hence, one can say that the commit occurs as soon as the header
is sufficiently changed to make it invalid.  Typically this happens
as soon as the first byte of the header is zeroed.
3.12. 2 Releasing The Lock
The last step in the commit process is to release the
exclusive lock so that other processes can once again
start accessing the database file.
In the diagram at the right, we show that the information
that was held in user space is cleared when the lock is released.
This used to be literally true for older versions of SQLite.  But
more recent versions of SQLite keep the user space information
in memory in case it might be needed again at the start of the
