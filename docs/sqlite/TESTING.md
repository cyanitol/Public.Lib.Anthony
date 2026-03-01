How SQLite Is Tested
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
How SQLite Is Tested
Table Of Contents
1. Introduction
1.1. Executive Summary
2. Test Harnesses
3. Anomaly Testing
3.1. Out-Of-Memory Testing
3.2. I/O Error Testing
3.3. Crash Testing
3.4. Compound failure tests
4. Fuzz Testing
4.1. SQL Fuzz
4.1.1. SQL Fuzz Using The American Fuzzy Lop Fuzzer
4.1.2. Google OSS Fuzz
4.1.3. The dbsqlfuzz and jfuzz fuzzers
4.1.4. Other third-party fuzzers
4.1.5. The fuzzcheck test harness
4.1.6. Tension Between Fuzz Testing And 100% MC/DC Testing
4.2. Malformed Database Files
4.3. Boundary Value Tests
5. Regression Testing
6. Automatic Resource Leak Detection
7. Test Coverage
7.1. Statement versus branch coverage
7.2. Coverage testing of defensive code
7.3. Forcing coverage of boundary values and boolean vector tests
7.4. Branch coverage versus MC/DC
7.5. Measuring branch coverage
7.6. Mutation testing
7.7. Experience with full test coverage
8. Dynamic Analysis
8.1. Assert
8.2. Valgrind
8.3. Memsys2
8.4. Mutex Asserts
8.5. Journal Tests
8.6. Undefined Behavior Checks
9. Disabled Optimization Tests
10. Checklists
11. Static Analysis
12. Summary
1. Introduction
The reliability and robustness of SQLite is achieved in part
by thorough and careful testing.
As of version 3.42.0 (2023-05-16),
the SQLite library consists of approximately
155.8 KSLOC of C code.
(KSLOC means thousands of "Source Lines Of Code" or, in other words,
lines of code excluding blank lines and comments.)
By comparison, the project has
590 times as much
test code and test scripts -
92053.1 KSLOC.
1.1. Executive Summary
 Four independently developed test harnesses
 100% branch test coverage in an as-deployed configuration
 Millions and millions of test cases
 Out-of-memory tests
 I/O error tests
 Crash and power loss tests
 Fuzz tests
 Boundary value tests
 Disabled optimization tests
 Regression tests
 Malformed database tests
 Extensive use of assert() and run-time checks
 Valgrind analysis
 Undefined behavior checks
 Checklists
2. Test Harnesses
There are four independent test harnesses used for testing the
core SQLite library.
Each test harness is designed, maintained, and managed separately
from the others.
The TCL Tests are the original tests for SQLite.
They are contained in the same source tree as the
SQLite core and like the SQLite core are in the public domain.  The
TCL tests are the primary tests used during development.
The TCL tests are written using the
TCL scripting language.
The TCL test harness itself consists of 27.2 KSLOC
of C code used to create the TCL interface.  The test scripts are contained
in 1390 files totaling
23.2MB in size.  There are
51445 distinct test cases, but many of the test
cases are parameterized and run multiple times (with different parameters)
so that on a full test run millions of
separate tests are performed.
The TH3 test harness is a set of proprietary tests, written in
C that provide 100% branch test coverage
(and 100% MC/DC test coverage) to
the core SQLite library.  The TH3 tests are designed to run
on embedded and specialized platforms that would not easily support
TCL or other workstation services.  TH3 tests use only the published
SQLite interfaces. TH3 consists of about
76.9 MB or 1055.4 KSLOC
of C code implementing 50362 distinct test cases.
TH3 tests are heavily parameterized, though, so a full-coverage test runs
about 2.4 million different test
instances.
The cases that provide 100% branch test coverage constitute
a subset of the total TH3 test suite.  A soak test
prior to release does about
248.5 million tests.
Additional information on TH3 is available separately.
The SQL Logic Test
or SLT test harness is used to run huge numbers
of SQL statements against both SQLite and several other SQL database engines
and verify that they all get the same answers.  SLT currently compares
SQLite against PostgreSQL, MySQL, Microsoft SQL Server, and Oracle 10g.
SLT runs 7.2 million queries comprising
1.12GB of test data.
The dbsqlfuzz engine is a
proprietary fuzz tester.  Other fuzzers for SQLite
mutate either the SQL inputs or the database file.  Dbsqlfuzz mutates
both the SQL and the database file at the same time, and is thus able
to reach new error states.  Dbsqlfuzz is built using the
libFuzzer framework of LLVM
with a custom mutator.  There are
336 seed files. The dbsqlfuzz fuzzer
runs about one billion test mutations per day.
Dbsqlfuzz helps ensure
that SQLite is robust against attack via malicious SQL or database
inputs.
In addition to the four main test harnesses, there are many other
small programs that implement specialized tests.  Here are a few
examples:
The "speedtest1.c" program
estimates the performance of SQLite under a typical workload.
The "mptester.c" program is a stress test for multiple processes
concurrently reading and writing a single database.
The "threadtest3.c" program is a stress test for multiple threads using
SQLite simultaneously.
The "fuzzershell.c" program is used to
run some fuzz tests.
The "jfuzz" program is a libfuzzer-based fuzzer for
JSONB inputs to the JSON SQL functions.
All of the tests above must run successfully, on multiple platforms
and under multiple compile-time configurations,
before each release of SQLite.
Prior to each check-in to the SQLite source tree, developers
typically run a subset (called "veryquick") of the Tcl tests
consisting of about
304.7 thousand test cases.
The veryquick tests include most tests other than the anomaly, fuzz, and
soak tests.  The idea behind the veryquick tests are that they are
sufficient to catch most errors, but also run in only a few minutes
instead of a few hours.
3. Anomaly Testing
Anomaly tests are tests designed to verify the correct behavior
of SQLite when something goes wrong.  It is (relatively) easy to build
an SQL database engine that behaves correctly on well-formed inputs
on a fully functional computer.  It is more difficult to build a system
that responds sanely to invalid inputs and continues to function following
system malfunctions.  The anomaly tests are designed to verify the latter
behavior.
3.1. Out-Of-Memory Testing
SQLite, like all SQL database engines, makes extensive use of
malloc()  (See the separate report on
dynamic memory allocation in SQLite for
additional detail.)
On servers and workstations, malloc() never fails in practice and so correct
handling of out-of-memory (OOM) errors is not particularly important.
But on embedded devices, OOM errors are frighteningly common and since
SQLite is frequently used on embedded devices, it is important that
SQLite be able to gracefully handle OOM errors.
OOM testing is accomplished by simulating OOM errors.
SQLite allows an application to substitute an alternative malloc()
implementation using the sqlite3_config(SQLITE_CONFIG_MALLOC,...)
interface.  The TCL and TH3 test harnesses are both capable of
inserting a modified version of malloc() that can be rigged to fail
after a certain number of allocations.  These instrumented mallocs
can be set to fail only once and then start working again, or to
continue failing after the first failure.  OOM tests are done in a
loop.  On the first iteration of the loop, the instrumented malloc
is rigged to fail on the first allocation.  Then some SQLite operation
is carried out and checks are done to make sure SQLite handled the
OOM error correctly.  Then the time-to-failure counter
on the instrumented malloc is increased by one and the test is
repeated.  The loop continues until the entire operation runs to
completion without ever encountering a simulated OOM failure.
Tests like this are run twice, once with the instrumented malloc
set to fail only once, and again with the instrumented malloc set
to fail continuously after the first failure.
3.2. I/O Error Testing
I/O error testing seeks to verify that SQLite responds sanely
to failed I/O operations.  I/O errors might result from a full disk drive,
malfunctioning disk hardware, network outages when using a network
file system, system configuration or permission changes that occur in the
middle of an SQL operation, or other hardware or operating system
malfunctions.  Whatever the cause, it is important that SQLite be able
to respond correctly to these errors and I/O error testing seeks to
verify that it does.
I/O error testing is similar in concept to OOM testing; I/O errors
are simulated and checks are made to verify that SQLite responds
correctly to the simulated errors.  I/O errors are simulated in both
the TCL and TH3 test harnesses by inserting a new
Virtual File System object that is specially rigged
to simulate an I/O error after a set number of I/O operations.
As with OOM error testing, the I/O error simulators can be set to
fail just once, or to fail continuously after the first failure.
Tests are run in a loop, slowly increasing the point of failure until
the test case runs to completion without error.  The loop is run twice,
once with the I/O error simulator set to simulate only a single failure
and a second time with it set to fail all I/O operations after the first
failure.
In I/O error tests, after the I/O error simulation failure mechanism
is disabled, the database is examined using
PRAGMA integrity_check to make sure that the I/O error has not
introduced database corruption.
3.3. Crash Testing
Crash testing seeks to demonstrate that an SQLite database will not
go corrupt if the application or operating system crashes or if there
is a power failure in the middle of a database update.  A separate
white-paper titled
Atomic Commit in SQLite describes the
defensive measures SQLite takes to prevent database corruption following
a crash.  Crash tests strive to verify that those defensive measures
are working correctly.
It is impractical to do crash testing using real power failures, of
course, and so crash testing is done in simulation.  An alternative
Virtual File System is inserted that allows the test
harness to simulate the state of the database file following a crash.
In the TCL test harness, the crash simulation is done in a separate
process.  The main testing process spawns a child process which runs
some SQLite operation and randomly crashes somewhere in the middle of
a write operation.  A special VFS randomly reorders and corrupts
the unsynchronized
write operations to simulate the effect of buffered filesystems.  After
the child dies, the original test process opens and reads the test
database and verifies that the changes attempted by the child either
completed successfully or else were completely rolled back.  The
integrity_check PRAGMA is used to make sure no database corruption
occurs.
The TH3 test harness needs to run on embedded systems that do not
necessarily have the ability to spawn child processes, so it uses
an in-memory VFS to simulate crashes.  The in-memory VFS can be rigged
to make a snapshot of the entire filesystem after a set number of I/O
operations.  Crash tests run in a loop.  On each iteration of the loop,
the point at which a snapshot is made is advanced until the SQLite
operations being tested run to completion without ever hitting a
snapshot.  Within the loop, after the SQLite operation under test has
completed, the filesystem is reverted to the snapshot and random file
damage is introduced that is characteristic of the kinds of damage
one expects to see following a power loss.  Then the database is opened
and checks are made to ensure that it is well-formed and that the
transaction either ran to completion or was completely rolled back.
The interior of the loop is repeated multiple times for each
snapshot with different random damage each time.
3.4. Compound failure tests
The test suites for SQLite also explore the result of stacking
multiple failures.  For example, tests are run to ensure correct behavior
when an I/O error or OOM fault occurs while trying to recover from a
prior crash.
4. Fuzz Testing
Fuzz testing
seeks to establish that SQLite responds correctly to invalid, out-of-range,
or malformed inputs.
4.1. SQL Fuzz
SQL fuzz testing consists of creating syntactically correct yet
wildly nonsensical SQL statements and feeding them to SQLite to see
what it will do with them.  Usually some kind of error is returned
(such as "no such table").  Sometimes, purely by chance, the SQL
statement also happens to be semantically correct.  In that case, the
resulting prepared statement is run to make sure it gives a reasonable
result.
4.1.1. SQL Fuzz Using The American Fuzzy Lop Fuzzer
The concept of fuzz testing has been around for decades, but fuzz
testing was not an effective way to find bugs until 2014 when
Michal Zalewski invented the first practical profile-guided fuzzer,
American Fuzzy Lop or "AFL".
Unlike prior fuzzers that blindly generate random inputs, AFL
instruments the program being tested (by modifying the assembly-language
output from the C compiler) and uses that instrumentation to detect when
an input causes the program to do something different - to follow
a new control path or loop a different number of times.  Inputs that provoke
new behavior are retained and further mutated.  In this way, AFL is able
to "discover" new behaviors of the program under test, including behaviors
that were never envisioned by the designers.
AFL proved adept at finding arcane bugs in SQLite.
Most of the findings have been assert() statements where the conditional
was false under obscure circumstances.  But AFL has also found
a fair number of crash bugs in SQLite, and even a few cases where SQLite
computed incorrect results.
Because of its past success, AFL became a standard part of the testing
strategy for SQLite beginning with version 3.8.10 (2015-05-07) until
it was superseded by better fuzzers in version 3.29.0 (2019-07-10).
4.1.2. Google OSS Fuzz
Beginning in 2016, a team of engineers at Google started the
OSS Fuzz project.
OSS Fuzz uses a AFL-style guided fuzzer running on Google's infrastructure.
The Fuzzer automatically downloads the latest check-ins for participating
projects, fuzzes them, and sends email to the developers reporting any
problems.  When a fix is checked in, the fuzzer automatically detects this
and emails a confirmation to the developers.
SQLite is one of many open-source projects that OSS Fuzz tests. The
test/ossfuzz.c source file
in the SQLite repository is SQLite's interface to OSS fuzz.
OSS Fuzz no longer finds historical bugs in SQLite.  But it is still
running and does occasionally find issues in new development check-ins.
Examples:
&#91;1&#93;
&#91;2&#93;
&#91;3&#93;.
4.1.3. The dbsqlfuzz and jfuzz fuzzers
Beginning in late 2018, SQLite has been fuzzed using a proprietary
fuzzer called "dbsqlfuzz".  Dbsqlfuzz is built using the
libFuzzer framework of LLVM.
The dbsqlfuzz fuzzer mutates both the SQL input and the database file
at the same time.  Dbsqlfuzz uses a custom
Structure-Aware Mutator
on a specialized input file that defines both an input database and SQL
text to be run against that database. Because it mutates both the input
database and the input SQL at the same time, dbsqlfuzz has been able to
find some obscure faults in SQLite that were missed by prior fuzzers that
mutated only SQL inputs or only the database file.
The SQLite developers keep dbsqlfuzz running against trunk in about
16 cores at all times.  Each instance of dbsqlfuzz program is able to
evalutes about 400 test cases per second, meaning that about 500 million
cases are checked every day.
The dbsqlfuzz fuzzer has been very successful at hardening the
SQLite code base against malicious attack.  Since dbsqlfuzz has been
added to the SQLite internal test suite, bug reports from external
fuzzers such as OSSFuzz have all but stopped.
Note that dbsqlfuzz is not the Protobuf-based structure-aware
fuzzer for SQLite that is used by Chromium and described in the
Structure-Aware Mutator article.
There is no connection between these two fuzzers, other than the fact that they
are both based on libFuzzer
The Protobuf fuzzer for SQLite is written and maintained by the Chromium
team at Google, whereas dbsqlfuzz is written and maintained by the original
SQLite developers.  Having multiple independently-developed fuzzers for SQLite
is good, as it means that obscure issues are more likely to be uncovered.
Near the end of January 2024, a second libFuzzer-based tool called
"jfuzz" came into use.  Jfuzz generates corrupt JSONB blobs and feeds
them into the JSON SQL functions to verify that the JSON functions
are able to safely and efficiently deal with corrupt binary inputs.
4.1.4. Other third-party fuzzers
SQLite seems to be a popular target for third-parties to fuzz.
The developers hear about many attempts to fuzz SQLite
and they do occasionally get bug reports found by independent
fuzzers.  All such reports are promptly fixed, so the product is
improved and that the entire SQLite user community benefits.
This mechanism of having many independent testers is similar to
Linus's law:
"given enough eyeballs, all bugs are shallow".
One fuzzing researcher of particular note is
Manuel Rigger.
Most fuzzers only look for assertion faults, crashes, undefined behavior (UB),
or other easily detected anomalies.  Dr. Rigger's fuzzers, on the other hand,
are able to find cases where SQLite computes an incorrect answer.
Rigger has found
many such cases.
Most of these finds are obscure corner cases involving type
conversions and affinity transformations, and a good number of the finds
are against unreleased features.  Nevertheless, his finds are still important
as they are real bugs,
and the SQLite developers are grateful to be able to identify and fix
the underlying problems.
4.1.5. The fuzzcheck test harness
Historical test cases from AFL, OSS Fuzz, and dbsqlfuzz are
collected in a set of database files in the main SQLite source tree
and then rerun by the "fuzzcheck" utility program whenever one runs
"make test".  Fuzzcheck only runs a few thousand "interesting" cases
out of the billions of cases that the various fuzzers have
examined over the years.  "Interesting" cases are cases that exhibit
previously unseen behavior.  Actual bugs found by fuzzers are always
included among the interesting test cases, but most of the cases run
by fuzzcheck were never actual bugs.
4.1.6. Tension Between Fuzz Testing And 100% MC/DC Testing
Fuzz testing and 100% MC/DC testing are in tension with
one another.
That is to say, code tested to 100% MC/DC will tend to be
more vulnerable to problems found by fuzzing and code that performs
well during fuzz testing will tend to have (much) less than
100% MC/DC.
This is because MC/DC testing discourages defensive code with
unreachable branches, but without defensive code, a fuzzer is
more likely to find a path that causes problems.  MC/DC testing
seems to work well for building code that is robust during
normal use, whereas fuzz testing is good for building code that is
robust against malicious attack.
Of course, users would prefer code that is both robust in normal
use and resistant to malicious attack.  The SQLite developers are
dedicated to providing that.  The purpose of this section is merely
to point out that doing both at the same time is difficult.
For much of its history SQLite has been focused on 100% MC/DC testing.
Resistance to fuzzing attacks only became a concern with the introduction
of AFL in 2014.  For a while there, fuzzers were finding many problems
in SQLite.  In more recent years, the testing strategy of SQLite has
evolved to place more emphasis on fuzz testing.  We still maintain
100% MC/DC of the core SQLite code, but most testing CPU cycles are
now devoted to fuzzing.
While fuzz testing and 100% MC/DC testing are in tension, they
are not completely at cross-purposes.  The fact that the SQlite test
suite does test to 100% MC/DC means that when fuzzers do find problems,
those problems can be fixed quickly and with little risk of introducing
new errors.
4.2. Malformed Database Files
There are numerous test cases that verify that SQLite is able to
deal with malformed database files.
These tests first build a well-formed database file, then add
corruption by changing one or more bytes in the file by some means
other than SQLite.  Then SQLite is used to read the database.
In some cases, the bytes changes are in the middle of data.
This causes the content of the database to change while keeping the
database well-formed.
In other cases, unused bytes of the file are modified, which has
no effect on the integrity of the database.
The interesting cases are when bytes of the file that
define database structure get changed.  The malformed database tests
verify that SQLite finds the file format errors and reports them
using the SQLITE_CORRUPT return code without overflowing
buffers, dereferencing NULL pointers, or performing other
unwholesome actions.
The dbsqlfuzz fuzzer also does an excellent job of verifying
that SQLite responds sanely to malformed database files.
4.3. Boundary Value Tests
