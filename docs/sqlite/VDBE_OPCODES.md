The SQLite Bytecode Engine
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
The SQLite Bytecode Engine
Table Of Contents
1. Executive Summary
2. Introduction
2.1. VDBE Source Code
2.2. Instruction Format
2.3. Registers
2.4. B-Tree Cursors
2.5. Subroutines, Coroutines, and Subprograms
2.6. Self-Altering Code
3. Viewing The Bytecode
4. The Opcodes
1. Executive Summary
SQLite works by translating SQL statements into bytecode and
then running that bytecode in a virtual machine.  This document
describes how the bytecode engine works.
This document describes SQLite internals.  The information provided
here is not needed for routine application development using SQLite.
This document is intended for people who want to delve more deeply into
the internal operation of SQLite.
The bytecode engine is not an API of SQLite.  Details
about the bytecode engine change from one release of SQLite to the next.
Applications that use SQLite should not depend on any of the details
found in this document.
See the document "Why SQLite Uses Bytecode" for some reasons why
SQLite prefers to use bytecode to implement SQL.
2. Introduction
SQLite works by translating each SQL statement into bytecode and
then running that bytecode.
A prepared statement in SQLite is mostly just the bytecode needed to
implement the corresponding SQL.  The sqlite3_prepare_v2() interface
is a compiler that translates SQL into bytecode.
The sqlite3_step() interface is the virtual machine that runs the
bytecode contained within the prepared statement.
The bytecode virtual machine is the heart of SQLite.
Programmers who want to understand how SQLite operates internally
must be familiar with the bytecode engine.
Historically, the bytecode engine in SQLite is called the
"Virtual DataBase Engine" or "VDBE".  This website uses the terms
"bytecode engine", "VDBE", "virtual machine", and "bytecode virtual
machine" interchangeably, as they all mean the same thing.
This article also uses the terms "bytecode program" and
"prepared statement" interchangeably, as they are mostly the same thing.
2.1. VDBE Source Code
The source code to the bytecode engine is in the
vdbe.c source
file.  The opcode definitions in this document are derived
from comments in that source file. The
source code comments are the canonical source of information
about the bytecode engine.  When in doubt, refer to the source code.
In addition to the primary vdbe.c source code file, there are
other helper code files in the source tree, all of whose names
begin with "vdbe" - short for "Virtual DataBase Engine".
Remember that the names and meanings of opcodes often change from
one release of SQLite to the next.  So if you are studying the EXPLAIN
output from SQLite, you should reference the version of this document
(or the vdbe.c source code)
that corresponds to the version of SQLite that ran the EXPLAIN.
Otherwise, the description of the opcodes may not be accurate.
This document is derived from SQLite
 version 3.51.2 check-in
b270f8339eb13 dated 2026-01-09.
2.2. Instruction Format
A bytecoded program in SQLite consists of one or more instructions.
Each instruction has an opcode and
five operands named P1, P2  P3, P4, and P5.  The P1, P2, and P3
operands are 32-bit signed integers.  These operands often refer to
registers.  For instructions that operate on b-tree cursors,
the P1 operand is usually the cursor number.
For jump instructions, P2 is usually the jump destination.
P4 may be a 32-bit signed integer, a 64-bit signed integer, a
64-bit floating point value, a string literal, a Blob literal,
a pointer to a collating sequence comparison function, or a
pointer to the implementation of an application-defined SQL
function, or various other things.  P5 is a 16-bit unsigned integer
normally used to hold flags.  Bits of the P5 flag can sometimes affect
the opcode in subtle ways.  For example, if the
SQLITE_NULLEQ (0x0080) bit of the P5 operand
is set on the Eq opcode, then the NULL values compare
equal to one another.  Otherwise NULL values compare different
from one another.
Some opcodes use all five operands.  Some opcodes use
one or two.  Some opcodes use none of the operands.
The bytecode engine begins execution on instruction number 0.
Execution continues until a Halt instruction is seen, or until
the program counter becomes greater than the address of
last instruction, or until there is an error.
When the bytecode engine halts, all memory
that it allocated is released and all database cursors it may
have had open are closed.  If the execution stopped due to an
error, any pending transactions are terminated and changes made
to the database are rolled back.
The ResultRow opcode causes the
bytecode engine to pause and the corresponding sqlite3_step()
call to return SQLITE_ROW.  Before invoking
ResultRow, the bytecoded program will
have loaded the results for a single row of a query into a series
of registers.  C-language APIs such as sqlite3_column_int()
or sqlite3_column_text() extract the query results from those
registers.  The bytecode engine resumes with the next instruction
after the ResultRow on the next call
to sqlite3_step().
2.3. Registers
Every bytecode program has a fixed (but potentially large) number of
registers.  A single register can hold a variety of objects:
 A NULL value
 A signed 64-bit integer
 An IEEE double-precision (64-bit) floating point number
 An arbitrary length string
 An arbitrary length BLOB
 A RowSet object (See the RowSetAdd, RowSetRead, and
                      RowSetTest opcodes)
 A Frame object (Used by subprograms - see Program)
A register can also be "Undefined" meaning that it holds no value
at all.  Undefined is different from NULL.  Depending on compile-time
options, an attempt to read an undefined register will usually cause
a run-time error.  If the code generator (sqlite3_prepare_v2())
ever generates a prepared statement that reads an Undefined register,
that is a bug in the code generator.
Registers are numbered beginning with 0.
Most opcodes refer to at least one register.
The number of registers in a single prepared statement is fixed
at compile-time.  The content of all registers is cleared when
a prepared statement is reset or
finalized.
The internal Mem object stores the value for a single register.
The abstract sqlite3_value object that is exposed in the API is really
just a Mem object or register.
2.4. B-Tree Cursors
A prepared statement can have
zero or more open cursors.  Each cursor is identified by a
small integer, which is usually the P1 parameter to the opcode
that uses the cursor.
There can be multiple cursors open on the same index or table.
All cursors operate independently, even cursors pointing to the same
indices or tables.
The only way for the virtual machine to interact with a database
file is through a cursor.
Instructions in the virtual machine can create a new cursor 
(ex: OpenRead or OpenWrite),
read data from a cursor (Column),
advance the cursor to the next entry in the table
(ex: Next or Prev), and so forth.
All cursors are automatically
closed when the prepared statement is reset or
finalized.
2.5. Subroutines, Coroutines, and Subprograms
The bytecode engine has no stack on which to store the return address
of a subroutine.  Return addresses must be stored in registers.
Hence, bytecode subroutines are not reentrant.
The Gosub opcode stores the current program counter into
register P1 then jumps to address P2.  The Return opcode jumps
to address P1+1.  Hence, every subroutine is associated with two integers:
the address of the entry point in the subroutine and the register number
that is used to hold the return address.
The Yield opcode swaps the value of the program counter with
the integer value in register P1.  This opcode is used to implement
coroutines.  Coroutines are often used to implement subqueries from
which content is pulled on an as-needed basis.
Triggers need to be reentrant.
Since bytecode
subroutines are not reentrant a different mechanism must be used to
implement triggers.  Each trigger is implemented using a separate bytecode
program with its own opcodes, program counter, and register set.  The
Program opcode invokes the trigger subprogram.  The Program instruction
allocates and initializes a fresh register set for each invocation of the
subprogram, so subprograms can be reentrant and recursive.  The
Param opcode is used by subprograms to access content in registers
of the calling bytecode program.
2.6. Self-Altering Code
Some opcodes are self-altering.
For example, the Init opcode (which is always the first opcode
in every bytecode program) increments its P1 operand.  Subsequent
Once opcodes compare their P1 operands to the P1 value for
the Init opcode in order to determine if the one-time initialization
code that follows should be skipped.
Another example is the String8 opcode which converts its P4
operand from UTF-8 into the correct database string encoding, then
converts itself into a String opcode.
3. Viewing The Bytecode
Every SQL statement that SQLite interprets results in a program
for the virtual machine.  But if the SQL statement begins with
the keyword EXPLAIN the virtual machine will not execute the
program.  Instead, the instructions of the program will be returned,
one instruction per row,
like a query result.  This feature is useful for debugging and
for learning how the virtual machine operates.  For example:
$ sqlite3 ex1.db
sqlite> explain delete from tbl1 where two<20;
addr  opcode         p1    p2    p3    p4             p5  comment      
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     12    0                    00  Start at 12  
1     Null           0     1     0                    00  r[1]=NULL    
2     OpenWrite      0     2     0     3              00  root=2 iDb=0; tbl1
3     Rewind         0     10    0                    00               
4       Column         0     1     2                    00  r[2]=tbl1.two
5       Ge             3     9     2     (BINARY)       51  if r[2]>=r[3] goto 9
6       Rowid          0     4     0                    00  r[4]=rowid   
7       Once           0     8     0                    00               
8       Delete         0     1     0     tbl1           02               
9     Next           0     4     0                    01               
10    Noop           0     0     0                    00               
11    Halt           0     0     0                    00               
12    Transaction    0     1     1     0              01  usesStmtJournal=0
13    TableLock      0     2     1     tbl1           00  iDb=0 root=2 write=1
14    Integer        20    3     0                    00  r[3]=20      
15    Goto           0     1     0                    00
Any application can run an EXPLAIN query to get output similar to
the above.
However, indentation to show the loop structure is not generated
by the SQLite core.  The command-line shell contains extra logic
for indenting loops.
Also, the "comment" column in the EXPLAIN output
is only provided if SQLite is compiled with the
-DSQLITE_ENABLE_EXPLAIN_COMMENTS options.
When SQLite is compiled with the SQLITE_DEBUG compile-time option,
extra PRAGMA commands are available that are useful for debugging and
for exploring the operation of the VDBE.  For example the vdbe_trace
pragma can be enabled to cause a disassembly of each VDBE opcode to be
printed on standard output as the opcode is executed.  These debugging
pragmas include:
 PRAGMA parser_trace
 PRAGMA vdbe_addoptrace
 PRAGMA vdbe_debug
 PRAGMA vdbe_listing
 PRAGMA vdbe_trace
4. The Opcodes
There are currently 191
opcodes defined by the virtual machine.
All currently defined opcodes are described in the table below.
This table was generated automatically by scanning the source code
from the file
vdbe.c.
Remember: The VDBE opcodes are not part of the interface 
definition for SQLite.  The number of opcodes and their names and meanings
change from one release of SQLite to the next.
The opcodes shown in the table below are valid for SQLite
 version 3.51.2 check-in
b270f8339eb13 dated 2026-01-09.
  .optab td {vertical-align:top; padding: 1ex 1ex;}
  Opcode NameDescription
Abortable
Verify that an Abort can happen.  Assert if an Abort at this point
might cause database corruption.  This opcode only appears in debugging
builds.
An Abort is safe if either there have been no writes, or if there is
an active statement journal.
Add
Add the value in register P1 to the value in register P2
and store the result in register P3.
If either input is NULL, the result is NULL.
AddImm
Add the constant P2 to the value in register P1.
The result is always an integer.
To force any register to be an integer, just add 0.
Affinity
Apply affinities to a range of P2 registers starting with P1.
P4 is a string that is P2 characters long. The N-th character of the
string indicates the column affinity that should be used for the N-th
memory cell in the range.
AggFinal
P1 is the memory location that is the accumulator for an aggregate
or window function.  Execute the finalizer function
for an aggregate and store the result in P1.
P2 is the number of arguments that the step function takes and
P4 is a pointer to the FuncDef for this function.  The P2
argument is not used by this opcode.  It is only there to disambiguate
functions that can take varying numbers of arguments.  The
P4 argument is only needed for the case where
the step function was not previously called.
AggInverse
Execute the xInverse function for an aggregate.
The function has P5 arguments.  P4 is a pointer to the
FuncDef structure that specifies the function.  Register P3 is the
accumulator.
The P5 arguments are taken from register P2 and its
successors.
AggStep
Execute the xStep function for an aggregate.
The function has P5 arguments.  P4 is a pointer to the
FuncDef structure that specifies the function.  Register P3 is the
accumulator.
The P5 arguments are taken from register P2 and its
successors.
AggStep1
Execute the xStep (if P1==0) or xInverse (if P1!=0) function for an
aggregate.  The function has P5 arguments.  P4 is a pointer to the
FuncDef structure that specifies the function.  Register P3 is the
accumulator.
The P5 arguments are taken from register P2 and its
successors.
This opcode is initially coded as OP_AggStep0.  On first evaluation,
the FuncDef stored in P4 is converted into an sqlite3_context and
the opcode is changed.  In this way, the initialization of the
sqlite3_context only happens once, instead of on each call to the
step function.
AggValue
Invoke the xValue() function and store the result in register P3.
P2 is the number of arguments that the step function takes and
P4 is a pointer to the FuncDef for this function.  The P2
argument is not used by this opcode.  It is only there to disambiguate
functions that can take varying numbers of arguments.  The
P4 argument is only needed for the case where
the step function was not previously called.
And
Take the logical AND of the values in registers P1 and P2 and
write the result into register P3.
If either P1 or P2 is 0 (false) then the result is 0 even if
the other input is NULL.  A NULL and true or two NULLs give
a NULL output.
AutoCommit
Set the database auto-commit flag to P1 (1 or 0). If P2 is true, roll
back any currently active btree transactions. If there are any active
VMs (apart from this one), then a ROLLBACK fails.  A COMMIT fails if
there are active writing VMs or active VMs that use shared cache.
This instruction causes the VM to halt.
BeginSubrtn
Mark the beginning of a subroutine that can be entered in-line
or that can be called using Gosub.  The subroutine should
be terminated by an Return instruction that has a P1 operand that
is the same as the P2 operand to this opcode and that has P3 set to 1.
If the subroutine is entered in-line, then the Return will simply
fall through.  But if the subroutine is entered using Gosub, then
the Return will jump back to the first instruction after the Gosub.
This routine works by loading a NULL into the P2 register.  When the
return address register contains a NULL, the Return instruction is
a no-op that simply falls through to the next instruction (assuming that
the Return opcode has a P3 value of 1).  Thus if the subroutine is
entered in-line, then the Return will cause in-line execution to
continue.  But if the subroutine is entered via Gosub, then the
Return will cause a return to the address following the Gosub.
This opcode is identical to Null.  It has a different name
only to make the byte code easier to read and verify.
BitAnd
Take the bit-wise AND of the values in register P1 and P2 and
store the result in register P3.
If either input is NULL, the result is NULL.
BitNot
Interpret the content of register P1 as an integer.  Store the
ones-complement of the P1 value into register P2.  If P1 holds
a NULL then store a NULL in P2.
BitOr
Take the bit-wise OR of the values in register P1 and P2 and
store the result in register P3.
If either input is NULL, the result is NULL.
Blob
P4 points to a blob of data P1 bytes long.  Store this
blob in register P2.  If P4 is a NULL pointer, then construct
a zero-filled blob that is P1 bytes long in P2.
Cast
Force the value in register P1 to be the type defined by P2.
 P2=='A' &rarr; BLOB
 P2=='B' &rarr; TEXT
 P2=='C' &rarr; NUMERIC
 P2=='D' &rarr; INTEGER
 P2=='E' &rarr; REAL
A NULL value is not changed by this routine.  It remains NULL.
Checkpoint
Checkpoint database P1. This is a no-op if P1 is not currently in
WAL mode. Parameter P2 is one of SQLITE_CHECKPOINT_PASSIVE, FULL,
RESTART, or TRUNCATE.  Write 1 or 0 into mem&#91;P3&#93; if the checkpoint returns
SQLITE_BUSY or not, respectively.  Write the number of pages in the
WAL after the checkpoint into mem&#91;P3+1&#93; and the number of pages
in the WAL that have been checkpointed after the checkpoint
completes into mem&#91;P3+2&#93;.  However on an error, mem&#91;P3+1&#93; and
mem&#91;P3+2&#93; are initialized to -1.
Clear
Delete all contents of the database table or index whose root page
in the database file is given by P1.  But, unlike Destroy, do not
remove the table or index from the database file.
The table being cleared is in the main database file if P2==0.  If
P2==1 then the table to be cleared is in the auxiliary database file
that is used to store tables create using CREATE TEMPORARY TABLE.
If the P3 value is non-zero, then the row change count is incremented
by the number of rows in the table being cleared. If P3 is greater
than zero, then the value stored in register P3 is also incremented
by the number of rows in the table being cleared.
See also: Destroy
Close
Close a cursor previously opened as P1.  If P1 is not
currently open, this instruction is a no-op.
ClrSubtype
Clear the subtype from register P1.
CollSeq
P4 is a pointer to a CollSeq object. If the next call to a user function
or aggregate calls sqlite3GetFuncCollSeq(), this collation sequence will
be returned. This is used by the built-in min(), max() and nullif()
functions.
If P1 is not zero, then it is a register that a subsequent min() or
max() aggregate will set to 1 if the current row is not the minimum or
maximum.  The P1 register is initialized to 0 by this instruction.
The interface used by the implementation of the aforementioned functions
to retrieve the collation sequence set by this opcode is not available
publicly.  Only built-in functions have access to this feature.
Column
Interpret the data that cursor P1 points to as a structure built using
the MakeRecord instruction.  (See the MakeRecord opcode for additional
information about the format of the data.)  Extract the P2-th column
from this record.  If there are less than (P2+1)
values in the record, extract a NULL.
The value extracted is stored in register P3.
If the record contains fewer than P2 fields, then extract a NULL.  Or,
if the P4 argument is a P4_MEM use the value of the P4 argument as
the result.
If the OPFLAG_LENGTHARG bit is set in P5 then the result is guaranteed
to only be used by the length() function or the equivalent.  The content
of large blobs is not loaded, thus saving CPU cycles.  If the
OPFLAG_TYPEOFARG bit is set then the result will only be used by the
typeof() function or the IS NULL or IS NOT NULL operators or the
equivalent.  In this case, all content loading can be omitted.
ColumnsUsed
This opcode (which only exists if SQLite was compiled with
SQLITE_ENABLE_COLUMN_USED_MASK) identifies which columns of the
table or index for cursor P1 are used.  P4 is a 64-bit integer
(P4_INT64) in which the first 63 bits are one for each of the
first 63 columns of the table or index that are actually used
by the cursor.  The high-order bit is set if any column after
the 64th is used.
