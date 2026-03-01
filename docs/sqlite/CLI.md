Command Line Shell For SQLite
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
Command Line Shell For SQLite
Table Of Contents
1. Getting Started
1.1. SQLite command-line program versus the SQLite library
1.2. GUI Alternatives To The CLI
1.3. Starting the CLI
1.4. Double-click Startup On Windows
2. Special commands to sqlite3 (dot-commands)
3. Rules for "dot-commands", SQL and More
3.1. Line Structure
3.2. Dot-command arguments
3.3. Dot-command execution
4. Changing Output Formats
4.1. Control of line endings
4.2. Control characters in output
5. Querying the database schema
6. Opening Database Files
7. Redirecting I/O
7.1. Writing results to a file
7.2. Reading SQL from a file
7.3. File I/O Functions
7.4. The edit() SQL function
7.5. Importing files as CSV or other formats
7.6. Export to CSV
7.6.1.  Export to Excel 
7.6.2.  Export to TSV (tab separated values)
8. Accessing ZIP Archives As Database Files
8.1. How ZIP archive access is implemented
9. Converting An Entire Database To A Text File
10. Recover Data From a Corrupted Database
11. Loading Extensions
12. Cryptographic Hashes Of Database Content
13. Database Content Self-Tests
14. SQLite Archive Support
14.1.  SQLite Archive Create Command 
14.2.  SQLite Archive Extract Command 
14.3.  SQLite Archive List Command 
14.4.  SQLite Archive Insert And Update Commands 
14.5.  SQLite Archive Remove Command 
14.6.  Operations On ZIP Archives 
14.7.  SQL Used To Implement SQLite Archive Operations 
15. SQL Parameters
16. Index Recommendations (SQLite Expert)
17. Working With Multiple Database Connections
18. Miscellaneous Extension Features
19. Other Dot Commands
20. Using sqlite3 in a shell script
21. Marking The End Of An SQL Statement
22. More Details On How To Start The CLI
22.1. Extra command-line arguments
22.2. Command-line Options
22.3. The --safe command-line option
22.3.1. Bypassing --safe restrictions for specific commands
22.4. The --unsafe-testing command-line option
22.5. The --no-utf8 and --utf8 command-line options
23. Compiling the sqlite3 program from sources
23.1.  Do-It-Yourself Builds 
1. Getting Started
The SQLite project provides a simple command-line program named
sqlite3 (or sqlite3.exe on Windows)
that allows the user to manually enter and execute SQL
statements against an SQLite database or against a
ZIP archive.  This document provides a brief
introduction on how to use the sqlite3 program.
1.1. SQLite command-line program versus the SQLite library
The SQLite library is code that implements an SQL database engine.
The "sqlite3" command-line program or "CLI" is an application that
accepts user input and passes it down into the SQLite library for
evaluation.  Understand that these are two different things.  When
somebody says "SQLite" or "sqlite3" they might be referring to either
the SQLite library itself, or the CLI that provides a human interface
to the library.  You will often need to use context to figure out exactly
which of these two things the speaker is referring to.
This document is about the CLI, not the underlying SQLite library.
1.2. GUI Alternatives To The CLI
The sqlite3 program is written by and for the core SQLite
developers and is the officially supported way to accessing 
SQLite database files interactively.
However, some users might prefer a Graphical User Interface (GUI).
Several such programs are available from third-parties.
One of those is
Visual DB,
a sponsor of the SQLite project:
Thanks to Visual DB for helping us make SQLite better for everyone!
1.3. Starting the CLI
Start the sqlite3 program by typing "sqlite3" at the
command prompt, optionally followed
by the name of the file that holds the SQLite database
(or ZIP archive).  If the named
file does not exist, a new database file with the given name will be
created automatically.  If no database file is specified on the
command-line, a transient in-memory database is used.  This in-memory
database is deleted when the program exits.
On startup, the sqlite3 program will show a brief banner
message then prompt you to enter SQL.  Type in SQL statements (terminated
by a semicolon), press "Enter" and the SQL will be executed.
For example, to create a new SQLite database named "ex1"
with a single table named "tbl1", you might do this:
$ sqlite3 ex1
SQLite version 3.36.0 2021-06-18 18:36:39
Enter ".help" for usage hints.
sqlite> create table tbl1(one text, two int);
sqlite> insert into tbl1 values('hello!',10);
sqlite> insert into tbl1 values('goodbye', 20);
sqlite> select * from tbl1;
hello!|10
goodbye|20
sqlite>
Terminate the sqlite3 program by typing your system
End-Of-File character (usually a Control-D).  Use the interrupt
character (usually a Control-C) to stop a long-running SQL statement.
Make sure you type a semicolon at the end of each SQL command!
The sqlite3 program looks for a semicolon to know when your SQL command is
complete.  If you omit the semicolon, sqlite3 will give you a
continuation prompt and wait for you to enter more text to
complete the SQL command.  This feature allows you to
enter SQL commands that span multiple lines.  For example:
sqlite> CREATE TABLE tbl2 (
   ...>   f1 varchar(30) primary key,
   ...>   f2 text,
   ...>   f3 real
   ...> );
sqlite>
1.4. Double-click Startup On Windows
Windows users can double-click on the sqlite3.exe icon to cause
the command-line shell to pop-up a terminal window running SQLite.  However,
because double-clicking starts the sqlite3.exe without command-line arguments,
no database file will have been specified, so SQLite will use a transient
in-memory database that is deleted when the session exits.
To use a persistent disk file as the database, enter the ".open" command
immediately after the terminal window starts up:
SQLite version 3.36.0 2021-06-18 18:36:39
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .open ex1.db
sqlite>
The example above causes the database file named "ex1.db" to be opened
and used.  The "ex1.db" file is created if it does not previously exist.
You might want to
use a full pathname to ensure that the file is in the directory that you
think it is in.  Use forward-slashes as the directory separator character.
In other words use "c:/work/ex1.db", not "c:\work\ex1.db".
Alternatively, you can create a new database using the default temporary
storage, then save that database into a disk file using the ".save" command:
SQLite version 3.36.0 2021-06-18 18:36:39
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> ... many SQL commands omitted ...
sqlite> .save ex1.db
sqlite>
Be careful when using the ".save" command as it will overwrite any
preexisting database files having the same name without prompting for
confirmation.  As with the ".open" command, you might want to use a
full pathname with forward-slash directory separators to avoid ambiguity.
2. Special commands to sqlite3 (dot-commands)
Most of the time, sqlite3 just reads lines of input and passes them
on to the SQLite library for execution.
But input lines that begin with a dot (".")
are intercepted and interpreted by the sqlite3 program itself.
These "dot commands" are typically used to change the output format
of queries, or to execute certain prepackaged query statements.
There were originally just a few dot commands, but over the years
many new features have accumulated so that today there are over 60.
For a listing of the available dot commands, you can enter ".help" with
no arguments.  Or enter ".help TOPIC" for detailed information about TOPIC.
The list of available dot-commands follows:
sqlite> .help
.archive ...             Manage SQL archives
.auth ON|OFF             Show authorizer callbacks
.backup ?DB? FILE        Backup DB (default "main") to FILE
.bail on|off             Stop after hitting an error.  Default OFF
.cd DIRECTORY            Change the working directory to DIRECTORY
.changes on|off          Show number of rows changed by SQL
.check GLOB              Fail if output since .testcase does not match
.clone NEWDB             Clone data into NEWDB from the existing database
.connection &#91;close&#93; &#91;#&#93;  Open or close an auxiliary database connection
.crlf ?on|off?           Whether or not to use \r\n line endings
.databases               List names and files of attached databases
.dbconfig ?op? ?val?     List or change sqlite3_db_config() options
.dbinfo ?DB?             Show status information about the database
.dbtotxt                 Hex dump of the database file
.dump ?OBJECTS?          Render database content as SQL
.echo on|off             Turn command echo on or off
.eqp on|off|full|...     Enable or disable automatic EXPLAIN QUERY PLAN
.excel                   Display the output of next command in spreadsheet
.exit ?CODE?             Exit this program with return-code CODE
.expert                  EXPERIMENTAL. Suggest indexes for queries
.explain ?on|off|auto?   Change the EXPLAIN formatting mode.  Default: auto
.filectrl CMD ...        Run various sqlite3_file_control() operations
.fullschema ?--indent?   Show schema and the content of sqlite_stat tables
.headers on|off          Turn display of headers on or off
.help ?-all? ?PATTERN?   Show help text for PATTERN
.import FILE TABLE       Import data from FILE into TABLE
.imposter INDEX TABLE    Create imposter table TABLE on index INDEX
.indexes ?TABLE?         Show names of indexes
.intck ?STEPS_PER_UNLOCK?  Run an incremental integrity check on the db
.limit ?LIMIT? ?VAL?     Display or change the value of an SQLITE_LIMIT
.lint OPTIONS            Report potential schema issues.
.load FILE ?ENTRY?       Load an extension library
.log FILE|on|off         Turn logging on or off.  FILE can be stderr/stdout
.mode ?MODE? ?OPTIONS?   Set output mode
.nonce STRING            Suspend safe mode for one command if nonce matches
.nullvalue STRING        Use STRING in place of NULL values
.once ?OPTIONS? ?FILE?   Output for the next SQL command only to FILE
.open ?OPTIONS? ?FILE?   Close existing database and reopen FILE
.output ?FILE?           Send output to FILE or stdout if FILE is omitted
.parameter CMD ...       Manage SQL parameter bindings
.print STRING...         Print literal STRING
.progress N              Invoke progress handler after every N opcodes
.prompt MAIN CONTINUE    Replace the standard prompts
.quit                    Stop interpreting input stream, exit if primary.
.read FILE               Read input from FILE or command output
.recover                 Recover as much data as possible from corrupt db.
.restore ?DB? FILE       Restore content of DB (default "main") from FILE
.save ?OPTIONS? FILE     Write database to FILE (an alias for .backup ...)
.scanstats on|off|est    Turn sqlite3_stmt_scanstatus() metrics on or off
.schema ?PATTERN?        Show the CREATE statements matching PATTERN
.separator COL ?ROW?     Change the column and row separators
.session ?NAME? CMD ...  Create or control sessions
.sha3sum ...             Compute a SHA3 hash of database content
.shell CMD ARGS...       Run CMD ARGS... in a system shell
.show                    Show the current values for various settings
.stats ?ARG?             Show stats or turn stats on or off
.system CMD ARGS...      Run CMD ARGS... in a system shell
.tables ?TABLE?          List names of tables matching LIKE pattern TABLE
.timeout MS              Try opening locked tables for MS milliseconds
.timer on|off            Turn SQL timer on or off
.trace ?OPTIONS?         Output each SQL statement as it is run
.unmodule NAME ...       Unregister virtual table modules
.version                 Show source, library and compiler versions
.vfsinfo ?AUX?           Information about the top-level VFS
.vfslist                 List all available VFSes
.vfsname ?AUX?           Print the name of the VFS stack
.width NUM1 NUM2 ...     Set minimum column widths for columnar output
.www                     Display output of the next command in web browser
sqlite>
3. Rules for "dot-commands", SQL and More
3.1. Line Structure
The CLI's input is parsed into a sequence consisting of:
    SQL statements;
    dot-commands; or
    CLI comments
SQL statements are free-form, and can be spread across multiple lines,
  with whitespace or SQL comments embedded anywhere.
  They are terminated by either a ';' character at the end of an input line,
  or a '/' character or the word "go" on a line by itself.
  When not at the end of an input line, the ';' character
  acts to separate SQL statements.
  Trailing whitespace is ignored for purposes of termination.
A dot-command has a more restrictive structure:
It must begin with its "." at the left margin
    with no preceding whitespace.
It must be entirely contained on a single input line.
It cannot occur in the middle of an ordinary SQL
    statement.  In other words, it cannot occur at a
    continuation prompt.
There is no comment syntax for dot-commands.
The CLI also accepts whole-line comments that
begin with a '#' character and extend to the end of the line.
There can be no whitespace prior to the '#'.
3.2. Dot-command arguments
The arguments passed to dot-commands are parsed from the command tail,
  per these rules:
  The trailing newline and any other trailing whitespace is discarded;
  Whitespace immediately following the dot-command name, or any argument
    input end bound is discarded;
  An argument input begins with any non-whitespace character;
  An argument input ends with a character which
    depends upon its leading character thusly:
    for a leading single-quote ('), a single-quote acts
      as the end delimiter;
    for a leading double-quote ("), an unescaped double-quote
      acts as the end delimiter;
    for any other leading character, the end delimiter is
      any whitespace; and
    the command tail end acts as the end delimiter for any argument;
  Within a double-quoted argument input, a backslash-escaped double-quote
    is part of the argument rather than its terminating quote;
  Within a double-quoted argument, traditional C-string literal, backslash
    escape sequence translation is done; and
  Argument input delimiters (the bounding quotes or whitespace)
    are discarded to yield the passed argument.
3.3. Dot-command execution
The dot-commands
are interpreted by the sqlite3.exe command-line program, not by
SQLite itself.  So none of the dot-commands will work as an argument
to SQLite interfaces such as sqlite3_prepare() or sqlite3_exec().
4. Changing Output Formats
The sqlite3 program is able to show the results of a query
in 14 different output formats:
 ascii
 box
 csv
 column
 html
 insert
 json
 line
 list
 markdown
 quote
 table
 tabs
 tcl
You can use the ".mode" dot command to switch between these output
formats.
The default output mode is "list".  In
list mode, each row of a query result is written on one line of
output and each column within that row is separated by a specific
separator string.  The default separator is a pipe symbol ("|").
List mode is especially useful when you are going to send the output
of a query to another program (such as AWK) for additional processing.
sqlite> .mode list
sqlite> select * from tbl1;
hello!|10
goodbye|20
sqlite>
Type ".mode" with no arguments to show the current mode:
sqlite> .mode
current output mode: list
sqlite>
Use the ".separator" dot command to change the separator.
For example, to change the separator to a comma and
a space, you could do this:
sqlite> .separator ", "
sqlite> select * from tbl1;
hello!, 10
goodbye, 20
sqlite>
The next ".mode" command might reset the ".separator" back to some
default value (depending on its arguments).
So you will likely need to repeat the ".separator" command whenever you
change modes if you want to continue using a non-standard separator.
In "quote" mode, the output is formatted as SQL literals.  Strings are
enclosed in single-quotes and internal single-quotes are escaped by doubling.
Blobs are displayed in hexadecimal blob literal notation (Ex: x'abcd').
Numbers are displayed as ASCII text and NULL values are shown as "NULL".
All columns are separated from each other by a comma (or whatever alternative
character is selected using ".separator").
sqlite> .mode quote
sqlite> select * from tbl1;
'hello!',10
'goodbye',20
sqlite>
In "line" mode, each column in a row of the database
is shown on a line by itself.  Each line consists of the column
name, an equal sign and the column data.  Successive records are
separated by a blank line.  Here is an example of line mode
output:
sqlite> .mode line
sqlite> select * from tbl1;
one = hello!
two = 10
one = goodbye
two = 20
sqlite>
In column mode, each record is shown on a separate line with the
data aligned in columns.  For example:
sqlite> .mode column
sqlite> select * from tbl1;
one       two
--------  ---
hello!    10
goodbye   20
sqlite>
In "column" mode (and also in "box", "table", and "markdown" modes)
the width of columns adjusts automatically.  But you can override this,
providing a specified width for each column using the ".width" command.
The arguments to ".width" are integers which are the number of
characters to devote to each column.  Negative numbers mean right-justify.
Thus:
sqlite> .width 12 -6
sqlite> select * from tbl1;
one              two
------------  ------
hello!            10
goodbye           20
sqlite>
A width of 0 means the column width is chosen automatically.
Unspecified column widths become zero.  Hence, the command
".width" with no arguments resets all column widths to zero and
hence causes all column widths to be determined automatically.
The "column" mode is a tabular output format.  Other
tabular output formats are "box", "markdown", and "table":
sqlite> .width
sqlite> .mode markdown
sqlite> select * from tbl1;
|   one   | two |
|---------|-----|
| hello!  | 10  |
| goodbye | 20  |
sqlite> .mode table
sqlite> select * from tbl1;
+---------+-----+
|   one   | two |
+---------+-----+
| hello!  | 10  |
| goodbye | 20  |
+---------+-----+
sqlite> .mode box
sqlite> select * from tbl1;
┌─────────┬─────┐
│   one   │ two │
├─────────┼─────┤
│ hello!  │ 10  │
│ goodbye │ 20  │
└─────────┴─────┘
sqlite>
The columnar modes accept some additional options to control formatting.
The "--wrap N" option (where N is an integer) causes columns
to wrap text that is longer than N characters.  Wrapping is disabled if
N is zero.
sqlite> insert into tbl1 values('The quick fox jumps over a lazy brown dog.',90);
sqlite> .mode box --wrap 30
sqlite> select * from tbl1 where two>50;
┌────────────────────────────────┬─────┐
