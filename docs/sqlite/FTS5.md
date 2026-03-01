SQLite FTS5 Extension
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
SQLite FTS5 Extension
Table Of Contents
1. Overview of FTS5
2. Compiling and Using FTS5
2.1. Building FTS5 as part of SQLite
2.2. Building a Loadable Extension
3. Full-text Query Syntax
3.1. FTS5 Strings
3.2. FTS5 Phrases
3.3. FTS5 Prefix Queries
3.4. FTS5 Initial Token Queries
3.5. FTS5 NEAR Queries
3.6. FTS5 Column Filters
3.7. FTS5 Boolean Operators
4. FTS5 Table Creation and Initialization
4.1. The UNINDEXED column option
4.2. Prefix Indexes
4.3. Tokenizers
4.3.1. Unicode61 Tokenizer
4.3.2. Ascii Tokenizer
4.3.3. Porter Tokenizer
4.3.4. The Trigram Tokenizer
4.4. External Content and Contentless Tables
4.4.1. Contentless Tables
4.4.2. Contentless-Delete Tables
4.4.3. External Content Tables
4.4.4. External Content Table Pitfalls
4.4.4.1. Updates and Deletes on Contentless Tables
4.5. The Columnsize Option
4.6. The Detail Option
4.7. The Tokendata Option
4.8. The Locale Option
4.9. The Contentless-Unindexed Option
5.  Auxiliary Functions 
5.1. Built-in Auxiliary Functions
5.1.1. The bm25() function
5.1.2. The highlight() function
5.1.3. The snippet() function
5.1.4. The fts5_get_locale() function
5.1.5. The fts5_insttoken() function
5.2. Sorting by Auxiliary Function Results
6. Special INSERT Commands
6.1. The 'automerge' Configuration Option
6.2. The 'crisismerge' Configuration Option
6.3. The 'delete' Command
6.4. The 'delete-all' Command
6.5. The 'deletemerge' Configuration Option
6.6. The 'insttoken' Configuration Option
6.7. The 'integrity-check' Command
6.8. The 'merge' Command
6.9. The 'optimize' Command
6.10. The 'pgsz' Configuration Option
6.11. The 'rank' Configuration Option
6.12. The 'rebuild' Command
6.13. The 'secure-delete' Configuration Option
6.14. The 'usermerge' Configuration Option
7. Extending FTS5
7.1. Custom Tokenizers
7.1.1. Synonym Support
7.2. Custom Auxiliary Functions
7.2.1. Custom Auxiliary Functions API Overview
7.2.2. Custom Auxiliary Functions API Reference
8. The fts5vocab Virtual Table Module
9. FTS5 Data Structures
9.1. Varint Format
9.2. The FTS Index (%_idx and %_data tables)
9.2.1. The %_data Table Rowid Space
9.2.2. Structure Record Format
9.2.3. Averages Record Format
9.2.4. Segment B-Tree Format
9.2.4.1. The Key/Doclist Format
9.2.4.2. Pagination
9.2.4.3. Segment Index Format
9.2.4.4. Doclist Index Format
9.3. Document Sizes Table (%_docsize table)
9.4. The Table Contents (%_content table)
9.5. Configuration Options (%_config table)
Appendix A: Comparison with FTS3/4
 Application Porting Guide 
 Changes to CREATE VIRTUAL TABLE statements 
 Changes to SELECT statements 
 Auxiliary Function Changes 
 Other Issues
Summary of Technical Differences
1. Overview of FTS5
FTS5 is an SQLite virtual table module that provides
full-text search
functionality to database applications. In their most elementary form,
full-text search engines allow the user to efficiently search a large
collection of documents for the subset that contain one or more instances of a
search term. The search functionality provided to world wide web users by
Google is, among other things, a full-text search
engine, as it allows users to search for all documents on the web that contain,
for example, the term "fts5".
To use FTS5, the user creates an FTS5 virtual table with one or more
columns. For example:
CREATE VIRTUAL TABLE email USING fts5(sender, title, body);
It is an error to add types, constraints or PRIMARY KEY declarations to
a CREATE VIRTUAL TABLE statement used to create an FTS5 table. Once created,
an FTS5 table may be populated using INSERT, UPDATE or DELETE statements
like any other table. Like any other table with no PRIMARY KEY declaration, an
FTS5 table has an implicit INTEGER PRIMARY KEY field named rowid.
Not shown in the example above is that there are also
various options that may be provided to FTS5 as
part of the CREATE VIRTUAL TABLE statement to configure various aspects of the
new table. These may be used to modify the way in which the FTS5 table extracts
terms from documents and queries, to create extra indexes on disk to speed up
prefix queries, or to create an FTS5 table that acts as an index on content
stored elsewhere.
Once populated, there are three ways to execute a full-text query against
the contents of an FTS5 table:
 Using a MATCH operator in the WHERE clause of a SELECT statement, or
     Using an equals ("=") operator in the WHERE clause of a SELECT statement, or
     using the table-valued function syntax.
If using the MATCH or = operators, the expression to the left of the MATCH
   operator is usually the name of the FTS5 table (the exception is when
   specifying a column-filter). The expression on the right
   must be a text value specifying the term to search for. For the table-valued
   function syntax, the term to search for is specified as the first table argument.
   For example:
-- Query for all rows that contain at least once instance of the term
-- "fts5" (in any column). The following three queries are equivalent.
SELECT * FROM email WHERE email MATCH 'fts5';
SELECT * FROM email WHERE email = 'fts5';
SELECT * FROM email('fts5');
 By default, FTS5 full-text searches are case-independent. Like any other
SQL query that does not contain an ORDER BY clause, the example above returns
results in an arbitrary order. To sort results by relevance (most to least
relevant), an ORDER BY may be added to a full-text query as follows:
-- Query for all rows that contain at least once instance of the term
-- "fts5" (in any column). Return results in order from best to worst
-- match.  
SELECT * FROM email WHERE email MATCH 'fts5' ORDER BY rank;
 As well as the column values and rowid of a matching row, an application
may use FTS5 auxiliary functions to retrieve extra information regarding
the matched row. For example, an auxiliary function may be used to retrieve
a copy of a column value for a matched row with all instances of the matched
term surrounded by html <b></b> tags. Auxiliary functions are
invoked in the same way as SQLite scalar functions, except that the name
of the FTS5 table is specified as the first argument. For example:
-- Query for rows that match "fts5". Return a copy of the "body" column
-- of each row with the matches surrounded by <b></b> tags.
SELECT highlight(email, 2, '<b>', '</b>') FROM email('fts5');
A description of the available auxiliary functions, and more details
regarding configuration of the special "rank" column, are
available below. Custom auxiliary functions may also be implemented in C and registered with
FTS5, just as custom SQL functions may be registered with the SQLite core.
 As well as searching for all rows that contain a term, FTS5 allows
the user to search for rows that contain:
   any terms that begin with a specified prefix,
   "phrases" - sequences of terms or prefix terms that must feature in a
       document for it to match the query,
   sets of terms, prefix terms or phrases that appear within a specified
       proximity of each other (these are called "NEAR queries"), or
   boolean combinations of any of the above.
 Such advanced searches are requested by providing a more complicated
FTS5 query string as the text to the right of the MATCH operator (or =
operator, or as the first argument to a table-valued function syntax). The
full query syntax is described here.
2. Compiling and Using FTS5
2.1. Building FTS5 as part of SQLite
As of version 3.9.0 (2015-10-14),
FTS5 is included as part of the SQLite amalgamation.
If using the canonical source tree, FTS5 is
enabled by specifying the "--enable-fts5" option when running the configure
script.  (FTS5 is currently disabled by default for the
source-tree configure script and enabled by default for
the amalgamation configure script, but these defaults might
change in the future.)
Or, if sqlite3.c is compiled using some other build system, by arranging for
the SQLITE_ENABLE_FTS5 pre-processor symbol to be defined.
2.2. Building a Loadable Extension
Alternatively, FTS5 may be built as a loadable extension.
The canonical FTS5 source code consists of a series of *.c and other files
in the "ext/fts5" directory of the SQLite source tree. A build process reduces
this to just two files - "fts5.c" and "fts5.h" - which may be used to build an
SQLite loadable extension.
   Obtain the latest SQLite code from fossil.
   Create a Makefile as described in How To Compile SQLite.
   Build the "fts5.c" target, which also creates fts5.h.
$ wget -c https://sqlite.org/src/tarball/SQLite-trunk.tgz?uuid=trunk -O SQLite-trunk.tgz
.... output ...
$ tar -xzf SQLite-trunk.tgz
$ cd SQLite-trunk
$ ./configure && make fts5.c
... lots of output ...
$ ls fts5.&#91;ch]
fts5.c        fts5.h
  The code in "fts5.c" may then be compiled into a loadable extension or
  statically linked into an application as described in
  Compiling Loadable Extensions. There are two entry points defined, both
  of which do the same thing:
   sqlite3_fts_init
   sqlite3_fts5_init
  The other file, "fts5.h", is not required to compile the FTS5 extension.
  It is used by applications that implement custom FTS5 tokenizers or auxiliary functions.
3. Full-text Query Syntax
The following block contains a summary of the FTS query syntax in BNF form.
A detailed explanation follows.
<phrase>    := string &#91;*]
<phrase>    := <phrase> + <phrase>
<neargroup> := NEAR ( <phrase> <phrase> ... &#91;, N] )
<query>     := &#91; &#91;-] <colspec> :] &#91;&#94;] <phrase>
<query>     := &#91; &#91;-] <colspec> :] <neargroup>
<query>     := &#91; &#91;-] <colspec> :] ( <query> )
<query>     := <query> AND <query>
<query>     := <query> OR <query>
<query>     := <query> NOT <query>
<colspec>   := colname
<colspec>   := { colname1 colname2 ... }
3.1. FTS5 Strings
Within an FTS expression a string may be specified in one of two ways:
   By enclosing it in double quotes ("). Within a string, any embedded
       double quote characters may be escaped SQL-style - by adding a second
       double-quote character.
   As an FTS5 bareword that is not "AND", "OR" or "NOT" (case sensitive).
       An FTS5 bareword is a string of one or more consecutive characters that
       are all either:
          Non-ASCII range characters (i.e. unicode codepoints greater
              than 127), or
          One of the 52 upper and lower case ASCII characters, or
          One of the 10 decimal digit ASCII characters, or
          The underscore character (unicode codepoint 95).
          The substitute character (unicode codepoint 26).
       Strings that include any other characters must be quoted. Characters
       that are not currently allowed in barewords, are not quote characters and
       do not currently serve any special purpose in FTS5 query expressions may
       at some point in the future be allowed in barewords or used to implement
       new query functionality. This means that queries that are currently
       syntax errors because they include such a character outside of a quoted
       string may be interpreted differently by some future version of FTS5.
3.2. FTS5 Phrases
Each string in an fts5 query is parsed ("tokenized") by the 
tokenizer and a list of zero or more tokens, or
terms, extracted. For example, the default tokenizer tokenizes the string "alpha
beta gamma" to three separate tokens - "alpha", "beta" and "gamma" - in that
order.
FTS queries are made up of phrases. A phrase is an ordered list of
one or more tokens. The tokens from each string in the query each make up a
single phrase. Two phrases can be concatenated into a single large phrase
using the "+" operator. For example, assuming the tokenizer module being used
tokenizes the input "one.two.three" to three separate tokens, the following
four queries all specify the same phrase:
... MATCH '"one two three"'
... MATCH 'one + two + three'
... MATCH '"one two" + three'
... MATCH 'one.two.three'
A phrase matches a document if the document contains at least one sub-sequence
of tokens that matches the sequence of tokens that make up the phrase.
3.3. FTS5 Prefix Queries
If a "*" character follows a string within an FTS expression, then the final
token extracted from the string is marked as a prefix token. As you
might expect, a prefix token matches any document token of which it is a
prefix. For example, the first two queries in the following block will match
any document that contains the token "one" immediately followed by the token
"two" and then any token that begins with "thr".
... MATCH '"one two thr" * '
... MATCH 'one + two + thr*'
... MATCH '"one two thr*"'      -- May not work as expected!
The final query in the block above may not work as expected. Because the
"*" character is inside the double-quotes, it will be passed to the tokenizer,
which will likely discard it (or perhaps, depending on the specific tokenizer
in use, include it as part of the final token) instead of recognizing it as
a special FTS character.
3.4. FTS5 Initial Token Queries
If a "&#94;" character appears immediately before a phrase that is not part of a
NEAR query, then that phrase only matches a document only if it starts at the
first token in a column. The "&#94;" syntax may be combined with a
column filter, but may not be inserted into the middle of
a phrase.
... MATCH '&#94;one'              -- first token in any column must be "one"
... MATCH '&#94; one + two'       -- phrase "one two" must appear at start of a column
... MATCH '&#94; "one two"'       -- same as previous 
... MATCH 'a : &#94;two'          -- first token of column "a" must be "two"
... MATCH 'NEAR(&#94;one, two)'   -- syntax error! 
... MATCH 'one + &#94;two'        -- syntax error! 
... MATCH '"&#94;one two"'        -- May not work as expected!
3.5. FTS5 NEAR Queries
Two or more phrases may be grouped into a NEAR group. A NEAR group
is specified by the token "NEAR" (case sensitive) followed by an open
parenthesis character, followed by two or more whitespace separated phrases, optionally followed by a comma and the numeric parameter N, followed by
a close parenthesis. For example:
... MATCH 'NEAR("one two" "three four", 10)'
... MATCH 'NEAR("one two" thr* + four)'
If no N parameter is supplied, it defaults to 10. A NEAR group
matches a document if the document contains at least one clump of tokens that:
   contains at least one instance of each phrase, and
   for which the number of tokens between the end of the first phrase
       and the beginning of the last phrase in the clump is less than or equal to N.
For example:
CREATE VIRTUAL TABLE ft USING fts5(x);
INSERT INTO ft(rowid, x) VALUES(1, 'A B C D x x x E F x');
... MATCH 'NEAR(e d, 4)';                      -- Matches!
... MATCH 'NEAR(e d, 3)';                      -- Matches!
... MATCH 'NEAR(e d, 2)';                      -- Does not match!
... MATCH 'NEAR("c d" "e f", 3)';              -- Matches!
... MATCH 'NEAR("c"   "e f", 3)';              -- Does not match!
... MATCH 'NEAR(a d e, 6)';                    -- Matches!
... MATCH 'NEAR(a d e, 5)';                    -- Does not match!
... MATCH 'NEAR("a b c d" "b c" "e f", 4)';    -- Matches!
... MATCH 'NEAR("a b c d" "b c" "e f", 3)';    -- Does not match!
3.6. FTS5 Column Filters
A single phrase or NEAR group may be restricted to matching text within a
specified column of the FTS table by prefixing it with the column name
followed by a colon character. Or to a set of columns by prefixing it
with a whitespace separated list of column names enclosed in braces
("curly brackets") followed by a colon character. Column names may be specified
using either of the two forms described for strings above. Unlike strings that
are part of phrases, column names are not passed to the tokenizer module.
Column names are case-insensitive in the usual way for SQLite column names -
upper/lower case equivalence is understood for ASCII-range characters only.
... MATCH 'colname : NEAR("one two" "three four", 10)'
... MATCH '"colname" : one + two + three'
... MATCH '{col1 col2} : NEAR("one two" "three four", 10)'
... MATCH '{col2 col1 col3} : one + two + three'
If a column filter specification is preceded by a "-" character, then
it is interpreted as a list of column not to match against. For example:
-- Search for matches in all columns except "colname"
... MATCH '- colname : NEAR("one two" "three four", 10)'
-- Search for matches in all columns except "col1", "col2" and "col3"
... MATCH '- {col2 col1 col3} : one + two + three'
Column filter specifications may also be applied to arbitrary expressions
enclosed in parentheses. In this case the column filter applies to all
phrases within the expression. Nested column filter operations may only
further restrict the subset of columns matched, they can not be used to
re-enable filtered columns. For example:
-- The following are equivalent:
... MATCH '{a b} : ( {b c} : "hello" AND "world" )'
... MATCH '(b : "hello") AND ({a b} : "world")'
Finally, a column filter for a single column may be specified by using
the column name as the LHS of a MATCH operator (instead of the usual
table name). For example:
-- Given the following table
CREATE VIRTUAL TABLE ft USING fts5(a, b, c);
-- The following are equivalent
SELECT * FROM ft WHERE b MATCH 'uvw AND xyz';
SELECT * FROM ft WHERE ft MATCH 'b : (uvw AND xyz)';
-- This query cannot match any rows (since all columns are filtered out): 
SELECT * FROM ft WHERE b MATCH 'a : xyz';
3.7. FTS5 Boolean Operators
Phrases and NEAR groups may be arranged into expressions using boolean
operators. In order of precedence, from highest (tightest grouping) to
lowest (loosest grouping), the operators are:
  Operator Function
  <query1> NOT <query2>
      Matches if query1 matches and query2 does not match.
  <query1> AND <query2>
      Matches if both query1 and query2 match.
  <query1> OR <query2>
      Matches if either query1 or query2 match.
Parentheses may be used to group expressions in order to modify operator
precedence in the usual ways. For example:
-- Because NOT groups more tightly than OR, either of the following may
-- be used to match all documents that contain the token "two" but not
-- "three", or contain the token "one".  
... MATCH 'one OR two NOT three'
... MATCH 'one OR (two NOT three)'
-- Matches documents that contain at least one instance of either "one"
-- or "two", but do not contain any instances of token "three".
... MATCH '(one OR two) NOT three'
Phrases and NEAR groups may also be connected by implicit AND operators.
For simplicity, these are not shown in the BNF grammar above. Essentially, any
sequence of phrases or NEAR groups (including those restricted to matching
specified columns) separated only by whitespace are handled as if there were an
implicit AND operator between each pair of phrases or NEAR groups. Implicit
AND operators are never inserted after or before an expression enclosed in
parentheses. Implicit AND operators group more tightly than all other
operators, including NOT. For example:
... MATCH 'one two three'         -- 'one AND two AND three'
... MATCH 'three "one two"'       -- 'three AND "one two"'
... MATCH 'NEAR(one two) three'   -- 'NEAR(one two) AND three'
... MATCH 'one OR two three'      -- 'one OR two AND three'
... MATCH 'one NOT two three'     -- 'one NOT (two AND three)'
... MATCH '(one OR two) three'    -- Syntax error!
... MATCH 'func(one two)'         -- Syntax error!
4. FTS5 Table Creation and Initialization
Each argument specified as part of a "CREATE VIRTUAL TABLE ... USING fts5
..." statement is either a column declaration or a configuration option. A
column declaration consists of one or more whitespace separated FTS5
barewords or string literals quoted in any manner acceptable to SQLite.
The first string or bareword in a column declaration is the column name. It
is an error to attempt to name an fts5 table column "rowid" or "rank", or to
assign the same name to a column as is used by the table itself. This is not
supported.
Each subsequent string or bareword in a column declaration is a column
option that modifies the behavior of that column. Column options are
case-independent. Unlike the SQLite core, FTS5 considers unrecognized column
options to be errors. Currently, the only option recognized is
"UNINDEXED" (see below).
A configuration option consists of an FTS5 bareword - the option name -
followed by an "=" character, followed by the option value. The option value is
specified using either a single FTS5 bareword or a string literal, again quoted
in any manner acceptable to the SQLite core. For example:
CREATE VIRTUAL TABLE mail USING fts5(sender, title, body, tokenize = 'porter ascii');
 There are currently the following configuration options:
   The "tokenize" option, used to configure a custom tokenizer.
   The "prefix" option, used to add prefix indexes
       to an FTS5 table.
   The "content" option, used to make the FTS5 table an
       external content or contentless table.
   The "content_rowid" option, used to set the rowid field of an
       external content table.
   The "columnsize" option, used to configure
       whether or not the size in tokens of each value in the FTS5 table is
       stored separately within the database.
   The "detail" option. This option may be used
       to reduce the size of the FTS index on disk by omitting some information
       from it.
4.1. The UNINDEXED column option
The contents of columns qualified with the UNINDEXED column option are not
added to the FTS index. This means that for the purposes of MATCH queries and
FTS5 auxiliary functions, the column contains no matchable tokens.
For example, to avoid adding the contents of the "uuid" field to the FTS
index:
CREATE VIRTUAL TABLE customers USING fts5(name, addr, uuid UNINDEXED);
4.2. Prefix Indexes
 By default, FTS5 maintains a single index recording the location of each
token instance within the document set. This means that querying for complete
