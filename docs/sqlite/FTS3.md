SQLite FTS3 and FTS4 Extensions
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
SQLite FTS3 and FTS4 Extensions
Table Of Contents
1. Introduction to FTS3 and FTS4
1.1. Differences between FTS3 and FTS4
1.2. Creating and Destroying FTS Tables
1.3. Populating FTS Tables
1.4. Simple FTS Queries
1.5. Summary
2. Compiling and Enabling FTS3 and FTS4
3. Full-text Index Queries
3.1.
Set Operations Using The Enhanced Query Syntax
3.2. Set Operations Using The Standard Query Syntax
4. Auxiliary Functions - Snippet, Offsets and Matchinfo
4.1. The Offsets Function
4.2. The Snippet Function
4.3. The Matchinfo Function
5. Fts4aux - Direct Access to the Full-Text Index
6. FTS4 Options
6.1. The compress= and uncompress= options
6.2. The content= option 
6.2.1.  Contentless FTS4 Tables 
6.2.2.  External Content FTS4 Tables 
6.3. The languageid= option
6.4. The matchinfo= option
6.5. The notindexed= option
6.6. The prefix= option
7. Special Commands For FTS3 and FTS4
7.1. The "optimize" command
7.2. The "rebuild" command
7.3. The "integrity-check" command
7.4. The "merge=X,Y" command
7.5. The "automerge=N" command
8. Tokenizers
8.1. Custom (Application Defined) Tokenizers
8.2. Querying Tokenizers
9. Data Structures
9.1. Shadow Tables
9.2. Variable Length Integer (varint) Format
9.3. Segment B-Tree Format
9.3.1. Segment B-Tree Leaf Nodes
9.3.2. Segment B-Tree Interior Nodes
9.4. Doclist Format
10. Limitations
10.1.  UTF-16 byte-order-mark problem 
Appendix A: Search Application Tips
 Overview
  FTS3 and FTS4 are SQLite virtual table modules that allow users to perform
  full-text searches on a set of documents. The most common (and effective)
  way to describe full-text searches is "what Google, Yahoo, and Bing do
  with documents placed on the World Wide Web". Users input a term, or series
  of terms, perhaps connected by a binary operator or grouped together into a
  phrase, and the full-text query system finds the set of documents that best
  matches those terms considering the operators and groupings the user has
  specified. This article describes the deployment and usage of FTS3 and FTS4.
  FTS1 and FTS2 are obsolete full-text search modules for SQLite.  There are known
  issues with these older modules and their use should be avoided.
  Portions of the original FTS3 code were contributed to the SQLite project
  by Scott Hess of Google. It is now
  developed and maintained as part of SQLite.
1. Introduction to FTS3 and FTS4
  The FTS3 and FTS4 extension modules allow users to create special tables with a
  built-in full-text index (hereafter "FTS tables"). The full-text index
  allows the user to efficiently query the database for all rows that contain
  one or more words (hereafter "tokens"), even if the table
  contains many large documents.
  For example, if each of the 517430 documents in the
  "Enron E-Mail Dataset"
  is inserted into both an FTS table and an ordinary SQLite table
  created using the following SQL script:
CREATE VIRTUAL TABLE enrondata1 USING fts3(content TEXT);     /* FTS3 table */
CREATE TABLE enrondata2(content TEXT);                        /* Ordinary table */
  Then either of the two queries below may be executed to find the number of
  documents in the database that contain the word "linux" (351). Using one
  desktop PC hardware configuration, the query on the FTS3 table returns in
  approximately 0.03 seconds, versus 22.5 for querying the ordinary table.
SELECT count(*) FROM enrondata1 WHERE content MATCH 'linux';  /* 0.03 seconds */
SELECT count(*) FROM enrondata2 WHERE content LIKE '%linux%'; /* 22.5 seconds */
  Of course, the two queries above are not entirely equivalent. For example
  the LIKE query matches rows that contain terms such as "linuxophobe"
  or "EnterpriseLinux" (as it happens, the Enron E-Mail Dataset does not
  actually contain any such terms), whereas the MATCH query on the FTS3 table
  selects only those rows that contain "linux" as a discrete token. Both
  searches are case-insensitive. The FTS3 table consumes around 2006 MB on
  disk compared to just 1453 MB for the ordinary table. Using the same
  hardware configuration used to perform the SELECT queries above, the FTS3
  table took just under 31 minutes to populate, versus 25 for the ordinary
  table.
1.1. Differences between FTS3 and FTS4
  FTS3 and FTS4 are nearly identical. They share most of their code in common,
  and their interfaces are the same. The differences are:
   FTS4 contains query performance optimizations that may significantly
       improve the performance of full-text queries that contain terms that are
       very common (present in a large percentage of table rows).
   FTS4 supports some additional options that may be used with the 
       matchinfo() function.
   Because it stores extra information on disk in two new
       shadow tables in order to support the performance
       optimizations and extra matchinfo() options, FTS4 tables may consume more
       disk space than the equivalent table created using FTS3. Usually the overhead
       is 1-2% or less, but may be as high as 10% if the documents stored in the
       FTS table are very small. The overhead may be reduced by specifying the
       directive "matchinfo=fts3" as part of the FTS4 table
       declaration, but this comes at the expense of sacrificing some of the
       extra supported matchinfo() options.
   FTS4 provides hooks (the compress and uncompress
       options) allowing data to be stored in a compressed
       form, reducing disk usage and IO.
  FTS4 is an enhancement to FTS3.
  FTS3 has been available since SQLite version 3.5.0 (2007-09-04)
  The enhancements for FTS4 were added with SQLite version 3.7.4
  (2010-12-07).
  Which module, FTS3 or FTS4, should you use in your application?  FTS4 is
  sometimes significantly faster than FTS3, even orders of magnitude faster
  depending on the query, though in the common case the performance of the two
  modules is similar. FTS4 also offers the enhanced matchinfo() outputs which
  can be useful in ranking the results of a MATCH operation.  On the
  other hand, in the absence of a matchinfo=fts3 directive FTS4 requires a little
  more disk space than FTS3, though only a percent of two in most cases.
  For newer applications, FTS4 is recommended; though if compatibility with older
  versions of SQLite is important, then FTS3 will usually serve just as well.
1.2. Creating and Destroying FTS Tables
  Like other virtual table types, new FTS tables are created using a
  CREATE VIRTUAL TABLE statement. The module name, which follows
  the USING keyword, is either "fts3" or "fts4". The virtual table module arguments may
  be left empty, in which case an FTS table with a single user-defined
  column named "content" is created. Alternatively, the module arguments
  may be passed a list of comma separated column names.
  If column names are explicitly provided for the FTS table as part of
  the CREATE VIRTUAL TABLE statement, then a datatype name may be optionally
  specified for each column. This is pure syntactic sugar, the
  supplied typenames are not used by FTS or the SQLite core for any
  purpose. The same applies to any constraints specified along with an
  FTS column name - they are parsed but not used or recorded by the system
  in any way.
-- Create an FTS table named "data" with one column - "content":
CREATE VIRTUAL TABLE data USING fts3();
-- Create an FTS table named "pages" with three columns:
CREATE VIRTUAL TABLE pages USING fts4(title, keywords, body);
-- Create an FTS table named "mail" with two columns. Datatypes
-- and column constraints are specified along with each column. These
-- are completely ignored by FTS and SQLite. 
CREATE VIRTUAL TABLE mail USING fts3(
  subject VARCHAR(256) NOT NULL,
  body TEXT CHECK(length(body)<10240)
);
  As well as a list of columns, the module arguments passed to a CREATE
  VIRTUAL TABLE statement used to create an FTS table may be used to specify
  a tokenizer. This is done by specifying a string of the form
  "tokenize=<tokenizer name> <tokenizer args>" in place of a column
  name, where <tokenizer name> is the name of the tokenizer to use and
  <tokenizer args> is an optional list of whitespace separated qualifiers
  to pass to the tokenizer implementation. A tokenizer specification may be
  placed anywhere in the column list, but at most one tokenizer declaration is
  allowed for each CREATE VIRTUAL TABLE statement. See below for a
  detailed description of using (and, if necessary, implementing) a tokenizer.
-- Create an FTS table named "papers" with two columns that uses
-- the tokenizer "porter".
CREATE VIRTUAL TABLE papers USING fts3(author, document, tokenize=porter);
-- Create an FTS table with a single column - "content" - that uses
-- the "simple" tokenizer.
CREATE VIRTUAL TABLE data USING fts4(tokenize=simple);
-- Create an FTS table with two columns that uses the "icu" tokenizer.
-- The qualifier "en_AU" is passed to the tokenizer implementation
CREATE VIRTUAL TABLE names USING fts3(a, b, tokenize=icu en_AU);
  FTS tables may be dropped from the database using an ordinary DROP TABLE
  statement. For example:
-- Create, then immediately drop, an FTS4 table.
CREATE VIRTUAL TABLE data USING fts4();
DROP TABLE data;
1.3. Populating FTS Tables
    FTS tables are populated using INSERT, UPDATE and DELETE
    statements in the same way as ordinary SQLite tables are.
    As well as the columns named by the user (or the "content" column if no
    module arguments were specified as part of the CREATE VIRTUAL TABLE
    statement), each FTS table has a "rowid" column. The rowid of an FTS
    table behaves in the same way as the rowid column of an ordinary SQLite
    table, except that the values stored in the rowid column of an FTS table
    remain unchanged if the database is rebuilt using the VACUUM command.
    For FTS tables, "docid" is allowed as an alias along with the usual "rowid",
    "oid" and "_oid_" identifiers. Attempting to insert or update a row with a
    docid value that already exists in the table is an error, just as it would
    be with an ordinary SQLite table.
    There is one other subtle difference between "docid" and the normal SQLite
    aliases for the rowid column. Normally, if an INSERT or UPDATE statement
    assigns discrete values to two or more aliases of the rowid column, SQLite
    writes the rightmost of such values specified in the INSERT or UPDATE
    statement to the database. However, assigning a non-NULL value to both
    the "docid" and one or more of the SQLite rowid aliases when inserting or
    updating an FTS table is considered an error. See below for an example.
-- Create an FTS table
CREATE VIRTUAL TABLE pages USING fts4(title, body);
-- Insert a row with a specific docid value.
INSERT INTO pages(docid, title, body) VALUES(53, 'Home Page', 'SQLite is a software...');
-- Insert a row and allow FTS to assign a docid value using the same algorithm as
-- SQLite uses for ordinary tables. In this case the new docid will be 54,
-- one greater than the largest docid currently present in the table.
INSERT INTO pages(title, body) VALUES('Download', 'All SQLite source code...');
-- Change the title of the row just inserted.
UPDATE pages SET title = 'Download SQLite' WHERE rowid = 54;
-- Delete the entire table contents.
DELETE FROM pages;
-- The following is an error. It is not possible to assign non-NULL values to both
-- the rowid and docid columns of an FTS table.
INSERT INTO pages(rowid, docid, title, body) VALUES(1, 2, 'A title', 'A document body');
    To support full-text queries, FTS maintains an inverted index that maps
    from each unique term or word that appears in the dataset to the locations
    in which it appears within the table contents. For the curious, a
    complete description of the data structure used to store
    this index within the database file appears below. A feature of
    this data structure is that at any time the database may contain not
    one index b-tree, but several different b-trees that are incrementally
    merged as rows are inserted, updated and deleted. This technique improves
    performance when writing to an FTS table, but causes some overhead for
    full-text queries that use the index. Evaluating the special "optimize" command,
    an SQL statement of the
    form "INSERT INTO <fts-table>(<fts-table>) VALUES('optimize')",
    causes FTS to merge all existing index b-trees into a single large
    b-tree containing the entire index. This can be an expensive operation,
    but may speed up future queries.
    For example, to optimize the full-text index for an FTS table named
    "docs":
-- Optimize the internal structure of FTS table "docs".
INSERT INTO docs(docs) VALUES('optimize');
    The statement above may appear syntactically incorrect to some. Refer to
    the section describing the simple fts queries for an explanation.
    There is another, deprecated, method for invoking the optimize
    operation using a SELECT statement. New code should use statements
    similar to the INSERT above to optimize FTS structures.
1.4. Simple FTS Queries
  As for all other SQLite tables, virtual or otherwise, data is retrieved
  from FTS tables using a SELECT statement.
  FTS tables can be queried efficiently using SELECT statements of two
  different forms:
    Query by rowid. If the WHERE clause of the SELECT statement
    contains a sub-clause of the form "rowid = ?", where ? is an SQL expression,
    FTS is able to retrieve the requested row directly using the equivalent
    of an SQLite INTEGER PRIMARY KEY index.
    Full-text query. If the WHERE clause of the SELECT statement contains
    a sub-clause of the form "<column> MATCH ?", FTS is able to use
    the built-in full-text index to restrict the search to those documents
    that match the full-text query string specified as the right-hand operand
    of the MATCH clause.
  If neither of these two query strategies can be used, all
  queries on FTS tables are implemented using a linear scan of the entire
  table. If the table contains large amounts of data, this may be an
  impractical approach (the first example on this page shows that a linear
  scan of 1.5 GB of data takes around 30 seconds using a modern PC).
-- The examples in this block assume the following FTS table:
CREATE VIRTUAL TABLE mail USING fts3(subject, body);
SELECT * FROM mail WHERE rowid = 15;                -- Fast. Rowid lookup.
SELECT * FROM mail WHERE body MATCH 'sqlite';       -- Fast. Full-text query.
SELECT * FROM mail WHERE mail MATCH 'search';       -- Fast. Full-text query.
SELECT * FROM mail WHERE rowid BETWEEN 15 AND 20;   -- Fast. Rowid lookup.
SELECT * FROM mail WHERE subject = 'database';      -- Slow. Linear scan.
SELECT * FROM mail WHERE subject MATCH 'database';  -- Fast. Full-text query.
  In all of the full-text queries above, the right-hand operand of the MATCH
  operator is a string consisting of a single term. In this case, the MATCH
  expression evaluates to true for all documents that contain one or more
  instances of the specified word ("sqlite", "search" or "database", depending
  on which example you look at). Specifying a single term as the right-hand
  operand of the MATCH operator results in the simplest and most common type
  of full-text query possible. However more complicated queries are possible,
  including phrase searches, term-prefix searches and searches for documents
  containing combinations of terms occurring within a defined proximity of each
  other. The various ways in which the full-text index may be queried are
  described below.
  Normally, full-text queries are case-insensitive. However, this
  is dependent on the specific tokenizer used by the FTS table
  being queried. Refer to the section on tokenizers for details.
  The paragraph above notes that a MATCH operator with a simple term as the
  right-hand operand evaluates to true for all documents that contain the
  specified term. In this context, the "document" may refer to either the
  data stored in a single column of a row of an FTS table, or to the contents
  of all columns in a single row, depending on the identifier used as the
  left-hand operand to the MATCH operator. If the identifier specified as
  the left-hand operand of the MATCH operator is an FTS table column name,
  then the document that the search term must be contained in is the value
  stored in the specified column. However, if the identifier is the name
  of the FTS table itself, then the MATCH operator evaluates to true
  for each row of the FTS table for which any column contains the search
  term. The following example demonstrates this:
-- Example schema
CREATE VIRTUAL TABLE mail USING fts3(subject, body);
-- Example table population
INSERT INTO mail(docid, subject, body) VALUES(1, 'software feedback', 'found it too slow');
INSERT INTO mail(docid, subject, body) VALUES(2, 'software feedback', 'no feedback');
INSERT INTO mail(docid, subject, body) VALUES(3, 'slow lunch order',  'was a software problem');
-- Example queries
SELECT * FROM mail WHERE subject MATCH 'software';    -- Selects rows 1 and 2
SELECT * FROM mail WHERE body    MATCH 'feedback';    -- Selects row 2
SELECT * FROM mail WHERE mail    MATCH 'software';    -- Selects rows 1, 2 and 3
SELECT * FROM mail WHERE mail    MATCH 'slow';        -- Selects rows 1 and 3
  At first glance, the final two full-text queries in the example above seem
  to be syntactically incorrect, as there is a table name ("mail") used as
  an SQL expression. The reason this is acceptable is that each FTS table
  actually has a HIDDEN column with the same name
  as the table itself (in this case, "mail"). The value stored in this
  column is not meaningful to the application, but can be used as the
  left-hand operand to a MATCH operator. This special column may also be
  passed as an argument to the FTS auxiliary functions.
  The following example illustrates the above. The expressions "docs",
  "docs.docs" and "main.docs.docs" all refer to column "docs". However, the
  expression "main.docs" does not refer to any column. It could be used to
  refer to a table, but a table name is not allowed in the context in which
  it is used below.
-- Example schema
CREATE VIRTUAL TABLE docs USING fts4(content);
-- Example queries
SELECT * FROM docs WHERE docs MATCH 'sqlite';              -- OK.
SELECT * FROM docs WHERE docs.docs MATCH 'sqlite';         -- OK.
SELECT * FROM docs WHERE main.docs.docs MATCH 'sqlite';    -- OK.
SELECT * FROM docs WHERE main.docs MATCH 'sqlite';         -- Error.
1.5. Summary
  From the users point of view, FTS tables are similar to ordinary SQLite
  tables in many ways. Data may be added to, modified within and removed
  from FTS tables using the INSERT, UPDATE and DELETE commands just as
  it may be with ordinary tables. Similarly, the SELECT command may be used
  to query data. The following list summarizes the differences between FTS
  and ordinary tables:
    As with all virtual table types, it is not possible to create indices or
    triggers attached to FTS tables. Nor is it possible to use the ALTER TABLE
    command to add extra columns to FTS tables (although it is possible to use
    ALTER TABLE to rename an FTS table).
    Data-types specified as part of the "CREATE VIRTUAL TABLE" statement
    used to create an FTS table are ignored completely. Instead of the
    normal rules for applying type affinity to inserted values, all
    values inserted into FTS table columns (except the special rowid
    column) are converted to type TEXT before being stored.
    FTS tables permit the special alias "docid" to be used to refer to the
    rowid column supported by all virtual tables.
    The FTS MATCH operator is supported for queries based on the built-in
    full-text index.
    The FTS auxiliary functions, snippet(), offsets(), and matchinfo() are
    available to support full-text queries.
    Every FTS table has a hidden column with the
    same name as the table itself. The value contained in each row for the
    hidden column is a blob that is only useful as the left operand of a
    MATCH operator, or as the left-most argument to one
    of the FTS auxiliary functions.
2. Compiling and Enabling FTS3 and FTS4
  Although FTS3 and FTS4 are included with the SQLite core source code, they are not
  enabled by default. To build SQLite with FTS functionality enabled, define
  the preprocessor macro SQLITE_ENABLE_FTS3 when compiling. New applications
  should also define the SQLITE_ENABLE_FTS3_PARENTHESIS macro to enable the
  enhanced query syntax (see below). Usually, this is done by adding the
  following two switches to the compiler command line:
-DSQLITE_ENABLE_FTS3
-DSQLITE_ENABLE_FTS3_PARENTHESIS
  Note that enabling FTS3 also makes FTS4 available.  There is not a separate
  SQLITE_ENABLE_FTS4 compile-time option.  A build of SQLite either supports
  both FTS3 and FTS4 or it supports neither.
  If using the canonical build system, setting the CPPFLAGS
  environment variable while running the 'configure' script is an easy
  way to set these macros. For example, the following command:
CPPFLAGS="-DSQLITE_ENABLE_FTS3_PARENTHESIS" ./configure --enable-fts3 <configure options>
  where <configure options> are those options normally passed to
  the configure script, if any.
  Because FTS3 and FTS4 are virtual tables, The SQLITE_ENABLE_FTS3 compile-time option
  is incompatible with the SQLITE_OMIT_VIRTUALTABLE option.
  If a build of SQLite does not include the FTS modules, then any attempt to prepare an
  SQL statement to create an FTS3 or FTS4 table or to drop or access an existing
  FTS table in any way will fail. The error message returned will be similar
  to "no such module: ftsN" (where N is either 3 or 4).
  If the C version of the ICU library
  is available, then FTS may also be compiled with the SQLITE_ENABLE_ICU
  pre-processor macro defined. Compiling with this macro enables an FTS
  tokenizer that uses the ICU library to split a document into terms
  (words) using the conventions for a specified language and locale.
-DSQLITE_ENABLE_ICU
  In the canonical build tree version 3.48 or higher ICU can be enabled
  using configure script flags: see ./configure --help for
  details.
3. Full-text Index Queries
  The most useful thing about FTS tables is the queries that may be
  performed using the built-in full-text index. Full-text queries are
  performed by specifying a clause of the form
  "<column> MATCH <full-text query expression>" as part of the WHERE
  clause of a SELECT statement that reads data from an FTS table.
  Simple FTS queries that return all documents that
  contain a given term are described above. In that discussion the right-hand
  operand of the MATCH operator was assumed to be a string consisting of a
  single term. This section describes the more complex query types supported
  by FTS tables, and how they may be utilized by specifying a more
  complex query expression as the right-hand operand of a MATCH operator.
  FTS tables support three basic query types:
  Token or token prefix queries.
    An FTS table may be queried for all documents that contain a specified
    term (the simple case described above), or for
    all documents that contain a term with a specified prefix. As we have
    seen, the query expression for a specific term is simply the term itself.
    The query expression used to search for a term prefix is the prefix
    itself with a '*' character appended to it. For example:
-- Virtual table declaration
CREATE VIRTUAL TABLE docs USING fts3(title, body);
-- Query for all documents containing the term "linux":
SELECT * FROM docs WHERE docs MATCH 'linux';
-- Query for all documents containing a term with the prefix "lin". This will match
-- all documents that contain "linux", but also those that contain terms "linear",
--"linker", "linguistic" and so on.
SELECT * FROM docs WHERE docs MATCH 'lin*';
    Normally, a token or token prefix query is matched against the FTS table
    column specified as the left-hand side of the MATCH operator. Or, if the
    special column with the same name as the FTS table itself is specified,
    against all columns. This may be overridden by specifying a column-name
    followed by a ":" character before a basic term query. There may be space
    between the ":" and the term to query for, but not between the column-name
    and the ":" character. For example:
-- Query the database for documents for which the term "linux" appears in
-- the document title, and the term "problems" appears in either the title
-- or body of the document.
SELECT * FROM docs WHERE docs MATCH 'title:linux problems';
-- Query the database for documents for which the term "linux" appears in
-- the document title, and the term "driver" appears in the body of the document
-- ("driver" may also appear in the title, but this alone will not satisfy the
-- query criteria).
SELECT * FROM docs WHERE body MATCH 'title:linux driver';
    If the FTS table is an FTS4 table (not FTS3), a token may also be prefixed
