The SQLite Query Optimizer Overview
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
The SQLite Query Optimizer Overview
Table Of Contents
1. Introduction
2. WHERE Clause Analysis
2.1. Index Term Usage Examples
3. The BETWEEN Optimization
4. OR Optimizations
4.1. Converting OR-connected constraint into an IN operator
4.2. Evaluating OR constraints separately and taking the UNION of the result
5. The LIKE Optimization
6. The Skip-Scan Optimization
7. Joins
7.1. Manual Control Of Join Order
7.1.1. Manual Control Of Query Plans Using SQLITE_STAT Tables
7.1.2. Manual Control of Query Plans using CROSS JOIN
8. Choosing Between Multiple Indexes
8.1. Disqualifying WHERE Clause Terms using Unary-"+"
8.2. Range Queries
9. Covering Indexes
10. ORDER BY Optimizations
10.1. Partial ORDER BY via Index
11. Subquery Flattening
12. Subquery Co-routines
12.1. Using Co-routines to Defer Work until after the Sorting
13. The MIN/MAX Optimization
14. Automatic Query-Time Indexes
14.1. Hash Joins
15. The Predicate Push-Down Optimization
16. The OUTER JOIN Strength Reduction Optimization
17. The Omit OUTER JOIN Optimization
18. The Constant Propagation Optimization
1. Introduction
  This document provides an overview of how the query planner and optimizer
  for SQLite works.
  Given a single SQL statement, there might be dozens, hundreds, or even
  thousands of ways to implement that statement, depending on the complexity
  of the statement itself and of the underlying database schema.  The 
  task of the query planner is to select the algorithm that minimizes
  disk I/O and CPU overhead.
  Additional background information is available in the
  indexing tutorial document.
  The Next Generation Query Planner document provides more detail on
  how the join order is chosen.
2. WHERE Clause Analysis
  Prior to analysis, the following transformations are made
  to shift all join constraints into the WHERE clause:
All NATURAL joins are converted into joins with a USING clause.
All USING clauses (including ones created by the previous step)
    are converted into equivalent ON clauses.
All ON clauses (include ones created by the previous step)
    are added as new conjuncts (AND-connected terms) in the WHERE clause.
  SQLite makes no distinction between join constraints that occur in the
  WHERE clause and constraints in the ON clause of an inner join, since that
  distinction does not affect the outcome.  However, there is
  a difference between ON clause constraints and WHERE clause constraints for
  outer joins.  Therefore, when SQLite moves an ON clause constraint from an
  outer join over to the WHERE clause it adds special tags to the Abstract
  Syntax Tree (AST) to indicate that the constraint came from an outer join
  and from which outer join it came. There is no way to add those tags in
  pure SQL text.  Hence, the SQL input must use ON clauses on outer joins.
  But in the internal AST, all constraints are part of the WHERE clause,
  because having everything in one place simplifies processing.
  After all constraints have been shifted into the WHERE clause,
  The WHERE clause is broken up into conjuncts (hereafter called
  "terms").  In other words, the WHERE clause is broken up into pieces
  separated from the others by an AND operator.
  If the WHERE clause is composed of constraints separated by the OR
  operator (disjuncts) then the entire clause is considered to be a single "term"
  to which the OR-clause optimization is applied.
  All terms of the WHERE clause are analyzed to see if they can be
  satisfied using indexes.
  To be usable by an index a term must usually be of one of the following
  forms:
  column = expression
  column IS expression
  column > expression
  column >= expression
  column < expression
  column <= expression
  expression = column
  expression IS column
  expression > column
  expression >= column
  expression < column
  expression <= column
  column IN (expression-list)
  column IN (subquery)
  column IS NULL
  column LIKE pattern
  column GLOB pattern
  If an index is created using a statement like this:
CREATE INDEX idx_ex1 ON ex1(a,b,c,d,e,...,y,z);
  Then the index might be used if the initial columns of the index
  (columns a, b, and so forth) appear in WHERE clause terms.
  The initial columns of the index must be used with
  the = or IN or IS operators.  
  The right-most column that is used can employ inequalities.  
  For the right-most
  column of an index that is used, there can be up to two inequalities
  that must sandwich the allowed values of the column between two extremes.
  It is not necessary for every column of an index to appear in a
  WHERE clause term in order for that index to be used. 
  However, there cannot be gaps in the columns of the index that are used.
  Thus for the example index above, if there is no WHERE clause term
  that constrains column c, then terms that constrain columns a and b can
  be used with the index but not terms that constrain columns d through z.
  Similarly, index columns will not normally be used (for indexing purposes)
  if they are to the right of a 
  column that is constrained only by inequalities.
  (See the skip-scan optimization below for the exception.)
  In the case of indexes on expressions, whenever the word "column" is
  used in the foregoing text, one can substitute "indexed expression"
  (meaning a copy of the expression that appears in the CREATE INDEX
  statement) and everything will work the same.
2.1. Index Term Usage Examples
  For the index above and WHERE clause like this:
... WHERE a=5 AND b IN (1,2,3) AND c IS NULL AND d='hello'
  The first four columns a, b, c, and d of the index would be usable since
  those four columns form a prefix of the index and are all bound by
  equality constraints.
  For the index above and WHERE clause like this:
... WHERE a=5 AND b IN (1,2,3) AND c>12 AND d='hello'
  Only columns a, b, and c of the index would be usable.  The d column
  would not be usable because it occurs to the right of c and c is
  constrained only by inequalities.
  For the index above and WHERE clause like this:
... WHERE a=5 AND b IN (1,2,3) AND d='hello'
  Only columns a and b of the index would be usable.  The d column
  would not be usable because column c is not constrained and there can
  be no gaps in the set of columns that usable by the index.
  For the index above and WHERE clause like this:
... WHERE b IN (1,2,3) AND c NOT NULL AND d='hello'
  The index is not usable at all because the left-most column of the
  index (column "a") is not constrained.  Assuming there are no other
  indexes, the query above would result in a full table scan.
  For the index above and WHERE clause like this:
... WHERE a=5 OR b IN (1,2,3) OR c NOT NULL OR d='hello'
  The index is not usable because the WHERE clause terms are connected
  by OR instead of AND. This query would result in a full table scan.
  However, if three additional indexes are added that contain columns
  b, c, and d as their left-most columns, then the
  OR-clause optimization might apply.
3. The BETWEEN Optimization
  If a term of the WHERE clause is of the following form:
  expr1 BETWEEN expr2 AND expr3
  Then two "virtual" terms are added as follows:
  expr1 >= expr2 AND expr1 <= expr3
  Virtual terms are used for analysis only and do not cause any byte-code
  to be generated.
  If both virtual terms end up being used as constraints on an index,
  then the original BETWEEN term is omitted and the corresponding test
  is not performed on input rows.
  Thus if the BETWEEN term ends up being used as an index constraint
  no tests are ever performed on that term.
  On the other hand, the
  virtual terms themselves never causes tests to be performed on
  input rows.
  Thus if the BETWEEN term is not used as an index constraint and
  instead must be used to test input rows, the expr1 expression is
  only evaluated once.
4. OR Optimizations
  WHERE clause constraints that are connected by OR instead of AND can
  be handled in two different ways.
4.1. Converting OR-connected constraint into an IN operator
  If a term consists of multiple subterms containing a common column
  name and separated by OR, like this:
  column = expr1 OR column = expr2 OR column = expr3 OR ...
  Then that term is rewritten as follows:
  column IN (expr1,expr2,expr3,...)
  The rewritten term then might go on to constrain an index using the
  normal rules for IN operators.  Note that column must be
  the same column in every OR-connected subterm,
  although the column can occur on either the left or the right side of
  the = operator.
4.2. Evaluating OR constraints separately and taking the UNION of the result
  If and only if the previously described conversion of OR to an IN operator
  does not work, the second OR-clause optimization is attempted.
  Suppose the OR clause consists of multiple subterms as follows:
  expr1 OR expr2 OR expr3
  Individual subterms might be a single comparison expression like
  a=5 or x>y or they can be 
  LIKE or BETWEEN expressions, or a subterm
  can be a parenthesized list of AND-connected sub-subterms.
  Each subterm is analyzed as if it were itself the entire WHERE clause
  in order to see if the subterm is indexable by itself.
  If every subterm of an OR clause is separately indexable
  then the OR clause might be coded such that a separate index is used
  to evaluate each term of the OR clause.  One way to think about how
  SQLite uses separate indexes for each OR clause term is to imagine
  that the WHERE clause where rewritten as follows:
  rowid IN (SELECT rowid FROM table WHERE expr1
            UNION SELECT rowid FROM table WHERE expr2
            UNION SELECT rowid FROM table WHERE expr3)
  The rewritten expression above is conceptual; WHERE clauses containing
  OR are not really rewritten this way.
  The actual implementation of the OR clause uses a mechanism that is
  more efficient and that works even for WITHOUT ROWID tables or 
  tables in which the "rowid" is inaccessible.  Nevertheless,
  the essence of the implementation is captured by the statement
  above:  Separate indexes are used to find candidate result rows
  from each OR clause term and the final result is the union of
  those rows.
  Note that in most cases, SQLite will only use a single index for each
  table in the FROM clause of a query.  The second OR-clause optimization
  described here is the exception to that rule.  With an OR-clause,
  a different index might be used for each subterm in the OR-clause.
  For any given query, the fact that the OR-clause optimization described
  here can be used does not guarantee that it will be used.
  SQLite uses a cost-based query planner that estimates the CPU and
  disk I/O costs of various competing query plans and chooses the plan
  that it thinks will be the fastest.  If there are many OR terms in
  the WHERE clause or if some of the indexes on individual OR-clause 
  subterms are not very selective, then SQLite might decide that it is
  faster to use a different query algorithm, or even a full-table scan.
  Application developers can use the
  EXPLAIN QUERY PLAN prefix on a statement to get a
  high-level overview of the chosen query strategy.
5. The LIKE Optimization
  A WHERE-clause term that uses the LIKE or GLOB operator
  can sometimes be used with an index to do a range search, 
  almost as if the LIKE or GLOB were an alternative to a BETWEEN
  operator.
  There are many conditions on this optimization:
  The right-hand side of the LIKE or GLOB must be either a string literal
      or a parameter bound to a string literal
      that does not begin with a wildcard character.
  It must not be possible to make the LIKE or GLOB operator true by
      having a numeric value (instead of a string or blob) on the
      left-hand side. This means that either:
       the left-hand side of the LIKE or GLOB operator is the name
           of an indexed column with TEXT affinity, or
       the right-hand side pattern argument does not begin with a
           minus sign ("-") or a digit.
      This constraint arises from the fact that numbers do not sort in
      lexicographical order.  For example: 9<10 but '9'>'10'.
  The built-in functions used to implement LIKE and GLOB must not
      have been overloaded using the sqlite3_create_function() API.
  For the GLOB operator, the column must be indexed using the 
      built-in BINARY collating sequence.
  For the LIKE operator, if case_sensitive_like mode is enabled then
      the column must be indexed using the built-in BINARY collating sequence,
      or if case_sensitive_like mode is disabled then the column must be
      indexed using the built-in NOCASE collating sequence.
  If the ESCAPE option is used, the ESCAPE character must be ASCII,
      or a single-byte character in UTF-8.
  The LIKE operator has two modes that can be set by a
  pragma.  The
  default mode is for LIKE comparisons to be insensitive to differences
  of case for latin1 characters.  Thus, by default, the following
  expression is true:
'a' LIKE 'A'
  If the case_sensitive_like pragma is enabled as follows:
PRAGMA case_sensitive_like=ON;
  Then the LIKE operator pays attention to case and the example above would
  evaluate to false.  Note that case insensitivity only applies to
  latin1 characters - basically the upper and lower case letters of English
  in the lower 127 byte codes of ASCII.  International character sets
  are case sensitive in SQLite unless an application-defined
  collating sequence and like() SQL function are provided that
  take non-ASCII characters into account.
  If an application-defined collating sequence and/or like() SQL
  function are provided, the LIKE optimization described here will never
  be taken.
  The LIKE operator is case insensitive by default because this is what
  the SQL standard requires.  You can change the default behavior at
  compile time by using the SQLITE_CASE_SENSITIVE_LIKE command-line option
  to the compiler.
  The LIKE optimization might occur if the column named on the left of the
  operator is indexed using the built-in BINARY collating sequence and
  case_sensitive_like is turned on.  Or the optimization might occur if
  the column is indexed using the built-in NOCASE collating sequence and the 
  case_sensitive_like mode is off.  These are the only two combinations
  under which LIKE operators will be optimized.
  The GLOB operator is always case sensitive.  The column on the left side
  of the GLOB operator must always use the built-in BINARY collating sequence
  or no attempt will be made to optimize that operator with indexes.
  The LIKE optimization will only be attempted if
  the right-hand side of the GLOB or LIKE operator is either a
  literal string or a parameter that has been bound
  to a string literal.  The string literal must not
  begin with a wildcard; if the right-hand side begins with a wildcard
  character then this optimization is not attempted.  If the right-hand side 
  is a parameter that is bound to a string, then this optimization is
  only attempted if the prepared statement containing the expression
  was compiled with sqlite3_prepare_v2() or sqlite3_prepare16_v2().
  The LIKE optimization is not attempted if the
  right-hand side is a parameter and the statement was prepared using
  sqlite3_prepare() or sqlite3_prepare16().
  Suppose the initial sequence of non-wildcard characters on the right-hand
  side of the LIKE or GLOB operator is x.  We are using a single 
  character to denote this non-wildcard prefix but the reader should
  understand that the prefix can consist of more than 1 character.
  Let y be the smallest string that is the same length as /x/ but which
  compares greater than x.  For example, if x is
  'hello' then
  y would be 'hellp'.
  The LIKE and GLOB optimizations consist of adding two virtual terms
  like this:
  column >= x AND column < y
  Under most circumstances, the original LIKE or GLOB operator is still
  tested against each input row even if the virtual terms are used to
  constrain an index.  This is because we do not know what additional
  constraints may be imposed by characters to the right
  of the x prefix.  However, if there is only a single
  global wildcard to the right of x, then the original LIKE or 
  GLOB test is disabled.
  In other words, if the pattern is like this:
  column LIKE x%
  column GLOB x*
  then the original LIKE or GLOB tests are disabled when the virtual
  terms constrain an index because in that case we know that all of the
  rows selected by the index will pass the LIKE or GLOB test.
  Note that when the right-hand side of a LIKE or GLOB operator is
  a parameter and the statement is prepared using sqlite3_prepare_v2()
  or sqlite3_prepare16_v2() then the statement is automatically reparsed
  and recompiled on the first sqlite3_step() call of each run if the binding
  to the right-hand side parameter has changed since the previous run.
  This reparse and recompile is essentially the same action that occurs
  following a schema change.  The recompile is necessary so that the query
  planner can examine the new value bound to the right-hand side of the
  LIKE or GLOB operator and determine whether or not to employ the
  optimization described above.
6. The Skip-Scan Optimization
  The general rule is that indexes are only useful if there are 
  WHERE-clause constraints on the left-most columns of the index.
  However, in some cases,
  SQLite is able to use an index even if the first few columns of
  the index are omitted from the WHERE clause but later columns 
  are included.
  Consider a table such as the following:
CREATE TABLE people(
  name TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  height INT NOT NULL, -- in cm
  CHECK( role IN ('student','teacher') )
);
CREATE INDEX people_idx1 ON people(role, height);
  The people table has one entry for each person in a large
  organization.  Each person is either a "student" or a "teacher",
  as determined by the "role" field.  The table also records the height in
  centimeters of each person.  The role and height are indexed.
  Notice that the left-most column of the index is not very
  selective - it only contains two possible values.
  Now consider a query to find the names of everyone in the
  organization that is 180cm tall or taller:
SELECT name FROM people WHERE height>=180;
  Because the left-most column of the index does not appear in the
  WHERE clause of the query, one is tempted to conclude that the
  index is not usable here.  However, SQLite is able to use the index.
  Conceptually, SQLite uses the index as if the query were more
  like the following:
SELECT name FROM people
 WHERE role IN (SELECT DISTINCT role FROM people)
   AND height>=180;
  Or this:
SELECT name FROM people WHERE role='teacher' AND height>=180
UNION ALL
SELECT name FROM people WHERE role='student' AND height>=180;
  The alternative query formulations shown above are conceptual only.
  SQLite does not really transform the query. 
  The actual query plan is like this:
  SQLite locates the first possible value for "role", which it
  can do by rewinding the "people_idx1" index to the beginning and reading
  the first record.  SQLite stores this first "role" value in an
  internal variable that we will here call "$role".  Then SQLite
  runs a query like: "SELECT name FROM people WHERE role=$role AND height>=180".
  This query has an equality constraint on the left-most column of the
  index and so the index can be used to resolve that query.  Once
  that query is finished, SQLite then uses the "people_idx1" index to
  locate the next value of the "role" column, using code that is logically
  similar to "SELECT role FROM people WHERE role>$role LIMIT 1".
  This new "role" value overwrites the $role variable, and the process
  repeats until all possible values for "role" have been examined.
  We call this kind of index usage a "skip-scan" because the database
  engine is basically doing a full scan of the index but it optimizes the
  scan (making it less than "full") by occasionally skipping ahead to the
  next candidate value.
  SQLite might use a skip-scan on an index if it knows that the first
  one or more columns contain many duplication values.
  If there are too few duplicates
  in the left-most columns of the index, then it would
  be faster to simply step ahead to the next value, and thus do
  a full table scan, than to do a binary search on an index to locate
  the next left-column value.
  The only way that SQLite can know that there are many duplicates
  in the left-most columns of an index
  is if the ANALYZE command has been run
  on the database.
  Without the results of ANALYZE, SQLite has to guess at the "shape" of
  the data in the table, and the default guess is that there are an average
  of 10 duplicates for every value in the left-most column of the index.
  Skip-scan only becomes profitable (it only gets to be faster than
  a full table scan) when the number of duplicates is about 18 or more.
  Hence, a skip-scan is never used on a database that has not been analyzed.
7. Joins
  SQLite implements joins as nested loops.
  The default order of the nested loops in a join is for the left-most
  table in the FROM clause to form the outer loop and the right-most
  table to form the inner loop.
  However, SQLite will nest the loops in a different order if doing so
  will help it to select better indexes.
  Inner joins can be freely reordered.  However outer joins are
  neither commutative nor associative and hence will not be reordered.
  Inner joins to the left and right of an outer join might be reordered
  if the optimizer thinks that is advantageous but outer joins are
  always evaluated in the order in which they occur.
  SQLite treats the CROSS JOIN operator specially.
  The CROSS JOIN operator is commutative, in theory.  However, SQLite chooses to
  never reorder tables in a CROSS JOIN.  This provides a mechanism
  by which the programmer can force SQLite to choose a particular loop nesting
  order.  
  When selecting the order of tables in a join, SQLite uses an efficient
  polynomial-time algorithm graph algorithm described in
  the Next Generation Query Planner document.  Because of this,
  SQLite is able to plan queries with 50- or 60-way joins in a matter of
  microseconds
  Join reordering is automatic and usually works well enough that
  programmers do not have to think about it, especially if ANALYZE
