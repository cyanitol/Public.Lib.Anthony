Indexes On Expressions
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
Indexes On Expressions
Normally, an SQL index references columns of a table.  But an index
can also be formed on expressions involving table columns.
As an example, consider the following table that tracks
dollar-amount changes on various "accounts":
CREATE TABLE account_change(
  chng_id INTEGER PRIMARY KEY,
  acct_no INTEGER REFERENCES account,
  location INTEGER REFERENCES locations,
  amt INTEGER,  -- in cents
  authority TEXT,
  comment TEXT
);
CREATE INDEX acctchng_magnitude ON account_change(acct_no, abs(amt));
Each entry in the account_change table records a deposit or a withdrawal
into an account.  Deposits have a positive "amt" and withdrawals have
a negative "amt".
The acctchng_magnitude index is over the account number ("acct_no") and
on the absolute value of the amount.  This index allows one to do 
efficient queries over the magnitude of a change to the account.
For example, to list all changes to account number $xyz that are
more than $100.00, one can say:
SELECT * FROM account_change WHERE acct_no=$xyz AND abs(amt)>=10000;
Or, to list all changes to one particular account ($xyz) in order of
decreasing magnitude, one can write:
SELECT * FROM account_change WHERE acct_no=$xyz
 ORDER BY abs(amt) DESC;
Both of the above example queries would work fine without the
acctchng_magnitude index.
The acctchng_magnitude index merely helps the queries to run
faster, especially on databases where there are many entries in
the table for each account.
1. How To Use Indexes On Expressions
Use a CREATE INDEX statement to create a new index on one or more
expressions just like you would to create an index on columns.  The only
difference is that expressions are listed as the elements to be indexed
rather than column names.
The SQLite query planner will consider using an index on an expression
when the expression that is indexed appears in the WHERE clause or in
the ORDER BY clause of a query, exactly as it is written in the
CREATE INDEX statement.  The query planner does not do algebra.  In order
to match WHERE clause constraints and ORDER BY terms to indexes, SQLite
requires that the expressions be the same, except for minor syntactic
differences such as white-space changes.  So if you have:
CREATE TABLE t2(x,y,z);
CREATE INDEX t2xy ON t2(x+y);
And then you run the query:
SELECT * FROM t2 WHERE y+x=22;
Then the index will not be used because 
the expression on the CREATE INDEX
statement (x+y) is not the same as the expression as it appears in the 
query (y+x).  The two expressions might be mathematically equivalent, but
the SQLite query planner insists that they be the same, not merely
equivalent.  Consider rewriting the query thusly:
SELECT * FROM t2 WHERE x+y=22;
This second query will likely use the index because now the expression
in the WHERE clause (x+y) matches the expression in the index exactly.
2. Restrictions
There are certain reasonable restrictions on expressions that appear in
CREATE INDEX statements:
Expressions in CREATE INDEX statements
may only refer to columns of the table being indexed, not to
columns in other tables.
Expressions in CREATE INDEX statements
may contain function calls, but only to functions whose output
is always determined completely by its input parameters (a.k.a.:
deterministic functions).  Obviously, functions like random() will not
work well in an index.  But also functions like sqlite_version(), though
they are constant across any one database connection, are not constant
across the life of the underlying database file, and hence may not be
used in a CREATE INDEX statement.
Note that application-defined SQL functions are by default considered
non-deterministic and may not be used in a CREATE INDEX statement unless
the SQLITE_DETERMINISTIC flag is used when the function is registered.
Expressions in CREATE INDEX statements may not use subqueries.
Expressions may only be used in CREATE INDEX statements, not within
UNIQUE or PRIMARY KEY constraints within the CREATE TABLE statement.
3. Compatibility
The ability to index expressions was added to SQLite with 
version 3.9.0 (2015-10-14).  A database that uses an index on
expressions will not be usable by earlier versions of SQLite.
This page was last updated on 2023-02-11 20:57:33Z 
