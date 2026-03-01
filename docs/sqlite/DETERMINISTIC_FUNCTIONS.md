Deterministic SQL Functions
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
Deterministic SQL Functions
1. Overview
SQL functions in SQLite can be either "deterministic" or "non-deterministic".
A deterministic function always gives the same answer when it has
the same inputs.  Most built-in SQL functions in SQLite are
deterministic.  For example, the abs(X) function always returns
the same answer as long as its input X is the same.
Non-deterministic functions might give different answers on each
invocation, even if the arguments are always the same.  The following
are examples of non-deterministic functions:
 random()
 changes()
 last_insert_rowid()
 sqlite3_version()
The random() function is obviously non-deterministic because it gives
a different answer every time it is invoked.  The answers from changes()
and last_insert_rowid() depend on prior SQL statements, and so they
are also non-deterministic. The
sqlite3_version() function is mostly constant, but it can change when
SQLite is upgraded, and so even though it always returns the same answer
for any particular session, because it can change answers across sessions
it is still considered non-deterministic.
2. Restrictions on the use of non-deterministic functions
There are some contexts in SQLite that do not allow the use of
non-deterministic functions:
In the expression of a CHECK constraint.
In the WHERE clause of a partial index.
In an expression used as part of an expression index.
In the expression of a generated column.
In the cases above, the values returned by the function affects the
information stored in the database file.  The values of functions
in CHECK constraints determines which entries are valid for a table,
and functions in the WHERE clause of a partial index or in an index on
an expression compute values stored in the index b-tree.
If any of these functions later returns a different
value, then the database might no longer be well-formed.  
Hence, to avoid database corruption,
only deterministic functions can be used in the contexts
above.
3. Special-case Processing For Date/Time Functions
The built-in date and time functions of SQLite are a special case.
These functions are usually considered deterministic.  However, if
these functions use the string "now" as the date, or if they use
the localtime modifier or the utc modifier, then they are
considered non-deterministic.  Because the function inputs are
not necessarily known until run-time, the date/time functions will
throw an exception if they encounter any of the non-deterministic
features in a context where only deterministic functions are allowed.
Prior to SQLite 3.20.0 (2017-08-01) all date/time functions were
always considered non-deterministic.  The ability for date/time functions
to be deterministic sometimes and non-deterministic at other times,
depending on their arguments, was added for the 3.20.0 release.
3.1. Bug fix in version 3.35.2
When the enhancement was made to SQLite 3.20.0 such that date/time
functions would be considered deterministic as they do not depend
on the current time, one case was overlooked:
Many of the date/time functions can be called
with no arguments at all.  These no-argument date/time functions
behave as if they had a single "'now'" argument.
Thus "datetime()" and
"datetime('now')" both yield the current date and time.
However, only the second form was recognized as non-deterministic.
This meant that developers could sneak the non-deterministic
"datetime()" form into CHECK constraints, index
expressions, generated column expressions, and similar places
where non-deterministic functions make no sense.
This oversight was fixed in version 3.35.2 (2021-03-17).
However, there may be legacy databases in circulation that were created
by SQLite version 3.20.0 through 3.35.1 that have non-deterministic
date/time functions in their schemas.
4. Application-defined deterministic functions
By default, application-defined SQL functions are considered to
be non-deterministic.  However, if the 4th parameter to
sqlite3_create_function_v2() is OR-ed with 
SQLITE_DETERMINISTIC, then SQLite will treat that function as if it
were deterministic.
Note that if a non-deterministic function is tagged with
SQLITE_DETERMINISTIC and if that function ends up being used in
the WHERE clause of a partial index or in an
expression index, then when the function begins to return different
answers, the associated index may become corrupt.  If an SQL function
is nearly deterministic (which is to say, if it only rarely changes,
like sqlite_version()) and it is used in an index that becomes
corrupt, the corruption can be fixed by running REINDEX.
The interfaces necessary to construct a function that is sometimes
deterministic and sometimes non-deterministic depending on their
inputs, such as the built-in date/time functions, are not published.
Generic application-defined SQL functions must
be always deterministic or always non-deterministic.
