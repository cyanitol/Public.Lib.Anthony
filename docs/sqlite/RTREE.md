The SQLite R*Tree Module
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
The SQLite R*Tree Module
Table Of Contents
1. Overview
2. Compiling The R*Tree Module
3. Using the R*Tree Module
3.1. Creating An R*Tree Index
3.1.1. Column naming details
3.2. Populating An R*Tree Index
3.3. Querying An R*Tree Index
3.4. Roundoff Error
3.5. Reading And Writing At The Same Time
4. Using R*Trees Effectively
4.1. Auxiliary Columns
4.1.1. Limitations
5. Integer-Valued R-Trees
6. Custom R-Tree Queries
6.1. The Legacy xGeom Callback
6.2. The New xQueryFunc Callback
6.3. Additional Considerations for Custom Queries
7. Implementation Details
7.1. Shadow Tables
7.2. Integrity Check using the rtreecheck() SQL function
1. Overview
An R-Tree is a special
index that is designed for doing range queries.  R-Trees are most commonly
used in geospatial systems where each entry is a rectangle with minimum and
maximum X and Y coordinates.  Given a query rectangle, an R-Tree is able
to quickly find all entries that are contained within the query rectangle
or which overlap the query rectangle.  This idea is easily extended to
three dimensions for use in CAD systems.  R-Trees also find use in time-domain
range look-ups.  For example, suppose a database records the starting and
ending times for a large number of events.  A R-Tree is able to quickly
find all events that were active at any time during a given
time interval, or all events that started during a particular time interval,
or all events that both started and ended within a given time interval.
And so forth.
The R-Tree concept originated with
Toni Guttman:
R-Trees: A Dynamic Index Structure for Spatial Searching,
Proc. 1984 ACM SIGMOD International Conference on Management of Data,
pp. 47-57.
The implementation found in SQLite is a refinement of Guttman's original
idea, commonly called "R*Trees", that was described by
Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
The R*-Tree: An Efficient and Robust Access Method for Points
and Rectangles. SIGMOD Conference 1990: 322-331.
2. Compiling The R*Tree Module
The source code to the SQLite R*Tree module is included as part
of the amalgamation.  However, depending on configuration options
and the particular version of SQLite you are using, it may or may not
be enabled by default.  To ensure that the R*Tree module is enabled,
simply compile with the SQLITE_ENABLE_RTREE 
C-preprocessor macro defined.  With many compilers, this is accomplished
by adding the option "-DSQLITE_ENABLE_RTREE=1" to the compiler
command-line.
3. Using the R*Tree Module
The SQLite R*Tree module is implemented as a
virtual table.  Each R*Tree index is a
virtual table with an odd number of columns between 3 and 11.
The first column is always a 64-bit signed integer primary key.
The other columns are pairs, one pair per dimension, containing the
minimum and maximum values for that dimension, respectively.
A 1-dimensional R*Tree thus has 3 columns.
A 2-dimensional R*Tree has 5 columns.
A 3-dimensional R*Tree has 7 columns.
A 4-dimensional R*Tree has 9 columns.
And a 5-dimensional R*Tree has 11 columns.  The SQLite R*Tree implementation
does not support R*Trees wider than 5 dimensions.
The first column of an SQLite R*Tree is similar to an integer primary
key column of a normal SQLite table. It may only store a 64-bit signed
integer value. Inserting a NULL value into this column causes SQLite
to automatically generate a new unique primary key value. If an attempt
is made to insert any other non-integer value into this column,
the r-tree module silently converts it to an integer before writing it
into the database.
The min/max-value pair columns are stored as 32-bit floating point values for
"rtree" virtual tables or as 32-bit signed integers in "rtree_i32" virtual
tables.  Unlike regular SQLite tables which can store data in a variety of
datatypes and formats, the R*Tree rigidly enforce these storage types.
If any other type of value is inserted into such a column, the r-tree
module silently converts it to the required type before writing the
new record to the database.
3.1. Creating An R*Tree Index
A new R*Tree index is created as follows:
CREATE VIRTUAL TABLE <name> USING rtree(<column-names>);
The <name> is the name your application chooses for the
R*Tree index and <column-names> is a comma separated list
of between 3 and 11 columns.
The virtual <name> table creates three shadow tables to actually
store its content.  The names of these shadow tables are:
<name>_node
<name>_rowid
<name>_parent
The shadow tables are ordinary SQLite data tables.  You can query them
directly if you like, though this unlikely to reveal anything particularly
useful.
And you can UPDATE, DELETE, INSERT or even DROP
the shadow tables, though doing so will corrupt your R*Tree index.
So it is best to simply ignore the shadow tables.  Recognize that they
hold your R*Tree index information and let it go as that.
As an example, consider creating a two-dimensional R*Tree index for use in
spatial queries:
CREATE VIRTUAL TABLE demo_index USING rtree(
   id,              -- Integer primary key
   minX, maxX,      -- Minimum and maximum X coordinate
   minY, maxY       -- Minimum and maximum Y coordinate
);
3.1.1. Column naming details
In the argments to "rtree" in the CREATE VIRTUAL TABLE statement, the
names of the columns are taken from the first token of each argument.
All subsequent tokens within each argument are silently ignored.
This means, for example, that if you try to give a column a
type affinity or add a constraint such as UNIQUE or NOT NULL or DEFAULT to
a column, those extra tokens are accepted as valid, but they do not change
the behavior of the rtree.
In an RTREE virtual table, the first column always has a
type affinity of INTEGER and all other data columns have a
type affinity of REAL.
In an RTREE_I32 virtual table, all columns have type affinity of INTEGER.
Recommended practice is to omit any extra tokens in the rtree specification.
Let each argument to "rtree" be a single ordinary label that is the name of
the corresponding column, and omit all other tokens from the argument list.
3.2. Populating An R*Tree Index
The usual INSERT, UPDATE, and DELETE commands work on an R*Tree
index just like on regular tables.  So to insert some data into our sample
R*Tree index, we can do something like this:
INSERT INTO demo_index VALUES
  (28215, -80.781227, -80.604706, 35.208813, 35.297367),
  (28216, -80.957283, -80.840599, 35.235920, 35.367825),
  (28217, -80.960869, -80.869431, 35.133682, 35.208233),
  (28226, -80.878983, -80.778275, 35.060287, 35.154446),
  (28227, -80.745544, -80.555382, 35.130215, 35.236916),
  (28244, -80.844208, -80.841988, 35.223728, 35.225471),
  (28262, -80.809074, -80.682938, 35.276207, 35.377747),
  (28269, -80.851471, -80.735718, 35.272560, 35.407925),
  (28270, -80.794983, -80.728966, 35.059872, 35.161823),
  (28273, -80.994766, -80.875259, 35.074734, 35.172836),
  (28277, -80.876793, -80.767586, 35.001709, 35.101063),
  (28278, -81.058029, -80.956375, 35.044701, 35.223812),
  (28280, -80.844208, -80.841972, 35.225468, 35.227203),
  (28282, -80.846382, -80.844193, 35.223972, 35.225655);
The entries above are bounding boxes (longitude and latitude) for 14
zipcodes near Charlotte, NC.  A real database would have many thousands,
millions, or billions of such entries, but this small 14-row sample will
be sufficient to illustrate the ideas.
3.3. Querying An R*Tree Index
Any valid query will work against an R*Tree index.  The R*Tree
implementation just makes some kinds of queries especially
efficient.  Queries against the primary key are efficient:
SELECT * FROM demo_index WHERE id=28269;
Of course, an ordinary SQLite table will also do a query against its
integer primary key efficiently, so the previous is not important.
The big reason for using an R*Tree is so that
you can efficiently do range queries against the coordinate
ranges.  For example, the main office of the SQLite project is
located at 35.37785, -80.77470.
To find which zipcodes might service that office, one could write:
SELECT id FROM demo_index
 WHERE minX<=-80.77470 AND maxX>=-80.77470
   AND minY<=35.37785  AND maxY>=35.37785;
The query above will quickly locate all zipcodes that contain
the SQLite main office in their bounding box, even if the
R*Tree contains many entries.  The previous is an example
of a "contained-within" query.  The R*Tree also supports "overlapping"
queries.  For example, to find all zipcode bounding boxes that overlap
with the 28269 zipcode:
SELECT A.id FROM demo_index AS A, demo_index AS B
 WHERE A.maxX>=B.minX AND A.minX<=B.maxX
   AND A.maxY>=B.minY AND A.minY<=B.maxY
   AND B.id=28269;
This second query will find both the 28269 entry (since every bounding box
overlaps with itself) and also any other zipcodes that are close enough to
28269 that their bounding boxes overlap.
Note that it is not necessary for all coordinates in an R*Tree index
to be constrained in order for the index search to be efficient.
One might, for example, want to query all objects that overlap with
the 35th parallel:
SELECT id FROM demo_index
 WHERE maxY>=35.0  AND minY<=35.0;
But, generally speaking, the more constraints that the R*Tree module
has to work with, and the smaller the bounding box, the faster the
results will come back.
3.4. Roundoff Error
By default, coordinates are stored in an R*Tree using 32-bit floating
point values.  When a coordinate cannot be exactly represented by a
32-bit floating point number, the lower-bound coordinates are rounded down
and the upper-bound coordinates are rounded up.  Thus, bounding boxes might
be slightly larger than specified, but will never be any smaller.  This
is exactly what is desired for doing the more common "overlapping" queries
where the application wants to find every entry in the R*Tree that overlaps
a query bounding box.  Rounding the entry bounding boxes outward might cause a
few extra entries to appear in an overlapping query if the edge of the
entry bounding box corresponds to an edge of the query bounding box.  But
the overlapping query will never miss a valid table entry.
However, for a "contained-within" style query, rounding the bounding
boxes outward might cause some entries to be excluded from the result set
if the edge of the entry bounding box corresponds to the edge of the query
bounding box.  To guard against this, applications should expand their
contained-within query boxes slightly (by 0.000012%) by rounding down the
lower coordinates and rounding up the top coordinates, in each dimension.
3.5. Reading And Writing At The Same Time
It is the nature of the Guttman R-Tree algorithm that any write might
radically restructure the tree, and in the process change the scan order
of the nodes.  For this reason, it is not generally possible to modify
the R-Tree in the middle of a query of the R-Tree.  Attempts to do so
will fail with a SQLITE_LOCKED "database table is locked" error.
So, for example, suppose an application runs one query against an R-Tree like
this:
SELECT id FROM demo_index
 WHERE maxY>=35.0  AND minY<=35.0;
Then for each "id" value returned, suppose the application creates an
UPDATE statement like the following and binds the "id" value returned against
the "?1" parameter:
UPDATE demo_index SET maxY=maxY+0.5 WHERE id=?1;
Then the UPDATE might fail with an SQLITE_LOCKED error.  The reason is that
the initial query has not run to completion.  It is remembering its place
in the middle of a scan of the R-Tree.  So an update to the R-Tree cannot
be tolerated as this would disrupt the scan.
This is a limitation of the R-Tree extension only.  Ordinary tables in
SQLite are able to read and write at the same time.  Other virtual tables
might (or might not) also have that capability.  And R-Tree can appear to read
and write at the same time in some circumstances, if it can figure out how
to reliably run the query to completion before starting the update.  But
you shouldn't count on that for every query.  Generally speaking, it is
best to avoid running queries and updates to the same R-Tree at the same
time.
If you really need to update an R-Tree based on complex queries against
the same R-Tree, it is best to run the complex queries first and store
the results in a temporary table, then update the R-Tree based on the values
stored in the temporary table.
4. Using R*Trees Effectively
For SQLite versions prior to 3.24.0 (2018-06-04),
the only information that an R*Tree index stores about an object is
its integer ID and its bounding box.  Additional information needs to
be stored in separate tables and related to the R*Tree index using
the primary key.  For the example above, one might create an auxiliary
table as follows:
CREATE TABLE demo_data(
  id INTEGER PRIMARY KEY,  -- primary key
  objname TEXT,            -- name of the object
  objtype TEXT,            -- object type
  boundary BLOB            -- detailed boundary of object
);
In this example, the demo_data.boundary field is intended to hold some
kind of binary representation of the precise boundaries of the object.
The R*Tree index only holds an axis-aligned rectangular boundary for the
object.  The R*Tree boundary is just an approximation of the true object
boundary.  So what typically happens is that the R*Tree index is used to
narrow a search down to a list of candidate objects and then more detailed
and expensive computations are done on each candidate to find if the
candidate truly meets the search criteria.
Key Point:
An R*Tree index does not normally provide the exact answer but merely
reduces the set of potential answers from millions to dozens.
Suppose the demo_data.boundary field holds some proprietary data description
of a complex two-dimensional boundary for a zipcode and suppose that the
application has used the sqlite3_create_function() interface to
created an application-defined function "contained_in(boundary,lat,long)"
that accepts the demo_data.boundary object and a latitute and longitude
and returns return true or false if the lat/long is contained within
the boundary.
One may assume that "contained_in()" is a relatively slow
functions that we do not want to invoke too frequently.
Then an efficient way to find the specific ZIP code for the main
SQLite office would be to run a query like this:
SELECT objname FROM demo_data, demo_index
 WHERE demo_data.id=demo_index.id
   AND contained_in(demo_data.boundary, 35.37785, -80.77470)
   AND minX<=-80.77470 AND maxX>=-80.77470
   AND minY<=35.37785  AND maxY>=35.37785;
Notice how the query above works:  The R*Tree index runs in the outer
loop to find entries that contain the SQLite main office in their
boundary box.
For each row found, SQLite looks up
the corresponding entry in the demo_data table.  It then uses the boundary
field from the demo_data table as a parameter to the contained_in()
function and if that function returns true, then we know the sought after
coordinate is in that ZIP code boundary.
One would get the same answer without the use of the R*Tree index
using the following simpler query:
SELECT objname FROM demo_data
 WHERE contained_in(demo_data.boundary, 35.37785, -80.77470);
The problem with this latter query is that it must apply the
contained_in() function to all entries in the demo_data table.
The use of the R*Tree in the penultimate query reduces the number of
calls to contained_in() function to a small subset of the entire table.
The R*Tree index did not find the exact answer itself, it merely
limited the search space.
4.1. Auxiliary Columns
Beginning with SQLite version 3.24.0 (2018-06-04), r-tree tables
can have auxiliary columns that store arbitrary data.
Auxiliary columns can be used in place of
secondary tables such as "demo_data".
Auxiliary columns are marked with a "+" symbol before the column name.
Auxiliary columns must come after all of the coordinate boundary columns.
An RTREE table can have no more than 100 columns total.  In other words,
the count of columns including the integer primary key column,
the coordinate boundary columns, and all auxiliary columns must be 100 or less.
The following example shows an r-tree table with auxiliary columns that
is equivalent to the two tables "demo_index" and "demo_data" above:
CREATE VIRTUAL TABLE demo_index2 USING rtree(
   id,              -- Integer primary key
   minX, maxX,      -- Minimum and maximum X coordinate
   minY, maxY,      -- Minimum and maximum Y coordinate
   +objname TEXT,   -- name of the object
   +objtype TEXT,   -- object type
   +boundary BLOB   -- detailed boundary of object
);
By combining location data and related information into the same
table, auxiliary columns can provide a cleaner model
and reduce the need for joins.
For example, the earlier
join between demo_index and demo_data can now
be written as a simple query, like this:
SELECT objname FROM demo_index2
 WHERE contained_in(boundary, 35.37785, -80.77470)
   AND minX<=-80.77470 AND maxX>=-80.77470
   AND minY<=35.37785  AND maxY>=35.37785;
4.1.1. Limitations
For auxiliary columns, only the name of the column matters.
The type affinity is ignored.
Constraints such as NOT NULL, UNIQUE, REFERENCES, or CHECK
are also ignored.  However, future versions
of SQLite might start paying attention to the type affinity and
constraints, so users of auxiliary columns are advised to leave
both blank, to avoid future compatibility problems.
5. Integer-Valued R-Trees
The default virtual table ("rtree") stores coordinates as
single-precision (4-byte) floating point numbers.  If integer coordinates
are desired, declare the table using "rtree_i32" instead:
CREATE VIRTUAL TABLE intrtree USING rtree_i32(id,x0,x1,y0,y1,z0,z1);
An rtree_i32 stores coordinates as 32-bit signed integers.
Even though it stores values using integers, the rtree_i32 virtual
table still uses floating point computations internally as part of
the r-tree algorithm.
6. Custom R-Tree Queries
By using standard SQL expressions in the WHERE clause of a SELECT query,
a programmer can query for all R*Tree entries that
intersect with or are contained within a particular bounding-box.
Custom R*Tree queries, using the MATCH
operator in the WHERE clause of a SELECT, allow the programmer to query for
the set of R*Tree entries that intersect any arbitrary region or shape, not
just a box.  This capability is useful, for example, in computing the
subset of objects in the R*Tree that are visible from a camera positioned
in 3-D space.
Regions for custom R*Tree queries are defined by R*Tree geometry callbacks
implemented by the application and registered with SQLite via a call to one
of the following two APIs:
int sqlite3_rtree_query_callback(
  sqlite3 *db,
  const char *zQueryFunc,
  int (*xQueryFunc)(sqlite3_rtree_query_info*),
  void *pContext,
  void (*xDestructor)(void*)
);
int sqlite3_rtree_geometry_callback(
  sqlite3 *db,
  const char *zGeom,
  int (*xGeom)(sqlite3_rtree_geometry *, int nCoord, double *aCoord, int *pRes),
  void *pContext
);
The sqlite3_rtree_query_callback() became available with SQLite
version 3.8.5 (2014-06-04) and is the preferred interface.
The sqlite3_rtree_geometry_callback() is an older and less flexible
interface that is supported for backwards compatibility.
A call to one of the above APIs creates a new SQL function named by the
second parameter (zQueryFunc or zGeom).  When that SQL function appears
on the right-hand side of the MATCH operator and the left-hand side of the
MATCH operator is any column in the R*Tree virtual table, then the callback
defined by the third argument (xQueryFunc or xGeom) is invoked to determine
if a particular object or subtree overlaps the desired region.
For example, a query like the following might be used to find all
R*Tree entries that overlap with a circle centered at 45.3,22.9 with a
radius of 5.0:
SELECT id FROM demo_index WHERE id MATCH circle(45.3, 22.9, 5.0)
The SQL syntax for custom queries is the same regardless of which
interface, sqlite3_rtree_geometry_callback() or sqlite3_rtree_query_callback(),
is used to register the SQL function.  However, the newer query-style
callbacks give the application greater control over how the query proceeds.
6.1. The Legacy xGeom Callback
The legacy xGeom callback is invoked with four arguments.  The first
argument is a pointer to an sqlite3_rtree_geometry structure which provides
information about how the SQL function was invoked.  The second argument
is the number of coordinates in each r-tree entry, and is always the same
for any given R*Tree.  The number of coordinates is 2 for a 1-dimensional R*Tree,
4 for a 2-dimensional R*Tree, 6 for a 3-dimensional R*Tree, and so forth.
The third argument, aCoord[], is an array of nCoord coordinates that defines
a bounding box to be tested.  The last argument is a pointer into which
the callback result should be written.  The result is zero
if the bounding-box defined by aCoord[] is completely outside
the region defined by the xGeom callback and the result is non-zero if
the bounding-box is inside or overlaps with the xGeom region.  The xGeom
callback should normally return SQLITE_OK.  If xGeom returns anything other
than SQLITE_OK, then the r-tree query will abort with an error.
The sqlite3_rtree_geometry structure that the first argument to the
xGeom callback points to has a structure shown below.  The exact same
sqlite3_rtree_geometry
structure is used for every callback for same MATCH operator in the same
query.  The contents of the sqlite3_rtree_geometry
structure are initialized by SQLite but are
not subsequently modified.  The callback is free to make changes to the
pUser and xDelUser elements of the structure if desired.
typedef struct sqlite3_rtree_geometry sqlite3_rtree_geometry;
struct sqlite3_rtree_geometry {
  void *pContext;                 /* Copy of pContext passed to s_r_g_c() */
  int nParam;                     /* Size of array aParam */
  double *aParam;                 /* Parameters passed to SQL geom function */
  void *pUser;                    /* Callback implementation user data */
  void (*xDelUser)(void *);       /* Called by SQLite to clean up pUser */
};
The pContext member of the sqlite3_rtree_geometry
structure is always set to a copy of the pContext
argument passed to sqlite3_rtree_geometry_callback() when the
callback is registered. The aParam[] array (size nParam) contains the parameter
values passed to the SQL function on the right-hand side of the MATCH operator.
In the example "circle" query above, nParam would be set to 3 and the aParam[]
array would contain the three values 45.3, 22.9 and 5.0.
The pUser and xDelUser members of the sqlite3_rtree_geometry structure are
