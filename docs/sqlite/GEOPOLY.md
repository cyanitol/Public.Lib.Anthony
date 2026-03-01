The Geopoly Interface To The SQLite R*Tree Module
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
The Geopoly Interface To The SQLite R*Tree Module
Table Of Contents
1. Overview
1.1. GeoJSON
1.2. Binary storage format
2. Using The Geopoly Extension
2.1. Queries
3. Special Functions
3.1. The geopoly_overlap(P1,P2) Function
3.2. The geopoly_within(P1,P2) Function
3.3. The geopoly_area(P) Function
3.4. The geopoly_blob(P) Function
3.5. The geopoly_json(P) Function
3.6. The geopoly_svg(P,...) Function
3.7. The geopoly_bbox(P) and geopoly_group_bbox(P) Functions
3.8. The geopoly_contains_point(P,X,Y) Function
3.9. The geopoly_xform(P,A,B,C,D,E,F) Function
3.10. The geopoly_regular(X,Y,R,N) Function
3.11. The geopoly_ccw(J) Function
4. Implementation Details
4.1. Binary Encoding of Polygons
4.2. Shadow Tables
1. Overview
The Geopoly module is an alternative interface to the R-Tree extension that uses
the GeoJSON notation
(RFC-7946) to describe two-dimensional
polygons.  Geopoly includes functions for detecting when one polygon is
contained within or overlaps with another, for computing the
area enclosed by a polygon, for doing linear transformations of polygons,
for rendering polygons as
SVG, and other
similar operations.
The source code for Geopoly is included in the amalgamation.  However,
depending on configuration options and the particular version of SQLite
you are using, the Geopoly extension may or may not be enabled by default.
To ensure that Geopoly is enabled for your build, 
add the -DSQLITE_ENABLE_GEOPOLY=1 compile-time option.
Geopoly operates on "simple" polygons - that is, polygons for which
the boundary does not intersect itself.  Geopoly thus extends the capabilities
of the R-Tree extension which can only deal with rectangular areas.
On the other hand, the R-Tree extension is
able to handle between 1 and 5 coordinate dimensions, whereas Geopoly is restricted
to 2-dimensional shapes only.
Each polygon in the Geopoly module can be associated with an arbitrary
number of auxiliary data fields.
1.1. GeoJSON
The GeoJSON standard is syntax for
exchanging geospatial information using JSON.  GeoJSON is a rich standard
that can describe nearly any kind of geospatial content.
The Geopoly module only understands
a small subset of GeoJSON, but a critical subset.  
In particular, GeoJSON understands
the JSON array of vertexes that describes a simple polygon.
A polygon is defined by its vertexes.
Each vertex is a JSON array of two numeric values which are the
X and Y coordinates of the vertex.
A polygon is a JSON array of at least four of these vertexes, 
and hence is an array of arrays.
The first and last vertex in the array must be the same.
The polygon follows the right-hand rule:  When tracing a line from
one vertex to the next, the area to the right of the line is outside
of the polygon and the area to the left is inside the polygon.
In other words, the net rotation of the vertexes is counter-clockwise.
For example, the following JSON describes an isosceles triangle, sitting
on the X axis and with an area of 0.5:
&#91;&#91;0,0],&#91;1,0],&#91;0.5,1],&#91;0,0]]
A triangle has three vertexes, but the GeoJSON description of the triangle
has 4 vertexes because the first and last vertex are duplicates.
1.2. Binary storage format
Internally, Geopoly stores polygons in a binary format - an SQL BLOB.
Details of the binary format are given below.
All of the Geopoly interfaces are able to accept polygons in either the
GeoJSON format or in the binary format.
2. Using The Geopoly Extension
A geopoly table is created as follows:
CREATE VIRTUAL TABLE newtab USING geopoly(a,b,c);
The statement above creates a new geopoly table named "newtab".
Every geopoly table contains a built-in integer "rowid" column
and a "_shape" column that contains
the polygon associated with that row of the table.
The example above also defines three auxiliary data columns 
named "a", "b", and "c" that can store whatever additional
information the application needs to associate
with each polygon.  If there is no need to store auxiliary
information, the list of auxiliary columns can be omitted.
Store new polygons in the table using ordinary INSERT statements:
INSERT INTO newtab(_shape) VALUES('&#91;&#91;0,0],&#91;1,0],&#91;0.5,1],&#91;0,0]]');
UPDATE and DELETE statements work similarly.
2.1. Queries
To query the geopoly table using an indexed geospatial search, 
use one of the functions geopoly_overlap()
or geopoly_within() as a boolean function in the WHERE clause,
with the "_shape" column as the first argument to the function.
For example:
SELECT * FROM newtab WHERE geopoly_overlap(_shape, $query_polygon);
The previous example will return every row for which the _shape
overlaps the polygon in the $query_polygon parameter.  The
geopoly_within() function works similarly, but only returns rows for
which the _shape is completely contained within $query_polygon.
Queries (and also DELETE and UPDATE statements) in which the WHERE
clause contains a bare geopoly_overlap() or geopoly_within() function
make use of the underlying R*Tree data structures for a fast lookup that
only has to examine a subset of the rows in the table.  The number of
rows examined depends, of course, on the size of the $query_polygon.
Large $query_polygons will normally need to look at more rows than small
ones.
Queries against the rowid of a geopoly table are also very quick, even
for tables with a vast number of rows.
However, none of the auxiliary data columns are indexes, and so queries
against the auxiliary data columns will involve a full table scan.
3. Special Functions
The geopoly module defines several new SQL functions that are useful for
dealing with polygons.  All polygon arguments to these functions can be
either the GeoJSON format or the internal binary format.
3.1. The geopoly_overlap(P1,P2) Function
If P1 and P2 are both polygons, then the geopoly_overlap(P1,P2) function returns
a non-zero integer if there is any overlap between P1 and P2, or it returns
zero if P1 and P2 completely disjoint.
If either P1 or P2 is not a polygon, this routine returns NULL.
The geopoly_overlap(P1,P2) function is special in that the geopoly virtual
table knows how to use R*Tree indexes to optimize queries in which the 
WHERE clause uses geopoly_overlap() as a boolean function.  Only the
geopoly_overlap(P1,P2) and geopoly_within(P1,P2) functions have this
capability.
3.2. The geopoly_within(P1,P2) Function
If P1 and P2 are both polygons, then the geopoly_within(P1,P2) function returns
a non-zero integer if P1 is completely contained within P2, or it returns zero
if any part of P1 is outside of P2.  If P1 and P2 are the same polygon, this routine
returns non-zero.
If either P1 or P2 is not a polygon, this routine returns NULL.
The geopoly_within(P1,P2) function is special in that the geopoly virtual
table knows how to use R*Tree indexes to optimize queries in which the 
WHERE clause uses geopoly_within() as a boolean function.  Only the
geopoly_within(P1,P2) and geopoly_overlap(P1,P2) functions have this
capability.
3.3. The geopoly_area(P) Function
If P is a polygon, then geopoly_area(P) returns the area enclosed by
that polygon.  If P is not a polygon, geopoly_area(P) returns NULL.
3.4. The geopoly_blob(P) Function
If P is a polygon, then geopoly_blob(P) returns the binary encoding
of that polygon as a BLOB.
If P is not a polygon, geopoly_blob(P) returns NULL.
3.5. The geopoly_json(P) Function
If P is a polygon, then geopoly_json(P) returns the GeoJSON representation
of that polygon as a TEXT string.
If P is not a polygon, geopoly_json(P) returns NULL.
3.6. The geopoly_svg(P,...) Function
If P is a polygon, then geopoly_svg(P,...) returns a text string which is a
Scalable Vector Graphics (SVG)
representation of that polygon.  If there is more one argument, then second
and subsequent arguments are added as attributes to each SVG glyph.  For example:
SELECT geopoly_svg($polygon,'class="poly"','style="fill:blue;"');
If P is not a polygon, geopoly_svg(P,...) returns NULL.
Note that geopoly uses a traditional right-handed cartesian coordinate system
with the origin at the lower left, whereas SVG uses a left-handed coordinate
system with the origin at the upper left.  The geopoly_svg() routine makes no
attempt to transform the coordinate system, so the displayed images are shown
in mirror image and rotated.  If that is undesirable, the geopoly_xform() routine
can be used to transform the output from cartesian to SVG coordinates prior to
passing the polygons into geopoly_svg().
3.7. The geopoly_bbox(P) and geopoly_group_bbox(P) Functions
If P is a polygon, then geopoly_bbox(P) returns a new polygon that is
the smallest (axis-aligned) rectangle completely enclosing P.
If P is not a polygon, geopoly_bbox(P) returns NULL.
The geopoly_group_bbox(P) function is an aggregate version of geopoly_bbox(P).
The geopoly_group_bbox(P) function returns the smallest rectangle that will
enclose all P values seen during aggregation.
3.8. The geopoly_contains_point(P,X,Y) Function
If P is a polygon, then geopoly_contains_point(P,X,Y) returns a 
non-zero integer if and only
if the coordinate X,Y is inside or on the boundary of the polygon P.
If P is not a polygon, geopoly_contains_point(P,X,Y) returns NULL.
3.9. The geopoly_xform(P,A,B,C,D,E,F) Function
The geopoly_xform(P,A,B,C,D,E,F) function returns a new polygon that is an
affine transformation of the polygon P and where the transformation
is defined by values A,B,C,D,E,F. If P is not a valid polygon, this
routine returns NULL.
The transformation converts each vertex of the polygon according to the
following formula:
x1 = A*x0 + B*y0 + E
y1 = C*x0 + D*y0 + F
So, for example, to move a polygon by some amount DX, DY without changing
its shape, use:
geopoly_xform($polygon, 1, 0, 0, 1, $DX, $DY)
To rotate a polygon by R radians around the point 0, 0:
geopoly_xform($polygon, cos($R), sin($R), -sin($R), cos($R), 0, 0)
Note that a transformation that flips the polygon might cause the
order of vertexes to be reversed.  In other words, the transformation
might cause the vertexes to circulate in clockwise order instead of
counter-clockwise.  This can be corrected by sending the result
through the geopoly_ccw() function after transformation.
3.10. The geopoly_regular(X,Y,R,N) Function
The geopoly_regular(X,Y,R,N) function returns a convex, simple, regular,
equilateral, equiangular polygon with N sides, centered at X,Y, and with
a circumradius of R.  Or, if R is negative or if N is less than 3, the
function returns NULL.  The N value is capped at 1000 so that the routine
will never render a polygon with more than 1000 sides even if the N value
is larger than 1000.
As an example, the following graphic:
 3
 4
 5
 6
 7
 8
 10
 12
 16
 20
Was generated by this script:
SELECT '<svg width="600" height="300">';
WITH t1(x,y,n,color) AS (VALUES
   (100,100,3,'red'),
   (200,100,4,'orange'),
   (300,100,5,'green'),
   (400,100,6,'blue'),
   (500,100,7,'purple'),
   (100,200,8,'red'),
   (200,200,10,'orange'),
   (300,200,12,'green'),
   (400,200,16,'blue'),
   (500,200,20,'purple')
)
SELECT
   geopoly_svg(geopoly_regular(x,y,40,n),
        printf('style="fill:none;stroke:%s;stroke-width:2"',color))
   || printf(' <text x="%d" y="%d" alignment-baseline="central" text-anchor="middle">%d</text>',x,y+6,n)
  FROM t1;
SELECT '</svg>';
3.11. The geopoly_ccw(J) Function
The geopoly_ccw(J) function returns the polygon J with counter-clockwise (CCW) rotation.
RFC-7946 requires that polygons use CCW rotation.
But the spec also observes that many legacy GeoJSON files do not following the spec and
contain polygons with clockwise (CW) rotation.  The geopoly_ccw() function is useful for
applications that are reading legacy GeoJSON scripts.  If the input to geopoly_ccw() is
a correctly-formatted polygon, then no changes are made.  However, if the circulation of
the input polygon is backwards, then geopoly_ccw() reverses the circulation order so that
it conforms to the spec and so that it will work correctly with the Geopoly module.
4. Implementation Details
The geopoly module is an extension to the R-Tree extension.  Geopoly
uses the same underlying logic and shadow tables as the R-Tree extension.
Geopoly merely presents a different interface, and provides some extra logic
to compute polygon decoding, overlap, and containment.
4.1. Binary Encoding of Polygons
Geopoly stores all polygons internally using a binary format.  A binary
polygon consists of a 4-byte header following by an array of coordinate
pairs in which each dimension of each coordinate is a 32-bit floating point
number.
The first byte of the header is a flag byte.  The least significant bit
of the flag byte determines whether the coordinate pairs that follow the
header are stored big-endian or little-endian.  A value of 0 for the least
significant bit means big-endian and a value of 1 means little endian.
Other bits of the first byte in the header are reserved for future expansion.
The next three bytes in the header record the number of vertexes in the polygon
as a big-endian integer.  Thus there is an upper bound of about 16 million
vertexes per polygon.
Following the header is the array of coordinate pairs.  Each coordinate is
a 32-bit floating point number.  The use of 32-bit floating point values for
coordinates means that any point on the earth's surface can be mapped with
a resolution of approximately 2.5 meters.  Higher resolutions are of course
possible if the map is restricted to a single continent or country.
Note that the resolution of coordinates in the geopoly module is similar
in magnitude to daily movement of points on the earth's surface due to
tidal forces.
The list of coordinates in the binary format contains no redundancy.  
The last coordinate is not a repeat of the first as it is with GeoJSON.  
Hence, there is always one fewer coordinate pair in the binary representation of
a polygon compared to the GeoJSON representation.
4.2. Shadow Tables
The geopoly module is built on top of the R-Tree extension and uses the
same underlying shadow tables and algorithms.  For indexing purposes, each
polygon is represented in the shadow tables as a rectangular bounding box.
The underlying R-Tree implementation uses bounding boxes to limit the search
space.  Then the geoploy_overlap() and/or geopoly_within() routines further
refine the search to the exact answer.
This page was last updated on 2025-07-12 15:11:36Z 
