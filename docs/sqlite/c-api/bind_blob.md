Binding Values To Prepared Statements
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
SQLite C Interface
Binding Values To Prepared Statements
int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
int sqlite3_bind_blob64(sqlite3_stmt*, int, const void*, sqlite3_uint64,
                        void(*)(void*));
int sqlite3_bind_double(sqlite3_stmt*, int, double);
int sqlite3_bind_int(sqlite3_stmt*, int, int);
int sqlite3_bind_int64(sqlite3_stmt*, int, sqlite3_int64);
int sqlite3_bind_null(sqlite3_stmt*, int);
int sqlite3_bind_text(sqlite3_stmt*,int,const char*,int,void(*)(void*));
int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int, void(*)(void*));
int sqlite3_bind_text64(sqlite3_stmt*, int, const char*, sqlite3_uint64,
                         void(*)(void*), unsigned char encoding);
int sqlite3_bind_value(sqlite3_stmt*, int, const sqlite3_value*);
int sqlite3_bind_pointer(sqlite3_stmt*, int, void*, const char*,void(*)(void*));
int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int n);
int sqlite3_bind_zeroblob64(sqlite3_stmt*, int, sqlite3_uint64);
In the SQL statement text input to sqlite3_prepare_v2() and its variants,
literals may be replaced by a parameter that matches one of the following
templates:
  ?
  ?NNN
  :VVV
  @VVV
  $VVV
In the templates above, NNN represents an integer literal,
and VVV represents an alphanumeric identifier.  The values of these
parameters (also called "host parameter names" or "SQL parameters")
can be set using the sqlite3_bind_*() routines defined here.
The first argument to the sqlite3_bind_*() routines is always
a pointer to the sqlite3_stmt object returned from
sqlite3_prepare_v2() or its variants.
The second argument is the index of the SQL parameter to be set.
The leftmost SQL parameter has an index of 1.  When the same named
SQL parameter is used more than once, second and subsequent
occurrences have the same index as the first occurrence.
The index for named parameters can be looked up using the
sqlite3_bind_parameter_index() API if desired.  The index
for "?NNN" parameters is the value of NNN.
The NNN value must be between 1 and the sqlite3_limit()
parameter SQLITE_LIMIT_VARIABLE_NUMBER (default value: 32766).
The third argument is the value to bind to the parameter.
If the third parameter to sqlite3_bind_text() or sqlite3_bind_text16()
or sqlite3_bind_blob() is a NULL pointer then the fourth parameter
is ignored and the end result is the same as sqlite3_bind_null().
If the third parameter to sqlite3_bind_text() is not NULL, then
it should be a pointer to well-formed UTF8 text.
If the third parameter to sqlite3_bind_text16() is not NULL, then
it should be a pointer to well-formed UTF16 text.
If the third parameter to sqlite3_bind_text64() is not NULL, then
it should be a pointer to a well-formed unicode string that is
either UTF8 if the sixth parameter is SQLITE_UTF8, or UTF16
otherwise.
 The byte-order of
UTF16 input text is determined by the byte-order mark (BOM, U+FEFF)
found in the first character, which is removed, or in the absence of a BOM
the byte order is the native byte order of the host
machine for sqlite3_bind_text16() or the byte order specified in
the 6th parameter for sqlite3_bind_text64().
If UTF16 input text contains invalid unicode
characters, then SQLite might change those invalid characters
into the unicode replacement character: U+FFFD.
In those routines that have a fourth argument, its value is the
number of bytes in the parameter.  To be clear: the value is the
number of bytes in the value, not the number of characters.
If the fourth parameter to sqlite3_bind_text() or sqlite3_bind_text16()
is negative, then the length of the string is
the number of bytes up to the first zero terminator.
If the fourth parameter to sqlite3_bind_blob() is negative, then
the behavior is undefined.
If a non-negative fourth parameter is provided to sqlite3_bind_text()
or sqlite3_bind_text16() or sqlite3_bind_text64() then
that parameter must be the byte offset
where the NUL terminator would occur assuming the string were NUL
terminated.  If any NUL characters occur at byte offsets less than
the value of the fourth parameter then the resulting string value will
contain embedded NULs.  The result of expressions involving strings
with embedded NULs is undefined.
The fifth argument to the BLOB and string binding interfaces controls
or indicates the lifetime of the object referenced by the third parameter.
These three options exist:
 (1) A destructor to dispose of the BLOB or string after SQLite has finished
with it may be passed. It is called to dispose of the BLOB or string even
if the call to the bind API fails, except the destructor is not called if
the third parameter is a NULL pointer or the fourth parameter is negative.
 (2) The special constant, SQLITE_STATIC, may be passed to indicate that
the application remains responsible for disposing of the object. In this
case, the object and the provided pointer to it must remain valid until
either the prepared statement is finalized or the same SQL parameter is
bound to something else, whichever occurs sooner.
 (3) The constant, SQLITE_TRANSIENT, may be passed to indicate that the
object is to be copied prior to the return from sqlite3_bind_*(). The
object and pointer to it must remain valid until then. SQLite will then
manage the lifetime of its private copy.
The sixth argument to sqlite3_bind_text64() must be one of
SQLITE_UTF8, SQLITE_UTF16, SQLITE_UTF16BE, or SQLITE_UTF16LE
to specify the encoding of the text in the third parameter.  If
the sixth argument to sqlite3_bind_text64() is not one of the
allowed values shown above, or if the text encoding is different
from the encoding specified by the sixth parameter, then the behavior
is undefined.
The sqlite3_bind_zeroblob() routine binds a BLOB of length N that
is filled with zeroes.  A zeroblob uses a fixed amount of memory
(just an integer to hold its size) while it is being processed.
Zeroblobs are intended to serve as placeholders for BLOBs whose
content is later written using
incremental BLOB I/O routines.
A negative value for the zeroblob results in a zero-length BLOB.
The sqlite3_bind_pointer(S,I,P,T,D) routine causes the I-th parameter in
prepared statement S to have an SQL value of NULL, but to also be
associated with the pointer P of type T.  D is either a NULL pointer or
a pointer to a destructor function for P. SQLite will invoke the
destructor D with a single argument of P when it is finished using
P, even if the call to sqlite3_bind_pointer() fails.  Due to a
historical design quirk, results are undefined if D is
SQLITE_TRANSIENT. The T parameter should be a static string,
preferably a string literal. The sqlite3_bind_pointer() routine is
part of the pointer passing interface added for SQLite 3.20.0.
If any of the sqlite3_bind_*() routines are called with a NULL pointer
for the prepared statement or with a prepared statement for which
sqlite3_step() has been called more recently than sqlite3_reset(),
then the call will return SQLITE_MISUSE.  If any sqlite3_bind_()
routine is passed a prepared statement that has been finalized, the
result is undefined and probably harmful.
Bindings are not cleared by the sqlite3_reset() routine.
Unbound parameters are interpreted as NULL.
The sqlite3_bind_* routines return SQLITE_OK on success or an
error code if anything goes wrong.
SQLITE_TOOBIG might be returned if the size of a string or BLOB
exceeds limits imposed by sqlite3_limit(SQLITE_LIMIT_LENGTH) or
SQLITE_MAX_LENGTH.
SQLITE_RANGE is returned if the parameter
index is out of range.  SQLITE_NOMEM is returned if malloc() fails.
See also: sqlite3_bind_parameter_count(),
sqlite3_bind_parameter_name(), and sqlite3_bind_parameter_index().
See also lists of
  Objects,
  Constants, and
  Functions.
