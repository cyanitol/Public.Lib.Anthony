Memory Allocation Subsystem
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
Memory Allocation Subsystem
void *sqlite3_malloc(int);
void *sqlite3_malloc64(sqlite3_uint64);
void *sqlite3_realloc(void*, int);
void *sqlite3_realloc64(void*, sqlite3_uint64);
void sqlite3_free(void*);
sqlite3_uint64 sqlite3_msize(void*);
The SQLite core uses these three routines for all of its own
internal memory allocation needs. "Core" in the previous sentence
does not include operating-system specific VFS implementation.  The
Windows VFS uses native malloc() and free() for some operations.
The sqlite3_malloc() routine returns a pointer to a block
of memory at least N bytes in length, where N is the parameter.
If sqlite3_malloc() is unable to obtain sufficient free
memory, it returns a NULL pointer.  If the parameter N to
sqlite3_malloc() is zero or negative then sqlite3_malloc() returns
a NULL pointer.
The sqlite3_malloc64(N) routine works just like
sqlite3_malloc(N) except that N is an unsigned 64-bit integer instead
of a signed 32-bit integer.
Calling sqlite3_free() with a pointer previously returned
by sqlite3_malloc() or sqlite3_realloc() releases that memory so
that it might be reused.  The sqlite3_free() routine is
a no-op if it is called with a NULL pointer.  Passing a NULL pointer
to sqlite3_free() is harmless.  After being freed, memory
should neither be read nor written.  Even reading previously freed
memory might result in a segmentation fault or other severe error.
Memory corruption, a segmentation fault, or other severe error
might result if sqlite3_free() is called with a non-NULL pointer that
was not obtained from sqlite3_malloc() or sqlite3_realloc().
The sqlite3_realloc(X,N) interface attempts to resize a
prior memory allocation X to be at least N bytes.
If the X parameter to sqlite3_realloc(X,N)
is a NULL pointer then its behavior is identical to calling
sqlite3_malloc(N).
If the N parameter to sqlite3_realloc(X,N) is zero or
negative then the behavior is exactly the same as calling
sqlite3_free(X).
sqlite3_realloc(X,N) returns a pointer to a memory allocation
of at least N bytes in size or NULL if insufficient memory is available.
If M is the size of the prior allocation, then min(N,M) bytes of the
prior allocation are copied into the beginning of the buffer returned
by sqlite3_realloc(X,N) and the prior allocation is freed.
If sqlite3_realloc(X,N) returns NULL and N is positive, then the
prior allocation is not freed.
The sqlite3_realloc64(X,N) interface works the same as
sqlite3_realloc(X,N) except that N is a 64-bit unsigned integer instead
of a 32-bit signed integer.
If X is a memory allocation previously obtained from sqlite3_malloc(),
sqlite3_malloc64(), sqlite3_realloc(), or sqlite3_realloc64(), then
sqlite3_msize(X) returns the size of that memory allocation in bytes.
The value returned by sqlite3_msize(X) might be larger than the number
of bytes requested when X was allocated.  If X is a NULL pointer then
sqlite3_msize(X) returns zero.  If X points to something that is not
the beginning of memory allocation, or if it points to a formerly
valid memory allocation that has now been freed, then the behavior
of sqlite3_msize(X) is undefined and possibly harmful.
The memory returned by sqlite3_malloc(), sqlite3_realloc(),
sqlite3_malloc64(), and sqlite3_realloc64()
is always aligned to at least an 8 byte boundary, or to a
4 byte boundary if the SQLITE_4_BYTE_ALIGNED_MALLOC compile-time
option is used.
The pointer arguments to sqlite3_free() and sqlite3_realloc()
must be either NULL or else pointers obtained from a prior
invocation of sqlite3_malloc() or sqlite3_realloc() that have
not yet been released.
The application must not read or write any part of
a block of memory after it has been released using
sqlite3_free() or sqlite3_realloc().
See also lists of
  Objects,
  Constants, and
  Functions.
