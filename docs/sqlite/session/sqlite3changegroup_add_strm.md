Streaming Versions of API functions.
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
Session Module C InterfaceStreaming Versions of API functions.int sqlite3changeset_apply_strm(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int (*xInput)(void *pIn, void *pData, int *pnData), /* Input function */
  void *pIn,                                          /* First arg for xInput */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    const char *zTab              /* Table name */
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx                      /* First argument passed to xConflict */
);
int sqlite3changeset_apply_v2_strm(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int (*xInput)(void *pIn, void *pData, int *pnData), /* Input function */
  void *pIn,                                          /* First arg for xInput */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    const char *zTab              /* Table name */
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx,                     /* First argument passed to xConflict */
  void **ppRebase, int *pnRebase,
  int flags
);
int sqlite3changeset_apply_v3_strm(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int (*xInput)(void *pIn, void *pData, int *pnData), /* Input function */
  void *pIn,                                          /* First arg for xInput */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    sqlite3_changeset_iter *p
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx,                     /* First argument passed to xConflict */
  void **ppRebase, int *pnRebase,
  int flags
);
int sqlite3changeset_concat_strm(
  int (*xInputA)(void *pIn, void *pData, int *pnData),
  void *pInA,
  int (*xInputB)(void *pIn, void *pData, int *pnData),
  void *pInB,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changeset_invert_strm(
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changeset_start_strm(
  sqlite3_changeset_iter **pp,
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn
);
int sqlite3changeset_start_v2_strm(
  sqlite3_changeset_iter **pp,
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn,
  int flags
);
int sqlite3session_changeset_strm(
  sqlite3_session *pSession,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3session_patchset_strm(
  sqlite3_session *pSession,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changegroup_add_strm(sqlite3_changegroup*, 
    int (*xInput)(void *pIn, void *pData, int *pnData),
    void *pIn
);
int sqlite3changegroup_output_strm(sqlite3_changegroup*,
    int (*xOutput)(void *pOut, const void *pData, int nData), 
    void *pOut
);
int sqlite3rebaser_rebase_strm(
  sqlite3_rebaser *pRebaser,
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
The six streaming API xxx_strm() functions serve similar purposes to the 
corresponding non-streaming API functions:
  Streaming functionNon-streaming equivalent
  sqlite3changeset_apply_strmsqlite3changeset_apply 
  sqlite3changeset_apply_strm_v2sqlite3changeset_apply_v2 
  sqlite3changeset_concat_strmsqlite3changeset_concat 
  sqlite3changeset_invert_strmsqlite3changeset_invert 
  sqlite3changeset_start_strmsqlite3changeset_start 
  sqlite3session_changeset_strmsqlite3session_changeset 
  sqlite3session_patchset_strmsqlite3session_patchset 
Non-streaming functions that accept changesets (or patchsets) as input
require that the entire changeset be stored in a single buffer in memory. 
Similarly, those that return a changeset or patchset do so by returning 
a pointer to a single large buffer allocated using sqlite3_malloc(). 
Normally this is convenient. However, if an application running in a 
low-memory environment is required to handle very large changesets, the
large contiguous memory allocations required can become onerous.
In order to avoid this problem, instead of a single large buffer, input
is passed to a streaming API functions by way of a callback function that
the sessions module invokes to incrementally request input data as it is
required. In all cases, a pair of API function parameters such as
       int nChangeset,
       void *pChangeset,
Is replaced by:
       int (*xInput)(void *pIn, void *pData, int *pnData),
       void *pIn,
Each time the xInput callback is invoked by the sessions module, the first
argument passed is a copy of the supplied pIn context pointer. The second 
argument, pData, points to a buffer (*pnData) bytes in size. Assuming no 
error occurs the xInput method should copy up to (*pnData) bytes of data 
into the buffer and set (*pnData) to the actual number of bytes copied 
before returning SQLITE_OK. If the input is completely exhausted, (*pnData) 
should be set to zero to indicate this. Or, if an error occurs, an SQLite 
error code should be returned. In all cases, if an xInput callback returns
an error, all processing is abandoned and the streaming API function
returns a copy of the error code to the caller.
In the case of sqlite3changeset_start_strm(), the xInput callback may be
invoked by the sessions module at any point during the lifetime of the
iterator. If such an xInput callback returns an error, the iterator enters
an error state, whereby all subsequent calls to iterator functions 
immediately fail with the same error code as returned by xInput.
Similarly, streaming API functions that return changesets (or patchsets)
return them in chunks by way of a callback function instead of via a
pointer to a single large buffer. In this case, a pair of parameters such
as:
       int *pnChangeset,
       void **ppChangeset,
Is replaced by:
       int (*xOutput)(void *pOut, const void *pData, int nData),
       void *pOut
The xOutput callback is invoked zero or more times to return data to
the application. The first parameter passed to each call is a copy of the
pOut pointer supplied by the application. The second parameter, pData,
points to a buffer nData bytes in size containing the chunk of output
data being returned. If the xOutput callback successfully processes the
supplied data, it should return SQLITE_OK to indicate success. Otherwise,
it should return some other SQLite error code. In this case processing
is immediately abandoned and the streaming API function returns a copy
of the xOutput error code to the application.
The sessions module never invokes an xOutput callback with the third 
parameter set to a value less than or equal to zero. Other than this,
no guarantees are made as to the size of the chunks of data returned.
See also lists of
  Objects,
  Constants, and
  Functions.
