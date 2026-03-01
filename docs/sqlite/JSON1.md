JSON Functions And Operators
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
JSON Functions And Operators
Table Of Contents
1. Overview
2. Compiling in JSON Support
3. Interface Overview
3.1. JSON arguments
3.2. JSONB
3.2.1. The JSONB format
3.2.2. Handling of malformed JSONB
3.3. PATH arguments
3.4. VALUE arguments
3.5. Compatibility
3.6. JSON5 Extensions
3.7. Performance Considerations
3.8. The JSON BLOB Input Bug
4. Function Details
4.1. The json() function
4.2. The jsonb() function
4.3. The json_array() function
4.4. The jsonb_array() function
4.5. The json_array_length() function
4.6. The json_error_position() function
4.7. The json_extract() function
4.8. The jsonb_extract() function
4.9. The -> and ->> operators
4.10. The json_insert(), json_replace, and json_set() functions
4.11. The jsonb_insert(), jsonb_replace, and jsonb_set() functions
4.12. The json_object() function
4.13. The jsonb_object() function
4.14. The json_patch() function
4.15. The jsonb_patch() function
4.16. The json_pretty() function
4.17. The json_remove() function
4.18. The jsonb_remove() function
4.19. The json_type() function
4.20. The json_valid() function
4.21. The json_quote() function
4.22. Array and object aggregate functions
4.23. Table valued functions for parsing JSON:
json_each(), jsonb_each(), json_tree(), and jsonb_tree()
4.23.1. Examples using json_each() and json_tree()
1. Overview
By default, SQLite supports thirty functions and two operators for
dealing with JSON values.  There are also four table-valued functions
that can be used to decompose a JSON string. All of the functions listed
below have the SQLITE_INNOCUOUS and SQLITE_DETERMINISTIC flags.
There are twenty-six scalar functions and operators:
json(json)
jsonb(json)
json_array(value1,value2,...)
jsonb_array(value1,value2,...)
json_array_length(json)json_array_length(json,path)
json_error_position(json)
json_extract(json,path,...)
jsonb_extract(json,path,...)
json -> path
json ->> path
json_insert(json,path,value,...)
jsonb_insert(json,path,value,...)
json_object(label1,value1,...)
jsonb_object(label1,value1,...)
json_patch(json1,json2)
jsonb_patch(json1,json2)
json_pretty(json)
json_remove(json,path,...)
jsonb_remove(json,path,...)
json_replace(json,path,value,...)
jsonb_replace(json,path,value,...)
json_set(json,path,value,...)
jsonb_set(json,path,value,...)
json_type(json)json_type(json,path)
json_valid(json)json_valid(json,flags)
json_quote(value)
There are four aggregate SQL functions:
json_group_array(value)
jsonb_group_array(value)
json_group_object(label,value)
jsonb_group_object(name,value)
The four table-valued functions are:
json_each(json)json_each(json,path)
json_tree(json)json_tree(json,path)
jsonb_each(json)jsonb_each(json,path)
jsonb_tree(json)jsonb_tree(json,path)
.jans {color: #050;}
.jex {color: #025;}
2. Compiling in JSON Support
The JSON functions and operators are built into SQLite by default,
as of SQLite version 3.38.0 (2022-02-22).  They can be omitted
by adding the -DSQLITE_OMIT_JSON compile-time option.  Prior to
version 3.38.0, the JSON functions were an extension that would only
be included in builds if the -DSQLITE_ENABLE_JSON1 compile-time option
was included.  In other words, the JSON functions went from being
opt-in with SQLite version 3.37.2 and earlier to opt-out with
SQLite version 3.38.0 and later.
3. Interface Overview
SQLite stores JSON as ordinary text.
Backwards compatibility constraints mean that SQLite is only able to
store values that are NULL, integers, floating-point numbers, text,
and BLOBs.  It is not possible to add a new "JSON" type.
3.1. JSON arguments
For functions that accept JSON as their first argument, that argument
can be a JSON object, array, number, string, or null.  SQLite numeric
values and NULL values are interpreted as JSON numbers and nulls, respectively.
SQLite text values can be understood as JSON objects, arrays, or strings.
If an SQLite text value that is not a well-formed JSON object, array, or
string is passed into a JSON function, that function will usually throw
an error.  (Exceptions to this rule are json_valid(),
json_quote(), and json_error_position().)
These routines understand all
rfc-8259 JSON syntax
and also JSON5 extensions.  JSON text
generated by these routines always strictly conforms to the
canonical JSON definition and does not contain any JSON5
or other extensions.  The ability to read and understand JSON5 was added in
version 3.42.0 (2023-05-16).
Prior versions of SQLite would only read canonical JSON.
3.2. JSONB
Beginning with version 3.45.0 (2024-01-15), SQLite allows its
internal "parse tree" representation of JSON to be stored on disk,
as a BLOB, in a format that we call "JSONB".  By storing SQLite's internal
binary representation of JSON directly in the database, applications
can bypass the overhead of parsing and rendering JSON when reading and
updating JSON values.  The internal JSONB format also uses slightly
less disk space then text JSON.
Any SQL function parameter that accepts text JSON as an input will also
accept a BLOB in the JSONB format.  The function will operate the
same in either case, except that it will run faster when
the input is JSONB, since it does not need to run the JSON parser.
Most SQL functions that return JSON text have a corresponding function
that returns the equivalent JSONB.  The functions that return JSON
in the text format begin with "json_" and functions that
return the binary JSONB format begin with "jsonb_".
3.2.1. The JSONB format
JSONB is a binary representation of JSON used by SQLite and
is intended for internal use by SQLite only.  Applications
should not use JSONB outside of SQLite nor try to reverse-engineer the
JSONB format.
The "JSONB" name is inspired by PostgreSQL, but the
on-disk format for SQLite's JSONB is not the same as PostgreSQL's.
The two formats have the same name, but are not binary compatible.
The PostgreSQL JSONB format claims to offer O(1)
lookup of elements in objects and arrays.  SQLite's JSONB format makes no
such claim.  SQLite's JSONB has O(N) time complexity for
most operations in SQLite, just like text JSON.  The advantage of JSONB in
SQLite is that it is smaller and faster than text JSON - potentially several
times faster. There is space in the
on-disk JSONB format to add enhancements and future versions of SQLite might
include options to provide O(1) lookup of elements in JSONB, but no such
capability is currently available.
3.2.2. Handling of malformed JSONB
The JSONB that is generated by SQLite will always be well-formed.  If you
follow recommended practice and
treat JSONB as an opaque BLOB, then you will not have any problems.  But
JSONB is just a BLOB, so a mischievous programmer could devise BLOBs
that are similar to JSONB but that are technically malformed.  When
misformatted JSONB is feed into JSON functions, any of the following
might happen:
The SQL statement might abort with a "malformed JSON" error.
The correct answer might be returned, if the malformed parts of
the JSONB blob do not impact the answer.
A goofy or nonsensical answer might be returned.
The way in which SQLite handles invalid JSONB might change
from one version of SQLite to the next.  The system follows
the garbage-in/garbage-out rule:  If you feed the JSON functions invalid
JSONB, you get back an invalid answer.  If you are in doubt about the
validity of our JSONB, use the json_valid() function to verify it.
We do make this one promise:
Malformed JSONB will never cause a memory
error or similar problem that might lead to a vulnerability.
Invalid JSONB might lead to crazy answers,
or it might cause queries to abort, but it won't cause a crash.
3.3. PATH arguments
For functions that accept PATH arguments, that PATH must be well-formed or
else the function will throw an error.
A well-formed PATH is a text value that begins with exactly one
'$' character followed by zero or more instances
of ".objectlabel" or "&#91;arrayindex&#93;".
The arrayindex is usually a non-negative integer N.  In
that case, the array element selected is the N-th element
of the array, starting with zero on the left.
The arrayindex can also be of the form "#-N"
in which case the element selected is the N-th from the
right.  The last element of the array is "#-1".  Think of
the "#" characters as the "number of elements in the array".  Then
the expression "#-1" evaluates to the integer that corresponds to 
the last entry in the array.  It is sometimes useful for the array
index to be just the # character, for example when appending
a value to an existing JSON array:
json_set('[0,1,2]','$[#]','new')
&rarr; '[0,1,2,"new"]'
3.4. VALUE arguments
For functions that accept "value" arguments (also shown as
"value1" and "value2"),
those arguments are usually understood
to be literal strings that are quoted and become JSON string values
in the result.  Even if the input value strings look like 
well-formed JSON, they are still interpreted as literal strings in the
result.
However, if a value argument comes directly from the result of another
JSON function or from the -> operator (but not the ->> operator),
then the argument is understood to be actual JSON and
the complete JSON is inserted rather than a quoted string.
For example, in the following call to json_object(), the value
argument looks like a well-formed JSON array.  However, because it is just
ordinary SQL text, it is interpreted as a literal string and added to the
result as a quoted string:
json_object('ex','[52,3.14159]')
&rarr; '{"ex":"[52,3.14159]"}'
json_object('ex',('[52,3.14159]'->>'$'))
&rarr; '{"ex":"[52,3.14159]"}'
But if the value argument in the outer json_object() call is the
result of another JSON function like json() or json_array(), then
the value is understood to be actual JSON and is inserted as such:
json_object('ex',json('[52,3.14159]'))
&rarr; '{"ex":[52,3.14159]}'
json_object('ex',json_array(52,3.14159))
&rarr; '{"ex":[52,3.14159]}'
json_object('ex','[52,3.14159]'->'$')
&rarr; '{"ex":[52,3.14159]}'
To be clear: "json" arguments are always interpreted as JSON
regardless of where the value for that argument comes from.  But
"value" arguments are only interpreted as JSON if those arguments
come directly from another JSON function or the -> operator.
Within JSON value arguments interpreted as JSON strings, Unicode escape
sequences are not treated as equivalent to the characters or escaped
control characters represented by the expressed Unicode code point.
Such escape sequences are not translated or specially treated; they
are treated as plain text by SQLite's JSON functions.
3.5. Compatibility
The current implementation of this JSON library uses a recursive descent
parser.  In order to avoid using excess stack space, any JSON input that has
more than 1000 levels of nesting is considered invalid.   Limits on nesting
depth are allowed for compatible implementations of JSON by
RFC-8259 section 9.
3.6. JSON5 Extensions
Beginning in version 3.42.0 (2023-05-16), these routines will
read and interpret input JSON text that includes
JSON5 extensions.  However, JSON text generated
by these routines will always be strictly conforming to the 
canonical definition of JSON.
Here is a synopsis of JSON5 extensions (adapted from the
JSON5 specification):
 Object keys may be unquoted identifiers.
 Objects may have a single trailing comma.
 Arrays may have a single trailing comma.
 Strings may be single quoted.
 Strings may span multiple lines by escaping new line characters.
 Strings may include new character escapes.
 Numbers may be hexadecimal.
 Numbers may have a leading or trailing decimal point.
 Numbers may be "Infinity", "-Infinity", and "NaN".
 Numbers may begin with an explicit plus sign.
 Single (//...) and multi-line (/*...*/) comments are allowed.
 Additional white space characters are allowed.
To convert string X from JSON5 into canonical JSON, invoke
"json(X)".  The output of the "json()" function will be canonical
JSON regardless of any JSON5 extensions that are present in the input.
For backwards compatibility, the json_valid(X) function without a
"flags" argument continues
to report false for inputs that are not canonical JSON, even if the
input is JSON5 that the function is able to understand.  To determine
whether or not an input string is valid JSON5, include the 0x02 bit
in the "flags" argument to json_valid:  "json_valid(X,2)".
These routines understand all of JSON5, plus a little more.
SQLite extends the JSON5 syntax in these two ways:
Strict JSON5 requires that
unquoted object keys must be ECMAScript 5.1 IdentifierNames.  But large
unicode tables and lots of code is required in order to determine whether or
not a key is an ECMAScript 5.1 IdentifierName.  For this reason,
SQLite allows object keys to include any unicode characters
greater than U+007f that are not whitespace characters.  This relaxed
definition of "identifier" greatly simplifies the implementation and allows
the JSON parser to be smaller and run faster.
JSON5 allows floating-point infinities to be expressed as
"Infinity", "-Infinity", or "+Infinity"
in exactly that case - the initial "I" is capitalized and all other
characters are lower case.  SQLite also allows the abbreviation "Inf"
to be used in place of "Infinity" and it allows both keywords
to appear in any combination of upper and lower case letters.
Similarly,
JSON5 allows "NaN" for not-a-number.  SQLite extends this to also allow
"QNaN" and "SNaN" in any combination of upper and lower case letters.
Note that SQLite interprets NaN, QNaN, and SNaN as just an alternative
spellings for "null".
This extension has been added because (we are told) there exists a lot
of JSON in the wild that includes these non-standard representations
for infinity and not-a-number.
3.7. Performance Considerations
Most JSON functions do their internal processing using JSONB.  So if the
input is text, they first must translate the input text into JSONB.
If the input is already in the JSONB format, no translation is needed,
that step can be skipped, and performance is faster.
For that reason,
when an argument to one JSON function is supplied by another
JSON function, it is usually more efficient to use the "jsonb_"
variant for the function used as the argument.  
  ... json_insert(A,'$.b',json(C)) ...
    &larr; Less efficient.
  ... json_insert(A,'$.b',jsonb(C)) ...
    &larr; More efficient.
The aggregate JSON SQL functions are an exception to this rule.  Those
functions all do their processing using text instead of JSONB.  So for the
aggregate JSON SQL functions, it is more efficient for the arguments
to be supplied using "json_" functions than "jsonb_"
functions.
  ... json_group_array(json(A))) ...
    &larr; More efficient.
  ... json_group_array(jsonb(A))) ...
    &larr; Less efficient.
3.8. The JSON BLOB Input Bug
If a JSON input is a BLOB that is not JSONB and that looks like
text JSON when cast to text, then it is accepted as text JSON.
This is actually a long-standing bug in the original implementation
that the SQLite developers were unaware of.  The documentation stated
that a BLOB input to a JSON function should raise an error.  But in the
actual implementation, the input would be accepted as long
as the BLOB content was a valid JSON string in the text encoding of
the database.
This JSON BLOB input bug was accidentally fixed when the JSON routines
were reimplemented for the 3.45.0 release (2024-01-15).
That caused breakage in applications that had come to depend on the old
behavior.  (In defense of those applications:  they were often lured into
using BLOBs as JSON by the readfile() SQL function
available in the CLI.  Readfile() was used to read JSON from disk files,
but readfile() returns a BLOB.  And that worked for them, so why not just
do it?)
For backwards bug-compatibility,
the (formerly incorrect) legacy behavior of interpreting BLOBs as text JSON
if no other interpretation works
is hereby documented and is officially supported in
version 3.45.1 (2024-01-30) and all subsequent releases.
Beware, however, that there exist BLOBs which are both valid JSONB and
which are valid JSON after being cast to text.  For example, the
BLOB x'33343535' is valid JSONB, specifically the integer 456,
and when cast to text is the valid JSON integer literal 3456.  Therefore,
if you have legacy databases in which you store JSON text as a BLOB
and you want to continue using those database with SQLite 3.45.0 or later,
you will do well to run an UPDATE on that legacy JSON text so that it
is actually stored as TEXT and not as a BLOB.
4. Function Details
The following sections provide additional detail on the operation of
the various JSON functions and operators:
4.1. The json() function
The json(X) function verifies that its argument X is a valid
JSON string or JSONB blob and returns a minified version of that JSON string
with all unnecessary whitespace removed.  If X is not a well-formed
JSON string or JSONB blob, then this routine throws an error.
If the input is JSON5 text, then it is converted into canonical
RFC-8259 text prior to being returned.
If the argument X to json(X) contains JSON objects with duplicate
labels, then it is undefined whether or not the duplicates are
preserved.  The current implementation preserves duplicates.
However, future enhancements
to this routine may choose to silently remove duplicates.
Example:
json(' { "this" : "is", "a": [ "test" ] } ')
&rarr; '{"this":"is","a":["test"]}'
4.2. The jsonb() function
The jsonb(X) function returns the binary JSONB representation
of the JSON provided as argument X.  An error is raised if X is
TEXT that does not have valid JSON syntax.
If X is a BLOB and appears to be JSONB,
then this routine simply returns a copy of X.
Only the outer-most element of the JSONB input is examined, however.
The deep structure of the JSONB is not validated.
4.3. The json_array() function
The json_array() SQL function accepts zero or more arguments and
returns a well-formed JSON array that is composed from those arguments.
If any argument to json_array() is a BLOB then an error is thrown.
An argument with SQL type TEXT is normally converted into a quoted 
JSON string.  However, if the argument is the output from another json1
function, then it is stored as JSON.  This allows calls to json_array()
and json_object() to be nested.  The json() function can also
be used to force strings to be recognized as JSON.
Examples:
json_array(1,2,'3',4)
&rarr; '[1,2,"3",4]'
json_array('[1,2]')
&rarr; '["[1,2]"]'
json_array(json_array(1,2))
&rarr; '[[1,2]]'
json_array(1,null,'3','[4,5]','{"six":7.7}')
&rarr; '[1,null,"3","[4,5]","{\"six\":7.7}"]'
json_array(1,null,'3',json('[4,5]'),json('{"six":7.7}'))
&rarr; '[1,null,"3",[4,5],{"six":7.7}]'
4.4. The jsonb_array() function
The jsonb_array() SQL function works just like the json_array()
function except that it returns the constructed JSON array in the
SQLite's private JSONB format rather than in the standard
RFC 8259 text format.
4.5. The json_array_length() function
The json_array_length(X) function returns the number of elements
in the JSON array X, or 0 if X is some kind of JSON value other
than an array.  The json_array_length(X,P) locates the array at path P
within X and returns the length of that array, or 0 if path P locates
an element in X that is not a JSON array, and NULL if path P does not
locate any element of X.  Errors are thrown if either X is not 
well-formed JSON or if P is not a well-formed path.
Examples:
json_array_length('[1,2,3,4]')
&rarr; 4
json_array_length('[1,2,3,4]', '$')
&rarr; 4
json_array_length('[1,2,3,4]', '$[2]')
&rarr; 0
json_array_length('{"one":[1,2,3]}')
&rarr; 0
json_array_length('{"one":[1,2,3]}', '$.one')
&rarr; 3
json_array_length('{"one":[1,2,3]}', '$.two')
&rarr; NULL
4.6. The json_error_position() function
The json_error_position(X) function returns 0 if the input X is a
well-formed JSON or JSON5 string.  If the input X contains one or more
syntax errors, then this function returns the character position of the
first syntax error.  The left-most character is position 1.
If the input X is a BLOB, then this routine returns 0 if X is
a well-formed JSONB blob.  If the return value is positive, then it
