Floating Point Numbers
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
Floating Point Numbers
Table Of Contents
1. How SQLite Stores Numbers
1.1. Floating-Point Accuracy
1.2. Floating Point Numbers
1.2.1. Unrepresentable numbers
1.2.2. Is it close enough?
2. Extensions For Dealing With Floating Point Numbers
2.1. The ieee754.c Extension
2.1.1. The ieee754() function
2.1.2. The ieee754_mantissa() and ieee754_exponent() functions
2.1.3. The ieee754_from_blob() and ieee754_to_blob() functions
2.2. The decimal.c Extension
2.2.1. The decimal_add(A,B), decimal_sub(A,B), and decimal_mul(A,B) functions
2.2.2. The decimal_pow2(N) function
2.2.3. The decimal(X) and decimal_exp(X) functions
2.2.4. The decimal_cmp(X) function
2.2.5. The decimal_sum(X) aggregate function
2.2.6. The decimal collating sequence
1. How SQLite Stores Numbers
SQLite stores integer values in the 64-bit 
twos-complement
format&sup1.
This gives a storage range of -9223372036854775808 to +9223372036854775807,
inclusive.  Integers within this range are exact.
So-called "REAL" or floating point values are stored in the
IEEE 754
Binary-64
format&sup1.
This gives a range of positive values between approximately
1.7976931348623157e+308 and 4.9406564584124654e-324 with an equivalent
range of negative values.  A binary64 can also be 0.0 (and -0.0), positive
and negative infinity and "NaN" or "Not-a-Number".  Floating point
values are approximate.
Pay close attention to the last sentence in the previous paragraph:
Floating point values are approximate.
If you need an exact answer, you should not use binary64 floating-point
values, in SQLite or in any other product.  This is not an SQLite limitation.
It is a mathematical limitation inherent in the design of floating-point numbers.
&mdash;&sup1;
Exception:  The R-Tree extension stores information as 32-bit floating
point or integer values.
1.1. Floating-Point Accuracy
SQLite promises to preserve the 15 most significant digits of a floating
point value.  However, it makes no guarantees about the accuracy of
computations on floating point values, as no such guarantees are possible.
Performing math on floating-point values introduces error.
For example, consider what happens if you attempt to subtract two floating-point
numbers of similar magnitude:
1152693165.1106291898
-1152693165.1106280772
0.0000011126
The result shown above (0.0000011126) is the correct answer.  But if you
do this computation using binary64 floating-point, the answer you get is
0.00000095367431640625 - an error of about 14%.  If you do many similar
computations as part of your program, the errors add up so that your final
result might be completely meaningless.
The error arises because only about the first 15 significant digits of
each number are stored accurately, and the first difference between the two numbers
being subtracted is in the 16th digit.  
1.2. Floating Point Numbers
The binary64 floating-point format uses 64 bits per number.  Hence there
are 1.845e+19 different possible floating point values.  On the other hand
there are infinitely many real numbers in the range of 
1.7977e+308 and 4.9407e-324.  It follows then that binary64 cannot possibly
represent all possible real numbers within that range.  Approximations are
required.
An IEEE 754 floating-point value is an integer multiplied by a power
of two:
M &times 2E
The M value is the "mantissa" and E is the "exponent".  Both
M and E are integers.
For Binary64, M is a 53-bit integer and E is an 11-bit integer that is
offset so that represents a range of values between -1074 and +972, inclusive.
(NB:  The usual description of IEEE 754 is more complex, and it is important
to understand the added complexity if you really want to appreciate the details,
merits, and limitations of IEEE 754.  However, the integer description shown
here, while not exactly right, is easier to understand and is sufficient for
the purposes of this article.)
1.2.1. Unrepresentable numbers
Not every decimal number with fewer than 16 significant digits can be
represented exactly as a binary64 number.  In fact, most decimal numbers
with digits to the right of the decimal point lack an exact binary64
equivalent.  For example, if you have a database column that is intended
to hold an item price in dollars and cents, the only cents value that
can be exactly represented are 0.00, 0.25, 0.50, and 0.75.  Any other
numbers to the right of the decimal point result in an approximation.
If you provide a "price" value of 47.49, that number will be represented
in binary64 as:
6683623321994527 &times; 2-47
Which works out to be:
47.49000000000000198951966012828052043914794921875
That number is very close to 47.49, but it is not exact.  It is a little
too big.  If we reduce M by one to 6683623321994526 so that we have the
next smaller possible binary64 value, we get:
47.4899999999999948840923025272786617279052734375
This second number is too small.
The first number is closer to the desired value of 47.49, so that is the
one that gets used.  But it is not exact.  Most decimal values work this
way in IEEE 754.  Remember the key point we made above:
Floating point values are approximate.
If you remember nothing else about floating-point values, 
please don't forget this one key idea.
1.2.2. Is it close enough?
The precision provided by IEEE 754 Binary64 is sufficient for most computations.
For example, if "47.49" represents a price and inflation is running
at 2% per year, then the price is going up by about 0.0000000301 dollars per
second.  The error in the recorded value of 47.49 represents about 66 nanoseconds
worth of inflation.  So if the 47.49 price is exact
when you enter it, then the effects of inflation will cause the true value to
exactly equal the value actually stored
(47.4900000000000019895196601282805204391479492187) in less than 
one ten-millionth of a second.
Surely that level of precision is sufficient for most purposes?
2. Extensions For Dealing With Floating Point Numbers
2.1. The ieee754.c Extension
The ieee754 extension converts a floating point number between its
binary64 representation and the M&times;2E format.
In other words in the expression:
F = M &times 2E
The ieee754 extension converts between F and (M,E) and back again.
The ieee754 extension is not part of the amalgamation, but it is included
by default in the CLI.  If you want to include the ieee754 extension in your
application, you will need to compile and load it separately.
2.1.1. The ieee754() function
The ieee754(F) SQL function takes a single floating-point argument
as its input and returns a string that looks like this:
'ieee754(M,E)'
Except that the M and E are replaced by the mantissa and exponent of the
floating point number.  For example:
sqlite> .mode box
sqlite> SELECT ieee754(47.49) AS x;
┌───────────────────────────────┐
│               x               │
├───────────────────────────────┤
│ ieee754(6683623321994527,-47) │
└───────────────────────────────┘
Going in the other direction, the 2-argument version of ieee754() takes
the M and E values and converts them into the corresponding F value:
sqlite> select ieee754(6683623321994527,-47) as x;
┌───────┐
│   x   │
├───────┤
│ 47.49 │
└───────┘
2.1.2. The ieee754_mantissa() and ieee754_exponent() functions
The text output of the one-argument form of ieee754() is great for human
readability, but it is awkward to use as part of a larger expression.  Hence
the ieee754_mantissa() and ieee754_exponent() routines were added to return
the M and E values corresponding to their single argument F
value.
For example:
sqlite> .mode box
sqlite> SELECT ieee754_mantissa(47.49) AS M, ieee754_exponent(47.49) AS E;
┌──────────────────┬─────┐
│        M         │  E  │
├──────────────────┼─────┤
│ 6683623321994527 │ -47 │
└──────────────────┴─────┘
2.1.3. The ieee754_from_blob() and ieee754_to_blob() functions
The ieee754_to_blob(F) SQL function converts the floating point number F
into an 8-byte BLOB that is the big-endian binary64 encoding of that number.
The ieee754_from_blob(B) function goes the other way, converting an 8-byte
blob into the floating-point value that the binary64 encoding represents.
So, for example, if you read
on
Wikipedia that the encoding for the minimum positive binary64 value is
0x0000000000000001, then you can find the corresponding floating point value
like this:
sqlite> .mode box
sqlite> SELECT ieee754_from_blob(x'0000000000000001') AS F;
┌───────────────────────┐
│           F           │
├───────────────────────┤
│ 4.94065645841247e-324 │
└───────────────────────┘
Or go the other way:
sqlite> .mode box
sqlite> SELECT quote(ieee754_to_blob(4.94065645841247e-324)) AS binary64;
┌─────────────────────┐
│      binary64       │
├─────────────────────┤
│ X'0000000000000001' │
└─────────────────────┘
2.2. The decimal.c Extension
The decimal extension provides arbitrary-precision decimal arithmetic on
numbers stored as text strings.  Because the numbers are stored to arbitrary
precision and as text, no approximations are needed.  Computations can be
done exactly.
The decimal extension is not (currently) part of the SQLite amalgamation.
However, it is included in the CLI.  The source code to this extension
can be found at
ext/misc/decimal.c.
The decimal extension supports the following SQL functions and collating sequences:
2.2.1. The decimal_add(A,B), decimal_sub(A,B), and decimal_mul(A,B) functions
These functions compute the sum, difference, and product, respectively, of
the two inputs A and B and return the result as text.  The inputs can be either
decimal text or numeric values.  Numeric inputs are converted into decimal
text before the computation is performed.
There is no "decimal_div(A,B)" function because division does not always
have a finite-length decimal result.
2.2.2. The decimal_pow2(N) function
The decimal_pow2(N) function computes the exact decimal representation of
the N-th power of 2.  N must be an integer between -20000 and +20000.
This function can be slow and can use a lot of memory for large values of N.
2.2.3. The decimal(X) and decimal_exp(X) functions
The decimal(X) and decimal_exp(X) generate a decimal representation for input X.
The input X can be an integer or a floating point number, or a text decimal.
The decimal_exp(X) function returns the result in exponential notation (with a "e+NN"
at the end) and decimal(X) returns a pure decimal (without the "e+NN").
If the input
X is a floating point value, it is expanded to its exact decimal equivalent.  For
example:
sqlite> .mode qbox
sqlite> select decimal(47.49);
┌──────────────────────────────────────────────────────┐
│                    decimal(47.49)                    │
├──────────────────────────────────────────────────────┤
│ '47.49000000000000198951966012828052043914794921875' │
└──────────────────────────────────────────────────────┘
2.2.4. The decimal_cmp(X) function
The decimal_cmp(A,B) function compares two decimal values A and B.  The result will
be negative, zero, or positive if A is less than, equal to, or greater than B,
respectively.
2.2.5. The decimal_sum(X) aggregate function
The decimal_sum(X) function is an aggregate, like the built-in
sum() aggregate function, except that decimal_sum() computes its result
to arbitrary precision and is therefore precise.
2.2.6. The decimal collating sequence
The decimal extension provides the "decimal" collating sequence
that compares decimal text strings in numeric order.
This page was last updated on 2025-11-13 07:12:58Z 
