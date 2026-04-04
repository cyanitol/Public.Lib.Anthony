// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// RegisterScalarFunctions registers all scalar functions.
func RegisterScalarFunctions(r *Registry) {
	// String functions
	r.Register(NewScalarFunc("length", 1, lengthFunc))
	r.Register(NewScalarFunc("substr", -1, substrFunc))    // 2 or 3 args
	r.Register(NewScalarFunc("substring", -1, substrFunc)) // alias for substr
	r.Register(NewScalarFunc("upper", 1, upperFunc))
	r.Register(NewScalarFunc("lower", 1, lowerFunc))
	r.Register(NewScalarFunc("trim", -1, trimFunc))   // 1 or 2 args
	r.Register(NewScalarFunc("ltrim", -1, ltrimFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("rtrim", -1, rtrimFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("replace", 3, replaceFunc))
	r.Register(NewScalarFunc("instr", 2, instrFunc))
	r.Register(NewScalarFunc("hex", 1, hexFunc))
	r.Register(NewScalarFunc("unhex", -1, unhexFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("quote", 1, quoteFunc))
	r.Register(NewScalarFunc("unicode", 1, unicodeFunc))
	r.Register(NewScalarFunc("char", -1, charFunc)) // variadic

	// Type functions
	r.Register(NewScalarFunc("typeof", 1, typeofFunc))
	r.Register(NewScalarFunc("coalesce", -1, coalesceFunc)) // variadic
	r.Register(NewScalarFunc("ifnull", 2, ifnullFunc))
	r.Register(NewScalarFunc("nullif", 2, nullifFunc))
	r.Register(NewScalarFunc("iif", 3, iifFunc))

	// Optimization hint functions
	r.Register(NewScalarFunc("likely", 1, likelyFunc))
	r.Register(NewScalarFunc("unlikely", 1, unlikelyFunc))
	r.Register(NewScalarFunc("likelihood", 2, likelihoodFunc))

	// Blob functions
	r.Register(NewScalarFunc("zeroblob", 1, zeroblobFunc))

	// Scalar min/max (multi-arg versions, different from aggregate min/max)
	// Use RegisterUser with -1 (variadic) to give them priority over aggregate versions
	r.RegisterUser(NewScalarFunc("min", -1, minScalarFunc), -1)
	r.RegisterUser(NewScalarFunc("max", -1, maxScalarFunc), -1)

	// Printf and format functions (format is an alias for printf)
	r.Register(NewScalarFunc("printf", -1, printfFunc))
	r.Register(NewScalarFunc("format", -1, printfFunc))
}

// lengthFunc implements length(X)
// Returns the number of characters in X (UTF-8 aware for text)
func lengthFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	switch args[0].Type() {
	case TypeBlob:
		return NewIntValue(int64(args[0].Bytes())), nil
	case TypeInteger, TypeFloat:
		s := args[0].AsString()
		return NewIntValue(int64(utf8.RuneCountInString(s))), nil
	case TypeText:
		s := args[0].AsString()
		return NewIntValue(int64(utf8.RuneCountInString(s))), nil
	default:
		return NewNullValue(), nil
	}
}

// substrFunc implements substr(X, Y [, Z])
// Returns a substring of X starting at Y with length Z (or to end if Z omitted)
// Y is 1-indexed; negative Y counts from end
func substrFunc(args []Value) (Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("substr() requires 2 or 3 arguments")
	}
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	isBlob := args[0].Type() == TypeBlob
	length := substrInputLength(args[0], isBlob)

	start := args[1].AsInt64()
	subLen, null := substrParseLength(args, length)
	if null {
		return NewNullValue(), nil
	}

	hasExplicitLength := len(args) == 3
	start, subLen, null = substrAdjustStart(args[1], start, subLen, length, hasExplicitLength)
	if null {
		return NewNullValue(), nil
	}

	start, subLen = substrAdjustNegLen(start, subLen)

	if isBlob {
		return substrBlobResult(args[0].AsBlob(), start, subLen, length), nil
	}
	return substrTextResult(args[0].AsString(), start, subLen), nil
}

// substrInputLength returns the logical length of the substr input value.
// For blobs this is the byte count; for text it is the rune count.
func substrInputLength(v Value, isBlob bool) int {
	if isBlob {
		return len(v.AsBlob())
	}
	return utf8.RuneCountInString(v.AsString())
}

// substrParseLength resolves the optional third argument into a subLen.
// The second return value is true when the caller must return NULL.
func substrParseLength(args []Value, length int) (subLen int64, returnNull bool) {
	if len(args) != 3 {
		return int64(length), false
	}
	if args[2].IsNull() {
		return 0, true
	}
	return args[2].AsInt64(), false
}

// substrAdjustStart converts the 1-based SQLite start position to a 0-based
// index and adjusts subLen when a negative start overflows the left boundary.
// The third return value is true when the caller must return NULL.
func substrAdjustStart(startArg Value, start, subLen int64, length int, hasExplicitLength bool) (int64, int64, bool) {
	if start < 0 {
		return adjustNegativeStart(start, subLen, length)
	}
	if start > 0 {
		return start - 1, subLen, false
	}
	return adjustZeroStart(startArg, subLen, hasExplicitLength)
}

// adjustNegativeStart handles negative start positions in substr
func adjustNegativeStart(start, subLen int64, length int) (int64, int64, bool) {
	start = int64(length) + start
	if start < 0 {
		if subLen >= 0 {
			subLen += start
		} else {
			subLen = 0
		}
		start = 0
	}
	return start, subLen, false
}

// adjustZeroStart handles zero start position in substr
func adjustZeroStart(startArg Value, subLen int64, hasExplicitLength bool) (int64, int64, bool) {
	if startArg.IsNull() {
		return 0, 0, true
	}
	// Position 0 in SQLite means "start before the first character"
	// This wastes one character of length, but only when length is explicit
	if hasExplicitLength && subLen > 0 {
		subLen--
	}
	return 0, subLen, false
}

// substrAdjustNegLen handles a negative subLen, which in SQLite means
// "return characters that lie before the start position".
func substrAdjustNegLen(start, subLen int64) (int64, int64) {
	if subLen >= 0 {
		return start, subLen
	}
	if subLen < -start {
		subLen = start
	} else {
		subLen = -subLen
	}
	start -= subLen
	if start < 0 {
		start = 0
	}
	if subLen < 0 {
		subLen = 0
	}
	return start, subLen
}

// substrBlobResult extracts a byte slice from data using pre-adjusted,
// 0-based start and subLen values.
func substrBlobResult(data []byte, start, subLen int64, length int) Value {
	if start >= int64(length) {
		return NewBlobValue([]byte{})
	}
	end := start + subLen
	if end > int64(length) {
		end = int64(length)
	}
	return NewBlobValue(data[start:end])
}

// substrTextResult extracts a rune slice from s using pre-adjusted,
// 0-based start and subLen values.
func substrTextResult(s string, start, subLen int64) Value {
	runes := []rune(s)
	if start >= int64(len(runes)) {
		return NewTextValue("")
	}
	end := start + subLen
	if end > int64(len(runes)) {
		end = int64(len(runes))
	}
	return NewTextValue(string(runes[start:end]))
}

// upperFunc implements upper(X)
func upperFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}
	return NewTextValue(strings.ToUpper(args[0].AsString())), nil
}

// lowerFunc implements lower(X)
func lowerFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}
	return NewTextValue(strings.ToLower(args[0].AsString())), nil
}

// makeTrimFunc creates a trim function parameterized by the trim direction.
func makeTrimFunc(trimFn func(string, string) string) func([]Value) (Value, error) {
	return func(args []Value) (Value, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, fmt.Errorf("trim function requires 1 or 2 arguments")
		}
		if args[0].IsNull() {
			return NewNullValue(), nil
		}
		s := args[0].AsString()
		cutset := " "
		if len(args) == 2 && !args[1].IsNull() {
			cutset = args[1].AsString()
		}
		return NewTextValue(trimFn(s, cutset)), nil
	}
}

var (
	trimFunc  = makeTrimFunc(strings.Trim)
	ltrimFunc = makeTrimFunc(strings.TrimLeft)
	rtrimFunc = makeTrimFunc(strings.TrimRight)
)

// replaceFunc implements replace(X, Y, Z)
// Replaces all occurrences of Y in X with Z
func replaceFunc(args []Value) (Value, error) {
	// SQLite returns NULL if ANY argument is NULL
	if args[0].IsNull() || args[1].IsNull() || args[2].IsNull() {
		return NewNullValue(), nil
	}

	x := args[0].AsString()
	y := args[1].AsString()
	z := args[2].AsString()

	// Handle empty pattern
	if y == "" {
		return NewTextValue(x), nil
	}

	return NewTextValue(strings.ReplaceAll(x, y, z)), nil
}

// instrFunc implements instr(X, Y)
// Returns the 1-indexed position of the first occurrence of Y in X, or 0 if not found
func instrFunc(args []Value) (Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	// Handle both as blobs
	if args[0].Type() == TypeBlob && args[1].Type() == TypeBlob {
		return instrBlobSearch(args[0], args[1])
	}

	// Text-based search (UTF-8 aware)
	return instrTextSearch(args[0], args[1])
}

// instrBlobSearch performs binary search in blob data
func instrBlobSearch(haystack, needle Value) (Value, error) {
	haystackBytes := haystack.AsBlob()
	needleBytes := needle.AsBlob()
	idx := bytes.Index(haystackBytes, needleBytes)
	if idx < 0 {
		return NewIntValue(0), nil
	}
	return NewIntValue(int64(idx + 1)), nil
}

// instrTextSearch performs UTF-8 aware text search
func instrTextSearch(haystack, needle Value) (Value, error) {
	haystackStr := haystack.AsString()
	needleStr := needle.AsString()

	if needleStr == "" {
		return NewIntValue(1), nil
	}

	idx := strings.Index(haystackStr, needleStr)
	if idx < 0 {
		return NewIntValue(0), nil
	}

	// Convert byte index to character index
	charIdx := utf8.RuneCountInString(haystackStr[:idx])
	return NewIntValue(int64(charIdx + 1)), nil
}

// hexFunc implements hex(X)
// Returns hex representation of X
// SQLite converts all values to their text representation first, then hex encodes the UTF-8 bytes
func hexFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	// Convert to string first (matching SQLite behavior)
	// For integers: 123 -> "123" -> hex("123") = "313233"
	// For text/blob: use as-is
	var data []byte
	if args[0].Type() == TypeBlob {
		data = args[0].AsBlob()
	} else {
		// Convert to string representation, then to bytes
		data = []byte(args[0].AsString())
	}
	return NewTextValue(strings.ToUpper(hex.EncodeToString(data))), nil
}

// unhexFunc implements unhex(X [, Y])
// Decodes hex string X, optionally ignoring characters in Y
func unhexFunc(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("unhex() requires 1 or 2 arguments")
	}
	if args[0].IsNull() {
		return NewNullValue(), nil
	}
	hexStr := filterIgnoredChars(args)
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return NewNullValue(), nil
	}
	return NewBlobValue(decoded), nil
}

// filterIgnoredChars removes ignored characters from the hex string.
func filterIgnoredChars(args []Value) string {
	hexStr := args[0].AsString()
	if len(args) < 2 || args[1].IsNull() {
		return hexStr
	}
	ignore := args[1].AsString()
	var filtered strings.Builder
	for _, r := range hexStr {
		if !strings.ContainsRune(ignore, r) {
			filtered.WriteRune(r)
		}
	}
	return filtered.String()
}

// quoteFunc implements quote(X)
// Returns SQL literal representation of X
func quoteFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewTextValue("NULL"), nil
	}

	switch args[0].Type() {
	case TypeInteger:
		return NewTextValue(fmt.Sprintf("%d", args[0].AsInt64())), nil
	case TypeFloat:
		f := args[0].AsFloat64()
		return NewTextValue(fmt.Sprintf("%g", f)), nil
	case TypeText:
		s := args[0].AsString()
		// Escape single quotes
		escaped := strings.ReplaceAll(s, "'", "''")
		return NewTextValue(fmt.Sprintf("'%s'", escaped)), nil
	case TypeBlob:
		data := args[0].AsBlob()
		hexStr := hex.EncodeToString(data)
		return NewTextValue(fmt.Sprintf("X'%s'", strings.ToUpper(hexStr))), nil
	default:
		return NewTextValue("NULL"), nil
	}
}

// unicodeFunc implements unicode(X)
// Returns the Unicode code point of the first character of X
func unicodeFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	s := args[0].AsString()
	if s == "" {
		return NewNullValue(), nil
	}

	r, _ := utf8.DecodeRuneInString(s)
	return NewIntValue(int64(r)), nil
}

// charFunc implements char(X1, X2, ..., XN)
// Returns a string composed of characters with Unicode code points
func charFunc(args []Value) (Value, error) {
	var result strings.Builder

	for _, arg := range args {
		if arg.IsNull() {
			continue
		}

		codePoint := arg.AsInt64()
		// Invalid code points become replacement character
		if codePoint < 0 || codePoint > 0x10FFFF {
			codePoint = 0xFFFD
		}

		result.WriteRune(rune(codePoint))
	}

	return NewTextValue(result.String()), nil
}

// typeofFunc implements typeof(X)
// Returns the type of X as a string
func typeofFunc(args []Value) (Value, error) {
	return NewTextValue(args[0].Type().String()), nil
}

// coalesceFunc implements coalesce(X, Y, ...)
// Returns the first non-NULL argument
func coalesceFunc(args []Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("coalesce() requires at least 1 argument")
	}

	for _, arg := range args {
		if !arg.IsNull() {
			return arg, nil
		}
	}

	return NewNullValue(), nil
}

// ifnullFunc implements ifnull(X, Y)
// Returns X if X is not NULL, otherwise Y
func ifnullFunc(args []Value) (Value, error) {
	if !args[0].IsNull() {
		return args[0], nil
	}
	return args[1], nil
}

// nullifFunc implements nullif(X, Y)
// Returns NULL if X == Y, otherwise X
func nullifFunc(args []Value) (Value, error) {
	// If both are NULL, they are equal
	if args[0].IsNull() && args[1].IsNull() {
		return NewNullValue(), nil
	}

	// If one is NULL, they are not equal
	if args[0].IsNull() || args[1].IsNull() {
		return args[0], nil
	}

	// Compare values
	if compareValues(args[0], args[1]) == 0 {
		return NewNullValue(), nil
	}

	return args[0], nil
}

// iifFunc implements iif(X, Y, Z)
// Returns Y if X is true, otherwise Z
func iifFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return args[2], nil // NULL is false
	}

	// Determine truthiness
	isTrue := false
	switch args[0].Type() {
	case TypeInteger:
		isTrue = args[0].AsInt64() != 0
	case TypeFloat:
		isTrue = args[0].AsFloat64() != 0.0
	case TypeText:
		// Non-empty string is true if it can be parsed as non-zero number
		f := args[0].AsFloat64()
		isTrue = f != 0.0
	}

	if isTrue {
		return args[1], nil
	}
	return args[2], nil
}

// maxBlobSize is the maximum allowed blob size (1 GiB), matching SQLite's
// default SQLITE_MAX_LENGTH.
const maxBlobSize = 1_000_000_000

// zeroblobFunc implements zeroblob(N)
// Returns a blob of N zero bytes
func zeroblobFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	n := args[0].AsInt64()
	if n < 0 {
		n = 0
	}
	if n > maxBlobSize {
		return nil, fmt.Errorf("zeroblob(%d) exceeds maximum blob size of %d bytes", n, maxBlobSize)
	}

	blob := make([]byte, n)
	return NewBlobValue(blob), nil
}

var typeComparators = map[ValueType]func(a, b Value) int{
	TypeInteger: func(a, b Value) int { return cmpOrdered(a.AsInt64(), b.AsInt64()) },
	TypeFloat:   func(a, b Value) int { return cmpOrdered(a.AsFloat64(), b.AsFloat64()) },
	TypeText:    func(a, b Value) int { return strings.Compare(a.AsString(), b.AsString()) },
	TypeBlob:    func(a, b Value) int { return bytes.Compare(a.AsBlob(), b.AsBlob()) },
}

func cmpOrdered[T int64 | float64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func nullCompare(a, b Value) (int, bool) {
	if a.IsNull() && b.IsNull() {
		return 0, true
	}
	if a.IsNull() {
		return -1, true
	}
	if b.IsNull() {
		return 1, true
	}
	return 0, false
}

func compareValues(a, b Value) int {
	if n, ok := nullCompare(a, b); ok {
		return n
	}
	if a.Type() != b.Type() {
		return int(a.Type()) - int(b.Type())
	}
	if cmp, ok := typeComparators[a.Type()]; ok {
		return cmp(a, b)
	}
	return 0
}

// likelyFunc implements likely(X)
// Returns the argument X unchanged. It's a hint to the query planner that X is probably TRUE.
func likelyFunc(args []Value) (Value, error) {
	return args[0], nil
}

// unlikelyFunc implements unlikely(X)
// Returns the argument X unchanged. It's a hint to the query planner that X is probably FALSE.
func unlikelyFunc(args []Value) (Value, error) {
	return args[0], nil
}

// likelihoodFunc implements likelihood(X, P)
// Returns the argument X unchanged. P is a probability between 0.0 and 1.0 that is a hint
// to the query planner about the likelihood of X being TRUE. For our implementation,
// it just passes through the first value and optionally validates the probability argument.
func likelihoodFunc(args []Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("likelihood() requires exactly 2 arguments")
	}
	// Optionally validate that args[1] is a number between 0 and 1
	if !args[1].IsNull() {
		prob := args[1].AsFloat64()
		if prob < 0.0 || prob > 1.0 {
			return nil, fmt.Errorf("likelihood probability must be between 0.0 and 1.0")
		}
	}
	return args[0], nil
}

// printfFormatSpec holds parsed format specifier information
type printfFormatSpec struct {
	width     int
	precision int
	specifier byte
	leftAlign bool // -
	showSign  bool // +
	spaceSign bool // ' '
	altForm   bool // #
	zeroPad   bool // 0
	thousands bool // ,
	widthStar bool // * for dynamic width
	precStar  bool // * for dynamic precision
}

// parsePrintfFlags parses format flags (-+#0 etc) and returns new position
func parsePrintfFlags(format string, pos int, spec *printfFormatSpec) int {
	flagMap := map[byte]*bool{
		'-': &spec.leftAlign, '+': &spec.showSign, ' ': &spec.spaceSign,
		'#': &spec.altForm, '0': &spec.zeroPad, ',': &spec.thousands,
	}
	for pos < len(format) {
		if ptr, ok := flagMap[format[pos]]; ok {
			*ptr = true
			pos++
		} else {
			break
		}
	}
	return pos
}

// parsePrintfWidth parses width specifier (number or *) and returns new position
func parsePrintfWidth(format string, pos int, args []Value, argIdx *int, spec *printfFormatSpec) int {
	if pos < len(format) && format[pos] == '*' {
		spec.widthStar = true
		if *argIdx < len(args) {
			spec.width = int(args[*argIdx].AsInt64())
			*argIdx++
		}
		return pos + 1
	}
	spec.width, pos = parseDecimalNumber(format, pos)
	return pos
}

// parsePrintfPrecision parses precision specifier (.number or .*) and returns new position
func parsePrintfPrecision(format string, pos int, args []Value, argIdx *int, spec *printfFormatSpec) int {
	if pos >= len(format) || format[pos] != '.' {
		return pos
	}
	pos++ // skip '.'
	if pos < len(format) && format[pos] == '*' {
		spec.precStar = true
		if *argIdx < len(args) {
			spec.precision = int(args[*argIdx].AsInt64())
			*argIdx++
		}
		return pos + 1
	}
	spec.precision, pos = parseDecimalNumber(format, pos)
	return pos
}

// skipLengthModifiers skips C-style length modifiers (l, h, L, j, t)
func skipLengthModifiers(format string, pos int) int {
	for pos < len(format) {
		c := format[pos]
		if c == 'l' || c == 'h' || c == 'L' || c == 'j' || c == 't' {
			pos++
		} else {
			break
		}
	}
	return pos
}

// parsePrintfFormatSpec parses a format specifier starting at pos (after the %)
func parsePrintfFormatSpec(format string, pos int, args []Value, argIdx *int) (printfFormatSpec, int) {
	spec := printfFormatSpec{precision: -1}
	pos = parsePrintfFlags(format, pos, &spec)
	pos = parsePrintfWidth(format, pos, args, argIdx, &spec)
	pos = parsePrintfPrecision(format, pos, args, argIdx, &spec)
	pos = skipLengthModifiers(format, pos)
	if pos < len(format) {
		spec.specifier = format[pos]
		pos++
	}
	return spec, pos
}

// parseDecimalNumber parses a decimal number starting at pos
func parseDecimalNumber(format string, pos int) (int, int) {
	num := 0
	for pos < len(format) && format[pos] >= '0' && format[pos] <= '9' {
		num = num*10 + int(format[pos]-'0')
		pos++
	}
	return num, pos
}

// formatPrintfInteger formats integer values (%d, %i, %u)
func formatPrintfInteger(spec printfFormatSpec, arg Value) string {
	var val int64
	if arg.IsNull() {
		val = 0
	} else {
		val = arg.AsInt64()
	}

	var numStr string
	if spec.specifier == 'u' {
		numStr = fmt.Sprintf("%d", uint64(val))
	} else {
		numStr = fmt.Sprintf("%d", val)
	}

	// Handle thousands separator
	if spec.thousands && val != 0 {
		numStr = addThousandsSeparator(numStr)
	}

	// Handle sign
	if val >= 0 {
		if spec.showSign {
			numStr = "+" + numStr
		} else if spec.spaceSign {
			numStr = " " + numStr
		}
	}

	// Apply width padding
	return applyPadding(numStr, spec)
}

// addThousandsSeparator adds commas to a numeric string
func addThousandsSeparator(s string) string {
	negative := s[0] == '-'
	if negative {
		s = s[1:]
	}
	var result strings.Builder
	n := len(s)
	for i, c := range s {
		if i > 0 && (n-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	if negative {
		return "-" + result.String()
	}
	return result.String()
}

// hasSignPrefix checks if the string starts with a sign character.
func hasSignPrefix(s string) bool {
	return len(s) > 0 && (s[0] == '-' || s[0] == '+' || s[0] == ' ')
}

// applyZeroPadding applies zero padding to a string, respecting sign prefix.
func applyZeroPadding(s string, padding int) string {
	if hasSignPrefix(s) {
		return string(s[0]) + strings.Repeat("0", padding) + s[1:]
	}
	return strings.Repeat("0", padding) + s
}

// applyPadding applies width and alignment to a formatted value
func applyPadding(s string, spec printfFormatSpec) string {
	if spec.width <= len(s) {
		return s
	}
	padding := spec.width - len(s)
	if spec.leftAlign {
		return s + strings.Repeat(" ", padding)
	}
	if spec.zeroPad && !spec.leftAlign {
		return applyZeroPadding(s, padding)
	}
	return strings.Repeat(" ", padding) + s
}

// formatPrintfFloat formats floating-point values (%f, %F, %e, %E, %g, %G)
func formatPrintfFloat(spec printfFormatSpec, arg Value) string {
	var val float64
	if arg.IsNull() {
		val = 0
	} else {
		val = arg.AsFloat64()
	}

	prec := 6 // default precision
	if spec.precision >= 0 {
		prec = spec.precision
	}

	var numStr string
	switch spec.specifier {
	case 'e', 'E':
		numStr = fmt.Sprintf("%.*"+string(spec.specifier), prec, val)
	case 'g', 'G':
		numStr = fmt.Sprintf("%.*"+string(spec.specifier), prec, val)
	default:
		numStr = fmt.Sprintf("%.*f", prec, val)
	}

	// Handle sign
	if val >= 0 {
		if spec.showSign {
			numStr = "+" + numStr
		} else if spec.spaceSign {
			numStr = " " + numStr
		}
	}

	return applyPadding(numStr, spec)
}

// formatPrintfHex formats hexadecimal values (%x, %X)
func formatPrintfHex(spec printfFormatSpec, arg Value) string {
	var val int64
	if arg.IsNull() {
		val = 0
	} else {
		val = arg.AsInt64()
	}

	var numStr string
	if spec.specifier == 'X' {
		numStr = fmt.Sprintf("%X", uint64(val))
	} else {
		numStr = fmt.Sprintf("%x", uint64(val))
	}

	if spec.altForm && val != 0 {
		if spec.specifier == 'X' {
			numStr = "0X" + numStr
		} else {
			numStr = "0x" + numStr
		}
	}

	return applyPadding(numStr, spec)
}

// formatPrintfOctal formats octal values (%o)
func formatPrintfOctal(spec printfFormatSpec, arg Value) string {
	var val int64
	if arg.IsNull() {
		val = 0
	} else {
		val = arg.AsInt64()
	}

	numStr := fmt.Sprintf("%o", val)
	if spec.altForm && val != 0 {
		numStr = "0" + numStr
	}
	return applyPadding(numStr, spec)
}

// formatPrintfString formats string values (%s)
func formatPrintfString(spec printfFormatSpec, arg Value) string {
	if arg.IsNull() {
		return applyPadding("", spec)
	}
	s := arg.AsString()
	if spec.precision >= 0 && spec.precision < len(s) {
		s = s[:spec.precision]
	}
	return applyPadding(s, spec)
}

// formatPrintfChar formats character values (%c)
func formatPrintfChar(spec printfFormatSpec, arg Value) string {
	var ch string
	if arg.IsNull() {
		ch = ""
	} else if arg.Type() == TypeText {
		// For string argument, use first character
		s := arg.AsString()
		if len(s) > 0 {
			r, _ := utf8.DecodeRuneInString(s)
			ch = string(r)
		}
	} else {
		// For integer, interpret as Unicode code point
		val := arg.AsInt64()
		if val == 0 {
			ch = ""
		} else {
			ch = string(rune(val))
		}
	}

	// SQLite's %c with precision repeats the character
	if spec.precision > 0 && len(ch) > 0 {
		ch = strings.Repeat(ch, spec.precision)
	}

	return applyPadding(ch, spec)
}

// formatPrintfQuoted formats SQL-quoted string values (%q, %Q)
func formatPrintfQuoted(spec printfFormatSpec, arg Value) string {
	if arg.IsNull() {
		if spec.specifier == 'Q' {
			return "NULL"
		}
		return ""
	}
	s := arg.AsString()
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

// printfFormatHandler is a function that formats a value according to a format spec
type printfFormatHandler func(printfFormatSpec, Value) string

// printfFormatHandlers maps format specifiers to their handler functions
var printfFormatHandlers = map[byte]printfFormatHandler{
	'd': formatPrintfInteger,
	'i': formatPrintfInteger,
	'u': formatPrintfInteger,
	'f': formatPrintfFloat,
	'F': formatPrintfFloat,
	'e': formatPrintfFloat,
	'E': formatPrintfFloat,
	'g': formatPrintfFloat,
	'G': formatPrintfFloat,
	'x': formatPrintfHex,
	'X': formatPrintfHex,
	'o': formatPrintfOctal,
	's': formatPrintfString,
	'z': formatPrintfString, // %z is like %s but for compatibility
	'c': formatPrintfChar,
	'q': formatPrintfQuoted,
	'Q': formatPrintfQuoted,
}

// processPrintfFormatCode handles a format specifier at the current position
// Returns the new position
func processPrintfFormatCode(format string, pos int, args []Value, argIdx *int, result *strings.Builder) int {
	// Handle %%
	if format[pos] == '%' {
		result.WriteByte('%')
		return pos + 1
	}

	// Parse format specifier (may consume args for * width/precision)
	spec, newPos := parsePrintfFormatSpec(format, pos, args, argIdx)

	// Handle special specifiers
	switch spec.specifier {
	case 'n':
		// %n is silently ignored in SQLite
		return newPos
	case 'p':
		// %p formats as uppercase hex
		var val int64
		if *argIdx < len(args) {
			val = args[*argIdx].AsInt64()
			*argIdx++
		}
		result.WriteString(fmt.Sprintf("%X", val))
		return newPos
	}

	// Look up handler and format
	handler, ok := printfFormatHandlers[spec.specifier]
	if !ok {
		// Unknown specifier: do not consume an argument.
		result.WriteByte('%')
		if spec.specifier != 0 {
			result.WriteByte(spec.specifier)
		}
		return newPos
	}

	// Get argument value
	var arg Value
	if *argIdx < len(args) {
		arg = args[*argIdx]
		*argIdx++
	} else {
		arg = NewNullValue()
	}

	result.WriteString(handler(spec, arg))

	return newPos
}

// printfFunc implements printf(FORMAT, ...) - formatted string output
func printfFunc(args []Value) (Value, error) {
	// printf() and format() require at least one argument (the format string)
	if len(args) == 0 {
		return nil, fmt.Errorf("printf() requires at least 1 argument")
	}
	// If format string is NULL, return NULL
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	format := args[0].AsString()
	argIdx := 1
	var result strings.Builder

	i := 0
	for i < len(format) {
		if format[i] != '%' {
			result.WriteByte(format[i])
			i++
			continue
		}

		// Found %
		i++
		if i >= len(format) {
			result.WriteByte('%')
			break
		}

		i = processPrintfFormatCode(format, i, args, &argIdx, &result)
	}

	return NewTextValue(result.String()), nil
}
