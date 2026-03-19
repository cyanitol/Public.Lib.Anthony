// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // current reading position (after current char)
	ch      byte // current char under examination
	line    int  // current line number
	col     int  // current column number
}

// NewLexer creates a new Lexer for the given SQL input.
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input: input,
		line:  1,
		col:   0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances position.
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
	l.col++
}

// peekChar returns the next character without advancing position.
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// peekAhead returns the character n positions ahead without advancing.
func (l *Lexer) peekAhead(n int) byte {
	pos := l.readPos + n - 1
	if pos >= len(l.input) {
		return 0
	}
	return l.input[pos]
}

// simpleTokenMap maps single characters to their token types.
var simpleTokenMap = map[byte]TokenType{
	';': TK_SEMI,
	'(': TK_LP,
	')': TK_RP,
	',': TK_COMMA,
	'+': TK_PLUS,
	'*': TK_STAR,
	'%': TK_REM,
	'~': TK_BITNOT,
	'&': TK_BITAND,
}

// tokenScanner is a function type for scanning complex tokens.
type tokenScanner func(*Lexer) Token

// complexTokenScanners maps characters to their scanner functions.
var complexTokenScanners = map[byte]tokenScanner{
	'.':  (*Lexer).scanDot,
	'-':  (*Lexer).scanMinus,
	'/':  (*Lexer).scanSlash,
	'|':  (*Lexer).scanPipe,
	'=':  (*Lexer).scanEquals,
	'<':  (*Lexer).scanLessThan,
	'>':  (*Lexer).scanGreaterThan,
	'!':  (*Lexer).scanBang,
	'\'': (*Lexer).scanSingleQuote,
	'"':  (*Lexer).scanDoubleQuote,
	'`':  (*Lexer).scanBacktick,
	'[':  (*Lexer).scanBracket,
	'?':  (*Lexer).scanQuestion,
	'@':  (*Lexer).scanNamedVar,
	'#':  (*Lexer).scanNamedVar,
	':':  (*Lexer).scanNamedVar,
	'$':  (*Lexer).scanDollar,
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	// Handle EOF
	if l.ch == 0 {
		return Token{Type: TK_EOF, Lexeme: "", Pos: l.pos, Line: l.line, Col: l.col}
	}

	// Check for simple single-character tokens
	if tokType, ok := simpleTokenMap[l.ch]; ok {
		tok := Token{Type: tokType, Lexeme: string(l.ch), Pos: l.pos, Line: l.line, Col: l.col}
		l.readChar()
		return tok
	}

	// Check for complex tokens
	if scanner, ok := complexTokenScanners[l.ch]; ok {
		return scanner(l)
	}

	// Default handling (identifiers, numbers)
	return l.scanDefault()
}

// scanSingleQuote handles single-quoted strings.
func (l *Lexer) scanSingleQuote() Token {
	return l.readString('\'')
}

// scanDoubleQuote handles double-quoted identifiers.
func (l *Lexer) scanDoubleQuote() Token {
	return l.readQuotedIdentifier('"')
}

// scanBacktick handles backtick-quoted identifiers.
func (l *Lexer) scanBacktick() Token {
	return l.readQuotedIdentifier('`')
}

// scanBracket handles bracketed identifiers.
func (l *Lexer) scanBracket() Token {
	return l.readBracketedIdentifier()
}

// scanQuestion handles variable placeholders.
func (l *Lexer) scanQuestion() Token {
	return l.readVariable()
}

// scanNamedVar handles named variable placeholders (@, #, :).
func (l *Lexer) scanNamedVar() Token {
	return l.readNamedVariable()
}

// scanDot handles the '.' character which may start a number.
func (l *Lexer) scanDot() Token {
	if isDigit(l.peekChar()) {
		return l.readNumber()
	}
	tok := Token{Type: TK_DOT, Lexeme: string(l.ch), Pos: l.pos, Line: l.line, Col: l.col}
	l.readChar()
	return tok
}

// scanMinus handles '-', '--', '->', and '->>'.
func (l *Lexer) scanMinus() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	if l.peekChar() == '-' {
		return l.readLineComment()
	}
	if l.peekChar() == '>' {
		l.readChar()
		if l.peekChar() == '>' {
			tok.Type = TK_PTR
			tok.Lexeme = "->>"
			l.readChar()
			l.readChar()
		} else {
			tok.Type = TK_PTR
			tok.Lexeme = "->"
			l.readChar()
		}
		return tok
	}
	tok.Type = TK_MINUS
	tok.Lexeme = string(l.ch)
	l.readChar()
	return tok
}

// scanSlash handles '/' and block comments '/*'.
func (l *Lexer) scanSlash() Token {
	if l.peekChar() == '*' {
		return l.readBlockComment()
	}
	tok := Token{Type: TK_SLASH, Lexeme: string(l.ch), Pos: l.pos, Line: l.line, Col: l.col}
	l.readChar()
	return tok
}

// scanPipe handles '|' and '||'.
func (l *Lexer) scanPipe() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	if l.peekChar() == '|' {
		tok.Type = TK_CONCAT
		tok.Lexeme = "||"
		l.readChar()
		l.readChar()
	} else {
		tok.Type = TK_BITOR
		tok.Lexeme = string(l.ch)
		l.readChar()
	}
	return tok
}

// scanEquals handles '=' and '=='.
func (l *Lexer) scanEquals() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	if l.peekChar() == '=' {
		tok.Type = TK_EQ
		tok.Lexeme = "=="
		l.readChar()
		l.readChar()
	} else {
		tok.Type = TK_EQ
		tok.Lexeme = string(l.ch)
		l.readChar()
	}
	return tok
}

// scanLessThan handles '<', '<=', '<>', and '<<'.
func (l *Lexer) scanLessThan() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	switch l.peekChar() {
	case '=':
		tok.Type = TK_LE
		tok.Lexeme = "<="
		l.readChar()
		l.readChar()
	case '>':
		tok.Type = TK_NE
		tok.Lexeme = "<>"
		l.readChar()
		l.readChar()
	case '<':
		tok.Type = TK_LSHIFT
		tok.Lexeme = "<<"
		l.readChar()
		l.readChar()
	default:
		tok.Type = TK_LT
		tok.Lexeme = string(l.ch)
		l.readChar()
	}
	return tok
}

// scanGreaterThan handles '>', '>=', and '>>'.
func (l *Lexer) scanGreaterThan() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	switch l.peekChar() {
	case '=':
		tok.Type = TK_GE
		tok.Lexeme = ">="
		l.readChar()
		l.readChar()
	case '>':
		tok.Type = TK_RSHIFT
		tok.Lexeme = ">>"
		l.readChar()
		l.readChar()
	default:
		tok.Type = TK_GT
		tok.Lexeme = string(l.ch)
		l.readChar()
	}
	return tok
}

// scanBang handles '!' and '!='.
func (l *Lexer) scanBang() Token {
	tok := Token{Pos: l.pos, Line: l.line, Col: l.col}
	if l.peekChar() == '=' {
		tok.Type = TK_NE
		tok.Lexeme = "!="
		l.readChar()
		l.readChar()
	} else {
		tok.Type = TK_ILLEGAL
		tok.Lexeme = string(l.ch)
		l.readChar()
	}
	return tok
}

// scanDollar handles '$' which may start a named variable.
func (l *Lexer) scanDollar() Token {
	if isLetter(l.peekChar()) || l.peekChar() == '_' {
		return l.readNamedVariable()
	}
	tok := Token{Type: TK_ILLEGAL, Lexeme: string(l.ch), Pos: l.pos, Line: l.line, Col: l.col}
	l.readChar()
	return tok
}

// scanDefault handles identifiers, numbers, and illegal characters.
func (l *Lexer) scanDefault() Token {
	if isLetter(l.ch) || l.ch == '_' {
		return l.readIdentifierOrKeyword()
	}
	if isDigit(l.ch) {
		return l.readNumber()
	}
	tok := Token{Type: TK_ILLEGAL, Lexeme: string(l.ch), Pos: l.pos, Line: l.line, Col: l.col}
	l.readChar()
	return tok
}

// skipWhitespace skips whitespace characters and updates line/col tracking.
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		if l.ch == '\n' {
			l.line++
			l.col = 0
		}
		l.readChar()
	}
}

// readIdentifierOrKeyword reads an identifier or keyword.
func (l *Lexer) readIdentifierOrKeyword() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	// Handle X'...' blob literals
	if l.isBlobLiteralStart() {
		l.readChar() // consume 'x' or 'X'
		l.readChar() // consume '\''
		return l.readBlobLiteral(startPos, startLine, startCol)
	}

	l.readIdentifierChars()

	lexeme := l.input[startPos:l.pos]
	tokType := lookupKeyword(lexeme)

	return Token{
		Type:   tokType,
		Lexeme: lexeme,
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

func (l *Lexer) isBlobLiteralStart() bool {
	return (l.ch == 'x' || l.ch == 'X') && l.peekChar() == '\''
}

func (l *Lexer) readIdentifierChars() {
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '$' {
		l.readChar()
	}
}

// readNumber reads a numeric literal (integer or float).
func (l *Lexer) readNumber() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	// Handle hexadecimal: 0x...
	if l.ch == '0' && (l.peekChar() == 'x' || l.peekChar() == 'X') {
		return l.readHexNumber(startPos, startLine, startCol)
	}

	tokType := l.readDecimalNumber()

	return Token{
		Type:   tokType,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readHexNumber reads a hexadecimal number literal.
func (l *Lexer) readHexNumber(startPos, startLine, startCol int) Token {
	l.readChar() // consume '0'
	l.readChar() // consume 'x' or 'X'
	for isHexDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return Token{
		Type:   TK_INTEGER,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readDecimalNumber reads the decimal part of a number and returns the token type.
func (l *Lexer) readDecimalNumber() TokenType {
	tokType := TK_INTEGER

	// Read integer part
	l.consumeDigits()

	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		tokType = TK_FLOAT
		l.readChar() // consume '.'
		l.consumeDigits()
	}

	// Check for scientific notation
	if l.ch == 'e' || l.ch == 'E' {
		tokType = TK_FLOAT
		l.readChar()
		l.consumeExponentSign()
		l.consumeDigits()
	}

	return tokType
}

// consumeDigits reads consecutive digits and underscores.
func (l *Lexer) consumeDigits() {
	for isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
}

// consumeExponentSign consumes an optional + or - sign.
func (l *Lexer) consumeExponentSign() {
	if l.ch == '+' || l.ch == '-' {
		l.readChar()
	}
}

// readQuotedToken reads a quoted token (string literal or identifier) enclosed
// in the given quote character, returning a token of the specified type.
func (l *Lexer) readQuotedToken(quote byte, tokType TokenType) Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume opening quote

	for l.ch != 0 {
		if l.ch == quote {
			// Check for escaped quote (doubled quote)
			if l.peekChar() == quote {
				l.readChar() // consume first quote
				l.readChar() // consume second quote
			} else {
				l.readChar() // consume closing quote
				break
			}
		} else {
			if l.ch == '\n' {
				l.line++
				l.col = 0
			}
			l.readChar()
		}
	}

	return Token{
		Type:   tokType,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readString reads a string literal enclosed in single quotes.
func (l *Lexer) readString(quote byte) Token {
	return l.readQuotedToken(quote, TK_STRING)
}

// readQuotedIdentifier reads a quoted identifier (double-quoted or backticked).
func (l *Lexer) readQuotedIdentifier(quote byte) Token {
	return l.readQuotedToken(quote, TK_ID)
}

// readBracketedIdentifier reads a bracketed identifier [...].
func (l *Lexer) readBracketedIdentifier() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume '['

	for l.ch != 0 && l.ch != ']' {
		if l.ch == '\n' {
			l.line++
			l.col = 0
		}
		l.readChar()
	}

	if l.ch == ']' {
		l.readChar() // consume ']'
	}

	return Token{
		Type:   TK_ID,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readBlobLiteral reads a blob literal X'...'.
func (l *Lexer) readBlobLiteral(startPos, startLine, startCol int) Token {
	// We're already past X'
	for isHexDigit(l.ch) {
		l.readChar()
	}

	if l.ch == '\'' {
		l.readChar() // consume closing quote
	}

	return Token{
		Type:   TK_BLOB,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readVariable reads a positional parameter (?NNN).
func (l *Lexer) readVariable() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume '?'

	for isDigit(l.ch) {
		l.readChar()
	}

	return Token{
		Type:   TK_VARIABLE,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readNamedVariable reads a named parameter (@name, :name, #name, $name).
func (l *Lexer) readNamedVariable() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume prefix

	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}

	return Token{
		Type:   TK_VARIABLE,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readLineComment reads a line comment (-- ...).
func (l *Lexer) readLineComment() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume first '-'
	l.readChar() // consume second '-'

	for l.ch != 0 && l.ch != '\n' {
		l.readChar()
	}

	return Token{
		Type:   TK_COMMENT,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// readBlockComment reads a block comment (/* ... */).
func (l *Lexer) readBlockComment() Token {
	startPos := l.pos
	startLine := l.line
	startCol := l.col

	l.readChar() // consume '/'
	l.readChar() // consume '*'

	for l.ch != 0 {
		if l.ch == '\n' {
			l.line++
			l.col = 0
		}
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar() // consume '*'
			l.readChar() // consume '/'
			break
		}
		l.readChar()
	}

	return Token{
		Type:   TK_COMMENT,
		Lexeme: l.input[startPos:l.pos],
		Pos:    startPos,
		Line:   startLine,
		Col:    startCol,
	}
}

// Helper functions

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isHexDigit(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

// keywordMap maps uppercase keyword strings to their token types.
var keywordMap = map[string]TokenType{
	"SELECT":        TK_SELECT,
	"FROM":          TK_FROM,
	"WHERE":         TK_WHERE,
	"INSERT":        TK_INSERT,
	"INTO":          TK_INTO,
	"VALUES":        TK_VALUES,
	"UPDATE":        TK_UPDATE,
	"SET":           TK_SET,
	"DELETE":        TK_DELETE,
	"CREATE":        TK_CREATE,
	"TABLE":         TK_TABLE,
	"INDEX":         TK_INDEX,
	"VIEW":          TK_VIEW,
	"TRIGGER":       TK_TRIGGER,
	"DROP":          TK_DROP,
	"ALTER":         TK_ALTER,
	"RENAME":        TK_RENAME,
	"ADD":           TK_ADD,
	"COLUMN":        TK_COLUMN,
	"TO":            TK_TO,
	"ORDER":         TK_ORDER,
	"BY":            TK_BY,
	"GROUP":         TK_GROUP,
	"HAVING":        TK_HAVING,
	"LIMIT":         TK_LIMIT,
	"OFFSET":        TK_OFFSET,
	"DISTINCT":      TK_DISTINCT,
	"ALL":           TK_ALL,
	"ASC":           TK_ASC,
	"DESC":          TK_DESC,
	"JOIN":          TK_JOIN,
	"LEFT":          TK_LEFT,
	"RIGHT":         TK_RIGHT,
	"FULL":          TK_FULL,
	"INNER":         TK_INNER,
	"OUTER":         TK_OUTER,
	"CROSS":         TK_CROSS,
	"NATURAL":       TK_NATURAL,
	"ON":            TK_ON,
	"USING":         TK_USING,
	"AND":           TK_AND,
	"OR":            TK_OR,
	"NOT":           TK_NOT,
	"IS":            TK_IS,
	"IN":            TK_IN,
	"LIKE":          TK_LIKE,
	"GLOB":          TK_GLOB,
	"BETWEEN":       TK_BETWEEN,
	"CASE":          TK_CASE,
	"WHEN":          TK_WHEN,
	"THEN":          TK_THEN,
	"ELSE":          TK_ELSE,
	"END":           TK_END,
	"NULL":          TK_NULL,
	"INTEGER":       TK_INTEGER_TYPE,
	"REAL":          TK_REAL,
	"TEXT":          TK_TEXT,
	"BLOB":          TK_BLOB_TYPE,
	"NUMERIC":       TK_NUMERIC,
	"PRIMARY":       TK_PRIMARY,
	"KEY":           TK_KEY,
	"UNIQUE":        TK_UNIQUE,
	"CHECK":         TK_CHECK,
	"DEFAULT":       TK_DEFAULT,
	"CONSTRAINT":    TK_CONSTRAINT,
	"FOREIGN":       TK_FOREIGN,
	"REFERENCES":    TK_REFERENCES,
	"AUTOINCREMENT": TK_AUTOINCREMENT,
	"COLLATE":       TK_COLLATE,
	"AS":            TK_AS,
	"IF":            TK_IF,
	"EXISTS":        TK_EXISTS,
	"TEMPORARY":     TK_TEMP,
	"TEMP":          TK_TEMP,
	"VIRTUAL":       TK_VIRTUAL,
	"BEGIN":         TK_BEGIN,
	"COMMIT":        TK_COMMIT,
	"ROLLBACK":      TK_ROLLBACK,
	"TRANSACTION":   TK_TRANSACTION,
	"SAVEPOINT":     TK_SAVEPOINT,
	"RELEASE":       TK_RELEASE,
	"DEFERRED":      TK_DEFERRED,
	"IMMEDIATE":     TK_IMMEDIATE,
	"EXCLUSIVE":     TK_EXCLUSIVE,
	"WITH":          TK_WITH,
	"RECURSIVE":     TK_RECURSIVE,
	"EXPLAIN":       TK_EXPLAIN,
	"QUERY":         TK_QUERY,
	"PLAN":          TK_PLAN,
	"PRAGMA":        TK_PRAGMA,
	"ANALYZE":       TK_ANALYZE,
	"ATTACH":        TK_ATTACH,
	"DETACH":        TK_DETACH,
	"DATABASE":      TK_DATABASE,
	"VACUUM":        TK_VACUUM,
	"REINDEX":       TK_REINDEX,
	"ISNULL":        TK_ISNULL,
	"NOTNULL":       TK_NOTNULL,
	"OVER":          TK_OVER,
	"PARTITION":     TK_PARTITION,
	"ROWS":          TK_ROWS,
	"RANGE":         TK_RANGE,
	"UNBOUNDED":     TK_UNBOUNDED,
	"CURRENT":       TK_CURRENT,
	"FOLLOWING":     TK_FOLLOWING,
	"PRECEDING":     TK_PRECEDING,
	"FILTER":        TK_FILTER,
	"WINDOW":        TK_WINDOW,
	"GROUPS":        TK_GROUPS,
	"EXCLUDE":       TK_EXCLUDE,
	"TIES":          TK_TIES,
	"OTHERS":        TK_OTHERS,
	"UNION":         TK_UNION,
	"EXCEPT":        TK_EXCEPT,
	"INTERSECT":     TK_INTERSECT,
	"CAST":          TK_CAST,
	"ESCAPE":        TK_ESCAPE,
	"MATCH":         TK_MATCH,
	"REGEXP":        TK_REGEXP,
	"ABORT":         TK_ABORT,
	"ACTION":        TK_ACTION,
	"AFTER":         TK_AFTER,
	"BEFORE":        TK_BEFORE,
	"CASCADE":       TK_CASCADE,
	"CONFLICT":      TK_CONFLICT,
	"FAIL":          TK_FAIL,
	"IGNORE":        TK_IGNORE,
	"REPLACE":       TK_REPLACE,
	"RESTRICT":      TK_RESTRICT,
	"NO":            TK_NO,
	"EACH":          TK_EACH,
	"FOR":           TK_FOR,
	"ROW":           TK_ROW,
	"INITIALLY":     TK_INITIALLY,
	"DEFERRABLE":    TK_DEFERRABLE,
	"INDEXED":       TK_INDEXED,
	"WITHOUT":       TK_WITHOUT,
	"ROWID":         TK_ROWID,
	"STRICT":        TK_STRICT,
	"GENERATED":     TK_GENERATED,
	"ALWAYS":        TK_ALWAYS,
	"STORED":        TK_STORED,
	"INSTEAD":       TK_INSTEAD,
	"OF":            TK_OF,
	"DO":            TK_DO,
	"NOTHING":       TK_NOTHING,
	"RETURNING":     TK_RETURNING,
}

// lookupKeyword returns the token type for a keyword, or TK_ID if not a keyword.
func lookupKeyword(ident string) TokenType {
	if tokType, ok := keywordMap[strings.ToUpper(ident)]; ok {
		return tokType
	}
	return TK_ID
}

// TokenizeAll tokenizes the entire input and returns all tokens (excluding whitespace).
func TokenizeAll(input string) ([]Token, error) {
	lexer := NewLexer(input)
	var tokens []Token

	for {
		tok := lexer.NextToken()
		if tok.Type == TK_SPACE || tok.Type == TK_COMMENT {
			continue
		}
		tokens = append(tokens, tok)
		if tok.Type == TK_EOF {
			break
		}
		if tok.Type == TK_ILLEGAL {
			return tokens, fmt.Errorf("illegal token at line %d, col %d: %q", tok.Line, tok.Col, tok.Lexeme)
		}
	}

	return tokens, nil
}

// isMatchingQuote checks if a string starts and ends with the same quote character.
func isMatchingQuote(s string, quote byte) bool {
	return len(s) >= 2 && s[0] == quote && s[len(s)-1] == quote
}

// unquoteStandard removes standard quotes (', ", `) and unescapes doubled quotes.
func unquoteStandard(s string) string {
	inner := s[1 : len(s)-1]
	quote := string(s[0])
	return strings.ReplaceAll(inner, quote+quote, quote)
}

// Unquote removes quotes from a quoted identifier or string.
func Unquote(s string) string {
	if len(s) < 2 {
		return s
	}

	// Handle standard quote types
	if isMatchingQuote(s, '\'') || isMatchingQuote(s, '"') || isMatchingQuote(s, '`') {
		return unquoteStandard(s)
	}

	// Handle bracketed identifiers
	if s[0] == '[' && s[len(s)-1] == ']' {
		return s[1 : len(s)-1]
	}

	return s
}

// IsIdentChar returns true if the rune can be part of an unquoted identifier.
func IsIdentChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '$'
}
