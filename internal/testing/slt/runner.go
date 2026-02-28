package slt

import (
	"bufio"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Runner executes SQL Logic Tests from .test files
type Runner struct {
	db              *sql.DB
	hashThreshold   int // Number of rows above which to use hash comparison
	skipOnError     bool
	verbose         bool
	currentFile     string
	lineNumber      int
	totalTests      int
	passedTests     int
	failedTests     int
	skippedTests    int
}

// NewRunner creates a new SLT runner with the given database connection
func NewRunner(db *sql.DB) *Runner {
	return &Runner{
		db:            db,
		hashThreshold: 100, // Default from SQLite SLT
		skipOnError:   false,
		verbose:       false,
	}
}

// SetHashThreshold sets the number of rows above which to use hash comparison
func (r *Runner) SetHashThreshold(threshold int) {
	r.hashThreshold = threshold
}

// SetSkipOnError sets whether to skip remaining tests after an error
func (r *Runner) SetSkipOnError(skip bool) {
	r.skipOnError = skip
}

// SetVerbose sets whether to print verbose output
func (r *Runner) SetVerbose(verbose bool) {
	r.verbose = verbose
}

// TestResult represents the result of running a single test
type TestResult struct {
	File       string
	Line       int
	TestType   string // "statement", "query", "hash-threshold"
	SQL        string
	Expected   string
	Actual     string
	Passed     bool
	Error      error
}

// RunFile executes all tests in a .test file
func (r *Runner) RunFile(filename string) ([]TestResult, error) {
	r.currentFile = filename
	r.lineNumber = 0

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open test file: %w", err)
	}
	defer file.Close()

	return r.runTests(file)
}

// RunString executes tests from a string (useful for testing)
func (r *Runner) RunString(content string) ([]TestResult, error) {
	r.currentFile = "<string>"
	r.lineNumber = 0

	return r.runTests(strings.NewReader(content))
}

// runTests executes tests from an io.Reader
func (r *Runner) runTests(reader io.Reader) ([]TestResult, error) {
	scanner := bufio.NewScanner(reader)
	var results []TestResult
	var currentTest *Test
	var currentSQL strings.Builder

	for scanner.Scan() {
		r.lineNumber++
		line := scanner.Text()

		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		// Handle directives that complete the previous test and start a new one
		if r.handleStatementDirective(line, &currentTest, &currentSQL, &results) {
			continue
		}
		if r.handleQueryDirective(line, &currentTest, &currentSQL, &results) {
			continue
		}
		if r.handleHashDirective(line, &results) {
			continue
		}

		// Accumulate SQL or expected results for current test
		r.accumulateTestData(line, currentTest, &currentSQL)
	}

	// Execute final test if exists
	if currentTest != nil {
		result := r.executeTest(currentTest, currentSQL.String())
		results = append(results, result)
	}

	if err := scanner.Err(); err != nil {
		return results, fmt.Errorf("error reading test file: %w", err)
	}

	return results, nil
}

// handleStatementDirective processes statement directive lines
func (r *Runner) handleStatementDirective(line string, currentTest **Test, currentSQL *strings.Builder, results *[]TestResult) bool {
	if !strings.HasPrefix(line, "statement") {
		return false
	}

	// Execute previous test if exists
	if *currentTest != nil {
		result := r.executeTest(*currentTest, currentSQL.String())
		*results = append(*results, result)
	}

	// Start new statement test
	*currentTest = &Test{
		Type: "statement",
		Line: r.lineNumber,
	}
	(*currentTest).parseStatementDirective(line)
	currentSQL.Reset()
	return true
}

// handleQueryDirective processes query directive lines
func (r *Runner) handleQueryDirective(line string, currentTest **Test, currentSQL *strings.Builder, results *[]TestResult) bool {
	if !strings.HasPrefix(line, "query") {
		return false
	}

	// Execute previous test if exists
	if *currentTest != nil {
		result := r.executeTest(*currentTest, currentSQL.String())
		*results = append(*results, result)
	}

	// Start new query test
	*currentTest = &Test{
		Type: "query",
		Line: r.lineNumber,
	}
	(*currentTest).parseQueryDirective(line)
	currentSQL.Reset()
	return true
}

// handleHashDirective processes hash-threshold directive lines
func (r *Runner) handleHashDirective(line string, results *[]TestResult) bool {
	if !strings.HasPrefix(line, "hash-threshold") {
		return false
	}

	result := r.handleHashThreshold(line)
	*results = append(*results, result)
	return true
}

// accumulateTestData accumulates SQL or expected results for the current test
func (r *Runner) accumulateTestData(line string, currentTest *Test, currentSQL *strings.Builder) {
	if currentTest == nil {
		return
	}

	if currentTest.State == "sql" {
		r.accumulateSQLOrResults(line, currentTest, currentSQL)
	} else if currentTest.State == "results" {
		currentTest.Expected = append(currentTest.Expected, line)
	}
}

// accumulateSQLOrResults accumulates SQL lines or transitions to results state
func (r *Runner) accumulateSQLOrResults(line string, currentTest *Test, currentSQL *strings.Builder) {
	// Check if this line starts expected results (for queries)
	if currentTest.Type == "query" && strings.HasPrefix(line, "----") {
		currentTest.State = "results"
		return
	}

	// Accumulate SQL
	if currentSQL.Len() > 0 {
		currentSQL.WriteString("\n")
	}
	currentSQL.WriteString(line)
}

// Test represents a single SLT test
type Test struct {
	Type         string   // "statement" or "query"
	Line         int
	ExpectOK     bool     // For statements: expect success
	ExpectError  bool     // For statements: expect error
	ColumnTypes  string   // For queries: I=int, T=text, R=real
	SortMode     string   // For queries: nosort, rowsort, valuesort
	Label        string   // Optional test label
	Expected     []string // Expected results (for queries)
	State        string   // "sql" or "results"
}

// parseStatementDirective parses a statement directive line
// Format: statement [ok|error] [label]
func (t *Test) parseStatementDirective(line string) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		t.ExpectOK = true
		t.State = "sql"
		return
	}

	switch parts[1] {
	case "ok":
		t.ExpectOK = true
	case "error":
		t.ExpectError = true
	default:
		// If not ok/error, treat as label
		t.ExpectOK = true
		t.Label = parts[1]
	}

	if len(parts) > 2 {
		t.Label = parts[2]
	}

	t.State = "sql"
}

// parseQueryDirective parses a query directive line
// Format: query <types> [sortmode] [label]
func (t *Test) parseQueryDirective(line string) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		t.State = "sql"
		return
	}

	t.ColumnTypes = parts[1]

	if len(parts) > 2 {
		// Check if it's a sort mode
		if strings.Contains(parts[2], "sort") {
			t.SortMode = parts[2]
			if len(parts) > 3 {
				t.Label = parts[3]
			}
		} else {
			t.Label = parts[2]
		}
	}

	t.State = "sql"
}

// handleHashThreshold processes a hash-threshold directive
func (r *Runner) handleHashThreshold(line string) TestResult {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return TestResult{
			File:     r.currentFile,
			Line:     r.lineNumber,
			TestType: "hash-threshold",
			Passed:   false,
			Error:    fmt.Errorf("invalid hash-threshold directive: %s", line),
		}
	}

	threshold, err := strconv.Atoi(parts[1])
	if err != nil {
		return TestResult{
			File:     r.currentFile,
			Line:     r.lineNumber,
			TestType: "hash-threshold",
			Passed:   false,
			Error:    fmt.Errorf("invalid hash-threshold value: %w", err),
		}
	}

	r.hashThreshold = threshold

	return TestResult{
		File:     r.currentFile,
		Line:     r.lineNumber,
		TestType: "hash-threshold",
		Expected: strconv.Itoa(threshold),
		Actual:   strconv.Itoa(threshold),
		Passed:   true,
	}
}

// executeTest executes a single test
func (r *Runner) executeTest(test *Test, sql string) TestResult {
	r.totalTests++

	result := TestResult{
		File:     r.currentFile,
		Line:     test.Line,
		TestType: test.Type,
		SQL:      sql,
	}

	sql = strings.TrimSpace(sql)
	if sql == "" {
		result.Error = fmt.Errorf("empty SQL statement")
		result.Passed = false
		r.failedTests++
		return result
	}

	if test.Type == "statement" {
		return r.executeStatement(test, sql, result)
	} else if test.Type == "query" {
		return r.executeQuery(test, sql, result)
	}

	result.Error = fmt.Errorf("unknown test type: %s", test.Type)
	result.Passed = false
	r.failedTests++
	return result
}

// executeStatement executes a statement test
func (r *Runner) executeStatement(test *Test, sql string, result TestResult) TestResult {
	ctx := context.Background()
	_, err := r.db.ExecContext(ctx, sql)

	if test.ExpectError {
		if err != nil {
			result.Expected = "error"
			result.Actual = "error"
			result.Passed = true
			r.passedTests++
		} else {
			result.Expected = "error"
			result.Actual = "ok"
			result.Passed = false
			result.Error = fmt.Errorf("expected error but got success")
			r.failedTests++
		}
	} else {
		if err != nil {
			result.Expected = "ok"
			result.Actual = "error"
			result.Passed = false
			result.Error = err
			r.failedTests++
		} else {
			result.Expected = "ok"
			result.Actual = "ok"
			result.Passed = true
			r.passedTests++
		}
	}

	return result
}

// executeQuery executes a query test
func (r *Runner) executeQuery(test *Test, sql string, result TestResult) TestResult {
	ctx := context.Background()
	rows, err := r.db.QueryContext(ctx, sql)
	if err != nil {
		result.Expected = formatExpected(test.Expected)
		result.Actual = "error"
		result.Passed = false
		result.Error = err
		r.failedTests++
		return result
	}
	defer rows.Close()

	// Get column count
	columns, err := rows.Columns()
	if err != nil {
		result.Error = err
		result.Passed = false
		r.failedTests++
		return result
	}

	// Validate column count matches expected types
	if len(test.ColumnTypes) != len(columns) {
		result.Expected = formatExpected(test.Expected)
		result.Actual = fmt.Sprintf("column count mismatch: expected %d, got %d",
			len(test.ColumnTypes), len(columns))
		result.Passed = false
		result.Error = fmt.Errorf("column count mismatch")
		r.failedTests++
		return result
	}

	// Collect all rows
	var actualRows [][]string
	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			result.Error = err
			result.Passed = false
			r.failedTests++
			return result
		}

		// Convert values to strings
		row := make([]string, len(columns))
		for i, val := range values {
			row[i] = formatValue(val)
		}
		actualRows = append(actualRows, row)
	}

	if err := rows.Err(); err != nil {
		result.Error = err
		result.Passed = false
		r.failedTests++
		return result
	}

	// Apply sorting if specified
	actualRows = r.applySorting(actualRows, test.SortMode)

	// Check if we should use hash comparison
	if len(actualRows) > r.hashThreshold {
		return r.compareWithHash(test, actualRows, result)
	}

	// Compare results
	return r.compareResults(test, actualRows, result)
}

// applySorting applies the specified sort mode to the result rows
func (r *Runner) applySorting(rows [][]string, sortMode string) [][]string {
	if sortMode == "nosort" || sortMode == "" {
		return rows
	}

	if sortMode == "rowsort" {
		// Sort by entire rows
		sort.Slice(rows, func(i, j int) bool {
			return strings.Join(rows[i], "\t") < strings.Join(rows[j], "\t")
		})
	} else if sortMode == "valuesort" {
		// Flatten all values and sort them
		var values []string
		for _, row := range rows {
			values = append(values, row...)
		}
		sort.Strings(values)

		// Reconstruct rows from sorted values
		colCount := len(rows[0])
		rows = make([][]string, 0)
		for i := 0; i < len(values); i += colCount {
			end := i + colCount
			if end > len(values) {
				end = len(values)
			}
			rows = append(rows, values[i:end])
		}
	}

	return rows
}

// compareResults compares actual results with expected results
func (r *Runner) compareResults(test *Test, actualRows [][]string, result TestResult) TestResult {
	// Format expected and actual
	result.Expected = formatExpected(test.Expected)
	result.Actual = formatActual(actualRows)

	// Compare
	if result.Expected == result.Actual {
		result.Passed = true
		r.passedTests++
	} else {
		result.Passed = false
		result.Error = fmt.Errorf("result mismatch")
		r.failedTests++
	}

	return result
}

// compareWithHash compares results using MD5 hash
func (r *Runner) compareWithHash(test *Test, actualRows [][]string, result TestResult) TestResult {
	// The expected result should be a single hash value
	expectedHash := ""
	if len(test.Expected) > 0 {
		expectedHash = strings.TrimSpace(test.Expected[0])
	}

	// Calculate actual hash
	actualHash := calculateHash(actualRows)

	result.Expected = expectedHash
	result.Actual = actualHash

	if expectedHash == actualHash {
		result.Passed = true
		r.passedTests++
	} else {
		result.Passed = false
		result.Error = fmt.Errorf("hash mismatch")
		r.failedTests++
	}

	return result
}

// formatValue converts a database value to a string for comparison
func formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		// Format float in a consistent way
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatExpected formats the expected results for display
func formatExpected(expected []string) string {
	return strings.Join(expected, "\n")
}

// formatActual formats the actual results for display
func formatActual(rows [][]string) string {
	var lines []string
	for _, row := range rows {
		lines = append(lines, strings.Join(row, "\t"))
	}
	return strings.Join(lines, "\n")
}

// calculateHash calculates MD5 hash of the result rows
func calculateHash(rows [][]string) string {
	h := md5.New()
	for _, row := range rows {
		for _, val := range row {
			h.Write([]byte(val))
			h.Write([]byte("\n"))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// GetStats returns test statistics
func (r *Runner) GetStats() (total, passed, failed, skipped int) {
	return r.totalTests, r.passedTests, r.failedTests, r.skippedTests
}

// ResetStats resets test statistics
func (r *Runner) ResetStats() {
	r.totalTests = 0
	r.passedTests = 0
	r.failedTests = 0
	r.skippedTests = 0
}

// PrintSummary prints a summary of test results
func (r *Runner) PrintSummary() {
	fmt.Printf("\nTest Summary:\n")
	fmt.Printf("  Total:   %d\n", r.totalTests)
	fmt.Printf("  Passed:  %d\n", r.passedTests)
	fmt.Printf("  Failed:  %d\n", r.failedTests)
	fmt.Printf("  Skipped: %d\n", r.skippedTests)
	if r.totalTests > 0 {
		passRate := float64(r.passedTests) / float64(r.totalTests) * 100
		fmt.Printf("  Pass Rate: %.2f%%\n", passRate)
	}
}
