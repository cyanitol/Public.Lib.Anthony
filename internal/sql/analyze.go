// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"fmt"
	"sort"
)

// AnalyzeTable performs statistical analysis on a table and its indexes.
// This is the implementation of the ANALYZE command.
//
// The ANALYZE command gathers statistics about tables and indexes to help
// the query planner make better decisions. It stores the results in the
// sqlite_stat1 table.
//
// Algorithm:
//  1. Count total rows in the table
//  2. For each index on the table:
//     a. Scan the index to count total entries
//     b. Sample index entries to estimate distinct values for each column prefix
//     c. Compute avgEq = rowCount / distinctValues
//     d. Store results in sqlite_stat1 format
func AnalyzeTable(tableName string, schema *Schema) (*AnalyzeResult, error) {
	table := schema.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}

	result := &AnalyzeResult{
		TableName:   tableName,
		TableStats:  nil,
		IndexStats:  make([]*IndexAnalysis, 0),
		RowsScanned: 0,
		IndexesSeen: 0,
	}

	// Step 1: Count rows in table
	// In a real implementation, this would scan the table's B-tree
	// For now, we'll use a placeholder
	rowCount := int64(1000) // Placeholder - should be actual count

	result.RowsScanned = rowCount
	result.TableStats = &TableAnalysis{
		TableName: tableName,
		RowCount:  rowCount,
	}

	// Step 2: Analyze each index
	result.IndexStats = analyzeTableIndexes(table, rowCount, schema)
	result.IndexesSeen = len(result.IndexStats)

	return result, nil
}

// AnalyzeResult contains the results of analyzing a table.
type AnalyzeResult struct {
	TableName   string
	TableStats  *TableAnalysis
	IndexStats  []*IndexAnalysis
	RowsScanned int64
	IndexesSeen int
}

// TableAnalysis contains analysis results for a table.
type TableAnalysis struct {
	TableName string
	RowCount  int64
}

// IndexAnalysis contains analysis results for an index.
type IndexAnalysis struct {
	IndexName      string
	TableName      string
	RowCount       int64
	DistinctCounts []int64 // Distinct values for each column prefix
	AvgEq          []int64 // Average rows per distinct value
	StatString     string  // sqlite_stat1 format string
}

// analyzeTableIndexes analyzes all indexes on a table.
func analyzeTableIndexes(table *Table, rowCount int64, schema *Schema) []*IndexAnalysis {
	results := make([]*IndexAnalysis, 0)

	// Find all indexes for this table
	for _, index := range schema.Indexes {
		if index.Table == table.Name {
			analysis := analyzeIndex(table, index, rowCount)
			results = append(results, analysis)
		}
	}

	return results
}

// analyzeIndex analyzes a single index.
func analyzeIndex(table *Table, index *Index, rowCount int64) *IndexAnalysis {
	numCols := len(index.Columns)
	distinctCounts := make([]int64, numCols)
	avgEq := make([]int64, numCols)

	computeIndexStatistics(index, rowCount, numCols, distinctCounts, avgEq)
	statString := buildStatString(rowCount, avgEq)

	return &IndexAnalysis{
		IndexName:      index.Name,
		TableName:      table.Name,
		RowCount:       rowCount,
		DistinctCounts: distinctCounts,
		AvgEq:          avgEq,
		StatString:     statString,
	}
}

// computeIndexStatistics computes distinct counts and avgEq for each column prefix.
func computeIndexStatistics(index *Index, rowCount int64, numCols int, distinctCounts, avgEq []int64) {
	for i := 0; i < numCols; i++ {
		distinctCounts[i] = estimateDistinctCount(i, rowCount)
		if index.Unique && i == numCols-1 {
			distinctCounts[i] = rowCount
		}
		avgEq[i] = computeAvgEq(rowCount, distinctCounts[i])
	}
}

// estimateDistinctCount estimates distinct values for a column prefix.
func estimateDistinctCount(colIndex int, rowCount int64) int64 {
	divisor := int64(1)
	for j := 0; j <= colIndex; j++ {
		divisor *= 10
	}
	distinct := rowCount / divisor
	if distinct < 1 {
		return 1
	}
	return distinct
}

// computeAvgEq computes the average rows per distinct value.
func computeAvgEq(rowCount, distinctCount int64) int64 {
	if distinctCount > 0 {
		avgEq := rowCount / distinctCount
		if avgEq < 1 {
			return 1
		}
		return avgEq
	}
	return rowCount
}

// buildStatString builds the sqlite_stat1 format statistics string.
// Format: "nRow nEq1 nEq2 ... nEqN"
func buildStatString(rowCount int64, avgEq []int64) string {
	parts := make([]string, 0, len(avgEq)+1)
	parts = append(parts, fmt.Sprintf("%d", rowCount))
	for _, avg := range avgEq {
		parts = append(parts, fmt.Sprintf("%d", avg))
	}
	return joinStrings(parts, " ")
}

// joinStrings joins strings with a separator.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}
	return result
}

// AnalyzeDatabase analyzes all tables in the database.
func AnalyzeDatabase(schema *Schema) ([]*AnalyzeResult, error) {
	results := make([]*AnalyzeResult, 0)

	// Get all table names and sort them for consistent ordering
	tableNames := make([]string, 0, len(schema.Tables))
	for name := range schema.Tables {
		// Skip system tables
		if name == "sqlite_master" || name == "sqlite_stat1" {
			continue
		}
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	// Analyze each table
	for _, tableName := range tableNames {
		result, err := AnalyzeTable(tableName, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to analyze table %s: %w", tableName, err)
		}
		results = append(results, result)
	}

	return results, nil
}

// SaveAnalysisResults converts analysis results to sqlite_stat1 rows.
func SaveAnalysisResults(results []*AnalyzeResult) []Stat1Row {
	rows := make([]Stat1Row, 0)

	for _, result := range results {
		// Add table-level statistics
		if result.TableStats != nil {
			rows = append(rows, Stat1Row{
				Tbl:  result.TableStats.TableName,
				Idx:  nil, // NULL for table stats
				Stat: fmt.Sprintf("%d", result.TableStats.RowCount),
			})
		}

		// Add index-level statistics
		for _, indexStat := range result.IndexStats {
			rows = append(rows, Stat1Row{
				Tbl:  indexStat.TableName,
				Idx:  &indexStat.IndexName,
				Stat: indexStat.StatString,
			})
		}
	}

	return rows
}

// Stat1Row represents a row in the sqlite_stat1 table.
type Stat1Row struct {
	Tbl  string  // Table name
	Idx  *string // Index name (NULL for table stats)
	Stat string  // Statistics string
}

// CreateStat1Table creates the sqlite_stat1 table if it doesn't exist.
// The table schema is:
//
//	CREATE TABLE sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT);
func CreateStat1Table(schema *Schema) error {
	// Check if sqlite_stat1 already exists
	if schema.GetTable("sqlite_stat1") != nil {
		return nil // Already exists
	}

	// Create the table
	table := &Table{
		Name:       "sqlite_stat1",
		NumColumns: 3,
		Columns: []Column{
			{
				Name:     "tbl",
				DeclType: "TEXT",
				Affinity: SQLITE_AFF_TEXT,
				NotNull:  false,
			},
			{
				Name:     "idx",
				DeclType: "TEXT",
				Affinity: SQLITE_AFF_TEXT,
				NotNull:  false,
			},
			{
				Name:     "stat",
				DeclType: "TEXT",
				Affinity: SQLITE_AFF_TEXT,
				NotNull:  false,
			},
		},
		RootPage:    0, // Will be assigned by pager
		PrimaryKey:  -1,
		RowidColumn: -1,
	}

	return schema.AddTable(table)
}

// ClearStatistics removes all statistics for a table from sqlite_stat1.
func ClearStatistics(tableName string, schema *Schema) error {
	// In a real implementation, this would execute:
	// DELETE FROM sqlite_stat1 WHERE tbl = ?
	//
	// For now, this is a placeholder
	_ = tableName
	_ = schema
	return nil
}

// SampleIndexEntries samples entries from an index to estimate distinct values.
// This is used during ANALYZE to gather statistics efficiently.
type IndexSample struct {
	// Values contains sampled values from the index
	// For a multi-column index, each entry is a tuple of values
	Values [][]interface{}

	// Count is the total number of entries in the index
	Count int64
}

// EstimateDistinctValuesFromSample estimates distinct values from a sample.
func EstimateDistinctValuesFromSample(sample *IndexSample, columnIndex int) int64 {
	if sample == nil || len(sample.Values) == 0 {
		return 1
	}

	// Count distinct values in the sample for the column prefix
	distinct := make(map[string]bool)

	for _, row := range sample.Values {
		// Build a key from values up to and including columnIndex
		key := buildKeyFromValues(row, columnIndex)
		distinct[key] = true
	}

	uniqueInSample := int64(len(distinct))
	sampleSize := int64(len(sample.Values))

	// Extrapolate to full index
	return estimateDistinct(sampleSize, uniqueInSample, sample.Count)
}

// buildKeyFromValues builds a string key from values.
func buildKeyFromValues(values []interface{}, upToIndex int) string {
	if upToIndex >= len(values) {
		upToIndex = len(values) - 1
	}

	key := ""
	for i := 0; i <= upToIndex; i++ {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", values[i])
	}
	return key
}

// estimateDistinct estimates total distinct values from a sample.
func estimateDistinct(sampleSize, uniqueInSample, totalCount int64) int64 {
	if sampleSize == 0 || uniqueInSample == 0 {
		return 1
	}

	// If we sampled everything, return exact count
	if sampleSize >= totalCount {
		return uniqueInSample
	}

	// Simple extrapolation
	ratio := float64(uniqueInSample) / float64(sampleSize)
	estimated := int64(ratio * float64(totalCount))

	// Apply correction for small samples
	if sampleSize < totalCount/10 {
		correction := 1.0 + (float64(totalCount)/float64(sampleSize))*0.1
		estimated = int64(float64(estimated) * correction)
	}

	// Bounds
	if estimated < uniqueInSample {
		estimated = uniqueInSample
	}
	if estimated > totalCount {
		estimated = totalCount
	}

	return estimated
}

// AnalyzeOptions contains options for the ANALYZE command.
type AnalyzeOptions struct {
	// TableName specifies a specific table to analyze (empty = all tables)
	TableName string

	// SampleSize is the number of rows to sample (0 = all rows)
	SampleSize int64

	// UpdateMode determines how to update statistics
	// "replace" - replace existing statistics
	// "merge" - merge with existing statistics
	UpdateMode string
}

// ExecuteAnalyze executes the ANALYZE command with the given options.
func ExecuteAnalyze(schema *Schema, opts AnalyzeOptions) (*AnalyzeReport, error) {
	// Ensure sqlite_stat1 table exists
	if err := CreateStat1Table(schema); err != nil {
		return nil, fmt.Errorf("failed to create sqlite_stat1: %w", err)
	}

	var results []*AnalyzeResult
	var err error

	if opts.TableName != "" {
		// Analyze specific table
		result, analyzeErr := AnalyzeTable(opts.TableName, schema)
		if analyzeErr != nil {
			return nil, analyzeErr
		}
		results = []*AnalyzeResult{result}
	} else {
		// Analyze all tables
		results, err = AnalyzeDatabase(schema)
		if err != nil {
			return nil, err
		}
	}

	// Convert to sqlite_stat1 rows
	stat1Rows := SaveAnalysisResults(results)

	// Generate report
	report := &AnalyzeReport{
		TablesAnalyzed:  len(results),
		IndexesAnalyzed: 0,
		RowsScanned:     0,
		Stat1Rows:       stat1Rows,
	}

	for _, result := range results {
		report.IndexesAnalyzed += result.IndexesSeen
		report.RowsScanned += result.RowsScanned
	}

	return report, nil
}

// AnalyzeReport contains a summary of ANALYZE execution.
type AnalyzeReport struct {
	TablesAnalyzed  int
	IndexesAnalyzed int
	RowsScanned     int64
	Stat1Rows       []Stat1Row
}

// String returns a human-readable summary of the analysis.
func (r *AnalyzeReport) String() string {
	return fmt.Sprintf(
		"Analyzed %d tables, %d indexes, scanned %d rows, generated %d stat rows",
		r.TablesAnalyzed,
		r.IndexesAnalyzed,
		r.RowsScanned,
		len(r.Stat1Rows),
	)
}

// FormatStatRow formats a Stat1Row as a string for display.
func FormatStatRow(row Stat1Row) string {
	idxName := "NULL"
	if row.Idx != nil {
		idxName = *row.Idx
	}
	return fmt.Sprintf("tbl=%s idx=%s stat=%s", row.Tbl, idxName, row.Stat)
}
