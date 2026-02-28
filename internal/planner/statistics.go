package planner

import (
	"fmt"
	"strconv"
	"strings"
)

// Statistics holds statistical information for query optimization.
// This is based on SQLite's sqlite_stat1 table format.
type Statistics struct {
	// TableStats maps table name to table statistics
	TableStats map[string]*TableStatistics

	// IndexStats maps index name to index statistics
	IndexStats map[string]*IndexStatistics
}

// TableStatistics contains statistics for a table.
type TableStatistics struct {
	// TableName is the name of the table
	TableName string

	// RowCount is the estimated number of rows in the table
	RowCount int64

	// RowLogEst is the LogEst version of RowCount
	RowLogEst LogEst
}

// IndexStatistics contains statistics for an index.
type IndexStatistics struct {
	// IndexName is the name of the index
	IndexName string

	// TableName is the name of the table this index belongs to
	TableName string

	// Stat is the sqlite_stat1 format statistics string
	// Format: "nRow nEq1 nEq2 ... nEqN"
	// where nRow is total rows, nEqI is average rows per distinct value
	// for the first I columns
	Stat string

	// RowCount is the total number of rows in the table
	RowCount int64

	// SampleSize is the number of rows sampled for statistics
	SampleSize int64

	// AvgEq contains average number of rows with the same value
	// for prefixes of the index columns
	// AvgEq[0] is for the first column, AvgEq[1] for first two columns, etc.
	AvgEq []int64

	// ColumnStats contains LogEst estimates for each column prefix
	// ColumnStats[i] is the estimated distinct values for the first i+1 columns
	ColumnStats []LogEst
}

// NewStatistics creates a new empty Statistics object.
func NewStatistics() *Statistics {
	return &Statistics{
		TableStats: make(map[string]*TableStatistics),
		IndexStats: make(map[string]*IndexStatistics),
	}
}

// LoadStatistics reads statistics from the sqlite_stat1 table.
// The sqlite_stat1 table has the schema:
//
//	CREATE TABLE sqlite_stat1(tbl, idx, stat);
//
// where:
//   - tbl is the table name
//   - idx is the index name (or NULL for table stats)
//   - stat is a space-separated list of integers
func LoadStatistics(rows []StatRow) (*Statistics, error) {
	stats := NewStatistics()

	for _, row := range rows {
		if row.Idx == "" || row.Idx == "null" || row.Idx == "NULL" {
			// Table-level statistics
			if err := stats.loadTableStats(row.Tbl, row.Stat); err != nil {
				return nil, fmt.Errorf("failed to load table stats for %s: %w", row.Tbl, err)
			}
		} else {
			// Index-level statistics
			if err := stats.loadIndexStats(row.Tbl, row.Idx, row.Stat); err != nil {
				return nil, fmt.Errorf("failed to load index stats for %s.%s: %w", row.Tbl, row.Idx, err)
			}
		}
	}

	return stats, nil
}

// StatRow represents a row from sqlite_stat1.
type StatRow struct {
	Tbl  string // Table name
	Idx  string // Index name (or empty/NULL for table stats)
	Stat string // Statistics string
}

// loadTableStats parses and loads table-level statistics.
func (s *Statistics) loadTableStats(tableName string, statString string) error {
	parts := strings.Fields(statString)
	if len(parts) == 0 {
		return fmt.Errorf("empty stat string")
	}

	rowCount, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid row count: %w", err)
	}

	s.TableStats[tableName] = &TableStatistics{
		TableName: tableName,
		RowCount:  rowCount,
		RowLogEst: NewLogEst(rowCount),
	}

	return nil
}

// loadIndexStats parses and loads index-level statistics.
func (s *Statistics) loadIndexStats(tableName, indexName, statString string) error {
	parts := strings.Fields(statString)
	if len(parts) == 0 {
		return fmt.Errorf("empty stat string")
	}

	// First number is total row count
	rowCount, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid row count: %w", err)
	}

	// Remaining numbers are average rows per distinct value for each column prefix
	avgEq := make([]int64, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		avgEq[i-1], err = strconv.ParseInt(parts[i], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid avgEq value at position %d: %w", i, err)
		}
	}

	// Convert to LogEst estimates
	columnStats := make([]LogEst, len(avgEq))
	for i, avg := range avgEq {
		if avg > 0 {
			// Estimate distinct values: rowCount / avgEq
			distinctValues := rowCount / avg
			if distinctValues < 1 {
				distinctValues = 1
			}
			columnStats[i] = NewLogEst(distinctValues)
		} else {
			columnStats[i] = NewLogEst(rowCount)
		}
	}

	s.IndexStats[indexName] = &IndexStatistics{
		IndexName:   indexName,
		TableName:   tableName,
		Stat:        statString,
		RowCount:    rowCount,
		SampleSize:  rowCount, // Assume full table scan for now
		AvgEq:       avgEq,
		ColumnStats: columnStats,
	}

	return nil
}

// SaveStatistics converts statistics to sqlite_stat1 format rows.
func SaveStatistics(stats *Statistics) []StatRow {
	rows := make([]StatRow, 0)

	// Save table statistics
	for _, tableStat := range stats.TableStats {
		rows = append(rows, StatRow{
			Tbl:  tableStat.TableName,
			Idx:  "",
			Stat: fmt.Sprintf("%d", tableStat.RowCount),
		})
	}

	// Save index statistics
	for _, indexStat := range stats.IndexStats {
		rows = append(rows, StatRow{
			Tbl:  indexStat.TableName,
			Idx:  indexStat.IndexName,
			Stat: indexStat.Stat,
		})
	}

	return rows
}

// GetTableStats returns statistics for a table.
func (s *Statistics) GetTableStats(tableName string) *TableStatistics {
	return s.TableStats[tableName]
}

// GetIndexStats returns statistics for an index.
func (s *Statistics) GetIndexStats(indexName string) *IndexStatistics {
	return s.IndexStats[indexName]
}

// EstimateRows estimates the number of rows that will be returned
// for a query using the given index with nEq equality constraints.
func EstimateRows(indexStats *IndexStatistics, nEq int, hasRange bool) LogEst {
	if indexStats == nil {
		// No statistics available, use default estimate
		return LogEst(100) // Assume ~1000 rows
	}

	// Start with total row count
	nOut := NewLogEst(indexStats.RowCount)

	// Apply selectivity for equality constraints
	if nEq > 0 {
		if nEq <= len(indexStats.ColumnStats) {
			// Use actual statistics
			nOut = indexStats.ColumnStats[nEq-1]
		} else {
			// Extrapolate from last available stat
			lastStat := indexStats.ColumnStats[len(indexStats.ColumnStats)-1]
			// Each additional equality reduces by ~10x
			for i := len(indexStats.ColumnStats); i < nEq; i++ {
				lastStat += selectivityEq
				if lastStat < 0 {
					lastStat = 0
					break
				}
			}
			nOut = lastStat
		}
	}

	// Apply selectivity for range constraint
	if hasRange {
		nOut += selectivityRange
		if nOut < 0 {
			nOut = 0
		}
	}

	return nOut
}

// EstimateSelectivity estimates the selectivity of a WHERE term.
// Returns a LogEst value representing the fraction of rows that match.
// Negative values mean fewer rows (more selective).
func EstimateSelectivity(term *WhereTerm, stats *Statistics) LogEst {
	// If we have specific statistics for this term, use them
	if term.Operator == WO_EQ {
		return estimateEqualitySelectivity(term, stats)
	}

	if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
		return estimateRangeSelectivity(term, stats)
	}

	if term.Operator == WO_IN {
		return estimateInSelectivity(term, stats)
	}

	if term.Operator == WO_ISNULL {
		return selectivityNull
	}

	// Default selectivity
	return truthProbDefault
}

// estimateEqualitySelectivity estimates selectivity for equality constraints.
func estimateEqualitySelectivity(term *WhereTerm, stats *Statistics) LogEst {
	// Check if the value is a common small integer
	if val, ok := term.RightValue.(int); ok {
		if val >= -1 && val <= 1 {
			return truthProbSmallInt
		}
	}

	// Check for string pattern matching
	if val, ok := term.RightValue.(string); ok {
		if len(val) == 0 {
			return selectivityEq // Empty strings are relatively common
		}
	}

	// Default equality selectivity
	return selectivityEq
}

// estimateRangeSelectivity estimates selectivity for range constraints.
func estimateRangeSelectivity(term *WhereTerm, stats *Statistics) LogEst {
	// Range queries typically match about 1/8 of rows
	// This is a heuristic - more sophisticated analysis could look at
	// the actual range bounds and distribution
	return selectivityRange
}

// estimateInSelectivity estimates selectivity for IN operator.
func estimateInSelectivity(term *WhereTerm, stats *Statistics) LogEst {
	// IN operator selectivity depends on list size
	// Default assumption: list of ~5 items
	return selectivityIn
}

// ApplyStatisticsToTable applies loaded statistics to a TableInfo object.
func ApplyStatisticsToTable(table *TableInfo, stats *Statistics) {
	if tableStat := stats.GetTableStats(table.Name); tableStat != nil {
		table.RowCount = tableStat.RowCount
		table.RowLogEst = tableStat.RowLogEst
	}

	// Apply statistics to indexes
	for _, index := range table.Indexes {
		if indexStat := stats.GetIndexStats(index.Name); indexStat != nil {
			index.RowCount = indexStat.RowCount
			index.RowLogEst = NewLogEst(indexStat.RowCount)
			index.ColumnStats = indexStat.ColumnStats
		}
	}
}

// ApplyStatisticsToIndex applies loaded statistics to an IndexInfo object.
func ApplyStatisticsToIndex(index *IndexInfo, stats *Statistics) {
	if indexStat := stats.GetIndexStats(index.Name); indexStat != nil {
		index.RowCount = indexStat.RowCount
		index.RowLogEst = NewLogEst(indexStat.RowCount)
		index.ColumnStats = indexStat.ColumnStats
	}
}

// ComputeTableStatistics computes statistics for a table by scanning it.
// This is used during ANALYZE command execution.
func ComputeTableStatistics(tableName string, rowCount int64) *TableStatistics {
	return &TableStatistics{
		TableName: tableName,
		RowCount:  rowCount,
		RowLogEst: NewLogEst(rowCount),
	}
}

// ComputeIndexStatistics computes statistics for an index by sampling.
// The algorithm:
// 1. Scan the index and count total rows
// 2. For each column prefix, estimate the number of distinct values
// 3. Compute avgEq = rowCount / distinctValues for each prefix
func ComputeIndexStatistics(
	tableName string,
	indexName string,
	rowCount int64,
	distinctCounts []int64,
) *IndexStatistics {
	avgEq := make([]int64, len(distinctCounts))
	columnStats := make([]LogEst, len(distinctCounts))

	for i, distinctCount := range distinctCounts {
		if distinctCount > 0 {
			avgEq[i] = rowCount / distinctCount
			if avgEq[i] < 1 {
				avgEq[i] = 1
			}
			columnStats[i] = NewLogEst(distinctCount)
		} else {
			avgEq[i] = rowCount
			columnStats[i] = 0
		}
	}

	// Build stat string
	statParts := []string{fmt.Sprintf("%d", rowCount)}
	for _, avg := range avgEq {
		statParts = append(statParts, fmt.Sprintf("%d", avg))
	}
	statString := strings.Join(statParts, " ")

	return &IndexStatistics{
		IndexName:   indexName,
		TableName:   tableName,
		Stat:        statString,
		RowCount:    rowCount,
		SampleSize:  rowCount,
		AvgEq:       avgEq,
		ColumnStats: columnStats,
	}
}

// EstimateDistinctValues estimates the number of distinct values in a column.
// This is a helper function for computing index statistics during ANALYZE.
// Returns a rough estimate based on sampling.
func EstimateDistinctValues(sampleSize int64, uniqueInSample int64, totalRows int64) int64 {
	if sampleSize == 0 || uniqueInSample == 0 {
		return 1
	}

	// If we sampled the entire table, return exact count
	if sampleSize >= totalRows {
		return uniqueInSample
	}

	// Extrapolate from sample
	// Simple formula: estimated = (uniqueInSample / sampleSize) * totalRows
	ratio := float64(uniqueInSample) / float64(sampleSize)
	estimated := int64(ratio * float64(totalRows))

	// Apply correction factor for small samples
	// (Smaller samples tend to underestimate diversity)
	if sampleSize < totalRows/10 {
		correction := 1.0 + (float64(totalRows)/float64(sampleSize))*0.1
		estimated = int64(float64(estimated) * correction)
	}

	// Bounds checking
	if estimated < uniqueInSample {
		estimated = uniqueInSample
	}
	if estimated > totalRows {
		estimated = totalRows
	}

	return estimated
}

// MergeStatistics merges two Statistics objects.
// This is useful when updating statistics incrementally.
func MergeStatistics(s1, s2 *Statistics) *Statistics {
	merged := NewStatistics()

	// Merge table statistics (s2 overwrites s1)
	for name, stat := range s1.TableStats {
		merged.TableStats[name] = stat
	}
	for name, stat := range s2.TableStats {
		merged.TableStats[name] = stat
	}

	// Merge index statistics (s2 overwrites s1)
	for name, stat := range s1.IndexStats {
		merged.IndexStats[name] = stat
	}
	for name, stat := range s2.IndexStats {
		merged.IndexStats[name] = stat
	}

	return merged
}

// ClearStatistics clears all statistics for a table and its indexes.
func (s *Statistics) ClearStatistics(tableName string) {
	// Remove table statistics
	delete(s.TableStats, tableName)

	// Remove index statistics
	toDelete := make([]string, 0)
	for indexName, indexStat := range s.IndexStats {
		if indexStat.TableName == tableName {
			toDelete = append(toDelete, indexName)
		}
	}
	for _, indexName := range toDelete {
		delete(s.IndexStats, indexName)
	}
}

// ValidateStatistics performs sanity checks on statistics.
func (s *Statistics) ValidateStatistics() error {
	// Check table statistics
	for name, stat := range s.TableStats {
		if stat.RowCount < 0 {
			return fmt.Errorf("table %s has negative row count: %d", name, stat.RowCount)
		}
	}

	// Check index statistics
	for name, stat := range s.IndexStats {
		if stat.RowCount < 0 {
			return fmt.Errorf("index %s has negative row count: %d", name, stat.RowCount)
		}

		// Check that avgEq values are reasonable
		for i, avg := range stat.AvgEq {
			if avg < 1 {
				return fmt.Errorf("index %s has invalid avgEq[%d]: %d", name, i, avg)
			}
			if avg > stat.RowCount {
				return fmt.Errorf("index %s has avgEq[%d]=%d > rowCount=%d", name, i, avg, stat.RowCount)
			}
		}
	}

	return nil
}

// String returns a human-readable representation of statistics.
func (s *Statistics) String() string {
	var sb strings.Builder

	sb.WriteString("Statistics:\n")

	// Table statistics
	if len(s.TableStats) > 0 {
		sb.WriteString("  Tables:\n")
		for name, stat := range s.TableStats {
			sb.WriteString(fmt.Sprintf("    %s: %d rows\n", name, stat.RowCount))
		}
	}

	// Index statistics
	if len(s.IndexStats) > 0 {
		sb.WriteString("  Indexes:\n")
		for name, stat := range s.IndexStats {
			sb.WriteString(fmt.Sprintf("    %s.%s: %s\n", stat.TableName, name, stat.Stat))
		}
	}

	return sb.String()
}
