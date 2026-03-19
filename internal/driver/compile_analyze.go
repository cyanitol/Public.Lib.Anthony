// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileAnalyze compiles an ANALYZE statement.
// ANALYZE collects statistics about tables and indexes and stores them in sqlite_stat1.
func (s *Stmt) compileAnalyze(vm *vdbe.VDBE, stmt *parser.AnalyzeStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Ensure sqlite_stat1 table exists
	if err := s.ensureSqliteStat1Table(); err != nil {
		return nil, fmt.Errorf("failed to create sqlite_stat1: %w", err)
	}

	// Collect and store statistics
	if err := s.collectAndStoreStatistics(stmt); err != nil {
		return nil, fmt.Errorf("ANALYZE failed: %w", err)
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// ensureSqliteStat1Table creates the sqlite_stat1 table if it doesn't exist.
func (s *Stmt) ensureSqliteStat1Table() error {
	if _, exists := s.conn.schema.GetTable("sqlite_stat1"); exists {
		return nil
	}
	return s.conn.ExecDDL("CREATE TABLE sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT)")
}

// collectAndStoreStatistics gathers statistics for tables and indexes.
func (s *Stmt) collectAndStoreStatistics(stmt *parser.AnalyzeStmt) error {
	// Clear existing statistics
	if err := s.clearStatistics(stmt); err != nil {
		return err
	}

	tables := s.resolveTablesToAnalyze(stmt)
	for _, tbl := range tables {
		if err := s.analyzeTable(tbl); err != nil {
			return err
		}
	}
	return nil
}

// clearStatistics removes old statistics for the targeted tables.
func (s *Stmt) clearStatistics(stmt *parser.AnalyzeStmt) error {
	if stmt.Name != "" {
		_, err := s.conn.ExecDML("DELETE FROM sqlite_stat1 WHERE tbl = ?", stmt.Name)
		return err
	}
	_, err := s.conn.ExecDML("DELETE FROM sqlite_stat1")
	return err
}

// resolveTablesToAnalyze determines which tables to analyze.
func (s *Stmt) resolveTablesToAnalyze(stmt *parser.AnalyzeStmt) []*schema.Table {
	if stmt.Name != "" {
		return s.resolveNamedTarget(stmt.Name)
	}
	return s.allUserTables()
}

// resolveNamedTarget resolves a named ANALYZE target (table or index).
func (s *Stmt) resolveNamedTarget(name string) []*schema.Table {
	// Try as table name first
	if tbl, ok := s.conn.schema.GetTable(name); ok {
		return []*schema.Table{tbl}
	}
	// Try as index name — analyze the index's parent table
	if idx, ok := s.conn.schema.GetIndex(name); ok {
		if tbl, ok := s.conn.schema.GetTable(idx.Table); ok {
			return []*schema.Table{tbl}
		}
	}
	return nil
}

// allUserTables returns all non-system, non-virtual tables.
func (s *Stmt) allUserTables() []*schema.Table {
	var tables []*schema.Table
	for _, name := range s.conn.schema.ListTables() {
		if isSystemTable(name) {
			continue
		}
		if tbl, ok := s.conn.schema.GetTable(name); ok {
			if !tbl.IsVirtual {
				tables = append(tables, tbl)
			}
		}
	}
	return tables
}

// isSystemTable checks if a table name is a SQLite system table.
func isSystemTable(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasPrefix(lower, "sqlite_")
}

// analyzeTable collects statistics for a single table and its indexes.
func (s *Stmt) analyzeTable(tbl *schema.Table) error {
	rowCount, err := s.countTableRows(tbl.Name)
	if err != nil {
		return err
	}

	// Update in-memory table stats
	tbl.SetTableStats(&schema.TableStats{RowCount: rowCount})

	// Insert table-level stat (tbl, NULL, rowcount)
	if _, err := s.conn.ExecDML(
		"INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES(?, NULL, ?)",
		tbl.Name, fmt.Sprintf("%d", rowCount),
	); err != nil {
		return err
	}

	// Analyze each index on this table
	return s.analyzeTableIndexes(tbl, rowCount)
}

// analyzeTableIndexes collects statistics for all indexes on a table.
func (s *Stmt) analyzeTableIndexes(tbl *schema.Table, rowCount int64) error {
	indexes := s.conn.schema.GetTableIndexes(tbl.Name)
	for _, idx := range indexes {
		stat, err := s.computeIndexStat(tbl.Name, idx, rowCount)
		if err != nil {
			return err
		}
		if _, err := s.conn.ExecDML(
			"INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES(?, ?, ?)",
			tbl.Name, idx.Name, stat,
		); err != nil {
			return err
		}
	}
	return nil
}

// countTableRows counts the number of rows in a table.
func (s *Stmt) countTableRows(tableName string) (int64, error) {
	rows, err := s.conn.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(tableName)))
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, nil
	}
	return analyzeToInt64(rows[0][0]), nil
}

// computeIndexStat computes the sqlite_stat1 stat string for an index.
// Format: "total_rows distinct_col1 distinct_col1_col2 ..."
func (s *Stmt) computeIndexStat(tableName string, idx *schema.Index, rowCount int64) (string, error) {
	parts := []string{fmt.Sprintf("%d", rowCount)}

	for i := range idx.Columns {
		distinct, err := s.countDistinctPrefix(tableName, idx.Columns[:i+1])
		if err != nil {
			return "", err
		}
		avgRows := computeAvgRowsPerKey(rowCount, distinct)
		parts = append(parts, fmt.Sprintf("%d", avgRows))
	}

	return strings.Join(parts, " "), nil
}

// countDistinctPrefix counts distinct values for a column prefix.
func (s *Stmt) countDistinctPrefix(tableName string, columns []string) (int64, error) {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdentifier(col)
	}
	colList := strings.Join(quoted, ", ")
	query := fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", colList, quoteIdentifier(tableName))

	// For multi-column distinct, we need a different approach
	if len(columns) > 1 {
		// Use a grouped count approach for multi-column prefixes
		query = fmt.Sprintf(
			"SELECT COUNT(*) FROM (SELECT DISTINCT %s FROM %s)",
			colList, quoteIdentifier(tableName),
		)
	}

	rows, err := s.conn.Query(query)
	if err != nil {
		// If the query fails (e.g., subquery not supported), estimate
		return estimateDistinct(0), nil
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 1, nil
	}
	val := analyzeToInt64(rows[0][0])
	if val <= 0 {
		return 1, nil
	}
	return val, nil
}

// computeAvgRowsPerKey computes the average number of rows per distinct key.
func computeAvgRowsPerKey(rowCount, distinctCount int64) int64 {
	if distinctCount <= 0 {
		return rowCount
	}
	avg := rowCount / distinctCount
	if avg <= 0 {
		return 1
	}
	return avg
}

// estimateDistinct provides a fallback distinct estimate.
func estimateDistinct(rowCount int64) int64 {
	if rowCount <= 0 {
		return 1
	}
	return rowCount / 10
}

// quoteIdentifier wraps an identifier in double quotes for safety.
func quoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// analyzeToInt64 converts an interface value to int64.
func analyzeToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		var n int64
		fmt.Sscanf(val, "%d", &n)
		return n
	default:
		return 0
	}
}
