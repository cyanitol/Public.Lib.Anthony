// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"fmt"
	"strings"
)

// RenameColumn renames a column in a table's schema.
// Returns an error if the old column is not found or the new name conflicts.
func (s *Schema) RenameColumn(tableName, oldCol, newCol string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, err := s.findTableLocked(tableName)
	if err != nil {
		return err
	}

	if err := s.validateColumnRename(table, oldCol, newCol); err != nil {
		return err
	}

	s.applyColumnRename(table, oldCol, newCol)
	table.SQL = RebuildCreateTableSQL(table)
	s.updateIndexColumnRefs(tableName, oldCol, newCol)

	return nil
}

// DropColumn removes a column from a table's schema.
// Returns an error if the column cannot be dropped.
func (s *Schema) DropColumn(tableName, colName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, err := s.findTableLocked(tableName)
	if err != nil {
		return err
	}

	if err := s.validateColumnDrop(table, colName); err != nil {
		return err
	}

	s.removeColumn(table, colName)
	table.SQL = RebuildCreateTableSQL(table)

	return nil
}

// UpdateTableSQL regenerates the SQL for a table after schema changes.
// This should be called after any modification to table metadata.
func (s *Schema) UpdateTableSQL(tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, err := s.findTableLocked(tableName)
	if err != nil {
		return
	}
	table.SQL = RebuildCreateTableSQL(table)
}

// findTableLocked finds a table by name. Caller must hold the lock.
func (s *Schema) findTableLocked(name string) (*Table, error) {
	lowerName := strings.ToLower(name)
	for tableName, table := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			return table, nil
		}
	}
	return nil, fmt.Errorf("table not found: %s", name)
}

// validateColumnRename checks that a column rename is valid.
func (s *Schema) validateColumnRename(table *Table, oldCol, newCol string) error {
	if _, found := table.GetColumn(oldCol); !found {
		return fmt.Errorf("column %q not found in table %q", oldCol, table.Name)
	}
	if _, found := table.GetColumn(newCol); found {
		return fmt.Errorf("column %q already exists in table %q", newCol, table.Name)
	}
	return nil
}

// applyColumnRename updates the column name in the table and primary key list.
func (s *Schema) applyColumnRename(table *Table, oldCol, newCol string) {
	lowerOld := strings.ToLower(oldCol)

	for _, col := range table.Columns {
		if strings.ToLower(col.Name) == lowerOld {
			col.Name = newCol
			break
		}
	}

	for i, pk := range table.PrimaryKey {
		if strings.ToLower(pk) == lowerOld {
			table.PrimaryKey[i] = newCol
		}
	}
}

// updateIndexColumnRefs updates column references in indexes for the table.
func (s *Schema) updateIndexColumnRefs(tableName, oldCol, newCol string) {
	lowerTable := strings.ToLower(tableName)
	lowerOld := strings.ToLower(oldCol)

	for _, idx := range s.Indexes {
		if strings.ToLower(idx.Table) != lowerTable {
			continue
		}
		for i, col := range idx.Columns {
			if strings.ToLower(col) == lowerOld {
				idx.Columns[i] = newCol
			}
		}
		idx.SQL = rebuildCreateIndexSQL(idx)
	}
}

// validateColumnDrop checks that a column can be dropped.
func (s *Schema) validateColumnDrop(table *Table, colName string) error {
	colIdx := table.GetColumnIndex(colName)
	if colIdx == -1 {
		return fmt.Errorf("column %q not found in table %q", colName, table.Name)
	}
	if len(table.Columns) <= 1 {
		return fmt.Errorf("cannot drop the last column of table %q", table.Name)
	}
	if err := s.checkColumnNotPK(table, colName); err != nil {
		return err
	}
	if err := s.checkColumnNotIndexed(table.Name, colName); err != nil {
		return err
	}
	return nil
}

// checkColumnNotPK returns an error if the column is part of the primary key.
func (s *Schema) checkColumnNotPK(table *Table, colName string) error {
	lowerCol := strings.ToLower(colName)
	for _, pk := range table.PrimaryKey {
		if strings.ToLower(pk) == lowerCol {
			return fmt.Errorf("cannot drop PRIMARY KEY column %q", colName)
		}
	}
	return nil
}

// checkColumnNotIndexed returns an error if the column is used in an index.
func (s *Schema) checkColumnNotIndexed(tableName, colName string) error {
	lowerTable := strings.ToLower(tableName)
	lowerCol := strings.ToLower(colName)

	for _, idx := range s.Indexes {
		if strings.ToLower(idx.Table) != lowerTable {
			continue
		}
		for _, ic := range idx.Columns {
			if strings.ToLower(ic) == lowerCol {
				return fmt.Errorf("cannot drop column %q: used by index %q", colName, idx.Name)
			}
		}
	}
	return nil
}

// removeColumn removes a column from the table's column list.
func (s *Schema) removeColumn(table *Table, colName string) {
	colIdx := table.GetColumnIndex(colName)
	if colIdx >= 0 {
		table.Columns = append(table.Columns[:colIdx], table.Columns[colIdx+1:]...)
	}
}

// RebuildCreateTableSQL regenerates CREATE TABLE SQL from schema metadata.
func RebuildCreateTableSQL(table *Table) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(table.Name)
	sb.WriteString(" (")

	for i, col := range table.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeColumnSQL(&sb, col)
	}

	writeTableConstraintSQL(&sb, table)
	sb.WriteString(")")

	if table.WithoutRowID {
		sb.WriteString(" WITHOUT ROWID")
	}
	if table.Strict {
		sb.WriteString(" STRICT")
	}

	return sb.String()
}

// writeColumnSQL writes a single column definition to the builder.
func writeColumnSQL(sb *strings.Builder, col *Column) {
	sb.WriteString(col.Name)
	if col.Type != "" {
		sb.WriteString(" ")
		sb.WriteString(col.Type)
	}
	writeColumnConstraintSQL(sb, col)
}

// writeColumnConstraintSQL writes column-level constraints.
func writeColumnConstraintSQL(sb *strings.Builder, col *Column) {
	if col.PrimaryKey {
		sb.WriteString(" PRIMARY KEY")
		if col.Autoincrement {
			sb.WriteString(" AUTOINCREMENT")
		}
	}
	if col.NotNull {
		sb.WriteString(" NOT NULL")
	}
	if col.Unique {
		sb.WriteString(" UNIQUE")
	}
	writeColumnDefaultAndCollate(sb, col)
}

// writeColumnDefaultAndCollate writes DEFAULT and COLLATE constraints.
func writeColumnDefaultAndCollate(sb *strings.Builder, col *Column) {
	if col.Default != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(fmt.Sprintf("%v", col.Default))
	}
	if col.Collation != "" {
		sb.WriteString(" COLLATE ")
		sb.WriteString(col.Collation)
	}
}

// writeTableConstraintSQL writes table-level constraints to the builder.
func writeTableConstraintSQL(sb *strings.Builder, table *Table) {
	for _, tc := range table.Constraints {
		sb.WriteString(", ")
		writeOneTableConstraint(sb, &tc)
	}
}

// writeOneTableConstraint writes a single table-level constraint.
func writeOneTableConstraint(sb *strings.Builder, tc *TableConstraint) {
	if tc.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(tc.Name)
		sb.WriteString(" ")
	}
	switch tc.Type {
	case ConstraintPrimaryKey:
		sb.WriteString("PRIMARY KEY (")
		sb.WriteString(strings.Join(tc.Columns, ", "))
		sb.WriteString(")")
	case ConstraintUnique:
		sb.WriteString("UNIQUE (")
		sb.WriteString(strings.Join(tc.Columns, ", "))
		sb.WriteString(")")
	case ConstraintCheck:
		sb.WriteString("CHECK (")
		sb.WriteString(tc.Expression)
		sb.WriteString(")")
	case ConstraintForeignKey:
		sb.WriteString("FOREIGN KEY (")
		sb.WriteString(strings.Join(tc.Columns, ", "))
		sb.WriteString(")")
	}
}

// rebuildCreateIndexSQL regenerates CREATE INDEX SQL from schema metadata.
func rebuildCreateIndexSQL(idx *Index) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if idx.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(idx.Name)
	sb.WriteString(" ON ")
	sb.WriteString(idx.Table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(idx.Columns, ", "))
	sb.WriteString(")")

	if idx.Partial && idx.Where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(idx.Where)
	}

	return sb.String()
}

// UpdateRenameTableSQL updates the stored SQL and index SQL after a table rename.
// This is called from RenameTable to ensure SQL is consistent.
func (s *Schema) UpdateRenameTableSQL(newName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, err := s.findTableLocked(newName)
	if err != nil {
		return
	}
	table.SQL = RebuildCreateTableSQL(table)

	// Update index SQL for indexes on this table
	lowerName := strings.ToLower(newName)
	for _, idx := range s.Indexes {
		if strings.ToLower(idx.Table) == lowerName {
			idx.SQL = rebuildCreateIndexSQL(idx)
		}
	}
}
