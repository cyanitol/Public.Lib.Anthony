// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

// MC/DC (Modified Condition/Decision Coverage) tests for the schema package.
//
// For each compound boolean condition of the form A && B or A || B, we supply
// N+1 test cases so that every sub-condition independently flips the overall
// outcome.  Each sub-test name contains the literal string "MCDC" so that the
// test run filter `-run MCDC` selects exactly this file.

import (
	"testing"
)

// ---------------------------------------------------------------------------
// 1.  Column.IsIntegerPrimaryKey()
//
//     Condition: c.PrimaryKey && (c.Type == "INTEGER" || c.Type == "INT")
//
//     Sub-conditions:
//       A = c.PrimaryKey
//       B = c.Type == "INTEGER"
//       C = c.Type == "INT"
//     Compound: A && (B || C)
//
//     MC/DC pairs (truth table excerpt):
//       case 1:  A=T B=T C=x  → true   (B independently makes B||C true)
//       case 2:  A=T B=F C=T  → true   (C independently makes B||C true)
//       case 3:  A=T B=F C=F  → false  (B||C=false flips outcome vs cases 1,2)
//       case 4:  A=F B=T C=x  → false  (A=false flips outcome vs case 1)
// ---------------------------------------------------------------------------

func TestMCDC_IsIntegerPrimaryKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		primaryKey bool
		colType    string
		want       bool
	}{
		// A=T, B=T (type "INTEGER") → true
		{"MCDC_pk_true_type_INTEGER", true, "INTEGER", true},
		// A=T, C=T (type "INT") → true
		{"MCDC_pk_true_type_INT", true, "INT", true},
		// A=T, B=F, C=F (type "TEXT") → false  (B||C flips outcome)
		{"MCDC_pk_true_type_TEXT", true, "TEXT", false},
		// A=F, B=T (type "INTEGER") → false  (A flips outcome)
		{"MCDC_pk_false_type_INTEGER", false, "INTEGER", false},
		// A=F, C=T (type "INT") → false
		{"MCDC_pk_false_type_INT", false, "INT", false},
		// A=F, B=F, C=F → false (baseline all-false)
		{"MCDC_pk_false_type_TEXT", false, "TEXT", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			col := &Column{PrimaryKey: tt.primaryKey, Type: tt.colType}
			got := col.IsIntegerPrimaryKey()
			if got != tt.want {
				t.Errorf("IsIntegerPrimaryKey() = %v, want %v (PK=%v Type=%q)",
					got, tt.want, tt.primaryKey, tt.colType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 2.  isRowidAlias()
//
//     Condition: lowerName == "rowid" || lowerName == "_rowid_" || lowerName == "oid"
//
//     Sub-conditions:
//       A = name == "rowid"
//       B = name == "_rowid_"
//       C = name == "oid"
//     Compound: A || B || C
//
//     MC/DC pairs:
//       case 1:  A=T B=F C=F → true   (A independently flips outcome)
//       case 2:  A=F B=T C=F → true   (B independently flips outcome)
//       case 3:  A=F B=F C=T → true   (C independently flips outcome)
//       case 4:  A=F B=F C=F → false  (baseline all-false)
// ---------------------------------------------------------------------------

func TestMCDC_IsRowidAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// A=T: "rowid" → true
		{"MCDC_rowid_name_rowid", "rowid", true},
		// B=T: "_rowid_" → true
		{"MCDC_rowid_name__rowid_", "_rowid_", true},
		// C=T: "oid" → true
		{"MCDC_rowid_name_oid", "oid", true},
		// all false: other name → false
		{"MCDC_rowid_name_other", "id", false},
		// case-insensitive: "ROWID" → true (A=T via ToLower)
		{"MCDC_rowid_name_ROWID_upper", "ROWID", true},
		// case-insensitive: "OID" → true (C=T via ToLower)
		{"MCDC_rowid_name_OID_upper", "OID", true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isRowidAlias(tt.input)
			if got != tt.want {
				t.Errorf("isRowidAlias(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 3.  IsNumericAffinity()
//
//     Condition: aff == AffinityNumeric || aff == AffinityInteger || aff == AffinityReal
//
//     Sub-conditions:
//       A = aff == AffinityNumeric
//       B = aff == AffinityInteger
//       C = aff == AffinityReal
//     Compound: A || B || C
//
//     MC/DC pairs:
//       case 1:  A=T B=F C=F → true   (A independently flips)
//       case 2:  A=F B=T C=F → true   (B independently flips)
//       case 3:  A=F B=F C=T → true   (C independently flips)
//       case 4:  A=F B=F C=F → false  (baseline: non-numeric affinity)
// ---------------------------------------------------------------------------

func TestMCDC_IsNumericAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		aff  Affinity
		want bool
	}{
		// A=T: NUMERIC → true
		{"MCDC_numeric_NUMERIC", AffinityNumeric, true},
		// B=T: INTEGER → true
		{"MCDC_numeric_INTEGER", AffinityInteger, true},
		// C=T: REAL → true
		{"MCDC_numeric_REAL", AffinityReal, true},
		// all false: TEXT → false
		{"MCDC_numeric_TEXT", AffinityText, false},
		// all false: BLOB → false
		{"MCDC_numeric_BLOB", AffinityBlob, false},
		// all false: NONE → false
		{"MCDC_numeric_NONE", AffinityNone, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := IsNumericAffinity(tt.aff)
			if got != tt.want {
				t.Errorf("IsNumericAffinity(%v) = %v, want %v", tt.aff, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4.  isAutoIndex()
//
//     Condition: len(name) > len(prefix) && name[:len(prefix)] == prefix
//     where prefix = "sqlite_autoindex"
//
//     Sub-conditions:
//       A = len(name) > len("sqlite_autoindex")       (16 chars)
//       B = name[:16] == "sqlite_autoindex"
//     Compound: A && B
//
//     MC/DC pairs:
//       case 1:  A=T B=T → true   (both true → result true)
//       case 2:  A=T B=F → false  (B flips outcome)
//       case 3:  A=F B=x → false  (A flips outcome; B short-circuits to false)
//
//     Note: A=F, B=T is structurally impossible because if len<16 the slice
//     would panic – Go short-circuits &&, so A=F prevents B evaluation.
// ---------------------------------------------------------------------------

func TestMCDC_IsAutoIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// A=T, B=T: well-formed auto-index name → true
		{"MCDC_autoindex_valid", "sqlite_autoindex_users_1", true},
		// A=T, B=F: long name but wrong prefix → false
		{"MCDC_autoindex_wrong_prefix", "xqlite_autoindex_users_1", false},
		// A=F: name exactly equal to prefix (len==16, not >) → false
		{"MCDC_autoindex_exact_prefix_len", "sqlite_autoindex", false},
		// A=F: name shorter than prefix → false
		{"MCDC_autoindex_short_name", "sqlite_auto", false},
		// A=F: empty name → false
		{"MCDC_autoindex_empty", "", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isAutoIndex(tt.input)
			if got != tt.want {
				t.Errorf("isAutoIndex(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5.  SequenceManager.UpdateSequence()
//
//     Condition: !exists || rowid > current
//
//     Sub-conditions:
//       A = !exists  (sequence entry absent)
//       B = rowid > current
//     Compound: A || B
//
//     MC/DC pairs:
//       case 1:  A=T (entry absent)           → always updates (A independently)
//       case 2:  A=F B=T (exists, rowid >)    → updates       (B independently)
//       case 3:  A=F B=F (exists, rowid ≤)    → does not update
// ---------------------------------------------------------------------------

func TestMCDC_UpdateSequence(t *testing.T) {
	t.Parallel()

	// case 1: A=T – no entry exists yet; any rowid should be stored.
	t.Run("MCDC_update_seq_no_entry_A_true", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.UpdateSequence("tbl", 42)
		if got := sm.GetSequence("tbl"); got != 42 {
			t.Errorf("expected sequence 42, got %d", got)
		}
	})

	// case 2: A=F, B=T – entry exists with value 10; insert 20 (rowid > current).
	t.Run("MCDC_update_seq_entry_exists_rowid_greater_B_true", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.SetSequence("tbl", 10)
		sm.UpdateSequence("tbl", 20)
		if got := sm.GetSequence("tbl"); got != 20 {
			t.Errorf("expected sequence 20, got %d", got)
		}
	})

	// case 3: A=F, B=F – entry exists with value 10; insert 5 (rowid ≤ current).
	t.Run("MCDC_update_seq_entry_exists_rowid_not_greater_B_false", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.SetSequence("tbl", 10)
		sm.UpdateSequence("tbl", 5)
		if got := sm.GetSequence("tbl"); got != 10 {
			t.Errorf("expected sequence to remain 10, got %d", got)
		}
	})

	// boundary: A=F, B=F – equal value should NOT update.
	t.Run("MCDC_update_seq_entry_exists_rowid_equal_B_false", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.SetSequence("tbl", 10)
		sm.UpdateSequence("tbl", 10)
		if got := sm.GetSequence("tbl"); got != 10 {
			t.Errorf("expected sequence to remain 10, got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 6.  GenerateAutoincrementRowid()
//
//     Condition: hasExplicitRowid && explicitRowid != 0
//
//     Sub-conditions:
//       A = hasExplicitRowid
//       B = explicitRowid != 0
//     Compound: A && B
//
//     MC/DC pairs:
//       case 1:  A=T B=T → uses explicit rowid path
//       case 2:  A=T B=F (rowid == 0) → generates next sequence
//       case 3:  A=F B=T → generates next sequence (A flips outcome)
//       case 4:  A=F B=F → generates next sequence
// ---------------------------------------------------------------------------

func TestMCDC_GenerateAutoincrementRowid(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → explicit rowid used
	t.Run("MCDC_autoincrement_explicit_rowid_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.InitSequence("tbl")
		got, err := GenerateAutoincrementRowid(sm, "tbl", 99, true, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 99 {
			t.Errorf("expected rowid 99, got %d", got)
		}
	})

	// case 2: A=T, B=F (explicitRowid == 0) → generate next sequence
	t.Run("MCDC_autoincrement_explicit_zero_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.InitSequence("tbl")
		got, err := GenerateAutoincrementRowid(sm, "tbl", 0, true, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 1 {
			t.Errorf("expected generated rowid 1, got %d", got)
		}
	})

	// case 3: A=F, B=T → generate next sequence (A flips outcome)
	t.Run("MCDC_autoincrement_no_explicit_rowid_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.InitSequence("tbl")
		got, err := GenerateAutoincrementRowid(sm, "tbl", 50, false, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 1 {
			t.Errorf("expected generated rowid 1, got %d", got)
		}
	})

	// case 4: A=F, B=F → generate next sequence
	t.Run("MCDC_autoincrement_no_explicit_rowid_A_false_B_false", func(t *testing.T) {
		t.Parallel()
		sm := NewSequenceManager()
		sm.InitSequence("tbl")
		got, err := GenerateAutoincrementRowid(sm, "tbl", 0, false, 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 6 {
			t.Errorf("expected generated rowid 6 (currentMaxRowid+1), got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 7.  checkDuplicatePath()
//
//     Condition: db.Path != "" && db.Path != ":memory:" && db.Path == filePath
//
//     Sub-conditions:
//       A = db.Path != ""
//       B = db.Path != ":memory:"
//       C = db.Path == filePath
//     Compound: A && B && C
//
//     MC/DC pairs (for the inner loop that returns an error):
//       case 1:  A=T B=T C=T → conflict detected (error returned)
//       case 2:  A=T B=T C=F → no conflict (C flips outcome)
//       case 3:  A=T B=F C=T → no conflict (B flips outcome; path is ":memory:")
//       case 4:  A=F B=x C=x → no conflict (A flips outcome; empty path)
//
//     Note: The outer guard `if filePath == "" || filePath == ":memory:"` is
//     tested separately to document that guard.
// ---------------------------------------------------------------------------

func TestMCDC_CheckDuplicatePath(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T, C=T → duplicate path error
	t.Run("MCDC_duplicate_path_A_true_B_true_C_true", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: "/data/db.sqlite"}
		err := dr.checkDuplicatePath("/data/db.sqlite", "new")
		if err == nil {
			t.Error("expected duplicate path error, got nil")
		}
	})

	// case 2: A=T, B=T, C=F → different path, no error
	t.Run("MCDC_duplicate_path_A_true_B_true_C_false", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: "/data/db.sqlite"}
		err := dr.checkDuplicatePath("/data/other.sqlite", "new")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// case 3: A=T, B=F, C=T → existing path is ":memory:", no error
	t.Run("MCDC_duplicate_path_A_true_B_false_C_memory", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: ":memory:"}
		err := dr.checkDuplicatePath(":memory:", "new")
		// The outer guard in checkDuplicatePath returns nil for ":memory:" filePath
		if err != nil {
			t.Errorf("unexpected error for :memory: path: %v", err)
		}
	})

	// case 4: A=F → existing path is empty, no error
	t.Run("MCDC_duplicate_path_A_false_empty_existing", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: ""}
		err := dr.checkDuplicatePath("/data/db.sqlite", "new")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// outer guard: filePath == "" → immediate nil
	t.Run("MCDC_duplicate_path_file_empty_guard", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: "/data/db.sqlite"}
		err := dr.checkDuplicatePath("", "new")
		if err != nil {
			t.Errorf("unexpected error for empty filePath: %v", err)
		}
	})

	// outer guard: filePath == ":memory:" → immediate nil
	t.Run("MCDC_duplicate_path_file_memory_guard", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		dr.databases["other"] = &Database{Name: "other", Path: "/data/db.sqlite"}
		err := dr.checkDuplicatePath(":memory:", "new")
		if err != nil {
			t.Errorf("unexpected error for :memory: filePath: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 8.  buildMasterRows() index inclusion condition
//
//     Condition: !isAutoIndex(name) || index.SQL != ""
//
//     Sub-conditions:
//       A = !isAutoIndex(name)   (not an auto-index)
//       B = index.SQL != ""      (has SQL)
//     Compound: A || B
//
//     MC/DC pairs:
//       case 1:  A=T B=F → included (A independently)
//       case 2:  A=F B=T → included (B independently)
//       case 3:  A=F B=F → excluded
// ---------------------------------------------------------------------------

func TestMCDC_BuildMasterRows_IndexInclusion(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=F → regular index with no SQL is still included
	t.Run("MCDC_master_rows_normal_index_no_sql_A_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Indexes["my_index"] = &Index{Name: "my_index", Table: "t", SQL: ""}
		rows := s.buildMasterRows()
		found := false
		for _, r := range rows {
			if r.Name == "my_index" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected normal index (non-auto) with empty SQL to be included in master rows")
		}
	})

	// case 2: A=F, B=T → auto-index with SQL is included
	t.Run("MCDC_master_rows_autoindex_with_sql_B_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		autoName := "sqlite_autoindex_users_1"
		s.Indexes[autoName] = &Index{Name: autoName, Table: "users", SQL: "CREATE UNIQUE INDEX sqlite_autoindex_users_1 ON users(email)"}
		rows := s.buildMasterRows()
		found := false
		for _, r := range rows {
			if r.Name == autoName {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected auto-index with SQL to be included in master rows")
		}
	})

	// case 3: A=F, B=F → auto-index with no SQL is excluded
	t.Run("MCDC_master_rows_autoindex_no_sql_excluded", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		autoName := "sqlite_autoindex_orders_1"
		s.Indexes[autoName] = &Index{Name: autoName, Table: "orders", SQL: ""}
		rows := s.buildMasterRows()
		for _, r := range rows {
			if r.Name == autoName {
				t.Error("expected auto-index with no SQL to be excluded from master rows")
				break
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 9.  rebuildCreateIndexSQL() partial-index WHERE clause
//
//     Condition: idx.Partial && idx.Where != ""
//
//     Sub-conditions:
//       A = idx.Partial
//       B = idx.Where != ""
//     Compound: A && B
//
//     MC/DC pairs:
//       case 1:  A=T B=T → WHERE clause appended to SQL
//       case 2:  A=T B=F → WHERE clause NOT appended (B flips outcome)
//       case 3:  A=F B=T → WHERE clause NOT appended (A flips outcome)
//       case 4:  A=F B=F → WHERE clause NOT appended (baseline all-false)
// ---------------------------------------------------------------------------

func TestMCDC_RebuildCreateIndexSQL_PartialWhere(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → WHERE appears in output
	t.Run("MCDC_rebuild_index_sql_partial_true_where_set_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		idx := &Index{
			Name:    "idx_active",
			Table:   "users",
			Columns: []string{"email"},
			Partial: true,
			Where:   "active = 1",
		}
		sql := rebuildCreateIndexSQL(idx)
		if !containsSubstr(sql, "WHERE") {
			t.Errorf("expected WHERE clause in SQL, got: %s", sql)
		}
	})

	// case 2: A=T, B=F → WHERE does NOT appear (Where is empty)
	t.Run("MCDC_rebuild_index_sql_partial_true_where_empty_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		idx := &Index{
			Name:    "idx_active",
			Table:   "users",
			Columns: []string{"email"},
			Partial: true,
			Where:   "",
		}
		sql := rebuildCreateIndexSQL(idx)
		if containsSubstr(sql, "WHERE") {
			t.Errorf("expected no WHERE clause when Where is empty, got: %s", sql)
		}
	})

	// case 3: A=F, B=T → WHERE does NOT appear (Partial is false)
	t.Run("MCDC_rebuild_index_sql_partial_false_where_set_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		idx := &Index{
			Name:    "idx_active",
			Table:   "users",
			Columns: []string{"email"},
			Partial: false,
			Where:   "active = 1",
		}
		sql := rebuildCreateIndexSQL(idx)
		if containsSubstr(sql, "WHERE") {
			t.Errorf("expected no WHERE clause when Partial=false, got: %s", sql)
		}
	})

	// case 4: A=F, B=F → WHERE does NOT appear
	t.Run("MCDC_rebuild_index_sql_partial_false_where_empty_A_false_B_false", func(t *testing.T) {
		t.Parallel()
		idx := &Index{
			Name:    "idx_active",
			Table:   "users",
			Columns: []string{"email"},
			Partial: false,
			Where:   "",
		}
		sql := rebuildCreateIndexSQL(idx)
		if containsSubstr(sql, "WHERE") {
			t.Errorf("expected no WHERE clause, got: %s", sql)
		}
	})
}

// ---------------------------------------------------------------------------
// 10. ValidateAutoincrementColumn() type check
//
//     Condition: col.Type != "INTEGER" && col.Type != "INT"
//
//     Sub-conditions:
//       A = col.Type != "INTEGER"
//       B = col.Type != "INT"
//     Compound: A && B  (true means invalid type → return error)
//
//     MC/DC pairs:
//       case 1:  A=T B=T → error (type is neither INTEGER nor INT)
//       case 2:  A=F B=T → no error on type (type is "INTEGER")
//       case 3:  A=T B=F → no error on type (type is "INT")
//       case 4:  A=F B=F → impossible (cannot be both "INTEGER" and "INT")
// ---------------------------------------------------------------------------

func TestMCDC_ValidateAutoincrementColumn_TypeCheck(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → type is "TEXT", error expected
	t.Run("MCDC_validate_autoincrement_type_TEXT_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		table := &Table{
			Name: "tbl",
			Columns: []*Column{
				{Name: "id", Type: "TEXT", PrimaryKey: true, Autoincrement: true},
			},
		}
		err := table.ValidateAutoincrementColumn()
		if err == nil {
			t.Error("expected error for AUTOINCREMENT on TEXT column")
		}
	})

	// case 2: A=F, B=T → type is "INTEGER", no error
	t.Run("MCDC_validate_autoincrement_type_INTEGER_A_false", func(t *testing.T) {
		t.Parallel()
		table := &Table{
			Name: "tbl",
			Columns: []*Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: true},
			},
		}
		err := table.ValidateAutoincrementColumn()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// case 3: A=T, B=F → type is "INT", no error
	t.Run("MCDC_validate_autoincrement_type_INT_B_false", func(t *testing.T) {
		t.Parallel()
		table := &Table{
			Name: "tbl",
			Columns: []*Column{
				{Name: "id", Type: "INT", PrimaryKey: true, Autoincrement: true},
			},
		}
		err := table.ValidateAutoincrementColumn()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 11. GetColumnIndexWithRowidAliases()
//
//     Two internal conditions exercised:
//       (a) idx >= 0  (column found by exact name)
//       (b) !isRowidAlias(name)  (not a rowid alias → return -1)
//       (c) INTEGER PK exists   (findIntegerPrimaryKeyIndex() >= 0)
//
//     Return values and their MC/DC assignments:
//       exact match found               →  idx  (≥0)
//       not found, not alias            → -1   (b independently)
//       not found, alias, IPK exists    →  ipk  (c=T)
//       not found, alias, no IPK        → -2   (c=F)
// ---------------------------------------------------------------------------

func TestMCDC_GetColumnIndexWithRowidAliases(t *testing.T) {
	t.Parallel()

	table := &Table{
		Columns: []*Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
	}

	// (a) exact match → index 0
	t.Run("MCDC_rowid_aliases_exact_match", func(t *testing.T) {
		t.Parallel()
		got := table.GetColumnIndexWithRowidAliases("id")
		if got != 0 {
			t.Errorf("expected 0, got %d", got)
		}
	})

	// (b) not found, not a rowid alias → -1
	t.Run("MCDC_rowid_aliases_not_found_not_alias", func(t *testing.T) {
		t.Parallel()
		got := table.GetColumnIndexWithRowidAliases("unknown_col")
		if got != -1 {
			t.Errorf("expected -1, got %d", got)
		}
	})

	// (c) c=T: rowid alias and INTEGER PK exists → returns IPK column index (0)
	t.Run("MCDC_rowid_aliases_alias_ipk_exists_C_true", func(t *testing.T) {
		t.Parallel()
		got := table.GetColumnIndexWithRowidAliases("rowid")
		if got != 0 {
			t.Errorf("expected 0 (IPK column index), got %d", got)
		}
	})

	// (c) c=F: rowid alias but no INTEGER PK → -2
	t.Run("MCDC_rowid_aliases_alias_no_ipk_C_false", func(t *testing.T) {
		t.Parallel()
		noIPKTable := &Table{
			Columns: []*Column{
				{Name: "name", Type: "TEXT"},
			},
		}
		got := noIPKTable.GetColumnIndexWithRowidAliases("rowid")
		if got != -2 {
			t.Errorf("expected -2 (rowid alias, no IPK), got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 12. findIntegerPrimaryKeyIndex()
//
//     Condition: col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")
//     (same compound as IsIntegerPrimaryKey but exercised via the index search)
//
//     MC/DC pairs covered through table scan:
//       case 1: PK=T, Type="INTEGER" → found at index 0
//       case 2: PK=T, Type="INT"     → found at index 0
//       case 3: PK=T, Type="TEXT"    → not found (-1)
//       case 4: PK=F, Type="INTEGER" → not found (-1)
// ---------------------------------------------------------------------------

func TestMCDC_FindIntegerPrimaryKeyIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		primaryKey bool
		colType    string
		wantFound  bool
	}{
		{"MCDC_find_ipk_pk_true_INTEGER", true, "INTEGER", true},
		{"MCDC_find_ipk_pk_true_INT", true, "INT", true},
		{"MCDC_find_ipk_pk_true_TEXT", true, "TEXT", false},
		{"MCDC_find_ipk_pk_false_INTEGER", false, "INTEGER", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			table := &Table{
				Columns: []*Column{
					{Name: "id", Type: tt.colType, PrimaryKey: tt.primaryKey},
				},
			}
			idx := table.findIntegerPrimaryKeyIndex()
			found := idx >= 0
			if found != tt.wantFound {
				t.Errorf("findIntegerPrimaryKeyIndex() found=%v, want %v (PK=%v Type=%q)",
					found, tt.wantFound, tt.primaryKey, tt.colType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 13. GetRecordColumnNames() exclusion condition
//
//     Condition: col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")
//     (columns matching this condition are excluded from the record payload)
//
//     MC/DC pairs – we verify which columns are excluded:
//       case 1: PK=T, Type="INTEGER" → excluded from record names
//       case 2: PK=T, Type="INT"     → excluded from record names
//       case 3: PK=T, Type="TEXT"    → included in record names
//       case 4: PK=F, Type="INTEGER" → included in record names
// ---------------------------------------------------------------------------

func TestMCDC_GetRecordColumnNames_Exclusion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		primaryKey   bool
		colType      string
		wantExcluded bool
	}{
		{"MCDC_record_cols_pk_true_INTEGER_excluded", true, "INTEGER", true},
		{"MCDC_record_cols_pk_true_INT_excluded", true, "INT", true},
		{"MCDC_record_cols_pk_true_TEXT_included", true, "TEXT", false},
		{"MCDC_record_cols_pk_false_INTEGER_included", false, "INTEGER", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			table := &Table{
				Columns: []*Column{
					{Name: "target", Type: tt.colType, PrimaryKey: tt.primaryKey},
					{Name: "other", Type: "TEXT"},
				},
			}
			names := table.GetRecordColumnNames()
			excluded := true
			for _, n := range names {
				if n == "target" {
					excluded = false
					break
				}
			}
			if excluded != tt.wantExcluded {
				t.Errorf("GetRecordColumnNames() excluded=%v, want %v (PK=%v Type=%q)",
					excluded, tt.wantExcluded, tt.primaryKey, tt.colType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 14. GetRowidColumnName() – same compound condition, verifies return value
//
//     MC/DC pairs identical to GetRecordColumnNames above.
// ---------------------------------------------------------------------------

func TestMCDC_GetRowidColumnName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		primaryKey bool
		colType    string
		wantName   string // "" means no rowid column found
	}{
		{"MCDC_rowid_col_pk_true_INTEGER", true, "INTEGER", "id"},
		{"MCDC_rowid_col_pk_true_INT", true, "INT", "id"},
		{"MCDC_rowid_col_pk_true_TEXT", true, "TEXT", ""},
		{"MCDC_rowid_col_pk_false_INTEGER", false, "INTEGER", ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			table := &Table{
				Columns: []*Column{
					{Name: "id", Type: tt.colType, PrimaryKey: tt.primaryKey},
				},
			}
			got := table.GetRowidColumnName()
			if got != tt.wantName {
				t.Errorf("GetRowidColumnName() = %q, want %q (PK=%v Type=%q)",
					got, tt.wantName, tt.primaryKey, tt.colType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

// containsSubstr reports whether s contains the substring sub.
func containsSubstr(s, sub string) bool {
	return len(s) >= len(sub) && findSubstring(s, sub)
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
