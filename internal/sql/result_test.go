package sql

import (
	"testing"
)

// Test NewResultCompiler
func TestNewResultCompiler(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	if rc == nil {
		t.Fatal("NewResultCompiler returned nil")
	}
	if rc.parse != parse {
		t.Error("ResultCompiler.parse not set correctly")
	}
}

// Test ExpandResultColumns with SELECT *
func TestExpandResultColumnsStar(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
			{Name: "email", DeclType: "TEXT"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_ASTERISK}},
			},
		},
		Src: srcList,
	}

	err := rc.ExpandResultColumns(sel)
	if err != nil {
		t.Fatalf("ExpandResultColumns failed: %v", err)
	}

	if sel.EList.Len() != 3 {
		t.Errorf("Expected 3 columns, got %d", sel.EList.Len())
	}
}

// Test ExpandResultColumns with table.*
func TestExpandResultColumnsTableStar(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "users"},
					Right: &Expr{Op: TK_ASTERISK},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ExpandResultColumns(sel)
	if err != nil {
		t.Fatalf("ExpandResultColumns failed: %v", err)
	}

	if sel.EList.Len() != 2 {
		t.Errorf("Expected 2 columns, got %d", sel.EList.Len())
	}
}

// Test ExpandResultColumns with multiple tables
func TestExpandResultColumnsMultipleTables(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table1 := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	table2 := &Table{
		Name:       "posts",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "title", DeclType: "TEXT"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table1, Cursor: 0})
	srcList.Append(SrcListItem{Table: table2, Cursor: 1})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_ASTERISK}},
			},
		},
		Src: srcList,
	}

	err := rc.ExpandResultColumns(sel)
	if err != nil {
		t.Fatalf("ExpandResultColumns failed: %v", err)
	}

	if sel.EList.Len() != 4 {
		t.Errorf("Expected 4 columns, got %d", sel.EList.Len())
	}
}

// Test ExpandResultColumns error cases
func TestExpandResultColumnsErrors(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	tests := []struct {
		name    string
		sel     *Select
		wantErr bool
	}{
		{
			name: "nil_expression_list",
			sel: &Select{
				EList: nil,
			},
			wantErr: true,
		},
		{
			name: "star_without_tables",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_ASTERISK}},
					},
				},
				Src: nil,
			},
			wantErr: true,
		},
		{
			name: "table_star_not_found",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{
							Op:    TK_DOT,
							Left:  &Expr{Op: TK_ID, StringValue: "nonexistent"},
							Right: &Expr{Op: TK_ASTERISK},
						}},
					},
				},
				Src: NewSrcList(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rc.ExpandResultColumns(tt.sel)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExpandResultColumns() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test GenerateColumnNames
func TestGenerateColumnNames(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:        TK_COLUMN,
						Table:     0,
						Column:    0,
						ColumnRef: &table.Columns[0],
					},
					Name: "user_id",
				},
				{
					Expr: &Expr{
						Op:        TK_COLUMN,
						Table:     0,
						Column:    1,
						ColumnRef: &table.Columns[1],
					},
				},
			},
		},
	}

	err := rc.GenerateColumnNames(sel)
	if err != nil {
		t.Fatalf("GenerateColumnNames failed: %v", err)
	}

	// Verify column names were set
	if parse.Vdbe.NumCols != 2 {
		t.Errorf("Expected 2 columns, got %d", parse.Vdbe.NumCols)
	}
}

// Test computeColumnName
func TestComputeColumnName(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
		},
	}

	sel := &Select{}

	tests := []struct {
		name     string
		item     *ExprListItem
		idx      int
		expected string
	}{
		{
			name: "as_clause",
			item: &ExprListItem{
				Name: "user_id",
				Expr: &Expr{Op: TK_COLUMN},
			},
			idx:      0,
			expected: "user_id",
		},
		{
			name: "column_reference",
			item: &ExprListItem{
				Expr: &Expr{
					Op:        TK_COLUMN,
					ColumnRef: &table.Columns[0],
				},
			},
			idx:      0,
			expected: "id",
		},
		{
			name: "identifier",
			item: &ExprListItem{
				Expr: &Expr{
					Op:          TK_ID,
					StringValue: "mycolumn",
				},
			},
			idx:      0,
			expected: "mycolumn",
		},
		{
			name: "generated",
			item: &ExprListItem{
				Expr: &Expr{Op: TK_INTEGER, IntValue: 42},
			},
			idx:      5,
			expected: "column6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rc.computeColumnName(sel, tt.item, tt.idx)
			if result != tt.expected {
				t.Errorf("computeColumnName() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Test computeDeclaredType
func TestComputeDeclaredType(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
		},
	}

	sel := &Select{}

	tests := []struct {
		name     string
		expr     *Expr
		expected string
	}{
		{
			name: "column_reference",
			expr: &Expr{
				Op:        TK_COLUMN,
				ColumnRef: &table.Columns[0],
			},
			expected: "INTEGER",
		},
		{
			name:     "integer",
			expr:     &Expr{Op: TK_INTEGER, IntValue: 42},
			expected: "INTEGER",
		},
		{
			name:     "float",
			expr:     &Expr{Op: TK_FLOAT},
			expected: "REAL",
		},
		{
			name:     "string",
			expr:     &Expr{Op: TK_STRING, StringValue: "test"},
			expected: "TEXT",
		},
		{
			name:     "blob",
			expr:     &Expr{Op: TK_BLOB},
			expected: "TEXT",
		},
		{
			name:     "null",
			expr:     &Expr{Op: TK_NULL},
			expected: "",
		},
		{
			name:     "nil_expr",
			expr:     nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rc.computeDeclaredType(sel, tt.expr)
			if result != tt.expected {
				t.Errorf("computeDeclaredType() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Test ResolveResultColumns
func TestResolveResultColumns(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 5})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "id"}},
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "name"}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns failed: %v", err)
	}

	// Check that columns were resolved
	item0 := sel.EList.Get(0)
	if item0.Expr.Table != 5 {
		t.Errorf("Column 0 Table = %d, want 5", item0.Expr.Table)
	}
	if item0.Expr.Column != 0 {
		t.Errorf("Column 0 Column = %d, want 0", item0.Expr.Column)
	}

	item1 := sel.EList.Get(1)
	if item1.Expr.Table != 5 {
		t.Errorf("Column 1 Table = %d, want 5", item1.Expr.Table)
	}
	if item1.Expr.Column != 1 {
		t.Errorf("Column 1 Column = %d, want 1", item1.Expr.Column)
	}
}

// Test ResolveResultColumns with qualified column
func TestResolveResultColumnsQualified(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 3})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "users"},
					Right: &Expr{Op: TK_ID, StringValue: "id"},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns failed: %v", err)
	}

	item := sel.EList.Get(0)
	if item.Expr.Op != TK_COLUMN {
		t.Error("Expression should be converted to TK_COLUMN")
	}
	if item.Expr.Table != 3 {
		t.Errorf("Table = %d, want 3", item.Expr.Table)
	}
	if item.Expr.Column != 0 {
		t.Errorf("Column = %d, want 0", item.Expr.Column)
	}
}

// Test ResolveResultColumns error cases
func TestResolveResultColumnsErrors(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	tests := []struct {
		name string
		sel  *Select
	}{
		{
			name: "column_not_found",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_COLUMN, StringValue: "nonexistent"}},
					},
				},
				Src: NewSrcList(),
			},
		},
		{
			name: "qualified_table_not_found",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{
							Op:    TK_DOT,
							Left:  &Expr{Op: TK_ID, StringValue: "nonexistent"},
							Right: &Expr{Op: TK_ID, StringValue: "id"},
						}},
					},
				},
				Src: NewSrcList(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rc.ResolveResultColumns(tt.sel)
			if err == nil {
				t.Error("Expected error, got nil")
			}
		})
	}
}

// Test findTableByName
func TestFindTableByName(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table1 := &Table{Name: "users"}
	table2 := &Table{Name: "posts"}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table1, Cursor: 0})
	srcList.Append(SrcListItem{Table: table2, Cursor: 1, Alias: "p"})

	sel := &Select{Src: srcList}

	tests := []struct {
		name      string
		tableName string
		wantFound bool
		wantTable *Table
	}{
		{"find_by_name", "users", true, table1},
		{"find_by_alias", "p", true, table2},
		{"not_found", "nonexistent", false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rc.findTableByName(sel, tt.tableName)
			if tt.wantFound {
				if result == nil {
					t.Error("Expected to find table, got nil")
				} else if result.Table != tt.wantTable {
					t.Error("Found wrong table")
				}
			} else {
				if result != nil {
					t.Error("Expected nil, found table")
				}
			}
		})
	}
}

// Test ComputeColumnAffinity
func TestComputeColumnAffinity(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	column := &Column{
		Name:     "id",
		Affinity: SQLITE_AFF_INTEGER,
	}

	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name:     "nil",
			expr:     nil,
			expected: SQLITE_AFF_BLOB,
		},
		{
			name: "column_reference",
			expr: &Expr{
				Op:        TK_COLUMN,
				ColumnRef: column,
			},
			expected: SQLITE_AFF_INTEGER,
		},
		{
			name:     "integer",
			expr:     &Expr{Op: TK_INTEGER},
			expected: SQLITE_AFF_INTEGER,
		},
		{
			name:     "float",
			expr:     &Expr{Op: TK_FLOAT},
			expected: SQLITE_AFF_REAL,
		},
		{
			name:     "string",
			expr:     &Expr{Op: TK_STRING},
			expected: SQLITE_AFF_TEXT,
		},
		{
			name:     "blob",
			expr:     &Expr{Op: TK_BLOB},
			expected: SQLITE_AFF_BLOB,
		},
		{
			name:     "null",
			expr:     &Expr{Op: TK_NULL},
			expected: SQLITE_AFF_NONE,
		},
		{
			name: "binary_expr",
			expr: &Expr{
				Op:   TK_PLUS,
				Left: &Expr{Op: TK_INTEGER},
			},
			expected: SQLITE_AFF_INTEGER,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rc.ComputeColumnAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("ComputeColumnAffinity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test extractQualifiedNames
func TestResultExtractQualifiedNames(t *testing.T) {
	tests := []struct {
		name      string
		expr      *Expr
		wantTable string
		wantCol   string
		wantErr   bool
	}{
		{
			name: "valid",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantTable: "users",
			wantCol:   "id",
			wantErr:   false,
		},
		{
			name: "invalid_left",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_INTEGER},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantErr: true,
		},
		{
			name: "invalid_right",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_INTEGER},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, colName, err := extractQualifiedNames(tt.expr)

			if (err != nil) != tt.wantErr {
				t.Errorf("extractQualifiedNames() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				if tableName != tt.wantTable {
					t.Errorf("tableName = %q, want %q", tableName, tt.wantTable)
				}
				if colName != tt.wantCol {
					t.Errorf("colName = %q, want %q", colName, tt.wantCol)
				}
			}
		})
	}
}

// Test findTableInSrc
func TestFindTableInSrc(t *testing.T) {
	table1 := &Table{Name: "users"}
	table2 := &Table{Name: "posts"}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table1, Cursor: 0})
	srcList.Append(SrcListItem{Table: table2, Cursor: 1, Alias: "p"})

	tests := []struct {
		name      string
		tableName string
		wantFound bool
	}{
		{"find_by_name", "users", true},
		{"find_by_alias", "p", true},
		{"not_found", "nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findTableInSrc(srcList, tt.tableName)
			if tt.wantFound {
				if result == nil {
					t.Error("Expected to find table, got nil")
				}
			} else {
				if result != nil {
					t.Error("Expected nil, found table")
				}
			}
		})
	}
}

// Test findColumnInTable
func TestFindColumnInTable(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
			{Name: "email", DeclType: "TEXT"},
		},
	}

	tests := []struct {
		name      string
		colName   string
		wantFound bool
		wantIdx   int
	}{
		{"find_id", "id", true, 0},
		{"find_name", "name", true, 1},
		{"find_email", "email", true, 2},
		{"not_found", "nonexistent", false, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, idx := findColumnInTable(table, tt.colName)

			if tt.wantFound {
				if col == nil {
					t.Error("Expected to find column, got nil")
				}
				if idx != tt.wantIdx {
					t.Errorf("idx = %d, want %d", idx, tt.wantIdx)
				}
			} else {
				if col != nil {
					t.Error("Expected nil, found column")
				}
				if idx != -1 {
					t.Errorf("idx = %d, want -1", idx)
				}
			}
		})
	}
}

// Test isTableStar
func TestIsTableStar(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	tests := []struct {
		name     string
		expr     *Expr
		expected bool
	}{
		{
			name: "table_star",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_ASTERISK},
			},
			expected: true,
		},
		{
			name: "not_dot",
			expr: &Expr{
				Op: TK_COLUMN,
			},
			expected: false,
		},
		{
			name: "dot_without_asterisk",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rc.isTableStar(tt.expr)
			if result != tt.expected {
				t.Errorf("isTableStar() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test appendTableColumns
func TestAppendTableColumns(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	srcItem := &SrcListItem{
		Table:  table,
		Cursor: 5,
	}

	result := NewExprList()
	appendTableColumns(srcItem, result)

	if result.Len() != 2 {
		t.Errorf("Expected 2 columns, got %d", result.Len())
	}

	item0 := result.Get(0)
	if item0.Name != "id" {
		t.Errorf("Item 0 Name = %q, want %q", item0.Name, "id")
	}
	if item0.Expr.Op != TK_COLUMN {
		t.Error("Item 0 should be TK_COLUMN")
	}
	if item0.Expr.Table != 5 {
		t.Errorf("Item 0 Table = %d, want 5", item0.Expr.Table)
	}

	item1 := result.Get(1)
	if item1.Name != "name" {
		t.Errorf("Item 1 Name = %q, want %q", item1.Name, "name")
	}
}

// Test columnRefAffinity
func TestColumnRefAffinity(t *testing.T) {
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "with_column_ref",
			expr: &Expr{
				ColumnRef: &Column{Affinity: SQLITE_AFF_INTEGER},
			},
			expected: SQLITE_AFF_INTEGER,
		},
		{
			name:     "without_column_ref",
			expr:     &Expr{},
			expected: SQLITE_AFF_BLOB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := columnRefAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("columnRefAffinity() = %v, want %v", result, tt.expected)
			}
		})
	}
}
