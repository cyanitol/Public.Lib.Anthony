package sql

import (
	"strings"
	"testing"
)

func TestCompileInsert(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *InsertStmt
		tableRoot int
		wantErr   bool
		checkFunc func(*testing.T, *Program)
	}{
		{
			name: "single_row",
			stmt: &InsertStmt{
				Table:   "users",
				Columns: []string{"id", "name"},
				Values: [][]Value{
					{IntValue(1), TextValue("Alice")},
				},
			},
			tableRoot: 100,
			checkFunc: func(t *testing.T, p *Program) {
				if len(p.Instructions) == 0 {
					t.Error("no instructions generated")
				}

				// Check first and last instructions
				if p.Instructions[0].OpCode != OpInit {
					t.Error("first instruction should be Init")
				}

				lastIdx := len(p.Instructions) - 1
				if p.Instructions[lastIdx].OpCode != OpHalt {
					t.Error("last instruction should be Halt")
				}

				// Verify OpenWrite is present
				hasOpenWrite := false
				for _, inst := range p.Instructions {
					if inst.OpCode == OpOpenWrite {
						hasOpenWrite = true
						if inst.P2 != 100 {
							t.Errorf("OpenWrite P2 = %d, want 100", inst.P2)
						}
						break
					}
				}
				if !hasOpenWrite {
					t.Error("missing OpenWrite instruction")
				}
			},
		},
		{
			name: "multiple_rows",
			stmt: &InsertStmt{
				Table:   "data",
				Columns: []string{"x", "y"},
				Values: [][]Value{
					{IntValue(1), IntValue(2)},
					{IntValue(3), IntValue(4)},
					{IntValue(5), IntValue(6)},
				},
			},
			tableRoot: 200,
			checkFunc: func(t *testing.T, p *Program) {
				// Count Insert operations
				insertCount := 0
				for _, inst := range p.Instructions {
					if inst.OpCode == OpInsert {
						insertCount++
					}
				}
				if insertCount != 3 {
					t.Errorf("expected 3 Insert ops, got %d", insertCount)
				}
			},
		},
		{
			name: "mixed_types",
			stmt: &InsertStmt{
				Table:   "mixed",
				Columns: []string{"id", "name", "score", "data"},
				Values: [][]Value{
					{
						IntValue(1),
						TextValue("test"),
						FloatValue(98.5),
						BlobValue([]byte{1, 2, 3}),
					},
				},
			},
			tableRoot: 300,
		},
		{
			name: "with_nulls",
			stmt: &InsertStmt{
				Table:   "nulls",
				Columns: []string{"a", "b", "c"},
				Values: [][]Value{
					{IntValue(1), NullValue(), TextValue("x")},
				},
			},
			tableRoot: 400,
		},
		{
			name:      "nil_statement",
			stmt:      nil,
			tableRoot: 100,
			wantErr:   true,
		},
		{
			name: "no_values",
			stmt: &InsertStmt{
				Table:   "empty",
				Columns: []string{"a"},
				Values:  [][]Value{},
			},
			tableRoot: 100,
			wantErr:   true,
		},
		{
			name: "mismatched_columns",
			stmt: &InsertStmt{
				Table:   "mismatch",
				Columns: []string{"a", "b"},
				Values: [][]Value{
					{IntValue(1)}, // Only 1 value, need 2
				},
			},
			tableRoot: 100,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog, err := CompileInsert(tt.stmt, tt.tableRoot)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if prog == nil {
				t.Fatal("program is nil")
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, prog)
			}
		})
	}
}

func TestValidateInsert(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *InsertStmt
		wantErr bool
	}{
		{
			name: "valid",
			stmt: &InsertStmt{
				Table:   "test",
				Columns: []string{"a"},
				Values:  [][]Value{{IntValue(1)}},
			},
			wantErr: false,
		},
		{
			name:    "nil",
			stmt:    nil,
			wantErr: true,
		},
		{
			name: "no_table",
			stmt: &InsertStmt{
				Table:   "",
				Columns: []string{"a"},
				Values:  [][]Value{{IntValue(1)}},
			},
			wantErr: true,
		},
		{
			name: "no_values",
			stmt: &InsertStmt{
				Table:   "test",
				Columns: []string{"a"},
				Values:  [][]Value{},
			},
			wantErr: true,
		},
		{
			name: "inconsistent_row_lengths",
			stmt: &InsertStmt{
				Table:   "test",
				Columns: []string{"a", "b"},
				Values: [][]Value{
					{IntValue(1), IntValue(2)},
					{IntValue(3)}, // Wrong length
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateInsert(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProgramDisassemble(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "test",
		Columns: []string{"id", "name"},
		Values: [][]Value{
			{IntValue(1), TextValue("Alice")},
		},
	}

	prog, err := CompileInsert(stmt, 100)
	if err != nil {
		t.Fatal(err)
	}

	output := prog.Disassemble()
	if output == "" {
		t.Error("disassembly is empty")
	}

	// Check for key elements
	required := []string{"Init", "OpenWrite", "NewRowid", "MakeRecord", "Insert", "Close", "Halt"}
	for _, keyword := range required {
		if !strings.Contains(output, keyword) {
			t.Errorf("disassembly missing keyword: %s", keyword)
		}
	}
}

func TestNewInsertStmt(t *testing.T) {
	table := "users"
	columns := []string{"id", "name"}
	values := [][]Value{
		{IntValue(1), TextValue("Alice")},
	}

	stmt := NewInsertStmt(table, columns, values)

	if stmt.Table != table {
		t.Errorf("Table = %q, want %q", stmt.Table, table)
	}

	if len(stmt.Columns) != len(columns) {
		t.Errorf("Columns count = %d, want %d", len(stmt.Columns), len(columns))
	}

	if len(stmt.Values) != len(values) {
		t.Errorf("Values count = %d, want %d", len(stmt.Values), len(values))
	}
}

func TestCompileInsertWithAutoInc(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "auto_table",
		Columns: []string{"name"},
		Values: [][]Value{
			{TextValue("test")},
		},
	}

	prog, err := CompileInsertWithAutoInc(stmt, 100, true)
	if err != nil {
		t.Fatal(err)
	}

	if prog == nil {
		t.Fatal("program is nil")
	}

	// Verify NewRowid is present (handles auto-increment)
	hasNewRowid := false
	for _, inst := range prog.Instructions {
		if inst.OpCode == OpNewRowid {
			hasNewRowid = true
			break
		}
	}

	if !hasNewRowid {
		t.Error("expected NewRowid instruction for auto-increment")
	}
}

func TestInstructionString(t *testing.T) {
	tests := []struct {
		opcode OpCode
		want   string
	}{
		{OpInit, "Init"},
		{OpHalt, "Halt"},
		{OpInsert, "Insert"},
		{OpOpenWrite, "OpenWrite"},
		{OpMakeRecord, "MakeRecord"},
		{OpNewRowid, "NewRowid"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.opcode.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProgramRegisterAllocation(t *testing.T) {
	prog := &Program{}

	reg1 := prog.allocReg()
	reg2 := prog.allocReg()
	reg3 := prog.allocRegs(5)

	if reg1 != 0 {
		t.Errorf("first register = %d, want 0", reg1)
	}

	if reg2 != 1 {
		t.Errorf("second register = %d, want 1", reg2)
	}

	if reg3 != 2 {
		t.Errorf("register block start = %d, want 2", reg3)
	}

	if prog.NumRegisters != 7 {
		t.Errorf("total registers = %d, want 7", prog.NumRegisters)
	}
}

// Benchmark tests
func BenchmarkCompileInsertSingleRow(b *testing.B) {
	stmt := &InsertStmt{
		Table:   "bench",
		Columns: []string{"id", "name", "value"},
		Values: [][]Value{
			{IntValue(1), TextValue("test"), FloatValue(3.14)},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompileInsert(stmt, 100)
	}
}

func BenchmarkCompileInsertMultipleRows(b *testing.B) {
	values := make([][]Value, 100)
	for i := 0; i < 100; i++ {
		values[i] = []Value{
			IntValue(int64(i)),
			TextValue("name" + string(rune(i))),
			FloatValue(float64(i) * 3.14),
		}
	}

	stmt := &InsertStmt{
		Table:   "bench",
		Columns: []string{"id", "name", "value"},
		Values:  values,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompileInsert(stmt, 100)
	}
}
