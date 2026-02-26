package sql

import (
	"fmt"
)

// This file contains examples of how to use the SELECT compiler.

// Example1_SimpleSelect demonstrates compiling a simple SELECT statement.
// SQL: SELECT id, name FROM users WHERE age > 18
func Example1_SimpleSelect() error {
	// Create parse context
	db := &Database{Name: "test.db"}
	parse := &Parse{
		DB: db,
	}

	// Define the users table
	usersTable := &Table{
		Name:       "users",
		NumColumns: 3,
		RootPage:   2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER, PrimaryKey: true},
			{Name: "name", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
			{Name: "age", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER},
		},
	}

	// Build SELECT AST
	sel := &Select{
		Op:       TK_SELECT,
		SelFlags: SF_Resolved,
		SelectID: 1,

		// SELECT id, name
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:     TK_COLUMN,
						Table:  0, // Cursor 0
						Column: 0, // id column
					},
					Name: "id",
				},
				{
					Expr: &Expr{
						Op:     TK_COLUMN,
						Table:  0,
						Column: 1, // name column
					},
					Name: "name",
				},
			},
		},

		// FROM users
		Src: &SrcList{
			Items: []SrcListItem{
				{
					Name:   "users",
					Table:  usersTable,
					Cursor: 0,
				},
			},
		},

		// WHERE age > 18
		Where: &Expr{
			Op: TK_GT,
			Left: &Expr{
				Op:     TK_COLUMN,
				Table:  0,
				Column: 2, // age column
			},
			Right: &Expr{
				Op:       TK_INTEGER,
				IntValue: 18,
			},
		},
	}

	// Set up destination
	dest := &SelectDest{}
	InitSelectDest(dest, SRT_Output, 0)

	// Compile SELECT
	compiler := NewSelectCompiler(parse)
	if err := compiler.CompileSelect(sel, dest); err != nil {
		return fmt.Errorf("compilation failed: %w", err)
	}

	// Display generated VDBE code
	fmt.Println("Generated VDBE program for: SELECT id, name FROM users WHERE age > 18")
	fmt.Println()
	DisplayVdbeProgram(parse.Vdbe)

	return nil
}

// Example2_SelectWithOrderBy demonstrates SELECT with ORDER BY.
// SQL: SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10
func Example2_SelectWithOrderBy() error {
	db := &Database{Name: "test.db"}
	parse := &Parse{DB: db}

	employeesTable := &Table{
		Name:       "employees",
		NumColumns: 3,
		RootPage:   3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER},
			{Name: "name", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
			{Name: "salary", DeclType: "REAL", Affinity: SQLITE_AFF_REAL},
		},
	}

	sel := &Select{
		Op:       TK_SELECT,
		SelFlags: SF_Resolved,
		SelectID: 1,

		// SELECT name, salary
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1},
					Name: "name",
				},
				{
					Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 2},
					Name: "salary",
				},
			},
		},

		// FROM employees
		Src: &SrcList{
			Items: []SrcListItem{
				{Name: "employees", Table: employeesTable, Cursor: 0},
			},
		},

		// ORDER BY salary DESC
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{
					Expr:      &Expr{Op: TK_COLUMN, Table: 0, Column: 2},
					SortOrder: SQLITE_SO_DESC,
				},
			},
		},

		// LIMIT 10
		Limit: 10,
	}

	dest := &SelectDest{}
	InitSelectDest(dest, SRT_Output, 0)

	compiler := NewSelectCompiler(parse)
	if err := compiler.CompileSelect(sel, dest); err != nil {
		return err
	}

	fmt.Println("Generated VDBE program for: SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10")
	fmt.Println()
	DisplayVdbeProgram(parse.Vdbe)

	return nil
}

// Example3_SelectWithGroupBy demonstrates SELECT with GROUP BY and aggregates.
// SQL: SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department
func Example3_SelectWithGroupBy() error {
	db := &Database{Name: "test.db"}
	parse := &Parse{DB: db}

	employeesTable := &Table{
		Name:       "employees",
		NumColumns: 3,
		RootPage:   3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER},
			{Name: "department", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
			{Name: "salary", DeclType: "REAL", Affinity: SQLITE_AFF_REAL},
		},
	}

	// Define COUNT and AVG functions
	countFunc := &FuncDef{Name: "count", NumArgs: 0}
	avgFunc := &FuncDef{Name: "avg", NumArgs: 1}

	sel := &Select{
		Op:       TK_SELECT,
		SelFlags: SF_Resolved | SF_Aggregate,
		SelectID: 1,

		// SELECT department, COUNT(*), AVG(salary)
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1},
					Name: "department",
				},
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: countFunc,
					},
					Name: "COUNT(*)",
				},
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: avgFunc,
						List: &ExprList{
							Items: []ExprListItem{
								{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 2}},
							},
						},
					},
					Name: "AVG(salary)",
				},
			},
		},

		// FROM employees
		Src: &SrcList{
			Items: []SrcListItem{
				{Name: "employees", Table: employeesTable, Cursor: 0},
			},
		},

		// GROUP BY department
		GroupBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}},
			},
		},
	}

	dest := &SelectDest{}
	InitSelectDest(dest, SRT_Output, 0)

	compiler := NewSelectCompiler(parse)
	if err := compiler.CompileSelect(sel, dest); err != nil {
		return err
	}

	fmt.Println("Generated VDBE program for: SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department")
	fmt.Println()
	DisplayVdbeProgram(parse.Vdbe)

	return nil
}

// Example4_SelectDistinct demonstrates SELECT DISTINCT.
// SQL: SELECT DISTINCT city FROM customers
func Example4_SelectDistinct() error {
	db := &Database{Name: "test.db"}
	parse := &Parse{DB: db}

	customersTable := &Table{
		Name:       "customers",
		NumColumns: 3,
		RootPage:   4,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER},
			{Name: "name", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
			{Name: "city", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
		},
	}

	sel := &Select{
		Op:       TK_SELECT,
		SelFlags: SF_Resolved | SF_Distinct,
		SelectID: 1,

		// SELECT city
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 2},
					Name: "city",
				},
			},
		},

		// FROM customers
		Src: &SrcList{
			Items: []SrcListItem{
				{Name: "customers", Table: customersTable, Cursor: 0},
			},
		},
	}

	dest := &SelectDest{}
	InitSelectDest(dest, SRT_Output, 0)

	compiler := NewSelectCompiler(parse)
	if err := compiler.CompileSelect(sel, dest); err != nil {
		return err
	}

	fmt.Println("Generated VDBE program for: SELECT DISTINCT city FROM customers")
	fmt.Println()
	DisplayVdbeProgram(parse.Vdbe)

	return nil
}

// DisplayVdbeProgram displays a VDBE program in human-readable format.
func DisplayVdbeProgram(vdbe *Vdbe) {
	if vdbe == nil {
		fmt.Println("No VDBE program")
		return
	}

	fmt.Printf("VDBE Program (%d instructions):\n", len(vdbe.Ops))
	fmt.Println("----------------------------------------")

	for addr, op := range vdbe.Ops {
		displayVdbeOp(addr, op)
	}

	fmt.Println("----------------------------------------")
	displayVdbeColumns(vdbe)
}

// displayVdbeOp prints a single VDBE operation.
func displayVdbeOp(addr int, op VdbeOp) {
	fmt.Printf("%4d: %-20s P1=%-3d P2=%-3d P3=%-3d",
		addr, OpcodeToString(op.Opcode), op.P1, op.P2, op.P3)

	if op.P4 != nil {
		fmt.Printf(" P4=%v", op.P4)
	}
	if op.P5 != 0 {
		fmt.Printf(" P5=%d", op.P5)
	}
	if op.Comment != "" {
		fmt.Printf(" ; %s", op.Comment)
	}
	fmt.Println()
}

// displayVdbeColumns prints VDBE result columns.
func displayVdbeColumns(vdbe *Vdbe) {
	if vdbe.NumCols == 0 {
		return
	}
	fmt.Printf("Result columns (%d):\n", vdbe.NumCols)
	for i := 0; i < vdbe.NumCols; i++ {
		fmt.Printf("  %d: %s", i, vdbe.ColNames[i])
		if vdbe.ColTypes[i] != "" {
			fmt.Printf(" (%s)", vdbe.ColTypes[i])
		}
		fmt.Println()
	}
}

// OpcodeToString converts an opcode to its string representation.
func OpcodeToString(op Opcode) string {
	names := map[Opcode]string{
		OP_Init:          "Init",
		OP_Halt:          "Halt",
		OP_OpenRead:      "OpenRead",
		OP_OpenWrite:     "OpenWrite",
		OP_OpenEphemeral: "OpenEphemeral",
		OP_SorterOpen:    "SorterOpen",
		OP_Close:         "Close",
		OP_Rewind:        "Rewind",
		OP_Next:          "Next",
		OP_Column:        "Column",
		OP_ResultRow:     "ResultRow",
		OP_MakeRecord:    "MakeRecord",
		OP_Insert:        "Insert",
		OP_NewRowid:      "NewRowid",
		OP_IdxInsert:     "IdxInsert",
		OP_Integer:       "Integer",
		OP_String8:       "String8",
		OP_Null:          "Null",
		OP_Copy:          "Copy",
		OP_IfNot:         "IfNot",
		OP_IfPos:         "IfPos",
		OP_Goto:          "Goto",
		OP_AddImm:        "AddImm",
		OP_SorterSort:    "SorterSort",
		OP_SorterData:    "SorterData",
		OP_SorterNext:    "SorterNext",
		OP_SorterInsert:  "SorterInsert",
		OP_OpenPseudo:    "OpenPseudo",
		OP_Sequence:      "Sequence",
		OP_Compare:       "Compare",
		OP_Found:         "Found",
		OP_Yield:         "Yield",
		OP_IsNull:        "IsNull",
		OP_Add:           "Add",
		OP_Gt:            "Gt",
		OP_Lt:            "Lt",
		OP_Ge:            "Ge",
		OP_Le:            "Le",
		OP_Eq:            "Eq",
		OP_Ne:            "Ne",
	}

	if name, ok := names[op]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", op)
}

// RunExamples runs all examples.
func RunExamples() {
	fmt.Println("=== SELECT Compiler Examples ===")

	examples := []struct {
		name string
		fn   func() error
	}{
		{"Simple SELECT", Example1_SimpleSelect},
		{"SELECT with ORDER BY", Example2_SelectWithOrderBy},
		{"SELECT with GROUP BY", Example3_SelectWithGroupBy},
		{"SELECT DISTINCT", Example4_SelectDistinct},
	}

	for _, ex := range examples {
		fmt.Printf("\n### Example: %s ###\n\n", ex.name)
		if err := ex.fn(); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		fmt.Println()
	}
}
