// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner_test

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
)

// Example demonstrates basic query planning
func Example_basicQuery() {
	// Create a table with indexes
	table := &planner.TableInfo{
		Name:      "products",
		Cursor:    0,
		RowCount:  100000,
		RowLogEst: planner.NewLogEst(100000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "category", Index: 2, Type: "TEXT", NotNull: true},
			{Name: "price", Index: 3, Type: "REAL", NotNull: false},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name:      "pk_products",
				Table:     "products",
				Unique:    true,
				Primary:   true,
				RowCount:  100000,
				RowLogEst: planner.NewLogEst(100000),
				Columns: []planner.IndexColumn{
					{Name: "id", Index: 0, Ascending: true},
				},
				ColumnStats: []planner.LogEst{0}, // Unique
			},
			{
				Name:      "idx_category_price",
				Table:     "products",
				Unique:    false,
				Primary:   false,
				RowCount:  100000,
				RowLogEst: planner.NewLogEst(100000),
				Columns: []planner.IndexColumn{
					{Name: "category", Index: 2, Ascending: true},
					{Name: "price", Index: 3, Ascending: true},
				},
				ColumnStats: []planner.LogEst{
					planner.NewLogEst(50), // ~2000 rows per category
					planner.NewLogEst(5),  // ~32 rows per category+price
				},
			},
		},
	}

	// Create WHERE clause: category = 'Electronics' AND price < 100
	whereClause := &planner.WhereClause{
		Terms: []*planner.WhereTerm{
			{
				Operator:   planner.WO_EQ,
				LeftCursor: 0,
				LeftColumn: 2, // category
				RightValue: "Electronics",
			},
			{
				Operator:   planner.WO_LT,
				LeftCursor: 0,
				LeftColumn: 3, // price
				RightValue: 100.0,
			},
		},
	}

	// Plan the query
	p := planner.NewPlanner()
	info, err := p.PlanQuery([]*planner.TableInfo{table}, whereClause)
	if err != nil {
		panic(err)
	}

	// Explain the plan
	fmt.Println(p.ExplainPlan(info))

	// Output should show use of idx_category_price
}

// Example demonstrates join planning
func Example_joinQuery() {
	// Create tables
	customers := &planner.TableInfo{
		Name:      "customers",
		Cursor:    0,
		RowCount:  10000,
		RowLogEst: planner.NewLogEst(10000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "country", Index: 2, Type: "TEXT", NotNull: false},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name:        "pk_customers",
				Table:       "customers",
				Unique:      true,
				Primary:     true,
				RowCount:    10000,
				RowLogEst:   planner.NewLogEst(10000),
				Columns:     []planner.IndexColumn{{Name: "id", Index: 0}},
				ColumnStats: []planner.LogEst{0},
			},
		},
	}

	orders := &planner.TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  100000,
		RowLogEst: planner.NewLogEst(100000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "customer_id", Index: 1, Type: "INTEGER", NotNull: true},
			{Name: "amount", Index: 2, Type: "REAL", NotNull: true},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name:        "idx_customer_id",
				Table:       "orders",
				Unique:      false,
				RowCount:    100000,
				RowLogEst:   planner.NewLogEst(100000),
				Columns:     []planner.IndexColumn{{Name: "customer_id", Index: 1}},
				ColumnStats: []planner.LogEst{planner.NewLogEst(10)}, // ~10 orders per customer
			},
		},
	}

	// WHERE clause: customers.id = 123 AND orders.customer_id = customers.id
	whereClause := &planner.WhereClause{
		Terms: []*planner.WhereTerm{
			{
				Operator:   planner.WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0, // customers.id
				RightValue: 123,
			},
			{
				Operator:    planner.WO_EQ,
				LeftCursor:  1,
				LeftColumn:  1,                       // orders.customer_id
				PrereqRight: planner.Bitmask(1 << 0), // References customers
			},
		},
	}

	// Plan the query
	p := planner.NewPlanner()
	info, err := p.PlanQuery([]*planner.TableInfo{customers, orders}, whereClause)
	if err != nil {
		panic(err)
	}

	fmt.Println(p.ExplainPlan(info))

	// Should show nested loop join with customers first (has constant constraint)
}

// Example demonstrates index selection
func Example_indexSelection() {
	// Create table with multiple indexes
	table := &planner.TableInfo{
		Name:      "events",
		Cursor:    0,
		RowCount:  1000000,
		RowLogEst: planner.NewLogEst(1000000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
			{Name: "timestamp", Index: 2, Type: "INTEGER"},
			{Name: "type", Index: 3, Type: "TEXT"},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name:        "idx_user_id",
				Table:       "events",
				Columns:     []planner.IndexColumn{{Name: "user_id", Index: 1}},
				RowLogEst:   planner.NewLogEst(1000000),
				ColumnStats: []planner.LogEst{planner.NewLogEst(100)}, // ~100 events per user
			},
			{
				Name:        "idx_timestamp",
				Table:       "events",
				Columns:     []planner.IndexColumn{{Name: "timestamp", Index: 2}},
				RowLogEst:   planner.NewLogEst(1000000),
				ColumnStats: []planner.LogEst{planner.NewLogEst(1000)}, // Less selective
			},
			{
				Name:  "idx_user_timestamp",
				Table: "events",
				Columns: []planner.IndexColumn{
					{Name: "user_id", Index: 1},
					{Name: "timestamp", Index: 2},
				},
				RowLogEst: planner.NewLogEst(1000000),
				ColumnStats: []planner.LogEst{
					planner.NewLogEst(100), // By user
					planner.NewLogEst(1),   // By user+time
				},
			},
		},
	}

	// WHERE clause: user_id = 42 AND timestamp > 1000000
	terms := []*planner.WhereTerm{
		{
			Operator:   planner.WO_EQ,
			LeftCursor: 0,
			LeftColumn: 1, // user_id
			RightValue: 42,
		},
		{
			Operator:   planner.WO_GT,
			LeftCursor: 0,
			LeftColumn: 2, // timestamp
			RightValue: 1000000,
		},
	}

	// Select best index
	cm := planner.NewCostModel()
	selector := planner.NewIndexSelector(table, terms, cm)
	bestIndex := selector.SelectBestIndex()

	if bestIndex != nil {
		fmt.Printf("Selected index: %s\n", bestIndex.Name)

		// Analyze how the index will be used
		usage := selector.AnalyzeIndexUsage(bestIndex, []string{"id", "type"})
		fmt.Printf("Usage: %s\n", usage.Explain())
	}

	// Should select idx_user_timestamp (compound index on both constrained columns)
}

// Example demonstrates cost comparison
func Example_costComparison() {
	cm := planner.NewCostModel()

	// Create a simple table
	table := &planner.TableInfo{
		Name:      "items",
		RowCount:  50000,
		RowLogEst: planner.NewLogEst(50000),
	}

	// Compare full scan vs index scan
	fullScanCost, fullScanRows := cm.EstimateFullScan(table)

	// Simulate an index with selectivity
	index := &planner.IndexInfo{
		Name:        "idx_items",
		RowLogEst:   table.RowLogEst,
		ColumnStats: []planner.LogEst{planner.NewLogEst(100)}, // 1% selectivity
	}

	indexCost, indexRows := cm.EstimateIndexScan(table, index, nil, 1, false, false)

	fmt.Printf("Full scan: cost=%d, rows=%d\n", fullScanCost.ToInt(), fullScanRows.ToInt())
	fmt.Printf("Index scan: cost=%d, rows=%d\n", indexCost.ToInt(), indexRows.ToInt())

	if cm.CompareCosts(indexCost, indexRows, fullScanCost, fullScanRows) {
		fmt.Println("Index scan is better")
	} else {
		fmt.Println("Full scan is better")
	}
}

// Example demonstrates query optimization with expressions
func Example_queryOptimization() {
	p := planner.NewPlanner()

	// Create table
	table := &planner.TableInfo{
		Name:      "employees",
		Cursor:    0,
		RowCount:  5000,
		RowLogEst: planner.NewLogEst(5000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
			{Name: "dept", Index: 1, Type: "TEXT"},
			{Name: "salary", Index: 2, Type: "REAL"},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name: "idx_dept_salary",
				Columns: []planner.IndexColumn{
					{Name: "dept", Index: 1},
					{Name: "salary", Index: 2},
				},
				RowLogEst:   planner.NewLogEst(5000),
				ColumnStats: []planner.LogEst{planner.NewLogEst(50), planner.NewLogEst(5)},
			},
		},
	}

	// Build WHERE expression: dept = 'Sales' AND salary > 50000
	col1 := &planner.ColumnExpr{Table: "employees", Column: "dept", Cursor: 0}
	val1 := &planner.ValueExpr{Value: "Sales"}
	expr1 := &planner.BinaryExpr{Op: "=", Left: col1, Right: val1}

	col2 := &planner.ColumnExpr{Table: "employees", Column: "salary", Cursor: 0}
	val2 := &planner.ValueExpr{Value: 50000.0}
	expr2 := &planner.BinaryExpr{Op: ">", Left: col2, Right: val2}

	andExpr := &planner.AndExpr{Terms: []planner.Expr{expr1, expr2}}

	// Optimize WHERE clause
	whereClause, err := p.OptimizeWhereClause(andExpr, []*planner.TableInfo{table})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Optimized WHERE clause has %d terms\n", len(whereClause.Terms))

	// Plan the query
	info, err := p.PlanQuery([]*planner.TableInfo{table}, whereClause)
	if err != nil {
		panic(err)
	}

	fmt.Println(p.ExplainPlan(info))
}

// Example demonstrates practical query planning scenario
func Example_practicalScenario() {
	// Scenario: E-commerce database with products and orders

	// Products table
	products := &planner.TableInfo{
		Name:      "products",
		Cursor:    0,
		RowCount:  10000,
		RowLogEst: planner.NewLogEst(10000),
		Columns: []planner.ColumnInfo{
			{Name: "id", Index: 0},
			{Name: "category", Index: 1},
			{Name: "price", Index: 2},
		},
		Indexes: []*planner.IndexInfo{
			{
				Name:        "pk_products",
				Unique:      true,
				Primary:     true,
				Columns:     []planner.IndexColumn{{Name: "id", Index: 0}},
				RowLogEst:   planner.NewLogEst(10000),
				ColumnStats: []planner.LogEst{0},
			},
		},
	}

	// Query: SELECT * FROM products WHERE category = 'Electronics' AND price BETWEEN 100 AND 500
	whereClause := &planner.WhereClause{
		Terms: []*planner.WhereTerm{
			{
				Operator:   planner.WO_EQ,
				LeftCursor: 0,
				LeftColumn: 1,
				RightValue: "Electronics",
			},
			{
				Operator:   planner.WO_GE,
				LeftCursor: 0,
				LeftColumn: 2,
				RightValue: 100.0,
			},
			{
				Operator:   planner.WO_LE,
				LeftCursor: 0,
				LeftColumn: 2,
				RightValue: 500.0,
			},
		},
	}

	p := planner.NewPlanner()
	info, err := p.PlanQuery([]*planner.TableInfo{products}, whereClause)
	if err != nil {
		panic(err)
	}

	// Validate the plan
	if err := p.ValidatePlan(info); err != nil {
		panic(err)
	}

	fmt.Printf("Query plan is valid\n")
	fmt.Printf("Estimated rows: %d\n", info.NOut.ToInt())
	fmt.Println("\nDetailed plan:")
	fmt.Println(p.ExplainPlan(info))
}
size before=%d, size after=%d",
		info.FramesCheckpointed, info.FramesRemaining, info.WALSizeBefore, info.WALSizeAfter)
}

func TestCheckpointWithInfo_Restart(t *testing.T) {
	t.Parallel()
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i), uint32(i))
	}

	// Checkpoint with info
	info, err := wal.CheckpointWithInfo(CheckpointRestart)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if info.FramesCheckpointed != 3 {
		t.Errorf("Expected 3 frames checkpointed, got %d", info.FramesCheckpointed)
	}

	if info.FramesRemaining != 0 {
		t.Errorf("Expected 0 frames remaining, got %d", info.FramesRemaining)
	}

	// After restart, WAL should only have header
	if info.WALSizeAfter != WALHeaderSize {
		t.Errorf("WAL size after restart should be %d, got %d",
			WALHeaderSize, info.WALSizeAfter)
	}
}

func TestCheckpointWithInfo_Truncate(t *testing.T) {
	t.Parallel()
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i), uint32(i))
	}

	// Checkpoint with info
	info, err := wal.CheckpointWithInfo(CheckpointTruncate)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if info.FramesCheckpointed != 3 {
		t.Errorf("Expected 3 frames checkpointed, got %d", info.FramesCheckpointed)
	}

	if info.FramesRemaining != 0 {
		t.Errorf("Expected 0 frames remaining, got %d", info.FramesRemaining)
	}

	// After truncate, WAL should be empty
	if info.WALSizeAfter != 0 {
		t.Errorf("WAL size after truncate should be 0, got %d", info.WALSizeAfter)
	}
}

// Test reopening after checkpoint

func TestCheckpoint_ReopenAfterRestart(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create database file
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 10; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	// Create WAL and write frames
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*50), uint32(i))
	}

	// Checkpoint and close
	_, _, err = wal.CheckpointWithMode(CheckpointRestart)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
	wal.Close()

	// Reopen WAL
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// WAL should be empty after restart
	if wal2.FrameCount() != 0 {
		t.Errorf("Reopened WAL should be empty, got %d frames", wal2.FrameCount())
	}

	// Should be able to write new frames
	writeTestFrameToWAL(t, wal2, 5, 0xEE, 5)

	if wal2.FrameCount() != 1 {
		t.Errorf("Expected 1 frame in reopened WAL, got %d", wal2.FrameCount())
	}
}

// Test large WAL checkpoint

func TestCheckpoint_LargeWAL(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping slow test in short mode")
	}

	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write many frames
	numPages := 50
	for i := 1; i <= numPages; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i%256), uint32(i))
	}

	// Checkpoint all
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numPages {
		t.Errorf("Expected %d frames checkpointed, got %d", numPages, checkpointed)
	}

	// Spot check some pages
	testPages := []Pgno{1, 10, 25, 40, 50}
	for _, pageNum := range testPages {
		verifyPageInDatabase(t, dbFile, pageNum, byte(pageNum%256), DefaultPageSize)
	}
}

// Benchmark tests

func BenchmarkCheckpointPassive(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointPassive)
		b.StopTimer()

		wal.Close()
		os.Truncate(dbFile+"-wal", 0)
	}
}

func BenchmarkCheckpointFull(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointFull)
		b.StopTimer()

		wal.Close()
		os.Truncate(dbFile+"-wal", 0)
	}
}

func BenchmarkCheckpointRestart(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointRestart)
		b.StopTimer()

		wal.Close()
	}
}
