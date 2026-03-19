// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vtab

import (
	"testing"
)

// TestModuleRegistry tests the module registry functionality.
func TestModuleRegistry(t *testing.T) {
	t.Parallel()
	registry := NewModuleRegistry()

	// Test registering a module
	module := &testModule{name: "test_module"}
	err := registry.RegisterModule("test", module)
	if err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Test retrieving a registered module
	retrieved := registry.GetModule("test")
	if retrieved == nil {
		t.Fatal("Failed to retrieve registered module")
	}
	if retrieved != module {
		t.Error("Retrieved module is not the same as registered module")
	}

	// Test HasModule
	if !registry.HasModule("test") {
		t.Error("HasModule returned false for registered module")
	}
	if registry.HasModule("nonexistent") {
		t.Error("HasModule returned true for non-existent module")
	}

	// Test duplicate registration
	err = registry.RegisterModule("test", module)
	if err == nil {
		t.Error("Expected error when registering duplicate module")
	}

	// Test ListModules
	modules := registry.ListModules()
	if len(modules) != 1 {
		t.Errorf("Expected 1 module, got %d", len(modules))
	}
	if len(modules) > 0 && modules[0] != "test" {
		t.Errorf("Expected module name 'test', got %s", modules[0])
	}

	// Test UnregisterModule
	err = registry.UnregisterModule("test")
	if err != nil {
		t.Fatalf("Failed to unregister module: %v", err)
	}

	if registry.HasModule("test") {
		t.Error("Module still exists after unregistration")
	}

	// Test unregistering non-existent module
	err = registry.UnregisterModule("nonexistent")
	if err == nil {
		t.Error("Expected error when unregistering non-existent module")
	}

	// Test Clear
	registry.RegisterModule("m1", module)
	registry.RegisterModule("m2", module)
	registry.Clear()
	if len(registry.ListModules()) != 0 {
		t.Error("Registry not empty after Clear()")
	}
}

// TestGlobalRegistry tests the global registry functions.
func TestGlobalRegistry(t *testing.T) {
	t.Parallel()
	// Clear any existing modules
	DefaultRegistry().Clear()

	module := &testModule{name: "global_test"}

	// Test RegisterModule
	err := RegisterModule("global", module)
	if err != nil {
		t.Fatalf("Failed to register module globally: %v", err)
	}

	// Test GetModule
	retrieved := GetModule("global")
	if retrieved == nil {
		t.Fatal("Failed to retrieve globally registered module")
	}

	// Test HasModule
	if !HasModule("global") {
		t.Error("HasModule returned false for globally registered module")
	}

	// Test ListModules
	modules := ListModules()
	found := false
	for _, name := range modules {
		name := name
		if name == "global" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Globally registered module not in list")
	}

	// Clean up
	UnregisterModule("global")
}

// TestIndexInfo tests the IndexInfo structure and methods.
func TestIndexInfo(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(3)

	// Test initialization
	if len(info.Constraints) != 3 {
		t.Errorf("Expected 3 constraints, got %d", len(info.Constraints))
	}
	if len(info.ConstraintUsage) != 3 {
		t.Errorf("Expected 3 constraint usages, got %d", len(info.ConstraintUsage))
	}
	if info.EstimatedCost != 1000000.0 {
		t.Errorf("Expected default cost 1000000.0, got %f", info.EstimatedCost)
	}

	// Test SetConstraintUsage
	info.SetConstraintUsage(0, 1, true)
	if info.ConstraintUsage[0].ArgvIndex != 1 {
		t.Errorf("Expected ArgvIndex 1, got %d", info.ConstraintUsage[0].ArgvIndex)
	}
	if !info.ConstraintUsage[0].Omit {
		t.Error("Expected Omit to be true")
	}

	// Test CountUsableConstraints
	info.Constraints[0].Usable = true
	info.Constraints[1].Usable = true
	info.Constraints[2].Usable = false
	count := info.CountUsableConstraints()
	if count != 2 {
		t.Errorf("Expected 2 usable constraints, got %d", count)
	}

	// Test FindConstraint
	info.Constraints[0].Column = 0
	info.Constraints[0].Op = ConstraintEQ
	info.Constraints[0].Usable = true
	idx := info.FindConstraint(0, ConstraintEQ)
	if idx != 0 {
		t.Errorf("Expected constraint at index 0, got %d", idx)
	}

	idx = info.FindConstraint(5, ConstraintEQ)
	if idx != -1 {
		t.Errorf("Expected -1 for non-existent constraint, got %d", idx)
	}

	// Test HasOrderBy
	if info.HasOrderBy() {
		t.Error("Expected HasOrderBy to be false initially")
	}
	info.OrderBy = append(info.OrderBy, OrderBy{Column: 0, Desc: false})
	if !info.HasOrderBy() {
		t.Error("Expected HasOrderBy to be true after adding order by")
	}

	// Test IsColumnUsed
	info.ColUsed = 0x05 // Binary: 00000101 (columns 0 and 2 used)
	if !info.IsColumnUsed(0) {
		t.Error("Expected column 0 to be used")
	}
	if info.IsColumnUsed(1) {
		t.Error("Expected column 1 to not be used")
	}
	if !info.IsColumnUsed(2) {
		t.Error("Expected column 2 to be used")
	}
}

// TestConstraintOp tests the ConstraintOp string representation.
func TestConstraintOp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op       ConstraintOp
		expected string
	}{
		{ConstraintEQ, "="},
		{ConstraintGT, ">"},
		{ConstraintLE, "<="},
		{ConstraintLT, "<"},
		{ConstraintGE, ">="},
		{ConstraintNE, "!="},
		{ConstraintIsNull, "IS NULL"},
		{ConstraintIsNotNull, "IS NOT NULL"},
		{ConstraintLike, "LIKE"},
		{ConstraintGlob, "GLOB"},
		{ConstraintMatch, "MATCH"},
		{ConstraintRegexp, "REGEXP"},
	}

	for _, tt := range tests {
		tt := tt
		result := tt.op.String()
		if result != tt.expected {
			t.Errorf("Expected %s.String() = %q, got %q", tt.op, tt.expected, result)
		}
	}
}

// TestBaseVirtualTable tests the base virtual table implementation.
func TestBaseVirtualTable(t *testing.T) {
	t.Parallel()
	base := &BaseVirtualTable{}

	// Test BestIndex (should succeed with default values)
	info := NewIndexInfo(0)
	err := base.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// Test Open (should fail - not implemented)
	_, err = base.Open()
	if err == nil {
		t.Error("Expected Open to return error")
	}

	// Test Disconnect (should succeed - no-op)
	err = base.Disconnect()
	if err != nil {
		t.Errorf("Disconnect failed: %v", err)
	}

	// Test Destroy (should fail - not supported)
	err = base.Destroy()
	if err == nil {
		t.Error("Expected Destroy to return error")
	}

	// Test Update (should fail - read-only)
	_, err = base.Update(1, []interface{}{})
	if err == nil {
		t.Error("Expected Update to return error")
	}

	// Test transaction methods (should succeed - no-ops)
	if err := base.Begin(); err != nil {
		t.Errorf("Begin failed: %v", err)
	}
	if err := base.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
	if err := base.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
	if err := base.Rollback(); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// Test Rename (should fail - not supported)
	err = base.Rename("new_name")
	if err == nil {
		t.Error("Expected Rename to return error")
	}
}

// TestBaseCursor tests the base cursor implementation.
func TestBaseCursor(t *testing.T) {
	t.Parallel()
	cursor := &BaseCursor{}

	// Test Filter (should succeed and set EOF)
	err := cursor.Filter(0, "", nil)
	if err != nil {
		t.Errorf("Filter failed: %v", err)
	}
	if !cursor.EOF() {
		t.Error("Expected EOF after Filter")
	}

	// Test Next (should set EOF)
	cursor.eof = false
	err = cursor.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if !cursor.EOF() {
		t.Error("Expected EOF after Next")
	}

	// Test Column (should fail - not implemented)
	_, err = cursor.Column(0)
	if err == nil {
		t.Error("Expected Column to return error")
	}

	// Test Rowid (should fail - not implemented)
	_, err = cursor.Rowid()
	if err == nil {
		t.Error("Expected Rowid to return error")
	}

	// Test Close (should succeed - no-op)
	err = cursor.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// Test helpers

// testModule is a simple test module implementation.
type testModule struct {
	BaseModule
	name string
}

func (m *testModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (VirtualTable, string, error) {
	return &testVirtualTable{
		module: m,
	}, "CREATE TABLE test(id INTEGER, name TEXT)", nil
}

// testVirtualTable is a simple test virtual table implementation.
type testVirtualTable struct {
	BaseVirtualTable
	module *testModule
}

func (t *testVirtualTable) Open() (VirtualCursor, error) {
	return &testCursor{
		table: t,
		rows: [][]interface{}{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
			{int64(3), "Charlie"},
		},
		pos: -1,
	}, nil
}

// testCursor is a simple test cursor implementation.
type testCursor struct {
	BaseCursor
	table *testVirtualTable
	rows  [][]interface{}
	pos   int
}

func (c *testCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	c.pos = 0
	return nil
}

func (c *testCursor) Next() error {
	c.pos++
	return nil
}

func (c *testCursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.rows)
}

func (c *testCursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, nil
	}
	if index < 0 || index >= len(c.rows[c.pos]) {
		return nil, nil
	}
	return c.rows[c.pos][index], nil
}

func (c *testCursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, nil
	}
	return int64(c.pos), nil
}

// TestVirtualTableCursor tests a complete virtual table implementation.
func TestVirtualTableCursor(t *testing.T) {
	t.Parallel()
	module := &testModule{name: "test"}
	vtable, schema, err := module.Connect(nil, "test", "main", "test", nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	// Test Filter
	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Iterate through rows
	count := 0
	for !cursor.EOF() {
		// Test Column
		id, err := cursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		name, err := cursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}

		// Test Rowid
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}

		t.Logf("Row %d: id=%v, name=%v, rowid=%v", count, id, name, rowid)

		count++
		err = cursor.Next()
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

// TestBaseModuleCreate tests the BaseModule Create method.
func TestBaseModuleCreate(t *testing.T) {
	t.Parallel()
	base := &BaseModule{}

	_, _, err := base.Create(nil, "test", "main", "test_table", []string{})
	if err == nil {
		t.Error("Expected error from BaseModule.Create")
	}

	if err != nil {
		t.Logf("BaseModule.Create error: %v", err)
	}
}

// TestBaseModuleConnect tests the BaseModule Connect method.
func TestBaseModuleConnect(t *testing.T) {
	t.Parallel()
	base := &BaseModule{}

	_, _, err := base.Connect(nil, "test", "main", "test_table", []string{})
	if err == nil {
		t.Error("Expected error from BaseModule.Connect")
	}

	if err != nil {
		t.Logf("BaseModule.Connect error: %v", err)
	}
}

// TestIsColumnUsedAllCases tests IsColumnUsed with various column indices.
func TestIsColumnUsedAllCases(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(0)

	tests := []struct {
		colUsed uint64
		column  int
		want    bool
	}{
		{0x01, 0, true},     // Bit 0 set
		{0x02, 1, true},     // Bit 1 set
		{0x04, 2, true},     // Bit 2 set
		{0x01, 1, false},    // Bit 1 not set
		{0x00, 0, false},    // No bits set
		{0xFF, 7, true},     // Bit 7 set
		{0xFF, 8, false},    // Bit 8 not set in 0xFF
		{1 << 63, 63, true}, // Highest bit set
	}

	for _, tt := range tests {
		tt := tt
		info.ColUsed = tt.colUsed
		result := info.IsColumnUsed(tt.column)
		if result != tt.want {
			t.Errorf("IsColumnUsed(col=%d, colUsed=0x%X) = %v, want %v",
				tt.column, tt.colUsed, result, tt.want)
		}
	}
}

// TestConstraintOpStringUnknown tests String method with unknown constraint op.
func TestConstraintOpStringUnknown(t *testing.T) {
	t.Parallel()
	// Test with invalid constraint op value
	op := ConstraintOp(999)
	str := op.String()

	// Should return some string representation
	if str == "" {
		t.Error("Expected non-empty string for unknown constraint op")
	}

	t.Logf("Unknown constraint op string: %s", str)
}

// TestIndexInfoManyConstraints tests IndexInfo with many constraints.
func TestIndexInfoManyConstraints(t *testing.T) {
	t.Parallel()
	// Test with large number of constraints
	info := NewIndexInfo(100)

	if len(info.Constraints) != 100 {
		t.Errorf("Expected 100 constraints, got %d", len(info.Constraints))
	}

	if len(info.ConstraintUsage) != 100 {
		t.Errorf("Expected 100 constraint usages, got %d", len(info.ConstraintUsage))
	}

	// Set usage for various constraints
	for i := 0; i < 100; i += 10 {
		info.SetConstraintUsage(i, i+1, i%2 == 0)
	}

	// Verify settings
	for i := 0; i < 100; i += 10 {
		if info.ConstraintUsage[i].ArgvIndex != i+1 {
			t.Errorf("Constraint %d argv index = %d, want %d",
				i, info.ConstraintUsage[i].ArgvIndex, i+1)
		}
	}
}

// TestFindConstraintNotFound tests FindConstraint when constraint not found.
func TestFindConstraintNotFound(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(5)

	// Set up constraints
	for i := 0; i < 5; i++ {
		info.Constraints[i].Column = i
		info.Constraints[i].Op = ConstraintEQ
		info.Constraints[i].Usable = true
	}

	// Search for non-existent column
	idx := info.FindConstraint(10, ConstraintEQ)
	if idx != -1 {
		t.Errorf("Expected -1 for non-existent column, got %d", idx)
	}

	// Search for non-existent operator
	idx = info.FindConstraint(0, ConstraintMatch)
	if idx != -1 {
		t.Errorf("Expected -1 for non-existent operator, got %d", idx)
	}

	// Search for unusable constraint
	info.Constraints[2].Usable = false
	idx = info.FindConstraint(2, ConstraintEQ)
	if idx != -1 {
		t.Errorf("Expected -1 for unusable constraint, got %d", idx)
	}
}

// TestIsColumnUsedLargeColumn tests IsColumnUsed with large column numbers.
func TestIsColumnUsedLargeColumn(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(0)

	// Test column 63 (highest valid bit)
	info.ColUsed = 1 << 63
	result := info.IsColumnUsed(63)
	if !result {
		t.Error("Expected true for column 63 when bit 63 is set")
	}

	// Test with column number at or beyond 64
	// The implementation may handle this differently
	result = info.IsColumnUsed(64)
	t.Logf("IsColumnUsed(64) = %v", result)

	// Test column 0 when bit 63 is set
	result = info.IsColumnUsed(0)
	if result {
		t.Error("Expected false for column 0 when only bit 63 is set")
	}
}

// TestCountUsableConstraintsNone tests CountUsableConstraints with no usable constraints.
func TestCountUsableConstraintsNone(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(5)

	// Set all constraints to unusable
	for i := 0; i < 5; i++ {
		info.Constraints[i].Usable = false
	}

	count := info.CountUsableConstraints()
	if count != 0 {
		t.Errorf("Expected 0 usable constraints, got %d", count)
	}
}

// TestHasOrderByEmpty tests HasOrderBy with empty order by list.
func TestHasOrderByEmpty(t *testing.T) {
	t.Parallel()
	info := NewIndexInfo(0)

	if info.HasOrderBy() {
		t.Error("Expected false for empty order by list")
	}

	// Add empty OrderBy slice
	info.OrderBy = []OrderBy{}
	if info.HasOrderBy() {
		t.Error("Expected false for empty order by slice")
	}
}
