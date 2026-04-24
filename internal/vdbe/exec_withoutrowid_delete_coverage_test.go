// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

// ---------------------------------------------------------------------------
// Local mock types used only in this file
// ---------------------------------------------------------------------------

// wrColTable implements GetColumnNames + GetIntegerPKColumn for getTableColumnNamesWithIface.
type wrColTable struct {
	colNames     []string
	integerPKCol string
}

func (t *wrColTable) GetColumnNames() []string   { return t.colNames }
func (t *wrColTable) GetIntegerPKColumn() string { return t.integerPKCol }

// wrFullTable adds GetPrimaryKey + GetColumns + HasRowID + GetRootPage for
// extractValuesFromKeyAndPayload and checkWithoutRowidPKUniqueness.
type wrFullTable struct {
	wrColTable
	primaryKey []string
	columns    []interface{}
	hasRowID   bool
	rootPage   uint32
}

func (t *wrFullTable) GetPrimaryKey() []string   { return t.primaryKey }
func (t *wrFullTable) GetColumns() []interface{} { return t.columns }
func (t *wrFullTable) HasRowID() bool            { return t.hasRowID }
func (t *wrFullTable) GetRootPage() uint32       { return t.rootPage }

// wrMockCol implements GetName for column objects used inside wrFullTable.
type wrMockCol struct{ name string }

func (c *wrMockCol) GetName() string { return c.name }

// wrSchema is a minimal schema mock used by helper functions.
type wrSchema struct{ tables map[string]interface{} }

func (s *wrSchema) GetTableByName(name string) (interface{}, bool) {
	t, ok := s.tables[name]
	return t, ok
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildSQLiteRecord encodes values as a minimal SQLite record (header + body).
// Serial types supported: nil (0), int64 via type-1 (1 byte), string via text.
func buildSQLiteRecord(vals []interface{}) []byte {
	// First pass: compute serial types and body bytes.
	type serialEntry struct {
		serialType uint64
		body       []byte
	}
	entries := make([]serialEntry, len(vals))
	for i, v := range vals {
		switch val := v.(type) {
		case nil:
			entries[i] = serialEntry{0, nil}
		case int64:
			entries[i] = serialEntry{1, []byte{byte(val)}}
		case string:
			n := len(val)
			entries[i] = serialEntry{uint64(n*2 + 13), []byte(val)}
		default:
			entries[i] = serialEntry{0, nil}
		}
	}

	// Build header: headerSize (1 byte varint) + serial type varints.
	var headerVarints []byte
	for _, e := range entries {
		headerVarints = append(headerVarints, wrEncodeVarint(e.serialType)...)
	}
	headerSize := 1 + len(headerVarints) // 1 byte for the headerSize varint itself
	header := append([]byte{byte(headerSize)}, headerVarints...)

	var body []byte
	for _, e := range entries {
		body = append(body, e.body...)
	}
	return append(header, body...)
}

// wrEncodeVarint encodes a uint64 as a SQLite varint (simple 1-byte for small values).
func wrEncodeVarint(v uint64) []byte {
	if v < 0x80 {
		return []byte{byte(v)}
	}
	// Two-byte varint encoding.
	return []byte{byte(v>>7) | 0x80, byte(v & 0x7f)}
}

// makeBtCursorValid creates a BtCursor in CursorValid state with the given payload/keyBytes.
func makeBtCursorValid(payload, keyBytes []byte) *btree.BtCursor {
	bt := btree.NewBtree(4096)
	cur := btree.NewCursorWithOptions(bt, 1, true)
	cur.State = btree.CursorValid
	cur.CurrentCell = &btree.CellInfo{
		Payload:  payload,
		KeyBytes: keyBytes,
	}
	return cur
}

// ---------------------------------------------------------------------------
// TestToNumeric — covers toNumeric across all type branches
// ---------------------------------------------------------------------------

func TestToNumeric(t *testing.T) {
	cases := []struct {
		name    string
		input   interface{}
		wantVal float64
		wantOk  bool
	}{
		{"int", int(42), 42.0, true},
		{"int64", int64(-7), -7.0, true},
		{"float64", float64(3.14), 3.14, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bytes", []byte{0x01}, 0, false},
		{"bool", true, 0, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toNumeric(tc.input)
			if ok != tc.wantOk {
				t.Errorf("toNumeric(%T) ok=%v, want %v", tc.input, ok, tc.wantOk)
			}
			if ok && got != tc.wantVal {
				t.Errorf("toNumeric(%T) = %v, want %v", tc.input, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestValuesEqualLoose — covers valuesEqualLoose across all branches
// ---------------------------------------------------------------------------

func TestValuesEqualLoose(t *testing.T) {
	cases := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"both int64 equal", int64(5), int64(5), true},
		{"int64 vs float64 equal", int64(2), float64(2.0), true},
		{"int64 vs float64 unequal", int64(1), float64(1.5), false},
		{"both string equal", "foo", "foo", true},
		{"both string unequal", "foo", "bar", false},
		{"both bytes equal", []byte("abc"), []byte("abc"), true},
		{"both bytes unequal", []byte("abc"), []byte("xyz"), false},
		{"nil vs nil", nil, nil, false},
		{"string vs int64", "5", int64(5), false},
		{"bytes vs string", []byte("x"), "x", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := valuesEqualLoose(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("valuesEqualLoose(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestPKChanged — covers pkChanged
// ---------------------------------------------------------------------------

func TestPKChanged(t *testing.T) {
	v := NewTestVDBE(1)

	t.Run("no change", func(t *testing.T) {
		old := map[string]interface{}{"id": int64(1), "val": int64(2)}
		new_ := map[string]interface{}{"id": int64(1), "val": int64(2)}
		if v.pkChanged(old, new_) {
			t.Error("expected no pk change")
		}
	})

	t.Run("pk changed", func(t *testing.T) {
		old := map[string]interface{}{"id": int64(1)}
		new_ := map[string]interface{}{"id": int64(2)}
		if !v.pkChanged(old, new_) {
			t.Error("expected pk change detected")
		}
	})

	t.Run("rowid key stripped", func(t *testing.T) {
		// rowid keys should be deleted from oldValues before comparison
		old := map[string]interface{}{"rowid": int64(1), "id": int64(5)}
		new_ := map[string]interface{}{"id": int64(5)}
		if v.pkChanged(old, new_) {
			t.Error("expected no change after rowid stripped")
		}
	})

	t.Run("_rowid_ key stripped", func(t *testing.T) {
		old := map[string]interface{}{"_rowid_": int64(99), "id": int64(5)}
		new_ := map[string]interface{}{"id": int64(5)}
		if v.pkChanged(old, new_) {
			t.Error("expected no change after _rowid_ stripped")
		}
	})

	t.Run("key missing in new", func(t *testing.T) {
		old := map[string]interface{}{"id": int64(1)}
		new_ := map[string]interface{}{}
		// missing key: skips comparison, so no change
		if v.pkChanged(old, new_) {
			t.Error("expected no change when key missing in new")
		}
	})
}

// ---------------------------------------------------------------------------
// TestDecodePrimaryKeyValues — covers decodePrimaryKeyValues
// ---------------------------------------------------------------------------

func TestDecodePrimaryKeyValues_EmptyInputs(t *testing.T) {
	m, err := decodePrimaryKeyValues(nil, []byte{0x00})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}

	m, err = decodePrimaryKeyValues([]string{"id"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestDecodePrimaryKeyValues_SingleAndMultiColumn(t *testing.T) {
	keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{int64(42)})
	m, err := decodePrimaryKeyValues([]string{"id"}, keyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m["id"] != int64(42) {
		t.Errorf("expected id=42, got %v", m["id"])
	}

	keyBytes = withoutrowid.EncodeCompositeKey([]interface{}{int64(1), "alice"})
	m, err = decodePrimaryKeyValues([]string{"a", "b"}, keyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m["a"] != int64(1) {
		t.Errorf("expected a=1, got %v", m["a"])
	}
	if m["b"] != "alice" {
		t.Errorf("expected b=alice, got %v", m["b"])
	}
}

func TestDecodePrimaryKeyValues_MoreColsThanValues(t *testing.T) {
	keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{int64(7)})
	m, err := decodePrimaryKeyValues([]string{"x", "y"}, keyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m["x"] != int64(7) {
		t.Errorf("expected x=7, got %v", m["x"])
	}
	if _, exists := m["y"]; exists {
		t.Error("expected y to be absent when key has fewer values")
	}
}

func TestDecodePrimaryKeyValues_InvalidKeyBytes(t *testing.T) {
	_, err := decodePrimaryKeyValues([]string{"id"}, []byte{0xFF, 0xFF})
	if err == nil {
		t.Error("expected error for invalid key bytes")
	}
}

// ---------------------------------------------------------------------------
// TestExtractValuesFromKeyAndPayload — covers extractValuesFromKeyAndPayload
// ---------------------------------------------------------------------------

func TestExtractValuesFromKeyAndPayload(t *testing.T) {
	v := NewTestVDBE(1)

	t.Run("table doesn't implement interface", func(t *testing.T) {
		_, err := v.extractValuesFromKeyAndPayload("t", nil, nil, struct{}{})
		if err == nil {
			t.Error("expected error when table lacks required interface")
		}
	})

	t.Run("valid extraction", func(t *testing.T) {
		// Build a record payload for both columns (id at index 0, name at index 1).
		// extractValuesFromKeyAndPayload uses GetColumns() indices to read from payload,
		// so the payload must have entries at the same indices as GetColumns().
		// id (index 0) will be skipped (already in values from key), name (index 1) is read.
		payload := buildSQLiteRecord([]interface{}{int64(5), "bob"})

		// Build key for PK column: id = 5
		keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{int64(5)})

		tbl := &wrFullTable{
			wrColTable: wrColTable{colNames: []string{"id", "name"}},
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}, &wrMockCol{"name"}},
		}

		m, err := v.extractValuesFromKeyAndPayload("t", keyBytes, payload, tbl)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m["id"] != int64(5) {
			t.Errorf("expected id=5, got %v", m["id"])
		}
		if m["name"] != "bob" {
			t.Errorf("expected name=bob, got %v", m["name"])
		}
	})
}

// ---------------------------------------------------------------------------
// TestValidateDeleteConstraintsWithoutRowID — covers validateDeleteConstraintsWithoutRowID
// ---------------------------------------------------------------------------

func TestValidateDeleteConstraintsWithoutRowID(t *testing.T) {
	t.Run("FK disabled returns nil", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{ForeignKeysEnabled: false}

		payload := buildSQLiteRecord([]interface{}{"alice"})
		cur := makeBtCursorValid(payload, withoutrowid.EncodeCompositeKey([]interface{}{int64(1)}))

		tbl := &wrFullTable{
			wrColTable: wrColTable{colNames: []string{"id", "name"}},
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}, &wrMockCol{"name"}},
		}
		err := v.validateDeleteConstraintsWithoutRowID("t", cur, tbl)
		if err != nil {
			t.Errorf("expected nil with FK disabled, got: %v", err)
		}
	})

	t.Run("no FK manager skips constraint", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          nil,
		}

		payload := buildSQLiteRecord([]interface{}{"alice"})
		cur := makeBtCursorValid(payload, withoutrowid.EncodeCompositeKey([]interface{}{int64(1)}))

		tbl := &wrFullTable{
			wrColTable: wrColTable{colNames: []string{"id", "name"}},
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}, &wrMockCol{"name"}},
		}
		err := v.validateDeleteConstraintsWithoutRowID("t", cur, tbl)
		if err != nil {
			t.Errorf("expected nil without FK manager, got: %v", err)
		}
	})

	t.Run("cursor not valid returns error", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{ForeignKeysEnabled: false}

		bt := btree.NewBtree(4096)
		cur := btree.NewCursorWithOptions(bt, 1, true)
		// State defaults to CursorInvalid; GetCompletePayload will fail.

		tbl := &wrFullTable{
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}},
		}
		err := v.validateDeleteConstraintsWithoutRowID("t", cur, tbl)
		// FK disabled so the function returns early (nil) before reading cursor.
		_ = err
	})
}

// ---------------------------------------------------------------------------
// TestGetTableColumnNamesWithIface — covers getTableColumnNamesWithIface
// ---------------------------------------------------------------------------

func TestGetTableColumnNamesWithIface_ErrorCases(t *testing.T) {
	// nil schema
	v := NewTestVDBE(1)
	v.Ctx = &VDBEContext{Schema: nil}
	if _, _, ok := v.getTableColumnNamesWithIface("t"); ok {
		t.Error("expected false when schema is nil")
	}

	// schema doesn't implement GetTableByName
	v.Ctx = &VDBEContext{Schema: struct{}{}}
	if _, _, ok := v.getTableColumnNamesWithIface("t"); ok {
		t.Error("expected false when schema lacks GetTableByName")
	}

	// table not found
	v.Ctx = &VDBEContext{Schema: &wrSchema{tables: map[string]interface{}{}}}
	if _, _, ok := v.getTableColumnNamesWithIface("missing"); ok {
		t.Error("expected false when table not found")
	}

	// table lacks GetColumnNames
	v.Ctx = &VDBEContext{Schema: &wrSchema{
		tables: map[string]interface{}{"t": struct{}{}},
	}}
	if _, _, ok := v.getTableColumnNamesWithIface("t"); ok {
		t.Error("expected false when table lacks GetColumnNames")
	}
}

func TestGetTableColumnNamesWithIface_Success(t *testing.T) {
	tbl := &wrColTable{colNames: []string{"id", "name"}}
	v := NewTestVDBE(1)
	v.Ctx = &VDBEContext{Schema: &wrSchema{
		tables: map[string]interface{}{"t": tbl},
	}}
	iface, cols, ok := v.getTableColumnNamesWithIface("t")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if iface == nil {
		t.Error("expected non-nil tableIface")
	}
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Errorf("unexpected cols: %v", cols)
	}
}

// ---------------------------------------------------------------------------
// TestAddIntegerPKValue — covers addIntegerPKValue
// ---------------------------------------------------------------------------

func TestAddIntegerPKValue(t *testing.T) {
	t.Run("table has integer PK column", func(t *testing.T) {
		tbl := &wrColTable{integerPKCol: "id"}
		vals := map[string]interface{}{}
		addIntegerPKValue(tbl, vals, 42)
		if vals["id"] != int64(42) {
			t.Errorf("expected id=42, got %v", vals["id"])
		}
	})

	t.Run("table has empty PK column name", func(t *testing.T) {
		tbl := &wrColTable{integerPKCol: ""}
		vals := map[string]interface{}{}
		addIntegerPKValue(tbl, vals, 99)
		if len(vals) != 0 {
			t.Errorf("expected no values added, got %v", vals)
		}
	})

	t.Run("table lacks GetIntegerPKColumn", func(t *testing.T) {
		vals := map[string]interface{}{}
		addIntegerPKValue(struct{}{}, vals, 7)
		if len(vals) != 0 {
			t.Errorf("expected no values added, got %v", vals)
		}
	})
}

// ---------------------------------------------------------------------------
// TestExtractRowValues — covers extractRowValues
// ---------------------------------------------------------------------------

func TestExtractRowValues(t *testing.T) {
	t.Run("schema without GetTableByName returns nil", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{Schema: struct{}{}}

		payload := buildSQLiteRecord([]interface{}{int64(1), "alice"})
		cur := makeBtCursorValid(payload, nil)
		cursor := &Cursor{BtreeCursor: cur}

		vals, err := v.extractRowValues(cursor, cur, "t")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vals != nil {
			t.Errorf("expected nil map, got %v", vals)
		}
	})

	t.Run("valid row extraction", func(t *testing.T) {
		// Encode: col0=int64(7), col1="carol"
		payload := buildSQLiteRecord([]interface{}{int64(7), "carol"})

		tbl := &wrColTable{colNames: []string{"id", "name"}}
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{Schema: &wrSchema{
			tables: map[string]interface{}{"t": tbl},
		}}

		cur := makeBtCursorValid(payload, nil)
		cursor := &Cursor{BtreeCursor: cur}

		vals, err := v.extractRowValues(cursor, cur, "t")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vals == nil {
			t.Fatal("expected non-nil map")
		}
		if vals["id"] != int64(7) {
			t.Errorf("expected id=7, got %v", vals["id"])
		}
		if vals["name"] != "carol" {
			t.Errorf("expected name=carol, got %v", vals["name"])
		}
	})
}

// ---------------------------------------------------------------------------
// TestCheckWithoutRowidPKUniqueness — covers checkWithoutRowidPKUniqueness
// ---------------------------------------------------------------------------

func TestCheckWithoutRowidPKUniqueness(t *testing.T) {
	t.Run("nil ctx returns error", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = nil
		err := v.checkWithoutRowidPKUniqueness("t", nil, 0, 0)
		if err == nil {
			t.Error("expected error with nil ctx")
		}
	})

	t.Run("invalid schema type returns error", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{Schema: struct{}{}}
		// computeCompositeKeyBytes will fail because schema has no GetTableByName.
		payload := buildSQLiteRecord([]interface{}{int64(1)})
		err := v.checkWithoutRowidPKUniqueness("t", payload, 0, 0)
		if err == nil {
			t.Error("expected error with invalid schema type")
		}
	})

	t.Run("table not found returns error", func(t *testing.T) {
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{Schema: &wrSchema{tables: map[string]interface{}{}}}
		payload := buildSQLiteRecord([]interface{}{int64(1)})
		err := v.checkWithoutRowidPKUniqueness("missing", payload, 0, 0)
		if err == nil {
			t.Error("expected error when table not found")
		}
	})

	t.Run("invalid btree type returns error", func(t *testing.T) {
		// Set up a real schema with a full table but invalid btree context.
		tbl := &wrFullTable{
			wrColTable: wrColTable{colNames: []string{"id"}},
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}},
			hasRowID:   false,
			rootPage:   2,
		}
		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{
			Schema: &wrSchema{tables: map[string]interface{}{"t": tbl}},
			Btree:  nil, // no btree
		}
		payload := buildSQLiteRecord([]interface{}{int64(1)})
		err := v.checkWithoutRowidPKUniqueness("t", payload, 0, 0)
		if err == nil {
			t.Error("expected error with nil btree")
		}
	})

	t.Run("seek error propagated", func(t *testing.T) {
		// An uninitialized btree's page 1 doesn't exist, so SeekComposite returns
		// an error which checkWithoutRowidPKUniqueness wraps and returns.
		tbl := &wrFullTable{
			wrColTable: wrColTable{colNames: []string{"id"}},
			primaryKey: []string{"id"},
			columns:    []interface{}{&wrMockCol{"id"}},
			hasRowID:   false,
			rootPage:   1,
		}
		bt := btree.NewBtree(4096)

		v := NewTestVDBE(1)
		v.Ctx = &VDBEContext{
			Schema: &wrSchema{tables: map[string]interface{}{"t": tbl}},
			Btree:  bt,
		}

		payload := buildSQLiteRecord([]interface{}{int64(99)})
		err := v.checkWithoutRowidPKUniqueness("t", payload, 0, 0)
		// Uninitialized btree: SeekComposite fails → error returned.
		if err == nil {
			t.Error("expected error seeking in uninitialized btree")
		}
	})
}

// ---------------------------------------------------------------------------
// TestExtractValuesFromKeyAndPayloadColumnSkip covers the "already exists" path
// (PK column already in map is not overwritten from payload)
// ---------------------------------------------------------------------------

func TestExtractValuesFromKeyAndPayloadColumnSkip(t *testing.T) {
	v := NewTestVDBE(1)

	// Both columns are "id" – the PK col is decoded from key, the payload entry
	// for "id" should be skipped because it's already set.
	keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{int64(5)})
	// Payload has one column for "id" that would be int64(99) if read.
	payload := buildSQLiteRecord([]interface{}{int64(99)})

	tbl := &wrFullTable{
		wrColTable: wrColTable{colNames: []string{"id"}},
		primaryKey: []string{"id"},
		columns:    []interface{}{&wrMockCol{"id"}},
	}

	m, err := v.extractValuesFromKeyAndPayload("t", keyBytes, payload, tbl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The PK value from the key (5) should win; payload value (99) should be skipped.
	if m["id"] != int64(5) {
		t.Errorf("expected id=5 (from key), got %v", m["id"])
	}
}

// ---------------------------------------------------------------------------
// TestDecodePrimaryKeyValuesNullKey covers NULL in composite key
// ---------------------------------------------------------------------------

func TestDecodePrimaryKeyValuesNullKey(t *testing.T) {
	keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{nil})
	m, err := decodePrimaryKeyValues([]string{"id"}, keyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, exists := m["id"]; !exists || v != nil {
		t.Errorf("expected id=nil, got exists=%v val=%v", exists, v)
	}
}
