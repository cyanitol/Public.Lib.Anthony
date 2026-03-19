// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

// NOTE: The rtree package does not yet have a persistence.go file with a
// ShadowTableManager. These tests exercise binary serialization of R-Tree
// data structures (entries, bounding boxes, nextID) to validate that the
// data model supports round-trip persistence. When persistence.go is added,
// these helpers and tests can be adapted to use the real ShadowTableManager.

// encodeBBox serializes a BoundingBox to bytes.
func encodeBBox(bbox *BoundingBox) []byte {
	buf := new(bytes.Buffer)
	dims := int32(len(bbox.Min))
	binary.Write(buf, binary.LittleEndian, dims)
	for i := 0; i < int(dims); i++ {
		binary.Write(buf, binary.LittleEndian, bbox.Min[i])
		binary.Write(buf, binary.LittleEndian, bbox.Max[i])
	}
	return buf.Bytes()
}

// decodeBBox deserializes a BoundingBox from bytes.
func decodeBBox(data []byte) *BoundingBox {
	if len(data) < 4 {
		return nil
	}
	buf := bytes.NewReader(data)
	var dims int32
	if err := binary.Read(buf, binary.LittleEndian, &dims); err != nil {
		return nil
	}
	bbox := NewBoundingBox(int(dims))
	for i := int32(0); i < dims; i++ {
		binary.Read(buf, binary.LittleEndian, &bbox.Min[i])
		binary.Read(buf, binary.LittleEndian, &bbox.Max[i])
	}
	return bbox
}

// encodeEntry serializes an Entry (id + bounding box) to bytes.
func encodeEntry(e *Entry) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, e.ID)
	bboxBytes := encodeBBox(e.BBox)
	buf.Write(bboxBytes)
	return buf.Bytes()
}

// decodeEntry deserializes an Entry from bytes.
func decodeEntry(data []byte) *Entry {
	if len(data) < 8 {
		return nil
	}
	buf := bytes.NewReader(data)
	var id int64
	if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
		return nil
	}
	remaining := data[8:]
	bbox := decodeBBox(remaining)
	if bbox == nil {
		return nil
	}
	return &Entry{ID: id, BBox: bbox}
}

// encodeEntries serializes a slice of entries.
func encodeEntries(entries []*Entry) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(len(entries)))
	for _, e := range entries {
		entryBytes := encodeEntry(e)
		binary.Write(buf, binary.LittleEndian, int32(len(entryBytes)))
		buf.Write(entryBytes)
	}
	return buf.Bytes()
}

// decodeEntries deserializes a slice of entries.
func decodeEntries(data []byte) []*Entry {
	if len(data) < 4 {
		return nil
	}
	buf := bytes.NewReader(data)
	var count int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil
	}
	entries := make([]*Entry, 0, count)
	for i := int32(0); i < count; i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			break
		}
		chunk := make([]byte, size)
		if _, err := buf.Read(chunk); err != nil {
			break
		}
		e := decodeEntry(chunk)
		if e != nil {
			entries = append(entries, e)
		}
	}
	return entries
}

// encodeNextID serializes an int64 nextID value.
func encodeNextID(id int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(id))
	return buf
}

// decodeNextID deserializes an int64 nextID value.
func decodeNextID(data []byte) int64 {
	if len(data) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data))
}

// TestPersistenceBBoxRoundTrip verifies bounding box binary serialization.
func TestPersistenceBBoxRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bbox *BoundingBox
	}{
		{
			name: "2D box",
			bbox: &BoundingBox{
				Min: []float64{1.0, 2.0},
				Max: []float64{3.0, 4.0},
			},
		},
		{
			name: "3D box",
			bbox: &BoundingBox{
				Min: []float64{-10.5, 0.0, 100.0},
				Max: []float64{10.5, 50.0, 200.0},
			},
		},
		{
			name: "point box",
			bbox: &BoundingBox{
				Min: []float64{5.0, 5.0},
				Max: []float64{5.0, 5.0},
			},
		},
		{
			name: "large coordinates",
			bbox: &BoundingBox{
				Min: []float64{-1e15, -1e15},
				Max: []float64{1e15, 1e15},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := encodeBBox(tt.bbox)
			decoded := decodeBBox(data)
			if decoded == nil {
				t.Fatal("decodeBBox returned nil")
			}
			if !tt.bbox.Equal(decoded) {
				t.Errorf("round-trip failed: want %v, got %v", tt.bbox, decoded)
			}
		})
	}
}

// TestPersistenceBBoxDecodeShort verifies graceful handling of short blobs.
func TestPersistenceBBoxDecodeShort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "nil", data: nil},
		{name: "empty", data: []byte{}},
		{name: "too short", data: []byte{1, 2}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := decodeBBox(tt.data)
			if result != nil {
				t.Errorf("expected nil for short data, got %v", result)
			}
		})
	}
}

// TestPersistenceEntryRoundTrip verifies entry serialization round-trip.
func TestPersistenceEntryRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry *Entry
	}{
		{
			name: "basic 2D entry",
			entry: &Entry{
				ID: 42,
				BBox: &BoundingBox{
					Min: []float64{1.0, 2.0},
					Max: []float64{3.0, 4.0},
				},
			},
		},
		{
			name: "zero ID",
			entry: &Entry{
				ID: 0,
				BBox: &BoundingBox{
					Min: []float64{0.0, 0.0},
					Max: []float64{1.0, 1.0},
				},
			},
		},
		{
			name: "large ID",
			entry: &Entry{
				ID: math.MaxInt64,
				BBox: &BoundingBox{
					Min: []float64{-180.0, -90.0},
					Max: []float64{180.0, 90.0},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := encodeEntry(tt.entry)
			decoded := decodeEntry(data)
			if decoded == nil {
				t.Fatal("decodeEntry returned nil")
			}
			if decoded.ID != tt.entry.ID {
				t.Errorf("ID: want %d, got %d", tt.entry.ID, decoded.ID)
			}
			if !tt.entry.BBox.Equal(decoded.BBox) {
				t.Errorf("BBox mismatch: want %v, got %v", tt.entry.BBox, decoded.BBox)
			}
		})
	}
}

// TestPersistenceEntryDecodeShort verifies graceful handling of short entry data.
func TestPersistenceEntryDecodeShort(t *testing.T) {
	t.Parallel()
	result := decodeEntry([]byte{1, 2, 3})
	if result != nil {
		t.Errorf("expected nil for short data, got %v", result)
	}
}

// TestPersistenceEntriesRoundTrip verifies batch entry serialization.
func TestPersistenceEntriesRoundTrip(t *testing.T) {
	t.Parallel()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{-1, -1}, Max: []float64{1, 1}}},
	}

	data := encodeEntries(entries)
	decoded := decodeEntries(data)

	if len(decoded) != len(entries) {
		t.Fatalf("count: want %d, got %d", len(entries), len(decoded))
	}

	for i, want := range entries {
		got := decoded[i]
		if got.ID != want.ID {
			t.Errorf("entry[%d] ID: want %d, got %d", i, want.ID, got.ID)
		}
		if !want.BBox.Equal(got.BBox) {
			t.Errorf("entry[%d] BBox mismatch", i)
		}
	}
}

// TestPersistenceEntriesEmpty verifies encoding/decoding of empty entry list.
func TestPersistenceEntriesEmpty(t *testing.T) {
	t.Parallel()

	data := encodeEntries(nil)
	decoded := decodeEntries(data)
	if len(decoded) != 0 {
		t.Errorf("expected 0 entries, got %d", len(decoded))
	}
}

// TestPersistenceEntriesDecodeShort verifies graceful handling of short data.
func TestPersistenceEntriesDecodeShort(t *testing.T) {
	t.Parallel()
	result := decodeEntries([]byte{1})
	if result != nil {
		t.Errorf("expected nil for short data, got %v", result)
	}
}

// TestPersistenceNextIDRoundTrip verifies nextID serialization.
func TestPersistenceNextIDRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   int64
	}{
		{name: "zero", id: 0},
		{name: "one", id: 1},
		{name: "large", id: 999999},
		{name: "max", id: math.MaxInt64},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := encodeNextID(tt.id)
			decoded := decodeNextID(data)
			if decoded != tt.id {
				t.Errorf("want %d, got %d", tt.id, decoded)
			}
		})
	}
}

// TestPersistenceNextIDDecodeShort verifies short data returns zero.
func TestPersistenceNextIDDecodeShort(t *testing.T) {
	t.Parallel()
	if id := decodeNextID([]byte{1, 2}); id != 0 {
		t.Errorf("expected 0, got %d", id)
	}
}

// TestPersistenceNilDB verifies that nil db-like scenarios are gracefully handled.
// Since rtree does not yet have a ShadowTableManager, this tests that the RTree
// data structures work correctly in the in-memory-only mode (no persistence layer).
func TestPersistenceNilDB(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_persist", []string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rt := table.(*RTree)

	// Insert an entry via Update.
	coords := []interface{}{nil, nil, float64(1), float64(10), float64(2), float64(20)}
	id, err := rt.Update(len(coords), coords)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Verify retrieval.
	entry, ok := rt.GetEntry(id)
	if !ok {
		t.Fatalf("entry %d not found after insert", id)
	}

	wantMin := []float64{1, 2}
	wantMax := []float64{10, 20}
	if !reflect.DeepEqual(entry.BBox.Min, wantMin) {
		t.Errorf("Min: want %v, got %v", wantMin, entry.BBox.Min)
	}
	if !reflect.DeepEqual(entry.BBox.Max, wantMax) {
		t.Errorf("Max: want %v, got %v", wantMax, entry.BBox.Max)
	}

	// Destroy should succeed (no-op for persistence with nil db).
	if err := rt.Destroy(); err != nil {
		t.Errorf("Destroy() error: %v", err)
	}
	if rt.Count() != 0 {
		t.Errorf("expected 0 entries after Destroy, got %d", rt.Count())
	}
}

// insertPersistenceTestEntries inserts entries and returns inserted IDs.
func insertPersistenceTestEntries(t *testing.T, rt *RTree) []int64 {
	t.Helper()
	type testEntry struct {
		minX, maxX, minY, maxY float64
	}
	inserts := []testEntry{
		{0, 10, 0, 10},
		{5, 15, 5, 15},
		{-20, -10, -20, -10},
	}

	var ids []int64
	for _, ins := range inserts {
		args := []interface{}{nil, nil, ins.minX, ins.maxX, ins.minY, ins.maxY}
		id, err := rt.Update(len(args), args)
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// collectEntries retrieves entries by ID from the RTree.
func collectEntries(t *testing.T, rt *RTree, ids []int64) []*Entry {
	t.Helper()
	entries := make([]*Entry, 0, len(ids))
	for _, id := range ids {
		e, ok := rt.GetEntry(id)
		if !ok {
			t.Fatalf("entry %d not found", id)
		}
		entries = append(entries, e)
	}
	return entries
}

// TestPersistenceRTreeSaveLoadRoundTrip tests saving RTree entries to binary
// format and reloading them to reconstruct equivalent state.
func TestPersistenceRTreeSaveLoadRoundTrip(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "geo", []string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	rt := table.(*RTree)

	ids := insertPersistenceTestEntries(t, rt)
	origEntries := collectEntries(t, rt, ids)

	blob := encodeEntries(origEntries)
	nextIDBlob := encodeNextID(rt.nextID)
	restored := decodeEntries(blob)
	restoredNextID := decodeNextID(nextIDBlob)

	if len(restored) != len(origEntries) {
		t.Fatalf("entry count: want %d, got %d", len(origEntries), len(restored))
	}

	for i, orig := range origEntries {
		got := restored[i]
		if got.ID != orig.ID {
			t.Errorf("entry[%d] ID: want %d, got %d", i, orig.ID, got.ID)
		}
		if !orig.BBox.Equal(got.BBox) {
			t.Errorf("entry[%d] BBox mismatch", i)
		}
	}

	if restoredNextID != rt.nextID {
		t.Errorf("nextID: want %d, got %d", rt.nextID, restoredNextID)
	}
}
