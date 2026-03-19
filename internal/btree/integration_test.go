// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestIntegrationLargeDataset tests inserting and querying a large dataset
func TestIntegrationLargeDataset(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 500, 50)

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := verifyOrderedForward(t, cursor2)

	if count < 100 {
		t.Errorf("Expected at least 100 records, got %d", count)
	}

	t.Logf("Successfully inserted and verified %d records", count)
}

// TestIntegrationMixedOperations tests a mix of insert, seek, delete
func TestIntegrationMixedOperations(t *testing.T) {
	t.Parallel()
	bt := NewBtree(2048)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 99, 1)
	integrationMixedSeeks(t, cursor)
	integrationMixedDeletes(cursor)
	insertRows(cursor, 101, 150, 1)

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := countForward(cursor2)
	t.Logf("Final count: %d records", count)
}

func integrationMixedSeeks(t *testing.T, cursor *BtCursor) {
	t.Helper()
	testKeys := []int64{1, 25, 50, 75, 99}
	for _, key := range testKeys {
		found, err := cursor.SeekRowid(key)
		if err != nil {
			t.Logf("SeekRowid(%d): %v", key, err)
		}
		if !found {
			t.Logf("SeekRowid(%d) not found", key)
		}
	}
}

func integrationMixedDeletes(cursor *BtCursor) {
	for i := int64(10); i <= 30; i += 2 {
		found, err := cursor.SeekRowid(i)
		if err != nil {
			continue
		}
		if found {
			cursor.Delete()
		}
	}
}

// TestIntegrationVeryLargePayloads tests inserting very large payloads
func TestIntegrationVeryLargePayloads(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	integrationLargePayloadInsert(t, cursor)
	integrationLargePayloadVerify(t, cursor)
}

func integrationLargePayloadInsert(t *testing.T, cursor *BtCursor) {
	t.Helper()
	for i := int64(1); i <= 20; i++ {
		payloadSize := 6000 + int(i)*100
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte((i + int64(j)) % 256)
		}
		if err := cursor.Insert(i*10, payload); err != nil {
			t.Logf("Insert %d (size %d): %v", i, payloadSize, err)
		}
	}
}

func integrationLargePayloadVerify(t *testing.T, cursor *BtCursor) {
	t.Helper()
	for i := int64(1); i <= 20; i++ {
		found, err := cursor.SeekRowid(i * 10)
		if err != nil || !found {
			continue
		}
		payload, err := cursor.GetPayloadWithOverflow()
		if err != nil {
			t.Logf("GetPayloadWithOverflow for key %d: %v", i*10, err)
			continue
		}
		expectedSize := 6000 + int(i)*100
		if len(payload) != expectedSize {
			t.Errorf("Key %d: payload size = %d, want %d", i*10, len(payload), expectedSize)
		}
	}
}

// TestIntegrationBackwardIteration tests iterating backward through tree
func TestIntegrationBackwardIteration(t *testing.T) {
	t.Parallel()
	bt := NewBtree(2048)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 50, 1)

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := verifyOrderedBackward(t, cursor2)

	if count < 10 {
		t.Errorf("Expected at least 10 records in backward iteration, got %d", count)
	}

	t.Logf("Backward iteration verified %d records", count)
}

// TestIntegrationIndexOperations tests index cursor operations
func TestIntegrationIndexOperations(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage failed: %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	insertIndexEntriesN(cursor, 100, func(i int) []byte {
		return []byte{byte('a' + (i % 26)), byte(i / 26)}
	})

	count := countIndexForward(cursor)
	if count < 10 {
		t.Errorf("Expected at least 10 index entries, got %d", count)
	}

	cursor.MoveToLast()
	backCount := navigateIndexBackward(cursor, 10)

	t.Logf("Index: forward %d entries, backward %d entries", count, backCount)
}

// TestIntegrationRandomAccess tests random access patterns
func TestIntegrationRandomAccess(t *testing.T) {
	t.Parallel()
	bt := NewBtree(2048)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	keys := []int64{50, 10, 90, 30, 70, 20, 60, 40, 80, 100}
	for _, key := range keys {
		cursor.Insert(key, []byte{byte(key)})
	}

	integrationRandomAccessSeeks(t, cursor)

	cursor2 := NewCursor(bt, cursor.RootPage)
	orderedCount := verifyOrderedForward(t, cursor2)
	t.Logf("Random access test: %d ordered keys", orderedCount)
}

func integrationRandomAccessSeeks(t *testing.T, cursor *BtCursor) {
	t.Helper()
	accessKeys := []int64{30, 80, 10, 100, 50}
	for _, key := range accessKeys {
		found, err := cursor.SeekRowid(key)
		if err != nil {
			t.Logf("SeekRowid(%d): %v", key, err)
			continue
		}
		if !found {
			t.Logf("SeekRowid(%d) not found", key)
			continue
		}
		gotKey := cursor.GetKey()
		if gotKey != key {
			t.Errorf("SeekRowid(%d): got key %d", key, gotKey)
		}
	}
}

// TestIntegrationTreeGrowth tests tree growing from leaf to multi-level
func TestIntegrationTreeGrowth(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 300, 1)

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := countForward(cursor2)

	t.Logf("Tree growth test: %d records, final root page %d", count, cursor.RootPage)

	if count < 50 {
		t.Errorf("Expected at least 50 records after growth, got %d", count)
	}
}
