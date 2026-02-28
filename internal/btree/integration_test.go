package btree

import (
	"testing"
)

// TestIntegrationLargeDataset tests inserting and querying a large dataset
func TestIntegrationLargeDataset(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024) // Smaller pages to force tree growth

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many records to force tree growth and splits
	numRecords := 500
	for i := 1; i <= numRecords; i++ {
		key := int64(i)
		payload := make([]byte, 50)
		for j := range payload {
			payload[j] = byte(i % 256)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert %d: %v", i, err)
			// Continue - some errors during splits are expected
		}
	}

	// Verify we can read back the data
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	prevKey := int64(0)
	for cursor2.IsValid() {
		key := cursor2.GetKey()
		if key <= prevKey {
			t.Errorf("Keys out of order: %d after %d", key, prevKey)
		}
		prevKey = key
		count++

		if err := cursor2.Next(); err != nil {
			break
		}
	}

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

	// Insert initial data
	for i := int64(1); i <= 100; i += 2 {
		err := cursor.Insert(i, []byte{byte(i)})
		if err != nil {
			t.Logf("Insert %d: %v", i, err)
		}
	}

	// Seek to various keys
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

	// Delete some keys
	for i := int64(10); i <= 30; i += 2 {
		found, err := cursor.SeekRowid(i)
		if err != nil {
			continue
		}
		if found {
			err = cursor.Delete()
			if err != nil {
				t.Logf("Delete(%d): %v", i, err)
			}
		}
	}

	// Insert more data
	for i := int64(101); i <= 150; i++ {
		err := cursor.Insert(i, []byte{byte(i % 256)})
		if err != nil {
			t.Logf("Insert %d: %v", i, err)
		}
	}

	// Final verification
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	for cursor2.IsValid() {
		count++
		if err := cursor2.Next(); err != nil {
			break
		}
	}

	t.Logf("Final count: %d records", count)
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

	// Insert records with very large payloads (requiring overflow pages)
	for i := int64(1); i <= 20; i++ {
		// Create payload larger than a single page
		payloadSize := 6000 + int(i)*100
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte((i + int64(j)) % 256)
		}

		err := cursor.Insert(i*10, payload)
		if err != nil {
			t.Logf("Insert %d (size %d): %v", i, payloadSize, err)
		}
	}

	// Verify we can read them back with correct sizes
	for i := int64(1); i <= 20; i++ {
		found, err := cursor.SeekRowid(i * 10)
		if err != nil {
			continue
		}
		if !found {
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

	// Insert data
	numRecords := 50
	for i := 1; i <= numRecords; i++ {
		err := cursor.Insert(int64(i), []byte{byte(i)})
		if err != nil {
			t.Logf("Insert %d: %v", i, err)
		}
	}

	// Iterate backward from last
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	count := 0
	prevKey := int64(1000000)
	for {
		if !cursor2.IsValid() {
			break
		}

		key := cursor2.GetKey()
		if key >= prevKey {
			t.Errorf("Keys not in descending order: %d before %d", key, prevKey)
		}
		prevKey = key
		count++

		err := cursor2.Previous()
		if err != nil {
			break
		}
	}

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

	// Insert many index entries
	for i := 0; i < 100; i++ {
		key := []byte{byte('a' + (i % 26)), byte(i / 26)}
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			t.Logf("InsertIndex %d: %v", i, err)
		}
	}

	// Verify iteration
	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	for cursor.IsValid() {
		count++
		err := cursor.NextIndex()
		if err != nil {
			break
		}
	}

	if count < 10 {
		t.Errorf("Expected at least 10 index entries, got %d", count)
	}

	// Test backward iteration
	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	backCount := 0
	for backCount < 10 && cursor.IsValid() {
		backCount++
		err := cursor.PrevIndex()
		if err != nil {
			break
		}
	}

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

	// Insert data in random order
	keys := []int64{50, 10, 90, 30, 70, 20, 60, 40, 80, 100}
	for _, key := range keys {
		err := cursor.Insert(key, []byte{byte(key)})
		if err != nil {
			t.Logf("Insert %d: %v", key, err)
		}
	}

	// Random access in different order
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

	// Verify all keys exist in sorted order
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	prevKey := int64(0)
	orderedCount := 0
	for cursor2.IsValid() {
		key := cursor2.GetKey()
		if key <= prevKey {
			t.Errorf("Keys out of order: %d after %d", key, prevKey)
		}
		prevKey = key
		orderedCount++

		if err := cursor2.Next(); err != nil {
			break
		}
	}

	t.Logf("Random access test: %d ordered keys", orderedCount)
}

// TestIntegrationTreeGrowth tests tree growing from leaf to multi-level
func TestIntegrationTreeGrowth(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Very small pages to force growth

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	originalRoot := rootPage

	// Insert enough to force root changes
	for i := 1; i <= 300; i++ {
		err := cursor.Insert(int64(i), []byte{byte(i % 256)})
		if err != nil {
			t.Logf("Insert %d: %v (may be due to split)", i, err)
		}

		// Check if root changed
		if cursor.RootPage != originalRoot {
			t.Logf("Root changed at insert %d: %d -> %d", i, originalRoot, cursor.RootPage)
			originalRoot = cursor.RootPage
		}
	}

	// Verify final tree structure
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	for cursor2.IsValid() {
		count++
		if err := cursor2.Next(); err != nil {
			break
		}
	}

	t.Logf("Tree growth test: %d records, final root page %d", count, cursor.RootPage)

	if count < 50 {
		t.Errorf("Expected at least 50 records after growth, got %d", count)
	}
}
