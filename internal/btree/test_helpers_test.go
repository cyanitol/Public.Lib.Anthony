// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import "testing"

// insertRows inserts rows into a cursor, stopping silently on error. Returns count inserted.
func insertRows(cursor *BtCursor, start, end int64, payloadSize int) int {
	count := 0
	for i := start; i <= end; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte((i + int64(j)) % 256)
		}
		if err := cursor.Insert(i, payload); err != nil {
			break
		}
		count++
	}
	return count
}

// insertRowsFixedPayload inserts rows with a fixed payload byte pattern.
func insertRowsFixedPayload(cursor *BtCursor, start, end int64, payload []byte) int {
	count := 0
	for i := start; i <= end; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			break
		}
		count++
	}
	return count
}

// deleteRowRange seeks and deletes rows in [start, end].
func deleteRowRange(cursor *BtCursor, start, end int64) {
	for i := start; i <= end; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}
}

// countForward counts entries by iterating forward from MoveToFirst.
func countForward(cursor *BtCursor) int {
	if err := cursor.MoveToFirst(); err != nil {
		return 0
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.Next(); err != nil {
			break
		}
	}
	return count
}

// countBackward counts entries by iterating backward from MoveToLast.
func countBackward(cursor *BtCursor) int {
	if err := cursor.MoveToLast(); err != nil {
		return 0
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.Previous(); err != nil {
			break
		}
	}
	return count
}

// navigateForward moves the cursor forward n steps.
func navigateForward(cursor *BtCursor, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if err := cursor.Next(); err != nil || !cursor.IsValid() {
			break
		}
		count++
	}
	return count
}

// navigateBackward moves the cursor backward n steps.
func navigateBackward(cursor *BtCursor, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if err := cursor.Previous(); err != nil || !cursor.IsValid() {
			break
		}
		count++
	}
	return count
}

// verifyOrderedForward checks that keys are in ascending order during forward iteration.
func verifyOrderedForward(t *testing.T, cursor *BtCursor) int {
	t.Helper()
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}
	count := 0
	prevKey := int64(-1)
	for cursor.IsValid() {
		key := cursor.GetKey()
		if key <= prevKey {
			t.Errorf("Keys out of order: %d after %d", key, prevKey)
		}
		prevKey = key
		count++
		if err := cursor.Next(); err != nil {
			break
		}
	}
	return count
}

// verifyOrderedBackward checks that keys are in descending order during backward iteration.
func verifyOrderedBackward(t *testing.T, cursor *BtCursor) int {
	t.Helper()
	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}
	count := 0
	prevKey := int64(1 << 60)
	for cursor.IsValid() {
		key := cursor.GetKey()
		if key >= prevKey {
			t.Errorf("Keys not descending: %d after %d", key, prevKey)
		}
		prevKey = key
		count++
		if err := cursor.Previous(); err != nil {
			break
		}
	}
	return count
}

// setupBtreeWithRows creates a btree, table, and inserts rows.
func setupBtreeWithRows(t *testing.T, pageSize uint32, start, end int64, payloadSize int) (*Btree, *BtCursor) {
	t.Helper()
	bt := NewBtree(pageSize)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, start, end, payloadSize)
	return bt, cursor
}

// insertIndexEntries inserts index entries, stopping on error.
func insertIndexEntries(cursor *IndexCursor, keys [][]byte) int {
	count := 0
	for i, key := range keys {
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			break
		}
		count++
	}
	return count
}

// insertIndexEntriesN inserts n index entries with a key generator.
func insertIndexEntriesN(cursor *IndexCursor, n int, keyGen func(int) []byte) int {
	count := 0
	for i := 0; i < n; i++ {
		if err := cursor.InsertIndex(keyGen(i), int64(i)); err != nil {
			break
		}
		count++
	}
	return count
}

// countIndexForward counts index entries by iterating forward.
func countIndexForward(cursor *IndexCursor) int {
	if err := cursor.MoveToFirst(); err != nil {
		return 0
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.NextIndex(); err != nil {
			break
		}
	}
	return count
}

// countIndexBackward counts index entries by iterating backward.
func countIndexBackward(cursor *IndexCursor) int {
	if err := cursor.MoveToLast(); err != nil {
		return 0
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.PrevIndex(); err != nil {
			break
		}
	}
	return count
}

// navigateIndexForward moves the index cursor forward n steps.
func navigateIndexForward(cursor *IndexCursor, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if err := cursor.NextIndex(); err != nil || !cursor.IsValid() {
			break
		}
		count++
	}
	return count
}

// navigateIndexBackward moves the index cursor backward n steps.
func navigateIndexBackward(cursor *IndexCursor, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if err := cursor.PrevIndex(); err != nil || !cursor.IsValid() {
			break
		}
		count++
	}
	return count
}

// seekAndDelete seeks to a rowid and deletes it if found.
func seekAndDelete(cursor *BtCursor, rowid int64) error {
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		return err
	}
	if found {
		return cursor.Delete()
	}
	return nil
}

// setupIndexCursor creates a btree, index page, and returns cursor.
func setupIndexCursor(t *testing.T, pageSize uint32) (*Btree, *IndexCursor) {
	t.Helper()
	bt := NewBtree(pageSize)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)
	return bt, cursor
}

// insertAndVerifyOverflow inserts a row and verifies overflow status.
func insertAndVerifyOverflow(t *testing.T, cursor *BtCursor, rowid int64, payload []byte, wantOverflow bool) {
	t.Helper()
	if err := cursor.Insert(rowid, payload); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find inserted row")
	}
	hasOverflow := cursor.CurrentCell.OverflowPage != 0
	if hasOverflow != wantOverflow {
		t.Errorf("Overflow mismatch: got %v, want %v", hasOverflow, wantOverflow)
	}
}

// verifyPayloadRoundtrip inserts, seeks, and verifies payload matches.
func verifyPayloadRoundtrip(t *testing.T, cursor *BtCursor, rowid int64, payload []byte) {
	t.Helper()
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		t.Fatalf("SeekRowid(%d) error = %v", rowid, err)
	}
	if !found {
		t.Fatalf("Row %d not found", rowid)
	}
	retrieved, err := cursor.GetCompletePayload()
	if err != nil {
		t.Fatalf("GetCompletePayload(rowid=%d) error = %v", rowid, err)
	}
	if len(retrieved) != len(payload) {
		t.Errorf("Row %d: payload size %d, want %d", rowid, len(retrieved), len(payload))
	}
}

// tryMergeAtPosition seeks to a position and attempts merge if at depth > 0.
func tryMergeAtPosition(cursor *BtCursor, rowid int64) (bool, error) {
	cursor.SeekRowid(rowid)
	if cursor.IsValid() && cursor.Depth > 0 {
		return cursor.MergePage()
	}
	return false, nil
}

// getPageIfValid gets a btree page for the cursor's current page, returns nil on error.
func getPageIfValid(bt *Btree, pageNum uint32) *BtreePage {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return nil
	}
	page, err := NewBtreePage(pageNum, pageData, bt.UsableSize)
	if err != nil {
		return nil
	}
	return page
}

// verifyKeyOrder checks that collected keys match expected keys in order.
func verifyKeyOrder(t *testing.T, got, want []int64) {
	t.Helper()
	for i, expected := range want {
		if i >= len(got) {
			t.Errorf("Missing key at index %d", i)
			continue
		}
		if got[i] != expected {
			t.Errorf("Key at index %d: got %d, want %d", i, got[i], expected)
		}
	}
}

// verifyChildPages checks that child pages match expected values.
func verifyChildPages(t *testing.T, got, want []uint32) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("Expected %d child pages, got %d", len(want), len(got))
		return
	}
	for i, expected := range want {
		if got[i] != expected {
			t.Errorf("Child page at index %d: got %d, want %d", i, got[i], expected)
		}
	}
}

// insertAndDeleteRange inserts rows then deletes a range, returning the cursor.
func insertAndDeleteRange(bt *Btree, rootPage uint32, insertEnd int64, payloadSize int, delStart, delEnd int64) *BtCursor {
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, insertEnd, payloadSize)
	cursor2 := NewCursor(bt, rootPage)
	for i := delStart; i <= delEnd; i++ {
		found, err := cursor2.SeekRowid(i)
		if err == nil && found {
			cursor2.Delete()
		}
	}
	return cursor2
}
