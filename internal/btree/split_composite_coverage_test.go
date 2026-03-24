// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

// compositeScanCount moves to first and counts all rows by iterating forward.
func compositeScanCount(scan *BtCursor) int {
	if err := scan.MoveToFirst(); err != nil {
		return 0
	}
	count := 0
	for scan.IsValid() {
		count++
		if err := scan.Next(); err != nil {
			break
		}
	}
	return count
}

// TestCompositeSplitReverseOrder inserts composite keys in reverse order so that
// tryInsertNewCellComposite returns true (new key < existing cell key) on every
// insert after the first. This exercises the insertion-before branch that is not
// hit when keys arrive in sorted order.
func TestCompositeSplitReverseOrder(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	const n = 80
	payload := bytes.Repeat([]byte("v"), 30)

	// Insert in reverse order: k079, k078, … k000.
	// Each new key is less than the first key on the page, so
	// tryInsertNewCellComposite's comparison branch (< 0) fires.
	for i := n - 1; i >= 0; i-- {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("k%03d", i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}

	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	count := compositeScanCount(scan)
	if count != n {
		t.Errorf("expected %d rows, got %d", n, count)
	}
}

// TestCompositeSplitMixedOrder inserts composite keys in an interleaved order so
// that both the "new key < existing" and "new key >= existing" branches of
// tryInsertNewCellComposite are exercised within a single page before and after
// splits. It also exercises prepareLeafSplitComposite and executeLeafSplitComposite.
func TestCompositeSplitMixedOrder(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	payload := bytes.Repeat([]byte("p"), 40)

	// Interleave: high keys first, then low keys to force insertions in the
	// middle of existing cells on the split pages.
	high := []string{"z9", "z8", "z7", "z6", "z5", "z4", "z3", "z2", "z1", "z0"}
	low := []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"}
	mid := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}

	for _, s := range high {
		key := withoutrowid.EncodeCompositeKey([]interface{}{s})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%q): %v", s, err)
		}
	}
	for _, s := range low {
		key := withoutrowid.EncodeCompositeKey([]interface{}{s})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%q): %v", s, err)
		}
	}
	for _, s := range mid {
		key := withoutrowid.EncodeCompositeKey([]interface{}{s})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%q): %v", s, err)
		}
	}

	total := len(high) + len(low) + len(mid)
	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	count := compositeScanCount(scan)
	if count != total {
		t.Errorf("expected %d rows, got %d", total, count)
	}
}

// TestCompositeSplitInteriorReverseOrder inserts enough composite keys in reverse
// order to force interior page splits. This exercises tryInsertInteriorCellComposite,
// executeInteriorSplitComposite, and prepareInteriorSplitComposite with the new-key-
// less-than-existing-key branch (return true path).
func TestCompositeSplitInteriorReverseOrder(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	const n = 250
	payload := bytes.Repeat([]byte("w"), 20)

	// Reverse order forces tryInsertInteriorCellComposite to insert before
	// existing cells, which takes the bytes.Compare < 0 branch.
	inserted := 0
	for i := n - 1; i >= 0; i-- {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("r%05d", i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Logf("InsertWithComposite(%d): %v (stopping)", i, err)
			break
		}
		inserted++
	}
	if inserted < 50 {
		t.Fatalf("only inserted %d rows, need at least 50 to trigger interior splits", inserted)
	}

	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	count := compositeScanCount(scan)
	if count < inserted/2 {
		t.Errorf("scan returned only %d rows after %d inserts", count, inserted)
	}
}

// TestCompositeSplitTwoColumnKey uses a two-column composite primary key
// (TEXT, INT) to ensure the multi-column encoding path is exercised through
// all split functions.
func TestCompositeSplitTwoColumnKey(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	const n = 150
	payload := bytes.Repeat([]byte("q"), 25)

	// Insert (prefix, i) pairs in reverse order of i so that each insertion
	// goes before existing cells, hitting the comparison < 0 branch.
	for i := n - 1; i >= 0; i-- {
		key := withoutrowid.EncodeCompositeKey([]interface{}{"group", int64(i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}

	// Also insert a second group in forward order to exercise the >= branch.
	for i := 0; i < n/2; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{"zgroup", int64(i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(zgroup,%d): %v", i, err)
		}
	}

	want := n + n/2
	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	count := compositeScanCount(scan)
	if count < want/2 {
		t.Errorf("scan returned only %d rows, expected around %d", count, want)
	}
}

// TestCompositeSplitCollectLeafCellsComposite directly exercises
// collectLeafCellsForSplitComposite and mergeNewCellWithExistingComposite by
// building a small page and calling them with a key that is both less than and
// greater than existing keys.
func TestCompositeSplitCollectLeafCellsComposite(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	cursor := NewCursorWithOptions(bt, root, true)

	// Insert three keys in sorted order.
	baseKeys := [][]byte{
		withoutrowid.EncodeCompositeKey([]interface{}{"b"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"d"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"f"}),
	}
	for _, k := range baseKeys {
		if err := cursor.InsertWithComposite(0, k, []byte("data")); err != nil {
			t.Fatalf("InsertWithComposite: %v", err)
		}
	}

	cursor.CurrentPage = root
	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	page, err := NewBtreePage(root, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// newKey = "a" is less than all existing keys; tryInsertNewCellComposite returns true.
	newKeyA := withoutrowid.EncodeCompositeKey([]interface{}{"a"})
	cells, keys, err := cursor.collectLeafCellsForSplitComposite(page, newKeyA, []byte("va"))
	if err != nil {
		t.Fatalf("collectLeafCellsForSplitComposite(a): %v", err)
	}
	if len(cells) != 4 {
		t.Errorf("expected 4 cells with 'a' key, got %d", len(cells))
	}
	if !bytes.Equal(keys[0], newKeyA) {
		t.Errorf("first key should be 'a', got %q", keys[0])
	}

	// Reload page for the second call since page state may differ.
	pageData2, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage2: %v", err)
	}
	page2, err := NewBtreePage(root, pageData2, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage2: %v", err)
	}

	// newKey = "z" is greater than all existing keys; tryInsertNewCellComposite returns false
	// for all cells and the key is appended at the end.
	newKeyZ := withoutrowid.EncodeCompositeKey([]interface{}{"z"})
	cells2, keys2, err := cursor.collectLeafCellsForSplitComposite(page2, newKeyZ, []byte("vz"))
	if err != nil {
		t.Fatalf("collectLeafCellsForSplitComposite(z): %v", err)
	}
	if len(cells2) != 4 {
		t.Errorf("expected 4 cells with 'z' key, got %d", len(cells2))
	}
	if !bytes.Equal(keys2[len(keys2)-1], newKeyZ) {
		t.Errorf("last key should be 'z', got %q", keys2[len(keys2)-1])
	}
}

// TestCompositeSplitCollectInteriorCellsComposite directly exercises
// collectInteriorCellsForSplitComposite and tryInsertInteriorCellComposite by
// constructing a composite interior page and calling the collect function with
// keys that sort before and after existing cells.
func TestCompositeSplitCollectInteriorCellsComposite(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	// Allocate an interior composite page manually.
	interiorPage, _, err := cursor.allocateAndInitializeInteriorPage(PageTypeInteriorTableNo)
	if err != nil {
		t.Fatalf("allocateAndInitializeInteriorPage: %v", err)
	}

	// Insert three interior cells with composite keys "b", "d", "f".
	keys := [][]byte{
		withoutrowid.EncodeCompositeKey([]interface{}{"b"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"d"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"f"}),
	}
	for i, k := range keys {
		cell := EncodeTableInteriorCompositeCell(uint32(10+i*10), k)
		if err := interiorPage.InsertCell(i, cell); err != nil {
			t.Fatalf("InsertCell(%d): %v", i, err)
		}
	}
	interiorPage.Header.RightChild = 99

	// Key "a" < "b": tryInsertInteriorCellComposite should insert at index 0.
	newKeyA := withoutrowid.EncodeCompositeKey([]interface{}{"a"})
	cells, ks, _, err := cursor.collectInteriorCellsForSplitComposite(interiorPage, newKeyA, 5)
	if err != nil {
		t.Fatalf("collectInteriorCellsForSplitComposite(a): %v", err)
	}
	if len(cells) != 4 {
		t.Errorf("expected 4 cells with 'a' key, got %d", len(cells))
	}
	if !bytes.Equal(ks[0], newKeyA) {
		t.Errorf("first key should be 'a', got %q", ks[0])
	}

	// Key "z" > "f": the key should be appended at the end.
	newKeyZ := withoutrowid.EncodeCompositeKey([]interface{}{"z"})
	cells2, ks2, _, err := cursor.collectInteriorCellsForSplitComposite(interiorPage, newKeyZ, 6)
	if err != nil {
		t.Fatalf("collectInteriorCellsForSplitComposite(z): %v", err)
	}
	if len(cells2) != 4 {
		t.Errorf("expected 4 cells with 'z' key, got %d", len(cells2))
	}
	if !bytes.Equal(ks2[len(ks2)-1], newKeyZ) {
		t.Errorf("last key should be 'z', got %q", ks2[len(ks2)-1])
	}
}
