// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

func TestCompositeSplitKeepsRootConsistent(t *testing.T) {
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	cursor := NewCursorWithOptions(bt, root, true)

	payload := bytes.Repeat([]byte("x"), 400)
	keys := [][]byte{
		withoutrowid.EncodeCompositeKey([]interface{}{"a00", "b00"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"a01", "b01"}),
	}

	for i, k := range keys {
		if err := cursor.InsertWithComposite(0, k, payload); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
		rootHeader := headerInfo(t, bt, cursor.RootPage)
		origHeader := headerInfo(t, bt, root)
		children := collectInteriorChildren(t, bt, cursor.RootPage)
		childHeaders := make([]string, 0, len(children))
		for _, child := range children {
			childHeaders = append(childHeaders, headerInfo(t, bt, child))
		}
		all := dumpAllHeaders(t, bt)

		// Open a fresh cursor using the updated root and ensure all rows are reachable.
		scan := NewCursorWithOptions(bt, cursor.RootPage, true)
		if err := scan.MoveToFirst(); err != nil {
			t.Fatalf("after insert %d MoveToFirst failed: %v (root=%d original=%d, rootHdr=%s, origHdr=%s, children=%v, childHdrs=%v)", i, err, cursor.RootPage, root, rootHeader, origHeader, children, childHeaders)
		}

		count := 0
		steps := 0
		visited := make([][]byte, 0, 10)
		for scan.IsValid() && steps < 10 {
			visited = append(visited, append([]byte(nil), scan.GetKeyBytes()...))
			count++
			if err := scan.Next(); err != nil {
				break
			}
			steps++
		}

		if count != i+1 {
			t.Fatalf("after insert %d expected %d rows, got %d (rootHdr=%s, origHdr=%s, childHdrs=%v, visited=%q, all=%v)", i, i+1, count, rootHeader, origHeader, childHeaders, visited, all)
		}
		if steps >= 10 {
			t.Fatalf("iteration did not terminate after insert %d (rootHdr=%s, visited=%q, all=%v)", i, rootHeader, visited, all)
		}
	}
}

func headerInfo(t *testing.T, bt *Btree, pgno uint32) string {
	t.Helper()
	data, err := bt.GetPage(pgno)
	if err != nil {
		return fmt.Sprintf("pg %d: get err=%v", pgno, err)
	}
	h, err := ParsePageHeader(data, pgno)
	if err != nil {
		return fmt.Sprintf("pg %d: parse err=%v", pgno, err)
	}
	return fmt.Sprintf("pg %d type=0x%02x leaf=%t int=%t cells=%d right=%d", pgno, h.PageType, h.IsLeaf, h.IsInterior, h.NumCells, h.RightChild)
}

func collectInteriorChildren(t *testing.T, bt *Btree, pgno uint32) []uint32 {
	t.Helper()
	data, err := bt.GetPage(pgno)
	if err != nil {
		return nil
	}
	hdr, err := ParsePageHeader(data, pgno)
	if err != nil || !hdr.IsInterior {
		return nil
	}
	children := make([]uint32, 0, int(hdr.NumCells)+1)
	for i := 0; i < int(hdr.NumCells); i++ {
		ptr, err := hdr.GetCellPointer(data, i)
		if err != nil {
			continue
		}
		cell, err := ParseCell(hdr.PageType, data[ptr:], bt.UsableSize)
		if err != nil {
			continue
		}
		children = append(children, cell.ChildPage)
	}
	children = append(children, hdr.RightChild)
	return children
}

func dumpAllHeaders(t *testing.T, bt *Btree) map[uint32]string {
	t.Helper()
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	headers := make(map[uint32]string, len(bt.Pages))
	for pgno := range bt.Pages {
		headers[pgno] = headerInfo(t, bt, pgno)
	}
	return headers
}
