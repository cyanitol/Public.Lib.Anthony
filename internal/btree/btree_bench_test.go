// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// BenchmarkBtreeInsert measures sequential inserts of b.N rows.
func BenchmarkBtreeInsert(b *testing.B) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		b.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cursor.Insert(int64(i+1), payload); err != nil {
			b.Fatalf("Insert(%d) error = %v", i+1, err)
		}
	}
}

// BenchmarkBtreeSeek measures point lookups by key over a pre-populated tree.
// Uses a single-page dataset (20 rows) to stay within stable insert range.
func BenchmarkBtreeSeek(b *testing.B) {
	const preload = 20
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		b.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= preload; i++ {
		if err := cursor.Insert(i, []byte("seekdata")); err != nil {
			b.Fatalf("setup Insert(%d) error = %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowid := int64((i % preload) + 1)
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			b.Fatalf("SeekRowid(%d) error = %v", rowid, err)
		}
		if !found {
			b.Fatalf("SeekRowid(%d) not found", rowid)
		}
	}
}

// BenchmarkBtreeRangeScan measures a forward range scan over consecutive rows.
// Uses a single-page dataset (20 rows) to stay within stable insert range.
func BenchmarkBtreeRangeScan(b *testing.B) {
	const preload = 20
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		b.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= preload; i++ {
		if err := cursor.Insert(i, []byte("scandata")); err != nil {
			b.Fatalf("setup Insert(%d) error = %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cursor.MoveToFirst(); err != nil {
			b.Fatalf("MoveToFirst() error = %v", err)
		}
		for cursor.IsValid() {
			if err := cursor.Next(); err != nil {
				break
			}
		}
	}
}

// BenchmarkBtreeDelete measures deleting rows from a pre-populated tree.
// Inserts a fixed 20-row dataset then deletes rows cyclically for b.N iterations.
func BenchmarkBtreeDelete(b *testing.B) {
	const preload = 20
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		b.Fatalf("CreateTable() error = %v", err)
	}
	payload := []byte("deldata!")
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= preload; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			b.Fatalf("setup Insert(%d) error = %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowid := int64((i % preload) + 1)
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			b.Fatalf("SeekRowid(%d) error = %v", rowid, err)
		}
		if found {
			if err := cursor.Delete(); err != nil {
				b.Fatalf("Delete(%d) error = %v", rowid, err)
			}
			// Re-insert to keep the dataset stable across iterations.
			if err := cursor.Insert(rowid, payload); err != nil {
				b.Fatalf("re-Insert(%d) error = %v", rowid, err)
			}
		}
	}
}
