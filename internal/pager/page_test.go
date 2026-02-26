package pager

import (
	"bytes"
	"sync"
	"testing"
)

func TestNewDbPage(t *testing.T) {
	pgno := Pgno(5)
	pageSize := 4096

	page := NewDbPage(pgno, pageSize)

	if page.Pgno != pgno {
		t.Errorf("Pgno = %d, want %d", page.Pgno, pgno)
	}

	if len(page.Data) != pageSize {
		t.Errorf("Data length = %d, want %d", len(page.Data), pageSize)
	}

	if page.Flags != PageFlagClean {
		t.Errorf("Flags = %d, want %d", page.Flags, PageFlagClean)
	}

	if page.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", page.RefCount)
	}
}

func TestDbPage_IsDirty(t *testing.T) {
	page := NewDbPage(1, 4096)

	if page.IsDirty() {
		t.Error("New page should not be dirty")
	}

	page.MakeDirty()

	if !page.IsDirty() {
		t.Error("Page should be dirty after MakeDirty()")
	}
}

func TestDbPage_MakeDirty(t *testing.T) {
	page := NewDbPage(1, 4096)

	page.MakeDirty()

	if page.Flags&PageFlagDirty == 0 {
		t.Error("PageFlagDirty not set")
	}

	if page.Flags&PageFlagClean != 0 {
		t.Error("PageFlagClean should not be set")
	}
}

func TestDbPage_MakeClean(t *testing.T) {
	page := NewDbPage(1, 4096)
	page.MakeDirty()

	page.MakeClean()

	if page.Flags&PageFlagClean == 0 {
		t.Error("PageFlagClean not set")
	}

	if page.Flags&PageFlagDirty != 0 {
		t.Error("PageFlagDirty should not be set")
	}
}

func TestDbPage_RefCount(t *testing.T) {
	page := NewDbPage(1, 4096)

	if page.GetRefCount() != 1 {
		t.Errorf("Initial RefCount = %d, want 1", page.GetRefCount())
	}

	page.Ref()
	if page.GetRefCount() != 2 {
		t.Errorf("RefCount after Ref() = %d, want 2", page.GetRefCount())
	}

	page.Ref()
	if page.GetRefCount() != 3 {
		t.Errorf("RefCount after second Ref() = %d, want 3", page.GetRefCount())
	}

	page.Unref()
	if page.GetRefCount() != 2 {
		t.Errorf("RefCount after Unref() = %d, want 2", page.GetRefCount())
	}

	page.Unref()
	page.Unref()
	if page.GetRefCount() != 0 {
		t.Errorf("RefCount after all Unref() = %d, want 0", page.GetRefCount())
	}

	// Test that unref doesn't go negative
	page.Unref()
	if page.GetRefCount() != 0 {
		t.Errorf("RefCount after extra Unref() = %d, want 0", page.GetRefCount())
	}
}

func TestDbPage_Write(t *testing.T) {
	page := NewDbPage(1, 4096)
	testData := []byte("Hello, World!")

	err := page.Write(0, testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if !bytes.Equal(page.Data[0:len(testData)], testData) {
		t.Errorf("Data not written correctly")
	}

	if !page.IsDirty() {
		t.Error("Page should be dirty after Write()")
	}

	if !page.IsWriteable() {
		t.Error("Page should be writeable after Write()")
	}
}

func TestDbPage_Write_InvalidOffset(t *testing.T) {
	page := NewDbPage(1, 4096)

	tests := []struct {
		name   string
		offset int
		data   []byte
	}{
		{"negative offset", -1, []byte("test")},
		{"offset too large", 5000, []byte("test")},
		{"data extends beyond page", 4090, []byte("test data that is too long")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := page.Write(tt.offset, tt.data)
			if err == nil {
				t.Error("Write() expected error, got nil")
			}
		})
	}
}

func TestDbPage_Read(t *testing.T) {
	page := NewDbPage(1, 4096)
	testData := []byte("Hello, World!")
	copy(page.Data, testData)

	data, err := page.Read(0, len(testData))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("Read() = %v, want %v", data, testData)
	}
}

func TestDbPage_Read_InvalidOffset(t *testing.T) {
	page := NewDbPage(1, 4096)

	tests := []struct {
		name   string
		offset int
		length int
	}{
		{"negative offset", -1, 10},
		{"offset too large", 5000, 10},
		{"length extends beyond page", 4090, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := page.Read(tt.offset, tt.length)
			if err == nil {
				t.Error("Read() expected error, got nil")
			}
		})
	}
}

func TestDbPage_Zero(t *testing.T) {
	page := NewDbPage(1, 4096)
	copy(page.Data, []byte("Some data"))

	page.Zero()

	// Check all bytes are zero
	for i, b := range page.Data {
		if b != 0 {
			t.Errorf("Data[%d] = %d, want 0", i, b)
			break
		}
	}

	if !page.IsDirty() {
		t.Error("Page should be dirty after Zero()")
	}
}

func TestDbPage_Clone(t *testing.T) {
	original := NewDbPage(5, 4096)
	testData := []byte("Test data")
	copy(original.Data, testData)
	original.MakeDirty()

	clone := original.Clone()

	// Check that data is copied
	if !bytes.Equal(clone.Data, original.Data) {
		t.Error("Clone data doesn't match original")
	}

	// Check that it's a deep copy
	clone.Data[0] = 'X'
	if original.Data[0] == 'X' {
		t.Error("Modifying clone affected original")
	}

	// Check metadata
	if clone.Pgno != original.Pgno {
		t.Errorf("Clone Pgno = %d, want %d", clone.Pgno, original.Pgno)
	}

	if clone.Flags != original.Flags {
		t.Errorf("Clone Flags = %d, want %d", clone.Flags, original.Flags)
	}

	// Clone should have its own reference count starting at 1
	if clone.GetRefCount() != 1 {
		t.Errorf("Clone RefCount = %d, want 1", clone.GetRefCount())
	}
}

func TestDbPage_ConcurrentAccess(t *testing.T) {
	page := NewDbPage(1, 4096)
	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = page.Read(0, 100)
		}()
	}

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(val byte) {
			defer wg.Done()
			data := []byte{val}
			_ = page.Write(int(val), data)
		}(byte(i))
	}

	// Concurrent ref/unref
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page.Ref()
			page.Unref()
		}()
	}

	wg.Wait()
}

func TestNewPageCache(t *testing.T) {
	pageSize := 4096
	maxPages := 100

	cache := NewPageCache(pageSize, maxPages)

	if cache == nil {
		t.Fatal("NewPageCache() returned nil")
	}

	if cache.pageSize != pageSize {
		t.Errorf("pageSize = %d, want %d", cache.pageSize, pageSize)
	}

	if cache.maxPages != maxPages {
		t.Errorf("maxPages = %d, want %d", cache.maxPages, maxPages)
	}

	if cache.Size() != 0 {
		t.Errorf("Initial size = %d, want 0", cache.Size())
	}
}

func TestPageCache_PutAndGet(t *testing.T) {
	cache := NewPageCache(4096, 100)
	page := NewDbPage(5, 4096)

	err := cache.Put(page)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got := cache.Get(5)
	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.Pgno != page.Pgno {
		t.Errorf("Get() Pgno = %d, want %d", got.Pgno, page.Pgno)
	}
}

func TestPageCache_Get_NotFound(t *testing.T) {
	cache := NewPageCache(4096, 100)

	got := cache.Get(999)
	if got != nil {
		t.Errorf("Get() = %v, want nil", got)
	}
}

func TestPageCache_Remove(t *testing.T) {
	cache := NewPageCache(4096, 100)
	page := NewDbPage(5, 4096)

	_ = cache.Put(page)
	cache.Remove(5)

	got := cache.Get(5)
	if got != nil {
		t.Error("Get() after Remove() should return nil")
	}
}

func TestPageCache_Clear(t *testing.T) {
	cache := NewPageCache(4096, 100)

	for i := 1; i <= 10; i++ {
		page := NewDbPage(Pgno(i), 4096)
		_ = cache.Put(page)
	}

	if cache.Size() != 10 {
		t.Errorf("Size before Clear() = %d, want 10", cache.Size())
	}

	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Size after Clear() = %d, want 0", cache.Size())
	}
}

func TestPageCache_DirtyPages(t *testing.T) {
	cache := NewPageCache(4096, 100)

	// Add clean pages
	for i := 1; i <= 5; i++ {
		page := NewDbPage(Pgno(i), 4096)
		_ = cache.Put(page)
	}

	// Add dirty pages
	for i := 6; i <= 10; i++ {
		page := NewDbPage(Pgno(i), 4096)
		page.MakeDirty()
		_ = cache.Put(page)
	}

	dirtyPages := cache.GetDirtyPages()
	if len(dirtyPages) != 5 {
		t.Errorf("GetDirtyPages() returned %d pages, want 5", len(dirtyPages))
	}

	// Verify all returned pages are dirty
	for _, page := range dirtyPages {
		if !page.IsDirty() {
			t.Errorf("Page %d is not dirty", page.Pgno)
		}
	}
}

func TestPageCache_MakeClean(t *testing.T) {
	cache := NewPageCache(4096, 100)

	// Add dirty pages
	for i := 1; i <= 5; i++ {
		page := NewDbPage(Pgno(i), 4096)
		page.MakeDirty()
		_ = cache.Put(page)
	}

	dirtyBefore := len(cache.GetDirtyPages())
	if dirtyBefore != 5 {
		t.Errorf("Dirty pages before = %d, want 5", dirtyBefore)
	}

	cache.MakeClean()

	dirtyAfter := len(cache.GetDirtyPages())
	if dirtyAfter != 0 {
		t.Errorf("Dirty pages after = %d, want 0", dirtyAfter)
	}
}

func TestPageCache_Eviction(t *testing.T) {
	cache := NewPageCache(4096, 5)

	// Fill cache to capacity
	for i := 1; i <= 5; i++ {
		page := NewDbPage(Pgno(i), 4096)
		page.Unref() // Make refcount 0 so it can be evicted
		_ = cache.Put(page)
	}

	// Add one more page (should trigger eviction)
	page := NewDbPage(6, 4096)
	page.Unref()
	err := cache.Put(page)

	if err != nil {
		t.Fatalf("Put() after cache full error = %v", err)
	}

	// Cache should still be at capacity
	if cache.Size() > 5 {
		t.Errorf("Size after eviction = %d, want <= 5", cache.Size())
	}
}

func TestPageCache_NoEvictDirtyPages(t *testing.T) {
	cache := NewPageCache(4096, 5)

	// Fill cache with dirty pages
	for i := 1; i <= 5; i++ {
		page := NewDbPage(Pgno(i), 4096)
		page.MakeDirty()
		page.Unref()
		_ = cache.Put(page)
	}

	// Try to add another page (should fail because all pages are dirty)
	page := NewDbPage(6, 4096)
	page.Unref()
	err := cache.Put(page)

	if err == nil {
		t.Error("Put() should fail when cache is full of dirty pages")
	}
}

func BenchmarkDbPage_Write(b *testing.B) {
	page := NewDbPage(1, 4096)
	data := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = page.Write(0, data)
	}
}

func BenchmarkDbPage_Read(b *testing.B) {
	page := NewDbPage(1, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = page.Read(0, 100)
	}
}

func BenchmarkPageCache_Put(b *testing.B) {
	cache := NewPageCache(4096, 10000)
	pages := make([]*DbPage, b.N)
	for i := 0; i < b.N; i++ {
		pages[i] = NewDbPage(Pgno(i+1), 4096)
		pages[i].Unref()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Put(pages[i])
	}
}

func BenchmarkPageCache_Get(b *testing.B) {
	cache := NewPageCache(4096, 1000)
	for i := 1; i <= 1000; i++ {
		page := NewDbPage(Pgno(i), 4096)
		_ = cache.Put(page)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Get(Pgno((i % 1000) + 1))
	}
}
