// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"errors"
	"fmt"
	"sync"
)

// Backup errors
var (
	ErrBackupFinished = errors.New("backup already finished")
	ErrBackupSrcClose = errors.New("source pager closed during backup")
	ErrBackupDstClose = errors.New("destination pager closed during backup")
)

// BackupProgressFunc is called after each Step to report progress.
// It receives the number of pages remaining and the total page count.
type BackupProgressFunc func(remaining, total int)

// Backup copies pages from a source pager to a destination pager.
// It supports incremental copying via Step and allows concurrent
// reads on the source database during the backup.
type Backup struct {
	// Source and destination pagers
	src *Pager
	dst *Pager

	// Total number of pages in the source at backup start
	totalPages Pgno

	// Next page to copy (1-based)
	nextPage Pgno

	// Whether the backup has been finished or aborted
	done bool

	// Optional progress callback
	progress BackupProgressFunc

	// Mutex for thread-safe operations
	mu sync.Mutex
}

// NewBackup creates a new Backup that will copy pages from src to dst.
// The destination pager should be open and writable.
// The source pager should be open (read-only access is sufficient).
func NewBackup(src, dst *Pager) (*Backup, error) {
	if src == nil || dst == nil {
		return nil, errors.New("source and destination pagers must not be nil")
	}
	if dst.readOnly {
		return nil, errors.New("destination pager must be writable")
	}
	if src.pageSize != dst.pageSize {
		return nil, fmt.Errorf("page size mismatch: src=%d dst=%d", src.pageSize, dst.pageSize)
	}

	srcPages := src.PageCount()
	if srcPages == 0 {
		return nil, errors.New("source database is empty")
	}

	return &Backup{
		src:        src,
		dst:        dst,
		totalPages: srcPages,
		nextPage:   1,
	}, nil
}

// SetProgress sets the progress callback for the backup.
func (b *Backup) SetProgress(fn BackupProgressFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.progress = fn
}

// Step copies up to nPages from the source to the destination.
// Returns true when all pages have been copied (backup complete).
// Returns false if there are more pages to copy.
func (b *Backup) Step(nPages int) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done {
		return true, ErrBackupFinished
	}

	if nPages <= 0 {
		nPages = int(b.totalPages)
	}

	// Refresh total page count in case source grew
	currentTotal := b.src.PageCount()
	if currentTotal > b.totalPages {
		b.totalPages = currentTotal
	}

	copied, err := b.copyPages(nPages)
	if err != nil {
		return false, err
	}

	remaining := int(b.totalPages) - int(b.nextPage) + 1
	if remaining < 0 {
		remaining = 0
	}

	if b.progress != nil && copied > 0 {
		b.progress(remaining, int(b.totalPages))
	}

	return b.nextPage > b.totalPages, nil
}

// copyPages copies up to nPages from src to dst starting at b.nextPage.
func (b *Backup) copyPages(nPages int) (int, error) {
	copied := 0
	for copied < nPages && b.nextPage <= b.totalPages {
		if err := b.copySinglePage(b.nextPage); err != nil {
			return copied, fmt.Errorf("backup page %d: %w", b.nextPage, err)
		}
		b.nextPage++
		copied++
	}
	return copied, nil
}

// copySinglePage reads one page from src and writes it to dst.
func (b *Backup) copySinglePage(pgno Pgno) error {
	srcPage, err := b.src.Get(pgno)
	if err != nil {
		return fmt.Errorf("read source page: %w", err)
	}
	defer b.src.Put(srcPage)

	return b.writeDestPage(pgno, srcPage.Data)
}

// writeDestPage writes page data to the destination pager.
func (b *Backup) writeDestPage(pgno Pgno, data []byte) error {
	dstPage, err := b.ensureDestPage(pgno)
	if err != nil {
		return err
	}
	defer b.dst.Put(dstPage)

	copy(dstPage.Data, data)
	return b.dst.Write(dstPage)
}

// ensureDestPage gets or creates a page in the destination.
func (b *Backup) ensureDestPage(pgno Pgno) (*DbPage, error) {
	// Extend destination if needed
	if pgno > b.dst.PageCount() {
		b.dst.mu.Lock()
		if pgno > b.dst.dbSize {
			b.dst.dbSize = pgno
		}
		b.dst.mu.Unlock()
	}
	return b.dst.Get(pgno)
}

// Finish completes the backup by committing the destination.
// After Finish, the Backup object should not be used again.
func (b *Backup) Finish() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done {
		return ErrBackupFinished
	}
	b.done = true

	// Update destination header to match source
	return b.syncDestHeader()
}

// syncDestHeader copies the database header from source to destination.
func (b *Backup) syncDestHeader() error {
	srcHeader := b.src.GetHeader()
	if srcHeader == nil {
		return nil
	}

	dstHeader := b.dst.GetHeader()
	if dstHeader == nil {
		return nil
	}

	dstHeader.DatabaseSize = uint32(b.totalPages)
	dstHeader.FreelistTrunk = srcHeader.FreelistTrunk
	dstHeader.FreelistCount = srcHeader.FreelistCount
	dstHeader.FileChangeCounter++

	return nil
}

// Remaining returns the number of pages left to copy.
func (b *Backup) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done || b.nextPage > b.totalPages {
		return 0
	}
	return int(b.totalPages) - int(b.nextPage) + 1
}

// Total returns the total number of pages in the source database.
func (b *Backup) Total() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.totalPages)
}

// PagesCopied returns the number of pages copied so far.
func (b *Backup) PagesCopied() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.nextPage <= 1 {
		return 0
	}
	return int(b.nextPage) - 1
}
