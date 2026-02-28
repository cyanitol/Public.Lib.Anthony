package pager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// VacuumOptions contains options for the VACUUM operation.
type VacuumOptions struct {
	// IntoFile specifies the filename for VACUUM INTO (optional)
	IntoFile string
	// Schema specifies the database schema to vacuum (optional, default is main)
	Schema string
	// SourceSchema contains the source database schema for VACUUM INTO
	// This is needed because schema may not be persisted to sqlite_master yet
	SourceSchema interface{} // *schema.Schema, but avoiding import cycle
}

// Vacuum rebuilds the database file from scratch, removing unused pages
// and consolidating free space. This operation:
// 1. Creates a new temporary database file
// 2. Copies all live data from the old database to the new one
// 3. Replaces the old database with the new one
//
// This implementation follows SQLite's VACUUM behavior.
func (p *Pager) Vacuum(opts *VacuumOptions) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.validateVacuumPreconditions(); err != nil {
		return err
	}

	targetFile := p.getVacuumTargetFile(opts)
	tempFilename, cleanup, err := p.createVacuumTempFile()
	if err != nil {
		return err
	}
	defer cleanup(&err)

	if err = p.vacuumToFile(tempFilename); err != nil {
		return fmt.Errorf("vacuum failed: %w", err)
	}

	if err = p.closeCurrentDatabase(); err != nil {
		return err
	}

	if err = p.replaceDatabase(tempFilename, targetFile, opts); err != nil {
		return err
	}

	return p.reloadDatabaseAfterVacuum()
}

// validateVacuumPreconditions checks if VACUUM can be performed.
func (p *Pager) validateVacuumPreconditions() error {
	if p.readOnly {
		return ErrReadOnly
	}
	if p.state != PagerStateOpen {
		return ErrTransactionOpen
	}
	return nil
}

// getVacuumTargetFile determines the target file for VACUUM.
func (p *Pager) getVacuumTargetFile(opts *VacuumOptions) string {
	if opts != nil && opts.IntoFile != "" {
		return opts.IntoFile
	}
	return p.filename
}

// createVacuumTempFile creates a temporary file for VACUUM.
func (p *Pager) createVacuumTempFile() (string, func(*error), error) {
	tempFile, err := os.CreateTemp("", "vacuum-*.db")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilename := tempFile.Name()
	tempFile.Close()

	cleanup := func(err *error) {
		if *err != nil {
			os.Remove(tempFilename)
		}
	}

	return tempFilename, cleanup, nil
}

// closeCurrentDatabase closes the current database file.
func (p *Pager) closeCurrentDatabase() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	p.file = nil
	return nil
}

// replaceDatabase replaces the old database with the vacuumed one.
func (p *Pager) replaceDatabase(tempFilename, targetFile string, opts *VacuumOptions) error {
	if opts != nil && opts.IntoFile != "" {
		return p.replaceForVacuumInto(tempFilename, targetFile)
	}
	return p.replaceForVacuumInPlace(tempFilename)
}

// replaceForVacuumInto handles VACUUM INTO operation.
func (p *Pager) replaceForVacuumInto(tempFilename, targetFile string) error {
	if err := copyFile(tempFilename, targetFile); err != nil {
		return fmt.Errorf("failed to copy vacuumed database: %w", err)
	}
	os.Remove(tempFilename)

	file, err := os.OpenFile(p.filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	p.file = file
	return nil
}

// replaceForVacuumInPlace handles in-place VACUUM operation.
func (p *Pager) replaceForVacuumInPlace(tempFilename string) error {
	if err := os.Remove(p.filename); err != nil {
		return fmt.Errorf("failed to remove old database: %w", err)
	}

	if err := os.Rename(tempFilename, p.filename); err != nil {
		return fmt.Errorf("failed to rename vacuumed database: %w", err)
	}

	file, err := os.OpenFile(p.filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	p.file = file
	return nil
}

// reloadDatabaseAfterVacuum reloads the database state after VACUUM.
func (p *Pager) reloadDatabaseAfterVacuum() error {
	p.cache.Clear()

	if err := p.readHeader(); err != nil {
		return fmt.Errorf("failed to read header after vacuum: %w", err)
	}

	stat, err := p.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat database: %w", err)
	}
	p.dbSize = Pgno(stat.Size() / int64(p.pageSize))

	if p.freeList != nil {
		p.freeList.Initialize(0, 0)
	}

	return nil
}

// vacuumToFile performs the actual vacuum operation, writing the compacted
// database to a new file. This method:
// 1. Opens a new pager for the target file
// 2. Copies the database header
// 3. Copies all used pages in sequential order
// 4. Skips all free pages
func (p *Pager) vacuumToFile(targetFilename string) error {
	// Open target pager
	targetPager, err := OpenWithPageSize(targetFilename, false, p.pageSize)
	if err != nil {
		return fmt.Errorf("failed to open target file: %w", err)
	}
	defer targetPager.Close()

	// Copy database header from source to target
	if err = p.copyHeader(targetPager); err != nil {
		return fmt.Errorf("failed to copy header: %w", err)
	}

	// Copy all live pages, compacting as we go
	if err = p.copyLivePages(targetPager); err != nil {
		return fmt.Errorf("failed to copy pages: %w", err)
	}

	// Update target header to reflect new state (no free pages)
	targetPager.header.FreelistTrunk = 0
	targetPager.header.FreelistCount = 0
	targetPager.header.FileChangeCounter++

	// Write updated header to page 1 (use regular Get/Write since we don't hold target's lock)
	page1, err := targetPager.Get(1)
	if err != nil {
		return fmt.Errorf("failed to get page 1: %w", err)
	}

	// Mark page as dirty before modifying
	if err = targetPager.Write(page1); err != nil {
		targetPager.Put(page1)
		return fmt.Errorf("failed to mark page 1 dirty: %w", err)
	}

	headerData := targetPager.header.Serialize()
	copy(page1.Data, headerData)
	targetPager.Put(page1)

	// Commit target pager
	if targetPager.state == PagerStateWriterCachemod ||
	   targetPager.state == PagerStateWriterDbmod {
		err = targetPager.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit target: %w", err)
		}
	}

	return nil
}

// copyHeader copies the database header from this pager to the target pager.
func (p *Pager) copyHeader(target *Pager) error {
	// Get page 1 from source (source pager lock is held by caller)
	sourcePage, err := p.getLocked(1)
	if err != nil {
		return fmt.Errorf("failed to get source page 1: %w", err)
	}
	defer p.Put(sourcePage)

	// Get page 1 from target (use regular Get since we don't hold target's lock)
	targetPage, err := target.Get(1)
	if err != nil {
		return fmt.Errorf("failed to get target page 1: %w", err)
	}
	defer target.Put(targetPage)

	// Mark target page as dirty (use regular Write since we don't hold target's lock)
	if err = target.Write(targetPage); err != nil {
		return fmt.Errorf("failed to mark target page dirty: %w", err)
	}

	// Copy header data (first 100 bytes)
	copy(targetPage.Data[:DatabaseHeaderSize], sourcePage.Data[:DatabaseHeaderSize])

	return nil
}

// copyLivePages copies all live (non-free) pages from source to target,
// compacting them into sequential order.
func (p *Pager) copyLivePages(target *Pager) error {
	freePages, err := p.buildFreePageSet()
	if err != nil {
		return err
	}

	targetPageNum := Pgno(1)
	for sourcePageNum := Pgno(1); sourcePageNum <= p.dbSize; sourcePageNum++ {
		if freePages[sourcePageNum] {
			continue
		}

		if err := p.copySinglePage(sourcePageNum, targetPageNum, target); err != nil {
			return err
		}
		targetPageNum++
	}

	return nil
}

// buildFreePageSet builds a set of free pages to skip during VACUUM.
func (p *Pager) buildFreePageSet() (map[Pgno]bool, error) {
	freePages := make(map[Pgno]bool)
	if p.freeList == nil {
		return freePages, nil
	}

	if err := p.collectFreePages(freePages); err != nil {
		return nil, fmt.Errorf("failed to collect free pages: %w", err)
	}
	return freePages, nil
}

// copySinglePage copies a single page from source to target.
func (p *Pager) copySinglePage(sourcePageNum, targetPageNum Pgno, target *Pager) error {
	sourcePage, err := p.getLocked(sourcePageNum)
	if err != nil {
		return fmt.Errorf("failed to get source page %d: %w", sourcePageNum, err)
	}
	defer p.Put(sourcePage)

	targetPage, err := target.Get(targetPageNum)
	if err != nil {
		return fmt.Errorf("failed to get target page %d: %w", targetPageNum, err)
	}
	defer target.Put(targetPage)

	if err = target.Write(targetPage); err != nil {
		return fmt.Errorf("failed to mark target page dirty: %w", err)
	}

	p.copyPageData(sourcePage, targetPage, sourcePageNum)
	return nil
}

// copyPageData copies data from source page to target page.
func (p *Pager) copyPageData(sourcePage, targetPage *DbPage, sourcePageNum Pgno) {
	offset := 0
	if sourcePageNum == 1 {
		offset = DatabaseHeaderSize
	}
	copy(targetPage.Data[offset:], sourcePage.Data[offset:])
}

// collectFreePages walks the free list and collects all free page numbers.
func (p *Pager) collectFreePages(freePages map[Pgno]bool) error {
	if p.header.FreelistTrunk == 0 {
		return nil
	}

	trunkPage := Pgno(p.header.FreelistTrunk)
	for trunkPage != 0 {
		nextTrunk, err := p.processTrunkPage(trunkPage, freePages)
		if err != nil {
			return err
		}
		trunkPage = nextTrunk
	}

	return nil
}

// processTrunkPage processes a single trunk page in the free list.
func (p *Pager) processTrunkPage(trunkPage Pgno, freePages map[Pgno]bool) (Pgno, error) {
	page, err := p.getLocked(trunkPage)
	if err != nil {
		return 0, fmt.Errorf("failed to get trunk page %d: %w", trunkPage, err)
	}
	defer p.Put(page)

	freePages[trunkPage] = true

	data := p.getTrunkPageData(page)
	nextTrunk := Pgno(binary.BigEndian.Uint32(data[0:4]))
	numLeaves := binary.BigEndian.Uint32(data[4:8])

	p.collectLeafPages(data, numLeaves, freePages)

	return nextTrunk, nil
}

// getTrunkPageData returns the trunk page data, skipping header if on page 1.
func (p *Pager) getTrunkPageData(page *DbPage) []byte {
	if page.Pgno == 1 {
		return page.Data[DatabaseHeaderSize:]
	}
	return page.Data
}

// collectLeafPages collects all leaf page numbers from a trunk page.
func (p *Pager) collectLeafPages(data []byte, numLeaves uint32, freePages map[Pgno]bool) {
	maxLeaves := uint32(FreeListMaxLeafPages(p.pageSize))
	if numLeaves > maxLeaves {
		numLeaves = maxLeaves
	}

	for i := uint32(0); i < numLeaves; i++ {
		offset := 8 + (i * 4)
		leafPage := Pgno(binary.BigEndian.Uint32(data[offset : offset+4]))
		freePages[leafPage] = true
	}
}

// allocateLocked allocates a new page without acquiring the lock.
// The caller must hold the pager lock.
func (p *Pager) allocateLocked() (*DbPage, error) {
	// Increment database size
	p.dbSize++
	newPageNum := p.dbSize

	// Get the new page (will be created)
	page, err := p.getLocked(newPageNum)
	if err != nil {
		p.dbSize-- // Rollback on error
		return nil, err
	}

	// Zero out the page
	for i := range page.Data {
		page.Data[i] = 0
	}

	return page, nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return destFile.Sync()
}
