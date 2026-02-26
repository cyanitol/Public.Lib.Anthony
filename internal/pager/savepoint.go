package pager

import (
	"errors"
	"fmt"
	"sync"
)

// Savepoint represents a named savepoint within a transaction.
// Savepoints allow partial rollback of a transaction.
type Savepoint struct {
	// Name of the savepoint
	name string

	// Database size at the time of savepoint creation
	dbSize Pgno

	// Original page states (for pages modified after this savepoint)
	// Maps page number to original page data
	pageStates map[Pgno][]byte

	// Journal file offset at savepoint creation
	journalOffset int64

	// Number of pages in journal at savepoint creation
	journalPageCount int
}

// SavepointManager manages savepoints for a transaction.
type SavepointManager struct {
	// Stack of savepoints (newest first)
	savepoints []*Savepoint

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewSavepointManager creates a new savepoint manager.
func NewSavepointManager() *SavepointManager {
	return &SavepointManager{
		savepoints: make([]*Savepoint, 0),
	}
}

// Savepoint creates a new savepoint with the given name.
func (p *Pager) Savepoint(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Can only create savepoints in a write transaction
	if p.state < PagerStateWriterLocked {
		return errors.New("savepoint requires active write transaction")
	}

	if p.state == PagerStateError {
		return p.errCode
	}

	if name == "" {
		return errors.New("savepoint name cannot be empty")
	}

	// Check if savepoint with this name already exists
	for _, sp := range p.getSavepoints() {
		if sp.name == name {
			return fmt.Errorf("savepoint %s already exists", name)
		}
	}

	// Create new savepoint
	sp := &Savepoint{
		name:             name,
		dbSize:           p.dbSize,
		pageStates:       make(map[Pgno][]byte),
		journalPageCount: 0,
	}

	// If journal is open, record its current state
	if p.journalFile != nil {
		offset, err := p.journalFile.Seek(0, 1) // Get current position
		if err == nil {
			sp.journalOffset = offset
		}
	}

	// Add to savepoint stack
	p.addSavepoint(sp)

	return nil
}

// Release releases a savepoint and all savepoints created after it.
func (p *Pager) Release(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Can only release savepoints in a write transaction
	if p.state < PagerStateWriterLocked {
		return errors.New("release requires active write transaction")
	}

	if p.state == PagerStateError {
		return p.errCode
	}

	// Find the savepoint
	savepoints := p.getSavepoints()
	index := -1
	for i, sp := range savepoints {
		if sp.name == name {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("no such savepoint: %s", name)
	}

	// Remove this savepoint and all newer ones
	p.releaseSavepoints(index)

	return nil
}

// RollbackTo rolls back to a savepoint, undoing all changes made after it.
func (p *Pager) RollbackTo(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Can only rollback to savepoints in a write transaction
	if p.state < PagerStateWriterLocked {
		return errors.New("rollback to savepoint requires active write transaction")
	}

	if p.state == PagerStateError {
		return p.errCode
	}

	// Find the savepoint
	savepoints := p.getSavepoints()
	index := -1
	var targetSavepoint *Savepoint
	for i, sp := range savepoints {
		if sp.name == name {
			index = i
			targetSavepoint = sp
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("no such savepoint: %s", name)
	}

	// Restore page states from newer savepoints
	if err := p.restoreToSavepoint(targetSavepoint, index); err != nil {
		return err
	}

	// Remove newer savepoints (but keep the target savepoint)
	if index > 0 {
		p.releaseSavepoints(index - 1)
	}

	return nil
}

// ClearSavepoints removes all savepoints.
// This is called when a transaction commits or rolls back.
func (p *Pager) ClearSavepoints() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearSavepointsLocked()
}

// clearSavepointsLocked clears all savepoints with lock already held.
func (p *Pager) clearSavepointsLocked() {
	p.savepoints = nil
}

// savePageState saves the current state of a page before modification.
// This is used to support savepoint rollback.
func (p *Pager) savePageState(page *DbPage) error {
	// Get all active savepoints
	savepoints := p.getSavepoints()

	// For each savepoint that doesn't have this page saved, save it
	for _, sp := range savepoints {
		if _, exists := sp.pageStates[page.Pgno]; !exists {
			// Make a copy of the page data
			pageData := make([]byte, len(page.Data))
			copy(pageData, page.Data)
			sp.pageStates[page.Pgno] = pageData
		}
	}

	return nil
}

// restoreToSavepoint restores the database state to the given savepoint.
func (p *Pager) restoreToSavepoint(sp *Savepoint, index int) error {
	// Get all savepoints newer than the target
	savepoints := p.getSavepoints()

	// Collect all pages that need to be restored
	pagesToRestore := make(map[Pgno][]byte)

	// When rolling back to a savepoint, we need to restore pages to their state
	// AT that savepoint. We work from oldest to newest (target to current),
	// collecting the FIRST occurrence of each page (which is the state at the savepoint).

	// Start with the target savepoint
	for pgno, data := range sp.pageStates {
		pagesToRestore[pgno] = data
	}

	// Then check newer savepoints (from oldest to newest after target)
	// For savepoints [sp1, sp2, sp3] (newest first), if target is sp1 at index 2,
	// we check sp2 (index 1) and sp3 (index 0)
	for i := index - 1; i >= 0; i-- {
		newer := savepoints[i]
		for pgno, data := range newer.pageStates {
			// Only take the first version we find (from older savepoints)
			if _, exists := pagesToRestore[pgno]; !exists {
				pagesToRestore[pgno] = data
			}
		}
	}

	// Restore all collected pages to cache
	for pgno, data := range pagesToRestore {
		// Get the page from cache
		page := p.cache.Get(pgno)
		if page != nil {
			// Restore the data
			copy(page.Data, data)
			page.MakeDirty()
		} else {
			// Page not in cache - create and add it
			page = NewDbPage(pgno, p.pageSize)
			copy(page.Data, data)
			page.MakeDirty()
			p.cache.Put(page)
		}
	}

	// Restore database size
	p.dbSize = sp.dbSize

	return nil
}

// addSavepoint adds a savepoint to the stack.
func (p *Pager) addSavepoint(sp *Savepoint) {
	if p.savepoints == nil {
		p.savepoints = make([]*Savepoint, 0)
	}
	// Add to the beginning (stack)
	p.savepoints = append([]*Savepoint{sp}, p.savepoints...)
}

// releaseSavepoints removes savepoints from index 0 to the given index (inclusive).
func (p *Pager) releaseSavepoints(index int) {
	if index < 0 || index >= len(p.savepoints) {
		return
	}
	// Keep only savepoints after the index
	p.savepoints = p.savepoints[index+1:]
}

// getSavepoints returns the current savepoint stack.
func (p *Pager) getSavepoints() []*Savepoint {
	if p.savepoints == nil {
		return []*Savepoint{}
	}
	return p.savepoints
}

// HasSavepoint returns true if a savepoint with the given name exists.
func (p *Pager) HasSavepoint(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, sp := range p.getSavepoints() {
		if sp.name == name {
			return true
		}
	}
	return false
}

// GetSavepointNames returns the names of all active savepoints.
func (p *Pager) GetSavepointNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	savepoints := p.getSavepoints()
	names := make([]string, len(savepoints))
	for i, sp := range savepoints {
		names[i] = sp.name
	}
	return names
}

// savepointCount returns the number of active savepoints.
func (p *Pager) savepointCount() int {
	return len(p.getSavepoints())
}

// Add savepoints field to Pager (this would be added to pager.go in practice)
// For now, we'll document that this field needs to be added to the Pager struct:
// savepoints []*Savepoint

// Note: The Pager struct in pager.go needs to have this field added:
// savepoints []*Savepoint
