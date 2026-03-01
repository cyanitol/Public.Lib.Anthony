// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
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
	Name string

	// Database size at the time of savepoint creation
	DbSize Pgno

	// Original page states (for pages modified after this savepoint)
	// Maps page number to original page data
	Pages map[Pgno][]byte

	// Journal file offset at savepoint creation
	JournalOffset int64

	// Number of pages in journal at savepoint creation
	JournalPageCount int
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

// NewSavepoint creates a new savepoint with the given name.
func NewSavepoint(name string) *Savepoint {
	return &Savepoint{
		Name:  name,
		Pages: make(map[Pgno][]byte),
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
		if sp.Name == name {
			return fmt.Errorf("savepoint %s already exists", name)
		}
	}

	// Create new savepoint
	sp := &Savepoint{
		Name:             name,
		DbSize:           p.dbSize,
		Pages:            make(map[Pgno][]byte),
		JournalPageCount: 0,
	}

	// If journal is open, record its current state
	if p.journalFile != nil {
		offset, err := p.journalFile.Seek(0, 1) // Get current position
		if err == nil {
			sp.JournalOffset = offset
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
		if sp.Name == name {
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
		if sp.Name == name {
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
		if _, exists := sp.Pages[page.Pgno]; !exists {
			// Make a copy of the page data
			pageData := make([]byte, len(page.Data))
			copy(pageData, page.Data)
			sp.Pages[page.Pgno] = pageData
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
	for pgno, data := range sp.Pages {
		pagesToRestore[pgno] = data
	}

	// Then check newer savepoints (from oldest to newest after target)
	// For savepoints [sp1, sp2, sp3] (newest first), if target is sp1 at index 2,
	// we check sp2 (index 1) and sp3 (index 0)
	for i := index - 1; i >= 0; i-- {
		newer := savepoints[i]
		for pgno, data := range newer.Pages {
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
	p.dbSize = sp.DbSize

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
		if sp.Name == name {
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
		names[i] = sp.Name
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
alse},
		{"BloomFilter", SetupBloomFilter, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := cm.EstimateSetupCost(tt.setupType, nRows)
			if tt.expectZero && cost != 0 {
				t.Errorf("Expected zero cost for %v, got %d", tt.setupType, cost)
			}
			if !tt.expectZero && cost <= 0 {
				t.Errorf("Expected positive cost for %v, got %d", tt.setupType, cost)
			}
		})
	}
}

func TestCalculateLoopCost(t *testing.T) {
	cm := NewCostModel()

	loop := &WhereLoop{
		Setup: LogEst(100),
		Run:   LogEst(500),
	}

	cost := cm.CalculateLoopCost(loop)
	expected := loop.Setup + loop.Run

	if cost != expected {
		t.Errorf("Expected cost %d, got %d", expected, cost)
	}
}

func TestCombineLoopCostsEmpty(t *testing.T) {
	cm := NewCostModel()

	totalCost, totalRows := cm.CombineLoopCosts([]*WhereLoop{})

	if totalCost != 0 {
		t.Errorf("Expected zero cost for empty loops, got %d", totalCost)
	}
	if totalRows != 0 {
		t.Errorf("Expected zero rows for empty loops, got %d", totalRows)
	}
}

func TestCombineLoopCostsSingle(t *testing.T) {
	cm := NewCostModel()

	loop := &WhereLoop{
		Setup: LogEst(10),
		Run:   LogEst(100),
		NOut:  NewLogEst(50),
	}

	totalCost, totalRows := cm.CombineLoopCosts([]*WhereLoop{loop})

	if totalCost <= 0 {
		t.Error("Total cost should be positive")
	}
	if totalRows != loop.NOut {
		t.Errorf("Total rows should equal loop output")
	}
}

func TestCombineLoopCostsMultiple(t *testing.T) {
	cm := NewCostModel()

	loops := []*WhereLoop{
		{Setup: LogEst(10), Run: LogEst(100), NOut: NewLogEst(10)},
		{Setup: LogEst(5), Run: LogEst(50), NOut: NewLogEst(5)},
		{Setup: LogEst(2), Run: LogEst(20), NOut: NewLogEst(2)},
	}

	totalCost, totalRows := cm.CombineLoopCosts(loops)

	if totalCost <= 0 {
		t.Error("Total cost should be positive")
	}
	if totalRows <= 0 {
		t.Error("Total rows should be positive")
	}

	// Cost should include all setup costs
	minCost := loops[0].Setup + loops[1].Setup + loops[2].Setup
	if totalCost < minCost {
		t.Errorf("Total cost should be at least %d, got %d", minCost, totalCost)
	}
}

func TestEstimateCoveringIndex(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "col1"},
			{Name: "col2"},
			{Name: "col3"},
		},
	}

	tests := []struct {
		name           string
		neededColumns  []string
		expectedResult bool
	}{
		{"all columns covered", []string{"col1", "col2"}, true},
		{"subset covered", []string{"col1"}, true},
		{"not covered", []string{"col1", "col4"}, false},
		{"empty needed", []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.EstimateCoveringIndex(index, tt.neededColumns)
			if result != tt.expectedResult {
				t.Errorf("Expected %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

func TestSelectBestLoop(t *testing.T) {
	cm := NewCostModel()

	loops := []*WhereLoop{
		{Setup: 0, Run: 1000, NOut: 100},
		{Setup: 0, Run: 500, NOut: 50},
		{Setup: 0, Run: 2000, NOut: 200},
	}

	best := cm.SelectBestLoop(loops)
	if best == nil {
		t.Fatal("SelectBestLoop returned nil")
	}

	// Should select the loop with lowest cost
	if best.Run != 500 {
		t.Errorf("Expected best loop with cost 500, got %d", best.Run)
	}
}

func TestSelectBestLoopEmpty(t *testing.T) {
	cm := NewCostModel()

	best := cm.SelectBestLoop([]*WhereLoop{})
	if best != nil {
		t.Error("Expected nil for empty loop list")
	}
}

func TestEstimateOrderByCost(t *testing.T) {
	cm := NewCostModel()

	nRows := NewLogEst(1000)
	cost := cm.EstimateOrderByCost(nRows)

	if cost <= 0 {
		t.Error("Order by cost should be positive")
	}

	// Should be same as sort cost
	sortCost := cm.EstimateSetupCost(SetupSort, nRows)
	if cost != sortCost {
		t.Errorf("Expected cost %d, got %d", sortCost, cost)
	}
}

func TestCheckOrderByOptimization(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "col1", Index: 0, Ascending: true},
			{Name: "col2", Index: 1, Ascending: true},
			{Name: "col3", Index: 2, Ascending: false},
		},
	}

	tests := []struct {
		name     string
		orderBy  []OrderByColumn
		nEq      int
		expected bool
	}{
		{
			name: "matches after nEq",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: true},
				{Column: "col3", Ascending: false},
			},
			nEq:      1,
			expected: true,
		},
		{
			name: "doesn't match column",
			orderBy: []OrderByColumn{
				{Column: "col4", Ascending: true},
			},
			nEq:      1,
			expected: false,
		},
		{
			name: "doesn't match direction",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: false},
			},
			nEq:      1,
			expected: false,
		},
		{
			name:     "all columns used for equality",
			orderBy:  []OrderByColumn{{Column: "col1", Ascending: true}},
			nEq:      3,
			expected: false,
		},
		{
			name: "not enough index columns",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: true},
				{Column: "col3", Ascending: false},
				{Column: "col4", Ascending: true},
			},
			nEq:      1,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.CheckOrderByOptimization(index, tt.orderBy, tt.nEq)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEstimateOutputRowsComprehensive(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		RowLogEst: NewLogEst(10000),
		ColumnStats: []LogEst{
			NewLogEst(100),
			NewLogEst(10),
			NewLogEst(1),
		},
	}

	tests := []struct {
		nEq      int
		expected LogEst
	}{
		{0, index.RowLogEst},
		{1, NewLogEst(100)},
		{2, NewLogEst(10)},
		{3, NewLogEst(1)},
	}

	for _, tt := range tests {
		result := cm.estimateOutputRows(index, tt.nEq)
		if result != tt.expected {
			t.Errorf("For nEq=%d, expected %d, got %d", tt.nEq, tt.expected, result)
		}
	}
}

func TestApplySelectivityReductionsComprehensive(t *testing.T) {
	cm := NewCostModel()

	nOut := NewLogEst(10000)

	result := cm.applySelectivityReductions(nOut, 3)

	// Should be significantly reduced
	if result >= nOut {
		t.Error("Output should be reduced")
	}

	// Should not go negative
	if result < 0 {
		t.Error("Output should not be negative")
	}
}

func TestCalculateLookupCostComprehensive(t *testing.T) {
	cm := NewCostModel()

	nOut := NewLogEst(100)

	costNonCovering := cm.calculateLookupCost(nOut, false)
	costCovering := cm.calculateLookupCost(nOut, true)

	if costCovering >= costNonCovering {
		t.Error("Covering index should be cheaper")
	}

	if costNonCovering <= 0 || costCovering <= 0 {
		t.Error("Costs should be positive")
	}
}

func TestEstimateUniqueLookupComprehensive(t *testing.T) {
	cm := NewCostModel()

	costNonCovering, nOutNonCovering := cm.estimateUniqueLookup(false)
	costCovering, nOutCovering := cm.estimateUniqueLookup(true)

	// Both should return 1 row
	if nOutNonCovering != 0 || nOutCovering != 0 {
		t.Error("Unique lookup should return exactly 1 row (LogEst=0)")
	}

	// Covering should be cheaper
	if costCovering >= costNonCovering {
		t.Error("Covering unique lookup should be cheaper")
	}
}
