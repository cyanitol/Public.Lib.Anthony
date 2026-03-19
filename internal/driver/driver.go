// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
)

// dbState represents shared state for a database file
type dbState struct {
	pager    pager.PagerInterface
	btree    *btree.Btree
	schema   *schema.Schema
	refCnt   int
	inMemory bool // True for :memory: databases

	// writeMu serializes write operations (VM execution + commit) across all
	// connections sharing the same underlying database. Without this, two
	// connections could race: one modifying page data through the btree while
	// another commits (writes dirty pages to disk), causing a data race on the
	// shared page buffer.
	writeMu sync.Mutex
}

// Driver implements database/sql/driver.Driver for SQLite.
type Driver struct {
	mu          sync.Mutex
	conns       map[string]*Conn
	dbs         map[string]*dbState // Shared database state per file
	memoryCount int64               // Counter for unique memory database IDs (atomic)
}

// sqliteDriver is the singleton driver instance
var sqliteDriver = &Driver{
	conns: make(map[string]*Conn),
	dbs:   make(map[string]*dbState),
}

// DriverName is the registered name for this internal pure Go SQLite driver.
// This is different from the main "sqlite" driver to avoid conflicts.
const DriverName = "sqlite_internal"

// init registers the driver with database/sql
func init() {
	sql.Register(DriverName, sqliteDriver)
}

// Open opens a connection to the database.
// The name is the database file path, optionally with query parameters.
func (d *Driver) Open(name string) (driver.Conn, error) {
	return d.OpenConnector(name)
}

// OpenConnector returns a connector for the database.
func (d *Driver) OpenConnector(name string) (driver.Conn, error) {
	// Parse the DSN to extract filename and configuration
	dsn, err := ParseDSN(name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	filename := dsn.Filename
	isMemory := filename == "" || filename == ":memory:" || dsn.Config.Pager.MemoryDB

	d.mu.Lock()
	defer d.mu.Unlock()
	d.initMaps()

	if isMemory {
		// For in-memory databases, create a unique state per connection
		state, err := d.newMemoryDBState()
		if err != nil {
			return nil, fmt.Errorf("failed to open memory database: %w", err)
		}
		// Assign a unique ID to each memory database connection
		memoryID := fmt.Sprintf(":memory:%d", atomic.AddInt64(&d.memoryCount, 1))
		return d.createMemoryConnection(memoryID, state, dsn.Config)
	}

	state, exists := d.getOrCreateState(filename, dsn.Config.Pager.ReadOnly)
	if state == nil {
		return nil, fmt.Errorf("failed to open database: state creation failed")
	}

	return d.createConnection(filename, state, exists, dsn.Config)
}

// initMaps initializes maps if needed.
func (d *Driver) initMaps() {
	if d.conns == nil {
		d.conns = make(map[string]*Conn)
	}
	if d.dbs == nil {
		d.dbs = make(map[string]*dbState)
	}
}

// getOrCreateState gets or creates database state.
func (d *Driver) getOrCreateState(filename string, readOnly bool) (*dbState, bool) {
	state, exists := d.dbs[filename]
	if exists {
		state.refCnt++
		return state, true
	}
	state, err := d.newDBState(filename, readOnly)
	if err != nil {
		return nil, false
	}
	state.refCnt++
	d.dbs[filename] = state
	return state, false
}

// newDBState creates a new database state.
func (d *Driver) newDBState(filename string, readOnly bool) (*dbState, error) {
	pgr, err := pager.Open(filename, readOnly)
	if err != nil {
		return nil, err
	}
	bt := btree.NewBtree(uint32(pgr.PageSize()))
	bt.Provider = newPagerProvider(pgr)

	// Wire up rollback callback so btree cache is cleared when pager rolls back.
	// This is critical for WITHOUT ROWID tables that maintain separate page caches.
	pgr.RollbackCallback = bt.ClearCache

	return &dbState{
		pager:    pgr,
		btree:    bt,
		schema:   schema.NewSchema(),
		refCnt:   0,
		inMemory: false,
	}, nil
}

// newMemoryDBState creates a new in-memory database state.
func (d *Driver) newMemoryDBState() (*dbState, error) {
	// Use default page size of 4096 for memory databases
	const defaultPageSize = 4096
	pgr, err := pager.OpenMemory(defaultPageSize)
	if err != nil {
		return nil, err
	}
	bt := btree.NewBtree(uint32(pgr.PageSize()))
	bt.Provider = newMemoryPagerProvider(pgr)

	// Wire up rollback callback so btree cache is cleared when pager rolls back.
	// This is critical for WITHOUT ROWID tables that maintain separate page caches.
	pgr.RollbackCallback = bt.ClearCache

	return &dbState{
		pager:    pgr,
		btree:    bt,
		schema:   schema.NewSchema(),
		refCnt:   1, // Memory databases are not shared
		inMemory: true,
	}, nil
}

// createConnection creates a new connection with the given state.
func (d *Driver) createConnection(filename string, state *dbState, existed bool, config *DriverConfig) (driver.Conn, error) {
	secCfg := d.getSecurityConfig(filename, config)
	conn := d.buildConnection(filename, state, secCfg)

	if err := conn.openDatabase(existed); err != nil {
		d.releaseState(filename, state)
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	if err := conn.applyConfig(config); err != nil {
		d.releaseState(filename, state)
		return nil, fmt.Errorf("failed to apply configuration: %w", err)
	}

	d.conns[filename] = conn
	return conn, nil
}

// getSecurityConfig gets or creates security config for connection
func (d *Driver) getSecurityConfig(filename string, config *DriverConfig) *security.SecurityConfig {
	secCfg := config.Security
	if secCfg == nil {
		secCfg = security.DefaultSecurityConfig()
	}

	if filename != "" && filename != ":memory:" {
		dbDir := filepath.Dir(filename)
		if dbDir != "" && dbDir != "." {
			secCfg.DatabaseRoot = dbDir
		}
	}

	return secCfg
}

// buildConnection constructs a new Conn object
func (d *Driver) buildConnection(filename string, state *dbState, secCfg *security.SecurityConfig) *Conn {
	return &Conn{
		driver:         d,
		filename:       filename,
		pager:          state.pager,
		btree:          state.btree,
		schema:         state.schema,
		dbRegistry:     schema.NewDatabaseRegistry(),
		stmts:          make(map[*Stmt]struct{}),
		stmtCache:      NewStmtCache(100),
		writeMu:        &state.writeMu,
		securityConfig: secCfg,
	}
}

// createMemoryConnection creates a new in-memory database connection.
func (d *Driver) createMemoryConnection(memoryID string, state *dbState, config *DriverConfig) (driver.Conn, error) {
	// Use provided security config or default
	secCfg := config.Security
	if secCfg == nil {
		secCfg = security.DefaultSecurityConfig()
	}

	conn := &Conn{
		driver:         d,
		filename:       memoryID,
		pager:          state.pager,
		btree:          state.btree,
		schema:         state.schema,
		dbRegistry:     schema.NewDatabaseRegistry(),
		stmts:          make(map[*Stmt]struct{}),
		stmtCache:      NewStmtCache(100), // Default cache size of 100
		writeMu:        &state.writeMu,
		securityConfig: secCfg,
	}
	// Memory databases are always new, so schema never pre-loaded
	if err := conn.openDatabase(false); err != nil {
		state.pager.Close()
		return nil, fmt.Errorf("failed to initialize memory database: %w", err)
	}

	// Apply DSN configuration settings via PRAGMA statements
	if err := conn.applyConfig(config); err != nil {
		state.pager.Close()
		return nil, fmt.Errorf("failed to apply configuration: %w", err)
	}

	// Track memory connection (each gets unique ID)
	d.conns[memoryID] = conn
	return conn, nil
}

// releaseState decrements ref count and cleans up if needed.
func (d *Driver) releaseState(filename string, state *dbState) {
	state.refCnt--
	if state.refCnt == 0 {
		state.pager.Close()
		delete(d.dbs, filename)
	}
}

// GetDriver returns the singleton driver instance.
func GetDriver() *Driver {
	return sqliteDriver
}

// pagerProvider implements btree.PageProvider to bridge btree and pager
type pagerProvider struct {
	pager    *pager.Pager
	nextPage uint32
}

// newPagerProvider creates a new pager provider
func newPagerProvider(pgr *pager.Pager) *pagerProvider {
	return &pagerProvider{
		pager:    pgr,
		nextPage: uint32(pgr.PageCount()) + 1,
	}
}

// GetPageData retrieves page data from the pager
func (pp *pagerProvider) GetPageData(pgno uint32) ([]byte, error) {
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return nil, err
	}
	return page.GetData(), nil
}

// AllocatePageData allocates a new page
func (pp *pagerProvider) AllocatePageData() (uint32, []byte, error) {
	pgno := pp.nextPage
	pp.nextPage++
	// Get the page through the pager so it's in the pager's cache
	// and modifications will be tracked properly
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return 0, nil, err
	}
	// Mark it as dirty so it gets written during commit
	if err := pp.pager.Write(page); err != nil {
		return 0, nil, err
	}
	return pgno, page.GetData(), nil
}

// MarkDirty marks a page as dirty and journals it for rollback support
func (pp *pagerProvider) MarkDirty(pgno uint32) error {
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return err
	}
	// Call Write() which journals the page before marking it dirty
	// This is crucial for transaction rollback support
	if err := pp.pager.Write(page); err != nil {
		return err
	}
	return nil
}

// memoryPagerProvider implements btree.PageProvider for in-memory databases
type memoryPagerProvider struct {
	pager    *pager.MemoryPager
	nextPage uint32
}

// newMemoryPagerProvider creates a new memory pager provider
func newMemoryPagerProvider(pgr *pager.MemoryPager) *memoryPagerProvider {
	return &memoryPagerProvider{
		pager:    pgr,
		nextPage: uint32(pgr.PageCount()) + 1,
	}
}

// GetPageData retrieves page data from the memory pager
func (pp *memoryPagerProvider) GetPageData(pgno uint32) ([]byte, error) {
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return nil, err
	}
	return page.GetData(), nil
}

// AllocatePageData allocates a new page
func (pp *memoryPagerProvider) AllocatePageData() (uint32, []byte, error) {
	pgno := pp.nextPage
	pp.nextPage++
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return 0, nil, err
	}
	if err := pp.pager.Write(page); err != nil {
		return 0, nil, err
	}
	return pgno, page.GetData(), nil
}

// MarkDirty marks a page as dirty
func (pp *memoryPagerProvider) MarkDirty(pgno uint32) error {
	page, err := pp.pager.Get(pager.Pgno(pgno))
	if err != nil {
		return err
	}
	if err := pp.pager.Write(page); err != nil {
		return err
	}
	return nil
}
