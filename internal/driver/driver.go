package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// dbState represents shared state for a database file
type dbState struct {
	pager  *pager.Pager
	btree  *btree.Btree
	schema *schema.Schema
	refCnt int
}

// Driver implements database/sql/driver.Driver for SQLite.
type Driver struct {
	mu    sync.Mutex
	conns map[string]*Conn
	dbs   map[string]*dbState // Shared database state per file
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
	filename := name
	if filename == "" || filename == ":memory:" {
		return nil, fmt.Errorf("in-memory databases not yet supported")
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.initMaps()

	state, exists := d.getOrCreateState(filename)
	if state == nil {
		return nil, fmt.Errorf("failed to open database: state creation failed")
	}

	return d.createConnection(filename, state, exists)
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
func (d *Driver) getOrCreateState(filename string) (*dbState, bool) {
	state, exists := d.dbs[filename]
	if exists {
		state.refCnt++
		return state, true
	}
	state, err := d.newDBState(filename)
	if err != nil {
		return nil, false
	}
	state.refCnt++
	d.dbs[filename] = state
	return state, false
}

// newDBState creates a new database state.
func (d *Driver) newDBState(filename string) (*dbState, error) {
	pgr, err := pager.Open(filename, false)
	if err != nil {
		return nil, err
	}
	bt := btree.NewBtree(uint32(pgr.PageSize()))
	bt.Provider = newPagerProvider(pgr)
	return &dbState{
		pager:  pgr,
		btree:  bt,
		schema: schema.NewSchema(),
		refCnt: 0,
	}, nil
}

// createConnection creates a new connection with the given state.
func (d *Driver) createConnection(filename string, state *dbState, existed bool) (driver.Conn, error) {
	conn := &Conn{
		driver:   d,
		filename: filename,
		pager:    state.pager,
		btree:    state.btree,
		schema:   state.schema,
		stmts:    make(map[*Stmt]struct{}),
	}
	if err := conn.openDatabase(existed); err != nil {
		d.releaseState(filename, state)
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	d.conns[filename] = conn
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
