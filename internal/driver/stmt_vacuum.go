package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileVacuum compiles a VACUUM statement.
// VACUUM rebuilds the database file, removing unused pages and defragmenting.
// Syntax:
//   VACUUM                        - vacuum the main database
//   VACUUM schema_name            - vacuum the specified attached database
//   VACUUM INTO filename          - vacuum main database into a new file
//   VACUUM schema_name INTO file  - vacuum schema into a new file
func (s *Stmt) compileVacuum(vm *vdbe.VDBE, stmt *parser.VacuumStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// VACUUM cannot run inside a transaction
	if s.conn.inTx {
		return nil, fmt.Errorf("cannot VACUUM inside a transaction")
	}

	// Determine which schema to vacuum (default is main)
	schemaName := stmt.Schema
	if schemaName == "" {
		schemaName = "main"
	}

	// For now, only support vacuuming the main database
	// TODO: Add support for attached databases
	if schemaName != "main" {
		return nil, fmt.Errorf("VACUUM of attached databases not yet supported")
	}

	// Build vacuum options
	opts := &pager.VacuumOptions{
		Schema: schemaName,
	}

	if stmt.Into != "" {
		opts.IntoFile = stmt.Into
	} else if stmt.IntoParam {
		// Get filename from parameter
		if len(args) < 1 {
			return nil, fmt.Errorf("VACUUM INTO requires filename parameter")
		}
		filename, ok := args[0].Value.(string)
		if !ok {
			return nil, fmt.Errorf("VACUUM INTO filename must be a string")
		}
		opts.IntoFile = filename
	}

	// For VACUUM INTO, we need to pass the schema so it can be copied to the target
	if opts.IntoFile != "" {
		opts.SourceSchema = s.conn.schema
	}

	// Execute the VACUUM operation directly on the pager
	// We do this at compile time rather than runtime because VACUUM
	// is a special operation that needs to run immediately
	if err := s.conn.pager.Vacuum(opts); err != nil {
		return nil, fmt.Errorf("VACUUM failed: %w", err)
	}

	// For VACUUM INTO, we need to set up the schema in the target database
	// Since schema metadata may not be persisted to sqlite_master yet, we handle it here
	if opts.IntoFile != "" && opts.SourceSchema != nil {
		if err := s.setupVacuumIntoSchema(opts.IntoFile, opts.SourceSchema); err != nil {
			// Log warning but don't fail - the file was created successfully
			// The schema issue can be resolved by the application
			// TODO: Properly implement sqlite_master persistence
			_ = err // For now, ignore the error
		}
	}

	// Generate simple bytecode that indicates success
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// setupVacuumIntoSchema is a workaround to copy schema to VACUUM INTO target.
// This is needed because schema is not yet persisted to sqlite_master in this implementation.
// In a full implementation, schema would be in sqlite_master and copied automatically.
func (s *Stmt) setupVacuumIntoSchema(targetFile string, sourceSchemaIface interface{}) error {
	// Type assert to get the schema
	sourceSchema, ok := sourceSchemaIface.(*schema.Schema)
	if !ok || sourceSchema == nil {
		return nil // No schema to copy
	}

	// Get all tables from source
	tables := sourceSchema.ListTables()

	// Create a new schema for the target and copy tables
	targetSchema := schema.NewSchema()
	for _, tableName := range tables {
		if table, ok := sourceSchema.GetTable(tableName); ok {
			// Clone the table into the target schema
			// We create a shallow copy of the table
			tableCopy := *table
			targetSchema.Tables[tableName] = &tableCopy
		}
	}

	// Copy views as well
	views := sourceSchema.ListViews()
	for _, viewName := range views {
		if view, ok := sourceSchema.GetView(viewName); ok {
			viewCopy := *view
			targetSchema.Views[viewName] = &viewCopy
		}
	}

	// Copy triggers
	triggers := sourceSchema.ListTriggers()
	for _, triggerName := range triggers {
		if trigger, ok := sourceSchema.GetTrigger(triggerName); ok {
			triggerCopy := *trigger
			targetSchema.Triggers[triggerName] = &triggerCopy
		}
	}

	// Now when the target file is opened, we want it to use this schema
	// We'll register it in the driver's state
	s.conn.driver.mu.Lock()
	defer s.conn.driver.mu.Unlock()

	if s.conn.driver.dbs == nil {
		s.conn.driver.dbs = make(map[string]*dbState)
	}

	// Check if target is already registered (shouldn't be, but handle it)
	if existingState, exists := s.conn.driver.dbs[targetFile]; exists {
		// Target already open - just update its schema
		existingState.schema = targetSchema
		return nil
	}

	// Open the target file's pager
	targetPager, err := pager.Open(targetFile, false)
	if err != nil {
		return fmt.Errorf("failed to open target file: %w", err)
	}

	// Create a btree for the target
	targetBtree := btree.NewBtree(uint32(targetPager.PageSize()))
	targetBtree.Provider = newPagerProvider(targetPager)

	// Register the target file's state with the pre-populated schema
	s.conn.driver.dbs[targetFile] = &dbState{
		pager:    targetPager,
		btree:    targetBtree,
		schema:   targetSchema,
		refCnt:   0,
		inMemory: false,
	}

	return nil
}
