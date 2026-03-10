// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileVacuum compiles a VACUUM statement.
// VACUUM rebuilds the database file, removing unused pages and defragmenting.
// Syntax:
//
//	VACUUM                        - vacuum the main database
//	VACUUM schema_name            - vacuum the specified attached database
//	VACUUM INTO filename          - vacuum main database into a new file
//	VACUUM schema_name INTO file  - vacuum schema into a new file
func (s *Stmt) compileVacuum(vm *vdbe.VDBE, stmt *parser.VacuumStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	if err := s.validateVacuumContext(); err != nil {
		return nil, err
	}

	schemaName, err := s.resolveVacuumSchema(stmt)
	if err != nil {
		return nil, err
	}

	opts, err := s.buildVacuumOptions(stmt, args, schemaName)
	if err != nil {
		return nil, err
	}

	if err := s.executeVacuum(opts); err != nil {
		return nil, err
	}

	s.finalizeVacuumBytecode(vm)
	return vm, nil
}

// validateVacuumContext checks if VACUUM can run in the current context.
func (s *Stmt) validateVacuumContext() error {
	if s.conn.inTx {
		return fmt.Errorf("cannot VACUUM inside a transaction")
	}
	// Auto-commit any pending write transaction before VACUUM
	// This is needed because VACUUM requires no active transactions
	if s.conn.pager.InWriteTransaction() {
		if err := s.conn.pager.Commit(); err != nil {
			return fmt.Errorf("failed to commit pending write transaction before VACUUM: %w", err)
		}
	}
	// Also try to end any read transaction - VACUUM requires no active transactions at all
	// We don't check first since there's no InReadTransaction() in the interface,
	// so we just try to end it and ignore "no transaction" errors
	if err := s.conn.pager.EndRead(); err != nil {
		// Only return error if it's not a "no transaction" error
		if err.Error() != "no transaction active" && err.Error() != "no read transaction to end" {
			return fmt.Errorf("failed to end read transaction before VACUUM: %w", err)
		}
	}
	return nil
}

// resolveVacuumSchema determines which schema to vacuum.
func (s *Stmt) resolveVacuumSchema(stmt *parser.VacuumStmt) (string, error) {
	schemaName := stmt.Schema
	if schemaName == "" {
		schemaName = "main"
	}

	// For now, only support vacuuming the main database
	// TODO: Add support for attached databases
	if schemaName != "main" {
		return "", fmt.Errorf("VACUUM of attached databases not yet supported")
	}

	return schemaName, nil
}

// buildVacuumOptions constructs the vacuum options from the statement.
func (s *Stmt) buildVacuumOptions(stmt *parser.VacuumStmt, args []driver.NamedValue, schemaName string) (*pager.VacuumOptions, error) {
	opts := &pager.VacuumOptions{
		Schema: schemaName,
	}

	if err := s.setVacuumIntoFile(opts, stmt, args); err != nil {
		return nil, err
	}

	// For VACUUM INTO, we need to pass the schema so it can be copied to the target
	if opts.IntoFile != "" {
		opts.SourceSchema = s.conn.schema
	}

	return opts, nil
}

// setVacuumIntoFile sets the INTO file in vacuum options.
func (s *Stmt) setVacuumIntoFile(opts *pager.VacuumOptions, stmt *parser.VacuumStmt, args []driver.NamedValue) error {
	var filename string
	if stmt.Into != "" {
		filename = stmt.Into
	} else if stmt.IntoParam {
		var err error
		filename, err = s.getIntoFilenameFromArgs(args)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	// Validate the database file path for security
	validatedPath, err := s.validateDatabasePath(filename)
	if err != nil {
		return fmt.Errorf("invalid VACUUM INTO path: %w", err)
	}

	opts.IntoFile = validatedPath
	return nil
}

// getIntoFilenameFromArgs extracts the filename from VACUUM INTO parameter.
func (s *Stmt) getIntoFilenameFromArgs(args []driver.NamedValue) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("VACUUM INTO requires filename parameter")
	}
	filename, ok := args[0].Value.(string)
	if !ok {
		return "", fmt.Errorf("VACUUM INTO filename must be a string")
	}
	return filename, nil
}

// executeVacuum performs the actual vacuum operation.
func (s *Stmt) executeVacuum(opts *pager.VacuumOptions) error {
	// Store schema before VACUUM
	schemaBackup := s.cloneSchema(s.conn.schema)

	// Execute the VACUUM operation directly on the pager
	// We do this at compile time rather than runtime because VACUUM
	// is a special operation that needs to run immediately
	if err := s.conn.pager.Vacuum(opts); err != nil {
		return fmt.Errorf("VACUUM failed: %w", err)
	}

	// After VACUUM completes and database is reopened, persist schema to sqlite_master
	// This ensures the schema is correctly written to the rebuilt database
	if opts.IntoFile == "" && s.conn.btree != nil {
		if err := schemaBackup.SaveToMaster(s.conn.btree); err != nil {
			return fmt.Errorf("failed to persist schema after VACUUM: %w", err)
		}
		// Commit the schema changes to ensure they're persisted to disk
		// Only commit if there's an active write transaction (SaveToMaster may start one)
		if s.conn.pager.InWriteTransaction() {
			if err := s.conn.pager.Commit(); err != nil {
				return fmt.Errorf("failed to commit schema after VACUUM: %w", err)
			}
		}
		// Reload schema from the persisted data to ensure consistency
		s.conn.schema = schemaBackup
	}

	// For VACUUM INTO, we need to set up the schema in the target database
	if opts.IntoFile != "" && opts.SourceSchema != nil {
		if err := s.setupVacuumIntoSchema(opts.IntoFile, schemaBackup); err != nil {
			return fmt.Errorf("failed to setup VACUUM INTO schema: %w", err)
		}
	}

	return nil
}

// finalizeVacuumBytecode generates the final bytecode for VACUUM.
func (s *Stmt) finalizeVacuumBytecode(vm *vdbe.VDBE) {
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
}

// setupVacuumIntoSchema copies and persists schema to VACUUM INTO target.
// This ensures the target database has the complete schema in sqlite_master.
func (s *Stmt) setupVacuumIntoSchema(targetFile string, sourceSchema *schema.Schema) error {
	if sourceSchema == nil {
		return nil // No schema to copy
	}

	targetSchema := s.cloneSchema(sourceSchema)

	// Register the target schema in driver state
	if err := s.registerTargetSchema(targetFile, targetSchema); err != nil {
		return err
	}

	// Get the target btree to persist schema
	s.conn.driver.mu.Lock()
	dbState, exists := s.conn.driver.dbs[targetFile]
	s.conn.driver.mu.Unlock()

	if !exists || dbState.btree == nil {
		return fmt.Errorf("target database state not found")
	}

	// Persist schema to sqlite_master in target database
	if err := targetSchema.SaveToMaster(dbState.btree); err != nil {
		return fmt.Errorf("failed to save schema to target sqlite_master: %w", err)
	}

	// Commit the schema changes to disk if there's an active write transaction
	// SaveToMaster may have started a transaction if it wrote pages
	if dbState.pager.InWriteTransaction() {
		if err := dbState.pager.Commit(); err != nil {
			return fmt.Errorf("failed to commit schema to target database: %w", err)
		}
	}

	return nil
}

// validateSourceSchema validates and type-asserts the source schema.
func (s *Stmt) validateSourceSchema(sourceSchemaIface interface{}) *schema.Schema {
	sourceSchema, ok := sourceSchemaIface.(*schema.Schema)
	if !ok || sourceSchema == nil {
		return nil
	}
	return sourceSchema
}

// cloneSchema creates a deep copy of the source schema.
func (s *Stmt) cloneSchema(sourceSchema *schema.Schema) *schema.Schema {
	targetSchema := schema.NewSchema()

	s.cloneTables(sourceSchema, targetSchema)
	s.cloneViews(sourceSchema, targetSchema)
	s.cloneTriggers(sourceSchema, targetSchema)

	return targetSchema
}

// cloneTables copies all tables from source to target schema.
func (s *Stmt) cloneTables(sourceSchema, targetSchema *schema.Schema) {
	tables := sourceSchema.ListTables()
	for _, tableName := range tables {
		if table, ok := sourceSchema.GetTable(tableName); ok {
			tableCopy := *table
			targetSchema.AddTableDirect(&tableCopy)
		}
	}
}

// cloneViews copies all views from source to target schema.
func (s *Stmt) cloneViews(sourceSchema, targetSchema *schema.Schema) {
	views := sourceSchema.ListViews()
	for _, viewName := range views {
		if view, ok := sourceSchema.GetView(viewName); ok {
			viewCopy := *view
			targetSchema.AddViewDirect(&viewCopy)
		}
	}
}

// cloneTriggers copies all triggers from source to target schema.
func (s *Stmt) cloneTriggers(sourceSchema, targetSchema *schema.Schema) {
	triggers := sourceSchema.ListTriggers()
	for _, triggerName := range triggers {
		if trigger, ok := sourceSchema.GetTrigger(triggerName); ok {
			triggerCopy := *trigger
			targetSchema.AddTriggerDirect(&triggerCopy)
		}
	}
}

// registerTargetSchema registers the target schema in the driver's state.
func (s *Stmt) registerTargetSchema(targetFile string, targetSchema *schema.Schema) error {
	s.conn.driver.mu.Lock()
	defer s.conn.driver.mu.Unlock()

	if s.conn.driver.dbs == nil {
		s.conn.driver.dbs = make(map[string]*dbState)
	}

	// Check if target is already registered (shouldn't be, but handle it)
	if existingState, exists := s.conn.driver.dbs[targetFile]; exists {
		existingState.schema = targetSchema
		return nil
	}

	return s.createTargetDbState(targetFile, targetSchema)
}

// createTargetDbState creates and registers a new database state for the target file.
func (s *Stmt) createTargetDbState(targetFile string, targetSchema *schema.Schema) error {
	// Validate the target file path (it's already validated in setVacuumIntoFile, but double-check)
	validatedPath, err := s.validateDatabasePath(targetFile)
	if err != nil {
		return fmt.Errorf("invalid target file path: %w", err)
	}

	targetPager, err := pager.Open(validatedPath, false)
	if err != nil {
		return fmt.Errorf("failed to open target file: %w", err)
	}

	targetBtree := btree.NewBtree(uint32(targetPager.PageSize()))
	targetBtree.Provider = newPagerProvider(targetPager)

	s.conn.driver.dbs[validatedPath] = &dbState{
		pager:    targetPager,
		btree:    targetBtree,
		schema:   targetSchema,
		refCnt:   0,
		inMemory: false,
	}

	return nil
}
