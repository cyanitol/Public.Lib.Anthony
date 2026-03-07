// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Rows implements database/sql/driver.Rows for SQLite.
// It provides iteration over query results by executing the compiled VDBE program.
// The VDBE manages btree cursors internally through OpOpenRead/OpOpenWrite opcodes,
// and iterates using OpRewind/OpNext opcodes that interact with the btree layer.
type Rows struct {
	stmt    *Stmt
	vdbe    *vdbe.VDBE
	columns []string
	ctx     context.Context
	closed  bool
}

// Columns returns the column names.
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	if r.vdbe != nil {
		r.vdbe.Finalize()
		r.vdbe = nil
	}

	return nil
}

// Next advances to the next row and populates dest with the row values.
//
// Integration with btree cursors:
// The VDBE program compiled from SELECT statements contains opcodes that:
// 1. OpOpenRead - Opens a btree cursor on the table's root page
// 2. OpRewind - Moves the btree cursor to the first entry
// 3. OpColumn - Reads column data from the current btree cursor position
// 4. OpResultRow - Packages the columns into a result row
// 5. OpNext - Advances the btree cursor to the next entry and loops
//
// Each call to Step() executes ONE VDBE opcode. We need to loop until:
// - StateRowReady is reached (result row available)
// - StateHalt is reached (no more rows)
// - An error occurs
//
// For empty tables, the first call to Next() will return io.EOF,
// which is the correct behavior per the database/sql/driver interface.
// This allows iteration with rows.Next() to simply not execute (zero iterations)
// without generating an error for the caller.
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	if err := r.checkContextCancellation(); err != nil {
		return err
	}

	if err := r.stepUntilRowReady(); err != nil {
		return err
	}

	return r.copyResultRow(dest)
}

func (r *Rows) checkContextCancellation() error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		return nil
	}
}

func (r *Rows) stepUntilRowReady() error {
	for {
		if err := r.checkContextCancellation(); err != nil {
			return err
		}

		hasMore, err := r.vdbe.Step()
		if err != nil {
			return err
		}

		// hasMore == false means the VDBE has halted (no more rows)
		// This is normal for empty tables or when all rows have been consumed
		// Return io.EOF to signal end of result set per driver.Rows interface
		if !hasMore {
			return io.EOF
		}

		if r.vdbe.State == vdbe.StateRowReady {
			return nil
		}
	}
}

func (r *Rows) copyResultRow(dest []driver.Value) error {
	if r.vdbe.ResultRow == nil {
		return io.EOF
	}

	if len(dest) < len(r.vdbe.ResultRow) {
		return driver.ErrSkip
	}

	for i, mem := range r.vdbe.ResultRow {
		dest[i] = memToValue(mem)
	}
	return nil
}

// ColumnTypeScanType returns the scan type for a column.
func (r *Rows) ColumnTypeScanType(index int) interface{} {
	// SQLite is dynamically typed, so we return interface{}
	return nil
}

// ColumnTypeDatabaseTypeName returns the database type name for a column.
// SQLite uses dynamic typing, so the type is determined by the actual value.
// The column metadata could also be retrieved from the schema, but the
// actual type affinity comes from the data stored in btree cells.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if !r.hasValidResultRow(index) {
		return ""
	}
	return getMemTypeName(r.vdbe.ResultRow[index])
}

// hasValidResultRow checks if we have a valid result row with the given index.
func (r *Rows) hasValidResultRow(index int) bool {
	return r.vdbe.ResultRow != nil && index < len(r.vdbe.ResultRow)
}

// getMemTypeName returns the type name for a memory cell.
func getMemTypeName(mem *vdbe.Mem) string {
	if mem.IsNull() {
		return "NULL"
	}
	if mem.IsInt() {
		return "INTEGER"
	}
	if mem.IsReal() {
		return "REAL"
	}
	if mem.IsStr() {
		return "TEXT"
	}
	if mem.IsBlob() {
		return "BLOB"
	}
	return ""
}

// memToValue converts a VDBE memory cell to a driver.Value.
func memToValue(mem *vdbe.Mem) driver.Value {
	if mem.IsNull() {
		return nil
	} else if mem.IsInt() {
		return mem.IntValue()
	} else if mem.IsReal() {
		return mem.RealValue()
	} else if mem.IsStr() {
		return mem.StrValue()
	} else if mem.IsBlob() {
		return mem.BlobValue()
	}
	return nil
}
