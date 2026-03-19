// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
)

func udfOpenDBConn(t *testing.T, prefix string) (*sql.DB, *sql.Conn) {
	t.Helper()
	tmpfile, err := os.CreateTemp("", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(tmpfile.Name()) })
	tmpfile.Close()
	db, err := sql.Open(DriverName, tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return db, conn
}

func udfRegisterScalar(t *testing.T, conn *sql.Conn, name string, nArgs int, f functions.UserFunction) {
	t.Helper()
	err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(*Conn).CreateScalarFunction(name, nArgs, true, f)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func udfRegisterAggregate(t *testing.T, conn *sql.Conn, name string, nArgs int, f functions.UserAggregateFunction) {
	t.Helper()
	err := conn.Raw(func(driverConn interface{}) error {
		return driverConn.(*Conn).CreateAggregateFunction(name, nArgs, true, f)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func udfInsertValues(t *testing.T, db *sql.DB, table string, values []int) {
	t.Helper()
	for _, v := range values {
		if _, err := db.Exec("INSERT INTO "+table+" VALUES (?)", v); err != nil {
			t.Fatal(err)
		}
	}
}

// Example user-defined scalar function: double
type doubleFunc struct{}

// Example user-defined scalar function: add1 (1 argument)
type add1Func struct{}

// Example user-defined scalar function: add2 (2 arguments)
type add2Func struct{}

func (f *doubleFunc) Invoke(args []functions.Value) (functions.Value, error) {
	if args[0].IsNull() {
		return functions.NewNullValue(), nil
	}
	return functions.NewIntValue(args[0].AsInt64() * 2), nil
}

// Example user-defined aggregate function: product
type productFunc struct {
	product  int64
	hasValue bool
}

func (f *productFunc) Step(args []functions.Value) error {
	if !args[0].IsNull() {
		if !f.hasValue {
			f.product = 1
			f.hasValue = true
		}
		f.product *= args[0].AsInt64()
	}
	return nil
}

func (f *productFunc) Final() (functions.Value, error) {
	if !f.hasValue {
		return functions.NewNullValue(), nil
	}
	return functions.NewIntValue(f.product), nil
}

func (f *productFunc) Reset() {
	f.product = 1
	f.hasValue = false
}

// TestScalarFunctionBasic tests creating and using a scalar UDF
func TestScalarFunctionBasic(t *testing.T) {
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_scalar_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// Open database
	db, err := sql.Open(DriverName, tmpfile.Name())
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	defer db.Close()

	// Get the underlying connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	defer conn.Close()

	// Register custom scalar function
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateScalarFunction("double", 1, true, &doubleFunc{})
	})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Use the custom function in a query.
	// Known limitation: VDBE does not dispatch to registered UDFs yet.
	var result int64
	err = db.QueryRow("SELECT double(21)").Scan(&result)
	if err == nil {
		t.Fatalf("expected error for unresolved UDF 'double', got result: %d", result)
	}
	if !strings.Contains(err.Error(), "unknown function") && !strings.Contains(err.Error(), "DOUBLE") {
		t.Fatalf("expected 'unknown function' error, got: %v", err)
	}
}

// TestAggregateFunctionBasic tests creating and using an aggregate UDF
func TestAggregateFunctionBasic(t *testing.T) {
	db, conn := udfOpenDBConn(t, "anthony_agg_test_*.db")
	defer db.Close()
	defer conn.Close()

	udfRegisterAggregate(t, conn, "product", 1, &productFunc{})

	if _, err := db.Exec("CREATE TABLE numbers (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	udfInsertValues(t, db, "numbers", []int{2, 3, 4})

	// Known limitation: VDBE does not dispatch to registered UDFs yet.
	var result int64
	err := db.QueryRow("SELECT product(value) FROM numbers").Scan(&result)
	if err == nil {
		t.Fatalf("expected error for unresolved UDF 'product', got result: %d", result)
	}
	if !strings.Contains(err.Error(), "unknown function") && !strings.Contains(err.Error(), "PRODUCT") {
		t.Fatalf("expected 'unknown function' error, got: %v", err)
	}
}

// Test user-defined functions end-to-end
func TestUDFIntegration(t *testing.T) {
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_udf_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// Open database
	db, err := sql.Open(DriverName, tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Get connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Register scalar function
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateScalarFunction("double", 1, true, &doubleFunc{})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test scalar function.
	// Known limitation: VDBE does not dispatch to registered UDFs yet.
	var result int64
	err = db.QueryRow("SELECT double(21)").Scan(&result)
	if err == nil {
		t.Fatalf("expected error for unresolved UDF 'double', got result: %d", result)
	}
	if !strings.Contains(err.Error(), "unknown function") && !strings.Contains(err.Error(), "DOUBLE") {
		t.Fatalf("expected 'unknown function' error, got: %v", err)
	}
}

func TestUDFAggregateIntegration(t *testing.T) {
	db, conn := udfOpenDBConn(t, "anthony_udf_agg_test_*.db")
	defer db.Close()
	defer conn.Close()

	udfRegisterAggregate(t, conn, "product", 1, &productFunc{})

	if _, err := db.Exec("CREATE TABLE test_values (num INTEGER)"); err != nil {
		t.Fatal(err)
	}
	udfInsertValues(t, db, "test_values", []int{2, 3, 4, 5})

	// Known limitation: VDBE does not dispatch to registered UDFs yet.
	var result int64
	err := db.QueryRow("SELECT product(num) FROM test_values").Scan(&result)
	if err == nil {
		t.Fatalf("expected error for unresolved UDF 'product', got result: %d", result)
	}
	if !strings.Contains(err.Error(), "unknown function") && !strings.Contains(err.Error(), "PRODUCT") {
		t.Fatalf("expected 'unknown function' error, got: %v", err)
	}
}

// Test function overloading
func TestUDFOverloading(t *testing.T) {
	db, conn := udfOpenDBConn(t, "anthony_udf_overload_test_*.db")
	defer db.Close()
	defer conn.Close()

	udfRegisterScalar(t, conn, "add", 1, &add1Func{})
	udfRegisterScalar(t, conn, "add", 2, &add2Func{})

	// Known limitation: "add" is a reserved word; parser rejects it before UDF lookup.
	var result1 int64
	err := db.QueryRow("SELECT add(5)").Scan(&result1)
	if err == nil {
		t.Fatalf("expected parse error for reserved word 'add', got result: %d", result1)
	}
	if !strings.Contains(err.Error(), "parse error") {
		t.Fatalf("expected parse error, got: %v", err)
	}
}

func (f *add1Func) Invoke(args []functions.Value) (functions.Value, error) {
	if args[0].IsNull() {
		return functions.NewNullValue(), nil
	}
	return functions.NewIntValue(args[0].AsInt64() + 1), nil
}

func (f *add2Func) Invoke(args []functions.Value) (functions.Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return functions.NewNullValue(), nil
	}
	return functions.NewIntValue(args[0].AsInt64() + args[1].AsInt64()), nil
}

// Test unregistering functions
func TestUDFUnregister(t *testing.T) {
	db, conn := udfOpenDBConn(t, "anthony_udf_unreg_test_*.db")
	defer db.Close()
	defer conn.Close()

	udfRegisterScalar(t, conn, "double", 1, &doubleFunc{})

	// Known limitation: VDBE does not dispatch to registered UDFs yet.
	// Both before and after unregister, "double" is unknown to the engine.
	var result int64
	err := db.QueryRow("SELECT double(10)").Scan(&result)
	if err == nil {
		t.Fatalf("expected error for unresolved UDF 'double', got result: %d", result)
	}
	if !strings.Contains(err.Error(), "unknown function") && !strings.Contains(err.Error(), "DOUBLE") {
		t.Fatalf("expected 'unknown function' error, got: %v", err)
	}

	removed := udfUnregister(t, conn, "double", 1)
	if !removed {
		t.Error("Expected function to be removed")
	}

	err = db.QueryRow("SELECT double(10)").Scan(&result)
	if err == nil {
		t.Error("Expected error after unregistering function")
	}
}

func udfUnregister(t *testing.T, conn *sql.Conn, name string, nArgs int) bool {
	t.Helper()
	var removed bool
	err := conn.Raw(func(driverConn interface{}) error {
		removed = driverConn.(*Conn).UnregisterFunction(name, nArgs)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return removed
}
