// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
)

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
	t.Skip("UDF integration requires full VDBE function execution support")
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

	// Use the custom function in a query
	var result int64
	err = db.QueryRow("SELECT double(21)").Scan(&result)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

// TestAggregateFunctionBasic tests creating and using an aggregate UDF
func TestAggregateFunctionBasic(t *testing.T) {
	t.Skip("UDF integration requires full VDBE function execution support")
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_agg_test_*.db")
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

	// Register custom aggregate function
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateAggregateFunction("product", 1, true, &productFunc{})
	})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	// Create table and insert data
	_, err = db.Exec("CREATE TABLE numbers (value INTEGER)")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	for _, v := range []int{2, 3, 4} {
		_, err = db.Exec("INSERT INTO numbers (value) VALUES (?)", v)
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
	}

	// Use the custom aggregate function
	var result int64
	err = db.QueryRow("SELECT product(value) FROM numbers").Scan(&result)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	if result != 24 {
		t.Errorf("Expected 24, got %d", result)
	}
}

// Test user-defined functions end-to-end
func TestUDFIntegration(t *testing.T) {
	t.Skip("UDF integration requires full VDBE function execution support")
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

	// Test scalar function
	var result int64
	err = db.QueryRow("SELECT double(21)").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}

	// Test with NULL
	var nullResult sql.NullInt64
	err = db.QueryRow("SELECT double(NULL)").Scan(&nullResult)
	if err != nil {
		t.Fatal(err)
	}

	if nullResult.Valid {
		t.Error("Expected NULL result")
	}
}

func TestUDFAggregateIntegration(t *testing.T) {
	t.Skip("UDF integration requires full VDBE function execution support")
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_udf_agg_test_*.db")
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

	// Register aggregate function
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateAggregateFunction("product", 1, true, &productFunc{})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create table
	_, err = db.Exec("CREATE TABLE test_values (num INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	values := []int{2, 3, 4, 5}
	for _, v := range values {
		_, err = db.Exec("INSERT INTO test_values (num) VALUES (?)", v)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test aggregate function
	var result int64
	err = db.QueryRow("SELECT product(num) FROM test_values").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	expected := int64(2 * 3 * 4 * 5)
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}

// Test function overloading
func TestUDFOverloading(t *testing.T) {
	t.Skip("UDF integration requires full VDBE function execution support")
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_udf_overload_test_*.db")
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

	// Register "add1" function with 1 arg
	add1 := &add1Func{}

	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateScalarFunction("add", 1, true, add1)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Register "add2" function with 2 args
	add2 := &add2Func{}

	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateScalarFunction("add", 2, true, add2)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test 1-arg version
	var result1 int64
	err = db.QueryRow("SELECT add(5)").Scan(&result1)
	if err != nil {
		t.Fatal(err)
	}

	if result1 != 6 {
		t.Errorf("Expected 6, got %d", result1)
	}

	// Test 2-arg version
	var result2 int64
	err = db.QueryRow("SELECT add(3, 4)").Scan(&result2)
	if err != nil {
		t.Fatal(err)
	}

	if result2 != 7 {
		t.Errorf("Expected 7, got %d", result2)
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
	t.Skip("UDF integration requires full VDBE function execution support")
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "anthony_udf_unreg_test_*.db")
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

	// Register function
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		return driverConn2.CreateScalarFunction("double", 1, true, &doubleFunc{})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify it works
	var result int64
	err = db.QueryRow("SELECT double(10)").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	if result != 20 {
		t.Errorf("Expected 20, got %d", result)
	}

	// Unregister it
	var removed bool
	err = conn.Raw(func(driverConn interface{}) error {
		driverConn2 := driverConn.(*Conn)
		removed = driverConn2.UnregisterFunction("double", 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !removed {
		t.Error("Expected function to be removed")
	}

	// Verify it no longer works (should fail during execution)
	// The function won't be found, so this should error
	err = db.QueryRow("SELECT double(10)").Scan(&result)
	if err == nil {
		t.Error("Expected error after unregistering function")
	}
}
