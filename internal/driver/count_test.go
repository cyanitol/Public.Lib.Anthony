package driver

import (
	"database/sql"
	"os"
	"testing"
)

// TestCountWithPreparedStatement tests that COUNT(*) works with prepared statements
func TestCountWithPreparedStatement(t *testing.T) {
	dbFile := "test_count_prepared.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some test data
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert row 1: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("failed to insert row 2: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
	if err != nil {
		t.Fatalf("failed to insert row 3: %v", err)
	}

	// Test COUNT(*) with prepared statement
	t.Run("COUNT(*) with prepared statement", func(t *testing.T) {
		t.Parallel()
		stmt, err := db.Prepare("SELECT COUNT(*) FROM users")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 3 {
			t.Errorf("COUNT(*) = %d, want 3", count)
		}
	})

	// Test COUNT(*) with direct query (for comparison)
	t.Run("COUNT(*) with direct query", func(t *testing.T) {
		t.Parallel()
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 3 {
			t.Errorf("COUNT(*) = %d, want 3", count)
		}
	})

	// Test COUNT(column) with prepared statement
	t.Run("COUNT(column) with prepared statement", func(t *testing.T) {
		t.Parallel()
		stmt, err := db.Prepare("SELECT COUNT(name) FROM users")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 3 {
			t.Errorf("COUNT(name) = %d, want 3", count)
		}
	})

	// Test empty table COUNT
	t.Run("COUNT(*) on empty table", func(t *testing.T) {
		t.Parallel()
		_, err = db.Exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("failed to create empty table: %v", err)
		}

		stmt, err := db.Prepare("SELECT COUNT(*) FROM empty_table")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 0 {
			t.Errorf("COUNT(*) on empty table = %d, want 0", count)
		}
	})
}

// TestCountWithParameters tests COUNT with WHERE clause using parameters
func TestCountWithParameters(t *testing.T) {
	dbFile := "test_count_params.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 10)")
	if err != nil {
		t.Fatalf("failed to insert row 1: %v", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 20)")
	if err != nil {
		t.Fatalf("failed to insert row 2: %v", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 15)")
	if err != nil {
		t.Fatalf("failed to insert row 3: %v", err)
	}

	// Test COUNT with WHERE clause - note: WHERE is not yet implemented in this simplified version
	// This test is here for future validation
	t.Run("COUNT(*) total", func(t *testing.T) {
		t.Parallel()
		stmt, err := db.Prepare("SELECT COUNT(*) FROM products")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 3 {
			t.Errorf("COUNT(*) = %d, want 3", count)
		}
	})
}

// TestMultipleAggregates tests multiple aggregate functions in one query
func TestMultipleAggregates(t *testing.T) {
	dbFile := "test_count_multi_agg.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE sales (id INTEGER PRIMARY KEY, amount INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO sales (id, amount) VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Test just COUNT for now (SUM, MIN, MAX may need additional implementation)
	t.Run("COUNT only", func(t *testing.T) {
		t.Parallel()
		stmt, err := db.Prepare("SELECT COUNT(*) FROM sales")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 5 {
			t.Errorf("COUNT(*) = %d, want 5", count)
		}
	})
}
