package driver

import (
	"database/sql"
	"os"
	"testing"
)

// TestAggregateCountStar tests COUNT(*) functionality
func TestAggregateCountStar(t *testing.T) {
	t.Parallel()
	dbFile := "test_count_star.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE items (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO items VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO items VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO items VALUES (3, 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COUNT(*)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT(*) query failed: %v", err)
	}
	if count != 3 {
		t.Errorf("COUNT(*) = %d, want 3", count)
	}

	// Test COUNT(*) with WHERE
	err = db.QueryRow("SELECT COUNT(*) FROM items WHERE value > 100").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT(*) with WHERE failed: %v", err)
	}
	if count != 2 {
		t.Errorf("COUNT(*) with WHERE = %d, want 2", count)
	}
}

// TestAggregateSum tests SUM function
func TestAggregateSum(t *testing.T) {
	t.Parallel()
	dbFile := "test_sum.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE sales (id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (3, 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var sum int
	err = db.QueryRow("SELECT SUM(amount) FROM sales").Scan(&sum)
	if err != nil {
		t.Fatalf("SUM query failed: %v", err)
	}
	if sum != 600 {
		t.Errorf("SUM(amount) = %d, want 600", sum)
	}
}

// TestAggregateAvg tests AVG function
func TestAggregateAvg(t *testing.T) {
	t.Parallel()
	dbFile := "test_avg.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE scores (id INTEGER, score INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO scores VALUES (1, 80)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO scores VALUES (2, 90)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO scores VALUES (3, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var avg float64
	err = db.QueryRow("SELECT AVG(score) FROM scores").Scan(&avg)
	if err != nil {
		t.Fatalf("AVG query failed: %v", err)
	}
	if avg != 90.0 {
		t.Errorf("AVG(score) = %f, want 90.0", avg)
	}
}

// TestAggregateMin tests MIN function
func TestAggregateMin(t *testing.T) {
	t.Parallel()
	dbFile := "test_min.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE temps (id INTEGER, temp INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO temps VALUES (1, 25)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO temps VALUES (2, 15)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO temps VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var min int
	err = db.QueryRow("SELECT MIN(temp) FROM temps").Scan(&min)
	if err != nil {
		t.Fatalf("MIN query failed: %v", err)
	}
	if min != 15 {
		t.Errorf("MIN(temp) = %d, want 15", min)
	}
}

// TestAggregateMax tests MAX function
func TestAggregateMax(t *testing.T) {
	t.Parallel()
	dbFile := "test_max.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE temps (id INTEGER, temp INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO temps VALUES (1, 25)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO temps VALUES (2, 15)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO temps VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var max int
	err = db.QueryRow("SELECT MAX(temp) FROM temps").Scan(&max)
	if err != nil {
		t.Fatalf("MAX query failed: %v", err)
	}
	if max != 30 {
		t.Errorf("MAX(temp) = %d, want 30", max)
	}
}

// TestAggregateTotal tests TOTAL function
func TestAggregateTotal(t *testing.T) {
	t.Parallel()
	dbFile := "test_total.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nums (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO nums VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO nums VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var total float64
	err = db.QueryRow("SELECT TOTAL(value) FROM nums").Scan(&total)
	if err != nil {
		t.Fatalf("TOTAL query failed: %v", err)
	}
	if total != 30.0 {
		t.Errorf("TOTAL(value) = %f, want 30.0", total)
	}
}

// TestAggregateCount tests COUNT(column) function
func TestAggregateCount(t *testing.T) {
	t.Parallel()
	dbFile := "test_count_col.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE data (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO data VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO data VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(value) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT(column) query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("COUNT(value) = %d, want 2", count)
	}
}

// TestMultipleAggregatesExtended tests multiple aggregate functions in one query
func TestMultipleAggregatesExtended(t *testing.T) {
	t.Parallel()
	dbFile := "test_multi_agg.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE metrics (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	vals := []int{10, 20, 30, 40, 50}
	for _, v := range vals {
		_, err = db.Exec("INSERT INTO metrics VALUES (?, ?)", len(vals), v)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	var count, sum, min, max int
	var avg float64
	err = db.QueryRow("SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM metrics").
		Scan(&count, &sum, &avg, &min, &max)
	if err != nil {
		t.Fatalf("Multiple aggregates query failed: %v", err)
	}

	if count != 5 {
		t.Errorf("COUNT(*) = %d, want 5", count)
	}
	if sum != 150 {
		t.Errorf("SUM(value) = %d, want 150", sum)
	}
	if avg != 30.0 {
		t.Errorf("AVG(value) = %f, want 30.0", avg)
	}
	if min != 10 {
		t.Errorf("MIN(value) = %d, want 10", min)
	}
	if max != 50 {
		t.Errorf("MAX(value) = %d, want 50", max)
	}
}
