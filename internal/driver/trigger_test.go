package driver

import (
	"database/sql"
	"testing"
)

// TestCreateTrigger tests basic trigger creation.
func TestCreateTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger
	_, err = db.Exec(`
		CREATE TRIGGER log_insert
		AFTER INSERT ON users
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Verify trigger was created by trying to create it again (should fail)
	_, err = db.Exec(`
		CREATE TRIGGER log_insert
		AFTER INSERT ON users
		BEGIN
			SELECT 1;
		END
	`)
	if err == nil {
		t.Error("Expected error when creating duplicate trigger, got nil")
	}
}

// TestCreateTriggerIfNotExists tests IF NOT EXISTS clause.
func TestCreateTriggerIfNotExists(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger
	_, err = db.Exec(`
		CREATE TRIGGER IF NOT EXISTS my_trigger
		BEFORE INSERT ON users
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Create the same trigger again with IF NOT EXISTS (should succeed)
	_, err = db.Exec(`
		CREATE TRIGGER IF NOT EXISTS my_trigger
		BEFORE INSERT ON users
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Errorf("Expected no error with IF NOT EXISTS, got: %v", err)
	}
}

// TestDropTrigger tests basic trigger dropping.
func TestDropTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create a test table
	_, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger
	_, err = db.Exec(`
		CREATE TRIGGER my_trigger
		AFTER INSERT ON users
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Drop the trigger
	_, err = db.Exec(`DROP TRIGGER my_trigger`)
	if err != nil {
		t.Fatalf("Failed to drop trigger: %v", err)
	}

	// Try to drop it again (should fail)
	_, err = db.Exec(`DROP TRIGGER my_trigger`)
	if err == nil {
		t.Error("Expected error when dropping non-existent trigger, got nil")
	}
}

// TestDropTriggerIfExists tests IF EXISTS clause.
func TestDropTriggerIfExists(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Drop a non-existent trigger with IF EXISTS (should succeed)
	_, err := db.Exec(`DROP TRIGGER IF EXISTS nonexistent_trigger`)
	if err != nil {
		t.Errorf("Expected no error with IF EXISTS, got: %v", err)
	}
}

// TestBeforeInsertTrigger tests BEFORE INSERT triggers.
func TestBeforeInsertTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE log (id INTEGER PRIMARY KEY, message TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create log table: %v", err)
	}

	// Create BEFORE INSERT trigger
	_, err = db.Exec(`
		CREATE TRIGGER before_insert_user
		BEFORE INSERT ON users
		BEGIN
			INSERT INTO log (message) VALUES ('before insert');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Note: Trigger execution is marked with TODO comments in the code
	// The trigger is created but not yet executed
	// This test validates trigger creation and compilation

	// Insert a user (trigger would fire here in full implementation)
	_, err = db.Exec(`INSERT INTO users (name) VALUES ('Alice')`)
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}

	// Verify user was inserted
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count users: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 user, got %d", count)
	}

	// TODO: Once trigger execution is fully implemented, verify log table has entry
}

// TestAfterInsertTrigger tests AFTER INSERT triggers.
func TestAfterInsertTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE audit (id INTEGER PRIMARY KEY, action TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	// Create AFTER INSERT trigger
	_, err = db.Exec(`
		CREATE TRIGGER after_insert_product
		AFTER INSERT ON products
		BEGIN
			INSERT INTO audit (action) VALUES ('product added');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert a product
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Widget', 9.99)`)
	if err != nil {
		t.Fatalf("Failed to insert product: %v", err)
	}
}

// TestBeforeUpdateTrigger tests BEFORE UPDATE triggers.
func TestBeforeUpdateTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary REAL)`)
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE salary_log (id INTEGER PRIMARY KEY, emp_id INTEGER, old_salary REAL, new_salary REAL)`)
	if err != nil {
		t.Fatalf("Failed to create salary_log table: %v", err)
	}

	// Create BEFORE UPDATE trigger
	_, err = db.Exec(`
		CREATE TRIGGER before_update_salary
		BEFORE UPDATE ON employees
		BEGIN
			INSERT INTO salary_log (emp_id, old_salary, new_salary) VALUES (1, 0, 0);
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert an employee
	_, err = db.Exec(`INSERT INTO employees (name, salary) VALUES ('Bob', 50000)`)
	if err != nil {
		t.Fatalf("Failed to insert employee: %v", err)
	}

	// Update the employee (trigger would fire here)
	_, err = db.Exec(`UPDATE employees SET salary = 60000 WHERE name = 'Bob'`)
	if err != nil {
		t.Fatalf("Failed to update employee: %v", err)
	}
}

// TestAfterUpdateTrigger tests AFTER UPDATE triggers.
func TestAfterUpdateTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create inventory table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE inventory_log (id INTEGER PRIMARY KEY, product TEXT, action TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create inventory_log table: %v", err)
	}

	// Create AFTER UPDATE trigger
	_, err = db.Exec(`
		CREATE TRIGGER after_update_inventory
		AFTER UPDATE ON inventory
		BEGIN
			INSERT INTO inventory_log (product, action) VALUES ('item', 'updated');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert inventory item
	_, err = db.Exec(`INSERT INTO inventory (product, quantity) VALUES ('Widget', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert inventory: %v", err)
	}

	// Update inventory (trigger would fire here)
	_, err = db.Exec(`UPDATE inventory SET quantity = 90 WHERE product = 'Widget'`)
	if err != nil {
		t.Fatalf("Failed to update inventory: %v", err)
	}
}

// TestBeforeDeleteTrigger tests BEFORE DELETE triggers.
func TestBeforeDeleteTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, username TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create accounts table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE deleted_accounts (id INTEGER PRIMARY KEY, username TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create deleted_accounts table: %v", err)
	}

	// Create BEFORE DELETE trigger
	_, err = db.Exec(`
		CREATE TRIGGER before_delete_account
		BEFORE DELETE ON accounts
		BEGIN
			INSERT INTO deleted_accounts (username) VALUES ('user');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert an account
	_, err = db.Exec(`INSERT INTO accounts (username) VALUES ('testuser')`)
	if err != nil {
		t.Fatalf("Failed to insert account: %v", err)
	}

	// Delete the account (trigger would fire here)
	_, err = db.Exec(`DELETE FROM accounts WHERE username = 'testuser'`)
	if err != nil {
		t.Fatalf("Failed to delete account: %v", err)
	}
}

// TestAfterDeleteTrigger tests AFTER DELETE triggers.
func TestAfterDeleteTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create tables
	_, err := db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE order_log (id INTEGER PRIMARY KEY, message TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create order_log table: %v", err)
	}

	// Create AFTER DELETE trigger
	_, err = db.Exec(`
		CREATE TRIGGER after_delete_order
		AFTER DELETE ON orders
		BEGIN
			INSERT INTO order_log (message) VALUES ('order deleted');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert an order
	_, err = db.Exec(`INSERT INTO orders (customer_id, total) VALUES (1, 99.99)`)
	if err != nil {
		t.Fatalf("Failed to insert order: %v", err)
	}

	// Delete the order (trigger would fire here)
	_, err = db.Exec(`DELETE FROM orders WHERE customer_id = 1`)
	if err != nil {
		t.Fatalf("Failed to delete order: %v", err)
	}
}

// TestTriggerWithForEachRow tests FOR EACH ROW clause.
func TestTriggerWithForEachRow(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger with FOR EACH ROW
	_, err = db.Exec(`
		CREATE TRIGGER item_trigger
		AFTER INSERT ON items
		FOR EACH ROW
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert multiple items
	_, err = db.Exec(`INSERT INTO items (name) VALUES ('Item1')`)
	if err != nil {
		t.Fatalf("Failed to insert item: %v", err)
	}
}

// TestTriggerWithWhenClause tests WHEN clause.
func TestTriggerWithWhenClause(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger with WHEN clause
	_, err = db.Exec(`
		CREATE TRIGGER check_age
		BEFORE INSERT ON users
		WHEN NEW.age < 18
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert users (trigger should only fire for age < 18)
	_, err = db.Exec(`INSERT INTO users (age) VALUES (25)`)
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}

	_, err = db.Exec(`INSERT INTO users (age) VALUES (16)`)
	if err != nil {
		t.Fatalf("Failed to insert underage user: %v", err)
	}
}

// TestUpdateOfTrigger tests UPDATE OF specific columns.
func TestUpdateOfTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec(`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary REAL, department TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger for specific column updates
	_, err = db.Exec(`
		CREATE TRIGGER salary_change
		AFTER UPDATE OF salary ON employees
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert employee
	_, err = db.Exec(`INSERT INTO employees (name, salary, department) VALUES ('Alice', 50000, 'Engineering')`)
	if err != nil {
		t.Fatalf("Failed to insert employee: %v", err)
	}

	// Update salary (should trigger)
	_, err = db.Exec(`UPDATE employees SET salary = 60000 WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("Failed to update salary: %v", err)
	}

	// Update department (should NOT trigger)
	_, err = db.Exec(`UPDATE employees SET department = 'Sales' WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("Failed to update department: %v", err)
	}
}

// TestMultipleTriggers tests multiple triggers on the same table.
func TestMultipleTriggers(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec(`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create multiple triggers
	_, err = db.Exec(`
		CREATE TRIGGER trigger1
		BEFORE INSERT ON products
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger1: %v", err)
	}

	_, err = db.Exec(`
		CREATE TRIGGER trigger2
		AFTER INSERT ON products
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger2: %v", err)
	}

	_, err = db.Exec(`
		CREATE TRIGGER trigger3
		BEFORE UPDATE ON products
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger3: %v", err)
	}

	// Insert product (should fire trigger1 and trigger2)
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Widget', 9.99)`)
	if err != nil {
		t.Fatalf("Failed to insert product: %v", err)
	}

	// Update product (should fire trigger3)
	_, err = db.Exec(`UPDATE products SET price = 10.99 WHERE name = 'Widget'`)
	if err != nil {
		t.Fatalf("Failed to update product: %v", err)
	}
}

// TestTempTrigger tests temporary triggers.
func TestTempTrigger(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec(`CREATE TABLE temp_test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create temporary trigger
	_, err = db.Exec(`
		CREATE TEMP TRIGGER temp_trigger
		AFTER INSERT ON temp_test
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create temp trigger: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO temp_test (value) VALUES ('test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
}

// openTestDB is a helper function to open an in-memory test database.
func openTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}
