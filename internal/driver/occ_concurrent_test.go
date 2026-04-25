// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// doCounterIncrement reads the current counter, sleeps briefly to widen the
// window between read and write, then writes counter+1 inside one transaction.
// It returns ErrWriteConflict if the commit detects a version mismatch.
func doCounterIncrement(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	var counter int
	if err := tx.QueryRow("SELECT v FROM counter WHERE id = 1").Scan(&counter); err != nil {
		_ = tx.Rollback()
		return err
	}

	time.Sleep(time.Microsecond * 10)

	if _, err = tx.Exec("UPDATE counter SET v = ? WHERE id = 1", counter+1); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// doCounterWithRetry retries doCounterIncrement on ErrWriteConflict using
// exponential backoff. It returns nil once a commit succeeds.
func doCounterWithRetry(db *sql.DB, maxRetries int) error {
	backoff := time.Microsecond * 50
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := doCounterIncrement(db)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrWriteConflict) {
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return ErrWriteConflict
}

// TestOCCConcurrentWriters launches 10 goroutines that each increment a shared
// counter via read-modify-write transactions. Each goroutine retries on
// ErrWriteConflict. After all goroutines finish, the counter must equal 10,
// proving every write eventually succeeded.
//
// The driver serialises write transactions through a shared write mutex and
// pager lock, so conflicts at commit time occur when a goroutine's startVersion
// snapshot is stale relative to the current writeVersion. Workers that lose the
// race detect this via ErrWriteConflict and retry.
func TestOCCConcurrentWriters(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "occ_concurrent.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	// One connection: the pager only supports one write transaction at a time.
	// All goroutines share this connection and serialise through database/sql.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	occSeedCounter(t, db)

	const numWorkers = 10
	const maxRetries = 5
	failCount := occRunCounterWorkers(db, numWorkers, maxRetries)

	if n := atomic.LoadInt64(&failCount); n > 0 {
		t.Errorf("%d goroutine(s) failed to commit after retries", n)
	}

	occAssertCounter(t, db, numWorkers)
}

// TestOCCVersionMonotonicity verifies that the writeVersion advances by exactly
// one after each successful commit, and that a transaction with a stale
// startVersion gets ErrWriteConflict on commit.
//
// Each round:
//  1. Record the version before the commit.
//  2. Commit an empty write transaction on "writer" — version must become before+1.
//  3. Begin a write transaction on "observer", rewind its startVersion to the
//     pre-commit value, then commit — must get ErrWriteConflict.
func TestOCCVersionMonotonicity(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "occ_monotonic.db")

	setup, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	setup.SetMaxOpenConns(1)
	if _, err := setup.Exec("CREATE TABLE mono (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	setup.Close()

	writer := openRawConn(t, dbPath)
	defer writer.Close()

	observer := openRawConn(t, dbPath)
	defer observer.Close()

	if writer.writeVersion == nil {
		t.Fatal("writeVersion is nil")
	}

	const rounds = 5
	for i := 0; i < rounds; i++ {
		if err := runMonotonicityRound(t, writer, observer, i); err != nil {
			t.Errorf("round %d: %v", i, err)
		}
	}
}

func occSeedCounter(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO counter (id, v) VALUES (1, 0)"); err != nil {
		t.Fatalf("insert initial row: %v", err)
	}
}

func occRunCounterWorkers(db *sql.DB, numWorkers, maxRetries int) int64 {
	var wg sync.WaitGroup
	var failCount int64
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := doCounterWithRetry(db, maxRetries); err != nil {
				atomic.AddInt64(&failCount, 1)
			}
		}()
	}
	wg.Wait()
	return failCount
}

func occAssertCounter(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var finalVal int
	if err := db.QueryRow("SELECT v FROM counter WHERE id = 1").Scan(&finalVal); err != nil {
		t.Fatalf("read final counter: %v", err)
	}
	if finalVal != want {
		t.Errorf("counter = %d, want %d", finalVal, want)
	}
}

// runMonotonicityRound performs one version-advance cycle and conflict check.
func runMonotonicityRound(t *testing.T, writer, observer *Conn, round int) error {
	t.Helper()

	before := atomic.LoadUint64(writer.writeVersion)

	if err := commitEmptyTx(t, writer); err != nil {
		return err
	}

	after := atomic.LoadUint64(writer.writeVersion)
	if after != before+1 {
		t.Errorf("round %d: writeVersion %d → %d, want +1", round, before, after)
	}

	return expectConflictOnObserver(t, observer, before)
}

// commitEmptyTx opens and immediately commits a write transaction on conn.
func commitEmptyTx(t *testing.T, conn *Conn) error {
	t.Helper()
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return err
	}
	return tx.Commit()
}

// expectConflictOnObserver begins a write tx on observer, rewinds startVersion
// to staleVersion (simulating a snapshot taken before the last commit), then
// commits — expecting ErrWriteConflict.
func expectConflictOnObserver(t *testing.T, conn *Conn, staleVersion uint64) error {
	t.Helper()
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return err
	}
	conn.startVersion = staleVersion
	err = tx.Commit()
	if err == nil {
		return errors.New("expected ErrWriteConflict, got nil")
	}
	if !errors.Is(err, ErrWriteConflict) {
		return err
	}
	return nil
}
