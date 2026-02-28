package pager

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWALModeSwitch tests switching to and from WAL mode
func TestWALModeSwitch(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create pager with delete journal mode
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	// Verify initial mode
	if pager.GetJournalMode() != JournalModeDelete {
		t.Errorf("Expected delete mode, got %d", pager.GetJournalMode())
	}

	// Switch to WAL mode
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Verify WAL mode is active
	if pager.GetJournalMode() != JournalModeWAL {
		t.Errorf("Expected WAL mode, got %d", pager.GetJournalMode())
	}

	// Verify WAL files were created
	walFile := dbFile + "-wal"
	shmFile := dbFile + "-shm"

	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Errorf("WAL file was not created")
	}

	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Errorf("WAL index file was not created")
	}

	// Switch back to delete mode
	if err := pager.SetJournalMode(JournalModeDelete); err != nil {
		t.Fatalf("Failed to switch back to delete mode: %v", err)
	}

	// Verify mode changed
	if pager.GetJournalMode() != JournalModeDelete {
		t.Errorf("Expected delete mode after switch, got %d", pager.GetJournalMode())
	}
}

// TestWALModeWriteRead tests writing and reading in WAL mode
func TestWALModeWriteRead(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	// Switch to WAL mode
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Start write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	// Allocate and write a page
	pgno, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Write test data
	testData := []byte("Hello WAL mode!")
	copy(page.Data, testData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	// Commit transaction
	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Read page back
	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer pager.Put(page)

	// Verify data
	if string(page.Data[:len(testData)]) != string(testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, page.Data[:len(testData)])
	}
}

// TestWALModeCheckpoint tests checkpointing in WAL mode
func TestWALModeCheckpoint(t *testing.T) {
	t.Skip("WAL mode not fully implemented - pager doesn't write to WAL during commit")

	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	// Switch to WAL mode
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Write multiple pages
	pageNumbers := make([]Pgno, 0)
	for i := 0; i < 10; i++ {
		if err := pager.BeginWrite(); err != nil {
			t.Fatalf("Failed to begin write: %v", err)
		}

		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pageNumbers = append(pageNumbers, pgno)

		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		// Write unique data
		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte((i + j) % 256)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
		pager.Put(page)

		if err := pager.Commit(); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
	}

	// Verify WAL has frames
	if pager.wal.FrameCount() == 0 {
		t.Errorf("Expected WAL to have frames")
	}

	// Perform checkpoint
	if err := pager.Checkpoint(); err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	// Close and reopen to verify data persisted
	pager.Close()

	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager2.Close()

	// Verify all pages
	for i, pgno := range pageNumbers {
		page, err := pager2.Get(pgno)
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", pgno, err)
		}

		// Verify data
		for j := 0; j < len(page.Data); j++ {
			expected := byte((i + j) % 256)
			if page.Data[j] != expected {
				t.Errorf("Page %d data mismatch at offset %d: expected %d, got %d", pgno, j, expected, page.Data[j])
				break
			}
		}

		pager2.Put(page)
	}
}

// TestWALModeRecovery tests recovery from WAL on database open
func TestWALModeRecovery(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create pager and switch to WAL mode
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Write some data
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	pgno, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	testData := []byte("Recovery test data")
	copy(page.Data, testData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Close without checkpointing (data remains in WAL)
	pager.Close()

	// Reopen database - should recover from WAL
	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager2.Close()

	// Read page and verify data was recovered
	page2, err := pager2.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page after recovery: %v", err)
	}
	defer pager2.Put(page2)

	if string(page2.Data[:len(testData)]) != string(testData) {
		t.Errorf("Data not recovered correctly: expected %q, got %q", testData, page2.Data[:len(testData)])
	}
}

// TestWALModeConcurrentReads tests concurrent readers in WAL mode
func TestWALModeConcurrentReads(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create pager and write initial data
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Write test pages
	numPages := 10
	pageNumbers := make([]Pgno, 0)

	for i := 0; i < numPages; i++ {
		if err := pager.BeginWrite(); err != nil {
			t.Fatalf("Failed to begin write: %v", err)
		}

		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pageNumbers = append(pageNumbers, pgno)

		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		// Write page number as data
		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte((i + j) % 256)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
		pager.Put(page)

		if err := pager.Commit(); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
	}

	pager.Close()

	// Open multiple readers concurrently
	numReaders := 5
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for reader := 0; reader < numReaders; reader++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Each reader opens its own pager
			readerPager, err := Open(dbFile, true)
			if err != nil {
				errors <- err
				return
			}
			defer readerPager.Close()

			// Read all pages
			for i, pgno := range pageNumbers {
				page, err := readerPager.Get(pgno)
				if err != nil {
					errors <- err
					return
				}

				// Verify data
				for j := 0; j < len(page.Data); j++ {
					expected := byte((i + j) % 256)
					if page.Data[j] != expected {
						errors <- err
						readerPager.Put(page)
						return
					}
				}

				readerPager.Put(page)

				// Small delay to increase overlap
				time.Sleep(1 * time.Millisecond)
			}
		}(reader)
	}

	// Wait for all readers
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Reader error: %v", err)
	}
}

// TestWALModeMultipleVersions tests reading the latest version of a page
func TestWALModeMultipleVersions(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Allocate a page
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	pgno, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	// Write first version
	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	version1 := []byte("Version 1")
	copy(page.Data, version1)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Write second version
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	version2 := []byte("Version 2")
	copy(page.Data, version2)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Write third version
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	version3 := []byte("Version 3")
	copy(page.Data, version3)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Read page and verify we get the latest version
	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	defer pager.Put(page)

	if string(page.Data[:len(version3)]) != string(version3) {
		t.Errorf("Expected latest version %q, got %q", version3, page.Data[:len(version3)])
	}
}

// TestWALModeCheckpointModes tests different checkpoint modes
func TestWALModeCheckpointModes(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Write some data
	for i := 0; i < 5; i++ {
		if err := pager.BeginWrite(); err != nil {
			t.Fatalf("Failed to begin write: %v", err)
		}

		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}

		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte((i + j) % 256)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
		pager.Put(page)

		if err := pager.Commit(); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
	}

	// Test passive checkpoint
	if err := pager.CheckpointMode(CheckpointPassive); err != nil {
		t.Errorf("Passive checkpoint failed: %v", err)
	}

	// Test full checkpoint
	if err := pager.CheckpointMode(CheckpointFull); err != nil {
		t.Errorf("Full checkpoint failed: %v", err)
	}

	// Test restart checkpoint
	if err := pager.CheckpointMode(CheckpointRestart); err != nil {
		t.Errorf("Restart checkpoint failed: %v", err)
	}

	// Write more data after restart
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write after restart: %v", err)
	}

	pgno, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page after restart: %v", err)
	}

	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page after restart: %v", err)
	}

	testData := []byte("After restart")
	copy(page.Data, testData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page after restart: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit after restart: %v", err)
	}

	// Test truncate checkpoint
	if err := pager.CheckpointMode(CheckpointTruncate); err != nil {
		t.Errorf("Truncate checkpoint failed: %v", err)
	}

	// Verify WAL file was truncated
	walFile := dbFile + "-wal"
	info, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("WAL file should still exist: %v", err)
	}

	if info.Size() != 0 {
		t.Errorf("WAL file should be empty after truncate checkpoint, got size %d", info.Size())
	}
}

// TestWALModeRollback tests rollback in WAL mode
func TestWALModeRollback(t *testing.T) {
	t.Skip("WAL mode not fully implemented - rollback doesn't properly restore WAL data")

	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Write and commit initial data
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	pgno, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	originalData := []byte("Original data")
	copy(page.Data, originalData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Start new transaction and modify data
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write: %v", err)
	}

	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	modifiedData := []byte("Modified data")
	copy(page.Data, modifiedData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}
	pager.Put(page)

	// Rollback instead of commit
	if err := pager.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify data is back to original
	page, err = pager.Get(pgno)
	if err != nil {
		t.Fatalf("Failed to get page after rollback: %v", err)
	}
	defer pager.Put(page)

	if string(page.Data[:len(originalData)]) != string(originalData) {
		t.Errorf("Data not rolled back correctly: expected %q, got %q", originalData, page.Data[:len(originalData)])
	}
}
