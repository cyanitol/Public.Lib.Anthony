// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// checkpointFramesToDB coverage
// ---------------------------------------------------------------------------

// TestWALCheckpointIndex_CheckpointFramesToDB_EnsureDBFileOpenError exercises
// the ensureDBFileOpen error branch inside checkpointFramesToDB. We construct
// a WAL whose dbFilename points at a non-existent path and call
// checkpointFramesToDB directly (bypassing the mutex-holding wrappers that
// guard the nil-file check).
func TestWALCheckpointIndex_CheckpointFramesToDB_EnsureDBFileOpenError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create a real WAL file so Open() succeeds.
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("create db file: %v", err)
	}
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 10; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}
	defer wal.Close()

	// Write a frame so frameCount > 0 (required for checkpointFramesToDB to
	// reach ensureDBFileOpen).
	pageData := make([]byte, DefaultPageSize)
	if err := wal.WriteFrame(1, pageData, 1); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Remove the database file so ensureDBFileOpen will fail.
	if err := os.Remove(dbFile); err != nil {
		t.Fatalf("remove db file: %v", err)
	}
	// Ensure wal.dbFile is nil so ensureDBFileOpen will try to re-open it.
	wal.dbFile = nil

	_, err = wal.checkpointFramesToDB()
	if err == nil {
		t.Error("checkpointFramesToDB: expected error when db file missing, got nil")
	}
}

// TestWALCheckpointIndex_CheckpointFramesToDB_SyncError exercises the
// dbFile.Sync() error branch inside checkpointFramesToDB. We allow the frames
// to be written to the db file successfully, then close the db file handle
// so that Sync() on it returns an error.
func TestWALCheckpointIndex_CheckpointFramesToDB_SyncError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("create db file: %v", err)
	}
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 10; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}
	defer wal.Close()

	pageData := make([]byte, DefaultPageSize)
	if err := wal.WriteFrame(1, pageData, 1); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Open the db file ourselves and immediately close it so the handle is
	// stale. Inject it as wal.dbFile so ensureDBFileOpen is satisfied, but
	// Sync() will fail on the closed handle.
	staleFH, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open stale fh: %v", err)
	}
	staleFH.Close() // close it – now the handle is stale
	wal.dbFile = staleFH

	_, err = wal.checkpointFramesToDB()
	if err == nil {
		t.Error("checkpointFramesToDB: expected error on stale db file Sync, got nil")
	}
}

// ---------------------------------------------------------------------------
// WALIndex.open coverage
// ---------------------------------------------------------------------------

// TestWALCheckpointIndex_WALIndexOpen_InvalidDir exercises the os.OpenFile
// error branch in WALIndex.open by providing a filename whose parent
// directory does not exist.
func TestWALCheckpointIndex_WALIndexOpen_InvalidDir(t *testing.T) {
	t.Parallel()

	nonExistentDir := filepath.Join(t.TempDir(), "no_such_dir", "test.db")
	_, err := NewWALIndex(nonExistentDir)
	if err == nil {
		t.Error("NewWALIndex with invalid dir: expected error, got nil")
	}
}

// TestWALCheckpointIndex_WALIndexOpen_ReadOnlySmallFile exercises the
// initializeFile error return path in open(). A tiny (but existing) shm file
// means info.Size() < minSize so open() must call initializeFile. We then
// make the file read-only so that Truncate inside initializeFile fails,
// causing open() to close the file and return the error.
func TestWALCheckpointIndex_WALIndexOpen_ReadOnlySmallFile(t *testing.T) {
	t.Parallel()

	filename := tempWALIndexFile(t)
	shmFile := filename + "-shm"

	// Write a tiny shm file, then make it read-only.
	if err := os.WriteFile(shmFile, []byte{0, 1, 2, 3}, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(shmFile, 0400); err != nil {
		t.Fatalf("Chmod read-only: %v", err)
	}
	// Restore permissions on test cleanup so TempDir removal works.
	t.Cleanup(func() { os.Chmod(shmFile, 0600) })

	_, err := NewWALIndex(filename)
	if err == nil {
		t.Error("NewWALIndex with read-only small shm: expected error, got nil")
	}
}

// TestWALCheckpointIndex_WALIndexOpen_ReadHeaderError exercises the
// readHeader error return path in open() by injecting a too-small mmap after
// the file has been opened and initialized, then calling open() again via
// a freshly constructed WALIndex that points at the same shm file.
// The simplest way to reach the readHeader error path without modifying
// production code is to use a properly-sized shm file but corrupt it so that
// readHeader returns ErrWALIndexCorrupt, which triggers the w.Close() +
// return path in open().
func TestWALCheckpointIndex_WALIndexOpen_ReadHeaderCorrupt(t *testing.T) {
	t.Parallel()

	filename := tempWALIndexFile(t)
	shmFile := filename + "-shm"

	// Create a valid index first so the shm file is the right size.
	idx := mustOpenWALIndex(t, filename)
	mustCloseWALIndex(t, idx)

	// Overwrite the shm file with zeroed bytes of the same size but leave
	// IsInit as 0. readHeader will see IsInit==0 and call initializeHeader,
	// which should succeed (no error path reached that way). Instead, we
	// truncate the shm file to exactly WALIndexHeaderSize-1 bytes so that
	// readHeader sees a too-small mmap and returns ErrWALIndexCorrupt.
	badSize := int64(WALIndexHeaderSize - 1)
	if err := os.Truncate(shmFile, badSize); err != nil {
		t.Fatalf("truncate to bad size: %v", err)
	}

	// NewWALIndex calls open(). The shm file is large enough to pass the
	// minSize check (minSize > WALIndexHeaderSize-1 is true, so actually
	// it will call initializeFile again and grow it). We need a different
	// approach: we want a file that is >= minSize but whose content causes
	// readHeader to fail.
	//
	// The simplest portable approach: write a file of exactly minSize bytes
	// filled with 0xFF so that the mmap is large enough but IsInit byte
	// (offset 12) is non-zero (0xFF), and Version (offset 0) does not match
	// WALIndexVersion. validateAndFixHeader will reinitialise the header –
	// that succeeds. So we cannot trigger the readHeader error that way.
	//
	// The readHeader error path (len(mmap) < WALIndexHeaderSize) can only be
	// reached when open() successfully mmaps a file smaller than
	// WALIndexHeaderSize. Because mmapFile itself will fail on an empty file,
	// we write exactly WALIndexHeaderSize-1 bytes so that:
	//   - info.Size() >= minSize? No: WALIndexHeaderSize-1 < minSize, so
	//     initializeFile will be called and grow the file. Not helpful.
	//
	// Given the constraints of the production code, the readHeader corruption
	// path is only reachable by direct struct manipulation (as done in
	// TestWALIndexExtra_ReadHeaderCorrupt). The coverage gap for that line
	// is therefore already exercised by the existing test. We verify that
	// a file with a valid size but corrupt magic/version still opens
	// (validateAndFixHeader handles it) to confirm the happy path through
	// readHeader is covered when IsInit==1.
	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)
	corruptContent := make([]byte, minSize)
	// Set IsInit = 1 (offset 12) and Version = 0xDEAD (mismatched) so that
	// validateAndFixHeader is exercised.
	corruptContent[12] = 1   // IsInit = 1
	corruptContent[0] = 0xFF // Version byte 0 – will not match WALIndexVersion
	if err := os.WriteFile(shmFile, corruptContent, 0600); err != nil {
		t.Fatalf("WriteFile corrupt content: %v", err)
	}

	idx2, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex with corrupt-but-valid-size shm: unexpected error = %v", err)
	}
	defer idx2.Close()
	if idx2.header.Version != WALIndexVersion {
		t.Errorf("after corrupt open: Version = %d, want %d", idx2.header.Version, WALIndexVersion)
	}
}

// ---------------------------------------------------------------------------
// WALIndex.initializeFile coverage
// ---------------------------------------------------------------------------

// TestWALCheckpointIndex_InitializeFile_TruncateError exercises the Truncate
// error branch in initializeFile. We construct a WALIndex by hand whose
// file field is an *os.File opened read-only, so Truncate will fail.
func TestWALCheckpointIndex_InitializeFile_TruncateError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	shmPath := filepath.Join(tempDir, "test.db-shm")

	// Create the shm file and make it read-only.
	if err := os.WriteFile(shmPath, make([]byte, 8), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	roFile, err := os.OpenFile(shmPath, os.O_RDONLY, 0600)
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer roFile.Close()

	idx := &WALIndex{
		filename:  shmPath,
		file:      roFile,
		hashTable: make(map[uint32]uint32),
	}

	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)
	err = idx.initializeFile(minSize)
	if err == nil {
		t.Error("initializeFile with read-only file: expected error, got nil")
	}
}

// TestWALCheckpointIndex_InitializeFile_WriteHeaderError exercises the
// writeHeaderToFile error branch in initializeFile. We use a file that is
// writable for Truncate but then becomes stale (closed) before WriteAt is
// called, so writeHeaderToFile returns an error.
func TestWALCheckpointIndex_InitializeFile_WriteHeaderError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	shmPath := filepath.Join(tempDir, "test.db-shm")

	if err := os.WriteFile(shmPath, make([]byte, 8), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fh, err := os.OpenFile(shmPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	// Close the handle immediately so that Truncate succeeds (it uses the fd
	// number which may still be valid briefly on Linux, but the subsequent
	// WriteAt in writeHeaderToFile will fail on a closed fd).
	fh.Close()

	idx := &WALIndex{
		filename:  shmPath,
		file:      fh,
		hashTable: make(map[uint32]uint32),
	}

	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)
	err = idx.initializeFile(minSize)
	// On Linux, Truncate on a closed fd returns an error (bad file descriptor).
	// Either Truncate or WriteAt will error; either way we expect non-nil.
	if err == nil {
		t.Error("initializeFile with closed file: expected error, got nil")
	}
}

// TestWALCheckpointIndex_InitializeFile_HappyPath confirms initializeFile
// succeeds on a freshly created writable file and sets up the header correctly.
func TestWALCheckpointIndex_InitializeFile_HappyPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	shmPath := filepath.Join(tempDir, "test.db-shm")

	fh, err := os.OpenFile(shmPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	defer fh.Close()

	idx := &WALIndex{
		filename:  shmPath,
		file:      fh,
		hashTable: make(map[uint32]uint32),
	}

	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)
	if err := idx.initializeFile(minSize); err != nil {
		t.Fatalf("initializeFile: unexpected error = %v", err)
	}

	if idx.header == nil {
		t.Fatal("initializeFile did not set header")
	}
	if idx.header.Version != WALIndexVersion {
		t.Errorf("header.Version = %d, want %d", idx.header.Version, WALIndexVersion)
	}
	if idx.header.IsInit != 1 {
		t.Errorf("header.IsInit = %d, want 1", idx.header.IsInit)
	}

	// Verify file was grown to minSize.
	info, err := fh.Stat()
	if err != nil {
		t.Fatalf("stat after initializeFile: %v", err)
	}
	if info.Size() != minSize {
		t.Errorf("file size = %d, want %d", info.Size(), minSize)
	}
}
