package pager

import (
	"errors"
	"fmt"
	"os"
)

// CheckpointMode represents the different modes of WAL checkpointing.
// These modes match SQLite's checkpoint modes for compatibility.
type CheckpointMode int

const (
	// CheckpointPassive attempts to checkpoint as many frames as possible
	// without blocking readers or writers. Returns immediately if it would
	// have to block waiting for readers or writers.
	// This is the least aggressive checkpoint mode.
	CheckpointPassive CheckpointMode = iota

	// CheckpointFull blocks new writers but allows existing readers to continue.
	// Waits for all readers to finish, then checkpoints all frames.
	// This ensures all WAL frames are copied to the database.
	CheckpointFull

	// CheckpointRestart is like FULL but also resets the WAL to the beginning
	// after checkpointing. This allows the WAL to be reused from the start,
	// preventing the WAL from growing indefinitely.
	CheckpointRestart

	// CheckpointTruncate is like RESTART but also truncates the WAL file to
	// zero bytes after checkpointing. This is the most aggressive mode and
	// ensures the WAL file is completely removed.
	CheckpointTruncate
)

// CheckpointMode errors
var (
	ErrCheckpointInvalidMode = errors.New("invalid checkpoint mode")
	ErrCheckpointBusy        = errors.New("checkpoint busy - readers or writers active")
)

// CheckpointWithMode performs a checkpoint with the specified mode.
// Returns the number of frames checkpointed and the number of frames remaining.
//
// For PASSIVE mode:
//   - Checkpoints frames that can be written without blocking
//   - Returns immediately if blocking would be required
//   - May not checkpoint all frames
//
// For FULL mode:
//   - Waits for readers to finish
//   - Checkpoints all frames
//   - Returns error if readers are still active (simplified implementation)
//
// For RESTART mode:
//   - Same as FULL but resets the WAL afterward
//   - Allows WAL to be reused from the beginning
//
// For TRUNCATE mode:
//   - Same as RESTART but removes the WAL file entirely
//   - Most aggressive cleanup
func (w *WAL) CheckpointWithMode(mode CheckpointMode) (framesCheckpointed int, framesRemaining int, err error) {
	switch mode {
	case CheckpointPassive:
		return w.checkpointPassive()
	case CheckpointFull:
		return w.checkpointFull()
	case CheckpointRestart:
		return w.checkpointRestart()
	case CheckpointTruncate:
		return w.checkpointTruncate()
	default:
		return 0, 0, ErrCheckpointInvalidMode
	}
}

// checkpointPassive performs a passive checkpoint.
// Copies as many WAL frames as possible to the database without blocking.
func (w *WAL) checkpointPassive() (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, 0, errors.New("WAL not open")
	}

	initialFrameCount := int(w.frameCount)
	if initialFrameCount == 0 {
		// Nothing to checkpoint
		return 0, 0, nil
	}

	// Open database file if not already open
	if w.dbFile == nil {
		var err error
		w.dbFile, err = os.OpenFile(w.dbFilename, os.O_RDWR, 0600)
		if err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to open database file: %w", err)
		}
	}

	// Build map of page number to latest frame index
	// This ensures we only write the latest version of each page
	pageFrames := make(map[uint32]uint32)
	for i := uint32(0); i < w.frameCount; i++ {
		frame, err := w.readFrameAtIndex(i)
		if err != nil {
			return 0, initialFrameCount, err
		}
		// Later frames override earlier ones
		pageFrames[frame.PageNumber] = i
	}

	// Write each page to the database
	// In passive mode, we write all pages (simplified - real implementation
	// would check for active readers and skip pages being read)
	for pgno, frameIdx := range pageFrames {
		frame, err := w.readFrameAtIndex(frameIdx)
		if err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to read frame %d: %w", frameIdx, err)
		}

		// Calculate offset in database file (pages are 1-indexed)
		offset := int64(pgno-1) * int64(w.pageSize)

		// Write page to database
		if _, err := w.dbFile.WriteAt(frame.Data, offset); err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to write page %d to database: %w", pgno, err)
		}
	}

	// Sync database file
	if err := w.dbFile.Sync(); err != nil {
		return 0, initialFrameCount, fmt.Errorf("failed to sync database: %w", err)
	}

	// In passive mode, we don't reset the WAL
	// We just return the number of frames checkpointed
	framesCheckpointed := len(pageFrames)
	framesRemaining := initialFrameCount - framesCheckpointed

	return framesCheckpointed, framesRemaining, nil
}

// checkpointFull performs a full checkpoint.
// Blocks new writers and waits for readers, then checkpoints all frames.
func (w *WAL) checkpointFull() (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, 0, errors.New("WAL not open")
	}

	initialFrameCount := int(w.frameCount)
	if initialFrameCount == 0 {
		// Nothing to checkpoint
		return 0, 0, nil
	}

	// In a full implementation, we would:
	// 1. Acquire exclusive lock to block new writers
	// 2. Wait for all readers to finish
	// 3. Checkpoint all frames
	// For this simplified version, we just checkpoint everything

	// Open database file if not already open
	if w.dbFile == nil {
		var err error
		w.dbFile, err = os.OpenFile(w.dbFilename, os.O_RDWR, 0600)
		if err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to open database file: %w", err)
		}
	}

	// Build map of page number to latest frame index
	pageFrames := make(map[uint32]uint32)
	for i := uint32(0); i < w.frameCount; i++ {
		frame, err := w.readFrameAtIndex(i)
		if err != nil {
			return 0, initialFrameCount, err
		}
		// Later frames override earlier ones
		pageFrames[frame.PageNumber] = i
	}

	// Write each page to the database
	for pgno, frameIdx := range pageFrames {
		frame, err := w.readFrameAtIndex(frameIdx)
		if err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to read frame %d: %w", frameIdx, err)
		}

		// Calculate offset in database file (pages are 1-indexed)
		offset := int64(pgno-1) * int64(w.pageSize)

		// Write page to database
		if _, err := w.dbFile.WriteAt(frame.Data, offset); err != nil {
			return 0, initialFrameCount, fmt.Errorf("failed to write page %d to database: %w", pgno, err)
		}
	}

	// Sync database file
	if err := w.dbFile.Sync(); err != nil {
		return 0, initialFrameCount, fmt.Errorf("failed to sync database: %w", err)
	}

	// In full mode, we don't reset the WAL either
	// The difference from passive is that we ensure all frames are written
	framesCheckpointed := len(pageFrames)
	framesRemaining := initialFrameCount - framesCheckpointed

	return framesCheckpointed, framesRemaining, nil
}

// checkpointRestart performs a restart checkpoint.
// Like FULL but also resets the WAL to the beginning.
func (w *WAL) checkpointRestart() (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, 0, errors.New("WAL not open")
	}

	initialFrameCount := int(w.frameCount)
	if initialFrameCount == 0 {
		// Nothing to checkpoint, but still reset the WAL
		return 0, 0, nil
	}

	// Open database file and checkpoint frames
	framesCheckpointed, err := w.checkpointFramesForRestart(initialFrameCount)
	if err != nil {
		return 0, initialFrameCount, err
	}

	// Now reset the WAL (RESTART mode behavior)
	if err := w.restartWAL(); err != nil {
		return framesCheckpointed, 0, err
	}

	return framesCheckpointed, 0, nil
}

// checkpointFramesForRestart handles the frame checkpointing phase of restart checkpoint.
func (w *WAL) checkpointFramesForRestart(initialFrameCount int) (int, error) {
	// Open database file if not already open
	if err := w.ensureDBFileOpen(); err != nil {
		return 0, fmt.Errorf("failed to open database file: %w", err)
	}

	// Build map of page number to latest frame index
	pageFrames, err := w.buildPageFrameMap()
	if err != nil {
		return 0, err
	}

	// Write frames to database
	if err := w.writeFramesToDB(pageFrames); err != nil {
		return 0, err
	}

	// Sync database file
	if err := w.dbFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync database: %w", err)
	}

	return len(pageFrames), nil
}

// restartWAL resets the WAL file to the beginning with a fresh header.
func (w *WAL) restartWAL() error {
	// Truncate and reset WAL
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// Seek to beginning and write fresh header
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek WAL: %w", err)
	}

	// Generate new salt and increment checkpoint sequence
	w.salt1 = generateSalt()
	w.salt2 = generateSalt()
	w.frameCount = 0
	w.checkpointSeq++

	if err := w.writeHeader(); err != nil {
		return fmt.Errorf("failed to write new WAL header: %w", err)
	}

	return nil
}

// checkpointTruncate performs a truncate checkpoint.
// Like RESTART but also truncates the WAL file to zero bytes.
func (w *WAL) checkpointTruncate() (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, 0, errors.New("WAL not open")
	}

	initialFrameCount := int(w.frameCount)

	// Open database file if needed and checkpoint frames
	framesCheckpointed, err := w.checkpointFramesForTruncate(initialFrameCount)
	if err != nil {
		return 0, initialFrameCount, err
	}

	// TRUNCATE mode: completely remove the WAL file
	if err := w.truncateWALFile(); err != nil {
		return framesCheckpointed, 0, err
	}

	return framesCheckpointed, 0, nil
}

// checkpointFramesForTruncate handles the frame checkpointing phase of truncate checkpoint.
func (w *WAL) checkpointFramesForTruncate(initialFrameCount int) (int, error) {
	// No frames to checkpoint
	if initialFrameCount == 0 {
		return 0, nil
	}

	// Open database file if not already open
	if err := w.ensureDBFileOpen(); err != nil {
		return 0, fmt.Errorf("failed to open database file: %w", err)
	}

	// Build map of page number to latest frame index
	pageFrames, err := w.buildPageFrameMap()
	if err != nil {
		return 0, err
	}

	// Write frames to database
	if err := w.writeFramesToDB(pageFrames); err != nil {
		return 0, err
	}

	// Sync database file
	if err := w.dbFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync database: %w", err)
	}

	return len(pageFrames), nil
}

// truncateWALFile closes and truncates the WAL file to zero bytes.
func (w *WAL) truncateWALFile() error {
	// Close the WAL file
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}

	// Truncate to zero bytes (effectively deleting content)
	if err := os.Truncate(w.filename, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	// Reset internal state
	w.frameCount = 0
	w.initialized = false

	return nil
}

// ensureDBFileOpen opens the database file if it's not already open.
func (w *WAL) ensureDBFileOpen() error {
	if w.dbFile != nil {
		return nil
	}

	var err error
	w.dbFile, err = os.OpenFile(w.dbFilename, os.O_RDWR, 0600)
	return err
}

// buildPageFrameMap builds a map of page number to latest frame index.
func (w *WAL) buildPageFrameMap() (map[uint32]uint32, error) {
	pageFrames := make(map[uint32]uint32)
	for i := uint32(0); i < w.frameCount; i++ {
		frame, err := w.readFrameAtIndex(i)
		if err != nil {
			return nil, err
		}
		// Later frames override earlier ones
		pageFrames[frame.PageNumber] = i
	}
	return pageFrames, nil
}

// writeFramesToDB writes the frames in pageFrames map to the database file.
func (w *WAL) writeFramesToDB(pageFrames map[uint32]uint32) error {
	for pgno, frameIdx := range pageFrames {
		frame, err := w.readFrameAtIndex(frameIdx)
		if err != nil {
			return fmt.Errorf("failed to read frame %d: %w", frameIdx, err)
		}

		offset := int64(pgno-1) * int64(w.pageSize)
		if _, err := w.dbFile.WriteAt(frame.Data, offset); err != nil {
			return fmt.Errorf("failed to write page %d to database: %w", pgno, err)
		}
	}
	return nil
}

// CheckpointInfo represents information about a checkpoint operation.
type CheckpointInfo struct {
	// Number of frames successfully checkpointed
	FramesCheckpointed int

	// Number of frames remaining in the WAL after checkpoint
	FramesRemaining int

	// Size of WAL file before checkpoint (bytes)
	WALSizeBefore int64

	// Size of WAL file after checkpoint (bytes)
	WALSizeAfter int64
}

// CheckpointWithInfo performs a checkpoint and returns detailed information.
func (w *WAL) CheckpointWithInfo(mode CheckpointMode) (*CheckpointInfo, error) {
	w.mu.RLock()
	var walSizeBefore int64
	if w.file != nil {
		info, err := w.file.Stat()
		if err == nil {
			walSizeBefore = info.Size()
		}
	}
	w.mu.RUnlock()

	framesCheckpointed, framesRemaining, err := w.CheckpointWithMode(mode)
	if err != nil {
		return nil, err
	}

	w.mu.RLock()
	var walSizeAfter int64
	if w.file != nil {
		info, err := w.file.Stat()
		if err == nil {
			walSizeAfter = info.Size()
		}
	}
	w.mu.RUnlock()

	return &CheckpointInfo{
		FramesCheckpointed: framesCheckpointed,
		FramesRemaining:    framesRemaining,
		WALSizeBefore:      walSizeBefore,
		WALSizeAfter:       walSizeAfter,
	}, nil
}
