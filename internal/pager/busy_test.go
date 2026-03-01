package pager

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDefaultBusyHandler tests the default busy handler with exponential backoff
func TestDefaultBusyHandler(t *testing.T) {
	t.Parallel()
	t.Run("respects timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		handler := NewDefaultBusyHandler(timeout)

		start := time.Now()
		count := 0
		for handler.Busy(count) {
			count++
			// Safety limit to prevent infinite loop in test
			if count > 1000 {
				t.Fatal("Handler did not respect timeout")
			}
		}
		elapsed := time.Since(start)

		// Should have taken approximately the timeout duration
		// Allow 50ms tolerance for timing variance
		if elapsed < timeout {
			t.Errorf("Handler returned too early: %v < %v", elapsed, timeout)
		}
		if elapsed > timeout+50*time.Millisecond {
			t.Errorf("Handler took too long: %v > %v", elapsed, timeout+50*time.Millisecond)
		}

		if count < 2 {
			t.Errorf("Handler should have retried at least twice, got %d", count)
		}
	})

	t.Run("exponential backoff increases delay", func(t *testing.T) {
		handler := NewDefaultBusyHandler(10 * time.Second)

		// First few calls should have increasing delays
		delays := []time.Duration{}
		for i := 0; i < 5; i++ {
			start := time.Now()
			if !handler.Busy(i) {
				t.Fatal("Handler returned false too early")
			}
			delays = append(delays, time.Since(start))
		}

		// Each delay should generally be larger than the previous (accounting for jitter)
		// Just check that we have some variety in delays
		allSame := true
		for i := 1; i < len(delays); i++ {
			if delays[i] != delays[0] {
				allSame = false
				break
			}
		}
		if allSame {
			t.Error("All delays were identical, expected exponential backoff")
		}
	})

	t.Run("caps delay at maximum", func(t *testing.T) {
		handler := NewDefaultBusyHandler(1 * time.Second)

		// After many retries, delay should cap at maxDelay (100ms)
		start := time.Now()
		// Count 20 should definitely hit the cap
		if !handler.Busy(20) {
			t.Fatal("Handler returned false unexpectedly")
		}
		elapsed := time.Since(start)

		// Should be close to maxDelay (100ms), allowing for jitter
		if elapsed > 150*time.Millisecond {
			t.Errorf("Delay exceeded maximum: %v > 150ms", elapsed)
		}
	})

	t.Run("reset clears state", func(t *testing.T) {
		handler := NewDefaultBusyHandler(50 * time.Millisecond)

		// Use up most of the timeout
		for i := 0; i < 3; i++ {
			handler.Busy(i)
		}

		// Reset
		handler.Reset()

		// Should now have full timeout available again
		start := time.Now()
		count := 0
		for handler.Busy(count) {
			count++
			if count > 100 {
				break
			}
		}
		elapsed := time.Since(start)

		// Should take approximately the full timeout again
		if elapsed < 40*time.Millisecond {
			t.Errorf("After reset, handler timeout too short: %v", elapsed)
		}
	})
}

// TestBusyTimeout tests the timeout-based busy handler
func TestBusyTimeout(t *testing.T) {
	t.Parallel()
	t.Run("retries until timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		handler := BusyTimeout(timeout)

		start := time.Now()
		count := 0
		for handler.Busy(count) {
			count++
			if count > 1000 {
				t.Fatal("Handler did not respect timeout")
			}
		}
		elapsed := time.Since(start)

		// Should have taken approximately the timeout duration
		if elapsed < timeout {
			t.Errorf("Handler returned too early: %v < %v", elapsed, timeout)
		}
		if elapsed > timeout+50*time.Millisecond {
			t.Errorf("Handler took too long: %v > %v", elapsed, timeout+50*time.Millisecond)
		}

		// Should have retried multiple times (100ms / 10ms = ~10 times)
		if count < 5 {
			t.Errorf("Expected at least 5 retries, got %d", count)
		}
	})

	t.Run("tracks total retries", func(t *testing.T) {
		handler := BusyTimeout(50 * time.Millisecond)
		th := handler.(*TimeoutBusyHandler)

		count := 0
		for handler.Busy(count) {
			count++
		}

		totalRetries := th.GetTotalRetries()
		if totalRetries != count {
			t.Errorf("Expected %d total retries, got %d", count, totalRetries)
		}
	})

	t.Run("zero timeout returns immediately", func(t *testing.T) {
		handler := BusyTimeout(0)

		start := time.Now()
		result := handler.Busy(0)
		elapsed := time.Since(start)

		if result {
			t.Error("Expected immediate failure with zero timeout")
		}
		if elapsed > 10*time.Millisecond {
			t.Errorf("Zero timeout took too long: %v", elapsed)
		}
	})
}

// TestBusyCallback tests the callback-based busy handler
func TestBusyCallback(t *testing.T) {
	t.Parallel()
	t.Run("invokes callback with count", func(t *testing.T) {
		callCount := 0
		maxRetries := 5

		handler := BusyCallback(func(count int) bool {
			if count != callCount {
				t.Errorf("Expected count %d, got %d", callCount, count)
			}
			callCount++
			return count < maxRetries
		})

		count := 0
		for handler.Busy(count) {
			count++
		}

		if count != maxRetries {
			t.Errorf("Expected %d retries, got %d", maxRetries, count)
		}
	})

	t.Run("nil callback returns false", func(t *testing.T) {
		handler := &CallbackBusyHandler{callback: nil}

		result := handler.Busy(0)
		if result {
			t.Error("Expected false for nil callback")
		}
	})

	t.Run("custom retry logic", func(t *testing.T) {
		retries := 0
		maxRetries := 3
		delay := 10 * time.Millisecond

		handler := BusyCallback(func(count int) bool {
			if count >= maxRetries {
				return false
			}
			retries++
			time.Sleep(delay)
			return true
		})

		start := time.Now()
		count := 0
		for handler.Busy(count) {
			count++
		}
		elapsed := time.Since(start)

		if retries != maxRetries {
			t.Errorf("Expected %d retries, got %d", maxRetries, retries)
		}

		// Should have slept for delay * maxRetries
		expectedTime := delay * time.Duration(maxRetries)
		if elapsed < expectedTime {
			t.Errorf("Expected at least %v, got %v", expectedTime, elapsed)
		}
	})
}

// TestNoBusyHandler tests the no-retry busy handler
func TestNoBusyHandler(t *testing.T) {
	t.Parallel()
	handler := &NoBusyHandler{}

	// Should always return false
	if handler.Busy(0) {
		t.Error("NoBusyHandler should always return false")
	}
	if handler.Busy(100) {
		t.Error("NoBusyHandler should always return false")
	}
}

// TestPagerBusyHandler tests busy handler integration with pager
func TestPagerBusyHandler(t *testing.T) {
	t.Parallel()
	t.Run("set and get busy handler", func(t *testing.T) {
		pager := newPager("test.db", DefaultPageSize, false)
		defer func() {
			if pager.file != nil {
				pager.Close()
			}
		}()

		// Initially no handler
		if pager.GetBusyHandler() != nil {
			t.Error("Expected no busy handler initially")
		}

		// Set a handler
		handler := NewDefaultBusyHandler(5 * time.Second)
		pager.WithBusyHandler(handler)

		// Get it back
		retrieved := pager.GetBusyHandler()
		if retrieved != handler {
			t.Error("Retrieved handler does not match set handler")
		}

		// Set to nil
		pager.WithBusyHandler(nil)
		if pager.GetBusyHandler() != nil {
			t.Error("Expected nil after setting to nil")
		}
	})

	t.Run("busy handler is invoked on lock contention", func(t *testing.T) {
		// This test verifies that the busy handler is called
		// when a lock cannot be acquired
		pager := newPager("test.db", DefaultPageSize, false)
		defer func() {
			if pager.file != nil {
				pager.Close()
			}
		}()

		invoked := atomic.Int32{}
		handler := BusyCallback(func(count int) bool {
			invoked.Add(1)
			// Fail after first call for quick test
			return false
		})

		pager.WithBusyHandler(handler)

		// Manually simulate lock contention by calling invokeBusyHandler
		result := pager.invokeBusyHandler(0)
		if result {
			t.Error("Expected handler to return false")
		}
		if invoked.Load() != 1 {
			t.Errorf("Expected handler to be invoked once, got %d", invoked.Load())
		}
	})

	t.Run("no handler means immediate failure", func(t *testing.T) {
		pager := newPager("test.db", DefaultPageSize, false)
		defer func() {
			if pager.file != nil {
				pager.Close()
			}
		}()

		// No handler set
		result := pager.invokeBusyHandler(0)
		if result {
			t.Error("Expected false when no handler is set")
		}
	})
}

// TestConcurrentLockAcquisition tests busy handler behavior with concurrent access
func TestConcurrentLockAcquisition(t *testing.T) {
	t.Parallel()
	t.Run("multiple goroutines with busy handler", func(t *testing.T) {
		// This is a simulation test since we don't have real file locking yet
		// We test that the busy handler mechanism works correctly
		successCount := atomic.Int32{}
		failCount := atomic.Int32{}

		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				handler := BusyTimeout(50 * time.Millisecond)
				count := 0
				retried := false

				// Simulate trying to acquire a lock
				for {
					// Randomly succeed or fail to simulate contention
					if count > 0 {
						retried = true
					}

					// Simulate success after a few retries
					if count >= 2 {
						successCount.Add(1)
						break
					}

					if !handler.Busy(count) {
						failCount.Add(1)
						break
					}
					count++
				}

				if retried && count >= 2 {
					// Successfully retried
					t.Logf("Goroutine %d succeeded after %d retries", id, count)
				}
			}(i)
		}

		wg.Wait()

		total := successCount.Load() + failCount.Load()
		if total != int32(numGoroutines) {
			t.Errorf("Expected %d total operations, got %d", numGoroutines, total)
		}

		t.Logf("Success: %d, Failed: %d", successCount.Load(), failCount.Load())
	})
}

// TestBusyHandlerEdgeCases tests edge cases and boundary conditions
func TestBusyHandlerEdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("very short timeout", func(t *testing.T) {
		handler := NewDefaultBusyHandler(1 * time.Nanosecond)

		// Should return false almost immediately
		result := handler.Busy(0)
		if result {
			t.Error("Expected immediate failure with nanosecond timeout")
		}
	})

	t.Run("very long timeout", func(t *testing.T) {
		handler := NewDefaultBusyHandler(1 * time.Hour)

		// Should be able to retry many times
		count := 0
		for i := 0; i < 100; i++ {
			if !handler.Busy(i) {
				break
			}
			count++
		}

		if count < 50 {
			t.Errorf("Expected many retries with long timeout, got %d", count)
		}
	})

	t.Run("negative timeout treated as zero", func(t *testing.T) {
		// The handler should handle negative timeouts gracefully
		handler := BusyTimeout(-1 * time.Second)

		result := handler.Busy(0)
		if result {
			t.Error("Expected immediate failure with negative timeout")
		}
	})

	t.Run("callback panic is not caught", func(t *testing.T) {
		handler := BusyCallback(func(count int) bool {
			if count == 0 {
				panic("test panic")
			}
			return false
		})

		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic to propagate")
			}
		}()

		handler.Busy(0)
	})
}

// TestBusyHandlerTiming tests precise timing behavior
func TestBusyHandlerTiming(t *testing.T) {
	t.Parallel()
	t.Run("default handler minimum delay", func(t *testing.T) {
		handler := NewDefaultBusyHandler(1 * time.Second)

		// First call should sleep for approximately minDelay (1ms)
		start := time.Now()
		result := handler.Busy(0)
		elapsed := time.Since(start)

		if !result {
			t.Fatal("Handler should not timeout on first call")
		}

		// Should be close to 1ms (allow variance for jitter)
		if elapsed < 500*time.Microsecond || elapsed > 5*time.Millisecond {
			t.Logf("First delay: %v (expected ~1ms with jitter)", elapsed)
		}
	})

	t.Run("timeout handler fixed delay", func(t *testing.T) {
		handler := BusyTimeout(1 * time.Second)

		// Each call should sleep for approximately 10ms
		start := time.Now()
		result := handler.Busy(0)
		elapsed := time.Since(start)

		if !result {
			t.Fatal("Handler should not timeout on first call")
		}

		// Should be close to 10ms
		if elapsed < 8*time.Millisecond || elapsed > 15*time.Millisecond {
			t.Logf("Delay: %v (expected ~10ms)", elapsed)
		}
	})
}

// TestBusyHandlerThreadSafety tests thread safety of busy handlers
func TestBusyHandlerThreadSafety(t *testing.T) {
	t.Parallel()
	t.Run("concurrent access to same handler", func(t *testing.T) {
		handler := NewDefaultBusyHandler(100 * time.Millisecond)

		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Each goroutine tries to use the handler
				// This tests for race conditions
				for j := 0; j < 5; j++ {
					handler.Busy(j)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent set and get on pager", func(t *testing.T) {
		pager := newPager("test.db", DefaultPageSize, false)
		defer func() {
			if pager.file != nil {
				pager.Close()
			}
		}()

		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Set handler
				handler := NewDefaultBusyHandler(time.Duration(id+1) * time.Millisecond)
				pager.WithBusyHandler(handler)

				// Get handler
				_ = pager.GetBusyHandler()

				// Invoke handler
				pager.invokeBusyHandler(0)
			}(i)
		}

		wg.Wait()
	})
}

// BenchmarkDefaultBusyHandler benchmarks the default busy handler
func BenchmarkDefaultBusyHandler(b *testing.B) {
	handler := NewDefaultBusyHandler(1 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.Busy(i % 10) // Cycle through retry counts
	}
}

// BenchmarkBusyTimeout benchmarks the timeout busy handler
func BenchmarkBusyTimeout(b *testing.B) {
	handler := BusyTimeout(1 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.Busy(i % 10)
	}
}

// BenchmarkBusyCallback benchmarks the callback busy handler
func BenchmarkBusyCallback(b *testing.B) {
	handler := BusyCallback(func(count int) bool {
		time.Sleep(1 * time.Millisecond)
		return count < 5
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.Busy(i % 10)
	}
}

// TestAcquireSharedLockWithRetry tests shared lock acquisition with retry
func TestAcquireSharedLockWithRetry(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_retry_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Open pager with busy handler
	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	handler := NewDefaultBusyHandler(100 * time.Millisecond)
	pager.WithBusyHandler(handler)

	// Test successful acquisition - this will exercise the retry path
	err = pager.acquireSharedLockWithRetry()
	if err != nil {
		t.Errorf("failed to acquire shared lock: %v", err)
	}

	// Verify lock state
	if pager.lockState < LockShared {
		t.Error("lock state should be at least shared")
	}
}

// TestAcquireReservedLockWithRetry tests reserved lock acquisition with retry
func TestAcquireReservedLockWithRetry(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_retry_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	handler := NewDefaultBusyHandler(100 * time.Millisecond)
	pager.WithBusyHandler(handler)

	// First acquire shared lock
	if err := pager.acquireSharedLockWithRetry(); err != nil {
		t.Fatalf("failed to acquire shared lock: %v", err)
	}

	// Then acquire reserved lock
	err = pager.acquireReservedLockWithRetry()
	if err != nil {
		t.Errorf("failed to acquire reserved lock: %v", err)
	}

	// Verify lock state
	if pager.lockState < LockReserved {
		t.Error("lock state should be at least reserved")
	}
}

// TestAcquireExclusiveLockWithRetry tests exclusive lock acquisition with retry
func TestAcquireExclusiveLockWithRetry(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_retry_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	handler := NewDefaultBusyHandler(100 * time.Millisecond)
	pager.WithBusyHandler(handler)

	// First acquire shared lock
	if err := pager.acquireSharedLockWithRetry(); err != nil {
		t.Fatalf("failed to acquire shared lock: %v", err)
	}

	// Then reserved lock
	if err := pager.acquireReservedLockWithRetry(); err != nil {
		t.Fatalf("failed to acquire reserved lock: %v", err)
	}

	// Then exclusive lock
	err = pager.acquireExclusiveLockWithRetry()
	if err != nil {
		t.Errorf("failed to acquire exclusive lock: %v", err)
	}

	// Verify lock state
	if pager.lockState < LockExclusive {
		t.Error("lock state should be exclusive")
	}
}

// TestTryAcquireSharedLock tests tryAcquireSharedLock
func TestTryAcquireSharedLock(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Try to acquire shared lock
	err = pager.tryAcquireSharedLock()
	if err != nil {
		t.Errorf("failed to try acquire shared lock: %v", err)
	}

	if pager.lockState < LockShared {
		t.Error("should have acquired shared lock")
	}
}

// TestTryAcquireReservedLock tests tryAcquireReservedLock
func TestTryAcquireReservedLock(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// First need a write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire reserved lock
	err = pager.tryAcquireReservedLock()
	if err != nil {
		t.Errorf("failed to try acquire reserved lock: %v", err)
	}

	if pager.lockState < LockReserved {
		t.Error("should have acquired reserved lock")
	}
}

// TestTryAcquireExclusiveLock tests tryAcquireExclusiveLock
func TestTryAcquireExclusiveLock(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Need write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire exclusive lock
	err = pager.tryAcquireExclusiveLock()
	if err != nil {
		t.Errorf("failed to try acquire exclusive lock: %v", err)
	}

	if pager.lockState < LockExclusive {
		t.Error("should have acquired exclusive lock")
	}
}
