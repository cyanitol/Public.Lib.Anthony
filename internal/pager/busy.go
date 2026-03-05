// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"math/rand"
	"time"
)

/*
Busy Handler Implementation

This file implements SQLite-compatible busy handler functionality for handling
database lock contention gracefully. When a database operation cannot acquire
a necessary lock, the busy handler is invoked to determine whether to retry
or return an error.

Design

The busy handler interface provides a pluggable retry mechanism:

  - BusyHandler interface: Defines the contract for custom handlers
  - DefaultBusyHandler: Exponential backoff with jitter (recommended)
  - TimeoutBusyHandler: Simple timeout with fixed retry interval
  - CallbackBusyHandler: Wraps a custom callback function
  - NoBusyHandler: No retries, immediate failure

Lock Acquisition Flow

When a lock cannot be acquired:

 1. The pager calls invokeBusyHandler(count) where count is the retry attempt
 2. The busy handler decides whether to retry (returns true) or fail (returns false)
 3. If true, the handler typically sleeps before returning
 4. The pager retries the lock acquisition
 5. This repeats until the lock is acquired or the handler returns false

Default Behavior

The DefaultBusyHandler implements exponential backoff with jitter:

  - Initial delay: 1ms
  - Maximum delay: 100ms (capped)
  - Total timeout: Configurable (default 5s)
  - Delay formula: min(minDelay * 2^count, maxDelay) ± 25% jitter

This matches SQLite's default busy timeout behavior and provides good
performance under typical lock contention scenarios.

Usage Examples

Basic timeout-based handler:

	p, _ := pager.Open("mydb.db", false)
	p.WithBusyHandler(pager.BusyTimeout(5 * time.Second))

Exponential backoff (recommended):

	p, _ := pager.Open("mydb.db", false)
	handler := pager.NewDefaultBusyHandler(5 * time.Second)
	p.WithBusyHandler(handler)

Custom callback:

	handler := pager.BusyCallback(func(count int) bool {
		if count > 10 {
			return false  // Give up after 10 retries
		}
		time.Sleep(100 * time.Millisecond)
		return true
	})
	p.WithBusyHandler(handler)

Integration with Pager

The busy handler is automatically invoked during lock acquisition:

  - acquireSharedLock: When beginning a read transaction
  - acquireReservedLock: When beginning a write transaction
  - acquireExclusiveLock: When committing changes

This ensures transparent retry behavior without requiring changes to
higher-level code.
*/

// BusyHandler is an interface for handling database lock contention.
// When a database operation fails due to locking, the busy handler is invoked
// to decide whether to retry the operation or return an error.
//
// This is based on SQLite's sqlite3_busy_handler mechanism.
type BusyHandler interface {
	// Busy is called when a lock cannot be acquired.
	// count is the number of times this handler has been invoked for the current lock.
	// Returns true to retry the operation, false to return SQLITE_BUSY error.
	Busy(count int) bool
}

// DefaultBusyHandler implements exponential backoff with jitter for lock retries.
// This handler sleeps for progressively longer periods between retry attempts,
// similar to SQLite's default busy handler.
type DefaultBusyHandler struct {
	// timeout is the total time allowed for retry attempts
	timeout time.Duration

	// startTime tracks when the first retry began
	startTime time.Time

	// minDelay is the initial delay (default 1ms)
	minDelay time.Duration

	// maxDelay is the maximum delay between retries (default 100ms)
	maxDelay time.Duration

	// rng is used to add jitter to delays
	rng *rand.Rand
}

// NewDefaultBusyHandler creates a new busy handler with the specified timeout.
// The handler uses exponential backoff starting at 1ms, capping at 100ms,
// and will retry for up to the specified timeout duration.
func NewDefaultBusyHandler(timeout time.Duration) *DefaultBusyHandler {
	return &DefaultBusyHandler{
		timeout:  timeout,
		minDelay: 1 * time.Millisecond,
		maxDelay: 100 * time.Millisecond,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Busy implements the BusyHandler interface.
// It sleeps for an exponentially increasing duration with jitter,
// and returns false once the total timeout has been exceeded.
func (h *DefaultBusyHandler) Busy(count int) bool {
	h.initializeStartTime(count)

	elapsed := time.Since(h.startTime)
	if elapsed >= h.timeout {
		return false
	}

	delay := h.calculateDelayWithBackoff(count, elapsed)
	h.sleepWithDelay(delay)

	return true
}

// initializeStartTime initializes the start time on first call.
func (h *DefaultBusyHandler) initializeStartTime(count int) {
	if count == 0 || h.startTime.IsZero() {
		h.startTime = time.Now()
	}
}

// calculateDelayWithBackoff calculates the delay with exponential backoff and jitter.
func (h *DefaultBusyHandler) calculateDelayWithBackoff(count int, elapsed time.Duration) time.Duration {
	delay := h.calculateExponentialDelay(count)
	delay = h.addJitter(delay)
	delay = h.capDelayToRemaining(delay, elapsed)
	return delay
}

// calculateExponentialDelay calculates exponential backoff delay.
func (h *DefaultBusyHandler) calculateExponentialDelay(count int) time.Duration {
	delay := h.minDelay
	for i := 0; i < count && delay < h.maxDelay; i++ {
		delay *= 2
	}
	if delay > h.maxDelay {
		delay = h.maxDelay
	}
	return delay
}

// addJitter adds +/- 25% jitter to the delay.
func (h *DefaultBusyHandler) addJitter(delay time.Duration) time.Duration {
	jitter := time.Duration(h.rng.Int63n(int64(delay / 2)))
	return delay - delay/4 + jitter
}

// capDelayToRemaining ensures delay doesn't exceed remaining timeout.
func (h *DefaultBusyHandler) capDelayToRemaining(delay, elapsed time.Duration) time.Duration {
	remaining := h.timeout - elapsed
	if delay > remaining {
		return remaining
	}
	return delay
}

// sleepWithDelay sleeps for the specified duration if positive.
func (h *DefaultBusyHandler) sleepWithDelay(delay time.Duration) {
	if delay > 0 {
		time.Sleep(delay)
	}
}

// Reset resets the busy handler state, clearing the start time.
// This is called when a lock is successfully acquired or the operation completes.
func (h *DefaultBusyHandler) Reset() {
	h.startTime = time.Time{}
}

// CallbackBusyHandler wraps a callback function as a BusyHandler.
// This allows custom retry logic to be provided as a simple function.
type CallbackBusyHandler struct {
	callback func(count int) bool
}

// BusyCallback creates a BusyHandler from a callback function.
// The callback receives the retry count and returns true to retry,
// false to abort and return an error.
//
// Example:
//
//	handler := BusyCallback(func(count int) bool {
//	    if count > 10 {
//	        return false  // Give up after 10 retries
//	    }
//	    time.Sleep(100 * time.Millisecond)
//	    return true  // Retry
//	})
func BusyCallback(callback func(count int) bool) BusyHandler {
	return &CallbackBusyHandler{callback: callback}
}

// Busy implements the BusyHandler interface by delegating to the callback.
func (h *CallbackBusyHandler) Busy(count int) bool {
	if h.callback == nil {
		return false
	}
	return h.callback(count)
}

// TimeoutBusyHandler is a simple busy handler that retries for a fixed duration.
// It sleeps for a fixed interval between retries until the timeout is reached.
type TimeoutBusyHandler struct {
	timeout      time.Duration
	retryDelay   time.Duration
	startTime    time.Time
	totalRetries int
}

// BusyTimeout creates a timeout-based busy handler.
// The handler will retry the operation for up to the specified duration,
// sleeping for 10ms between each retry attempt.
//
// This is similar to SQLite's sqlite3_busy_timeout() function.
func BusyTimeout(timeout time.Duration) BusyHandler {
	return &TimeoutBusyHandler{
		timeout:    timeout,
		retryDelay: 10 * time.Millisecond,
	}
}

// Busy implements the BusyHandler interface.
// It sleeps for a fixed interval and returns false once timeout is exceeded.
func (h *TimeoutBusyHandler) Busy(count int) bool {
	// Initialize start time on first call or if it's been reset
	if count == 0 || h.startTime.IsZero() {
		h.startTime = time.Now()
		h.totalRetries = 0
	}

	// Check if we've exceeded the timeout
	elapsed := time.Since(h.startTime)
	if elapsed >= h.timeout {
		return false
	}

	// Increment retries only when we're actually going to retry
	h.totalRetries++

	// Calculate remaining time
	remaining := h.timeout - elapsed

	// Sleep for retry delay or remaining time, whichever is shorter
	sleepDuration := h.retryDelay
	if sleepDuration > remaining {
		sleepDuration = remaining
	}

	if sleepDuration > 0 {
		time.Sleep(sleepDuration)
	}

	return true
}

// GetTotalRetries returns the total number of retries attempted.
func (h *TimeoutBusyHandler) GetTotalRetries() int {
	return h.totalRetries
}

// NoBusyHandler is a busy handler that never retries.
// Any lock contention immediately returns an error.
type NoBusyHandler struct{}

// Busy always returns false, causing immediate failure on lock contention.
func (h *NoBusyHandler) Busy(count int) bool {
	return false
}

// WithBusyHandler sets the busy handler for the pager.
// The busy handler is invoked when a lock cannot be acquired,
// allowing custom retry logic or timeout behavior.
//
// Pass nil to disable the busy handler (locks will fail immediately).
func (p *Pager) WithBusyHandler(handler BusyHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.busyHandler = handler
}

// GetBusyHandler returns the current busy handler, or nil if none is set.
func (p *Pager) GetBusyHandler() BusyHandler {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.busyHandler
}

// invokeBusyHandler invokes the busy handler if one is set.
// Returns true if the operation should be retried, false otherwise.
func (p *Pager) invokeBusyHandler(count int) bool {
	if p.busyHandler == nil {
		return false
	}
	return p.busyHandler.Busy(count)
}

// acquireSharedLockWithRetry attempts to acquire a shared lock with busy handler support.
// This is an enhanced version of acquireSharedLock that retries using the busy handler.
func (p *Pager) acquireSharedLockWithRetry() error {
	retryCount := 0
	for {
		// Try to acquire the lock
		err := p.tryAcquireSharedLock()
		if err == nil {
			return nil
		}

		// If the error is not a lock error, return it immediately
		if err != ErrDatabaseLocked {
			return err
		}

		// Invoke busy handler
		if !p.invokeBusyHandler(retryCount) {
			return ErrDatabaseLocked
		}

		retryCount++
	}
}

// tryAcquireSharedLock attempts to acquire a shared lock without retrying.
// Returns ErrDatabaseLocked if the lock cannot be acquired.
func (p *Pager) tryAcquireSharedLock() error {
	if p.lockState >= LockShared {
		return nil
	}

	// In a real implementation, this would use file locking (flock/fcntl)
	// For simplicity, we just update the state
	// TODO: Implement actual file locking that can fail with EWOULDBLOCK
	p.lockState = LockShared
	p.state = PagerStateReader

	return nil
}

// acquireReservedLockWithRetry attempts to acquire a reserved lock with busy handler support.
func (p *Pager) acquireReservedLockWithRetry() error {
	retryCount := 0
	for {
		err := p.tryAcquireReservedLock()
		if err == nil {
			return nil
		}

		if err != ErrDatabaseLocked {
			return err
		}

		if !p.invokeBusyHandler(retryCount) {
			return ErrDatabaseLocked
		}

		retryCount++
	}
}

// tryAcquireReservedLock attempts to acquire a reserved lock without retrying.
func (p *Pager) tryAcquireReservedLock() error {
	if p.readOnly {
		return ErrReadOnly
	}

	if p.lockState >= LockReserved {
		return nil
	}

	// In a real implementation, this would use file locking
	// TODO: Implement actual file locking
	p.lockState = LockReserved

	return nil
}

// acquireExclusiveLockWithRetry attempts to acquire an exclusive lock with busy handler support.
func (p *Pager) acquireExclusiveLockWithRetry() error {
	retryCount := 0
	for {
		err := p.tryAcquireExclusiveLock()
		if err == nil {
			return nil
		}

		if err != ErrDatabaseLocked {
			return err
		}

		if !p.invokeBusyHandler(retryCount) {
			return ErrDatabaseLocked
		}

		retryCount++
	}
}

// tryAcquireExclusiveLock attempts to acquire an exclusive lock without retrying.
func (p *Pager) tryAcquireExclusiveLock() error {
	if p.lockState >= LockExclusive {
		return nil
	}

	// In a real implementation, this would use file locking
	// TODO: Implement actual file locking
	p.lockState = LockExclusive

	return nil
}
