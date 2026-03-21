// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !(unix || linux || darwin || freebsd || openbsd || netbsd || windows)

package pager

// Fallback lock implementation for platforms without file locking (e.g. WASM).
// Uses in-process mutual exclusion only — no cross-process locking.

// initPlatform is a no-op on unsupported platforms.
func (lm *LockManager) initPlatform() error {
	return nil
}

// acquireLockPlatform is a no-op on unsupported platforms.
// Thread safety is handled by the LockManager's own mutex.
func (lm *LockManager) acquireLockPlatform(level LockLevel) error {
	return nil
}

// releaseLockPlatform is a no-op on unsupported platforms.
func (lm *LockManager) releaseLockPlatform(level LockLevel) error {
	return nil
}

// cleanupPlatform is a no-op on unsupported platforms.
func (lm *LockManager) cleanupPlatform() error {
	return nil
}
