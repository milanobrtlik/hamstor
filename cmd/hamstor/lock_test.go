package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestAcquireDBLockContention is the one failure that really means a second
// hamstor is running. flock is held per open file description, so a second
// OpenFile in this same process contends exactly as another process would.
func TestAcquireDBLockContention(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hamstor.db")

	first, err := acquireDBLock(dbPath)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Close()

	_, err = acquireDBLock(dbPath)
	if !errors.Is(err, errLockHeld) {
		t.Fatalf("second acquire returned %v, want errLockHeld", err)
	}
}

// TestAcquireDBLockUnopenableIsNotContention pins the misreport: main used to
// answer every acquireDBLock failure with "another instance is using <db>",
// including the common one where the lock file simply cannot be opened —
// systemd runs the daemon as root, so /var/lib/hamstor belongs to root and a
// user running `hamstor gc` gets EACCES. That message sends someone hunting for
// a process that does not exist.
func TestAcquireDBLockUnopenableIsNotContention(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "no-such-dir", "hamstor.db")

	f, err := acquireDBLock(dbPath)
	if err == nil {
		f.Close()
		t.Fatal("acquiring a lock under a missing directory succeeded")
	}
	if errors.Is(err, errLockHeld) {
		t.Fatal("an unopenable lock file was reported as contention with another instance")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error = %v, want it to carry the underlying cause", err)
	}
}

// TestAcquireDBLockReleasedOnClose guards the property the daemon depends on:
// the lock lasts exactly as long as the returned file is open.
func TestAcquireDBLockReleasedOnClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hamstor.db")

	first, err := acquireDBLock(dbPath)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	first.Close()

	second, err := acquireDBLock(dbPath)
	if err != nil {
		t.Fatalf("acquire after close: %v", err)
	}
	second.Close()
}
