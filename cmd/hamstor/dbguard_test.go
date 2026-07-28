package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestDBMustExist pins which subcommands refuse to be handed a fresh database.
// The list is the whole guard: a subcommand added to main without being
// considered here defaults to "may create one", which for anything that deletes
// S3 objects means it may delete all of them.
func TestDBMustExist(t *testing.T) {
	tests := []struct {
		subcmd string
		want   bool
		why    string
	}{
		{"gc", true, "an empty database makes every object in the bucket an orphan"},
		{"compact", true, "reads a filesystem that must already exist"},
		{"fsck", true, "would report status: OK about a database it just created"},
		{"purge-s3", false, "wiping the bucket after losing the database is a real workflow, and it prompts"},
		{"restore", false, "creating the database is its whole job"},
		{"", false, "mount mode bootstraps a new filesystem on first run"},
		{"cache", false, "never opens the database"},
		{"version", false, "never opens the database"},
	}
	for _, tt := range tests {
		if got := dbMustExist(tt.subcmd); got != tt.want {
			t.Errorf("dbMustExist(%q) = %v, want %v: %s", tt.subcmd, got, tt.want, tt.why)
		}
	}
}

// TestOpenDBPathMissingLeavesNothingBehind is the ordering assertion, and the
// reason openDBPath exists as a function at all. Checking existence after
// MkdirAll and acquireDBLock would still refuse the run, but it would leave a
// data/ directory and a hamstor.db.lock in whatever directory the caller
// happened to be standing in — which is exactly the litter a wrong-working
// -directory run leaves today, and it makes the next such run look plausible.
func TestOpenDBPathMissingLeavesNothingBehind(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "data", "hamstor.db")

	f, err := openDBPath(dbPath, true, true)
	if err == nil {
		f.Close()
		t.Fatal("openDBPath created a database that did not exist")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error = %v, want it to carry fs.ErrNotExist", err)
	}

	if _, statErr := os.Stat(filepath.Join(root, "data")); !errors.Is(statErr, fs.ErrNotExist) {
		t.Error("the db directory was created before the existence check")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("openDBPath left %s behind", e.Name())
	}
}

// TestOpenDBPathExistingTakesLock: the guard must not cost the lock. A refused
// run that still locked, or an allowed run that did not, both break the
// single-instance guarantee gc depends on.
func TestOpenDBPathExistingTakesLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hamstor.db")
	if err := os.WriteFile(dbPath, nil, 0o600); err != nil {
		t.Fatalf("create db: %v", err)
	}

	f, err := openDBPath(dbPath, true, true)
	if err != nil {
		t.Fatalf("openDBPath on an existing database: %v", err)
	}
	if _, err := acquireDBLock(dbPath); !errors.Is(err, errLockHeld) {
		f.Close()
		t.Fatalf("second acquire returned %v, want errLockHeld — openDBPath did not hold the lock", err)
	}
	f.Close()
}

// TestOpenDBPathReadOnlySkipsLock: a read-only subcommand must be runnable
// against a live mount, which means not taking the exclusive lock the daemon
// holds. It still has to check existence — `hamstor thumbs sync` on a typo'd
// --db must report the typo, not create an empty database and report an empty
// library.
func TestOpenDBPathReadOnlySkipsLock(t *testing.T) {
	if !readOnlySubcmd("thumbs") {
		t.Fatal("thumbs is no longer read-only; this test guards the wrong thing")
	}

	dbPath := filepath.Join(t.TempDir(), "hamstor.db")
	if err := os.WriteFile(dbPath, nil, 0o600); err != nil {
		t.Fatalf("create db: %v", err)
	}

	// Stand in for the running daemon.
	held, err := acquireDBLock(dbPath)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	defer held.Close()

	f, err := openDBPath(dbPath, true, false)
	if err != nil {
		t.Fatalf("read-only openDBPath while the lock is held: %v", err)
	}
	if f != nil {
		f.Close()
		t.Error("read-only openDBPath returned a lock file")
	}

	missing := filepath.Join(t.TempDir(), "nope.db")
	if _, err := openDBPath(missing, true, false); err == nil {
		t.Error("read-only openDBPath accepted a database that does not exist")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Error("read-only openDBPath created the database it was meant to refuse")
	}
}

// TestOpenDBPathCreatesForMount pins the other side: first boot and `hamstor
// restore` must still be able to bring a database into existence, directory and
// all. A guard that refused here would make the daemon unstartable on a fresh
// machine.
func TestOpenDBPathCreatesForMount(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "data", "hamstor.db")

	f, err := openDBPath(dbPath, false, true)
	if err != nil {
		t.Fatalf("openDBPath for mount mode: %v", err)
	}
	defer f.Close()

	if _, err := os.Stat(filepath.Join(root, "data")); err != nil {
		t.Errorf("db directory was not created: %v", err)
	}
}
