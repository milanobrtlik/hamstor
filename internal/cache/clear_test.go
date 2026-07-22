package cache

import (
	"os"
	"path/filepath"
	"testing"
)

// TestClearReportsRemovalFailure pins the bug that made `hamstor cache clear`
// print "cache cleared" over a cache it had not touched.
//
// The failure is not exotic: the daemon runs from systemd as root and creates
// --cache-dir as root, so the CLI run by a user cannot unlink anything under it.
// Clear discarded RemoveAll's error and returned nil, so the command reported
// success and the next cold-read measurement silently ran against warm data.
func TestClearReportsRemovalFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not stop RemoveAll")
	}

	dir := t.TempDir()
	c, err := New(dir, 1<<20)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := c.Put("ab/abcdef", []byte("payload")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Make the shard directory non-writable so the entry inside it cannot be
	// unlinked. Restored via t.Cleanup so TempDir teardown still works.
	shard := filepath.Join(dir, "ab")
	if err := os.Chmod(shard, 0o500); err != nil {
		t.Fatalf("chmod shard: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shard, 0o700) })

	if err := c.Clear(); err == nil {
		t.Fatal("Clear reported success while the entry it could not remove is still there")
	}

	if _, err := os.Stat(filepath.Join(shard, "abcdef")); err != nil {
		t.Fatalf("entry vanished after a failed Clear, so this test proves nothing: %v", err)
	}

	// A cache that still holds data must not advertise itself as empty: eviction
	// would then believe it has the whole budget free.
	if total, count := c.Size(); total == 0 || count == 0 {
		t.Errorf("Size() = (%d bytes, %d entries) after a failed Clear, want the surviving entry counted", total, count)
	}
}

// TestClearEmptiesCacheWhenPermitted is the positive half: with the permissions
// it normally has, Clear must actually empty the directory and zero the
// accounting. Without this, the test above would pass against a Clear that
// always failed.
func TestClearEmptiesCacheWhenPermitted(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<20)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	for _, key := range []string{"ab/one", "cd/two", "ef/three"} {
		if err := c.Put(key, []byte("payload")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	if err := c.Clear(); err != nil {
		t.Fatalf("clear: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("cache dir still holds %d entries after Clear", len(entries))
	}
	if total, count := c.Size(); total != 0 || count != 0 {
		t.Errorf("Size() = (%d bytes, %d entries) after a successful Clear, want zero", total, count)
	}
}
