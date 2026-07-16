package volume

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRestoreClaimReturnsClaimWhenOrigGone verifies the normal path: when no
// newer staging file has appeared, a claimed file is renamed back to its
// original path so a later scan can pick it up again.
func TestRestoreClaimReturnsClaimWhenOrigGone(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "42")
	claim := orig + ".packing"

	if err := os.WriteFile(claim, []byte("claimed data"), 0o644); err != nil {
		t.Fatalf("write claim: %v", err)
	}

	restoreClaim(claim, orig)

	got, err := os.ReadFile(orig)
	if err != nil {
		t.Fatalf("read restored staging file: %v", err)
	}
	if string(got) != "claimed data" {
		t.Errorf("restored content = %q, want %q", got, "claimed data")
	}
	if _, err := os.Stat(claim); !os.IsNotExist(err) {
		t.Errorf("claim file still present after restore (err=%v)", err)
	}
}

// TestRestoreClaimDropsStaleClaimWhenNewerStagingExists is the regression test
// for the silent lost write: a concurrent overwrite Flush stages fresh data at
// the original path while the builder holds a claim on the old data. Restoring
// the claim with a bare os.Rename would clobber the newer file while the DB
// advertises the new mtime/size. The stale claim must be dropped instead.
func TestRestoreClaimDropsStaleClaimWhenNewerStagingExists(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "42")
	claim := orig + ".packing"

	if err := os.WriteFile(claim, []byte("stale claimed data"), 0o644); err != nil {
		t.Fatalf("write claim: %v", err)
	}
	// A concurrent Flush stages newer data at the original path.
	if err := os.WriteFile(orig, []byte("newer data"), 0o644); err != nil {
		t.Fatalf("write newer staging file: %v", err)
	}

	restoreClaim(claim, orig)

	got, err := os.ReadFile(orig)
	if err != nil {
		t.Fatalf("read staging file: %v", err)
	}
	if string(got) != "newer data" {
		t.Errorf("newer staging file was clobbered: content = %q, want %q", got, "newer data")
	}
	if _, err := os.Stat(claim); !os.IsNotExist(err) {
		t.Errorf("stale claim not dropped (err=%v)", err)
	}
}

// TestRestoreClaimMissingClaimIsNoop verifies restoreClaim tolerates an already
// removed claim (e.g. a concurrent cleanup) without creating anything.
func TestRestoreClaimMissingClaimIsNoop(t *testing.T) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "42")

	restoreClaim(orig+".packing", orig)

	if _, err := os.Stat(orig); !os.IsNotExist(err) {
		t.Errorf("orig unexpectedly created (err=%v)", err)
	}
}
