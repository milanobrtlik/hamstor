package hfuse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

// brokenStore returns a store aimed at a bucket that does not exist, so uploads
// fail the way an S3 outage does while the rest of the process keeps working.
func brokenStore(t *testing.T) *s3store.Store {
	t.Helper()
	cfg := testutil.RequireS3(t)
	store, err := s3store.New(context.Background(), "hamstor-no-such-bucket", cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create broken store: %v", err)
	}
	return store
}

// TestCleanupKeepsPendingWithRetainedData is the regression test for silent data
// loss on a failed upload: Cleanup must not delete a pending inode whose bytes
// are still retained, because RecoverPending will finish that upload on a later
// start. Deleting it here throws away recoverable data and orphans the retained
// file — which is what happened before Cleanup consulted pendingDir.
func TestCleanupKeepsPendingWithRetainedData(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	retained, err := hfs.DB.InsertInode(1, "retained.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert retained: %v", err)
	}
	lost, err := hfs.DB.InsertInode(1, "lost.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert lost: %v", err)
	}

	// Only the first one has its bytes on disk.
	if err := os.WriteFile(filepath.Join(pendingDir, fmt.Sprintf("%d.4", retained)), []byte("data"), 0o600); err != nil {
		t.Fatalf("write retained file: %v", err)
	}

	if err := Cleanup(hfs.DB, hfs.Store, pendingDir); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	if _, err := hfs.DB.GetInode(retained); err != nil {
		t.Errorf("pending inode with retained data was deleted: %v", err)
	}
	if _, err := hfs.DB.GetInode(lost); err == nil {
		t.Error("pending inode with no retained data should have been deleted")
	}
}

// TestRecoverPendingUploadsRetainedBytes verifies the recovery path end to end:
// retained bytes are uploaded and the inode committed with its LOGICAL size —
// not the size of the file on disk, which differs from it under encryption.
func TestRecoverPendingUploadsRetainedBytes(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	id, err := hfs.DB.InsertInode(1, "recovered.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	// Stored bytes are longer than the logical size, as ciphertext would be.
	stored := []byte("stored-bytes-with-overhead")
	const logicalSize = 12
	if err := os.WriteFile(filepath.Join(pendingDir, fmt.Sprintf("%d.%d", id, logicalSize)), stored, 0o600); err != nil {
		t.Fatalf("write retained file: %v", err)
	}

	if err := RecoverPending(hfs.DB, hfs.Store, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode after recovery: %v", err)
	}
	if meta.Status != "committed" {
		t.Errorf("status = %q, want committed", meta.Status)
	}
	if meta.S3Key == "" {
		t.Fatal("no S3 key after recovery")
	}
	if meta.Size != logicalSize {
		t.Errorf("size = %d, want the logical size %d (not the stored length %d)", meta.Size, logicalSize, len(stored))
	}
	t.Cleanup(func() { hfs.Store.Delete(context.Background(), meta.S3Key) })

	got, err := hfs.Store.Download(context.Background(), meta.S3Key)
	if err != nil {
		t.Fatalf("download recovered object: %v", err)
	}
	if string(got) != string(stored) {
		t.Errorf("uploaded %q, want the retained bytes verbatim %q", got, stored)
	}

	if entries, _ := os.ReadDir(pendingDir); len(entries) != 0 {
		t.Errorf("retained file still present after successful recovery: %v", entries)
	}
}

// TestCheckStagedDataFindsRegularFiles is the regression test for the file-type
// mask: the staged query selected with "mode & S_IFLNK = 0", but S_IFREG
// (0x8000) and S_IFLNK (0xA000) share a bit, so every regular file was filtered
// out and the check reported nothing — including when the data was truly gone.
// The type must be matched through S_IFMT.
func TestCheckStagedDataFindsRegularFiles(t *testing.T) {
	hfs, _ := setupTest(t)
	stagingDir := t.TempDir()

	// Committed, no S3 key, no staging file: unreadable.
	lost, err := hfs.DB.InsertInode(1, "lost.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert lost: %v", err)
	}
	if _, err := hfs.DB.CommitInode(lost, "", 8); err != nil {
		t.Fatalf("commit lost: %v", err)
	}

	// Committed, no S3 key, but its bytes are staged: fine, awaiting packing.
	staged, err := hfs.DB.InsertInode(1, "staged.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert staged: %v", err)
	}
	if _, err := hfs.DB.CommitInode(staged, "", 8); err != nil {
		t.Fatalf("commit staged: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, fmt.Sprintf("%d", staged)), []byte("contents"), 0o600); err != nil {
		t.Fatalf("write staging file: %v", err)
	}

	missing, err := CheckStagedData(hfs.DB, stagingDir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}

	if len(missing) != 1 {
		t.Fatalf("found %d unreadable files, want exactly 1 (regular files must not be filtered out by the type mask)", len(missing))
	}
	if missing[0].ID != lost {
		t.Errorf("reported inode %d (%s), want the one with no staging file", missing[0].ID, missing[0].Name)
	}
}

// TestRecoverPendingKeepsBytesWhenUploadFails verifies that a recovery attempt
// that cannot reach S3 leaves the retained file alone to retry on the next
// start, rather than dropping the only copy of the data.
func TestRecoverPendingKeepsBytesWhenUploadFails(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	id, err := hfs.DB.InsertInode(1, "unreachable.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	path := filepath.Join(pendingDir, fmt.Sprintf("%d.4", id))
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatalf("write retained file: %v", err)
	}

	// A store pointed at a bucket that does not exist: uploads fail, nothing else does.
	broken := brokenStore(t)
	if err := RecoverPending(hfs.DB, broken, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Errorf("retained bytes were dropped after a failed upload: %v", err)
	}
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("inode gone: %v", err)
	}
	if meta.Status != "pending" {
		t.Errorf("status = %q, want it left pending for the next retry", meta.Status)
	}
}
