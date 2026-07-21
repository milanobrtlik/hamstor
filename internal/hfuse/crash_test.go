package hfuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

func setupTest(t *testing.T) (*HamstorFS, string) {
	t.Helper()

	cfg := testutil.RequireS3(t)

	dbPath := t.TempDir() + "/test.db"
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	store, err := s3store.New(context.Background(), cfg.Bucket, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	hfs := &HamstorFS{DB: database, Store: store, UploadSem: make(chan struct{}, 8)}
	t.Cleanup(func() { database.Close() })
	return hfs, dbPath
}

func TestCrashBeforeCommit(t *testing.T) {
	hfs, dbPath := setupTest(t)
	ctx := context.Background()

	// Create a file (inserts pending row in SQLite)
	fileID, err := hfs.DB.InsertInode(1, "crash-test.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	// Simulate the Flush flow manually:
	// 1. Upload to S3
	content := []byte("data that will be orphaned")
	s3Key := fmt.Sprintf("crash-test-%d", fileID)
	if err := hfs.Store.Upload(ctx, s3Key, content); err != nil {
		t.Fatalf("upload: %v", err)
	}

	// 2. "Crash" here — do NOT call CommitInode
	//    The S3 object exists, SQLite row is still 'pending' with no s3_key

	// Verify: inode is still pending
	meta, err := hfs.DB.GetInode(fileID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("expected status 'pending', got %q", meta.Status)
	}

	// Verify: S3 object exists
	data, err := hfs.Store.Download(ctx, s3Key)
	if err != nil {
		t.Fatalf("download should succeed: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("content mismatch")
	}

	// --- Simulate restart ---
	// Close and reopen DB (fresh connection, like a real restart)
	hfs.DB.Close()
	database2, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer database2.Close()

	// Run startup cleanup
	if err := Cleanup(database2, hfs.Store, ""); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify: pending inode is gone from SQLite
	_, err = database2.GetInode(fileID)
	if err == nil {
		t.Fatal("expected inode to be deleted after cleanup")
	}

	// Verify: only root inode remains
	pending, err := database2.GetPending()
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending))
	}
}

func TestCrashHookInFlush(t *testing.T) {
	hfs, dbPath := setupTest(t)

	// Set the crash hook — it panics to simulate process death.
	// The async upload goroutine recovers panics internally.
	crashCalled := false
	hfs.TestCrashBeforeCommit = func() {
		crashCalled = true
		panic("simulated crash")
	}

	// Create a file
	fileID, err := hfs.DB.InsertInode(1, "hook-test.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	// Call Flush (starts async upload) then wait for it to complete
	handle := NewTestHandle(hfs, fileID, true)
	handle.TestWrite([]byte("some data"))
	handle.TestFlush()
	handle.WaitUpload()

	if !crashCalled {
		t.Fatal("crash hook was not called")
	}

	// The inode should still be pending (commit never happened due to panic)
	meta, err := hfs.DB.GetInode(fileID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("expected 'pending', got %q", meta.Status)
	}

	// Restart + cleanup
	hfs.DB.Close()
	database2, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer database2.Close()

	if err := Cleanup(database2, hfs.Store, ""); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	_, err = database2.GetInode(fileID)
	if err == nil {
		t.Fatal("expected inode to be cleaned up")
	}
}

// TestCrashBetweenBlockUploadAndCommit covers the window the block layout widened:
// a multi-block flush uploads N objects and only then opens the transaction, so a
// crash in between leaves objects in the bucket that no row names.
//
// What must hold is that nothing is half-committed. A file with rows for the
// blocks that made it and none for the rest reads as data followed by zeroes, and
// nothing ever reports it — which is precisely why the commit is one transaction
// after all the uploads rather than a row per object.
//
// A real crash also cannot retain anything: retention runs in the failure branch
// of the upload loop, and there is no failure here, just death. So the pending
// directory stays empty and Cleanup removes the inode on the next start, naming
// it. The orphaned objects are GC phase 1's problem.
func TestCrashBetweenBlockUploadAndCommit(t *testing.T) {
	hfs, dbPath := setupTest(t)
	hfs.SpillDir = t.TempDir()
	pendingDir := filepath.Join(filepath.Dir(dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatalf("pending dir: %v", err)
	}
	hfs.PendingDir = pendingDir

	crashed := false
	hfs.TestCrashBeforeCommit = func() {
		crashed = true
		panic("simulated crash")
	}

	fileID, err := hfs.DB.InsertInode(1, "crash-blocks.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	handle := NewTestHandle(hfs, fileID, true)
	if errno := handle.TestWriteAt(bytes.Repeat([]byte("c"), 2*db.BlockSize), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	handle.TestFlush()
	handle.WaitUpload()
	handle.TestRelease()

	if !crashed {
		t.Fatal("crash hook was not called — the upload never reached the commit window")
	}

	if blocks, _ := hfs.DB.BlocksForInode(fileID); len(blocks) != 0 {
		t.Fatalf("crash left %d block row(s); a file with rows for some blocks and not others reads as "+
			"data followed by zeroes and nothing reports it", len(blocks))
	}
	meta, err := hfs.DB.GetInode(fileID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("status %q, want pending", meta.Status)
	}
	if entries, _ := os.ReadDir(pendingDir); len(entries) != 0 {
		t.Errorf("a crash retained %v; retention only runs when an upload reports an error, and death is "+
			"not an error", entryNames(entries))
	}

	// --- restart ---
	hfs.DB.Close()
	database2, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer database2.Close()

	if err := RecoverPending(database2, hfs.Store, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if err := Cleanup(database2, hfs.Store, pendingDir); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := database2.GetInode(fileID); err == nil {
		t.Fatal("a pending inode with nothing retained must be cleaned up")
	}
}
