package hfuse

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

func testEnv(testKey, fallbackKey, defaultVal string) string {
	if v := os.Getenv(testKey); v != "" {
		return v
	}
	if v := os.Getenv(fallbackKey); v != "" {
		return v
	}
	return defaultVal
}

func setupTest(t *testing.T) (*HamstorFS, string) {
	t.Helper()

	dbPath := t.TempDir() + "/test.db"
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	endpoint := testEnv("HAMSTOR_TEST_ENDPOINT", "HAMSTOR_ENDPOINT", "http://localhost:3900")
	bucket := testEnv("HAMSTOR_TEST_BUCKET", "HAMSTOR_BUCKET", "hamstor")
	accessKey := testEnv("HAMSTOR_TEST_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "")
	secretKey := testEnv("HAMSTOR_TEST_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "")
	region := testEnv("HAMSTOR_TEST_REGION", "AWS_REGION", "")

	store, err := s3store.New(context.Background(), bucket, endpoint, accessKey, secretKey, region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	hfs := &HamstorFS{DB: database, Store: store}
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
	if err := Cleanup(database2, hfs.Store); err != nil {
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

	if err := Cleanup(database2, hfs.Store); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	_, err = database2.GetInode(fileID)
	if err == nil {
		t.Fatal("expected inode to be cleaned up")
	}
}
