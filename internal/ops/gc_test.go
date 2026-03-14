package ops

import (
	"context"
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

func setupGCTest(t *testing.T) (*db.DB, *s3store.Store) {
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

	t.Cleanup(func() { database.Close() })
	return database, store
}

func TestGCOrphanedInodes(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	// Create a directory (id will be >1)
	dirID, err := database.InsertInode(1, "testdir", 0o40755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}

	// Create files inside the directory
	fileID1, err := database.InsertInode(dirID, "file1.txt", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert file1: %v", err)
	}
	fileID2, err := database.InsertInode(dirID, "file2.txt", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert file2: %v", err)
	}

	// Upload S3 objects for the files
	key1 := "gc-test-orphan-1"
	key2 := "gc-test-orphan-2"
	if err := store.Upload(ctx, key1, []byte("data1")); err != nil {
		t.Fatalf("upload key1: %v", err)
	}
	if err := store.Upload(ctx, key2, []byte("data2")); err != nil {
		t.Fatalf("upload key2: %v", err)
	}

	// Commit files with S3 keys
	if _, err := database.CommitInode(fileID1, key1, 5); err != nil {
		t.Fatalf("commit file1: %v", err)
	}
	if _, err := database.CommitInode(fileID2, key2, 5); err != nil {
		t.Fatalf("commit file2: %v", err)
	}

	// Simulate the bug: delete directory WITHOUT deleting children
	if err := database.DeleteInode(dirID); err != nil {
		t.Fatalf("delete dir: %v", err)
	}

	// Verify: files are orphaned (parent doesn't exist)
	orphans, err := database.GetOrphanedInodes()
	if err != nil {
		t.Fatalf("get orphans: %v", err)
	}
	if len(orphans) != 2 {
		t.Fatalf("expected 2 orphans, got %d", len(orphans))
	}

	// Run GC
	result, err := GC(ctx, database, store, false)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if result.DBOrphans != 2 {
		t.Fatalf("expected 2 db orphans, got %d", result.DBOrphans)
	}

	// Verify: orphaned inodes are gone from DB
	orphans, err = database.GetOrphanedInodes()
	if err != nil {
		t.Fatalf("get orphans after gc: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("expected 0 orphans after gc, got %d", len(orphans))
	}

	// Verify: S3 objects are deleted
	_, err = store.Download(ctx, key1)
	if err == nil {
		t.Fatal("expected S3 key1 to be deleted")
	}
	_, err = store.Download(ctx, key2)
	if err == nil {
		t.Fatal("expected S3 key2 to be deleted")
	}
}

func TestGCOrphanedInodesDryRun(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	// Create orphaned file (parent_id=999 doesn't exist)
	fileID, err := database.InsertInode(999, "orphan.txt", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	key := "gc-test-dryrun-orphan"
	if err := store.Upload(ctx, key, []byte("data")); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, err := database.CommitInode(fileID, key, 4); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Run GC in dry-run mode
	result, err := GC(ctx, database, store, true)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if result.DBOrphans != 1 {
		t.Fatalf("expected 1 db orphan, got %d", result.DBOrphans)
	}
	if result.OrphansDeleted != 0 {
		t.Fatalf("dry-run should not delete, got %d deleted", result.OrphansDeleted)
	}

	// Verify: inode still exists (dry-run)
	_, err = database.GetInode(fileID)
	if err != nil {
		t.Fatal("inode should still exist in dry-run")
	}

	// Cleanup
	store.Delete(ctx, key)
	database.DeleteInode(fileID)
}
