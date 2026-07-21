package ops

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

func setupGCTest(t *testing.T) (*db.DB, *s3store.Store) {
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

	t.Cleanup(func() { database.Close() })
	return database, store
}

// TestGCPhase1KeepsBlockObjects covers the deletion loop itself, which nothing
// else does: the two tests below exercise phase 2 (orphaned inodes in the DB),
// while phase 1 — compare the key set against a bucket listing, delete the
// difference — had no coverage at all. It is also the single most destructive
// place in the block layout: a block set missing from AllS3KeySet means the
// first `hamstor gc` deletes every large file in one DeleteObjects call.
//
// The run is scoped on purpose. Zero grace is what makes the assertion mean
// anything: with the production grace period phase 1 skips a freshly uploaded
// object before it ever compares its key, so the object would survive even a GC
// that had lost track of it. The prefix then keeps that zero grace from
// reaching the objects the hfuse, volume and s3store tests are using in the same
// bucket — `go test ./...` runs those packages in parallel with this one.
func TestGCPhase1KeepsBlockObjects(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("gctest-blocks-%d/", time.Now().UnixNano())
	blockKey := prefix + "block"
	orphanKey := prefix + "orphan"
	t.Cleanup(func() {
		store.Delete(ctx, blockKey)
		store.Delete(ctx, orphanKey)
	})

	blockData := []byte("block zero contents")
	if err := store.Upload(ctx, blockKey, blockData); err != nil {
		t.Fatalf("upload block: %v", err)
	}

	inodeID, err := database.InsertInode(1, "big.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	committed, _, err := database.CommitBlocks(inodeID,
		[]db.BlockCommit{{Index: 0, S3Key: blockKey, Size: int64(len(blockData))}},
		int64(len(blockData)))
	if err != nil {
		t.Fatalf("commit blocks: %v", err)
	}
	if !committed {
		t.Fatal("commit blocks reported the inode as gone")
	}

	// Control object: referenced by nothing, so GC must delete it. Without it a
	// green result would only prove that nothing was deleted at all.
	if err := store.Upload(ctx, orphanKey, []byte("nobody references this")); err != nil {
		t.Fatalf("upload control object: %v", err)
	}

	result, err := gcScoped(ctx, database, store, false, gcOptions{grace: 0, listPrefix: prefix})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}

	// Check the control first: if the deletion loop did not run, everything
	// below passes for the wrong reason.
	if _, err := store.Download(ctx, orphanKey); err == nil {
		t.Fatal("the control object survived: the deletion loop never ran, so this test proves nothing")
	}
	got, err := store.Download(ctx, blockKey)
	if err != nil {
		t.Fatalf("gc deleted a live block object (its key is in blocks): %v", err)
	}
	if !bytes.Equal(got, blockData) {
		t.Errorf("block object contents = %q, want %q", got, blockData)
	}
	if result.OrphansFound != 1 || result.OrphansDeleted != 1 {
		t.Errorf("gc found %d orphans and deleted %d, want 1 and 1 (the control object alone)",
			result.OrphansFound, result.OrphansDeleted)
	}
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

	// Commit the files as blocks — the only shape an inode's own data can take.
	// This is what makes the test cover phase 2's real hazard: the block rows go
	// away with the inode through ON DELETE CASCADE, so GC has to collect the
	// keys FROM the delete. Reading them off the inode row, as it used to, finds
	// nothing and leaves both objects in the bucket.
	if _, _, err := database.CommitBlocks(fileID1, []db.BlockCommit{{Index: 0, S3Key: key1, Size: 5}}, 5); err != nil {
		t.Fatalf("commit file1: %v", err)
	}
	if _, _, err := database.CommitBlocks(fileID2, []db.BlockCommit{{Index: 0, S3Key: key2, Size: 5}}, 5); err != nil {
		t.Fatalf("commit file2: %v", err)
	}

	// Simulate the bug: delete directory WITHOUT deleting children
	if _, err := database.DeleteInode(dirID); err != nil {
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
	if _, _, err := database.CommitBlocks(fileID, []db.BlockCommit{{Index: 0, S3Key: key, Size: 4}}, 4); err != nil {
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
