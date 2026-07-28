package ops

import (
	"bytes"
	"context"
	"errors"
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

// TestGCRefusesWhenTheDatabaseKnowsNothing reproduces the wrong-working
// -directory bug. `hamstor gc` with the default relative --db created a fresh
// empty database, which db.Open plus seedRoot make indistinguishable from a
// filesystem holding no files, and phase 1 then handed the entire bucket to one
// DeleteBatch.
//
// setupGCTest already hands out exactly that database — a db.Open on a path in
// t.TempDir() — so the only thing this test adds is objects in the bucket. That
// is what makes it a reproduction rather than a simulation of one.
func TestGCRefusesWhenTheDatabaseKnowsNothing(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("gcguard-%d/", time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	t.Cleanup(func() {
		for _, k := range keys {
			store.Delete(ctx, k)
		}
	})
	for _, k := range keys {
		if err := store.Upload(ctx, k, []byte("data nobody in this database has heard of")); err != nil {
			t.Fatalf("upload %s: %v", k, err)
		}
	}

	// Zero grace makes all three deletable, which is what the bug's ten-minute-old
	// bucket looked like. The prefix keeps that zero grace off the objects the
	// other packages are using in this same bucket.
	result, err := gcScoped(ctx, database, store, false, gcOptions{grace: 0, listPrefix: prefix})

	var guard *GCGuardError
	if !errors.As(err, &guard) {
		t.Fatalf("gc returned %v (result %+v), want a GCGuardError: a database that knows "+
			"nothing about a populated bucket must not delete it", err, result)
	}
	if guard.Matched != 0 || guard.Orphans != 3 {
		t.Errorf("guard saw matched=%d orphans=%d, want 0 and 3", guard.Matched, guard.Orphans)
	}

	// The assertion that carries the test: without it a pass proves only that an
	// error came back, not that the objects survived it.
	for _, k := range keys {
		if _, err := store.Download(ctx, k); err != nil {
			t.Errorf("gc deleted %s despite refusing the run: %v", k, err)
		}
	}
	if result.OrphansDeleted != 0 {
		t.Errorf("gc reported %d deletions on a refused run", result.OrphansDeleted)
	}
}

// TestGCAllowMassDeleteOverridesGuard is the twin of the test above, and it is
// what proves the guard was the only thing that stopped it: same setup, same
// counts, one field different.
func TestGCAllowMassDeleteOverridesGuard(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("gcguard-override-%d/", time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	t.Cleanup(func() {
		for _, k := range keys {
			store.Delete(ctx, k)
		}
	})
	for _, k := range keys {
		if err := store.Upload(ctx, k, []byte("data nobody in this database has heard of")); err != nil {
			t.Fatalf("upload %s: %v", k, err)
		}
	}

	result, err := gcScoped(ctx, database, store, false,
		gcOptions{grace: 0, listPrefix: prefix, allowMassDelete: true})
	if err != nil {
		t.Fatalf("gc with allowMassDelete: %v", err)
	}
	if result.OrphansDeleted != 3 {
		t.Errorf("deleted %d objects, want 3 — the escape hatch has to actually work", result.OrphansDeleted)
	}
	for _, k := range keys {
		if _, err := store.Download(ctx, k); err == nil {
			t.Errorf("%s survived a run that was explicitly allowed to delete it", k)
		}
	}
}

// TestGCDryRunReportsTheRefusalAfterTheListing pins that --dry-run still shows
// the operator what it would have deleted. Returning the refusal early would
// withhold exactly the listing needed to judge it.
func TestGCDryRunReportsTheRefusalAfterTheListing(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("gcguard-dryrun-%d/", time.Now().UnixNano())
	key := prefix + "a"
	t.Cleanup(func() { store.Delete(ctx, key) })
	if err := store.Upload(ctx, key, []byte("data")); err != nil {
		t.Fatalf("upload: %v", err)
	}

	result, err := gcScoped(ctx, database, store, true, gcOptions{grace: 0, listPrefix: prefix})

	var guard *GCGuardError
	if !errors.As(err, &guard) {
		t.Fatalf("dry run returned %v, want a GCGuardError", err)
	}
	if result.OrphansFound != 1 {
		t.Errorf("dry run reported %d orphans, want 1 — the report must complete before the refusal",
			result.OrphansFound)
	}
	if _, err := store.Download(ctx, key); err != nil {
		t.Errorf("dry run deleted %s: %v", key, err)
	}
}

// TestGCOrphanedInodes covers phase 2. Like every test here it scopes phase 1 to
// a prefix it owns: `go test ./...` runs the hfuse, volume and s3store packages
// against this same bucket in parallel, and an unscoped listing hands their
// objects to a database that has never heard of them — so the run deletes
// everything in the bucket older than the grace period. That is not a
// hypothetical: it is the wrong-working-directory bug this package now guards
// against, and it was live in these two tests. Phase 2 and phase 3 never look at
// the listing, so scoping it away costs this test nothing.
func TestGCOrphanedInodes(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("gctest-orphans-%d/", time.Now().UnixNano())

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
	key1 := prefix + "orphan-1"
	key2 := prefix + "orphan-2"
	t.Cleanup(func() {
		store.Delete(ctx, key1)
		store.Delete(ctx, key2)
	})
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
	result, err := gcScoped(ctx, database, store, false, gcOptions{grace: gcGracePeriod, listPrefix: prefix})
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

	prefix := fmt.Sprintf("gctest-dryrun-%d/", time.Now().UnixNano())

	// Create orphaned file (parent_id=999 doesn't exist)
	fileID, err := database.InsertInode(999, "orphan.txt", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	key := prefix + "orphan"
	if err := store.Upload(ctx, key, []byte("data")); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, _, err := database.CommitBlocks(fileID, []db.BlockCommit{{Index: 0, S3Key: key, Size: 4}}, 4); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Run GC in dry-run mode
	result, err := gcScoped(ctx, database, store, true, gcOptions{grace: gcGracePeriod, listPrefix: prefix})
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
