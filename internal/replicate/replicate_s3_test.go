package replicate_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/replicate"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

// TestColdStartRestore is the regression guard for the cold-start fix: a fresh
// machine (no local DB) must reconstruct the metadata database from S3 via a
// snapshot alone. It mirrors the production sequence Restore -> Start -> app
// db.Open, writes inodes, takes a graceful-shutdown snapshot, wipes all local
// state, then restores onto a clean directory and checks the inodes came back.
//
// Needs a reachable S3 endpoint (Garage by default); skips otherwise. Each run
// uses a unique S3 prefix and deletes it on cleanup so it never touches real data.
func TestColdStartRestore(t *testing.T) {
	s3cfg := testutil.RequireS3(t)
	ctx := context.Background()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "hamstor.db")
	prefix := fmt.Sprintf("litestream-test/coldstart-%d", time.Now().UnixNano())

	cfg := replicate.Config{
		DBPath:            dbPath,
		Bucket:            s3cfg.Bucket,
		Endpoint:          s3cfg.Endpoint,
		Region:            s3cfg.Region,
		Path:              prefix,
		AccessKeyID:       s3cfg.AccessKey,
		SecretAccessKey:   s3cfg.SecretKey,
		SnapshotInterval:  time.Hour,
		SnapshotRetention: 24 * time.Hour,
	}

	// Clean up every object written under the test prefix, whatever happens.
	cleanupStore, err := s3store.New(ctx, s3cfg.Bucket, s3cfg.Endpoint, s3cfg.AccessKey, s3cfg.SecretKey, s3cfg.Region)
	if err != nil {
		t.Fatalf("s3store.New: %v", err)
	}
	t.Cleanup(func() {
		keys, err := cleanupStore.List(context.Background(), prefix+"/")
		if err != nil {
			t.Logf("cleanup list %s: %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			if _, err := cleanupStore.DeleteBatch(context.Background(), keys); err != nil {
				t.Logf("cleanup delete %d keys: %v", len(keys), err)
			}
		}
	})

	// --- Phase 1: writer machine ---
	rep := replicate.New(cfg)
	if err := rep.Restore(ctx); err != nil {
		t.Fatalf("restore (first run, expected no backup): %v", err)
	}
	if err := rep.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}

	const n = 200
	want := make(map[string]int64, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("file-%03d", i)
		id, err := database.InsertInode(1, name, 0o100644, "committed")
		if err != nil {
			t.Fatalf("insert inode %s: %v", name, err)
		}
		want[name] = id
	}

	// Graceful shutdown: app closes its SQL connections, then Stop forces the
	// snapshot and closes the Store.
	if err := database.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if err := rep.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// --- Phase 2: wipe the machine (simulate a fresh host) ---
	for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove %s: %v", p, err)
		}
	}
	// litestream's local meta dir is ".<name>-litestream" next to the DB.
	if err := os.RemoveAll(filepath.Join(dir, ".hamstor.db-litestream")); err != nil {
		t.Fatalf("remove litestream meta dir: %v", err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("db not wiped, restore would be skipped: stat err = %v", err)
	}

	// --- Phase 3: fresh machine cold restore ---
	rep2 := replicate.New(cfg)
	if err := rep2.Restore(ctx); err != nil {
		t.Fatalf("cold restore: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("cold restore produced no DB: %v", err)
	}

	// --- Phase 4: assert the inode table came back intact ---
	restored, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen restored db: %v", err)
	}
	defer restored.Close()

	children, err := restored.ListAllChildren(1)
	if err != nil {
		t.Fatalf("list children after restore: %v", err)
	}
	if len(children) != n {
		t.Errorf("restored child count = %d, want %d", len(children), n)
	}
	for name, id := range want {
		got, err := restored.LookupChild(1, name)
		if err != nil {
			t.Fatalf("lookup %s after restore: %v", name, err)
		}
		if got.ID != id {
			t.Errorf("%s: inode id after restore = %d, want %d", name, got.ID, id)
		}
	}
}
