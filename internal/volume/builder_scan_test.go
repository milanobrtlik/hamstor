package volume

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

// testRig is a Builder wired to a fresh SQLite DB and the test S3 store, with a
// staging dir, but WITHOUT the background run() goroutine — tests drive
// scanAndSeal/drain directly for determinism. It tracks the inodes it stages so
// Cleanup can delete their volume objects from the shared test bucket.
type testRig struct {
	b   *Builder
	db  *db.DB
	ids []int64
}

func newTestRig(t *testing.T) *testRig {
	t.Helper()
	cfg := testutil.RequireS3(t)
	ctx := context.Background()

	database, err := db.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	store, err := s3store.New(ctx, cfg.Bucket, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("s3store.New: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	r := &testRig{
		b: &Builder{
			db:         database,
			store:      store,
			stagingDir: t.TempDir(),
			notify:     make(chan struct{}, 1),
			done:       make(chan struct{}),
			ctx:        cctx,
			cancel:     cancel,
		},
		db: database,
	}

	// Delete every volume object the test wrote to the shared bucket.
	t.Cleanup(func() {
		seen := map[string]bool{}
		for _, id := range r.ids {
			m, err := database.GetInode(id)
			if err != nil || m.VolS3Key == "" || seen[m.VolS3Key] {
				continue
			}
			seen[m.VolS3Key] = true
			store.Delete(context.Background(), m.VolS3Key)
		}
	})

	return r
}

// stage creates a committed, still-unpacked inode and writes its staging file,
// exactly the state scanAndSeal expects to find and pack.
func (r *testRig) stage(t *testing.T, data []byte) int64 {
	t.Helper()
	name := "f" + strconv.Itoa(len(r.ids))
	id, err := r.db.InsertInode(1, name, 0o644, "committed")
	if err != nil {
		t.Fatalf("InsertInode: %v", err)
	}
	if err := os.WriteFile(r.b.StagePath(id), data, 0o600); err != nil {
		t.Fatalf("write staging file: %v", err)
	}
	r.ids = append(r.ids, id)
	return id
}

// packed reports whether the inode now points at a volume (was sealed).
func (r *testRig) packed(t *testing.T, id int64) bool {
	t.Helper()
	m, err := r.db.GetInode(id)
	if err != nil {
		t.Fatalf("GetInode %d: %v", id, err)
	}
	return m.VolS3Key != ""
}

// stagingCount returns how many staging files remain (plain + claimed), so a
// test can assert the directory drained.
func (r *testRig) stagingCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(r.b.stagingDir)
	if err != nil {
		t.Fatalf("ReadDir staging: %v", err)
	}
	return len(entries)
}

// withBudget temporarily lowers scanBudgetBytes/maxBatchEntries so a test can
// exercise the budget and entry-cap paths with a handful of files.
func withBudget(t *testing.T, bytes int64, entries int) {
	t.Helper()
	origBytes, origEntries := scanBudgetBytes, maxBatchEntries
	scanBudgetBytes, maxBatchEntries = bytes, entries
	t.Cleanup(func() { scanBudgetBytes, maxBatchEntries = origBytes, origEntries })
}

// TestScanAndSealTruncatedBatchSeals is the regression test for the churn fix:
// when a notify pass fills the entry cap with tiny files (a real backlog behind
// it) it must SEAL what it claimed, not claim thousands of files and rename
// them all back. Here the cap is 2 and three tiny files are staged, so the scan
// truncates with a file remaining — the two it took must be packed.
func TestScanAndSealTruncatedBatchSeals(t *testing.T) {
	r := newTestRig(t)
	withBudget(t, TargetVolumeSize, 2) // entry cap 2, byte budget out of reach

	a := r.stage(t, []byte("aa"))
	b := r.stage(t, []byte("bb"))
	c := r.stage(t, []byte("cc"))

	sealed, more := r.b.scanAndSeal(false) // notify path, forceSmall=false
	if sealed != 2 {
		t.Fatalf("scanAndSeal sealed %d files, want 2 (truncated batch must seal)", sealed)
	}
	if !more {
		t.Errorf("scanAndSeal reported more=false, want true (entry cap left a backlog)")
	}
	packedCount := 0
	for _, id := range []int64{a, b, c} {
		if r.packed(t, id) {
			packedCount++
		}
	}
	if packedCount != 2 {
		t.Errorf("packed %d inodes, want 2", packedCount)
	}
	if got := r.stagingCount(t); got != 1 {
		t.Errorf("staging has %d files, want 1 (the untaken remainder)", got)
	}
}

// TestScanAndSealRestoresSmallCompleteBatch verifies the packing-efficiency
// path is preserved: a small batch that is NOT truncated (no backlog) and NOT
// forced waits for more data — it restores its claims and seals nothing, so the
// builder keeps accumulating toward a full volume instead of emitting a tiny one.
func TestScanAndSealRestoresSmallCompleteBatch(t *testing.T) {
	r := newTestRig(t)
	// Default 8 MB budget, high entry cap: three tiny files fit under both.

	ids := []int64{r.stage(t, []byte("x")), r.stage(t, []byte("y")), r.stage(t, []byte("z"))}

	if sealed, more := r.b.scanAndSeal(false); sealed != 0 || more {
		t.Fatalf("scanAndSeal = (%d, %v), want (0, false) (small batch must wait)", sealed, more)
	}
	for _, id := range ids {
		if r.packed(t, id) {
			t.Errorf("inode %d was packed, but a sub-budget notify batch must be restored", id)
		}
	}
	if got := r.stagingCount(t); got != 3 {
		t.Errorf("staging has %d files, want 3 (all restored)", got)
	}
}

// TestScanAndSealBudgetStopFillsVolume checks the byte budget replaces the old
// 64-entry cap: enough tiny files to cross the budget seal into one full volume
// (not the ~243 KB, 64-needle object the entry cap used to produce).
func TestScanAndSealBudgetStopFillsVolume(t *testing.T) {
	r := newTestRig(t)
	withBudget(t, 1<<20, maxBatchEntries) // 1 MB budget, real entry cap

	// 5 x 256 KB = 1.25 MB: crosses the 1 MB budget, leaving a remainder.
	const needle = 256 << 10
	var ids []int64
	for range 5 {
		ids = append(ids, r.stage(t, make([]byte, needle)))
	}

	sealed, _ := r.b.scanAndSeal(false)
	if sealed != 4 {
		t.Fatalf("scanAndSeal sealed %d files, want 4 (budget stops at 1 MB)", sealed)
	}

	// The four sealed inodes must share one volume of ~1 MB.
	volKeys := map[string]bool{}
	for _, id := range ids {
		if m, _ := r.db.GetInode(id); m.VolS3Key != "" {
			volKeys[m.VolS3Key] = true
		}
	}
	if len(volKeys) != 1 {
		t.Errorf("sealed inodes span %d volumes, want 1", len(volKeys))
	}
}

// TestDrainEmptiesBacklog exercises Change B: a backlog larger than one pass
// drains to empty back-to-back (not one batch per fallback tick), packing into
// multiple full volumes, and the loop terminates.
func TestDrainEmptiesBacklog(t *testing.T) {
	r := newTestRig(t)
	withBudget(t, 1<<20, maxBatchEntries) // 1 MB volumes

	const needle = 256 << 10
	var ids []int64
	for range 10 { // ~2.5 MB -> ~3 volumes
		ids = append(ids, r.stage(t, make([]byte, needle)))
	}

	r.b.drain(true) // ticker semantics: seal everything

	if got := r.stagingCount(t); got != 0 {
		t.Errorf("staging has %d files after drain, want 0", got)
	}
	volKeys := map[string]bool{}
	for _, id := range ids {
		if !r.packed(t, id) {
			t.Errorf("inode %d not packed after drain", id)
		}
		if m, _ := r.db.GetInode(id); m.VolS3Key != "" {
			volKeys[m.VolS3Key] = true
		}
	}
	// 10 x 256 KB at a 1 MB budget = two full 1 MB volumes + one 512 KB tail.
	// A larger count means drain looped into tiny per-file volumes (the churn
	// the stop-on-partial-tail rule prevents).
	if len(volKeys) != 3 {
		t.Errorf("backlog packed into %d volumes, want 3 (full volumes + one tail)", len(volKeys))
	}
}

// TestDrainNotifyTerminatesOnSmallBatch verifies drain(false) does not spin on a
// sub-budget backlog: scanAndSeal restores and returns 0, so drain exits after
// one pass with the files left staged for a later tick. (If it looped it would
// hang the test.)
func TestDrainNotifyTerminatesOnSmallBatch(t *testing.T) {
	r := newTestRig(t)

	ids := []int64{r.stage(t, []byte("a")), r.stage(t, []byte("b"))}

	done := make(chan struct{})
	go func() { r.b.drain(false); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drain(false) did not terminate on a small batch (spin loop)")
	}

	for _, id := range ids {
		if r.packed(t, id) {
			t.Errorf("inode %d packed, but notify path must wait on a small batch", id)
		}
	}
	if got := r.stagingCount(t); got != 2 {
		t.Errorf("staging has %d files, want 2 (restored)", got)
	}
}
