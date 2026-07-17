package hfuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
	"github.com/milan/hamstor/internal/volume"
)

// setupStaging is setupTest plus a volume builder, which puts files of
// MaxNeedleSize or less on the synchronous staging path instead of the async
// upload path. The two paths diverge sharply enough that a test written for one
// proves nothing about the other, so each test says which one it wants.
func setupStaging(t *testing.T) (*HamstorFS, string) {
	t.Helper()

	hfs, dbPath := setupTest(t)
	stagingDir := filepath.Join(filepath.Dir(dbPath), "staging")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("staging dir: %v", err)
	}
	hfs.SpillDir = t.TempDir()
	hfs.VolumeBuilder = volume.NewBuilder(hfs.DB, hfs.Store, stagingDir)
	t.Cleanup(func() { hfs.VolumeBuilder.Close() })
	return hfs, dbPath
}

func mustInsert(t *testing.T, hfs *HamstorFS, name string) int64 {
	t.Helper()
	id, err := hfs.DB.InsertInode(1, name, syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	return id
}

// readBack reads the file's committed contents through a fresh handle, the way a
// reopen would. Any state left over from the writers is torn down first, so this
// exercises storage rather than a warm buffer.
func readBack(t *testing.T, hfs *HamstorFS, id int64, n int) []byte {
	t.Helper()
	if live := hfs.liveWriteStates(); live != 0 {
		t.Fatalf("write state leaked before readback: %d live", live)
	}
	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()
	got, errno := rh.TestRead(n, 0)
	if errno != 0 {
		t.Fatalf("read back: errno %v", errno)
	}
	return got
}

// TestConcurrentHandlesBothOffsetsSurvive is the pwrite scenario: two handles
// open on one inode, each writing a different region, closed one after the
// other. POSIX gives both writes; before the shared write state, each handle
// held a private snapshot of the whole file and the last flush wrote its
// snapshot back wholesale, so one of the two marks always died.
func TestConcurrentHandlesBothOffsetsSurvive(t *testing.T) {
	hfs, _ := setupTest(t) // async path: no volume builder
	id := mustInsert(t, hfs, "pwrite-two-handles.txt")

	a := NewTestHandle(hfs, id, true)
	b := NewTestHandle(hfs, id, true)

	if errno := a.TestWriteAt([]byte("AAAA"), 0); errno != 0 {
		t.Fatalf("write A: %v", errno)
	}
	if errno := b.TestWriteAt([]byte("BBBB"), 50); errno != 0 {
		t.Fatalf("write B: %v", errno)
	}

	a.TestFlush()
	a.WaitUpload()
	a.TestRelease()
	b.TestFlush()
	b.WaitUpload()
	b.TestRelease()

	got := readBack(t, hfs, id, 64)
	if len(got) != 54 {
		t.Fatalf("size: want 54, got %d (%q)", len(got), got)
	}
	if string(got[0:4]) != "AAAA" {
		t.Errorf("A's write lost: bytes 0-3 = %q, want \"AAAA\"", got[0:4])
	}
	if string(got[50:54]) != "BBBB" {
		t.Errorf("B's write lost: bytes 50-53 = %q, want \"BBBB\"", got[50:54])
	}
}

// TestConcurrentHandlesAppend is the shared-log scenario: two appenders
// interleaving writes. Needs both halves of the fix — the shared buffer, and
// Write honouring O_APPEND itself. The kernel derives append offsets from its
// cached st_size, so both appenders are told to write at the same place and one
// overwrites the other even when the buffer is shared.
func TestConcurrentHandlesAppend(t *testing.T) {
	hfs, _ := setupTest(t)
	id := mustInsert(t, hfs, "shared-log.txt")

	a := NewTestAppendHandle(hfs, id, true)
	b := NewTestAppendHandle(hfs, id, true)

	// Offset 0 throughout: that is what the kernel hands an appender whose
	// cached size is stale, and what Write must ignore.
	for i := 0; i < 3; i++ {
		if errno := a.TestWriteAt([]byte("A\n"), 0); errno != 0 {
			t.Fatalf("append A: %v", errno)
		}
		if errno := b.TestWriteAt([]byte("B\n"), 0); errno != 0 {
			t.Fatalf("append B: %v", errno)
		}
	}

	a.TestFlush()
	a.WaitUpload()
	a.TestRelease()
	b.TestFlush()
	b.WaitUpload()
	b.TestRelease()

	got := readBack(t, hfs, id, 64)
	if lines := strings.Count(string(got), "\n"); lines != 6 {
		t.Fatalf("want 6 lines, got %d: %q", lines, got)
	}
	if a, b := strings.Count(string(got), "A"), strings.Count(string(got), "B"); a != 3 || b != 3 {
		t.Fatalf("want 3 A's and 3 B's, got %d and %d: %q", a, b, got)
	}
}

// TestSharedStateReadableAfterFlush covers the window a remount hides: one
// handle flushes while a sibling stays open, and the sibling then reads and
// writes through the same state. The staging path hands its buffer to the
// staging file and nils it, so a state left marked loaded here would serve zero
// bytes and let the next write rebuild the file out of nothing.
func TestSharedStateReadableAfterFlush(t *testing.T) {
	hfs, _ := setupStaging(t) // staging path: small file, synchronous commit
	id := mustInsert(t, hfs, "shared-after-flush.txt")

	a := NewTestHandle(hfs, id, true)
	b := NewTestHandle(hfs, id, true)

	if errno := a.TestWriteAt([]byte("hello world"), 0); errno != 0 {
		t.Fatalf("write A: %v", errno)
	}
	if errno := a.TestFlush(); errno != 0 {
		t.Fatalf("flush A: %v", errno)
	}
	a.WaitUpload()

	// B is still open. Reading through it must show what A flushed, not EOF.
	got, errno := b.TestRead(32, 0)
	if errno != 0 {
		t.Fatalf("read through sibling handle: %v", errno)
	}
	if string(got) != "hello world" {
		t.Fatalf("sibling read after flush: got %q, want %q", got, "hello world")
	}

	// And a write through B must extend those contents, not a zero-filled void.
	if errno := b.TestWriteAt([]byte("!"), 11); errno != 0 {
		t.Fatalf("write B: %v", errno)
	}
	if errno := b.TestFlush(); errno != 0 {
		t.Fatalf("flush B: %v", errno)
	}
	b.WaitUpload()
	a.TestRelease()
	b.TestRelease()

	got = readBack(t, hfs, id, 32)
	if string(got) != "hello world!" {
		t.Fatalf("after sibling write: got %q, want %q", got, "hello world!")
	}
}

// TestFailedUploadPoisonsSharedState checks that a failed upload does not let a
// sibling handle quietly commit over the retained bytes. If it could, the inode
// would flip from 'pending' to 'committed' and RecoverPending would delete the
// retained copy on the next start as stale — losing the file outright on the
// strength of one transient S3 error.
func TestFailedUploadPoisonsSharedState(t *testing.T) {
	hfs, dbPath := setupTest(t)
	pendingDir := filepath.Join(filepath.Dir(dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatalf("pending dir: %v", err)
	}
	hfs.PendingDir = pendingDir

	// Point the store at a bucket that does not exist so the upload fails.
	badStore, err := s3store.New(context.Background(), "hamstor-no-such-bucket-"+t.Name(),
		testutil.RequireS3(t).Endpoint, testutil.RequireS3(t).AccessKey,
		testutil.RequireS3(t).SecretKey, testutil.RequireS3(t).Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	hfs.Store = badStore

	id := mustInsert(t, hfs, "poisoned.txt")
	a := NewTestHandle(hfs, id, true)
	b := NewTestHandle(hfs, id, true)

	if errno := a.TestWriteAt([]byte("important"), 0); errno != 0 {
		t.Fatalf("write A: %v", errno)
	}
	a.TestFlush()
	a.WaitUpload()

	// The bytes are retained for RecoverPending; the inode must still be pending.
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("status: want pending, got %q", meta.Status)
	}
	entries, err := os.ReadDir(pendingDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("retained bytes missing from %s (err %v)", pendingDir, err)
	}

	// B must not be handed an empty buffer to build on.
	if _, errno := b.TestRead(16, 0); errno != syscall.EIO {
		t.Errorf("read on poisoned state: want EIO, got %v", errno)
	}
	if errno := b.TestWriteAt([]byte("x"), 0); errno != syscall.EIO {
		t.Errorf("write on poisoned state: want EIO, got %v", errno)
	}

	a.TestRelease()
	b.TestRelease()

	// The retained copy must still be the only truth: nothing committed over it.
	meta, err = hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("status after sibling attempts: want pending, got %q", meta.Status)
	}
}

// TestReopenDuringInflightUploadSeesNewData covers the sequential form of the
// same race: the writer closes, and a reopen lands while the upload is still in
// flight. Until it commits, the DB still names the old key, so a fresh state
// would load pre-upload contents and write over them.
func TestReopenDuringInflightUploadSeesNewData(t *testing.T) {
	hfs, _ := setupTest(t)
	id := mustInsert(t, hfs, "reopen-inflight.txt")

	release := make(chan struct{})
	hfs.TestCrashBeforeCommit = func() { <-release }

	a := NewTestHandle(hfs, id, true)
	if errno := a.TestWriteAt([]byte("first"), 0); errno != 0 {
		t.Fatalf("write A: %v", errno)
	}
	a.TestFlush() // upload starts, then parks in the hook before committing
	a.TestRelease()

	// Reopen while the upload is parked. The state is still held by the upload
	// goroutine, so this attaches to it rather than reading the stale key.
	reopened := make(chan syscall.Errno, 1)
	go func() {
		b := NewTestHandle(hfs, id, false)
		defer b.TestRelease()
		errno := b.TestWriteAt([]byte("!"), 5)
		if errno != 0 {
			reopened <- errno
			return
		}
		errno = b.TestFlush()
		b.WaitUpload()
		reopened <- errno
	}()

	close(release)
	if errno := <-reopened; errno != 0 {
		t.Fatalf("reopened write: %v", errno)
	}
	hfs.InflightUploads.Wait()

	got := readBack(t, hfs, id, 32)
	if string(got) != "first!" {
		t.Fatalf("reopen during in-flight upload: got %q, want %q", got, "first!")
	}
}

// TestPathTruncateReachesOpenHandle covers truncate(2) by path, which arrives
// with no file handle at all. It has to reach the shared buffer, or the open
// handle flushes its pre-truncate contents afterwards and resurrects the tail.
func TestPathTruncateReachesOpenHandle(t *testing.T) {
	hfs, _ := setupStaging(t)
	id := mustInsert(t, hfs, "path-truncate.txt")

	h := NewTestHandle(hfs, id, true)
	if errno := h.TestWriteAt([]byte("hello world"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}

	// What Setattr does for a path-based truncate: no handle in hand.
	st := hfs.tryAcquireWrite(id)
	if st == nil {
		t.Fatal("no shared state for an inode with an open handle")
	}
	if errno := truncateWriteState(st, 5); errno != 0 {
		t.Fatalf("truncate shared state: %v", errno)
	}
	hfs.releaseWrite(id, st)

	if errno := h.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	h.WaitUpload()
	h.TestRelease()

	got := readBack(t, hfs, id, 32)
	if string(got) != "hello" {
		t.Fatalf("truncated tail came back: got %q, want %q", got, "hello")
	}
}

// TestWriteStateReleasedOnOpenError guards the reference the registry takes in
// Open. Its failure paths return no handle, so no Release ever arrives for them
// and a missed hand-back pins the state for the life of the mount.
func TestWriteStateReleasedOnOpenError(t *testing.T) {
	cfg := testutil.RequireS3(t)
	hfs, _ := setupTest(t)

	id := mustInsert(t, hfs, "gone.txt")
	// A committed inode naming an S3 key that cannot be fetched: the write
	// preload in Open then fails and returns no handle.
	if _, err := hfs.DB.CommitInode(id, "aa/does-not-exist-"+t.Name(), 10); err != nil {
		t.Fatalf("commit inode: %v", err)
	}
	badStore, err := s3store.New(context.Background(), "hamstor-no-such-bucket-open",
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	hfs.Store = badStore

	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_RDWR))
	if errno == 0 {
		t.Fatalf("Open unexpectedly succeeded (fh %v); the test needs a failing preload", fh)
	}

	if live := hfs.liveWriteStates(); live != 0 {
		t.Fatalf("Open leaked write state on its error path: %d live", live)
	}
}

// TestOpenTruncWithCacheBackedSibling drives node.Open directly, which the other
// tests do not: they build handles straight from the registry. O_TRUNC is the one
// path sharing made unconditional, so it is exactly where a shared state left
// over from a reader can be found in a shape Open did not expect.
//
// A cache-backed state serves reads from cacheFile and rebuilds buf from it on
// the first write. Emptying only buf therefore truncates nothing: `> file` comes
// back the full length of the old file.
func TestOpenTruncWithCacheBackedSibling(t *testing.T) {
	hfs, _ := setupTest(t) // async path: >MaxNeedleSize file gets a standalone key
	cacheDir := t.TempDir()
	dc, err := cache.New(cacheDir, 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = dc
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "trunc-me.bin")

	// Write a file large enough to become a standalone S3 object, so that a
	// later load goes through the disk cache and leaves cacheFile set.
	orig := bytes.Repeat([]byte("O"), 300*1024)
	w := NewTestHandle(hfs, id, true)
	if errno := w.TestWriteAt(orig, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	w.TestFlush()
	w.WaitUpload()
	w.TestRelease()

	// A reader holds the file open, populating the shared state's cacheFile.
	reader := NewTestHandle(hfs, id, false)
	defer reader.TestRelease()
	if _, errno := reader.TestRead(16, 0); errno != 0 {
		t.Fatalf("reader read: %v", errno)
	}
	reader.h.st.mu.Lock()
	cached := reader.h.st.cacheFile != nil
	reader.h.st.mu.Unlock()
	if !cached {
		t.Skip("state is not cache-backed; nothing for this test to catch")
	}

	// Now `> file` while the reader is still open.
	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_WRONLY|syscall.O_TRUNC))
	if errno != 0 {
		t.Fatalf("open O_TRUNC: %v", errno)
	}
	th := &TestHandle{h: fh.(*HamstorHandle)}
	if errno := th.TestWriteAt([]byte("hi\n"), 0); errno != 0 {
		t.Fatalf("write after trunc: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != 3 {
		t.Fatalf("`> file` left %d bytes, want 3: the truncated content came back", meta.Size)
	}
}

// TestStaleFileSizeDoesNotClampSiblingsWrites covers the reader half of sharing.
// readLoaded clamps clean reads to the handle's open-time fileSize; once the
// buffer is shared, a handle that opened when the file was short and never
// loaded it itself (because a sibling did) would clamp everyone's contents down
// to its own stale idea of the size.
//
// This is what cost a line in the live two-appender run: `wc -l` opened while
// the log was 15 bytes, a sibling reloaded the state clean at 18, and the read
// came back clamped to 15.
func TestStaleFileSizeDoesNotClampSiblingsWrites(t *testing.T) {
	hfs, _ := setupStaging(t)
	ctx := context.Background()

	root := &HamstorNode{hfs: hfs, inodeID: 1}
	id := mustInsert(t, hfs, "grow.txt")
	w0 := NewTestHandle(hfs, id, true)
	if errno := w0.TestWriteAt([]byte("AAAAA"), 0); errno != 0 {
		t.Fatalf("seed write: %v", errno)
	}
	w0.TestFlush()
	w0.WaitUpload()
	w0.TestRelease()

	n := &HamstorNode{hfs: hfs, inodeID: id}
	_ = root

	// R opens while the file is 5 bytes and does not read yet.
	rfh, _, errno := n.Open(ctx, uint32(syscall.O_RDONLY))
	if errno != 0 {
		t.Fatalf("open R: %v", errno)
	}
	r := &TestHandle{h: rfh.(*HamstorHandle)}
	defer r.TestRelease()

	// A writer grows the file to 18 bytes and commits.
	wfh, _, errno := n.Open(ctx, uint32(syscall.O_RDWR))
	if errno != 0 {
		t.Fatalf("open W: %v", errno)
	}
	w := &TestHandle{h: wfh.(*HamstorHandle)}
	if errno := w.TestWriteAt([]byte("BBBBBBBBBBBBB"), 5); errno != 0 {
		t.Fatalf("grow write: %v", errno)
	}
	if errno := w.TestFlush(); errno != 0 {
		t.Fatalf("flush W: %v", errno)
	}
	w.WaitUpload()
	w.TestRelease()

	// A third handle loads the shared state clean at its new size.
	cfh, _, errno := n.Open(ctx, uint32(syscall.O_RDONLY))
	if errno != 0 {
		t.Fatalf("open C: %v", errno)
	}
	c := &TestHandle{h: cfh.(*HamstorHandle)}
	defer c.TestRelease()
	if _, errno := c.TestRead(32, 0); errno != 0 {
		t.Fatalf("read C: %v", errno)
	}

	// R now reads through the shared, already-loaded state. Its own fileSize is
	// still 5; the file is 18.
	got, errno := r.TestRead(32, 0)
	if errno != 0 {
		t.Fatalf("read R: %v", errno)
	}
	if len(got) != 18 {
		t.Fatalf("stale fileSize clamped the read to %d bytes (%q), want 18", len(got), got)
	}
}

// TestConcurrentAppendersViaOpen is the live two-appender scenario in process:
// two writers each doing open(O_APPEND) / write / close, repeatedly, through the
// real node.Open. Every line must survive.
//
// It goes through node.Open on purpose. Open both loads from storage and clamps
// the shared buffer to a size it read before locking the state, so a sibling's
// flush in that window makes the clamp cut their line off — a loss no test built
// on registry handles can see. It reproduced at roughly one round in three on a
// live mount.
func TestConcurrentAppendersViaOpen(t *testing.T) {
	hfs, _ := setupStaging(t)
	ctx := context.Background()

	const rounds = 12
	for round := 0; round < rounds; round++ {
		name := fmt.Sprintf("appendrace%d.log", round)
		id := mustInsert(t, hfs, name)
		seed := NewTestHandle(hfs, id, true)
		seed.TestFlush() // commit an empty file
		seed.TestRelease()

		n := &HamstorNode{hfs: hfs, inodeID: id}
		var wg sync.WaitGroup
		for _, who := range []string{"A", "B"} {
			wg.Add(1)
			go func(who string) {
				defer wg.Done()
				for i := 1; i <= 3; i++ {
					fh, _, errno := n.Open(ctx, uint32(syscall.O_WRONLY|syscall.O_APPEND))
					if errno != 0 {
						t.Errorf("%s%d open: %v", who, i, errno)
						return
					}
					h := fh.(*HamstorHandle)
					// Offset 0 is what the kernel hands an appender with a stale
					// cached size; Write must ignore it.
					if _, errno := h.Write(ctx, []byte(who+strconv.Itoa(i)+"\n"), 0); errno != 0 {
						t.Errorf("%s%d write: %v", who, i, errno)
						return
					}
					if errno := h.Flush(ctx); errno != 0 {
						t.Errorf("%s%d flush: %v", who, i, errno)
						return
					}
					h.Release(ctx)
				}
			}(who)
		}
		wg.Wait()
		if t.Failed() {
			return
		}

		hfs.InflightUploads.Wait()
		got := readBack(t, hfs, id, 128)
		if lines := strings.Count(string(got), "\n"); lines != 6 {
			m, _ := hfs.DB.GetInode(id)
			sp := hfs.VolumeBuilder.StagePath(id)
			raw, rerr := os.ReadFile(sp)
			t.Fatalf("round %d: %d lines, want 6: %q\n  DB: size=%d s3=%q vol=%q volsize=%d\n  staging file: %d bytes %q (err %v)",
				round, lines, got, m.Size, m.S3Key, m.VolS3Key, m.VolSize, len(raw), raw, rerr)
		}
		for _, who := range []string{"A", "B"} {
			for i := 1; i <= 3; i++ {
				if !strings.Contains(string(got), who+strconv.Itoa(i)+"\n") {
					t.Fatalf("round %d: %s%d lost: %q", round, who, i, got)
				}
			}
		}
	}
}

// TestOpenTruncEmptyFileNoUpload guards the other side of the O_TRUNC gate: for a
// file with no stored data there is nothing to truncate, and marking it dirty
// would cost an empty upload and commit on every `> file`.
func TestOpenTruncEmptyFileNoUpload(t *testing.T) {
	hfs := setupDBOnly(t)
	id, err := hfs.DB.InsertInode(1, "empty.txt", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_WRONLY|syscall.O_TRUNC))
	if errno != 0 {
		t.Fatalf("open: %v", errno)
	}
	h := fh.(*HamstorHandle)
	h.st.mu.Lock()
	dirty := h.st.dirty
	h.st.mu.Unlock()
	if dirty {
		t.Error("`> emptyfile` marked the state dirty; every redirect would upload an empty object")
	}
	// Flush must not reach the Store, which is nil in this fixture.
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	h.Release(context.Background())
	if live := hfs.liveWriteStates(); live != 0 {
		t.Fatalf("leaked state: %d", live)
	}
}

var _ = db.InodeMeta{}
