package hfuse

import (
	"bytes"
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/db"
)

// getattrSize drives Getattr the way stat(2) does — no file handle.
func getattrSize(t *testing.T, hfs *HamstorFS, id int64) int64 {
	t.Helper()
	var out fuse.AttrOut
	n := &HamstorNode{hfs: hfs, inodeID: id}
	if errno := n.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("getattr: %v", errno)
	}
	return int64(out.Attr.Size)
}

// TestGetattrReportsInFlightSize covers the complaint that started this: a file
// written through the mount reads back as 0 bytes for as long as its upload
// takes, because Flush is asynchronous and the size only becomes real at
// CommitBlocks. Nothing distinguishes that from an empty file, so a slow upload
// is indistinguishable from data loss — which is exactly how it was reported.
//
// The window is unavoidable (close(2) does not promise durability without
// fsync), but reporting 0 through it is not: the shared write state knows the
// size the whole time.
func TestGetattrReportsInFlightSize(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	release := make(chan struct{})
	reached := make(chan struct{})
	hfs.TestCrashBeforeCommit = func() {
		close(reached)
		<-release
	}

	content := bytes.Repeat([]byte("v"), db.BlockSize+1024)
	id := mustInsert(t, hfs, "inflight.bin")
	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt(content, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}

	// The upload is now parked between "every block is in S3" and "the size is in
	// the DB" — the whole of the reported window, held open.
	<-reached
	if meta, err := hfs.DB.GetInode(id); err != nil {
		t.Fatalf("get inode: %v", err)
	} else if meta.Size != 0 {
		t.Fatalf("setup: the DB already knows the size (%d); the window this test is about is not open", meta.Size)
	}
	if got := getattrSize(t, hfs, id); got != int64(len(content)) {
		t.Errorf("stat reports %d bytes while the upload is in flight, want %d — indistinguishable from an empty file",
			got, len(content))
	}

	close(release)
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()
	blocksOf(t, hfs, id)

	if got := getattrSize(t, hfs, id); got != int64(len(content)) {
		t.Errorf("stat reports %d bytes after the commit, want %d", got, len(content))
	}
}

// TestGetattrFollowsATruncateDown is the other direction, and the one a naive
// "report whichever is larger" would get wrong: the shared state's size is
// authoritative when it disagrees with the DB, not merely an upper bound. A hint
// that only ever grows would report the pre-truncate size for as long as a
// handle stayed open.
func TestGetattrFollowsATruncateDown(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := bytes.Repeat([]byte("t"), 4096)
	id := mustInsert(t, hfs, "shrink.bin")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	// A handle stays open across the truncate, so the shared state survives and
	// its hint is what Getattr consults.
	th := NewTestHandle(hfs, id, false)
	defer th.TestRelease()
	if _, errno := th.h.Read(context.Background(), make([]byte, 16), 0); errno != 0 {
		t.Fatalf("read to load the state: %v", errno)
	}
	if got := getattrSize(t, hfs, id); got != int64(len(content)) {
		t.Fatalf("stat reports %d with a loaded handle, want %d", got, len(content))
	}

	setSize(t, &HamstorNode{hfs: hfs, inodeID: id}, 100)
	if got := getattrSize(t, hfs, id); got != 100 {
		t.Errorf("stat reports %d after truncating to 100: the hint outlived the truncate", got)
	}
}

// TestOpenTruncLeavesTheInodeCommittedAndEmpty names the state that was reported
// as silent data loss, so the next person who finds it in the DB finds this test
// rather than a hypothesis.
//
// go-fuse never negotiates CAP_ATOMIC_O_TRUNC, so the kernel implements
// open(O_TRUNC) as a VFS truncate: FUSE_SETATTR with size 0, sent BEFORE
// FUSE_OPEN. db.SetAttr deletes every block row, Setattr deletes the objects,
// and the status is left alone — so an overwritten file reads
// `committed / size 0 / no blocks` from the moment it is opened until the
// rewrite's async upload commits. Measured on a live B2 mount, that window was
// seconds; under memory pressure it was long enough to look permanent.
//
// It is correct POSIX (the file IS zero-length after O_TRUNC), and it is why
// retention cannot key on status — see TestFailedOverwriteAfterTruncateRetainsTheSet.
func TestOpenTruncLeavesTheInodeCommittedAndEmpty(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := bytes.Repeat([]byte("c"), db.BlockSize+512)
	id := mustInsert(t, hfs, "overwrite-me.bin")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	setSize(t, &HamstorNode{hfs: hfs, inodeID: id}, 0) // what the kernel sends for O_TRUNC

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	has, err := hfs.DB.HasBlocks(id)
	if err != nil {
		t.Fatalf("has blocks: %v", err)
	}
	if meta.Status != "committed" || meta.Size != 0 || has {
		t.Fatalf("after O_TRUNC: %s/%d/has=%v, want committed/0/false", meta.Status, meta.Size, has)
	}
}

// TestFlushBetweenCreateAndFirstWriteKeepsTheWrites is the hazard H1 named while
// this was still thought to be data loss: a FLUSH arriving before anything has
// been written commits the file at zero, and if that also consumed the state a
// later real write would have nothing left to commit.
//
// It is not hypothetical — polling a live mount at 5 ms resolution, a file
// created by dd(1) is 'committed' at size 0 within milliseconds, long before the
// data arrives. It is survivable only because the not-dirty branch commits at the
// inode's own size and clears isNew, leaving the writes that follow to reload and
// commit normally. Nothing else pins that, and simplifying that branch would turn
// every `dd` into a 0-byte file.
func TestFlushBetweenCreateAndFirstWriteKeepsTheWrites(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "created-then-flushed.bin")
	th := NewTestHandle(hfs, id, true)

	// The early FLUSH: nothing written yet.
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("early flush: %v", errno)
	}
	th.WaitUpload()
	if meta, err := hfs.DB.GetInode(id); err != nil {
		t.Fatalf("get inode: %v", err)
	} else if meta.Status != "committed" || meta.Size != 0 {
		t.Fatalf("after the early flush: %s/%d, want committed/0", meta.Status, meta.Size)
	}

	content := bytes.Repeat([]byte("w"), db.BlockSize+7)
	if errno := th.TestWriteAt(content, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()
	blocksOf(t, hfs, id)

	if got := readBack(t, hfs, id, len(content)); !bytes.Equal(got, content) {
		t.Fatalf("the writes after the early flush were lost: read %d bytes, want %d", len(got), len(content))
	}
}

// TestInFlightSizeIsAbsentWithoutAWriteState guards the sentinel. atomic.Int64
// starts at zero, so a state whose size was never recorded would claim the file
// is empty — turning a helper meant to fix a spurious 0 into a way of inventing
// one.
func TestInFlightSizeIsAbsentWithoutAWriteState(t *testing.T) {
	hfs, _ := setupTest(t)

	id := mustInsert(t, hfs, "no-handle.bin")
	if _, err := hfs.DB.CommitInode(id, 1234); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Nobody has it open: the DB is the only authority.
	if got := getattrSize(t, hfs, id); got != 1234 {
		t.Errorf("stat reports %d with no open handle, want 1234", got)
	}

	// Freshly created state, nothing written yet: still the DB.
	st := hfs.acquireWrite(id)
	defer hfs.releaseWrite(id, st)
	if got := getattrSize(t, hfs, id); got != 1234 {
		t.Errorf("stat reports %d for a fresh write state, want 1234 — the sentinel is not -1", got)
	}
}
