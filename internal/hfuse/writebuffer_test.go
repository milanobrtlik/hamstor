package hfuse

import (
	"syscall"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/db"
)

// The write-buffer (Phase A) bounds the un-uploaded bytes buffered locally and
// blocks Write past that bound, so a bulk copy paces to the S3 drain rate and the
// spill dir stays bounded. These tests cover the two things that make it safe: the
// per-DIRTIED-BLOCK accounting (a sparse write must not charge its logical size),
// that the accounting is leak-free across a full flush cycle, and that the
// admission gate blocks and wakes correctly — including the single-file exemption
// that is the whole reason it cannot deadlock.

// TestWriteBufferChargesPerDirtiedBlockNotSize is the sparse case. A 1 KB write at
// offset 4 GiB into a fresh file must charge ONE block against the budget, not
// 4 GiB — the spill file is sparse, so the holes cost nothing on disk and must
// cost nothing in the accounting either. This is why the budget counts dirty
// blocks and never spillSize.
func TestWriteBufferChargesPerDirtiedBlockNotSize(t *testing.T) {
	hfs := setupDBOnly(t)
	hfs.SpillDir = t.TempDir()

	id, err := hfs.DB.InsertInode(1, "sparse.bin", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt([]byte("hello, sparse world"), 4<<30); errno != 0 {
		t.Fatalf("sparse write: %v", errno)
	}

	if got := hfs.dirtyBytes; got != db.BlockSize {
		t.Fatalf("a 1 KB sparse write into a 4 GiB offset charged %d bytes, want one block (%d): the budget is counting holes",
			got, int64(db.BlockSize))
	}

	// The free() backstop returns the charge when the last reference drops, even
	// though this handle never flushed.
	th.TestRelease()
	if got := hfs.dirtyBytes; got != 0 {
		t.Fatalf("dirtyBytes = %d after release, want 0: the free() accounting backstop leaked", got)
	}
}

// TestWriteBufferAccountingIsLeakFreeAfterFlush exercises the charge transfer: a
// real write/flush/upload/release cycle must return the budget to exactly 0. The
// charge is handed from the state to the upload goroutine at Flush and released
// once the bytes leave the spill tier; a missed release leaks budget until the
// mount restarts, which eventually blocks every write forever.
func TestWriteBufferAccountingIsLeakFreeAfterFlush(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	id, err := hfs.DB.InsertInode(1, "big.bin", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	// Larger than one block so several blocks are charged and then released.
	data := make([]byte, 3*db.BlockSize+1234)
	for i := range data {
		data[i] = byte(i)
	}

	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt(data, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if got := hfs.dirtyBytes; got != 4*db.BlockSize {
		t.Fatalf("after writing 3 blocks + a tail, dirtyBytes = %d, want %d", got, int64(4*db.BlockSize))
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait() // returns only after the charge is released (defer order)

	if got := hfs.dirtyBytes; got != 0 {
		t.Fatalf("dirtyBytes = %d after a full flush cycle, want 0: the write-buffer charge leaked", got)
	}

	// Clean up the objects the flush wrote.
	if blocks, bErr := hfs.DB.BlocksForInode(id); bErr == nil {
		for _, b := range blocks {
			hfs.Store.Delete(t.Context(), b.S3Key)
		}
	}
}

// admitInBackground runs admitWrite(n) in a goroutine and returns a channel that
// closes when it returns.
func admitInBackground(hfs *HamstorFS, n int64) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		hfs.admitWrite(n)
		close(done)
	}()
	return done
}

func assertReturns(t *testing.T, done <-chan struct{}, within time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatal(msg)
	}
}

func assertBlocks(t *testing.T, done <-chan struct{}, forAtLeast time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	case <-time.After(forAtLeast):
	}
}

// TestAdmitWriteSingleFileExemption is the anti-deadlock guarantee. When the
// budget is exhausted but NOTHING is draining (InflightCount == 0), admitWrite
// must return at once — otherwise a single file larger than the budget, which has
// no upload in flight until its own close, would block waiting for a drain that
// can never come.
func TestAdmitWriteSingleFileExemption(t *testing.T) {
	hfs := &HamstorFS{WriteBuffer: db.BlockSize}
	hfs.addDirtyBytes(10 * db.BlockSize) // well over budget
	// InflightCount stays 0: nothing is draining.

	assertReturns(t, admitInBackground(hfs, 1), time.Second,
		"admitWrite blocked while over budget with no upload in flight: the single-file exemption is broken, this deadlocks a large file")
}

// TestAdmitWriteBlocksThenWakesOnDrain covers the pacing path: over budget WITH an
// upload in flight, admitWrite blocks; when dirtyBytes falls back under the budget
// it wakes and returns. This is what paces a many-file copy to the S3 rate.
func TestAdmitWriteBlocksThenWakesOnDrain(t *testing.T) {
	hfs := &HamstorFS{WriteBuffer: db.BlockSize}
	hfs.addDirtyBytes(db.BlockSize) // exactly at budget
	hfs.InflightCount.Store(1)      // something is draining

	done := admitInBackground(hfs, 1) // BlockSize + 1 > BlockSize -> must block
	assertBlocks(t, done, 100*time.Millisecond,
		"admitWrite returned while over budget with an upload in flight: no backpressure")

	hfs.addDirtyBytes(-db.BlockSize) // the drain: bytes uploaded and released
	assertReturns(t, done, time.Second,
		"admitWrite did not wake after dirtyBytes fell under budget")
}

// TestAdmitWriteWakesWhenLastUploadDrains covers the other wake path, the one the
// dedicated wakeWriters() call exists for: the budget is still exhausted, but the
// last in-flight upload finishes, so the exemption now applies and the writer must
// be released. A falling dirtyBytes alone would not cover this — the bytes freeing
// the writer belong to a different, still-open file.
func TestAdmitWriteWakesWhenLastUploadDrains(t *testing.T) {
	hfs := &HamstorFS{WriteBuffer: db.BlockSize}
	hfs.addDirtyBytes(10 * db.BlockSize) // over budget, and it stays that way
	hfs.InflightCount.Store(1)

	done := admitInBackground(hfs, 1)
	assertBlocks(t, done, 100*time.Millisecond,
		"admitWrite returned while over budget with an upload in flight")

	// The last upload drains without dropping us under budget (its bytes belonged
	// to another open file). The exemption now applies; wakeWriters must release.
	hfs.InflightCount.Add(-1)
	hfs.wakeWriters()
	assertReturns(t, done, time.Second,
		"admitWrite did not wake when the last upload drained: the exemption was not re-evaluated")
}
