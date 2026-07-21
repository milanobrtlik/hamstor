package hfuse

import (
	"bytes"
	"context"
	"image"
	pngenc "image/png"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/db"
)

// Lazy materialization is what the block layout was for. Until this step a file
// was still read and written whole — reopening a 3 GB log to append one line
// downloaded 3 GB first. Now the backing store is sparse and a block arrives
// only when something actually touches it.
//
// The tests below split into two halves, and the second is the dangerous one.
// "It does not fetch what it does not need" fails loudly (EIO, or a wrong byte
// count). "It fetches what it is about to partially overwrite" fails SILENTLY:
// the flush uploads the whole dirty block, so whatever was never faulted goes to
// S3 as zeroes, and nobody finds out until that region is read again.

// dropObject deletes a block's object from the bucket while leaving its row
// intact, so that any code path which fetches that block fails loudly.
//
// This is how "it did not download" is asserted here. Counting requests would
// need a seam through s3store; destroying the object the step promises not to
// touch proves the same thing and cannot pass by accident — before this step
// every one of these tests returned EIO.
func dropObject(t *testing.T, hfs *HamstorFS, key string) {
	t.Helper()
	if err := hfs.Store.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete object %s: %v", key, err)
	}
}

// blockContent builds n blocks plus tail bytes, each block filled with its own
// letter so a misplaced or unfetched block cannot look correct.
func blockContent(blocks int, tail int) []byte {
	size := blocks*db.BlockSize + tail
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('A' + (i/db.BlockSize)%26)
	}
	return content
}

// TestOpenForWriteFetchesNoUntouchedBlock is the headline of this step. Opening
// a file for writing used to download all of it (the read-modify-write the block
// layout exists to retire); now it attaches a sparse store and fetches only what
// the write actually needs.
//
// Proved by destruction: the objects of the blocks the write does not touch are
// deleted from the bucket first. A preload that still downloads the file — or a
// flush that still rewrites every block — cannot survive that.
func TestOpenForWriteFetchesNoUntouchedBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(3, 0)
	id := mustInsert(t, hfs, "append-target.bin")
	writeAt(t, hfs, id, content, 0, true)

	before := blocksOf(t, hfs, id)
	if len(before) != 3 {
		t.Fatalf("want 3 blocks, got %d", len(before))
	}
	dropObject(t, hfs, before[0].S3Key)
	dropObject(t, hfs, before[2].S3Key)

	// Open for writing the way the kernel does, so the write preload in Open is
	// what is under test and not just the handle's own load path.
	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_RDWR))
	if errno != 0 {
		t.Fatalf("open for write: %v — the preload still fetched blocks it does not need", errno)
	}
	th := &TestHandle{h: fh.(*HamstorHandle)}

	patch := []byte("PATCHED")
	patchOff := int64(db.BlockSize) + 4096
	if errno := th.TestWriteAt(patch, patchOff); errno != 0 {
		t.Fatalf("write into block 1: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	after := blocksOf(t, hfs, id)
	if len(after) != 3 {
		t.Fatalf("want 3 blocks after the patch, got %d", len(after))
	}
	if after[0].S3Key != before[0].S3Key || after[2].S3Key != before[2].S3Key {
		t.Error("an untouched block was rewritten; the flush is not honouring the dirty set")
	}
	if after[1].S3Key == before[1].S3Key {
		t.Fatal("the patched block reused its key")
	}

	// The patched block must carry the WHOLE block, not just the patched bytes:
	// its unwritten remainder had to be faulted in before the write landed.
	got, gErr := hfs.Store.Download(context.Background(), after[1].S3Key)
	if gErr != nil {
		t.Fatalf("download patched block: %v", gErr)
	}
	want := make([]byte, db.BlockSize)
	copy(want, content[db.BlockSize:2*db.BlockSize])
	copy(want[4096:], patch)
	if !bytes.Equal(got, want) {
		t.Fatal("the rewritten block does not match the original block with the patch applied")
	}
}

// TestReadFetchesOnlyTheBlocksItServes is the read half: a small read of a large
// file must fetch the block it lands in and nothing else. Same destructive
// proof — the other blocks' objects are gone.
func TestReadFetchesOnlyTheBlocksItServes(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(3, 0)
	id := mustInsert(t, hfs, "read-one-block.bin")
	writeAt(t, hfs, id, content, 0, true)

	blocks := blocksOf(t, hfs, id)
	dropObject(t, hfs, blocks[0].S3Key)
	dropObject(t, hfs, blocks[1].S3Key)

	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()

	off := int64(2*db.BlockSize) + 1000
	got, errno := rh.TestRead(64, off)
	if errno != 0 {
		t.Fatalf("read inside block 2: %v — the read fetched blocks it does not serve", errno)
	}
	if !bytes.Equal(got, content[off:off+64]) {
		t.Fatalf("read %q, want %q", got[:8], content[off:off+8])
	}
}

// TestPartialBlockWritePreservesRestOfBlock guards the invariant whose violation
// is silent: dirtyBlocks must be a subset of presentBlocks, because a flush
// uploads whole blocks. A write of seven bytes that does not first fault the
// block in would send 8 MiB where everything except those seven bytes is zero,
// and the loss appears only when that region is read back.
//
// Both shapes are covered: a full interior block, and the short last block,
// whose live extent is not a whole BlockSize and so takes a different path
// through the coverage test.
func TestPartialBlockWritePreservesRestOfBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(2, 4096)
	id := mustInsert(t, hfs, "rmw.bin")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	// Inside a full block, and inside the short tail block.
	for _, off := range []int64{int64(db.BlockSize) + 999, int64(2*db.BlockSize) + 100} {
		patch := []byte("KEEPME!")
		writeAt(t, hfs, id, patch, off, false)
		copy(content[off:], patch)
		blocksOf(t, hfs, id)
	}

	got := readBack(t, hfs, id, len(content))
	if len(got) != len(content) {
		t.Fatalf("read back %d bytes, want %d", len(got), len(content))
	}
	if !bytes.Equal(got, content) {
		for i := range got {
			if got[i] != content[i] {
				t.Fatalf("first difference at %d: got %q, want %q — a partial write zeroed the rest of its block",
					i, got[i], content[i])
			}
		}
	}
}

// TestReadAcrossBlockBoundary reads a few bytes that straddle two blocks, so the
// read has to fault both and splice them at the right offset. A one-block
// off-by-one shows up here as the wrong letter rather than as something subtler
// later.
func TestReadAcrossBlockBoundary(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(2, 128)
	id := mustInsert(t, hfs, "boundary.bin")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()

	off := int64(db.BlockSize) - 4
	got, errno := rh.TestRead(8, off)
	if errno != 0 {
		t.Fatalf("read across the boundary: %v", errno)
	}
	if !bytes.Equal(got, content[off:off+8]) {
		t.Fatalf("read %q across the block boundary, want %q", got, content[off:off+8])
	}
}

// TestReadStraddlesHole covers the other absence. A block with no row is a hole
// and must read as zeroes WITHOUT being fetched; a block that has a row but is
// not local must be fetched. Confusing the two is a bug in either direction, and
// a read that crosses from one into the other exercises both in a single call.
func TestReadStraddlesHole(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "straddle.bin")
	tail := []byte("far away")
	off := int64(5 * db.BlockSize)
	writeAt(t, hfs, id, tail, off, true)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 1 {
		t.Fatalf("want 1 block for a sparse write, got %d", len(blocks))
	}

	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()

	got, errno := rh.TestRead(8, off-4)
	if errno != 0 {
		t.Fatalf("read across the hole boundary: %v", errno)
	}
	want := append(make([]byte, 4), tail[:4]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("read %q at the hole boundary, want %q", got, want)
	}
}

// TestSparseWriteAfterFtruncateMaterializesOneBlock is what `dd seek=` actually
// does, and it is not what TestSparseWriteMaterializesOneBlock does.
//
// dd, truncate(1) and every preallocating downloader call ftruncate BEFORE the
// first write, so inodes.size is already large when that write arrives. A flush
// that decides "this file has prior storage to preserve" from size alone then
// rewrites the whole file: measured on a live mount, `dd bs=1M seek=4096 count=1`
// on a new file committed 513 block rows and pushed 4 GiB of zeroes to S3 for one
// kilobyte of data. The existing sparse test writes at an offset without the
// ftruncate, so it cannot see this at all — which is exactly why driving a real
// mount was worth doing.
func TestSparseWriteAfterFtruncateMaterializesOneBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	const size = 4 << 30 // 4 GiB, as dd bs=1M seek=4096 produces
	id := mustInsert(t, hfs, "preallocated.bin")

	th := NewTestHandle(hfs, id, true)
	// ftruncate first, exactly as dd does, and through the path truncate(2)
	// takes: the shared state exists but has not loaded anything yet.
	n := &HamstorNode{hfs: hfs, inodeID: id}
	setSize(t, n, size)

	tail := []byte("written at the very end")
	if errno := th.TestWriteAt(tail, size); errno != 0 {
		t.Fatalf("sparse write: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 1 {
		t.Fatalf("a %d-byte sparse write into a preallocated %d-byte file produced %d block rows, want 1: the hole was materialized",
			len(tail), int64(size), len(blocks))
	}
	if blocks[0].Index != size/db.BlockSize {
		t.Fatalf("block index %d, want %d", blocks[0].Index, int64(size/db.BlockSize))
	}

	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()
	got, errno := rh.TestRead(len(tail), size)
	if errno != 0 {
		t.Fatalf("read the tail: %v", errno)
	}
	if !bytes.Equal(got, tail) {
		t.Fatalf("tail reads %q, want %q", got, tail)
	}
}

// TestTruncateOnNewFileSurvivesFlush covers `truncate -s 5T newfile`, which ended
// up as a 0-byte file.
//
// A brand-new file that is closed without a write commits at size zero, which is
// almost always right — except that ftruncate can have set a size without
// dirtying anything, and with nothing loaded there is no buffer for
// truncateWriteState to resize, so only the DB knows. The kernel sends a FLUSH
// immediately after CREATE and another on close, and the second one committed
// over the size the truncate had just written.
func TestTruncateOnNewFileSurvivesFlush(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	const huge = int64(5) << 40 // 5 TB
	id := mustInsert(t, hfs, "preallocate-me.bin")
	th := NewTestHandle(hfs, id, true)

	// The FLUSH the kernel sends straight after CREATE, before anything happens.
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush after create: %v", errno)
	}

	n := &HamstorNode{hfs: hfs, inodeID: id}
	setSize(t, n, huge)

	// Closing the file must not undo it.
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush on close: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != huge {
		t.Fatalf("size is %d after closing, want %d: the flush committed over the truncate", meta.Size, huge)
	}
	if blocks := blocksOf(t, hfs, id); len(blocks) != 0 {
		t.Fatalf("preallocating produced %d block(s), want none", len(blocks))
	}
}

// TestPreallocatedFileReadsAsHoles covers the shape `truncate -s 4G` leaves
// behind: committed, size set, and no storage anywhere. That is a sparse file,
// not a broken one, and every read path has to say so.
//
// It used to be read as "staged but the staging file is missing", because those
// two states are identical in the inode row. The read then spent five retries
// hunting for a staging file that never existed and returned EIO — for a plain
// `dd if=/dev/zero of=X bs=1M seek=4096 count=1`, which is how the failure was
// found on a live mount. What tells them apart is the size: flushStaged is only
// reached at or below MaxNeedleSize, so anything bigger was never staged.
func TestPreallocatedFileReadsAsHoles(t *testing.T) {
	hfs, _ := setupStaging(t) // the builder must be present: it is what made this ambiguous

	const size = 4 << 30
	id := mustInsert(t, hfs, "preallocated.bin")
	if _, err := hfs.DB.CommitInode(id, 0); err != nil {
		t.Fatalf("commit empty: %v", err)
	}
	n := &HamstorNode{hfs: hfs, inodeID: id}
	setSize(t, n, size)

	rh := NewTestHandle(hfs, id, false)
	defer rh.TestRelease()
	got, errno := rh.TestRead(32, size/2)
	if errno != 0 {
		t.Fatalf("read inside a preallocated file: %v — it was mistaken for a staged file whose data is gone", errno)
	}
	if !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("a hole reads %q, want zeroes", got)
	}

	// It must not be reported as unreadable either: that warning is the signal
	// for a DB restored without its staging disk, and it is worthless if ordinary
	// sparse files set it off at every boot.
	staged, err := hfs.DB.GetStagedInodes()
	if err != nil {
		t.Fatalf("staged inodes: %v", err)
	}
	for _, s := range staged {
		if s.ID == id {
			t.Fatal("a preallocated sparse file is reported as committed-with-no-data")
		}
	}
}

// TestTruncateUpBeyondMaterializableSize covers ftruncate UP, which must stay a
// metadata change. Growing a file to 5 TB is legal and instant on a real
// filesystem, and it has to be here too: materializing the gap would mean
// allocating five terabytes of heap (the old code appended the zeroes to buf and
// would have killed the mount) or writing 655360 objects of zeroes.
func TestTruncateUpBeyondMaterializableSize(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(0, 4096)
	id := mustInsert(t, hfs, "grow-huge.bin")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	const huge = int64(5) << 40 // 5 TB

	th := NewTestHandle(hfs, id, false)
	// Load the state first, the way a truncate on an open file finds it.
	if _, errno := th.TestRead(16, 0); errno != 0 {
		t.Fatalf("read: %v", errno)
	}
	if errno := hfs.truncateWriteState(th.h.st, huge); errno != 0 {
		t.Fatalf("truncate up: %v", errno)
	}

	th.h.st.mu.Lock()
	size := th.h.st.size
	spilled := th.h.st.spillFile != nil
	th.h.st.mu.Unlock()
	if size != huge {
		t.Fatalf("state size %d after growing, want %d", size, huge)
	}
	if !spilled {
		t.Fatal("growing past the spill threshold kept the file on the heap; 5 TB of zeroes would be allocated")
	}

	// Reading deep inside the gap must serve zeroes without fetching anything.
	got, errno := th.TestRead(32, huge-int64(db.BlockSize))
	if errno != 0 {
		t.Fatalf("read inside the gap: %v", errno)
	}
	if !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("the gap reads %q, want zeroes", got)
	}

	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush after growing: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != huge {
		t.Fatalf("committed size %d, want %d", meta.Size, huge)
	}
	after := blocksOf(t, hfs, id)
	if len(after) != 1 {
		t.Fatalf("growing to 5 TB produced %d block rows, want 1 — the gap was materialized", len(after))
	}
}

// TestSetattrShrinkWithoutHandleDropsBlocks is the regression test for the last
// item of the design's call-site inventory, and the one place in this step where
// the old behaviour was already wrong rather than merely slow.
//
// truncate(2) on a path with nothing open never reaches Flush: tryAcquireWrite
// returns nil, so CommitBlocks never runs and the block rows past the new end
// survive. Reads stay correct because readLoaded clamps to the size, which is
// exactly why nobody noticed — until the file is grown again and the surviving
// blocks serve their old contents where it must read as zeroes. Measured before
// the fix: 8 MiB of stale data came back.
func TestSetattrShrinkWithoutHandleDropsBlocks(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	const full = 3 * db.BlockSize
	content := bytes.Repeat([]byte("z"), full)
	id := mustInsert(t, hfs, "shrink-no-handle.bin")
	writeAt(t, hfs, id, content, 0, true)

	before := blocksOf(t, hfs, id)
	if len(before) != 3 {
		t.Fatalf("want 3 blocks, got %d", len(before))
	}
	// No handle anywhere: this is the path the fix is about.
	if live := hfs.liveWriteStates(); live != 0 {
		t.Fatalf("a write state is still open (%d); the test would take the flush path instead", live)
	}

	n := &HamstorNode{hfs: hfs, inodeID: id}
	setSize(t, n, 16)

	after := blocksOf(t, hfs, id)
	if len(after) != 1 {
		t.Fatalf("truncate(2) to 16 bytes left %d block rows, want 1", len(after))
	}
	for _, i := range []int{1, 2} {
		if objectExists(t, hfs, before[i].S3Key) {
			t.Errorf("block %d's object survived the truncate; nothing references it and GC cannot see it", i)
		}
	}

	// Grow it back. The resurrected region must read as zeroes.
	setSize(t, n, full)

	got := readBack(t, hfs, id, full)
	if len(got) != full {
		t.Fatalf("read back %d bytes, want %d", len(got), full)
	}
	if !bytes.Equal(got[:16], content[:16]) {
		t.Fatal("the surviving head of the file changed")
	}
	for i := 16; i < full; i++ {
		if got[i] != 0 {
			t.Fatalf("byte %d after shrink-and-grow is %q, want zero: the old tail was resurrected", i, got[i])
		}
	}
}

// TestShrinkIntoBlockDoesNotResurrectItsTail is the case the design actually
// measured (1048560 bytes coming back on a 1 MiB file) and the one its stated
// fix does not reach: a file that fits inside a single block has nothing past
// the end to delete, so dropping whole blocks changes nothing at all here.
//
// Both routes to a shrink are covered, because they trim the block set through
// different code: truncate(2) with nothing open goes through SetAttr, while
// ftruncate on an open file dirties no block and so reaches CommitBlocks with an
// empty set.
func TestShrinkIntoBlockDoesNotResurrectItsTail(t *testing.T) {
	for _, tc := range []struct {
		name       string
		openHandle bool
	}{
		{"truncate(2), nothing open", false},
		{"ftruncate through an open handle", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hfs, _ := setupTest(t)
			hfs.SpillDir = t.TempDir()

			// One block, so the shrink cuts into it rather than between blocks.
			const full = 1 << 20
			content := bytes.Repeat([]byte("z"), full)
			id := mustInsert(t, hfs, "tail.bin")
			writeAt(t, hfs, id, content, 0, true)
			blocksOf(t, hfs, id)

			n := &HamstorNode{hfs: hfs, inodeID: id}
			if tc.openHandle {
				th := NewTestHandle(hfs, id, false)
				if _, errno := th.TestRead(16, 0); errno != 0 {
					t.Fatalf("read: %v", errno)
				}
				if errno := hfs.truncateWriteState(th.h.st, 16); errno != 0 {
					t.Fatalf("ftruncate: %v", errno)
				}
				if errno := th.TestFlush(); errno != 0 {
					t.Fatalf("flush: %v", errno)
				}
				th.WaitUpload()
				th.TestRelease()
				hfs.InflightUploads.Wait()
			} else {
				setSize(t, n, 16)
			}
			blocksOf(t, hfs, id)

			// Short view first: this part was already right, and stays right.
			if got := readBack(t, hfs, id, full); len(got) != 16 {
				t.Fatalf("the truncated file reads %d bytes, want 16", len(got))
			}

			// Now grow it back. Everything past the cut must be zeroes.
			setSize(t, n, full)
			got := readBack(t, hfs, id, full)
			if len(got) != full {
				t.Fatalf("read back %d bytes after growing, want %d", len(got), full)
			}
			for i := 16; i < full; i++ {
				if got[i] != 0 {
					t.Fatalf("byte %d is %q after shrink-and-grow, want zero: the block's old tail was resurrected",
						i, got[i])
				}
			}
		})
	}
}

// setSize drives Setattr the way truncate(2) does — no file handle.
func setSize(t *testing.T, n *HamstorNode, size int64) {
	t.Helper()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = uint64(size)
	var out fuse.AttrOut
	if errno := n.Setattr(context.Background(), nil, in, &out); errno != 0 {
		t.Fatalf("setattr to %d: %v", size, errno)
	}
	if out.Attr.Size != uint64(size) {
		t.Fatalf("setattr reported size %d, want %d", out.Attr.Size, size)
	}
}

// TestFlushRefusesUnmaterializedDirtyBlock covers the assertion that turns this
// step's silent failure mode into a loud one. The invariant (a dirty block is
// always present) is held by construction, which is precisely why it is checked:
// every previous step of this migration retired an assumption the compiler could
// not see, and this one would upload zeroes over live data.
func TestFlushRefusesUnmaterializedDirtyBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	content := blockContent(2, 0)
	id := mustInsert(t, hfs, "unmaterialized.bin")
	writeAt(t, hfs, id, content, 0, true)
	before := blocksOf(t, hfs, id)

	th := NewTestHandle(hfs, id, false)
	defer th.TestRelease()
	// Attach the state without faulting block 1.
	if _, errno := th.TestRead(16, 0); errno != 0 {
		t.Fatalf("read: %v", errno)
	}

	th.h.st.mu.Lock()
	if !th.h.st.blockBacked {
		th.h.st.mu.Unlock()
		t.Fatal("state is not block-backed; the test would guard nothing")
	}
	if th.h.st.present(1) {
		th.h.st.mu.Unlock()
		t.Fatal("block 1 is already present; the read faulted more than it served")
	}
	// Claim block 1 changed without materializing it — what a Write that skipped
	// materializeForWrite would leave behind.
	th.h.st.dirtyBlocks = map[int64]struct{}{1: {}}
	th.h.st.dirty = true
	th.h.st.mu.Unlock()

	if errno := th.TestFlush(); errno != syscall.EIO {
		t.Fatalf("flush returned %v, want EIO: it was about to upload a block of zeroes", errno)
	}

	after := blocksOf(t, hfs, id)
	if len(after) != len(before) || after[1].S3Key != before[1].S3Key {
		t.Fatal("the refused flush still changed the committed block set")
	}
	if !objectExists(t, hfs, before[1].S3Key) {
		t.Fatal("the refused flush deleted a live object")
	}
}

// TestThumbnailSkippedAfterPartialOverwrite is the wiring half of the thumbnail
// contract: the predicate test below pins what the rule says, this one pins that
// the flush actually obeys it.
//
// The failure it guards is the nastiest kind in this step — a preview rendered
// from a snapshot full of holes, written into the freedesktop cache as a
// perfectly valid-looking thumbnail, never corrected. Skipping instead leaves
// the previous thumbnail stale, which the viewer repairs on its own by reading
// the file through the mount.
//
// The fixture is a real PNG followed by padding, which decoders accept (the
// stream ends at IEND): that keeps the file above one block without the cost of
// encoding megabytes of noise, and it puts the image data in block 0 so a
// partial commit of block 0 WOULD still decode. Without the guard a thumbnail
// really would be produced here, so the assertion is not vacuous.
func TestThumbnailSkippedAfterPartialOverwrite(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.Mountpoint = t.TempDir()
	hfs.ThumbSem = make(chan struct{}, 2)
	cacheHome := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	thumbDir := filepath.Join(cacheHome, "thumbnails")

	var png bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for i := range img.Pix {
		img.Pix[i] = byte(i)
	}
	if err := pngenc.Encode(&png, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	content := make([]byte, 2*db.BlockSize)
	copy(content, png.Bytes())

	id := mustInsert(t, hfs, "photo.png")
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id)

	// Positive control: a whole-file write does produce a thumbnail. Without
	// this the absence check below would pass for any reason at all.
	if !waitForThumb(t, thumbDir, true) {
		t.Fatal("no thumbnail after writing the whole file; the fixture proves nothing")
	}
	if err := os.RemoveAll(thumbDir); err != nil {
		t.Fatalf("clear thumbnails: %v", err)
	}

	// Patch a few bytes inside block 0, well past the image data. The commit
	// carries block 0 only, so the snapshot has a hole where block 1 belongs.
	writeAt(t, hfs, id, []byte("PATCHED"), 4096, false)
	blocksOf(t, hfs, id)
	if waitForThumb(t, thumbDir, false) {
		t.Fatal("a partial overwrite regenerated the thumbnail from a snapshot with holes")
	}

	// Control again: the machinery still works, so the absence above was the
	// guard and not a thumbnailer that had quietly stopped running.
	writeAt(t, hfs, id, content, 0, false)
	blocksOf(t, hfs, id)
	if !waitForThumb(t, thumbDir, true) {
		t.Fatal("a full rewrite did not regenerate the thumbnail")
	}
}

// waitForThumb polls the thumbnail cache. wantPresent picks the direction: it
// returns as soon as a thumbnail appears, and otherwise waits out the window so
// that "nothing was generated" means the thumbnailer had its chance.
func waitForThumb(t *testing.T, thumbDir string, wantPresent bool) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	if !wantPresent {
		deadline = time.Now().Add(500 * time.Millisecond)
	}
	for {
		found := false
		filepath.WalkDir(thumbDir, func(_ string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWholeFileSnapshotPredicate pins the thumbnail contract (see
// wholeFileSnapshot). It needs no S3 and so cannot be skipped, which matters:
// the failure it guards against is a thumbnail rendered from holes and then
// cached as a valid preview — silent, and never corrected on its own.
func TestWholeFileSnapshotPredicate(t *testing.T) {
	cases := []struct {
		name     string
		blocks   []db.BlockCommit
		lastLive int64
		want     bool
	}{
		{"whole small file", []db.BlockCommit{{Index: 0}}, 0, true},
		{"whole multi-block file", []db.BlockCommit{{Index: 0}, {Index: 1}, {Index: 2}}, 2, true},
		{"partial overwrite of one interior block", []db.BlockCommit{{Index: 1}}, 2, false},
		{"partial overwrite starting at zero", []db.BlockCommit{{Index: 0}, {Index: 1}}, 2, false},
		{"sparse file with a hole", []db.BlockCommit{{Index: 0}, {Index: 2}}, 2, false},
		{"nothing committed", nil, 2, false},
		{"empty file", nil, -1, false},
	}
	for _, c := range cases {
		if got := wholeFileSnapshot(c.blocks, c.lastLive); got != c.want {
			t.Errorf("%s: wholeFileSnapshot = %v, want %v", c.name, got, c.want)
		}
	}
}
