package hfuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
	"github.com/milan/hamstor/internal/volume"
)

// The block layout stores a large file as N objects of db.BlockSize instead of
// one. These tests cover what that buys and what it risks: that a partial
// overwrite touches only the blocks it changed, that holes stay holes, that
// shrinking drops the objects past the new end, and that a file converting from
// some other storage shape carries all of itself across.

// blocksOf returns the inode's block rows and schedules the objects for
// deletion.
func blocksOf(t *testing.T, hfs *HamstorFS, id int64) []db.BlockCommit {
	t.Helper()
	blocks, err := hfs.DB.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode %d: %v", id, err)
	}
	t.Cleanup(func() {
		for _, b := range blocks {
			hfs.Store.Delete(context.Background(), b.S3Key)
		}
	})
	return blocks
}

// writeAt writes through a fresh handle on an existing inode and flushes it,
// the way a reopen-and-modify does.
func writeAt(t *testing.T, hfs *HamstorFS, id int64, data []byte, off int64, isNew bool) {
	t.Helper()
	th := NewTestHandle(hfs, id, isNew)
	if errno := th.TestWriteAt(data, off); errno != 0 {
		t.Fatalf("write at %d: %v", off, errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()
}

// objectExists reports whether the bucket still holds the key.
func objectExists(t *testing.T, hfs *HamstorFS, key string) bool {
	t.Helper()
	_, err := hfs.Store.Download(context.Background(), key)
	return err == nil
}

// TestBlocksSpanBlockBoundary is the base case: a file larger than one block
// becomes several objects, and reading it back reassembles them in order. A
// wrong offset anywhere in the assembly shows up here as scrambled content
// rather than as something subtler later.
func TestBlocksSpanBlockBoundary(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	// Distinct bytes per block so a misplaced block cannot look correct.
	size := 2*db.BlockSize + 4096
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('A' + (i / db.BlockSize))
	}

	id := mustInsert(t, hfs, "spanning.bin")
	writeAt(t, hfs, id, content, 0, true)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 3 {
		t.Fatalf("want 3 blocks for %d bytes, got %d", size, len(blocks))
	}
	for i, b := range blocks {
		if b.Index != int64(i) {
			t.Fatalf("block %d has index %d", i, b.Index)
		}
		want := int64(db.BlockSize)
		if i == 2 {
			want = 4096
		}
		if b.Size != want {
			t.Errorf("block %d stored size %d, want %d (blocks are not padded)", i, b.Size, want)
		}
	}

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != int64(size) {
		t.Fatalf("inode size %d, want %d", meta.Size, size)
	}
	if got := readBack(t, hfs, id, size); !bytes.Equal(got, content) {
		t.Fatalf("read back %d bytes; content differs from what was written", len(got))
	}
}

// TestPartialOverwriteRewritesOnlyTouchedBlock is the point of the whole layout,
// and the sharpest edge in D5's rules about orphaned keys. Rewriting a few bytes
// must replace exactly one object: the untouched blocks must keep their keys
// (returning the whole previous set as orphaned would delete the live data of a
// file that was just written correctly), and the superseded object must actually
// be deleted (leaving it makes every overwrite leak).
func TestPartialOverwriteRewritesOnlyTouchedBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	size := 3 * db.BlockSize
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('a' + (i / db.BlockSize))
	}

	id := mustInsert(t, hfs, "partial.bin")
	writeAt(t, hfs, id, content, 0, true)

	before := blocksOf(t, hfs, id)
	if len(before) != 3 {
		t.Fatalf("want 3 blocks, got %d", len(before))
	}

	// Poke a few bytes inside block 1 only.
	patch := []byte("PATCHED")
	patchOff := int64(db.BlockSize) + 128
	writeAt(t, hfs, id, patch, patchOff, false)
	copy(content[patchOff:], patch)

	after := blocksOf(t, hfs, id)
	if len(after) != 3 {
		t.Fatalf("want 3 blocks after the patch, got %d", len(after))
	}
	if after[0].S3Key != before[0].S3Key {
		t.Error("block 0 was rewritten by a write that never touched it")
	}
	if after[2].S3Key != before[2].S3Key {
		t.Error("block 2 was rewritten by a write that never touched it")
	}
	if after[1].S3Key == before[1].S3Key {
		t.Fatal("the patched block reused its key; readers would keep serving the old cache entry")
	}

	// The untouched blocks' objects must still be there: this is the assertion
	// that fails loudly if orphaned ever becomes "all the previous keys".
	for _, i := range []int{0, 2} {
		if !objectExists(t, hfs, after[i].S3Key) {
			t.Fatalf("block %d's object was deleted although the block is still live", i)
		}
	}
	if objectExists(t, hfs, before[1].S3Key) {
		t.Error("superseded block object left in the bucket")
	}

	if got := readBack(t, hfs, id, size); !bytes.Equal(got, content) {
		t.Fatal("read back does not match the patched content")
	}
}

// TestShrinkDropsBlocksPastEndOfFile covers the DELETE half of the commit. The
// rows must go, or the file's length would disagree with its blocks and a later
// grow would resurrect the old bytes instead of reading zeroes; the objects must
// go, or they leak forever — GC cannot collect them while a row still points at
// them.
func TestShrinkDropsBlocksPastEndOfFile(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	size := 3 * db.BlockSize
	content := bytes.Repeat([]byte("z"), size)
	id := mustInsert(t, hfs, "shrink.bin")
	writeAt(t, hfs, id, content, 0, true)

	before := blocksOf(t, hfs, id)
	if len(before) != 3 {
		t.Fatalf("want 3 blocks, got %d", len(before))
	}

	// Truncate into block 0, then flush through an open handle.
	th := NewTestHandle(hfs, id, false)
	if errno := th.TestWriteAt([]byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	errno := hfs.truncateWriteState(th.h.st, 16)
	if errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	after := blocksOf(t, hfs, id)
	if len(after) != 1 {
		t.Fatalf("want 1 block after shrinking to 16 bytes, got %d", len(after))
	}
	for _, i := range []int{1, 2} {
		if objectExists(t, hfs, before[i].S3Key) {
			t.Errorf("block %d's object survived the shrink; nothing references it and GC cannot see it", i)
		}
	}

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != 16 {
		t.Fatalf("size %d, want 16", meta.Size)
	}
}

// TestSparseWriteMaterializesOneBlock is the property that makes holes free: a
// write far past the end of an empty file must produce ONE row, not one per
// block of the gap. Getting this wrong is not a correctness bug — the file reads
// back the same either way — which is exactly why it needs a test.
func TestSparseWriteMaterializesOneBlock(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "sparse.bin")
	tail := []byte("far away")
	off := int64(5 * db.BlockSize)
	writeAt(t, hfs, id, tail, off, true)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 1 {
		t.Fatalf("want 1 block for a sparse write at offset %d, got %d — the hole was materialized", off, len(blocks))
	}
	if blocks[0].Index != 5 {
		t.Fatalf("block index %d, want 5", blocks[0].Index)
	}

	// The hole reads as zeroes, and the tail is where it was put.
	got := readBack(t, hfs, id, int(off)+len(tail))
	if int64(len(got)) != off+int64(len(tail)) {
		t.Fatalf("read back %d bytes, want %d", len(got), off+int64(len(tail)))
	}
	if !bytes.Equal(got[off:], tail) {
		t.Fatalf("tail reads %q, want %q", got[off:], tail)
	}
	for i := int64(0); i < off; i++ {
		if got[i] != 0 {
			t.Fatalf("hole byte at %d is %q, want zero", i, got[i])
		}
	}
}

// TestEncryptedBlocksRoundTrip covers D6. Once a file is N objects each one must
// be independently decryptable, or the file is unreadable. crypto.Encrypt emits
// a fresh nonce per call, so calling it per block is what makes that true — and
// each stored object must be longer than its plaintext by the GCM overhead,
// which is the check that proves each was encrypted separately rather than once
// as a whole.
func TestEncryptedBlocksRoundTrip(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	enc, err := crypto.New("block-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("crypto: %v", err)
	}
	hfs.Encryptor = enc

	size := 2*db.BlockSize + 1024
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i)
	}

	id := mustInsert(t, hfs, "secret.bin")
	writeAt(t, hfs, id, content, 0, true)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 3 {
		t.Fatalf("want 3 blocks, got %d", len(blocks))
	}
	for _, b := range blocks {
		raw, dErr := hfs.Store.Download(context.Background(), b.S3Key)
		if dErr != nil {
			t.Fatalf("download block %d: %v", b.Index, dErr)
		}
		if !crypto.IsEncrypted(raw) {
			t.Fatalf("block %d went to S3 as plaintext", b.Index)
		}
		plain, decErr := enc.Decrypt(raw)
		if decErr != nil {
			t.Fatalf("block %d is not independently decryptable: %v", b.Index, decErr)
		}
		if int64(len(plain)) != b.Size {
			t.Fatalf("block %d decrypts to %d bytes, but the row says %d", b.Index, len(plain), b.Size)
		}
	}

	if got := readBack(t, hfs, id, size); !bytes.Equal(got, content) {
		t.Fatal("encrypted round trip does not match the plaintext written")
	}
}

// TestStagedFileGrowingIntoBlocksStaysReadable is C8 in the design's inventory,
// on the hot path. A staged file that outgrows MaxNeedleSize commits as blocks,
// which leaves it with neither an s3_key nor a vol_s3_key — the exact shape the
// read path used to read as "still staged". It would then hunt for a staging
// file the overwrite had correctly removed and return EIO for a healthy file.
func TestStagedFileGrowingIntoBlocksStaysReadable(t *testing.T) {
	hfs, _ := setupStaging(t)

	id := mustInsert(t, hfs, "grower.log")
	small := bytes.Repeat([]byte("s"), 1024)
	writeAt(t, hfs, id, small, 0, true)

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != int64(len(small)) {
		t.Fatalf("staged size %d, want %d", meta.Size, len(small))
	}
	if _, statErr := os.Stat(hfs.VolumeBuilder.StagePath(id)); statErr != nil {
		t.Fatalf("small file was not staged: %v", statErr)
	}

	// Grow it past MaxNeedleSize so the next flush takes the block path.
	big := bytes.Repeat([]byte("B"), volume.MaxNeedleSize+4096)
	writeAt(t, hfs, id, big, 0, false)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) == 0 {
		t.Fatal("growing past MaxNeedleSize did not store the file as blocks")
	}

	// The staging file must be gone: left behind, the builder would claim it,
	// pack it and keep retrying the commit on every notify.
	if _, statErr := os.Stat(hfs.VolumeBuilder.StagePath(id)); !os.IsNotExist(statErr) {
		t.Errorf("staging file outlived the block commit (stat err %v)", statErr)
	}

	if got := readBack(t, hfs, id, len(big)); !bytes.Equal(got, big) {
		t.Fatalf("read back %d bytes, want %d", len(got), len(big))
	}
}

// TestBlockFileShrinkingStaysBlocks pins the "once blocks, always blocks" rule.
// Falling back to volume staging would commit through CommitInode, which knows
// nothing about blocks: the rows would survive and the read path, which checks
// blocks first, would go on serving the pre-shrink version.
func TestBlockFileShrinkingStaysBlocks(t *testing.T) {
	hfs, _ := setupStaging(t)
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "shrinker.bin")
	big := bytes.Repeat([]byte("B"), volume.MaxNeedleSize+4096)
	writeAt(t, hfs, id, big, 0, true)
	if len(blocksOf(t, hfs, id)) == 0 {
		t.Fatal("large file was not stored as blocks")
	}

	// Overwrite with something small enough to be a needle.
	small := []byte("tiny")
	th := NewTestHandle(hfs, id, false)
	if errno := th.TestWriteAt(small, 0); errno != 0 {
		t.Fatalf("overwrite: %v", errno)
	}
	errno := hfs.truncateWriteState(th.h.st, int64(len(small)))
	if errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 1 {
		t.Fatalf("want the shrunken file to stay 1 block, got %d", len(blocks))
	}
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.VolS3Key != "" {
		t.Fatal("shrinking sent a block file back to volume staging; its blocks would keep serving the old content")
	}
	if got := readBack(t, hfs, id, 64); !bytes.Equal(got, small) {
		t.Fatalf("read back %q, want %q", got, small)
	}
}

// TestConvertingToBlocksRewritesUntouchedData guards the "converting rewrites
// the whole file" rule, which no other test reaches.
//
// CommitBlocks drops the previous storage wholesale, so a flush that converts an
// inode to blocks must write every block, not just the dirty ones — otherwise
// the part the writer never touched is left with no storage at all and reads
// back as a hole full of zeroes.
//
// The write has to be SPARSE to catch it. TestStagedFileGrowingIntoBlocksStays-
// Readable also converts, but by overwriting from offset 0, which dirties every
// block anyway and so passes either way. Here only block 1 is written, and block
// 0 survives solely because the flush noticed it was converting.
//
// This replaces a version built on a hand-made whole-file object. That shape is
// gone with inodes.s3_key; a needle or staging file is the only thing left to
// convert FROM, and both fit in block 0 — which is exactly what makes the
// sparse write the one way to still express the bug.
func TestConvertingToBlocksRewritesUntouchedData(t *testing.T) {
	hfs, _ := setupStaging(t)
	hfs.SpillDir = t.TempDir()

	// A small file, committed as a staging file — under MaxNeedleSize, so all of
	// it lives inside block 0.
	id := mustInsert(t, hfs, "grows-sparsely.bin")
	head := bytes.Repeat([]byte("h"), 1024)
	writeAt(t, hfs, id, head, 0, true)

	// Now write far past the end, in block 1. Only block 1 is dirty; block 0 is
	// carried over from the staging file the commit is about to drop.
	tailOff := int64(db.BlockSize) + 4096
	tail := bytes.Repeat([]byte("t"), 512)
	writeAt(t, hfs, id, tail, tailOff, false)

	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 2 {
		t.Fatalf("got %d blocks, want 2: converting from a staging file must rewrite block 0 too, "+
			"or the first %d bytes have no storage left", len(blocks), len(head))
	}

	size := int(tailOff) + len(tail)
	want := make([]byte, size)
	copy(want, head)
	copy(want[tailOff:], tail)
	if got := readBack(t, hfs, id, size); !bytes.Equal(got, want) {
		t.Fatal("converted file does not read back as it was written: the untouched head became a hole")
	}
}

// TestMultiBlockUploadFailureRetainsTheSet is the same test inverted, and it is
// the artefact that shows the debt from the block-layout step 3 is paid.
//
// It used to assert that a multi-block file retained NOTHING: the pending format
// was a single file named "<inode>.<size>", which cannot describe a set, so the
// honest behaviour was to say DATA LOST rather than half-retain something
// recovery would misread. Retention is now a directory of blocks, so the same
// failure must keep every block it meant to upload — including the ones that had
// already gone up before the failure, which nothing references and GC will
// remove.
//
// The rest is unchanged: the inode stays pending and the state stays poisoned,
// so no sibling can commit an empty file over the top of the retained copy.
func TestMultiBlockUploadFailureRetainsTheSet(t *testing.T) {
	cfg := testutil.RequireS3(t)
	hfs, dbPath := setupTest(t)
	hfs.SpillDir = t.TempDir()
	pendingDir := filepath.Join(filepath.Dir(dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatalf("pending dir: %v", err)
	}
	hfs.PendingDir = pendingDir

	// Point the store at a bucket that does not exist so every upload fails.
	badStore, err := s3store.New(context.Background(),
		"hamstor-no-such-bucket-"+strings.ToLower(t.Name()),
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	hfs.Store = badStore

	id := mustInsert(t, hfs, "doomed.bin")
	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt(bytes.Repeat([]byte("d"), 2*db.BlockSize), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	th.TestFlush()
	th.WaitUpload()

	// The set is one directory named by the bare inode number, and nothing else:
	// a leftover <id>.tmp-* would mean the commit rename never happened.
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		t.Fatalf("read pending dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != fmt.Sprintf("%d", id) || !entries[0].IsDir() {
		t.Fatalf("pending dir holds %v, want exactly the directory %d — a multi-block upload must retain its whole set",
			entryNames(entries), id)
	}
	if !hasRetainedData(pendingDir, id) {
		t.Fatal("hasRetainedData does not see the set, so Cleanup would delete the inode and orphan it")
	}

	set, err := readPendingSet(filepath.Join(pendingDir, fmt.Sprintf("%d", id)))
	if err != nil {
		t.Fatalf("retained set is unreadable, so recovery will refuse it: %v", err)
	}
	if set.FileSize != 2*db.BlockSize {
		t.Errorf("retained file size %d, want %d", set.FileSize, 2*db.BlockSize)
	}
	if len(set.Blocks) != 2 {
		t.Fatalf("retained %d block(s), want both — the blocks that uploaded before the failure are not in the "+
			"blocks table, so GC removes them and recovery cannot rely on them", len(set.Blocks))
	}
	for i, b := range set.Blocks {
		if b.Index != int64(i) || b.Size != db.BlockSize {
			t.Errorf("retained block %d = %+v, want index %d with a full extent", i, b, i)
		}
	}

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Fatalf("status %q, want pending", meta.Status)
	}
	if n := len(blocksOf(t, hfs, id)); n != 0 {
		t.Fatalf("failed flush committed %d block row(s); a row whose object does not exist is an unreadable file", n)
	}

	// A sibling must not be handed an empty buffer to build on.
	sib := NewTestHandle(hfs, id, false)
	if _, errno := sib.TestRead(16, 0); errno != syscall.EIO {
		t.Errorf("read on poisoned state: want EIO, got %v", errno)
	}
	sib.TestRelease()
	th.TestRelease()
}

// TestBlockFileIsNotReportedAsMissingData covers C7. Every block-stored file has
// an empty s3_key and an empty vol_s3_key, which is how the "committed but has
// no data in S3" check used to recognise a genuinely unreadable file. Without
// blocks in that predicate the mount warns about every healthy large file at
// every boot and fsck exits non-zero — destroying the signal for the case the
// check exists to catch.
func TestBlockFileIsNotReportedAsMissingData(t *testing.T) {
	hfs, dbPath := setupTest(t)
	hfs.SpillDir = t.TempDir()
	stagingDir := filepath.Join(filepath.Dir(dbPath), "staging")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("staging dir: %v", err)
	}

	id := mustInsert(t, hfs, "healthy.bin")
	writeAt(t, hfs, id, bytes.Repeat([]byte("h"), db.BlockSize+7), 0, true)
	blocksOf(t, hfs, id) // registers cleanup

	missing, err := CheckStagedData(hfs.DB, stagingDir)
	if err != nil {
		t.Fatalf("check staged data: %v", err)
	}
	for _, m := range missing {
		if m.ID == id {
			t.Fatal("a block-stored file was reported as having no data in S3")
		}
	}

	res, err := hfs.DB.Fsck()
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if res.StagedFiles != 0 {
		t.Fatalf("fsck counts %d file(s) as staged-with-no-S3-data; a healthy block file must not be one of them",
			res.StagedFiles)
	}
}
