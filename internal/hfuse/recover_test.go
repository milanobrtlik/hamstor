package hfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

// brokenStore returns a store aimed at a bucket that does not exist, so uploads
// fail the way an S3 outage does while the rest of the process keeps working.
func brokenStore(t *testing.T) *s3store.Store {
	t.Helper()
	cfg := testutil.RequireS3(t)
	store, err := s3store.New(context.Background(), "hamstor-no-such-bucket", cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create broken store: %v", err)
	}
	return store
}

// entryNames renders a directory listing for a failure message: which files are
// there matters more than how many.
func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// writeRetainedSet builds a retained block set by hand, the way a failed flush
// would have left one. blocks carry {index, plaintext} and are stored back to
// back; pad is appended to each stored body to stand in for the GCM overhead an
// encrypted mount adds, which is what makes stored length differ from logical
// extent.
func writeRetainedSet(t *testing.T, pendingDir string, inodeID, fileSize int64, pad int, blocks map[int64][]byte) string {
	t.Helper()
	dir := filepath.Join(pendingDir, fmt.Sprintf("%d", inodeID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("make retained dir: %v", err)
	}

	indexes := make([]int64, 0, len(blocks))
	for idx := range blocks {
		indexes = append(indexes, idx)
	}
	slices.Sort(indexes)

	var data []byte
	meta := pendingMeta{Version: pendingMetaVersion, FileSize: fileSize}
	for _, idx := range indexes {
		body := append(append([]byte{}, blocks[idx]...), bytes.Repeat([]byte{0xAB}, pad)...)
		meta.Blocks = append(meta.Blocks, pendingBlock{
			Index:  idx,
			Size:   int64(len(blocks[idx])),
			Off:    int64(len(data)),
			Stored: int64(len(body)),
		})
		data = append(data, body...)
	}
	if err := os.WriteFile(filepath.Join(dir, "data"), data, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta"), raw, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return dir
}

// TestCleanupKeepsPendingWithRetainedData is the regression test for silent data
// loss on a failed upload: Cleanup must not delete a pending inode whose bytes
// are still retained, because RecoverPending will finish that upload on a later
// start. Deleting it here throws away recoverable data and orphans the retained
// set — which is what happened before Cleanup consulted pendingDir.
func TestCleanupKeepsPendingWithRetainedData(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	retained, err := hfs.DB.InsertInode(1, "retained.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert retained: %v", err)
	}
	lost, err := hfs.DB.InsertInode(1, "lost.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert lost: %v", err)
	}

	// Only the first one has its bytes on disk.
	writeRetainedSet(t, pendingDir, retained, 4, 0, map[int64][]byte{0: []byte("data")})

	if err := Cleanup(hfs.DB, hfs.Store, pendingDir); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	if _, err := hfs.DB.GetInode(retained); err != nil {
		t.Errorf("pending inode with retained data was deleted: %v", err)
	}
	if _, err := hfs.DB.GetInode(lost); err == nil {
		t.Error("pending inode with no retained data should have been deleted")
	}
}

// truncatedThenFailed builds the shape this pair of tests is about: an inode
// that is 'committed' and yet has no storage at all.
//
// It is not a contrived state. go-fuse never negotiates CAP_ATOMIC_O_TRUNC, so
// the kernel implements open(O_TRUNC) as a VFS truncate — FUSE_SETATTR with
// size 0, sent BEFORE FUSE_OPEN. db.SetAttr then deletes every block row and
// Setattr deletes the objects, while the status stays 'committed'. Every
// overwrite of an existing file passes through it.
func truncatedThenFailed(t *testing.T, hfs *HamstorFS, name string, content []byte) int64 {
	t.Helper()
	id := mustInsert(t, hfs, name)
	writeAt(t, hfs, id, content, 0, true)
	if has, err := hfs.DB.HasBlocks(id); err != nil || !has {
		t.Fatalf("setup: the file should be stored as blocks first (has=%v err=%v)", has, err)
	}
	setSize(t, &HamstorNode{hfs: hfs, inodeID: id}, 0)

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("setup: get inode: %v", err)
	}
	has, err := hfs.DB.HasBlocks(id)
	if err != nil {
		t.Fatalf("setup: has blocks: %v", err)
	}
	if meta.Status != "committed" || meta.Size != 0 || has {
		t.Fatalf("setup: want the post-O_TRUNC shape (committed, size 0, no blocks), got %s/%d/has=%v",
			meta.Status, meta.Size, has)
	}
	return id
}

// TestFailedOverwriteAfterTruncateRetainsTheSet is the overwrite half of the
// retain/recover contract, and it was missing.
//
// Retention used to be gated on the inode having been 'pending' when the flush
// started, on the reasoning that a committed inode still has its previous
// version to fall back on. After open(O_TRUNC) that is false twice over: the
// truncate already deleted the old blocks AND their objects, so a failed upload
// dropped the only remaining copy of the file and logged "previous version kept,
// this write lost" — the reassuring sentence, for total loss. It is the same
// class of bug as ad0ff5f, left open for this case.
//
// So the question retention asks must be "does anything survive this failure?",
// not "was this inode pending?".
func TestFailedOverwriteAfterTruncateRetainsTheSet(t *testing.T) {
	cfg := testutil.RequireS3(t)
	hfs, dbPath := setupTest(t)
	hfs.SpillDir = t.TempDir()
	pendingDir := filepath.Join(filepath.Dir(dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatalf("pending dir: %v", err)
	}
	hfs.PendingDir = pendingDir

	id := truncatedThenFailed(t, hfs, "overwritten.bin", bytes.Repeat([]byte("o"), db.BlockSize+64))

	// From here the store is unreachable, so the rewrite's upload fails.
	badStore, err := s3store.New(context.Background(),
		"hamstor-no-such-bucket-"+strings.ToLower(t.Name()),
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	hfs.Store = badStore

	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	rewrite := bytes.Repeat([]byte("n"), db.BlockSize+64)
	th := NewTestHandle(hfs, id, false)
	if errno := th.TestWriteAt(rewrite, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	th.TestFlush()
	th.WaitUpload()
	th.TestRelease()

	if !hasRetainedData(pendingDir, id) {
		t.Fatalf("nothing was retained for inode %d: the rewrite is the only copy of this file, "+
			"because the truncate that preceded it already deleted the old blocks and their objects", id)
	}
	set, err := readPendingSet(pendingSetPath(pendingDir, id))
	if err != nil {
		t.Fatalf("retained set is unreadable, so recovery will refuse it: %v", err)
	}
	if set.FileSize != int64(len(rewrite)) {
		t.Errorf("retained file size %d, want %d", set.FileSize, len(rewrite))
	}

	// The inode goes back to 'pending', which is what the retained set means: it
	// has no durable storage. It also keeps RecoverPending's staleness test
	// honest, and hides the file rather than showing a 0-byte one the user might
	// overwrite or delete before the next start recovers it.
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Status != "pending" {
		t.Errorf("inode status %q after retention, want pending", meta.Status)
	}

	if strings.Contains(logs.String(), "previous version kept") {
		t.Errorf("the failure was reported as survivable, but the truncate left no previous version:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "data retained") {
		t.Errorf("retention happened but was not reported as such:\n%s", logs.String())
	}
}

// TestRecoverKeepsSetForCommittedInodeWithoutStorage is the other end of the
// same format, and it has to move with it: RecoverPending drops a set whose
// inode is already 'committed', on the reasoning that a later write made it
// durable. A committed inode with NO storage is the counter-example the
// truncate path creates, and dropping its set is the very loss retention just
// prevented.
//
// The stale case still has to work, so both are asserted here — one predicate,
// both directions.
func TestRecoverKeepsSetForCommittedInodeWithoutStorage(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()
	ctx := context.Background()

	// A committed inode that has no storage: its set is the only copy.
	orphaned, err := hfs.DB.InsertInode(1, "truncated.bin", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	body := []byte("the rewrite that never reached S3")
	writeRetainedSet(t, pendingDir, orphaned, int64(len(body)), 0, map[int64][]byte{0: body})

	// A committed inode that DOES have storage: its set is genuinely stale.
	durable, err := hfs.DB.InsertInode(1, "durable.bin", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	live := []byte("committed and durable")
	key := s3store.NewKey()
	if uErr := hfs.Store.Upload(ctx, key, live); uErr != nil {
		t.Fatalf("upload live block: %v", uErr)
	}
	t.Cleanup(func() { hfs.Store.Delete(ctx, key) })
	if _, _, cErr := hfs.DB.CommitBlocks(durable,
		[]db.BlockCommit{{Index: 0, S3Key: key, Size: int64(len(live))}}, int64(len(live))); cErr != nil {
		t.Fatalf("commit live block: %v", cErr)
	}
	writeRetainedSet(t, pendingDir, durable, 4, 0, map[int64][]byte{0: []byte("old!")})

	if err := RecoverPending(hfs.DB, hfs.Store, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	meta, err := hfs.DB.GetInode(orphaned)
	if err != nil {
		t.Fatalf("get recovered inode: %v", err)
	}
	if meta.Size != int64(len(body)) {
		t.Errorf("recovered size %d, want %d — the set for a committed inode with no storage was dropped as stale",
			meta.Size, len(body))
	}
	blocksOf(t, hfs, orphaned)
	if hasRetainedData(pendingDir, orphaned) {
		t.Error("the recovered set was left behind")
	}

	if hasRetainedData(pendingDir, durable) {
		t.Error("a set for a committed inode that already has storage must still be dropped as stale")
	}
	if m, gErr := hfs.DB.GetInode(durable); gErr != nil || m.Size != int64(len(live)) {
		t.Errorf("the durable inode was overwritten by its stale set: size %d, want %d (err %v)",
			m.Size, len(live), gErr)
	}
}

// TestHasRetainedDataMatchesExactInode pins the trap the design flagged for the
// retention rewrite. The old lookup globbed "<id>.*", and the hazard in that is
// not the dot but the prefix: "12*" also matches 123 and 1234, so one inode's
// retained set would keep a completely unrelated pending inode alive — and that
// inode would then never be recovered, because there is nothing under its own
// name to recover. The directory name is matched exactly.
//
// No S3, so this one never skips.
func TestHasRetainedDataMatchesExactInode(t *testing.T) {
	pendingDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(pendingDir, "123"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if !hasRetainedData(pendingDir, 123) {
		t.Error("inode 123 has a retained set and must be seen")
	}
	for _, other := range []int64{1, 12, 1234, 12345} {
		if hasRetainedData(pendingDir, other) {
			t.Errorf("inode %d has no retained set, but pending/123 was taken for one", other)
		}
	}

	// A set still being built is not a set: it is what a crash mid-retention
	// leaves, and Cleanup must not keep an inode alive on the strength of it.
	if err := os.MkdirAll(filepath.Join(pendingDir, "77.tmp-1"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if hasRetainedData(pendingDir, 77) {
		t.Error("an unfinished <id>.tmp-* set must not count as retained data")
	}
}

// TestRecoverPendingUploadsRetainedBytes verifies the recovery path end to end
// on a multi-block set: every block is uploaded and the whole set committed in
// one transaction, at its LOGICAL sizes — not the stored lengths, which are
// longer under encryption.
func TestRecoverPendingUploadsRetainedBytes(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	id, err := hfs.DB.InsertInode(1, "recovered.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	// Two blocks, sparse: block 1 is a hole and has no row, which recovery must
	// leave alone rather than invent. Each stored body is longer than its logical
	// extent, as ciphertext is.
	head := bytes.Repeat([]byte("h"), 4096)
	tail := bytes.Repeat([]byte("t"), 512)
	const pad = 29
	fileSize := 2*int64(db.BlockSize) + int64(len(tail))
	writeRetainedSet(t, pendingDir, id, fileSize, pad, map[int64][]byte{0: head, 2: tail})

	if err := RecoverPending(hfs.DB, hfs.Store, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode after recovery: %v", err)
	}
	if meta.Status != "committed" {
		t.Errorf("status = %q, want committed", meta.Status)
	}
	if meta.Size != fileSize {
		t.Errorf("size = %d, want the file size from the meta %d", meta.Size, fileSize)
	}

	// Recovery has to commit the set the way the flush would have: as blocks.
	// Committing it as anything else produces an inode the read path cannot find,
	// and then deletes the retained copy.
	blocks, err := hfs.DB.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks after recovery: %v", err)
	}
	t.Cleanup(func() {
		for _, b := range blocks {
			hfs.Store.Delete(context.Background(), b.S3Key)
		}
	})
	if len(blocks) != 2 || blocks[0].Index != 0 || blocks[1].Index != 2 {
		t.Fatalf("blocks after recovery = %+v, want exactly blocks 0 and 2 (block 1 is a hole)", blocks)
	}
	want := map[int64][]byte{0: head, 2: tail}
	for _, b := range blocks {
		if b.Size != int64(len(want[b.Index])) {
			t.Errorf("block %d size = %d, want the logical extent %d — recording the stored length %d instead "+
				"makes the file read long", b.Index, b.Size, len(want[b.Index]), len(want[b.Index])+pad)
		}
		got, err := hfs.Store.Download(context.Background(), b.S3Key)
		if err != nil {
			t.Fatalf("download recovered block %d: %v", b.Index, err)
		}
		wantStored := append(append([]byte{}, want[b.Index]...), bytes.Repeat([]byte{0xAB}, pad)...)
		if !bytes.Equal(got, wantStored) {
			t.Errorf("block %d uploaded %d bytes, want the retained bytes verbatim (%d)", b.Index, len(got), len(wantStored))
		}
	}

	if entries, _ := os.ReadDir(pendingDir); len(entries) != 0 {
		t.Errorf("retained set still present after successful recovery: %v", entryNames(entries))
	}
}

// TestRecoverPendingRefusesIncompleteSet covers what a crash mid-retention
// leaves behind. Only a directory named by a bare inode number and holding a
// meta that parses is a set; a half-built one is not, and neither is a file from
// a build that retained differently.
//
// Nothing of it may be committed — an incomplete set commits a file that is part
// zeroes — and nothing of it may be deleted either, because those bytes are
// still somebody's only copy. Naming them in the log is the whole remedy.
//
// No S3: the store is nil, so any attempt to upload would panic the test rather
// than pass quietly.
func TestRecoverPendingRefusesIncompleteSet(t *testing.T) {
	hfs := setupDBOnly(t)
	pendingDir := t.TempDir()

	// (a) data written, meta never landed.
	noMeta, err := hfs.DB.InsertInode(1, "no-meta.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	dir := writeRetainedSet(t, pendingDir, noMeta, 8, 0, map[int64][]byte{0: []byte("contents")})
	if err := os.Remove(filepath.Join(dir, "meta")); err != nil {
		t.Fatalf("remove meta: %v", err)
	}

	// (b) meta truncated mid-write.
	badMeta, err := hfs.DB.InsertInode(1, "bad-meta.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	dir = writeRetainedSet(t, pendingDir, badMeta, 8, 0, map[int64][]byte{0: []byte("contents")})
	raw, err := os.ReadFile(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta"), raw[:len(raw)/2], 0o600); err != nil {
		t.Fatalf("truncate meta: %v", err)
	}

	// (c) the temp directory a retention was still building, and (d) a file left
	// by the pre-block retention format.
	if err := os.MkdirAll(filepath.Join(pendingDir, "999.tmp-1"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pendingDir, "888.4"), []byte("old!"), 0o600); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	if err := RecoverPending(hfs.DB, nil, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	for _, id := range []int64{noMeta, badMeta} {
		if blocks, _ := hfs.DB.BlocksForInode(id); len(blocks) != 0 {
			t.Errorf("inode %d got %d block row(s) from an unusable set; the rows would point at "+
				"objects that were never uploaded", id, len(blocks))
		}
		m, err := hfs.DB.GetInode(id)
		if err != nil {
			t.Fatalf("inode %d gone: %v", id, err)
		}
		if m.Status != "pending" {
			t.Errorf("inode %d status = %q, want it left pending", id, m.Status)
		}
	}

	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		t.Fatalf("read pending dir: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("pending dir holds %v, want all four leftovers kept — recovery must never delete bytes it "+
			"cannot interpret, only name them", entryNames(entries))
	}
}

// TestRetainedSetCommitsAtomically covers the property the old format got for
// free by carrying its metadata in its filename: a retained set exists whole or
// not at all. It is built under <id>.tmp-* and renamed into place in one
// rename(2), so no crash can leave a visible set with a missing or half-written
// meta — which recovery would read as an incomplete block set, at a step whose
// only purpose is surviving a crash.
//
// No S3.
func TestRetainedSetCommitsAtomically(t *testing.T) {
	hfs := setupDBOnly(t)
	pendingDir := t.TempDir()
	hfs.PendingDir = pendingDir

	snap, err := os.CreateTemp(t.TempDir(), "snap-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer snap.Close()
	payload := bytes.Repeat([]byte("x"), 1024)
	if _, err := snap.WriteAt(payload, 0); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	blocks := []pendingBlock{{Index: 0, Size: int64(len(payload))}}
	retained, took := hfs.retainPendingUpload(42, int64(len(payload)), blocks, snap, snap.Name())
	if !retained {
		t.Fatal("retention failed")
	}
	if !took {
		t.Error("without encryption the snapshot IS the data file and must be moved in, not copied")
	}

	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		t.Fatalf("read pending dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "42" {
		t.Fatalf("pending dir holds %v, want only the committed set — a leftover <id>.tmp-* means the "+
			"rename that commits the set never happened", entryNames(entries))
	}

	set, err := readPendingSet(filepath.Join(pendingDir, "42"))
	if err != nil {
		t.Fatalf("a visible set must always parse: %v", err)
	}
	if len(set.Blocks) != 1 || set.Blocks[0].Size != int64(len(payload)) || set.FileSize != int64(len(payload)) {
		t.Fatalf("retained set = %+v, want one block of %d bytes", set, len(payload))
	}

	data, err := os.ReadFile(filepath.Join(pendingDir, "42", "data"))
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if !bytes.Equal(data[set.Blocks[0].Off:set.Blocks[0].Off+set.Blocks[0].Stored], payload) {
		t.Error("the retained bytes are not what the upload was going to send")
	}
}

// TestCheckStagedDataFindsRegularFiles is the regression test for the file-type
// mask: the staged query selected with "mode & S_IFLNK = 0", but S_IFREG
// (0x8000) and S_IFLNK (0xA000) share a bit, so every regular file was filtered
// out and the check reported nothing — including when the data was truly gone.
// The type must be matched through S_IFMT.
func TestCheckStagedDataFindsRegularFiles(t *testing.T) {
	hfs, _ := setupTest(t)
	stagingDir := t.TempDir()

	// Committed, no S3 key, no staging file: unreadable.
	lost, err := hfs.DB.InsertInode(1, "lost.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert lost: %v", err)
	}
	if _, err := hfs.DB.CommitInode(lost, 8); err != nil {
		t.Fatalf("commit lost: %v", err)
	}

	// Committed, no S3 key, but its bytes are staged: fine, awaiting packing.
	staged, err := hfs.DB.InsertInode(1, "staged.txt", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert staged: %v", err)
	}
	if _, err := hfs.DB.CommitInode(staged, 8); err != nil {
		t.Fatalf("commit staged: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, fmt.Sprintf("%d", staged)), []byte("contents"), 0o600); err != nil {
		t.Fatalf("write staging file: %v", err)
	}

	missing, err := CheckStagedData(hfs.DB, stagingDir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}

	if len(missing) != 1 {
		t.Fatalf("found %d unreadable files, want exactly 1 (regular files must not be filtered out by the type mask)", len(missing))
	}
	if missing[0].ID != lost {
		t.Errorf("reported inode %d (%s), want the one with no staging file", missing[0].ID, missing[0].Name)
	}
}

// TestRecoverPendingKeepsBytesWhenUploadFails verifies that a recovery attempt
// that cannot reach S3 leaves the retained set alone to retry on the next start,
// rather than dropping the only copy of the data. The set is multi-block, so it
// also covers the half-uploaded case: block 0 may well have gone up before the
// failure, and neither it nor the directory may be treated as progress.
func TestRecoverPendingKeepsBytesWhenUploadFails(t *testing.T) {
	hfs, _ := setupTest(t)
	pendingDir := t.TempDir()

	id, err := hfs.DB.InsertInode(1, "unreachable.bin", 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	path := writeRetainedSet(t, pendingDir, id, int64(db.BlockSize)+4, 0, map[int64][]byte{
		0: bytes.Repeat([]byte("a"), db.BlockSize),
		1: []byte("tail"),
	})

	// A store pointed at a bucket that does not exist: uploads fail, nothing else does.
	broken := brokenStore(t)
	if err := RecoverPending(hfs.DB, broken, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Errorf("retained bytes were dropped after a failed upload: %v", err)
	}
	if _, err := readPendingSet(path); err != nil {
		t.Errorf("retained set is no longer usable after a failed recovery: %v", err)
	}
	if blocks, _ := hfs.DB.BlocksForInode(id); len(blocks) != 0 {
		t.Errorf("a failed recovery committed %d block row(s); the set commits whole or not at all", len(blocks))
	}
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("inode gone: %v", err)
	}
	if meta.Status != "pending" {
		t.Errorf("status = %q, want it left pending for the next retry", meta.Status)
	}
}

// TestRetainedSetIsCiphertext is the whole reason retention re-encrypts from the
// snapshot instead of keeping the plaintext and sealing it at recovery time.
//
// RecoverPending takes a DB and a store and nothing else: it has no encryptor and
// no passphrase, by design, so a mount that comes back with a different one (or
// none) still finishes the upload. That only works if the retained bytes are
// already exactly what the object was going to be. The plaintext spill file is
// the nearest source of a block's bytes and retaining it would be the natural
// implementation — and it would kill this property silently, because nothing
// fails until someone reads the recovered file and gets ciphertext.
//
// The proof is end to end: retain under a failing store, recover without ever
// handing recovery the key, then read the file back through the encryptor and
// compare.
func TestRetainedSetIsCiphertext(t *testing.T) {
	cfg := testutil.RequireS3(t)
	hfs, dbPath := setupTest(t)
	hfs.SpillDir = t.TempDir()
	pendingDir := filepath.Join(filepath.Dir(dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatalf("pending dir: %v", err)
	}
	hfs.PendingDir = pendingDir

	enc, err := crypto.New("retain-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("crypto: %v", err)
	}
	hfs.Encryptor = enc

	good := hfs.Store
	bad, err := s3store.New(context.Background(), "hamstor-no-such-bucket-"+strings.ToLower(t.Name()),
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	hfs.Store = bad

	size := db.BlockSize + 4096
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i * 7)
	}

	id := mustInsert(t, hfs, "secret-doomed.bin")
	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt(content, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	th.TestFlush()
	th.WaitUpload()
	th.TestRelease()

	set, err := readPendingSet(filepath.Join(pendingDir, fmt.Sprintf("%d", id)))
	if err != nil {
		t.Fatalf("retained set: %v", err)
	}
	if len(set.Blocks) != 2 {
		t.Fatalf("retained %d block(s), want 2", len(set.Blocks))
	}
	data, err := os.ReadFile(filepath.Join(pendingDir, fmt.Sprintf("%d", id), "data"))
	if err != nil {
		t.Fatalf("read retained data: %v", err)
	}
	for _, b := range set.Blocks {
		if b.Stored <= b.Size {
			t.Errorf("block %d stores %d bytes for an extent of %d — it was retained as plaintext, so "+
				"recovery would need the passphrase it does not have", b.Index, b.Stored, b.Size)
		}
		if !crypto.IsEncrypted(data[b.Off : b.Off+b.Stored]) {
			t.Errorf("block %d was retained without a version byte: it is not what the upload would have sent", b.Index)
		}
	}

	// Recovery, with the store working again and no encryptor anywhere in reach.
	hfs.Store = good
	if err := RecoverPending(hfs.DB, good, pendingDir); err != nil {
		t.Fatalf("recover: %v", err)
	}
	blocks := blocksOf(t, hfs, id)
	if len(blocks) != 2 {
		t.Fatalf("after recovery: %d block row(s), want 2", len(blocks))
	}

	if got := readBack(t, hfs, id, size); !bytes.Equal(got, content) {
		t.Fatal("the recovered file does not decrypt back to what was written: the retained bytes were not ciphertext")
	}
}
