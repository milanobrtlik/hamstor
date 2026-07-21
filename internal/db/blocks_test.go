package db

import (
	"database/sql"
	"fmt"
	"slices"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	database, err := Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

// newFile inserts a regular file under the root inode.
func newFile(t *testing.T, d *DB, name string) int64 {
	t.Helper()
	id, err := d.InsertInode(1, name, 0o100644, "pending")
	if err != nil {
		t.Fatalf("insert inode %q: %v", name, err)
	}
	return id
}

// TestAllS3KeySetIncludesBlocks guards the single most destructive omission in
// the block layout: ops.GC deletes every object in the bucket that is not in
// AllS3KeySet, so a block set missing from that union means the first
// `hamstor gc` deletes every large file, in one batch, with no error anywhere.
//
// It needs no S3, deliberately: TestGCPhase1KeepsBlockObjects covers the same
// hazard end to end but skips without credentials, and this defense must not be
// skippable.
func TestAllS3KeySetIncludesBlocks(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "big.bin")

	committed, orphaned, err := d.CommitBlocks(id, []BlockCommit{
		{Index: 0, S3Key: "aa/block-zero", Size: BlockSize},
		{Index: 1, S3Key: "bb/block-one", Size: 100},
	}, BlockSize+100)
	if err != nil {
		t.Fatalf("commit blocks: %v", err)
	}
	if !committed {
		t.Fatal("commit blocks reported the inode as gone")
	}
	if len(orphaned) != 0 {
		t.Fatalf("first commit orphaned %v, want nothing", orphaned)
	}

	set, err := d.AllS3KeySet()
	if err != nil {
		t.Fatalf("all s3 key set: %v", err)
	}
	for _, key := range []string{"aa/block-zero", "bb/block-one"} {
		if _, ok := set[key]; !ok {
			t.Errorf("block key %q is missing from AllS3KeySet: gc would delete it as an orphan", key)
		}
	}
}

func TestCommitBlocksReplacesAndTruncates(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "big.bin")

	// Three full blocks.
	if _, _, err := d.CommitBlocks(id, []BlockCommit{
		{Index: 0, S3Key: "k0", Size: BlockSize},
		{Index: 1, S3Key: "k1", Size: BlockSize},
		{Index: 2, S3Key: "k2", Size: BlockSize},
	}, 3*BlockSize); err != nil {
		t.Fatalf("initial commit: %v", err)
	}

	// Rewrite block 1 only, same size. Block 0 and 2 are untouched and must not
	// be reported as orphans — a flush only ever hands over the dirty blocks, so
	// returning the whole previous set here would delete live data.
	_, orphaned, err := d.CommitBlocks(id, []BlockCommit{
		{Index: 1, S3Key: "k1-new", Size: BlockSize},
	}, 3*BlockSize)
	if err != nil {
		t.Fatalf("rewrite block 1: %v", err)
	}
	if !slices.Equal(orphaned, []string{"k1"}) {
		t.Fatalf("rewrite orphaned %v, want [k1]", orphaned)
	}

	blocks, err := d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode: %v", err)
	}
	want := []BlockCommit{
		{Index: 0, S3Key: "k0", Size: BlockSize},
		{Index: 1, S3Key: "k1-new", Size: BlockSize},
		{Index: 2, S3Key: "k2", Size: BlockSize},
	}
	if !slices.Equal(blocks, want) {
		t.Fatalf("blocks = %v, want %v", blocks, want)
	}

	// Shrink to one and a half blocks: block 2 falls off the end.
	_, orphaned, err = d.CommitBlocks(id, nil, BlockSize+BlockSize/2)
	if err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if !slices.Equal(orphaned, []string{"k2"}) {
		t.Fatalf("shrink orphaned %v, want [k2]", orphaned)
	}
	blocks, err = d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode after shrink: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("after shrink: %d blocks, want 2", len(blocks))
	}

	// The stored object of block 1 is still a whole block even though the file
	// now ends mid-block: blocks.size is the object's length, not the live
	// extent, and the file length comes from inodes.size alone.
	if blocks[1].Size != BlockSize {
		t.Errorf("block 1 size = %d, want %d (the stored object was not rewritten)", blocks[1].Size, int64(BlockSize))
	}
	meta, err := d.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != BlockSize+BlockSize/2 {
		t.Errorf("inode size = %d, want %d", meta.Size, BlockSize+BlockSize/2)
	}
	if meta.Status != "committed" {
		t.Errorf("inode status = %q, want committed", meta.Status)
	}

	// Truncating to empty drops everything.
	_, orphaned, err = d.CommitBlocks(id, nil, 0)
	if err != nil {
		t.Fatalf("truncate to empty: %v", err)
	}
	if !slices.Equal(orphaned, []string{"k0", "k1-new"}) {
		t.Fatalf("truncate orphaned %v, want [k0 k1-new]", orphaned)
	}
	blocks, err = d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode after truncate: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("after truncate to empty: %d blocks, want 0", len(blocks))
	}
}

// A block past the new end of file is a caller bug: committing the row would
// upsert it and then delete it again in the same transaction, leaving an object
// in the bucket that no row has ever referenced.
func TestCommitBlocksRejectsBlockPastEOF(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "big.bin")

	if _, _, err := d.CommitBlocks(id, []BlockCommit{{Index: 1, S3Key: "k1", Size: 10}}, 10); err == nil {
		t.Fatal("committing block 1 of a 10-byte file succeeded, want an error")
	}
	blocks, err := d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("rejected commit wrote %d rows", len(blocks))
	}
}

// Growing past MaxNeedleSize turns a needle into blocks. The volume it used to
// live in must be decremented in the commit's own transaction, or every such
// growth leaves the volume inflated by a dead needle nobody subtracts.
func TestCommitBlocksDecrementsVolume(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "grew.bin")
	sibling := newFile(t, d, "stayed.bin")

	// A staged small file is committed with an empty key before the builder
	// packs it; the builder only ever moves committed inodes into a volume.
	for _, staged := range []int64{id, sibling} {
		if _, err := d.CommitInode(staged, "", 1024); err != nil {
			t.Fatalf("commit staged inode %d: %v", staged, err)
		}
	}

	// Two needles in one volume, so the assertion below can tell a decrement of
	// exactly this needle apart from the volume being zeroed wholesale.
	if err := d.InsertVolume("vol-1", 4096, 0, 0, 0, "open"); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	if _, err := d.CommitNeedlesToVolume("vol-1", 4096, []NeedleCommit{
		{InodeID: id, Offset: 0, Size: 1024},
		{InodeID: sibling, Offset: 1024, Size: 1024},
	}, false, ""); err != nil {
		t.Fatalf("commit needles: %v", err)
	}

	_, orphaned, err := d.CommitBlocks(id, []BlockCommit{{Index: 0, S3Key: "k0", Size: 300000}}, 300000)
	if err != nil {
		t.Fatalf("commit blocks: %v", err)
	}
	// The volume object is shared with other needles; only GC phase 3 may
	// delete it, so it must never come back as an orphan here.
	if slices.Contains(orphaned, "vol-1") {
		t.Fatalf("orphaned %v contains the volume object, which is shared", orphaned)
	}

	vols, err := d.GetVolumesForCompaction(0)
	if err != nil {
		t.Fatalf("query volumes: %v", err)
	}
	var found bool
	for _, v := range vols {
		if v.S3Key != "vol-1" {
			continue
		}
		found = true
		if v.LiveCount != 1 {
			t.Errorf("volume live_count = %d, want 1 (the sibling needle)", v.LiveCount)
		}
		if v.LiveSize != 1024 {
			t.Errorf("volume live_size = %d, want 1024 (the sibling needle)", v.LiveSize)
		}
	}
	if !found {
		t.Fatal("volume vol-1 disappeared")
	}

	meta, err := d.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.VolS3Key != "" || meta.VolSize != 0 {
		t.Errorf("inode still references volume %q size %d", meta.VolS3Key, meta.VolSize)
	}
}

// A file stored as one whole-file object before the block layout keeps its
// s3_key until something overwrites it. CommitBlocks must clear it and hand it
// back: leaving it set would keep the read path serving the pre-overwrite
// object and keep AllS3KeySet protecting it forever.
func TestCommitBlocksClearsWholeFileKey(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "legacy.bin")

	if _, err := d.CommitInode(id, "old-whole-file", 500000); err != nil {
		t.Fatalf("commit whole file: %v", err)
	}

	_, orphaned, err := d.CommitBlocks(id, []BlockCommit{{Index: 0, S3Key: "k0", Size: 400000}}, 400000)
	if err != nil {
		t.Fatalf("commit blocks: %v", err)
	}
	if !slices.Contains(orphaned, "old-whole-file") {
		t.Fatalf("orphaned %v does not contain the replaced whole-file key", orphaned)
	}

	meta, err := d.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.S3Key != "" {
		t.Errorf("inode still points at whole-file object %q", meta.S3Key)
	}

	set, err := d.AllS3KeySet()
	if err != nil {
		t.Fatalf("all s3 key set: %v", err)
	}
	if _, ok := set["old-whole-file"]; ok {
		t.Error("the replaced whole-file key is still protected by AllS3KeySet")
	}
}

func TestCommitBlocksMissingInode(t *testing.T) {
	d := openTestDB(t)

	committed, orphaned, err := d.CommitBlocks(9999, []BlockCommit{{Index: 0, S3Key: "k0", Size: 10}}, 10)
	if err != nil {
		t.Fatalf("commit blocks for a missing inode: %v", err)
	}
	if committed {
		t.Error("committed = true for an inode that does not exist")
	}
	if len(orphaned) != 0 {
		t.Errorf("orphaned = %v, want nothing", orphaned)
	}
}

func TestDeleteBlocksForInode(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "big.bin")

	if _, _, err := d.CommitBlocks(id, []BlockCommit{
		{Index: 0, S3Key: "k0", Size: BlockSize},
		{Index: 1, S3Key: "k1", Size: 10},
	}, BlockSize+10); err != nil {
		t.Fatalf("commit blocks: %v", err)
	}

	keys, err := d.DeleteBlocksForInode(id)
	if err != nil {
		t.Fatalf("delete blocks: %v", err)
	}
	if !slices.Equal(keys, []string{"k0", "k1"}) {
		t.Fatalf("deleted keys = %v, want [k0 k1]", keys)
	}
	blocks, err := d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("%d block rows survived the delete", len(blocks))
	}
}

// The table arrives by migration, so the case that matters is a database
// written before it existed: Open must add it, twice in a row must be a no-op,
// and a file still stored as one whole-file object must keep working alongside
// blocks while inodes.s3_key is still around.
func TestBlocksMigrationOnPreExistingDB(t *testing.T) {
	path := t.TempDir() + "/legacy.db"

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`
		CREATE TABLE inodes (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER NOT NULL,
			name TEXT NOT NULL, mode INTEGER NOT NULL, size INTEGER NOT NULL DEFAULT 0, s3_key TEXT,
			status TEXT NOT NULL DEFAULT 'committed', mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL,
			uid INTEGER NOT NULL DEFAULT 0, gid INTEGER NOT NULL DEFAULT 0, symlink_target TEXT,
			UNIQUE(parent_id, name));
		CREATE TABLE config (key TEXT PRIMARY KEY, value BLOB NOT NULL);
		INSERT INTO inodes (id, parent_id, name, mode, status, mtime_ns, ctime_ns)
			VALUES (1, 0, '', 16877, 'committed', 1, 1);
		INSERT INTO inodes (id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns)
			VALUES (2, 1, 'whole.bin', 33188, 9, 'aa/legacy', 'committed', 1, 1);`); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}
	raw.Close()

	// Second open exercises the config-guarded migration doing nothing.
	for i := range 2 {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		id := newFile(t, d, fmt.Sprintf("blocks-%d.bin", i))
		if _, _, err := d.CommitBlocks(id,
			[]BlockCommit{{Index: 0, S3Key: fmt.Sprintf("bb/block-%d", i), Size: 9}}, 9); err != nil {
			t.Fatalf("commit blocks after open %d: %v", i, err)
		}
		set, err := d.AllS3KeySet()
		if err != nil {
			t.Fatalf("all s3 key set after open %d: %v", i, err)
		}
		for _, key := range []string{"aa/legacy", fmt.Sprintf("bb/block-%d", i)} {
			if _, ok := set[key]; !ok {
				t.Errorf("open %d: %q missing from AllS3KeySet", i, key)
			}
		}
		d.Close()
	}
}

// Deleting the inode takes its blocks with it through ON DELETE CASCADE, which
// is why the keys have to be read before the row goes: afterwards nothing knows
// them, and the objects can only be found by a bucket listing.
func TestDeleteInodeCascadesToBlocks(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "big.bin")

	if _, _, err := d.CommitBlocks(id, []BlockCommit{{Index: 0, S3Key: "k0", Size: 10}}, 10); err != nil {
		t.Fatalf("commit blocks: %v", err)
	}
	if err := d.DeleteInode(id); err != nil {
		t.Fatalf("delete inode: %v", err)
	}

	blocks, err := d.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("%d block rows outlived their inode", len(blocks))
	}
	set, err := d.AllS3KeySet()
	if err != nil {
		t.Fatalf("all s3 key set: %v", err)
	}
	if _, ok := set["k0"]; ok {
		t.Error("k0 is still in AllS3KeySet after its inode was deleted")
	}
}
