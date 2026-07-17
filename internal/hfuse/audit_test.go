package hfuse

import (
	"syscall"
	"testing"

	"github.com/milan/hamstor/internal/db"
)

// TestCommitInodeDecrementsVolumeAtomically verifies the fix for the
// non-atomic MarkNeedleDead + CommitInode bug: when a volume-packed inode is
// overwritten with a standalone object, CommitInode must decrement the old
// volume's live_count in the SAME transaction that clears the inode's vol
// columns, so a crash window can never leave a referenced volume at
// live_count=0 for GC to delete.
func TestCommitInodeDecrementsVolumeAtomically(t *testing.T) {
	hfs := setupDBOnly(t)
	database := hfs.DB

	id, err := database.InsertInode(1, "packed.txt", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}

	volKey := "aa/vol-commit-test"
	if err := database.InsertVolume(volKey, 0, 0, 0, 0, "open"); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	ids, err := database.CommitNeedlesToVolume(volKey, 100,
		[]db.NeedleCommit{{InodeID: id, Offset: 0, Size: 100, MtimeNs: 0}}, true, "")
	if err != nil {
		t.Fatalf("commit needle: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 committed needle, got %d", len(ids))
	}

	meta, err := database.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.VolS3Key != volKey {
		t.Fatalf("inode should reference volume, got vol_s3_key=%q", meta.VolS3Key)
	}

	// Overwrite the inode as a standalone object. CommitInode must atomically
	// decrement the volume's live_count to 0 and clear the vol columns.
	ok, err := database.CommitInode(id, "bb/new-standalone-key", 50)
	if err != nil {
		t.Fatalf("commit inode: %v", err)
	}
	if !ok {
		t.Fatal("commit inode should report the row as still present")
	}

	meta, err = database.GetInode(id)
	if err != nil {
		t.Fatalf("get inode after commit: %v", err)
	}
	if meta.VolS3Key != "" {
		t.Fatalf("vol_s3_key should be cleared, got %q", meta.VolS3Key)
	}
	if meta.S3Key != "bb/new-standalone-key" {
		t.Fatalf("s3_key=%q, want bb/new-standalone-key", meta.S3Key)
	}

	// The volume must now register as empty (live_count=0) so GC can reclaim it.
	empty, err := database.GetEmptyVolumes(0)
	if err != nil {
		t.Fatalf("get empty volumes: %v", err)
	}
	found := false
	for _, v := range empty {
		if v.S3Key == volKey {
			found = true
			if v.LiveCount != 0 {
				t.Fatalf("volume live_count=%d, want 0", v.LiveCount)
			}
		}
	}
	if !found {
		t.Fatal("volume should be empty (live_count=0) after overwrite, but was not reclaimable")
	}
}

// TestDeleteInodeWithVolumeNoDoubleDecrement verifies that after an overwrite
// already moved a needle off its volume (clearing vol_s3_key), a subsequent
// DeleteInodeWithVolume using the caller's stale volume key does NOT decrement
// that volume a second time, because it self-derives the reference from the row.
func TestDeleteInodeWithVolumeNoDoubleDecrement(t *testing.T) {
	hfs := setupDBOnly(t)
	database := hfs.DB

	// Two inodes packed into one volume so live_count starts at 2.
	id1, err := database.InsertInode(1, "a.txt", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert a: %v", err)
	}
	id2, err := database.InsertInode(1, "b.txt", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert b: %v", err)
	}
	volKey := "cc/vol-dd-test"
	if err := database.InsertVolume(volKey, 0, 0, 0, 0, "open"); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	if _, err := database.CommitNeedlesToVolume(volKey, 200, []db.NeedleCommit{
		{InodeID: id1, Offset: 0, Size: 100, MtimeNs: 0},
		{InodeID: id2, Offset: 100, Size: 100, MtimeNs: 0},
	}, true, ""); err != nil {
		t.Fatalf("commit needles: %v", err)
	}

	// Overwrite id1 (CommitInode decrements live_count 2 -> 1 and clears its vol ref).
	if _, err := database.CommitInode(id1, "dd/standalone", 10); err != nil {
		t.Fatalf("commit inode: %v", err)
	}

	// Now delete id1 passing the STALE volKey it used to reference. Because the
	// row's vol_s3_key is already NULL, no second decrement may happen.
	if err := database.DeleteInodeWithVolume(id1, volKey); err != nil {
		t.Fatalf("delete inode with volume: %v", err)
	}

	// id2 is still live in the volume, so live_count must be exactly 1, not 0.
	empty, err := database.GetEmptyVolumes(0)
	if err != nil {
		t.Fatalf("get empty volumes: %v", err)
	}
	for _, v := range empty {
		if v.S3Key == volKey {
			t.Fatalf("volume wrongly reported empty (live_count=%d); a double-decrement undercounted live needles", v.LiveCount)
		}
	}

	// Deleting id2 (still references the volume) must bring it to 0.
	if err := database.DeleteInodeWithVolume(id2, volKey); err != nil {
		t.Fatalf("delete inode 2: %v", err)
	}
	empty, err = database.GetEmptyVolumes(0)
	if err != nil {
		t.Fatalf("get empty volumes 2: %v", err)
	}
	found := false
	for _, v := range empty {
		if v.S3Key == volKey {
			found = true
		}
	}
	if !found {
		t.Fatal("volume should be empty after both needles removed")
	}
}

// TestReadLoadedClampsToLogicalSize verifies that a clean (non-dirty) handle
// whose buffer holds more bytes than the inode's logical size — e.g. after a
// truncate that shrank the file without rewriting storage — serves only the
// truncated view and not stale bytes past EOF.
func TestReadLoadedClampsToLogicalSize(t *testing.T) {
	hfs := setupDBOnly(t)

	h := newHandle(hfs, 1, false)
	h.st.buf = []byte("helloworld")
	// The logical size is smaller than the buffer, and lives on the shared state:
	// clamping to the handle's own fileSize would let a handle that opened when
	// the file was shorter cut down what every other handle sees.
	h.st.size = 5
	h.fileSize = 5
	h.st.loaded = true
	h.st.dirty = false

	dest := make([]byte, 100)
	res, errno := h.readLoaded(dest, 0)
	if errno != 0 {
		t.Fatalf("read errno %v", errno)
	}
	got, _ := res.Bytes(nil)
	if string(got) != "hello" {
		t.Fatalf("clean read past truncate: got %q, want %q", string(got), "hello")
	}

	// A read starting past the logical EOF returns nothing.
	res, errno = h.readLoaded(dest, 7)
	if errno != 0 {
		t.Fatalf("read past EOF errno %v", errno)
	}
	got, _ = res.Bytes(nil)
	if len(got) != 0 {
		t.Fatalf("read past truncated EOF should be empty, got %q", string(got))
	}

	// A dirty state is authoritative on its buffer length (writes may extend
	// past the stored size), so no clamp is applied.
	h.st.dirty = true
	res, errno = h.readLoaded(dest, 0)
	if errno != 0 {
		t.Fatalf("dirty read errno %v", errno)
	}
	got, _ = res.Bytes(nil)
	if string(got) != "helloworld" {
		t.Fatalf("dirty read should not clamp: got %q, want %q", string(got), "helloworld")
	}
}
