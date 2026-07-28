package db

import (
	"slices"
	"testing"
)

func putThumb(t *testing.T, d *DB, id int64, key string, mtimeNs int64) {
	t.Helper()
	ok, old, err := d.PutThumbnail(id, ThumbRecord{
		S3Key: key, Offset: 0, Length: 900, NormalLen: 400, SrcMtimeNs: mtimeNs,
	})
	if err != nil {
		t.Fatalf("put thumbnail: %v", err)
	}
	if !ok {
		t.Fatalf("put thumbnail refused for inode %d at mtime %d", id, mtimeNs)
	}
	_ = old
}

// mtimeOf reads an inode's mtime, which PutThumbnail compares against.
func mtimeOf(t *testing.T, d *DB, id int64) int64 {
	t.Helper()
	meta, err := d.GetInode(id)
	if err != nil {
		t.Fatalf("get inode %d: %v", id, err)
	}
	return meta.MtimeNs
}

// TestAllS3KeySetIncludesThumbnails is the sibling of
// TestAllS3KeySetIncludesBlocks and guards the same class of catastrophe: GC
// deletes every object in the bucket that is not in AllS3KeySet, so a storage
// shape missing from that union is not a leak but a delete. A thumbnail table
// left out of it means the first `hamstor gc` after the grace period silently
// destroys every stored thumbnail — the exact data this feature exists to keep.
//
// Needs no S3, deliberately: this defense must not be skippable.
func TestAllS3KeySetIncludesThumbnails(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "photo.png")
	putThumb(t, d, id, "cc/thumb-object", mtimeOf(t, d, id))

	set, err := d.AllS3KeySet()
	if err != nil {
		t.Fatalf("all s3 key set: %v", err)
	}
	if _, ok := set["cc/thumb-object"]; !ok {
		t.Error("thumbnail key is missing from AllS3KeySet: gc would delete it as an orphan")
	}
}

// TestPutThumbnailRefusesStaleRender is the CAS. A render job can sit in the
// queue while the file is overwritten under it; storing that thumbnail would
// stamp the OLD image with the NEW mtime, so every staleness check afterwards
// would confirm it as current. Nothing would ever repair it.
func TestPutThumbnailRefusesStaleRender(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "photo.png")

	staleMtime := mtimeOf(t, d, id) - 1
	ok, old, err := d.PutThumbnail(id, ThumbRecord{
		S3Key: "aa/stale", Length: 10, NormalLen: 5, SrcMtimeNs: staleMtime,
	})
	if err != nil {
		t.Fatalf("put thumbnail: %v", err)
	}
	if ok {
		t.Fatal("a render of a superseded version was accepted")
	}
	if old != "" {
		t.Errorf("refused put reported a superseded key %q", old)
	}
	if _, found, err := d.GetThumbnail(id); err != nil || found {
		t.Errorf("refused put stored a row anyway (found=%v, err=%v)", found, err)
	}
}

// TestPutThumbnailReturnsSupersededKey: replacing a thumbnail must hand back the
// object it displaced, inside the transaction that drops the row. Afterwards
// nothing knows that key and only a bucket listing could find it again.
func TestPutThumbnailReturnsSupersededKey(t *testing.T) {
	d := openTestDB(t)
	id := newFile(t, d, "photo.png")
	mtime := mtimeOf(t, d, id)

	putThumb(t, d, id, "aa/first", mtime)
	ok, old, err := d.PutThumbnail(id, ThumbRecord{
		S3Key: "bb/second", Length: 900, NormalLen: 400, SrcMtimeNs: mtime,
	})
	if err != nil || !ok {
		t.Fatalf("second put: ok=%v err=%v", ok, err)
	}
	if old != "aa/first" {
		t.Errorf("superseded key = %q, want aa/first (that object now leaks)", old)
	}

	rec, found, err := d.GetThumbnail(id)
	if err != nil || !found {
		t.Fatalf("get thumbnail: found=%v err=%v", found, err)
	}
	if rec.S3Key != "bb/second" {
		t.Errorf("stored key = %q, want bb/second", rec.S3Key)
	}
}

// TestDeleteInodeReturnsThumbnailKey: the row cascades away with the inode, so
// the delete transaction is the last moment anyone can name that object. It
// rides in the same slice as the block keys precisely so that every existing
// caller — Unlink, deleteTree, Rename over a target, Cleanup, GC phase 2 —
// drops it correctly without being touched.
func TestDeleteInodeReturnsThumbnailKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		del  func(d *DB, id int64) ([]string, error)
	}{
		{"DeleteInode", func(d *DB, id int64) ([]string, error) { return d.DeleteInode(id) }},
		{"DeleteInodeWithVolume", func(d *DB, id int64) ([]string, error) { return d.DeleteInodeWithVolume(id, "") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := openTestDB(t)
			id := newFile(t, d, "photo.png")
			if _, _, err := d.CommitBlocks(id, []BlockCommit{
				{Index: 0, S3Key: "aa/block", Size: 100},
			}, 100); err != nil {
				t.Fatalf("commit blocks: %v", err)
			}
			putThumb(t, d, id, "cc/thumb", mtimeOf(t, d, id))

			keys, err := tc.del(d, id)
			if err != nil {
				t.Fatalf("delete: %v", err)
			}
			if !slices.Contains(keys, "cc/thumb") {
				t.Errorf("delete returned %v, missing the thumbnail key: that object leaks", keys)
			}
			if !slices.Contains(keys, "aa/block") {
				t.Errorf("delete returned %v, missing the block key", keys)
			}
		})
	}
}

// TestAllThumbnailTargetsBuildsPaths checks the recursive CTE against the same
// answer InodePath gives, since materialization keys the freedesktop hash off
// that path and a wrong one writes a thumbnail nothing will ever look up.
func TestAllThumbnailTargetsBuildsPaths(t *testing.T) {
	d := openTestDB(t)
	dirID, err := d.InsertInode(1, "photos", 0o40755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}
	subID, err := d.InsertInode(dirID, "2026", 0o40755, "committed")
	if err != nil {
		t.Fatalf("insert subdir: %v", err)
	}
	fileID, err := d.InsertInode(subID, "a.png", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	putThumb(t, d, fileID, "dd/thumb", mtimeOf(t, d, fileID))

	targets, err := d.AllThumbnailTargets()
	if err != nil {
		t.Fatalf("all thumbnail targets: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1", len(targets))
	}

	want, err := d.InodePath(fileID)
	if err != nil {
		t.Fatalf("inode path: %v", err)
	}
	if targets[0].Path != want {
		t.Errorf("path = %q, want %q (InodePath is the authority here)", targets[0].Path, want)
	}
	if targets[0].Rec.NormalLen != 400 || targets[0].Rec.S3Key != "dd/thumb" {
		t.Errorf("record round-tripped wrong: %+v", targets[0].Rec)
	}
}

// TestThumbnaillessImagesSkipsWhatIsAlreadyDone is what makes backfill
// idempotent and therefore safe to run on every mount: the predicate is the
// absence of a row, so a finished backfill costs one query and nothing else.
func TestThumbnaillessImagesSkipsWhatIsAlreadyDone(t *testing.T) {
	d := openTestDB(t)

	withThumb := newFile(t, d, "done.png")
	without := newFile(t, d, "todo.png")
	for _, id := range []int64{withThumb, without} {
		size := int64(1000)
		if _, err := d.SetAttr(id, &size, nil, nil); err != nil {
			t.Fatalf("set size: %v", err)
		}
		if _, err := d.CommitInode(id, size); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	putThumb(t, d, withThumb, "ee/thumb", mtimeOf(t, d, withThumb))

	// A directory and an empty file must never show up as backfill candidates.
	if _, err := d.InsertInode(1, "adir", 0o40755, "committed"); err != nil {
		t.Fatalf("insert dir: %v", err)
	}
	empty := newFile(t, d, "empty.png")
	if _, err := d.CommitInode(empty, 0); err != nil {
		t.Fatalf("commit empty: %v", err)
	}

	got, err := d.ThumbnaillessImages()
	if err != nil {
		t.Fatalf("thumbnailless images: %v", err)
	}
	var names []string
	for _, tt := range got {
		names = append(names, tt.Path)
	}
	if !slices.Equal(names, []string{"todo.png"}) {
		t.Errorf("candidates = %v, want [todo.png]", names)
	}
}
