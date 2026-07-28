package hfuse

import (
	"bytes"
	"context"
	"image"
	pngenc "image/png"
	"os"
	"path/filepath"
	"testing"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/thumb"
)

// pngFixture is a real image padded to size, so it decodes but is big enough to
// exercise whichever storage shape the caller wants.
func pngFixture(t *testing.T, total int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 300, 200))
	for i := range img.Pix {
		img.Pix[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := pngenc.Encode(&buf, img); err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	if total < buf.Len() {
		return buf.Bytes()
	}
	out := make([]byte, total)
	copy(out, buf.Bytes())
	return out
}

// TestBackfillRendersFromStoredFiles covers the case that made backfill worth
// writing: images that were already in the filesystem before thumbnails were
// stored durably. There is nothing to materialize for them, so the only way to
// get one is to read the original — which is exactly why it is opt-in.
//
// The read goes through the ordinary read path with an in-package handle rather
// than through the mount, since the daemon opening its own mountpoint would be
// a FUSE request served by the process making it. This test is what proves that
// handle works without an *fs.Inode.
func TestBackfillRendersFromStoredFiles(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.Mountpoint = t.TempDir()
	hfs.ThumbSem = make(chan struct{}, 2)
	thumbDir := filepath.Join(t.TempDir(), "thumbnails")
	hfs.ThumbCache = thumb.Cache{Dir: thumbDir, Uid: -1, Gid: -1}
	ctx := context.Background()

	// Two blocks, so the read path has to reassemble rather than serve one
	// object — the shape a real photo takes.
	content := pngFixture(t, 2*db.BlockSize)
	id := mustInsert(t, hfs, "old-photo.png")
	writeAt(t, hfs, id, content, 0, true)

	// Wipe what the write path produced. What is left is exactly the state a
	// pre-existing library is in: a stored file, and no thumbnail anywhere.
	if key, err := hfs.DB.DeleteThumbnail(id); err != nil {
		t.Fatalf("clear stored thumbnail: %v", err)
	} else if key != "" {
		hfs.dropObjects(ctx, []string{key})
	}
	if err := os.RemoveAll(thumbDir); err != nil {
		t.Fatalf("clear thumbnail cache: %v", err)
	}
	if _, found, _ := hfs.DB.GetThumbnail(id); found {
		t.Fatal("the fixture still has a stored thumbnail; this would pass for the wrong reason")
	}

	stats, err := hfs.BackfillThumbnails(ctx)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if stats.Rendered != 1 || stats.Failed != 0 {
		t.Fatalf("backfill = %s, want 1 rendered and 0 failed", stats)
	}

	// Both halves: durable, so another machine gets it...
	rec, found, err := hfs.DB.GetThumbnail(id)
	if err != nil || !found {
		t.Fatalf("backfill stored no thumbnail: found=%v err=%v", found, err)
	}
	if rec.NormalLen <= 0 || rec.Length <= rec.NormalLen {
		t.Errorf("stored record looks wrong: %+v", rec)
	}
	// ...and local, so this machine gets it now.
	if !waitForThumb(t, thumbDir, true) {
		t.Error("backfill wrote no thumbnail into the freedesktop cache")
	}

	// Idempotent: the predicate is the absence of a row, so a finished library
	// costs one query. This is what makes it safe to leave enabled.
	stats, err = hfs.BackfillThumbnails(ctx)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if stats.Rendered != 0 {
		t.Errorf("second backfill rendered %d, want 0 (it is re-reading originals every mount)", stats.Rendered)
	}
}

// TestBackfillIgnoresNonImages: the candidate query cannot filter on extension
// (internal/thumb owns that list), so the caller must. Without it every file in
// the filesystem would be downloaded in full to find out it is not an image.
func TestBackfillIgnoresNonImages(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.Mountpoint = t.TempDir()
	hfs.ThumbSem = make(chan struct{}, 2)
	hfs.ThumbCache = thumb.Cache{Dir: filepath.Join(t.TempDir(), "thumbnails"), Uid: -1, Gid: -1}

	id := mustInsert(t, hfs, "notes.txt")
	writeAt(t, hfs, id, []byte("just some text"), 0, true)

	stats, err := hfs.BackfillThumbnails(context.Background())
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if stats.Rendered != 0 || stats.Failed != 0 || stats.Skipped != 0 {
		t.Errorf("backfill = %s, want everything zero: a .txt should never be read at all", stats)
	}
}

// TestBackfillDisabledWithoutACache: with nowhere to write, backfill must not
// read a single original. Downloading a whole library to render thumbnails that
// are then dropped is the most expensive possible no-op.
func TestBackfillDisabledWithoutACache(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.Mountpoint = t.TempDir()
	hfs.ThumbSem = make(chan struct{}, 2)
	// No ThumbCache.

	id := mustInsert(t, hfs, "photo.png")
	writeAt(t, hfs, id, pngFixture(t, 4096), 0, true)

	stats, err := hfs.BackfillThumbnails(context.Background())
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if stats.Rendered != 0 {
		t.Errorf("backfill rendered %d with no cache directory", stats.Rendered)
	}
	if _, found, _ := hfs.DB.GetThumbnail(id); found {
		t.Error("backfill stored a thumbnail with thumbnails disabled")
	}
}
