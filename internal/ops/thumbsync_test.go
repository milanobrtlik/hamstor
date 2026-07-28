package ops

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
)

// storeRenderedThumb uploads a rendered pair the way hfuse.storeThumb does and
// records it, returning the S3 key.
func storeRenderedThumb(t *testing.T, database *db.DB, store *s3store.Store, enc *crypto.Encryptor, inodeID int64, r thumb.Rendered) string {
	t.Helper()
	ctx := context.Background()

	body := append(append([]byte{}, r.Normal...), r.Large...)
	if enc != nil {
		sealed, err := enc.Encrypt(body)
		if err != nil {
			t.Fatalf("encrypt thumbnail: %v", err)
		}
		body = sealed
	}
	key := fmt.Sprintf("thumbsync-%d/%d", time.Now().UnixNano(), inodeID)
	if err := store.Upload(ctx, key, body); err != nil {
		t.Fatalf("upload thumbnail: %v", err)
	}
	t.Cleanup(func() { store.Delete(ctx, key) })

	meta, err := database.GetInode(inodeID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	ok, _, err := database.PutThumbnail(inodeID, db.ThumbRecord{
		S3Key: key, Length: int64(len(body)), NormalLen: int64(len(r.Normal)), SrcMtimeNs: meta.MtimeNs,
	})
	if err != nil || !ok {
		t.Fatalf("put thumbnail: ok=%v err=%v", ok, err)
	}
	return key
}

func fixtureRendered(t *testing.T) thumb.Rendered {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 400, 300))
	for i := range img.Pix {
		img.Pix[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	r, err := thumb.Render(buf.Bytes())
	if err != nil {
		t.Fatalf("render fixture: %v", err)
	}
	return r
}

func cachedThumbPath(dir, sub, mountpoint, relPath string) string {
	uri := "file://" + filepath.Join(mountpoint, relPath)
	return filepath.Join(dir, sub, fmt.Sprintf("%x.png", md5.Sum([]byte(uri))))
}

// TestSyncThumbnailsNeedsNoOriginals is the entire claim of durable thumbnails,
// stated as a test: a machine with an empty freedesktop cache populates it
// without reading a single original.
//
// The originals are never uploaded at all. That is the assertion — not a
// counter that could be miscounted, but the plain fact that the bytes this
// would have to fetch do not exist in the bucket, and the pass succeeds anyway.
func TestSyncThumbnailsNeedsNoOriginals(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()
	mountpoint := "/mnt/hamstor"
	cacheDir := filepath.Join(t.TempDir(), "thumbnails")
	cache := thumb.Cache{Dir: cacheDir, Uid: -1, Gid: -1}

	dirID, err := database.InsertInode(1, "photos", 0o40755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}
	inodeID, err := database.InsertInode(dirID, "a.png", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	// Blocks pointing at objects that were NEVER uploaded. If sync touched an
	// original it would fail here rather than quietly succeed.
	if _, _, err := database.CommitBlocks(inodeID, []db.BlockCommit{
		{Index: 0, S3Key: "zz/does-not-exist", Size: 1000},
	}, 1000); err != nil {
		t.Fatalf("commit blocks: %v", err)
	}

	want := fixtureRendered(t)
	storeRenderedThumb(t, database, store, nil, inodeID, want)

	stats, err := SyncThumbnails(ctx, database, store, nil, cache, mountpoint)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.Written != 1 || stats.Failed != 0 {
		t.Fatalf("sync = %s, want 1 written and 0 failed", stats)
	}

	for _, tc := range []struct {
		sub  string
		want []byte
	}{{"normal", want.Normal}, {"large", want.Large}} {
		got, err := os.ReadFile(cachedThumbPath(cacheDir, tc.sub, mountpoint, "photos/a.png"))
		if err != nil {
			t.Fatalf("%s thumbnail not materialized: %v", tc.sub, err)
		}
		if _, _, err := image.Decode(bytes.NewReader(got)); err != nil {
			t.Errorf("%s thumbnail does not decode: %v", tc.sub, err)
		}
		// The stamped copy is the rendered bytes plus tEXt chunks, so it must
		// contain them — this catches normal and large being swapped, which
		// nothing else would notice.
		if !bytes.Contains(got, tc.want[len(tc.want)-64:]) {
			t.Errorf("%s thumbnail is not the %s render (sizes swapped?)", tc.sub, tc.sub)
		}
	}

	// Second pass must be free: this is what makes it safe on every mount.
	stats, err = SyncThumbnails(ctx, database, store, nil, cache, mountpoint)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if stats.Written != 0 || stats.Skipped != 1 {
		t.Errorf("second sync = %s, want 0 written and 1 skipped (Cache.Has is not idempotent)", stats)
	}
}

// TestSyncThumbnailsRoundTripsUnderEncryption: the stored body is one sealed
// message, so the split point is a PLAINTEXT offset. Reading it as an offset
// into the ciphertext would silently hand back two halves of garbage.
func TestSyncThumbnailsRoundTripsUnderEncryption(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()
	mountpoint := "/mnt/hamstor"
	cacheDir := filepath.Join(t.TempDir(), "thumbnails")
	cache := thumb.Cache{Dir: cacheDir, Uid: -1, Gid: -1}

	salt, err := crypto.GenerateSalt()
	if err != nil {
		t.Fatalf("salt: %v", err)
	}
	enc, err := crypto.New("test passphrase", salt)
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}

	inodeID, err := database.InsertInode(1, "b.png", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	want := fixtureRendered(t)
	storeRenderedThumb(t, database, store, enc, inodeID, want)

	stats, err := SyncThumbnails(ctx, database, store, enc, cache, mountpoint)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.Written != 1 {
		t.Fatalf("sync = %s, want 1 written", stats)
	}

	got, err := os.ReadFile(cachedThumbPath(cacheDir, "large", mountpoint, "b.png"))
	if err != nil {
		t.Fatalf("large thumbnail not materialized: %v", err)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("materialized thumbnail does not decode: %v", err)
	}
	if cfg.Width > 256 || cfg.Height > 256 {
		t.Errorf("large is %dx%d; the body was split at the wrong offset", cfg.Width, cfg.Height)
	}
}

// TestSyncThumbnailsSkipsStaleRenders: a stored thumbnail whose src_mtime_ns no
// longer matches the file describes a version that is gone. Materializing it
// would write the OLD image stamped with the CURRENT mtime, so every later
// staleness check would accept it and nothing would ever repair it. Skipping
// leaves the desktop to render its own, which is always the safe direction.
func TestSyncThumbnailsSkipsStaleRenders(t *testing.T) {
	database, store := setupGCTest(t)
	ctx := context.Background()
	mountpoint := "/mnt/hamstor"
	cacheDir := filepath.Join(t.TempDir(), "thumbnails")
	cache := thumb.Cache{Dir: cacheDir, Uid: -1, Gid: -1}

	inodeID, err := database.InsertInode(1, "c.png", 0o100644, "committed")
	if err != nil {
		t.Fatalf("insert inode: %v", err)
	}
	storeRenderedThumb(t, database, store, nil, inodeID, fixtureRendered(t))

	// The file changes after its thumbnail was stored.
	newMtime := time.Now().Add(time.Hour).UnixNano()
	if _, err := database.SetAttr(inodeID, nil, nil, &newMtime); err != nil {
		t.Fatalf("touch inode: %v", err)
	}

	stats, err := SyncThumbnails(ctx, database, store, nil, cache, mountpoint)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.Written != 0 || stats.Skipped != 1 {
		t.Errorf("sync = %s, want 0 written and 1 skipped", stats)
	}
	if _, err := os.Stat(cachedThumbPath(cacheDir, "large", mountpoint, "c.png")); !os.IsNotExist(err) {
		t.Error("a stale render was materialized and stamped with the current mtime")
	}
}
