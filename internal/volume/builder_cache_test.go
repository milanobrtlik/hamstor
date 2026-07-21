package volume

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/milan/hamstor/internal/cache"
)

// withCache gives the rig's builder a real disk cache and returns it.
func (r *testRig) withCache(t *testing.T) *cache.DiskCache {
	t.Helper()
	c, err := cache.New(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	r.b.cache = c
	return c
}

// assertCachedVolumeMatchesS3 checks the entry the builder left behind is usable
// by the read path: it must live under the key readNeedle looks up
// ("volobj/<volKey>") and hold the volume object byte for byte, since needle
// spans are recorded against the object in S3 and sliced out of this file.
func assertCachedVolumeMatchesS3(t *testing.T, r *testRig, c *cache.DiskCache, volKey string) []byte {
	t.Helper()
	cacheKey := "volobj/" + volKey

	if !c.Has(cacheKey) {
		t.Fatalf("volume %s not cached after sealing — the bytes we just packed will be downloaded back", volKey)
	}
	f, err := c.Open(cacheKey)
	if err != nil {
		t.Fatalf("open cached volume: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read cached volume: %v", err)
	}

	want, err := r.b.store.Download(context.Background(), volKey)
	if err != nil {
		t.Fatalf("download volume from S3: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cached volume differs from the S3 object (%d vs %d bytes)", len(got), len(want))
	}
	return got
}

// TestSealBatchCachesVolume is the write-side half of the whole-volume read:
// after the builder seals a volume, the object must already be in the disk cache
// under the key readNeedle looks for. Without it every packed file makes its data
// travel local disk -> S3 -> local disk before it can be read once.
func TestSealBatchCachesVolume(t *testing.T) {
	r := newTestRig(t)
	c := r.withCache(t)

	first := []byte("first needle")
	second := bytes.Repeat([]byte("b"), 400)
	a := r.stage(t, first)
	b := r.stage(t, second)

	if sealed, _ := r.b.scanAndSeal(true); sealed != 2 {
		t.Fatalf("scanAndSeal sealed %d files, want 2", sealed)
	}

	ma, err := r.db.GetInode(a)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if ma.VolS3Key == "" {
		t.Fatal("inode not packed")
	}
	volData := assertCachedVolumeMatchesS3(t, r, c, ma.VolS3Key)

	// Both needles must be readable out of the cached volume at their recorded
	// spans — that is exactly what readNeedle does on a cache hit.
	for _, tc := range []struct {
		id   int64
		want []byte
	}{{a, first}, {b, second}} {
		m, err := r.db.GetInode(tc.id)
		if err != nil {
			t.Fatalf("GetInode: %v", err)
		}
		if m.VolS3Key != ma.VolS3Key {
			t.Fatalf("inode %d landed in volume %s, want %s", tc.id, m.VolS3Key, ma.VolS3Key)
		}
		if m.VolOffset+m.VolSize > int64(len(volData)) {
			t.Fatalf("needle [%d:%d] out of range for cached volume (len %d)",
				m.VolOffset, m.VolOffset+m.VolSize, len(volData))
		}
		if got := volData[m.VolOffset : m.VolOffset+m.VolSize]; !bytes.Equal(got, tc.want) {
			t.Errorf("needle from cached volume = %q, want %q", got, tc.want)
		}
	}
}

// TestFlushInodeCachesVolume covers the on-demand (Fsync) seal path, which packs
// a single staged file into its own volume and must cache it the same way.
func TestFlushInodeCachesVolume(t *testing.T) {
	r := newTestRig(t)
	c := r.withCache(t)

	id := r.stage(t, []byte("fsynced content"))
	if err := r.b.FlushInode(id); err != nil {
		t.Fatalf("FlushInode: %v", err)
	}

	m, err := r.db.GetInode(id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if m.VolS3Key == "" {
		t.Fatal("inode not packed after FlushInode")
	}
	assertCachedVolumeMatchesS3(t, r, c, m.VolS3Key)
}

// TestSealWithoutCacheStillPacks guards the nil-cache mount (--cache-size 0):
// the put is an optimisation, so its absence must not disturb sealing.
func TestSealWithoutCacheStillPacks(t *testing.T) {
	r := newTestRig(t) // no withCache: b.cache stays nil

	id := r.stage(t, []byte("uncached"))
	if sealed, _ := r.b.scanAndSeal(true); sealed != 1 {
		t.Fatalf("scanAndSeal sealed %d files, want 1", sealed)
	}
	if !r.packed(t, id) {
		t.Error("inode not packed without a disk cache")
	}
}
