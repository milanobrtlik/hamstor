package hfuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
)

// setupAsyncCache is setupTest with a disk cache and a spill directory, and
// deliberately WITHOUT a volume builder: with no builder every non-empty write
// takes the async upload path in flushAsync, which is the one that used to throw
// its local copy away.
func setupAsyncCache(t *testing.T, maxBytes int64) (*HamstorFS, *cache.DiskCache) {
	t.Helper()
	hfs, _ := setupTest(t)
	c, err := cache.New(t.TempDir(), maxBytes)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = c
	hfs.SpillDir = t.TempDir()
	return hfs, c
}

// blockKeys returns the inode's block object keys in index order, and arranges
// for them to be deleted when the test ends. A flush now commits a SET of
// objects rather than one, so "the file's key" is only meaningful for a file
// small enough to be a single block — see soleBlockKey.
func blockKeys(t *testing.T, hfs *HamstorFS, id int64) []string {
	t.Helper()
	blocks, err := hfs.DB.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks for inode %d: %v", id, err)
	}
	keys := make([]string, len(blocks))
	for i, b := range blocks {
		keys[i] = b.S3Key
	}
	t.Cleanup(func() {
		for _, k := range keys {
			hfs.Store.Delete(context.Background(), k)
		}
	})
	return keys
}

// soleBlockKey is blockKeys for a file that must be exactly one block, which is
// every file below db.BlockSize. It also proves the flush stored the file as
// blocks at all: before this step it would have been one whole-file object named
// by inodes.s3_key, which the async path no longer writes.
func soleBlockKey(t *testing.T, hfs *HamstorFS, id int64) string {
	t.Helper()
	keys := blockKeys(t, hfs, id)
	if len(keys) != 1 {
		t.Fatalf("want exactly 1 block, got %d", len(keys))
	}
	return keys[0]
}

// writeAndFlush writes content to a new inode through a handle and waits for the
// async upload to run to completion — not just to publish its result, which is
// what WaitUpload alone gives (close(att.done) fires before the goroutine's last
// defers). Returns the file's single block key.
func writeAndFlush(t *testing.T, hfs *HamstorFS, name string, content []byte) (int64, string) {
	t.Helper()

	id := mustInsert(t, hfs, name)
	th := NewTestHandle(hfs, id, true)
	if errno := th.TestWriteAt(content, 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	return id, soleBlockKey(t, hfs, id)
}

// TestFlushCachesUploadedFile proves the write-side copy is kept: the bytes we
// just uploaded are still on this disk, so a reopen must not download them back.
// The proof is deletion — the S3 object is removed after the flush, and the read
// can then only succeed from the cache.
func TestFlushCachesUploadedFile(t *testing.T) {
	hfs, c := setupAsyncCache(t, 1<<30)

	content := bytes.Repeat([]byte("hamstor"), 4096)
	id, key := writeAndFlush(t, hfs, "kept.bin", content)

	if !c.Has(key) {
		t.Fatal("flush did not cache the uploaded file")
	}
	if err := hfs.Store.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete S3 object: %v", err)
	}
	if got := readBack(t, hfs, id, len(content)); !bytes.Equal(got, content) {
		t.Fatalf("read back %d bytes, want %d — it did not come from the cache", len(got), len(content))
	}
}

// TestFlushCachesPlaintextNotCiphertext is the trap in this change: under
// encryption the whole object is encrypted at upload time, and a cache entry is
// served straight back as the file's contents. Caching what went to S3 would
// hand ciphertext to every reader.
func TestFlushCachesPlaintextNotCiphertext(t *testing.T) {
	hfs, c := setupAsyncCache(t, 1<<30)
	enc, err := crypto.New("test-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("crypto: %v", err)
	}
	hfs.Encryptor = enc

	content := bytes.Repeat([]byte("secret"), 4096)
	id, key := writeAndFlush(t, hfs, "kept.enc", content)

	f, err := c.Open(key)
	if err != nil {
		t.Fatalf("flush did not cache the uploaded file: %v", err)
	}
	cached, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		t.Fatalf("read cache entry: %v", err)
	}
	if !bytes.Equal(cached, content) {
		if crypto.IsEncrypted(cached) {
			t.Fatal("cache holds the ciphertext; it is served as file contents, so readers would get encrypted bytes")
		}
		t.Fatalf("cache holds %d bytes, want the %d-byte plaintext", len(cached), len(content))
	}

	// And it is actually used: with the object gone, the read can only come from
	// the cache — undecrypted, which is what the entry must already be.
	if err := hfs.Store.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete S3 object: %v", err)
	}
	if got := readBack(t, hfs, id, len(content)); !bytes.Equal(got, content) {
		t.Fatalf("read back %q..., want the plaintext", got[:min(16, len(got))])
	}
}

// TestFlushCachesSpilledFile covers the case that motivated the change: a file
// past spillThreshold, whose contents never exist as a heap buffer at all. That
// is exactly the shape the old condition (plainBuf != nil) excluded, so a large
// file was re-downloaded in full the next time it was opened for writing.
func TestFlushCachesSpilledFile(t *testing.T) {
	hfs, c := setupAsyncCache(t, 1<<30)
	ctx := context.Background()

	id := mustInsert(t, hfs, "big.bin")
	th := NewTestHandle(hfs, id, true)
	head := []byte("head")
	if errno := th.TestWriteAt(head, 0); errno != 0 {
		t.Fatalf("write head: %v", errno)
	}
	// Push past the spill threshold so the contents live in st.spillFile.
	tailOff := int64(spillThreshold + 1)
	if errno := th.TestWriteAt([]byte("X"), tailOff); errno != 0 {
		t.Fatalf("write tail: %v", errno)
	}
	if th.h.st.spillFile == nil {
		t.Fatal("write past spillThreshold should have spilled to disk")
	}
	spillName := th.h.st.spillFile.Name()

	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	// Only the two blocks that were written exist: everything between them is a
	// hole. That is the sparse-file property the layout is supposed to have, and
	// it is worth asserting here — the alternative (uploading the gap as zeroes)
	// would be seven 8 MiB objects of nothing.
	keys := blockKeys(t, hfs, id)
	if len(keys) != 2 {
		t.Fatalf("want 2 blocks (the head and the tail), got %d — the hole between them was materialized", len(keys))
	}
	for _, k := range keys {
		if !c.Has(k) {
			t.Fatalf("flush did not cache block %s of the spilled file", k)
		}
	}
	if _, err := os.Stat(spillName); !os.IsNotExist(err) {
		t.Fatalf("spill file %s outlived the flush (stat err %v)", spillName, err)
	}

	// With the objects gone the reopen has nothing to download, so a correct
	// preload can only come from the cache.
	for _, k := range keys {
		if err := hfs.Store.Delete(ctx, k); err != nil {
			t.Fatalf("delete S3 object: %v", err)
		}
	}
	got := readBack(t, hfs, id, len(head))
	if !bytes.Equal(got, head) {
		t.Fatalf("read back %q, want %q", got, head)
	}
}

// TestFlushSkipsCacheForOversizedFile keeps one file from evicting most of the
// cache on its way in.
func TestFlushSkipsCacheForOversizedFile(t *testing.T) {
	content := bytes.Repeat([]byte("x"), 8192)
	// Cache limit just under twice the file: maxCacheShare admits nothing this
	// large, but the entry would comfortably fit if the guard were missing.
	hfs, c := setupAsyncCache(t, int64(len(content))*2-1)

	id, key := writeAndFlush(t, hfs, "oversized.bin", content)

	if c.Has(key) {
		t.Fatalf("cached a file claiming more than 1/%d of the cache", maxCacheShare)
	}
	// Still perfectly readable, just from S3.
	if got := readBack(t, hfs, id, len(content)); !bytes.Equal(got, content) {
		t.Fatalf("read back %d bytes, want %d", len(got), len(content))
	}
}

// TestFlushEvictsSupersededKey covers the other half of the cache put: after an
// overwrite the old object is deleted from S3, so its cache entry is dead weight
// that only LRU would ever clear.
func TestFlushEvictsSupersededKey(t *testing.T) {
	hfs, c := setupAsyncCache(t, 1<<30)
	ctx := context.Background()

	id, oldKey := writeAndFlush(t, hfs, "over.bin", bytes.Repeat([]byte("A"), 4096))
	if !c.Has(oldKey) {
		t.Fatal("first flush did not cache the file")
	}

	updated := bytes.Repeat([]byte("B"), 4096)
	th := NewTestHandle(hfs, id, false)
	if errno := th.TestWriteAt(updated, 0); errno != 0 {
		t.Fatalf("overwrite: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	newKey := soleBlockKey(t, hfs, id)
	if newKey == oldKey {
		t.Fatal("overwrite reused the block's S3 key")
	}
	if c.Has(oldKey) {
		t.Fatal("superseded key left in the cache")
	}
	if !c.Has(newKey) {
		t.Fatal("new version not cached")
	}
	// The superseded object must be gone from S3 too — CommitBlocks reports it as
	// orphaned from inside its own transaction, and the flush deletes it after.
	if _, err := hfs.Store.Download(ctx, oldKey); err == nil {
		t.Fatal("superseded block object still in the bucket")
	}
	if got := readBack(t, hfs, id, len(updated)); !bytes.Equal(got, updated) {
		t.Fatalf("read back %q..., want the updated content", got[:min(8, len(got))])
	}
}
