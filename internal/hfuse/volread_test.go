package hfuse

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// stagePackedVolume builds one volume object holding several needles (encrypting
// each if the mount has an encryptor, exactly as the builder would) and creates
// a committed, packed inode per needle. It returns the volume key and the inode
// ids so a test can read them back through loadFromVolume.
func stagePackedVolume(t *testing.T, hfs *HamstorFS, names []string, contents [][]byte) (string, []int64) {
	t.Helper()
	ctx := context.Background()

	var blob []byte
	type span struct{ off, size int64 }
	layout := make([]span, len(contents))
	for i, content := range contents {
		needle := content
		if hfs.Encryptor != nil {
			enc, err := hfs.Encryptor.Encrypt(content)
			if err != nil {
				t.Fatalf("encrypt needle %d: %v", i, err)
			}
			needle = enc
		}
		layout[i] = span{int64(len(blob)), int64(len(needle))}
		blob = append(blob, needle...)
	}

	volKey := s3store.NewKey()
	if err := hfs.Store.Upload(ctx, volKey, blob); err != nil {
		t.Fatalf("upload volume: %v", err)
	}
	t.Cleanup(func() { hfs.Store.Delete(ctx, volKey) })

	if err := hfs.DB.InsertVolume(volKey, int64(len(blob)), 0, 0, 0, "open"); err != nil {
		t.Fatalf("insert volume: %v", err)
	}

	ids := make([]int64, len(contents))
	needles := make([]db.NeedleCommit, len(contents))
	for i, content := range contents {
		id, err := hfs.DB.InsertInode(1, names[i], syscall.S_IFREG|0o644, "pending")
		if err != nil {
			t.Fatalf("insert inode %s: %v", names[i], err)
		}
		// CommitInode sets the logical size and leaves the inode committed and
		// unpacked (s3_key and vol_s3_key empty), which is what
		// CommitNeedlesToVolume's onlyUnpacked path requires.
		if _, err := hfs.DB.CommitInode(id, "", int64(len(content))); err != nil {
			t.Fatalf("commit inode %s: %v", names[i], err)
		}
		ids[i] = id
		needles[i] = db.NeedleCommit{InodeID: id, Offset: layout[i].off, Size: layout[i].size}
	}
	if _, err := hfs.DB.CommitNeedlesToVolume(volKey, int64(len(blob)), needles, true, ""); err != nil {
		t.Fatalf("commit needles: %v", err)
	}
	return volKey, ids
}

// TestWholeVolumeReadServesSiblingsFromCache proves the read-side win: reading
// one packed file downloads and caches the WHOLE volume, so a sibling packed in
// the same volume is served locally. The proof is deletion — after the first
// read the S3 object is removed, and the second read must still succeed, which
// it can only do from the cached volume.
func TestWholeVolumeReadServesSiblingsFromCache(t *testing.T) {
	hfs, _ := setupTest(t)
	c, err := cache.New(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = c

	a := bytes.Repeat([]byte("A"), 300)
	b := []byte("second needle content")
	volKey, ids := stagePackedVolume(t, hfs, []string{"a", "b"}, [][]byte{a, b})

	// First read downloads and caches the whole volume.
	if got := readBack(t, hfs, ids[0], len(a)); !bytes.Equal(got, a) {
		t.Fatalf("needle A = %q, want %q", got, a)
	}
	if !hfs.Cache.Has("volobj/" + volKey) {
		t.Fatal("whole volume not cached after first read")
	}

	// Delete the S3 object. A correct second read can now only come from cache.
	if err := hfs.Store.Delete(context.Background(), volKey); err != nil {
		t.Fatalf("delete volume: %v", err)
	}
	if got := readBack(t, hfs, ids[1], len(b)); !bytes.Equal(got, b) {
		t.Fatalf("needle B = %q, want %q — sibling not served from cached volume", got, b)
	}
}

// packStaged seals everything currently staged into volumes. Close()'s final
// sweep is the only trigger that packs a sub-budget batch on the spot — the
// notify path deliberately waits for more data and the fallback tick is 5s away
// — so packing in a test means closing the builder. It is handed back as nil
// because Close is not idempotent and setupStagingCache also closes it; the
// filesystem is read-only from here on.
func packStaged(t *testing.T, hfs *HamstorFS) {
	t.Helper()
	b := hfs.VolumeBuilder
	hfs.VolumeBuilder = nil
	if err := b.Close(); err != nil {
		t.Fatalf("close builder: %v", err)
	}
}

// TestPackedFilesReadFromBuilderSeededCache is the write-side counterpart of the
// two tests around it: the builder holds the whole volume in memory as it seals
// it, so the first read of a packed file should never have to download those
// same bytes back. The proof is again deletion — nothing reads the files before
// the volume object is removed from S3, so a successful read afterwards can only
// come from the cache the builder itself seeded.
func TestPackedFilesReadFromBuilderSeededCache(t *testing.T) {
	c, err := cache.New(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs, _ := setupStagingCache(t, c)

	contents := map[string][]byte{
		"packed-a.txt": bytes.Repeat([]byte("alpha "), 50),
		"packed-b.txt": []byte("bravo payload"),
	}
	ids := map[string]int64{}
	for name, data := range contents {
		id := mustInsert(t, hfs, name)
		h := NewTestHandle(hfs, id, true)
		if errno := h.TestWriteAt(data, 0); errno != 0 {
			t.Fatalf("write %s: %v", name, errno)
		}
		if errno := h.TestFlush(); errno != 0 {
			t.Fatalf("flush %s: %v", name, errno)
		}
		h.WaitUpload()
		h.TestRelease()
		ids[name] = id
	}

	packStaged(t, hfs)

	// Every file must be packed, and the volumes must already be cached — before
	// a single read has happened.
	volKeys := map[string]bool{}
	for name, id := range ids {
		m, err := hfs.DB.GetInode(id)
		if err != nil {
			t.Fatalf("GetInode %s: %v", name, err)
		}
		if m.VolS3Key == "" {
			t.Fatalf("%s was not packed into a volume", name)
		}
		if !c.Has("volobj/" + m.VolS3Key) {
			t.Fatalf("volume %s not cached by the builder — reading %s will download the bytes it just uploaded",
				m.VolS3Key, name)
		}
		volKeys[m.VolS3Key] = true
	}

	// Remove the volumes from S3. Reads can now only be served locally.
	for volKey := range volKeys {
		if err := hfs.Store.Delete(context.Background(), volKey); err != nil {
			t.Fatalf("delete volume %s: %v", volKey, err)
		}
	}
	for name, id := range ids {
		want := contents[name]
		if got := readBack(t, hfs, id, len(want)); !bytes.Equal(got, want) {
			t.Fatalf("%s = %q, want %q — not served from the builder-seeded cache", name, got, want)
		}
	}
}

// TestWholeVolumeReadDecryptsNeedleFromCache is the same proof for an encrypted
// mount: each needle is individually encrypted inside the volume, so serving a
// sibling from the cached volume must slice its ciphertext range and decrypt it.
func TestWholeVolumeReadDecryptsNeedleFromCache(t *testing.T) {
	hfs, _ := setupTest(t)
	enc, err := crypto.New("test-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	hfs.Encryptor = enc
	c, err := cache.New(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = c

	a := bytes.Repeat([]byte("secret-A"), 40)
	b := []byte("secret-B payload")
	volKey, ids := stagePackedVolume(t, hfs, []string{"ea", "eb"}, [][]byte{a, b})

	if got := readBack(t, hfs, ids[0], len(a)); !bytes.Equal(got, a) {
		t.Fatalf("decrypted needle A mismatch")
	}
	if err := hfs.Store.Delete(context.Background(), volKey); err != nil {
		t.Fatalf("delete volume: %v", err)
	}
	if got := readBack(t, hfs, ids[1], len(b)); !bytes.Equal(got, b) {
		t.Fatalf("decrypted needle B from cached volume = %q, want %q", got, b)
	}
}
