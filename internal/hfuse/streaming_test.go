package hfuse

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
)

// Streaming mode serves a media file block by block instead of materializing all
// of it: rate-limited, bounded in memory by --stream-buffer, and deliberately
// outside the disk cache.
//
// It had NO coverage until this step, and that is how it came to be broken for
// two steps running: Open enabled it for any unencrypted media file without
// asking whether the file had an object to range into, and setupTest leaves
// StreamRate at zero, so every test took the non-streaming path. These tests
// therefore assert twice over — that the bytes are right, AND that they came
// through streaming — because "the read worked" is exactly what stayed true
// while the feature was dead.

// fuseReadSize is what the kernel actually asks for at a time (max_read), which
// is the shape that matters here: a whole block is 64 of these, so serving one
// read per block would be a 64x amplification without the ring.
const fuseReadSize = 128 << 10

// setupStreaming is setupTest with streaming configured the way main.go does.
func setupStreaming(t *testing.T, rateMBs, bufferMB int) *HamstorFS {
	t.Helper()
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.StreamRate = rateMBs
	hfs.StreamBuffer = bufferMB
	return hfs
}

// writeMediaBlocks stores a media file of exactly nBlocks blocks and returns its
// contents. Every byte is derived from its offset, so a block served at the
// wrong place, a slice off by one, or a ring entry returned for the wrong index
// all show up as a content mismatch rather than as something subtler.
func writeMediaBlocks(t *testing.T, hfs *HamstorFS, name string, nBlocks int) (int64, []byte) {
	t.Helper()
	content := make([]byte, nBlocks*db.BlockSize)
	for i := range content {
		content[i] = byte(i) ^ byte(i/db.BlockSize)
	}
	id := mustInsert(t, hfs, name)
	writeAt(t, hfs, id, content, 0, true)
	if blocks := blocksOf(t, hfs, id); len(blocks) != nBlocks {
		t.Fatalf("want %d blocks, got %d", nBlocks, len(blocks))
	}
	return id, content
}

// openStream opens the inode read-only, the way a media player does, and asserts
// whether streaming engaged. That assertion is the point: without it a test
// passes just as happily through ensureLoaded, which is how streaming stayed
// broken through two steps.
func openStream(t *testing.T, hfs *HamstorFS, id int64, wantStreaming bool) (*HamstorHandle, func()) {
	t.Helper()
	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_RDONLY))
	if errno != 0 {
		t.Fatalf("open read-only: %v", errno)
	}
	h := fh.(*HamstorHandle)
	if h.streaming != wantStreaming {
		t.Fatalf("handle.streaming = %v, want %v", h.streaming, wantStreaming)
	}
	return h, func() { h.Release(context.Background()) }
}

// readAt reads through the handle exactly as the kernel would.
func readAt(t *testing.T, h *HamstorHandle, off int64, n int) []byte {
	t.Helper()
	dest := make([]byte, n)
	res, errno := h.Read(context.Background(), dest, off)
	if errno != 0 {
		t.Fatalf("read %d bytes at %d: %v", n, off, errno)
	}
	out, status := res.Bytes(dest)
	if !status.Ok() {
		t.Fatalf("read result at %d: %v", off, status)
	}
	return out
}

// readWhole reads the file front to back in kernel-sized reads.
func readWhole(t *testing.T, h *HamstorHandle, size int64) []byte {
	t.Helper()
	got := make([]byte, 0, size)
	for off := int64(0); off < size; off += fuseReadSize {
		got = append(got, readAt(t, h, off, fuseReadSize)...)
	}
	return got
}

// TestStreamingMediaFileReads is the base case: a media file opened read-only is
// served through streaming, and what comes out is the file.
//
// The content check alone is not enough and never was — before this step the
// same assertion passed through ensureLoaded while streaming was switched off
// entirely. So it also proves the streaming path did the work: the ring holds
// blocks, and the shared state was never given a backing store at all.
func TestStreamingMediaFileReads(t *testing.T) {
	hfs := setupStreaming(t, 5, 16) // the defaults from main.go
	id, content := writeMediaBlocks(t, hfs, "clip.mp4", 2)

	h, release := openStream(t, hfs, id, true)
	defer release()

	got := readWhole(t, h, int64(len(content)))
	if !bytes.Equal(got, content) {
		t.Fatalf("streamed %d bytes, want %d, equal=%v", len(got), len(content), bytes.Equal(got, content))
	}
	if len(h.streamBlocks) == 0 {
		t.Fatal("the stream ring is empty: the read did not come through streaming")
	}

	h.st.mu.Lock()
	loaded, bufLen := h.st.loaded, len(h.st.buf)
	h.st.mu.Unlock()
	if loaded || bufLen != 0 {
		t.Fatalf("streaming materialized the shared state (loaded=%v, %d bytes buffered)", loaded, bufLen)
	}
}

// TestStreamingRateLimit covers --stream-rate and the seek reset. One file
// serves both because writing 16 MiB twice buys nothing; each subtest opens its
// own handle, so each gets a fresh rate limiter.
//
// stream-buffer 8 makes the arithmetic exact: the bucket starts with one block
// of credit, so the first block is free and the second must be earned at the
// configured rate.
func TestStreamingRateLimit(t *testing.T) {
	const rateMBs = 16 // 8 MiB block => 0.5s per block once the burst is spent
	hfs := setupStreaming(t, rateMBs, 8)
	id, content := writeMediaBlocks(t, hfs, "movie.mp4", 2)
	size := int64(len(content))

	// Control FIRST, while nothing is warm: the same bytes over the same
	// network, with streaming off. Without it, "the read was slow" could mean
	// anything — a slow endpoint, a slow disk — rather than the limiter. Running
	// it first also gives the streamed read whatever warmth it produced, so the
	// comparison below is the conservative direction.
	hfs.StreamRate = 0
	ctrl, releaseCtrl := openStream(t, hfs, id, false)
	ctrlStart := time.Now()
	if got := readWhole(t, ctrl, size); !bytes.Equal(got, content) {
		t.Fatal("control read returned the wrong bytes")
	}
	unlimited := time.Since(ctrlStart)
	releaseCtrl()
	hfs.StreamRate = rateMBs

	t.Run("honors the configured rate", func(t *testing.T) {
		h, release := openStream(t, hfs, id, true)
		defer release()

		start := time.Now()
		got := readWhole(t, h, size)
		elapsed := time.Since(start)

		if !bytes.Equal(got, content) {
			t.Fatal("rate-limited read returned the wrong bytes")
		}
		// 16 MiB fetched against 8 MiB of burst leaves 8 MiB to earn at 16 MiB/s,
		// so the floor is 500ms however fast the endpoint is. Only a lower bound
		// is asserted; an upper one would be a flake waiting to happen.
		const floor = 350 * time.Millisecond
		if elapsed < floor {
			t.Fatalf("streaming 16 MiB at %d MB/s took %v, want at least %v: the rate limit is not being applied",
				rateMBs, elapsed, floor)
		}
		if elapsed < unlimited+250*time.Millisecond {
			t.Fatalf("rate-limited read took %v against %v unlimited: the delay is not attributable to the limiter",
				elapsed, unlimited)
		}
	})

	t.Run("seek resets the rate limiter", func(t *testing.T) {
		h, release := openStream(t, hfs, id, true)
		defer release()

		// Play block 0 through, contiguously. Seek detection is offset-exact, so
		// credit only accumulates across reads that continue one another —
		// jumping straight from offset 0 to the block boundary reads as a seek
		// and resets the very thing this subtest needs to have built up.
		for off := int64(0); off < db.BlockSize; off += fuseReadSize {
			readAt(t, h, off, fuseReadSize)
		}

		stallStart := time.Now()
		readAt(t, h, db.BlockSize, fuseReadSize) // block 1, has to be earned
		stall := time.Since(stallStart)
		if stall < 250*time.Millisecond {
			t.Fatalf("the sequential step into block 1 took %v: nothing was earned, so this test proves nothing", stall)
		}

		// Seek back. stream-buffer 8 holds one block, so block 0 is gone from the
		// ring and has to be fetched again — but a player that jumps must not sit
		// through credit its own earlier reads used up.
		seekStart := time.Now()
		got := readAt(t, h, 0, fuseReadSize)
		seek := time.Since(seekStart)

		if !bytes.Equal(got, content[:fuseReadSize]) {
			t.Fatal("read after seek returned the wrong bytes")
		}
		if seek > stall/2 {
			t.Fatalf("read after seeking took %v against a %v sequential stall: the limiter was not reset",
				seek, stall)
		}
	})
}

// TestStreamingMemoryStaysBounded covers --stream-buffer: the handle's footprint
// follows the buffer setting, not the size of the file.
//
// This is a hard requirement rather than tidiness — main.go sets
// debug.SetMemoryLimit(150 << 20), so a per-handle footprint proportional to a
// film is a mount that dies. The design's own note warned that carrying the old
// floor of 4 across to an 8 MiB unit would pin 32 MiB per open file; the floor
// is 1 for exactly that reason.
func TestStreamingMemoryStaysBounded(t *testing.T) {
	const bufferMB = 16 // => 2 blocks of ring for a 3-block file
	hfs := setupStreaming(t, 4096, bufferMB)
	id, content := writeMediaBlocks(t, hfs, "feature.mkv", 3)
	size := int64(len(content))

	h, release := openStream(t, hfs, id, true)
	defer release()

	if want := bufferMB << 20 / db.BlockSize; h.streamBlocksCap != want {
		t.Fatalf("ring cap %d for --stream-buffer %d, want %d", h.streamBlocksCap, bufferMB, want)
	}

	got := make([]byte, 0, size)
	var peak int
	for off := int64(0); off < size; off += fuseReadSize {
		got = append(got, readAt(t, h, off, fuseReadSize)...)

		var held int
		for _, sb := range h.streamBlocks {
			held += len(sb.data)
		}
		if held > peak {
			peak = held
		}
		if len(h.streamBlocks) > h.streamBlocksCap {
			t.Fatalf("ring holds %d blocks at offset %d, cap is %d", len(h.streamBlocks), off, h.streamBlocksCap)
		}
		if held > bufferMB<<20 {
			t.Fatalf("ring holds %d bytes at offset %d, --stream-buffer allows %d", held, off, bufferMB<<20)
		}
	}

	// Eviction has to have happened, or the bound was never tested.
	if peak < db.BlockSize {
		t.Fatalf("the ring peaked at %d bytes: the file was never actually streamed", peak)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("streaming a file larger than the ring returned the wrong bytes: eviction corrupted the read")
	}

	h.st.mu.Lock()
	loaded, bufLen := h.st.loaded, len(h.st.buf)
	h.st.mu.Unlock()
	if loaded || bufLen != 0 {
		t.Fatalf("streaming materialized the shared state (loaded=%v, %d bytes buffered)", loaded, bufLen)
	}

	// The control: the same file through the ordinary path holds ALL of it. That
	// is what makes the numbers above mean something rather than being small by
	// accident.
	hfs.StreamRate = 0
	ctrl, releaseCtrl := openStream(t, hfs, id, false)
	defer releaseCtrl()
	readAt(t, ctrl, 0, fuseReadSize)
	ctrl.st.mu.Lock()
	ctrlBuf := len(ctrl.st.buf)
	ctrl.st.mu.Unlock()
	if int64(ctrlBuf) != size {
		t.Fatalf("control handle buffered %d bytes of a %d-byte file; the comparison is not what it claims", ctrlBuf, size)
	}
}

// TestStreamingSkipsDiskCache covers the fourth requirement: streaming is the
// read path that deliberately goes around the disk cache. A film watched once
// would otherwise evict most of what the cache holds and then, being the biggest
// entry in it, buy nothing for what it displaced.
func TestStreamingSkipsDiskCache(t *testing.T) {
	hfs := setupStreaming(t, 4096, 16)
	// Written with no cache attached, or cacheBlock seeds it at write time and
	// the assertion below would be measuring the wrong thing.
	id, content := writeMediaBlocks(t, hfs, "documentary.mp4", 2)

	c, err := cache.New(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = c

	h, release := openStream(t, hfs, id, true)
	if got := readWhole(t, h, int64(len(content))); !bytes.Equal(got, content) {
		t.Fatal("streamed read returned the wrong bytes")
	}
	release()

	if total, count := c.Size(); total != 0 || count != 0 {
		t.Fatalf("streaming put %d bytes in %d cache entries; it must not write to the cache", total, count)
	}

	// Control: the ordinary read path DOES cache, so the zero above is a property
	// of streaming and not of a cache that was never wired up.
	hfs.StreamRate = 0
	ctrl, releaseCtrl := openStream(t, hfs, id, false)
	defer releaseCtrl()
	readAt(t, ctrl, 0, fuseReadSize)
	if total, _ := c.Size(); total == 0 {
		t.Fatal("the ordinary read path cached nothing either: the cache is not wired up, so this test proves nothing")
	}
}

// TestStreamingUnderEncryption covers the fifth requirement and retires the old
// "no range reads or streaming under encryption" rule.
//
// That rule existed because a stored object was one whole-file AES-256-GCM blob,
// so a byte range of it was undecryptable ciphertext. Under the block layout
// each block is its own [version][nonce][ct+tag], so an object decrypts on its
// own — which is precisely what streaming needs.
func TestStreamingUnderEncryption(t *testing.T) {
	hfs := setupStreaming(t, 4096, 16)
	enc, err := crypto.New("stream-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("crypto: %v", err)
	}
	hfs.Encryptor = enc

	id, content := writeMediaBlocks(t, hfs, "encrypted.mkv", 2)

	h, release := openStream(t, hfs, id, true)
	defer release()

	got := readWhole(t, h, int64(len(content)))
	if !bytes.Equal(got, content) {
		t.Fatalf("streamed %d bytes of an encrypted file; content does not match", len(got))
	}
	if len(h.streamBlocks) == 0 {
		t.Fatal("the stream ring is empty: this went through the full-download path, not streaming")
	}
}

// TestStreamingNeedleBackedMediaIsNotStreamed guards the trap that dropping the
// encryption condition opened.
//
// Streaming resolves every block through the blocks table, where a missing row
// means a hole. A media file small enough to be packed as a volume needle has no
// rows at all, so streaming would serve it as silence — the whole file zeroed,
// no error anywhere. The old gate hid this behind "unencrypted only"; with that
// gone, any small .mp3 or .m4a lands in it, which is why Open requires blocks.
func TestStreamingNeedleBackedMediaIsNotStreamed(t *testing.T) {
	hfs, _ := setupStagingCache(t, nil)
	hfs.StreamRate = 5
	hfs.StreamBuffer = 16

	content := bytes.Repeat([]byte("ID3 audio frame "), 64) // well under MaxNeedleSize
	id := mustInsert(t, hfs, "jingle.mp3")
	writeAt(t, hfs, id, content, 0, true)

	has, err := hfs.DB.HasBlocks(id)
	if err != nil {
		t.Fatalf("has blocks: %v", err)
	}
	if has {
		t.Fatal("the file was stored as blocks; this test needs the needle/staged shape to mean anything")
	}

	h, release := openStream(t, hfs, id, false)
	defer release()

	got := readAt(t, h, 0, len(content))
	if !bytes.Equal(got, content) {
		t.Fatalf("a media file with no block rows read back as %d bytes of the wrong content — streamed as holes",
			len(got))
	}
}

// TestStreamingServesHolesAsZeroes covers the other half of the block/hole
// distinction on the streaming path. A block with no row was never stored, so it
// must cost neither an S3 fetch nor rate-limiter credit — at the rate set here a
// single charged block would take eight seconds, so a stall is unmissable.
func TestStreamingServesHolesAsZeroes(t *testing.T) {
	hfs := setupStreaming(t, 1, 16) // 1 MB/s: one charged block would cost ~8s
	id := mustInsert(t, hfs, "sparse.mp4")

	// truncate(2) then write at the end, as dd seek= does: blocks 0 and 1 are
	// holes, only block 2 is ever stored.
	n := &HamstorNode{hfs: hfs, inodeID: id}
	setSize(t, n, 2*db.BlockSize)

	tail := []byte("the only bytes this file ever stored")
	th := NewTestHandle(hfs, id, false)
	if errno := th.TestWriteAt(tail, 2*db.BlockSize); errno != 0 {
		t.Fatalf("sparse write: %v", errno)
	}
	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()
	if blocks := blocksOf(t, hfs, id); len(blocks) != 1 {
		t.Fatalf("want 1 block row, got %d: the holes were materialized", len(blocks))
	}

	h, release := openStream(t, hfs, id, true)
	defer release()

	start := time.Now()
	got := readAt(t, h, 0, fuseReadSize)
	holeRead := time.Since(start)

	if len(got) != fuseReadSize {
		t.Fatalf("hole read returned %d bytes, want %d", len(got), fuseReadSize)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("hole byte at %d is %q, want zero", i, b)
		}
	}
	if holeRead > time.Second {
		t.Fatalf("reading a hole took %v: it was charged to the rate limiter or fetched from S3", holeRead)
	}
	if len(h.streamBlocks) != 0 {
		t.Fatalf("the ring holds %d entries after reading only holes: zeroes are displacing real blocks",
			len(h.streamBlocks))
	}

	// And the one block that does exist still reads correctly across the holes.
	if got := readAt(t, h, 2*db.BlockSize, len(tail)); !bytes.Equal(got, tail) {
		t.Fatalf("tail reads %q, want %q", got, tail)
	}
}

// TestStreamingOverInheritedCacheDir starts where a real upgrade starts: on a
// --cache-dir that is kept across reinstalls and still holds the chunk
// directories an older version wrote.
//
// The tests above all run on a fresh temp dir, where that state cannot occur —
// which is exactly the gap that let two earlier steps ship a fixture that did
// not match the world. os.Open succeeds on a directory, so a stale chunk
// directory found at a key's path turns into a few KB of zeros served as file
// contents.
func TestStreamingOverInheritedCacheDir(t *testing.T) {
	cacheDir := t.TempDir()
	inherited := filepath.Join(cacheDir, "3f", "3f000000-0000-0000-0000-0000000000ff")
	if err := os.MkdirAll(inherited, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inherited, "chunk-000000"), bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	hfs := setupStreaming(t, 4096, 16)
	id, content := writeMediaBlocks(t, hfs, "inherited.mp4", 2)

	c, err := cache.New(cacheDir, 1<<30)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	hfs.Cache = c

	if _, err := os.Stat(inherited); !os.IsNotExist(err) {
		t.Fatalf("the inherited chunk directory survived cache.New: %v", err)
	}

	h, release := openStream(t, hfs, id, true)
	defer release()
	if got := readWhole(t, h, int64(len(content))); !bytes.Equal(got, content) {
		t.Fatal("streaming over an inherited cache directory returned the wrong bytes")
	}
	if total, count := c.Size(); total != 0 || count != 0 {
		t.Fatalf("cache holds %d bytes in %d entries: either streaming wrote to it, or the sweep left something",
			total, count)
	}
}
