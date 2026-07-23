package hfuse

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

// Phase B: write-time eviction lets a file larger than the local disk be copied.
// During a large sequential write, completed blocks are uploaded and committed as
// a growing prefix and hole-punched out of the spill, so the local footprint stays
// within --write-buffer. These tests prove it stays bounded, reads back correctly
// (including the evicted blocks, which must re-download), works under encryption,
// and — the resilience the design was chosen for — leaves a VALID TRUNCATED file
// when an upload fails partway.

// spillDiskUsage sums the ACTUAL disk blocks used by files under dir (st_blocks *
// 512), which respects hole punching: a spill file whose committed blocks were
// punched reports only what is still resident, not its logical length.
func spillDiskUsage(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		info, err := os.Stat(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			total += stat.Blocks * 512
		}
	}
	return total
}

// freshWriteHandle opens a handle the way Create does for a brand-new file, so
// write-time eviction is eligible.
func freshWriteHandle(hfs *HamstorFS, id int64) *TestHandle {
	th := NewTestHandle(hfs, id, true)
	th.h.st.mu.Lock()
	th.h.st.freshWrite = true
	th.h.st.mu.Unlock()
	return th
}

// writeSequential copies content into the handle in 1 MiB chunks, sampling peak
// spill-disk usage after each chunk. Returns the peak.
func writeSequential(t *testing.T, th *TestHandle, spillDir string, content []byte) int64 {
	t.Helper()
	const chunk = 1 << 20
	var peak int64
	for off := 0; off < len(content); off += chunk {
		end := min(off+chunk, len(content))
		if errno := th.TestWriteAt(content[off:end], int64(off)); errno != 0 {
			t.Fatalf("write at %d: %v", off, errno)
		}
		if u := spillDiskUsage(t, spillDir); u > peak {
			peak = u
		}
	}
	return peak
}

func runEvictionCopy(t *testing.T, hfs *HamstorFS) {
	t.Helper()
	spillDir := t.TempDir()
	hfs.SpillDir = spillDir
	hfs.WriteBuffer = 2 * db.BlockSize // tiny buffer: force eviction well before the end

	if !hfs.holePunchSupported() {
		t.Skip("spill filesystem does not support hole punching")
	}

	const nBlocks = 6
	content := blockContent(nBlocks, 4096) // 6 full blocks + a short tail
	total := int64(len(content))

	id := mustInsert(t, hfs, "evict.bin")
	th := freshWriteHandle(hfs, id)

	peak := writeSequential(t, th, spillDir, content)

	// The whole point: the local footprint stayed near the buffer, not the file.
	if peak > hfs.WriteBuffer+3*db.BlockSize {
		t.Fatalf("peak spill %d bytes exceeded buffer %d + margin: eviction is not bounding the footprint",
			peak, hfs.WriteBuffer)
	}
	if peak >= total {
		t.Fatalf("peak spill %d reached the whole-file size %d: nothing was evicted", peak, total)
	}

	// Eviction must actually have committed a prefix mid-write.
	th.h.st.mu.Lock()
	ce := th.h.st.committedExtent
	th.h.st.mu.Unlock()
	if ce == 0 {
		t.Fatal("committedExtent is 0: no block was evicted during the copy")
	}

	if errno := th.TestFlush(); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	th.WaitUpload()
	th.TestRelease()
	hfs.InflightUploads.Wait()

	// The file is whole and consistent in the DB.
	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size != total {
		t.Fatalf("final size %d, want %d", meta.Size, total)
	}
	blocks, err := hfs.DB.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks: %v", err)
	}
	if len(blocks) != nBlocks+1 {
		t.Fatalf("got %d blocks, want %d", len(blocks), nBlocks+1)
	}
	var sum int64
	for _, b := range blocks {
		sum += b.Size
	}
	if sum != total {
		t.Fatalf("SUM(blocks.size)=%d, want %d: the block set does not cover the file", sum, total)
	}
	t.Cleanup(func() {
		for _, b := range blocks {
			hfs.Store.Delete(t.Context(), b.S3Key)
		}
	})

	// Full readback: the evicted blocks were punched from the spill, so this
	// re-downloads them from S3 (decrypting per block when encrypted).
	if hfs.dirtyBytes != 0 {
		t.Fatalf("dirtyBytes = %d after the copy settled, want 0: eviction leaked budget", hfs.dirtyBytes)
	}
	got := readBack(t, hfs, id, len(content))
	if !bytes.Equal(got, content) {
		t.Fatal("readback mismatch after eviction: an evicted block came back wrong")
	}
}

// TestWriteEvictionCopiesFileLargerThanBuffer is the headline case, unencrypted.
func TestWriteEvictionCopiesFileLargerThanBuffer(t *testing.T) {
	hfs, _ := setupTest(t)
	runEvictionCopy(t, hfs)
}

// TestWriteEvictionCopiesFileLargerThanBufferEncrypted proves eviction holds under
// encryption: each evicted block is sealed independently and must decrypt on its
// own when read back.
func TestWriteEvictionCopiesFileLargerThanBufferEncrypted(t *testing.T) {
	hfs, _ := setupTest(t)
	enc, err := crypto.New("evict-test-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("new encryptor: %v", err)
	}
	hfs.Encryptor = enc
	runEvictionCopy(t, hfs)
}

// TestWriteEvictionUploadFailureLeavesValidPrefix is the resilience guarantee the
// incremental-prefix design was chosen for: if S3 breaks partway through a large
// copy, the blocks committed so far form a VALID TRUNCATED file (size == the
// committed extent, SUM(blocks.size) == size, and it reads back), not corruption
// and not a total loss.
func TestWriteEvictionUploadFailureLeavesValidPrefix(t *testing.T) {
	cfg := testutil.RequireS3(t)
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.WriteBuffer = 2 * db.BlockSize
	if !hfs.holePunchSupported() {
		t.Skip("spill filesystem does not support hole punching")
	}
	goodStore := hfs.Store

	badStore, err := s3store.New(context.Background(),
		"hamstor-no-such-bucket-"+strings.ToLower(t.Name()),
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("bad store: %v", err)
	}

	const nBlocks = 6
	content := blockContent(nBlocks, 0)
	id := mustInsert(t, hfs, "half.bin")
	th := freshWriteHandle(hfs, id)

	const chunk = 1 << 20
	failedAt := -1
	swapped := false
	for off := 0; off < len(content); off += chunk {
		// Once at least one block has been evicted successfully, break S3 so the
		// next eviction fails mid-copy.
		if !swapped {
			th.h.st.mu.Lock()
			ce := th.h.st.committedExtent
			th.h.st.mu.Unlock()
			if ce >= db.BlockSize {
				hfs.Store = badStore
				swapped = true
			}
		}
		end := min(off+chunk, len(content))
		if errno := th.TestWriteAt(content[off:end], int64(off)); errno != 0 {
			if errno != syscall.EIO {
				t.Fatalf("write at %d failed with %v, want EIO", off, errno)
			}
			failedAt = off
			break
		}
	}
	if !swapped {
		t.Fatal("no block was evicted before the store was broken; widen the test file")
	}
	if failedAt < 0 {
		t.Fatal("the copy never failed even though the store was broken mid-write")
	}

	// Put the good store back so the committed prefix can be inspected and read.
	hfs.Store = goodStore
	th.TestRelease()
	hfs.InflightUploads.Wait()

	meta, err := hfs.DB.GetInode(id)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if meta.Size == 0 || meta.Size >= int64(len(content)) {
		t.Fatalf("prefix size %d is not a proper truncation of %d bytes", meta.Size, len(content))
	}
	if meta.Size%db.BlockSize != 0 {
		t.Fatalf("prefix size %d is not block-aligned: the committed extent is not a clean prefix", meta.Size)
	}
	blocks, err := hfs.DB.BlocksForInode(id)
	if err != nil {
		t.Fatalf("blocks: %v", err)
	}
	var sum int64
	for _, b := range blocks {
		sum += b.Size
	}
	if sum != meta.Size {
		t.Fatalf("SUM(blocks.size)=%d != size %d: the prefix is not self-consistent (holes)", sum, meta.Size)
	}
	t.Cleanup(func() {
		for _, b := range blocks {
			hfs.Store.Delete(t.Context(), b.S3Key)
		}
	})

	got := readBack(t, hfs, id, int(meta.Size))
	if !bytes.Equal(got, content[:meta.Size]) {
		t.Fatal("the committed prefix read back wrong after a mid-copy failure")
	}
}

// TestTrackSequentialDisablesEvictionOnGapOrRevisit checks the gate that keeps
// eviction from truncating: any write that leaves a gap above the frontier or
// revisits the committed prefix disables eviction, leaving the prefix valid.
func TestTrackSequentialDisablesEvictionOnGapOrRevisit(t *testing.T) {
	t.Run("gap above the frontier", func(t *testing.T) {
		st := &inodeWrite{}
		st.trackSequential(0, db.BlockSize)      // 0 .. 8 MiB
		st.trackSequential(db.BlockSize, 4096)   // contiguous, still fine
		if st.evictBroken {
			t.Fatal("a contiguous append should not break eviction")
		}
		st.trackSequential(10*db.BlockSize, 4096) // gap: leaves a hole
		if !st.evictBroken {
			t.Fatal("a gap above the frontier must disable eviction")
		}
	})
	t.Run("revisit below the committed prefix", func(t *testing.T) {
		st := &inodeWrite{committedExtent: 4 * db.BlockSize, seqHead: 6 * db.BlockSize}
		st.trackSequential(2*db.BlockSize, 4096) // below committedExtent
		if !st.evictBroken {
			t.Fatal("a write into the already-committed prefix must disable eviction")
		}
	})
	t.Run("append extends the frontier", func(t *testing.T) {
		st := &inodeWrite{seqHead: 3 * db.BlockSize}
		st.trackSequential(3*db.BlockSize, db.BlockSize)
		if st.evictBroken {
			t.Fatal("append at the frontier must stay sequential")
		}
		if st.seqHead != 4*db.BlockSize {
			t.Fatalf("seqHead = %d, want %d", st.seqHead, int64(4*db.BlockSize))
		}
	})
}
