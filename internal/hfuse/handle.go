package hfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/ratelimit"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
	"github.com/milan/hamstor/internal/volume"
)

// spillThreshold is the size at which writes switch from memory to a temp file.
//
// One block, because that is the unit in which data leaves the process anyway: a
// flush uploads whole blocks, and an encrypted block is a single GCM message, so
// holding more than one on the heap buys nothing. The number is set against
// debug.SetMemoryLimit(150 << 20) in main.go, not picked for roundness — say so
// here, or the next person who raises it reintroduces the following.
//
// It used to be 64 MiB, which is the whole process budget for one file, and the
// comparisons were `>` rather than `>=`, so a file of exactly 64 MiB never
// spilled at all. Getting a buffer that large means growing it by append, which
// is geometric: at the last reallocation the old array and the new one are both
// live, so one such file transiently needs ~96-128 MiB. Measured on a live
// encrypted B2 mount: four parallel 64 MiB writes peaked at 313 MB RSS and took
// 40 s to commit. Nothing errors when that happens — the GC just runs
// continuously and every upload crawls, which from outside looks like data loss,
// because close(2) succeeds while stat(2) reports 0 until the commit lands.
const spillThreshold = db.BlockSize

type HamstorHandle struct {
	hfs     *HamstorFS
	inodeID int64
	// inode is the kernel-facing inode, used to invalidate cached attributes
	// once an async upload commits the real size. nil in tests that build a
	// handle without a mounted tree; the notify is then skipped.
	inode *fs.Inode

	// st is the file state shared with every other handle open on this inode:
	// buffer, dirty/loaded flags, spill and cache files, upload attempts. It is
	// never nil, and st.mu guards all of it — this handle has no lock of its
	// own. Obtained from hfs.acquireWrite, dropped in Release.
	st *inodeWrite

	// fileSize is this handle's own view of the logical size, taken at open time
	// and refreshed only when this handle loads.
	//
	// It is NOT the file's size and must never be used as one. A handle that
	// never loads the state (a sibling got there first) keeps its open-time value
	// forever, so anything shared — notably readLoaded's clamp — must use
	// st.size instead.
	fileSize int64

	// appendMode records O_APPEND from Open. The kernel computes append offsets
	// from its cached st_size (attr timeout, 60s by default), so two appenders
	// are handed the same stale offset and overwrite each other — a shared
	// buffer alone does not fix that. Write ignores the kernel's offset for
	// these handles and appends at the true shared end of file. Linux already
	// ignores the offset for pwrite() on an O_APPEND fd, so this matches it.
	appendMode bool

	released bool

	// Streaming mode (multimedia files). See readStreaming: the handle serves
	// the file a block at a time out of streamBlocks, rate-limited, and puts
	// nothing in the disk cache.
	streaming   bool
	rateLimiter *ratelimit.Bucket
	// streamBlocks is a ring of recently served blocks, and it is the whole of
	// this handle's memory budget: at most streamBlocksCap * db.BlockSize.
	//
	// It cannot be dropped in favour of the disk cache the way the block fault
	// path uses it, because streaming deliberately does not write there — a
	// 4 GB film played once would evict everything else. Without a ring, every
	// 128 KB read the kernel sends would re-download the whole 8 MiB block it
	// falls in.
	streamBlocks    []streamBlock
	streamBlocksCap int
	lastStreamOff   int64 // for seek detection
}

type streamBlock struct {
	index int64
	data  []byte
}

var (
	_ fs.FileReader   = (*HamstorHandle)(nil)
	_ fs.FileWriter   = (*HamstorHandle)(nil)
	_ fs.FileFlusher  = (*HamstorHandle)(nil)
	_ fs.FileReleaser = (*HamstorHandle)(nil)
	_ fs.FileFsyncer  = (*HamstorHandle)(nil)
)

// ensureLoaded populates the shared state's contents from storage. Must be
// called with st.mu held; it may release and reacquire it while waiting for an
// in-flight upload.
func (h *HamstorHandle) ensureLoaded(ctx context.Context) syscall.Errno {
	if h.st.poisoned != nil {
		return syscall.EIO
	}
	if h.st.loaded {
		return 0
	}
	if h.st.isNew {
		h.st.buf = []byte{}
		h.st.loaded = true
		h.st.setSize(0)
		return 0
	}

	// An upload of this inode may be in flight, in which case the DB still names
	// the key it is about to replace. Loading that would hand back content
	// predating the upload — harmless for a read on its own, but it becomes the
	// base the next write builds on, and committing that silently drops
	// everything the upload carried. Wait it out.
	//
	// This costs nothing on the fast read paths: Read tries streaming and
	// chunked reads before it ever gets here, and both bypass the shared buffer.
	// What reaches this point already faces a full download.
	if errno := h.st.awaitUpload(); errno != 0 {
		return errno
	}
	// awaitUpload may have dropped the lock; re-check what it could have changed.
	if h.st.poisoned != nil {
		return syscall.EIO
	}
	if h.st.loaded {
		return 0
	}

	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		return toErrno(err)
	}
	if meta.VolS3Key != "" {
		return h.loadFromVolume(ctx, meta)
	}

	// Stored as blocks. This must be tested BEFORE the staged branch below: a
	// block-stored file has no vol_s3_key, which is exactly the shape that branch
	// takes as "staged but not yet packed". It would then hunt for a staging file
	// that never existed and return EIO for a healthy file.
	//
	// size == 0 skips the query because a zero-length file cannot have blocks —
	// CommitBlocks deletes them all at size 0.
	//
	// HasBlocks, not BlocksForInode: this only has to route, and a 4 TB file has
	// 524288 rows. Pulling them to learn one bit would put the cost of the file's
	// size back on every open, which is the cost this step exists to remove.
	if meta.Size > 0 {
		has, bErr := h.hfs.DB.HasBlocks(h.inodeID)
		if bErr != nil {
			return toErrno(bErr)
		}
		// Larger than a needle means it was never staged (flushStaged is only
		// reached below that size), so with no needle and no blocks it is not a
		// file whose data went missing — it is one big hole. `truncate -s 4G` and
		// `dd seek=` both produce exactly that shape, and treating it as staged
		// sent the read hunting for a staging file that never existed and
		// answered EIO after five retries. Attaching gives it a sparse store
		// whose blocks all resolve to holes, which is what it is.
		if has || meta.Size > volume.MaxNeedleSize {
			return h.attachBlocks(meta.Size)
		}
	}

	// File staged locally, not yet packed into a volume
	if meta.Size > 0 && h.hfs.VolumeBuilder != nil {
		data, size, errno := h.hfs.readStaged(ctx, h.inodeID)
		if errno != 0 {
			return errno
		}
		if data == nil {
			// It gained real storage while we looked — the builder packed it into
			// a volume, or an overwrite grew it past MaxNeedleSize and committed
			// it as blocks. Start over rather than guess.
			meta2, err2 := h.hfs.DB.GetInode(h.inodeID)
			if err2 != nil {
				return toErrno(err2)
			}
			if meta2.VolS3Key != "" {
				return h.loadFromVolume(ctx, meta2)
			}
			if meta2.Size > 0 {
				has, bErr := h.hfs.DB.HasBlocks(h.inodeID)
				if bErr != nil {
					return toErrno(bErr)
				}
				if has || meta2.Size > volume.MaxNeedleSize {
					return h.attachBlocks(meta2.Size)
				}
			}
			return syscall.EIO
		}
		h.st.buf = data
		h.st.loaded = true
		h.st.wholeLoaded = true // a staging file: the next block commit drops it
		h.st.setSize(size)
		h.fileSize = size
		return 0
	}

	// Nothing left to load from: no needle, no blocks, no staging file. That is
	// an empty file — the only remaining shape with no storage of its own, now
	// that an inode cannot name a whole-file object.
	h.st.buf = []byte{}
	h.st.loaded = true
	h.st.setSize(meta.Size)
	h.fileSize = meta.Size
	return 0
}

// loadFromVolume loads a file packed in a volume S3 object into the handle.
//
// The read is served from the WHOLE volume object, not a byte-range slice of it:
// on a cache miss the entire volume (<=8 MB) is downloaded once and cached, then
// every needle in it is read locally. Files in one directory are almost always
// packed into the same one or two volumes (the builder packs in copy order,
// which follows directory traversal), so browsing a folder cold drops from one
// S3 request per file to one per volume — the read-side counterpart of the
// write-side packing. Individual reads pay at most ~8 MB of extra download that
// their siblings then reuse; with no cache to amortise it (or a dirty buffer we
// must not clobber) it falls back to the old per-needle range read.
func (h *HamstorHandle) loadFromVolume(ctx context.Context, meta *db.InodeMeta) syscall.Errno {
	h.fileSize = meta.Size

	data, err := h.readNeedle(ctx, meta)
	if err != nil {
		log.Printf("hamstor: volume read failed for inode %d (vol %s offset %d): %v",
			h.inodeID, meta.VolS3Key, meta.VolOffset, err)
		return toErrno(err)
	}
	if h.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
		data, err = h.hfs.Encryptor.Decrypt(data)
		if err != nil {
			log.Printf("hamstor: volume decrypt failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
	}

	h.st.buf = data
	h.st.loaded = true
	h.st.wholeLoaded = true // a needle: the next block commit drops it wholesale
	h.st.setSize(meta.Size)
	return 0
}

// attachBlocks gives the shared state a backing store for a block-stored file
// WITHOUT fetching anything: a zeroed heap buffer for small files, a sparse
// temp file above spillThreshold. Blocks arrive later, one at a time, through
// faultBlock.
//
// This is where the whole layout finally pays: opening a 3 GB log to append one
// line used to download 3 GB first, and a file above the download limit could
// not be opened for writing at all. Now opening costs one file creation, and the
// append touches one block.
//
// The store is exactly size bytes long, and that is load-bearing rather than
// tidy: logicalSize() reports the end of file from it, and readLoaded and Write
// index into it by absolute offset. A store shorter than the file would silently
// truncate; longer, and a flush would commit bytes past the end.
func (h *HamstorHandle) attachBlocks(size int64) syscall.Errno {
	if size >= spillThreshold {
		f, err := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
		if err != nil {
			log.Printf("hamstor: block spill create for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		// Sparse: the hole costs no disk and reads as zeroes, which is exactly
		// what an unfaulted block and a real hole both need it to do.
		if err := f.Truncate(size); err != nil {
			f.Close()
			os.Remove(f.Name())
			log.Printf("hamstor: block spill size for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		h.st.spillFile = f
		h.st.spillSize = size
	} else {
		h.st.buf = make([]byte, size)
	}

	h.st.blockBacked = true
	h.st.wholeLoaded = false // sparse by construction: blocks arrive one by one
	h.st.presentBlocks = nil // nothing is materialized yet
	h.st.loaded = true
	h.st.setSize(size)
	h.fileSize = size
	return 0
}

// faultBlock materializes one block into the backing store. Must be called with
// st.mu held.
//
// Two different absences meet here and must not be confused. A block with no row
// is a HOLE: the store already reads as zeroes there, so it is marked present
// without a fetch — which is what keeps a write at offset 1 GB costing one
// object rather than 128. A block that has a row but is not present is simply
// not local yet, and serving it without fetching would hand back zeroes for real
// data.
//
// The fetched object is clamped TWICE, against two different over-lengths, and
// dropping either one resurrects data a truncate was supposed to remove:
//
//   - to the block's live extent within the file, min(BlockSize, size-start),
//     because st.size is the only authority on how long the file is;
//   - to b.Size, how many of the block's bytes the file still claims. A truncate
//     that cut into this block only shortened that number — the object was not
//     rewritten — so the object still holds the old tail. Grow the file back and
//     the live extent alone would happily place all of it.
func (h *HamstorHandle) faultBlock(ctx context.Context, idx int64) syscall.Errno {
	if h.st.present(idx) {
		return 0
	}
	start := idx * db.BlockSize
	if start >= h.st.size {
		// Wholly past the end of file: nothing to serve, and nothing to fetch.
		h.st.markPresent(idx)
		return 0
	}

	b, ok, err := h.hfs.DB.BlockAt(h.inodeID, idx)
	if err != nil {
		return toErrno(err)
	}
	if !ok {
		h.st.markPresent(idx) // hole
		return 0
	}

	data, err := h.fetchBlock(ctx, b)
	if err != nil {
		log.Printf("hamstor: block read failed for inode %d block %d (%s): %v",
			h.inodeID, idx, b.S3Key, err)
		return toErrno(err)
	}
	data = clampBlock(data, h.st.size, start, b.Size)

	if h.st.spillFile != nil {
		if _, werr := h.st.spillFile.WriteAt(data, start); werr != nil {
			log.Printf("hamstor: block place for inode %d block %d: %v", h.inodeID, idx, werr)
			return syscall.EIO
		}
	} else {
		copy(h.st.buf[start:], data)
	}
	h.st.markPresent(idx)
	return 0
}

// materializeRange faults in every block overlapping [off, off+n) that a read is
// about to serve. Must be called with st.mu held.
func (h *HamstorHandle) materializeRange(ctx context.Context, off, n int64) syscall.Errno {
	if !h.st.blockBacked || n <= 0 || off < 0 {
		return 0
	}
	last := off + n - 1
	if last >= h.st.size {
		last = h.st.size - 1
	}
	if last < off {
		return 0
	}
	for b := off / db.BlockSize; b <= last/db.BlockSize; b++ {
		if errno := h.faultBlock(ctx, b); errno != 0 {
			return errno
		}
	}
	return 0
}

// materializeForWrite faults in the blocks a write is about to overwrite only in
// part. Must be called with st.mu held and BEFORE the write lands.
//
// This is the sharpest edge of lazy materialization, and it fails silently. A
// flush uploads a whole dirty block; if the block was never fetched, everything
// the write did not itself cover is a zero in the backing store, and those
// zeroes go to S3 as the file's contents. Nothing errors, nothing logs, and the
// damage surfaces whenever someone next reads that region.
//
// A block is skipped only when nothing can be lost: either it stored nothing
// (the write is past the old end of file, so the region is a hole either way),
// or the write covers every byte that was stored in it. Sequential writes —
// cp, a fresh copy, a whole-file rewrite — hit the second case for every full
// block, so the common path still fetches nothing.
func (h *HamstorHandle) materializeForWrite(ctx context.Context, off, n int64) syscall.Errno {
	if !h.st.blockBacked || n <= 0 || off < 0 {
		return 0
	}
	oldSize := h.st.size
	for b := off / db.BlockSize; b <= (off+n-1)/db.BlockSize; b++ {
		start := b * db.BlockSize
		stored := min(start+int64(db.BlockSize), oldSize)
		if start >= stored {
			continue // nothing stored in this block yet
		}
		if off <= start && off+n >= stored {
			continue // the write replaces everything that was stored in it
		}
		if errno := h.faultBlock(ctx, b); errno != 0 {
			return errno
		}
	}
	return 0
}

// fetchBlock returns one block's plaintext, from the disk cache when it is
// there and from S3 otherwise.
//
// The cache holds PLAINTEXT, unlike the whole-volume entries: a block entry is
// handed straight back as file contents, so caching the ciphertext would serve
// encrypted bytes as the file. Each block is its own object under its own key, so a new version
// of a block is a new key and the old entry can never be mistaken for the new
// one.
func (h *HamstorHandle) fetchBlock(ctx context.Context, b db.BlockCommit) ([]byte, error) {
	if data, ok := h.blockFromCache(b.S3Key); ok {
		return data, nil
	}
	data, err := h.downloadBlock(ctx, b)
	if err != nil {
		return nil, err
	}
	if h.hfs.Cache != nil {
		if putErr := h.hfs.Cache.Put(b.S3Key, data); putErr != nil {
			log.Printf("hamstor: cache put block %s: %v", b.S3Key, putErr)
		}
	}
	return data, nil
}

// blockFromCache returns one block's plaintext if the disk cache holds it.
//
// Split out of fetchBlock so the streaming path can share the lookup without
// sharing the Put that follows it: streaming reads local bytes gladly but must
// never seed the cache, or one film played once evicts everything else in it.
func (h *HamstorHandle) blockFromCache(key string) ([]byte, bool) {
	if h.hfs.Cache == nil {
		return nil, false
	}
	f, err := h.hfs.Cache.Open(key)
	if err != nil {
		return nil, false
	}
	data, rerr := io.ReadAll(f)
	f.Close()
	if rerr != nil {
		return nil, false
	}
	return data, true
}

// downloadBlock fetches one block's object from S3 and decrypts it.
//
// Each block is encrypted on its own — crypto.Encrypt emits a fresh
// [version][nonce][ct+tag] per call — so a block object is independently
// decryptable. That is what lets streaming range over an encrypted file at all,
// which the whole-file layout could not do.
func (h *HamstorHandle) downloadBlock(ctx context.Context, b db.BlockCommit) ([]byte, error) {
	data, err := h.hfs.Store.Download(ctx, b.S3Key)
	if err != nil {
		return nil, err
	}
	if h.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
		data, err = h.hfs.Encryptor.Decrypt(data)
		if err != nil {
			return nil, fmt.Errorf("decrypt block %d (%s): %w", b.Index, b.S3Key, err)
		}
	}
	return data, nil
}

// clampBlock cuts a fetched block down to the bytes the file still claims.
//
// Both clamps are load-bearing and neither implies the other, which is why this
// is one function rather than an expression repeated at each fetch site:
//
//   - size-start is the block's live extent within the file, because the logical
//     size is the only authority on how long the file is;
//   - stored is b.Size, how many of the block's bytes the file still claims. A
//     truncate that cut into this block only shortened that number — the object
//     was not rewritten — so it still holds the old tail, and the live extent
//     alone would happily place all of it when the file grows back.
func clampBlock(data []byte, size, start, stored int64) []byte {
	if extent := min(int64(db.BlockSize), size-start, stored); int64(len(data)) > extent {
		if extent < 0 {
			extent = 0
		}
		return data[:extent]
	}
	return data
}

// readNeedle returns the raw (still-encrypted, if the mount is encrypted) bytes
// of one needle. It serves them from the cached whole-volume object when it can,
// downloading the volume once (deduped across sibling inodes) on a miss.
func (h *HamstorHandle) readNeedle(ctx context.Context, meta *db.InodeMeta) ([]byte, error) {
	// No cache to amortise a whole-volume fetch, or a dirty buffer we must not
	// disturb: fall back to a per-needle range read (the original behaviour).
	if h.hfs.Cache == nil || h.st.dirty {
		return h.hfs.Store.DownloadRange(ctx, meta.VolS3Key, meta.VolOffset, meta.VolSize)
	}

	volCacheKey := "volobj/" + meta.VolS3Key

	// Fast path: read just this needle's bytes out of the cached whole volume.
	if f, err := h.hfs.Cache.Open(volCacheKey); err == nil {
		buf := make([]byte, meta.VolSize)
		n, rerr := f.ReadAt(buf, meta.VolOffset)
		f.Close()
		if (rerr == nil || rerr == io.EOF) && int64(n) == meta.VolSize {
			return buf, nil
		}
		// Short or failed read (e.g. cache file truncated by eviction) — re-fetch.
	}

	// Miss: fetch the whole volume once, then slice this needle out of it.
	volData, err := h.fetchVolume(meta.VolS3Key, volCacheKey)
	if err != nil {
		return nil, err
	}
	if meta.VolOffset < 0 || meta.VolOffset+meta.VolSize > int64(len(volData)) {
		return nil, fmt.Errorf("needle [%d:%d] out of range for volume %s (len %d)",
			meta.VolOffset, meta.VolOffset+meta.VolSize, meta.VolS3Key, len(volData))
	}
	buf := make([]byte, meta.VolSize)
	copy(buf, volData[meta.VolOffset:meta.VolOffset+meta.VolSize])
	return buf, nil
}

// fetchVolume downloads the whole volume object and caches it, deduping
// concurrent callers for the same volume via singleflight so a parallel browse
// of a packed directory issues one download per volume, not one per file.
//
// The download runs on a detached context: the volume fill benefits every
// sibling reader, so one reader cancelling (e.g. a file manager abandoning a
// thumbnail) must not fail the shared fetch for the others. Store.Download's own
// retry bounds it.
func (h *HamstorHandle) fetchVolume(volKey, volCacheKey string) ([]byte, error) {
	v, err, _ := h.hfs.volumeFetch.Do(volKey, func() (any, error) {
		// A racing caller may have cached it between our fast-path miss and now.
		if f, oerr := h.hfs.Cache.Open(volCacheKey); oerr == nil {
			data, rerr := io.ReadAll(f)
			f.Close()
			if rerr == nil {
				return data, nil
			}
		}
		data, derr := h.hfs.Store.Download(context.Background(), volKey)
		if derr != nil {
			return nil, derr
		}
		if putErr := h.hfs.Cache.Put(volCacheKey, data); putErr != nil {
			log.Printf("hamstor: cache put whole volume %s: %v", volCacheKey, putErr)
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (h *HamstorHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()

	// Streaming mode for multimedia files — rate-limited, bounded memory, no
	// disk cache. Skipped once the shared state is dirty: streaming reads the
	// committed blocks, so with a writer's buffer sitting in front of them it
	// would serve the version that writer is in the middle of replacing.
	if h.streaming && !h.st.dirty {
		return h.readStreaming(ctx, dest, off)
	}

	// Fast path: the state is attached, so the read is served from the backing
	// store — faulting in whatever part of it this read needs and no more.
	if h.st.loaded {
		return h.readLoaded(ctx, dest, off)
	}

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return nil, errno
	}
	return h.readLoaded(ctx, dest, off)
}

func (h *HamstorHandle) readLoaded(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Fetch what this read actually needs, and only that. For a block-backed
	// state the store is sparse: a region that was never faulted reads as zeroes,
	// which is indistinguishable from a hole until presentBlocks is consulted.
	if errno := h.materializeRange(ctx, off, int64(len(dest))); errno != 0 {
		return nil, errno
	}

	// Clamp clean (non-dirty) reads to the logical size. A truncate that shrank
	// the inode may not have rewritten the backing object (e.g. truncate() on a
	// path with no open write handle), so the cache file / buffer can hold stale
	// bytes past EOF; clamping here serves the correct truncated view. For dirty
	// states the buffer/spill length is authoritative (writes may have extended
	// past the stored size), so no clamp is applied.
	//
	// Clamp to the SHARED size, never to h.fileSize: a handle that opened when
	// the file was shorter and never loaded it itself (a sibling got there first)
	// still holds its open-time snapshot, and clamping to that would cut
	// everyone's contents down to it.
	clampLen := func(n int) int {
		if h.st.dirty {
			return n
		}
		avail := h.st.size - off
		if avail < 0 {
			avail = 0 // read starts past EOF — return nothing
		}
		if int64(n) > avail {
			return int(avail)
		}
		return n
	}

	// Read from spill file
	if h.st.spillFile != nil {
		if off >= h.st.spillSize {
			return fuse.ReadResultData(nil), 0
		}
		n, err := h.st.spillFile.ReadAt(dest, off)
		if n == 0 && err == io.EOF {
			return fuse.ReadResultData(nil), 0
		}
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(dest[:clampLen(n)]), 0
	}

	// Read from in-memory buffer
	if off >= int64(len(h.st.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.st.buf)) {
		end = int64(len(h.st.buf))
	}
	if !h.st.dirty && h.st.size >= 0 && end > h.st.size {
		end = h.st.size
	}
	if end <= off {
		return fuse.ReadResultData(nil), 0
	}
	// Return a copy, not a slice aliasing h.st.buf: go-fuse copies the bytes out
	// after Read releases h.st.mu, which could race a concurrent Write that
	// reallocates or mutates h.st.buf and tear the in-flight read.
	out := make([]byte, end-off)
	copy(out, h.st.buf[off:end])
	return fuse.ReadResultData(out), 0
}

// --- Streaming mode (multimedia) ---

// readStreaming serves a media file one block at a time, rate-limited, with a
// bounded memory footprint and without seeding the disk cache. Called with
// st.mu held; it releases and re-acquires the lock while waiting on the rate
// limiter.
//
// It is the one read path with a footprint that does not grow with the file.
// Everything else attaches a backing store the length of the file and faults
// into it, so a sequential read of a 4 GB film materializes 4 GB in the spill
// directory. Here nothing is attached at all: the blocks a read touches are
// fetched, served, and dropped when the ring wraps.
//
// The disk cache is read but never written. A film watched once would otherwise
// evict most of what the cache holds and then, being the largest thing in it,
// buy nothing for what it displaced. Bytes already local are still served from
// there, and — because they cost no S3 bandwidth — without paying the rate
// limiter.
func (h *HamstorHandle) readStreaming(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= h.fileSize {
		return fuse.ReadResultData(nil), 0
	}

	length := int64(len(dest))
	if off+length > h.fileSize {
		length = h.fileSize - off
	}

	// Detect seek: a non-sequential read must not queue behind credit the
	// sequential playback before it used up. A player that jumps to a chapter
	// would otherwise sit through the wait its own earlier reads earned.
	if h.rateLimiter != nil && off != h.lastStreamOff {
		h.rateLimiter.Reset()
	}

	buf := make([]byte, 0, length)
	for bi := off / db.BlockSize; bi <= (off+length-1)/db.BlockSize; bi++ {
		start := bi * db.BlockSize

		data, errno := h.streamBlockData(ctx, bi, start)
		if errno != 0 {
			return nil, errno
		}

		// Block-relative bounds of what this read wants, capped at the block's
		// live extent within the file.
		lo := max(int64(0), off-start)
		hi := min(int64(db.BlockSize), off+length-start, h.fileSize-start)

		// Whatever the object supplied, then zeroes to the live extent. The
		// padding is not a corner case: a hole arrives here as no data at all,
		// and a block whose stored size was shortened by a truncate the file has
		// since grown back past must read as zeroes there — cutting the read
		// short instead would report a hole in the middle of a file as EOF.
		if end := min(hi, int64(len(data))); end > lo {
			buf = append(buf, data[lo:end]...)
			lo = end
		}
		if hi > lo {
			buf = append(buf, make([]byte, hi-lo)...)
		}
	}

	h.lastStreamOff = off + int64(len(buf))
	return fuse.ReadResultData(buf), 0
}

// streamBlockData returns whatever block bi stores, from the ring, from the disk
// cache, or from S3 behind the rate limiter. Called with st.mu held; it may
// release and re-acquire it.
//
// It returns NOTHING for a hole (a block with no row) rather than a block of
// zeroes: the caller pads to the live extent anyway, and a hole must cost
// neither a fetch nor rate-limiter credit — the same distinction faultBlock
// draws. Charging for holes would stall playback of a sparse file on bytes that
// were never stored, and materializing them would put 8 MiB of zeroes in the
// ring for every hole crossed, evicting blocks that cost something to obtain.
func (h *HamstorHandle) streamBlockData(ctx context.Context, bi, start int64) ([]byte, syscall.Errno) {
	if data := h.getStreamBlock(bi); data != nil {
		return data, 0
	}

	b, ok, err := h.hfs.DB.BlockAt(h.inodeID, bi)
	if err != nil {
		return nil, toErrno(err)
	}
	if !ok {
		return nil, 0 // hole
	}

	if data, hit := h.blockFromCache(b.S3Key); hit {
		data = clampBlock(data, h.fileSize, start, b.Size)
		h.putStreamBlock(bi, data)
		return data, 0
	}

	if h.rateLimiter != nil {
		extent := min(int64(db.BlockSize), h.fileSize-start, b.Size)
		h.st.mu.Unlock()
		werr := h.rateLimiter.Wait(ctx, int(extent))
		h.st.mu.Lock()
		if werr != nil {
			return nil, syscall.EINTR
		}
		if h.released {
			return nil, syscall.EIO
		}
		// Another read on this handle may have fetched it while we waited.
		if data := h.getStreamBlock(bi); data != nil {
			return data, 0
		}
	}

	data, err := h.downloadBlock(ctx, b)
	if err != nil {
		log.Printf("hamstor: stream block fetch failed for inode %d block %d (%s): %v",
			h.inodeID, bi, b.S3Key, err)
		return nil, toErrno(err)
	}
	data = clampBlock(data, h.fileSize, start, b.Size)
	h.putStreamBlock(bi, data)
	return data, 0
}

// getStreamBlock returns a block from the in-memory ring, or nil.
func (h *HamstorHandle) getStreamBlock(index int64) []byte {
	for _, sb := range h.streamBlocks {
		if sb.index == index {
			return sb.data
		}
	}
	return nil
}

// putStreamBlock stores a block in the ring, dropping the oldest when full.
//
// The cap is the handle's entire memory budget and it is derived from
// --stream-buffer in Open. It has a floor of one block, not four: at an 8 MiB
// unit a floor of four would pin 32 MiB per open media file against the
// process's 150 MB limit.
func (h *HamstorHandle) putStreamBlock(index int64, data []byte) {
	for _, sb := range h.streamBlocks {
		if sb.index == index {
			return
		}
	}
	if h.streamBlocksCap > 0 {
		for len(h.streamBlocks) >= h.streamBlocksCap {
			h.streamBlocks = h.streamBlocks[1:]
		}
	}
	h.streamBlocks = append(h.streamBlocks, streamBlock{index: index, data: data})
}

// spillState moves an inode's in-memory contents to a temp file, for large
// writes and for growing a file past the point where holding it on the heap
// makes sense. Must be called with st.mu held.
//
// It moves the backing store and nothing else: the bytes keep their offsets, so
// presentBlocks and dirtyBlocks stay true of the result and must not be reset
// here. Clearing them would make a materialized block look absent (a re-fetch,
// harmless) or a dirty one look clean (a lost write, not harmless).
func (hfs *HamstorFS) spillState(st *inodeWrite) error {
	if st.spillFile != nil {
		return nil
	}
	f, err := os.CreateTemp(hfs.SpillDir, "hamstor-spill-*")
	if err != nil {
		return err
	}
	if len(st.buf) > 0 {
		if _, err := f.Write(st.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
	}
	st.spillFile = f
	st.spillSize = int64(len(st.buf))
	st.buf = nil // free memory
	return nil
}

func (h *HamstorHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// Backpressure: block here, holding no lock, until the un-uploaded footprint
	// has room, so a bulk copy paces to the S3 drain rate and the spill dir stays
	// bounded. A no-op unless --write-buffer is set. See HamstorFS.admitWrite.
	h.hfs.admitWrite(int64(len(data)))

	h.st.mu.Lock()
	defer h.st.mu.Unlock()

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return 0, errno
	}

	// O_APPEND: ignore the offset the kernel handed us. It computed it from its
	// cached st_size, which two appenders both read before either write lands,
	// so both are told to write at the same place and one silently overwrites
	// the other. The shared state knows the real end of file. (Linux likewise
	// ignores the offset for pwrite() on an O_APPEND fd, so this matches it.)
	// Must come after ensureLoaded above, which is what settles where the
	// contents actually live.
	if h.appendMode {
		off = h.st.logicalSize()
	}

	// Fetch what this write is about to overwrite only in part, before it lands.
	// Skipping this does not fail here — it fails at the next flush, which
	// uploads the whole dirty block and so writes zeroes over everything the
	// write did not cover. Must come after the O_APPEND resolution above, which
	// is what settles where the write really goes.
	if errno := h.materializeForWrite(ctx, off, int64(len(data))); errno != 0 {
		return 0, errno
	}

	// If writing to spill file
	if h.st.spillFile != nil {
		end := off + int64(len(data))
		if end > h.st.spillSize {
			if err := h.st.spillFile.Truncate(end); err != nil {
				log.Printf("hamstor: spill grow to %d for inode %d: %v", end, h.inodeID, err)
				return 0, syscall.EIO
			}
			h.st.spillSize = end
		}
		if _, err := h.st.spillFile.WriteAt(data, off); err != nil {
			log.Printf("hamstor: spill write %d bytes at %d for inode %d: %v", len(data), off, h.inodeID, err)
			return 0, syscall.EIO
		}
		h.st.dirty = true
		h.st.markDirtyRange(off, int64(len(data)))
		h.st.setSize(h.st.spillSize)
		return uint32(len(data)), 0
	}

	// Check if we should spill to disk
	end := off + int64(len(data))
	if end >= spillThreshold {
		if err := h.hfs.spillState(h.st); err != nil {
			log.Printf("hamstor: spill to disk failed: %v", err)
			return 0, syscall.EIO
		}
		if end > h.st.spillSize {
			if err := h.st.spillFile.Truncate(end); err != nil {
				log.Printf("hamstor: spill grow to %d for inode %d: %v", end, h.inodeID, err)
				return 0, syscall.EIO
			}
			h.st.spillSize = end
		}
		if _, err := h.st.spillFile.WriteAt(data, off); err != nil {
			log.Printf("hamstor: spill write %d bytes at %d for inode %d: %v", len(data), off, h.inodeID, err)
			return 0, syscall.EIO
		}
		h.st.dirty = true
		h.st.markDirtyRange(off, int64(len(data)))
		h.st.setSize(h.st.spillSize)
		return uint32(len(data)), 0
	}

	// In-memory write
	if end > int64(len(h.st.buf)) {
		h.st.buf = append(h.st.buf, make([]byte, end-int64(len(h.st.buf)))...)
	}
	copy(h.st.buf[off:], data)
	h.st.dirty = true
	h.st.markDirtyRange(off, int64(len(data)))
	h.st.setSize(int64(len(h.st.buf)))
	return uint32(len(data)), 0
}

func (h *HamstorHandle) Flush(ctx context.Context) syscall.Errno {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()

	if h.st.poisoned != nil {
		return syscall.EIO
	}

	// Not dirty: either nothing was written, or another handle sharing this
	// inode already flushed what we would have. FUSE sends one FLUSH per
	// close(2), so with N handles open this is what collapses N uploads into
	// one — the first close to find the state dirty uploads, the rest no-op.
	if !h.st.dirty {
		if h.st.isNew {
			// Commit at whatever size the inode already has, not at zero. A
			// brand-new file usually is empty, but ftruncate can have set a size
			// without dirtying anything: nothing is loaded, so truncateWriteState
			// had no buffer to resize and only the DB knows. Passing 0 here made
			// `truncate -s 5T newfile` end up as a 0-byte file — the kernel sends
			// a FLUSH straight after CREATE, and a second one on close then
			// committed over the size the truncate had just written.
			size := int64(0)
			if h.st.loaded {
				size = h.st.size
			} else if m, err := h.hfs.DB.GetInode(h.inodeID); err == nil {
				size = m.Size
			}
			if _, err := h.hfs.DB.CommitInode(h.inodeID, size); err != nil {
				return toErrno(err)
			}
			h.st.isNew = false
		}
		return 0
	}

	// Wait for a previous attempt on this inode before reading anything from
	// the DB below: until it commits, the inode still points at the key it is
	// about to replace, so both the staging path's GetInode and the async
	// path's oldKey would be stale. Two flushes that each read the same oldKey
	// end up with the loser deleting the winner's live object.
	if errno := h.st.awaitUpload(); errno != 0 {
		return errno
	}

	// Spill data to disk BEFORE waiting on the semaphore. During bulk copy,
	// thousands of FUSE goroutines block on the semaphore — if each holds
	// file data in memory, that's thousands * avg_file_size = hundreds of MB.
	// By writing to disk first and nil'ing h.st.buf, blocked goroutines hold
	// only a file path, not data.
	var uploadFile *os.File
	var bufSize int64
	canVolumePack := h.hfs.VolumeBuilder != nil

	// Once an inode is stored as blocks it stays that way, even if a later write
	// shrinks it below MaxNeedleSize. The staging path commits through
	// CommitInode, which knows nothing about blocks: the rows would survive the
	// commit and the read path — which checks blocks first — would go on serving
	// the pre-shrink version. One row for a small file is the cheaper end of that
	// trade. isNew skips the query, so a bulk copy of new files pays nothing.
	hasBlocks := false
	if !h.st.isNew {
		hb, hbErr := h.hfs.DB.HasBlocks(h.inodeID)
		if hbErr != nil {
			return toErrno(hbErr)
		}
		hasBlocks = hb
	}
	canStage := canVolumePack && !hasBlocks

	if h.st.spillFile != nil {
		// Already on disk — just take ownership
		uploadFile = h.st.spillFile
		bufSize = h.st.spillSize
		h.st.spillFile = nil
	} else if len(h.st.buf) > 0 {
		bufSize = int64(len(h.st.buf))
		if bufSize <= int64(volume.MaxNeedleSize) && canStage {
			// Small file going to volume staging — keep buf, skip temp spill
		} else {
			sf, sfErr := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
			if sfErr != nil {
				return syscall.EIO
			}
			if _, sfErr = sf.Write(h.st.buf); sfErr != nil {
				sf.Close()
				os.Remove(sf.Name())
				return syscall.EIO
			}
			uploadFile = sf
			h.st.buf = nil // free memory immediately — data is on disk now
		}
	} else {
		// Empty file
		bufSize = 0
	}

	// Publish the attempt before mu is released for the first time (below, for
	// UploadSem). Between that release and the goroutine launch, loaded is
	// false and the DB still names the old key; without a published attempt a
	// concurrent load has nothing to tell it to wait, and would take the old
	// key as the base for the next version. The staging path publishes one too,
	// even though it never starts a goroutine, so that awaitUpload and Fsync
	// behave the same on both paths and staging takes its turn in the per-inode
	// order rather than racing an in-flight async upload.
	att := newUploadAttempt()
	h.st.cur = att
	h.st.loaded = false
	h.st.dirty = false
	h.st.isNew = false

	// Hand the dirty set to the upload along with the bytes. Clearing it here,
	// under the same lock that took the backing store, is what makes the pair
	// consistent: a write landing after this point starts a fresh set and will be
	// carried by the NEXT flush, which awaitUpload orders behind this one.
	dirty := h.st.dirtyBlocks
	h.st.dirtyBlocks = nil

	// Transfer the write-buffer charge to the upload. The dirty bytes stay on disk
	// (or in buf) until the upload releases them, so accountedBlocks is zeroed here
	// — this flush no longer owns them — and the same count is returned to the
	// budget by flushStaged/flushAsync once the bytes are actually gone. Captured
	// under mu, exactly like the dirty set above.
	dirtyCharge := h.st.accountedBlocks
	h.st.accountedBlocks = 0

	// The presence map describes the backing store, so it is handed over with it
	// rather than kept. This is the deliberate answer to D4's snapshot copy: the
	// state does NOT keep a live sparse materialization across a flush, it gives
	// the store up exactly as it always has, so the upload goroutine still holds
	// bytes nobody else can reach and "the goroutine never takes st.mu" needs no
	// new mechanism to stay true. The next access re-attaches and re-faults, from
	// the disk cache that cacheBlock just seeded. Keeping the materialization
	// would buy a re-fault after a mid-write fsync and cost two invariants whose
	// violation is silent.
	blockBacked := h.st.blockBacked
	present := h.st.presentBlocks
	wholeLoaded := h.st.wholeLoaded
	h.st.presentBlocks = nil
	h.st.blockBacked = false
	h.st.wholeLoaded = false

	// materialized answers, for the upload, whether a block really was in the
	// store when this flush took it. A state that was not block-backed was loaded
	// whole, so every block of it is present by construction.
	materialized := func(b int64) bool {
		if !blockBacked {
			return true
		}
		_, ok := present[b]
		return ok
	}

	// Small files: stage to disk, commit immediately, volume builder packs later.
	if bufSize > 0 && bufSize <= int64(volume.MaxNeedleSize) && canStage {
		errno := h.flushStaged(uploadFile, bufSize, dirtyCharge)
		if errno != 0 {
			// The bytes are no longer anywhere: buf was handed to stageData and
			// nil'd, and this path retains nothing (unlike the async one). A
			// sibling handle that loaded the inode back now would get a base
			// that never contained them and commit over the gap, so poison the
			// state instead and let close(2)/fsync report it.
			att.err = fmt.Errorf("stage flush failed: errno %d", errno)
			h.st.poisoned = att.err
		}
		close(att.done)
		return errno
	}

	return h.flushAsync(att, uploadFile, bufSize, dirty, hasBlocks, wholeLoaded, materialized, dirtyCharge)
}

// flushStaged writes a small file to the volume staging directory and commits it
// immediately; the volume builder packs it into a volume object later. Called
// with st.mu held, and keeps it for the whole path — there is no upload
// goroutine here, so the attempt is already complete when this returns.
func (h *HamstorHandle) flushStaged(uploadFile *os.File, bufSize int64, dirtyCharge int) syscall.Errno {
	// Staging is synchronous, so unlike the async path there is no upload goroutine
	// to hand the charge to: return it to the write-buffer budget on every exit.
	if dirtyCharge > 0 {
		defer h.hfs.addDirtyBytes(-int64(dirtyCharge) * db.BlockSize)
	}
	{
		// There is no previous standalone object to clean up: an inode names no
		// object of its own, and an inode that already has blocks never reaches
		// this path (canStage is gated on !hasBlocks). The old volume needle, if
		// any, is accounted for atomically by CommitInode below, which decrements
		// the previously-referenced volume in the same transaction that clears
		// the inode's vol columns.
		stageMeta, stageMetaErr := h.hfs.DB.GetInode(h.inodeID)

		var stageData []byte
		if uploadFile != nil {
			// Data already on disk (spill file from large Write that was truncated)
			stageData = make([]byte, bufSize)
			if _, err := uploadFile.ReadAt(stageData, 0); err != nil && err != io.EOF {
				uploadFile.Close()
				os.Remove(uploadFile.Name())
				return syscall.EIO
			}
			uploadFile.Close()
			os.Remove(uploadFile.Name())
		} else if h.st.buf != nil {
			// Data still in memory — use directly, skip temp file round-trip
			stageData = h.st.buf
			h.st.buf = nil
		}
		if stageData == nil {
			return syscall.EIO
		}

		// Capture a plaintext thumbnail source before stageData is replaced by
		// its ciphertext below. Unlike the spill path there is no plaintext file
		// on disk to reuse here, so an image costs one extra write of at most
		// MaxNeedleSize; the staged file itself cannot be used because it may be
		// encrypted and the builder may pack and delete it at any moment.
		var stageThumbSrc string
		defer func() {
			if stageThumbSrc != "" {
				os.Remove(stageThumbSrc)
			}
		}()
		if stageMetaErr == nil && thumb.IsImageExt(stageMeta.Name) {
			if tf, tErr := os.CreateTemp(h.hfs.SpillDir, "hamstor-thumb-*"); tErr == nil {
				_, wErr := tf.Write(stageData)
				tf.Close()
				if wErr == nil {
					stageThumbSrc = tf.Name()
				} else {
					os.Remove(tf.Name())
					log.Printf("hamstor: thumb source write for inode %d: %v", h.inodeID, wErr)
				}
			} else {
				log.Printf("hamstor: thumb source create for inode %d: %v", h.inodeID, tErr)
			}
		}

		// Encrypt before staging (per-needle encryption)
		if h.hfs.Encryptor != nil && len(stageData) > 0 {
			encrypted, encErr := h.hfs.Encryptor.Encrypt(stageData)
			if encErr != nil {
				log.Printf("hamstor: stage encrypt failed for inode %d: %v", h.inodeID, encErr)
				return syscall.EIO
			}
			stageData = encrypted
		}

		// Write to staging file atomically (tmp + rename) so the builder
		// goroutine never reads a partially-written file.
		stagePath := h.hfs.VolumeBuilder.StagePath(h.inodeID)
		tmpPath := stagePath + ".tmp"
		if err := os.WriteFile(tmpPath, stageData, 0o600); err != nil {
			os.Remove(tmpPath)
			log.Printf("hamstor: stage write failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		if err := os.Rename(tmpPath, stagePath); err != nil {
			os.Remove(tmpPath)
			log.Printf("hamstor: stage rename failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}

		// Commit immediately — file is visible right away
		committed, err := h.hfs.DB.CommitInode(h.inodeID, bufSize)
		if err != nil {
			os.Remove(stagePath)
			return toErrno(err)
		}
		if !committed {
			// The inode was unlinked while we staged. Nothing references these
			// bytes now, so drop the staging file instead of leaving it for
			// CleanupStagingDir to find on the next boot.
			os.Remove(stagePath)
			return 0
		}

		// Notify builder that new staged file is available
		h.hfs.VolumeBuilder.NotifyStaged()

		// Ownership of the thumbnail source passes to scheduleThumb.
		if stageMetaErr == nil {
			h.hfs.scheduleThumb(h.inodeID, stageMeta.Name, stageThumbSrc)
			stageThumbSrc = ""
		}
		return 0
	}
}

// flushAsync uploads the file's dirty blocks in the background and commits them
// once they all land. Called with st.mu held and returns with it held (Flush's
// defer unlocks); it drops the lock around the upload semaphore.
//
// att is already published in st.cur, so a concurrent load waits for this upload
// instead of reading keys it is about to replace. There is one attempt per
// flush, never one per block: a per-block attempt would make awaitUpload wait on
// a set, splinter poisoning into "some blocks failed", and leave Fsync with
// nothing single to wait for. att.err is the first block's error; the rest are
// logged.
//
// dirty is the set of block indexes written since the last flush. hasBlocks says
// whether the inode was ALREADY stored as blocks, which decides whether the
// dirty set is a complete description of what must be uploaded — see below.
// materialized reports whether a block was really in the snapshot when Flush
// took it, which is what the check below turns from a silent corruption into an
// error.
func (h *HamstorHandle) flushAsync(att *uploadAttempt, uploadFile *os.File, bufSize int64, dirty map[int64]struct{}, hasBlocks, wholeLoaded bool, materialized func(int64) bool, dirtyCharge int) syscall.Errno {
	// releaseCharge returns this flush's write-buffer charge to the budget exactly
	// once: here on an early return, or from the upload goroutine's defer once the
	// spill is gone. addDirtyBytes broadcasts, so a blocked writer re-checks. Only
	// one path ever runs it — an early return returns without launching the
	// goroutine — so the dirtyCharge guard needs no lock of its own.
	releaseCharge := func() {
		if dirtyCharge > 0 {
			h.hfs.addDirtyBytes(-int64(dirtyCharge) * db.BlockSize)
			dirtyCharge = 0
		}
	}

	// Now wait for upload slot — this goroutine holds no file data in RAM.
	h.st.mu.Unlock()
	h.hfs.UploadSem <- struct{}{}
	h.st.mu.Lock()

	// Read the inode only now. Flush already waited for any previous attempt to
	// commit, so what we read here reflects the version this upload really
	// supersedes. (The keys being replaced are read again inside CommitBlocks'
	// transaction — this read decides only what SHAPE the old storage has.)
	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		<-h.hfs.UploadSem
		if uploadFile != nil {
			uploadFile.Close()
			os.Remove(uploadFile.Name())
		}
		releaseCharge()
		att.err = err
		h.st.poisoned = err
		close(att.done)
		return toErrno(err)
	}
	fileName := meta.Name
	// Retention is worth exactly as much as what a failure would otherwise
	// destroy, so the question is whether anything survives one — never what the
	// status says. Where a previous version does survive (a needle, a staging
	// file, older blocks; only the keys this attempt uploaded get dropped) there
	// is nothing to keep, and retaining anyway would re-encrypt and copy a whole
	// file to disk to build something the next boot deletes.
	//
	// This used to test meta.Status == "pending", and the case that falls through
	// the gap is every overwrite: open(O_TRUNC) leaves the inode 'committed' with
	// no storage at all (see inodeHasStorage), so a failed rewrite dropped the
	// file's only remaining copy and reported it as survivable.
	hasPrev := inodeHasStorage(h.hfs.DB, meta)

	// Which blocks must this upload write?
	//
	// For a file already stored as blocks, exactly the dirty ones: every other
	// block keeps its key, and uploading them again would be pure waste. But when
	// the file is stored as ANYTHING ELSE — a volume needle, a staging file —
	// CommitBlocks drops that storage wholesale (it decrements the volume and
	// clears the vol columns). Committing only the dirty blocks would then leave
	// the rest of the file with no storage at all: change one byte of a needle
	// and everything but that byte silently becomes holes. So converting to
	// blocks rewrites the whole file, once.
	//
	// A file with no prior storage is NOT converting, and must not be forced to
	// write every block: a sparse write at offset 1 GB would then upload 128
	// blocks of zeroes instead of one.
	//
	// The test for that is how the backing store was filled, never inodes.size.
	// Size cannot tell a staged file from a hole — both are "size > 0, no blocks,
	// no needle" — and every real tool that writes sparsely calls ftruncate
	// FIRST, so the size is already set before the first write arrives. Measured
	// on a live mount with size as the test: `dd bs=1M seek=4096 count=1` on a
	// new file committed 513 block rows and uploaded 4 GiB of zeroes for one
	// kilobyte of data. vol_s3_key stays in as a cross-check on the needle case,
	// where guessing wrong costs data rather than bandwidth.
	converting := !hasBlocks && (meta.VolS3Key != "" || wholeLoaded)

	lastLive := int64(-1)
	if bufSize > 0 {
		lastLive = (bufSize - 1) / db.BlockSize
	}
	var indexes []int64
	if converting {
		for b := int64(0); b <= lastLive; b++ {
			indexes = append(indexes, b)
		}
	} else {
		for b := range dirty {
			// Trim against the size being committed. A truncate that shrank the
			// file after the write leaves indexes past the new end in the set, and
			// CommitBlocks rejects those outright — upserting a row the same
			// transaction would delete leaves an object nothing ever referenced.
			if b >= 0 && b <= lastLive {
				indexes = append(indexes, b)
			}
		}
		slices.Sort(indexes)
	}

	// Every block about to be uploaded must really be in the snapshot. Under lazy
	// materialization the store is sparse, so a block that was dirtied without
	// first being faulted — or a `converting` file that was only partially
	// materialized — would upload zeroes over everything the write did not cover.
	// That failure is silent and permanent: nothing errors, and the damage
	// surfaces whenever someone next reads that region, possibly months later.
	//
	// The invariant is held by construction (Write faults before it writes, and
	// conversion only ever happens from a needle or a staging file, both loaded
	// whole), which is exactly why it needs a check rather than a comment: the
	// previous steps each retired an assumption the compiler could not see.
	for _, idx := range indexes {
		if materialized(idx) {
			continue
		}
		aErr := fmt.Errorf("inode %d: block %d is dirty but was never materialized", h.inodeID, idx)
		log.Printf("hamstor: refusing to upload an unmaterialized block: %v", aErr)
		<-h.hfs.UploadSem
		if uploadFile != nil {
			uploadFile.Close()
			os.Remove(uploadFile.Name())
		}
		releaseCharge()
		att.err = aErr
		h.st.poisoned = aErr
		close(att.done)
		return syscall.EIO
	}

	hfs := h.hfs
	inodeID := h.inodeID
	st := h.st
	inode := h.inode

	// The upload outlives this handle — Release deliberately does not wait for
	// it. Hold a reference so the shared state survives until the commit lands;
	// otherwise a reopen in that window builds a fresh state, reads the
	// pre-upload key from the DB and writes on top of it.
	hfs.retainUpload(st)
	hfs.InflightUploads.Add(1)
	hfs.InflightCount.Add(1)

	go func() {
		// LIFO ordering of these three matters. Registered first so it runs LAST:
		// InflightUploads.Wait() (used by shutdown, and by tests) must not return
		// until the write-buffer charge has already been returned below it.
		defer hfs.InflightUploads.Done()
		// Runs before Done(), after the decrement: returns this flush's charge to
		// the budget and broadcasts, so a blocked writer re-checks. The bytes have
		// left the spill tier by now — removed, or moved into pending/ on the
		// retention path — either way they no longer count.
		defer releaseCharge()
		// Runs first of the three: decrement then wake, so a writer blocked on the
		// single-file exemption (InflightCount == 0) is re-evaluated even when this
		// upload had no charge of its own. A waiter may wake here and again at
		// releaseCharge — both harmless, cond waiters re-check.
		defer func() {
			hfs.InflightCount.Add(-1)
			hfs.wakeWriters()
		}()
		defer hfs.releaseUpload(inodeID, st)
		// Closing att.done releases every waiter, so it must come after both the
		// commit and the last write to att.err. Defers run LIFO, so this line
		// running before the two above is what orders it correctly.
		defer close(att.done)
		defer func() { <-hfs.UploadSem }()
		// Hand stat(2) back to the DB, whichever way this ends: on success the
		// commit below has just recorded the size, and on failure the state is
		// poisoned and the DB (or the retained set) is the truth. Registered AFTER
		// close(att.done) so LIFO runs it BEFORE — a waiter that wakes up must not
		// find a size the commit has already superseded.
		defer st.clearVisibleSize()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("hamstor: async upload panic: %v", r)
				att.err = fmt.Errorf("panic: %v", r)
			}
		}()

		uploadCtx := hfs.uploadContext()

		// snapPath is the snapshot file: it holds the plaintext of exactly what
		// we are uploading, and nobody else knows about it. That is what keeps
		// this goroutine off st.mu — the rule the whole shared write state rests
		// on. Flush handed it over under the lock; the state kept nothing.
		//
		// It outlives the uploads on every path: a failed one may retain it, and
		// a committed one seeds the disk cache and the thumbnailer from it — the
		// bytes are already here, so downloading them back on the next open is
		// pure waste. Ownership passes to scheduleThumb at the end (it removes
		// non-images itself); until then this defer covers every early return.
		snapPath := ""
		if uploadFile != nil {
			snapPath = uploadFile.Name()
		}
		defer func() {
			if uploadFile != nil {
				uploadFile.Close()
			}
			if snapPath != "" {
				os.Remove(snapPath)
			}
		}()

		blocks := make([]db.BlockCommit, 0, len(indexes))
		uploaded := make([]string, 0, len(indexes))

		// Every upload must succeed before the transaction opens. A committed row
		// whose object does not exist is an unreadable file with no error path —
		// the GET fails whenever someone reads it, perhaps months later.
		var uploadErr error
		for _, idx := range indexes {
			start := idx * db.BlockSize
			extent := min(int64(db.BlockSize), bufSize-start)
			if extent <= 0 {
				continue
			}
			blockKey := s3store.NewKey()

			if hfs.Encryptor == nil {
				// Stream the block straight off the snapshot — no file data on the
				// Go heap. A SectionReader is a ReaderAt+Seeker, which is also what
				// keeps the SDK on its no-allocation path (it slices the body
				// itself rather than filling a PartSize buffer). Buffering here
				// instead would put up to one block per in-flight upload on the
				// heap, and UploadSem admits 32 of them against a 150 MB limit.
				body := io.NewSectionReader(uploadFile, start, extent)
				if err := hfs.Store.UploadReader(uploadCtx, blockKey, body, extent); err != nil {
					uploadErr = err
					break
				}
			} else if err := hfs.uploadSealedBlock(uploadCtx, blockKey, uploadFile, start, extent, inodeID, idx); err != nil {
				uploadErr = err
				break
			}
			uploaded = append(uploaded, blockKey)
			blocks = append(blocks, db.BlockCommit{Index: idx, S3Key: blockKey, Size: extent})
		}

		if uploadErr != nil {
			// Nothing is committed: a file half old and half new is exactly the
			// silent corruption this layout was chosen to avoid. Drop whatever did
			// land, so the bucket is not left with objects nothing references.
			//
			// Unless this is the shutdown cancelling us, in which case every Delete
			// would fail on the same dead context and only slow the exit down. The
			// objects are not in the blocks table, so GC phase 1 removes them once
			// gcGracePeriod passes — which is what pendingMeta already relies on for
			// the blocks a failed flush had managed to upload.
			if uploadCtx.Err() != nil {
				if len(uploaded) > 0 {
					log.Printf("hamstor: shutdown cancelled the upload of inode %d, leaving %d object(s) for gc",
						inodeID, len(uploaded))
				}
			} else {
				for _, key := range uploaded {
					if delErr := hfs.Store.Delete(uploadCtx, key); delErr != nil {
						log.Printf("hamstor: cleanup after failed flush, delete %s: %v", key, delErr)
					}
				}
			}

			// Keep the data: cp has already reported success, so dropping it here
			// loses the file with nothing but a log line to show for it. The
			// retained set is re-uploaded by RecoverPending on the next start; the
			// inode stays 'pending' until then, which is what makes it recoverable.
			//
			// The whole set is retained, not just the blocks that had yet to go
			// up. The ones that did upload are not in the blocks table, so GC will
			// remove them — see pending.go. And only when this flush is the file's
			// only copy: see hasPrev above.
			retained := false
			if !hasPrev && bufSize > 0 {
				retain := make([]pendingBlock, 0, len(indexes))
				for _, idx := range indexes {
					extent := min(int64(db.BlockSize), bufSize-idx*db.BlockSize)
					if extent <= 0 {
						continue
					}
					retain = append(retain, pendingBlock{Index: idx, Size: extent})
				}
				ok, tookSnapshot := hfs.retainPendingUpload(inodeID, bufSize, retain, uploadFile, snapPath)
				retained = ok
				if tookSnapshot {
					snapPath = "" // moved into pending/, no longer ours to remove
					uploadFile.Close()
					uploadFile = nil
				}
			}
			if retained {
				// The set is the only copy now, so the inode has no durable
				// storage — which is what 'pending' means. Saying so is what keeps
				// RecoverPending from discarding the set as stale on the next
				// start, and it hides the file rather than showing a 0-byte one
				// the user might delete or overwrite before then. A no-op when the
				// inode was pending all along, which is the common case.
				if _, mErr := hfs.DB.MarkPending(inodeID); mErr != nil {
					log.Printf("hamstor: marking inode %d pending after retention: %v", inodeID, mErr)
				}
			}
			switch {
			case retained:
				log.Printf("hamstor: async upload failed for inode %d, data retained for retry on next start: %v", inodeID, uploadErr)
			case hasPrev:
				// Nothing was lost that S3 ever held: the file still reads as its
				// previous version, which is all close(2) without fsync promised.
				log.Printf("hamstor: async upload failed for inode %d (%s), previous version kept, this write lost: %v",
					inodeID, fileName, uploadErr)
			default:
				log.Printf("hamstor: async upload failed for inode %d (%s), DATA LOST: %v", inodeID, fileName, uploadErr)
			}
			att.err = uploadErr
			return
		}

		if hfs.TestCrashBeforeCommit != nil {
			hfs.TestCrashBeforeCommit()
		}

		// One transaction swaps the block set, drops the blocks past the new end
		// of file, records the size, and decrements the volume this inode used to
		// reference — needle -> blocks happens every time a small file grows past
		// MaxNeedleSize, so skipping that last part would inflate the volume by a
		// dead needle nobody ever subtracts. The keys it orphans are read INSIDE
		// that transaction, never from a snapshot taken before it: a snapshot is
		// how a losing flush deletes what a winning flush just committed.
		committed, orphaned, err := hfs.DB.CommitBlocks(inodeID, blocks, bufSize)
		if err != nil {
			log.Printf("hamstor: async commit failed: %v", err)
			for _, key := range uploaded {
				if delErr := hfs.Store.Delete(uploadCtx, key); delErr != nil {
					log.Printf("hamstor: async cleanup failed: %v", delErr)
				}
			}
			att.err = err
			return
		}
		if !committed {
			log.Printf("hamstor: inode %d deleted during upload, cleaning up %d block object(s)", inodeID, len(uploaded))
			for _, key := range uploaded {
				if delErr := hfs.Store.Delete(uploadCtx, key); delErr != nil {
					log.Printf("hamstor: orphan cleanup failed: %v", delErr)
				}
			}
			// The objects we just wrote are gone again, so record it: an fsync
			// that reported success here would be claiming durability for a
			// file whose only copy was deleted two lines up.
			att.err = fmt.Errorf("inode %d deleted during upload", inodeID)
			return
		}

		// Only now, and never before: a crash between the commit and here leaves
		// orphans for GC, while the reverse order deletes live data.
		hfs.dropObjects(uploadCtx, orphaned)

		// A staging file for an inode that now lives in blocks is stale. Left
		// behind it is not merely garbage: the builder would claim it, pack it and
		// try to commit a needle over the top of the blocks. CommitNeedlesToVolume
		// refuses that now, but it would then restore the claim and retry on every
		// notify, uploading an orphaned volume each time.
		if hfs.VolumeBuilder != nil {
			if rmErr := os.Remove(hfs.VolumeBuilder.StagePath(inodeID)); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("hamstor: remove superseded staging file for inode %d: %v", inodeID, rmErr)
			}
		}

		// The size only becomes real at CommitBlocks above, but the kernel has
		// almost certainly cached this inode's attributes already — the lookup
		// behind the caller's open/stat ran while the upload was still in flight
		// and saw size 0. Nothing refreshes that for AttrTimeout (60s by
		// default), so `ls -l` straight after `cp` reports a large file as 0
		// bytes. Drop the cached attributes so the next stat re-reads them.
		//
		// A negative offset invalidates attributes ONLY: the kernel skips the
		// page-cache range when off < 0, which is what we want — the data was
		// always correct, only the size was stale.
		if inode != nil {
			if errno := inode.NotifyContent(-1, 0); errno != 0 && errno != syscall.ENOSYS {
				log.Printf("hamstor: attr invalidate for inode %d: %v", inodeID, errno)
			}
		}

		// Keep the local copies: the bytes we just sent are still on this disk,
		// and without this the next open downloads them straight back from S3.
		for _, b := range blocks {
			hfs.cacheBlock(b.S3Key, uploadFile, b.Index*db.BlockSize, b.Size)
		}

		// Hand the snapshot to the thumbnailer, which removes it when done —
		// including when it is not an image, so this is also how a plain file's
		// snapshot gets cleaned up. Clearing snapPath transfers ownership away
		// from the defer above.
		//
		// Only a snapshot that covers the whole file is a valid thumbnail source
		// (see wholeFileSnapshot). Freshly copied images are fully dirty, which
		// is the case that matters.
		if wholeFileSnapshot(blocks, lastLive) {
			uploadFile.Close()
			uploadFile = nil
			hfs.scheduleThumb(inodeID, fileName, snapPath)
			snapPath = ""
		}

		// Periodically return freed pages to the OS to reduce RSS.
		hfs.MaybeFreeMem()
	}()

	return 0
}

// uploadSealedBlock encrypts one block out of the snapshot and PUTs it.
//
// GCM seals a whole message, so an encrypted block does go through memory — but
// one block of it, not one file. Encrypting per block is what makes every object
// independently decryptable, and since crypto.Encrypt emits
// [version][nonce][ct+tag] on every call, it is per-block nonces for free.
//
// It is a function rather than the inline branch it replaced so that the
// EncryptSem slot is released by a defer. The slot has to cover the PUT as well
// as the sealing — the sealed copy stays on the heap until the request is
// done — which means every early return between here and there would otherwise
// have to give it back by hand, and the one that forgets wedges the mount
// instead of failing.
func (hfs *HamstorFS) uploadSealedBlock(ctx context.Context, key string, src *os.File, start, extent, inodeID, idx int64) error {
	if hfs.EncryptSem != nil {
		hfs.EncryptSem <- struct{}{}
		defer func() { <-hfs.EncryptSem }()
	}

	plain := make([]byte, extent)
	if _, err := src.ReadAt(plain, start); err != nil && err != io.EOF {
		log.Printf("hamstor: snapshot read failed for inode %d block %d: %v", inodeID, idx, err)
		return fmt.Errorf("snapshot read block %d: %w", idx, err)
	}
	body, err := hfs.Encryptor.Encrypt(plain)
	plain = nil
	if err != nil {
		log.Printf("hamstor: encrypt failed for inode %d block %d: %v", inodeID, idx, err)
		return fmt.Errorf("encrypt block %d: %w", idx, err)
	}
	return hfs.Store.Upload(ctx, key, body)
}

// wholeFileSnapshot reports whether a committed block set covers the file
// entirely — blocks 0 through lastLive with no gaps — and therefore whether the
// snapshot the upload streamed from holds the file's complete plaintext.
//
// This is the thumbnail contract under lazy materialization, and it is the one
// place where getting it wrong is both silent and permanent. scheduleThumb's
// source must hold the WHOLE file: after a partial overwrite the snapshot has
// only the rewritten blocks at their natural offsets and holes everywhere else,
// so a thumbnail built from it renders those holes as black and is then stored
// in the freedesktop cache as a perfectly valid-looking preview of the file.
// Nothing breaks at build time and nothing ever corrects it.
//
// Skipping is the right answer rather than materializing the rest first: pulling
// a whole 2 GB PSD back to make a 256px preview is exactly the full download this
// layout removed. The cost of skipping is a STALE thumbnail, which the
// freedesktop mtime check makes the viewer regenerate on demand by reading the
// file through the mount, where it is complete. Stale and self-correcting beats
// wrong and permanent.
//
// A sparse file never qualifies (its holes have no rows, so the set has gaps),
// which is correct for the same reason.
func wholeFileSnapshot(blocks []db.BlockCommit, lastLive int64) bool {
	if lastLive < 0 || int64(len(blocks)) != lastLive+1 {
		return false
	}
	for i, b := range blocks {
		if b.Index != int64(i) {
			return false
		}
	}
	return true
}

func (h *HamstorHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	errno := h.Flush(ctx)
	if errno != 0 {
		return errno
	}
	// Wait on the shared attempt, not one of this handle's own: with several
	// handles open on the inode, the upload carrying our bytes may well have
	// been launched by whichever of them closed first.
	h.st.mu.Lock()
	att := h.st.cur
	h.st.mu.Unlock()
	if att != nil {
		<-att.done
		if att.err != nil {
			return syscall.EIO
		}
	}

	// Ensure S3 durability for staged files not yet packed into a volume.
	//
	// A block-stored file has no vol_s3_key either, so without the HasBlocks test
	// it lands here too — and FlushInode would go looking for a staging file that
	// never existed and report "staging file missing and no S3 reference",
	// turning fsync on a perfectly durable large file into EIO. Its durability
	// was already settled by the attempt we waited on above.
	//
	// durable answers "did this inode get real storage?" for the waiting loops
	// below, and must ask about blocks for the same reason: a concurrent
	// overwrite that grew the file past MaxNeedleSize commits blocks, not a
	// needle, so a vol_s3_key-only test would spin out every backoff and report
	// EIO for a file that is safely in S3.
	durable := func() bool {
		m, dbErr := h.hfs.DB.GetInode(h.inodeID)
		if dbErr != nil {
			return false
		}
		if m.VolS3Key != "" {
			return true
		}
		has, hErr := h.hfs.DB.HasBlocks(h.inodeID)
		return hErr == nil && has
	}

	if h.hfs.VolumeBuilder != nil {
		meta, err := h.hfs.DB.GetInode(h.inodeID)
		if err == nil && meta.VolS3Key == "" && meta.Size > 0 {
			if has, hErr := h.hfs.DB.HasBlocks(h.inodeID); hErr == nil && has {
				return 0
			}
			if err := h.hfs.VolumeBuilder.FlushInode(h.inodeID); err != nil {
				// If the builder is actively packing this file, wait for it to
				// finish, then re-drive the flush ourselves. Returning EIO just
				// because the builder happened to hold the .packing claim would
				// wrongly report a durability failure for data that is in-flight
				// and will succeed.
				if errors.Is(err, volume.ErrBeingPacked) {
					backoff := 200 * time.Millisecond
					for attempt := 0; attempt < 10; attempt++ {
						time.Sleep(backoff)
						if durable() {
							return 0 // builder finished
						}
						backoff *= 2
						if backoff > 2*time.Second {
							backoff = 2 * time.Second
						}
					}
					// Builder released the claim but the inode is still not
					// durable (it was superseded by a concurrent overwrite, or
					// the builder restored it). Re-drive the flush so we either
					// pack it now or observe genuine durability.
					if durable() {
						return 0
					}
					if err2 := h.hfs.VolumeBuilder.FlushInode(h.inodeID); err2 == nil {
						return 0
					} else if errors.Is(err2, volume.ErrBeingPacked) {
						// Still racing the builder; the data is in-flight, not lost.
						if durable() {
							return 0
						}
					}
				}
				log.Printf("hamstor: fsync flush inode %d: %v", h.inodeID, err)
				return syscall.EIO
			}
		}
	}

	return 0
}

func (h *HamstorHandle) Release(ctx context.Context) syscall.Errno {
	h.st.mu.Lock()
	h.released = true
	// Don't wait for uploadDone — the upload goroutine manages its own
	// lifecycle and logs errors. Blocking here causes goroutine pile-up
	// during bulk copy (32k blocked Release handlers = hundreds of MB stacks).
	// Graceful shutdown uses InflightUploads.Wait() to ensure completion.
	//
	// The stream ring is this handle's alone, and it holds up to
	// streamBlocksCap * 8 MiB. Drop it now rather than leave it to whenever the
	// kernel lets go of the FileHandle.
	h.streamBlocks = nil
	h.st.mu.Unlock()

	// Drop this handle's reference. The buffer and any spill file belong to the
	// shared state now, so they are freed by the last reference to go — which may
	// be this one, or an upload goroutine still in flight.
	h.hfs.releaseWrite(h.inodeID, h.st)
	return 0
}
