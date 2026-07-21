package hfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/ratelimit"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
	"github.com/milan/hamstor/internal/volume"
)

const readAheadChunks = 3 // prefetch 3 chunks ahead (~6 MB)

// spillThreshold is the size at which writes switch from memory to a temp file.
const spillThreshold = 64 << 20 // 64 MB

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

	// s3Key is ALWAYS EMPTY, and nothing assigns it any more: an inode names no
	// whole-file object, so there is no single key a range read could target.
	// It survives only to keep readChunked/getOrFetchChunk/prefetchChunks and
	// fetchStreamChunk compiling until they are rewritten to fetch per block,
	// which also closes their two gates: readChunked's `s3Key != ""` test never
	// passes, and Open no longer turns streaming on. Deleting the field means
	// deleting those five functions, and they are meant to be rewritten, not lost.
	//
	// It carries no invariant and nothing reads it — unlike inodeWrite.cacheFile,
	// which became equally unreachable and was removed outright because the code
	// around it kept rules for a state that could no longer occur.
	s3Key string
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

	// Chunk prefetch coordination
	prefetching    sync.Map           // int64 -> bool: chunks currently being fetched
	prefetchSem    chan struct{}      // limits concurrent prefetch goroutines
	prefetchCtx    context.Context    // shared context for all prefetch goroutines
	cancelPrefetch context.CancelFunc // cancels background prefetch goroutines

	// Streaming mode (multimedia files)
	streaming       bool
	rateLimiter     *ratelimit.Bucket
	streamChunks    []streamChunk // ring buffer of recent chunks
	streamChunksCap int
	lastStreamOff   int64 // for seek detection
}

type streamChunk struct {
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
		h.st.size = 0
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
	if meta.Size > 0 {
		blocks, bErr := h.hfs.DB.BlocksForInode(h.inodeID)
		if bErr != nil {
			return toErrno(bErr)
		}
		if len(blocks) > 0 {
			return h.loadFromBlocks(ctx, blocks, meta.Size)
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
				blocks, bErr := h.hfs.DB.BlocksForInode(h.inodeID)
				if bErr != nil {
					return toErrno(bErr)
				}
				if len(blocks) > 0 {
					return h.loadFromBlocks(ctx, blocks, meta2.Size)
				}
			}
			return syscall.EIO
		}
		h.st.buf = data
		h.st.loaded = true
		h.st.size = size
		h.fileSize = size
		return 0
	}

	// Nothing left to load from: no needle, no blocks, no staging file. That is
	// an empty file — the only remaining shape with no storage of its own, now
	// that an inode cannot name a whole-file object.
	h.st.buf = []byte{}
	h.st.loaded = true
	h.st.size = meta.Size
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
	h.st.size = meta.Size
	return 0
}

// loadFromBlocks assembles a block-stored file into the shared state's backing
// store: a heap buffer for small files, a spill file above spillThreshold so
// peak memory stays at one block regardless of file size.
//
// Reading stays deliberately whole-file in this step — every block is fetched
// even for a one-byte read. Lazy per-block faulting is a separate change, and
// keeping it separate is what makes a bug here show up as zeroes or EIO rather
// than as a silently stale version.
//
// Two different absences must not be confused. A block with no row is a HOLE:
// it reads as zeroes and is never fetched, which is what makes a write at offset
// 1 GB cost one object instead of 128 of them. A block whose stored object is
// shorter than its live extent is a truncate that shrank the file without
// rewriting the tail; the remainder is zeroes too. Only size is authoritative
// for the file's length — never the sum of the blocks' sizes.
func (h *HamstorHandle) loadFromBlocks(ctx context.Context, blocks []db.BlockCommit, size int64) syscall.Errno {
	var place func(data []byte, off int64) error
	var buf []byte
	var sf *os.File

	if size > spillThreshold {
		f, err := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
		if err != nil {
			log.Printf("hamstor: block spill create for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		// Sparse: holes cost no disk and read as zeroes, exactly as they should.
		if err := f.Truncate(size); err != nil {
			f.Close()
			os.Remove(f.Name())
			log.Printf("hamstor: block spill size for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		sf = f
		place = func(data []byte, off int64) error {
			_, werr := sf.WriteAt(data, off)
			return werr
		}
	} else {
		buf = make([]byte, size)
		place = func(data []byte, off int64) error {
			copy(buf[off:], data)
			return nil
		}
	}

	// Install the backing store only once every block is in it. Handing a
	// half-filled buffer to the shared state on an error path would publish a
	// file with zeroes where its data should be, and the next flush would commit
	// those zeroes over the real blocks.
	ok := false
	defer func() {
		if !ok && sf != nil {
			sf.Close()
			os.Remove(sf.Name())
		}
	}()

	for _, b := range blocks {
		start := b.Index * db.BlockSize
		if start >= size {
			// Past EOF: CommitBlocks drops these, so reaching one means the file
			// was shrunk by a path that only moved inodes.size. Its bytes are not
			// part of the file.
			continue
		}
		extent := min(int64(db.BlockSize), size-start)
		data, err := h.fetchBlock(ctx, b)
		if err != nil {
			log.Printf("hamstor: block read failed for inode %d block %d (%s): %v",
				h.inodeID, b.Index, b.S3Key, err)
			return toErrno(err)
		}
		if int64(len(data)) > extent {
			data = data[:extent]
		}
		if err := place(data, start); err != nil {
			log.Printf("hamstor: block place for inode %d block %d: %v", h.inodeID, b.Index, err)
			return syscall.EIO
		}
	}

	ok = true
	if sf != nil {
		h.st.spillFile = sf
		h.st.spillSize = size
	} else {
		h.st.buf = buf
	}
	h.st.loaded = true
	h.st.size = size
	h.fileSize = size
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
	if h.hfs.Cache != nil {
		if f, err := h.hfs.Cache.Open(b.S3Key); err == nil {
			data, rerr := io.ReadAll(f)
			f.Close()
			if rerr == nil {
				return data, nil
			}
		}
	}

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
	if h.hfs.Cache != nil {
		if putErr := h.hfs.Cache.Put(b.S3Key, data); putErr != nil {
			log.Printf("hamstor: cache put block %s: %v", b.S3Key, putErr)
		}
	}
	return data, nil
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

	// Streaming mode for multimedia files — rate-limited, no disk cache
	if h.streaming {
		return h.readStreaming(ctx, dest, off)
	}

	// Fast path: already loaded (full file in buf/cache/spill)
	if h.st.loaded {
		return h.readLoaded(dest, off)
	}

	// Chunk-based path: unencrypted, has S3 key, not dirty, cache available —
	// and the whole file is not already cached. Range-reading an object we hold
	// in full locally would download bytes we have, and PutChunk deletes the
	// whole-file entry to put its chunk directory at that path, so the first
	// read after a flush would throw away the copy the flush just kept.
	if h.s3Key != "" && h.hfs.Encryptor == nil && !h.st.dirty && h.hfs.Cache != nil &&
		!h.hfs.Cache.Has(h.s3Key) {
		if off >= h.fileSize {
			return fuse.ReadResultData(nil), 0
		}
		result, errno := h.readChunked(ctx, dest, off)
		if errno == 0 {
			return result, 0
		}
		log.Printf("hamstor: chunked read failed, falling back to full download")
	}

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return nil, errno
	}
	return h.readLoaded(dest, off)
}

// readChunked serves a read from chunk-based cache, fetching missing chunks from S3.
func (h *HamstorHandle) readChunked(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	length := int64(len(dest))
	if off+length > h.fileSize {
		length = h.fileSize - off
	}
	if length <= 0 {
		return fuse.ReadResultData(nil), 0
	}

	firstChunk := cache.ChunkIndex(off)
	lastChunk := cache.ChunkIndex(off + length - 1)

	// Collect data from chunks
	buf := make([]byte, 0, length)
	for ci := firstChunk; ci <= lastChunk; ci++ {
		chunkData, err := h.getOrFetchChunk(ctx, ci)
		if err != nil {
			return nil, syscall.EIO
		}

		// Calculate the slice of this chunk that overlaps [off, off+length)
		chunkStart := ci * cache.ChunkSize
		sliceStart := int64(0)
		if off > chunkStart {
			sliceStart = off - chunkStart
		}
		sliceEnd := int64(len(chunkData))
		if chunkStart+sliceEnd > off+length {
			sliceEnd = off + length - chunkStart
		}
		if sliceStart < int64(len(chunkData)) && sliceEnd > sliceStart {
			buf = append(buf, chunkData[sliceStart:sliceEnd]...)
		}
	}

	// Prefetch ahead in background
	h.prefetchChunks(lastChunk+1, readAheadChunks)

	return fuse.ReadResultData(buf), 0
}

// getOrFetchChunk returns chunk data from cache or fetches it from S3.
// Called with h.st.mu held — serializes downloads per handle.
func (h *HamstorHandle) getOrFetchChunk(ctx context.Context, index int64) ([]byte, error) {
	// Try cache first
	if data, err := h.hfs.Cache.GetChunk(h.s3Key, index); err == nil {
		return data, nil
	}

	// Fetch from S3 via range request
	chunkStart := index * cache.ChunkSize
	chunkLen := int64(cache.ChunkSize)
	if chunkStart+chunkLen > h.fileSize {
		chunkLen = h.fileSize - chunkStart
	}
	if chunkLen <= 0 {
		return nil, nil
	}

	data, err := h.hfs.Store.DownloadRange(ctx, h.s3Key, chunkStart, chunkLen)
	if err != nil {
		return nil, err
	}

	// Cache the chunk (best-effort)
	if putErr := h.hfs.Cache.PutChunk(h.s3Key, index, data); putErr != nil {
		log.Printf("hamstor: chunk cache put failed: %v", putErr)
	}

	return data, nil
}

// prefetchChunks fetches upcoming chunks in the background.
// Uses per-chunk deduplication and a semaphore to limit concurrency.
func (h *HamstorHandle) prefetchChunks(startIndex int64, count int) {
	if h.fileSize <= 0 {
		return
	}
	maxChunk := cache.ChunkIndex(h.fileSize-1) + 1
	s3Key := h.s3Key
	hfs := h.hfs
	fileSize := h.fileSize

	// Lazy init semaphore and cancel context (max 2 concurrent prefetches).
	// The context is created ONCE per handle and reused for every prefetch
	// batch, so Release()'s single cancelPrefetch() call reliably aborts every
	// in-flight prefetch goroutine the handle ever launched. (Previously a fresh
	// context+cancel was created on every read, dropping earlier CancelFuncs and
	// leaking goroutines that could no longer be cancelled.)
	if h.prefetchSem == nil {
		h.prefetchSem = make(chan struct{}, 2)
	}
	if h.cancelPrefetch == nil {
		h.prefetchCtx, h.cancelPrefetch = context.WithCancel(context.Background())
	}
	ctx := h.prefetchCtx

	for i := 0; i < count; i++ {
		ci := startIndex + int64(i)
		if ci >= maxChunk {
			break
		}
		if hfs.Cache.HasChunk(s3Key, ci) {
			continue
		}
		// Deduplicate: skip if this chunk is already being fetched
		if _, loaded := h.prefetching.LoadOrStore(ci, true); loaded {
			continue
		}
		// Try to acquire semaphore (non-blocking)
		select {
		case h.prefetchSem <- struct{}{}:
		default:
			h.prefetching.Delete(ci)
			return // all prefetch slots busy, stop
		}
		go func(idx int64) {
			defer func() { <-h.prefetchSem }()
			defer h.prefetching.Delete(idx)

			chunkStart := idx * cache.ChunkSize
			chunkLen := int64(cache.ChunkSize)
			if chunkStart+chunkLen > fileSize {
				chunkLen = fileSize - chunkStart
			}
			data, err := hfs.Store.DownloadRange(ctx, s3Key, chunkStart, chunkLen)
			if err != nil {
				return
			}
			hfs.Cache.PutChunk(s3Key, idx, data)
		}(ci)
	}
}

func (h *HamstorHandle) readLoaded(dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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

// readStreaming serves reads for multimedia files with rate limiting and in-memory LRU.
func (h *HamstorHandle) readStreaming(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= h.fileSize {
		return fuse.ReadResultData(nil), 0
	}

	length := int64(len(dest))
	if off+length > h.fileSize {
		length = h.fileSize - off
	}

	// Detect seek: non-sequential read → reset rate limiter for fast resume
	if h.rateLimiter != nil && off != h.lastStreamOff {
		h.rateLimiter.Reset()
	}

	firstChunk := cache.ChunkIndex(off)
	lastChunk := cache.ChunkIndex(off + length - 1)

	buf := make([]byte, 0, length)
	for ci := firstChunk; ci <= lastChunk; ci++ {
		chunkData := h.getStreamChunk(ci)
		if chunkData == nil {
			// Rate limit before S3 download
			if h.rateLimiter != nil {
				h.st.mu.Unlock()
				if err := h.rateLimiter.Wait(ctx, cache.ChunkSize); err != nil {
					h.st.mu.Lock()
					return nil, syscall.EINTR
				}
				h.st.mu.Lock()
				// Revalidate after re-acquiring lock
				if h.released {
					return nil, syscall.EIO
				}
			}

			// Check memory cache again (another Read might have fetched it while we waited)
			chunkData = h.getStreamChunk(ci)
			if chunkData == nil {
				var err error
				chunkData, err = h.fetchStreamChunk(ctx, ci)
				if err != nil {
					log.Printf("hamstor: stream chunk fetch failed: %v", err)
					return nil, syscall.EIO
				}
			}
		}

		// Slice the chunk to the requested range
		chunkStart := ci * cache.ChunkSize
		sliceStart := int64(0)
		if off > chunkStart {
			sliceStart = off - chunkStart
		}
		sliceEnd := int64(len(chunkData))
		if chunkStart+sliceEnd > off+length {
			sliceEnd = off + length - chunkStart
		}
		if sliceStart < int64(len(chunkData)) && sliceEnd > sliceStart {
			buf = append(buf, chunkData[sliceStart:sliceEnd]...)
		}
	}

	h.lastStreamOff = off + int64(len(buf))
	return fuse.ReadResultData(buf), 0
}

// fetchStreamChunk downloads a chunk from S3 and stores it in the memory LRU.
func (h *HamstorHandle) fetchStreamChunk(ctx context.Context, index int64) ([]byte, error) {
	chunkStart := index * cache.ChunkSize
	chunkLen := int64(cache.ChunkSize)
	if chunkStart+chunkLen > h.fileSize {
		chunkLen = h.fileSize - chunkStart
	}
	if chunkLen <= 0 {
		return nil, nil
	}

	data, err := h.hfs.Store.DownloadRange(ctx, h.s3Key, chunkStart, chunkLen)
	if err != nil {
		return nil, err
	}

	h.putStreamChunk(index, data)
	return data, nil
}

// getStreamChunk returns chunk data from the in-memory ring buffer, or nil.
func (h *HamstorHandle) getStreamChunk(index int64) []byte {
	for _, sc := range h.streamChunks {
		if sc.index == index {
			return sc.data
		}
	}
	return nil
}

// putStreamChunk stores a chunk in the in-memory ring buffer, evicting the oldest if full.
func (h *HamstorHandle) putStreamChunk(index int64, data []byte) {
	// Don't store duplicates
	for _, sc := range h.streamChunks {
		if sc.index == index {
			return
		}
	}
	if len(h.streamChunks) >= h.streamChunksCap && h.streamChunksCap > 0 {
		h.streamChunks = h.streamChunks[1:]
	}
	h.streamChunks = append(h.streamChunks, streamChunk{index: index, data: data})
}

// spillToDisk moves in-memory buf to a temp file for large file writes.
func (h *HamstorHandle) spillToDisk() error {
	if h.st.spillFile != nil {
		return nil
	}
	f, err := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
	if err != nil {
		return err
	}
	if len(h.st.buf) > 0 {
		if _, err := f.Write(h.st.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
	}
	h.st.spillFile = f
	h.st.spillSize = int64(len(h.st.buf))
	h.st.buf = nil // free memory
	return nil
}

func (h *HamstorHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
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

	// If writing to spill file
	if h.st.spillFile != nil {
		end := off + int64(len(data))
		if end > h.st.spillSize {
			if err := h.st.spillFile.Truncate(end); err != nil {
				return 0, syscall.EIO
			}
			h.st.spillSize = end
		}
		if _, err := h.st.spillFile.WriteAt(data, off); err != nil {
			return 0, syscall.EIO
		}
		h.st.dirty = true
		h.st.markDirtyRange(off, int64(len(data)))
		h.st.size = h.st.spillSize
		return uint32(len(data)), 0
	}

	// Check if we should spill to disk
	end := off + int64(len(data))
	if end > spillThreshold {
		if err := h.spillToDisk(); err != nil {
			log.Printf("hamstor: spill to disk failed: %v", err)
			return 0, syscall.EIO
		}
		if end > h.st.spillSize {
			if err := h.st.spillFile.Truncate(end); err != nil {
				return 0, syscall.EIO
			}
			h.st.spillSize = end
		}
		if _, err := h.st.spillFile.WriteAt(data, off); err != nil {
			return 0, syscall.EIO
		}
		h.st.dirty = true
		h.st.markDirtyRange(off, int64(len(data)))
		h.st.size = h.st.spillSize
		return uint32(len(data)), 0
	}

	// In-memory write
	if end > int64(len(h.st.buf)) {
		h.st.buf = append(h.st.buf, make([]byte, end-int64(len(h.st.buf)))...)
	}
	copy(h.st.buf[off:], data)
	h.st.dirty = true
	h.st.markDirtyRange(off, int64(len(data)))
	h.st.size = int64(len(h.st.buf))
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
			if _, err := h.hfs.DB.CommitInode(h.inodeID, 0); err != nil {
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

	// Small files: stage to disk, commit immediately, volume builder packs later.
	if bufSize > 0 && bufSize <= int64(volume.MaxNeedleSize) && canStage {
		errno := h.flushStaged(uploadFile, bufSize)
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

	return h.flushAsync(att, uploadFile, bufSize, dirty, hasBlocks)
}

// flushStaged writes a small file to the volume staging directory and commits it
// immediately; the volume builder packs it into a volume object later. Called
// with st.mu held, and keeps it for the whole path — there is no upload
// goroutine here, so the attempt is already complete when this returns.
func (h *HamstorHandle) flushStaged(uploadFile *os.File, bufSize int64) syscall.Errno {
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
func (h *HamstorHandle) flushAsync(att *uploadAttempt, uploadFile *os.File, bufSize int64, dirty map[int64]struct{}, hasBlocks bool) syscall.Errno {
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
		att.err = err
		h.st.poisoned = err
		close(att.done)
		return toErrno(err)
	}
	fileName := meta.Name

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
	// A brand-new file is not converting — it has no prior storage to lose — and
	// must NOT be forced to write every block, or a sparse write at offset 1 GB
	// would upload 128 blocks of zeroes instead of one block.
	converting := !hasBlocks && (meta.VolS3Key != "" || meta.Size > 0)

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

	go func() {
		defer hfs.InflightUploads.Done()
		defer hfs.releaseUpload(inodeID, st)
		// Closing att.done releases every waiter, so it must come after both the
		// commit and the last write to att.err. Defers run LIFO, so this line
		// running before the two above is what orders it correctly.
		defer close(att.done)
		defer func() { <-hfs.UploadSem }()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("hamstor: async upload panic: %v", r)
				att.err = fmt.Errorf("panic: %v", r)
			}
		}()

		uploadCtx := context.Background()

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

		// wholeFileInOneBlock is the only shape this step can still retain after a
		// failed upload: one block, at index 0, spanning the entire file, so the
		// object IS the file and the existing <id>.<size> pending format describes
		// it exactly. See the retention branch below.
		wholeFileInOneBlock := len(indexes) == 1 && indexes[0] == 0 && bufSize <= db.BlockSize

		blocks := make([]db.BlockCommit, 0, len(indexes))
		uploaded := make([]string, 0, len(indexes))

		// Every upload must succeed before the transaction opens. A committed row
		// whose object does not exist is an unreadable file with no error path —
		// the GET fails whenever someone reads it, perhaps months later.
		var uploadErr error
		var retainData []byte
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
			} else {
				// GCM needs the whole plaintext of what it seals, so an encrypted
				// block does go through memory — but one block of it, not one
				// file. Encrypting per block is what makes every object
				// independently decryptable, and since crypto.Encrypt emits
				// [version][nonce][ct+tag] on every call, it is also per-block
				// nonces for free.
				plain := make([]byte, extent)
				if _, rerr := uploadFile.ReadAt(plain, start); rerr != nil && rerr != io.EOF {
					log.Printf("hamstor: snapshot read failed for inode %d block %d: %v", inodeID, idx, rerr)
					uploadErr = fmt.Errorf("snapshot read block %d: %w", idx, rerr)
					break
				}
				body, encErr := hfs.Encryptor.Encrypt(plain)
				plain = nil
				if encErr != nil {
					log.Printf("hamstor: encrypt failed for inode %d block %d: %v", inodeID, idx, encErr)
					uploadErr = fmt.Errorf("encrypt block %d: %w", idx, encErr)
					break
				}
				if err := hfs.Store.Upload(uploadCtx, blockKey, body); err != nil {
					uploadErr = err
					if wholeFileInOneBlock {
						// Retention needs the bytes destined for the object
						// verbatim — recovery re-uploads them without the
						// passphrase — so under encryption that is the ciphertext,
						// never the plaintext snapshot.
						retainData = body
					}
					break
				}
			}
			uploaded = append(uploaded, blockKey)
			blocks = append(blocks, db.BlockCommit{Index: idx, S3Key: blockKey, Size: extent})
		}

		if uploadErr != nil {
			// Nothing is committed: a file half old and half new is exactly the
			// silent corruption this layout was chosen to avoid. Drop whatever did
			// land, so the bucket is not left with objects nothing references.
			for _, key := range uploaded {
				if delErr := hfs.Store.Delete(uploadCtx, key); delErr != nil {
					log.Printf("hamstor: cleanup after failed flush, delete %s: %v", key, delErr)
				}
			}

			// Keep the data: cp has already reported success, so dropping it here
			// loses the file with nothing but a log line to show for it. Retained
			// bytes are re-uploaded by RecoverPending on the next start; the inode
			// stays 'pending' until then, which is what makes it recoverable.
			//
			// retainPendingUpload still describes ONE object as <id>.<size>, so
			// only a single whole-file block fits it. A multi-block file retains
			// NOTHING and must say so — this is a deliberate two-step debt, not an
			// oversight, and the honest log line is the whole point of leaving it
			// visible until the retention format grows to a directory of blocks.
			retained := false
			if wholeFileInOneBlock && bufSize > 0 {
				retainSpill := ""
				if retainData == nil {
					// Nothing encrypted it, so the snapshot IS the object.
					retainSpill = snapPath
				}
				if hfs.retainPendingUpload(inodeID, bufSize, retainData, retainSpill) {
					retained = true
					if retainSpill != "" {
						snapPath = "" // renamed into pending/, no longer ours to remove
						uploadFile.Close()
						uploadFile = nil
					}
				}
			}
			if retained {
				log.Printf("hamstor: async upload failed for inode %d, data retained for retry on next start: %v", inodeID, uploadErr)
			} else {
				log.Printf("hamstor: async upload failed for inode %d, DATA LOST: %v", inodeID, uploadErr)
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
		// Only a snapshot that covers the whole file is a valid thumbnail source.
		// After a partial overwrite it holds just the rewritten blocks at their
		// natural offsets, with holes everywhere else — a picture of nothing.
		// Freshly copied images are fully dirty, which is the case that matters.
		if len(blocks) > 0 && blocks[0].Index == 0 && int64(len(blocks)) == lastLive+1 {
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
	if h.cancelPrefetch != nil {
		h.cancelPrefetch()
	}
	h.st.mu.Unlock()

	// Drop this handle's reference. The buffer and any spill file belong to the
	// shared state now, so they are freed by the last reference to go — which may
	// be this one, or an upload goroutine still in flight.
	h.hfs.releaseWrite(h.inodeID, h.st)
	return 0
}
