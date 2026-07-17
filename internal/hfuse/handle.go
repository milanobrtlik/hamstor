package hfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
	mu      sync.Mutex
	buf     []byte // in-memory write buffer (small files)
	dirty   bool
	loaded  bool
	isNew   bool

	// Spill file for large writes: when total size exceeds spillThreshold,
	// data is written to disk instead of keeping it all in memory.
	spillFile *os.File
	spillSize int64

	// Cache-backed read: when set, reads use ReadAt on this file
	// instead of keeping the entire file in buf.
	cacheFile *os.File
	s3Key     string // S3 key at open time
	fileSize  int64  // from DB metadata

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

	// Async upload state
	uploadDone chan struct{}
	uploadErr  error
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

func (h *HamstorHandle) ensureLoaded(ctx context.Context) syscall.Errno {
	if h.loaded {
		return 0
	}
	if h.isNew {
		h.buf = []byte{}
		h.loaded = true
		return 0
	}

	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		return toErrno(err)
	}
	if meta.VolS3Key != "" {
		return h.loadFromVolume(ctx, meta)
	}

	// File staged locally, not yet packed into a volume
	if meta.S3Key == "" && meta.VolS3Key == "" && meta.Size > 0 && h.hfs.VolumeBuilder != nil {
		stagePath := h.hfs.VolumeBuilder.StagePath(h.inodeID)
		data, err := os.ReadFile(stagePath)
		if err != nil {
			// Builder (.packing) or Fsync (.flushing) may have claimed the file.
			// Try reading from the claimed path — the data is identical and
			// reading is safe (the claimer only reads, never modifies).
			for _, suffix := range []string{".packing", ".flushing"} {
				if d, e := os.ReadFile(stagePath + suffix); e == nil {
					data = d
					err = nil
					break
				}
			}
		}
		if err != nil {
			// Staging file may have been packed by the builder between
			// GetInode and ReadFile. Re-read metadata and retry.
			meta2, err2 := h.hfs.DB.GetInode(h.inodeID)
			if err2 != nil {
				return toErrno(err2)
			}
			if meta2.VolS3Key != "" {
				return h.loadFromVolume(ctx, meta2)
			}
			log.Printf("hamstor: staged file read failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
		if h.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
			data, err = h.hfs.Encryptor.Decrypt(data)
			if err != nil {
				log.Printf("hamstor: staged file decrypt failed for inode %d: %v", h.inodeID, err)
				return syscall.EIO
			}
		}
		h.buf = data
		h.loaded = true
		h.fileSize = meta.Size
		return 0
	}

	if meta.S3Key == "" {
		h.buf = []byte{}
		h.loaded = true
		return 0
	}

	h.s3Key = meta.S3Key
	h.fileSize = meta.Size

	// Try cache first (read-only path)
	if h.hfs.Cache != nil && !h.dirty {
		if f, err := h.hfs.Cache.Open(meta.S3Key); err == nil {
			h.cacheFile = f
			h.loaded = true
			return 0
		}
	}

	data, err := h.hfs.Store.Download(ctx, meta.S3Key)
	if err != nil {
		return toErrno(err)
	}
	if h.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
		data, err = h.hfs.Encryptor.Decrypt(data)
		if err != nil {
			log.Printf("hamstor: decrypt failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
	}

	if h.hfs.Cache != nil {
		if putErr := h.hfs.Cache.Put(meta.S3Key, data); putErr != nil {
			log.Printf("hamstor: cache put failed for %s: %v", meta.S3Key, putErr)
		} else if !h.dirty {
			if f, openErr := h.hfs.Cache.Open(meta.S3Key); openErr == nil {
				h.cacheFile = f
				h.loaded = true
				return 0
			}
		}
	}

	h.buf = data
	h.loaded = true
	return 0
}

// loadFromVolume loads a file packed in a volume S3 object into the handle.
func (h *HamstorHandle) loadFromVolume(ctx context.Context, meta *db.InodeMeta) syscall.Errno {
	cacheKey := fmt.Sprintf("vol/%s/%d/%d", meta.VolS3Key, meta.VolOffset, meta.VolSize)
	h.fileSize = meta.Size

	// Try cache first
	if h.hfs.Cache != nil && !h.dirty {
		if f, cacheErr := h.hfs.Cache.Open(cacheKey); cacheErr == nil {
			h.cacheFile = f
			h.loaded = true
			return 0
		}
	}

	data, err := h.hfs.Store.DownloadRange(ctx, meta.VolS3Key, meta.VolOffset, meta.VolSize)
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

	if h.hfs.Cache != nil {
		if putErr := h.hfs.Cache.Put(cacheKey, data); putErr != nil {
			log.Printf("hamstor: cache put failed for vol needle %s: %v", cacheKey, putErr)
		} else if !h.dirty {
			if f, openErr := h.hfs.Cache.Open(cacheKey); openErr == nil {
				h.cacheFile = f
				h.loaded = true
				return 0
			}
		}
	}

	h.buf = data
	h.loaded = true
	return 0
}

func (h *HamstorHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Streaming mode for multimedia files — rate-limited, no disk cache
	if h.streaming {
		return h.readStreaming(ctx, dest, off)
	}

	// Fast path: already loaded (full file in buf/cache/spill)
	if h.loaded {
		return h.readLoaded(dest, off)
	}

	// Chunk-based path: unencrypted, has S3 key, not dirty, cache available
	if h.s3Key != "" && h.hfs.Encryptor == nil && !h.dirty && h.hfs.Cache != nil {
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
// Called with h.mu held — serializes downloads per handle.
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
	// handles the buffer/spill length is authoritative (writes may have extended
	// past fileSize), so no clamp is applied.
	clampLen := func(n int) int {
		if h.dirty {
			return n
		}
		avail := h.fileSize - off
		if avail < 0 {
			avail = 0 // read starts past EOF — return nothing
		}
		if int64(n) > avail {
			return int(avail)
		}
		return n
	}

	// Read from spill file
	if h.spillFile != nil {
		if off >= h.spillSize {
			return fuse.ReadResultData(nil), 0
		}
		n, err := h.spillFile.ReadAt(dest, off)
		if n == 0 && err == io.EOF {
			return fuse.ReadResultData(nil), 0
		}
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(dest[:clampLen(n)]), 0
	}

	// Read from cache file
	if h.cacheFile != nil {
		n, err := h.cacheFile.ReadAt(dest, off)
		if n == 0 && err == io.EOF {
			return fuse.ReadResultData(nil), 0
		}
		if err != nil && err != io.EOF {
			log.Printf("hamstor: cache read failed for inode %d: %v", h.inodeID, err)
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(dest[:clampLen(n)]), 0
	}

	// Read from in-memory buffer
	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	if !h.dirty && h.fileSize >= 0 && end > h.fileSize {
		end = h.fileSize
	}
	if end <= off {
		return fuse.ReadResultData(nil), 0
	}
	// Return a copy, not a slice aliasing h.buf: go-fuse copies the bytes out
	// after Read releases h.mu, which could race a concurrent Write that
	// reallocates or mutates h.buf and tear the in-flight read.
	out := make([]byte, end-off)
	copy(out, h.buf[off:end])
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
				h.mu.Unlock()
				if err := h.rateLimiter.Wait(ctx, cache.ChunkSize); err != nil {
					h.mu.Lock()
					return nil, syscall.EINTR
				}
				h.mu.Lock()
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
	if h.spillFile != nil {
		return nil
	}
	f, err := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
	if err != nil {
		return err
	}
	if len(h.buf) > 0 {
		if _, err := f.Write(h.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
	}
	h.spillFile = f
	h.spillSize = int64(len(h.buf))
	h.buf = nil // free memory
	return nil
}

func (h *HamstorHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return 0, errno
	}

	// Transition from cache-file mode to buf mode on first write
	if h.cacheFile != nil {
		info, err := h.cacheFile.Stat()
		if err != nil {
			return 0, syscall.EIO
		}
		sz := info.Size()
		if sz > spillThreshold {
			// Large file: spill directly to disk
			if err := h.spillToDisk(); err != nil {
				return 0, syscall.EIO
			}
			// spillToDisk created the spill file with empty buf content
			// Now copy cache file content to spill file
			h.spillFile.Truncate(0)
			h.spillFile.Seek(0, io.SeekStart)
			h.cacheFile.Seek(0, io.SeekStart)
			if _, err := io.Copy(h.spillFile, h.cacheFile); err != nil {
				return 0, syscall.EIO
			}
			h.spillSize = sz
		} else {
			h.buf = make([]byte, sz)
			if _, err := h.cacheFile.ReadAt(h.buf, 0); err != nil && err != io.EOF {
				return 0, syscall.EIO
			}
		}
		h.cacheFile.Close()
		h.cacheFile = nil
	}

	// If writing to spill file
	if h.spillFile != nil {
		end := off + int64(len(data))
		if end > h.spillSize {
			if err := h.spillFile.Truncate(end); err != nil {
				return 0, syscall.EIO
			}
			h.spillSize = end
		}
		if _, err := h.spillFile.WriteAt(data, off); err != nil {
			return 0, syscall.EIO
		}
		h.dirty = true
		return uint32(len(data)), 0
	}

	// Check if we should spill to disk
	end := off + int64(len(data))
	if end > spillThreshold {
		if err := h.spillToDisk(); err != nil {
			log.Printf("hamstor: spill to disk failed: %v", err)
			return 0, syscall.EIO
		}
		if end > h.spillSize {
			if err := h.spillFile.Truncate(end); err != nil {
				return 0, syscall.EIO
			}
			h.spillSize = end
		}
		if _, err := h.spillFile.WriteAt(data, off); err != nil {
			return 0, syscall.EIO
		}
		h.dirty = true
		return uint32(len(data)), 0
	}

	// In-memory write
	if end > int64(len(h.buf)) {
		h.buf = append(h.buf, make([]byte, end-int64(len(h.buf)))...)
	}
	copy(h.buf[off:], data)
	h.dirty = true
	return uint32(len(data)), 0
}

func (h *HamstorHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		if h.isNew {
			if _, err := h.hfs.DB.CommitInode(h.inodeID, "", 0); err != nil {
				return toErrno(err)
			}
			h.isNew = false
		}
		return 0
	}

	// Spill data to disk BEFORE waiting on the semaphore. During bulk copy,
	// thousands of FUSE goroutines block on the semaphore — if each holds
	// file data in memory, that's thousands * avg_file_size = hundreds of MB.
	// By writing to disk first and nil'ing h.buf, blocked goroutines hold
	// only a file path, not data.
	var uploadFile *os.File
	var bufSize int64
	canVolumePack := h.hfs.VolumeBuilder != nil

	if h.spillFile != nil {
		// Already on disk — just take ownership
		uploadFile = h.spillFile
		bufSize = h.spillSize
		h.spillFile = nil
	} else if len(h.buf) > 0 {
		bufSize = int64(len(h.buf))
		if bufSize <= int64(volume.MaxNeedleSize) && canVolumePack {
			// Small file going to volume staging — keep buf, skip temp spill
		} else {
			sf, sfErr := os.CreateTemp(h.hfs.SpillDir, "hamstor-spill-*")
			if sfErr != nil {
				return syscall.EIO
			}
			if _, sfErr = sf.Write(h.buf); sfErr != nil {
				sf.Close()
				os.Remove(sf.Name())
				return syscall.EIO
			}
			uploadFile = sf
			h.buf = nil // free memory immediately — data is on disk now
		}
	} else {
		// Empty file
		bufSize = 0
	}

	h.loaded = false
	h.dirty = false
	h.isNew = false

	// Small files: stage to disk, commit immediately, volume builder packs later.
	if bufSize > 0 && bufSize <= int64(volume.MaxNeedleSize) && canVolumePack {
		// Clean up the old standalone S3 object before overwriting. The old
		// volume needle (if any) is accounted for atomically by CommitInode
		// below, which decrements the previously-referenced volume in the same
		// transaction that clears the inode's vol columns.
		stageMeta, stageMetaErr := h.hfs.DB.GetInode(h.inodeID)
		if stageMetaErr == nil {
			if stageMeta.S3Key != "" {
				if delErr := h.hfs.Store.Delete(context.Background(), stageMeta.S3Key); delErr != nil {
					log.Printf("hamstor: stage overwrite delete old key %s: %v", stageMeta.S3Key, delErr)
				}
				if h.hfs.Cache != nil {
					h.hfs.Cache.Evict(stageMeta.S3Key)
				}
			}
		}

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
		} else if h.buf != nil {
			// Data still in memory — use directly, skip temp file round-trip
			stageData = h.buf
			h.buf = nil
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
		if _, err := h.hfs.DB.CommitInode(h.inodeID, "", bufSize); err != nil {
			os.Remove(stagePath)
			return toErrno(err)
		}

		// Notify builder that new staged file is available
		h.hfs.VolumeBuilder.NotifyStaged()

		// Ownership of the thumbnail source passes to scheduleThumb.
		h.hfs.scheduleThumb(h.inodeID, stageMeta.Name, stageThumbSrc)
		stageThumbSrc = ""
		return 0
	}

	// Now wait for upload slot — this goroutine holds no file data in RAM.
	h.mu.Unlock()
	h.hfs.UploadSem <- struct{}{}
	h.mu.Lock()

	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		<-h.hfs.UploadSem
		if uploadFile != nil {
			uploadFile.Close()
			os.Remove(uploadFile.Name())
		}
		return toErrno(err)
	}
	oldKey := meta.S3Key
	fileName := meta.Name

	newKey := s3store.NewKey()

	hfs := h.hfs
	inodeID := h.inodeID

	// Serialize per-handle uploads: if a previous Flush launched an upload that
	// is still running, wait for it before reusing the shared uploadDone/uploadErr
	// fields. Otherwise two goroutines race on those fields (lost error, premature
	// channel reuse). The upload goroutine never takes h.mu, so releasing it here
	// to wait cannot deadlock.
	if prev := h.uploadDone; prev != nil {
		h.mu.Unlock()
		<-prev
		h.mu.Lock()
	}
	h.uploadDone = make(chan struct{})
	h.uploadErr = nil
	inode := h.inode
	hfs.InflightUploads.Add(1)

	go func() {
		defer hfs.InflightUploads.Done()
		defer close(h.uploadDone)
		defer func() { <-hfs.UploadSem }()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("hamstor: async upload panic: %v", r)
				h.uploadErr = fmt.Errorf("panic: %v", r)
			}
		}()

		uploadCtx := context.Background()

		// Read data from spill file. For encrypted uploads we need it in memory
		// (GCM needs full plaintext); for unencrypted we stream from disk.
		var plainBuf []byte
		var uploadData []byte

		// thumbSrc is the on-disk plaintext a thumbnail is built from. The spill
		// file already holds exactly that, so for images it is handed to the
		// thumbnailer instead of being deleted here — no second copy, and no
		// image kept on the heap while the job waits for a ThumbSem slot.
		// Ownership passes to scheduleThumb; until then this defer covers every
		// early return.
		var thumbSrc string
		defer func() {
			if thumbSrc != "" {
				os.Remove(thumbSrc)
			}
		}()
		keepThumbSrc := thumb.IsImageExt(fileName)

		if uploadFile != nil && hfs.Encryptor != nil {
			plainBuf = make([]byte, bufSize)
			if _, err := uploadFile.ReadAt(plainBuf, 0); err != nil && err != io.EOF {
				log.Printf("hamstor: spill read failed for inode %d: %v", inodeID, err)
				uploadFile.Close()
				os.Remove(uploadFile.Name())
				h.uploadErr = fmt.Errorf("spill read: %w", err)
				return
			}
			uploadFile.Close()
			if keepThumbSrc {
				thumbSrc = uploadFile.Name()
			} else {
				os.Remove(uploadFile.Name())
			}
			uploadFile = nil

			encrypted, encErr := hfs.Encryptor.Encrypt(plainBuf)
			if encErr != nil {
				log.Printf("hamstor: encrypt failed for inode %d: %v", inodeID, encErr)
				h.uploadErr = fmt.Errorf("encrypt: %w", encErr)
				return
			}
			uploadData = encrypted
			// Release the plaintext buffer now that the ciphertext exists, so
			// peak heap during an encrypted upload is ~one full-size buffer
			// instead of two (plaintext + ciphertext held simultaneously). It is
			// kept only when an update is about to be re-cached below; thumbnails
			// no longer need it, they read thumbSrc from disk.
			if hfs.Cache == nil || oldKey == "" {
				plainBuf = nil
			}
		}

		var uploadErr error
		// spillPath outlives uploadFile so a failed upload can retain the bytes;
		// on success it is removed (or handed to the thumbnailer) as before.
		spillPath := ""
		if uploadFile != nil {
			// Stream from spill file on disk — no file data on Go heap
			uploadFile.Seek(0, io.SeekStart)
			uploadErr = hfs.Store.UploadReader(uploadCtx, newKey, uploadFile, bufSize)
			spillPath = uploadFile.Name()
			uploadFile.Close()
			uploadFile = nil
			if uploadErr == nil {
				if keepThumbSrc {
					thumbSrc = spillPath
				} else {
					os.Remove(spillPath)
				}
				spillPath = ""
			}
		} else if uploadData != nil {
			uploadErr = hfs.Store.Upload(uploadCtx, newKey, uploadData)
		} else {
			// Empty file
			uploadErr = hfs.Store.Upload(uploadCtx, newKey, nil)
		}
		if uploadErr != nil {
			// Keep the data: cp has already reported success, so dropping it here
			// loses the file with nothing but a log line to show for it. Retained
			// bytes are re-uploaded by RecoverPending on the next start; the inode
			// stays 'pending' until then, which is what makes it recoverable.
			if bufSize > 0 && hfs.retainPendingUpload(inodeID, bufSize, uploadData, spillPath) {
				log.Printf("hamstor: async upload failed for inode %d, data retained for retry on next start: %v", inodeID, uploadErr)
			} else {
				log.Printf("hamstor: async upload failed for inode %d, DATA LOST: %v", inodeID, uploadErr)
				if spillPath != "" {
					os.Remove(spillPath)
				}
			}
			uploadData = nil
			h.uploadErr = uploadErr
			plainBuf = nil
			return
		}
		uploadData = nil

		if hfs.TestCrashBeforeCommit != nil {
			hfs.TestCrashBeforeCommit()
		}

		// CommitInode atomically decrements the volume the inode currently
		// references (re-read inside its transaction) and clears the vol columns,
		// so the old needle's accounting and the inode's new pointer commit or
		// roll back together — no crash window can leave a referenced volume at
		// live_count=0 for GC to delete.
		committed, err := hfs.DB.CommitInode(inodeID, newKey, bufSize)
		if err != nil {
			log.Printf("hamstor: async commit failed: %v", err)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: async cleanup failed: %v", delErr)
			}
			h.uploadErr = err
			plainBuf = nil
			return
		}
		if !committed {
			log.Printf("hamstor: inode %d deleted during upload, cleaning up S3 key %s", inodeID, newKey)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: orphan cleanup failed: %v", delErr)
			}
			plainBuf = nil
			return
		}

		if oldKey != "" && oldKey != newKey {
			if err := hfs.Store.Delete(uploadCtx, oldKey); err != nil {
				log.Printf("hamstor: flush delete old key %s: %v", oldKey, err)
			}
		}

		// The size only becomes real at CommitInode above, but the kernel has
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

		// Only cache file updates (oldKey != ""). New files are cached lazily
		// on first read, avoiding disk I/O and eviction scans during bulk copy.
		// plainBuf may be nil when streaming from spill file.
		if hfs.Cache != nil && oldKey != "" && plainBuf != nil {
			hfs.Cache.Evict(oldKey)
			if putErr := hfs.Cache.Put(newKey, plainBuf); putErr != nil {
				log.Printf("hamstor: cache put after flush: %v", putErr)
			}
		}

		plainBuf = nil // free after cache put; thumbnails read thumbSrc from disk

		// Hand the plaintext spill file to the thumbnailer, which removes it when
		// done. Clearing thumbSrc transfers ownership away from the defer above.
		hfs.scheduleThumb(inodeID, fileName, thumbSrc)
		thumbSrc = ""

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
	h.mu.Lock()
	done := h.uploadDone
	h.mu.Unlock()
	if done != nil {
		<-done
		if h.uploadErr != nil {
			return syscall.EIO
		}
	}

	// Ensure S3 durability for staged files not yet packed into a volume
	if h.hfs.VolumeBuilder != nil {
		meta, err := h.hfs.DB.GetInode(h.inodeID)
		if err == nil && meta.S3Key == "" && meta.VolS3Key == "" && meta.Size > 0 {
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
						m, dbErr := h.hfs.DB.GetInode(h.inodeID)
						if dbErr == nil && (m.VolS3Key != "" || m.S3Key != "") {
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
					if m, dbErr := h.hfs.DB.GetInode(h.inodeID); dbErr == nil && (m.VolS3Key != "" || m.S3Key != "") {
						return 0
					}
					if err2 := h.hfs.VolumeBuilder.FlushInode(h.inodeID); err2 == nil {
						return 0
					} else if errors.Is(err2, volume.ErrBeingPacked) {
						// Still racing the builder; the data is in-flight, not lost.
						if m, dbErr := h.hfs.DB.GetInode(h.inodeID); dbErr == nil && (m.VolS3Key != "" || m.S3Key != "") {
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
	h.mu.Lock()
	h.released = true
	// Don't wait for uploadDone — the upload goroutine manages its own
	// lifecycle and logs errors. Blocking here causes goroutine pile-up
	// during bulk copy (32k blocked Release handlers = hundreds of MB stacks).
	// Graceful shutdown uses InflightUploads.Wait() to ensure completion.
	if h.cancelPrefetch != nil {
		h.cancelPrefetch()
	}
	h.buf = nil
	if h.cacheFile != nil {
		h.cacheFile.Close()
		h.cacheFile = nil
	}
	// spillFile ownership is transferred to the upload goroutine in Flush.
	// Only clean up here if Flush wasn't called (file opened but never written).
	if h.spillFile != nil {
		name := h.spillFile.Name()
		h.spillFile.Close()
		os.Remove(name)
		h.spillFile = nil
	}
	h.mu.Unlock()
	return 0
}
