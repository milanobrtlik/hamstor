package hfuse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/ratelimit"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
)

const readAheadChunks = 3 // prefetch 3 chunks ahead (~6 MB)

// spillThreshold is the size at which writes switch from memory to a temp file.
const spillThreshold = 64 << 20 // 64 MB

type HamstorHandle struct {
	hfs     *HamstorFS
	inodeID int64
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
	prefetching sync.Map     // int64 -> bool: chunks currently being fetched
	prefetchSem chan struct{} // limits concurrent prefetch goroutines

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

	// Lazy init semaphore (max 2 concurrent prefetches)
	if h.prefetchSem == nil {
		h.prefetchSem = make(chan struct{}, 2)
	}

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
			data, err := hfs.Store.DownloadRange(context.Background(), s3Key, chunkStart, chunkLen)
			if err != nil {
				return
			}
			hfs.Cache.PutChunk(s3Key, idx, data)
		}(ci)
	}
}

func (h *HamstorHandle) readLoaded(dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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
		return fuse.ReadResultData(dest[:n]), 0
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
		return fuse.ReadResultData(dest[:n]), 0
	}

	// Read from in-memory buffer
	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return fuse.ReadResultData(h.buf[off:end]), 0
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
	f, err := os.CreateTemp("", "hamstor-spill-*")
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
			h.spillFile.Truncate(end)
			h.spillSize = end
		}
		h.spillFile.WriteAt(data, off)
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

	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		return toErrno(err)
	}
	oldKey := meta.S3Key
	fileName := meta.Name

	newKey := s3store.NewKey()

	// Read the plaintext data for upload
	var plainBuf []byte
	var bufSize int64
	if h.spillFile != nil {
		bufSize = h.spillSize
		plainBuf = make([]byte, bufSize)
		if _, err := h.spillFile.ReadAt(plainBuf, 0); err != nil && err != io.EOF {
			return syscall.EIO
		}
	} else {
		plainBuf = make([]byte, len(h.buf))
		copy(plainBuf, h.buf)
		bufSize = int64(len(h.buf))
	}

	var uploadData []byte
	if h.hfs.Encryptor != nil {
		encrypted, encErr := h.hfs.Encryptor.Encrypt(plainBuf)
		if encErr != nil {
			log.Printf("hamstor: encrypt failed for inode %d: %v", h.inodeID, encErr)
			return syscall.EIO
		}
		uploadData = encrypted
	} else {
		uploadData = plainBuf
	}

	hfs := h.hfs
	inodeID := h.inodeID

	h.uploadDone = make(chan struct{})

	go func() {
		defer close(h.uploadDone)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("hamstor: async upload panic: %v", r)
				h.uploadErr = fmt.Errorf("panic: %v", r)
			}
		}()

		uploadCtx := context.Background()

		// Use streaming upload for large files
		var uploadErr error
		if len(uploadData) > spillThreshold {
			uploadErr = hfs.Store.UploadReader(uploadCtx, newKey, bytes.NewReader(uploadData), int64(len(uploadData)))
		} else {
			uploadErr = hfs.Store.Upload(uploadCtx, newKey, uploadData)
		}
		if uploadErr != nil {
			log.Printf("hamstor: async upload failed: %v", uploadErr)
			h.uploadErr = uploadErr
			return
		}

		if hfs.TestCrashBeforeCommit != nil {
			hfs.TestCrashBeforeCommit()
		}

		committed, err := hfs.DB.CommitInode(inodeID, newKey, bufSize)
		if err != nil {
			log.Printf("hamstor: async commit failed: %v", err)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: async cleanup failed: %v", delErr)
			}
			h.uploadErr = err
			return
		}
		if !committed {
			log.Printf("hamstor: inode %d deleted during upload, cleaning up S3 key %s", inodeID, newKey)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: orphan cleanup failed: %v", delErr)
			}
			return
		}

		if oldKey != "" && oldKey != newKey {
			if err := hfs.Store.Delete(uploadCtx, oldKey); err != nil {
				log.Printf("hamstor: flush delete old key %s: %v", oldKey, err)
			}
		}

		if hfs.Cache != nil {
			if oldKey != "" {
				hfs.Cache.Evict(oldKey)
			}
			if putErr := hfs.Cache.Put(newKey, plainBuf); putErr != nil {
				log.Printf("hamstor: cache put after flush: %v", putErr)
			}
		}

		if thumb.IsImageExt(fileName) {
			if relPath, pathErr := hfs.DB.InodePath(inodeID); pathErr == nil {
				updated, err2 := hfs.DB.GetInode(inodeID)
				if err2 == nil {
					go thumb.Generate(hfs.Mountpoint, relPath, updated.MtimeNs/1e9, plainBuf)
				}
			}
		}
	}()

	h.dirty = false
	h.isNew = false
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
	return 0
}

func (h *HamstorHandle) Release(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	h.released = true
	done := h.uploadDone
	h.mu.Unlock()

	if done != nil {
		<-done
		if h.uploadErr != nil {
			log.Printf("hamstor: release: async upload failed for inode %d: %v", h.inodeID, h.uploadErr)
		}
	}

	h.mu.Lock()
	h.buf = nil
	if h.cacheFile != nil {
		h.cacheFile.Close()
		h.cacheFile = nil
	}
	if h.spillFile != nil {
		name := h.spillFile.Name()
		h.spillFile.Close()
		os.Remove(name)
		h.spillFile = nil
	}
	h.mu.Unlock()
	return 0
}
