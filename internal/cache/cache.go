package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DiskCache stores file data on local disk keyed by S3 key.
// S3 keys change on every file modification, providing natural cache invalidation.
// All public methods are safe for concurrent use.
type DiskCache struct {
	dir      string
	maxBytes int64
	mu       sync.RWMutex
}

// New creates a DiskCache at the given directory with a size limit in bytes.
func New(dir string, maxBytes int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &DiskCache{dir: dir, maxBytes: maxBytes}, nil
}

// path returns the on-disk path for a cache entry.
func (c *DiskCache) path(s3Key string) string {
	return filepath.Join(c.dir, s3Key)
}

// Has reports whether the given S3 key is cached.
func (c *DiskCache) Has(s3Key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, err := os.Stat(c.path(s3Key))
	return err == nil
}

// Open returns a read-only file handle to the cached data.
// The caller must close the returned file. Returns os.ErrNotExist if not cached.
// The returned file descriptor remains valid even if the cache entry is evicted,
// because Linux keeps the inode alive until all fds are closed.
func (c *DiskCache) Open(s3Key string) (*os.File, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return os.Open(c.path(s3Key))
}

// Put stores data in the cache. Writes to a temp file and renames atomically.
func (c *DiskCache) Put(s3Key string, data []byte) error {
	p := c.path(s3Key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(p), ".cache-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	c.mu.Lock()
	err = os.Rename(tmpName, p)
	c.mu.Unlock()

	if err != nil {
		os.Remove(tmpName)
		return err
	}

	c.evictLRU()
	return nil
}

// PutReader stores data from a reader in the cache. Atomic write via temp file.
func (c *DiskCache) PutReader(s3Key string, r io.Reader) error {
	p := c.path(s3Key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(p), ".cache-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	c.mu.Lock()
	err = os.Rename(tmpName, p)
	c.mu.Unlock()

	if err != nil {
		os.Remove(tmpName)
		return err
	}

	c.evictLRU()
	return nil
}

// Evict removes a specific cache entry.
func (c *DiskCache) Evict(s3Key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	os.Remove(c.path(s3Key))
}

// Size returns the total size of cached data in bytes and the number of entries.
func (c *DiskCache) Size() (totalBytes int64, count int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		totalBytes += info.Size()
		count++
		return nil
	})
	return
}

// Clear removes all cached data.
func (c *DiskCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		os.RemoveAll(filepath.Join(c.dir, e.Name()))
	}
	return nil
}

// --- Chunk-based operations ---

const ChunkSize = 2 << 20 // 2 MB

// ChunkIndex returns the chunk index for a byte offset.
func ChunkIndex(offset int64) int64 {
	return offset / ChunkSize
}

// chunkPath returns the on-disk path for a specific chunk.
func (c *DiskCache) chunkPath(s3Key string, index int64) string {
	return filepath.Join(c.dir, s3Key, fmt.Sprintf("chunk-%06d", index))
}

// GetChunk returns the data for a cached chunk, or os.ErrNotExist.
func (c *DiskCache) GetChunk(s3Key string, index int64) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return os.ReadFile(c.chunkPath(s3Key, index))
}

// PutChunk stores a single chunk in the cache.
func (c *DiskCache) PutChunk(s3Key string, index int64, data []byte) error {
	p := c.chunkPath(s3Key, index)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".chunk-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	c.mu.Lock()
	err = os.Rename(tmpName, p)
	c.mu.Unlock()
	if err != nil {
		os.Remove(tmpName)
		return err
	}
	c.evictLRU()
	return nil
}

// HasChunk reports whether a specific chunk is cached.
func (c *DiskCache) HasChunk(s3Key string, index int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, err := os.Stat(c.chunkPath(s3Key, index))
	return err == nil
}

type cacheEntry struct {
	path    string
	size    int64
	modTime int64
}

// evictLRU removes oldest entries until total size is under maxBytes.
// Uses a write lock internally to avoid holding it during the walk.
func (c *DiskCache) evictLRU() {
	// Phase 1: scan without lock (filesystem stat is safe, and stale data is acceptable)
	var entries []cacheEntry
	var totalSize int64

	filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		entries = append(entries, cacheEntry{
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime().UnixNano(),
		})
		totalSize += info.Size()
		return nil
	})

	if totalSize <= c.maxBytes {
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].modTime < entries[j].modTime
	})

	// Phase 2: delete with lock, one file at a time
	for _, e := range entries {
		if totalSize <= c.maxBytes {
			break
		}
		c.mu.Lock()
		err := os.Remove(e.path)
		c.mu.Unlock()
		if err == nil {
			totalSize -= e.size
		}
	}
}
