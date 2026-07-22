package cache

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DiskCache stores file data on local disk keyed by S3 key.
// S3 keys change on every file modification, providing natural cache invalidation.
// All public methods are safe for concurrent use.
type DiskCache struct {
	dir      string
	maxBytes int64
	mu       sync.RWMutex

	// approxSize tracks approximate total cache size to avoid full directory
	// scans on every Put. Recalibrated when evictLRU actually runs.
	approxSize atomic.Int64

	// evicting guards against many concurrent Put callers each launching a
	// full directory rescan when the size crosses maxBytes.
	evicting atomic.Bool
}

// evictLowWaterRatio is the fraction of maxBytes that evictLRU evicts down to.
// Leaving headroom below maxBytes prevents the expensive full rescan from
// re-triggering on every subsequent Put once the cache sits near its limit.
const evictLowWaterRatio = 90

// New creates a DiskCache at the given directory with a size limit in bytes.
func New(dir string, maxBytes int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &DiskCache{dir: dir, maxBytes: maxBytes}
	c.initApproxSize()
	return c, nil
}

// initApproxSize scans the cache directory to initialize the approximate size
// counter, and sweeps out the chunk directories left by versions that stored
// pieces of an object instead of the object.
//
// The sweep rides along on a walk that has to happen anyway, and it matters
// because a cache directory outlives the binary: --cache-dir is deliberately
// kept across reinstalls, so those directories sit there referenced by nothing
// and counted against --cache-size. Nothing would ever look one up again — every
// key a live row names is a fresh UUID — so they are pure dead weight, and
// evictLRU only reclaims them once the cache is full enough to evict at all.
//
// The signal is exact rather than structural: a cached key is always
// {2hex}/{uuid} or volobj/{2hex}/{uuid}, so a FILE named chunk-* can only be a
// chunk, and its parent can only be a chunk directory. Testing the shape instead
// ("a directory two levels down") would match volobj/{2hex} and delete the
// volume cache.
func (c *DiskCache) initApproxSize() {
	var total int64
	stale := make(map[string]int64)
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
		if strings.HasPrefix(d.Name(), "chunk-") {
			stale[filepath.Dir(path)] += info.Size()
			return nil
		}
		total += info.Size()
		return nil
	})
	c.approxSize.Store(total)

	var freed int64
	var removed int
	for dir, size := range stale {
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("hamstor: remove stale chunk directory %s: %v", dir, err)
			continue
		}
		freed += size
		removed++
	}
	if removed > 0 {
		log.Printf("hamstor: cache: removed %d stale chunk directories from an older version, %d bytes reclaimed",
			removed, freed)
	}
}

// path returns the on-disk path for a cache entry.
// Validates the key does not escape the cache directory.
func (c *DiskCache) path(s3Key string) string {
	p := filepath.Join(c.dir, s3Key)
	if !strings.HasPrefix(p, c.dir+string(filepath.Separator)) && p != c.dir {
		// Prevent path traversal (e.g., key containing "../")
		safe := strings.ReplaceAll(s3Key, "..", "_")
		safe = strings.ReplaceAll(safe, string(filepath.Separator), "_")
		return filepath.Join(c.dir, safe)
	}
	return p
}

// MaxBytes returns the cache's size limit.
func (c *DiskCache) MaxBytes() int64 { return c.maxBytes }

// Has reports whether the given S3 key is cached as a whole file.
//
// A directory at that path does not count: it is not the object, and every
// caller of Has and Open wants the object itself. See Open for what happens
// without the check.
func (c *DiskCache) Has(s3Key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, err := os.Stat(c.path(s3Key))
	return err == nil && !info.IsDir()
}

// Open returns a read-only file handle to the cached data.
// The caller must close the returned file. Returns os.ErrNotExist if not cached.
// The returned file descriptor remains valid even if the cache entry is evicted,
// because Linux keeps the inode alive until all fds are closed.
//
// A directory at the key's path is reported as absent. os.Open succeeds on a
// directory: the callers then allocate its stat size, get EISDIR from ReadAt,
// and — in HamstorNode.Open's write preload, which discards that error — take a
// few KB of zeros for the file's contents.
//
// The check outlived the chunk sub-cache that motivated it, deliberately. New's
// sweep clears the chunk directories an older version left in a --cache-dir kept
// across reinstalls, but a directory that turns up at a key's path some other
// way must not become file contents either, and the test costs an IsDir() on a
// Stat that happens anyway.
func (c *DiskCache) Open(s3Key string) (*os.File, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p := c.path(s3Key)
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, os.ErrNotExist
	}
	// Touch mtime so LRU eviction reflects actual access time, not write time.
	now := time.Now()
	os.Chtimes(p, now, now)
	return os.Open(p)
}

// Put stores data in the cache. Writes to a temp file and renames atomically.
func (c *DiskCache) Put(s3Key string, data []byte) error {
	p := c.path(s3Key)
	// A directory may sit at this path — a chunk directory from an older version
	// that New's sweep has not run over, for instance. Put stores the object as
	// one file, so clear whatever is there first.
	os.RemoveAll(p)
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

	c.approxSize.Add(int64(len(data)))
	if c.approxSize.Load() > c.maxBytes {
		c.evictLRU()
	}
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

	// Stat the temp file to get written size before renaming
	var written int64
	if info, statErr := os.Stat(tmpName); statErr == nil {
		written = info.Size()
	}

	c.mu.Lock()
	err = os.Rename(tmpName, p)
	c.mu.Unlock()

	if err != nil {
		os.Remove(tmpName)
		return err
	}

	c.approxSize.Add(written)
	if c.approxSize.Load() > c.maxBytes {
		c.evictLRU()
	}
	return nil
}

// Evict removes a specific cache entry.
func (c *DiskCache) Evict(s3Key string) {
	p := c.path(s3Key)
	var size int64
	if info, err := os.Stat(p); err == nil {
		if info.IsDir() {
			filepath.WalkDir(p, func(_ string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				if fi, e := d.Info(); e == nil {
					size += fi.Size()
				}
				return nil
			})
		} else {
			size = info.Size()
		}
	}
	c.mu.Lock()
	os.RemoveAll(p)
	c.mu.Unlock()
	if size > 0 {
		c.approxSize.Add(-size)
	}
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
//
// Every removal failure is reported. Discarding them made `hamstor cache clear`
// print "cache cleared" over a cache it had not touched, and that is the normal
// case rather than an exotic one: the daemon runs from systemd as root and
// creates --cache-dir as root, while the CLI is run by a user, so RemoveAll
// fails with EACCES on every entry. A cache that silently refuses to clear is
// worse than one that cannot, because the next cold-read measurement is taken
// against data that was supposed to be gone.
//
// approxSize is only zeroed when everything really went, for the same reason:
// claiming an empty cache while entries survive makes eviction believe it has
// the whole budget free.
func (c *DiskCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if rmErr := os.RemoveAll(filepath.Join(c.dir, e.Name())); rmErr != nil {
			errs = append(errs, rmErr)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	c.approxSize.Store(0)
	return nil
}

type cacheEntry struct {
	path    string
	size    int64
	modTime int64
}

// evictLRU removes oldest entries until total size is under a low-water mark
// below maxBytes. Entries are collected at the S3 key level (prefix/uuid) so
// that a directory found there is evicted as one unit. A single eviction runs at
// a time: concurrent Put callers that also cross the threshold skip launching a
// redundant full-tree rescan.
func (c *DiskCache) evictLRU() {
	if !c.evicting.CompareAndSwap(false, true) {
		return // another eviction is already scanning/deleting
	}
	defer c.evicting.Store(false)

	// Phase 1: scan at S3 key level (2 levels deep: prefix/uuid)
	var entries []cacheEntry
	var totalSize int64

	prefixes, _ := os.ReadDir(c.dir)
	for _, prefix := range prefixes {
		if !prefix.IsDir() {
			continue
		}
		prefixPath := filepath.Join(c.dir, prefix.Name())
		keys, _ := os.ReadDir(prefixPath)
		for _, key := range keys {
			if strings.HasPrefix(key.Name(), ".") {
				continue
			}
			keyPath := filepath.Join(prefixPath, key.Name())
			var size int64
			var modTime int64
			if key.IsDir() {
				// A directory at key level: sum what is under it and treat the
				// subtree as one entry.
				filepath.WalkDir(keyPath, func(path string, d os.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return nil
					}
					info, err := d.Info()
					if err != nil {
						return nil
					}
					size += info.Size()
					if t := info.ModTime().UnixNano(); t > modTime {
						modTime = t
					}
					return nil
				})
			} else {
				info, err := key.Info()
				if err != nil {
					continue
				}
				size = info.Size()
				modTime = info.ModTime().UnixNano()
			}
			entries = append(entries, cacheEntry{
				path:    keyPath,
				size:    size,
				modTime: modTime,
			})
			totalSize += size
		}
	}

	// Recalibrate approxSize from actual disk scan
	c.approxSize.Store(totalSize)

	if totalSize <= c.maxBytes {
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].modTime < entries[j].modTime
	})

	// Evict down to a low-water mark below maxBytes so the next batch of Puts
	// does not immediately re-cross the threshold and re-trigger a full rescan.
	target := c.maxBytes / 100 * evictLowWaterRatio

	// Phase 2: delete with lock, one entry at a time
	for _, e := range entries {
		if totalSize <= target {
			break
		}
		c.mu.Lock()
		err := os.RemoveAll(e.path)
		c.mu.Unlock()
		if err == nil {
			totalSize -= e.size
		}
	}
	c.approxSize.Store(totalSize)
}
