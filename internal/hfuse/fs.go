package hfuse

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

type HamstorFS struct {
	DB         *db.DB
	Store      *s3store.Store
	Mountpoint string
	Encryptor  *crypto.Encryptor // nil means no encryption
	Cache      *cache.DiskCache  // nil means no caching
	DefaultUid uint32
	DefaultGid uint32

	// Streaming mode config for multimedia files
	StreamRate   int // MB/s rate limit (0 = disabled)
	StreamBuffer int // MB memory buffer for recent chunks

	// SpillDir is the directory for spill temp files (large writes).
	// If empty, os.TempDir() is used.
	SpillDir string

	// InflightUploads tracks async upload goroutines for graceful shutdown.
	InflightUploads sync.WaitGroup

	// UploadSem limits concurrent async S3 uploads.
	UploadSem chan struct{}

	// ThumbSem limits concurrent thumbnail operations.
	ThumbSem chan struct{}

	// EntryTimeout controls how long the kernel caches directory entries.
	// Lower values reduce memory for large directory trees.
	EntryTimeout time.Duration
	// AttrTimeout controls how long the kernel caches inode attributes.
	AttrTimeout time.Duration

	// uploadCount tracks completed uploads for periodic FreeOSMemory calls.
	uploadCount atomic.Int64

	// TestCrashBeforeCommit, when non-nil, is called after S3 upload
	// but before SQLite commit. Tests use this to simulate a crash
	// in the critical window.
	TestCrashBeforeCommit func()
}

// FreeOSMemoryInterval controls how often completed uploads trigger
// debug.FreeOSMemory() to return freed pages to the OS.
const FreeOSMemoryInterval = 50

// MaybeFreeMem increments the upload counter and periodically calls
// debug.FreeOSMemory() to reduce RSS after bulk operations.
func (hfs *HamstorFS) MaybeFreeMem() {
	if hfs.uploadCount.Add(1)%FreeOSMemoryInterval == 0 {
		debug.FreeOSMemory()
	}
}

func Mount(mountpoint string, hfs *HamstorFS) (*fuse.Server, error) {
	root := &HamstorNode{
		hfs:     hfs,
		inodeID: 1,
	}
	entryTimeout := hfs.EntryTimeout
	attrTimeout := hfs.AttrTimeout
	if entryTimeout == 0 {
		entryTimeout = 60 * time.Second
	}
	if attrTimeout == 0 {
		attrTimeout = 60 * time.Second
	}
	opts := &fs.Options{}
	opts.EntryTimeout = &entryTimeout
	opts.AttrTimeout = &attrTimeout
	opts.MountOptions.AllowOther = true

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}
	return server, nil
}
