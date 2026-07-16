package hfuse

import (
	"fmt"
	"log"
	"os"
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
	"github.com/milan/hamstor/internal/thumb"
	"github.com/milan/hamstor/internal/volume"
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

	// VolumeBuilder packs small files into volume S3 objects.
	// nil means volume packing is disabled.
	VolumeBuilder *volume.Builder

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
// scheduleThumb generates a thumbnail for inodeID from srcPath, which must hold
// the file's plaintext bytes. Ownership of srcPath transfers to this call: it is
// removed once the thumbnail is done, or straight away if one is not wanted.
//
// The bytes are read only AFTER ThumbSem is acquired, so at most cap(ThumbSem)
// images sit on the heap at once. Handing the image itself to a queued goroutine
// instead would put one full-size buffer per pending file in RAM — with a bulk
// copy of a photo library that is thousands of buffers waiting on 4 slots, which
// is why the plaintext is passed as a path on disk rather than a []byte.
func (hfs *HamstorFS) scheduleThumb(inodeID int64, fileName, srcPath string) {
	if srcPath == "" {
		return
	}
	if !thumb.IsImageExt(fileName) || hfs.Mountpoint == "" {
		os.Remove(srcPath)
		return
	}
	// Resolve path and mtime up front: both are cheap DB reads, and doing them
	// here keeps the queued goroutine holding nothing but a few strings.
	relPath, err := hfs.DB.InodePath(inodeID)
	if err != nil {
		os.Remove(srcPath)
		return
	}
	meta, err := hfs.DB.GetInode(inodeID)
	if err != nil {
		os.Remove(srcPath)
		return
	}
	mtimeSec := meta.MtimeNs / 1e9

	go func() {
		defer os.Remove(srcPath)
		hfs.ThumbSem <- struct{}{}
		defer func() { <-hfs.ThumbSem }()

		data, err := os.ReadFile(srcPath)
		if err != nil {
			log.Printf("hamstor: thumb source read for inode %d: %v", inodeID, err)
			return
		}
		thumb.Generate(hfs.Mountpoint, relPath, mtimeSec, data)
	}()
}

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
