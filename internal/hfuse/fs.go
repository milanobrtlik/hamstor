package hfuse

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	// PendingDir holds the bytes of uploads that failed, so a later boot can
	// finish them (see retainPendingUpload / RecoverPending). Unlike SpillDir it
	// must NOT be wiped at startup — it is the only copy of that data left.
	// If empty, failed uploads are lost as before.
	PendingDir string

	// InflightUploads tracks async upload goroutines for graceful shutdown.
	InflightUploads sync.WaitGroup

	// UploadSem limits concurrent async S3 uploads.
	UploadSem chan struct{}

	// ThumbSem limits concurrent thumbnail operations.
	ThumbSem chan struct{}

	// thumbQueue feeds a fixed worker pool with pending thumbnail jobs; see
	// scheduleThumb. Started lazily so a bare HamstorFS literal stays usable.
	thumbQueue   chan thumbJob
	thumbStart   sync.Once
	thumbDropped atomic.Int64

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
// retainPendingUpload saves the exact bytes an upload was about to send to S3,
// so a later boot can finish it instead of the data being dropped. Without this
// a single transient S3 error during a copy loses the file outright: `cp` has
// already returned success, the inode stays 'pending', and the next startup's
// Cleanup deletes it — the only trace being one line in the daemon log.
//
// The retained bytes are whatever was destined for the object verbatim, i.e.
// ciphertext under encryption, so recovery can upload them without re-encrypting
// (and without needing the passphrase to match this boot's).
//
// Exactly one of data / spillPath is used; spillPath is renamed in, since it
// already holds the bytes and lives on the same filesystem. The logical size
// rides in the filename because a pending inode's size column is still 0 and,
// under encryption, the object is larger than the file it represents.
// Reports whether the data was safely retained.
func (hfs *HamstorFS) retainPendingUpload(inodeID, logicalSize int64, data []byte, spillPath string) bool {
	if hfs.PendingDir == "" {
		return false
	}
	dst := filepath.Join(hfs.PendingDir, fmt.Sprintf("%d.%d", inodeID, logicalSize))

	if spillPath != "" {
		if err := os.Rename(spillPath, dst); err != nil {
			log.Printf("hamstor: retain failed upload for inode %d: %v", inodeID, err)
			return false
		}
		return true
	}

	// tmp + rename so recovery never sees a half-written file.
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		os.Remove(tmp)
		log.Printf("hamstor: retain failed upload for inode %d: %v", inodeID, err)
		return false
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		log.Printf("hamstor: retain failed upload for inode %d: %v", inodeID, err)
		return false
	}
	return true
}

// thumbJob is a pending thumbnail, referenced by path so a queued job costs a
// few strings rather than a full image buffer.
type thumbJob struct {
	inodeID  int64
	relPath  string
	mtimeSec int64
	srcPath  string
}

const (
	// thumbQueueDepth bounds pending thumbnail jobs. Each job pins one plaintext
	// temp file, so the depth caps retained disk, not just memory: at most
	// depth * MaxNeedleSize (~256 MB) of temp files outlive their upload. Jobs
	// beyond it are dropped — thumbnails are best-effort and the desktop
	// regenerates missing ones on demand per the freedesktop spec — but the depth
	// is set well above a realistic burst so that stays the exception. Measured:
	// a 600-image copy backs up to ~420 pending, and unbounded it would grow with
	// the copy (a 32k photo import pinned tens of thousands of temp files).
	thumbQueueDepth = 1024

	// thumbWorkers is the fallback pool size when ThumbSem is unset.
	thumbWorkers = 4
)

// scheduleThumb queues a thumbnail for inodeID built from srcPath, which must
// hold the file's plaintext bytes. Ownership of srcPath transfers to this call:
// it is removed once the thumbnail is done, dropped, or unwanted.
//
// A worker reads the bytes only AFTER taking a slot, so at most pool-size images
// are on the heap at once. Handing the image itself to a queued goroutine — as
// this did before — put one full-size buffer per pending file in RAM: a bulk
// copy meant thousands of buffers waiting on 4 slots.
func (hfs *HamstorFS) scheduleThumb(inodeID int64, fileName, srcPath string) {
	if srcPath == "" {
		return
	}
	if !thumb.IsImageExt(fileName) || hfs.Mountpoint == "" {
		os.Remove(srcPath)
		return
	}
	// Resolve path and mtime up front: both are cheap DB reads, and doing them
	// here keeps a queued job holding nothing but a few strings.
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

	hfs.thumbStart.Do(hfs.startThumbWorkers)

	select {
	case hfs.thumbQueue <- thumbJob{inodeID: inodeID, relPath: relPath, mtimeSec: meta.MtimeNs / 1e9, srcPath: srcPath}:
	default:
		// Queue full: shed this one rather than pin its temp file indefinitely.
		os.Remove(srcPath)
		if n := hfs.thumbDropped.Add(1); n == 1 || n%thumbQueueDepth == 0 {
			log.Printf("hamstor: thumbnail queue full, skipped %d image(s) so far (they regenerate on demand)", n)
		}
	}
}

func (hfs *HamstorFS) startThumbWorkers() {
	hfs.thumbQueue = make(chan thumbJob, thumbQueueDepth)
	n := cap(hfs.ThumbSem)
	if n == 0 {
		n = thumbWorkers
	}
	for range n {
		go hfs.thumbWorker()
	}
}

func (hfs *HamstorFS) thumbWorker() {
	for job := range hfs.thumbQueue {
		func() {
			defer os.Remove(job.srcPath)
			if hfs.ThumbSem != nil {
				hfs.ThumbSem <- struct{}{}
				defer func() { <-hfs.ThumbSem }()
			}
			data, err := os.ReadFile(job.srcPath)
			if err != nil {
				log.Printf("hamstor: thumb source read for inode %d: %v", job.inodeID, err)
				return
			}
			thumb.Generate(hfs.Mountpoint, job.relPath, job.mtimeSec, data)
		}()
	}
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
