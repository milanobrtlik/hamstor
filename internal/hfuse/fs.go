package hfuse

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"

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

	// InflightUploads tracks async upload goroutines for graceful shutdown, and
	// InflightCount is the same population as a readable number — a WaitGroup
	// cannot be asked how many it is waiting for, and shutdown wants to say how
	// many uploads it is about to cancel.
	InflightUploads sync.WaitGroup
	InflightCount   atomic.Int64

	// UploadCtx is cancelled at shutdown to stop in-flight uploads. nil means
	// "never cancelled", which is what tests and the subcommands want.
	//
	// An upload deliberately does NOT inherit the FUSE request's context — it
	// outlives the close(2) that started it — so it needs one of its own to be
	// interruptible at all. Cancelling it is what makes shutdown bounded: waiting
	// for the uploads instead is unbounded, and at 4-6 MB/s to B2 a single large
	// file is minutes, which is how the daemon came to be SIGABRTed by systemd
	// mid-upload with nothing retained.
	//
	// Cancelling is safe precisely because it lands in the same failure path as a
	// dead endpoint: the bytes are retained under PendingDir and RecoverPending
	// finishes the upload on the next start. See uploadContext.
	UploadCtx context.Context

	// writeStates holds the per-inode write state shared by every open handle
	// (see inodeWrite). writeMu guards the map and the reference counts in it,
	// and nothing else — it is a leaf lock, never held across inodeWrite.mu, a
	// syscall, S3 or the DB.
	writeMu     sync.Mutex
	writeStates map[int64]*inodeWrite

	// UploadSem limits concurrent async S3 uploads.
	UploadSem chan struct{}

	// EncryptSem bounds how many block encryptions may be in flight at once.
	// nil means unbounded, which is right for tests and for the unencrypted
	// path — that one streams each block straight off the snapshot through
	// io.NewSectionReader and puts nothing on the heap.
	//
	// Encryption cannot: GCM seals a whole message, so an encrypted block holds
	// its plaintext AND its sealed copy, 2 * db.BlockSize, for as long as the PUT
	// takes. An UploadSem slot is held across a flush's ENTIRE block loop, so
	// with its capacity of 32 the ceiling was 512 MiB against
	// debug.SetMemoryLimit(150 << 20) — reachable by any bulk copy onto an
	// encrypted mount, and invisible when it happens: nothing errors, the GC just
	// runs continuously and every upload crawls.
	//
	// It is a second, narrower semaphore rather than a smaller UploadSem because
	// the two bound different things. UploadSem bounds requests in flight, which
	// is what keeps a bulk copy's throughput up; this bounds bytes on the heap,
	// and only the encrypted path spends any.
	EncryptSem chan struct{}

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

	// volumeFetch dedups whole-volume downloads so that when a directory of
	// files packed into the same volume is browsed cold, concurrent reads of
	// sibling inodes (each with its own inodeWrite lock) trigger a single
	// download of that volume object, not one per file. See loadFromVolume.
	volumeFetch singleflight.Group

	// TestCrashBeforeCommit, when non-nil, is called after S3 upload
	// but before SQLite commit. Tests use this to simulate a crash
	// in the critical window.
	TestCrashBeforeCommit func()

	// Write-buffer backpressure (Phase A). WriteBuffer caps the total un-uploaded
	// ("dirty") bytes buffered locally before Write blocks, so a bulk copy paces to
	// the S3 drain rate instead of letting the spill dir grow without bound. It is a
	// DISK budget, distinct from debug.SetMemoryLimit — the spill lives on disk.
	// <= 0 disables the gate entirely, which is what tests and the mutating
	// subcommands get by leaving it unset (behaviour identical to before Phase A).
	//
	// dirtyBytes is the accounted footprint: Σ over live write states of
	// inodeWrite.accountedBlocks × db.BlockSize, plus the bytes of in-flight uploads
	// whose spill has not yet been released. Charged per DIRTIED BLOCK, never by
	// spillSize — that counts sparse holes, so truncate -s 5T must cost ~0. dirtyMu
	// is a LEAF lock in the same class as writeMu: only the counter and the
	// broadcast touch it, never a st.mu / DB / S3 call held across it.
	WriteBuffer   int64
	dirtyMu       sync.Mutex
	dirtyCond     *sync.Cond
	dirtyCondOnce sync.Once
	dirtyBytes    int64

	// punchProbeOnce/punchOK cache whether the spill filesystem supports hole
	// punching, which write-time eviction (Phase B) needs to reclaim disk. Probed
	// on first use; if unsupported the mount keeps Phase A behaviour (a single file
	// needs local disk equal to its size).
	punchProbeOnce sync.Once
	punchOK        bool
}

// ensureDirtyCond lazily builds the write-buffer condition variable, so a bare
// HamstorFS literal (tests, subcommands) stays usable without an explicit init.
func (hfs *HamstorFS) ensureDirtyCond() {
	hfs.dirtyCondOnce.Do(func() { hfs.dirtyCond = sync.NewCond(&hfs.dirtyMu) })
}

// addDirtyBytes adjusts the accounted un-uploaded footprint and, when it falls,
// wakes writers blocked in admitWrite. Leaf lock: takes only dirtyMu, and
// broadcasts while holding it so a writer between its predicate check and Wait()
// cannot miss the wakeup. Safe to call while holding inodeWrite.mu (it never
// reaches back for st.mu), which is what lets per-block accounting ride inside
// markDirtyRange/dropBlocksPast.
func (hfs *HamstorFS) addDirtyBytes(delta int64) {
	if delta == 0 {
		return
	}
	hfs.ensureDirtyCond()
	hfs.dirtyMu.Lock()
	hfs.dirtyBytes += delta
	if delta < 0 {
		hfs.dirtyCond.Broadcast()
	}
	hfs.dirtyMu.Unlock()
}

// wakeWriters re-evaluates every blocked writer's admission condition. Called
// when the last in-flight upload drains, so the single-file exemption in
// admitWrite (InflightCount == 0) can release a writer that a falling dirtyBytes
// alone would not have. Broadcasts under dirtyMu — the same lock admitWrite reads
// InflightCount under — which closes the lost-wakeup window.
func (hfs *HamstorFS) wakeWriters() {
	hfs.ensureDirtyCond()
	hfs.dirtyMu.Lock()
	hfs.dirtyCond.Broadcast()
	hfs.dirtyMu.Unlock()
}

// admitWrite blocks until adding n bytes keeps the un-uploaded footprint within
// WriteBuffer, OR until nothing is draining. The second clause is the deadlock
// exemption: a single large file has no upload in flight until its own close, so
// InflightCount stays 0 while it is the sole writer and this returns at once —
// the file overflows to the disk tier up to its size (the documented Phase A
// single-file limit). With other files draining, InflightCount > 0 and this paces
// the writer to the S3 rate. Must be called holding NO lock (the wait would
// otherwise wedge the mount); Write calls it before taking st.mu.
func (hfs *HamstorFS) admitWrite(n int64) {
	if hfs.WriteBuffer <= 0 {
		return
	}
	hfs.ensureDirtyCond()
	hfs.dirtyMu.Lock()
	for hfs.dirtyBytes+n > hfs.WriteBuffer && hfs.InflightCount.Load() > 0 {
		hfs.dirtyCond.Wait()
	}
	hfs.dirtyMu.Unlock()
}

// uploadBlockBody PUTs one block read out of src[start:start+extent] under key,
// encrypting it first when a passphrase is set. Shared by the flush loop and the
// write-time eviction path so both keep the same heap profile: the unencrypted
// path streams straight off the file (zero heap, SDK no-alloc), the encrypted one
// seals a single block bounded by EncryptSem.
func (hfs *HamstorFS) uploadBlockBody(ctx context.Context, key string, src *os.File, start, extent, inodeID, idx int64) error {
	if hfs.Encryptor == nil {
		return hfs.Store.UploadReader(ctx, key, io.NewSectionReader(src, start, extent), extent)
	}
	return hfs.uploadSealedBlock(ctx, key, src, start, extent, inodeID, idx)
}

// punchHole releases the disk backing [off, off+length) of a spill file once the
// block there has been uploaded and committed, so a file larger than the local
// disk can still be copied. The logical size is kept (FALLOC_FL_KEEP_SIZE), so the
// region reads back as a hole and the offsets of later blocks do not move.
func punchHole(f *os.File, off, length int64) error {
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, length)
}

// holePunchSupported probes once whether the spill filesystem supports hole
// punching, caching the result. Eviction needs it to reclaim disk mid-copy; a
// filesystem without it (some tmpfs, network mounts) keeps Phase A behaviour.
func (hfs *HamstorFS) holePunchSupported() bool {
	hfs.punchProbeOnce.Do(func() {
		f, err := os.CreateTemp(hfs.SpillDir, "hamstor-punch-probe-*")
		if err != nil {
			return
		}
		defer os.Remove(f.Name())
		defer f.Close()
		if err := f.Truncate(int64(db.BlockSize)); err != nil {
			return
		}
		if err := punchHole(f, 0, int64(db.BlockSize)); err != nil {
			return
		}
		hfs.punchOK = true
	})
	return hfs.punchOK
}

// FreeOSMemoryInterval controls how often completed uploads trigger
// debug.FreeOSMemory() to return freed pages to the OS.
const FreeOSMemoryInterval = 50

// MaybeFreeMem increments the upload counter and periodically calls
// debug.FreeOSMemory() to reduce RSS after bulk operations.
// The retained-upload format, and retainPendingUpload with it, lives in
// pending.go — writer and reader side by side, so the two ends of it cannot
// drift apart.

// maxCacheShare caps how much of the disk cache one freshly uploaded file may
// claim: at most 1/maxCacheShare of it. A file bigger than that would evict most
// of the cache on the way in — and then, being the largest entry, buy little for
// what it displaced. With the default 10 GB cache this still admits a 4 GB video,
// which is the case that motivated caching at the write at all.
const maxCacheShare = 2

// cacheBlock seeds the disk cache with one block just committed under key, read
// straight out of the snapshot file the upload streamed from, so the next open
// reads it locally instead of downloading back bytes that were on this disk
// moments ago. It is the block-layout counterpart of volume.Builder.cacheVolume;
// without it, writing a large file and reopening it sends the data on a local
// disk -> S3 -> local disk round trip.
//
// src must hold the PLAINTEXT, never what went to S3: each block object is
// encrypted individually, while the cache is served as file contents (fetchBlock
// decrypts only what came from Store.Download). Caching the ciphertext would
// hand encrypted bytes back as the file. The section is read lazily by
// PutReader, so a whole file's worth of blocks never lands on the heap.
//
// Call only after CommitBlocks reported the inode committed. On the paths that
// bail out afterwards the objects are deleted again, and caching them would
// serve bytes nothing references. Failures are logged and ignored — this is an
// optimisation, never correctness.
func (hfs *HamstorFS) cacheBlock(key string, src *os.File, off, size int64) {
	if hfs.Cache == nil || src == nil || size <= 0 {
		return
	}
	if size*maxCacheShare > hfs.Cache.MaxBytes() {
		return
	}
	if err := hfs.Cache.PutReader(key, io.NewSectionReader(src, off, size)); err != nil {
		log.Printf("hamstor: cache put block %s: %v", key, err)
	}
}

// uploadContext is the context an async upload runs under: UploadCtx when one
// was wired up, and an uncancellable one otherwise. Keeping the nil case here
// rather than at each use site means a bare HamstorFS literal — every test, and
// the subcommands — still works without knowing about shutdown.
func (hfs *HamstorFS) uploadContext() context.Context {
	if hfs.UploadCtx != nil {
		return hfs.UploadCtx
	}
	return context.Background()
}

// applyInFlightSize overrides attr.Size with what the shared write state knows,
// when it knows something the DB does not.
//
// A file's size only becomes real at CommitBlocks, and Flush is asynchronous, so
// between close(2) and that commit stat(2) reports 0 for a file that was just
// written whole. Nothing tells that apart from an empty file — which is exactly
// how a slow upload gets reported as silent data loss. The write state has known
// the size since the first Write; this is only a matter of asking it.
//
// It takes writeMu (a leaf lock) and an atomic, and deliberately not st.mu: that
// one is held across block downloads from S3, so a stat behind it could block for
// tens of seconds. The refcount round trip is the same one Setattr does, and is
// what keeps the state from being torn down mid-read.
//
// This does NOT close the durability window — close(2) still promises nothing
// without fsync — it stops the window from looking like loss.
func (hfs *HamstorFS) applyInFlightSize(inodeID int64, attr *fuse.Attr) {
	st := hfs.tryAcquireWrite(inodeID)
	if st == nil {
		return
	}
	if s := st.visibleSize.Load(); s >= 0 {
		attr.Size = uint64(s)
	}
	hfs.releaseWrite(inodeID, st)
}

// dropObjects deletes the objects a commit orphaned and drops their cache
// entries. Call only AFTER the transaction that stopped referencing them: a
// crash in this order leaves objects for GC, while the reverse deletes live
// data. The keys come from inside that transaction, never from a snapshot taken
// before it — a snapshot is how a losing flush deletes what a winning flush just
// committed.
func (hfs *HamstorFS) dropObjects(ctx context.Context, keys []string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		if hfs.Cache != nil {
			hfs.Cache.Evict(key)
		}
		if err := hfs.Store.Delete(ctx, key); err != nil {
			log.Printf("hamstor: delete superseded object %s: %v", key, err)
		}
	}
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
