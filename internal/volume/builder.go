package volume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// ErrBeingPacked is returned by FlushInode when the staging file is currently
// being packed by the builder goroutine.
var ErrBeingPacked = errors.New("file is being packed by builder")

const (
	// MaxNeedleSize is the maximum file size that gets packed into a volume.
	// Files larger than this are uploaded as standalone S3 objects.
	MaxNeedleSize = 256 << 10 // 256 KB

	// TargetVolumeSize is the buffer threshold that triggers a volume upload.
	TargetVolumeSize = 8 << 20 // 8 MB

	// fallbackInterval is how often the builder checks for staged files
	// even without an explicit notification.
	fallbackInterval = 5 * time.Second

	// maxBatchEntries limits how many staged files are read into memory per
	// scan cycle. At 256 KB max each, 64 entries = ~16 MB worst case.
	// Remaining files are picked up on the next tick/notify cycle.
	maxBatchEntries = 64
)

// closeTimeout is the maximum time Close() will wait for the final
// scanAndSeal before cancelling in-flight S3 operations.
const closeTimeout = 30 * time.Second

// Builder scans a staging directory for small files and packs them into
// volume S3 objects. Files are written to staging by Flush() and committed
// immediately so they are visible right away. The builder runs in the
// background, collecting staged files and uploading them as volumes.
type Builder struct {
	db    *db.DB
	store *s3store.Store

	stagingDir string
	notify     chan struct{}
	done       chan struct{}
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewBuilder creates a volume builder that scans stagingDir for files to pack.
func NewBuilder(database *db.DB, store *s3store.Store, stagingDir string) *Builder {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Builder{
		db:         database,
		store:      store,
		stagingDir: stagingDir,
		notify:     make(chan struct{}, 1),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// StagePath returns the staging file path for the given inode.
func (b *Builder) StagePath(inodeID int64) string {
	return filepath.Join(b.stagingDir, strconv.FormatInt(inodeID, 10))
}

// restoreClaim returns a claimed staging file (claimPath, e.g. "<id>.packing"
// or "<id>.flushing") to its original path, UNLESS a newer staging file already
// exists there. A concurrent overwrite Flush writes a fresh staging file at
// origPath (tmp+rename) and bumps the inode's mtime; renaming the stale claim
// back over it would clobber the newer data while the DB advertises the new
// mtime/size — a silent lost write. In that case the stale claim is dropped.
//
// The check and the rename MUST be one operation. A stat followed by a rename
// leaves a window for the overwrite to land in between, and losing that race
// costs real data: the stale claim replaces the newer staging file, the DB still
// reports the newer size, and the next open reads a short buffer and appends
// over the top of it. That is one silently lost write per race, and it is easy
// to hit — a batch below TargetVolumeSize claims and restores every staged file
// on every notify, so an append-per-line workload runs this constantly. The
// mtime guard in CommitNeedlesToVolume is not a backstop for it: that guard
// protects the volume commit, not the staging file.
//
// RENAME_NOREPLACE gives exactly the needed semantics atomically. If the kernel
// or filesystem lacks it, fall back to stat+rename and accept the window rather
// than strand the claim.
func restoreClaim(claimPath, origPath string) {
	err := unix.Renameat2(unix.AT_FDCWD, claimPath, unix.AT_FDCWD, origPath, unix.RENAME_NOREPLACE)
	switch err {
	case nil:
		return
	case unix.EEXIST, unix.ENOTEMPTY:
		// A newer staging file is already there; our claim is stale.
		if rmErr := os.Remove(claimPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("hamstor: volume builder drop stale claim %s: %v", claimPath, rmErr)
		}
		return
	case unix.ENOENT:
		return // claim already gone
	case unix.ENOSYS, unix.EINVAL:
		// No RENAME_NOREPLACE here. Best effort, with the old window.
		if _, statErr := os.Stat(origPath); statErr == nil {
			if rmErr := os.Remove(claimPath); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("hamstor: volume builder drop stale claim %s: %v", claimPath, rmErr)
			}
			return
		}
		if rnErr := os.Rename(claimPath, origPath); rnErr != nil && !os.IsNotExist(rnErr) {
			log.Printf("hamstor: volume builder restore %s: %v", claimPath, rnErr)
		}
	default:
		log.Printf("hamstor: volume builder restore %s: %v", claimPath, err)
	}
}

// NotifyStaged signals the builder that a new file has been staged.
func (b *Builder) NotifyStaged() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// FlushInode packs a single staged file into a volume and uploads it to S3,
// providing on-demand S3 durability for Fsync.
func (b *Builder) FlushInode(inodeID int64) error {
	path := b.StagePath(inodeID)
	claimPath := path + ".flushing"

	// Atomically claim the staging file so the builder goroutine won't process it.
	if err := os.Rename(path, claimPath); err != nil {
		if os.IsNotExist(err) {
			// File is gone. Check if builder claimed it (.packing) or already packed it.
			packingPath := path + ".packing"
			if _, statErr := os.Stat(packingPath); statErr == nil {
				// Builder has claimed it but may not have finished yet.
				meta, dbErr := b.db.GetInode(inodeID)
				if dbErr != nil {
					return nil // inode deleted, ok
				}
				if meta.VolS3Key != "" || meta.S3Key != "" {
					return nil // already durable
				}
				return fmt.Errorf("flush inode %d: %w", inodeID, ErrBeingPacked)
			}
			// No staging file, no .packing — verify data is durable.
			meta, dbErr := b.db.GetInode(inodeID)
			if dbErr != nil {
				return nil // inode deleted, ok
			}
			if meta.VolS3Key != "" || meta.S3Key != "" {
				return nil // already durable
			}
			return fmt.Errorf("flush inode %d: staging file missing and no S3 reference", inodeID)
		}
		return fmt.Errorf("flush inode %d: claim staging: %w", inodeID, err)
	}

	// Capture mtime before reading data so the mtime check in
	// CommitNeedlesToVolume detects concurrent Flush modifications.
	meta, metaErr := b.db.GetInode(inodeID)
	if metaErr != nil {
		restoreClaim(claimPath, path)
		return nil // inode deleted, ok
	}

	data, err := os.ReadFile(claimPath)
	if err != nil {
		restoreClaim(claimPath, path) // put back for builder to retry
		return fmt.Errorf("flush inode %d: read staging: %w", inodeID, err)
	}

	volKey := s3store.NewKey()
	needle := db.NeedleCommit{InodeID: inodeID, Offset: 0, Size: int64(len(data)), MtimeNs: meta.MtimeNs}

	if err := b.db.InsertVolume(volKey, 0, 0, 0, 0, "open"); err != nil {
		restoreClaim(claimPath, path)
		return fmt.Errorf("flush inode %d: insert volume: %w", inodeID, err)
	}

	if err := b.store.Upload(b.ctx, volKey, data); err != nil {
		b.db.DeleteVolume(volKey)
		restoreClaim(claimPath, path)
		return fmt.Errorf("flush inode %d: upload: %w", inodeID, err)
	}

	committedIDs, err := b.db.CommitNeedlesToVolume(volKey, int64(len(data)), []db.NeedleCommit{needle}, true, "")
	if err != nil {
		b.db.DeleteVolume(volKey)
		b.store.Delete(b.ctx, volKey)
		restoreClaim(claimPath, path) // restore for retry
		return fmt.Errorf("flush inode %d: commit: %w", inodeID, err)
	}
	// Remove the claimed file. If no needles were committed, the uploaded S3
	// volume is orphaned and GC will clean it.
	if err := os.Remove(claimPath); err != nil && !os.IsNotExist(err) {
		log.Printf("hamstor: volume builder remove flushing %s: %v", claimPath, err)
	}
	// Zero needles committed means this flush did NOT make the current data
	// durable: either the inode was already packed (durable elsewhere) or it was
	// superseded by a concurrent overwrite whose fresh staging file now sits at
	// `path` and has not yet been packed. Only report success in the former case;
	// otherwise signal ErrBeingPacked so Fsync keeps waiting for the new data to
	// be packed rather than falsely reporting durability.
	if len(committedIDs) == 0 {
		if m, dbErr := b.db.GetInode(inodeID); dbErr == nil && m.VolS3Key == "" && m.S3Key == "" && m.Size > 0 {
			return ErrBeingPacked
		}
	}
	return nil
}

// Close stops the builder and seals any remaining staged files.
// Waits up to closeTimeout for in-flight S3 operations to finish.
func (b *Builder) Close() error {
	close(b.done)
	timer := time.AfterFunc(closeTimeout, func() {
		b.cancel()
	})
	b.wg.Wait()
	timer.Stop()
	b.cancel() // clean up context
	return nil
}

func (b *Builder) run() {
	defer b.wg.Done()
	ticker := time.NewTicker(fallbackInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			// Final sweep: drain the ENTIRE staging directory. scanAndSeal caps
			// each pass at maxBatchEntries for memory safety, so a single call
			// would leave any backlog (>64 files, common after a bulk copy)
			// stranded as committed inodes with no S3 backing. Loop until a pass
			// seals nothing more. The bound guards against a pathological
			// non-draining file (e.g. an inode that keeps failing to commit).
			for i := 0; i < 1<<20; i++ {
				if b.scanAndSeal(true) == 0 {
					break
				}
			}
			return
		case <-b.notify:
			b.scanAndSeal(false)
		case <-ticker.C:
			// Fallback: seal even small batches to avoid stale staging files
			b.scanAndSeal(true)
		}
	}
}

type stagedFile struct {
	inodeID int64
	data    []byte
	path    string
	mtimeNs int64 // mtime_ns at claim time, used to detect re-writes
}

// scanAndSeal reads staged files and packs them into volumes.
// If forceSmall is true, it seals even if total size is below TargetVolumeSize.
// Returns the number of staged files it sealed into volumes this pass (0 if it
// found nothing, or restored everything because the batch was too small and not
// forced), so the shutdown drain loop knows when the directory is exhausted.
func (b *Builder) scanAndSeal(forceSmall bool) int {
	entries, err := os.ReadDir(b.stagingDir)
	if err != nil || len(entries) == 0 {
		return 0
	}

	var staged []stagedFile
	totalSize := int64(0)
	for _, e := range entries {
		if len(staged) >= maxBatchEntries {
			break
		}
		if e.IsDir() {
			continue
		}
		// Skip temporary and in-flight files written by Flush (.tmp),
		// Fsync (.flushing), or a previous scan cycle (.packing)
		if strings.HasSuffix(e.Name(), ".tmp") || strings.HasSuffix(e.Name(), ".flushing") || strings.HasSuffix(e.Name(), ".packing") {
			continue
		}
		inodeID, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil {
			continue
		}
		path := filepath.Join(b.stagingDir, e.Name())
		claimPath := path + ".packing"

		// Capture mtime BEFORE claiming the file. If a concurrent Flush
		// updates mtime_ns between this read and CommitNeedlesToVolume,
		// the mtime check will detect the mismatch and skip the needle.
		// Reading after the claim would risk capturing a NEW mtime while
		// holding OLD data, causing stale data to pass the mtime check.
		// If the mtime cannot be read, skip this file rather than sealing it
		// with mtimeNs=0 (which would disable the mtime guard entirely and let
		// stale claimed data overwrite a concurrent re-stage). The file is left
		// in place for a later scan. Done before the claim, so no restore needed.
		meta, metaErr := b.db.GetInode(inodeID)
		if metaErr != nil {
			continue
		}
		mtimeNs := meta.MtimeNs

		// Atomically claim the staging file so a concurrent Flush cannot
		// overwrite it between our read and the eventual commit+remove.
		if err := os.Rename(path, claimPath); err != nil {
			if os.IsNotExist(err) {
				continue // claimed by FlushInode or deleted
			}
			log.Printf("hamstor: volume builder claim %s: %v", path, err)
			continue
		}

		data, err := os.ReadFile(claimPath)
		if err != nil {
			log.Printf("hamstor: volume builder read staged %s: %v", claimPath, err)
			// Put it back for the next cycle
			os.Rename(claimPath, path)
			continue
		}

		staged = append(staged, stagedFile{
			inodeID: inodeID,
			data:    data,
			path:    claimPath,
			mtimeNs: mtimeNs,
		})
		totalSize += int64(len(data))
	}

	if len(staged) == 0 {
		return 0
	}

	// Wait for more files unless forced (ticker/shutdown) or enough data
	if !forceSmall && totalSize < TargetVolumeSize {
		// Put claimed files back so the next scan can pick them up, unless a
		// concurrent overwrite already wrote a newer staging file at the path.
		for _, sf := range staged {
			restoreClaim(sf.path, strings.TrimSuffix(sf.path, ".packing"))
		}
		return 0
	}

	b.sealBatch(staged)
	return len(staged)
}

// sealBatch packs staged files into volumes, splitting at TargetVolumeSize boundaries.
func (b *Builder) sealBatch(staged []stagedFile) {
	var buf bytes.Buffer
	var needles []db.NeedleCommit
	var sealedPaths []string

	restorePaths := func(paths []string) {
		for _, p := range paths {
			restoreClaim(p, strings.TrimSuffix(p, ".packing"))
		}
	}

	flush := func() {
		if len(needles) == 0 {
			return
		}

		volKey := s3store.NewKey()
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		commits := make([]db.NeedleCommit, len(needles))
		copy(commits, needles)
		paths := make([]string, len(sealedPaths))
		copy(paths, sealedPaths)

		// Reset accumulators immediately — they've been copied into locals.
		// This prevents the tail flush() from reprocessing failed data.
		buf.Reset()
		needles = needles[:0]
		sealedPaths = sealedPaths[:0]

		if err := b.db.InsertVolume(volKey, 0, 0, 0, 0, "open"); err != nil {
			log.Printf("hamstor: volume builder insert volume: %v", err)
			restorePaths(paths)
			return
		}

		if err := b.store.Upload(b.ctx, volKey, data); err != nil {
			log.Printf("hamstor: volume builder upload %s: %v", volKey, err)
			b.db.DeleteVolume(volKey)
			restorePaths(paths)
			return
		}

		committedIDs, err := b.db.CommitNeedlesToVolume(volKey, int64(len(data)), commits, true, "")
		if err != nil {
			log.Printf("hamstor: volume builder commit %s: %v", volKey, err)
			b.db.DeleteVolume(volKey)
			b.store.Delete(b.ctx, volKey)
			restorePaths(paths)
			return
		}

		// Remove staging files only for needles that were actually committed.
		// Uncommitted needles may have been re-written (race with Flush) or deleted.
		committedSet := make(map[int64]bool, len(committedIDs))
		for _, id := range committedIDs {
			committedSet[id] = true
		}
		for i, p := range paths {
			if committedSet[commits[i].InodeID] {
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					log.Printf("hamstor: volume builder remove committed %s: %v", p, err)
				}
			} else if _, lookupErr := b.db.GetInode(commits[i].InodeID); lookupErr != nil {
				// Inode deleted — remove orphaned staging file
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					log.Printf("hamstor: volume builder remove orphaned %s: %v", p, err)
				}
			} else {
				// Needle not committed (superseded by a new Flush). Restore the
				// claim for a later pass unless a newer staging file already
				// exists at the original path.
				restoreClaim(p, strings.TrimSuffix(p, ".packing"))
			}
		}

		log.Printf("hamstor: volume %s sealed (%d needles, %d bytes)", volKey, len(committedIDs), len(data))
	}

	for _, sf := range staged {
		buf.Write(sf.data)
		needles = append(needles, db.NeedleCommit{
			InodeID: sf.inodeID,
			Offset:  int64(buf.Len()) - int64(len(sf.data)),
			Size:    int64(len(sf.data)),
			MtimeNs: sf.mtimeNs,
		})
		sealedPaths = append(sealedPaths, sf.path)

		if int64(buf.Len()) >= TargetVolumeSize {
			flush()
		}
	}

	// Seal remaining (tail batch)
	flush()
}
