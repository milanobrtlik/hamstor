package hfuse

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/milan/hamstor/internal/thumb"
)

// BackfillStats reports what a backfill pass did.
type BackfillStats struct {
	Rendered int
	Skipped  int
	Failed   int
}

func (s BackfillStats) String() string {
	return fmt.Sprintf("%d rendered, %d skipped, %d failed", s.Rendered, s.Skipped, s.Failed)
}

// BackfillThumbnails renders and stores thumbnails for images that have none.
//
// Every image written before thumbnails were stored durably is in this set, and
// so is every image copied in by a build that had the feature off. Unlike the
// sync pass, this one MUST read the originals — there is nothing else to render
// from — so it is opt-in (--thumbnails=backfill) and pays the download cost
// exactly once per image, forever. Doing it now is much cheaper than doing it
// later: it is the same bytes either way, but a library imported after this
// lands never needs it at all.
//
// Idempotence is the predicate itself (no row in thumbnails), so a finished
// library costs one query. No marker to keep in sync, and images arriving from
// another machine are picked up on the next pass without anything being reset.
func (hfs *HamstorFS) BackfillThumbnails(ctx context.Context) (BackfillStats, error) {
	var stats BackfillStats
	if !hfs.thumbnailsEnabled() {
		return stats, nil
	}

	candidates, err := hfs.DB.ThumbnaillessImages()
	if err != nil {
		return stats, err
	}

	var rendered, skipped, failed atomic.Int64
	var wg sync.WaitGroup

	for _, tt := range candidates {
		if ctx.Err() != nil {
			break
		}
		if !thumb.IsImageExt(filepath.Base(tt.Path)) {
			continue
		}
		// Bounding by size before reading, not after: thumb.Render rejects a
		// 100 MB image anyway, but only once it is on the heap and only after it
		// has been downloaded in full.
		if tt.Size > thumb.MaxImageBytes {
			skipped.Add(1)
			continue
		}

		wg.Add(1)
		go func(inodeID, size, mtimeNs int64, relPath string) {
			defer wg.Done()
			// The same slot every other thumbnail operation takes, and taken
			// BEFORE the file is read: that is what keeps one full-size image
			// per worker on the heap rather than one per candidate.
			if hfs.ThumbSem != nil {
				hfs.ThumbSem <- struct{}{}
				defer func() { <-hfs.ThumbSem }()
			}
			if ctx.Err() != nil {
				return
			}

			data, ok := hfs.readWholeInode(ctx, inodeID, size)
			if !ok {
				failed.Add(1)
				return
			}
			r, err := thumb.Render(data)
			if err != nil {
				// Not a failure worth reporting per file: a .png that is not a
				// png is the user's, and it will be one every pass.
				skipped.Add(1)
				return
			}
			hfs.ThumbCache.Write(hfs.Mountpoint, relPath, mtimeNs/1e9, r)
			hfs.storeThumb(ctx, inodeID, mtimeNs, r)
			rendered.Add(1)
		}(tt.InodeID, tt.Size, tt.MtimeNs, tt.Path)
	}
	wg.Wait()

	stats.Rendered = int(rendered.Load())
	stats.Skipped = int(skipped.Load())
	stats.Failed = int(failed.Load())
	return stats, ctx.Err()
}

// readWholeInode reads a committed file through the ordinary read path.
//
// It builds a read-only handle in-package rather than going through the mount:
// the daemon opening its own mountpoint would be a FUSE request served by the
// process making it. The handle carries no *fs.Inode, which is safe precisely
// because this never writes — h.inode is read in exactly one place, the
// NotifyContent in flushAsync.
//
// The result covers the WHOLE file, which is why the wholeFileSnapshot gate
// that governs write-time thumbnails does not belong here and must not be added:
// that gate exists because a flush's snapshot holds only the blocks it dirtied,
// with holes everywhere else. A committed file read end to end has no such
// problem — holes in it are the file's own, and the desktop would render them
// identically.
func (hfs *HamstorFS) readWholeInode(ctx context.Context, inodeID, size int64) ([]byte, bool) {
	st := hfs.acquireWrite(inodeID)
	defer hfs.releaseWrite(inodeID, st)

	// A writer holds unflushed bytes for this inode. Its own flush will produce
	// the thumbnail, from a snapshot that knows which blocks it covers; reading
	// around it here would race that for no gain.
	st.mu.Lock()
	dirty := st.dirty
	st.mu.Unlock()
	if dirty {
		return nil, false
	}

	h := &HamstorHandle{hfs: hfs, inodeID: inodeID, st: st, fileSize: size}
	out := make([]byte, size)

	const chunk = 1 << 20
	for off := int64(0); off < size; {
		end := min(off+chunk, size)
		res, errno := h.Read(ctx, out[off:end], off)
		if errno != 0 {
			log.Printf("hamstor: thumbnail backfill read inode %d at %d: %v", inodeID, off, errno)
			return nil, false
		}
		b, status := res.Bytes(out[off:end])
		if !status.Ok() {
			log.Printf("hamstor: thumbnail backfill read inode %d at %d: %v", inodeID, off, status)
			return nil, false
		}
		if len(b) == 0 {
			// Short of the recorded size: the file is not what the metadata
			// says, so render nothing rather than a truncated image.
			return nil, false
		}
		// A no-op when Bytes returned the destination itself, which is the
		// common case.
		copy(out[off:end], b)
		off += int64(len(b))
	}
	return out, true
}
