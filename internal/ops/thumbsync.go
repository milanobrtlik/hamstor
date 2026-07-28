package ops

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
)

// syncConcurrency bounds parallel thumbnail downloads. Each holds one small
// object (tens of KB), so this is a latency knob rather than a memory one: a
// library of tens of thousands of images is tens of thousands of round trips,
// and doing them one at a time is the difference between a pass that finishes
// while you browse and one you never see the end of.
const syncConcurrency = 16

// SyncStats reports what a materialization pass did.
type SyncStats struct {
	Written int
	Skipped int
	Failed  int
}

func (s SyncStats) String() string {
	return fmt.Sprintf("%d written, %d already current, %d failed", s.Written, s.Skipped, s.Failed)
}

// SyncThumbnails materializes stored thumbnails into a freedesktop cache.
//
// This is the half of durable thumbnails that the desktop can actually see. A
// freedesktop cache is a local directory belonging to one user; hamstor cannot
// make a file manager read from a bucket, so storing thumbnails durably is
// worth nothing on its own — something has to put them where the desktop looks.
//
// The pass is idempotent (Cache.Has skips what is already current), touches no
// hot path, and downloads no originals: that last property is the entire claim,
// so a new machine populates its thumbnail cache for the cost of the
// thumbnails, not the photos.
func SyncThumbnails(ctx context.Context, database *db.DB, store *s3store.Store, enc *crypto.Encryptor, cache thumb.Cache, mountpoint string) (SyncStats, error) {
	var stats SyncStats
	if cache.Dir == "" || mountpoint == "" {
		return stats, nil
	}

	targets, err := database.AllThumbnailTargets()
	if err != nil {
		return stats, err
	}

	var written, skipped, failed atomic.Int64
	sem := make(chan struct{}, syncConcurrency)
	var wg sync.WaitGroup

	for _, tt := range targets {
		if ctx.Err() != nil {
			break
		}
		// The stored render may predate the file's current contents — the file
		// was overwritten and the new thumbnail has not been stored yet, or
		// never will be. Materializing it would put a stale image in the cache
		// stamped with the CURRENT mtime, which every later staleness check
		// would then accept. Leave it; the desktop renders its own.
		if tt.Rec.SrcMtimeNs != tt.MtimeNs {
			skipped.Add(1)
			continue
		}
		if cache.Has(mountpoint, tt.Path, tt.MtimeNs/1e9) {
			skipped.Add(1)
			continue
		}

		wg.Add(1)
		go func(tt db.ThumbTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if ctx.Err() != nil {
				return
			}

			r, err := fetchThumb(ctx, store, enc, tt.Rec)
			if err != nil {
				log.Printf("hamstor: thumbnail sync %s: %v", tt.Path, err)
				failed.Add(1)
				return
			}
			cache.Write(mountpoint, tt.Path, tt.MtimeNs/1e9, r)
			written.Add(1)
		}(tt)
	}
	wg.Wait()

	stats.Written = int(written.Load())
	stats.Skipped = int(skipped.Load())
	stats.Failed = int(failed.Load())
	return stats, ctx.Err()
}

// fetchThumb downloads and unpacks one stored thumbnail.
//
// Decryption happens only here, on what came straight off Store.Download —
// the same rule the block path follows, and the reason stored thumbnails are
// deliberately kept OUT of the disk cache: a cache entry is served back as
// content, so caching the sealed body would hand ciphertext to a reader. The
// disk cache would be wrong for them anyway, since each is read once per
// machine and then lives in the freedesktop cache forever, and putting them
// there would evict real data to hold a copy nobody reads again.
func fetchThumb(ctx context.Context, store *s3store.Store, enc *crypto.Encryptor, rec db.ThumbRecord) (thumb.Rendered, error) {
	data, err := store.Download(ctx, rec.S3Key)
	if err != nil {
		return thumb.Rendered{}, err
	}
	// offset/length are 0/len while one object holds one thumbnail. Honouring
	// them here is what lets the writer start packing several into one object
	// without this side needing to change.
	if rec.Offset != 0 || (rec.Length != 0 && rec.Length != int64(len(data))) {
		if rec.Offset < 0 || rec.Length < 0 || rec.Offset+rec.Length > int64(len(data)) {
			return thumb.Rendered{}, fmt.Errorf("thumbnail extent %d+%d outside a %d-byte object", rec.Offset, rec.Length, len(data))
		}
		data = data[rec.Offset : rec.Offset+rec.Length]
	}
	if enc != nil && crypto.IsEncrypted(data) {
		data, err = enc.Decrypt(data)
		if err != nil {
			return thumb.Rendered{}, err
		}
	}
	if rec.NormalLen < 0 || rec.NormalLen > int64(len(data)) {
		return thumb.Rendered{}, fmt.Errorf("normal_len %d outside a %d-byte body", rec.NormalLen, len(data))
	}
	return thumb.Rendered{Normal: data[:rec.NormalLen], Large: data[rec.NormalLen:]}, nil
}
