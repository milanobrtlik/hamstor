package ops

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// CompactResult holds the results of a volume compaction run.
type CompactResult struct {
	VolumesScanned   int
	VolumesCompacted int
	NeedlesMoved     int
	BytesReclaimed   int64
	Errors           int
}

// Compact rewrites volumes with a high ratio of dead space into new, tighter volumes.
func Compact(ctx context.Context, database *db.DB, store *s3store.Store, deadRatio float64, dryRun bool) (*CompactResult, error) {
	volumes, err := database.GetVolumesForCompaction(deadRatio)
	if err != nil {
		return nil, fmt.Errorf("compact: get volumes: %w", err)
	}

	result := &CompactResult{VolumesScanned: len(volumes)}
	log.Printf("compact: found %d volumes exceeding %.0f%% dead ratio", len(volumes), deadRatio*100)

	for _, vol := range volumes {
		if dryRun {
			log.Printf("compact: would compact volume %s (total=%d, live=%d, needles=%d/%d)",
				vol.S3Key, vol.TotalSize, vol.LiveSize, vol.LiveCount, vol.NeedleCount)
			continue
		}

		moved, reclaimed, err := compactVolume(ctx, database, store, vol)
		if err != nil {
			log.Printf("compact: volume %s failed: %v", vol.S3Key, err)
			result.Errors++
			continue
		}
		result.VolumesCompacted++
		result.NeedlesMoved += moved
		result.BytesReclaimed += reclaimed
	}

	return result, nil
}

// reseal returns a volume to 'sealed' after a compaction attempt gave up on it.
//
// The error is logged rather than dropped because nothing else would ever
// surface it: GetVolumesForCompaction selects `status = 'sealed'`, so a rollback
// that silently fails leaves the volume pinned in 'compacting' — never chosen
// for compaction again, and its dead space never reclaimed. That is invisible
// from the outside, and this log line is the only trace it leaves.
func reseal(database *db.DB, key string) {
	if err := database.SetVolumeStatus(key, "sealed"); err != nil {
		log.Printf("compact: volume %s stuck in 'compacting' — rollback to sealed failed: %v", key, err)
	}
}

// dropNewVolume discards the half-built replacement volume after a failed
// compaction. Both halves are best-effort: whatever survives is an orphan (an
// object no row names, or a row with no live needles) and gc collects it on a
// later run. Logging is what keeps that orphan explicable when it shows up.
func dropNewVolume(ctx context.Context, database *db.DB, store *s3store.Store, key string, alsoS3 bool) {
	if alsoS3 {
		if err := store.Delete(ctx, key); err != nil {
			log.Printf("compact: drop new volume object %s: %v (left for gc)", key, err)
		}
	}
	if err := database.DeleteVolume(key); err != nil {
		log.Printf("compact: drop new volume row %s: %v (left for gc)", key, err)
	}
}

func compactVolume(ctx context.Context, database *db.DB, store *s3store.Store, vol db.VolumeRecord) (int, int64, error) {
	// Mark as compacting
	if err := database.SetVolumeStatus(vol.S3Key, "compacting"); err != nil {
		return 0, 0, fmt.Errorf("set compacting status: %w", err)
	}

	// Get live needles
	needles, err := database.NeedlesInVolume(vol.S3Key)
	if err != nil {
		reseal(database, vol.S3Key)
		return 0, 0, fmt.Errorf("get needles: %w", err)
	}

	if len(needles) == 0 {
		// All dead — just delete
		if err := store.Delete(ctx, vol.S3Key); err != nil {
			log.Printf("compact: delete empty volume %s S3: %v", vol.S3Key, err)
			reseal(database, vol.S3Key)
			return 0, 0, nil
		}
		if err := database.DeleteVolume(vol.S3Key); err != nil {
			log.Printf("compact: delete empty volume %s row: %v", vol.S3Key, err)
		}
		return 0, vol.TotalSize, nil
	}

	// Build new volume from live needles
	var buf bytes.Buffer
	commits := make([]db.NeedleCommit, 0, len(needles))

	for _, needle := range needles {
		// Download needle from old volume
		data, err := store.DownloadRange(ctx, vol.S3Key, needle.VolOffset, needle.VolSize)
		if err != nil {
			reseal(database, vol.S3Key)
			return 0, 0, fmt.Errorf("download needle inode %d: %w", needle.ID, err)
		}

		offset := int64(buf.Len())
		buf.Write(data)
		commits = append(commits, db.NeedleCommit{
			InodeID: needle.ID,
			Offset:  offset,
			Size:    int64(len(data)),
			MtimeNs: needle.MtimeNs,
		})
	}

	// Upload new volume
	newKey := s3store.NewKey()
	if err := database.InsertVolume(newKey, int64(buf.Len()), int64(buf.Len()), len(commits), len(commits), "open"); err != nil {
		reseal(database, vol.S3Key)
		return 0, 0, fmt.Errorf("insert new volume: %w", err)
	}

	if err := store.Upload(ctx, newKey, buf.Bytes()); err != nil {
		dropNewVolume(ctx, database, store, newKey, false)
		reseal(database, vol.S3Key)
		return 0, 0, fmt.Errorf("upload new volume: %w", err)
	}

	// Atomically commit needles to new volume
	committedIDs, err := database.CommitNeedlesToVolume(newKey, int64(buf.Len()), commits, false, vol.S3Key)
	if err != nil {
		// New volume uploaded but the DB refused it. Drop both halves; anything
		// that survives the drop is an orphan and gc collects it.
		dropNewVolume(ctx, database, store, newKey, true)
		reseal(database, vol.S3Key)
		return 0, 0, fmt.Errorf("commit to new volume: %w", err)
	}

	skipped := len(commits) - len(committedIDs)
	if skipped > 0 {
		log.Printf("compact: volume %s: %d/%d needles skipped (modified during compaction)", vol.S3Key, skipped, len(commits))
	}

	if len(committedIDs) == 0 {
		// Every needle was rewritten while we were compacting, so the volume we
		// just built holds nothing anyone references.
		dropNewVolume(ctx, database, store, newKey, true)
		reseal(database, vol.S3Key)
		return 0, 0, nil
	}

	// Delete old volume
	if err := store.Delete(ctx, vol.S3Key); err != nil {
		log.Printf("compact: delete old volume %s: %v (will retry next run)", vol.S3Key, err)
		reseal(database, vol.S3Key)
	} else if err := database.DeleteVolume(vol.S3Key); err != nil {
		log.Printf("compact: delete old volume row %s: %v", vol.S3Key, err)
	}

	// Reclaimed = old volume size - new volume size (actual S3 savings).
	// The new volume may contain born-dead space from skipped needles,
	// but that's accounted for in its live_size and will compact later.
	newVolSize := int64(buf.Len())
	reclaimed := vol.TotalSize - newVolSize
	log.Printf("compact: volume %s → %s (%d needles, reclaimed %d bytes)", vol.S3Key, newKey, len(committedIDs), reclaimed)
	return len(committedIDs), reclaimed, nil
}
