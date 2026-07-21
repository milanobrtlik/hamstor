package ops

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// gcGracePeriod is the minimum age of an S3 object before GC considers it
// orphaned. This prevents deleting objects from in-flight async uploads
// that haven't been committed to the DB yet.
const gcGracePeriod = 10 * time.Minute

type GCResult struct {
	OrphansFound   int
	OrphansDeleted int
	Errors         int
	DBOrphans      int
}

// gcOptions narrows what a GC run may look at. Production always uses the whole
// bucket and the full grace period; the phase 1 test sets both, because with the
// real grace period a freshly uploaded object is skipped before its key is ever
// compared against the database — so it would survive even a GC that has lost
// track of it, and the test would prove nothing. Scoping the listing then keeps
// that zero grace period from reaching the objects other packages' tests are
// using in the same bucket.
type gcOptions struct {
	grace      time.Duration
	listPrefix string
}

func GC(ctx context.Context, database *db.DB, store *s3store.Store, dryRun bool, excludePrefixes ...string) (*GCResult, error) {
	return gcScoped(ctx, database, store, dryRun, gcOptions{grace: gcGracePeriod}, excludePrefixes...)
}

func gcScoped(ctx context.Context, database *db.DB, store *s3store.Store, dryRun bool, opts gcOptions, excludePrefixes ...string) (*GCResult, error) {
	knownKeys, err := database.AllS3KeySet()
	if err != nil {
		return nil, fmt.Errorf("gc: load keys from db: %w", err)
	}
	log.Printf("gc: %d keys in database", len(knownKeys))

	s3Objects, err := store.ListObjects(ctx, opts.listPrefix)
	if err != nil {
		return nil, fmt.Errorf("gc: list s3 objects: %w", err)
	}
	log.Printf("gc: %d objects in S3", len(s3Objects))

	// Default exclude prefixes
	if len(excludePrefixes) == 0 {
		excludePrefixes = []string{"litestream/"}
	}

	cutoff := time.Now().Add(-opts.grace)
	result := &GCResult{}
	var orphanKeys []string
	for _, obj := range s3Objects {
		excluded := false
		for _, prefix := range excludePrefixes {
			if strings.HasPrefix(obj.Key, prefix) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		if _, ok := knownKeys[obj.Key]; ok {
			continue
		}
		// Skip recently created objects — they may be from in-flight async uploads
		if !obj.LastModified.IsZero() && obj.LastModified.After(cutoff) {
			continue
		}

		result.OrphansFound++
		if dryRun {
			log.Printf("gc: orphan (dry-run): %s", obj.Key)
			continue
		}
		orphanKeys = append(orphanKeys, obj.Key)
	}

	if len(orphanKeys) > 0 {
		deleted, err := store.DeleteBatch(ctx, orphanKeys)
		result.OrphansDeleted += deleted
		if err != nil {
			log.Printf("gc: batch delete: %v", err)
			result.Errors += len(orphanKeys) - deleted
		}
	}

	// Phase 2: find DB inodes whose parent no longer exists
	orphanedInodes, err := database.GetOrphanedInodes()
	if err != nil {
		return nil, fmt.Errorf("gc: find orphaned inodes: %w", err)
	}

	for _, meta := range orphanedInodes {
		result.DBOrphans++
		if dryRun {
			log.Printf("gc: db orphan (dry-run): inode %d %q vol_s3_key=%s", meta.ID, meta.Name, meta.VolS3Key)
		}
	}

	// Delete DB inode rows (atomically with volume stats update), then the block
	// objects they owned.
	//
	// The keys are collected FROM the delete, not before it. An orphaned inode's
	// blocks are removed by ON DELETE CASCADE, so after this call nobody knows
	// them any more — collecting them from the inode row, the way this used to,
	// found nothing at all and left every block of every orphaned file in the
	// bucket forever. Phase 1 would eventually sweep them, but only because it
	// now knows about blocks: the two omissions used to mask each other.
	if !dryRun {
		var dbOrphanS3Keys []string
		for _, meta := range orphanedInodes {
			orphaned, err := database.DeleteInodeWithVolume(meta.ID, meta.VolS3Key)
			if err != nil {
				log.Printf("gc: delete orphan inode %d: %v", meta.ID, err)
				result.Errors++
				continue
			}
			dbOrphanS3Keys = append(dbOrphanS3Keys, orphaned...)
			result.OrphansDeleted++
		}
		if len(dbOrphanS3Keys) > 0 {
			if _, err := store.DeleteBatch(ctx, dbOrphanS3Keys); err != nil {
				log.Printf("gc: batch delete db orphan block keys: %v", err)
				result.Errors += len(dbOrphanS3Keys)
			}
		}
	}

	// Phase 3: delete empty volumes (all needles deleted, older than grace period)
	graceNs := int64(opts.grace / time.Nanosecond)
	emptyVols, err := database.GetEmptyVolumes(graceNs)
	if err != nil {
		log.Printf("gc: get empty volumes: %v", err)
	}
	for _, vol := range emptyVols {
		result.OrphansFound++
		if dryRun {
			log.Printf("gc: empty volume (dry-run): %s", vol.S3Key)
			continue
		}
		if err := store.Delete(ctx, vol.S3Key); err != nil {
			log.Printf("gc: delete empty volume %s: %v", vol.S3Key, err)
			result.Errors++
			continue
		}
		if err := database.DeleteVolume(vol.S3Key); err != nil {
			log.Printf("gc: delete empty volume row %s: %v", vol.S3Key, err)
			result.Errors++
			continue
		}
		result.OrphansDeleted++
		log.Printf("gc: deleted empty volume: %s", vol.S3Key)
	}

	return result, nil
}
