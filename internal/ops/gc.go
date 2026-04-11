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

func GC(ctx context.Context, database *db.DB, store *s3store.Store, dryRun bool, excludePrefixes ...string) (*GCResult, error) {
	knownKeys, err := database.AllS3KeySet()
	if err != nil {
		return nil, fmt.Errorf("gc: load keys from db: %w", err)
	}
	log.Printf("gc: %d keys in database", len(knownKeys))

	s3Objects, err := store.ListObjects(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("gc: list s3 objects: %w", err)
	}
	log.Printf("gc: %d objects in S3", len(s3Objects))

	// Default exclude prefixes
	if len(excludePrefixes) == 0 {
		excludePrefixes = []string{"litestream/"}
	}

	cutoff := time.Now().Add(-gcGracePeriod)
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

	// Batch-delete S3 keys for DB orphans
	var dbOrphanS3Keys []string
	for _, meta := range orphanedInodes {
		result.DBOrphans++
		if dryRun {
			log.Printf("gc: db orphan (dry-run): inode %d %q s3_key=%s", meta.ID, meta.Name, meta.S3Key)
			continue
		}
		if meta.S3Key != "" {
			dbOrphanS3Keys = append(dbOrphanS3Keys, meta.S3Key)
		}
	}
	if len(dbOrphanS3Keys) > 0 {
		if _, err := store.DeleteBatch(ctx, dbOrphanS3Keys); err != nil {
			log.Printf("gc: batch delete db orphan s3 keys: %v", err)
			result.Errors += len(dbOrphanS3Keys)
		}
	}

	// Delete DB inode rows
	if !dryRun {
		for _, meta := range orphanedInodes {
			if err := database.DeleteInode(meta.ID); err != nil {
				log.Printf("gc: delete orphan inode %d: %v", meta.ID, err)
				result.Errors++
			} else {
				result.OrphansDeleted++
			}
		}
	}

	return result, nil
}
