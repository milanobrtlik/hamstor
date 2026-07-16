package ops

import (
	"context"
	"fmt"
	"log"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

func Migrate(ctx context.Context, database *db.DB, store *s3store.Store) error {
	records, err := database.AllS3Keys()
	if err != nil {
		return fmt.Errorf("migrate: load keys: %w", err)
	}

	var migrated, skipped, errorCount int
	for _, r := range records {
		if s3store.IsPrefixed(r.S3Key) {
			skipped++
			continue
		}

		newKey := r.S3Key[:2] + "/" + r.S3Key
		// Copy succeeds only after S3 confirms the destination object exists, so
		// the source is deleted only after both the copy and the DB pointer
		// update succeed. On any per-object failure, log and continue rather than
		// aborting the whole run — one bad object must not strand the rest. The
		// source is never deleted when the copy or DB update failed, so a
		// re-run (idempotent: prefixed keys are skipped) recovers cleanly.
		if err := store.Copy(ctx, r.S3Key, newKey); err != nil {
			log.Printf("migrate: copy %s: %v (skipping)", r.S3Key, err)
			errorCount++
			continue
		}
		if err := database.UpdateS3Key(r.ID, newKey); err != nil {
			log.Printf("migrate: update db for %s: %v (skipping; source kept)", r.S3Key, err)
			errorCount++
			continue
		}
		if err := store.Delete(ctx, r.S3Key); err != nil {
			log.Printf("migrate: delete old key %s: %v", r.S3Key, err)
		}

		migrated++
		if migrated%100 == 0 {
			log.Printf("migrate: %d keys migrated...", migrated)
		}
	}

	log.Printf("migrate: done — %d migrated, %d already prefixed, %d errors", migrated, skipped, errorCount)
	if errorCount > 0 {
		return fmt.Errorf("migrate: completed with %d errors", errorCount)
	}
	return nil
}
