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

	var migrated, skipped int
	for _, r := range records {
		if s3store.IsPrefixed(r.S3Key) {
			skipped++
			continue
		}

		newKey := r.S3Key[:2] + "/" + r.S3Key
		if err := store.Copy(ctx, r.S3Key, newKey); err != nil {
			return fmt.Errorf("migrate: copy %s: %w", r.S3Key, err)
		}
		if err := database.UpdateS3Key(r.ID, newKey); err != nil {
			return fmt.Errorf("migrate: update db for %s: %w", r.S3Key, err)
		}
		if err := store.Delete(ctx, r.S3Key); err != nil {
			log.Printf("migrate: delete old key %s: %v", r.S3Key, err)
		}

		migrated++
		if migrated%100 == 0 {
			log.Printf("migrate: %d keys migrated...", migrated)
		}
	}

	log.Printf("migrate: done — %d migrated, %d already prefixed", migrated, skipped)
	return nil
}
