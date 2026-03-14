package ops

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

type GCResult struct {
	OrphansFound   int
	OrphansDeleted int
	Errors         int
}

func GC(ctx context.Context, database *db.DB, store *s3store.Store, dryRun bool) (*GCResult, error) {
	knownKeys, err := database.AllS3KeySet()
	if err != nil {
		return nil, fmt.Errorf("gc: load keys from db: %w", err)
	}
	log.Printf("gc: %d keys in database", len(knownKeys))

	s3Keys, err := store.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("gc: list s3 objects: %w", err)
	}
	log.Printf("gc: %d objects in S3", len(s3Keys))

	result := &GCResult{}
	for _, key := range s3Keys {
		if strings.HasPrefix(key, "litestream/") {
			continue
		}
		if _, ok := knownKeys[key]; ok {
			continue
		}

		result.OrphansFound++
		if dryRun {
			log.Printf("gc: orphan (dry-run): %s", key)
			continue
		}

		if err := store.Delete(ctx, key); err != nil {
			log.Printf("gc: delete %s: %v", key, err)
			result.Errors++
		} else {
			result.OrphansDeleted++
		}
	}

	return result, nil
}
