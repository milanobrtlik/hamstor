package hfuse

import (
	"context"
	"log"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

func Cleanup(d *db.DB, store *s3store.Store) error {
	pending, err := d.GetPending()
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	log.Printf("hamstor: cleaning up %d pending entries", len(pending))
	for _, meta := range pending {
		if meta.S3Key != "" {
			if err := store.Delete(context.Background(), meta.S3Key); err != nil {
				log.Printf("hamstor: cleanup s3 delete %s: %v", meta.S3Key, err)
			}
		}
		if err := d.DeleteInode(meta.ID); err != nil {
			log.Printf("hamstor: cleanup delete inode %d: %v", meta.ID, err)
		}
	}
	return nil
}
