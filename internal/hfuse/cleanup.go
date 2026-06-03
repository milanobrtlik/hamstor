package hfuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

// CleanupVolumes removes open (incomplete) volumes left by a crash.
// Pending inodes referencing these volumes are cleaned up by Cleanup().
func CleanupVolumes(d *db.DB, store *s3store.Store) error {
	openVols, err := d.GetOpenVolumes()
	if err != nil {
		return err
	}
	if len(openVols) > 0 {
		log.Printf("hamstor: cleaning up %d open volumes", len(openVols))
	}
	for _, vol := range openVols {
		// Delete S3 object if it was uploaded before the crash
		if err := store.Delete(context.Background(), vol.S3Key); err != nil {
			log.Printf("hamstor: cleanup volume s3 delete %s: %v", vol.S3Key, err)
		}
		if err := d.DeleteVolume(vol.S3Key); err != nil {
			log.Printf("hamstor: cleanup delete volume %s: %v", vol.S3Key, err)
		}
	}

	// Reset volumes stuck in "compacting" state from a crash.
	// These were sealed before compaction started and are safe to return to "sealed".
	compactingVols, err := d.GetCompactingVolumes()
	if err != nil {
		return err
	}
	if len(compactingVols) > 0 {
		log.Printf("hamstor: resetting %d compacting volumes to sealed", len(compactingVols))
		for _, vol := range compactingVols {
			if err := d.SetVolumeStatus(vol.S3Key, "sealed"); err != nil {
				log.Printf("hamstor: reset compacting volume %s: %v", vol.S3Key, err)
			}
		}
	}

	return nil
}

// CleanupStagingDir removes orphaned staging files left by a crash.
// A staging file is orphaned if its inode no longer exists or already
// has an S3 key or volume reference.
func CleanupStagingDir(d *db.DB, stagingDir string) error {
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	cleaned := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Remove incomplete writes (.tmp) and abandoned Fsync claims (.flushing)
		if strings.HasSuffix(e.Name(), ".tmp") || strings.HasSuffix(e.Name(), ".flushing") {
			os.Remove(filepath.Join(stagingDir, e.Name()))
			cleaned++
			continue
		}
		// Interrupted builder claims (.packing): recover if inode still needs data
		if strings.HasSuffix(e.Name(), ".packing") {
			baseName := strings.TrimSuffix(e.Name(), ".packing")
			inodeID, parseErr := strconv.ParseInt(baseName, 10, 64)
			if parseErr != nil {
				os.Remove(filepath.Join(stagingDir, e.Name()))
				cleaned++
				continue
			}
			meta, lookupErr := d.GetInode(inodeID)
			if lookupErr != nil {
				// Inode deleted — safe to remove
				os.Remove(filepath.Join(stagingDir, e.Name()))
				cleaned++
				continue
			}
			if meta.S3Key != "" || meta.VolS3Key != "" {
				// Already has storage — staging file is stale
				os.Remove(filepath.Join(stagingDir, e.Name()))
				cleaned++
				continue
			}
			// Inode exists but has no storage — rename back so builder picks it up
			os.Rename(filepath.Join(stagingDir, e.Name()), filepath.Join(stagingDir, baseName))
			cleaned++
			continue
		}
		inodeID, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil {
			// Not a staging file — skip
			continue
		}
		meta, err := d.GetInode(inodeID)
		if err != nil {
			// Inode deleted — remove orphan
			os.Remove(filepath.Join(stagingDir, e.Name()))
			cleaned++
			continue
		}
		if meta.S3Key != "" || meta.VolS3Key != "" {
			// Already has storage — staging file is stale
			os.Remove(filepath.Join(stagingDir, e.Name()))
			cleaned++
		}
	}
	if cleaned > 0 {
		log.Printf("hamstor: cleaned %d orphaned staging files", cleaned)
	}
	return nil
}
