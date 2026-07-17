package hfuse

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// Cleanup removes pending inodes left by a crash. Run it AFTER RecoverPending.
//
// An inode with bytes still retained under pendingDir is skipped, not deleted:
// RecoverPending leaves those behind when the upload could not go through (S3
// down, say), and they are retried on the next start. Deleting them here would
// throw away recoverable data and then orphan the retained file, which is
// exactly the loss the retain/recover pair exists to prevent.
func Cleanup(d *db.DB, store *s3store.Store, pendingDir string) error {
	pending, err := d.GetPending()
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	var lost []db.InodeMeta
	for _, meta := range pending {
		if hasRetainedData(pendingDir, meta.ID) {
			continue
		}
		lost = append(lost, meta)
	}
	if len(lost) == 0 {
		return nil
	}

	// What is left really is unrecoverable — but name the files. A bare count
	// makes data loss look like routine housekeeping, and the user was told the
	// copy succeeded.
	log.Printf("hamstor: %d pending entries have no recoverable data, removing them:", len(lost))
	for _, meta := range lost {
		log.Printf("hamstor:   lost: %s (inode %d)", meta.Name, meta.ID)
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

// hasRetainedData reports whether pendingDir holds bytes for inodeID that a
// later start could still upload.
func hasRetainedData(pendingDir string, inodeID int64) bool {
	if pendingDir == "" {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(pendingDir, fmt.Sprintf("%d.*", inodeID)))
	if err != nil {
		return false
	}
	for _, m := range matches {
		if !strings.HasSuffix(m, ".tmp") {
			return true
		}
	}
	return false
}

// RecoverPending finishes uploads that failed in a previous run. The async
// upload path retains the exact bytes it meant to send under pendingDir, keyed
// by inode; here they are uploaded and the inode committed, turning what used to
// be silent data loss into a delay.
//
// Must run BEFORE Cleanup, which deletes every remaining pending inode.
//
// Retained bytes are uploaded verbatim: under encryption they are already
// ciphertext, so this neither needs nor consults the passphrase. A file whose
// inode is gone (unlinked, or already cleaned up by an older build) is dropped.
// Anything that fails to upload is left in place to try again next boot rather
// than deleted — a full disk is a better outcome than a lost file.
func RecoverPending(d *db.DB, store *s3store.Store, pendingDir string) error {
	if pendingDir == "" {
		return nil
	}
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	recovered, failed := 0, 0
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(pendingDir, e.Name())

		// "<inodeID>.<logicalSize>"
		dot := strings.LastIndex(e.Name(), ".")
		if dot <= 0 {
			log.Printf("hamstor: recover: unrecognized file %s, removing", e.Name())
			os.Remove(path)
			continue
		}
		inodeID, err1 := strconv.ParseInt(e.Name()[:dot], 10, 64)
		logicalSize, err2 := strconv.ParseInt(e.Name()[dot+1:], 10, 64)
		if err1 != nil || err2 != nil {
			log.Printf("hamstor: recover: unrecognized file %s, removing", e.Name())
			os.Remove(path)
			continue
		}

		meta, err := d.GetInode(inodeID)
		if err != nil {
			// Inode gone (unlinked, or an older build already cleaned it up).
			os.Remove(path)
			continue
		}
		if meta.Status == "committed" {
			// A later write already made this inode durable; the retained copy is
			// stale and must not overwrite it.
			os.Remove(path)
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			log.Printf("hamstor: recover: open %s: %v", e.Name(), err)
			failed++
			continue
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			failed++
			continue
		}

		key := s3store.NewKey()
		err = store.UploadReader(context.Background(), key, f, info.Size())
		f.Close()
		if err != nil {
			log.Printf("hamstor: recover: upload %s: %v (kept, will retry next start)", meta.Name, err)
			failed++
			continue
		}

		if _, err := d.CommitInode(inodeID, key, logicalSize); err != nil {
			log.Printf("hamstor: recover: commit %s: %v (kept, will retry next start)", meta.Name, err)
			store.Delete(context.Background(), key)
			failed++
			continue
		}
		os.Remove(path)
		log.Printf("hamstor: recovered %s (inode %d, %d bytes) from a failed upload", meta.Name, inodeID, logicalSize)
		recovered++
	}

	if recovered > 0 || failed > 0 {
		log.Printf("hamstor: recovery: %d file(s) restored, %d still pending", recovered, failed)
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
