package hfuse

import (
	"context"
	"fmt"
	"io"
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
		// Whatever blocks this half-finished upload managed to commit are
		// unreachable once the row is gone, and this is the last place their keys
		// are known.
		orphaned, err := d.DeleteInode(meta.ID)
		if err != nil {
			log.Printf("hamstor: cleanup delete inode %d: %v", meta.ID, err)
			continue
		}
		for _, key := range orphaned {
			if err := store.Delete(context.Background(), key); err != nil {
				log.Printf("hamstor: cleanup s3 delete %s: %v", key, err)
			}
		}
	}
	return nil
}

// RecoverPending finishes uploads that failed in a previous run. The async
// upload path retains the exact bytes it meant to send under
// pendingDir/<inodeID>/ (see pending.go); here the whole set is uploaded and the
// inode committed, turning what used to be silent data loss into a delay.
//
// Must run BEFORE Cleanup, which deletes every remaining pending inode.
//
// Retained bytes are uploaded verbatim: under encryption they are already
// ciphertext, so this neither needs nor consults the passphrase.
//
// It deletes bytes in exactly two cases, both of which prove them redundant: the
// set was uploaded and committed, or the inode it belongs to is gone or already
// committed by a later write. Everything else it can only name — a set whose
// meta will not parse, a half-built <id>.tmp-* directory, a file left by an
// older build — because those bytes are still somebody's only copy, and a
// startup path is the wrong place to decide otherwise. The nag repeats every
// boot until a human clears it, which is the intent.
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

	recovered, failed, stranded := 0, 0, 0
	for _, e := range entries {
		path := filepath.Join(pendingDir, e.Name())

		// A retained set is a DIRECTORY named by a bare inode number. Anything
		// else is either a set still being built when the process died
		// (<id>.tmp-*), or a leftover from a build that retained differently.
		if !e.IsDir() {
			log.Printf("hamstor: recover: %s is not a retained set (older format?) — left in place, nothing will read it", path)
			stranded++
			continue
		}
		inodeID, parseErr := strconv.ParseInt(e.Name(), 10, 64)
		if parseErr != nil {
			log.Printf("hamstor: recover: %s is an unfinished retained set — left in place, nothing will read it", path)
			stranded++
			continue
		}

		set, err := readPendingSet(path)
		if err != nil {
			// Only a complete set is recoverable, and only a rename makes one
			// visible — so this is a set corrupted after the fact, not one caught
			// mid-write. Either way nobody can commit it.
			log.Printf("hamstor: recover: retained set %s is unusable (%v) — left in place, nothing will read it", path, err)
			stranded++
			continue
		}

		meta, err := d.GetInode(inodeID)
		if err != nil {
			// Inode gone (unlinked, or already cleaned up by an earlier run).
			os.RemoveAll(path)
			continue
		}
		if meta.Status == "committed" && inodeHasStorage(d, meta) {
			// A later write already made this inode durable; the retained copy is
			// stale and must not overwrite it.
			//
			// Storage, not status alone. A 'committed' inode with no blocks and no
			// needle is what open(O_TRUNC) leaves behind (see inodeHasStorage), so
			// dropping its set here would discard the only copy of a rewrite that
			// failed — undoing the retention that had just saved it. Retention marks
			// such an inode back to 'pending', so this should not be reachable; it
			// is the same predicate on both sides precisely so that a missed mark
			// costs nothing.
			os.RemoveAll(path)
			continue
		}

		committed, err := recoverSet(d, store, path, inodeID, meta.Name, set)
		if err != nil {
			log.Printf("hamstor: recover: %s: %v (kept, will retry next start)", meta.Name, err)
			failed++
			continue
		}
		os.RemoveAll(path)
		if !committed {
			continue
		}
		log.Printf("hamstor: recovered %s (inode %d, %d bytes in %d block(s)) from a failed upload",
			meta.Name, inodeID, set.FileSize, len(set.Blocks))
		recovered++
	}

	if recovered > 0 || failed > 0 || stranded > 0 {
		log.Printf("hamstor: recovery: %d file(s) restored, %d still pending, %d unreadable leftover(s)",
			recovered, failed, stranded)
	}
	return nil
}

// recoverSet uploads a retained set and commits it in one transaction. On any
// error it deletes what this attempt uploaded and returns, leaving the retained
// directory for the caller to keep. It reports false with no error when the
// inode vanished mid-recovery: nothing to keep, nothing recovered.
//
// Every block goes up under a FRESH key, including blocks the failed flush had
// already uploaded. Those never reached the blocks table, so they are not in
// AllS3KeySet() either and GC phase 1 removes them once gcGracePeriod has
// passed: a daemon down for a weekend comes back to a bucket that no longer has
// them. There is nothing here to skip and nothing to reuse.
func recoverSet(d *db.DB, store *s3store.Store, dir string, inodeID int64, name string, set *pendingMeta) (bool, error) {
	ctx := context.Background()

	data, err := os.Open(filepath.Join(dir, "data"))
	if err != nil {
		return false, err
	}
	defer data.Close()

	uploaded := make([]string, 0, len(set.Blocks))
	blocks := make([]db.BlockCommit, 0, len(set.Blocks))
	drop := func() {
		for _, key := range uploaded {
			if delErr := store.Delete(ctx, key); delErr != nil {
				log.Printf("hamstor: recover: cleanup %s: %v", key, delErr)
			}
		}
	}

	// All of it, or none of it: a set committed halfway is a file half old and
	// half zeroes, which is exactly the silent corruption this layout exists to
	// avoid.
	for _, b := range set.Blocks {
		key := s3store.NewKey()
		body := io.NewSectionReader(data, b.Off, b.Stored)
		if err := store.UploadReader(ctx, key, body, b.Stored); err != nil {
			drop()
			return false, fmt.Errorf("upload block %d: %w", b.Index, err)
		}
		uploaded = append(uploaded, key)
		// Size is the LOGICAL extent from the meta, never b.Stored: under
		// encryption the object carries a version byte, a nonce and a tag on top
		// of the plaintext, and recording that as the block's extent makes the
		// file read long.
		blocks = append(blocks, db.BlockCommit{Index: b.Index, S3Key: key, Size: b.Size})
	}

	// Orphans should be empty: a retained set only ever belongs to an inode that
	// was never committed, so there is no previous storage to replace. Delete
	// whatever does come back rather than assume.
	committed, orphaned, err := d.CommitBlocks(inodeID, blocks, set.FileSize)
	if err != nil {
		drop()
		return false, fmt.Errorf("commit: %w", err)
	}
	if !committed {
		// The inode went away between the lookup above and here. Nothing
		// references these objects now, and the bytes belong to a file the user
		// deleted.
		log.Printf("hamstor: recover: inode %d (%s) disappeared during recovery, dropping %d object(s)",
			inodeID, name, len(uploaded))
		drop()
		return false, nil
	}
	for _, o := range orphaned {
		store.Delete(ctx, o)
	}
	return true, nil
}

// CheckStagedData returns committed files whose data is neither in S3 nor in the
// staging directory — reads of these return EIO and no retry will fix it.
//
// A small file is committed the moment it is staged, so it is durable only on
// local disk until the builder packs it. That is the documented trade for
// bulk-copy throughput and is fine while the disk is there. It stops being fine
// when the DB outlives the disk: restore the Litestream copy onto a fresh host
// and these inodes claim to be committed while their bytes never left the old
// machine. Surfacing them by name beats letting the user find out one EIO at a
// time.
//
// Run after CleanupStagingDir, which normalizes interrupted claims back to plain
// staging files, so a file mid-pack is not mistaken for a lost one.
func CheckStagedData(d *db.DB, stagingDir string) ([]db.InodeMeta, error) {
	staged, err := d.GetStagedInodes()
	if err != nil {
		return nil, err
	}
	var missing []db.InodeMeta
	for _, meta := range staged {
		matches, _ := filepath.Glob(filepath.Join(stagingDir, fmt.Sprintf("%d*", meta.ID)))
		if len(matches) == 0 {
			missing = append(missing, meta)
		}
	}
	return missing, nil
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
// A staging file is orphaned if its inode no longer exists or already has
// storage: an S3 key, a volume reference, or a set of blocks.
//
// Blocks count as storage here for the same reason they do everywhere else. A
// file that grew past MaxNeedleSize is committed as blocks and its staging file
// removed; if a crash lands between those two, the leftover is stale. Treating
// it as still-staged would rename it back for the builder, which would claim it,
// pack it, have the commit refused (the inode already has blocks) and restore
// the claim — then do it again on the next notify, for the life of the mount,
// uploading an orphaned volume each time round.
func CleanupStagingDir(d *db.DB, stagingDir string) error {
	// hasStorage answers "is this staging file stale?" for both the .packing
	// branch and the plain one, so the two cannot drift apart.
	hasStorage := func(meta *db.InodeMeta) bool {
		if meta.VolS3Key != "" {
			return true
		}
		has, err := d.HasBlocks(meta.ID)
		if err != nil {
			// Unknown: treat as still staged. Keeping a stale file costs disk;
			// deleting a live one costs the file.
			log.Printf("hamstor: staging cleanup, block lookup for inode %d: %v", meta.ID, err)
			return false
		}
		return has
	}

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
			if hasStorage(meta) {
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
		if hasStorage(meta) {
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
