package hfuse

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
)

// The retained-upload format lives here, both halves of it, so the writer
// (retainPendingUpload) and the reader (readPendingSet, RecoverPending) cannot
// drift apart. A second copy of "what does <db-dir>/pending/ look like" is how
// the two ends of a format stop agreeing, and every disagreement here is silent
// data loss: the bytes under pending/ are the file's only copy.
//
// Layout:
//
//	<db-dir>/pending/<inodeID>/meta      the JSON below
//	<db-dir>/pending/<inodeID>/data      the object bodies, back to back
//	<db-dir>/pending/<inodeID>.tmp-XXX/  a set still being built
//
// The set commits by renaming the temp directory onto <inodeID>, which is one
// atomic rename(2): the directory either exists complete or does not exist. That
// is what the old <inodeID>.<logicalSize> filename got for free by carrying its
// metadata in its name, and it is worth keeping — a step whose whole purpose is
// surviving a crash must not be half-written by one.

const pendingMetaVersion = 1

// pendingMeta describes one retained block set: everything RecoverPending needs
// to redo the flush that failed.
//
// It deliberately records NO S3 key. The blocks a failed flush managed to upload
// before it gave up are not in the blocks table, so they are not in
// AllS3KeySet() either, and GC phase 1 deletes them once gcGracePeriod has
// passed. A daemon that stays down over a weekend comes back to a bucket where
// they are simply gone. "Skip the blocks that are already uploaded" is therefore
// not an optimisation but a way to commit rows pointing at nothing — and this
// format cannot express it, which is the point.
type pendingMeta struct {
	Version int `json:"version"`
	// FileSize is the file's logical length, which is what CommitBlocks records
	// as inodes.size. It is not derivable from the blocks: a sparse set has
	// holes, and the last block may be shorter than its extent.
	FileSize int64          `json:"file_size"`
	Blocks   []pendingBlock `json:"blocks"`
}

// pendingBlock locates one block's object body inside data and says how long the
// file considers that block to be.
//
// Size and Stored differ under encryption exactly as the old format's
// <logicalSize> differed from the retained file's length on disk: the object
// carries a version byte, a nonce and a GCM tag on top of the plaintext.
// Committing Stored as the block's extent makes the file read long.
type pendingBlock struct {
	Index  int64 `json:"index"`  // block index within the file
	Size   int64 `json:"size"`   // plaintext bytes of this block that belong to the file
	Off    int64 `json:"off"`    // offset of the object body within data
	Stored int64 `json:"stored"` // length of that body: Size, or Size plus GCM overhead
}

// pendingSetPath is the one place that names a retained set's directory. Note
// there is no globbing anywhere in this file: the old hasRetainedData matched
// "<id>.*", and the trap in that is not the dot but the prefix — "123*" also
// matches 1234 and 12345, so inode 123 would be kept alive by another inode's
// retained bytes and then never recovered. An exact name has no such family of
// bugs.
func pendingSetPath(pendingDir string, inodeID int64) string {
	return filepath.Join(pendingDir, strconv.FormatInt(inodeID, 10))
}

// inodeHasStorage reports whether the inode's data is reachable from storage
// that outlives a failed flush: a volume needle, or a set of block rows.
//
// It is the question both ends of the retain/recover pair have to ask, which is
// why it is one function. Retention asks it as "is this flush the only copy?"
// and RecoverPending asks it as "did a later write make this set stale?" — and a
// second, differently-worded copy of it is exactly how the two ends of this
// format stop agreeing.
//
// Status is NOT the answer, though it was: a 'committed' inode looks durable and
// need not be. open(O_TRUNC) leaves one committed with no blocks and no needle,
// because go-fuse does not negotiate CAP_ATOMIC_O_TRUNC and the kernel therefore
// truncates through SETATTR before it opens — db.SetAttr drops every block row
// and the caller deletes the objects, all while the status stays 'committed'.
// Every overwrite of an existing file passes through that shape.
//
// Errors answer false: keeping a set that turns out to be redundant costs disk,
// dropping one that was not costs the file.
func inodeHasStorage(d *db.DB, meta *db.InodeMeta) bool {
	if meta.VolS3Key != "" {
		return true
	}
	has, err := d.HasBlocks(meta.ID)
	if err != nil {
		log.Printf("hamstor: storage lookup for inode %d: %v (assuming none)", meta.ID, err)
		return false
	}
	return has
}

// hasRetainedData reports whether pendingDir holds a retained set for inodeID
// that a later start could still upload.
//
// It answers on the directory's existence alone, not on whether the meta inside
// parses. Cleanup uses this to decide whether to delete a pending inode, and the
// conservative answer is the right one there: RecoverPending runs first and is
// the only thing that judges a set, so anything Cleanup can still see either was
// left deliberately (an upload that could not reach S3) or is a leftover
// RecoverPending declined to touch. Deleting the inode under either would throw
// away the bytes' only owner.
func hasRetainedData(pendingDir string, inodeID int64) bool {
	if pendingDir == "" {
		return false
	}
	info, err := os.Stat(pendingSetPath(pendingDir, inodeID))
	return err == nil && info.IsDir()
}

// readPendingSet loads and validates the meta of a retained set. Every bound it
// checks is one that would otherwise be applied to a CommitBlocks call: a bogus
// index or extent does not fail loudly, it commits a row that makes the file
// read wrong.
func readPendingSet(dir string) (*pendingMeta, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "meta"))
	if err != nil {
		return nil, err
	}
	var m pendingMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse meta: %w", err)
	}
	if m.Version != pendingMetaVersion {
		return nil, fmt.Errorf("meta version %d, want %d", m.Version, pendingMetaVersion)
	}
	if m.FileSize < 0 {
		return nil, fmt.Errorf("file_size %d", m.FileSize)
	}
	if len(m.Blocks) == 0 {
		return nil, fmt.Errorf("no blocks")
	}

	info, err := os.Stat(filepath.Join(dir, "data"))
	if err != nil {
		return nil, fmt.Errorf("stat data: %w", err)
	}
	dataLen := info.Size()

	lastLive := int64(-1)
	if m.FileSize > 0 {
		lastLive = (m.FileSize - 1) / db.BlockSize
	}
	seen := make(map[int64]struct{}, len(m.Blocks))
	for _, b := range m.Blocks {
		if b.Index < 0 || b.Index > lastLive {
			return nil, fmt.Errorf("block %d is outside a file of %d bytes", b.Index, m.FileSize)
		}
		if _, dup := seen[b.Index]; dup {
			return nil, fmt.Errorf("block %d listed twice", b.Index)
		}
		seen[b.Index] = struct{}{}
		if b.Size <= 0 || b.Size > db.BlockSize {
			return nil, fmt.Errorf("block %d has extent %d", b.Index, b.Size)
		}
		if b.Stored < b.Size {
			return nil, fmt.Errorf("block %d stores %d bytes for an extent of %d", b.Index, b.Stored, b.Size)
		}
		if b.Off < 0 || b.Off+b.Stored > dataLen {
			return nil, fmt.Errorf("block %d spans [%d,%d) of a %d-byte data file",
				b.Index, b.Off, b.Off+b.Stored, dataLen)
		}
	}
	return &m, nil
}

// retainPendingUpload saves the exact bytes an upload was about to send to S3,
// so a later boot can finish it instead of the data being dropped. Without this
// a single transient S3 error during a copy loses the file outright: `cp` has
// already returned success, the inode stays 'pending', and the next startup's
// Cleanup deletes it — the only trace being one line in the daemon log.
//
// blocks lists the whole set the flush meant to write, with logical extents;
// snap holds the plaintext of all of them at their natural offsets (Flush
// asserts every one of them was materialized). Retention covers the WHOLE set,
// including blocks that had already been uploaded before the failure — see the
// note on pendingMeta for why those cannot be trusted to still be there.
//
// The retained bytes are whatever was destined for the object verbatim, i.e.
// ciphertext under encryption, so recovery uploads them without re-encrypting
// and without needing the passphrase to match this boot's. Preserving that
// costs one more AES pass here, over a snapshot we already hold: the sealed
// bodies of the blocks that did upload were freed one at a time to keep the heap
// bounded, and keeping them all would put the whole file on it.
//
// Without encryption there is nothing to seal, so the snapshot IS the data file
// and moves in by rename — no copy, and it stays sparse. Reports whether the set
// was retained, and whether it took ownership of the snapshot.
func (hfs *HamstorFS) retainPendingUpload(inodeID, fileSize int64, blocks []pendingBlock, snap *os.File, snapPath string) (retained, consumedSnapshot bool) {
	if hfs.PendingDir == "" || len(blocks) == 0 || snap == nil {
		return false, false
	}

	tmpDir, err := os.MkdirTemp(hfs.PendingDir, strconv.FormatInt(inodeID, 10)+".tmp-")
	if err != nil {
		log.Printf("hamstor: retain failed upload for inode %d: %v", inodeID, err)
		return false, false
	}

	placed, consumedSnapshot, err := hfs.writeRetainedData(tmpDir, blocks, snap, snapPath)
	if err == nil {
		err = writeRetainedMeta(tmpDir, fileSize, placed)
	}
	if err == nil {
		// One rename, and the set exists. Until it lands, RecoverPending sees a
		// name that is not a bare inode number and leaves it alone.
		err = os.Rename(tmpDir, pendingSetPath(hfs.PendingDir, inodeID))
	}
	if err != nil {
		// Do NOT remove tmpDir: without encryption its data file is the renamed
		// snapshot, i.e. the only copy of the bytes. The caller names it in the
		// log so a human can still get at it.
		log.Printf("hamstor: retain failed upload for inode %d: %v (partial set left at %s)", inodeID, err, tmpDir)
		return false, consumedSnapshot
	}
	return true, consumedSnapshot
}

// writeRetainedData produces dir/data and fills in each block's Off/Stored.
func (hfs *HamstorFS) writeRetainedData(dir string, blocks []pendingBlock, snap *os.File, snapPath string) ([]pendingBlock, bool, error) {
	dst := filepath.Join(dir, "data")

	if hfs.Encryptor == nil {
		// The snapshot is already exactly what the objects were going to be, at
		// exactly the offsets a block index implies. Move it rather than copy it:
		// spill/ and pending/ are both under the DB directory, so this is a
		// rename within one filesystem, and the file stays sparse.
		if snapPath != "" {
			if err := os.Rename(snapPath, dst); err == nil {
				out := make([]pendingBlock, len(blocks))
				for i, b := range blocks {
					b.Off = b.Index * db.BlockSize
					b.Stored = b.Size
					out[i] = b
				}
				return out, true, nil
			}
			// A --spill-dir on another filesystem makes rename fail with EXDEV;
			// fall through and copy the extents instead of losing the data.
		}
		return copyRetainedData(dst, blocks, snap, nil)
	}
	return copyRetainedData(dst, blocks, snap, hfs.Encryptor)
}

// copyRetainedData writes each block's object body into dst back to back,
// sealing it first when enc is non-nil. One block at a time: this runs while the
// upload semaphore is still held, and 32 concurrent failures each buffering a
// whole file would dwarf the process memory limit.
func copyRetainedData(dst string, blocks []pendingBlock, snap *os.File, enc *crypto.Encryptor) ([]pendingBlock, bool, error) {
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
	}()

	out := make([]pendingBlock, 0, len(blocks))
	off := int64(0)
	for _, b := range blocks {
		start := b.Index * db.BlockSize
		var n int64
		if enc == nil {
			written, cerr := io.Copy(f, io.NewSectionReader(snap, start, b.Size))
			if cerr != nil {
				return nil, false, fmt.Errorf("copy block %d: %w", b.Index, cerr)
			}
			if written != b.Size {
				return nil, false, fmt.Errorf("copy block %d: wrote %d of %d bytes", b.Index, written, b.Size)
			}
			n = written
		} else {
			plain := make([]byte, b.Size)
			if _, rerr := snap.ReadAt(plain, start); rerr != nil && rerr != io.EOF {
				return nil, false, fmt.Errorf("read block %d: %w", b.Index, rerr)
			}
			body, eerr := enc.Encrypt(plain)
			plain = nil
			if eerr != nil {
				return nil, false, fmt.Errorf("encrypt block %d: %w", b.Index, eerr)
			}
			if _, werr := f.Write(body); werr != nil {
				return nil, false, fmt.Errorf("write block %d: %w", b.Index, werr)
			}
			n = int64(len(body))
		}
		b.Off = off
		b.Stored = n
		out = append(out, b)
		off += n
	}
	if err := f.Sync(); err != nil {
		return nil, false, err
	}
	closed = true
	return out, false, f.Close()
}

// writeRetainedMeta writes dir/meta. It is written after data and before the
// directory is renamed into place, so a set that is visible at all is complete.
func writeRetainedMeta(dir string, fileSize int64, blocks []pendingBlock) error {
	raw, err := json.Marshal(pendingMeta{
		Version:  pendingMetaVersion,
		FileSize: fileSize,
		Blocks:   blocks,
	})
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "meta")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
