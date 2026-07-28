package db

import (
	"database/sql"
	"errors"
	"fmt"
)

// ThumbRecord is one stored thumbnail: an S3 object holding the two rendered
// PNGs back to back, normal first.
type ThumbRecord struct {
	S3Key  string
	Offset int64
	Length int64
	// NormalLen splits the PLAINTEXT body: [0, NormalLen) is the 128px PNG and
	// the rest is the 256px one. Under encryption Length is the sealed size,
	// which is larger — the same reason blocks record a stored size separately
	// from the extent they represent.
	NormalLen  int64
	SrcMtimeNs int64
}

// ThumbTarget is a stored thumbnail together with everything needed to
// materialize it into a freedesktop cache: where the file lives now, and what
// its mtime is now.
type ThumbTarget struct {
	InodeID int64
	Path    string
	MtimeNs int64
	Size    int64
	Rec     ThumbRecord
}

// PutThumbnail records a rendered thumbnail, returning the S3 key it superseded
// (empty if none) for the caller to delete afterwards. The object is deleted
// after the transaction, never before: the reverse order drops live data if the
// commit then fails.
//
// The write is a compare-and-set on the source's mtime. A render job can sit in
// the queue (depth 1024) while the file is overwritten underneath it, and
// without the CAS the thumbnail of the superseded version would be stored
// against the new one — wrong, and permanently so, because it would carry the
// new mtime and therefore satisfy every staleness check that exists. ok=false
// means the caller must delete the object it just uploaded.
func (d *DB) PutThumbnail(inodeID int64, rec ThumbRecord) (ok bool, oldKey string, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback()

	var curMtime int64
	err = tx.QueryRow("SELECT mtime_ns FROM inodes WHERE id = ?", inodeID).Scan(&curMtime)
	if errors.Is(err, sql.ErrNoRows) {
		// The file was deleted while its thumbnail was being rendered.
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("put thumbnail: read mtime: %w", err)
	}
	if curMtime != rec.SrcMtimeNs {
		return false, "", nil
	}

	// Read the superseded key inside the transaction: INSERT OR REPLACE is about
	// to drop the row, and afterwards nothing knows that object's name.
	old, err := thumbKeyTx(tx, inodeID)
	if err != nil {
		return false, "", err
	}

	if _, err := tx.Exec(
		`INSERT OR REPLACE INTO thumbnails
		   (inode_id, s3_key, offset, length, normal_len, src_mtime_ns)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		inodeID, rec.S3Key, rec.Offset, rec.Length, rec.NormalLen, rec.SrcMtimeNs,
	); err != nil {
		return false, "", fmt.Errorf("put thumbnail: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, "", err
	}
	return true, old, nil
}

// GetThumbnail returns the stored thumbnail for one inode.
func (d *DB) GetThumbnail(inodeID int64) (ThumbRecord, bool, error) {
	var rec ThumbRecord
	err := d.db.QueryRow(
		"SELECT s3_key, offset, length, normal_len, src_mtime_ns FROM thumbnails WHERE inode_id = ?",
		inodeID,
	).Scan(&rec.S3Key, &rec.Offset, &rec.Length, &rec.NormalLen, &rec.SrcMtimeNs)
	if errors.Is(err, sql.ErrNoRows) {
		return ThumbRecord{}, false, nil
	}
	if err != nil {
		return ThumbRecord{}, false, fmt.Errorf("get thumbnail %d: %w", inodeID, err)
	}
	return rec, true, nil
}

// DeleteThumbnail drops the row and returns the S3 key to delete.
func (d *DB) DeleteThumbnail(inodeID int64) (string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	key, err := thumbKeyTx(tx, inodeID)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec("DELETE FROM thumbnails WHERE inode_id = ?", inodeID); err != nil {
		return "", err
	}
	return key, tx.Commit()
}

// thumbKeyTx reads an inode's thumbnail key inside an open transaction. Callers
// that delete the inode must take it here: ON DELETE CASCADE drops the row with
// the inode, and after that a bucket listing is the only way to find the object.
func thumbKeyTx(tx *sql.Tx, id int64) (string, error) {
	var key string
	err := tx.QueryRow("SELECT s3_key FROM thumbnails WHERE inode_id = ?", id).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("thumbnail key for inode %d: %w", id, err)
	}
	return key, nil
}

// AllThumbnailTargets returns every stored thumbnail with its file's current
// path and mtime, for materialization into a freedesktop cache.
//
// The path is built by a recursive CTE rather than by calling InodePath per row.
// InodePath walks to the root one query per level, which for a photo library is
// tens of thousands of queries times the directory depth — the difference
// between a pass that finishes and one nobody waits for.
func (d *DB) AllThumbnailTargets() ([]ThumbTarget, error) {
	rows, err := d.db.Query(`
		WITH RECURSIVE paths(id, path) AS (
			SELECT id, '' FROM inodes WHERE id = 1
			UNION ALL
			SELECT i.id, CASE WHEN p.path = '' THEN i.name ELSE p.path || '/' || i.name END
			  FROM inodes i JOIN paths p ON i.parent_id = p.id
		)
		SELECT t.inode_id, p.path, i.mtime_ns, i.size,
		       t.s3_key, t.offset, t.length, t.normal_len, t.src_mtime_ns
		  FROM thumbnails t
		  JOIN inodes i ON i.id = t.inode_id
		  JOIN paths  p ON p.id = t.inode_id`)
	if err != nil {
		return nil, fmt.Errorf("all thumbnail targets: %w", err)
	}
	defer rows.Close()

	var out []ThumbTarget
	for rows.Next() {
		var tt ThumbTarget
		if err := rows.Scan(&tt.InodeID, &tt.Path, &tt.MtimeNs, &tt.Size,
			&tt.Rec.S3Key, &tt.Rec.Offset, &tt.Rec.Length, &tt.Rec.NormalLen, &tt.Rec.SrcMtimeNs); err != nil {
			return nil, err
		}
		out = append(out, tt)
	}
	return out, rows.Err()
}

// ThumbnaillessImages returns committed regular files that have no stored
// thumbnail, for backfill to render. Extension filtering is left to the caller
// (internal/thumb owns the list); this only skips directories, symlinks and
// files with no data.
func (d *DB) ThumbnaillessImages() ([]ThumbTarget, error) {
	rows, err := d.db.Query(`
		WITH RECURSIVE paths(id, path) AS (
			SELECT id, '' FROM inodes WHERE id = 1
			UNION ALL
			SELECT i.id, CASE WHEN p.path = '' THEN i.name ELSE p.path || '/' || i.name END
			  FROM inodes i JOIN paths p ON i.parent_id = p.id
		)
		SELECT i.id, p.path, i.mtime_ns, i.size
		  FROM inodes i
		  JOIN paths p ON p.id = i.id
		 WHERE i.status = 'committed'
		   AND i.symlink_target IS NULL
		   AND (i.mode & 61440) = 32768
		   AND i.size > 0
		   AND NOT EXISTS (SELECT 1 FROM thumbnails t WHERE t.inode_id = i.id)`)
	if err != nil {
		return nil, fmt.Errorf("thumbnailless images: %w", err)
	}
	defer rows.Close()

	var out []ThumbTarget
	for rows.Next() {
		var tt ThumbTarget
		if err := rows.Scan(&tt.InodeID, &tt.Path, &tt.MtimeNs, &tt.Size); err != nil {
			return nil, err
		}
		out = append(out, tt)
	}
	return out, rows.Err()
}
