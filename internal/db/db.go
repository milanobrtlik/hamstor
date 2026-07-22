package db

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// isBusy reports whether err is a transient SQLite lock contention error.
func isBusy(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SQLITE_BUSY") || strings.Contains(s, "database is locked")
}

const schema = `
CREATE TABLE IF NOT EXISTS inodes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id       INTEGER NOT NULL,
    name            TEXT NOT NULL,
    mode            INTEGER NOT NULL,
    size            INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'committed',
    mtime_ns        INTEGER NOT NULL,
    ctime_ns        INTEGER NOT NULL,
    uid             INTEGER NOT NULL DEFAULT 0,
    gid             INTEGER NOT NULL DEFAULT 0,
    symlink_target  TEXT,
    UNIQUE(parent_id, name)
);
CREATE INDEX IF NOT EXISTS idx_inodes_parent_status ON inodes(parent_id, status);
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS xattrs (
    inode_id INTEGER NOT NULL,
    name     TEXT NOT NULL,
    value    BLOB NOT NULL,
    PRIMARY KEY (inode_id, name)
);
`

// Migrations for databases created before these columns existed.
var migrations = []string{
	"ALTER TABLE inodes ADD COLUMN uid INTEGER NOT NULL DEFAULT 0",
	"ALTER TABLE inodes ADD COLUMN gid INTEGER NOT NULL DEFAULT 0",
	"ALTER TABLE inodes ADD COLUMN symlink_target TEXT",
	`CREATE TABLE IF NOT EXISTS xattrs (
		inode_id INTEGER NOT NULL,
		name     TEXT NOT NULL,
		value    BLOB NOT NULL,
		PRIMARY KEY (inode_id, name)
	)`,
	"ALTER TABLE inodes ADD COLUMN vol_s3_key TEXT",
	"ALTER TABLE inodes ADD COLUMN vol_offset INTEGER",
	"ALTER TABLE inodes ADD COLUMN vol_size INTEGER",
}

const inodeCols = "id, parent_id, name, mode, size, status, mtime_ns, ctime_ns, uid, gid, symlink_target, vol_s3_key, vol_offset, vol_size"

// InodeMeta has no whole-file S3 key. A file's data lives in the blocks table
// (large files), in a volume needle (vol_s3_key), or in the staging directory
// until the builder packs it — never in an object named by the inode row.
type InodeMeta struct {
	ID            int64
	ParentID      int64
	Name          string
	Mode          uint32
	Size          int64
	Status        string
	MtimeNs       int64
	CtimeNs       int64
	Uid           uint32
	Gid           uint32
	SymlinkTarget string
	VolS3Key      string
	VolOffset     int64
	VolSize       int64
}

// BlockSize is the step of the block layout for large files: block b covers the
// plaintext range [b*BlockSize, min((b+1)*BlockSize, inodes.size)). Blocks are
// not padded — the last block of a 300 KB file is a 300 KB object.
//
// It lives here because CommitBlocks is given only the new file size and has to
// derive the last live index itself. s3store.UploadPartSize is deliberately
// twice this value, so one block is always one PutObject.
// See claudedocs/block-layout-design.md, D1.
const BlockSize = 8 << 20

// BlockCommit is one row of the blocks table, both when writing and when
// reading it back.
type BlockCommit struct {
	Index int64
	S3Key string
	// Size is how many of this block's plaintext bytes belong to the file.
	// Everything after it reads as zeroes, and the stored object may well be
	// longer: a truncate that cuts into a block does not rewrite it, it only
	// shortens this number, so the bytes past it are dead until the block is
	// next written. Reads must clamp to it — without that, growing a truncated
	// file back serves the old tail where POSIX requires zeroes.
	//
	// It is NOT a component of the file's length. Deriving one from SUM(size) is
	// a new version of an old bug: inodes.size (and at runtime inodeWrite.size)
	// is the only authority on how long a file is.
	Size int64
}

// VolumeRecord holds metadata for a volume S3 object.
type VolumeRecord struct {
	S3Key       string
	TotalSize   int64
	LiveSize    int64
	NeedleCount int
	LiveCount   int
	Status      string
	CreatedNs   int64
}

type DB struct {
	db *sql.DB
}

func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)", path)
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	sqldb.SetMaxOpenConns(8)

	if _, err := sqldb.Exec(schema); err != nil {
		sqldb.Close()
		return nil, fmt.Errorf("db schema: %w", err)
	}

	// Run migrations (ignore "duplicate column" and "already exists" errors)
	for _, m := range migrations {
		if _, err := sqldb.Exec(m); err != nil {
			errStr := err.Error()
			if !strings.Contains(errStr, "duplicate column") && !strings.Contains(errStr, "already exists") {
				sqldb.Close()
				return nil, fmt.Errorf("db migration %q: %w", m[:min(len(m), 50)], err)
			}
		}
	}

	d := &DB{db: sqldb}

	// Versioned migrations (tracked in config table to run only once)
	if err := d.runVersionedMigrations(); err != nil {
		sqldb.Close()
		return nil, err
	}

	if err := d.seedRoot(); err != nil {
		sqldb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) runVersionedMigrations() error {
	type versionedMigration struct {
		key string
		fn  func() error
	}

	migrations := []versionedMigration{
		{
			key: "volumes_table_v1",
			fn: func() error {
				_, err := d.db.Exec(`CREATE TABLE IF NOT EXISTS volumes (
					s3_key       TEXT PRIMARY KEY,
					total_size   INTEGER NOT NULL,
					live_size    INTEGER NOT NULL,
					needle_count INTEGER NOT NULL,
					live_count   INTEGER NOT NULL,
					status       TEXT NOT NULL DEFAULT 'open',
					created_ns   INTEGER NOT NULL
				)`)
				return err
			},
		},
		{
			key: "xattrs_fk_v1",
			fn: func() error {
				tx, err := d.db.Begin()
				if err != nil {
					return err
				}
				defer tx.Rollback()
				stmts := []string{
					`CREATE TABLE xattrs_new (
						inode_id INTEGER NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
						name     TEXT NOT NULL,
						value    BLOB NOT NULL,
						PRIMARY KEY (inode_id, name)
					)`,
					"INSERT OR IGNORE INTO xattrs_new SELECT * FROM xattrs",
					"DROP TABLE xattrs",
					"ALTER TABLE xattrs_new RENAME TO xattrs",
				}
				for _, s := range stmts {
					if _, err := tx.Exec(s); err != nil {
						return fmt.Errorf("migration xattrs_fk: %w", err)
					}
				}
				return tx.Commit()
			},
		},
		{
			key: "idx_vol_s3_key_v1",
			fn: func() error {
				_, err := d.db.Exec("CREATE INDEX IF NOT EXISTS idx_inodes_vol_s3_key ON inodes(vol_s3_key)")
				return err
			},
		},
		{
			// One row per block of a large file. WITHOUT ROWID makes the
			// primary key the table itself: no second B-tree and no implicit
			// rowid, so a (inode, block_index) lookup is one probe and the
			// write footprint is half that of a rowid table with a UNIQUE
			// index. Deliberately no index on s3_key — nobody needs the
			// reverse lookup (GC and fsck both scan the whole set), and it
			// would cost a write on every block commit.
			// See claudedocs/block-layout-design.md, D2 and section 2.
			key: "blocks_table_v1",
			fn: func() error {
				_, err := d.db.Exec(`CREATE TABLE IF NOT EXISTS blocks (
					inode_id     INTEGER NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
					block_index  INTEGER NOT NULL,
					s3_key       TEXT    NOT NULL,
					size         INTEGER NOT NULL,
					PRIMARY KEY (inode_id, block_index)
				) WITHOUT ROWID`)
				return err
			},
		},
	}

	for _, m := range migrations {
		// Check if already applied
		var applied int
		err := d.db.QueryRow("SELECT COUNT(*) FROM config WHERE key = ?", "migration_"+m.key).Scan(&applied)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", m.key, err)
		}
		if applied > 0 {
			continue
		}
		if err := m.fn(); err != nil {
			return fmt.Errorf("run migration %s: %w", m.key, err)
		}
		if _, err := d.db.Exec("INSERT INTO config (key, value) VALUES (?, ?)", "migration_"+m.key, "done"); err != nil {
			return fmt.Errorf("record migration %s: %w", m.key, err)
		}
	}
	return nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) seedRoot() error {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM inodes WHERE id = 1").Scan(&count)
	if err != nil {
		return fmt.Errorf("db seed check: %w", err)
	}
	if count > 0 {
		return nil
	}
	now := time.Now().UnixNano()
	mode := uint32(syscall.S_IFDIR | 0o755)
	_, err = d.db.Exec(
		"INSERT INTO inodes (id, parent_id, name, mode, size, status, mtime_ns, ctime_ns, uid, gid) VALUES (1, 0, '', ?, 0, 'committed', ?, ?, 0, 0)",
		mode, now, now,
	)
	if err != nil {
		return fmt.Errorf("db seed root: %w", err)
	}
	return nil
}

// scanInode scans a single row into InodeMeta.
func scanInode(scanner interface{ Scan(...any) error }) (*InodeMeta, error) {
	m := &InodeMeta{}
	var symlinkTarget sql.NullString
	var volS3Key sql.NullString
	var volOffset sql.NullInt64
	var volSize sql.NullInt64
	err := scanner.Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size,
		&m.Status, &m.MtimeNs, &m.CtimeNs, &m.Uid, &m.Gid, &symlinkTarget,
		&volS3Key, &volOffset, &volSize)
	if err != nil {
		return nil, err
	}
	m.SymlinkTarget = symlinkTarget.String
	m.VolS3Key = volS3Key.String
	m.VolOffset = volOffset.Int64
	m.VolSize = volSize.Int64
	return m, nil
}

// scanInodeRows scans multiple rows into a slice of InodeMeta.
func scanInodeRows(rows *sql.Rows) ([]InodeMeta, error) {
	defer rows.Close()
	var result []InodeMeta
	for rows.Next() {
		m, err := scanInode(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *m)
	}
	return result, rows.Err()
}

func (d *DB) GetInode(id int64) (*InodeMeta, error) {
	return scanInode(d.db.QueryRow("SELECT "+inodeCols+" FROM inodes WHERE id = ?", id))
}

func (d *DB) LookupChild(parentID int64, name string) (*InodeMeta, error) {
	return scanInode(d.db.QueryRow(
		"SELECT "+inodeCols+" FROM inodes WHERE parent_id = ? AND name = ? AND status = 'committed'",
		parentID, name,
	))
}

func (d *DB) ListChildren(parentID int64) ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT "+inodeCols+" FROM inodes WHERE parent_id = ? AND status = 'committed'",
		parentID,
	)
	if err != nil {
		return nil, err
	}
	return scanInodeRows(rows)
}

func (d *DB) ListAllChildren(parentID int64) ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT "+inodeCols+" FROM inodes WHERE parent_id = ?",
		parentID,
	)
	if err != nil {
		return nil, err
	}
	return scanInodeRows(rows)
}

func (d *DB) GetOrphanedInodes() ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT " + inodeCols + " FROM inodes WHERE parent_id > 0 AND parent_id NOT IN (SELECT id FROM inodes)",
	)
	if err != nil {
		return nil, err
	}
	return scanInodeRows(rows)
}

func (d *DB) InsertInode(parentID int64, name string, mode uint32, status string) (int64, error) {
	now := time.Now().UnixNano()
	res, err := d.db.Exec(
		"INSERT INTO inodes (parent_id, name, mode, size, status, mtime_ns, ctime_ns, uid, gid) VALUES (?, ?, ?, 0, ?, ?, ?, 0, 0)",
		parentID, name, mode, status, now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// InsertInodeWithOwner creates a new inode with specific uid/gid.
func (d *DB) InsertInodeWithOwner(parentID int64, name string, mode uint32, status string, uid, gid uint32) (int64, error) {
	now := time.Now().UnixNano()
	res, err := d.db.Exec(
		"INSERT INTO inodes (parent_id, name, mode, size, status, mtime_ns, ctime_ns, uid, gid) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
		parentID, name, mode, status, now, now, uid, gid,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// InsertSymlink creates a symlink inode.
func (d *DB) InsertSymlink(parentID int64, name string, target string, uid, gid uint32) (int64, error) {
	now := time.Now().UnixNano()
	mode := uint32(syscall.S_IFLNK | 0o777)
	res, err := d.db.Exec(
		"INSERT INTO inodes (parent_id, name, mode, size, status, mtime_ns, ctime_ns, uid, gid, symlink_target) VALUES (?, ?, ?, ?, 'committed', ?, ?, ?, ?, ?)",
		parentID, name, mode, len(target), now, now, uid, gid, target,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CommitInode marks an inode committed at the given size, with no object of its
// own: its data is either in the staging directory waiting for the volume
// builder, or there is none because the file is empty. Large files commit
// through CommitBlocks instead — this is the shape that owns no S3 key at all.
//
// If the inode currently references a volume needle, that volume's
// live_size/live_count are decremented in the SAME transaction so a crash can
// never leave a volume's live_count dropped while the inode still references it
// (which GetEmptyVolumes + GC would otherwise treat as deletable, destroying
// still-referenced data). The volume reference is re-read inside the transaction
// rather than trusted from a caller snapshot, so it stays correct even if the
// volume builder packed the inode during an async upload.
// Returns whether the inode still existed (RowsAffected > 0).
func (d *DB) CommitInode(id int64, size int64) (bool, error) {
	now := time.Now().UnixNano()
	var committed bool
	var err error
	for attempt := range 5 {
		committed, err = d.commitInodeTx(id, size, now)
		if err == nil {
			return committed, nil
		}
		if !isBusy(err) {
			return false, err
		}
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	return false, err
}

func (d *DB) commitInodeTx(id int64, size, now int64) (bool, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Decrement whichever volume currently owns this needle (read inside the tx).
	var curVolKey sql.NullString
	var volSize int64
	err = tx.QueryRow("SELECT vol_s3_key, COALESCE(vol_size, 0) FROM inodes WHERE id = ?", id).Scan(&curVolKey, &volSize)
	if errors.Is(err, sql.ErrNoRows) {
		// Inode was deleted (e.g. unlinked during an async upload). Nothing to commit.
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if curVolKey.Valid && curVolKey.String != "" {
		if _, err := tx.Exec(
			"UPDATE volumes SET live_size = MAX(live_size - ?, 0), live_count = MAX(live_count - 1, 0) WHERE s3_key = ?",
			volSize, curVolKey.String,
		); err != nil {
			return false, err
		}
	}

	res, err := tx.Exec(
		"UPDATE inodes SET size = ?, status = 'committed', mtime_ns = ?, vol_s3_key = NULL, vol_offset = NULL, vol_size = NULL WHERE id = ?",
		size, now, id,
	)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// MarkPending puts an inode back to 'pending'. Returns whether it still existed.
//
// The one caller is retention (hfuse: flushAsync): an inode whose only surviving
// copy is a retained set under pending/ has no durable storage, which is what
// 'pending' means. It is reached when a rewrite fails after open(O_TRUNC) has
// already deleted the previous version — the inode is 'committed' and empty at
// that point, and leaving it that way has two costs. RecoverPending drops a
// committed inode's set as stale, and a 0-byte file the user can see is one they
// may overwrite or delete before the next start recovers it. Pending inodes are
// invisible to LookupChild, so neither can happen.
func (d *DB) MarkPending(id int64) (bool, error) {
	res, err := d.db.Exec("UPDATE inodes SET status = 'pending' WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// DeleteInode removes the inode and returns the S3 keys of the blocks it owned,
// for the caller to delete AFTER this returns.
//
// Returning them is the whole point: the block rows go away with the inode
// through ON DELETE CASCADE, so this transaction is the last moment anyone knows
// those keys. A caller that ignores the return value leaks every object of every
// large file it deletes — but it is a build error to ignore it, which is why the
// keys come back rather than being left for the caller to fetch beforehand.
func (d *DB) DeleteInode(id int64) ([]string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	keys, err := blockKeysTx(tx, id)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec("DELETE FROM xattrs WHERE inode_id = ?", id); err != nil {
		return nil, err
	}
	if _, err := tx.Exec("DELETE FROM inodes WHERE id = ?", id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

// blockKeysTx reads an inode's block keys inside an open transaction. Callers
// that delete the inode must take them here, before the cascade drops the rows.
func blockKeysTx(tx *sql.Tx, id int64) ([]string, error) {
	rows, err := tx.Query("SELECT s3_key FROM blocks WHERE inode_id = ? ORDER BY block_index", id)
	if err != nil {
		return nil, fmt.Errorf("block keys for inode %d: %w", id, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (d *DB) RenameInode(id int64, newParentID int64, newName string) error {
	now := time.Now().UnixNano()
	_, err := d.db.Exec(
		"UPDATE inodes SET parent_id = ?, name = ?, ctime_ns = ? WHERE id = ?",
		newParentID, newName, now, id,
	)
	return err
}

// GetStagedInodes returns committed files whose data is not in S3 yet, i.e.
// those that depend on a local staging file. Callers pair this with the staging
// directory to tell "waiting to be packed" apart from "data is gone".
func (d *DB) GetStagedInodes() ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT "+inodeCols+" FROM inodes WHERE status = 'committed'"+
			" AND (vol_s3_key IS NULL OR vol_s3_key = '')"+
			noBlockRows+
			" AND mode & ? = ? AND id > 1 AND size > 0",
		uint32(syscall.S_IFMT), uint32(syscall.S_IFREG),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInodeRows(rows)
}

func (d *DB) GetPending() ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT " + inodeCols + " FROM inodes WHERE status = 'pending'",
	)
	if err != nil {
		return nil, err
	}
	return scanInodeRows(rows)
}

func (d *DB) InodePath(id int64) (string, error) {
	const maxDepth = 1000
	var parts []string
	current := id
	for current > 1 {
		if len(parts) >= maxDepth {
			return "", fmt.Errorf("inode path: depth limit exceeded (cycle?)")
		}
		m, err := d.GetInode(current)
		if err != nil {
			return "", err
		}
		parts = append(parts, m.Name)
		current = m.ParentID
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "/"), nil
}

// AllS3KeySet returns every S3 key the database still references. GC deletes
// every object in the bucket that is NOT in this set (ops/gc.go), so a storage
// shape missing from here is not a leak but a delete: the next `hamstor gc`
// removes live data in one batch, silently. Any new place that stores an S3 key
// belongs in this union on the same commit that introduces it.
//
// Two shapes, since inodes stopped naming objects of their own: volume objects
// (many needles each) and the blocks of large files (one object per block).
func (d *DB) AllS3KeySet() (map[string]struct{}, error) {
	set := make(map[string]struct{})

	for _, q := range []string{
		"SELECT s3_key FROM volumes",
		"SELECT s3_key FROM blocks",
	} {
		rows, err := d.db.Query(q)
		if err != nil {
			return nil, fmt.Errorf("all s3 keys (%s): %w", q, err)
		}
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return nil, err
			}
			set[key] = struct{}{}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return set, nil
}

// --- Volume packing ---

// InsertVolume creates a new volume record.
func (d *DB) InsertVolume(s3Key string, totalSize int64, liveSize int64, needleCount int, liveCount int, status string) error {
	now := time.Now().UnixNano()
	_, err := d.db.Exec(
		"INSERT INTO volumes (s3_key, total_size, live_size, needle_count, live_count, status, created_ns) VALUES (?, ?, ?, ?, ?, ?, ?)",
		s3Key, totalSize, liveSize, needleCount, liveCount, status, now,
	)
	return err
}

// SetVolumeStatus updates the status of a volume.
func (d *DB) SetVolumeStatus(s3Key string, status string) error {
	_, err := d.db.Exec("UPDATE volumes SET status = ? WHERE s3_key = ?", status, s3Key)
	return err
}

// GetOpenVolumes returns all volumes with status 'open'.
func (d *DB) GetOpenVolumes() ([]VolumeRecord, error) {
	return d.queryVolumes("SELECT s3_key, total_size, live_size, needle_count, live_count, status, created_ns FROM volumes WHERE status = 'open'")
}

// GetCompactingVolumes returns all volumes with status 'compacting' (stuck from a crash).
func (d *DB) GetCompactingVolumes() ([]VolumeRecord, error) {
	return d.queryVolumes("SELECT s3_key, total_size, live_size, needle_count, live_count, status, created_ns FROM volumes WHERE status = 'compacting'")
}

// GetEmptyVolumes returns sealed volumes with no live needles that are older
// than the grace period. The age check provides defense in depth against
// premature deletion if live_count is momentarily incorrect.
func (d *DB) GetEmptyVolumes(graceNs int64) ([]VolumeRecord, error) {
	cutoff := time.Now().UnixNano() - graceNs
	return d.queryVolumes(
		"SELECT s3_key, total_size, live_size, needle_count, live_count, status, created_ns FROM volumes WHERE status = 'sealed' AND live_count = 0 AND created_ns < ?",
		cutoff,
	)
}

// GetVolumesForCompaction returns sealed volumes where the dead space ratio exceeds the threshold.
func (d *DB) GetVolumesForCompaction(deadRatio float64) ([]VolumeRecord, error) {
	return d.queryVolumes(
		"SELECT s3_key, total_size, live_size, needle_count, live_count, status, created_ns FROM volumes WHERE status = 'sealed' AND total_size > 0 AND CAST(total_size - live_size AS REAL) / total_size > ?",
		deadRatio,
	)
}

func (d *DB) queryVolumes(query string, args ...any) ([]VolumeRecord, error) {
	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []VolumeRecord
	for rows.Next() {
		var v VolumeRecord
		if err := rows.Scan(&v.S3Key, &v.TotalSize, &v.LiveSize, &v.NeedleCount, &v.LiveCount, &v.Status, &v.CreatedNs); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// NeedlesInVolume returns all committed inodes stored in the given volume.
func (d *DB) NeedlesInVolume(volS3Key string) ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT "+inodeCols+" FROM inodes WHERE vol_s3_key = ? AND status = 'committed'",
		volS3Key,
	)
	if err != nil {
		return nil, err
	}
	return scanInodeRows(rows)
}

// DeleteInodeWithVolume atomically decrements volume stats and deletes the inode
// in a single transaction. The volume reference and needle size are re-read from
// the inode row INSIDE the transaction rather than trusted from the caller's
// snapshot: a concurrent overwrite (CommitInode) may have already moved the
// needle off this volume and decremented its stats, in which case the row now
// has a NULL vol_s3_key and we must not decrement a second time. The volS3Key
// parameter is retained for source compatibility but is intentionally ignored.
//
// Like DeleteInode it returns the inode's block keys for the caller to delete
// afterwards; the volume object is never among them, since it is shared with
// other needles and only GC may remove it.
func (d *DB) DeleteInodeWithVolume(id int64, volS3Key string) ([]string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var curVolKey sql.NullString
	var volSize int64
	err = tx.QueryRow("SELECT vol_s3_key, COALESCE(vol_size, 0) FROM inodes WHERE id = ?", id).Scan(&curVolKey, &volSize)
	if errors.Is(err, sql.ErrNoRows) {
		// Inode already gone — nothing to delete or decrement.
		return nil, tx.Commit()
	}
	if err != nil {
		return nil, fmt.Errorf("delete inode with volume: get vol ref: %w", err)
	}
	if curVolKey.Valid && curVolKey.String != "" {
		if _, err = tx.Exec(
			"UPDATE volumes SET live_size = MAX(live_size - ?, 0), live_count = MAX(live_count - 1, 0) WHERE s3_key = ?",
			volSize, curVolKey.String,
		); err != nil {
			return nil, fmt.Errorf("delete inode with volume: update volume: %w", err)
		}
	}

	keys, err := blockKeysTx(tx, id)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec("DELETE FROM xattrs WHERE inode_id = ?", id); err != nil {
		return nil, err
	}
	if _, err := tx.Exec("DELETE FROM inodes WHERE id = ?", id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

// DeleteVolume removes a volume record from the database.
func (d *DB) DeleteVolume(s3Key string) error {
	_, err := d.db.Exec("DELETE FROM volumes WHERE s3_key = ?", s3Key)
	return err
}

// CommitNeedlesToVolume atomically commits multiple inodes to a volume and seals it.
// When onlyUnpacked is true, only updates inodes that have no existing S3 key or
// volume reference (used by the builder to avoid overwriting superseded data).
// onlyUnpacked and expectedVolKey are mutually exclusive — using both produces
// a contradictory query that matches zero rows.
// Returns the list of inode IDs that were actually committed.
func (d *DB) CommitNeedlesToVolume(volS3Key string, totalSize int64, needles []NeedleCommit, onlyUnpacked bool, expectedVolKey string) ([]int64, error) {
	if onlyUnpacked && expectedVolKey != "" {
		return nil, fmt.Errorf("CommitNeedlesToVolume: onlyUnpacked and expectedVolKey are mutually exclusive")
	}

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	query := "UPDATE inodes SET vol_s3_key = ?, vol_offset = ?, vol_size = ? WHERE id = ? AND status = 'committed'"
	if onlyUnpacked {
		query += " AND (vol_s3_key IS NULL OR vol_s3_key = '')" + noBlockRows
	}

	liveSize := int64(0)
	committed := 0
	var committedIDs []int64

	for _, n := range needles {
		q := query
		args := []any{volS3Key, n.Offset, n.Size, n.InodeID}
		if expectedVolKey != "" {
			q += " AND vol_s3_key = ?"
			args = append(args, expectedVolKey)
		}
		if n.MtimeNs > 0 {
			q += " AND mtime_ns = ?"
			args = append(args, n.MtimeNs)
		}
		var res sql.Result
		res, err = tx.Exec(q, args...)
		if err != nil {
			return nil, fmt.Errorf("commit needle inode %d: %w", n.InodeID, err)
		}
		rows, _ := res.RowsAffected()
		if rows > 0 {
			liveSize += n.Size
			committed++
			committedIDs = append(committedIDs, n.InodeID)
		}
	}

	// needle_count = physical needles written into the object (matches total_size,
	// which counts all bytes including born-dead ones); live_count = still-live.
	_, err = tx.Exec(
		"UPDATE volumes SET status = 'sealed', total_size = ?, live_size = ?, needle_count = ?, live_count = ? WHERE s3_key = ?",
		totalSize, liveSize, len(needles), committed, volS3Key,
	)
	if err != nil {
		return nil, fmt.Errorf("seal volume: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return committedIDs, nil
}

// NeedleCommit holds the data needed to commit one needle to a volume.
type NeedleCommit struct {
	InodeID int64
	Offset  int64
	Size    int64 // encrypted (on-disk) size of the needle
	MtimeNs int64 // expected mtime_ns at claim time; 0 means skip check
}

// --- Blocks ---

// MaxNeedleSize is the largest file the volume builder will pack, and therefore
// the largest a staging file can ever be. volume.MaxNeedleSize aliases it; it
// lives here because the SQL below has to know it and volume already imports db,
// so the dependency cannot run the other way.
const MaxNeedleSize = 256 << 10 // 256 KB

// noBlockRows narrows "committed but has no data in S3" to the inodes that could
// really be sitting in the staging directory.
//
// Two things disqualify an inode, and both were learned the hard way. Data in
// blocks disqualifies it: establishing "no data" from vol_s3_key alone fires on
// every healthy large file, so GetStagedInodes/Fsck would report them unreadable
// at every boot and CommitNeedlesToVolume's onlyUnpacked clause would let the
// builder pack a forgotten staging file over an inode whose real data is in
// blocks.
//
// Being larger than MaxNeedleSize disqualifies it too, and that clause is what
// makes sparse files first-class. flushStaged is only reached below that size,
// so a bigger file has never had a staging file — if it has no needle and no
// blocks either, it is not missing its data, it is all holes. `truncate -s 4G`
// and `dd seek=` produce exactly that, and without this every boot warned
// "unreadable: sparse.bin (4294967296 bytes)" about a perfectly good file. That
// warning matters: it is the signal for a DB restored onto a machine without its
// staging disk, and a predicate that cries wolf on ordinary sparse files kills it.
var noBlockRows = " AND NOT EXISTS (SELECT 1 FROM blocks WHERE blocks.inode_id = inodes.id)" +
	fmt.Sprintf(" AND inodes.size <= %d", MaxNeedleSize)

// clampLastBlockSize records that a shrink cut INTO the last surviving block,
// rather than cleanly between two of them.
//
// Dropping the blocks past the new end is only half of what a shrink has to do,
// and the design only ever named that half. The other half is the block the new
// end falls inside: its object is not rewritten (deliberately — that would cost
// an upload per truncate), so it still holds the bytes past the cut. Reads stay
// correct while the file is short because they clamp to inodes.size, which is
// exactly why this hides. Grow the file again and those bytes are back inside
// the size, and the old tail is served where POSIX requires zeroes.
//
// This is the case that was actually measured on the pre-block layout
// (1048560 bytes resurrected on a 1 MiB file), and dropping whole blocks does
// nothing for it: a 1 MiB file truncated to 16 bytes has exactly one block row,
// so there is nothing past the end to delete.
//
// The fix is to shorten what the row claims is live. size then means "how many
// of this block's plaintext bytes belong to the file"; the object may be longer,
// and those extra bytes are dead until the block is next rewritten. Callers of
// this must be exactly the two places a file's length changes: CommitBlocks and
// SetAttr.
func clampLastBlockSize(tx *sql.Tx, id, size, lastLive int64) error {
	if lastLive < 0 {
		return nil
	}
	live := size - lastLive*BlockSize
	if _, err := tx.Exec(
		"UPDATE blocks SET size = ? WHERE inode_id = ? AND block_index = ? AND size > ?",
		live, id, lastLive, live,
	); err != nil {
		return fmt.Errorf("clamp last block of inode %d to %d: %w", id, live, err)
	}
	return nil
}

// CommitBlocks atomically replaces part of an inode's block set and marks the
// inode committed at the given size. It is the N-key generalization of
// CommitInode and follows the same rules (block-layout-design.md, D5):
//
//   - Every upload must already have succeeded when this is called. A committed
//     row whose object does not exist is an unreadable file with no error path:
//     the GET fails whenever someone reads it, which may be months later.
//   - The keys being replaced are read INSIDE the transaction, never from a
//     snapshot the caller took earlier. A snapshot is how a losing flush deletes
//     the object a winning flush just committed.
//   - The volume the inode used to reference is decremented in this same
//     transaction. Needle -> blocks happens every time a small file grows past
//     MaxNeedleSize, so skipping it leaves the volume inflated by a dead needle
//     nobody will ever subtract.
//
// blocks holds only the blocks that changed; any block not listed and still
// within the new size keeps its current key. Blocks past the new end of file
// are deleted.
//
// Returned orphaned keys are exactly the objects nothing references any more:
// replaced blocks and blocks cut off by the new size. The caller deletes them
// AFTER this returns — never before, since a
// crash in between only leaves orphans for GC, while the reverse order deletes
// live data. The volume object is never in that list: it is shared with other
// needles and only GC phase 3 may remove it.
//
// committed is false when the inode is gone (unlinked during an upload); the
// caller must then clean up whatever it uploaded.
func (d *DB) CommitBlocks(id int64, blocks []BlockCommit, size int64) (bool, []string, error) {
	if size < 0 {
		return false, nil, fmt.Errorf("commit blocks inode %d: negative size %d", id, size)
	}
	// Highest index still inside the file; -1 when the file is empty.
	lastLive := int64(-1)
	if size > 0 {
		lastLive = (size - 1) / BlockSize
	}
	for _, b := range blocks {
		if b.Index < 0 || b.Index > lastLive {
			return false, nil, fmt.Errorf(
				"commit blocks inode %d: block %d is outside a file of %d bytes (last live block is %d)",
				id, b.Index, size, lastLive)
		}
		if b.S3Key == "" || b.Size <= 0 {
			return false, nil, fmt.Errorf("commit blocks inode %d: block %d has key %q and size %d",
				id, b.Index, b.S3Key, b.Size)
		}
	}

	now := time.Now().UnixNano()
	var committed bool
	var orphaned []string
	var err error
	for attempt := range 5 {
		committed, orphaned, err = d.commitBlocksTx(id, blocks, size, lastLive, now)
		if err == nil {
			return committed, orphaned, nil
		}
		if !isBusy(err) {
			return false, nil, err
		}
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	return false, nil, err
}

func (d *DB) commitBlocksTx(id int64, blocks []BlockCommit, size, lastLive, now int64) (bool, []string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback()

	var curVolKey sql.NullString
	var volSize int64
	err = tx.QueryRow(
		"SELECT vol_s3_key, COALESCE(vol_size, 0) FROM inodes WHERE id = ?", id,
	).Scan(&curVolKey, &volSize)
	if errors.Is(err, sql.ErrNoRows) {
		// Inode was deleted (e.g. unlinked during an async upload).
		return false, nil, tx.Commit()
	}
	if err != nil {
		return false, nil, err
	}

	// Current block set, read inside the transaction.
	rows, err := tx.Query("SELECT block_index, s3_key FROM blocks WHERE inode_id = ?", id)
	if err != nil {
		return false, nil, fmt.Errorf("commit blocks inode %d: read current: %w", id, err)
	}
	old := make(map[int64]string)
	for rows.Next() {
		var idx int64
		var key string
		if err := rows.Scan(&idx, &key); err != nil {
			rows.Close()
			return false, nil, err
		}
		old[idx] = key
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, nil, err
	}

	if curVolKey.Valid && curVolKey.String != "" {
		if _, err := tx.Exec(
			"UPDATE volumes SET live_size = MAX(live_size - ?, 0), live_count = MAX(live_count - 1, 0) WHERE s3_key = ?",
			volSize, curVolKey.String,
		); err != nil {
			return false, nil, fmt.Errorf("commit blocks inode %d: decrement volume: %w", id, err)
		}
	}

	var orphaned []string
	for _, b := range blocks {
		if prev, ok := old[b.Index]; ok && prev != b.S3Key {
			orphaned = append(orphaned, prev)
		}
		if _, err := tx.Exec(
			`INSERT INTO blocks (inode_id, block_index, s3_key, size) VALUES (?, ?, ?, ?)
			 ON CONFLICT(inode_id, block_index) DO UPDATE SET s3_key = excluded.s3_key, size = excluded.size`,
			id, b.Index, b.S3Key, b.Size,
		); err != nil {
			return false, nil, fmt.Errorf("commit blocks inode %d: upsert block %d: %w", id, b.Index, err)
		}
	}

	for idx, key := range old {
		if idx > lastLive {
			orphaned = append(orphaned, key)
		}
	}
	if _, err := tx.Exec("DELETE FROM blocks WHERE inode_id = ? AND block_index > ?", id, lastLive); err != nil {
		return false, nil, fmt.Errorf("commit blocks inode %d: truncate past %d: %w", id, lastLive, err)
	}
	// A shrink can also cut into the last surviving block, which is not covered
	// by the DELETE above and which a flush does not necessarily rewrite: an
	// ftruncate that dirties nothing commits an empty block set at a smaller
	// size. A block just written in this same commit already records its live
	// extent, so this is a no-op for it.
	if err := clampLastBlockSize(tx, id, size, lastLive); err != nil {
		return false, nil, err
	}

	res, err := tx.Exec(
		"UPDATE inodes SET size = ?, status = 'committed', mtime_ns = ?,"+
			" vol_s3_key = NULL, vol_offset = NULL, vol_size = NULL WHERE id = ?",
		size, now, id,
	)
	if err != nil {
		return false, nil, err
	}
	if err := tx.Commit(); err != nil {
		return false, nil, err
	}
	n, _ := res.RowsAffected()
	// Map iteration above makes the order arbitrary; sorting keeps callers' logs
	// and tests stable.
	slices.Sort(orphaned)
	return n > 0, orphaned, nil
}

// BlocksForInode returns the inode's blocks ordered by index. A gap in the
// indexes is a hole: it reads as zeroes and is never fetched.
func (d *DB) BlocksForInode(id int64) ([]BlockCommit, error) {
	rows, err := d.db.Query(
		"SELECT block_index, s3_key, size FROM blocks WHERE inode_id = ? ORDER BY block_index", id)
	if err != nil {
		return nil, fmt.Errorf("blocks for inode %d: %w", id, err)
	}
	defer rows.Close()

	var result []BlockCommit
	for rows.Next() {
		var b BlockCommit
		if err := rows.Scan(&b.Index, &b.S3Key, &b.Size); err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, rows.Err()
}

// BlockAt returns one block's row, or ok=false when the inode has no row at that
// index — which is a HOLE, not an error: it reads as zeroes and is never
// fetched.
//
// This is the lookup lazy materialization faults through, and it must stay one
// row rather than reusing BlocksForInode. A 4 TB file has 524288 block rows, so
// loading the whole map to answer "where is block 7" would put the cost of the
// file's size on every single fault — the opposite of what faulting is for.
func (d *DB) BlockAt(id, index int64) (BlockCommit, bool, error) {
	b := BlockCommit{Index: index}
	err := d.db.QueryRow(
		"SELECT s3_key, size FROM blocks WHERE inode_id = ? AND block_index = ?", id, index,
	).Scan(&b.S3Key, &b.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return BlockCommit{}, false, nil
	}
	if err != nil {
		return BlockCommit{}, false, fmt.Errorf("block %d of inode %d: %w", index, id, err)
	}
	return b, true, nil
}

// DeleteBlocksForInode drops the inode's block rows and returns the keys they
// held, so the caller can delete the objects afterwards. Reading the keys and
// deleting the rows is one transaction because the only safe direction is
// "read keys -> drop rows -> delete objects": rows also disappear on their own
// through ON DELETE CASCADE, and once they have, nothing knows those keys.
func (d *DB) DeleteBlocksForInode(id int64) ([]string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT s3_key FROM blocks WHERE inode_id = ? ORDER BY block_index", id)
	if err != nil {
		return nil, fmt.Errorf("delete blocks for inode %d: %w", id, err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec("DELETE FROM blocks WHERE inode_id = ?", id); err != nil {
		return nil, fmt.Errorf("delete blocks for inode %d: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

// HasBlocks reports whether the inode's data lives in blocks. It is the cheap
// form of the question BlocksForInode answers expensively: callers that only
// need to route (Flush choosing a storage shape, Fsync deciding whether the
// volume builder owns this inode, CleanupStagingDir deciding whether a staging
// file is stale) must not pull 512 rows to learn one bit.
//
// A file that has blocks keeps using them even when a later write shrinks it
// below MaxNeedleSize. Falling back to the staging path would commit through
// CommitInode, which knows nothing about blocks: the rows would survive and the
// read path — which checks blocks first — would serve the pre-shrink version.
func (d *DB) HasBlocks(id int64) (bool, error) {
	var one int
	err := d.db.QueryRow("SELECT 1 FROM blocks WHERE inode_id = ? LIMIT 1", id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("has blocks for inode %d: %w", id, err)
	}
	return true, nil
}

func (d *DB) GetConfig(key string) ([]byte, error) {
	var val []byte
	err := d.db.QueryRow("SELECT value FROM config WHERE key = ?", key).Scan(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (d *DB) SetConfig(key string, value []byte) error {
	_, err := d.db.Exec(
		"INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
		key, value,
	)
	return err
}

// SetAttr applies a metadata change and returns the block objects the new size
// orphaned, for the caller to delete AFTER this returns — the same
// "read keys -> drop rows -> delete objects" direction as DeleteBlocksForInode,
// and for the same reason: the rows are the only record of those keys.
//
// Dropping the blocks past the new end is not bookkeeping, it is the difference
// between a shrink and a lie. Flush reaches CommitBlocks, which truncates the
// block set itself, but truncate(2) on a path with no open write handle never
// gets there: tryAcquireWrite returns nil, only inodes.size moves, and the rows
// beyond it survive. Reads stay correct (readLoaded clamps to the size), so
// nothing looks wrong — until the file is grown again and the resurrected blocks
// serve their old contents where the file should read as zeroes. Measured after
// step 3: shrinking a 3-block file to 16 bytes and growing it back returned
// 8 MiB of stale data.
//
// Growing selects nothing (a larger size can only raise lastLive), so this runs
// unconditionally rather than branching on the direction of the change.
func (d *DB) SetAttr(id int64, size *int64, mode *uint32, mtimeNs *int64) ([]string, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	var orphaned []string
	if size != nil {
		if *size < 0 {
			return nil, fmt.Errorf("set attr inode %d: negative size %d", id, *size)
		}
		// Highest index still inside the file; -1 when the file is empty.
		lastLive := int64(-1)
		if *size > 0 {
			lastLive = (*size - 1) / BlockSize
		}
		rows, qErr := tx.Query(
			"SELECT s3_key FROM blocks WHERE inode_id = ? AND block_index > ? ORDER BY block_index",
			id, lastLive)
		if qErr != nil {
			return nil, fmt.Errorf("set attr inode %d: read blocks past %d: %w", id, lastLive, qErr)
		}
		for rows.Next() {
			var key string
			if sErr := rows.Scan(&key); sErr != nil {
				rows.Close()
				return nil, sErr
			}
			orphaned = append(orphaned, key)
		}
		qErr = rows.Err()
		rows.Close()
		if qErr != nil {
			return nil, qErr
		}
		if _, err := tx.Exec(
			"DELETE FROM blocks WHERE inode_id = ? AND block_index > ?", id, lastLive,
		); err != nil {
			return nil, fmt.Errorf("set attr inode %d: truncate past %d: %w", id, lastLive, err)
		}
		if err := clampLastBlockSize(tx, id, *size, lastLive); err != nil {
			return nil, err
		}
		if _, err := tx.Exec("UPDATE inodes SET size = ?, mtime_ns = ? WHERE id = ?", *size, now, id); err != nil {
			return nil, err
		}
	}
	if mode != nil {
		if _, err := tx.Exec("UPDATE inodes SET mode = ?, ctime_ns = ? WHERE id = ?", *mode, now, id); err != nil {
			return nil, err
		}
	}
	if mtimeNs != nil {
		if _, err := tx.Exec("UPDATE inodes SET mtime_ns = ? WHERE id = ?", *mtimeNs, id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return orphaned, nil
}

// FixDefaultOwnership updates all inodes with uid=0 AND gid=0 to the given values.
func (d *DB) FixDefaultOwnership(uid, gid uint32) (int64, error) {
	res, err := d.db.Exec("UPDATE inodes SET uid = ?, gid = ? WHERE uid = 0 AND gid = 0", uid, gid)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SetOwner updates uid and/or gid for an inode.
func (d *DB) SetOwner(id int64, uid *uint32, gid *uint32) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	if uid != nil {
		if _, err := tx.Exec("UPDATE inodes SET uid = ?, ctime_ns = ? WHERE id = ?", *uid, now, id); err != nil {
			return err
		}
	}
	if gid != nil {
		if _, err := tx.Exec("UPDATE inodes SET gid = ?, ctime_ns = ? WHERE id = ?", *gid, now, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// --- Extended attributes ---

func (d *DB) GetXattr(inodeID int64, name string) ([]byte, error) {
	var val []byte
	err := d.db.QueryRow("SELECT value FROM xattrs WHERE inode_id = ? AND name = ?", inodeID, name).Scan(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (d *DB) SetXattr(inodeID int64, name string, value []byte) error {
	_, err := d.db.Exec(
		"INSERT OR REPLACE INTO xattrs (inode_id, name, value) VALUES (?, ?, ?)",
		inodeID, name, value,
	)
	return err
}

func (d *DB) RemoveXattr(inodeID int64, name string) error {
	res, err := d.db.Exec("DELETE FROM xattrs WHERE inode_id = ? AND name = ?", inodeID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (d *DB) ListXattrs(inodeID int64) ([]string, error) {
	rows, err := d.db.Query("SELECT name FROM xattrs WHERE inode_id = ?", inodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// --- Filesystem stats ---

type FSStats struct {
	FileCount int64
	DirCount  int64
	TotalSize int64
}

func (d *DB) GetFSStats() (*FSStats, error) {
	s := &FSStats{}
	err := d.db.QueryRow(
		"SELECT COALESCE(SUM(CASE WHEN mode & ? = 0 THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN mode & ? != 0 THEN 1 ELSE 0 END), 0), COALESCE(SUM(size), 0) FROM inodes WHERE status = 'committed'",
		uint32(syscall.S_IFDIR), uint32(syscall.S_IFDIR),
	).Scan(&s.FileCount, &s.DirCount, &s.TotalSize)
	return s, err
}

// --- Fsck ---

// FsckResult holds the results of a consistency check.
type FsckResult struct {
	OrphanedInodes   int
	PendingInodes    int
	StagedFiles      int // committed files with data in staging dir (not yet in S3)
	TotalInodes      int
	VolumeCount      int // sealed volumes
	VolumeMismatches int // volumes where live_count != actual committed needle count
}

func (d *DB) Fsck() (*FsckResult, error) {
	r := &FsckResult{}
	if err := d.db.QueryRow("SELECT COUNT(*) FROM inodes").Scan(&r.TotalInodes); err != nil {
		return nil, fmt.Errorf("fsck total: %w", err)
	}
	if err := d.db.QueryRow("SELECT COUNT(*) FROM inodes WHERE parent_id > 0 AND parent_id NOT IN (SELECT id FROM inodes)").Scan(&r.OrphanedInodes); err != nil {
		return nil, fmt.Errorf("fsck orphans: %w", err)
	}
	if err := d.db.QueryRow("SELECT COUNT(*) FROM inodes WHERE status = 'pending'").Scan(&r.PendingInodes); err != nil {
		return nil, fmt.Errorf("fsck pending: %w", err)
	}
	// Staged files: committed but data still in staging dir (not yet packed into
	// a volume). Select regular files by masking with S_IFMT: the type lives in
	// those 4 bits, it is not a set of independent flags. Testing
	// "mode & S_IFLNK = 0" excludes regular files, because S_IFREG (0x8000) and
	// S_IFLNK (0xA000) share a bit — this counter always reported 0.
	if err := d.db.QueryRow(
		"SELECT COUNT(*) FROM inodes WHERE status = 'committed' AND (vol_s3_key IS NULL OR vol_s3_key = '')"+noBlockRows+" AND mode & ? = ? AND id > 1 AND size > 0",
		uint32(syscall.S_IFMT), uint32(syscall.S_IFREG),
	).Scan(&r.StagedFiles); err != nil {
		return nil, fmt.Errorf("fsck staged: %w", err)
	}

	// Volume consistency: compare tracked live_count with actual needle count
	volRows, err := d.db.Query("SELECT s3_key, live_count FROM volumes WHERE status = 'sealed'")
	if err != nil {
		return nil, fmt.Errorf("fsck volumes: %w", err)
	}
	defer volRows.Close()
	for volRows.Next() {
		var s3Key string
		var liveCount int
		if err := volRows.Scan(&s3Key, &liveCount); err != nil {
			return nil, fmt.Errorf("fsck volume scan: %w", err)
		}
		r.VolumeCount++
		var actualCount int
		if err := d.db.QueryRow(
			"SELECT COUNT(*) FROM inodes WHERE vol_s3_key = ? AND status = 'committed'", s3Key,
		).Scan(&actualCount); err != nil {
			return nil, fmt.Errorf("fsck volume count %s: %w", s3Key, err)
		}
		if actualCount != liveCount {
			r.VolumeMismatches++
		}
	}
	if err := volRows.Err(); err != nil {
		return nil, fmt.Errorf("fsck volumes iter: %w", err)
	}

	return r, nil
}
