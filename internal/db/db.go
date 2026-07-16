package db

import (
	"database/sql"
	"errors"
	"fmt"
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
    s3_key          TEXT,
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

const inodeCols = "id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns, uid, gid, symlink_target, vol_s3_key, vol_offset, vol_size"

type InodeMeta struct {
	ID            int64
	ParentID      int64
	Name          string
	Mode          uint32
	Size          int64
	S3Key         string
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
	var s3key sql.NullString
	var symlinkTarget sql.NullString
	var volS3Key sql.NullString
	var volOffset sql.NullInt64
	var volSize sql.NullInt64
	err := scanner.Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size,
		&s3key, &m.Status, &m.MtimeNs, &m.CtimeNs, &m.Uid, &m.Gid, &symlinkTarget,
		&volS3Key, &volOffset, &volSize)
	if err != nil {
		return nil, err
	}
	m.S3Key = s3key.String
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

// CommitInode marks an inode committed, pointing it at s3Key (which may be ""
// for a staged needle) with the given size. If the inode currently references a
// volume needle, that volume's live_size/live_count are decremented in the SAME
// transaction so a crash can never leave a volume's live_count dropped while the
// inode still references it (which GetEmptyVolumes + GC would otherwise treat as
// deletable, destroying still-referenced data). The volume reference is re-read
// inside the transaction rather than trusted from a caller snapshot, so it stays
// correct even if the volume builder packed the inode during an async upload.
// Returns whether the inode still existed (RowsAffected > 0).
func (d *DB) CommitInode(id int64, s3Key string, size int64) (bool, error) {
	now := time.Now().UnixNano()
	var committed bool
	var err error
	for attempt := range 5 {
		committed, err = d.commitInodeTx(id, s3Key, size, now)
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

func (d *DB) commitInodeTx(id int64, s3Key string, size, now int64) (bool, error) {
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
		"UPDATE inodes SET s3_key = ?, size = ?, status = 'committed', mtime_ns = ?, vol_s3_key = NULL, vol_offset = NULL, vol_size = NULL WHERE id = ?",
		s3Key, size, now, id,
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

func (d *DB) DeleteInode(id int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("DELETE FROM xattrs WHERE inode_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM inodes WHERE id = ?", id); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) RenameInode(id int64, newParentID int64, newName string) error {
	now := time.Now().UnixNano()
	_, err := d.db.Exec(
		"UPDATE inodes SET parent_id = ?, name = ?, ctime_ns = ? WHERE id = ?",
		newParentID, newName, now, id,
	)
	return err
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

type S3KeyRecord struct {
	ID    int64
	S3Key string
}

func (d *DB) AllS3Keys() ([]S3KeyRecord, error) {
	rows, err := d.db.Query("SELECT id, s3_key FROM inodes WHERE s3_key IS NOT NULL AND s3_key != ''")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []S3KeyRecord
	for rows.Next() {
		var r S3KeyRecord
		if err := rows.Scan(&r.ID, &r.S3Key); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (d *DB) AllS3KeySet() (map[string]struct{}, error) {
	rows, err := d.db.Query("SELECT s3_key FROM inodes WHERE s3_key IS NOT NULL AND s3_key != ''")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := make(map[string]struct{})
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		set[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Include volume S3 keys
	vrows, err := d.db.Query("SELECT s3_key FROM volumes")
	if err != nil {
		return nil, err
	}
	defer vrows.Close()
	for vrows.Next() {
		var key string
		if err := vrows.Scan(&key); err != nil {
			return nil, err
		}
		set[key] = struct{}{}
	}
	return set, vrows.Err()
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
func (d *DB) DeleteInodeWithVolume(id int64, volS3Key string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var curVolKey sql.NullString
	var volSize int64
	err = tx.QueryRow("SELECT vol_s3_key, COALESCE(vol_size, 0) FROM inodes WHERE id = ?", id).Scan(&curVolKey, &volSize)
	if errors.Is(err, sql.ErrNoRows) {
		// Inode already gone — nothing to delete or decrement.
		return tx.Commit()
	}
	if err != nil {
		return fmt.Errorf("delete inode with volume: get vol ref: %w", err)
	}
	if curVolKey.Valid && curVolKey.String != "" {
		if _, err = tx.Exec(
			"UPDATE volumes SET live_size = MAX(live_size - ?, 0), live_count = MAX(live_count - 1, 0) WHERE s3_key = ?",
			volSize, curVolKey.String,
		); err != nil {
			return fmt.Errorf("delete inode with volume: update volume: %w", err)
		}
	}

	if _, err := tx.Exec("DELETE FROM xattrs WHERE inode_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM inodes WHERE id = ?", id); err != nil {
		return err
	}
	return tx.Commit()
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
		query += " AND (s3_key IS NULL OR s3_key = '') AND (vol_s3_key IS NULL OR vol_s3_key = '')"
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

func (d *DB) UpdateS3Key(id int64, newKey string) error {
	_, err := d.db.Exec("UPDATE inodes SET s3_key = ? WHERE id = ?", newKey, id)
	return err
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

func (d *DB) SetAttr(id int64, size *int64, mode *uint32, mtimeNs *int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	if size != nil {
		if _, err := tx.Exec("UPDATE inodes SET size = ?, mtime_ns = ? WHERE id = ?", *size, now, id); err != nil {
			return err
		}
	}
	if mode != nil {
		if _, err := tx.Exec("UPDATE inodes SET mode = ?, ctime_ns = ? WHERE id = ?", *mode, now, id); err != nil {
			return err
		}
	}
	if mtimeNs != nil {
		if _, err := tx.Exec("UPDATE inodes SET mtime_ns = ? WHERE id = ?", *mtimeNs, id); err != nil {
			return err
		}
	}
	return tx.Commit()
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
	// Staged files: committed but data still in staging dir (not yet packed into a volume)
	if err := d.db.QueryRow(
		"SELECT COUNT(*) FROM inodes WHERE status = 'committed' AND (s3_key IS NULL OR s3_key = '') AND (vol_s3_key IS NULL OR vol_s3_key = '') AND mode & ? = 0 AND mode & ? = 0 AND id > 1 AND size > 0",
		uint32(syscall.S_IFDIR), uint32(syscall.S_IFLNK),
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
