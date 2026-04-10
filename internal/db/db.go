package db

import (
	"database/sql"
	"fmt"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

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
}

const inodeCols = "id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns, uid, gid, symlink_target"

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
}

type DB struct {
	db *sql.DB
}

func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_txlock=immediate", path)
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=10000",
		"PRAGMA synchronous=NORMAL",
	}
	for _, p := range pragmas {
		if _, err := sqldb.Exec(p); err != nil {
			sqldb.Close()
			return nil, fmt.Errorf("db pragma %q: %w", p, err)
		}
	}

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
	err := scanner.Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size,
		&s3key, &m.Status, &m.MtimeNs, &m.CtimeNs, &m.Uid, &m.Gid, &symlinkTarget)
	if err != nil {
		return nil, err
	}
	m.S3Key = s3key.String
	m.SymlinkTarget = symlinkTarget.String
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
		"SELECT "+inodeCols+" FROM inodes WHERE parent_id = ? AND name = ?",
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
		"SELECT "+inodeCols+" FROM inodes WHERE parent_id > 0 AND parent_id NOT IN (SELECT id FROM inodes)",
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

func (d *DB) CommitInode(id int64, s3Key string, size int64) (bool, error) {
	now := time.Now().UnixNano()
	var res sql.Result
	var err error
	for attempt := range 5 {
		res, err = d.db.Exec(
			"UPDATE inodes SET s3_key = ?, size = ?, status = 'committed', mtime_ns = ? WHERE id = ?",
			s3Key, size, now, id,
		)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "SQLITE_BUSY") && !strings.Contains(err.Error(), "database is locked") {
			return false, err
		}
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	if err != nil {
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
		"SELECT "+inodeCols+" FROM inodes WHERE status = 'pending'",
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
	return set, rows.Err()
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
	OrphanedInodes int
	PendingInodes  int
	MissingS3Keys  int // inodes with status=committed but empty s3_key (non-dir, non-symlink)
	TotalInodes    int
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
	if err := d.db.QueryRow(
		"SELECT COUNT(*) FROM inodes WHERE status = 'committed' AND (s3_key IS NULL OR s3_key = '') AND mode & ? = 0 AND mode & ? = 0 AND id > 1 AND size > 0",
		uint32(syscall.S_IFDIR), uint32(syscall.S_IFLNK),
	).Scan(&r.MissingS3Keys); err != nil {
		return nil, fmt.Errorf("fsck missing keys: %w", err)
	}
	return r, nil
}
