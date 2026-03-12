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
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER NOT NULL,
    name      TEXT NOT NULL,
    mode      INTEGER NOT NULL,
    size      INTEGER NOT NULL DEFAULT 0,
    s3_key    TEXT,
    status    TEXT NOT NULL DEFAULT 'committed',
    mtime_ns  INTEGER NOT NULL,
    ctime_ns  INTEGER NOT NULL,
    UNIQUE(parent_id, name)
);
CREATE INDEX IF NOT EXISTS idx_inodes_parent_status ON inodes(parent_id, status);
`

type InodeMeta struct {
	ID       int64
	ParentID int64
	Name     string
	Mode     uint32
	Size     int64
	S3Key    string
	Status   string
	MtimeNs  int64
	CtimeNs  int64
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
		"PRAGMA busy_timeout=5000",
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

	d := &DB{db: sqldb}
	if err := d.seedRoot(); err != nil {
		sqldb.Close()
		return nil, err
	}
	return d, nil
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
		"INSERT INTO inodes (id, parent_id, name, mode, size, status, mtime_ns, ctime_ns) VALUES (1, 0, '', ?, 0, 'committed', ?, ?)",
		mode, now, now,
	)
	if err != nil {
		return fmt.Errorf("db seed root: %w", err)
	}
	return nil
}

func (d *DB) GetInode(id int64) (*InodeMeta, error) {
	m := &InodeMeta{}
	var s3key sql.NullString
	err := d.db.QueryRow(
		"SELECT id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns FROM inodes WHERE id = ?", id,
	).Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size, &s3key, &m.Status, &m.MtimeNs, &m.CtimeNs)
	if err != nil {
		return nil, err
	}
	m.S3Key = s3key.String
	return m, nil
}

func (d *DB) LookupChild(parentID int64, name string) (*InodeMeta, error) {
	m := &InodeMeta{}
	var s3key sql.NullString
	err := d.db.QueryRow(
		"SELECT id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns FROM inodes WHERE parent_id = ? AND name = ?",
		parentID, name,
	).Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size, &s3key, &m.Status, &m.MtimeNs, &m.CtimeNs)
	if err != nil {
		return nil, err
	}
	m.S3Key = s3key.String
	return m, nil
}

func (d *DB) ListChildren(parentID int64) ([]InodeMeta, error) {
	rows, err := d.db.Query(
		"SELECT id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns FROM inodes WHERE parent_id = ? AND status = 'committed'",
		parentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []InodeMeta
	for rows.Next() {
		var m InodeMeta
		var s3key sql.NullString
		if err := rows.Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size, &s3key, &m.Status, &m.MtimeNs, &m.CtimeNs); err != nil {
			return nil, err
		}
		m.S3Key = s3key.String
		result = append(result, m)
	}
	return result, rows.Err()
}

func (d *DB) InsertInode(parentID int64, name string, mode uint32, status string) (int64, error) {
	now := time.Now().UnixNano()
	res, err := d.db.Exec(
		"INSERT INTO inodes (parent_id, name, mode, size, status, mtime_ns, ctime_ns) VALUES (?, ?, ?, 0, ?, ?, ?)",
		parentID, name, mode, status, now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) CommitInode(id int64, s3Key string, size int64) error {
	now := time.Now().UnixNano()
	_, err := d.db.Exec(
		"UPDATE inodes SET s3_key = ?, size = ?, status = 'committed', mtime_ns = ? WHERE id = ?",
		s3Key, size, now, id,
	)
	return err
}

func (d *DB) DeleteInode(id int64) error {
	_, err := d.db.Exec("DELETE FROM inodes WHERE id = ?", id)
	return err
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
		"SELECT id, parent_id, name, mode, size, s3_key, status, mtime_ns, ctime_ns FROM inodes WHERE status = 'pending'",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []InodeMeta
	for rows.Next() {
		var m InodeMeta
		var s3key sql.NullString
		if err := rows.Scan(&m.ID, &m.ParentID, &m.Name, &m.Mode, &m.Size, &s3key, &m.Status, &m.MtimeNs, &m.CtimeNs); err != nil {
			return nil, err
		}
		m.S3Key = s3key.String
		result = append(result, m)
	}
	return result, rows.Err()
}

func (d *DB) InodePath(id int64) (string, error) {
	var parts []string
	current := id
	for current > 1 {
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

func (d *DB) SetAttr(id int64, size *int64, mode *uint32, mtimeNs *int64) error {
	now := time.Now().UnixNano()
	if size != nil {
		if _, err := d.db.Exec("UPDATE inodes SET size = ?, mtime_ns = ? WHERE id = ?", *size, now, id); err != nil {
			return err
		}
	}
	if mode != nil {
		if _, err := d.db.Exec("UPDATE inodes SET mode = ?, ctime_ns = ? WHERE id = ?", *mode, now, id); err != nil {
			return err
		}
	}
	if mtimeNs != nil {
		if _, err := d.db.Exec("UPDATE inodes SET mtime_ns = ? WHERE id = ?", *mtimeNs, id); err != nil {
			return err
		}
	}
	return nil
}
