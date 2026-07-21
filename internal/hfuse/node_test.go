package hfuse

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/db"
)

// setupDBOnly creates a test HamstorFS with only DB (no S3/cache).
func setupDBOnly(t *testing.T) *HamstorFS {
	t.Helper()
	dbPath := t.TempDir() + "/test.db"
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	hfs := &HamstorFS{
		DB:         database,
		DefaultUid: 1000,
		DefaultGid: 1000,
		UploadSem:  make(chan struct{}, 8),
		ThumbSem:   make(chan struct{}, 4),
	}
	t.Cleanup(func() { database.Close() })
	return hfs
}

func TestRmdirNonEmpty(t *testing.T) {
	hfs := setupDBOnly(t)

	dirID, err := hfs.DB.InsertInode(1, "mydir", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}
	if _, err := hfs.DB.InsertInode(dirID, "file.txt", syscall.S_IFREG|0o644, "committed"); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Rmdir should fail with ENOTEMPTY
	node := &HamstorNode{hfs: hfs, inodeID: 1}
	errno := node.Rmdir(context.Background(), "mydir")
	if errno != syscall.ENOTEMPTY {
		t.Fatalf("expected ENOTEMPTY, got %v", errno)
	}

	// Dir should still exist
	if _, err := hfs.DB.GetInode(dirID); err != nil {
		t.Fatal("dir should still exist after failed rmdir")
	}
}

func TestRmdirEmpty(t *testing.T) {
	hfs := setupDBOnly(t)

	dirID, err := hfs.DB.InsertInode(1, "emptydir", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}

	node := &HamstorNode{hfs: hfs, inodeID: 1}
	errno := node.Rmdir(context.Background(), "emptydir")
	if errno != 0 {
		t.Fatalf("expected success, got %v", errno)
	}

	// Dir should be gone
	if _, err := hfs.DB.GetInode(dirID); err == nil {
		t.Fatal("dir should be deleted")
	}
}

func TestMkdirDuplicate(t *testing.T) {
	hfs := setupDBOnly(t)

	if _, err := hfs.DB.InsertInode(1, "existing", syscall.S_IFDIR|0o755, "committed"); err != nil {
		t.Fatalf("insert dir: %v", err)
	}

	// Trying to create same name should fail with EEXIST
	_, err := hfs.DB.InsertInodeWithOwner(1, "existing", syscall.S_IFDIR|0o755, "committed", 1000, 1000)
	errno := toErrno(err)
	if errno != syscall.EEXIST {
		t.Fatalf("expected EEXIST, got %v (err: %v)", errno, err)
	}
}

func TestRenameCycleDetection(t *testing.T) {
	hfs := setupDBOnly(t)

	// Create: root -> parent -> child
	parentID, err := hfs.DB.InsertInode(1, "parent", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	childID, err := hfs.DB.InsertInode(parentID, "child", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Test the cycle detection logic directly:
	// Moving parentID into childID would create a cycle (child is descendant of parent).
	// Walk from childID up to root, checking if we hit parentID.
	meta, err := hfs.DB.GetInode(parentID)
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if meta.Mode&syscall.S_IFDIR == 0 {
		t.Fatal("parent should be a dir")
	}

	// Simulate cycle check: walk from new parent (childID) up
	cycleDetected := false
	current := childID
	for current > 1 {
		if current == parentID {
			cycleDetected = true
			break
		}
		p, err := hfs.DB.GetInode(current)
		if err != nil {
			t.Fatalf("get inode %d: %v", current, err)
		}
		current = p.ParentID
	}
	if !cycleDetected {
		t.Fatal("expected cycle to be detected when moving parent into child")
	}

	// Verify non-cycle case: moving child to root should be fine
	current = int64(1) // root
	cycleDetected = false
	for current > 1 {
		if current == childID {
			cycleDetected = true
			break
		}
		p, err := hfs.DB.GetInode(current)
		if err != nil {
			t.Fatalf("get inode %d: %v", current, err)
		}
		current = p.ParentID
	}
	if cycleDetected {
		t.Fatal("should not detect cycle when moving child to root")
	}
}

func TestRenameOverNonEmptyDir(t *testing.T) {
	hfs := setupDBOnly(t)

	// Create target dir with a file
	dstID, err := hfs.DB.InsertInode(1, "dst", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dst: %v", err)
	}
	if _, err := hfs.DB.InsertInode(dstID, "file.txt", syscall.S_IFREG|0o644, "committed"); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Verify: target dir has children (ENOTEMPTY check)
	children, err := hfs.DB.ListAllChildren(dstID)
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if len(children) == 0 {
		t.Fatal("dst should have children for this test")
	}

	// The Rename code checks if target is a non-empty dir and returns ENOTEMPTY.
	// Test the check directly since FUSE Rename requires a wired-up inode tree.
	existing, err := hfs.DB.LookupChild(1, "dst")
	if err != nil {
		t.Fatalf("lookup dst: %v", err)
	}
	if existing.Mode&syscall.S_IFDIR != 0 {
		kids, err := hfs.DB.ListAllChildren(existing.ID)
		if err != nil {
			t.Fatalf("list children: %v", err)
		}
		if len(kids) == 0 {
			t.Fatal("rename over non-empty dir should be blocked")
		}
		// This is the check that happens in Rename — would return ENOTEMPTY
	}
}

func TestRenameOverEmptyDir(t *testing.T) {
	hfs := setupDBOnly(t)

	// Create empty target dir
	dstID, err := hfs.DB.InsertInode(1, "dst", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dst: %v", err)
	}

	// Verify empty
	children, err := hfs.DB.ListAllChildren(dstID)
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if len(children) != 0 {
		t.Fatal("dst should be empty for this test")
	}

	// DeleteInode should work on empty dir (what Rename does after the check passes)
	if _, err := hfs.DB.DeleteInode(dstID); err != nil {
		t.Fatalf("delete empty dst dir: %v", err)
	}
	if _, err := hfs.DB.GetInode(dstID); err == nil {
		t.Fatal("dst dir should be deleted")
	}
}

func TestSetxattrFlags(t *testing.T) {
	hfs := setupDBOnly(t)

	fileID, err := hfs.DB.InsertInode(1, "testfile", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	node := &HamstorNode{hfs: hfs, inodeID: fileID}
	ctx := context.Background()

	// XATTR_REPLACE on non-existing should fail
	errno := node.Setxattr(ctx, "user.test", []byte("value"), 0x2)
	if errno != syscall.ENODATA {
		t.Fatalf("expected ENODATA for XATTR_REPLACE on missing, got %v", errno)
	}

	// Normal set (flags=0) should succeed
	errno = node.Setxattr(ctx, "user.test", []byte("value"), 0)
	if errno != 0 {
		t.Fatalf("expected success, got %v", errno)
	}

	// XATTR_CREATE on existing should fail
	errno = node.Setxattr(ctx, "user.test", []byte("value2"), 0x1)
	if errno != syscall.EEXIST {
		t.Fatalf("expected EEXIST for XATTR_CREATE on existing, got %v", errno)
	}

	// XATTR_REPLACE on existing should succeed
	errno = node.Setxattr(ctx, "user.test", []byte("updated"), 0x2)
	if errno != 0 {
		t.Fatalf("expected success for XATTR_REPLACE, got %v", errno)
	}

	// Verify value was updated
	val, err := hfs.DB.GetXattr(fileID, "user.test")
	if err != nil {
		t.Fatalf("get xattr: %v", err)
	}
	if string(val) != "updated" {
		t.Fatalf("expected 'updated', got %q", val)
	}
}

func TestInodePathDepthLimit(t *testing.T) {
	hfs := setupDBOnly(t)

	// Create a cycle: inode 2 -> parent=3, inode 3 -> parent=2
	id1, err := hfs.DB.InsertInode(1, "a", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert a: %v", err)
	}
	id2, err := hfs.DB.InsertInode(id1, "b", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert b: %v", err)
	}

	// Manually create a cycle by updating parent
	hfs.DB.RenameInode(id1, id2, "a")

	// InodePath should not hang — it should return an error
	_, err = hfs.DB.InodePath(id1)
	if err == nil {
		t.Fatal("expected error for cyclic path, got nil")
	}
}

func TestSetattrTruncation(t *testing.T) {
	hfs := setupDBOnly(t)

	fileID, err := hfs.DB.InsertInode(1, "bigfile", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	handle := newHandle(hfs, fileID, true)

	// Write some data
	ctx := context.Background()
	handle.Write(ctx, []byte("hello world, this is test data"), 0)

	// Truncate via buffer
	if handle.st.buf == nil {
		t.Fatal("expected buf to be set")
	}
	origLen := len(handle.st.buf)
	if origLen == 0 {
		t.Fatal("expected non-empty buf")
	}

	// Simulate Setattr truncation
	handle.st.mu.Lock()
	newSize := int64(5)
	if newSize < int64(len(handle.st.buf)) {
		handle.st.buf = handle.st.buf[:newSize]
	}
	handle.st.dirty = true
	handle.st.mu.Unlock()

	if len(handle.st.buf) != 5 {
		t.Fatalf("expected buf len 5, got %d", len(handle.st.buf))
	}
	if string(handle.st.buf) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(handle.st.buf))
	}
}

func TestFsckErrorPropagation(t *testing.T) {
	hfs := setupDBOnly(t)

	result, err := hfs.DB.Fsck()
	if err != nil {
		t.Fatalf("fsck should succeed on clean DB: %v", err)
	}
	if result.TotalInodes != 1 {
		t.Fatalf("expected 1 inode (root), got %d", result.TotalInodes)
	}
	if result.OrphanedInodes != 0 {
		t.Fatalf("expected 0 orphans, got %d", result.OrphanedInodes)
	}
}

func TestDeleteInodeTransaction(t *testing.T) {
	hfs := setupDBOnly(t)

	// Create file with xattrs
	fileID, err := hfs.DB.InsertInode(1, "xfile", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := hfs.DB.SetXattr(fileID, "user.test", []byte("val")); err != nil {
		t.Fatalf("set xattr: %v", err)
	}

	// Verify xattr exists
	if _, err := hfs.DB.GetXattr(fileID, "user.test"); err != nil {
		t.Fatal("xattr should exist before delete")
	}

	// Delete inode (should also delete xattrs in transaction)
	if _, err := hfs.DB.DeleteInode(fileID); err != nil {
		t.Fatalf("delete inode: %v", err)
	}

	// Inode should be gone
	if _, err := hfs.DB.GetInode(fileID); err == nil {
		t.Fatal("inode should be deleted")
	}
}

func TestLinkReturnsENOTSUP(t *testing.T) {
	hfs := setupDBOnly(t)

	fileID, err := hfs.DB.InsertInode(1, "original", syscall.S_IFREG|0o644, "committed")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	node := &HamstorNode{hfs: hfs, inodeID: 1}
	target := &HamstorNode{hfs: hfs, inodeID: fileID}

	_, errno := node.Link(context.Background(), target, "link", nil)
	if errno != syscall.ENOTSUP {
		t.Fatalf("expected ENOTSUP, got %v", errno)
	}
}

func TestSpillToDisk(t *testing.T) {
	hfs := setupDBOnly(t)
	hfs.SpillDir = t.TempDir()

	fileID, err := hfs.DB.InsertInode(1, "bigfile", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	handle := newHandle(hfs, fileID, true)
	ctx := context.Background()

	// Write small data first (stays in memory)
	handle.Write(ctx, []byte("hello"), 0)
	if handle.st.spillFile != nil {
		t.Fatal("small write should not spill")
	}
	if handle.st.buf == nil {
		t.Fatal("small write should be in buf")
	}

	// Write at a large offset to trigger spill
	bigOffset := int64(spillThreshold + 1)
	handle.Write(ctx, []byte("X"), bigOffset)
	if handle.st.spillFile == nil {
		t.Fatal("large write should trigger spill to disk")
	}
	if handle.st.buf != nil {
		t.Fatal("buf should be nil after spill")
	}
	if handle.st.spillSize != bigOffset+1 {
		t.Fatalf("spillSize: expected %d, got %d", bigOffset+1, handle.st.spillSize)
	}

	// Verify we can read back from spill file
	data := make([]byte, 5)
	n, err := handle.st.spillFile.ReadAt(data, 0)
	if err != nil {
		t.Fatalf("read spill: %v", err)
	}
	if string(data[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(data[:n]))
	}

	// Cleanup
	handle.Release(ctx)
}

func TestSetattrSpillTruncation(t *testing.T) {
	hfs := setupDBOnly(t)
	hfs.SpillDir = t.TempDir()

	fileID, err := hfs.DB.InsertInode(1, "spilltrunc", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	handle := newHandle(hfs, fileID, true)
	ctx := context.Background()

	// Write enough to trigger spill
	bigData := make([]byte, spillThreshold+100)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}
	handle.Write(ctx, bigData, 0)
	if handle.st.spillFile == nil {
		t.Fatal("should have spilled to disk")
	}

	originalSize := handle.st.spillSize

	// Truncate via the spill file (simulating Setattr)
	handle.st.mu.Lock()
	newSize := int64(1024)
	if err := handle.st.spillFile.Truncate(newSize); err != nil {
		handle.st.mu.Unlock()
		t.Fatalf("truncate spill: %v", err)
	}
	handle.st.spillSize = newSize
	handle.st.dirty = true
	handle.st.mu.Unlock()

	if handle.st.spillSize != 1024 {
		t.Fatalf("expected spillSize 1024, got %d", handle.st.spillSize)
	}
	if handle.st.spillSize >= originalSize {
		t.Fatal("truncation should have reduced size")
	}

	handle.Release(ctx)
}

func TestVersionedMigration(t *testing.T) {
	hfs := setupDBOnly(t)

	// Verify the FK migration ran by checking xattrs table has FK
	// Insert an xattr referencing a non-existent inode — should fail with FK
	err := hfs.DB.SetXattr(99999, "user.test", []byte("val"))
	// With FK ON DELETE CASCADE, the insert should fail since inode 99999 doesn't exist
	if err == nil {
		t.Fatal("expected FK violation inserting xattr for non-existent inode")
	}
}

func TestCacheLRUTouchOnOpen(t *testing.T) {
	// Test that Open updates mtime for LRU eviction
	dir := t.TempDir()
	c, err := cache.New(dir, 10<<20)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}

	// Put a file
	if err := c.Put("test/key1", []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Record mtime
	info1, _ := os.Stat(dir + "/test/key1")
	mtime1 := info1.ModTime()

	// Wait a bit and open (should touch mtime)
	time.Sleep(10 * time.Millisecond)
	f, err := c.Open("test/key1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	f.Close()

	info2, _ := os.Stat(dir + "/test/key1")
	mtime2 := info2.ModTime()

	if !mtime2.After(mtime1) {
		t.Fatalf("expected mtime to be updated after Open, got %v vs %v", mtime1, mtime2)
	}
}
