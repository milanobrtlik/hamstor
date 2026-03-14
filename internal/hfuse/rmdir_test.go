package hfuse

import (
	"context"
	"syscall"
	"testing"
)

func TestRmdirRecursive(t *testing.T) {
	hfs, _ := setupTest(t)
	ctx := context.Background()

	// Create dir with files
	dirID, err := hfs.DB.InsertInode(1, "mydir", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}

	// Create files inside dir, upload to S3
	for i, name := range []string{"a.txt", "b.txt", "c.txt"} {
		fileID, err := hfs.DB.InsertInode(dirID, name, syscall.S_IFREG|0o644, "pending")
		if err != nil {
			t.Fatalf("insert file %d: %v", i, err)
		}

		handle := NewTestHandle(hfs, fileID, true)
		handle.TestWrite([]byte("content-" + name))
		handle.TestFlush()
		handle.WaitUpload()
	}

	// Verify files are committed
	children, err := hfs.DB.ListAllChildren(dirID)
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if len(children) != 3 {
		t.Fatalf("expected 3 children, got %d", len(children))
	}

	// Collect S3 keys before deletion
	var s3Keys []string
	for _, c := range children {
		if c.S3Key != "" {
			s3Keys = append(s3Keys, c.S3Key)
		}
	}

	// Delete dir recursively
	if err := deleteTree(ctx, hfs, dirID); err != nil {
		t.Fatalf("deleteTree: %v", err)
	}

	// Verify: no children in DB
	remaining, err := hfs.DB.ListAllChildren(dirID)
	if err != nil {
		t.Fatalf("list children after: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 children, got %d", len(remaining))
	}

	// Verify: dir itself is gone
	_, err = hfs.DB.GetInode(dirID)
	if err == nil {
		t.Fatal("expected dir inode to be deleted")
	}

	// Verify: S3 objects are deleted
	for _, key := range s3Keys {
		_, err := hfs.Store.Download(ctx, key)
		if err == nil {
			t.Fatalf("expected S3 key %s to be deleted", key)
		}
	}
}

func TestRmdirNestedDirs(t *testing.T) {
	hfs, _ := setupTest(t)
	ctx := context.Background()

	// Create nested structure: top/sub/file.txt
	topID, err := hfs.DB.InsertInode(1, "top", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert top: %v", err)
	}
	subID, err := hfs.DB.InsertInode(topID, "sub", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert sub: %v", err)
	}
	fileID, err := hfs.DB.InsertInode(subID, "file.txt", syscall.S_IFREG|0o644, "pending")
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	handle := NewTestHandle(hfs, fileID, true)
	handle.TestWrite([]byte("nested content"))
	handle.TestFlush()
	handle.WaitUpload()

	// Get S3 key
	meta, err := hfs.DB.GetInode(fileID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	s3Key := meta.S3Key

	// Delete top recursively
	if err := deleteTree(ctx, hfs, topID); err != nil {
		t.Fatalf("deleteTree: %v", err)
	}

	// Verify: all gone from DB
	for _, id := range []int64{topID, subID, fileID} {
		_, err := hfs.DB.GetInode(id)
		if err == nil {
			t.Fatalf("expected inode %d to be deleted", id)
		}
	}

	// Verify: S3 object gone
	if s3Key != "" {
		_, err := hfs.Store.Download(ctx, s3Key)
		if err == nil {
			t.Fatal("expected S3 object to be deleted")
		}
	}
}

func TestRmdirEmptyDir(t *testing.T) {
	hfs, _ := setupTest(t)
	ctx := context.Background()

	dirID, err := hfs.DB.InsertInode(1, "empty", syscall.S_IFDIR|0o755, "committed")
	if err != nil {
		t.Fatalf("insert dir: %v", err)
	}

	if err := deleteTree(ctx, hfs, dirID); err != nil {
		t.Fatalf("deleteTree: %v", err)
	}

	_, err = hfs.DB.GetInode(dirID)
	if err == nil {
		t.Fatal("expected dir to be deleted")
	}
}
