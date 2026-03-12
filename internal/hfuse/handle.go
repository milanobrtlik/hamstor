package hfuse

import (
	"context"
	"log"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/thumb"
)

type HamstorHandle struct {
	hfs     *HamstorFS
	inodeID int64
	mu      sync.Mutex
	buf     []byte
	dirty   bool
	loaded  bool
	isNew   bool
}

var (
	_ fs.FileReader   = (*HamstorHandle)(nil)
	_ fs.FileWriter   = (*HamstorHandle)(nil)
	_ fs.FileFlusher  = (*HamstorHandle)(nil)
	_ fs.FileReleaser = (*HamstorHandle)(nil)
	_ fs.FileFsyncer  = (*HamstorHandle)(nil)
)

func (h *HamstorHandle) ensureLoaded(ctx context.Context) syscall.Errno {
	if h.loaded {
		return 0
	}
	if h.isNew {
		h.buf = []byte{}
		h.loaded = true
		return 0
	}

	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		return toErrno(err)
	}
	if meta.S3Key == "" {
		h.buf = []byte{}
		h.loaded = true
		return 0
	}

	data, err := h.hfs.Store.Download(ctx, meta.S3Key)
	if err != nil {
		return toErrno(err)
	}
	h.buf = data
	h.loaded = true
	return 0
}

func (h *HamstorHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return nil, errno
	}

	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return fuse.ReadResultData(h.buf[off:end]), 0
}

func (h *HamstorHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if errno := h.ensureLoaded(ctx); errno != 0 {
		return 0, errno
	}

	end := off + int64(len(data))
	if end > int64(len(h.buf)) {
		h.buf = append(h.buf, make([]byte, end-int64(len(h.buf)))...)
	}
	copy(h.buf[off:], data)
	h.dirty = true
	return uint32(len(data)), 0
}

func (h *HamstorHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		if h.isNew {
			if err := h.hfs.DB.CommitInode(h.inodeID, "", 0); err != nil {
				return toErrno(err)
			}
			h.isNew = false
		}
		return 0
	}

	// Get current meta to find old s3_key
	meta, err := h.hfs.DB.GetInode(h.inodeID)
	if err != nil {
		return toErrno(err)
	}
	oldKey := meta.S3Key

	// Generate new S3 key and upload
	newKey := uuid.New().String()
	if err := h.hfs.Store.Upload(ctx, newKey, h.buf); err != nil {
		log.Printf("hamstor: flush upload failed: %v", err)
		return syscall.EIO
	}

	// Test hook: simulate crash between S3 upload and SQLite commit
	if h.hfs.TestCrashBeforeCommit != nil {
		h.hfs.TestCrashBeforeCommit()
	}

	// Commit to SQLite
	if err := h.hfs.DB.CommitInode(h.inodeID, newKey, int64(len(h.buf))); err != nil {
		log.Printf("hamstor: flush commit failed: %v", err)
		// Best-effort cleanup of the uploaded object
		if delErr := h.hfs.Store.Delete(ctx, newKey); delErr != nil {
			log.Printf("hamstor: flush cleanup failed: %v", delErr)
		}
		return syscall.EIO
	}

	// Clean up old S3 object if it changed
	if oldKey != "" && oldKey != newKey {
		if err := h.hfs.Store.Delete(ctx, oldKey); err != nil {
			log.Printf("hamstor: flush delete old key %s: %v", oldKey, err)
		}
	}

	// Async thumbnail generation for image files
	if thumb.IsImageExt(meta.Name) {
		if relPath, pathErr := h.hfs.DB.InodePath(h.inodeID); pathErr == nil {
			updated, err2 := h.hfs.DB.GetInode(h.inodeID)
			if err2 == nil {
				imgData := make([]byte, len(h.buf))
				copy(imgData, h.buf)
				go thumb.Generate(h.hfs.Mountpoint, relPath, updated.MtimeNs/1e9, imgData)
			}
		}
	}

	h.dirty = false
	h.isNew = false
	return 0
}

func (h *HamstorHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.Flush(ctx)
}

func (h *HamstorHandle) Release(ctx context.Context) syscall.Errno {
	if errno := h.Flush(ctx); errno != 0 {
		log.Printf("hamstor: release flush failed for inode %d", h.inodeID)
	}

	h.mu.Lock()
	h.buf = nil
	h.mu.Unlock()
	return 0
}
