package hfuse

import (
	"context"
	"fmt"
	"log"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/s3store"
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

	// Async upload state
	uploadDone chan struct{}
	uploadErr  error
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
	if h.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
		data, err = h.hfs.Encryptor.Decrypt(data)
		if err != nil {
			log.Printf("hamstor: decrypt failed for inode %d: %v", h.inodeID, err)
			return syscall.EIO
		}
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
			if _, err := h.hfs.DB.CommitInode(h.inodeID, "", 0); err != nil {
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
	fileName := meta.Name

	// Generate new S3 key
	newKey := s3store.NewKey()
	bufCopy := make([]byte, len(h.buf))
	copy(bufCopy, h.buf)
	bufSize := int64(len(h.buf))

	// Keep plaintext for thumbnail generation
	plainBuf := bufCopy

	// Encrypt before upload
	if h.hfs.Encryptor != nil {
		encrypted, encErr := h.hfs.Encryptor.Encrypt(bufCopy)
		if encErr != nil {
			log.Printf("hamstor: encrypt failed for inode %d: %v", h.inodeID, encErr)
			return syscall.EIO
		}
		bufCopy = encrypted
	}

	// Capture state for async upload
	hfs := h.hfs
	inodeID := h.inodeID

	h.uploadDone = make(chan struct{})

	go func() {
		defer close(h.uploadDone)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("hamstor: async upload panic: %v", r)
				h.uploadErr = fmt.Errorf("panic: %v", r)
			}
		}()

		uploadCtx := context.Background()

		if err := hfs.Store.Upload(uploadCtx, newKey, bufCopy); err != nil {
			log.Printf("hamstor: async upload failed: %v", err)
			h.uploadErr = err
			return
		}

		// Test hook: simulate crash between S3 upload and SQLite commit
		if hfs.TestCrashBeforeCommit != nil {
			hfs.TestCrashBeforeCommit()
		}

		committed, err := hfs.DB.CommitInode(inodeID, newKey, bufSize)
		if err != nil {
			log.Printf("hamstor: async commit failed: %v", err)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: async cleanup failed: %v", delErr)
			}
			h.uploadErr = err
			return
		}
		if !committed {
			log.Printf("hamstor: inode %d deleted during upload, cleaning up S3 key %s", inodeID, newKey)
			if delErr := hfs.Store.Delete(uploadCtx, newKey); delErr != nil {
				log.Printf("hamstor: orphan cleanup failed: %v", delErr)
			}
			return
		}

		// Clean up old S3 object if it changed
		if oldKey != "" && oldKey != newKey {
			if err := hfs.Store.Delete(uploadCtx, oldKey); err != nil {
				log.Printf("hamstor: flush delete old key %s: %v", oldKey, err)
			}
		}

		// Async thumbnail generation for image files
		if thumb.IsImageExt(fileName) {
			if relPath, pathErr := hfs.DB.InodePath(inodeID); pathErr == nil {
				updated, err2 := hfs.DB.GetInode(inodeID)
				if err2 == nil {
					go thumb.Generate(hfs.Mountpoint, relPath, updated.MtimeNs/1e9, plainBuf)
				}
			}
		}
	}()

	h.dirty = false
	h.isNew = false
	return 0
}

func (h *HamstorHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	errno := h.Flush(ctx)
	if errno != 0 {
		return errno
	}
	// Wait for async upload to complete — fsync guarantees durability
	h.mu.Lock()
	done := h.uploadDone
	h.mu.Unlock()
	if done != nil {
		<-done
		if h.uploadErr != nil {
			return syscall.EIO
		}
	}
	return 0
}

func (h *HamstorHandle) Release(ctx context.Context) syscall.Errno {
	// Wait for any pending async upload
	h.mu.Lock()
	done := h.uploadDone
	h.mu.Unlock()

	if done != nil {
		<-done
		if h.uploadErr != nil {
			log.Printf("hamstor: release: async upload failed for inode %d: %v", h.inodeID, h.uploadErr)
		}
	}

	h.mu.Lock()
	h.buf = nil
	h.mu.Unlock()
	return 0
}
