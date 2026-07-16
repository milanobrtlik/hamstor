package hfuse

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/media"
	"github.com/milan/hamstor/internal/ratelimit"
	"github.com/milan/hamstor/internal/thumb"
	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type HamstorNode struct {
	fs.Inode
	hfs     *HamstorFS
	inodeID int64
}

var (
	_ fs.NodeGetattrer     = (*HamstorNode)(nil)
	_ fs.NodeSetattrer     = (*HamstorNode)(nil)
	_ fs.NodeLookuper      = (*HamstorNode)(nil)
	_ fs.NodeReaddirer     = (*HamstorNode)(nil)
	_ fs.NodeMkdirer       = (*HamstorNode)(nil)
	_ fs.NodeCreater       = (*HamstorNode)(nil)
	_ fs.NodeOpener        = (*HamstorNode)(nil)
	_ fs.NodeUnlinker      = (*HamstorNode)(nil)
	_ fs.NodeRmdirer       = (*HamstorNode)(nil)
	_ fs.NodeRenamer       = (*HamstorNode)(nil)
	_ fs.NodeStatfser      = (*HamstorNode)(nil)
	_ fs.NodeSymlinker     = (*HamstorNode)(nil)
	_ fs.NodeReadlinker    = (*HamstorNode)(nil)
	_ fs.NodeGetxattrer    = (*HamstorNode)(nil)
	_ fs.NodeSetxattrer    = (*HamstorNode)(nil)
	_ fs.NodeRemovexattrer = (*HamstorNode)(nil)
	_ fs.NodeListxattrer   = (*HamstorNode)(nil)
	_ fs.NodeLinker        = (*HamstorNode)(nil)
)

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, sql.ErrNoRows) {
		return syscall.ENOENT
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		code := sqliteErr.Code()
		if code == sqlite3.SQLITE_CONSTRAINT_UNIQUE || code == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
			return syscall.EEXIST
		}
	}
	log.Printf("hamstor: %v", err)
	return syscall.EIO
}

func fillAttr(meta *db.InodeMeta, out *fuse.Attr) {
	out.Ino = uint64(meta.ID)
	out.Mode = meta.Mode
	out.Size = uint64(meta.Size)
	out.Uid = meta.Uid
	out.Gid = meta.Gid
	out.Mtime = uint64(meta.MtimeNs / 1e9)
	out.Mtimensec = uint32(meta.MtimeNs % 1e9)
	out.Ctime = uint64(meta.CtimeNs / 1e9)
	out.Ctimensec = uint32(meta.CtimeNs % 1e9)
	out.Atime = out.Mtime
	out.Atimensec = out.Mtimensec
	if meta.Mode&syscall.S_IFDIR != 0 {
		out.Nlink = 2
	} else {
		out.Nlink = 1
	}
}

func (n *HamstorNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.hfs.DB.GetInode(n.inodeID)
	if err != nil {
		return toErrno(err)
	}
	fillAttr(meta, &out.Attr)
	return 0
}

func (n *HamstorNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var sizePtr *int64
	var modePtr *uint32
	var mtimePtr *int64

	if sz, ok := in.GetSize(); ok {
		s := int64(sz)
		sizePtr = &s

		// Truncate the active file handle's data if one is provided, so the
		// resize is captured and re-uploaded on Flush.
		if fh, ok := f.(*HamstorHandle); ok {
			fh.mu.Lock()
			if fh.spillFile != nil {
				if err := fh.spillFile.Truncate(s); err != nil {
					fh.mu.Unlock()
					log.Printf("hamstor: spill truncate failed: %v", err)
					return syscall.EIO
				}
				fh.spillSize = s
				fh.dirty = true
			} else {
				// A cache-backed (read) handle holds no mutable buffer. Materialize
				// it into buf so the truncation is durable on the next Flush
				// instead of only changing the DB size while storage keeps the old
				// bytes (which a later rewrite/append would then resurrect).
				if fh.cacheFile != nil {
					if info, statErr := fh.cacheFile.Stat(); statErr == nil {
						b := make([]byte, info.Size())
						if _, rerr := fh.cacheFile.ReadAt(b, 0); rerr == nil || rerr == io.EOF {
							fh.buf = b
							fh.cacheFile.Close()
							fh.cacheFile = nil
							fh.loaded = true
						}
					}
				}
				if fh.loaded && fh.cacheFile == nil {
					if s < int64(len(fh.buf)) {
						fh.buf = fh.buf[:s]
					} else if s > int64(len(fh.buf)) {
						fh.buf = append(fh.buf, make([]byte, s-int64(len(fh.buf)))...)
					}
					fh.dirty = true
				}
			}
			fh.fileSize = s
			fh.mu.Unlock()
		}
	}
	if m, ok := in.GetMode(); ok {
		meta, err := n.hfs.DB.GetInode(n.inodeID)
		if err != nil {
			return toErrno(err)
		}
		merged := (meta.Mode & syscall.S_IFMT) | (m & ^uint32(syscall.S_IFMT))
		modePtr = &merged
	}
	if mt, ok := in.GetMTime(); ok {
		ns := mt.UnixNano()
		mtimePtr = &ns
	}

	if err := n.hfs.DB.SetAttr(n.inodeID, sizePtr, modePtr, mtimePtr); err != nil {
		return toErrno(err)
	}

	// Handle chown
	if uid, ok := in.GetUID(); ok {
		if err := n.hfs.DB.SetOwner(n.inodeID, &uid, nil); err != nil {
			return toErrno(err)
		}
	}
	if gid, ok := in.GetGID(); ok {
		if err := n.hfs.DB.SetOwner(n.inodeID, nil, &gid); err != nil {
			return toErrno(err)
		}
	}

	return n.Getattr(ctx, f, out)
}

func (n *HamstorNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return nil, toErrno(err)
	}

	child := &HamstorNode{hfs: n.hfs, inodeID: meta.ID}
	stable := fs.StableAttr{Mode: meta.Mode, Ino: uint64(meta.ID)}
	inode := n.NewInode(ctx, child, stable)

	fillAttr(meta, &out.Attr)
	return inode, 0
}

func (n *HamstorNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	children, err := n.hfs.DB.ListChildren(n.inodeID)
	if err != nil {
		return nil, toErrno(err)
	}

	entries := make([]fuse.DirEntry, len(children))
	for i, c := range children {
		entries[i] = fuse.DirEntry{
			Mode: c.Mode,
			Name: c.Name,
			Ino:  uint64(c.ID),
		}
	}
	return fs.NewListDirStream(entries), 0
}

func (n *HamstorNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	caller, ok := fuse.FromContext(ctx)
	uid, gid := n.hfs.DefaultUid, n.hfs.DefaultGid
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	dirMode := mode | syscall.S_IFDIR
	newID, err := n.hfs.DB.InsertInodeWithOwner(n.inodeID, name, dirMode, "committed", uid, gid)
	if err != nil {
		return nil, toErrno(err)
	}

	child := &HamstorNode{hfs: n.hfs, inodeID: newID}
	stable := fs.StableAttr{Mode: dirMode, Ino: uint64(newID)}
	inode := n.NewInode(ctx, child, stable)

	out.Ino = uint64(newID)
	out.Mode = dirMode
	out.Uid = uid
	out.Gid = gid
	out.Nlink = 2
	return inode, 0
}

func (n *HamstorNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	caller, ok := fuse.FromContext(ctx)
	uid, gid := n.hfs.DefaultUid, n.hfs.DefaultGid
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	fileMode := mode | syscall.S_IFREG
	newID, err := n.hfs.DB.InsertInodeWithOwner(n.inodeID, name, fileMode, "pending", uid, gid)
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}

	child := &HamstorNode{hfs: n.hfs, inodeID: newID}
	stable := fs.StableAttr{Mode: fileMode, Ino: uint64(newID)}
	node := n.NewInode(ctx, child, stable)

	handle := &HamstorHandle{
		hfs:     n.hfs,
		inodeID: newID,
		isNew:   true,
	}

	out.Ino = uint64(newID)
	out.Mode = fileMode
	out.Uid = uid
	out.Gid = gid
	out.Nlink = 1
	return node, handle, fuse.FOPEN_DIRECT_IO, 0
}

func (n *HamstorNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	meta, err := n.hfs.DB.GetInode(n.inodeID)
	if err != nil {
		return nil, 0, toErrno(err)
	}

	handle := &HamstorHandle{
		hfs:      n.hfs,
		inodeID:  n.inodeID,
		s3Key:    meta.S3Key,
		fileSize: meta.Size,
	}

	// Preload data for files opened in write mode
	writeFlags := uint32(syscall.O_WRONLY | syscall.O_RDWR | syscall.O_APPEND | syscall.O_TRUNC)
	hasData := meta.S3Key != "" || meta.VolS3Key != "" ||
		(meta.Size > 0 && n.hfs.VolumeBuilder != nil)
	if flags&writeFlags != 0 && hasData {
		if flags&uint32(syscall.O_TRUNC) != 0 {
			handle.buf = []byte{}
			handle.loaded = true
			handle.dirty = true
		} else if meta.S3Key != "" {
			// Try cache first for write preload
			var data []byte
			if n.hfs.Cache != nil {
				if f, cacheErr := n.hfs.Cache.Open(meta.S3Key); cacheErr == nil {
					info, _ := f.Stat()
					data = make([]byte, info.Size())
					f.ReadAt(data, 0)
					f.Close()
				}
			}
			if data == nil {
				var dlErr error
				data, dlErr = n.hfs.Store.Download(ctx, meta.S3Key)
				if dlErr != nil {
					return nil, 0, toErrno(dlErr)
				}
				if n.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
					data, dlErr = n.hfs.Encryptor.Decrypt(data)
					if dlErr != nil {
						log.Printf("hamstor: decrypt failed for inode %d: %v", n.inodeID, dlErr)
						return nil, 0, syscall.EIO
					}
				}
				if n.hfs.Cache != nil {
					if putErr := n.hfs.Cache.Put(meta.S3Key, data); putErr != nil {
						log.Printf("hamstor: cache put on open: %v", putErr)
					}
				}
			}
			// Spill large preloads to disk to avoid OOM
			if int64(len(data)) > spillThreshold {
				sf, sfErr := os.CreateTemp(n.hfs.SpillDir, "hamstor-spill-*")
				if sfErr != nil {
					return nil, 0, syscall.EIO
				}
				if _, sfErr = sf.Write(data); sfErr != nil {
					sf.Close()
					os.Remove(sf.Name())
					return nil, 0, syscall.EIO
				}
				handle.spillFile = sf
				handle.spillSize = int64(len(data))
			} else {
				handle.buf = data
			}
			handle.loaded = true
		} else if meta.VolS3Key != "" {
			// Preload from volume (file is packed in a volume S3 object)
			data, dlErr := n.hfs.Store.DownloadRange(ctx, meta.VolS3Key, meta.VolOffset, meta.VolSize)
			if dlErr != nil {
				return nil, 0, toErrno(dlErr)
			}
			if n.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
				data, dlErr = n.hfs.Encryptor.Decrypt(data)
				if dlErr != nil {
					log.Printf("hamstor: decrypt failed for inode %d: %v", n.inodeID, dlErr)
					return nil, 0, syscall.EIO
				}
			}
			handle.buf = data
			handle.loaded = true
		} else if n.hfs.VolumeBuilder != nil {
			// Preload from staging dir (file staged but not yet packed)
			data, dlErr := n.openPreloadStaged(ctx)
			if dlErr != 0 {
				return nil, 0, dlErr
			}
			handle.buf = data
			handle.loaded = true
		}

		// If the stored object is longer than the inode's logical size — e.g. a
		// prior truncate() shrank the file without rewriting storage — clamp the
		// preloaded data to meta.Size so a subsequent rewrite or append does not
		// resurrect the truncated tail. (Not applied for O_TRUNC, which loads an
		// empty buffer.)
		if handle.loaded && flags&uint32(syscall.O_TRUNC) == 0 {
			if handle.spillFile != nil {
				if handle.spillSize > meta.Size {
					if err := handle.spillFile.Truncate(meta.Size); err == nil {
						handle.spillSize = meta.Size
					}
				}
			} else if int64(len(handle.buf)) > meta.Size {
				handle.buf = handle.buf[:meta.Size]
			}
		}
	}

	// Enable streaming mode for multimedia files opened read-only (separate from
	// write block above). Disabled under encryption: stored objects are a single
	// whole-file AES-256-GCM blob, so a byte-range slice is undecryptable
	// ciphertext. Encrypted media instead falls through to the full-download +
	// decrypt path in ensureLoaded.
	if flags&writeFlags == 0 && n.hfs.Encryptor == nil && media.IsMediaExt(meta.Name) && n.hfs.StreamRate > 0 {
		rateBps := float64(n.hfs.StreamRate) * (1 << 20) // MB/s to bytes/s
		burstBytes := float64(n.hfs.StreamBuffer) * (1 << 20)
		handle.streaming = true
		handle.rateLimiter = ratelimit.New(rateBps, burstBytes)
		handle.streamChunksCap = n.hfs.StreamBuffer * (1 << 20) / cache.ChunkSize
		if handle.streamChunksCap < 4 {
			handle.streamChunksCap = 4
		}
	}

	return handle, fuse.FOPEN_DIRECT_IO, 0
}

// openPreloadStaged loads data from a staging file for write preloading.
// Falls back to volume if the builder packed the file between GetInode and read.
func (n *HamstorNode) openPreloadStaged(ctx context.Context) ([]byte, syscall.Errno) {
	stagePath := n.hfs.VolumeBuilder.StagePath(n.inodeID)
	data, err := os.ReadFile(stagePath)
	if err != nil {
		// Builder (.packing) or Fsync (.flushing) may have claimed the file.
		for _, suffix := range []string{".packing", ".flushing"} {
			if d, e := os.ReadFile(stagePath + suffix); e == nil {
				data = d
				err = nil
				break
			}
		}
	}
	if err != nil {
		// Staging file may have been packed by the builder. Re-read metadata.
		meta2, metaErr := n.hfs.DB.GetInode(n.inodeID)
		if metaErr != nil {
			return nil, toErrno(metaErr)
		}
		if meta2.VolS3Key != "" {
			data, err = n.hfs.Store.DownloadRange(ctx, meta2.VolS3Key, meta2.VolOffset, meta2.VolSize)
			if err != nil {
				return nil, toErrno(err)
			}
		} else {
			log.Printf("hamstor: staged file read failed for inode %d: %v", n.inodeID, err)
			return nil, syscall.EIO
		}
	}
	if n.hfs.Encryptor != nil && crypto.IsEncrypted(data) {
		data, err = n.hfs.Encryptor.Decrypt(data)
		if err != nil {
			log.Printf("hamstor: decrypt failed for inode %d: %v", n.inodeID, err)
			return nil, syscall.EIO
		}
	}
	return data, 0
}

func (n *HamstorNode) Unlink(ctx context.Context, name string) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	if thumb.IsImageExt(meta.Name) {
		if relPath, err := n.hfs.DB.InodePath(meta.ID); err == nil {
			go func() {
				n.hfs.ThumbSem <- struct{}{}
				defer func() { <-n.hfs.ThumbSem }()
				thumb.RemoveThumbnails(n.hfs.Mountpoint, relPath)
			}()
		}
	}

	if meta.S3Key != "" {
		if n.hfs.Cache != nil {
			n.hfs.Cache.Evict(meta.S3Key)
		}
		if err := n.hfs.Store.Delete(ctx, meta.S3Key); err != nil {
			log.Printf("hamstor: unlink s3 delete %s: %v", meta.S3Key, err)
		}
	} else if meta.VolS3Key == "" && meta.Size > 0 && n.hfs.VolumeBuilder != nil {
		// File staged but not yet packed — remove staging file
		os.Remove(n.hfs.VolumeBuilder.StagePath(meta.ID))
	}

	if err := n.hfs.DB.DeleteInodeWithVolume(meta.ID, meta.VolS3Key); err != nil {
		return toErrno(err)
	}
	return 0
}

func (n *HamstorNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	children, err := n.hfs.DB.ListAllChildren(meta.ID)
	if err != nil {
		return toErrno(err)
	}
	if len(children) > 0 {
		return syscall.ENOTEMPTY
	}

	if err := n.hfs.DB.DeleteInode(meta.ID); err != nil {
		return toErrno(err)
	}
	return 0
}

func deleteTree(ctx context.Context, hfs *HamstorFS, dirID int64) error {
	children, err := hfs.DB.ListAllChildren(dirID)
	if err != nil {
		return err
	}

	for _, child := range children {
		if child.Mode&syscall.S_IFDIR != 0 {
			if err := deleteTree(ctx, hfs, child.ID); err != nil {
				return err
			}
		} else {
			if child.S3Key != "" {
				if hfs.Cache != nil {
					hfs.Cache.Evict(child.S3Key)
				}
				if err := hfs.Store.Delete(ctx, child.S3Key); err != nil {
					log.Printf("hamstor: rmdir delete s3 %s: %v", child.S3Key, err)
				}
			} else if child.VolS3Key == "" && child.Size > 0 && hfs.VolumeBuilder != nil {
				os.Remove(hfs.VolumeBuilder.StagePath(child.ID))
			}
			if thumb.IsImageExt(child.Name) {
				if relPath, pathErr := hfs.DB.InodePath(child.ID); pathErr == nil {
					go func() {
						hfs.ThumbSem <- struct{}{}
						defer func() { <-hfs.ThumbSem }()
						thumb.RemoveThumbnails(hfs.Mountpoint, relPath)
					}()
				}
			}
			if err := hfs.DB.DeleteInodeWithVolume(child.ID, child.VolS3Key); err != nil {
				return err
			}
		}
	}

	return hfs.DB.DeleteInode(dirID)
}

func (n *HamstorNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stats, err := n.hfs.DB.GetFSStats()
	if err != nil {
		// Fallback to basic values
		out.Bsize = 4096
		out.NameLen = 255
		out.Frsize = 4096
		return 0
	}

	out.Bsize = 4096
	out.Frsize = 4096
	out.NameLen = 255

	// Report files tracked in DB
	totalInodes := uint64(stats.FileCount + stats.DirCount)
	out.Files = totalInodes
	out.Ffree = ^uint64(0) - totalInodes // effectively unlimited

	// Report blocks based on total size
	totalBlocks := uint64(stats.TotalSize) / 4096
	if uint64(stats.TotalSize)%4096 != 0 {
		totalBlocks++
	}
	out.Blocks = totalBlocks + 1<<20 // report some headroom
	out.Bfree = 1 << 20              // report ~4 GB free (S3 is elastic)
	out.Bavail = out.Bfree

	return 0
}

func (n *HamstorNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	newParentNode := newParent.EmbeddedInode().Operations().(*HamstorNode)

	// Honor renameat2 flags. RENAME_EXCHANGE requires an atomic two-inode swap,
	// which the DB layer does not implement — refuse it rather than fall through
	// to the unconditional destroy-then-rename path below (which would delete the
	// target's data and leave the kernel inode tree inconsistent with the DB).
	// RENAME_NOREPLACE must fail with EEXIST if the target exists instead of
	// silently overwriting it. Returning non-zero also makes go-fuse skip its
	// post-op ExchangeChild, keeping the kernel tree consistent.
	const renameNoreplace = 0x1 // unix.RENAME_NOREPLACE
	if flags&fs.RENAME_EXCHANGE != 0 {
		return syscall.ENOSYS
	}
	if flags&renameNoreplace != 0 {
		if _, lerr := n.hfs.DB.LookupChild(newParentNode.inodeID, newName); lerr == nil {
			return syscall.EEXIST
		} else if !errors.Is(lerr, sql.ErrNoRows) {
			return toErrno(lerr)
		}
	}

	// Prevent moving a directory into itself or a descendant (cycle detection)
	if meta.Mode&syscall.S_IFDIR != 0 && newParentNode.inodeID != n.inodeID {
		current := newParentNode.inodeID
		for current > 1 {
			if current == meta.ID {
				return syscall.EINVAL
			}
			parent, err := n.hfs.DB.GetInode(current)
			if err != nil {
				return toErrno(err)
			}
			current = parent.ParentID
		}
	}

	if thumb.IsImageExt(meta.Name) {
		if oldRelPath, err := n.hfs.DB.InodePath(meta.ID); err == nil {
			go func() {
				n.hfs.ThumbSem <- struct{}{}
				defer func() { <-n.hfs.ThumbSem }()
				thumb.RemoveThumbnails(n.hfs.Mountpoint, oldRelPath)
			}()
		}
	}

	// Check if target exists -- if so, remove it
	existing, err := n.hfs.DB.LookupChild(newParentNode.inodeID, newName)
	if err == nil {
		// If target is a directory, it must be empty (POSIX)
		if existing.Mode&syscall.S_IFDIR != 0 {
			children, childErr := n.hfs.DB.ListAllChildren(existing.ID)
			if childErr != nil {
				return toErrno(childErr)
			}
			if len(children) > 0 {
				return syscall.ENOTEMPTY
			}
		}
		if thumb.IsImageExt(existing.Name) {
			if existingPath, err := n.hfs.DB.InodePath(existing.ID); err == nil {
				go func() {
					n.hfs.ThumbSem <- struct{}{}
					defer func() { <-n.hfs.ThumbSem }()
					thumb.RemoveThumbnails(n.hfs.Mountpoint, existingPath)
				}()
			}
		}
		if existing.S3Key != "" {
			if n.hfs.Cache != nil {
				n.hfs.Cache.Evict(existing.S3Key)
			}
			if delErr := n.hfs.Store.Delete(ctx, existing.S3Key); delErr != nil {
				log.Printf("hamstor: rename delete old s3 %s: %v", existing.S3Key, delErr)
			}
		} else if existing.VolS3Key == "" && existing.Size > 0 && n.hfs.VolumeBuilder != nil {
			os.Remove(n.hfs.VolumeBuilder.StagePath(existing.ID))
		}
		if delErr := n.hfs.DB.DeleteInodeWithVolume(existing.ID, existing.VolS3Key); delErr != nil {
			return toErrno(delErr)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return toErrno(err)
	}

	if err := n.hfs.DB.RenameInode(meta.ID, newParentNode.inodeID, newName); err != nil {
		return toErrno(err)
	}
	return 0
}

// --- Symlinks ---

func (n *HamstorNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	caller, ok := fuse.FromContext(ctx)
	uid, gid := n.hfs.DefaultUid, n.hfs.DefaultGid
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	newID, err := n.hfs.DB.InsertSymlink(n.inodeID, name, target, uid, gid)
	if err != nil {
		return nil, toErrno(err)
	}

	meta, err := n.hfs.DB.GetInode(newID)
	if err != nil {
		return nil, toErrno(err)
	}

	child := &HamstorNode{hfs: n.hfs, inodeID: newID}
	stable := fs.StableAttr{Mode: meta.Mode, Ino: uint64(newID)}
	inode := n.NewInode(ctx, child, stable)

	fillAttr(meta, &out.Attr)
	return inode, 0
}

func (n *HamstorNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	meta, err := n.hfs.DB.GetInode(n.inodeID)
	if err != nil {
		return nil, toErrno(err)
	}
	if meta.SymlinkTarget == "" {
		return nil, syscall.EINVAL
	}
	return []byte(meta.SymlinkTarget), 0
}

// --- Extended Attributes ---

func (n *HamstorNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	val, err := n.hfs.DB.GetXattr(n.inodeID, attr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, syscall.ENODATA
		}
		return 0, toErrno(err)
	}
	if len(dest) == 0 {
		return uint32(len(val)), 0
	}
	if len(val) > len(dest) {
		return 0, syscall.ERANGE
	}
	copy(dest, val)
	return uint32(len(val)), 0
}

func (n *HamstorNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	const (
		xattrCreate  = 0x1 // XATTR_CREATE: fail if exists
		xattrReplace = 0x2 // XATTR_REPLACE: fail if not exists
	)
	if flags&xattrCreate != 0 || flags&xattrReplace != 0 {
		_, err := n.hfs.DB.GetXattr(n.inodeID, attr)
		exists := err == nil
		if flags&xattrCreate != 0 && exists {
			return syscall.EEXIST
		}
		if flags&xattrReplace != 0 && !exists {
			return syscall.ENODATA
		}
	}
	if err := n.hfs.DB.SetXattr(n.inodeID, attr, data); err != nil {
		return toErrno(err)
	}
	return 0
}

func (n *HamstorNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if err := n.hfs.DB.RemoveXattr(n.inodeID, attr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ENODATA
		}
		return toErrno(err)
	}
	return 0
}

func (n *HamstorNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	names, err := n.hfs.DB.ListXattrs(n.inodeID)
	if err != nil {
		return 0, toErrno(err)
	}
	// Format: null-terminated names concatenated
	var totalLen uint32
	for _, name := range names {
		totalLen += uint32(len(name)) + 1
	}
	if len(dest) == 0 {
		return totalLen, 0
	}
	if totalLen > uint32(len(dest)) {
		return 0, syscall.ERANGE
	}
	offset := 0
	for _, name := range names {
		copy(dest[offset:], name)
		offset += len(name)
		dest[offset] = 0
		offset++
	}
	return totalLen, 0
}

// --- Unsupported operations ---

// Link returns ENOTSUP because S3-backed storage has no concept of hard links.
func (n *HamstorNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}
