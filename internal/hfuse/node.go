package hfuse

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/thumb"
)

type HamstorNode struct {
	fs.Inode
	hfs     *HamstorFS
	inodeID int64
}

var (
	_ fs.NodeGetattrer = (*HamstorNode)(nil)
	_ fs.NodeSetattrer = (*HamstorNode)(nil)
	_ fs.NodeLookuper  = (*HamstorNode)(nil)
	_ fs.NodeReaddirer = (*HamstorNode)(nil)
	_ fs.NodeMkdirer   = (*HamstorNode)(nil)
	_ fs.NodeCreater   = (*HamstorNode)(nil)
	_ fs.NodeOpener    = (*HamstorNode)(nil)
	_ fs.NodeUnlinker  = (*HamstorNode)(nil)
	_ fs.NodeRmdirer   = (*HamstorNode)(nil)
	_ fs.NodeRenamer   = (*HamstorNode)(nil)
	_ fs.NodeStatfser  = (*HamstorNode)(nil)
)

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, sql.ErrNoRows) {
		return syscall.ENOENT
	}
	log.Printf("hamstor: %v", err)
	return syscall.EIO
}

func (n *HamstorNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.hfs.DB.GetInode(n.inodeID)
	if err != nil {
		return toErrno(err)
	}
	caller, _ := fuse.FromContext(ctx)
	out.Ino = uint64(meta.ID)
	out.Mode = meta.Mode
	out.Uid = caller.Uid
	out.Gid = caller.Gid
	out.Size = uint64(meta.Size)
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
	return 0
}

func (n *HamstorNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var sizePtr *int64
	var modePtr *uint32
	var mtimePtr *int64

	if sz, ok := in.GetSize(); ok {
		s := int64(sz)
		sizePtr = &s
	}
	if m, ok := in.GetMode(); ok {
		modePtr = &m
	}
	if mt, ok := in.GetMTime(); ok {
		ns := mt.UnixNano()
		mtimePtr = &ns
	}

	if err := n.hfs.DB.SetAttr(n.inodeID, sizePtr, modePtr, mtimePtr); err != nil {
		return toErrno(err)
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

	out.Ino = uint64(meta.ID)
	out.Mode = meta.Mode
	out.Size = uint64(meta.Size)
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
	dirMode := mode | syscall.S_IFDIR
	newID, err := n.hfs.DB.InsertInode(n.inodeID, name, dirMode, "committed")
	if err != nil {
		return nil, toErrno(err)
	}

	child := &HamstorNode{hfs: n.hfs, inodeID: newID}
	stable := fs.StableAttr{Mode: dirMode, Ino: uint64(newID)}
	inode := n.NewInode(ctx, child, stable)

	out.Ino = uint64(newID)
	out.Mode = dirMode
	out.Nlink = 2
	return inode, 0
}

func (n *HamstorNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fileMode := mode | syscall.S_IFREG
	newID, err := n.hfs.DB.InsertInode(n.inodeID, name, fileMode, "pending")
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
	out.Nlink = 1
	return node, handle, fuse.FOPEN_DIRECT_IO, 0
}

func (n *HamstorNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	meta, err := n.hfs.DB.GetInode(n.inodeID)
	if err != nil {
		return nil, 0, toErrno(err)
	}

	handle := &HamstorHandle{
		hfs:     n.hfs,
		inodeID: n.inodeID,
	}

	// If opening for write and file has existing content, preload it
	writeFlags := uint32(syscall.O_WRONLY | syscall.O_RDWR | syscall.O_APPEND | syscall.O_TRUNC)
	if flags&writeFlags != 0 && meta.S3Key != "" {
		if flags&uint32(syscall.O_TRUNC) != 0 {
			handle.buf = []byte{}
			handle.loaded = true
			handle.dirty = true
		} else {
			data, err := n.hfs.Store.Download(ctx, meta.S3Key)
			if err != nil {
				return nil, 0, toErrno(err)
			}
			handle.buf = data
			handle.loaded = true
		}
	}

	return handle, fuse.FOPEN_DIRECT_IO, 0
}

func (n *HamstorNode) Unlink(ctx context.Context, name string) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	if thumb.IsImageExt(meta.Name) {
		if relPath, err := n.hfs.DB.InodePath(meta.ID); err == nil {
			go thumb.RemoveThumbnails(n.hfs.Mountpoint, relPath)
		}
	}

	if meta.S3Key != "" {
		if err := n.hfs.Store.Delete(ctx, meta.S3Key); err != nil {
			log.Printf("hamstor: unlink s3 delete %s: %v", meta.S3Key, err)
		}
	}

	if err := n.hfs.DB.DeleteInode(meta.ID); err != nil {
		return toErrno(err)
	}
	return 0
}

func (n *HamstorNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	children, err := n.hfs.DB.ListChildren(meta.ID)
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

func (n *HamstorNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Bsize = 4096
	out.NameLen = 255
	out.Frsize = 4096
	return 0
}

func (n *HamstorNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	meta, err := n.hfs.DB.LookupChild(n.inodeID, name)
	if err != nil {
		return toErrno(err)
	}

	newParentNode := newParent.EmbeddedInode().Operations().(*HamstorNode)

	// Remove old thumbnails (URI will change)
	if thumb.IsImageExt(meta.Name) {
		if oldRelPath, err := n.hfs.DB.InodePath(meta.ID); err == nil {
			go thumb.RemoveThumbnails(n.hfs.Mountpoint, oldRelPath)
		}
	}

	// Check if target exists — if so, remove it
	existing, err := n.hfs.DB.LookupChild(newParentNode.inodeID, newName)
	if err == nil {
		if thumb.IsImageExt(existing.Name) {
			if existingPath, err := n.hfs.DB.InodePath(existing.ID); err == nil {
				go thumb.RemoveThumbnails(n.hfs.Mountpoint, existingPath)
			}
		}
		if existing.S3Key != "" {
			if delErr := n.hfs.Store.Delete(ctx, existing.S3Key); delErr != nil {
				log.Printf("hamstor: rename delete old s3 %s: %v", existing.S3Key, delErr)
			}
		}
		if delErr := n.hfs.DB.DeleteInode(existing.ID); delErr != nil {
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
