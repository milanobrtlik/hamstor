package hfuse

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/media"
	"github.com/milan/hamstor/internal/ratelimit"
	"github.com/milan/hamstor/internal/thumb"
	"github.com/milan/hamstor/internal/volume"
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
	n.hfs.applyInFlightSize(n.inodeID, &out.Attr)
	return 0
}

func (n *HamstorNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var sizePtr *int64
	var modePtr *uint32
	var mtimePtr *int64

	if sz, ok := in.GetSize(); ok {
		s := int64(sz)
		sizePtr = &s

		// Resize the shared buffer so the truncation is captured and re-uploaded
		// on Flush. f is nil for a path-based truncate(2), so go through the
		// registry rather than the handle: without that, open handles keep their
		// pre-truncate contents and resurrect the tail on their next flush.
		st := n.hfs.tryAcquireWrite(n.inodeID)
		if st != nil {
			// Wait out any in-flight upload first. SetAttr below now deletes the
			// block rows past the new end and this returns their objects for
			// deletion; an upload committing after that re-inserts rows for
			// objects that are already gone, leaving the file with rows pointing
			// at nothing — EIO on the next read of that region. It is the same
			// rule as Open's write preload: do not act on a version of the inode
			// an upload is about to replace.
			st.mu.Lock()
			errno := st.awaitUpload()
			st.mu.Unlock()
			if errno == 0 {
				errno = n.hfs.truncateWriteState(st, s)
			}
			n.hfs.releaseWrite(n.inodeID, st)
			if errno != 0 {
				return errno
			}
		}
		if fh, ok := f.(*HamstorHandle); ok {
			fh.fileSize = s
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

	orphaned, err := n.hfs.DB.SetAttr(n.inodeID, sizePtr, modePtr, mtimePtr)
	if err != nil {
		return toErrno(err)
	}
	// The blocks a shrink cut off. Deleted only AFTER the transaction stopped
	// referencing them: a crash in this order leaves orphans for GC, while the
	// reverse deletes live data. Without this, truncate(2) on a path with no open
	// write handle leaves the rows behind and growing the file back serves the
	// old tail where it should read as zeroes.
	n.hfs.dropObjects(ctx, orphaned)

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
	// ls -l goes LOOKUP first and the kernel caches what comes back for
	// --attr-timeout (60s by default), so overriding only in Getattr would still
	// show 0 bytes for a file whose upload is in flight.
	n.hfs.applyInFlightSize(meta.ID, &out.Attr)
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

	st := n.hfs.acquireWrite(newID)
	st.mu.Lock()
	st.isNew = true
	st.freshWrite = true // created empty: write-time eviction may commit a growing prefix
	st.mu.Unlock()
	handle := &HamstorHandle{
		hfs:        n.hfs,
		inodeID:    newID,
		inode:      node,
		st:         st,
		appendMode: flags&uint32(syscall.O_APPEND) != 0,
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

	st := n.hfs.acquireWrite(n.inodeID)
	handle := &HamstorHandle{
		hfs:        n.hfs,
		inodeID:    n.inodeID,
		inode:      &n.Inode,
		st:         st,
		fileSize:   meta.Size,
		appendMode: flags&uint32(syscall.O_APPEND) != 0,
	}
	// Every failure below returns no handle, so Release will never come for it
	// and the reference would pin the state in the registry for the life of the
	// mount. Hand it back unless we make it to a successful return.
	ok := false
	defer func() {
		if !ok {
			n.hfs.releaseWrite(n.inodeID, st)
		}
	}()

	writeFlags := uint32(syscall.O_WRONLY | syscall.O_RDWR | syscall.O_APPEND | syscall.O_TRUNC)

	st.mu.Lock()
	defer st.mu.Unlock()

	// The preload below reads from storage and clamps the SHARED buffer to
	// meta.Size, so both must agree on which version of the file they mean. The
	// meta read at the top of Open does not qualify: it was taken before the
	// state was locked, and a sibling's flush in that window leaves it stale —
	// short. The clamp then cuts the sibling's write off the end of the shared
	// buffer, and the next append overwrites what is left of it. That is one
	// silently lost line per race.
	//
	// So for write opens, settle the inode under the lock: wait out any in-flight
	// upload (until it commits, the DB still names the key it is about to
	// replace) and re-read. Once st.mu is held no sibling flush can intervene.
	// Read-only opens skip this deliberately — they must not block behind an
	// upload, and Read's streaming and chunked paths never touch the shared
	// buffer anyway.
	if flags&writeFlags != 0 {
		if errno := st.awaitUpload(); errno != 0 {
			return nil, 0, errno
		}
		m, err := n.hfs.DB.GetInode(n.inodeID)
		if err != nil {
			return nil, 0, toErrno(err)
		}
		meta = m
		handle.fileSize = meta.Size
	}

	// Preload data for files opened in write mode.
	//
	// A block-stored file has no vol_s3_key, so it is found by looking for its
	// blocks — and it must be, twice over: hasData false would skip the preload
	// and leave an empty shared buffer for the first write to append to (and
	// then commit as the whole file), while hasData true without a block branch
	// below would send it down the staging path to hunt for a staging file that
	// never existed.
	// HasBlocks, not BlocksForInode: this is a routing question, and a 4 TB file
	// has 524288 block rows. Reading them all to learn one bit would charge every
	// open the file's size — which is what lazy materialization is here to stop.
	var hasBlocks bool
	if meta.VolS3Key == "" && meta.Size > 0 {
		b, bErr := n.hfs.DB.HasBlocks(n.inodeID)
		if bErr != nil {
			return nil, 0, toErrno(bErr)
		}
		hasBlocks = b
	}
	// Too big to have ever been staged, with no needle and no blocks: a sparse
	// file, not a file whose data went missing. It attaches like a block-stored
	// one and every block resolves to a hole. See ensureLoaded.
	sparse := meta.VolS3Key == "" && !hasBlocks && meta.Size > volume.MaxNeedleSize

	hasData := meta.VolS3Key != "" || hasBlocks || sparse ||
		(meta.Size > 0 && n.hfs.VolumeBuilder != nil)

	// O_TRUNC empties the file for everyone, so it applies even when another
	// handle has already loaded the shared state — unlike the preload below,
	// which is skipped in that case because the contents are already there.
	if flags&uint32(syscall.O_TRUNC) != 0 && (hasData || st.loaded) {
		// Do the one fallible step first: nothing below can fail, so the shared
		// state is never left half-truncated for other handles after an open(2)
		// that reported failure.
		if st.spillFile != nil {
			if err := st.spillFile.Truncate(0); err != nil {
				log.Printf("hamstor: spill truncate on open: %v", err)
				return nil, 0, syscall.EIO
			}
			st.spillSize = 0
		}
		st.buf = nil
		// Nothing written since the last flush survives an emptied file, and the
		// commit at size 0 drops every block anyway. The presence map goes with
		// it: an emptied file has no block to be present, and a stale entry would
		// tell a later read that a block it never fetched is already local. The
		// dropped dirty blocks return their write-buffer charge first.
		st.chargeBlocks(-st.accountedBlocks)
		st.dirtyBlocks = nil
		st.presentBlocks = nil
		st.blockBacked = false
		// Nothing of the old storage survives, so nothing has to be carried
		// across: the commit at size 0 drops the needle and every block.
		st.wholeLoaded = false
		st.loaded = true
		st.dirty = true
		st.setSize(0)
		// Emptied from empty: write-time eviction may engage on the rewrite. The
		// old objects were already dropped by the FUSE_SETATTR size-0 that precedes
		// O_TRUNC's open (see the Async Flush note in CLAUDE.md), so committing a
		// growing prefix over this inode truncates nothing that still exists.
		st.freshWrite = true
		st.evictBroken = false
		st.seqHead = 0
		st.committedExtent = 0
	} else if flags&writeFlags != 0 && hasData && !st.loaded {
		if hasBlocks || sparse {
			// Attach a sparse backing store and fetch NOTHING. Opening a large
			// file for writing used to download all of it — the read-modify-write
			// this whole layout exists to retire. Write faults in the blocks it
			// partially overwrites; readLoaded faults in what it serves.
			//
			// Shares attachBlocks with ensureLoaded rather than repeating it: the
			// store's length is an invariant the rest of the state reads off, and
			// a second copy gets that kind of thing subtly wrong.
			if errno := handle.attachBlocks(meta.Size); errno != 0 {
				return nil, 0, errno
			}
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
			st.buf = data
			st.loaded = true
			st.wholeLoaded = true // a needle: the next block commit drops it
		} else if n.hfs.VolumeBuilder != nil {
			// Preload from staging dir (file staged but not yet packed)
			data, settled, dlErr := n.openPreloadStaged(ctx, handle)
			if dlErr != 0 {
				return nil, 0, dlErr
			}
			if !settled {
				st.buf = data
				st.loaded = true
				st.wholeLoaded = true // a staging file: the next commit drops it
			}
		}

		// If the stored object is longer than the inode's logical size — e.g. a
		// prior truncate() shrank the file without rewriting storage — clamp the
		// preloaded data to meta.Size so a subsequent rewrite or append does not
		// resurrect the truncated tail. (Not applied for O_TRUNC, which loads an
		// empty buffer.)
		if st.loaded && flags&uint32(syscall.O_TRUNC) == 0 {
			if st.spillFile != nil {
				if st.spillSize > meta.Size {
					if err := st.spillFile.Truncate(meta.Size); err == nil {
						st.spillSize = meta.Size
					}
				}
			} else if int64(len(st.buf)) > meta.Size {
				st.buf = st.buf[:meta.Size]
			}
			st.setSize(meta.Size)
			// Anything recorded about blocks the file no longer has goes with
			// them: a present index that survives a shrink claims the store holds
			// that block, so growing the file again would serve the zeroes the
			// re-extension left rather than fetch the row that is still there.
			st.dropBlocksPast(meta.Size)
		}
	}

	// Streaming mode: serve a media file block by block, rate-limited, instead of
	// attaching a store the length of the file and faulting the whole thing into
	// it. See readStreaming.
	//
	// hasBlocks is a REQUIREMENT, not an optimisation, and it is the condition
	// that dropping "no encryption" makes load-bearing. Streaming resolves every
	// block through the blocks table, where no row means a hole — so a media file
	// stored as a volume needle or a staging file, which has no rows at all,
	// would be served as silence. Under the old encryption gate the case was
	// mostly out of reach; without it, any .mp3 or .m4a small enough to be packed
	// (up to MaxNeedleSize) lands right in it. Those fall through to
	// ensureLoaded, which reads them whole — they are at most a needle anyway.
	//
	// Encryption is no longer an obstacle: every block is its own
	// [version][nonce][ct+tag], so each object decrypts on its own. That was the
	// entire reason for the old restriction.
	if flags&writeFlags == 0 && hasBlocks && media.IsMediaExt(meta.Name) && n.hfs.StreamRate > 0 {
		handle.streaming = true
		handle.rateLimiter = ratelimit.New(
			float64(n.hfs.StreamRate<<20), float64(n.hfs.StreamBuffer<<20))
		// One block is the smallest unit that can be served — a block object is
		// one AES-GCM message, so there is no ranging inside it — which means a
		// --stream-buffer below 8 MiB cannot be honoured and rounds up to it.
		handle.streamBlocksCap = max(1, n.hfs.StreamBuffer<<20/db.BlockSize)
	}

	ok = true
	return handle, fuse.FOPEN_DIRECT_IO, 0
}

// openPreloadStaged loads data from a staging file for write preloading.
// Falls back to volume if the builder packed the file between GetInode and read.
// openPreloadStaged loads a staged file's plaintext for the Open write preload.
// Shares readStaged with ensureLoaded rather than repeating it: the staging file
// moves under readers constantly and the retry logic that rides that out is
// exactly what a second copy gets wrong.
//
// settled reports that the contents were loaded into the shared state directly
// (the block path does its own placement, because a large file must go to a
// spill file rather than come back as one buffer); the caller must then not
// assign to st.buf itself.
func (n *HamstorNode) openPreloadStaged(ctx context.Context, handle *HamstorHandle) (data []byte, settled bool, errno syscall.Errno) {
	data, _, errno = n.hfs.readStaged(ctx, n.inodeID)
	if errno != 0 {
		return nil, false, errno
	}
	if data == nil {
		// It gained real storage while we looked. Either the builder packed it
		// into a volume, or an overwrite grew it past MaxNeedleSize and committed
		// it as blocks.
		meta, err := n.hfs.DB.GetInode(n.inodeID)
		if err != nil {
			return nil, false, toErrno(err)
		}
		if meta.VolS3Key == "" {
			if meta.Size > 0 {
				has, bErr := n.hfs.DB.HasBlocks(n.inodeID)
				if bErr != nil {
					return nil, false, toErrno(bErr)
				}
				if has || meta.Size > volume.MaxNeedleSize {
					return nil, true, handle.attachBlocks(meta.Size)
				}
			}
			return nil, false, syscall.EIO
		}
		d, dlErr := n.hfs.Store.DownloadRange(ctx, meta.VolS3Key, meta.VolOffset, meta.VolSize)
		if dlErr != nil {
			return nil, false, toErrno(dlErr)
		}
		if n.hfs.Encryptor != nil && crypto.IsEncrypted(d) {
			d, dlErr = n.hfs.Encryptor.Decrypt(d)
			if dlErr != nil {
				log.Printf("hamstor: decrypt failed for inode %d: %v", n.inodeID, dlErr)
				return nil, false, syscall.EIO
			}
		}
		data = d
	}
	return data, false, 0
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

	if meta.VolS3Key == "" && meta.Size > 0 && n.hfs.VolumeBuilder != nil {
		// May be staged but not yet packed — remove the staging file. Harmless
		// when the file is stored as blocks instead: there is no such file.
		os.Remove(n.hfs.VolumeBuilder.StagePath(meta.ID))
	}

	// The block keys come back FROM the delete, not from meta: the rows go away
	// with the inode through ON DELETE CASCADE, so this call is the last moment
	// anyone knows them.
	orphaned, err := n.hfs.DB.DeleteInodeWithVolume(meta.ID, meta.VolS3Key)
	if err != nil {
		return toErrno(err)
	}
	n.hfs.dropObjects(ctx, orphaned)
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

	// A directory owns no blocks, but the keys still come back from the delete
	// rather than being assumed empty: the one caller who assumes wrong is the
	// one who leaks.
	orphaned, err := n.hfs.DB.DeleteInode(meta.ID)
	if err != nil {
		return toErrno(err)
	}
	n.hfs.dropObjects(ctx, orphaned)
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
			if child.VolS3Key == "" && child.Size > 0 && hfs.VolumeBuilder != nil {
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
			orphaned, delErr := hfs.DB.DeleteInodeWithVolume(child.ID, child.VolS3Key)
			if delErr != nil {
				return delErr
			}
			hfs.dropObjects(ctx, orphaned)
		}
	}

	orphaned, err := hfs.DB.DeleteInode(dirID)
	if err != nil {
		return err
	}
	hfs.dropObjects(ctx, orphaned)
	return nil
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
	// S3 is elastic, so free space is not a real quantity. Report a large,
	// fixed amount so a file manager's pre-flight statvfs check never refuses a
	// copy: GNOME Files rejected a 17 GB paste against the old 4 GB figure
	// ("12.2 GB more space is required"). 1 PiB is the "effectively unlimited"
	// value used by other object-store mounts — big enough that no realistic
	// copy is blocked, small enough that df -h shows a sane number. Total grows
	// with usage (used + free) so the used/free/total triple stays coherent.
	const freeBlocks = 1 << 38 // 1 PiB in 4096-byte blocks
	out.Blocks = totalBlocks + freeBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks

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
	//
	// EXCHANGE must refuse with EINVAL, not ENOSYS: on ENOSYS the kernel latches
	// fc->no_rename2 for the whole connection and answers EVERY later renameat2
	// with flags itself, so a single EXCHANGE attempt would silently break
	// RENAME_NOREPLACE mount-wide for the rest of its lifetime. EINVAL is also
	// what rename(2) documents for a flag the filesystem does not support.
	const renameNoreplace = 0x1 // unix.RENAME_NOREPLACE
	if flags&fs.RENAME_EXCHANGE != 0 {
		return syscall.EINVAL
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
		if existing.VolS3Key == "" && existing.Size > 0 && n.hfs.VolumeBuilder != nil {
			os.Remove(n.hfs.VolumeBuilder.StagePath(existing.ID))
		}
		orphaned, delErr := n.hfs.DB.DeleteInodeWithVolume(existing.ID, existing.VolS3Key)
		if delErr != nil {
			return toErrno(delErr)
		}
		n.hfs.dropObjects(ctx, orphaned)
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
