package hfuse

import (
	"io"
	"log"
	"os"
	"sync"
	"syscall"
)

// uploadAttempt is one attempt to put an inode's contents into S3.
//
// It is immutable once published: err is written only before close(done), so a
// reader that takes the pointer under inodeWrite.mu and then waits on done sees
// a consistent value without holding any lock. That is what lets the upload
// goroutine stay off inodeWrite.mu entirely (see the flush path in handle.go),
// which in turn is what makes it safe for Flush to release the lock and wait.
//
// It replaces the old per-handle uploadDone/uploadErr pair. Those were recycled
// in place on every flush; once the fields are shared between handles, that
// recycling is a race (one handle clears the error another is about to read).
type uploadAttempt struct {
	done chan struct{}
	err  error
}

func newUploadAttempt() *uploadAttempt {
	return &uploadAttempt{done: make(chan struct{})}
}

// finished reports whether the attempt has completed, without blocking.
func (a *uploadAttempt) finished() bool {
	select {
	case <-a.done:
		return true
	default:
		return false
	}
}

// inodeWrite is the mutable file state shared by every handle open on one inode.
//
// Splitting it per handle is what made two concurrent writers destroy each
// other's data: each open snapshotted the whole file into its own buffer and
// the last Flush wrote its snapshot back wholesale, so whichever handle closed
// last silently won. Sharing it gives real read-modify-write semantics, and a
// shared dirty flag means N closes produce one upload rather than N.
//
// Keyed by inode ID in HamstorFS.writeStates. That is only sound because inode
// IDs are never reused (db: id INTEGER PRIMARY KEY AUTOINCREMENT) — dropping
// AUTOINCREMENT would let a new file inherit a dead inode's buffer.
type inodeWrite struct {
	mu sync.Mutex

	buf    []byte // in-memory contents (small files)
	dirty  bool
	loaded bool
	isNew  bool

	// Spill file for large writes: when total size exceeds spillThreshold,
	// contents live here instead of on the heap.
	spillFile *os.File
	spillSize int64

	// Cache-backed read: when set, reads use ReadAt on this file instead of
	// holding the whole file in buf. Shared across handles; concurrent ReadAt
	// is safe (it is a pread and never moves the offset).
	cacheFile *os.File

	// cur is the most recent upload attempt, nil if there has never been one.
	// It is published before Flush releases mu for the first time, so a
	// concurrent load always knows to wait rather than read a key the in-flight
	// upload is about to replace.
	cur *uploadAttempt

	// poisoned is set when an upload attempt failed, and makes every later
	// operation on this inode return EIO.
	//
	// It matters most when the bytes were retained under <db-dir>/pending/ (see
	// retainPendingUpload): the inode then stays 'pending' and the retained copy
	// is the only surviving one. Without poisoning, a sibling handle reading the
	// inode back sees a pending row with size 0, loads an empty buffer, and its
	// commit flips the status to 'committed' — at which point RecoverPending
	// deletes the retained bytes as stale (cleanup.go). One transient S3 error
	// plus one open handle would destroy the file outright. This is the one place
	// where failing loudly beats merging.
	//
	// It dies with the state, which is fine for the case above: a 'pending' inode
	// is invisible to LookupChild, so once every handle closes there is no way to
	// reach the file again until RecoverPending finishes the upload on the next
	// start.
	//
	// Not every poisoning path retains bytes — a GetInode failure in flushAsync
	// has nothing to retain and simply loses them, reporting EIO to close(2).
	poisoned error

	// handleRefs and uploadRefs are guarded by HamstorFS.writeMu, NOT by mu.
	handleRefs int // live file handles
	uploadRefs int // in-flight upload goroutines
}

// truncateWriteState resizes an inode's shared contents to s, taking the lock
// itself. A no-op when nothing is loaded: there is no buffer to correct, and the
// DB size that Setattr writes is then the whole truth.
func truncateWriteState(st *inodeWrite, s int64) syscall.Errno {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.spillFile != nil {
		if err := st.spillFile.Truncate(s); err != nil {
			log.Printf("hamstor: spill truncate failed: %v", err)
			return syscall.EIO
		}
		st.spillSize = s
		st.dirty = true
		return 0
	}

	// A cache-backed state holds no mutable buffer. Materialize it so the
	// truncation is durable on the next Flush, instead of only changing the DB
	// size while storage keeps the old bytes (which a later rewrite or append
	// would then resurrect).
	if st.cacheFile != nil {
		if info, statErr := st.cacheFile.Stat(); statErr == nil {
			b := make([]byte, info.Size())
			if _, rerr := st.cacheFile.ReadAt(b, 0); rerr == nil || rerr == io.EOF {
				st.buf = b
				st.cacheFile.Close()
				st.cacheFile = nil
				st.loaded = true
			}
		}
	}
	if st.loaded && st.cacheFile == nil {
		if s < int64(len(st.buf)) {
			st.buf = st.buf[:s]
		} else if s > int64(len(st.buf)) {
			st.buf = append(st.buf, make([]byte, s-int64(len(st.buf)))...)
		}
		st.dirty = true
	}
	return 0
}

// logicalSize is the current end of file as the shared state sees it. Must be
// called with mu held, and only once the contents live in buf or the spill file
// (Write resolves a cache-backed state into one of those first).
func (st *inodeWrite) logicalSize() int64 {
	if st.spillFile != nil {
		return st.spillSize
	}
	return int64(len(st.buf))
}

// awaitUpload blocks until any in-flight upload attempt on this inode has
// finished, so the caller can trust what it then reads from the DB. Must be
// called with mu held; it releases mu while waiting and reacquires it before
// returning. Returns EIO if the attempt failed (see poisoned).
//
// The wait is why the upload goroutine must never take mu.
func (st *inodeWrite) awaitUpload() syscall.Errno {
	for {
		att := st.cur
		if att == nil {
			return 0
		}
		if !att.finished() {
			st.mu.Unlock()
			<-att.done
			st.mu.Lock()
			// Re-read st.cur: another handle may have started a new attempt
			// while we were off the lock.
			continue
		}
		if att.err != nil {
			st.poisoned = att.err
			return syscall.EIO
		}
		return 0
	}
}

// free releases whatever the state still owns. Called only after the entry has
// been removed from the registry with no refs left, so nothing else can reach it.
func (st *inodeWrite) free() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.buf = nil
	if st.cacheFile != nil {
		st.cacheFile.Close()
		st.cacheFile = nil
	}
	// Spill ownership transfers to the upload goroutine in Flush; a spill file
	// still here means Flush never ran (opened and written, never closed) or it
	// bailed early, so this is the last chance to clean it up.
	if st.spillFile != nil {
		name := st.spillFile.Name()
		st.spillFile.Close()
		os.Remove(name)
		st.spillFile = nil
	}
}

// Lock ordering: writeMu is a leaf. Never take inodeWrite.mu — or make a
// syscall, an S3 call, or a DB call — while holding it. acquireWrite and
// tryAcquireWrite return with writeMu released so the caller can then take
// st.mu; releaseWrite frees outside the lock. This is what lets Flush take
// writeMu while already holding st.mu (it bumps uploadRefs there) without
// risking the inversion.

// acquireWrite returns the shared write state for an inode, creating it if this
// is the first open, with a handle reference taken. Every successful call must
// be paired with releaseWrite — including on error paths in Open, which return
// no handle and so would never see a Release.
func (hfs *HamstorFS) acquireWrite(inodeID int64) *inodeWrite {
	hfs.writeMu.Lock()
	defer hfs.writeMu.Unlock()
	if hfs.writeStates == nil {
		hfs.writeStates = make(map[int64]*inodeWrite)
	}
	st := hfs.writeStates[inodeID]
	if st == nil {
		st = &inodeWrite{}
		hfs.writeStates[inodeID] = st
	}
	st.handleRefs++
	return st
}

// tryAcquireWrite returns the shared state only if one already exists, with a
// handle reference taken, or nil. Used by path-based operations (truncate) that
// must reach an open handle's buffer but have no business creating state for an
// inode nobody has open.
func (hfs *HamstorFS) tryAcquireWrite(inodeID int64) *inodeWrite {
	hfs.writeMu.Lock()
	defer hfs.writeMu.Unlock()
	st := hfs.writeStates[inodeID]
	if st == nil {
		return nil
	}
	st.handleRefs++
	return st
}

// releaseWrite drops a handle reference and tears the state down once nothing
// holds it.
func (hfs *HamstorFS) releaseWrite(inodeID int64, st *inodeWrite) {
	hfs.dropRef(inodeID, st, func() { st.handleRefs-- })
}

// retainUpload takes a reference on behalf of an upload goroutine. Without it a
// reopen during an in-flight upload would build a fresh state, read the old key
// from the DB (the upload has not committed yet) and write on top of it — the
// same loss this whole mechanism exists to prevent, just sequential.
//
// Safe to call while holding st.mu: writeMu is a leaf.
func (hfs *HamstorFS) retainUpload(st *inodeWrite) {
	hfs.writeMu.Lock()
	st.uploadRefs++
	hfs.writeMu.Unlock()
}

func (hfs *HamstorFS) releaseUpload(inodeID int64, st *inodeWrite) {
	hfs.dropRef(inodeID, st, func() { st.uploadRefs-- })
}

func (hfs *HamstorFS) dropRef(inodeID int64, st *inodeWrite, drop func()) {
	hfs.writeMu.Lock()
	drop()
	dead := st.handleRefs == 0 && st.uploadRefs == 0
	if dead {
		// Only delete if the map still points at this state. A previous
		// teardown plus a fresh acquire could already have replaced it.
		if hfs.writeStates[inodeID] == st {
			delete(hfs.writeStates, inodeID)
		}
	}
	hfs.writeMu.Unlock()
	if dead {
		st.free() // outside writeMu: free() closes files
	}
}

// liveWriteStates reports how many inodes currently hold shared write state.
// Tests use it to catch reference leaks, which otherwise show up only as slow
// memory growth over a long-lived mount.
func (hfs *HamstorFS) liveWriteStates() int {
	hfs.writeMu.Lock()
	defer hfs.writeMu.Unlock()
	return len(hfs.writeStates)
}
