package hfuse

import (
	"context"
	"syscall"
)

// TestHandle exposes handle internals for testing.
type TestHandle struct {
	h *HamstorHandle
}

// newHandle builds a HamstorHandle the way Open does, sharing the registry's
// write state for the inode. Tests must go through this rather than a bare
// struct literal, or they hold private state no other handle can see and stop
// testing the thing that matters.
func newHandle(hfs *HamstorFS, inodeID int64, isNew bool) *HamstorHandle {
	st := hfs.acquireWrite(inodeID)
	if isNew {
		st.mu.Lock()
		st.isNew = true
		st.mu.Unlock()
	}
	return &HamstorHandle{hfs: hfs, inodeID: inodeID, st: st}
}

// NewTestHandle builds a handle the way Open does — in particular it takes the
// shared write state from the registry rather than making a private one. Two
// TestHandles on the same inode therefore share state exactly as two open(2)
// calls would; building a bare &HamstorHandle{} here would route the tests
// around the very thing they are meant to cover.
func NewTestHandle(hfs *HamstorFS, inodeID int64, isNew bool) *TestHandle {
	return &TestHandle{h: newHandle(hfs, inodeID, isNew)}
}

// NewTestAppendHandle is NewTestHandle for a handle opened with O_APPEND.
func NewTestAppendHandle(hfs *HamstorFS, inodeID int64, isNew bool) *TestHandle {
	th := NewTestHandle(hfs, inodeID, isNew)
	th.h.appendMode = true
	return th
}

func (th *TestHandle) TestWrite(data []byte) {
	th.h.Write(context.Background(), data, 0)
}

// TestWriteAt writes at an explicit offset, as pwrite(2) does.
func (th *TestHandle) TestWriteAt(data []byte, off int64) syscall.Errno {
	_, errno := th.h.Write(context.Background(), data, off)
	return errno
}

// TestRead reads n bytes from off, returning what the handle served.
func (th *TestHandle) TestRead(n int, off int64) ([]byte, syscall.Errno) {
	dest := make([]byte, n)
	res, errno := th.h.Read(context.Background(), dest, off)
	if errno != 0 {
		return nil, errno
	}
	out, status := res.Bytes(dest)
	if !status.Ok() {
		return nil, syscall.EIO
	}
	return out, 0
}

func (th *TestHandle) TestFlush() syscall.Errno {
	return th.h.Flush(context.Background())
}

func (th *TestHandle) TestRelease() {
	th.h.Release(context.Background())
}

// WaitUpload blocks until the inode's current upload attempt finishes. It waits
// on the shared state, not on this handle, because the upload carrying this
// handle's bytes may have been launched by a sibling. On the volume-staging path
// the attempt is already complete when Flush returns, so this is a no-op there.
func (th *TestHandle) WaitUpload() {
	th.h.st.mu.Lock()
	att := th.h.st.cur
	th.h.st.mu.Unlock()
	if att != nil {
		<-att.done
	}
}
