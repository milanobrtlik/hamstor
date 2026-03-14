package hfuse

import "context"

// TestHandle exposes handle internals for testing.
type TestHandle struct {
	h *HamstorHandle
}

func NewTestHandle(hfs *HamstorFS, inodeID int64, isNew bool) *TestHandle {
	return &TestHandle{h: &HamstorHandle{
		hfs:     hfs,
		inodeID: inodeID,
		isNew:   isNew,
	}}
}

func (th *TestHandle) TestWrite(data []byte) {
	th.h.Write(context.Background(), data, 0)
}

func (th *TestHandle) TestFlush() {
	th.h.Flush(context.Background())
}

func (th *TestHandle) TestRelease() {
	th.h.Release(context.Background())
}

func (th *TestHandle) WaitUpload() {
	if th.h.uploadDone != nil {
		<-th.h.uploadDone
	}
}
