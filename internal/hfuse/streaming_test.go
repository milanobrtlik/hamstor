package hfuse

import (
	"bytes"
	"context"
	"syscall"
	"testing"
)

// TestStreamingMediaFileReads covers the one read path no other test touches.
//
// Streaming mode is on by default (`--stream-rate 5`) and Open turns it on for
// any read-only handle on a media file with no encryptor — without asking
// whether the file has a whole-file object to range-read. Read dispatches to it
// before anything else, and fetchStreamChunk range-reads h.s3Key.
//
// Once the write path stopped naming a whole-file object, h.s3Key became empty
// for every newly written file, so that range read asks S3 for the empty key and
// every media file on an unencrypted mount answers EIO. Nothing caught it
// because setupTest leaves StreamRate at zero, which is the only reason this
// test has to set it by hand.
func TestStreamingMediaFileReads(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	// The defaults from main.go. Streaming is opt-out, not opt-in.
	hfs.StreamRate = 5
	hfs.StreamBuffer = 16

	id := mustInsert(t, hfs, "clip.mp4")
	content := bytes.Repeat([]byte("frame"), 4096)
	writeAt(t, hfs, id, content, 0, true)
	blocksOf(t, hfs, id) // schedules the objects for deletion

	// Read-only open: exactly what a media player does, and the only flag
	// combination that enables streaming.
	n := &HamstorNode{hfs: hfs, inodeID: id}
	fh, _, errno := n.Open(context.Background(), uint32(syscall.O_RDONLY))
	if errno != 0 {
		t.Fatalf("open read-only: %v", errno)
	}
	th := &TestHandle{h: fh.(*HamstorHandle)}
	defer th.TestRelease()

	got, errno := th.TestRead(len(content), 0)
	if errno != 0 {
		t.Fatalf("read a healthy media file: %v", errno)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("read %d bytes, want %d", len(got), len(content))
	}
}
