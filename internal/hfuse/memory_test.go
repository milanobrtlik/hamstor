package hfuse

import (
	"bytes"
	"testing"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
)

// The write path runs against debug.SetMemoryLimit(150 << 20) (main.go). These
// tests pin the two places that used to spend that whole budget on a single
// file, because neither failure is visible as an error: the daemon simply falls
// into a GC death spiral and every upload crawls, which reads from the outside
// like data loss — close(2) succeeds, stat(2) says 0 for minutes, and the file
// only fills in later.
//
// Measured on the live B2 mount before the fix: four parallel 64 MiB writes
// peaked at 313 MB RSS and took 40 s to commit.

// TestWriteOfOneBlockDoesNotStayOnTheHeap holds the size at which writes leave
// memory.
//
// The old threshold was 64 MiB and the test was `end > spillThreshold`, which
// has two teeth. A file of exactly 64 MiB — the largest the write path would
// ever hold — never spilled at all, and getting there means growing st.buf by
// append, which is geometric: at the last reallocation the old array and the new
// one are both live, so one 64 MiB file transiently needs ~96-128 MiB.
//
// One block is the unit in which data leaves the process anyway (a flush uploads
// whole blocks, and an encrypted one is a single GCM message), so holding more
// than that on the heap buys nothing.
func TestWriteOfOneBlockDoesNotStayOnTheHeap(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()

	id := mustInsert(t, hfs, "one-block.bin")
	th := NewTestHandle(hfs, id, true)
	defer th.TestRelease()

	// Write it the way the kernel does: max_write-sized pieces, so the growth
	// path is exercised rather than one allocation of the final size.
	const piece = 128 << 10
	chunk := bytes.Repeat([]byte{0xA5}, piece)
	for off := int64(0); off < db.BlockSize; off += piece {
		if errno := th.TestWriteAt(chunk, off); errno != 0 {
			t.Fatalf("write at %d: %v", off, errno)
		}
	}

	if th.h.st.spillFile == nil {
		t.Fatalf("a file of one block (%d bytes) is still on the heap: spillThreshold is %d",
			db.BlockSize, spillThreshold)
	}
	if th.h.st.buf != nil {
		t.Fatalf("spilling left %d bytes of the file on the heap as well", len(th.h.st.buf))
	}
	if th.h.st.spillSize != db.BlockSize {
		t.Fatalf("spillSize %d, want %d", th.h.st.spillSize, db.BlockSize)
	}
}

// TestEncryptedFlushHonoursTheEncryptSemaphore is the deadlock guard for the
// bound on concurrent block encryption.
//
// Unlike the unencrypted path, which streams a block straight off the snapshot
// through io.NewSectionReader and allocates nothing, the encrypted one holds the
// plaintext AND the sealed copy of one block: 2 * 8 MiB, for as long as the PUT
// takes. The UploadSem slot is held across the whole block loop, so with its
// capacity of 32 the ceiling was 512 MiB — over three times the process limit,
// and reachable by any bulk copy onto an encrypted mount.
//
// A semaphore around that region is easy to leak on the error paths, and a leak
// shows up as a mount that wedges rather than as a failure, so drive several
// multi-block flushes through a capacity of one.
func TestEncryptedFlushHonoursTheEncryptSemaphore(t *testing.T) {
	hfs, _ := setupTest(t)
	hfs.SpillDir = t.TempDir()
	hfs.EncryptSem = make(chan struct{}, 1)
	enc, err := crypto.New("sem-passphrase", []byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("crypto: %v", err)
	}
	hfs.Encryptor = enc

	content := make([]byte, 2*db.BlockSize+7)
	for i := range content {
		content[i] = byte(i * 7)
	}

	for round := range 2 {
		id := mustInsert(t, hfs, "sem.bin")
		writeAt(t, hfs, id, content, 0, true)
		blocksOf(t, hfs, id) // registers the objects for cleanup
		if got := readBack(t, hfs, id, len(content)); !bytes.Equal(got, content) {
			t.Fatalf("round %d: encrypted round trip does not match", round)
		}
		if _, err := hfs.DB.DeleteInode(id); err != nil {
			t.Fatalf("round %d: delete inode: %v", round, err)
		}
	}

	// Every slot taken must have been given back, or the next flush blocks
	// forever — the failure this test exists for.
	select {
	case hfs.EncryptSem <- struct{}{}:
		<-hfs.EncryptSem
	default:
		t.Fatal("the encrypt semaphore was not released: a slot leaked on some path")
	}
}
