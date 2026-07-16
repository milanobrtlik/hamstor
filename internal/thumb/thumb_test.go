package thumb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

var pngSig = []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}

// pngChunk builds a well-formed PNG chunk: 4-byte length, 4-byte type, data,
// 4-byte CRC.
func pngChunk(typ string, data []byte) []byte {
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(len(data)))
	b.WriteString(typ)
	b.Write(data)
	crc := crc32.NewIEEE()
	crc.Write([]byte(typ))
	crc.Write(data)
	binary.Write(&b, binary.BigEndian, crc.Sum32())
	return b.Bytes()
}

// TestInsertTextChunksInsertsBeforeIDAT verifies the happy path: tEXt chunks are
// spliced in after IHDR and before the first IDAT, leaving both intact.
func TestInsertTextChunksInsertsBeforeIDAT(t *testing.T) {
	ihdr := pngChunk("IHDR", make([]byte, 13))
	idat := pngChunk("IDAT", []byte("pixels"))
	in := append(append(append([]byte{}, pngSig...), ihdr...), idat...)

	out := insertTextChunks(in, map[string]string{"Thumb::URI": "file:///x.jpg"})

	if len(out) <= len(in) {
		t.Fatalf("output not grown: got %d bytes, input was %d", len(out), len(in))
	}
	if !bytes.HasPrefix(out, append(append([]byte{}, pngSig...), ihdr...)) {
		t.Error("signature+IHDR not preserved at head of output")
	}
	if !bytes.HasSuffix(out, idat) {
		t.Error("IDAT chunk not preserved at tail of output")
	}
	textIdx := bytes.Index(out, []byte("tEXt"))
	idatIdx := bytes.Index(out, []byte("IDAT"))
	if textIdx < 0 {
		t.Fatal("no tEXt chunk inserted")
	}
	if textIdx > idatIdx {
		t.Errorf("tEXt inserted after IDAT (tEXt at %d, IDAT at %d)", textIdx, idatIdx)
	}
	if !bytes.Contains(out, []byte("file:///x.jpg")) {
		t.Error("tEXt value missing from output")
	}
}

// TestInsertTextChunksCorruptLengthNoPanic is the regression test for the
// out-of-range splice: a chunk declaring a bogus length walked `offset` past the
// end of the buffer, and the unguarded pngData[:offset] slice then panicked.
// A corrupt PNG must be returned unchanged instead.
func TestInsertTextChunksCorruptLengthNoPanic(t *testing.T) {
	// A chunk header claiming ~4 GB of data in a handful of bytes.
	bogus := make([]byte, 12)
	binary.BigEndian.PutUint32(bogus[0:4], 0xFFFFFF00)
	copy(bogus[4:8], "junk")
	in := append(append([]byte{}, pngSig...), bogus...)

	out := insertTextChunks(in, map[string]string{"Thumb::URI": "file:///x.jpg"})

	if !bytes.Equal(out, in) {
		t.Errorf("corrupt PNG modified: got %d bytes, want unchanged %d", len(out), len(in))
	}
}

// TestInsertTextChunksNoIDATUnchanged verifies that a PNG with no IDAT chunk is
// returned as-is rather than having tEXt chunks spliced onto the end, which
// would produce a malformed file.
func TestInsertTextChunksNoIDATUnchanged(t *testing.T) {
	ihdr := pngChunk("IHDR", make([]byte, 13))
	in := append(append([]byte{}, pngSig...), ihdr...)

	out := insertTextChunks(in, map[string]string{"Thumb::URI": "file:///x.jpg"})

	if !bytes.Equal(out, in) {
		t.Errorf("IDAT-less PNG modified: got %d bytes, want unchanged %d", len(out), len(in))
	}
}

// TestInsertTextChunksTruncatedUnchanged verifies a buffer shorter than the PNG
// signature is returned untouched.
func TestInsertTextChunksTruncatedUnchanged(t *testing.T) {
	in := []byte{0x89, 'P', 'N'}

	out := insertTextChunks(in, map[string]string{"Thumb::URI": "file:///x.jpg"})

	if !bytes.Equal(out, in) {
		t.Errorf("truncated buffer modified: got %v, want %v", out, in)
	}
}
