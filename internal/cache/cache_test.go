package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPutAndOpen(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "ab/ab123456-0000-0000-0000-000000000001"
	data := []byte("hello world")

	if c.Has(key) {
		t.Fatal("should not have key before Put")
	}

	if err := c.Put(key, data); err != nil {
		t.Fatal(err)
	}

	if !c.Has(key) {
		t.Fatal("should have key after Put")
	}

	f, err := c.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got := make([]byte, 64)
	n, _ := f.ReadAt(got, 0)
	got = got[:n]

	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestPutReader(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "cd/cd000000-0000-0000-0000-000000000002"
	data := []byte("streamed data")

	if err := c.PutReader(key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if !c.Has(key) {
		t.Fatal("should have key after PutReader")
	}

	f, err := c.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got := make([]byte, 64)
	n, _ := f.ReadAt(got, 0)
	if !bytes.Equal(got[:n], data) {
		t.Fatalf("got %q, want %q", got[:n], data)
	}
}

func TestEvict(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "ef/ef000000-0000-0000-0000-000000000003"
	if err := c.Put(key, []byte("data")); err != nil {
		t.Fatal(err)
	}

	c.Evict(key)

	if c.Has(key) {
		t.Fatal("should not have key after Evict")
	}
}

func TestEvictLRU(t *testing.T) {
	dir := t.TempDir()
	// Max 20 bytes
	c, err := New(dir, 20)
	if err != nil {
		t.Fatal(err)
	}

	// Put 3 entries of 10 bytes each = 30 bytes total, exceeds 20
	keys := []string{
		"a1/a1000000-0000-0000-0000-000000000001",
		"b2/b2000000-0000-0000-0000-000000000002",
		"c3/c3000000-0000-0000-0000-000000000003",
	}
	for _, key := range keys {
		data := make([]byte, 10)
		if err := c.Put(key, data); err != nil {
			t.Fatal(err)
		}
	}

	// After eviction, total should be <= 20 bytes
	// At least the oldest entry should be gone
	var totalSize int64
	var remaining int
	for _, key := range keys {
		if c.Has(key) {
			info, _ := os.Stat(c.path(key))
			totalSize += info.Size()
			remaining++
		}
	}

	if totalSize > 20 {
		t.Fatalf("cache size %d exceeds max 20", totalSize)
	}
	if remaining >= 3 {
		t.Fatal("expected at least one entry to be evicted")
	}
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "ff/ff000000-0000-0000-0000-000000000004"

	// Write initial data
	if err := c.Put(key, []byte("first")); err != nil {
		t.Fatal(err)
	}

	// Overwrite with new data
	if err := c.Put(key, []byte("second")); err != nil {
		t.Fatal(err)
	}

	f, err := c.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got := make([]byte, 64)
	n, _ := f.ReadAt(got, 0)
	if string(got[:n]) != "second" {
		t.Fatalf("got %q, want %q", got[:n], "second")
	}

	// No temp files left behind
	entries, _ := os.ReadDir(filepath.Join(dir, "ff"))
	for _, e := range entries {
		if e.Name()[0] == '.' {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

func TestOpenNotExist(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Open("zz/nonexistent")
	if !os.IsNotExist(err) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

// TestChunkDirIsNotAWholeFile: PutChunk leaves a directory at the key's path,
// and os.Open succeeds on a directory. Reporting that as a cached file makes
// callers allocate the directory's stat size, get EISDIR from ReadAt, and — in
// hfuse's write preload, which discards that error — serve a few KB of zeros as
// the file's contents.
func TestChunkDirIsNotAWholeFile(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "ee/ee000000-0000-0000-0000-000000000003"
	if err := c.PutChunk(key, 0, []byte("chunk zero")); err != nil {
		t.Fatal(err)
	}

	if c.Has(key) {
		t.Fatal("chunk directory reported as a cached whole file")
	}
	if _, err := c.Open(key); !os.IsNotExist(err) {
		t.Fatalf("Open on a chunk directory: got %v, want os.ErrNotExist", err)
	}
	if !c.HasChunk(key, 0) {
		t.Fatal("the chunk itself should still be cached")
	}
}
