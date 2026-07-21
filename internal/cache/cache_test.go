package cache

import (
	"bytes"
	"fmt"
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

// seedChunkDir writes a chunk directory the way a pre-block version of hamstor
// left one behind: a directory at the key's own path holding chunk-%06d files.
// Nothing in the current binary can create one, and that is the point — a
// --cache-dir is deliberately kept across reinstalls, so these arrive from the
// past rather than from anything a test could do through the API.
func seedChunkDir(t *testing.T, dir, key string, chunks ...[]byte) {
	t.Helper()
	p := filepath.Join(dir, key)
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, data := range chunks {
		name := filepath.Join(p, fmt.Sprintf("chunk-%06d", i))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestInheritedChunkDirIsNotAWholeFile: os.Open succeeds on a directory.
// Reporting one as a cached file makes callers allocate its stat size, get
// EISDIR from ReadAt, and — in hfuse's write preload, which discards that
// error — serve a few KB of zeros as the file's contents.
//
// New's sweep normally removes these, so the guard covers a directory that
// turns up at a key's path any other way: a cache directory added after start,
// or one seeded between New and the lookup.
func TestInheritedChunkDirIsNotAWholeFile(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	key := "ee/ee000000-0000-0000-0000-000000000003"
	seedChunkDir(t, dir, key, []byte("chunk zero"))

	if c.Has(key) {
		t.Fatal("directory at a key's path reported as a cached whole file")
	}
	if _, err := c.Open(key); !os.IsNotExist(err) {
		t.Fatalf("Open on a directory: got %v, want os.ErrNotExist", err)
	}
}

// TestNewSweepsInheritedChunkDirs: the chunk directories an older version left
// are referenced by nothing — every key a live row names is a fresh UUID — but
// they count against --cache-size and evictLRU only reclaims them once the
// cache is full enough to evict at all. New clears them during the walk it
// already does.
//
// The control entries matter more than the chunk directory: the sweep decides
// by file name (chunk-*), not by shape, and a shape test ("a directory two
// levels down") would delete volobj/{prefix}, i.e. the whole volume cache.
func TestNewSweepsInheritedChunkDirs(t *testing.T) {
	dir := t.TempDir()

	seedChunkDir(t, dir, "aa/aa000000-0000-0000-0000-00000000000a",
		bytes.Repeat([]byte("x"), 100), bytes.Repeat([]byte("y"), 200))

	live := map[string][]byte{
		"bb/bb000000-0000-0000-0000-00000000000b":        []byte("a block"),
		"volobj/cc/cc000000-0000-0000-0000-00000000000c": []byte("a whole volume"),
	}
	var liveBytes int64
	for key, data := range live {
		p := filepath.Join(dir, key)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		liveBytes += int64(len(data))
	}

	c, err := New(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}

	stale := filepath.Join(dir, "aa/aa000000-0000-0000-0000-00000000000a")
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale chunk directory survived New: %v", err)
	}
	for key := range live {
		if !c.Has(key) {
			t.Fatalf("New removed a live cache entry: %s", key)
		}
	}
	if total, count := c.Size(); total != liveBytes || count != len(live) {
		t.Fatalf("after sweep Size() = %d bytes / %d entries, want %d / %d",
			total, count, liveBytes, len(live))
	}
	if got := c.approxSize.Load(); got != liveBytes {
		t.Fatalf("approxSize counts swept bytes: got %d, want %d", got, liveBytes)
	}
}
