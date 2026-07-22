package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeEntry puts a cache entry on disk directly, with a chosen mtime, so a test
// can lay out an exact LRU order. Going through Put would stamp everything with
// now and evict along the way.
func writeEntry(t *testing.T, dir, key string, size int, age time.Duration) string {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", key, err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		t.Fatalf("write %s: %v", key, err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(p, when, when); err != nil {
		t.Fatalf("chtimes %s: %v", key, err)
	}
	return p
}

// TestEvictLRUKeepsVolumesInTheSameShard pins the bug where evicting one volume
// took every volume sharing its 2-hex shard with it.
//
// A volume caches under volobj/{2hex}/{uuid}, one level deeper than a block's
// {2hex}/{uuid}. The scan assumed two levels, so it read volobj as the prefix and
// each shard below it as a single entry, then evicted the shard whole: about
// 1/256 of every cached volume per step, and with it whichever volume in there
// was hot enough to be holding the rest resident.
func TestEvictLRUKeepsVolumesInTheSameShard(t *testing.T) {
	dir := t.TempDir()
	// 500 bytes budget, low-water mark 90% of it. Five 100-byte volumes in one
	// shard is 500, so pushing one more in forces a modest eviction — a couple of
	// entries, not the shard.
	c, err := New(dir, 500)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}

	var keys []string
	for i := range 6 {
		// Same 2-hex shard on purpose: this is the case that used to collapse.
		key := fmt.Sprintf("volobj/a1/0000000%d-0000-0000-0000-000000000000", i)
		keys = append(keys, key)
		// Oldest first, so entry 0 is the eviction candidate and entry 5 the
		// freshest.
		writeEntry(t, dir, key, 100, time.Duration(6-i)*time.Hour)
	}

	c.evictLRU()

	var survived, evicted []string
	for _, key := range keys {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err == nil {
			survived = append(survived, key)
		} else {
			evicted = append(evicted, key)
		}
	}

	if len(survived) == 0 {
		t.Fatal("every volume in the shard was evicted at once: the scan is still treating volobj/a1 as one entry")
	}
	if len(evicted) == 0 {
		t.Fatalf("nothing was evicted from %d bytes against a 500 byte budget, so this test proves nothing", 600)
	}
	// LRU order must hold per volume: the freshest one cannot be gone while an
	// older one survives.
	newest := keys[len(keys)-1]
	for _, key := range evicted {
		if key == newest {
			t.Errorf("the most recently used volume was evicted while %v survived", survived)
		}
	}
}

// TestEvictLRURanksAcrossKeyDepths checks the other half: a volume three levels
// down and a block two levels down compete on their own mtimes. Under the old
// scan a volume's recency was the newest mtime in its whole shard, so a cold
// volume could outrank a warmer block purely by sharing a shard with a hot one.
func TestEvictLRURanksAcrossKeyDepths(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 250)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}

	hotVolume := "volobj/a1/11111111-0000-0000-0000-000000000000"
	coldVolume := "volobj/a1/22222222-0000-0000-0000-000000000000"
	coldBlock := "b2/33333333-0000-0000-0000-000000000000"

	writeEntry(t, dir, hotVolume, 100, time.Minute)
	writeEntry(t, dir, coldVolume, 100, 10*time.Hour)
	writeEntry(t, dir, coldBlock, 100, 20*time.Hour)

	c.evictLRU()

	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(hotVolume))); err != nil {
		t.Error("the most recently used entry was evicted")
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(coldBlock))); err == nil {
		t.Error("the oldest entry survived while the budget was over")
	}
}
