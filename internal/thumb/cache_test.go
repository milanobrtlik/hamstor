package thumb

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

// renderFixture is a small real image, rendered the way the daemon renders one.
func renderFixture(t *testing.T) Rendered {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 300, 200))
	for i := range img.Pix {
		img.Pix[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	r, err := Render(buf.Bytes())
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return r
}

func thumbPath(dir, sub, mountpoint, relPath string) string {
	uri := fileURI(mountpoint, relPath)
	return filepath.Join(dir, sub, fmt.Sprintf("%x.png", md5.Sum([]byte(uri))))
}

// TestCacheWriteLandsWhereTheDesktopLooks pins the whole point of thumb.Cache:
// the file goes to the directory it was HANDED, under the freedesktop name, with
// the metadata that names it. The bug this replaces resolved the directory from
// $HOME inside this package, which systemd does not set for a unit without
// User= — so every write went nowhere and said nothing.
func TestCacheWriteLandsWhereTheDesktopLooks(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "thumbnails")
	c := Cache{Dir: dir, Uid: -1, Gid: -1}
	mountpoint, relPath := "/mnt/hamstor", "photos/a b.png"

	c.Write(mountpoint, relPath, 1234567890, renderFixture(t))

	for _, sub := range []string{"normal", "large"} {
		p := thumbPath(dir, sub, mountpoint, relPath)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("%s thumbnail not written: %v", sub, err)
		}
		if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
			t.Errorf("%s thumbnail does not decode: %v", sub, err)
		}
		// The metadata is what makes the desktop trust and revalidate it. A
		// thumbnail without Thumb::URI is not a freedesktop thumbnail.
		if uri, ok := readTextChunk(data, "Thumb::URI"); !ok || uri != fileURI(mountpoint, relPath) {
			t.Errorf("%s Thumb::URI = %q (ok=%v), want %q", sub, uri, ok, fileURI(mountpoint, relPath))
		}
		if mt, ok := readTextChunk(data, "Thumb::MTime"); !ok || mt != "1234567890" {
			t.Errorf("%s Thumb::MTime = %q (ok=%v), want 1234567890", sub, mt, ok)
		}
	}

	// The spec requires 0700 on the cache directories; a world-readable one
	// leaks a preview of every file the mount serves.
	for _, p := range []string{dir, filepath.Join(dir, "normal"), filepath.Join(dir, "large")} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if perm := fi.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s mode = %o, want 700", p, perm)
		}
	}

	c.Remove(mountpoint, relPath)
	for _, sub := range []string{"normal", "large"} {
		if _, err := os.Stat(thumbPath(dir, sub, mountpoint, relPath)); !os.IsNotExist(err) {
			t.Errorf("%s thumbnail survived Remove (err=%v)", sub, err)
		}
	}
}

// TestZeroCacheIsInert covers the disabled case. It must not panic, must not
// create anything, and must not report — a mount with no resolvable cache
// directory is a normal configuration, not an error on every image.
func TestZeroCacheIsInert(t *testing.T) {
	var c Cache
	c.Write("/mnt/hamstor", "a.png", 1, renderFixture(t))
	c.Remove("/mnt/hamstor", "a.png")
}

// TestRenderedCarriesNoMetadata is the invariant that lets one rendered pair be
// stored once and stamped per mountpoint everywhere it is materialized. If the
// URI leaked into Render's output, a stored thumbnail would be bound to the
// machine that made it and would be wrong on every other one.
func TestRenderedCarriesNoMetadata(t *testing.T) {
	r := renderFixture(t)
	for name, data := range map[string][]byte{"normal": r.Normal, "large": r.Large} {
		if _, ok := readTextChunk(data, "Thumb::URI"); ok {
			t.Errorf("%s: Render embedded Thumb::URI; it belongs to Cache.Write", name)
		}
		if _, ok := readTextChunk(data, "Thumb::MTime"); ok {
			t.Errorf("%s: Render embedded Thumb::MTime; it belongs to Cache.Write", name)
		}
	}
}

func TestRenderBoundsTheLongestSide(t *testing.T) {
	r := renderFixture(t)
	for _, tc := range []struct {
		name   string
		data   []byte
		maxDim int
	}{
		{"normal", r.Normal, 128},
		{"large", r.Large, 256},
	} {
		cfg, _, err := image.DecodeConfig(bytes.NewReader(tc.data))
		if err != nil {
			t.Fatalf("%s decode config: %v", tc.name, err)
		}
		if cfg.Width > tc.maxDim || cfg.Height > tc.maxDim {
			t.Errorf("%s is %dx%d, want both sides <= %d", tc.name, cfg.Width, cfg.Height, tc.maxDim)
		}
	}
}

func TestRenderRejectsNonImages(t *testing.T) {
	if _, err := Render([]byte("this is not an image")); err == nil {
		t.Fatal("Render accepted garbage; a decode failure must not become a thumbnail")
	}
}
