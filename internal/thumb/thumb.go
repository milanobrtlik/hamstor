package thumb

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "golang.org/x/image/bmp"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

var imageExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true,
	".gif": true, ".webp": true, ".bmp": true,
}

func IsImageExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return imageExts[ext]
}

// MaxImageBytes is the limit for image decoding to prevent decompression bombs.
// Exported so a caller that would have to FETCH the bytes first can refuse
// before paying for them rather than after.
const MaxImageBytes = 100 << 20 // 100 MB

// sizes are the freedesktop cache subdirectories and their bounding dimensions.
var sizes = []struct {
	dir    string
	maxDim int
}{
	{"normal", 128},
	{"large", 256},
}

// Rendered holds the two scaled PNGs for one image, WITHOUT the freedesktop
// tEXt metadata. The metadata is deliberately not in here: Thumb::URI contains
// the mountpoint, so a rendered thumbnail that carried it would be bound to the
// machine and path that produced it. Rendering and stamping are separate so the
// same bytes can be stored once and stamped per-mountpoint at every place they
// are materialized.
type Rendered struct {
	Normal []byte
	Large  []byte
}

// Render decodes an image and scales it to the freedesktop sizes.
func Render(imgData []byte) (Rendered, error) {
	if len(imgData) > MaxImageBytes {
		return Rendered{}, fmt.Errorf("image too large (%d bytes)", len(imgData))
	}
	img, _, err := image.Decode(bytes.NewReader(imgData))
	if err != nil {
		return Rendered{}, fmt.Errorf("decode: %w", err)
	}

	var out Rendered
	for _, s := range sizes {
		var buf bytes.Buffer
		if err := png.Encode(&buf, resize(img, s.maxDim)); err != nil {
			return Rendered{}, fmt.Errorf("encode %s: %w", s.dir, err)
		}
		if s.dir == "normal" {
			out.Normal = buf.Bytes()
		} else {
			out.Large = buf.Bytes()
		}
	}
	return out, nil
}

// Cache is one user's freedesktop thumbnail directory.
//
// The directory is passed in rather than resolved from the environment, which
// is what os.UserCacheDir() did. systemd sets $HOME only for units that have
// User= set (systemd.exec(5)), and the daemon's unit runs as root without one,
// so os.UserCacheDir() returned an error on every single image and thumbnails
// were written nowhere at all — for four months, silently, because the only
// report was one log line nobody was looking for. The target user is knowable
// (it is --uid, whose files these are), so it is resolved once at startup and
// handed here.
//
// A zero Cache (empty Dir) disables thumbnails entirely; every method is a
// no-op, so callers do not need to check.
type Cache struct {
	// Dir is the thumbnails directory itself, i.e. <cache home>/thumbnails.
	Dir string

	// Uid and Gid own what gets written. -1 leaves ownership alone, which is
	// right whenever the process already runs as the target user. Writing as
	// root into someone else's cache without this leaves files they cannot
	// replace, so their own thumbnailer breaks on every image we touched.
	Uid, Gid int
}

// Write stores both sizes for one file, stamped with the freedesktop metadata
// that names it: the URI the desktop will look it up by, and the source mtime
// it revalidates against.
func (c Cache) Write(mountpoint, relPath string, mtimeSec int64, r Rendered) {
	if c.Dir == "" {
		return
	}
	uri := fileURI(mountpoint, relPath)
	hash := fmt.Sprintf("%x", md5.Sum([]byte(uri)))
	meta := map[string]string{
		"Thumb::URI":   uri,
		"Thumb::MTime": fmt.Sprintf("%d", mtimeSec),
	}

	for _, s := range sizes {
		data := r.Normal
		if s.dir != "normal" {
			data = r.Large
		}
		if len(data) == 0 {
			continue
		}

		dir := filepath.Join(c.Dir, s.dir)
		// 0700 is required by the spec, and is also what makes the chown below
		// matter: a world-readable cache would leak thumbnails of every file.
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Printf("thumb: mkdir %s: %v", dir, err)
			continue
		}
		c.chown(c.Dir)
		c.chown(dir)

		outPath := filepath.Join(dir, hash+".png")
		tmpPath := outPath + ".tmp"
		if err := os.WriteFile(tmpPath, insertTextChunks(data, meta), 0o600); err != nil {
			log.Printf("thumb: write %s: %v", tmpPath, err)
			continue
		}
		// Chown before the rename, so the file is never visible at its final
		// name under the wrong owner.
		c.chown(tmpPath)
		if err := os.Rename(tmpPath, outPath); err != nil {
			log.Printf("thumb: rename %s: %v", outPath, err)
			os.Remove(tmpPath)
		}
	}
}

// Has reports whether both sizes are already present and current for one file.
//
// "Current" is the freedesktop rule: the stored Thumb::MTime must equal the
// source's mtime. This is what makes a materialization pass idempotent and
// therefore cheap enough to run on every mount — a finished library costs two
// stats and two small reads per image and no S3 traffic at all.
//
// Both sizes must be good. Requiring only one would leave a half-written pair
// in place forever, since nothing else ever revisits it.
func (c Cache) Has(mountpoint, relPath string, mtimeSec int64) bool {
	if c.Dir == "" {
		return false
	}
	hash := fmt.Sprintf("%x", md5.Sum([]byte(fileURI(mountpoint, relPath))))
	want := fmt.Sprintf("%d", mtimeSec)
	for _, s := range sizes {
		data, err := os.ReadFile(filepath.Join(c.Dir, s.dir, hash+".png"))
		if err != nil {
			return false
		}
		if got, ok := readTextChunk(data, "Thumb::MTime"); !ok || got != want {
			return false
		}
	}
	return true
}

// Remove drops both sizes for one file.
func (c Cache) Remove(mountpoint, relPath string) {
	if c.Dir == "" {
		return
	}
	hash := fmt.Sprintf("%x", md5.Sum([]byte(fileURI(mountpoint, relPath))))
	for _, s := range sizes {
		os.Remove(filepath.Join(c.Dir, s.dir, hash+".png"))
	}
}

// chown applies the target ownership, ignoring failure: when the process is not
// root it cannot chown, and it does not need to — it is already writing as the
// user who owns the directory.
func (c Cache) chown(path string) {
	if c.Uid < 0 && c.Gid < 0 {
		return
	}
	_ = os.Chown(path, c.Uid, c.Gid)
}

func fileURI(mountpoint, relPath string) string {
	absPath := filepath.Join(mountpoint, relPath)
	return "file://" + uriEncodePath(absPath)
}

func uriEncodePath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func resize(img image.Image, maxDim int) image.Image {
	bounds := img.Bounds()
	origW := bounds.Dx()
	origH := bounds.Dy()

	if origW <= maxDim && origH <= maxDim {
		return img
	}

	var newW, newH int
	if origW > origH {
		newW = maxDim
		newH = origH * maxDim / origW
	} else {
		newH = maxDim
		newW = origW * maxDim / origH
	}
	if newW < 1 {
		newW = 1
	}
	if newH < 1 {
		newH = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)
	return dst
}

// readTextChunk reads back one tEXt value written by insertTextChunks.
//
// It lives next to its writer deliberately: these two are the only agreement
// about what a stored thumbnail says about itself, and a disagreement between
// them is silent — a materializer that cannot read Thumb::MTime back either
// rewrites every thumbnail on every pass or trusts a stale one forever.
func readTextChunk(pngData []byte, key string) (string, bool) {
	const sigLen = 8
	offset := sigLen
	for offset+8 <= len(pngData) {
		chunkLen := int(binary.BigEndian.Uint32(pngData[offset : offset+4]))
		chunkType := string(pngData[offset+4 : offset+8])
		next := offset + 12 + chunkLen
		// Same bound as the writer: a corrupt length must not walk us off the
		// end, and IDAT means the header is over — tEXt chunks go before it.
		if chunkLen < 0 || next <= offset || next > len(pngData) {
			return "", false
		}
		if chunkType == "IDAT" {
			return "", false
		}
		if chunkType == "tEXt" {
			data := pngData[offset+8 : offset+8+chunkLen]
			if i := bytes.IndexByte(data, 0); i >= 0 && string(data[:i]) == key {
				return string(data[i+1:]), true
			}
		}
		offset = next
	}
	return "", false
}

// insertTextChunks inserts PNG tEXt chunks before the first IDAT chunk.
func insertTextChunks(pngData []byte, kvs map[string]string) []byte {
	// PNG: 8-byte signature, then chunks (4-byte len + 4-byte type + data + 4-byte CRC)
	const sigLen = 8
	if len(pngData) < sigLen {
		return pngData
	}

	// Find first IDAT chunk offset
	offset := sigLen
	found := false
	for offset+8 <= len(pngData) {
		chunkLen := int(binary.BigEndian.Uint32(pngData[offset : offset+4]))
		chunkType := string(pngData[offset+4 : offset+8])
		if chunkType == "IDAT" {
			found = true
			break
		}
		// Guard against a corrupt/overflowing chunk length walking us past the
		// buffer (the splice below would otherwise slice out of range).
		next := offset + 12 + chunkLen
		if chunkLen < 0 || next <= offset || next > len(pngData) {
			break
		}
		offset = next
	}
	// No IDAT found (truncated/non-standard PNG): return the input unchanged
	// rather than splicing tEXt chunks past the end and producing a malformed
	// file (or panicking on the out-of-range slice).
	if !found {
		return pngData
	}

	// Build tEXt chunks
	var chunks []byte
	for key, val := range kvs {
		data := append([]byte(key), 0)
		data = append(data, []byte(val)...)
		chunkType := []byte("tEXt")

		var chunk bytes.Buffer
		binary.Write(&chunk, binary.BigEndian, uint32(len(data)))
		chunk.Write(chunkType)
		chunk.Write(data)

		crc := crc32.NewIEEE()
		crc.Write(chunkType)
		crc.Write(data)
		binary.Write(&chunk, binary.BigEndian, crc.Sum32())

		chunks = append(chunks, chunk.Bytes()...)
	}

	// Splice: before-IDAT + tEXt chunks + IDAT-and-rest
	result := make([]byte, 0, len(pngData)+len(chunks))
	result = append(result, pngData[:offset]...)
	result = append(result, chunks...)
	result = append(result, pngData[offset:]...)
	return result
}
