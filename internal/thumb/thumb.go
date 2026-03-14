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

func Generate(mountpoint, relPath string, mtimeSec int64, imgData []byte) {
	img, _, err := image.Decode(bytes.NewReader(imgData))
	if err != nil {
		log.Printf("thumb: decode %s: %v", relPath, err)
		return
	}

	uri := fileURI(mountpoint, relPath)
	hash := fmt.Sprintf("%x", md5.Sum([]byte(uri)))
	mtimeStr := fmt.Sprintf("%d", mtimeSec)

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		log.Printf("thumb: cache dir: %v", err)
		return
	}
	thumbBase := filepath.Join(cacheDir, "thumbnails")

	sizes := []struct {
		dir    string
		maxDim int
	}{
		{"normal", 128},
		{"large", 256},
	}

	for _, s := range sizes {
		dir := filepath.Join(thumbBase, s.dir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Printf("thumb: mkdir %s: %v", dir, err)
			continue
		}

		scaled := resize(img, s.maxDim)
		var buf bytes.Buffer
		if err := png.Encode(&buf, scaled); err != nil {
			log.Printf("thumb: encode %s: %v", relPath, err)
			continue
		}

		withMeta := insertTextChunks(buf.Bytes(), map[string]string{
			"Thumb::URI":   uri,
			"Thumb::MTime": mtimeStr,
		})

		outPath := filepath.Join(dir, hash+".png")
		tmpPath := outPath + ".tmp"
		if err := os.WriteFile(tmpPath, withMeta, 0o600); err != nil {
			log.Printf("thumb: write %s: %v", tmpPath, err)
			continue
		}
		if err := os.Rename(tmpPath, outPath); err != nil {
			log.Printf("thumb: rename %s: %v", outPath, err)
			os.Remove(tmpPath)
		}
	}
}

func RemoveThumbnails(mountpoint, relPath string) {
	uri := fileURI(mountpoint, relPath)
	hash := fmt.Sprintf("%x", md5.Sum([]byte(uri)))

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return
	}
	thumbBase := filepath.Join(cacheDir, "thumbnails")

	for _, sub := range []string{"normal", "large"} {
		p := filepath.Join(thumbBase, sub, hash+".png")
		os.Remove(p)
	}
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

// insertTextChunks inserts PNG tEXt chunks before the first IDAT chunk.
func insertTextChunks(pngData []byte, kvs map[string]string) []byte {
	// PNG: 8-byte signature, then chunks (4-byte len + 4-byte type + data + 4-byte CRC)
	const sigLen = 8
	if len(pngData) < sigLen {
		return pngData
	}

	// Find first IDAT chunk offset
	offset := sigLen
	for offset+8 <= len(pngData) {
		chunkLen := int(binary.BigEndian.Uint32(pngData[offset : offset+4]))
		chunkType := string(pngData[offset+4 : offset+8])
		if chunkType == "IDAT" {
			break
		}
		offset += 12 + chunkLen // 4 len + 4 type + data + 4 crc
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
