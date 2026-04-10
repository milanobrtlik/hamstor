package media

import (
	"path/filepath"
	"strings"
)

var mediaExts = map[string]bool{
	// Video
	".mp4": true, ".mkv": true, ".avi": true, ".mov": true,
	".webm": true, ".wmv": true, ".flv": true, ".m4v": true,
	".ts": true, ".m2ts": true, ".vob": true, ".ogv": true,
	// Audio
	".mp3": true, ".flac": true, ".ogg": true, ".opus": true,
	".wav": true, ".aac": true, ".m4a": true, ".wma": true,
}

// IsMediaExt returns true if the filename has a multimedia extension.
func IsMediaExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return mediaExts[ext]
}
