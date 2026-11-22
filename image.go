package main

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif" // register decoders for dimension checks
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"strings"
)

// attachment is a file shipped with a submission; Ext keeps the original suffix.
type attachment struct {
	Ext  string
	Data []byte
}

// safeExt keeps a short file suffix that cannot escape the target directory.
func safeExt(ext string) string {
	if len(ext) > 13 || strings.ContainsAny(ext, `/\:`) {
		return ""
	}
	return ext
}

// imageMediaTypes are the formats agent engines accept.
var imageMediaTypes = []string{"image/png", "image/jpeg", "image/gif", "image/webp"}

// sniffImage reports the media type of image data from its content, not its name.
func sniffImage(data []byte) (string, error) {
	media := http.DetectContentType(data)
	for _, t := range imageMediaTypes {
		if media == t {
			return media, nil
		}
	}
	return "", fmt.Errorf("unsupported image format %s (supported: png, jpeg, gif, webp)", media)
}

// imageSize returns pixel dimensions for png, jpeg and gif; ok is false for other formats.
func imageSize(data []byte) (width, height int, ok bool) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0, false
	}
	return cfg.Width, cfg.Height, true
}
