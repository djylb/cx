package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pngFixture(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestClaudeImageMessage(t *testing.T) {
	dir := t.TempDir()
	data := pngFixture(t, 2, 2)
	// The name says .jpg; the media type comes from the content.
	path := filepath.Join(dir, "shot.jpg")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	gif := []byte("GIF89a\x01\x00\x01\x00\x00\x00\x00;")
	o := options{images: []string{path}}
	parts, err := o.inputs("describe")
	if err != nil {
		t.Fatal(err)
	}
	extra, _ := json.Marshal(fields{"type": "image", "url": "data:image/gif;base64," + base64.StdEncoding.EncodeToString(gif)})
	remote, _ := json.Marshal(fields{"type": "image", "url": "https://example.invalid/a.png"})
	parts = append(parts, extra, remote)
	got, err := claudeUserMessage(parts)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"message":{"content":[{"text":"describe","type":"text"},` +
		`{"source":{"data":"` + base64.StdEncoding.EncodeToString(data) + `","media_type":"image/png","type":"base64"},"type":"image"},` +
		`{"source":{"data":"` + base64.StdEncoding.EncodeToString(gif) + `","media_type":"image/gif","type":"base64"},"type":"image"},` +
		`{"source":{"type":"url","url":"https://example.invalid/a.png"},"type":"image"}],"role":"user"},"type":"user"}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	claude, _ := lookupEngine("claude")
	if err = claude.checkInput(parts); err != nil {
		t.Fatal(err)
	}
}

func TestClaudeImageLimits(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, data []byte) json.RawMessage {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(fields{"type": "localImage", "path": path})
		return b
	}
	large := append(pngFixture(t, 1, 1), make([]byte, claudeMaxImageBytes)...)
	for _, tc := range []struct {
		want string
		part json.RawMessage
	}{
		{"the Claude API limit is 5242880 bytes (5 MB)", write("big.png", large)},
		{"8001x1 pixels; the Claude API limit is 8000 pixels per side", write("wide.png", pngFixture(t, claudeMaxImageSide+1, 1))},
		{"unsupported image format text/plain", write("note.txt", []byte("plain text"))},
		{"must be base64 encoded", json.RawMessage(`{"type":"image","url":"data:image/png,raw"}`)},
	} {
		_, err := claudeContent([]json.RawMessage{tc.part})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("want %q, got %v", tc.want, err)
		}
	}
	many := make([]json.RawMessage, claudeMaxImagesTotal+1)
	for i := range many {
		many[i] = json.RawMessage(`{"type":"image","url":"https://example.invalid/a.png"}`)
	}
	if _, err := claudeContent(many); err == nil || !strings.Contains(err.Error(), "more than 100 images") {
		t.Fatal(err)
	}
}
