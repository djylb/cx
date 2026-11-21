package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func pythonReference(t *testing.T, rel string) (string, bool) {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", rel))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("reference is not a regular file: %s", path)
	}
	return path, true
}
