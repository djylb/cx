package main

import (
	"archive/tar"
	"bufio"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapIncrementalPrerequisite(t *testing.T) {
	ctx := snapTestCtx(t)
	root := t.TempDir()
	src := filepath.Join(root, "src")
	snapWriteTest(t, filepath.Join(src, "note"), "first")
	data := make([]byte, 128<<10)
	_, _ = rand.New(rand.NewSource(1)).Read(data)
	if e := os.WriteFile(filepath.Join(src, "shared"), data, 0600); e != nil {
		t.Fatal(e)
	}
	first, m, e := Freeze(ctx, src, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	store, _ := snapStore()
	old := filepath.Join(root, "old")
	if e = snapArchive(ctx, store, string(first), root, old, true); e != nil {
		t.Fatal(e)
	}
	snapWriteTest(t, filepath.Join(src, "note"), "second")
	second, _, e := Freeze(ctx, src, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r := m.Repositories[0]
	snapWriteTest(t, filepath.Join(root, r.key()), string(first))
	inc := filepath.Join(root, "inc")
	if e = snapArchive(ctx, store, string(second), root, inc, false); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(inc)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = f.Close() }()
	rd := tar.NewReader(f)
	declared := false
	for {
		h, nextErr := rd.Next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if !strings.HasSuffix(h.Name, ".bundle") {
			continue
		}
		if h.Size > int64(len(data)/4) {
			t.Fatalf("shared blob sent again: %d", h.Size)
		}
		header := bufio.NewReader(rd)
		for {
			line, readErr := header.ReadString('\n')
			if readErr != nil {
				t.Fatal(readErr)
			}
			if line == "\n" {
				break
			}
			declared = declared || strings.HasPrefix(line, "-"+r.OID+" ")
		}
	}
	if !declared {
		t.Fatal("missing sibling prerequisite")
	}
	target := t.TempDir()
	if e = snapImport(ctx, target, string(second), inc); e == nil {
		t.Fatal("accepted missing prerequisite")
	}
	if _, _, e = snapManifest(target, string(second)); !os.IsNotExist(e) {
		t.Fatal("incomplete manifest published")
	}
	if e = snapImport(ctx, target, string(first), old); e != nil {
		t.Fatal(e)
	}
	if e = snapImport(ctx, target, string(second), inc); e != nil {
		t.Fatal(e)
	}
}
