package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// contextMirror is a copy of the controller's context layers. The pushing
// controller stays the only place they are edited; the receiver dispatches its own
// jobs from the copy without contacting the source.
type contextMirror struct {
	Files  map[string][]byte `json:"files"`
	Source string            `json:"source"`
	Roots  []string          `json:"roots"`
}

// mirrorMarker records where a mirrored layer came from.
const mirrorMarker = ".mirror.json"

func configRoot() (string, error) {
	base, e := projectDirectory()
	if e != nil {
		return "", e
	}
	return filepath.Dir(base), nil
}

// contextRoots lists the context layer directories under the cx config directory.
func contextRoots(config string) ([]string, error) {
	roots := []string{"context"}
	for _, pattern := range []string{"projects/*/context", "hosts/*/context"} {
		found, e := filepath.Glob(filepath.Join(config, filepath.FromSlash(pattern)))
		if e != nil {
			return nil, e
		}
		for _, p := range found {
			if st, statErr := os.Stat(p); statErr == nil && st.IsDir() {
				rel, relErr := filepath.Rel(config, p)
				if relErr != nil {
					return nil, relErr
				}
				roots = append(roots, filepath.ToSlash(rel))
			}
		}
	}
	sort.Strings(roots)
	return roots, nil
}

func ctlContext(ctx context.Context, a []string) error {
	if len(a) > 0 && isHelp(a[len(a)-1]) {
		return ctlUsage("context", flag.ErrHelp)
	}
	if len(a) != 2 || a[0] != "push" {
		return ctlUsage("context", errors.New("context push HOST"))
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	h, e := hs.get(a[1])
	if e != nil {
		return e
	}
	config, e := configRoot()
	if e != nil {
		return e
	}
	roots, e := contextRoots(config)
	if e != nil {
		return e
	}
	m := contextMirror{Files: map[string][]byte{}}
	m.Source, _ = os.Hostname()
	for _, root := range roots {
		layer := map[string][]byte{}
		if e = readContextLayer(filepath.Join(config, filepath.FromSlash(root)), layer); e != nil {
			return e
		}
		delete(layer, mirrorMarker)
		for k, b := range layer {
			m.Files[root+"/"+k] = b
		}
	}
	m.Roots = roots
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	r, e := h.call(ctx, 60*time.Second, "context-put", string(b))
	if e != nil {
		return e
	}
	return r.output()
}

// nodeContextPut replaces each pushed layer directory with the pushed files.
// Layers the receiver keeps for itself (names not in Roots) are left alone.
func nodeContextPut(raw string) error {
	var m contextMirror
	if e := json.Unmarshal([]byte(raw), &m); e != nil {
		return e
	}
	config, e := configRoot()
	if e != nil {
		return e
	}
	sum := sha256.New()
	keys := make([]string, 0, len(m.Files))
	for k := range m.Files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	written := 0
	for _, root := range m.Roots {
		if !snapPath(root) {
			return fmt.Errorf("invalid context root %q", root)
		}
		dir := filepath.Join(config, filepath.FromSlash(root))
		want := map[string]bool{}
		for _, k := range keys {
			rel, ok := cutRoot(k, root)
			if !ok {
				continue
			}
			if !snapPath(rel) {
				return fmt.Errorf("invalid context path %q", k)
			}
			want[filepath.FromSlash(rel)] = true
			path := filepath.Join(dir, filepath.FromSlash(rel))
			if e = os.MkdirAll(filepath.Dir(path), 0o700); e != nil {
				return e
			}
			if old, readErr := readSharedFile(path); readErr != nil || string(old) != string(m.Files[k]) {
				if e = writeConfigFile(path, m.Files[k]); e != nil {
					return e
				}
			}
			sum.Write([]byte(k))
			sum.Write(m.Files[k])
			written++
		}
		if e = os.MkdirAll(dir, 0o700); e != nil {
			return e
		}
		if e = pruneMirror(dir, want); e != nil {
			return e
		}
		marker, _ := json.Marshal(fields{"source": m.Source, "time": time.Now().UTC(), "read_only": true})
		if e = writeConfigFile(filepath.Join(dir, mirrorMarker), marker); e != nil {
			return e
		}
	}
	return json.NewEncoder(os.Stdout).Encode(fields{"roots": len(m.Roots), "files": written, "sha256": fmt.Sprintf("%x", sum.Sum(nil))})
}

func cutRoot(key, root string) (string, bool) {
	prefix := root + "/"
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return "", false
	}
	return key[len(prefix):], true
}

// pruneMirror removes files the source no longer has, keeping the marker.
func pruneMirror(dir string, keep map[string]bool) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return e
		}
		rel, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		if rel == mirrorMarker || keep[rel] {
			return nil
		}
		return os.Remove(path)
	})
}

// writeConfigFile replaces a controller configuration file; nothing observes these
// files concurrently with a mirror push.
func writeConfigFile(path string, b []byte) error {
	return atomicWrite(path, b)
}
