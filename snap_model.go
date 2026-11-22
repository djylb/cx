package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

type SnapshotID string
type FreezeOptions struct{ History string }

// Manifest describes a frozen snapshot. Fields follow v1's sorted wire order.
type Manifest struct {
	Main         string             `json:"main"`
	Repositories []SnapshotRepo     `json:"repositories"`
	Version      int                `json:"version"`
	Workspace    *SnapshotWorkspace `json:"workspace"`
}

// SnapshotRepo describes one frozen repository. Field order is part of the v1 snapshot hash.
type SnapshotRepo struct {
	Base    string   `json:"base,omitempty"`
	Head    *string  `json:"head"`
	History bool     `json:"history,omitempty"`
	OID     string   `json:"oid"`
	Path    string   `json:"path"`
	RepoID  string   `json:"repo_id"`
	Shallow []string `json:"shallow,omitempty"`
	StoreID string   `json:"store_id,omitempty"`
}
type SnapshotWorkspace struct {
	Sum  *string `json:"sum"`
	Text string  `json:"text"`
}

func (r SnapshotRepo) key() string {
	if r.StoreID != "" {
		return r.StoreID
	}
	return r.RepoID
}
func (r SnapshotRepo) bare(store string) string { return filepath.Join(store, "repos", r.key()+".git") }
func snapStore() (string, error) {
	if s := os.Getenv("CX_SNAPSHOT_STORE"); s != "" {
		return filepath.Abs(s)
	}
	r, e := stateRoot()
	return filepath.Join(r, "snapshot"), e
}
func snapSum(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func snapJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if e := enc.Encode(v); e != nil {
		return nil, e
	}
	var result bytes.Buffer
	for _, r := range strings.TrimSuffix(b.String(), "\n") {
		if r < 127 {
			result.WriteRune(r)
		} else {
			for _, u := range utf16.Encode([]rune{r}) {
				_, _ = fmt.Fprintf(&result, "\\u%04x", u) // bytes.Buffer.Write never returns an error.
			}
		}
	}
	return result.Bytes(), nil
}
func snapCommand(ctx context.Context, dir string, env []string, in []byte, prog string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	c := exec.CommandContext(ctx, prog, args...)
	c.Dir = dir
	c.Env = append(snapProcessEnv(), env...)
	c.Stdin = bytes.NewReader(in)
	var diag bytes.Buffer
	c.Stderr = &diag
	b, e := c.Output()
	if e != nil {
		return b, fmt.Errorf("%s in %s: %w: %s", prog, dir, e, strings.TrimSpace(diag.String()))
	}
	return b, nil
}

type snapGit struct {
	ctx context.Context
	dir string
	env []string
}

func (g snapGit) input(b []byte, args ...string) ([]byte, error) {
	return snapCommand(g.ctx, g.dir, g.env, b, "git", append([]string{"-c", "core.hooksPath=" + os.DevNull, "-c", "core.fsmonitor=false", "-c", "commit.gpgSign=false"}, args...)...)
}
func (g snapGit) get(args ...string) (string, error) {
	b, e := g.input(nil, args...)
	return strings.TrimRight(string(b), "\r\n"), e
}
func snapWithin(root, path string) bool {
	r, e := filepath.Rel(root, path)
	return e == nil && (r == "." || filepath.IsLocal(r))
}
func snapPath(s string) bool {
	return utf8.ValidString(s) && s != "" && filepath.IsLocal(s) && filepath.ToSlash(filepath.Clean(s)) == s && !strings.ContainsAny(s, "\\:\x00") && s != "."
}
func snapHex(s string, n int) bool {
	if len(s) != n || s != strings.ToLower(s) {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func snapOID(s string) bool { return snapHex(s, 40) || snapHex(s, 64) }
func (m Manifest) validate() error {
	if m.Version != 1 || !snapPath(m.Main) {
		return errors.New("invalid snapshot header")
	}
	main := false
	for i, r := range m.Repositories {
		if !snapPath(r.Path) || !snapHex(r.key(), 64) || !snapHex(r.RepoID, 64) || !snapOID(r.OID) || r.Base != "" && !snapHex(r.Base, len(r.OID)) || r.Head != nil && !snapHex(*r.Head, len(r.OID)) {
			return errors.New("invalid snapshot repository")
		}
		for _, p := range r.Shallow {
			if !snapHex(p, len(r.OID)) {
				return errors.New("invalid shallow boundary")
			}
		}
		if len(r.Shallow) > 0 {
			data, e := snapJSON([]any{r.RepoID, r.Shallow})
			if e != nil {
				return e
			}
			if !r.History || r.StoreID != snapSum(data) {
				return errors.New("invalid shallow store identity")
			}
		}
		for _, other := range m.Repositories[:i] {
			if snapWithin(other.Path, r.Path) || snapWithin(r.Path, other.Path) {
				return errors.New("overlapping repository roots")
			}
		}
		main = main || snapWithin(r.Path, m.Main)
	}
	if !main {
		return errors.New("main outside repositories")
	}
	return nil
}
func snapImmutable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".snap-*")
	if e != nil {
		return e
	}
	defer removeBestEffort(f.Name())
	_, e = f.Write(data)
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	e = os.Link(f.Name(), path)
	if errors.Is(e, os.ErrExist) {
		b, err := readStateFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(b, data) {
			return errors.New("immutable snapshot record conflict")
		}
		return nil
	}
	return e
}

var snapSecret = regexp.MustCompile(`(?m)^-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----\s*$`)
var snapAuth = regexp.MustCompile(`(?im)^\s*[^#;\n=]*(?:auth|password|token|keyfile)\s*=`)

// Explicit repository and staging paths own the Git operation even when cx is
// launched by a Git hook with exported repository-local environment variables.
func snapProcessEnv() []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(key) {
		case "GIT_DIR", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE", "GIT_SHALLOW_FILE", "GIT_QUARANTINE_PATH":
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GIT_OPTIONAL_LOCKS=0", "LC_ALL=C")
}
