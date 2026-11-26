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
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

type SnapshotID string
type FreezeOptions struct {
	History      string
	Repositories []projectRepository
}

// Manifest describes a frozen snapshot.
type Manifest struct {
	Workspace    *SnapshotWorkspace `json:"workspace"`
	Main         string             `json:"main"`
	Repositories []SnapshotRepo     `json:"repositories"`
	Version      int                `json:"version"`
}

// SnapshotRepo describes one frozen repository.
type SnapshotRepo struct {
	Head    *string  `json:"head"`
	Base    string   `json:"base,omitempty"`
	OID     string   `json:"oid"`
	Path    string   `json:"path"`
	RepoID  string   `json:"repo_id"`
	StoreID string   `json:"store_id,omitempty"`
	Include []string `json:"include,omitempty"`
	Shallow []string `json:"shallow,omitempty"`
	History bool     `json:"history,omitempty"`
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

func (r SnapshotRepo) excludeIncludes(scope []string) []string {
	for _, name := range r.Include {
		scope = append(scope, ":(top,exclude,literal)"+name)
	}
	return scope
}

func snapStore() (string, error) {
	if s := os.Getenv("CX_SNAPSHOT_STORE"); s != "" {
		return filepath.Abs(s)
	}
	r, e := stateRoot()
	return filepath.Join(r, "snapshot"), e
}
func snapSum(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func snapJSON(v any) ([]byte, error) {
	// Encode objects with sorted keys for the v1 snapshot hash, independent
	// of how fields are laid out in memory.
	raw, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var canonical any
	if e = dec.Decode(&canonical); e != nil {
		return nil, e
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if e = enc.Encode(canonical); e != nil {
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
	config := []string{"-c", "core.hooksPath=" + os.DevNull, "-c", "core.fsmonitor=false", "-c", "commit.gpgSign=false"}
	if runtime.GOOS == "windows" {
		// Snapshot mirrors and bundle paths include content hashes.
		config = append(config, "-c", "core.longpaths=true")
	}
	return snapCommand(g.ctx, g.dir, g.env, b, "git", append(config, args...)...)
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
func snapIncludePath(s string) bool {
	return snapPath(s) && !slices.Contains(strings.Split(strings.ToLower(s), "/"), ".git")
}
func snapIncludePattern(s string) bool {
	_, e := path.Match(s, "")
	return e == nil && snapIncludePath(s)
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
		for j, name := range r.Include {
			if !snapIncludePath(name) || j > 0 && r.Include[j-1] >= name {
				return errors.New("invalid snapshot include paths")
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

// Resolve the source side of the frozen layout. A multi-repository project's
// Main may be the Git root even when the user submitted one of its subdirectories.
func (m Manifest) sourceRoot(ctx context.Context, src string) (string, error) {
	src, e := filepath.EvalSymlinks(src)
	if e != nil {
		return "", e
	}
	relativeRoot := func(p string) (string, bool) {
		root := p
		for range strings.SplitSeq(m.Main, "/") {
			root = filepath.Dir(root)
		}
		return root, filepath.Join(root, m.Main) == p
	}
	for _, r := range m.Repositories {
		if r.Path == m.Main {
			g := snapGit{ctx: ctx, dir: src}
			if top, gitErr := g.get("rev-parse", "--show-toplevel"); gitErr == nil {
				top, e = filepath.EvalSymlinks(top)
				if e != nil {
					return "", e
				}
				if root, ok := relativeRoot(top); ok {
					return root, nil
				}
			}
			break
		}
	}
	if root, ok := relativeRoot(src); ok {
		return root, nil
	}
	return "", errors.New("source does not match snapshot layout")
}
func snapImmutable(path string, data []byte) error {
	return publishImmutable(path, data, "")
}

func publishImmutable(path string, data []byte, durableRoot string) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := createJobTemp(dir, ".snap-")
	if e != nil {
		return e
	}
	defer removeBestEffort(f.Name())
	e = writeFrame(f.Write, data)
	if e == nil && durableRoot != "" {
		e = syncJobFile(f)
	}
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	e = os.Link(f.Name(), path)
	if errors.Is(e, os.ErrExist) {
		b, err := readSharedFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(b, data) {
			return errors.New("immutable snapshot record conflict")
		}
		e = nil
		if durableRoot != "" {
			e = syncExistingJobFile(path, syncJobFile)
		}
	}
	if e == nil && durableRoot != "" {
		e = syncJobTree(dir, durableRoot)
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
