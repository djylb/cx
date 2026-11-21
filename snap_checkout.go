package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

type PreparedSnapshot struct {
	Source      string            `json:"source"`
	Env         map[string]string `json:"env"`
	BaseOID     *string           `json:"base_oid"`
	SnapshotOID string            `json:"snapshot_oid"`
	ReviewDiff  *string           `json:"review_diff"`
}
type SnapshotPatch struct {
	Path         string `json:"path"`
	RepoID       string `json:"repo_id"`
	StoreID      string `json:"store_id"`
	SnapshotOID  string `json:"snapshot_oid"`
	SnapshotTree string `json:"snapshot_tree"`
	Patch        string `json:"patch"`
	Bytes        int    `json:"bytes"`
	Files        int    `json:"files"`
}
type SnapshotArtifact struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}
type SnapshotResult struct {
	Dir          string             `json:"-"`
	Repositories []SnapshotPatch    `json:"repositories"`
	Artifacts    []SnapshotArtifact `json:"artifacts"`
}

// Serialize worktree mutations with mirror writes on the same bare repository.
func (g snapGit) worktree(args ...string) error {
	lock, err := transferLock(g.ctx, g.dir+".lock")
	if err != nil {
		return err
	}
	defer lock.Close()
	_, err = g.get(append([]string{"worktree"}, args...)...)
	return err
}

func Materialize(ctx context.Context, job string, m Manifest, target string) (PreparedSnapshot, error) {
	var out PreparedSnapshot
	var e error
	if e = m.validate(); e != nil {
		return out, e
	}
	if target == "" {
		target, e = snapStore()
		if e != nil {
			return out, e
		}
	}
	target, e = filepath.Abs(target)
	if e != nil {
		return out, e
	}
	job, e = filepath.Abs(job)
	if e != nil {
		return out, e
	}
	if e = os.MkdirAll(job, 0700); e != nil {
		return out, e
	}
	lock, e := lockUntil(ctx, filepath.Join(job, ".materialize.lock"))
	if e != nil {
		return out, e
	}
	defer lock.Close()
	b, e := snapJSON(m)
	if e != nil {
		return out, e
	}
	if e = snapImmutable(filepath.Join(job, "input/manifest.json"), b); e != nil {
		return out, e
	}
	if e = readJSON(filepath.Join(job, "prepared.json"), &out); e == nil {
		return out, nil
	} else if !os.IsNotExist(e) {
		return out, e
	}
	work := filepath.Join(job, "work")
	if e = os.Mkdir(work, 0700); e != nil {
		return out, e
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(work)
		}
	}()
	tree := filepath.Join(work, "tree")
	out.Source = filepath.Join(tree, m.Main)
	out.Env = map[string]string{"GOWORK": "off", "GOFLAGS": "-mod=mod"}
	var review bytes.Buffer
	for _, r := range m.Repositories {
		dst := filepath.Join(tree, r.Path)
		if e = os.MkdirAll(dst, 0700); e != nil {
			return out, e
		}
		g := snapGit{ctx: ctx, dir: dst}
		mirror := snapGit{ctx: ctx, dir: r.bare(target)}
		if e = snapVerifyRepo(mirror, r); e != nil {
			return out, e
		}
		attributes := filepath.Join(mirror.dir, "info", "attributes")
		if err := snapImmutable(attributes, []byte("* -text !eol -crlf -ident !filter !working-tree-encoding\n")); err != nil {
			return out, err
		}
		if runtime.GOOS == "windows" {
			if e = snapWindowsNames(mirror, r.OID); e != nil {
				return out, e
			}
		}
		if err := mirror.worktree("add", "-q", "--force", "--detach", dst, r.OID); err != nil {
			return out, err
		}
		defer func() {
			if !ok {
				_ = mirror.worktree("remove", "--force", dst)
			}
		}()
		if runtime.GOOS == "windows" {
			if e = snapWindowsLinks(g); e != nil {
				return out, e
			}
		}
		if r.Base != "" {
			if _, e = g.get("reset", "-q", "--mixed", "-N", r.Base); e != nil {
				return out, e
			}
			prefix := ""
			if len(m.Repositories) > 1 {
				prefix = r.Path + "/"
			}
			b, e := g.input(nil, "diff", "--binary", "--no-ext-diff", "--no-textconv", "--src-prefix=a/"+prefix, "--dst-prefix=b/"+prefix, r.Base, r.OID, "--")
			if e != nil {
				return out, e
			}
			review.Write(b)
			p := filepath.Join(job, "review.diff")
			out.ReviewDiff = &p
		}
		if snapWithin(dst, out.Source) {
			out.SnapshotOID = r.OID
			if r.Base != "" {
				base := r.Base
				out.BaseOID = &base
			}
		}
	}
	if out.ReviewDiff != nil {
		if e = atomicWrite(*out.ReviewDiff, review.Bytes()); e != nil {
			return out, e
		}
	}
	if w := m.Workspace; w != nil {
		p := filepath.Join(work, "go.work")
		if e = os.WriteFile(p, []byte(w.Text), 0600); e != nil {
			return out, e
		}
		if w.Sum != nil {
			if e = os.WriteFile(p+".sum", []byte(*w.Sum), 0600); e != nil {
				return out, e
			}
		}
		out.Env["GOWORK"] = p
		out.Env["GOFLAGS"] = ""
	}
	if runtime.GOOS == "windows" {
		tmp := filepath.Join(job, "tmp")
		if e = os.MkdirAll(tmp, 0700); e != nil {
			return out, e
		}
		for _, k := range []string{"TEMP", "TMP", "GOTMPDIR"} {
			out.Env[k] = tmp
		}
		out.Env["PATH"] = os.Getenv("PATH") + string(os.PathListSeparator) + filepath.Join(os.Getenv("APPDATA"), "npm")
	}
	if e = writeJSON(filepath.Join(job, "prepared.json"), out); e != nil {
		return out, e
	}
	ok = true
	return out, nil
}
func Collect(ctx context.Context, job string) (SnapshotResult, error) {
	var m Manifest
	var out SnapshotResult
	raw, e := os.ReadFile(filepath.Join(job, "input/manifest.json"))
	if e != nil {
		return out, e
	}
	if e = json.Unmarshal(raw, &m); e != nil {
		return out, e
	}
	if e = m.validate(); e != nil {
		return out, e
	}
	job, e = filepath.Abs(job)
	if e != nil {
		return out, e
	}
	out.Dir, e = os.MkdirTemp(job, "snapshot-result-")
	if e != nil {
		return out, e
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(out.Dir)
		}
	}()
	tree := filepath.Join(job, "work/tree")
	art := filepath.Join(tree, m.Main, "artifacts")
	counts := map[string]int{}
	for _, r := range m.Repositories {
		counts[r.RepoID]++
	}
	for i, r := range m.Repositories {
		repo := filepath.Join(tree, r.Path)
		g := snapGit{ctx: ctx, dir: repo}
		index, e := filepath.Abs(filepath.Join(out.Dir, "index"))
		if e != nil {
			return out, e
		}
		obj, e := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
		if e != nil {
			return out, e
		}
		scratch, e := os.MkdirTemp(out.Dir, ".objects-")
		if e != nil {
			return out, e
		}
		defer os.RemoveAll(scratch)
		g.env = []string{"GIT_INDEX_FILE=" + index, "GIT_OPTIONAL_LOCKS=0", "GIT_OBJECT_DIRECTORY=" + scratch, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + obj}
		if _, e = g.get("read-tree", r.OID); e != nil {
			return out, e
		}
		scope := []string{"--", "."}
		if snapWithin(repo, art) {
			rel, e := filepath.Rel(repo, art)
			if e != nil {
				return out, e
			}
			scope = append(scope, ":(top,exclude,literal)"+filepath.ToSlash(rel))
		}
		if _, e = g.get(append([]string{"add", "-A"}, scope...)...); e != nil {
			return out, e
		}
		ignored, e := (snapGit{ctx: ctx, dir: repo}).input(nil, "ls-files", "-ci", "--exclude-standard", "-z")
		if e != nil {
			return out, e
		}
		var force bytes.Buffer
		for _, n := range snapList(string(ignored)) {
			p := filepath.Join(repo, n)
			if st, e := os.Lstat(p); e == nil && !st.IsDir() && !snapWithin(art, p) {
				force.WriteString(n)
				force.WriteByte(0)
			}
		}
		if force.Len() > 0 {
			if _, e = g.input(force.Bytes(), "--literal-pathspecs", "add", "-f", "--pathspec-from-file=-", "--pathspec-file-nul"); e != nil {
				return out, e
			}
		}
		if runtime.GOOS == "windows" {
			baseline := r.Base
			if baseline == "" {
				baseline = r.OID
			}
			if e = snapWindowsModes(g, baseline); e != nil {
				return out, e
			}
		}
		opts := []string{"diff", "--cached", "--no-ext-diff", "--no-textconv", "--no-renames", "--no-relative"}
		args := append(append([]string{}, opts...), "--binary", "--full-index", "--src-prefix=a/", "--dst-prefix=b/", r.OID)
		data, e := g.input(nil, append(args, scope...)...)
		if e != nil {
			return out, e
		}
		if len(data) > 0 {
			name := r.RepoID + ".patch"
			if counts[r.RepoID] > 1 {
				name = fmt.Sprintf("%s.%d.patch", r.RepoID, i)
			}
			if e = os.WriteFile(filepath.Join(out.Dir, name), data, 0600); e != nil {
				return out, e
			}
			tree, e := g.get("rev-parse", r.OID+"^{tree}")
			if e != nil {
				return out, e
			}
			names, e := g.input(nil, append(append(opts, "--name-only", "-z", r.OID), scope...)...)
			if e != nil {
				return out, e
			}
			out.Repositories = append(out.Repositories, SnapshotPatch{r.Path, r.RepoID, r.key(), r.OID, tree, name, len(data), bytes.Count(names, []byte{0})})
		}
		if e = os.Remove(index); e != nil {
			return out, e
		}
	}
	src := filepath.Dir(art)
	real, e := filepath.EvalSymlinks(src)
	if e != nil {
		return out, e
	}
	root, e := filepath.EvalSymlinks(tree)
	if e != nil {
		return out, e
	}
	if !snapWithin(root, real) {
		return out, errors.New("artifact directory escapes worktree")
	}
	if e = copySnapshotArtifacts(art, &out); e != nil {
		return out, e
	}
	ok = true
	return out, nil
}

func copySnapshotArtifacts(art string, out *SnapshotResult) error {
	if st, e := os.Lstat(art); e == nil && st.IsDir() {
		e = filepath.WalkDir(art, func(p string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if !d.Type().IsRegular() {
				return nil
			}
			rel, e := filepath.Rel(filepath.Dir(art), p)
			if e != nil {
				return e
			}
			rel = filepath.ToSlash(rel)
			if !snapPath(rel) || !snapPortablePath(rel) {
				return errors.New("invalid artifact path")
			}
			dst := filepath.Join(out.Dir, rel)
			if e = os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
				return e
			}
			if e = copyFile(p, dst); e != nil {
				return e
			}
			st, e := os.Stat(dst)
			if e != nil {
				return e
			}
			out.Artifacts = append(out.Artifacts, SnapshotArtifact{rel, st.Size()})
			return nil
		})
		if e != nil {
			return e
		}
	} else if e != nil && !os.IsNotExist(e) {
		return e
	}
	return nil
}
