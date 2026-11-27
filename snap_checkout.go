package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

type PreparedSnapshot struct {
	Env         map[string]string `json:"env"`
	BaseOID     *string           `json:"base_oid"`
	ReviewDiff  *string           `json:"review_diff"`
	Source      string            `json:"source"`
	SnapshotOID string            `json:"snapshot_oid"`
}

const preparedBlobRole = "prepared"

type blobRef struct {
	Role   string `json:"role"`
	SHA256 string `json:"sha256"`
}

func blobPath(dir, sum string) string {
	return filepath.Join(dir, "blobs", sum+".json")
}

func readPreparedSnapshot(ctx context.Context, dir string) (PreparedSnapshot, bool, error) {
	var prepared PreparedSnapshot
	record, _, err := latestStateKind(dir, "blob")
	if err != nil || record.Seq == 0 {
		return prepared, false, err
	}
	var ref blobRef
	if decodeErr := json.Unmarshal(record.Value, &ref); decodeErr != nil {
		return prepared, false, decodeErr
	}
	if syncErr := syncJobDir(filepath.Join(dir, "state")); syncErr != nil {
		return prepared, false, syncErr
	}
	if ref.Role != preparedBlobRole || !snapHex(ref.SHA256, 64) {
		return prepared, false, errors.New("invalid prepared blob reference")
	}
	f, err := openJobContent(ctx, blobPath(dir, ref.SHA256))
	if err != nil {
		return prepared, false, err
	}
	defer closeBestEffort(f)
	b, err := io.ReadAll(f)
	if err != nil {
		return prepared, false, err
	}
	if snapSum(b) != ref.SHA256 {
		return prepared, false, errors.New("prepared blob does not match its state record")
	}
	if err := json.Unmarshal(b, &prepared); err != nil {
		return prepared, false, err
	}
	return prepared, true, nil
}

// The checkout exists before runner registration and may be shared by forks.
// Its owner.lock serializes materialization and publication of its blob record.
func publishPreparedSnapshot(dir string, prepared PreparedSnapshot) error {
	b, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	ref := blobRef{Role: preparedBlobRole, SHA256: snapSum(b)}
	if publishErr := publishImmutable(blobPath(dir, ref.SHA256), b, filepath.Dir(dir)); publishErr != nil {
		return publishErr
	}
	w, err := openStateLog(dir)
	if err != nil {
		return err
	}
	defer closeBestEffort(w)
	record, err := newRecord("blob", ref)
	if err != nil {
		return err
	}
	return w.commit(record)
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
	defer closeBestEffort(lock)
	_, err = g.get(append([]string{"worktree"}, args...)...)
	return err
}

func removeUnpublishedWorktree(mirror snapGit, dst string) error {
	parent := diskPath(dst)
	relative, err := filepath.Rel(parent, dst)
	if err != nil {
		return err
	}
	physical, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	actual := filepath.Join(physical, relative)
	paths, err := mirror.get("worktree", "list", "--porcelain", "-z")
	if err != nil {
		return err
	}
	for field := range strings.SplitSeq(paths, "\x00") {
		path, ok := strings.CutPrefix(field, "worktree ")
		if ok && (filepath.Clean(path) == filepath.Clean(dst) || filepath.Clean(path) == actual) {
			return mirror.worktree("remove", "--force", dst)
		}
	}
	return nil
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
	lock, e := lockUntil(ctx, filepath.Join(job, "owner.lock"))
	if e != nil {
		return out, e
	}
	defer closeBestEffort(lock)
	b, e := snapJSON(m)
	if e != nil {
		return out, e
	}
	if e = snapImmutable(filepath.Join(job, "input/manifest.json"), b); e != nil {
		return out, e
	}
	if e = validateSnapshotRepos(ctx, target, m); e != nil {
		return out, fmt.Errorf("snapshot repository unavailable, republish snapshot %s: %w", snapSum(b), e)
	}
	if prepared, found, err := readPreparedSnapshot(ctx, job); err != nil || found {
		return prepared, err
	}
	work := filepath.Join(job, "work")
	tree := filepath.Join(work, "tree")
	if _, err := os.Stat(work); err == nil {
		// Recover an unpublished attempt while holding checkout ownership.
		for _, repo := range slices.Backward(m.Repositories) {
			mirror := snapGit{ctx: ctx, dir: repo.bare(target)}
			dst := filepath.Join(tree, repo.Path)
			if removeErr := removeUnpublishedWorktree(mirror, dst); removeErr != nil {
				return out, removeErr
			}
		}
		if removeErr := os.RemoveAll(work); removeErr != nil {
			return out, removeErr
		}
	} else if !os.IsNotExist(err) {
		return out, err
	}
	if e = os.Mkdir(work, 0700); e != nil {
		return out, e
	}
	var added []SnapshotRepo
	ok := false
	defer func() {
		if !ok {
			for _, r := range slices.Backward(added) {
				mirror := snapGit{ctx: ctx, dir: r.bare(target)}
				_ = mirror.worktree("remove", "--force", filepath.Join(tree, r.Path))
			}
			_ = os.RemoveAll(work)
		}
	}()
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
		added = append(added, r)
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
			args := []string{"diff", "--binary", "--no-ext-diff", "--no-textconv", "--src-prefix=a/" + prefix, "--dst-prefix=b/" + prefix, r.Base, r.OID, "--"}
			b, diffErr := g.input(nil, r.excludeIncludes(args)...)
			if diffErr != nil {
				return out, diffErr
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
		if e = snapImmutable(*out.ReviewDiff, review.Bytes()); e != nil {
			return out, e
		}
	}
	if w := m.Workspace; w != nil {
		p := filepath.Join(work, "go.work")
		if e = writeJobBytes(p, []byte(w.Text)); e != nil {
			return out, e
		}
		if w.Sum != nil {
			if e = writeJobBytes(p+".sum", []byte(*w.Sum)); e != nil {
				return out, e
			}
		}
		out.Env["GOWORK"] = p
		out.Env["GOFLAGS"] = ""
	}
	if runtime.GOOS == "windows" {
		out.Env["PATH"] = os.Getenv("PATH") + string(os.PathListSeparator) + filepath.Join(os.Getenv("APPDATA"), "npm")
	}
	if e = publishPreparedSnapshot(job, out); e != nil {
		// A link can already be visible if only its final directory flush
		// failed. Keep the materialization for a reader to confirm.
		record, _, _ := latestStateKind(job, "blob")
		ok = record.Seq != 0
		return out, e
	}
	ok = true
	return out, nil
}
func readCheckoutManifest(job string) (Manifest, error) {
	baseline, err := readJobBaseline(job)
	if err != nil {
		return Manifest{}, err
	}
	if err = checkRefreshInstall(job, baseline); err != nil {
		return Manifest{}, err
	}
	if baseline.Snapshot != "" {
		return loadSnapshot(baseline.Snapshot)
	}
	var m Manifest
	if err := readJSON(filepath.Join(job, "input", "manifest.json"), &m); err != nil {
		return m, err
	}
	return m, m.validate()
}

// Resolve the checkout through its working directory so forks keep the same
// repository roots as their source job.
func snapshotMemberRoots(root, cwd string) ([]string, error) {
	bound, err := bindWorkspace(root, cwd)
	if err != nil {
		return nil, err
	}
	for _, id := range bound.ids {
		job := filepath.Join(root, ".work", id)
		tree, err := filepath.EvalSymlinks(filepath.Join(job, "work", "tree"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !snapWithin(tree, bound.path) {
			continue
		}
		m, err := readCheckoutManifest(job)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if bound.path != filepath.Join(tree, m.Main) {
			continue
		}
		roots := make([]string, 0, len(m.Repositories))
		for _, repo := range m.Repositories {
			dir := filepath.Join(tree, repo.Path)
			physical, err := filepath.EvalSymlinks(dir)
			if err != nil {
				return nil, err
			}
			if physical != dir {
				return nil, fmt.Errorf("snapshot repository root changed: %s", repo.Path)
			}
			roots = append(roots, dir)
		}
		return roots, nil
	}
	return nil, nil
}

func Collect(ctx context.Context, job string) (SnapshotResult, error) {
	var out SnapshotResult
	m, e := readCheckoutManifest(job)
	if e != nil {
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
			_ = os.RemoveAll(out.Dir)
		}
	}()
	tree := filepath.Join(job, "work/tree")
	art := filepath.Join(tree, m.Main, "artifacts")
	counts := map[string]int{}
	for _, r := range m.Repositories {
		counts[r.RepoID]++
	}
	for i, r := range m.Repositories {
		if e = collectSnapshotPatch(ctx, &out, r, filepath.Join(tree, r.Path), art, i, counts[r.RepoID]); e != nil {
			return out, e
		}
	}
	src := filepath.Dir(art)
	source, e := filepath.EvalSymlinks(src)
	if e != nil {
		return out, e
	}
	root, e := filepath.EvalSymlinks(tree)
	if e != nil {
		return out, e
	}
	if !snapWithin(root, source) {
		return out, errors.New("artifact directory escapes worktree")
	}
	if e = copySnapshotArtifacts(art, &out); e != nil {
		return out, e
	}
	ok = true
	return out, nil
}

func collectSnapshotPatch(ctx context.Context, out *SnapshotResult, r SnapshotRepo, repo, art string, i, count int) error {
	g := snapGit{ctx: ctx, dir: repo}
	index, indexErr := filepath.Abs(filepath.Join(out.Dir, "index"))
	if indexErr != nil {
		return indexErr
	}
	obj, indexErr := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
	if indexErr != nil {
		return indexErr
	}
	scratch, indexErr := os.MkdirTemp(out.Dir, ".objects-")
	if indexErr != nil {
		return indexErr
	}
	defer removeTreeBestEffort(scratch)
	g.env = []string{"GIT_INDEX_FILE=" + index, "GIT_OPTIONAL_LOCKS=0", "GIT_OBJECT_DIRECTORY=" + scratch, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + obj}
	if _, indexErr = g.get("read-tree", r.OID); indexErr != nil {
		return indexErr
	}
	scope := []string{"--", "."}
	if snapWithin(repo, art) {
		rel, relErr := filepath.Rel(repo, art)
		if relErr != nil {
			return relErr
		}
		scope = append(scope, ":(top,exclude,literal)"+filepath.ToSlash(rel))
	}
	if _, indexErr = g.get(append([]string{"add", "-A"}, scope...)...); indexErr != nil {
		return indexErr
	}
	ignored, indexErr := (snapGit{ctx: ctx, dir: repo}).input(nil, "ls-files", "-ci", "--exclude-standard", "-z")
	if indexErr != nil {
		return indexErr
	}
	var force bytes.Buffer
	for _, n := range snapList(string(ignored)) {
		if _, found := slices.BinarySearch(r.Include, n); found {
			continue
		}
		p := filepath.Join(repo, n)
		if st, statErr := os.Lstat(p); statErr == nil && !st.IsDir() && !snapWithin(art, p) {
			force.WriteString(n)
			force.WriteByte(0)
		}
	}
	if force.Len() > 0 {
		if _, indexErr = g.input(force.Bytes(), "--literal-pathspecs", "add", "-f", "--pathspec-from-file=-", "--pathspec-file-nul"); indexErr != nil {
			return indexErr
		}
	}
	if runtime.GOOS == "windows" {
		baseline := r.Base
		if baseline == "" {
			baseline = r.OID
		}
		if indexErr = snapWindowsModes(g, baseline); indexErr != nil {
			return indexErr
		}
	}
	// Keep includes in the staging scope. Excluding the only tracked
	// child of an ignored directory makes Git reject the directory at add.
	scope = r.excludeIncludes(scope)
	opts := []string{"diff", "--cached", "--no-ext-diff", "--no-textconv", "--no-renames", "--no-relative"}
	args := append(append([]string{}, opts...), "--binary", "--full-index", "--src-prefix=a/", "--dst-prefix=b/", r.OID)
	data, indexErr := g.input(nil, append(args, scope...)...)
	if indexErr != nil {
		return indexErr
	}
	if len(data) > 0 {
		name := r.RepoID + ".patch"
		if count > 1 {
			name = fmt.Sprintf("%s.%d.patch", r.RepoID, i)
		}
		if indexErr = writeJobBytes(filepath.Join(out.Dir, name), data); indexErr != nil {
			return indexErr
		}
		baseTree, treeErr := g.get("rev-parse", r.OID+"^{tree}")
		if treeErr != nil {
			return treeErr
		}
		names, treeErr := g.input(nil, append(append(opts, "--name-only", "-z", r.OID), scope...)...)
		if treeErr != nil {
			return treeErr
		}
		out.Repositories = append(out.Repositories, SnapshotPatch{r.Path, r.RepoID, r.key(), r.OID, baseTree, name, len(data), bytes.Count(names, []byte{0})})
	}
	if indexErr = os.Remove(index); indexErr != nil {
		return indexErr
	}
	return nil
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
