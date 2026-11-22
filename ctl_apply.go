package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type submissionOrigin struct {
	History      string
	Source       string
	Snapshot     SnapshotID
	Role, Parent string
	JobOptions   []string
	CleanBase    bool
	Command      bool
}

func originFile(host, id, gen string) (string, error) {
	d, e := resultDest(host, id, gen, "0")
	if e != nil {
		return "", e
	}
	return filepath.Join(filepath.Dir(filepath.Dir(d)), "origin.json"), nil
}

type applyPlan struct {
	src, index, head, repo, base, patch string
	original, final, pack, delta        []byte
	git, work                           snapGit
}

func treeChanges(g snapGit, a, b string) (map[string]bool, error) {
	data, e := g.input(nil, "diff-tree", "--no-commit-id", "-r", "--no-renames", "--name-only", "-z", a, b)
	m := map[string]bool{}
	for _, p := range snapList(string(data)) {
		m[p] = true
	}
	return m, e
}
func overlayIndex(g snapGit, tree string, paths []string) error {
	current, e := g.get("write-tree")
	if e != nil {
		return e
	}
	changes, e := treeChanges(g, current, tree)
	if e != nil {
		return e
	}
	var selected bytes.Buffer
	for _, p := range paths {
		if changes[p] {
			selected.WriteString(p)
			selected.WriteByte(0)
		}
	}
	if selected.Len() > 0 {
		_, e = g.input(selected.Bytes(), "--literal-pathspecs", "restore", "--staged", "--source="+tree, "--pathspec-from-file=-", "--pathspec-file-nul")
	}
	return e
}

func planApply(ctx context.Context, store, src, patch, scratch string, row SnapshotPatch, base SnapshotRepo) (applyPlan, error) {
	g := snapGit{ctx: ctx, dir: src}
	p := applyPlan{src: src, git: g, repo: row.Path, patch: patch}
	if base.OID != row.SnapshotOID || base.RepoID != row.RepoID {
		return p, errors.New("patch differs from recorded snapshot")
	}
	top, e := g.get("rev-parse", "--show-toplevel")
	if e != nil {
		return p, e
	}
	src, e = filepath.EvalSymlinks(src)
	if e != nil {
		return p, e
	}
	if top != src {
		return p, errors.New("target must be repository root")
	}
	p.index, e = g.get("rev-parse", "--path-format=absolute", "--git-path", "index")
	if e != nil {
		return p, e
	}
	p.head, e = g.get("rev-parse", "HEAD")
	if e != nil {
		return p, e
	}
	p.base = p.head
	if base.Head != nil {
		p.base = *base.Head
	}
	objects, e := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
	if e != nil {
		return p, e
	}
	obj := filepath.Join(scratch, "objects")
	if e = os.MkdirAll(obj, 0700); e != nil {
		return p, e
	}
	index := filepath.Join(scratch, "index")
	p.original, e = os.ReadFile(p.index)
	if os.IsNotExist(e) {
		e = nil
	} else if e == nil {
		e = os.WriteFile(index, p.original, 0600)
	}
	if e != nil {
		return p, e
	}
	g.env = []string{"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.splitIndex", "GIT_CONFIG_VALUE_0=false", "GIT_INDEX_FILE=" + index, "GIT_OBJECT_DIRECTORY=" + obj, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + strings.Join([]string{objects, filepath.Join(base.bare(store), "objects")}, string(os.PathListSeparator)), "GIT_OPTIONAL_LOCKS=0"}
	stat, e := g.input(nil, "apply", "--numstat", "-z", patch)
	if e != nil {
		return p, e
	}
	var paths, present []string
	selected := map[string]bool{}
	tracked, e := g.input(nil, "ls-files", "-z")
	if e != nil {
		return p, e
	}
	trackedPaths := snapList(string(tracked))
	for _, line := range snapList(string(stat)) {
		f := strings.SplitN(line, "\t", 3)
		if len(f) != 3 || !snapPath(f[2]) || slices.Contains(strings.Split(strings.ToLower(f[2]), "/"), ".git") {
			return p, errors.New("invalid patch paths")
		}
		n := f[2]
		paths = append(paths, n)
		selected[n] = true
		if _, e = os.Lstat(filepath.Join(src, n)); e == nil || slices.Contains(trackedPaths, n) {
			present = append(present, n)
		} else if !os.IsNotExist(e) {
			return p, e
		}
	}
	staged, e := g.get("write-tree")
	if e != nil {
		return p, e
	}
	if len(present) > 0 {
		_, e = g.input([]byte(strings.Join(present, "\x00")+"\x00"), "--literal-pathspecs", "add", "-f", "-A", "--pathspec-from-file=-", "--pathspec-file-nul")
		if e != nil {
			return p, e
		}
	}
	ours, e := g.get("write-tree")
	if e != nil {
		return p, e
	}
	st, e := treeChanges(g, p.head, staged)
	if e != nil {
		return p, e
	}
	unst, e := treeChanges(g, staged, ours)
	if e != nil {
		return p, e
	}
	for n := range st {
		if unst[n] {
			return p, fmt.Errorf("overlapping staged and unstaged changes: %s", n)
		}
	}
	p.work = g
	p.work.env = append(slices.Clone(g.env), "GIT_INDEX_FILE="+filepath.Join(scratch, "work-index"))
	if e = copyFile(index, filepath.Join(scratch, "work-index")); e != nil {
		return p, e
	}
	if _, e = g.get("read-tree", row.SnapshotOID); e != nil {
		return p, e
	}
	if _, e = g.get("apply", "--cached", patch); e != nil {
		return p, e
	}
	theirs, e := g.get("write-tree")
	if e != nil {
		return p, e
	}
	if _, e = g.get("read-tree", row.SnapshotOID); e != nil {
		return p, e
	}
	changes, e := treeChanges(g, p.base, ours)
	if e != nil {
		return p, e
	}
	var fork []string
	for _, n := range paths {
		if !changes[n] {
			fork = append(fork, n)
		}
	}
	if e = overlayIndex(g, p.base, fork); e != nil {
		return p, e
	}
	ancestor, e := g.get("write-tree")
	if e != nil {
		return p, e
	}
	if e = overlayIndex(g, theirs, paths); e != nil {
		return p, e
	}
	diff := []string{"diff", "--cached", "--binary", "--full-index", "--no-renames", "--no-ext-diff", "--no-textconv", "--src-prefix=a/", "--dst-prefix=b/"}
	delta, e := g.input(nil, append(diff, ancestor)...)
	if e != nil {
		return p, e
	}
	if _, e = g.get("read-tree", ours); e != nil {
		return p, e
	}
	if len(delta) > 0 {
		if _, e = g.input(delta, "apply", "--cached", "--3way", "-"); e != nil {
			files, err := g.input(nil, "diff", "--name-only", "--diff-filter=U", "-z")
			if err == nil && len(files) > 0 {
				return p, mergeConflict(snapList(string(files)))
			}
			return p, e
		}
	}
	merged, e := g.get("write-tree")
	if e != nil {
		return p, e
	}
	changed, e := treeChanges(g, ours, merged)
	if e != nil {
		return p, e
	}
	for n := range changed {
		if !selected[n] {
			return p, fmt.Errorf("merge displaces %s", n)
		}
	}
	p.delta, e = g.input(nil, append(diff, ours)...)
	if e != nil {
		return p, e
	}
	if len(p.delta) > 0 {
		if _, e = g.input(p.delta, "apply", "--check", "-"); e != nil {
			return p, e
		}
	}
	if _, e = g.get("read-tree", staged); e != nil {
		return p, e
	}
	if p.original != nil {
		if e = os.WriteFile(index, p.original, 0600); e != nil {
			return p, e
		}
	}
	if e = overlayIndex(g, merged, paths); e != nil {
		return p, e
	}
	p.final, e = os.ReadFile(index)
	if e != nil {
		return p, e
	}
	p.pack, e = g.input([]byte(merged+"\n^"+p.head+"\n"), "pack-objects", "--stdout", "--revs")
	return p, e
}

func installPlans(plans []applyPlan, record func() error) (err error) {
	var locks []string
	defer func() {
		for _, path := range locks {
			_ = os.Remove(path)
		}
	}()
	var undo []func() error
	defer func() {
		if err != nil {
			for i := len(undo) - 1; i >= 0; i-- {
				err = errors.Join(err, undo[i]())
			}
		}
	}()
	for _, p := range plans {
		f, e := os.OpenFile(p.index+".lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_ = f.Close()
		locks = append(locks, p.index+".lock")
		current, e := os.ReadFile(p.index)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if !bytes.Equal(p.original, current) || (current == nil) != (p.original == nil) {
			return errors.New("index changed during preflight")
		}
		head, e := p.git.get("rev-parse", "HEAD")
		if e != nil || head != p.head {
			return errors.New("HEAD changed during preflight")
		}

	}
	for _, p := range plans {
		if _, e := p.git.input(p.pack, "unpack-objects", "-q"); e != nil {
			return e
		}
		if len(p.delta) > 0 {
			if _, e := p.work.input(p.delta, "apply", "--index", "-"); e != nil {
				return e
			}
			undo = append(undo, func() error { _, e := p.work.input(p.delta, "apply", "--index", "-R", "-"); return e })
		}
		if e := atomicWrite(p.index, p.final); e != nil {
			return e
		}
		undo = append(undo, func() error {
			if p.original == nil {
				return os.Remove(p.index)
			}
			return atomicWrite(p.index, p.original)
		})
	}
	return record()
}

type mergeConflict []string

func (m mergeConflict) Error() string { return "conflicts: " + strings.Join(m, ", ") }

// Unbased snapshots have no source commit for a linear merge. Check both
// the index and worktree against the patch in a private object directory.
func unbasedPreflight(ctx context.Context, src, patch, scratch string, exact bool) (snapGit, error) {
	g := snapGit{ctx: ctx, dir: src}
	top, e := g.get("rev-parse", "--show-toplevel")
	if e != nil {
		return g, e
	}
	if exact && top != src {
		return g, errors.New("source root changed")
	}
	g.dir = top
	index, e := g.get("rev-parse", "--path-format=absolute", "--git-path", "index")
	if e != nil {
		return g, e
	}
	objects, e := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
	if e != nil {
		return g, e
	}
	temporary := filepath.Join(scratch, "objects")
	if e = os.MkdirAll(temporary, 0700); e != nil {
		return g, e
	}
	private := filepath.Join(scratch, "index")
	if e = copyFile(index, private); e != nil && !os.IsNotExist(e) {
		return g, e
	}
	trial := g
	trial.env = []string{"GIT_OPTIONAL_LOCKS=0", "GIT_INDEX_FILE=" + private, "GIT_OBJECT_DIRECTORY=" + temporary, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + objects}
	if _, e = trial.get("-c", "core.splitIndex=false", "apply", "--check", "--index", patch); e != nil {
		return g, e
	}
	_, e = trial.get("-c", "core.splitIndex=false", "apply", "--cached", "--3way", patch)
	return g, e
}

func applyTurn(ctx context.Context, dir, id, host string, o ctlOptions, m turnResult) error {
	origin, store, manifest, e := applyOrigin(dir, host, id)
	if e != nil {
		return e
	}
	root := origin.Source
	for range strings.Split(manifest.Main, "/") {
		root = filepath.Dir(root)
	}
	if filepath.Join(root, manifest.Main) != origin.Source {
		return errors.New("snapshot source layout changed")
	}
	scratch, e := os.MkdirTemp("", "cx-preflight-")
	if e != nil {
		return e
	}
	defer removeTreeBestEffort(scratch)
	var plans []applyPlan
	var plain []applyPlan
	var conflicts []string
	var failures error
	count := 0
	for i, row := range m.Repositories {
		if o.repo != "" && o.repo != row.Path && o.repo != row.RepoID {
			continue
		}
		count++
		var base *SnapshotRepo
		for j := range manifest.Repositories {
			if manifest.Repositories[j].Path == row.Path {
				base = &manifest.Repositories[j]
				break
			}
		}
		if base == nil || !snapPath(row.Patch) {
			return errors.New("unexpected result repository")
		}
		src := filepath.Join(root, row.Path)
		if o.dir != "" {
			src, e = filepath.Abs(o.dir)
			if e != nil {
				return e
			}
		}
		patch := filepath.Join(dir, row.Patch)
		temp := filepath.Join(scratch, fmt.Sprint(i))
		if row.ResultBase != nil && row.Base != nil && (base.Head == nil || *row.Base != *base.Head) {
			return errors.New("result base differs from frozen source")
		}
		if base.Head == nil {
			g, err := unbasedPreflight(ctx, src, patch, temp, o.dir == "")
			if err != nil {
				failures = errors.Join(failures, fmt.Errorf("%s: %w", row.Path, err))
			} else {
				plain = append(plain, applyPlan{git: g, src: g.dir, repo: row.Path, patch: patch})
			}
			continue
		}
		if o.dir != "" {
			src, e = (snapGit{ctx: ctx, dir: src}).get("rev-parse", "--show-toplevel")
			if e != nil {
				return e
			}
		}
		plan, planErr := planApply(ctx, store, src, patch, temp, row.SnapshotPatch, *base)
		if planErr != nil {
			var conflict mergeConflict
			if errors.As(planErr, &conflict) {
				for _, name := range conflict {
					conflicts = append(conflicts, row.Path+"/"+name)
				}
			}
			failures = errors.Join(failures, fmt.Errorf("%s: %w", row.Path, planErr))
			continue
		}
		plans = append(plans, plan)
	}
	if (o.dir != "" || o.repo != "") && count != 1 {
		return errors.New("select exactly one changed repository")
	}
	if failures != nil {
		if e = writeJSON(dir+".conflicts.json", conflicts); e != nil {
			return e
		}
		fmt.Fprintln(os.Stderr, failures)
		return ctlExit(1)
	}
	p, _, _ := strings.Cut(id, "/")
	project, e := outputProject(p)
	if e != nil {
		return e
	}
	lock, e := lockUntil(ctx, filepath.Join(project, ".apply.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if len(plans) > 0 && len(plain) > 0 {
		return errors.New("mixed based and unbased patches; select a repository with --repo")
	}
	record := func(changes []applyPlan) error {
		log := filepath.Join(project, "apply-log.jsonl")
		b, readErr := os.ReadFile(log)
		if readErr != nil && !os.IsNotExist(readErr) {
			return readErr
		}
		for _, plan := range changes {
			var base any
			if plan.base != "" {
				base = plan.base
			}
			row, encodeErr := json.Marshal(fields{"job": id, "host": host, "base": base, "turn": m.Turn, "repo": plan.repo, "source": plan.src, "patch": plan.patch, "applied_at": time.Now().UTC().Format(time.RFC3339Nano)})
			if encodeErr != nil {
				return encodeErr
			}
			b = append(append(b, row...), '\n')
		}
		return atomicWrite(log, b)
	}
	if len(plans) > 0 {
		if e = installPlans(plans, func() error { return record(plans) }); e != nil {
			return e
		}
	}
	for _, p := range plain {
		if _, e = p.git.get("apply", "--3way", "--index", p.patch); e != nil {
			return e
		}
		if e = record([]applyPlan{p}); e != nil {
			return e
		}
	}
	var failed bool
	for _, p := range append(plans, plain...) {
		fmt.Println(p.repo + ":")
		b, e := p.git.input(nil, "--no-optional-locks", "status", "--short")
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			failed = true
		} else if _, e = os.Stdout.Write(b); e != nil {
			return e
		}
	}
	if count > 0 {
		fmt.Println("Changes are ready for review and commit.")
	}
	if failed {
		return ctlExit(10)
	}
	return nil
}
func outputProject(p string) (string, error) {
	base := os.Getenv("CX_HOME")
	if base == "" {
		d, e := os.UserCacheDir()
		if e != nil {
			return "", e
		}
		base = filepath.Join(d, "cx")
	}
	dir, e := jobPath(filepath.Join(base, "out"), p)
	if e != nil {
		return "", e
	}
	return dir, os.MkdirAll(dir, 0700)
}

func applyOrigin(dir, host, id string) (submissionOrigin, string, Manifest, error) {
	var origin submissionOrigin
	var m Manifest
	e := readJSON(filepath.Join(filepath.Dir(filepath.Dir(dir)), "origin.json"), &origin)
	store, err := snapStore()
	if err != nil {
		return origin, "", m, err
	}
	if os.IsNotExist(e) {
		route, routeErr := legacyRouteFile(id)
		if routeErr != nil {
			return origin, "", m, routeErr
		}
		var old struct {
			Host, Source, Parent, Role string
			Rerun                      map[string]any
			Snapshot                   SnapshotID `json:"snapshot_id"`
			Command                    bool       `json:"command_job"`
		}
		if routeErr = readJSON(route, &old); routeErr != nil {
			return origin, "", m, routeErr
		}
		if old.Host != host {
			return origin, "", m, errors.New("job route differs from result host")
		}
		origin.Source, origin.Snapshot, origin.Command = old.Source, old.Snapshot, old.Command
		origin.Role, origin.Parent = old.Role, old.Parent
		origin.History, _ = old.Rerun["with_history"].(string)
		origin.CleanBase, _ = old.Rerun["clean_base"].(bool)
		for _, key := range []string{"engine", "model", "effort", "sandbox", "idle_exit", "turn_timeout", "approval_timeout", "stall", "queue_timeout"} {
			if v, ok := old.Rerun[key].(string); ok && v != "" {
				origin.JobOptions = append(origin.JobOptions, "--"+strings.ReplaceAll(key, "_", "-"), v)
			}
		}
		for _, key := range []string{"network", "web_search", "interactive", "hard"} {
			if old.Rerun[key] == true {
				origin.JobOptions = append(origin.JobOptions, "--"+strings.ReplaceAll(key, "_", "-"))
			}
		}
		if old.Rerun["no_tools"] != true && old.Rerun["engine"] != "claude" {
			origin.JobOptions = append(origin.JobOptions, "--job-tools")
		}
		if _, statErr := os.Stat(filepath.Join(store, "manifests", string(old.Snapshot)+".json")); os.IsNotExist(statErr) {
			store = filepath.Join(os.Getenv("CX_HOME"), "out", "snapshot")
		}
	} else if e != nil {
		return origin, "", m, e
	}
	if origin.Snapshot == "" {
		return origin, "", m, errors.New("in-place jobs have no patch")
	}
	m, _, e = snapManifest(store, string(origin.Snapshot))
	return origin, store, m, e
}
