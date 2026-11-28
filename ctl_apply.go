package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type submissionOrigin struct {
	History       string
	Source        string
	Snapshot      SnapshotID
	Role, Parent  string
	JobOptions    []string
	ModuleOptions []string `json:",omitempty"`
	CleanBase     bool
	Command       bool
}

func originFile(host, id, gen string) (string, error) {
	d, e := resultDest(host, id, gen, "0")
	if e != nil {
		return "", e
	}
	return filepath.Join(filepath.Dir(filepath.Dir(d)), "origin.json"), nil
}

type applyPlan struct {
	src, index, head, repo, base, patch, resultTree string
	targetKey                                       string
	original, final, pack, delta                    []byte
	git, work                                       snapGit
	conflicts, paths                                []string
	since                                           int64
	incremental                                     bool
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

func applyTargetIndex(g snapGit) (string, string, error) {
	index, err := g.get("rev-parse", "--path-format=absolute", "--git-path", "index")
	if err != nil {
		return "", "", err
	}
	key, err := jobDirectoryKey(filepath.Dir(index))
	return index, key, err
}

func planApply(ctx context.Context, store, src, patch, scratch string, row turnPatch, base SnapshotRepo, allowConflicts bool) (applyPlan, error) {
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
	p.src = src
	p.index, p.targetKey, e = applyTargetIndex(g)
	if e != nil {
		return p, e
	}
	p.head, e = g.get("rev-parse", "--verify", "HEAD")
	if e != nil && base.Head != nil {
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
	alternates := []string{objects, filepath.Join(base.bare(store), "objects")}
	patchBase := row.SnapshotOID
	if row.mergeBase != "" {
		patchBase = row.mergeBase
		alternates = append(alternates, patch+".objects")
	}
	g.env = []string{"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.splitIndex", "GIT_CONFIG_VALUE_0=false", "GIT_INDEX_FILE=" + index, "GIT_OBJECT_DIRECTORY=" + obj, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + strings.Join(alternates, string(os.PathListSeparator)), "GIT_OPTIONAL_LOCKS=0"}
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
	p.paths = paths
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
	headTree := p.head
	if headTree == "" {
		headTree, e = g.get("mktree")
		if e != nil {
			return p, e
		}
	}
	st, e := treeChanges(g, headTree, staged)
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
	theirs, e := resultTree(g, patchBase, filepath.Dir(patch), turnPatch{SnapshotPatch: SnapshotPatch{
		SnapshotOID: patchBase, Patch: filepath.Base(patch),
	}})
	if e != nil {
		return p, e
	}
	ancestor, e := g.get("rev-parse", patchBase+"^{tree}")
	if e != nil {
		return p, e
	}
	if row.mergeBase == "" && base.Head != nil {
		if _, e = g.get("read-tree", patchBase); e != nil {
			return p, e
		}
		changes, err := treeChanges(g, p.base, ours)
		if err != nil {
			return p, err
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
		if ancestor, e = g.get("write-tree"); e != nil {
			return p, e
		}
	}
	if row.mergeBase != "" || base.Head == nil {
		p.base = ancestor
	}
	merged, stages, e := mergeApplyTrees(g, ancestor, ours, theirs)
	if e != nil {
		return p, e
	}
	for _, stage := range stages {
		_, name, _ := strings.Cut(stage, "\t")
		if !slices.Contains(p.conflicts, name) {
			p.conflicts = append(p.conflicts, name)
		}
	}
	if len(p.conflicts) > 0 && !allowConflicts {
		return p, nil
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
	if _, e = g.get("read-tree", merged); e != nil {
		return p, e
	}
	p.delta, e = g.input(nil, append(append([]string{}, patchDiff...), ours)...)
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
	if len(stages) > 0 {
		var info bytes.Buffer
		for _, name := range p.conflicts {
			if !selected[name] {
				return p, fmt.Errorf("merge conflict displaces %s", name)
			}
			info.WriteString("0 ")
			info.WriteString(strings.Repeat("0", len(base.OID)))
			info.WriteByte('\t')
			info.WriteString(name)
			info.WriteByte(0)
		}
		for _, stage := range stages {
			info.WriteString(stage)
			info.WriteByte(0)
		}
		if _, e = g.input(info.Bytes(), "update-index", "-z", "--index-info"); e != nil {
			return p, e
		}
	}
	if len(p.conflicts) == 0 {
		p.resultTree, e = g.get("write-tree")
		if e != nil {
			return p, e
		}
	}
	p.final, e = os.ReadFile(index)
	if e != nil {
		return p, e
	}
	revs := []string{merged}
	if p.resultTree != "" && p.resultTree != merged {
		revs = append(revs, p.resultTree)
	}
	for _, stage := range stages {
		meta, _, _ := strings.Cut(stage, "\t")
		revs = append(revs, strings.Fields(meta)[1])
	}
	if p.head != "" {
		revs = append(revs, "^"+p.head)
	}
	p.pack, e = g.input([]byte(strings.Join(revs, "\n")+"\n"), "pack-objects", "--stdout", "--revs")
	return p, e
}

var errApplyRollback = errors.New("could not roll back checkout changes")

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
			for _, u := range slices.Backward(undo) {
				if undoErr := u(); undoErr != nil {
					err = errors.Join(err, errApplyRollback, undoErr)
				}
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
		head, _ := p.git.get("rev-parse", "--verify", "HEAD")
		if head != p.head {
			return errors.New("HEAD changed during preflight")
		}

	}
	for _, p := range plans {
		if len(p.pack) != 0 {
			if _, e := p.git.input(p.pack, "unpack-objects", "-q"); e != nil {
				return e
			}
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

// merge-tree writes a result tree even when it reports conflicts. Its stage
// records preserve unmerged paths and the blobs needed to resolve them.
func mergeApplyTrees(g snapGit, ancestor, ours, theirs string) (tree string, stages []string, err error) {
	if ancestor == ours || ours == theirs {
		return theirs, nil, nil
	}
	if ancestor == theirs {
		return ours, nil, nil
	}
	g.env = append(slices.Clone(g.env), "GIT_AUTHOR_NAME=cx", "GIT_AUTHOR_EMAIL=cx@example.invalid",
		"GIT_COMMITTER_NAME=cx", "GIT_COMMITTER_EMAIL=cx@example.invalid")
	base, err := g.get("commit-tree", ancestor, "-m", "apply base")
	if err != nil {
		return "", nil, err
	}
	left, err := g.get("commit-tree", ours, "-p", base, "-m", "current worktree")
	if err != nil {
		return "", nil, err
	}
	right, err := g.get("commit-tree", theirs, "-p", base, "-m", "job result")
	if err != nil {
		return "", nil, err
	}
	out, err := g.input(nil, "-c", "merge.renames=false", "merge-tree", "--write-tree", "-z", left, right)
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) {
		return "", nil, err
	}
	entries := strings.Split(string(out), "\x00")
	if !snapOID(entries[0]) {
		return "", nil, errors.Join(errors.New("invalid git merge-tree result"), err)
	}
	for _, entry := range entries[1:] {
		if entry == "" {
			break
		}
		meta, name, ok := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if !ok || !snapPath(name) || len(fields) != 3 || !snapOID(fields[1]) {
			return "", nil, errors.New("invalid git merge-tree conflict path")
		}
		stages = append(stages, entry)
	}
	if err != nil && len(stages) == 0 {
		return "", nil, err
	}
	return entries[0], stages, nil
}

func applyTurn(ctx context.Context, dir, project, id, host, runID string, o ctlOptions, result turnResult, rows []selectedPatch) error {
	changed := 0
	for _, row := range rows {
		if row.Patch != "" {
			changed++
		}
	}
	if o.repo != "" && len(rows) != 1 || o.dir != "" && (changed > 1 || changed == 0 && len(rows) != 1) {
		return errors.New("select exactly one changed repository")
	}
	origin, store, manifest, e := applyOrigin(dir)
	if e != nil {
		return e
	}
	if changed == 0 {
		return nil
	}
	root, e := manifest.sourceRoot(ctx, origin.Source)
	if e != nil {
		return e
	}
	scratch, e := os.MkdirTemp("", "cx-preflight-")
	if e != nil {
		return e
	}
	defer removeTreeBestEffort(scratch)
	var plans []applyPlan
	var conflicts []string
	var failures error
	allowConflicts := o.allowConflicts && changed == 1
	for i, row := range rows {
		if row.Patch == "" {
			continue
		}
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
		patch := filepath.Join(row.dir, row.Patch)
		temp := filepath.Join(scratch, fmt.Sprint(i))
		if row.ResultBase != nil && row.Base != nil && (base.Head == nil || *row.Base != *base.Head) {
			return errors.New("result base differs from frozen source")
		}
		if o.dir != "" {
			src, e = (snapGit{ctx: ctx, dir: src}).get("rev-parse", "--show-toplevel")
			if e != nil {
				return e
			}
		}
		plan, planErr := planApply(ctx, store, src, patch, temp, row.turnPatch, *base, allowConflicts)
		if planErr != nil {
			failures = errors.Join(failures, fmt.Errorf("%s: %w", row.Path, planErr))
			continue
		}
		for _, name := range plan.conflicts {
			conflicts = append(conflicts, row.Path+"/"+name)
		}
		plan.since = row.since
		plan.incremental = row.mergeBase != ""
		plans = append(plans, plan)
	}
	if len(conflicts) > 0 {
		diagnosticf("Conflicting files: %s\n", strings.Join(conflicts, ", "))
		diagnosticf("To choose a different starting turn, use cx apply %s --since-turn N; to resolve on a fresh snapshot, use cx rebase %s TEXT.\n", id, id)
		if e = writeJSON(dir+".conflicts.json", conflicts); e != nil {
			return e
		}
	}
	if failures != nil {
		diagnosticf("%v\n", failures)
	}
	if failures != nil || len(conflicts) > 0 && !allowConflicts {
		return ctlExit(1)
	}
	record := func() error {
		if len(conflicts) > 0 {
			return nil
		}
		log := filepath.Join(project, "apply-log.jsonl")
		b, readErr := os.ReadFile(log)
		if readErr != nil && !os.IsNotExist(readErr) {
			return readErr
		}
		operation := rand.Text()
		for _, plan := range plans {
			var base any
			if plan.base != "" {
				base = plan.base
			}
			entry := fields{"job": id, "host": host, "run_id": runID, "base": base, "turn": result.Turn, "repo": plan.repo, "source": plan.src, "operation": operation, "target_repository": plan.index, "target_repository_key": plan.targetKey, "patch": plan.patch, "applied_at": timeshim.Or(contextClock(ctx)).Now().UTC().Format(time.RFC3339Nano)}
			entry["since_turn"] = plan.since
			entry["role"] = result.Role
			if result.Changeset != "" {
				entry["changeset"] = result.Changeset
				entry["modules"] = result.Modules
				entry["session"] = result.Session
				// apply stages patches and preserves HEAD.
				entry["target_head_at_apply"] = nil
				if plan.head != "" {
					entry["target_head_at_apply"] = plan.head
				}
				entry["result_tree"] = plan.resultTree
				entry["baseline"] = result.Baseline.Snapshot
				if result.Baseline.Snapshot == "" {
					entry["baseline"] = origin.Snapshot
				}
			}
			if plan.incremental {
				delete(entry, "patch")
				entry["result"] = filepath.Join(dir, "result.json")
			}
			row, encodeErr := json.Marshal(entry)
			if encodeErr != nil {
				return encodeErr
			}
			b = append(append(b, row...), '\n')
		}
		return atomicWrite(log, b)
	}
	if len(plans) > 0 {
		if e = installPlans(plans, record); e != nil {
			return e
		}
	}
	for _, plan := range plans {
		if plan.repo == result.ModuleRepository {
			for _, name := range hotspotFiles(plan.paths, result.Hotspots, result.FoldPaths) {
				fmt.Printf("Hotspot: %s/%s\n", plan.repo, name)
			}
		}
	}
	var failed bool
	for _, p := range plans {
		fmt.Println(p.repo + ":")
		b, e := p.git.input(nil, "--no-optional-locks", "status", "--short")
		if e != nil {
			diagnosticf("%v\n", e)
			failed = true
		} else if _, e = os.Stdout.Write(b); e != nil {
			return e
		}
	}
	if len(conflicts) > 0 {
		diagnosticf("Merge results written. Resolve the conflicts before committing.\n")
		return ctlExit(1)
	}
	if len(plans) > 0 {
		fmt.Println("Changes are ready for review and commit.")
	}
	if failed {
		return ctlExit(10)
	}
	return nil
}
func applyLogProject(id string) string {
	project, _, _ := strings.Cut(id, "/")
	return project
}

func applyGlobalHotspots(ctx context.Context, id string, origin submissionOrigin, result turnResult, m Manifest) (string, []string, error) {
	def, err := readProject(applyLogProject(id))
	if err != nil || def.ModulesFile == "" && len(def.ModulesCmd) == 0 {
		return "", nil, err
	}
	if result.Baseline.Snapshot != "" && result.Baseline.Snapshot != origin.Snapshot {
		m, err = loadSnapshot(result.Baseline.Snapshot)
		if err != nil {
			return "", nil, err
		}
	}
	root, err := m.sourceRoot(ctx, origin.Source)
	if err != nil {
		return "", nil, err
	}
	sources := make(map[string]string, len(m.Repositories))
	for _, member := range m.Repositories {
		sources[member.Path] = filepath.Join(root, member.Path)
	}
	repo, err := moduleRepository(ctx, m, sources, def)
	if err != nil {
		return "", nil, err
	}
	table, err := snapshotModuleTable(ctx, m, repo, def)
	if err != nil {
		return "", nil, err
	}
	return repo, table.Hotspots, nil
}

func outputProject(p string) (string, error) {
	dir, err := projectOutputPath(p)
	if err != nil {
		return "", err
	}
	return dir, os.MkdirAll(dir, 0700)
}

func projectOutputPath(p string) (string, error) {
	base, err := controllerCacheRoot()
	if err != nil {
		return "", err
	}
	return jobPath(filepath.Join(base, "out"), p)
}

func applyOrigin(dir string) (submissionOrigin, string, Manifest, error) {
	var origin submissionOrigin
	var m Manifest
	e := readJSON(filepath.Join(filepath.Dir(filepath.Dir(dir)), "origin.json"), &origin)
	store, err := snapStore()
	if err != nil {
		return origin, "", m, err
	}
	if e != nil {
		return origin, "", m, e
	}
	if origin.Snapshot == "" {
		return origin, "", m, errors.New("in-place jobs have no patch")
	}
	var result struct{ Baseline jobBaseline }
	if e = readJSON(filepath.Join(dir, "result.json"), &result); e != nil && !os.IsNotExist(e) {
		return origin, store, m, e
	}
	if result.Baseline.Snapshot != "" {
		origin.Snapshot = result.Baseline.Snapshot
		if result.Baseline.Source != "" {
			origin.Source = result.Baseline.Source
		}
	}
	m, _, e = snapManifest(store, string(origin.Snapshot))
	return origin, store, m, e
}

// preloadApplyHotspots fills global hotspots for jobs without declared modules.
func preloadApplyHotspots(ctx context.Context, id, dir string, result *turnResult) {
	if len(result.Modules) != 0 {
		return
	}
	origin, _, manifest, err := applyOrigin(dir)
	if err == nil {
		var repo string
		var hotspots []string
		if repo, hotspots, err = applyGlobalHotspots(ctx, id, origin, *result, manifest); err == nil {
			result.ModuleRepository, result.Hotspots = repo, hotspots
			return
		}
	}
	diagnosticf("cx: load global hotspots: %v\n", err)
}
