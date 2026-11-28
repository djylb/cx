package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

var patchDiff = []string{"diff", "--cached", "--binary", "--full-index", "--no-renames", "--no-ext-diff", "--no-textconv", "--no-relative", "--src-prefix=a/", "--dst-prefix=b/"}

// A sealed cumulative patch and the dispatch snapshot describe the entire
// turn tree, including an unchanged repository (which has no patch row).
func resultTree(g snapGit, snapshot, dir string, row turnPatch) (string, error) {
	if _, err := g.get("read-tree", snapshot); err != nil {
		return "", err
	}
	if row.Patch != "" {
		if !snapPath(row.Patch) || row.SnapshotOID != snapshot {
			return "", errors.New("result patch differs from recorded snapshot")
		}
		if _, err := g.get("apply", "--cached", "--whitespace=nowarn", filepath.Join(dir, row.Patch)); err != nil {
			return "", err
		}
	}
	return g.get("write-tree")
}

// selectedPatch describes a repository's range without modifying sealed results.
type selectedPatch struct {
	turnPatch
	dir   string
	since int64
}

func ctlSealedPatches(ctx context.Context, h endpoint, host, id, runID, dir, op string, o ctlOptions) error {
	var m turnResult
	if err := readJSON(filepath.Join(dir, "result.json"), &m); err != nil {
		return err
	}
	var project string
	if op == "apply" {
		var err error
		project, err = outputProject(applyLogProject(id))
		if err != nil {
			return err
		}
		lock, lockErr := lockUntil(ctx, filepath.Join(project, ".apply.lock"))
		if lockErr != nil {
			return lockErr
		}
		defer closeBestEffort(lock)
	}
	scratch, err := os.MkdirTemp("", "cx-incremental-")
	if err != nil {
		return err
	}
	defer removeTreeBestEffort(scratch)
	rows, err := selectPatches(ctx, h, host, id, runID, dir, scratch, o, m)
	if err != nil {
		return err
	}
	if op == "apply" {
		return applyTurn(ctx, dir, project, id, host, runID, o, m, rows)
	}
	if o.full {
		return writePatches(ctx, os.Stdout, rows, o)
	}
	var out bytes.Buffer
	if err = writePatches(ctx, &out, rows, o); err != nil {
		return err
	}
	return preview(out.Bytes(), o)
}

func selectPatches(ctx context.Context, h endpoint, host, id, runID, latest, scratch string, o ctlOptions, current turnResult) ([]selectedPatch, error) {
	first := max(1, current.Baseline.FromTurn)
	var since int64
	var applied map[string]appliedRepo
	switch {
	case o.sinceTurn != "":
		since, _ = strconv.ParseInt(o.sinceTurn, 10, 64)
		if since < 1 || since > current.Turn {
			return nil, fmt.Errorf("--since-turn must be between 1 and the latest completed turn (%d)", current.Turn)
		}
	case o.incremental:
		since = current.Turn
		if since < 1 {
			return nil, errors.New("--incremental requires a positive completed turn")
		}
	default:
		var readErr error
		applied, readErr = appliedToTargets(ctx, appliedJob{Host: host, Job: id, RunID: runID}, latest, o.dir)
		if readErr != nil {
			return nil, fmt.Errorf("apply log: %w", readErr)
		}
	}

	// The first turn on a refreshed baseline has no prior tree to compare.
	if since != 0 {
		since = max(first, since)
	}
	matches := func(path, repoID string) bool {
		return o.repo == "" || o.repo == path || o.repo == repoID
	}
	var rows []selectedPatch
	latestRows := make(map[string]turnPatch, len(current.Repositories))
	for _, row := range current.Repositories {
		latestRows[row.Path] = row
		if since <= first && applied[row.Path].Turn < first && matches(row.Path, row.RepoID) {
			rows = append(rows, selectedPatch{turnPatch: row, dir: latest, since: first})
		}
	}
	if since <= first && len(applied) == 0 {
		return rows, nil
	}

	_, store, manifest, err := applyOrigin(latest)
	if err != nil {
		return nil, err
	}
	type priorResult struct {
		dir  string
		rows map[string]turnPatch
	}
	previous := map[int64]priorResult{}
	for i, repo := range manifest.Repositories {
		if !matches(repo.Path, repo.RepoID) {
			continue
		}
		baseTurn := since - 1
		if since == 0 {
			baseTurn = applied[repo.Path].Turn
		}
		if baseTurn < first {
			continue
		}
		if baseTurn >= current.Turn {
			// All selected changes for this repository are already applied.
			rows = append(rows, selectedPatch{turnPatch: turnPatch{SnapshotPatch: SnapshotPatch{Path: repo.Path}}, since: baseTurn + 1})
			continue
		}
		prior, ok := previous[baseTurn]
		if !ok {
			turn := strconv.FormatInt(baseTurn, 10)
			prior.dir, err = resultDest(host, id, runID, turn)
			if err != nil {
				return nil, err
			}
			if err = fetchTurn(ctx, h, id, runID, turn, prior.dir, false); err != nil {
				if errors.Is(err, ctlExit(44)) {
					return nil, resultUnavailable(fmt.Sprintf("Sealed turn %d is unavailable for %s (%v). Use --since-turn 1 or cx rebase %s TEXT.", baseTurn, id, err, id))
				}
				return nil, err
			}
			var old turnResult
			if err = readJSON(filepath.Join(prior.dir, "result.json"), &old); err != nil {
				return nil, err
			}
			if old.Turn != baseTurn {
				return nil, errors.New("sealed turn number does not match requested range")
			}
			if old.Baseline.FromTurn != current.Baseline.FromTurn || old.Baseline.Snapshot != current.Baseline.Snapshot {
				return nil, errors.New("incremental range crosses a baseline refresh")
			}
			prior.rows = make(map[string]turnPatch, len(old.Repositories))
			for _, row := range old.Repositories {
				prior.rows[row.Path] = row
			}
			previous[baseTurn] = prior
		}
		row, deltaErr := incrementalPatch(ctx, store, prior.dir, latest, scratch, i, repo, prior.rows[repo.Path], latestRows[repo.Path])
		if deltaErr != nil {
			return nil, fmt.Errorf("compare turns %d and %d for %s: %w", baseTurn, current.Turn, repo.Path, deltaErr)
		}
		rows = append(rows, selectedPatch{turnPatch: row, dir: scratch, since: baseTurn + 1})
	}
	slices.SortFunc(rows, func(a, b selectedPatch) int { return strings.Compare(a.Path, b.Path) })
	return rows, nil
}

func appliedToTargets(ctx context.Context, job appliedJob, resultDir, target string) (map[string]appliedRepo, error) {
	var facts []appliedRepo
	err := readApplyLog(applyLogProject(job.Job), func(entry applyRecord) {
		if entry.appliedJob == job {
			facts = append(facts, entry.appliedRepo)
		}
	})
	if err != nil || len(facts) == 0 {
		return nil, err
	}
	var root string
	if target == "" {
		origin, _, manifest, err := applyOrigin(resultDir)
		if err != nil {
			return nil, err
		}
		root, err = manifest.sourceRoot(ctx, origin.Source)
		if err != nil {
			return nil, nil // A patch can be viewed away from its original checkout.
		}
	}
	type indexIdentity struct{ path, key string }
	indices := make(map[string]indexIdentity)
	applied := make(map[string]appliedRepo)
	for _, fact := range facts {
		path := target
		if path == "" {
			path = filepath.Join(root, fact.Repo)
		}
		index, found := indices[path]
		if !found {
			index.path, index.key, _ = applyTargetIndex(snapGit{ctx: ctx, dir: path})
			indices[path] = index
		}
		if index.key == "" {
			continue
		}
		if fact.TargetRepositoryKey == "" {
			if filepath.Clean(path) == filepath.Clean(fact.Source) {
				noteAppliedRepository(applied, fact)
			}
		} else if index.key == fact.TargetRepositoryKey && filepath.Clean(index.path) == filepath.Clean(fact.TargetRepository) {
			noteAppliedRepository(applied, fact)
		}
	}
	return applied, nil
}

func incrementalPatch(ctx context.Context, store, previous, latest, output string, i int, repo SnapshotRepo, prior, next turnPatch) (turnPatch, error) {
	empty := turnPatch{SnapshotPatch: SnapshotPatch{Path: repo.Path}}
	if prior.Patch == "" && next.Patch == "" {
		return empty, nil
	}
	name := fmt.Sprintf("%d.patch", i)
	objects := filepath.Join(output, name+".objects")
	if err := os.MkdirAll(objects, 0700); err != nil {
		return empty, err
	}
	g := snapGit{ctx: ctx, dir: repo.bare(store), env: []string{
		"GIT_OBJECT_DIRECTORY=" + objects, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(repo.bare(store), "objects"),
		"GIT_INDEX_FILE=" + filepath.Join(output, name+".index"), "GIT_OPTIONAL_LOCKS=0",
	}}
	base, err := resultTree(g, repo.OID, previous, prior)
	if err != nil {
		return empty, err
	}
	end, err := resultTree(g, repo.OID, latest, next)
	if err != nil {
		return empty, err
	}
	if base == end {
		return empty, nil
	}
	data, err := g.input(nil, append(append([]string{}, patchDiff...), base)...)
	if err != nil {
		return empty, err
	}
	names, err := g.input(nil, "diff-tree", "--no-commit-id", "-r", "--no-renames", "--name-only", "-z", base, end)
	if err != nil {
		return empty, err
	}
	if err = os.WriteFile(filepath.Join(output, name), data, 0600); err != nil {
		return empty, err
	}
	row := next
	if row.Patch == "" {
		row = prior
	}
	row.mergeBase, row.Patch = base, name
	row.Bytes, row.Files = len(data), bytes.Count(names, []byte{0})
	return row, nil
}

func writePatches(ctx context.Context, out io.Writer, rows []selectedPatch, o ctlOptions) error {
	if o.repo != "" && len(rows) != 1 {
		return errors.New("select a unique changed repository")
	}
	for _, row := range rows {
		if _, e := fmt.Fprintf(out, "# repo: %s\n# since turn %d\n", row.Path, row.since); e != nil {
			return e
		}
		if row.Patch == "" {
			continue
		}
		p := filepath.Join(row.dir, row.Patch)
		if o.full {
			b, e := readSharedFile(p)
			if e != nil {
				return e
			}
			if _, e = out.Write(b); e != nil {
				return e
			}
		} else {
			cmd := exec.CommandContext(ctx, "git", "apply", "--stat", p)
			cmd.Stdout = out
			cmd.Stderr = os.Stderr
			if e := cmd.Run(); e != nil {
				return e
			}
		}
	}
	return nil
}
