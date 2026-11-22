package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func incrementalResult(ctx context.Context, previous, latest, output string, baseTurn int64, current turnResult) (turnResult, error) {
	var old turnResult
	if err := readJSON(filepath.Join(previous, "result.json"), &old); err != nil {
		return current, err
	}
	if old.Turn != baseTurn || current.Turn <= old.Turn {
		return current, errors.New("sealed turn numbers do not match requested range")
	}
	_, store, manifest, err := applyOrigin(latest)
	if err != nil {
		return current, err
	}
	before, after := map[string]turnPatch{}, map[string]turnPatch{}
	for _, row := range old.Repositories {
		before[row.Path] = row
	}
	for _, row := range current.Repositories {
		after[row.Path] = row
	}
	current.Repositories = nil
	for i, repo := range manifest.Repositories {
		prior, next := before[repo.Path], after[repo.Path]
		if prior.Patch == "" && next.Patch == "" {
			continue
		}
		name := fmt.Sprintf("%d.patch", i)
		objects := filepath.Join(output, name+".objects")
		if err = os.MkdirAll(objects, 0700); err != nil {
			return current, err
		}
		g := snapGit{ctx: ctx, dir: repo.bare(store), env: []string{
			"GIT_OBJECT_DIRECTORY=" + objects, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(repo.bare(store), "objects"),
			"GIT_INDEX_FILE=" + filepath.Join(output, name+".index"), "GIT_OPTIONAL_LOCKS=0",
		}}
		base, err := resultTree(g, repo.OID, previous, prior)
		if err != nil {
			return current, fmt.Errorf("reconstruct sealed turn %d for %s: %w", old.Turn, repo.Path, err)
		}
		end, err := resultTree(g, repo.OID, latest, next)
		if err != nil {
			return current, fmt.Errorf("reconstruct sealed turn %d for %s: %w", current.Turn, repo.Path, err)
		}
		if base == end {
			continue
		}
		data, err := g.input(nil, append(append([]string{}, patchDiff...), base)...)
		if err != nil {
			return current, err
		}
		names, err := g.input(nil, "diff-tree", "--no-commit-id", "-r", "--no-renames", "--name-only", "-z", base, end)
		if err != nil {
			return current, err
		}
		if err = os.WriteFile(filepath.Join(output, name), data, 0600); err != nil {
			return current, err
		}
		row := next
		if row.Patch == "" {
			row = prior
		}
		row.mergeBase, row.Patch = base, name
		row.Bytes, row.Files = len(data), bytes.Count(names, []byte{0})
		current.Repositories = append(current.Repositories, row)
	}
	return current, writeJSON(filepath.Join(output, "result.json"), current)
}
