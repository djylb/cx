package main

import (
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

func mirror(ctx context.Context, hs endpoints, o ctlOptions) error {
	src := "."
	if len(o.pos) > 1 || len(o.pos) > 0 && o.project != "" {
		return errors.New("mirror [DIR | --project P]")
	}
	if len(o.pos) == 1 {
		src = o.pos[0]
	}
	if o.project != "" {
		if _, e := jobDir("", o.project+"/mirror"); e != nil {
			return e
		}
		p, e := projectAt(ctx, src)
		if e != nil {
			return e
		}
		if p != o.project {
			def, e := readProject(o.project)
			if e != nil {
				return e
			}
			if len(def.Repositories) != 1 {
				return errors.New("ambiguous project root; specify DIR")
			}
			src = def.Repositories[0].Path
		}
	}
	src, e := filepath.Abs(src)
	if e != nil {
		return e
	}
	src, e = filepath.EvalSymlinks(src)
	if e != nil {
		return e
	}
	id, m, e := Freeze(ctx, src, FreezeOptions{History: "HEAD"})
	if e != nil {
		return e
	}
	h, e := hs.get(defaultSubmitHost())
	if e != nil {
		return e
	}
	if e = Publish(ctx, h, id, nil); e != nil {
		return e
	}
	root := src
	for range strings.SplitSeq(m.Main, "/") {
		root = filepath.Dir(root)
	}
	refs := map[string]string{}
	for _, r := range m.Repositories {
		if !r.History || r.Head == nil {
			continue
		}
		repo := filepath.Join(root, r.Path)
		branch := probe(ctx, "git", "-C", repo, "symbolic-ref", "-q", "HEAD")
		if strings.HasPrefix(branch, "refs/heads/") && probe(ctx, "git", "-C", repo, "rev-parse", branch) == *r.Head {
			refs[r.Path] = branch
		}
	}
	if e = publishMirrorRefs(ctx, h, id, refs); e != nil {
		return e
	}
	fmt.Println(id)
	return nil
}
func publishMirrorRefs(ctx context.Context, h endpoint, id SnapshotID, refs map[string]string) error {
	b, e := json.Marshal(refs)
	if e != nil {
		return e
	}
	r, e := h.invoke(ctx, 30*time.Second, false, "mirror-refs", string(id), string(b))
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	return nil
}
func mirrorRefs(ctx context.Context, sid, data string) error {
	m, e := loadSnapshot(SnapshotID(sid))
	if e != nil {
		return e
	}
	store, e := snapStore()
	if e != nil {
		return e
	}
	var refs map[string]string
	if e = json.Unmarshal([]byte(data), &refs); e != nil {
		return e
	}
	for _, r := range m.Repositories {
		ref := refs[r.Path]
		if ref == "" {
			continue
		}
		if !r.History || r.Head == nil || !strings.HasPrefix(ref, "refs/heads/") {
			return errors.New("invalid mirror ref")
		}
		g := snapGit{ctx: ctx, dir: r.bare(store)}
		if _, e = g.get("check-ref-format", ref); e != nil {
			return e
		}
		if _, e = g.get("update-ref", ref, *r.Head); e != nil {
			return e
		}
	}
	return nil
}
func submitRebase(ctx context.Context, host, dest, result, id string, req rebaseRequest) error {
	dir, err := submissionDir(host, req.Child)
	if err != nil {
		return err
	}
	generation := filepath.Base(filepath.Dir(filepath.Dir(dest)))
	key := id + "/" + generation + "/" + filepath.Base(dest)
	var queued queuedJob
	err = readJSON(filepath.Join(dir, "input.json"), &queued)
	if err == nil {
		if queued.Rebase != key {
			return errors.New("rebase child name is bound to another submission")
		}
		done, submitErr := rebaseSubmitted(host, req.Child)
		if submitErr != nil || done {
			return submitErr
		}
		return queueJob(ctx, queued, true, true)
	}
	if !os.IsNotExist(err) {
		return err
	}
	origin, _, _, err := applyOrigin(dest)
	if err != nil {
		return err
	}
	attachments, err := rebaseAttachments(result, dest)
	if err != nil {
		return err
	}
	prompt, err := os.CreateTemp("", "cx-rebase-")
	if err != nil {
		return err
	}
	defer removeBestEffort(prompt.Name())
	note := ""
	if req.Host != "" {
		note = "\n\nThe checkout already includes the previous job's cumulative changes on top of the current source. Continue from those changes."
	}
	_, err = fmt.Fprintf(prompt, "%s%s\n\nRead the previous turn's result manifest at $CX_CONTEXT/rebase/result.json. The report is last.md, and patch and artifact paths are relative to the manifest directory. See conflicts.json there if present.\n", req.Text, note)
	if err = errors.Join(err, prompt.Close()); err != nil {
		return err
	}
	options := slices.DeleteFunc(slices.Clone(origin.JobOptions), func(v string) bool { return v == "--job-tools" })
	opts, err := parseCtl("run", append([]string{host, origin.Source, req.Child, prompt.Name()}, options...))
	if err != nil {
		return err
	}
	input := rebaseInput{key: key, files: attachments}
	if req.Host != "" {
		opts.profile = req.Profile
		input.carry = result
	}
	opts.noNetwork = !slices.Contains(origin.JobOptions, "--network")
	opts.noTools = !slices.Contains(origin.JobOptions, "--job-tools")
	opts.history = origin.History
	opts.cleanBase = origin.CleanBase
	opts.role = origin.Role
	opts.parent = origin.Parent
	opts.session = req.Session
	opts.explicitSession = req.Session != ""
	opts.noSession = req.Session == ""
	opts.sync = true
	hs, err := loadEndpoints()
	if err != nil {
		return err
	}
	return ctlSubmitWithRebase(ctx, hs, "run", opts, input)
}

func rebaseAttachments(dir, dest string) (map[string][]byte, error) {
	var result turnResult
	manifest, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(manifest, &result); err != nil {
		return nil, err
	}
	files := []string{"last.md"}
	for _, repo := range result.Repositories {
		files = append(files, repo.Patch)
	}
	for _, artifact := range result.Artifacts {
		files = append(files, artifact.Path)
	}
	attachments := map[string][]byte{"rebase/result.json": manifest}
	for _, name := range files {
		if !snapPath(name) {
			return nil, fmt.Errorf("invalid rebase result path %q", name)
		}
		data, readErr := os.ReadFile(filepath.Join(dir, name))
		if readErr != nil {
			return nil, readErr
		}
		attachments["rebase/"+name] = data
	}
	data, err := os.ReadFile(dest + ".conflicts.json")
	if err == nil {
		attachments["rebase/conflicts.json"] = data
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return attachments, nil
}

// Merge the sealed result into the freshly frozen trees. The new manifest
// retains the current source identity, history and workspace layout.
func carryRebaseChanges(ctx context.Context, fresh SnapshotID, result string) (SnapshotID, error) {
	_, store, original, err := applyOrigin(result)
	if err != nil {
		return "", err
	}
	var sealed turnResult
	if err = readJSON(filepath.Join(result, "result.json"), &sealed); err != nil {
		return "", err
	}
	if len(sealed.Repositories) == 0 {
		return fresh, nil
	}
	current, err := loadSnapshot(fresh)
	if err != nil {
		return "", err
	}
	scratch, err := os.MkdirTemp("", "cx-rebase-index-")
	if err != nil {
		return "", err
	}
	defer removeTreeBestEffort(scratch)
	seen := make(map[string]bool)
	for i, row := range sealed.Repositories {
		oldIndex := slices.IndexFunc(original.Repositories, func(r SnapshotRepo) bool { return r.Path == row.Path })
		newIndex := slices.IndexFunc(current.Repositories, func(r SnapshotRepo) bool { return r.Path == row.Path })
		if seen[row.Path] || oldIndex < 0 || newIndex < 0 || row.Patch == "" {
			return "", fmt.Errorf("unexpected result repository %q", row.Path)
		}
		seen[row.Path] = true
		before, now := original.Repositories[oldIndex], &current.Repositories[newIndex]
		if before.RepoID != row.RepoID || now.RepoID != before.RepoID ||
			row.ResultBase != nil && row.Base != nil && (before.Head == nil || *row.Base != *before.Head) {
			return "", fmt.Errorf("result repository %q differs from the recorded source", row.Path)
		}
		if err = mergeRebaseRepo(ctx, store, filepath.Join(scratch, fmt.Sprint(i)), result, row, before, now); err != nil {
			failure := fmt.Errorf("carry changes for %s: %w", row.Path, err)
			if errors.Is(err, ctlExit(1)) {
				diagnosticf("cx rebase: %v\n", failure)
			}
			return "", failure
		}
	}
	data, err := snapJSON(current)
	if err != nil {
		return "", err
	}
	id := snapSum(data)
	return SnapshotID(id), publishImmutable(filepath.Join(store, "manifests", id+".json"), data, filepath.Dir(store))
}

func mergeRebaseRepo(ctx context.Context, store, index, result string, row turnPatch, before SnapshotRepo, now *SnapshotRepo) error {
	g := snapGit{ctx: ctx, dir: now.bare(store), env: []string{"GIT_INDEX_FILE=" + index}}
	if old := before.bare(store); old != g.dir {
		g.env = append(g.env, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+filepath.Join(old, "objects"))
	}
	lock, err := lockUntil(ctx, g.dir+".lock")
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	theirs, err := resultTree(g, before.OID, result, row)
	if err != nil {
		return err
	}
	ancestor, err := g.get("rev-parse", before.OID+"^{tree}")
	if err != nil {
		return err
	}
	ours, err := g.get("rev-parse", now.OID+"^{tree}")
	if err != nil {
		return err
	}
	if before.Head != nil {
		// Apply treats paths that still match the original HEAD as clean.
		// The original snapshot base has that same tree even without history.
		paths, pathsErr := treeChanges(g, ancestor, theirs)
		if pathsErr != nil {
			return pathsErr
		}
		changed, changesErr := treeChanges(g, before.Base, ours)
		if changesErr != nil {
			return changesErr
		}
		var clean []string
		for path := range paths {
			if !changed[path] {
				clean = append(clean, path)
			}
		}
		slices.Sort(clean)
		if _, err = g.get("read-tree", ancestor); err != nil {
			return err
		}
		if err = overlayIndex(g, before.Base, clean); err != nil {
			return err
		}
		if ancestor, err = g.get("write-tree"); err != nil {
			return err
		}
	}
	merged, stages, err := mergeApplyTrees(g, ancestor, ours, theirs)
	if err != nil {
		return err
	}
	if len(stages) > 0 {
		var paths []string
		for _, stage := range stages {
			_, name, _ := strings.Cut(stage, "\t")
			if !slices.Contains(paths, name) {
				paths = append(paths, name)
			}
		}
		return fmt.Errorf("conflicting files: %s: %w", strings.Join(paths, ", "), ctlExit(1))
	}
	if merged == ours {
		return nil
	}
	if err = validateRebaseTree(g, now.Base, ours, merged); err != nil {
		return err
	}
	// Use the frozen source as the history boundary. A different shallow
	// store may supply the merge ancestor, but the output has only this parent.
	g.env = append(g.env, "GIT_AUTHOR_NAME=snapshot", "GIT_COMMITTER_NAME=snapshot",
		"GIT_AUTHOR_EMAIL=snapshot@invalid", "GIT_COMMITTER_EMAIL=snapshot@invalid",
		"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2000-01-01T00:00:00Z")
	args := []string{"commit-tree", merged, "-m", "Source snapshot v1"}
	if now.Base != "" {
		args = append(args, "-p", now.Base)
	}
	// Persist any merged blobs borrowed from the old store before publishing.
	// Rebuilding the tree with commit-tree alone can leave alternate-only blobs.
	if before.bare(store) != g.dir {
		oldStore := filepath.Join(before.bare(store), "objects")
		if err = persistRebaseTree(g, merged, oldStore); err != nil {
			return err
		}
	}
	now.OID, err = g.get(args...)
	if err != nil {
		return err
	}
	if _, err = g.get("update-ref", "refs/snapshots/"+now.OID, now.OID); err != nil {
		return err
	}
	return syncSnapshotRepo(g.dir)
}

func persistRebaseTree(g snapGit, tree, oldObjects string) error {
	pack, err := g.input([]byte(tree+"\n"), "pack-objects", "--stdout", "--revs")
	if err != nil {
		return err
	}
	g.env = slices.DeleteFunc(slices.Clone(g.env), func(v string) bool {
		return v == "GIT_ALTERNATE_OBJECT_DIRECTORIES="+oldObjects
	})
	_, err = g.input(pack, "unpack-objects", "-q")
	return err
}

// The current tree has already passed Freeze. Check changed untracked files
// and resolve links in the combined tree, where their targets may have changed.
func validateRebaseTree(g snapGit, head, ours, merged string) error {
	changed, err := treeChanges(g, ours, merged)
	if err != nil {
		return err
	}
	entries, err := g.input(nil, "ls-tree", "-rz", merged)
	if err != nil {
		return err
	}
	var tracked map[string]bool
	if head != "" {
		names, namesErr := g.input(nil, "ls-tree", "-rz", "--name-only", head)
		if namesErr != nil {
			return namesErr
		}
		tracked = make(map[string]bool)
		for _, name := range snapList(string(names)) {
			tracked[name] = true
		}
	}
	links := make(map[string]string)
	for _, entry := range snapList(string(entries)) {
		meta, name, _ := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if len(fields) != 3 || fields[1] != "blob" || changed[name] && !snapIncludePath(name) {
			return fmt.Errorf("unsupported snapshot entry: %s", name)
		}
		link := fields[0] == "120000"
		untracked := changed[name] && !tracked[name]
		if link || untracked {
			raw, readErr := g.input(nil, "cat-file", "blob", fields[2])
			if readErr != nil {
				return readErr
			}
			if untracked {
				if err = validateUntrackedSnapshotFile(name, raw); err != nil {
					return err
				}
			}
			if link {
				links[name] = string(raw)
			}
		}
	}
	root := g.dir // Resolve virtual paths with the same root boundary as Freeze.
	readLink := func(path string) (string, bool, error) {
		rel, relErr := filepath.Rel(root, path)
		target, ok := links[filepath.ToSlash(rel)]
		return target, ok, relErr
	}
	for name, target := range links {
		if !snapshotLinkInside(root, filepath.Dir(filepath.Join(root, name)), target, readLink) {
			return fmt.Errorf("nonportable symlink: %s", name)
		}
	}
	return nil
}
