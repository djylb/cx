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
func submitRebase(ctx context.Context, host, dest, id string, req rebaseRequest) error {
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
		done, err := rebaseSubmitted(host, req.Child)
		if err != nil || done {
			return err
		}
		return queueJob(ctx, queued, true)
	}
	if !os.IsNotExist(err) {
		return err
	}
	origin, _, _, err := applyOrigin(dest)
	if err != nil {
		return err
	}
	attachments, err := rebaseAttachments(dest)
	if err != nil {
		return err
	}
	prompt, err := os.CreateTemp("", "cx-rebase-")
	if err != nil {
		return err
	}
	defer removeBestEffort(prompt.Name())
	_, err = fmt.Fprintf(prompt, "%s\n\nRead the previous turn's result manifest at $CX_CONTEXT/rebase/result.json. The report is last.md, and patch and artifact paths are relative to the manifest directory. See conflicts.json there if present.\n", req.Text)
	if err = errors.Join(err, prompt.Close()); err != nil {
		return err
	}
	options := slices.DeleteFunc(slices.Clone(origin.JobOptions), func(v string) bool { return v == "--job-tools" })
	opts, err := parseCtl("run", append([]string{host, origin.Source, req.Child, prompt.Name()}, options...))
	if err != nil {
		return err
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
	return ctlSubmitWithRebase(ctx, hs, "run", opts, rebaseInput{key: key, files: attachments})
}

func rebaseAttachments(dir string) (map[string][]byte, error) {
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
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		attachments["rebase/"+name] = data
	}
	data, err := os.ReadFile(dir + ".conflicts.json")
	if err == nil {
		attachments["rebase/conflicts.json"] = data
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return attachments, nil
}
