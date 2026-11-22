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

	"github.com/BurntSushi/toml"
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
			dir, e := projectDirectory()
			if e != nil {
				return e
			}
			var def struct{ Repositories []struct{ Path string } }
			if _, e = toml.DecodeFile(filepath.Join(dir, o.project+".toml"), &def); e != nil {
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
	for range strings.Split(m.Main, "/") {
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
func rebaseTurn(ctx context.Context, dir, host, id, text string, m turnResult) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("rebase requires follow-up text")
	}
	origin, _, _, e := applyOrigin(dir, host, id)
	if e != nil {
		return e
	}
	if origin.Command || origin.Snapshot == "" {
		return errors.New("rebase requires a snapshot agent job")
	}
	p, j, ok := strings.Cut(id, "/")
	if !ok {
		return errors.New("rebase requires a project job")
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	lock, e := lockUntil(ctx, filepath.Join(filepath.Dir(dir), ".rebase.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	var child string
	for n := 1; ; n++ {
		suffix := fmt.Sprintf("-r%d", n)
		child = p + "/" + j[:min(len(j), 96-len(suffix))] + suffix
		queued, queueErr := submissionDir(host, child)
		if queueErr != nil {
			return queueErr
		}
		if _, queueErr = os.Stat(filepath.Join(queued, "input.json")); queueErr == nil {
			continue
		} else if !os.IsNotExist(queueErr) {
			return queueErr
		}
		r, queueErr := h.call(ctx, 15*time.Second, "status", "--job", child)
		if queueErr != nil {
			return queueErr
		}
		if r.code == 11 {
			break
		}
		if r.code != 0 {
			return r.output()
		}
	}
	prompt, e := os.CreateTemp("", "cx-rebase-")
	if e != nil {
		return e
	}
	defer removeBestEffort(prompt.Name())
	var conflicts []string
	if e = readJSON(dir+".conflicts.json", &conflicts); e != nil && !os.IsNotExist(e) {
		_ = prompt.Close()
		return e
	}
	_, e = fmt.Fprintln(prompt, text+"\n")
	if e == nil {
		e = json.NewEncoder(prompt).Encode(fields{"result_manifest": filepath.Join(dir, "result.json"), "conflicts": conflicts})
	}
	if e = errors.Join(e, prompt.Close()); e != nil {
		return e
	}
	options := slices.DeleteFunc(slices.Clone(origin.JobOptions), func(v string) bool { return v == "--job-tools" })
	opts, e := parseCtl("run", append([]string{host, origin.Source, child, prompt.Name()}, options...))
	if e != nil {
		return e
	}
	opts.noNetwork = !slices.Contains(origin.JobOptions, "--network")
	opts.noTools = !slices.Contains(origin.JobOptions, "--job-tools")
	opts.history = origin.History
	opts.cleanBase = origin.CleanBase
	opts.role = origin.Role
	opts.parent = origin.Parent
	if e = ctlSubmit(ctx, hs, "run", opts); e != nil {
		return e
	}
	queued, e := submissionDir(host, child)
	if e != nil {
		return e
	}
	var q queuedJob
	if e = readJSON(filepath.Join(queued, "input.json"), &q); e != nil {
		return e
	}
	path := filepath.Join(filepath.Dir(filepath.Dir(dir)), "rebases.json")
	var rows []fields
	if e = readJSON(path, &rows); e != nil && !os.IsNotExist(e) {
		return e
	}
	return writeJSON(path, append(rows, fields{"job": child, "turn": fmt.Sprintf("turn-%d", m.Turn), "snapshot": q.Request.Snapshot}))
}
