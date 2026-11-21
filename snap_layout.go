package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type snapModule struct{ Path, Version string }
type snapGoFile struct {
	Use     []struct{ DiskPath string }
	Replace []struct{ Old, New snapModule }
}

func snapLayout(ctx context.Context, src, tmp string) ([]string, string, *SnapshotWorkspace, bool, error) {
	git := snapGit{ctx: ctx, dir: src}
	top, e := git.get("rev-parse", "--show-toplevel")
	if e != nil {
		var exit *exec.ExitError
		if !errors.As(e, &exit) || !strings.Contains(e.Error(), "not a git repository") {
			return nil, "", nil, false, e
		}
		return []string{src}, filepath.Dir(src), nil, false, nil
	}
	repos := map[string]bool{top: true}
	var workspace *SnapshotWorkspace
	goRun := func(args ...string) (string, error) {
		b, e := snapCommand(ctx, src, []string{"GOTOOLCHAIN=local"}, nil, "go", args...)
		return strings.TrimRight(string(b), "\r\n"), e
	}
	goFile := func(kind, path string) (snapGoFile, error) {
		var m snapGoFile
		s, e := goRun(kind, "edit", "-json", path)
		if e == nil {
			e = json.Unmarshal([]byte(s), &m)
		}
		return m, e
	}
	var mods []string
	overrides := map[string]bool{}
	type edit struct{ flag, key, target string }
	var edits []edit
	resolve := func(dir, raw string) (string, error) {
		if !filepath.IsAbs(raw) {
			raw = filepath.Join(dir, raw)
		}
		target, e := filepath.EvalSymlinks(raw)
		if e != nil {
			return "", e
		}
		g := snapGit{ctx: ctx, dir: target}
		root, e := g.get("rev-parse", "--show-toplevel")
		if e == nil {
			repos[root] = true
		}
		return target, e
	}
	// Ordinary source trees require only Git. Invoke Go only for a module/workspace.
	work := os.Getenv("GOWORK")
	mod := ""
	for d := src; ; d = filepath.Dir(d) {
		if mod == "" {
			if _, e := os.Stat(filepath.Join(d, "go.mod")); e == nil {
				mod = d
			}
		}
		if work == "" {
			if _, e := os.Stat(filepath.Join(d, "go.work")); e == nil {
				work = filepath.Join(d, "go.work")
			}
		}
		if d == filepath.Dir(d) {
			break
		}
	}
	if work == "off" {
		work = ""
	}
	key := func(m snapModule) string {
		if m.Version != "" {
			return m.Path + "@" + m.Version
		}
		return m.Path
	}
	if work != "" {
		w, e := goFile("work", work)
		if e != nil {
			return nil, "", nil, false, e
		}
		for _, u := range w.Use {
			d, e := resolve(filepath.Dir(work), u.DiskPath)
			if e != nil {
				return nil, "", nil, false, e
			}
			mods = append(mods, d)
			edits = append(edits, edit{"use", u.DiskPath, d})
		}
		for _, r := range w.Replace {
			k := key(r.Old)
			overrides[k] = true
			if r.New.Version == "" {
				d, e := resolve(filepath.Dir(work), r.New.Path)
				if e != nil {
					return nil, "", nil, false, e
				}
				edits = append(edits, edit{"replace", k, d})
			}
		}
	} else if mod != "" {
		mods = []string{mod}
	}
	sort.Strings(mods)
	for _, d := range mods {
		m, e := goFile("mod", filepath.Join(d, "go.mod"))
		if e != nil {
			return nil, "", nil, false, e
		}
		for _, r := range m.Replace {
			k := key(r.Old)
			if r.New.Version != "" || overrides[k] || overrides[r.Old.Path] {
				continue
			}
			t, e := resolve(d, r.New.Path)
			if e != nil {
				return nil, "", nil, false, e
			}
			if filepath.IsAbs(r.New.Path) {
				if work == "" {
					return nil, "", nil, false, errors.New("absolute go.mod replace needs a go.work override")
				}
				edits = append(edits, edit{"replace", k, t})
			}
		}
	}
	root := filepath.Dir(top)
	var paths []string
	for p := range repos {
		paths = append(paths, p)
		for !snapWithin(root, p) {
			parent := filepath.Dir(root)
			if root == parent {
				return nil, "", nil, false, errors.New("no common workspace root")
			}
			root = parent
		}
	}
	sort.Strings(paths)
	if work != "" {
		portable := filepath.Join(tmp, "go.work")
		if e := copyFile(work, portable); e != nil {
			return nil, "", nil, false, e
		}
		args := []string{"work", "edit"}
		for _, x := range edits {
			rel, e := filepath.Rel(root, x.target)
			if e != nil {
				return nil, "", nil, false, e
			}
			v := "./tree/" + filepath.ToSlash(rel)
			if x.flag == "use" {
				args = append(args, "-dropuse="+x.key, "-use="+v)
			} else {
				args = append(args, "-replace="+x.key+"="+v)
			}
		}
		if _, e := goRun(append(args, portable)...); e != nil {
			return nil, "", nil, false, e
		}
		b, e := os.ReadFile(portable)
		if e != nil {
			return nil, "", nil, false, e
		}
		workspace = &SnapshotWorkspace{Text: string(b)}
		b, e = os.ReadFile(work + ".sum")
		if e == nil {
			s := string(b)
			workspace.Sum = &s
		} else if !os.IsNotExist(e) {
			return nil, "", nil, false, e
		}
	}
	return paths, root, workspace, true, nil
}
