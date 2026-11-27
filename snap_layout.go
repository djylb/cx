package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type snapshotLayout struct {
	repos      map[string][]string
	workspace  *SnapshotWorkspace
	root, main string
	git        bool
}

func snapLayout(ctx context.Context, src, tmp string, configured []projectRepository) (snapshotLayout, error) {
	var layout snapshotLayout
	git := snapGit{ctx: ctx, dir: src}
	top, e := git.get("rev-parse", "--show-toplevel")
	if e != nil {
		var exit *exec.ExitError
		if !errors.As(e, &exit) || !strings.Contains(e.Error(), "not a git repository") {
			return layout, e
		}
		return snapshotLayout{repos: map[string][]string{src: nil}, root: filepath.Dir(src), main: src}, nil
	}
	top, e = filepath.EvalSymlinks(top)
	if e != nil {
		return layout, e
	}
	repos, e := snapProjectRepos(ctx, top, configured)
	if e != nil {
		return layout, e
	}
	main := src
	if len(repos) > 1 {
		main = top
	}
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
		if e != nil {
			return "", e
		}
		root, e = filepath.EvalSymlinks(root)
		if _, exists := repos[root]; e == nil && !exists {
			repos[root] = nil
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
			return layout, e
		}
		for _, u := range w.Use {
			d, e := resolve(filepath.Dir(work), u.DiskPath)
			if e != nil {
				return layout, e
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
					return layout, e
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
			return layout, e
		}
		for _, r := range m.Replace {
			k := key(r.Old)
			if r.New.Version != "" || overrides[k] || overrides[r.Old.Path] {
				continue
			}
			t, e := resolve(d, r.New.Path)
			if e != nil {
				return layout, e
			}
			if filepath.IsAbs(r.New.Path) {
				if work == "" {
					return layout, errors.New("absolute go.mod replace needs a go.work override")
				}
				edits = append(edits, edit{"replace", k, t})
			}
		}
	}
	root := filepath.Dir(top)
	for p := range repos {
		for !snapWithin(root, p) {
			parent := filepath.Dir(root)
			if root == parent {
				return layout, errors.New("no common workspace root")
			}
			root = parent
		}
	}
	layout = snapshotLayout{repos: repos, root: root, main: main, git: true}
	if work != "" {
		portable := filepath.Join(tmp, "go.work")
		if e := copyFile(work, portable); e != nil {
			return layout, e
		}
		args := []string{"work", "edit"}
		for _, x := range edits {
			rel, e := filepath.Rel(root, x.target)
			if e != nil {
				return layout, e
			}
			v := "./tree/" + filepath.ToSlash(rel)
			if x.flag == "use" {
				args = append(args, "-dropuse="+x.key, "-use="+v)
			} else {
				args = append(args, "-replace="+x.key+"="+v)
			}
		}
		if _, e := goRun(append(args, portable)...); e != nil {
			return layout, e
		}
		b, e := os.ReadFile(portable)
		if e != nil {
			return layout, e
		}
		layout.workspace = &SnapshotWorkspace{Text: string(b)}
		b, e = os.ReadFile(work + ".sum")
		if e == nil {
			s := string(b)
			layout.workspace.Sum = &s
		} else if !os.IsNotExist(e) {
			return layout, e
		}
	}
	return layout, nil
}

// Use the submitting worktree for its repository and the configured checkouts
// for the other project members. An unrelated source keeps its own layout.
func snapProjectRepos(ctx context.Context, top string, configured []projectRepository) (map[string][]string, error) {
	repos := map[string][]string{top: nil}
	if len(configured) == 0 {
		return repos, nil
	}
	path, e := projectMemberPath(ctx, top)
	if e != nil {
		return nil, e
	}
	repoID, e := projectRepoIdentity(ctx, top)
	if e != nil {
		return nil, e
	}
	// The submitting worktree stands in for its member: the member with the
	// same working tree, else the one member sharing its repository.
	type member struct {
		def      projectRepository
		root, id string
	}
	var members []member
	var invalid error
	for _, repo := range configured {
		root, rootErr := projectMemberPath(ctx, repo.Path)
		var id string
		if rootErr == nil {
			id, rootErr = projectRepoIdentity(ctx, repo.Path)
		}
		if rootErr != nil {
			invalid = errors.Join(invalid, fmt.Errorf("project repository %s: %w", repo.Path, rootErr))
			continue
		}
		members = append(members, member{repo, root, id})
	}
	self := -1
	for i, m := range members {
		if m.root == path {
			self = i
		}
	}
	if self < 0 {
		for i, m := range members {
			if m.id == repoID {
				if self >= 0 {
					return nil, fmt.Errorf("%s matches several project worktrees of one repository; submit from one of them", top)
				}
				self = i
			}
		}
	}
	if self < 0 {
		return map[string][]string{top: nil}, nil
	}
	for i, m := range members {
		if i == self {
			repos[top] = append(repos[top], m.def.Include...)
			continue
		}
		repos[m.root] = append(repos[m.root], m.def.Include...)
	}
	return repos, invalid
}
