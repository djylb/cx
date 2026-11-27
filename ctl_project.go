package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
)

type projectRepository struct {
	Path       string   `toml:"path"`
	Branch     string   `toml:"integration_branch"`
	Visibility string   `toml:"visibility"`
	Include    []string `toml:"include,omitempty"`
	Publish    bool     `toml:"publish"`
}

type projectDefinition struct {
	Host         string              `toml:"default_host"`
	Repositories []projectRepository `toml:"repositories"`
}

func projectDirectory() (string, error) {
	config := os.Getenv("XDG_CONFIG_HOME")
	if config == "" {
		home, e := os.UserHomeDir()
		if e != nil {
			return "", e
		}
		config = filepath.Join(home, ".config")
	}
	return filepath.Join(config, "cx", "projects"), nil
}

func projectRepoIdentity(ctx context.Context, dir string) (string, error) {
	g := snapGit{ctx: ctx, dir: dir}
	identity, e := g.get("rev-parse", "--path-format=absolute", "--git-common-dir")
	if e != nil {
		return "", e
	}
	return filepath.EvalSymlinks(identity)
}

// projectMemberPath identifies a project member by its working tree, so other
// worktrees of the same repository are not taken for that member.
func projectMemberPath(ctx context.Context, dir string) (string, error) {
	top, e := (snapGit{ctx: ctx, dir: dir}).get("rev-parse", "--show-toplevel")
	if e != nil {
		return "", e
	}
	return filepath.EvalSymlinks(top)
}

func readProject(name string) (projectDefinition, error) {
	var def projectDefinition
	if _, e := jobDir("", name+"/project"); e != nil {
		return def, e
	}
	dir, e := projectDirectory()
	if e != nil {
		return def, e
	}
	_, e = toml.DecodeFile(filepath.Join(dir, name+".toml"), &def)
	return def, e
}

func resolveProject(ctx context.Context, dir, name string) (string, projectDefinition, error) {
	if name == "" {
		name = os.Getenv("CX_PROJECT")
	}
	if name != "" {
		def, e := readProject(name)
		if os.IsNotExist(e) {
			e = nil
		}
		return name, def, e
	}
	var matched string
	var def projectDefinition
	if identity, e := projectMemberPath(ctx, dir); e == nil {
		directory, e := projectDirectory()
		if e != nil {
			return "", def, e
		}
		files, e := filepath.Glob(filepath.Join(directory, "*.toml"))
		if e != nil {
			return "", def, e
		}
		for _, path := range files {
			var candidate projectDefinition
			if _, e = toml.DecodeFile(path, &candidate); e != nil {
				return "", def, e
			}
			for _, repo := range candidate.Repositories {
				repoID, repoErr := projectMemberPath(ctx, repo.Path)
				if repoErr != nil || repoID != identity {
					continue
				}
				if matched != "" {
					return "", def, errors.New("repository belongs to multiple projects; use --project or set CX_PROJECT")
				}
				matched, def = strings.TrimSuffix(filepath.Base(path), ".toml"), candidate
				break
			}
		}
	}
	if matched != "" {
		return matched, def, nil
	}
	p := probe(ctx, "git", "-C", dir, "rev-parse", "--show-toplevel")
	if p == "" {
		p, _ = filepath.Abs(dir)
	}
	p = regexp.MustCompile("[^A-Za-z0-9_.-]").ReplaceAllString(filepath.Base(p), "-")
	p = strings.TrimLeft(p, "._-")
	if p == "" {
		return "project", def, nil
	}
	return p[:min(48, len(p))], def, nil
}

func ctlProject(ctx context.Context, a []string) error {
	if len(a) == 0 {
		return ctlUsage("project", errors.New("project requires an action"))
	}
	op := "project-" + a[0]
	if _, ok := ctlCommands[op]; !ok {
		return ctlUsage("project", fmt.Errorf("unknown project action %q", a[0]))
	}
	if len(a) > 1 && isHelp(a[1]) {
		return ctlUsage(op, flag.ErrHelp)
	}
	if a[0] == "ls" {
		if len(a) != 1 {
			return ctlUsage(op, errors.New("unexpected project arguments"))
		}
		dir, e := projectDirectory()
		if e != nil {
			return e
		}
		files, e := filepath.Glob(filepath.Join(dir, "*.toml"))
		if e != nil {
			return e
		}
		for _, p := range files {
			fmt.Println(strings.TrimSuffix(filepath.Base(p), ".toml"))
		}
		return nil
	}
	if len(a) < 2 {
		return ctlUsage(op, errors.New("project name required"))
	}
	if strings.HasPrefix(a[1], "-") {
		return ctlUsage(op, errors.New("unexpected project arguments"))
	}
	if a[0] == "show" && len(a) == 3 && isHelp(a[2]) {
		return ctlUsage(op, flag.ErrHelp)
	}
	name := a[1]
	if _, e := jobDir("", name+"/project"); e != nil {
		return e
	}
	if a[0] == "show" {
		if len(a) != 2 {
			return ctlUsage(op, errors.New("unexpected project arguments"))
		}
		dir, e := projectDirectory()
		if e != nil {
			return e
		}
		path := filepath.Join(dir, name+".toml")
		var d map[string]any
		if _, e := toml.DecodeFile(path, &d); e != nil {
			return e
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(d)
	}
	host := defaultSubmitHost()
	var repos, includes []string
	for i := 2; i < len(a); {
		switch a[i] {
		case "-h", "--help":
			return ctlUsage(op, flag.ErrHelp)
		case "--repo":
			start := i + 1
			i = start
			for i < len(a) && !strings.HasPrefix(a[i], "-") {
				repos = append(repos, a[i])
				i++
			}
			if i == start {
				return ctlUsage(op, errors.New("--repo requires DIR"))
			}
		case "--default-host":
			if i+1 >= len(a) {
				return ctlUsage(op, errors.New("missing default host"))
			}
			host = a[i+1]
			i += 2
		case "--include":
			if i+1 >= len(a) {
				return ctlUsage(op, errors.New("--include requires REPO:PATH"))
			}
			includes = append(includes, a[i+1])
			i += 2
		default:
			return ctlUsage(op, fmt.Errorf("unexpected option %q", a[i]))
		}
	}
	if len(repos) == 0 {
		return ctlUsage(op, errors.New("project init requires --repo"))
	}
	dir, e := projectDirectory()
	if e != nil {
		return e
	}
	path := filepath.Join(dir, name+".toml")
	if _, validateErr := jobPath("", host); validateErr != nil {
		return validateErr
	}
	def := projectDefinition{Host: host}
	seen := map[string]bool{}
	for _, repo := range repos {
		list := probe(ctx, "git", "-C", repo, "worktree", "list", "--porcelain", "-z")
		row, _, _ := strings.Cut(list, "\x00")
		primary, ok := strings.CutPrefix(row, "worktree ")
		if !ok {
			return fmt.Errorf("invalid Git checkout: %s", repo)
		}
		primary, e = filepath.EvalSymlinks(primary)
		if e != nil {
			return e
		}
		if seen[primary] {
			continue
		}
		seen[primary] = true
		branch := probe(ctx, "git", "-C", primary, "symbolic-ref", "--short", "-q", "HEAD")
		if branch == "" {
			return fmt.Errorf("primary checkout needs an integration branch: %s", primary)
		}
		def.Repositories = append(def.Repositories, projectRepository{Path: primary, Branch: branch, Visibility: "private"})
	}
	for _, include := range includes {
		if e = addProjectInclude(ctx, def.Repositories, include); e != nil {
			return ctlUsage(op, e)
		}
	}
	var data bytes.Buffer
	if e = toml.NewEncoder(&data).Encode(def); e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}

	lock, e := lockUntil(ctx, filepath.Join(dir, ".init.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if _, e = os.Lstat(path); e == nil {
		return fmt.Errorf("project already exists: %s", name)
	} else if !os.IsNotExist(e) {
		return e
	}
	if e = atomicWrite(path, data.Bytes()); e != nil {
		return e
	}
	fmt.Println(name)
	return nil
}

func addProjectInclude(ctx context.Context, repos []projectRepository, spec string) error {
	// Look after the volume name so an absolute Windows path can name REPO.
	volume := filepath.VolumeName(spec)
	repo, pattern, ok := strings.Cut(spec[len(volume):], ":")
	if !ok || repo == "" || !snapIncludePattern(pattern) {
		return fmt.Errorf("invalid --include %q; expected REPO:PATH with a repository-relative path or glob", spec)
	}
	repo = volume + repo
	identity, _ := projectMemberPath(ctx, repo)
	match := -1
	for i, r := range repos {
		repoID, e := projectMemberPath(ctx, r.Path)
		if e != nil {
			return e
		}
		if identity == repoID || repo == filepath.Base(r.Path) {
			if match >= 0 {
				return fmt.Errorf("ambiguous --include repository %q; use its path", repo)
			}
			match = i
		}
	}
	if match < 0 {
		return fmt.Errorf("--include repository %q does not match --repo", repo)
	}
	repos[match].Include = append(repos[match].Include, pattern)
	return nil
}
