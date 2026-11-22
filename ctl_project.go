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
	"strings"

	"github.com/BurntSushi/toml"
)

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
	var repos []string
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
	type repository struct {
		Path       string `toml:"path"`
		Branch     string `toml:"integration_branch"`
		Visibility string `toml:"visibility"`
		Publish    bool   `toml:"publish"`
	}
	def := struct {
		Host         string       `toml:"default_host"`
		Repositories []repository `toml:"repositories"`
	}{Host: host}
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
		def.Repositories = append(def.Repositories, repository{primary, branch, "private", false})
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
