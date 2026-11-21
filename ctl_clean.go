package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// The lock lives outside the directories removed by clean so it also
// serializes the next submission for the same job ID.
func jobOperationLock(ctx context.Context, root, id string) (*os.File, error) {
	p, e := jobDir(filepath.Join(root, ".job-locks"), id)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return nil, e
	}
	return lockUntil(ctx, p+".lock")
}
func cleanNodeJob(ctx context.Context, id string) error {
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	lock, e := jobOperationLock(ctx, root, id)
	if e != nil {
		return e
	}
	defer lock.Close()
	work, e := jobDir(filepath.Join(root, ".work"), id)
	if e != nil {
		return e
	}
	scripts, e := jobDir(filepath.Join(root, ".scripts"), id)
	if e != nil {
		return e
	}
	if _, e = os.Stat(dir); os.IsNotExist(e) {
		return cleanPrivateWork(ctx, work, scripts)
	} else if e != nil {
		return e
	}
	if _, e = os.Stat(work); e == nil {
		user, err := workspaceUser(ctx, root, work, id)
		if err != nil {
			return err
		}
		if user != "" {
			return fmt.Errorf("%s is using this workspace", user)
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	// Explicit working directories belong to the caller. Only remove the
	// reserved job workspace and script directory, never config.Dir.
	if e = run(ctx, []string{"clean", "--job", id}); e != nil {
		return e
	}
	return cleanPrivateWork(ctx, work, scripts)
}

func workspaceUser(ctx context.Context, root, work, id string) (string, error) {
	var user string
	err := scanJobs(ctx, root, "", "", func(s status) (bool, error) {
		other := s.Job
		if s.Project != "" {
			other = s.Project + "/" + s.Job
		}
		if other == id || isTerminal(s.State) {
			return false, nil
		}
		d, err := jobDir(root, other)
		if err != nil {
			return false, err
		}
		var c config
		if err := readJSON(filepath.Join(d, "config.json"), &c); err != nil {
			return false, err
		}
		if snapWithin(work, c.Dir) {
			user = other
			return true, nil
		}
		return false, nil
	})
	return user, err
}

// Only paths allocated by startNodeJob are eligible for close-time removal.
func privateWorkspaceID(root, work string) string {
	base := filepath.Join(root, ".work")
	rel, err := filepath.Rel(base, work)
	if err != nil {
		return ""
	}
	id := filepath.ToSlash(rel)
	if _, err := jobDir(base, id); err != nil {
		return ""
	}
	return id
}

func (r *runner) releaseClosedWorkspace() error {
	ctx := r.ctx
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return err
	}
	id := privateWorkspaceID(r.root, work)
	if id == "" {
		return nil
	}
	lock, err := jobOperationLock(ctx, r.root, id)
	if err != nil {
		return err
	}
	defer lock.Close()
	if _, err = os.Stat(filepath.Join(work, "input", "manifest.json")); os.IsNotExist(err) {
		return nil // Explicit source, whose directory belongs to the caller.
	} else if err != nil {
		return err
	}
	if readyResult(r.s) {
		if _, err = sealTurn(ctx, r.dir, r.s, r.s.Finished); err != nil {
			return err
		}
	}
	user, err := workspaceUser(ctx, r.root, work, "")
	if err != nil || user != "" {
		return err // Last user to close reclaims the shared checkout.
	}
	if err := releaseSnapshotWorktrees(ctx, work); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(work, "work"))
}

func cleanControllerJob(ctx context.Context, h endpoint, host, id string) error {
	dir, e := submissionDir(host, id)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	schedule, e := lockUntil(ctx, filepath.Join(dir, "schedule.lock"))
	if e != nil {
		return e
	}
	defer schedule.Close()
	owner, e := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if e != nil {
		return e
	}
	if owner == nil {
		return errors.New("job is still being submitted")
	}
	defer owner.Close()
	var old legacyCleanup
	var oldPath string
	args := []string{"clean-job", id}
	if _, err := os.Stat(filepath.Join(dir, "input.json")); os.IsNotExist(err) {
		path, err := legacyRouteFile(id)
		if err == nil {
			err = readJSON(path, &old)
		}
		if err == nil && old.Host == host && old.Work != "" {
			l, err := lockUntil(ctx, strings.TrimSuffix(path, ".env")+".lock")
			if err != nil {
				return err
			}
			defer l.Close()
			if err = readJSON(path, &old); err != nil {
				return err
			}
			if old.Host != host || old.Project+"/"+old.Tag != id || old.Submit == "submitting" || !snapHex(old.Request, 64) {
				return errors.New("legacy registration changed or is still submitting")
			}
			args = append(args, old.Work, old.Request)
			oldPath = path
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	r, e := h.invoke(ctx, 90*time.Second, false, args...)
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	if oldPath != "" {
		if e = os.Remove(oldPath); e != nil {
			return e
		}
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, v := range entries {
		if v.IsDir() || v.Name() == "schedule.lock" || v.Name() == "worker.lock" {
			continue
		}
		if e = os.Remove(filepath.Join(dir, v.Name())); e != nil {
			return e
		}
	}
	path, e := routePath(id)
	if e != nil {
		return e
	}
	var saved string
	if e = readJSON(path, &saved); e == nil && saved == host {
		return os.Remove(path)
	} else if e != nil && !os.IsNotExist(e) {
		return e
	}
	return nil
}

func cleanPrivateWork(ctx context.Context, work, scripts string) error {
	if err := releaseSnapshotWorktrees(ctx, work); err != nil {
		return err
	}
	return errors.Join(os.RemoveAll(work), os.RemoveAll(scripts))
}
func releaseSnapshotWorktrees(ctx context.Context, work string) error {
	var m Manifest
	e := readJSON(filepath.Join(work, "input", "manifest.json"), &m)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if e = m.validate(); e != nil {
		return e
	}
	store, e := snapStore()
	if e != nil {
		return e
	}
	for _, r := range m.Repositories {
		target := filepath.Join(work, "work", "tree", r.Path)
		g := snapGit{ctx: ctx, dir: r.bare(store)}
		if _, e = os.Lstat(filepath.Join(target, ".git")); e == nil {
			if e = g.worktree("remove", "--force", target); e != nil {
				return e
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		if e = g.worktree("prune", "--expire", "now"); e != nil {
			return e
		}
	}
	return nil
}

type legacyCleanup struct {
	Host, Project, Tag, State, Work, Request, Mux, Socket string
	Submit                                                string `json:"submit_status"`
}

func cleanLegacyJob(ctx context.Context, id, work, token string) error {
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	// The old controller's private work root ends in cx-v2/jobs/PROJECT/TAG.
	work = filepath.FromSlash(work)
	suffix := filepath.Join("cx-v2", "jobs", filepath.FromSlash(id))
	if !filepath.IsAbs(work) || filepath.Clean(work) != work || !strings.HasSuffix(work, string(os.PathSeparator)+suffix) || !snapHex(token, 64) {
		return errors.New("invalid legacy workspace")
	}
	if _, e = os.Stat(work); os.IsNotExist(e) {
		if _, e = os.Stat(dir); os.IsNotExist(e) {
			return nil
		}
		return errors.New("missing legacy request")
	} else if e != nil {
		return e
	}
	lock, e := lockUntil(ctx, filepath.Join(filepath.Dir(work), "."+filepath.Base(work)+".lock"))
	if e != nil {
		return e
	}
	defer lock.Close()
	var old legacyCleanup
	if e = readJSON(filepath.Join(work, "request.json"), &old); e != nil {
		return e
	}
	if old.Request != token || old.Project+"/"+old.Tag != id || filepath.Clean(old.Work) != work || filepath.Clean(old.State) != filepath.Clean(root) {
		return errors.New("legacy registration changed")
	}
	var hadState bool
	if _, e = os.Stat(dir); e == nil {
		hadState = true
		if e = run(ctx, []string{"clean", "--job", id}); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if old.Mux != "" {
		ident, _ := json.Marshal(id)
		if old.Mux != "cxv2-"+snapSum(ident)[:32] {
			return errors.New("unexpected legacy session")
		}
		exe := "tmux"
		args := []string{}
		session := "=" + old.Mux
		if runtime.GOOS == "windows" {
			exe = psmuxProgram()
			args = []string{"-L", muxSocket()}
			session = old.Mux
		} else if old.Socket != "" {
			args = []string{"-S", old.Socket}
		}
		mctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if e = exec.CommandContext(mctx, exe, append(args, "has-session", "-t", session)...).Run(); e == nil {
			if !hadState {
				return errors.New("legacy worker active")
			}
			if e = exec.CommandContext(mctx, exe, append(args, "kill-session", "-t", session)...).Run(); e != nil {
				return e
			}
		} else {
			var exit *exec.ExitError
			if !errors.As(e, &exit) || exit.ExitCode() != 1 {
				return e
			}
		}
	}
	checkout := filepath.Join(work, "jobs", "checkout")
	var m Manifest
	if e = readJSON(filepath.Join(checkout, "input", "manifest.json"), &m); e == nil {
		if e = m.validate(); e != nil {
			return e
		}
		for _, r := range m.Repositories {
			target := filepath.Join(checkout, "work", "tree", r.Path)
			if info, err := os.Lstat(filepath.Join(target, ".git")); err == nil && info.Mode().IsRegular() {
				g := snapGit{ctx: ctx, dir: target}
				common, err := g.get("rev-parse", "--path-format=absolute", "--git-common-dir")
				if err != nil {
					return err
				}
				g.dir = common
				if err = g.worktree("remove", "--force", target); err != nil {
					return err
				}
				if err = g.worktree("prune", "--expire", "now"); err != nil {
					return err
				}
			} else if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	return os.RemoveAll(work)
}
