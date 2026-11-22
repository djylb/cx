package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

type workspaceBinding struct {
	path string
	key  string
	ids  []string
}

func bindWorkspace(root, cwd string) (workspaceBinding, error) {
	if cwd == "" {
		return workspaceBinding{}, nil
	}
	path, err := filepath.Abs(cwd)
	if err != nil {
		return workspaceBinding{}, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return workspaceBinding{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return workspaceBinding{}, err
	}
	if !info.IsDir() {
		return workspaceBinding{}, fmt.Errorf("working directory is not a directory: %s", path)
	}
	bound := workspaceBinding{path: path}
	base, err := filepath.EvalSymlinks(filepath.Join(root, ".work"))
	if os.IsNotExist(err) {
		return bound, nil
	}
	if err != nil {
		return workspaceBinding{}, err
	}
	rel, err := filepath.Rel(base, path)
	if err != nil || !filepath.IsLocal(rel) {
		return bound, nil
	}
	if rel == "." {
		return workspaceBinding{}, errors.New("working directory cannot be the workspace container")
	}
	// A workspace is addressed by a flat ID or by project/job. A nested
	// path can have both prefixes, so lock them in the usual ID order.
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for n := 1; n <= min(2, len(parts)); n++ {
		id := strings.Join(parts[:n], "/")
		if _, pathErr := jobDir(base, id); pathErr == nil {
			bound.ids = append(bound.ids, id)
		}
	}
	bound.key, err = jobDirectoryKey(path)
	return bound, err
}

func (b workspaceBinding) lock(ctx context.Context, root string, jobs ...string) (func(), error) {
	ids := append(slices.Clone(b.ids), jobs...)
	slices.Sort(ids)
	var locks []*os.File
	unlock := func() {
		for i := len(locks) - 1; i >= 0; i-- {
			closeBestEffort(locks[i])
		}
		locks = nil
	}
	for _, id := range slices.Compact(ids) {
		f, err := jobOperationLock(ctx, root, id)
		if err != nil {
			unlock()
			return nil, err
		}
		locks = append(locks, f)
	}
	if b.key != "" {
		if err := (jobIdentity{key: b.key}).checkDir(b.path); err != nil {
			unlock()
			return nil, fmt.Errorf("workspace changed before registration: %w", err)
		}
	}
	return unlock, nil
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
	defer closeBestEffort(lock)
	work, e := jobDir(filepath.Join(root, ".work"), id)
	if e != nil {
		return e
	}
	scripts, e := jobDir(filepath.Join(root, ".scripts"), id)
	if e != nil {
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
	if _, e = os.Stat(dir); os.IsNotExist(e) {
		project, job, ok := strings.Cut(id, "/")
		if !ok {
			project, job = "", id
		}
		return errors.Join(reapDetachedJobs(root, project, job), cleanPrivateWork(ctx, work, scripts))
	} else if e != nil {
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
	var err error
	work, err = filepath.EvalSymlinks(work)
	if err != nil {
		return "", err
	}
	var user string
	err = walkJobDirs(ctx, root, "", func(dir, _, _ string) (bool, bool, error) {
		s, _, statusErr := loadJobStatus(dir)
		if errors.Is(statusErr, os.ErrNotExist) {
			return false, false, nil
		}
		if statusErr != nil {
			return true, false, statusErr
		}
		other := s.Job
		if s.Project != "" {
			other = s.Project + "/" + s.Job
		}
		if other == id || s.final {
			return true, false, nil
		}
		var c config
		if configErr := readJobConfig(dir, &c); configErr != nil {
			return true, false, configErr
		}
		if !snapWithin(work, c.Dir) {
			cwd, resolveErr := filepath.EvalSymlinks(c.Dir)
			if os.IsNotExist(resolveErr) {
				return true, false, nil
			}
			if resolveErr != nil {
				return true, false, resolveErr
			}
			if !snapWithin(work, cwd) {
				return true, false, nil
			}
		}
		// The workspace operation lock prevents a coordinated restart.
		// This probe cannot acquire the registry while close holds owner.lock.
		free, probeErr := probeJobOwner(dir)
		if os.IsNotExist(probeErr) {
			return false, false, nil
		}
		if probeErr != nil {
			return true, false, probeErr
		}
		if free {
			pending, handoffErr := commandHandoffPending(dir, s)
			if handoffErr != nil || !pending {
				return true, false, handoffErr
			}
		}
		user = other
		return true, true, nil
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
	defer closeBestEffort(schedule)
	owner, e := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if e != nil {
		return e
	}
	if owner == nil {
		return errors.New("job is still being submitted")
	}
	defer closeBestEffort(owner)
	r, e := h.invoke(ctx, 90*time.Second, false, "clean-job", id)
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
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
