package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func (h *hostConfig) diskStatus(path string) (free, minimum uint64, warning string) {
	free, total, err := diskSpace(path)
	if err != nil {
		return 0, 0, fmt.Sprintf("disk free space unavailable: %v", err)
	}
	minimum = max(20_000_000_000, total/20)
	if h.MinFreeBytes != nil {
		minimum = *h.MinFreeBytes
	}
	if free < minimum {
		warning = fmt.Sprintf("disk free space %d bytes below min_free_bytes %d", free, minimum)
	}
	return free, minimum, warning
}

func addDiskStatus(f fields, path string, host *hostConfig) {
	free, minimum, warning := host.diskStatus(path)
	if minimum != 0 || warning == "" {
		f["disk_free_bytes"] = free
	}
	if warning != "" {
		f["disk_warning"] = warning
	}
}

func cargoCachePath(root string) string {
	return filepath.Join(root, "cache", "cargo-target")
}

// Only manage real directories within the state root.
func ensureCacheDir(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err == nil && !info.IsDir() {
		return fmt.Errorf("expected a cache directory at %s", path)
	}
	return err
}

func lockBuildCache(ctx context.Context, root string) (*os.File, error) {
	base := cargoCachePath(root)
	for _, path := range []string{filepath.Dir(base), base, filepath.Join(base, ".leases")} {
		if err := ensureCacheDir(path); err != nil {
			return nil, err
		}
	}
	return transferLock(ctx, filepath.Join(base, ".leases", ".maintenance"))
}

func (r *runner) prepareBuildCache() error {
	base := cargoCachePath(r.root)
	project := r.s.Project
	if project == "" {
		project = ".job-" + r.s.Job
	}
	if runtime.GOOS == "windows" {
		project = strings.ToLower(project)
	}
	path := filepath.Join(base, project)
	guard, err := lockBuildCache(r.ctx, r.root)
	if err != nil {
		return err
	}
	defer closeBestEffort(guard)
	if err = ensureCacheDir(path); err != nil {
		return err
	}
	if err = touchCache(path); err != nil {
		return err
	}
	r.cacheLease, err = lockFile(filepath.Join(base, ".leases", project), os.O_CREATE, trySharedLock)
	if err != nil {
		return err
	}
	if r.cacheLease == nil {
		return fmt.Errorf("build cache lease unavailable for %s", project)
	}
	if r.c.Env == nil {
		r.c.Env = map[string]string{}
	}
	r.c.Env["CARGO_TARGET_DIR"] = path
	r.c.Roots = append(r.c.Roots, path)
	return nil
}

// preparePackageCaches shares downloaded dependencies across projects on this node.
func (r *runner) preparePackageCaches() error {
	state, err := filepath.Abs(r.root)
	if err != nil {
		return err
	}
	root := filepath.Join(state, "cache", "packages")
	if err := ensureCacheDir(root); err != nil {
		return err
	}
	caches := []struct {
		dir  string
		keys []string
	}{
		{dir: "npm", keys: []string{"npm_config_cache"}},
		{dir: "pnpm", keys: []string{"PNPM_STORE_DIR", "npm_config_store_dir"}},
		{dir: "yarn", keys: []string{"YARN_CACHE_FOLDER"}},
		{dir: "pip", keys: []string{"PIP_CACHE_DIR"}},
		{dir: "cargo", keys: []string{"CARGO_HOME"}},
	}
	for _, cache := range caches {
		path := filepath.Join(root, cache.dir)
		if err := ensureCacheDir(path); err != nil {
			return err
		}
		r.c.Roots = append(r.c.Roots, path)
		for _, key := range cache.keys {
			if _, supplied := r.c.Env[key]; !supplied {
				r.c.localEnv = append(r.c.localEnv, key+"="+path)
			}
		}
	}

	// Cargo also keeps package-cache locks beside registry, so its entire home
	// is a writable root.
	return ensureCacheDir(filepath.Join(root, "cargo", "registry"))
}

func touchCache(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}

func (r *runner) releaseBuildCache() {
	_ = touchCache(r.c.Env["CARGO_TARGET_DIR"])
	_ = r.cacheLease.Close()
}

func maintainBuildCache(ctx context.Context, root string, host *hostConfig) (string, error) {
	guard, err := lockBuildCache(ctx, root)
	if err != nil {
		return "", err
	}
	defer closeBestEffort(guard)
	if err := trimBuildCache(ctx, cargoCachePath(root), host.CacheMaxBytes); err != nil {
		return "", err
	}
	_, _, warning := host.diskStatus(root)
	return warning, nil
}

func trimBuildCache(ctx context.Context, base string, limit uint64) error {
	type candidate struct {
		lastUsed time.Time
		name     string
		bytes    uint64
	}
	var candidates []candidate
	var total uint64
	entries, err := os.ReadDir(base)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if !ent.IsDir() || ent.Name() == ".leases" {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			return err
		}
		row := candidate{name: ent.Name(), lastUsed: info.ModTime()}
		if err := filepath.WalkDir(filepath.Join(base, ent.Name()), func(path string, d fs.DirEntry, err error) error {
			if os.IsNotExist(err) { // An active build can replace files concurrently.
				return nil
			}
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if d.Type().IsRegular() {
				info, err := d.Info()
				if err != nil && !os.IsNotExist(err) {
					return err
				}
				if err == nil {
					row.bytes += uint64(info.Size())
				}
			}
			return nil
		}); err != nil {
			return err
		}
		total += row.bytes
		candidates = append(candidates, row)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})
	for _, row := range candidates {
		if total <= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		lease, err := lockPath(filepath.Join(base, ".leases", row.name), os.O_CREATE)
		if err != nil {
			return err
		}
		if lease == nil {
			continue
		}
		err = os.RemoveAll(filepath.Join(base, row.name))
		_ = lease.Close()
		if err != nil {
			return err
		}
		total -= row.bytes
	}
	return nil
}
