package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
)

type diskStatus struct {
	err     error
	free    uint64
	minimum uint64
}

func (h *hostConfig) diskStatus(path string) diskStatus {
	free, total, err := diskSpace(path)
	return diskStatus{free: free, minimum: h.minFreeBytes(total), err: err}
}

func (d diskStatus) belowMinimum() bool {
	return d.err == nil && d.free < d.minimum
}

func (d diskStatus) queueReason() string {
	switch {
	case d.err != nil:
		return "disk free space unavailable"
	case d.belowMinimum():
		return "disk free space below min_free_bytes"
	default:
		return ""
	}
}

func addDiskStatus(f fields, path string, host *hostConfig) {
	d := host.diskStatus(path)
	switch {
	case d.err != nil:
		f["disk_warning"] = fmt.Sprintf("disk free space unavailable: %v", d.err)
	default:
		f["disk_free_bytes"] = d.free
		if d.belowMinimum() {
			f["disk_warning"] = fmt.Sprintf("disk free space %d bytes below min_free_bytes %d", d.free, d.minimum)
		}
	}
}

func cargoCachePath(root string) string {
	return filepath.Join(root, "cache", "cargo-target")
}

func packageCachePath(root string) string {
	return filepath.Join(root, "cache", "packages")
}

var packageCaches = []struct {
	dir  string
	keys []string
}{
	{dir: "npm", keys: []string{"npm_config_cache"}},
	{dir: "pnpm", keys: []string{"PNPM_STORE_DIR", "npm_config_store_dir"}},
	{dir: "yarn", keys: []string{"YARN_CACHE_FOLDER"}},
	{dir: "pip", keys: []string{"PIP_CACHE_DIR"}},
	{dir: "cargo", keys: []string{"CARGO_HOME"}},
}

// Only manage real directories within the state root.
func ensureCacheDir(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err == nil && !cacheDirectory(info) {
		return fmt.Errorf("expected a cache directory at %s", path)
	}
	return err
}

func lockCaches(ctx context.Context, root string) (*os.File, error) {
	path, err := cacheLockPath(root)
	if err != nil {
		return nil, err
	}
	return transferLock(ctx, path)
}

func cacheLockPath(root string) (string, error) {
	targets := cargoCachePath(root)
	for _, path := range []string{filepath.Dir(targets), targets, filepath.Join(targets, ".leases"), packageCachePath(root), cacheRecords(root), cachePins(root)} {
		if err := ensureCacheDir(path); err != nil {
			return "", err
		}
	}
	return filepath.Join(targets, ".leases", ".maintenance"), nil
}

func (r *runner) prepareCaches() error {
	guard, err := lockCaches(r.ctx, r.root)
	if err != nil {
		return err
	}
	defer closeBestEffort(guard)

	project := r.s.Project
	if project == "" {
		project = ".job-" + r.s.Job
	}
	if runtime.GOOS == "windows" {
		project = strings.ToLower(project)
	}
	targets := cargoCachePath(r.root)
	target := filepath.Join(targets, project)
	if err = prepareCacheEntry(r.root, filepath.Join("cargo-target", project)); err != nil {
		return err
	}
	if err = touchCache(target); err != nil {
		return err
	}
	packages := packageCachePath(r.root)
	for _, cache := range packageCaches {
		path := filepath.Join(packages, cache.dir)
		if cache.dir == "cargo" {
			err = ensureCacheDir(path)
		} else {
			err = prepareCacheEntry(r.root, filepath.Join("packages", cache.dir))
		}
		if err != nil {
			return err
		}
		r.c.Roots = append(r.c.Roots, path)
		for _, key := range cache.keys {
			if _, supplied := r.c.Env[key]; !supplied {
				r.c.localEnv = append(r.c.localEnv, key+"="+path)
			}
		}
	}
	// Cargo keeps package-cache locks beside registry, so its entire home is
	// a writable root. Eviction has a narrower scope than that writable root.
	for _, group := range cargoDownloads {
		parent := filepath.Join("packages", "cargo", group.dir)
		if err = ensureCacheDir(filepath.Join(r.root, "cache", parent)); err != nil {
			return err
		}
		for _, name := range group.children {
			if err = prepareCacheEntry(r.root, filepath.Join(parent, name)); err != nil {
				return err
			}
		}
	}

	use := &cacheUse{}
	if r.c.RunID != "" {
		use.path, err = jobPath(cachePins(r.root), r.c.RunID)
		if err != nil {
			return err
		}
		// A resume may share its run ID with surviving children. Keep the
		// still-live process groups recorded by its previous runner.
		if readErr := readJSON(use.path, &use.pin); readErr != nil && !os.IsNotExist(readErr) {
			return readErr
		}
		use.pin.Target, use.pin.PID, use.pin.Started = project, r.s.PID, r.s.RunnerStart
		// Register before launching a child. Unlike a runner handle, this
		// record survives a runner crash and an ordinary foreground exit.
		if err = writeJSON(use.path, use.pin); err != nil {
			return err
		}
	}
	r.targetLease, err = shareCache(filepath.Join(targets, ".leases", project))
	if err != nil {
		return err
	}
	r.packageLease, err = shareCache(filepath.Join(packages, ".lease"))
	if err != nil {
		closeBestEffort(r.targetLease)
		return err
	}
	if r.c.Env == nil {
		r.c.Env = map[string]string{}
	}
	use.leases = []*os.File{r.targetLease, r.packageLease}
	r.c.cacheUse = use
	r.c.Env["CARGO_TARGET_DIR"] = target
	r.c.Roots = append(r.c.Roots, target)
	return nil
}

func shareCache(path string) (*os.File, error) {
	lease, err := lockFile(path, os.O_CREATE, trySharedLock)
	if err == nil && lease == nil {
		err = fmt.Errorf("cache lease unavailable at %s", path)
	}
	return lease, err
}

func touchCache(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}

func (r *runner) releaseCaches() {
	_ = touchCache(r.c.Env["CARGO_TARGET_DIR"])
	if use := r.c.cacheUse; use != nil && use.path != "" && !cacheRunAlive(r.c.RunID, use.pin.Groups) {
		removeBestEffort(use.path)
	}
	closeBestEffort(r.targetLease)
	closeBestEffort(r.packageLease)
}

func cacheRecords(root string) string { return filepath.Join(root, "cache", ".entries") }
func cachePins(root string) string    { return filepath.Join(root, "cache", ".users") }

const cacheIdentityFile = ".cx-cache-id"

var cargoDownloads = []struct {
	dir      string
	children []string
}{
	{"registry", []string{"cache", "index", "src"}},
	{"git", []string{"db", "checkouts"}},
}

func cacheRecord(root, rel string) string {
	return filepath.Join(cacheRecords(root), snapSum([]byte(filepath.ToSlash(rel))))
}

type cacheIdentity struct {
	Directory string `json:"directory"`
	Nonce     string `json:"nonce"`
}

func prepareCacheEntry(root, rel string) error {
	path := filepath.Join(root, "cache", rel)
	if err := os.Mkdir(path, 0700); err != nil {
		if os.IsExist(err) {
			// Pre-existing directories are available to tools but are never
			// adopted for eviction. A replacement cannot inherit registration.
			return ensureCacheDir(path)
		}
		return err
	}
	key, err := jobDirectoryKey(path)
	if err != nil {
		return err
	}
	nonce := rand.Text()
	if err := writeJobBytes(filepath.Join(path, cacheIdentityFile), []byte(nonce)); err != nil {
		return err
	}
	return writeJSON(cacheRecord(root, rel), cacheIdentity{Directory: key, Nonce: nonce})
}

type cachePin struct {
	Target  string `json:"target"`
	Groups  []int  `json:"groups,omitempty"`
	PID     int    `json:"pid"`
	Started uint64 `json:"started"`
}

type cacheUse struct {
	path   string
	leases []*os.File
	pin    cachePin
}

func (c *config) startCachedCommand(cmd *exec.Cmd) (commandTree, error) {
	tree, err := startCommand(cmd, c.RunID)
	if err != nil {
		return tree, err
	}
	if use := c.cacheUse; use != nil && use.path != "" {
		group := cacheProcessGroup(cmd)
		if group != 0 && !slices.Contains(use.pin.Groups, group) {
			use.pin.Groups = append(use.pin.Groups, group)
			if err := writeJSON(use.path, use.pin); err != nil {
				killTree(cmd)
				err = errors.Join(err, waitProcess(cmd, 5*time.Second))
				tree.cleanup()
				return commandTree{}, fmt.Errorf("record cache process group: %w", err)
			}
		}
	}
	return tree, nil
}

// Active runner leases are authoritative while the runner is alive. The pins
// extend protection to surviving payloads after their runner closes its leases.
func activeCachePins(root string) (map[string]bool, bool, error) {
	entries, err := os.ReadDir(cachePins(root))
	if err != nil {
		return nil, false, err
	}
	targets := make(map[string]bool)
	for _, entry := range entries {
		if !entry.Type().IsRegular() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		path := filepath.Join(cachePins(root), entry.Name())
		var pin cachePin
		if err := readJSON(path, &pin); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, false, err
		}
		if (pin.PID > 0 && sameProcess(pin.Started, processStart(pin.PID))) || cacheRunAlive(entry.Name(), pin.Groups) {
			targets[pin.Target] = true
		} else {
			// The maintenance lock keeps a resumed runner from re-registering
			// until this check and removal are complete.
			removeBestEffort(path)
		}
	}
	return targets, len(targets) > 0, nil
}

func maintainReleasedCaches(ctx context.Context, root string) {
	// A payload may leave caches larger than they were at admission. Run
	// maintenance after releasing the leases, even on cancellation.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transferLimit)
	defer cancel()
	host, err := readHostConfig(root)
	if err == nil {
		err = maintainCaches(ctx, root, host, true)
	}
	if err != nil {
		diagnosticf("cache maintenance: %v\n", err)
	}
}

type cacheScanStamp struct {
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Limit    uint64    `json:"limit"`
	Minimum  uint64    `json:"minimum"`
	Pressure bool      `json:"pressure"`
}

func maintainCaches(ctx context.Context, root string, host *hostConfig, afterRun bool) error {
	return coordinateCacheScan(ctx, root, host.CacheMaxBytes, afterRun,
		func() diskStatus { return host.diskStatus(root) }, trimCaches)
}

func coordinateCacheScan(ctx context.Context, root string, limit uint64, afterRun bool, probe func() diskStatus,
	trim func(context.Context, string, uint64, func() diskStatus) (diskStatus, error)) error {
	path, err := cacheLockPath(root)
	if err != nil {
		return err
	}
	requested := time.Now()
	for {
		var guard *os.File
		if afterRun {
			// A release that occurred during an older scan may have made
			// previously pinned entries reclaimable. The exited worker can
			// wait for the next coalesced pass after publishing terminal state.
			guard, err = transferLock(ctx, path)
		} else {
			// Queued workers request maintenance without waiting for a host scan.
			guard, err = lockPath(path, os.O_CREATE)
		}
		if err != nil || guard == nil {
			return err
		}
		delay, err := scanCaches(ctx, root, filepath.Join(filepath.Dir(path), ".last-scan"),
			limit, afterRun, requested, probe, trim)
		closeBestEffort(guard)
		if err != nil || delay == 0 {
			return err
		}
		if err := pause(ctx, delay); err != nil {
			return err
		}
	}
}

func scanCaches(ctx context.Context, root, stampPath string, limit uint64, afterRun bool, requested time.Time,
	probe func() diskStatus, trim func(context.Context, string, uint64, func() diskStatus) (diskStatus, error)) (time.Duration, error) {
	disk := probe()
	pressure := disk.belowMinimum()
	if !afterRun && !pressure {
		return 0, nil
	}
	var stamp cacheScanStamp
	if err := readJSON(stampPath, &stamp); err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	interval := time.Second
	if pressure {
		interval = diskRecoveryInterval
	}
	elapsed := time.Since(stamp.Finished)
	if stamp.Limit == limit && stamp.Minimum == disk.minimum && stamp.Pressure == pressure {
		if afterRun && !stamp.Started.Before(requested) {
			return 0, nil
		}
		if elapsed >= 0 && elapsed < interval {
			if afterRun {
				return interval - elapsed, nil
			}
			return 0, nil
		}
	}
	started := time.Now()
	_, err := trim(ctx, root, limit, probe)
	// A canceled claimant can hand the work off to the next ticket. Record
	// completed attempts even if a cache could not be removed.
	if ctx.Err() == nil {
		err = errors.Join(err, writeJSON(stampPath, cacheScanStamp{
			Started: started, Finished: time.Now(), Limit: limit, Minimum: disk.minimum, Pressure: pressure,
		}))
	}
	return 0, err
}

type cacheCandidate struct {
	lastUsed time.Time
	path     string
	lease    string
	identity string
	bytes    uint64
}

func cacheCandidates(cache *os.Root, root string) ([]cacheCandidate, error) {
	var candidates []cacheCandidate
	add := func(path, lease string) error {
		var registered cacheIdentity
		if err := readJSON(cacheRecord(root, path), &registered); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for parent := path; parent != "."; parent = filepath.Dir(parent) {
			info, err := cache.Lstat(parent)
			if os.IsNotExist(err) || (err == nil && !cacheDirectory(info)) {
				return nil
			}
			if err != nil {
				return err
			}
		}
		key, err := jobDirectoryKey(filepath.Join(cache.Name(), path))
		if err != nil {
			return err
		}
		if key != registered.Directory || registered.Nonce == "" {
			return nil
		}
		marker, err := cache.ReadFile(filepath.Join(path, cacheIdentityFile))
		if os.IsNotExist(err) || (err == nil && string(marker) != registered.Nonce) {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := cache.Lstat(path)
		if err == nil {
			candidates = append(candidates, cacheCandidate{path: filepath.ToSlash(path), lease: lease, identity: registered.Directory, lastUsed: info.ModTime()})
		}
		return err
	}
	targets := cargoCachePath(root)
	entries, err := fs.ReadDir(cache.FS(), "cargo-target")
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		name := ent.Name()
		if !ent.IsDir() {
			continue
		}
		if _, pathErr := jobPath("", strings.TrimPrefix(name, ".job-")); pathErr != nil {
			continue
		}
		if err = add(filepath.Join("cargo-target", name), filepath.Join(targets, ".leases", name)); err != nil {
			return nil, err
		}
	}
	lease := filepath.Join(packageCachePath(root), ".lease")
	for _, pkg := range packageCaches {
		if pkg.dir != "cargo" {
			if err = add(filepath.Join("packages", pkg.dir), lease); err != nil {
				return nil, err
			}
		}
	}
	// Leave credentials, binaries, configuration and locks in CARGO_HOME.
	for _, group := range cargoDownloads {
		for _, name := range group.children {
			if err = add(filepath.Join("packages", "cargo", group.dir, name), lease); err != nil {
				return nil, err
			}
		}
	}
	return candidates, nil
}

func trimCaches(ctx context.Context, root string, limit uint64, probe func() diskStatus) (diskStatus, error) {
	return trimCacheTrees(ctx, root, limit, probe, cacheMountID)
}

func trimCacheTrees(ctx context.Context, root string, limit uint64, probe func() diskStatus, mountID func(*os.File) (uint64, error)) (diskStatus, error) {
	state, err := os.OpenRoot(root)
	if err != nil {
		return diskStatus{}, err
	}
	defer closeBestEffort(state)
	info, err := state.Lstat("cache")
	if err != nil {
		return diskStatus{}, err
	}
	if !cacheDirectory(info) {
		return probe(), nil
	}
	cache, err := state.OpenRoot("cache")
	if err != nil {
		return diskStatus{}, err
	}
	defer closeBestEffort(cache)
	mount, err := rootedMount(state, ".", mountID)
	if err != nil {
		return diskStatus{}, err
	}
	if id, mountErr := rootedMount(cache, ".", mountID); mountErr != nil || mount != id {
		return probe(), mountErr
	}
	candidates, err := cacheCandidates(cache, root)
	if err != nil {
		return diskStatus{}, err
	}
	var total uint64
	for i := range candidates {
		row := &candidates[i]
		if sizeErr := sizeCacheTree(ctx, cache, row, mount, mountID); sizeErr != nil {
			if errors.Is(sizeErr, errUnsafeCacheTree) || os.IsNotExist(sizeErr) {
				row.lease = ""
				continue
			}
			return diskStatus{}, sizeErr
		}
		total += row.bytes
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})
	disk := probe()
	if total <= limit && !disk.belowMinimum() {
		return disk, nil
	}
	targetsPinned, packagesPinned, err := activeCachePins(root)
	if err != nil {
		return disk, err
	}
	for _, row := range candidates {
		if total <= limit && !disk.belowMinimum() {
			break
		}
		if err := ctx.Err(); err != nil {
			return disk, err
		}
		if row.lease == "" || (row.bytes == 0 && !disk.belowMinimum()) {
			continue
		}
		if target, cargo := strings.CutPrefix(row.path, "cargo-target/"); cargo {
			if targetsPinned[target] {
				continue
			}
		} else if packagesPinned {
			continue
		}
		lease, err := lockPath(row.lease, 0)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return disk, err
		}
		if lease == nil {
			continue
		}
		err = removeCacheTree(ctx, cache, filepath.FromSlash(row.path), row.identity, mount, mountID)
		closeBestEffort(lease)
		if errors.Is(err, errUnsafeCacheTree) {
			continue
		}
		if err != nil {
			return disk, err
		}
		total -= row.bytes
		if disk.belowMinimum() {
			disk = probe()
		}
	}
	return disk, nil
}

var errUnsafeCacheTree = errors.New("cache tree identity or mount changed")

func rootedMount(root *os.Root, name string, mountID func(*os.File) (uint64, error)) (uint64, error) {
	f, err := root.Open(name)
	if err != nil {
		return 0, err
	}
	defer closeBestEffort(f)
	return mountID(f)
}

func sizeCacheTree(ctx context.Context, cache *os.Root, row *cacheCandidate, mount uint64, mountID func(*os.File) (uint64, error)) error {
	for dir := filepath.Dir(filepath.FromSlash(row.path)); dir != "."; dir = filepath.Dir(dir) {
		id, err := rootedMount(cache, dir, mountID)
		if err != nil {
			return err
		}
		if id != mount {
			return errUnsafeCacheTree
		}
	}
	return fs.WalkDir(cache.FS(), row.path, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) { // An active build can replace files concurrently.
			return nil
		}
		if err != nil {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if path == row.path+"/"+cacheIdentityFile || (!d.IsDir() && !d.Type().IsRegular()) {
			return nil
		}
		info, err := d.Info()
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			if !cacheDirectory(info) {
				return errUnsafeCacheTree
			}
			id, err := rootedMount(cache, filepath.FromSlash(path), mountID)
			if err != nil {
				return err
			}
			if id != mount {
				return errUnsafeCacheTree
			}
		}
		if info.Mode().IsRegular() {
			row.bytes += uint64(info.Size())
			if accessed := cacheAccessTime(info); accessed.After(row.lastUsed) {
				row.lastUsed = accessed
			}
		}
		if stamp := info.ModTime(); stamp.After(row.lastUsed) {
			row.lastUsed = stamp
		}
		return nil
	})
}

// Open each directory before enumerating it. A mount introduced after sizing
// cannot redirect recursive deletion into another filesystem or a bind source.
func removeCacheTree(ctx context.Context, parent *os.Root, name, expected string, mount uint64, mountID func(*os.File) (uint64, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := parent.Lstat(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		if !cacheDirectory(info) {
			return errUnsafeCacheTree
		}
		dir, err := parent.OpenRoot(name)
		if err != nil {
			return err
		}
		err = emptyCacheTree(ctx, dir, expected, mount, mountID)
		closeBestEffort(dir)
		if err != nil {
			return err
		}
	} else if expected != "" {
		return errUnsafeCacheTree
	}
	if err := parent.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func emptyCacheTree(ctx context.Context, dir *os.Root, expected string, mount uint64, mountID func(*os.File) (uint64, error)) error {
	f, err := dir.Open(".")
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	id, err := mountID(f)
	if err != nil {
		return err
	}
	if id != mount {
		return errUnsafeCacheTree
	}
	if expected != "" {
		key, keyErr := jobDirectoryFileKey(f)
		if keyErr != nil {
			return keyErr
		}
		if key != expected {
			return errUnsafeCacheTree
		}
	}
	entries, err := f.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if expected != "" && entry.Name() == cacheIdentityFile {
			continue
		}
		if err := removeCacheTree(ctx, dir, entry.Name(), "", mount, mountID); err != nil {
			return err
		}
	}
	if expected != "" {
		return removeCacheTree(ctx, dir, cacheIdentityFile, "", mount, mountID)
	}
	return nil
}
