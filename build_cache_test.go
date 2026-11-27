package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestCargoCacheKeyFromSource(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	checkout := func(job string, paths ...string) (string, []SnapshotRepo) {
		t.Helper()
		work := filepath.Join(root, ".work", job)
		tree := filepath.Join(work, "work", "tree")
		m := Manifest{Main: paths[0], Version: 1}
		for _, path := range paths {
			repo := filepath.Join(tree, path)
			if err := os.MkdirAll(filepath.Join(repo, "crate"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, "Cargo.toml"), []byte("[workspace]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			m.Repositories = append(m.Repositories, SnapshotRepo{
				Path: path, RepoID: snapSum([]byte(path)), StoreID: snapSum([]byte(path + "/store")), OID: strings.Repeat("a", 40),
			})
		}
		if err := os.Mkdir(filepath.Join(work, "input"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(work, "input", "manifest.json"), m); err != nil {
			t.Fatal(err)
		}
		return tree, m.Repositories
	}
	single, _ := checkout("flat", "standalone")
	multi, repos := checkout("p/frozen", "one/shared", "two/shared")
	other, _ := checkout("p/later", "one/shared", "two/shared")
	plain := filepath.Join(root, "plain source")
	local := filepath.Join(root, "in-place")
	if err := os.Mkdir(plain, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(local, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(local, ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(local, "crate"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		source string
		want   string
	}{
		{plain, "plain source"},
		{filepath.Join(local, "crate"), "in-place"},
		{filepath.Join(single, "standalone", "crate"), "standalone"},
		{filepath.Join(multi, "one", "shared", "crate"), repos[0].RepoID},
		{filepath.Join(multi, "two", "shared"), repos[1].RepoID},
		{filepath.Join(other, "two", "shared", "crate"), repos[1].RepoID},
	} {
		t.Run(tc.source, func(t *testing.T) {
			repo, err := cargoRepository(root, tc.source)
			if got := cargoCacheKey("p", "different-job", repo); err != nil || got != "p/"+tc.want {
				t.Fatalf("repository cache key = %q, want p/%s, err = %v", got, tc.want, err)
			}
		})
	}
	if got := cargoCacheKey("", "flat", "standalone"); got != ".job-flat/standalone" {
		t.Fatalf("flat job cache key = %q", got)
	}
}

func TestCargoTargetExplicitEnvironment(t *testing.T) {
	for _, name := range []string{"custom", "empty"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			custom := filepath.Join(root, "custom-target")
			makeCacheFixture(t, custom, 1000)
			value := custom
			if name == "empty" {
				value = ""
			}
			r := runner{root: root, ctx: t.Context(),
				s: status{Project: "p"},
				c: config{
					Dir: t.TempDir(), RunID: "explicit-target",
					Env:      map[string]string{cargoTargetEnv: value},
					localEnv: []string{cargoTargetEnv + "=profile-default"},
				},
			}
			if err := r.prepareCaches(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				r.releaseCaches()
				info, err := os.Stat(custom)
				if err != nil {
					t.Error(err)
				} else if !info.ModTime().Equal(time.Unix(1000, 0)) {
					t.Errorf("explicit target timestamp changed to %v", info.ModTime())
				}
			})
			cmd := exec.Command(os.Args[0])
			r.c.mark(cmd, "job")
			if !slices.Contains(cmd.Environ(), cargoTargetEnv+"="+value) {
				t.Fatalf("payload environment lost %s=%q", cargoTargetEnv, value)
			}
			if r.targetLease != nil || len(r.c.cacheUse.leases) != 1 || slices.Contains(r.c.Roots, custom) {
				t.Fatal("explicit target was managed as a cx cache")
			}
			if _, err := os.Stat(filepath.Join(cargoCachePath(root), "p")); !os.IsNotExist(err) {
				t.Fatalf("a managed target was created: %v", err)
			}
			var pin cachePin
			if err := readJSON(filepath.Join(cachePins(root), r.c.RunID), &pin); err != nil || pin.Target != "" {
				t.Fatalf("package pin = %+v, err = %v", pin, err)
			}
		})
	}
}

func TestCacheEvictsOnlyRegisteredTreesOnStateMount(t *testing.T) {
	root := t.TempDir()
	guard, err := lockCaches(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	base := filepath.Join(cargoCachePath(root), "p")
	if err = prepareCacheEntry(root, filepath.Join("cargo-target", "p")); err != nil {
		t.Fatal(err)
	}
	makeCacheFixture(t, base, 998)
	oldLease, err := shareCache(filepath.Join(cargoCachePath(root), ".leases", "p"))
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(oldLease)
	neverPrepared := filepath.Join(base, "unregistered")
	manual := filepath.Join(base, "manual")
	makeCacheFixture(t, neverPrepared, 999)
	makeCacheFixture(t, manual, 1000)
	for _, key := range []string{"manual", "replaced", "mounted", "mounted-late", "evict"} {
		prepareCacheFixture(t, root, "p", key)
	}
	replaced := filepath.Join(base, "replaced")
	marker, err := os.ReadFile(filepath.Join(replaced, cacheIdentityFile))
	if err != nil {
		t.Fatal(err)
	}
	if renameErr := os.Rename(replaced, filepath.Join(root, "retired")); renameErr != nil {
		t.Fatal(renameErr)
	}
	makeCacheFixture(t, replaced, 1001)
	if writeErr := os.WriteFile(filepath.Join(replaced, cacheIdentityFile), marker, 0600); writeErr != nil {
		t.Fatal(writeErr)
	}

	// Inject a mount ID change without a privileged bind mount. The mount
	// root and its parent are both genuine directories on the same device.
	mountpoint := filepath.Join(base, "mounted", "nested", "bind")
	lateMount := filepath.Join(base, "mounted-late", "bind")
	evict := filepath.Join(base, "evict")
	makeCacheFixture(t, mountpoint, 1002)
	makeCacheFixture(t, lateMount, 1003)
	makeCacheFixture(t, evict, 1004)
	lateProbes := 0
	_, err = trimCacheTrees(t.Context(), root, 0, func() diskStatus { return diskStatus{} }, func(f *os.File) (uint64, error) {
		name := filepath.Clean(f.Name())
		if strings.HasSuffix(name, filepath.Join("mounted", "nested", "bind")) {
			return 2, nil
		}
		if strings.HasSuffix(name, filepath.Join("mounted-late", "bind")) {
			lateProbes++
			if lateProbes > 1 {
				return 2, nil
			}
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lateProbes != 2 {
		t.Fatalf("visits to mount introduced after sizing: %d", lateProbes)
	}
	for _, path := range []string{base, neverPrepared, manual, replaced, mountpoint, lateMount} {
		if b, err := os.ReadFile(filepath.Join(path, "data")); err != nil || string(b) != "x" {
			t.Fatalf("protected sentinel in %s: %q, %v", path, b, err)
		}
	}
	for _, path := range []string{evict, cargoTargetLease(root, "p/unregistered")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unexpected %s: %v", path, err)
		}
	}
}

func TestCacheMaintenanceCoalescesAcrossQueueTickets(t *testing.T) {
	h := commands(t)
	policy, err := readHostConfig(h.root)
	if err != nil {
		t.Fatal(err)
	}
	minimum := ^uint64(0)
	policy.MinFreeBytes = &minimum
	if err := writeJSON(filepath.Join(h.root, hostConfigName), policy); err != nil {
		t.Fatal(err)
	}
	probe := func() diskStatus { return diskStatus{minimum: minimum} }
	acquired, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var scans atomic.Int32
	trim := func(ctx context.Context, _ string, _ uint64, probe func() diskStatus) (diskStatus, error) {
		if scans.Add(1) == 1 {
			close(acquired)
			select {
			case <-release:
			case <-ctx.Done():
				return diskStatus{}, ctx.Err()
			}
		}
		return probe(), nil
	}
	go func() {
		done <- coordinateCacheScan(h.ctx, h.root, policy.cacheMaxBytes(0), false, probe, trim)
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	select {
	case <-acquired:
	case err := <-done:
		t.Fatalf("maintenance failed before acquiring lock: %v", err)
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}

	for _, job := range []string{"queue-a", "queue-b", "queue-c"} {
		h.start(job, []string{"--queue-timeout", "2m"}, "after-parent")
	}
	// Every ticket can report disk pressure and process close while the one
	// expensive scan still holds the host maintenance lock.
	for _, job := range []string{"queue-a", "queue-b", "queue-c"} {
		h.cli(exitQueued, "wait", "--job", "p/"+job, "--timeout", "2m")
		closeCtx, cancel := context.WithCancel(h.ctx)
		closeHarness := *h
		closeHarness.ctx = closeCtx
		closeHarness.cli(0, "close", "--job", "p/"+job)
		cancel()
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := coordinateCacheScan(h.ctx, h.root, policy.cacheMaxBytes(0), false, probe, trim); err != nil {
			t.Fatal(err)
		}
	}
	if calls := scans.Load(); calls != 1 {
		t.Fatalf("full scans after three contenders: %d", calls)
	}
}

func cacheFixturePaths() []string {
	return []string{
		filepath.Join(os.Getenv("CARGO_TARGET_DIR"), "using-cache"),
		filepath.Join(os.Getenv("npm_config_cache"), "using-cache"),
	}
}

func launchCacheFixture(dropTracking bool) (launchErr error) {
	child := exec.Command(os.Args[0], "fixture-command", "cache-child")
	if dropTracking {
		// Leave the inherited process group as the only link to the run.
		child = exec.Command("/bin/sh", "-c", `unset CXRUN_RUN; exec 3>&- 4>&-; exec "$1" fixture-command cache-child`,
			"fixture", os.Args[0])
	} else {
		detachWorker(child)
	}
	if err := child.Start(); err != nil {
		return err
	}
	defer func() {
		if releaseErr := child.Process.Release(); releaseErr != nil {
			launchErr = errors.Join(launchErr, releaseErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
	defer cancel()
	if err := awaitFixtureFile(ctx, "cache-child.json"); err != nil {
		return err
	}
	return awaitFixtureFile(ctx, "cache-parent-finish")
}

func useCacheFixture() error {
	paths := cacheFixturePaths()
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("background payload"), 0600); err != nil {
			return err
		}
	}
	pid := os.Getpid()
	if err := writeJSON("cache-child.json", process{PID: pid, Start: processStart(pid)}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
	defer cancel()
	return awaitFixtureCondition(ctx, func() (bool, error) {
		if ready, err := fileExists("cache-finish"); ready || err != nil {
			return ready, err
		}
		for _, path := range paths {
			if _, err := os.Stat(path); err != nil {
				return false, err
			}
		}
		return false, nil
	})
}

func TestBackgroundCacheUseOutlivesForegroundRunner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Windows runner reaps the process tree before terminal publication")
	}
	t.Run("marked-detached", func(t *testing.T) { assertBackgroundCacheUse(t, false) })
	t.Run("unmarked-in-group", func(t *testing.T) { assertBackgroundCacheUse(t, true) })
}

func assertBackgroundCacheUse(t *testing.T, dropTracking bool) {
	t.Helper()
	h := commands(t)
	t.Cleanup(func() {
		for _, name := range []string{"cache-parent-finish", "cache-finish"} {
			_ = os.WriteFile(filepath.Join(h.work, name), nil, 0600)
		}
	})
	policy, err := readHostConfig(h.root)
	if err != nil {
		t.Fatal(err)
	}
	limit := uint64(1)
	policy.CacheMaxBytes = &limit
	if err := writeJSON(filepath.Join(h.root, hostConfigName), policy); err != nil {
		t.Fatal(err)
	}
	var run status
	args := []string{"start", "--project", "p", "--job", "bg-cache", "--dir", h.work, "--cmd",
		"--", h.exe, "fixture-command", "cache-parent"}
	if dropTracking {
		args = append(args, "drop-tracking")
	}
	started := h.cli(0, args...)
	if err := json.Unmarshal([]byte(started), &run); err != nil || run.PID == 0 || run.RunnerStart == 0 {
		t.Fatalf("runner identity: %s, %v", started, err)
	}
	if err := os.WriteFile(filepath.Join(h.work, "cache-parent-finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "wait", "--job", "p/bg-cache", "--timeout", "2m")
	awaitExit := func(p process) {
		t.Helper()
		if err := waitFixtureProcess(h.ctx, p); err != nil {
			t.Fatalf("process %d did not exit: %v", p.PID, err)
		}
	}
	awaitExit(process{PID: run.PID, Start: run.RunnerStart})
	var background process
	if err := readJSON(filepath.Join(h.work, "cache-child.json"), &background); err != nil {
		t.Fatal(err)
	}
	if current := processStart(background.PID); !sameProcess(background.Start, current) {
		t.Fatalf("background consumer %d did not survive foreground runner: start=%d, current=%d", background.PID, background.Start, current)
	}
	if !dropTracking {
		var view inventory
		if err := json.Unmarshal([]byte(h.cli(0, "ps", "--job", "p/bg-cache")), &view); err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(view.Processes, func(p process) bool {
			return p.PID == background.PID && sameProcess(background.Start, p.Start) && p.Marked
		}) {
			t.Fatalf("ps did not find the run marker for background consumer %d: %+v", background.PID, view)
		}
	}
	// A subsequent runner using the same run ID must retain the preceding
	// runner's process group after opening and closing its own leases.
	if dropTracking {
		resumed := runner{root: h.root, ctx: h.ctx, s: status{Project: "p"}, c: config{RunID: run.RunID, Dir: h.work}}
		if err := resumed.prepareCaches(); err != nil {
			t.Fatal(err)
		}
		resumed.releaseCaches()
	}
	probe := func() diskStatus { return diskStatus{} }
	if _, err := trimCaches(h.ctx, h.root, 1, probe); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(cargoCachePath(h.root), "p", filepath.Base(h.work))
	npm := filepath.Join(packageCachePath(h.root), "npm")
	for _, path := range []string{target, npm} {
		if b, err := os.ReadFile(filepath.Join(path, "using-cache")); err != nil || string(b) != "background payload" {
			t.Fatalf("active cache %s was evicted: %q, %v", path, b, err)
		}
	}
	if err := os.WriteFile(filepath.Join(h.work, "cache-finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	awaitExit(background)
	if _, err := trimCaches(h.ctx, h.root, 1, probe); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{target, npm} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("released cache %s was not evicted: %v", path, err)
		}
	}
}

func TestHealthyDiskPeriodicScanReclaimsUnleasedCache(t *testing.T) {
	root := t.TempDir()
	guard, err := lockCaches(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	// Model the reported overage with a 20-byte budget and 48-byte footprint.
	const limit = 20
	for key, size := range map[string]int{"pinned": 30, "idle": 18} {
		prepareCacheFixture(t, root, "p", key)
		if writeErr := os.WriteFile(filepath.Join(cargoCachePath(root), "p", key, "data"), make([]byte, size), 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	pinned, err := shareCache(cargoTargetLease(root, "p/pinned"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(pinned)
	probe := func() diskStatus { return diskStatus{free: 100, minimum: 10} }
	clock := timeshim.NewFake(time.Unix(1000, 0))
	ctx := clockContext(t.Context(), clock)
	if err := coordinateCacheScan(ctx, root, limit, false, probe, trimCaches); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cargoCachePath(root), "p", "idle")); !os.IsNotExist(err) {
		t.Errorf("idle target should have been reclaimed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cargoCachePath(root), "p", "pinned", "data")); err != nil {
		t.Errorf("pinned target should survive: %v", err)
	}
	f := fields{}
	addCacheWarning(f, root, limit)
	if got := f["cache_warning"]; got != "pinned cache exceeds cache_max_bytes (30 > 20 bytes at last scan)" {
		t.Errorf("unexpected warning for pinned cache over budget: %v", got)
	}
	closeBestEffort(pinned)
	if err := coordinateCacheScan(ctx, root, limit, false, probe, trimCaches); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cargoCachePath(root), "p", "pinned")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("scan should have been coalesced: %v", err)
	}
	clock.Advance(diskRecoveryInterval)
	if err := coordinateCacheScan(ctx, root, limit, false, probe, trimCaches); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("later scan should evict the released target: %v", err)
	}
	clear(f)
	addCacheWarning(f, root, limit)
	if f["cache_warning"] != nil {
		t.Fatalf("warning should clear after the next scan: %v", f)
	}
}

func TestGoModuleCacheEvictionRestoresWritePermission(t *testing.T) {
	root := t.TempDir()
	guard, err := lockCaches(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	rel := filepath.Join("packages", goModuleCacheDir)
	if err = prepareCacheEntry(root, rel); err != nil {
		t.Fatal(err)
	}
	lease, err := shareCache(filepath.Join(packageCachePath(root), ".lease"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cache", rel)
	module := makeReadOnlyCacheFixture(t, path)
	probe := func() diskStatus { return diskStatus{} }
	disk, err := trimCaches(t.Context(), root, 0, probe)
	closeBestEffort(lease)
	if err != nil || disk.pinnedBytes == 0 {
		t.Fatalf("leased module cache: %+v, %v", disk, err)
	}
	if _, err := os.Stat(module); err != nil {
		t.Fatal(err)
	}
	if _, err := trimCaches(t.Context(), root, 0, probe); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only module cache should have been reclaimed: %v", err)
	}
}

func makeReadOnlyCacheFixture(t *testing.T, root string) string {
	t.Helper()
	dir := filepath.Join(root, "module@v1.0.0")
	file := filepath.Join(dir, "go.mod")
	t.Cleanup(func() {
		if _, err := os.Stat(file); err == nil {
			_ = os.Chmod(file, 0600)
		}
		if _, err := os.Stat(dir); err == nil {
			_ = os.Chmod(dir, 0700)
		}
	})
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("module example.test/module\n"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestCacheBudgetDefaultAndExplicitZero(t *testing.T) {
	host := &hostConfig{}
	for _, tc := range []struct{ total, want uint64 }{
		{100_000_000_000, 10_000_000_000},
		{1_000_000_000_000, 50_000_000_000},
		{0, 50_000_000_000},
	} {
		if got := host.cacheMaxBytes(tc.total); got != tc.want {
			t.Errorf("volume %d: budget %d, want %d", tc.total, got, tc.want)
		}
	}
	var limit uint64
	host.CacheMaxBytes = &limit
	if got := host.cacheMaxBytes(100_000_000_000); got != limit {
		t.Errorf("explicit zero budget resolved to %d", got)
	}
}

func TestCacheScanKeepsMeasuredBytesWhenSubdirectoryDisappears(t *testing.T) {
	root := t.TempDir()
	guard, err := lockCaches(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	prepareCacheFixture(t, root, "p", "pinned")
	prepareCacheFixture(t, root, "p", "idle")
	idle := filepath.Join(cargoCachePath(root), "p", "idle")
	if err = os.WriteFile(filepath.Join(idle, "data"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(cargoCachePath(root), "p", "pinned")
	for _, name := range []string{"a-before", "z-after"} {
		if writeErr := os.WriteFile(filepath.Join(base, name), make([]byte, 6), 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err = os.Mkdir(filepath.Join(base, "m-vanished"), 0700); err != nil {
		t.Fatal(err)
	}
	pinned, err := shareCache(cargoTargetLease(root, "p/pinned"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(pinned)

	const limit = 10
	probe := func() diskStatus { return diskStatus{free: 100, minimum: 10} }
	clock := timeshim.NewFake(time.Unix(1000, 0))
	ctx := clockContext(t.Context(), clock)
	if err := writeJSON(cacheScanPath(root), cacheScanStamp{PinnedBytes: 30}); err != nil {
		t.Fatal(err)
	}
	faults := 0
	trim := func(ctx context.Context, root string, limit uint64, probe func() diskStatus) (diskStatus, error) {
		return trimCacheTrees(ctx, root, limit, probe, func(f *os.File) (uint64, error) {
			if filepath.Base(f.Name()) == "m-vanished" {
				faults++
				return 0, os.ErrNotExist
			}
			return 1, nil
		})
	}
	if err := coordinateCacheScan(ctx, root, limit, false, probe, trim); err != nil {
		t.Fatal(err)
	}
	if faults != 1 {
		t.Fatalf("expected one missing subdirectory during sizing, got %d", faults)
	}
	if _, err := os.Stat(idle); !os.IsNotExist(err) {
		t.Errorf("measured siblings should put the cache over budget and trigger eviction: %v", err)
	}
	f := fields{}
	addCacheWarning(f, root, limit)
	if got := f["cache_warning"]; got != "pinned cache exceeds cache_max_bytes (30 > 10 bytes at last scan)" {
		t.Errorf("partial scan should preserve the previous warning, got %v", got)
	}

	if err := os.Remove(filepath.Join(base, "m-vanished")); err != nil {
		t.Fatal(err)
	}
	closeBestEffort(pinned)
	clock.Advance(diskRecoveryInterval)
	if err := coordinateCacheScan(ctx, root, limit, false, probe, trimCaches); err != nil {
		t.Fatal(err)
	}
	clear(f)
	addCacheWarning(f, root, limit)
	if f["cache_warning"] != nil {
		t.Fatalf("complete scan should clear the warning after eviction: %v", f)
	}
}
