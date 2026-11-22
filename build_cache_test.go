package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheEvictsOnlyRegisteredTreesOnStateMount(t *testing.T) {
	root := t.TempDir()
	guard, err := lockCaches(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	base := cargoCachePath(root)
	neverPrepared := filepath.Join(base, "unregistered")
	manual := filepath.Join(base, "manual")
	makeCacheFixture(t, neverPrepared, 999)
	makeCacheFixture(t, manual, 1000)
	for _, key := range []string{"manual", "replaced", "mounted", "mounted-late", "evict"} {
		prepareCacheFixture(t, root, key)
	}
	replaced := filepath.Join(base, "replaced")
	marker, err := os.ReadFile(filepath.Join(replaced, cacheIdentityFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replaced, filepath.Join(root, "retired")); err != nil {
		t.Fatal(err)
	}
	makeCacheFixture(t, replaced, 1001)
	if err := os.WriteFile(filepath.Join(replaced, cacheIdentityFile), marker, 0600); err != nil {
		t.Fatal(err)
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
	for _, path := range []string{neverPrepared, manual, replaced, mountpoint, lateMount} {
		if b, err := os.ReadFile(filepath.Join(path, "data")); err != nil || string(b) != "x" {
			t.Fatalf("protected sentinel in %s: %q, %v", path, b, err)
		}
	}
	for _, path := range []string{evict, filepath.Join(base, ".leases", "unregistered")} {
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
		done <- coordinateCacheScan(h.ctx, h.root, policy.CacheMaxBytes, false, probe, trim)
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
		h.start(job, []string{"--queue-timeout", "12s"}, "after-parent")
	}
	// Every ticket can report disk pressure and process close while the one
	// expensive scan still holds the host maintenance lock.
	for _, job := range []string{"queue-a", "queue-b", "queue-c"} {
		h.cli(exitQueued, "wait", "--job", "p/"+job, "--timeout", "2s")
		closeCtx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
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
		if err := coordinateCacheScan(h.ctx, h.root, policy.CacheMaxBytes, false, probe, trim); err != nil {
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

func launchCacheFixture(dropTracking bool) error {
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
	defer child.Process.Release()
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat("cache-child.json"); err == nil {
			if _, err := os.Stat("cache-parent-finish"); err == nil {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("background cache fixture did not start")
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
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat("cache-finish"); err == nil {
			return nil
		}
		for _, path := range paths {
			if _, err := os.Stat(path); err != nil {
				return err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func TestBackgroundCacheUseOutlivesForegroundRunner(t *testing.T) {
	t.Run("marked-detached", func(t *testing.T) { assertBackgroundCacheUse(t, false) })
	if runtime.GOOS != "windows" {
		t.Run("unmarked-in-group", func(t *testing.T) { assertBackgroundCacheUse(t, true) })
	}
}

func assertBackgroundCacheUse(t *testing.T, dropTracking bool) {
	t.Helper()
	h := commands(t)
	t.Cleanup(func() { _ = os.WriteFile(filepath.Join(h.work, "cache-finish"), nil, 0600) })
	policy, err := readHostConfig(h.root)
	if err != nil {
		t.Fatal(err)
	}
	policy.CacheMaxBytes = 1
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
	h.cli(0, "wait", "--job", "p/bg-cache", "--timeout", "4s")
	awaitExit := func(p process) {
		t.Helper()
		for deadline := time.Now().Add(4 * time.Second); sameProcess(p.Start, processStart(p.PID)); {
			if time.Now().After(deadline) {
				t.Fatalf("process %d did not exit", p.PID)
			}
			time.Sleep(10 * time.Millisecond)
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
		resumed := runner{root: h.root, ctx: h.ctx, s: status{Project: "p"}, c: config{RunID: run.RunID}}
		if err := resumed.prepareCaches(); err != nil {
			t.Fatal(err)
		}
		resumed.releaseCaches()
	}
	probe := func() diskStatus { return diskStatus{} }
	if _, err := trimCaches(h.ctx, h.root, 1, probe); err != nil {
		t.Fatal(err)
	}
	target, npm := filepath.Join(cargoCachePath(h.root), "p"), filepath.Join(packageCachePath(h.root), "npm")
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
