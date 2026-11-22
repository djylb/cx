package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPreswitchTurnFailures(t *testing.T) {
	for _, mode := range []string{"failure-pair", "failure-only", "failure-eof", "failure-status", "failure-field", "failure-retry"} {
		t.Run(mode, func(t *testing.T) {
			f := newControllerFixture(t)
			root, work, prompt := runnerFixture(t)
			t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
			t.Setenv("FIXMODE", mode)
			id := "p/" + mode
			dir := filepath.Join(root, filepath.FromSlash(id))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = run(ctx, []string{"start", "--job", id, "--dir", work, "--prompt-file", prompt, "--sandbox", "workspace-write", "--network", "--idle-exit", "3s", "--session", "local_x"})
			}()
			defer func() {
				cancel()
				<-done // CLI assertions check the turn. The runner is canceled during cleanup.
			}()
			outcome := "failed"
			code := 1
			if mode == "failure-retry" {
				outcome = "completed"
				code = 0
			}
			cliOutput(t, code, nil, f.cx, "wait", "local:"+id, "--timeout", "3s")
			cliOutput(t, code, nil, f.cx, "wait", "--any", "--project", "p", "--timeout", "3s")
			var s status
			if err := loadJobStatusInto(dir, &s); err != nil || s.CompletedStatus != outcome || s.TurnStatus != outcome || s.Finished != 1 {
				t.Fatalf("turn status = %+v, %v", s, err)
			}
			report, err := readTurnReport(t.Context(), dir, s.LastCompleted)
			if err != nil || !bytes.Contains(report, []byte("last agent message")) {
				t.Fatalf("last.md = %q, %v", report, err)
			}
			if code == 1 {
				if !bytes.HasPrefix(report, []byte("STATUS: FAILED\n")) || !bytes.Contains(report, []byte("retry=false")) {
					t.Fatalf("failure report = %q", report)
				}
				if mode != "failure-status" {
					for _, fragment := range []string{"This content was flagged for possible cybersecurity risk", "code=431", "errorInfo=\"other\""} {
						if !bytes.Contains(report, []byte(fragment)) {
							t.Fatalf("missing %q from %q", fragment, report)
						}
					}
				}
			} else if string(report) != "last agent message" {
				t.Fatalf("successful report = %q", report)
			}
			args := []string{"result", "local:" + id}
			if code == 0 {
				args = append(args, "--wait")
			}
			if got := cliOutput(t, code, nil, f.cx, args...); !bytes.Contains(got, bytes.TrimSpace(report)) {
				t.Fatalf("result = %q, want report %q", got, report)
			}
			cliOutput(t, code, nil, f.cx, "result", "local:"+id, "--full")
		})
	}
}

func TestPreswitchCacheAndDiskAdmission(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	for _, name := range []string{"codex", "claude"} {
		if err := os.WriteFile(filepath.Join(f.home, name), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "min_free_bytes=18446744073709551615")
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "slots.test=1")
	pool := filepath.Join(root, ".admit", "classes", "test")
	if err := os.MkdirAll(pool, 0700); err != nil {
		t.Fatal(err)
	}
	slot, err := lockPath(filepath.Join(pool, "slot-0.lock"), os.O_CREATE)
	if err != nil || slot == nil {
		t.Fatalf("hold command slot: %v", err)
	}
	t.Cleanup(func() {
		if slot != nil {
			closeBestEffort(slot)
		}
	})
	cache := filepath.Join(packageCachePath(root), "cargo", "registry", "cache")
	prepareCacheFixture(t, root, "p")
	makeCacheFixture(t, cache, 1000)
	goCache := filepath.Join(f.home, "go-cache")
	t.Setenv("GOCACHE", goCache)
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "disk", "--queue-timeout", "30s", "--", "sh", "-c", "printf \"%s\\n%s\\n\" \"$CARGO_TARGET_DIR\" \"$GOCACHE\"")
	t.Cleanup(func() { cliOutput(t, 0, nil, f.cx, "close", "local:p/disk") })
	dir := filepath.Join(root, "p", "disk")
	one := []string{"wait", "local:p/disk"}
	anyJob := []string{"wait", "--any", "--project", "p", "--cursor", "disk"}
	assertQueued := func(reason string, args ...string) {
		t.Helper()
		got := cliOutput(t, exitQueued, nil, f.cx, args...)
		var s status
		if err := json.Unmarshal(got, &s); err != nil || s.State != "queued" || !strings.Contains(s.QueueReason, reason) {
			t.Fatalf("queue notice = %s, %v", got, err)
		}
	}
	assertQueued("min_free_bytes", append(one, "--timeout", "4s")...)
	assertQueued("min_free_bytes", append(anyJob, "--timeout", "4s")...)
	for deadline := time.Now().Add(3 * time.Second); ; {
		_, err := os.Stat(cache)
		if os.IsNotExist(err) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("cache should be reclaimed under disk pressure with the slot occupied: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, args := range [][]string{one, anyJob} {
		cliOutput(t, 124, nil, f.cx, append(args, "--timeout", "250ms")...)
	}
	for _, args := range [][]string{{"doctor", "local", "--json"}, {"overview", "--all", "--json"}} {
		if got := cliOutput(t, 0, nil, f.cx, args...); !bytes.Contains(got, []byte("\"disk_warning\"")) {
			t.Fatalf("%v: %s", args, got)
		}
	}
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "min_free_bytes=0")
	assertQueued("slots", append(one, "--timeout", "5s")...)
	// Neither cursor observes the clear before the same disk reason returns.
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "min_free_bytes=18446744073709551615")
	assertQueued("min_free_bytes", append(one, "--timeout", "4s")...)
	assertQueued("min_free_bytes", append(anyJob, "--timeout", "4s")...)
	src := filepath.Join(f.home, "agent-src")
	if err := os.Mkdir(src, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CXRUN_MONITOR_TIMEOUT", "4s")
	assertQueued("min_free_bytes", "agent", "--host", "local", "--dir", src, "--tag", "disk-agent", "--queue-timeout", "20s", "fixture")
	cliOutput(t, 0, nil, f.cx, "close", "local:p/disk-agent")
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "min_free_bytes=0")
	closeBestEffort(slot)
	slot = nil
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/disk", "--timeout", "3s")
	out, err := os.ReadFile(filepath.Join(dir, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	var done status
	if err = loadJobStatusInto(dir, &done); err != nil || done.QueueReason != "" {
		t.Fatalf("completed queue reason = %q, %v", done.QueueReason, err)
	}
	want := filepath.Join(cargoCachePath(root), "p") + "\n" + goCache + "\n"
	if string(out) != want {
		t.Fatalf("build env = %q, want %q", out, want)
	}

	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "cache_max_bytes=1")
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "grow-cache", "--queue-timeout", "5s", "--", "sh", "-c", `printf xx > "$CARGO_TARGET_DIR/new"`)
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/grow-cache", "--timeout", "3s")
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := os.Stat(filepath.Join(cargoCachePath(root), "p"))
		if os.IsNotExist(err) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("worker did not trim its cache after exit: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPreswitchCacheEvictsLeastRecentlyUsed(t *testing.T) {
	root := t.TempDir()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	active := runner{root: root, ctx: ctx, s: status{Project: "active"}}
	if err := active.prepareCaches(); err != nil {
		t.Fatal(err)
	}
	peer := runner{root: root, ctx: ctx, s: active.s}
	if err := peer.prepareCaches(); err != nil {
		t.Fatal(err)
	}
	base := cargoCachePath(root)
	prepareCacheFixture(t, root, "old")
	prepareCacheFixture(t, root, "new")
	makeCacheFixture(t, filepath.Join(base, "active"), 1000)
	makeCacheFixture(t, filepath.Join(base, "old"), 1002)
	makeCacheFixture(t, filepath.Join(base, "new"), 1003)
	cargo := filepath.Join(packageCachePath(root), "cargo")
	registry := filepath.Join(cargo, "registry", "cache")
	makeCacheFixture(t, registry, 1001)
	credential := filepath.Join(cargo, "credentials.toml")
	if err := os.WriteFile(credential, []byte("user data"), 0600); err != nil {
		t.Fatal(err)
	}
	peer.releaseCaches()
	zero := uint64(0)
	host := &hostConfig{CacheMaxBytes: 3, MinFreeBytes: &zero}
	if err := maintainCaches(ctx, root, host, true); err != nil {
		t.Fatalf("cache maintenance: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "old")); !os.IsNotExist(err) {
		t.Fatalf("an unleased target must be evicted while package and target leases are held: %v", err)
	}
	for _, path := range []string{registry, filepath.Join(base, "active"), filepath.Join(base, "new")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	active.releaseCaches()
	host.CacheMaxBytes = 2
	if err := maintainCaches(ctx, root, host, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(registry); !os.IsNotExist(err) {
		t.Fatalf("oldest package cache should be evicted after lease release: %v", err)
	}
	for _, path := range []string{credential, filepath.Join(base, "active"), filepath.Join(base, "new")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueueNoticePreservesPendingWaitEvents(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	c := config{RunID: "r", Engine: "codex"}
	r := runner{dir: dir, c: c, s: status{Job: "j", RunID: c.RunID, Engine: c.Engine, State: "waiting"}}
	if err := register(root, dir, c, r.s); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatalf("lock owner: %v", err)
	}
	defer closeBestEffort(owner)
	identity, _, start, err := captureJobIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	startFixtureWriter(t, &r)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	for _, turn := range []struct{ id, outcome string }{{"first", "failed"}, {"second", "completed"}} {
		r.s.Finished++
		r.s.LastCompleted = turn.id
		r.s.CompletedStatus = turn.outcome
		if err := r.save(); err != nil {
			t.Fatal(err)
		}
	}
	r.s.State = "queued"
	if err := r.setQueueReason("disk free space below min_free_bytes"); err != nil {
		t.Fatal(err)
	}
	mail := agentMail{Type: "send", To: "parent", Text: "fixture"}
	if err := appendMail(ctx, dir, identity, "send-1", &mail); err != nil {
		t.Fatal(err)
	}
	baseline := map[string]waitCursor{"j": {RunID: c.RunID, StateCursor: start.String()}}
	expect := func(code int, finished int64, mailSeq uint64) {
		t.Helper()
		report, err := pollWaitAny(ctx, root, "", "", baseline)
		if err != nil || report == nil || report.Code != code || (report.State == "queued") != (code == exitQueued) ||
			report.Finished != finished || report.OutboxSeq != mailSeq {
			t.Fatalf("wait report = %+v, %v", report, err)
		}
	}
	expect(exitQueued, 2, 0)
	if before := baseline["j"]; before.StateCursor != start.String() || before.OutboxCursor != "" || before.OutboxSeq != 0 ||
		before.Finished != 0 || before.QueueGeneration != r.s.QueueGeneration {
		t.Fatalf("queue notice advanced unrelated cursor fields: %+v", before)
	}
	expect(1, 1, 0)
	expect(0, 2, 0)
	expect(0, 2, mail.Seq)
}

func TestCachePressureRechecksDiskAndLeavesWorkspace(t *testing.T) {
	root := t.TempDir()
	prepareCacheFixture(t, root, "recent")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	guard, err := lockCaches(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(guard)
	old := filepath.Join(packageCachePath(root), "npm")
	recent := filepath.Join(cargoCachePath(root), "recent")
	work := filepath.Join(root, ".work", "p", "job")
	makeCacheFixture(t, old, 1000)
	makeCacheFixture(t, recent, 1001)
	makeCacheFixture(t, work, 999)
	if err := os.Symlink(work, filepath.Join(old, "workspace")); err != nil && runtime.GOOS != "windows" {
		t.Fatal(err)
	}
	stamp := time.Unix(1000, 0)
	if err := os.Chtimes(old, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	probes := 0
	disk, err := trimCaches(ctx, root, 100, func() diskStatus {
		probes++
		if _, err := os.Stat(old); os.IsNotExist(err) {
			return diskStatus{free: 2, minimum: 2}
		}
		return diskStatus{free: 1, minimum: 2}
	})
	if err != nil || disk.belowMinimum() || probes != 2 {
		t.Fatalf("disk recovery = %+v, probes = %d, err = %v", disk, probes, err)
	}
	for _, path := range []string{recent, work} {
		if b, err := os.ReadFile(filepath.Join(path, "data")); err != nil || string(b) != "x" {
			t.Fatalf("unexpected eviction of %s: %s, %v", path, b, err)
		}
	}
}

func prepareCacheFixture(t *testing.T, root, project string) {
	t.Helper()
	r := runner{root: root, ctx: t.Context(), s: status{Project: project}}
	if err := r.prepareCaches(); err != nil {
		t.Fatal(err)
	}
	r.releaseCaches()
}

func makeCacheFixture(t *testing.T, path string, seconds int64) {
	t.Helper()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(path, "data")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(seconds, 0)
	for _, name := range []string{file, path} {
		if err := os.Chtimes(name, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreswitchCloseKeepsResult(t *testing.T) {
	f := newControllerFixture(t)
	src := filepath.Join(f.home, "src")
	if err := os.Mkdir(src, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "file"), []byte("base\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "close", "--cmd", "--", "sh", "-c", "echo changed > file; echo report")
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/close", "--timeout", "3s")
	root := os.Getenv("CXRUN_STATE")
	work := filepath.Join(root, ".work", "p", "close")
	prep, found, err := readPreparedSnapshot(t.Context(), work)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	cliOutput(t, 0, nil, f.cx, "close", "local:p/close")
	cliOutput(t, 0, nil, f.cx, "close", "local:p/close")
	if _, err := os.Stat(prep.Source); !os.IsNotExist(err) {
		t.Fatalf("checkout still present: %v", err)
	}
	if got := cliOutput(t, 0, nil, f.cx, "patch", "local:p/close", "--full"); !bytes.Contains(got, []byte("+changed")) {
		t.Fatalf("patch = %q", got)
	}
	cliOutput(t, 0, nil, f.cx, "clean", "local:p/close")
	if _, err := os.Stat(work); !os.IsNotExist(err) {
		t.Fatalf("private workspace still present: %v", err)
	}
}

func TestPreswitchDeployWithoutModule(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_MODULE", filepath.Join(f.home, "missing"))
	t.Setenv("CX_SETUP_BINARY", "")
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte("#!/bin/sh\nwhile test \"$1\" != --; do shift; done\nshift 2\nexec sh -c \"$1\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := endpoint{SSH: "fixture", CX: f.cx, Platform: runtime.GOOS + "/" + runtime.GOARCH}
	hs := endpoints{"fixture": h}
	if err := deploy(ctx, hs, "fixture"); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fileHash(exe)
	if err != nil {
		t.Fatal(err)
	}
	h.Platform = "linux/amd64"
	hs["fixture"] = h
	path, err := cachePath(digest, h.Platform)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(exe, path); err != nil {
		t.Fatal(err)
	}
	if err = deploy(ctx, hs, "fixture"); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if cacheErr := rememberPlatformVersion(digest, h.Platform, digest); cacheErr != nil {
		t.Fatal(cacheErr)
	}
	if deployErr := deploy(ctx, hs, "fixture"); deployErr != nil {
		t.Fatal(deployErr)
	}
	r, err := submitCommand(ctx, hs, "fixture", []string{"submit-command", "p/no-module", f.home, "--", "echo", "ok"})
	if err != nil || r.code != 0 || !json.Valid(r.data) {
		t.Fatalf("submit = %+v, %v", r, err)
	}
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/no-module", "--timeout", "3s")
}
