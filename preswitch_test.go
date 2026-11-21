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
			done := make(chan error, 1)
			go func() {
				done <- run(ctx, []string{"start", "--job", id, "--dir", work, "--prompt-file", prompt, "--sandbox", "workspace-write", "--network", "--idle-exit", "3s", "--session", "local_x"})
			}()
			defer func() {
				cancel()
				<-done
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
			if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil || s.CompletedStatus != outcome || s.TurnStatus != outcome || s.Finished != 1 {
				t.Fatalf("turn status = %+v, %v", s, err)
			}
			report, err := os.ReadFile(filepath.Join(dir, "last.md"))
			if err != nil || !bytes.Contains(report, []byte("last agent message")) {
				t.Fatalf("last.md = %q, %v", report, err)
			}
			if code == 1 {
				if !bytes.HasPrefix(report, []byte("STATUS: FAILED\n")) || !bytes.Contains(report, []byte("retry=false")) {
					t.Fatalf("failure report = %q", report)
				}
				if mode != "failure-status" {
					for _, fragment := range []string{"This content was flagged for possible cybersecurity risk", "code=431", "codexErrorInfo=\"other\""} {
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
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "cache_max_bytes=4")
	goCache := filepath.Join(f.home, "go-cache")
	t.Setenv("GOCACHE", goCache)
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "disk", "--", "sh", "-c", "printf \"%s\\n%s\\n\" \"$CARGO_TARGET_DIR\" \"$GOCACHE\"")
	t.Cleanup(func() { cliOutput(t, 0, nil, f.cx, "close", "local:p/disk") })
	dir := filepath.Join(root, "p", "disk")
	deadline := time.Now().Add(3 * time.Second)
	for {
		var s status
		if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
			t.Fatal(err)
		}
		if s.State != "queued" {
			t.Fatalf("low disk status = %+v", s)
		}
		if strings.Contains(s.QueueReason, "min_free_bytes") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing disk queue reason")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, args := range [][]string{{"doctor", "local", "--json"}, {"overview", "--all", "--json"}} {
		if got := cliOutput(t, 0, nil, f.cx, args...); !bytes.Contains(got, []byte("\"disk_warning\"")) {
			t.Fatalf("%v: %s", args, got)
		}
	}
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "min_free_bytes=0")
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/disk", "--timeout", "3s")
	out, err := os.ReadFile(filepath.Join(dir, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	var done status
	if err = readJSON(filepath.Join(dir, "status.json"), &done); err != nil || done.QueueReason != "" {
		t.Fatalf("completed queue reason = %q, %v", done.QueueReason, err)
	}
	want := filepath.Join(cargoCachePath(root), "p") + "\n" + goCache + "\n"
	if string(out) != want {
		t.Fatalf("build env = %q, want %q", out, want)
	}
}

func TestPreswitchCacheEvictsLeastRecentlyUsed(t *testing.T) {
	root := t.TempDir()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	active := runner{root: root, ctx: ctx, s: status{Project: "active"}}
	if err := active.prepareBuildCache(); err != nil {
		t.Fatal(err)
	}
	defer active.releaseBuildCache()
	peer := runner{root: root, ctx: ctx, s: active.s}
	if err := peer.prepareBuildCache(); err != nil {
		t.Fatal(err)
	}
	defer peer.releaseBuildCache()
	base := cargoCachePath(root)
	for i, name := range []string{"active", "old", "new"} {
		path := filepath.Join(base, name)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(int64(1000+i), 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	zero := uint64(0)
	if warning, err := maintainBuildCache(ctx, root, &hostConfig{CacheMaxBytes: 2, MinFreeBytes: &zero}); err != nil || warning != "" {
		t.Fatalf("cache maintenance = %q, %v", warning, err)
	}
	for _, name := range []string{"active", "old", "new"} {
		_, err := os.Stat(filepath.Join(base, name))
		if (name == "old" && !os.IsNotExist(err)) || (name != "old" && err != nil) {
			t.Fatalf("cache %s: %v", name, err)
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
	var prep PreparedSnapshot
	if err := readJSON(filepath.Join(work, "prepared.json"), &prep); err != nil {
		t.Fatal(err)
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
	if err := rememberPlatformVersion(digest, h.Platform, digest); err != nil {
		t.Fatal(err)
	}
	if err := deploy(ctx, hs, "fixture"); err != nil {
		t.Fatal(err)
	}
	r, err := submitCommand(ctx, hs, "fixture", []string{"submit-command", "p/no-module", f.home, "--", "echo", "ok"})
	if err != nil || r.code != 0 || !json.Valid(r.data) {
		t.Fatalf("submit = %+v, %v", r, err)
	}
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/no-module", "--timeout", "3s")
}
