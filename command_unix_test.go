//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func waitFixtureProcess(ctx context.Context, p process) error {
	return awaitFixtureCondition(ctx, func() (bool, error) {
		return !sameProcess(p.Start, processStart(p.PID)), nil
	})
}

func TestDetachedCommandIgnoresHangup(t *testing.T) {
	h := commands(t)
	h.start("hup", []string{"--timeout", "2m"}, "after-parent")
	s := h.status("hup")
	group, err := syscall.Getpgid(s.PID)
	if err != nil || group != s.PID {
		t.Fatalf("worker pid=%d group=%d err=%v", s.PID, group, err)
	}
	if err = syscall.Kill(s.PID, syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(h.work, "parent-gone"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "wait", "--job", "p/hup")
}

func TestDetachedSleepLifecycle(t *testing.T) {
	for _, op := range []string{"close", "clean", "active-close"} {
		t.Run(op, func(t *testing.T) {
			name := "detached"
			h := commands(t)
			active := op == "active-close"
			if active {
				h.start(name, nil, "detached-parent")
			} else {
				h.start(name, nil, "detached-parent", "exit")
				h.cli(0, "wait", "--job", "p/"+name)
			}
			var child process
			childFile := filepath.Join(h.work, "sleep.json")
			if err := awaitFixtureFile(h.ctx, childFile); err != nil {
				t.Fatal("sleep did not start", err)
			}
			if err := readJSON(childFile, &child); err != nil {
				t.Fatal(err)
			}
			if child.Start == 0 {
				t.Fatal("missing sleep creation time")
			}
			t.Cleanup(func() {
				if sameProcess(child.Start, processStart(child.PID)) {
					_ = syscall.Kill(child.PID, syscall.SIGKILL)
				}
			})
			listed := func() bool {
				t.Helper()
				var v inventory
				b := h.cli(0, "ps", "--project", "p", "--job", name)
				if err := json.Unmarshal([]byte(b), &v); err != nil {
					t.Fatal(err)
				}
				for _, p := range v.Processes {
					if p.PID == child.PID && p.Start == child.Start && !p.InTree {
						return true
					}
				}
				return false
			}
			if err := awaitFixtureCondition(h.ctx, func() (bool, error) {
				return listed(), nil
			}); err != nil {
				t.Fatal("detached sleep missing before close", err)
			}
			if active {
				h.cli(0, "close", "--job", "p/"+name)
				h.cli(1, "wait", "--job", "p/"+name)
			} else {
				h.cli(0, op, "--job", "p/"+name)
			}
			if sameProcess(child.Start, processStart(child.PID)) {
				t.Fatal("sleep survived close")
			}
		})
	}
}
func TestReusedPIDSkipsSignal(t *testing.T) {
	cmd := shellCommand("exec /bin/sleep 5")
	cmd.Dir = t.TempDir()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	identity := process{PID: cmd.Process.Pid, Start: processStart(cmd.Process.Pid) + 1}
	if err := signalMember(identity, true, ""); !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	identity.Start = processStart(cmd.Process.Pid)
	if err := signalMember(identity, true, "different-run"); err == nil {
		t.Fatal("unmarked process accepted")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal(err)
	}
}
