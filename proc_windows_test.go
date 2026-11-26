//go:build windows

package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestAppServerInOwnedJobObject(t *testing.T) {
	root, work, _ := runnerFixture(t)
	s, err := openServer(config{Dir: work}, root, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = s.tree.cleanup()
		close(s.stop)
		closeBestEffort(s.in)
		_ = waitProcess(s.cmd, fixtureLimit, nil)
	}()
	if s.tree.job == 0 || !inJob(s.tree.process, s.tree.job) {
		t.Fatal("app-server outside owned job")
	}
}

func TestWindowsReapsUnmarkedGrandchild(t *testing.T) {
	for _, stop := range []string{"foreground-exit", "close", "runner-killed"} {
		t.Run(stop, func(t *testing.T) {
			h := commands(t)
			h.start("tree", nil, "detached-parent", "gated", "unmarked")
			path := filepath.Join(h.work, "sleep.json")
			if err := awaitFixtureFile(h.ctx, path); err != nil {
				t.Fatal(err)
			}
			var child process
			if err := readJSON(path, &child); err != nil {
				t.Fatal(err)
			}
			childHandle := fixtureProcessHandle(t, child, windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ|windows.PROCESS_TERMINATE)
			defer func() { _ = windows.TerminateProcess(childHandle, 1) }()
			run := h.status("tree")
			if marked, err := hasRun(childHandle, run.RunID); err != nil || marked {
				t.Fatalf("grandchild marker = %t, %v", marked, err)
			}
			job, err := openTrackedJob(run.RunID, jobQuery)
			if err != nil {
				t.Fatal(err)
			}
			member := inJob(childHandle, job)
			if stop == "runner-killed" {
				// Exercise the kernel's last-handle cleanup without invoking
				// any terminal status projection or explicit reclaim.
				_ = windows.CloseHandle(job)
			} else {
				// An independent observer holds the job open through teardown.
				defer func() { _ = windows.CloseHandle(job) }()
			}
			if !member {
				t.Fatal("grandchild was outside the job")
			}
			switch stop {
			case "foreground-exit":
				if err := os.WriteFile(filepath.Join(h.work, "allow-exit"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				h.cli(0, "wait", "--job", "p/tree")
			case "close":
				h.cli(0, "close", "--job", "p/tree")
			case "runner-killed":
				workerHandle := fixtureProcessHandle(t, process{PID: run.PID, Start: run.RunnerStart}, windows.PROCESS_TERMINATE)
				if err := windows.TerminateProcess(workerHandle, 1); err != nil {
					t.Fatal(err)
				}
			}
			var waitMS uint32
			if stop == "runner-killed" {
				waitMS = 5000
			}
			if got, err := windows.WaitForSingleObject(childHandle, waitMS); err != nil || got != windows.WAIT_OBJECT_0 {
				t.Fatalf("grandchild %d survived %s: wait=%d, %v", child.PID, stop, got, err)
			}
			if stop == "runner-killed" {
				h.cli(5, "wait", "--job", "p/tree")
			}
		})
	}
}

func fixtureProcessHandle(t *testing.T, p process, access uint32) windows.Handle {
	t.Helper()
	h, err := windows.OpenProcess(access|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(p.PID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = windows.CloseHandle(h) })
	if !sameProcess(p.Start, handleStart(h)) {
		t.Fatalf("process identity changed: PID %d", p.PID)
	}
	return h
}
