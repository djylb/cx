package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitFollowup(t *testing.T) {
	for _, mode := range []string{"wait-followup", "wait-enospc"} {
		t.Run(mode, func(t *testing.T) {
			f := newControllerFixture(t)
			h := newRuns(t, mode)
			gate := filepath.Join(h.work, "start-second")
			t.Setenv("FIX_WAIT_START", gate)
			const id = "p/job"
			done := h.start(id, "--idle-exit", "10s", "--max-restarts", "0")
			idle := func(s status) bool { return s.State == "waiting" && s.Finished == 1 }
			h.await(id, idle)

			nodeWait := func(code int, limit string) string {
				return string(cliOutput(t, code, nil, f.cx, "node", "wait", "--job", id, "--timeout", limit))
			}
			controllerWait := func(code int, limit string) string {
				return string(cliOutput(t, code, nil, f.cx, "wait", "local:"+id, "--timeout", limit))
			}
			checkTurn := func(out, id string) {
				t.Helper()
				if !strings.Contains(out, "completed_turn="+id+" outcome=completed") {
					t.Fatalf("wait: expected completion of %s, got %q", id, out)
				}
			}
			if mode == "wait-enospc" {
				if err := h.ctl("say", id, "second round"); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err == nil {
					t.Fatal("expected runner failure")
				}
				stderr, err := os.ReadFile(filepath.Join(h.root, id, "server.stderr.log"))
				if err != nil || !strings.Contains(string(stderr), "ENOSPC") {
					t.Fatalf("expected ENOSPC, got %q (%v)", stderr, err)
				}
				h.await(id, func(s status) bool { return s.State == "failed" && s.Finished == 1 && s.LastCompleted == "first" })
				resumed := make(chan error, 1)
				go func() { resumed <- h.ctl("resume", id) }()
				done = resumed
				h.await(id, idle)
			} else {
				checkTurn(nodeWait(0, "300ms"), "first")
				checkTurn(controllerWait(0, "300ms"), "first")
			}
			if err := h.ctl("say", id, "second round"); err != nil {
				t.Fatal(err)
			}
			// Check immediately after publication, then again with the app-server
			// holding the request before turn/started.
			nodeWait(124, "100ms")
			h.await(id, func(s status) bool {
				entries, err := os.ReadDir(filepath.Join(h.root, id, "inbox", "pending"))
				return err == nil && len(entries) == 1 && s.TurnStatus != "inProgress"
			})
			controllerWait(124, "150ms")
			if err := os.WriteFile(gate, nil, 0600); err != nil {
				t.Fatal(err)
			}
			h.await(id, func(s status) bool { return s.Turn == "second" && s.TurnStatus == "inProgress" })
			nodeWait(124, "100ms")
			controllerWait(124, "150ms")

			completed := make(chan error, 1)
			go func() {
				if err := pause(h.ctx, 250*time.Millisecond); err != nil {
					completed <- err
					return
				}
				completed <- h.ctl("rpc", id, "fixture/complete")
			}()
			checkTurn(controllerWait(0, "2s"), "second")
			if err := <-completed; err != nil {
				t.Fatal(err)
			}
			checkTurn(nodeWait(0, "300ms"), "second")
			if err := h.ctl("close", id); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func idleHistory(s status) bool {
	return s.Finished == 1 && s.LastCompleted == "first"
}

func TestWaitAnyFollowup(t *testing.T) {
	h := newRuns(t, "wait-followup")
	t.Setenv("GORACE", "atexit_sleep_ms=1")
	gate := filepath.Join(h.work, "start-second")
	t.Setenv("FIX_WAIT_START", gate)
	active, idle := "p/a-active", "p/z-idle"
	first := h.start(active, "--idle-exit", "10s")
	h.await(active, func(s status) bool { return s.State == "waiting" && idleHistory(s) })
	other := h.start(idle, "--idle-exit", "10s")
	h.await(idle, func(s status) bool { return s.State == "waiting" && idleHistory(s) })

	cursor := filepath.Join(t.TempDir(), "cursor.json")
	wait := func(code int, limit string) string {
		t.Helper()
		return fixtureCLI(t, code, "wait", "--any", "--project", "p", "--since-file", cursor, "--timeout", limit)
	}
	check := func(want waitReport) {
		t.Helper()
		var got waitReport
		out := wait(0, "3s")
		if err := json.Unmarshal([]byte(out), &got); err != nil || got != want {
			t.Fatalf("expected %+v, got %s (%v)", want, out, err)
		}
	}
	if err := h.ctl("say", active, "second round"); err != nil {
		t.Fatal(err)
	}
	check(waitReport{Project: "p", Job: "z-idle", State: "waiting", Outcome: "completed", Finished: 1})
	if out := wait(124, "150ms"); out != "" {
		t.Fatalf("wait --any returned stale completion: %q", out)
	}
	var saved map[string]waitCursor
	if err := readJSON(cursor, &saved); err != nil || len(saved) != 1 || saved[idle].Finished != 1 {
		t.Fatalf("cursor after other job and timeout: %+v, %v", saved, err)
	}

	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.await(active, func(s status) bool { return s.Turn == "second" && s.TurnStatus == "inProgress" })
	if out := wait(124, "150ms"); out != "" {
		t.Fatalf("wait --any returned stale completion: %q", out)
	}
	completed := make(chan error, 1)
	go func() {
		if err := pause(h.ctx, 250*time.Millisecond); err != nil {
			completed <- err
			return
		}
		completed <- h.ctl("rpc", active, "fixture/complete")
	}()
	check(waitReport{Project: "p", Job: "a-active", State: "waiting", Outcome: "completed", Finished: 2})
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if err := readJSON(cursor, &saved); err != nil || saved[active].Finished != 2 || saved[idle].Finished != 1 {
		t.Fatalf("cursor after completion: %+v, %v", saved, err)
	}
	wait(124, "150ms")
	for _, job := range []struct {
		id   string
		done <-chan error
	}{{active, first}, {idle, other}} {
		if err := h.ctl("close", job.id); err != nil {
			t.Fatal(err)
		}
		if err := <-job.done; err != nil {
			t.Fatal(err)
		}
	}
}
