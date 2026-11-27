package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestResultWaitPinsTurn(t *testing.T) {
	for _, tc := range []struct {
		name, state, turn, want string
	}{
		{name: "active", state: "running", want: "second report"},
		{name: "explicit-future", state: "waiting", turn: "3", want: "third report"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newControllerFixture(t)
			const id = "p/result-wait"
			s := status{Job: "result-wait", Project: "p", RunID: "fixture", Engine: "codex", State: "waiting"}
			r := waitFixture(t, os.Getenv("CXRUN_STATE"), id, s)
			publish := func(n int64, turn, report string) {
				t.Helper()
				r.s.State, r.s.Finished = "waiting", n
				r.s.Turn, r.s.LastCompleted = turn, turn
				r.s.TurnStatus, r.s.CompletedStatus = "completed", "completed"
				if err := r.publishTurnReport(turn, []byte(report)); err != nil {
					t.Fatal(err)
				}
				if err := r.save(); err != nil {
					t.Fatal(err)
				}
				if _, err := sealTurn(t.Context(), r.dir, r.s, n); err != nil {
					t.Fatal(err)
				}
			}
			publish(1, "first", "first report")
			if tc.state == "running" {
				r.s.State, r.s.Turn, r.s.TurnStatus = tc.state, "second", "inProgress"
				if err := r.save(); err != nil {
					t.Fatal(err)
				}
				if out := cliOutput(t, 0, nil, f.cx, "result", "--wait", "--full", "--turn", "1", "local:"+id); !bytes.Contains(out, []byte("\nfirst report\n")) {
					t.Fatalf("explicit historical result during the active turn: %q", out)
				}
			}
			// Run the actual node wire protocol through a disposable SSH carrier.
			// Hold the observation until the job has advanced past the target.
			ready := filepath.Join(f.home, "continue")
			requests := filepath.Join(f.home, "requests")
			setupPrefetchSSH(t, f, `
case "$request" in
  *'"observe-result"'*)
    while [ ! -f `+shellQuote(ready)+` ]; do sleep .02; done
    ;;
esac
printf '%s\n' "$request" | `+shellQuote(f.cx)+` node wire
`)
			ctx, cancel := fixtureContext(t)
			defer cancel()
			args := []string{"result", "--wait", "--full", "remote:" + id}
			if tc.turn != "" {
				args = append(args, "--turn", tc.turn)
			}
			cmd := exec.CommandContext(ctx, f.cx, args...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var waitErr error
			go func() {
				waitErr = cmd.Wait()
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				_ = os.WriteFile(ready, nil, 0600)
				<-done
			})
			for {
				b, err := os.ReadFile(requests)
				if err == nil && bytes.Contains(b, []byte(`"observe-result"`)) {
					break
				}
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				select {
				case <-done:
					t.Fatalf("result exited before completion: %v, stdout=%q stderr=%q", waitErr, &stdout, &stderr)
				default:
				}
				if err := pause(ctx, 10*time.Millisecond); err != nil {
					t.Fatal(err)
				}
			}
			publish(2, "second", "second report")
			publish(3, "third", "third report")
			publish(4, "fourth", "fourth report")
			if err := os.WriteFile(ready, nil, 0600); err != nil {
				t.Fatal(err)
			}
			<-done
			if waitErr != nil || !strings.Contains(stdout.String(), "\n"+tc.want+"\n") || stderr.Len() != 0 {
				t.Fatalf("result = %v, stdout=%q stderr=%q", waitErr, &stdout, &stderr)
			}
		})
	}
}

func TestResultWaitTimeoutAndUnavailableDiagnostics(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	s := status{Job: "busy", Project: "p", RunID: "fixture", Engine: "codex", State: "running",
		Finished: 1, Turn: "second", TurnStatus: "inProgress", LastCompleted: "first"}
	r := waitFixture(t, root, "p/busy", s)
	if out, diag := cliStreams(t, 124, nil, f.cx, "result", "--wait", "--full", "p/busy", "--timeout", "250ms"); len(out) != 0 || !strings.Contains(diag, "deadline exceeded") {
		t.Fatalf("timeout: stdout=%q stderr=%q", out, diag)
	}
	t.Setenv("CXRUN_MONITOR_TIMEOUT", "250ms")
	cliOutput(t, 124, nil, f.cx, "result", "--wait", "--turn", "3", "p/busy")
	t.Setenv("CXRUN_MONITOR_TIMEOUT", "")
	if out, diag := cliStreams(t, 44, nil, f.cx, "result", "--full", "p/busy"); len(out) != 0 || !strings.Contains(diag, "turn 2 not sealed yet") {
		t.Fatalf("unsealed current turn: stdout=%q stderr=%q", out, diag)
	}
	r.s.State, r.s.Turn, r.s.TurnStatus = "waiting", "first", "completed"
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	if out, diag := cliStreams(t, 44, nil, f.cx, "result", "--full", "p/busy"); len(out) != 0 || !strings.Contains(diag, "turn 1 report missing") {
		t.Fatalf("missing report: stdout=%q stderr=%q", out, diag)
	}
	r.s.State = "done"
	if err := r.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	if out, diag := cliStreams(t, 44, nil, f.cx, "result", "--wait", "--turn", "3", "p/busy"); len(out) != 0 || !strings.Contains(diag, "turn 3") || !strings.Contains(diag, "done") {
		t.Fatalf("future turn after terminal state: stdout=%q stderr=%q", out, diag)
	}
	if out, diag := cliStreams(t, 44, nil, f.cx, "node", "result-download", "p/busy", s.RunID, "1", "0"); len(out) != 0 || !strings.Contains(diag, "turn 1 report missing") {
		t.Fatalf("node missing report: stdout=%q stderr=%q", out, diag)
	}
}

func TestResultWaitBoundsSealing(t *testing.T) {
	f := newControllerFixture(t)
	s := status{Job: "sealing", Project: "p", RunID: "fixture", Engine: "codex", State: "waiting",
		Finished: 1, Turn: "first", LastCompleted: "first", TurnStatus: "completed", CompletedStatus: "completed"}
	r := waitFixture(t, os.Getenv("CXRUN_STATE"), "p/sealing", s)
	if err := r.publishTurnReport("first", []byte("first report")); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(r.dir, "result")
	if err := os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	lock, err := lockPath(filepath.Join(base, ".lock"), os.O_CREATE)
	if err != nil || lock == nil {
		t.Fatalf("hold result lock: %v, %v", lock, err)
	}
	defer closeBestEffort(lock)
	dest, err := resultDest("local", "p/sealing", s.RunID, "1")
	if err != nil {
		t.Fatal(err)
	}
	expireResultOperation(t, dest+".part", 44, "--wait", "--full", "local:p/sealing")
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "result", "--wait", "--full", "p/sealing", "--timeout", "2m"); !bytes.Contains(out, []byte("\nfirst report\n")) {
		t.Fatalf("idle result did not seal the latest completion: %q", out)
	}
}

func TestResultSealsInPlaceTurnBeforeNodeContinuation(t *testing.T) {
	f := newControllerFixture(t)
	h := newRuns(t, "question")
	const id = "p/in-place"
	done := h.start(id, "--idle-exit", "2m")
	h.await(id, func(s status) bool { return s.State == "waiting" && s.Finished == 1 })
	// Send directly to the node to exercise the runner without controller-side sealing.
	cliOutput(t, 0, nil, f.cx, "node", "say", "--job", id, "second round")
	h.await(id, func(s status) bool { return s.State == "waiting" && s.Finished == 2 })
	out := cliOutput(t, 0, nil, f.cx, "result", "local:"+id, "--wait", "--full", "--turn", "1")
	if !bytes.Contains(out, []byte("STATUS: QUESTION\nNext?")) {
		t.Fatalf("wrong historical result: %q", out)
	}
	if err := h.ctl("close", id); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResultDownloadDeadline(t *testing.T) {
	f := newControllerFixture(t)
	entered := filepath.Join(f.home, "download-started")
	setupPrefetchSSH(t, f, `
case "$request" in
  *'"status"'*)
    printf '%s\n' '{"run_id":"run","completed_turns":1,"state":"done","last_completed_turn_id":"first","last_completed_status":"completed"}'
    ;;
  *'"result-download"'*)
    touch `+shellQuote(entered)+`
    exec sleep 60
    ;;
  *) exit 2 ;;
esac
`)
	expireResultOperation(t, entered, 44, "remote:p/result-deadline")
}

func TestResultStatusDeadlineWithoutWait(t *testing.T) {
	f := newControllerFixture(t)
	entered := filepath.Join(f.home, "status-started")
	setupPrefetchSSH(t, f, `
case "$request" in
  *'"status"'*)
    touch `+shellQuote(entered)+`
    exec sleep 60
    ;;
  *) exit 2 ;;
esac
`)
	expireResultOperation(t, entered, 124, "remote:p/result-status")
}

func TestResultStatusIdleTimeout(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_TRANSFER_IDLE", "120")
	setupPrefetchSSH(t, f, `exec sleep 60`)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	start := time.Now()
	clock := timeshim.NewFake(start)
	done := make(chan error, 1)
	go func() {
		done <- dispatch(clockContext(ctx, clock), []string{"result", "remote:p/idle", "--timeout", "10m"})
	}()
	if err := clock.WaitDeadline(ctx, start.Add(120*time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(120 * time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, ctlExit(9)) || !strings.Contains(err.Error(), "look up result status:") || !strings.Contains(err.Error(), "no progress") {
			t.Fatalf("status idle timeout = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("status lookup ignored the idle timeout", ctx.Err())
	}
}

func expireResultOperation(t *testing.T, started string, want ctlExit, args ...string) {
	t.Helper()
	ctx, cancel := fixtureContext(t)
	defer cancel()
	const limit = time.Minute
	clock := timeshim.NewFake(time.Now())
	done := make(chan error, 1)
	startCtx, stopWaitingForStart := context.WithCancel(ctx)
	defer stopWaitingForStart()
	go func() {
		defer stopWaitingForStart()
		args = append([]string{"result", "--timeout", limit.String()}, args...)
		done <- dispatch(clockContext(ctx, clock), args)
	}()
	if err := awaitFixtureFile(startCtx, started); err != nil {
		select {
		case resultErr := <-done:
			t.Fatalf("result returned before starting the operation: %v", resultErr)
		default:
			t.Fatal(err)
		}
	}
	clock.Advance(limit)
	select {
	case err := <-done:
		if !errors.Is(err, want) || want == 44 && !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("result timeout = %v, want exit %d", err, want)
		}
	case <-ctx.Done():
		t.Fatal("operation ignored the result deadline", ctx.Err())
	}
}

func TestResultCollectionLimit(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "default"
		if explicit {
			name = "override"
		}
		t.Run(name, func(t *testing.T) {
			f := newControllerFixture(t)
			t.Setenv("CXRUN_MONITOR_TIMEOUT", "24h")
			t.Setenv("CX_TRANSFER_IDLE", "86400")
			entered := filepath.Join(f.home, "download-started")
			setupPrefetchSSH(t, f, `
case "$request" in
  *'"status"'*)
    printf '%s\n' '{"run_id":"run","completed_turns":1,"state":"done","last_completed_turn_id":"first"}'
    ;;
  *'"result-download"'*)
    touch `+shellQuote(entered)+`
    exec sleep 60
    ;;
  *) exit 2 ;;
esac
`)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			start := time.Now()
			clock := timeshim.NewFake(start)
			args := []string{"result", "remote:p/result-limit"}
			limit := collectLimit
			if explicit {
				limit = 2 * collectLimit
				args = append(args, "--timeout", limit.String())
			}
			done := make(chan error, 1)
			runCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
			finished := make(chan struct{})
			defer func() {
				stop()
				<-finished
			}()
			startCtx, stopWaiting := context.WithCancel(ctx)
			defer stopWaiting()
			go func() {
				defer close(finished)
				defer stopWaiting()
				done <- controller(clockContext(runCtx, clock), args)
			}()
			if err := awaitFixtureFile(startCtx, entered); err != nil {
				select {
				case resultErr := <-done:
					t.Fatalf("result returned before starting download: %v", resultErr)
				default:
					t.Fatal(err)
				}
			}
			deadlines := clock.Deadlines()
			if !slices.Contains(deadlines, start.Add(limit)) {
				t.Errorf("deadlines = %v, want collection deadline %v", deadlines, start.Add(limit))
			}
			if explicit {
				requests, err := os.ReadFile(filepath.Join(f.home, "requests"))
				if err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(string(requests)), "\n")
				var request invocation
				if err := json.Unmarshal([]byte(lines[len(lines)-1]), &request); err != nil {
					t.Fatal(err)
				}
				if request.Budget <= collectLimit {
					t.Errorf("wire budget = %v, want more than default %v", request.Budget, collectLimit)
				}
			}
			clock.Advance(limit)
			select {
			case err := <-done:
				if !errors.Is(err, ctlExit(44)) || !strings.Contains(err.Error(), "deadline exceeded") {
					t.Fatalf("collection timeout = %v, want exit 44 with deadline reason", err)
				}
			case <-ctx.Done():
				t.Fatal("collection ignored its deadline", ctx.Err())
			}
		})
	}
}
