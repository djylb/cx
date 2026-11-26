package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestAdoptConcurrentInboxes(t *testing.T) {
	for _, trigger := range []string{"running poll", "terminal publication", "owner released"} {
		t.Run(trigger, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "job")
			c := config{RunID: "run", Engine: "codex", Session: "original"}
			s := status{RunID: c.RunID, Job: "job", Engine: c.Engine, Session: c.Session, State: "running", Finished: 1}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			owner, registerErr := registerJob(ctx, root, dir, c, s, (*stateWriter).sync)
			if registerErr != nil {
				t.Fatal(registerErr)
			}
			defer closeBestEffort(owner)
			r := runner{dir: dir, root: root, c: c, s: s, ctx: ctx}
			startFixtureWriter(t, &r)
			// Each waiter installs its own fake retry timer after publishing
			// its inbox record. Let both issue their requests before consuming.
			queue := func(name string) func() {
				t.Helper()
				base := time.Now()
				clock := timeshim.NewFake(base)
				done := make(chan error, 1)
				go func() { done <- adoptJob(clockContext(ctx, clock), root, dir, name) }()
				if err := clock.WaitDeadline(ctx, base.Add(50*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				return func() {
					t.Helper()
					clock.Advance(50 * time.Millisecond)
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
			}
			first := queue("first")
			second := queue("second")
			switch trigger {
			case "running poll":
				if _, closing, err := r.takeMarker("close"); err != nil || closing {
					t.Fatalf("control poll: closing %v, %v", closing, err)
				}
				t.Cleanup(func() { closeBestEffort(r.inbox.tail) })
			case "terminal publication":
				r.s.State = "done"
				if err := r.saveTerminal(); err != nil {
					t.Fatal(err)
				}
			case "owner released":
				if err := r.stateLog.Close(); err != nil {
					t.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			first()
			second()
			in, err := readInboxState(dir)
			if err != nil || len(in.acks) != 2 || in.acks[1].Result != "delivered" || in.acks[2].Result != "delivered" {
				t.Fatalf("concurrent operation receipts: %+v (%v)", in, err)
			}
			saved, _, err := loadJobStatus(dir)
			final := trigger == "terminal publication"
			if err != nil || saved.Session != "second" || saved.RunID != c.RunID || saved.Finished != s.Finished || saved.final != final {
				t.Fatalf("latest status after concurrent adoption: %+v (%v)", saved, err)
			}
		})
	}
}

func TestAdoptTerminalCLI(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	parent := filepath.Join(root, "p")
	dir := filepath.Join(parent, "job")
	c := config{RunID: "run", Engine: "cmd", Project: "p", Session: "original"}
	s := status{RunID: c.RunID, Job: "job", Engine: c.Engine, Project: c.Project, Session: c.Session, State: "done",
		Finished: 1, LastCompleted: "1", CompletedStatus: "completed"}
	if err := register(parent, dir, c, s); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CX_SESSION", "")
	if _, diag := cliStreams(t, 2, nil, f.cx, "adopt", "local:p/job"); !strings.Contains(diag, "set CX_SESSION or use --session NAME") {
		t.Fatalf("unknown session diagnostic: %q", diag)
	}
	cliOutput(t, 0, nil, f.cx, "adopt", "local:p/job", "--session", "receiver")
	t.Setenv("CX_SESSION", "manager")
	cliOutput(t, 0, nil, f.cx, "adopt", "local:p/job")
	got, _, err := loadJobStatus(dir)
	if err != nil || !got.final || got.State != s.State || got.Session != "manager" || got.Finished != s.Finished || got.RunID != s.RunID ||
		got.LastCompleted != s.LastCompleted || got.CompletedStatus != s.CompletedStatus {
		t.Fatalf("terminal state changed unexpectedly: %+v (%v)", got, err)
	}
	for _, op := range []string{"ls", "overview"} {
		included := cliOutput(t, 0, nil, f.cx, op, "--json", "--session", "manager")
		excluded := cliOutput(t, 0, nil, f.cx, op, "--json", "--session", "original")
		if !json.Valid(included) || !strings.Contains(string(included), `"job":"job"`) || strings.Contains(string(excluded), `"job":"job"`) {
			t.Fatalf("%s session moved incorrectly: %s / %s", op, included, excluded)
		}
	}
}
