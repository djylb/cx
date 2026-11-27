package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestBriefRateLimitExpires(t *testing.T) {
	clock := timeshim.NewFake(time.Unix(100, 0))
	reset := clock.Now().Unix() + 10
	s := status{State: "waiting", Thread: "fixture", CompletedRateLimited: true}
	s.update(rateLimitSnapshot{Primary: &rateLimitReading{UsedPercent: 100, ResetsAt: &reset}})
	check := func(phase string, limited bool, deadline *int64) {
		t.Helper()
		raw, err := json.Marshal(briefStatus(t.Context(), "", s, clock.Now().Unix()))
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Status string
			Phase  string
			Rate   struct {
				Reset   *int64 `json:"reset_at"`
				Limited bool   `json:"rate_limited"`
			} `json:"rate_limit"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status != s.State || got.Phase != phase || got.Rate.Limited != limited ||
			(deadline == nil && got.Rate.Reset != nil) ||
			(deadline != nil && (got.Rate.Reset == nil || *got.Rate.Reset != *deadline)) {
			t.Fatalf("unexpected brief: %s", raw)
		}
	}
	check("rate_limited", true, &reset)
	s.CompletedRateLimited = false
	check("rate_limited", true, &reset)
	clock.Advance(10 * time.Second)
	check("idle", false, nil)
	s.CompletedRateLimited = true
	check("rate_limited", true, nil)
}

func TestSayReportsMissingThread(t *testing.T) {
	f := newControllerFixture(t)
	dir, err := jobDir(os.Getenv("CXRUN_STATE"), "p/ended")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, status{Project: "p", Job: "ended", RunID: "fixture", State: "done", Engine: "codex"}); err != nil {
		t.Fatal(err)
	}
	_, diagnostic := cliStreams(t, 1, nil, f.cx, "say", "local:p/ended", "continue")
	if !strings.Contains(diagnostic, "no saved thread") {
		t.Fatalf("unexpected diagnostic: %s", diagnostic)
	}
}

func TestControllerListBrief(t *testing.T) {
	f := newControllerFixture(t)
	dir, err := jobDir(os.Getenv("CXRUN_STATE"), "p/ended")
	if err != nil {
		t.Fatal(err)
	}
	s := status{Project: "p", Job: "ended", Session: "fixture", RunID: "fixture", State: "done", Engine: "codex", Thread: "saved"}
	if err := writeFixtureStatus(dir, s); err != nil {
		t.Fatal(err)
	}
	out := cliOutput(t, 0, nil, f.cx, "ls", "--brief", "--json")
	var row struct{ Status, Phase string }
	if err := json.Unmarshal(out, &row); err != nil || row.Status != "done" || row.Phase != "ended" {
		t.Fatalf("unexpected listing: %s (%v)", out, err)
	}
	out = cliOutput(t, 0, nil, f.cx, "ls", "--brief")
	if !strings.Contains(string(out), "PHASE") || !strings.Contains(string(out), "ended") {
		t.Fatalf("unexpected table: %s", out)
	}
}

func TestControllerSayResumesEndedThread(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	work := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(exe, filepath.Join(f.home, "codex")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FIXMODE", "sealed-patches")
	t.Setenv("CXRUN_PREAMBLE_FILE", "")
	prompt := filepath.Join(f.home, "prompt")
	if err := os.WriteFile(prompt, []byte("task"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	h := &testRuns{t: t, ctx: ctx, root: root, work: work, prompt: prompt, runs: map[string]chan error{}}
	const id = "p/resumable"
	done := make(chan error, 1)
	go func() { done <- run(ctx, h.args(id)) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(fixtureLimit):
			t.Error("fixture runner did not stop")
		}
	})
	initial := h.await(id, func(s status) bool { return s.State == "waiting" && s.Finished == 1 })
	cliOutput(t, 0, nil, f.cx, "close", "local:"+id)
	h.await(id, func(s status) bool { return s.State == "done" })
	out := cliOutput(t, 0, nil, f.cx, "say", "local:"+id, "second round")
	t.Cleanup(func() {
		cliOutput(t, 0, nil, f.cx, "close", "local:"+id)
	})
	if !msgName.MatchString(strings.TrimSpace(string(out))) {
		t.Fatalf("unexpected say receipt: %s", out)
	}
	final := h.await(id, func(s status) bool { return s.Finished == 2 })
	if final.RunID != initial.RunID || final.Thread != initial.Thread ||
		final.Restarts != initial.Restarts+1 || final.LastCompleted != "second" {
		t.Fatalf("unexpected resumed state: %+v", final)
	}
}
