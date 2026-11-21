package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitAnyConsumers(t *testing.T) {
	tmp := t.TempDir()
	root := filepath.Join(tmp, "state")
	t.Setenv("CXRUN_STATE", root)
	a := filepath.Join(tmp, "a.json")
	b := filepath.Join(tmp, "b.json")
	if err := os.WriteFile(b, nil, 0600); err != nil {
		t.Fatal(err)
	}
	registerJob := func(s status, runID string) string {
		t.Helper()
		parent := filepath.Join(root, s.Project)
		dir := filepath.Join(parent, s.Job)
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		if err := register(parent, dir, config{RunID: runID}, s); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	first := status{Project: "p", Job: "work", Session: "s", State: "waiting", RunID: "first"}
	dir := registerJob(first, first.RunID)
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer owner.Close()
	registerJob(status{Project: "p", Job: "other", Session: "other", State: "done"}, "other")
	registerJob(status{Project: "q", Job: "work", Session: "s", State: "stalled"}, "stalled")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- waitAny(ctx, root, "p", "s", a, &out) }()
	select {
	case err := <-done:
		t.Fatalf("returned before a matching completion: %s %v", &out, err)
	case <-time.After(80 * time.Millisecond):
	}
	first.Finished, first.CompletedStatus = 1, "failed"
	if err := writeJSON(filepath.Join(dir, "status.json"), first); err != nil {
		t.Fatal(err)
	}
	check := func(line string, expected waitReport) {
		t.Helper()
		var got waitReport
		if err := json.Unmarshal([]byte(line), &got); err != nil || got != expected || strings.Count(line, "\n") != 1 {
			t.Fatalf("wanted %+v, got %s %v", expected, line, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	failedTurn := waitReport{Project: "p", Job: "work", State: "waiting", Outcome: "failed", Finished: 1, Code: 1}
	check(out.String(), failedTurn)
	registerJob(status{Project: "p", Job: "z", Session: "s", State: "queue_timeout"}, "queued")
	call := func(file, project, session string) string {
		t.Helper()
		var out bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := waitAny(ctx, root, project, session, file, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	check(call(a, "p", "s"), waitReport{Project: "p", Job: "z", State: "queue_timeout", Outcome: "", Finished: 0, Code: 4})
	check(call(b, "p", "s"), failedTurn)
	check(call(b, "q", "s"), waitReport{Project: "q", Job: "work", State: "stalled", Outcome: "", Finished: 0, Code: 3})
	check(fixtureCLI(t, 0, "wait", "--any", "--project", "p", "--session", "other", "--since-file", a),
		waitReport{Project: "p", Job: "other", State: "done", Outcome: "", Finished: 0, Code: 0})
	if line := fixtureCLI(t, 124, "wait", "--any", "--project", "p", "--session", "s", "--since-file", a, "--timeout", "60ms"); line != "" {
		t.Fatal(line)
	}
	first.State = "done"
	if err := writeJSON(filepath.Join(dir, "status.json"), first); err != nil {
		t.Fatal(err)
	}
	// Closing after the (already reported) failed turn carries no new information: no event.
	if line := fixtureCLI(t, 124, "wait", "--any", "--project", "p", "--session", "s", "--since-file", a, "--timeout", "60ms"); line != "" {
		t.Fatal(line)
	}

	first.RunID, first.State, first.CompletedStatus = "replacement", "waiting", "completed"
	if err := writeJSON(filepath.Join(dir, "status.json"), first); err != nil {
		t.Fatal(err)
	}
	check(call(a, "p", "s"), waitReport{Project: "p", Job: "work", State: "waiting", Outcome: "completed", Finished: 1, Code: 0})
}

func TestScaleStatusFromAppServer(t *testing.T) {
	h := newRuns(t, "scale")
	done := h.start("scale", "--idle-exit", "10s")
	s := h.await("scale", func(s status) bool { return s.State == "waiting" && s.Finished == 1 })
	var usage struct{ Total struct{ TotalTokens int } }
	if err := json.Unmarshal(s.Usage, &usage); err != nil || usage.Total.TotalTokens != 42 {
		t.Fatal(string(s.Usage), err)
	}
	if s.CompletedStatus != "failed" || s.PlanStep != "run checks" || s.ChangedFiles != 2 ||
		s.FailedCommand == nil || *s.FailedCommand != (failedCommand{"test fixture", 17}) ||
		!s.RateLimited || s.ResetAt == nil || *s.ResetAt != 4102444800 ||
		s.Primary == nil || s.Primary.UsedPercent != 64 || s.Secondary == nil || s.Secondary.UsedPercent != 0 ||
		s.LastEvent.IsZero() {
		t.Fatalf("unexpected status %+v", s)
	}
	line := fixtureCLI(t, 0, "list", "--fields", "brief")
	var brief map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &brief); err != nil {
		t.Fatal(line, err)
	}
	keys := []string{"project", "job", "session", "status", "turn_id", "completed_turns", "plan_step", "changed_files", "failed_command", "last_event_time", "rate_limit"}
	for _, key := range keys {
		if brief[key] == nil {
			t.Fatalf("missing %s in %s", key, line)
		}
	}
	var rates rateLimitStatus
	if err := json.Unmarshal(brief["rate_limit"], &rates); err != nil || !rates.RateLimited ||
		rates.Primary == nil || rates.Primary.UsedPercent != 64 || len(brief) != len(keys) {
		t.Fatal(line, err)
	}
	progress, err := os.ReadFile(filepath.Join(h.root, "scale", "progress.log"))
	if err != nil || !bytes.Contains(progress, []byte("turn=first source=error retry=true")) ||
		!bytes.Contains(progress, []byte("turn=first source=turn/completed retry=false")) {
		t.Fatal(string(progress), err)
	}
	events, err := os.ReadFile(filepath.Join(h.root, "scale", "events.jsonl"))
	if err != nil || bytes.Count(events, []byte(`"method":"turn/rate_limited"`)) != 2 {
		t.Fatal(string(events), err)
	}
	if err := h.ctl("say", "scale", "second round"); err != nil {
		t.Fatal(err)
	}
	next := h.await("scale", func(s status) bool { return s.Turn == "second" })
	if next.ChangedFiles != 0 || next.PlanStep != "" || next.FailedCommand == nil {
		t.Fatalf("unexpected second turn %+v", next)
	}
	if err := h.ctl("close", "scale"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
