package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRefreshQueuesAndRebasesCheckout(t *testing.T) {
	f := newControllerFixture(t)
	ctx := snapTestCtx(t)
	source := filepath.Join(t.TempDir(), "source")
	snapWriteTest(t, filepath.Join(source, "upstream"), "old\n")
	snapWriteTest(t, filepath.Join(source, "conflict"), "old\n")
	git := func(args ...string) {
		t.Helper()
		cliOutput(t, 0, nil, "git", append([]string{"-C", source}, args...)...)
	}
	git("init", "-q")
	git("add", "--all")
	git("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "baseline")

	initial, manifest, err := Freeze(ctx, source, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const id = "p/refresh"
	s := status{Job: "refresh", Project: "p", RunID: "fixture", Engine: "codex", State: "running",
		Turn: "first", TurnStatus: "inProgress"}
	r := waitFixture(t, os.Getenv("CXRUN_STATE"), id, s)
	r.log, err = os.Create(filepath.Join(r.dir, "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeBestEffort(r.log) })
	work := filepath.Join(os.Getenv("CXRUN_STATE"), ".work", id)
	prep, err := Materialize(ctx, work, manifest, "")
	if err != nil {
		t.Fatal(err)
	}
	r.c.Dir = prep.Source
	if err = writeJSON(filepath.Join(work, "request.json"), jobSubmission{Snapshot: initial}); err != nil {
		t.Fatal(err)
	}
	origin, err := originFile("local", id, s.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Snapshot: initial, Source: source}); err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(prep.Source, "job"), "job change\n")
	snapWriteTest(t, filepath.Join(source, "upstream"), "new baseline\n")
	cliOutput(t, 0, nil, f.cx, "refresh", "local:"+id)
	if base, readErr := readJobBaseline(work); readErr != nil || base.Snapshot != "" {
		t.Fatalf("baseline changed during active turn: %+v, %v", base, readErr)
	}
	if err = r.finish(turn{ID: "first", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	// Seed an apply receipt for the old baseline without applying the job
	// change to the source snapshot already queued above.
	project, err := outputProject("p")
	if err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(project, "apply-log.jsonl"), "{\"job\":\"p/refresh\",\"host\":\"local\",\"run_id\":\"fixture\",\"repo\":\"source\",\"turn\":1}\n")

	if question, refreshErr := r.refreshBeforeTurn(); question || refreshErr != nil {
		report, _ := readTurnReport(ctx, r.dir, r.s.LastCompleted)
		t.Logf("report: %s", report)
		t.Fatalf("clean refresh: question=%v, error=%v", question, refreshErr)
	}
	data, err := os.ReadFile(filepath.Join(prep.Source, "upstream"))
	if err != nil || string(data) != "new baseline\n" {
		t.Fatalf("upstream = %q, %v", data, err)
	}
	data, err = os.ReadFile(filepath.Join(prep.Source, "job"))
	if err != nil || string(data) != "job change\n" {
		t.Fatalf("job = %q, %v", data, err)
	}
	base, err := readJobBaseline(work)
	if err != nil || base.Snapshot == initial || base.FromTurn != 2 {
		t.Fatalf("baseline = %+v, %v", base, err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "ls", "--all"); !strings.Contains(string(out), string(base.Snapshot)[:12]) || strings.Contains(string(out), "source=1") {
		t.Fatalf("listing = %s", out)
	}
	if err = r.finish(turn{ID: "second", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if patch := string(cliOutput(t, 0, nil, f.cx, "patch", "--full", "local:"+id)); strings.Contains(patch, "+new baseline") || !strings.Contains(patch, "+job change") || !strings.Contains(patch, "# since turn 2\n") {
		t.Fatalf("patch = %s", patch)
	}

	if patch := string(cliOutput(t, 0, nil, f.cx, "patch", "--full", "--turn", "1", "--since-turn", "1", "local:"+id)); !strings.Contains(patch, "+job change") {
		t.Fatalf("historical patch = %s", patch)
	}
	cliOutput(t, 0, nil, f.cx, "apply", "local:"+id)
	data, err = os.ReadFile(filepath.Join(source, "job"))
	if err != nil || string(data) != "job change\n" {
		t.Fatalf("applied job = %q, %v", data, err)
	}
	snapWriteTest(t, filepath.Join(prep.Source, "later"), "later job change\n")
	if err = r.finish(turn{ID: "third", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if patch := string(cliOutput(t, 0, nil, f.cx, "patch", "--full", "local:"+id)); !strings.Contains(patch, "# since turn 3\n") || !strings.Contains(patch, "+later job change") || strings.Contains(patch, "+job change") {
		t.Fatalf("same-baseline incremental patch = %s", patch)
	}
	if patch := string(cliOutput(t, 0, nil, f.cx, "patch", "--full", "--since-turn", "1", "local:"+id)); !strings.Contains(patch, "# since turn 2\n") || !strings.Contains(patch, "+job change") || !strings.Contains(patch, "+later job change") {
		t.Fatalf("cumulative patch on refreshed baseline = %s", patch)
	}
	// Keep this applied change out of the next source snapshot so the
	// conflict case still includes a carry-forward change.
	if err = os.Remove(filepath.Join(source, "job")); err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(prep.Source, "conflict"), "job version\n")
	snapWriteTest(t, filepath.Join(source, "conflict"), "upstream version\n")
	cliOutput(t, 0, nil, f.cx, "refresh", "local:"+id)
	if question, refreshErr := r.refreshBeforeTurn(); !question || refreshErr != nil {
		t.Fatalf("conflicting refresh: question=%v, error=%v", question, refreshErr)
	}
	report, err := readTurnReport(ctx, r.dir, r.s.LastCompleted)
	if err != nil || r.s.State != "waiting_input" || !strings.HasPrefix(string(report), "STATUS: QUESTION") || !strings.Contains(string(report), "source/conflict") {
		t.Fatalf("status = %s, report = %s, error = %v", r.s.State, report, err)
	}
	data, err = os.ReadFile(filepath.Join(prep.Source, "conflict"))
	if err != nil || !strings.Contains(string(data), "<<<<<<<") {
		t.Fatalf("conflict markers = %q, %v", data, err)
	}
	conflicted, err := readJobBaseline(work)
	if err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(source, "upstream"), "later upstream\n")
	cliOutput(t, 0, nil, f.cx, "refresh", "local:"+id, source)
	if question, refreshErr := r.refreshBeforeTurn(); !question || refreshErr != nil {
		t.Fatalf("unresolved refresh: question=%v, error=%v", question, refreshErr)
	}
	report, err = readTurnReport(ctx, r.dir, r.s.LastCompleted)
	if err != nil || !strings.Contains(string(report), "Baseline refresh failed") {
		t.Fatalf("failed refresh report = %q, %v", report, err)
	}
	after, err := readJobBaseline(work)
	if err != nil || after.Snapshot != conflicted.Snapshot || after.FromTurn != conflicted.FromTurn {
		t.Fatalf("failed refresh changed baseline: %+v, %v", after, err)
	}
	unchanged, err := os.ReadFile(filepath.Join(prep.Source, "conflict"))
	if err != nil || string(unchanged) != string(data) {
		t.Fatalf("failed refresh changed conflict markers: %q, %v", unchanged, err)
	}
}
