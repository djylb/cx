package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandResultDetails(t *testing.T) {
	f := newControllerFixture(t)
	payload := bytes.Repeat([]byte("first\r\n\x00\xfflast"), 900)
	work := filepath.Join(f.home, "work")
	if err := os.MkdirAll(filepath.Join(work, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "artifacts", "payload.bin"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	const job = "p/binary"
	cliOutput(t, 37, nil, f.cx, "exec", "local", "--tag", "binary", "--cwd", work, "--wait", "--",
		"sh", "-c", "cat artifacts/payload.bin; exit 37")

	if stdout := cliOutput(t, 0, nil, f.cx, "result", job, "--log", "--max-bytes", "1"); !bytes.Equal(stdout, payload) {
		t.Fatalf("result log changed: got %d bytes, want %d", len(stdout), len(payload))
	}
	var result resultOutput
	data := cliOutput(t, 0, nil, f.cx, "result", job, "--json", "--max-bytes", "1")
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err, string(data))
	}
	if result.Job != job || result.Engine != "cmd" || result.Status == nil || *result.Status != "failed" ||
		result.ExitCode == nil || *result.ExitCode != 37 || result.Turn != 1 || result.LogPath == nil ||
		filepath.Base(*result.LogPath) != "output.log" || filepath.Base(result.ReportPath) != "last.md" ||
		filepath.Base(result.PatchManifest) != "result.json" || len(result.Artifacts) != 1 {
		t.Fatalf("unexpected result metadata: %s", data)
	}
	for _, path := range []string{*result.LogPath, result.Artifacts[0].Path} {
		if actual, err := os.ReadFile(path); err != nil || !bytes.Equal(actual, payload) {
			t.Fatalf("%s: %d bytes, %v", path, len(actual), err)
		}
	}
	cliOutput(t, 2, nil, f.cx, "result", job, "--json", "--log")
	cliOutput(t, 2, nil, f.cx, "result", job, "--full", "--log")
}

func TestCodexResultDetailsFromSealedTurn(t *testing.T) {
	f := newControllerFixture(t)
	const job = "p/codex-log"
	dir := filepath.Join(os.Getenv("CXRUN_STATE"), filepath.FromSlash(job))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s := status{
		Job: "codex-log", Project: "p", State: "done", RunID: "fixture", Engine: "codex",
		Turn: "second", LastCompleted: "second", CompletedStatus: "completed", Finished: 2,
	}
	if err := writeFixtureStatus(dir, s, "second report\n"); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 0, nil, f.cx, "result", job, "--wait")
	first := "1\ttime\tturn/started\tfirst\n2\ttime\tturn/completed\tfirst completed\n"
	second := "3\ttime\tturn/start\treply → second\r\n4\ttime\tturn/completed\tsecond completed\r\n"
	trailing := "5\ttime\tdone\tagent exited\n"
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), []byte(first+second+trailing), 0600); err != nil {
		t.Fatal(err)
	}
	if data := cliOutput(t, 0, nil, f.cx, "result", job, "--log", "--wait"); string(data) != second {
		t.Fatalf("wrong turn bytes: %q", data)
	}
	data := cliOutput(t, 0, nil, f.cx, "result", job, "--json")
	var result resultOutput
	if err := json.Unmarshal(data, &result); err != nil || result.Engine != "codex" || result.Status == nil ||
		*result.Status != "done" || result.Turn != 2 || result.ExitCode != nil || result.LogPath == nil ||
		filepath.Base(*result.LogPath) != "progress.log" || result.Artifacts == nil {
		t.Fatalf("unexpected result metadata: %s (%v)", data, err)
	}
}

func TestResultDetailsKeepsCompletedLogWhenJobAdvances(t *testing.T) {
	s := status{Job: "log", State: "waiting", RunID: "run", Engine: "codex",
		Turn: "first", LastCompleted: "first", Finished: 1, TurnStatus: "completed", CompletedStatus: "completed"}
	r := waitFixture(t, t.TempDir(), "log", s)
	if err := r.publishTurnReport("first", []byte("first report")); err != nil {
		t.Fatal(err)
	}
	folder, err := sealTurn(t.Context(), r.dir, s, 1)
	if err != nil {
		t.Fatal(err)
	}
	first := "1\ttime\tturn/started\tfirst\n2\ttime\tturn/completed\tfirst completed\n"
	log := first + "3\ttime\tturn/started\tsecond\n"
	if err = os.WriteFile(filepath.Join(r.dir, "progress.log"), []byte(log), 0600); err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	progress := waitWriterFunc(func(p []byte) (int, error) {
		if r.s.Turn != "second" {
			r.s.State, r.s.Turn, r.s.TurnStatus = "running", "second", "inProgress"
			return len(p), r.save()
		}
		return len(p), nil
	})
	if err = stageResultDetails(t.Context(), r.dir, folder, stage, s, progress); err != nil {
		t.Fatal(err)
	}
	var m turnResult
	if err = readJSON(filepath.Join(stage, "result.json"), &m); err != nil || m.ResultDetails == nil || m.Status != nil || m.Log != "progress.log" {
		t.Fatalf("result details: %+v, %v", m.ResultDetails, err)
	}
	if b, err := os.ReadFile(filepath.Join(stage, m.Log)); err != nil || string(b) != first {
		t.Fatalf("archived log: %q, %v", b, err)
	}
}

func TestResultOutputClarity(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	const job = "p/report"
	dir := filepath.Join(os.Getenv("CXRUN_STATE"), filepath.FromSlash(job))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := status{Job: "report", Project: "p", RunID: "fixture", State: "done",
		Turn: "turn-id", LastCompleted: "turn-id", Finished: 1, CompletedStatus: "completed"}
	const report = "STATUS: DONE\nSummary.\n\n## Details\nExpanded report.\n"
	if err := writeFixtureStatus(dir, state, report); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}

	dest, err := resultDest("local", job, state.RunID, "1")
	if err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(dest, "last.md")
	out, diag := cliStreams(t, 0, nil, f.cx, "result", job, "--path")
	if string(out) != reportPath+"\n" || diag != "" {
		t.Fatalf("uncached report path: stdout=%q stderr=%q", out, diag)
	}
	out, diag = cliStreams(t, 0, nil, f.cx, "result", job, "--timeout", "1m")
	if string(out) != reportPath+"\nSTATUS: DONE\nSummary.\n" ||
		diag != "cx: summary shown; full report: "+reportPath+" (use --full)\n" {
		t.Fatalf("summary: stdout=%q stderr=%q", out, diag)
	}
	out, diag = cliStreams(t, 0, nil, f.cx, "result", job, "--full")
	if !bytes.Contains(out, []byte(report)) || strings.Contains(diag, "summary shown") {
		t.Fatalf("full report: stdout=%q stderr=%q", out, diag)
	}
	out, diag = cliStreams(t, 0, nil, f.cx, "result", job, "--turn", "1", "--path")
	if _, err := os.Stat(resultVariant(dest, true)); !os.IsNotExist(err) {
		t.Fatalf("--path created a detailed cache: %v", err)
	}
	var result resultOutput
	data := cliOutput(t, 0, nil, f.cx, "result", job, "--json")
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err, string(data))
	}
	if string(out) != reportPath+"\n" || !filepath.IsAbs(result.ReportPath) || diag != "" {
		t.Fatalf("report path: stdout=%q stderr=%q JSON=%s", out, diag, data)
	}
	if b, err := os.ReadFile(result.ReportPath); err != nil || string(b) != report {
		t.Fatalf("report at %s: %q, %v", result.ReportPath, b, err)
	}
	if err := os.Rename(dest, dest+".saved"); err != nil {
		t.Fatal(err)
	}
	out, diag = cliStreams(t, 0, nil, f.cx, "result", job, "--path")
	if string(out) != result.ReportPath+"\n" || diag != "" {
		t.Fatalf("cached detailed report path: stdout=%q stderr=%q", out, diag)
	}
	for _, flag := range []string{"--full", "--json", "--log"} {
		cliOutput(t, 2, nil, f.cx, "result", job, "--path", flag)
	}
}
