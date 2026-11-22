package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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

	if stdout := cliOutput(t, 12, nil, f.cx, "result", job, "--log"); len(stdout) != 0 {
		t.Fatalf("collection wrote to stdout: %q", stdout)
	}
	if stdout := cliOutput(t, 0, nil, f.cx, "result", job, "--log", "--wait", "--max-bytes", "1"); !bytes.Equal(stdout, payload) {
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
		Job: "codex-log", Project: "p", State: "done", RunID: "fixture",
		Turn: "second", LastCompleted: "second", CompletedStatus: "completed", Finished: 2,
	}
	if err := writeJSON(filepath.Join(dir, "status.json"), s); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "last.md"), []byte("second report\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 0, nil, f.cx, "result", job, "--wait")
	first := "time\tturn/started\tfirst\ntime\tturn/completed\tfirst completed\n"
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
