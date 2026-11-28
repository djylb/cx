package main

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestClaudeAndCodexReadPaths(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "app")
	r := runner{root: root, c: config{Dir: work}}
	for _, call := range []struct{ tool, input string }{
		{"Read", `{"file_path":"docs/start.md"}`},
		{"Grep", `{"path":"docs","pattern":"something"}`},
		{"Glob", `{"path":"src","pattern":"**/*"}`},
		{"NotebookRead", `{"notebook_path":"lab.ipynb"}`},
		{"Bash", `{"command":"cat notes.md"}`},
		{"Edit", `{"file_path":"omitted"}`},
		{"Read", `{"file_path":17}`},
		{"Read", `{"file_path":"../external"}`},
	} {
		read := claudeReadPaths(call.tool, json.RawMessage(call.input))
		r.recordReadPaths("one", work, read.paths, read.source)
	}
	exit := 0
	for _, i := range []item{
		{Type: "file_read", Path: filepath.Join(work, "docs", "start.md")},
		{Type: "command_execution", ExitCode: &exit, Command: `/bin/bash -lc 'sed -n "1,5p" "notes with spaces.md" docs/start.md'`},
		{Type: "commandExecution", ExitCode: &exit, Command: `rg --glob '*.go' -e x -- src/a.go src/b.go`, CommandActions: []commandReadAction{{Type: "read", Path: "src/a.go"}}},
		{Type: "commandExecution", ExitCode: &exit, Command: `cat "$secret" | tail`},
		{Type: "commandExecution", ExitCode: &exit, Command: `head -n 2 -- ./other`},
		{Type: "commandExecution", ExitCode: &exit, Command: `grep -r --include '*.txt' needle input`},
		{Type: "commandExecution", ExitCode: &exit, Command: `nl -w3 -s' ' subsection`, Cwd: "notes"},
	} {
		paths, source := codexReadPaths(i)
		r.recordReadPaths("one", i.Cwd, paths, source)
	}
	want := []readObservation{{"docs/start.md", "structured"}, {"docs", "structured"}, {"src", "structured"}, {"lab.ipynb", "structured"}, {"notes.md", "parsed"},
		{"notes with spaces.md", "parsed"}, {"src/a.go", "structured"}, {"other", "parsed"}, {"input", "parsed"}, {"notes/subsection", "parsed"}}
	if runtime.GOOS == "windows" {
		// Codex commands on Windows are not reparsed with a POSIX shell dialect.
		want = []readObservation{{"docs/start.md", "structured"}, {"docs", "structured"}, {"src", "structured"}, {"lab.ipynb", "structured"}, {"notes.md", "parsed"}, {"src/a.go", "structured"}}
	}
	if !slices.Equal(r.reads.paths, want) {
		t.Fatalf("reads %v, want %v", r.reads.paths, want)
	}
	r.recordReadPaths("two", work, []string{"docs"}, "structured")
	if !slices.Equal(r.reads.paths, []readObservation{{"docs", "structured"}}) {
		t.Fatalf("second turn has paths %v", r.reads.paths)
	}
}

func TestAcceptanceOutputTail(t *testing.T) {
	var tail commandTail
	_, _ = tail.Write([]byte(strings.Repeat("x", outputTailBytes+1)))
	_, _ = tail.Write([]byte("end"))
	if len(tail) != outputTailBytes || string(tail[len(tail)-3:]) != "end" || tail[0] != 'x' {
		t.Fatalf("tail size = %d, end = %q", len(tail), tail[len(tail)-3:])
	}
}
