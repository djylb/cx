package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextLayers(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	t.Setenv("CX_CONTEXT_DIR", "")
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(cfg, "cx", rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("context/MEMORY.md", "shared index")
	write("context/rules.md", "shared rules")
	write("projects/p/context/rules.md", "project rules")
	write("projects/p/context/notes/a.md", "note")
	var req jobSubmission
	if err := attachContext(&req, "p", false); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"MEMORY.md": "shared index", "rules.md": "project rules", "notes/a.md": "note"}
	if len(req.Context) != len(want) {
		t.Fatalf("context %v", req.Context)
	}
	for k, v := range want {
		if string(req.Context[k]) != v {
			t.Fatalf("%s = %q, want %q", k, req.Context[k], v)
		}
	}
	if !strings.Contains(req.Preamble, "$CX_CONTEXT/MEMORY.md") {
		t.Fatalf("preamble lacks index hint: %q", req.Preamble)
	}
	var none jobSubmission
	if err := attachContext(&none, "other", false); err != nil {
		t.Fatal(err)
	}
	if len(none.Context) != 2 {
		t.Fatalf("shared layer alone: %v", none.Context)
	}
}

func TestLatestPerAuthor(t *testing.T) {
	rows := []channelRow{{Author: "a", Seq: 1}, {Author: "b", Seq: 2}, {Author: "a", Seq: 3}, {Author: "c", Seq: 4}}
	got := latestPerAuthor(rows)
	if len(got) != 3 || got[0].Seq != 2 || got[1].Seq != 3 || got[2].Seq != 4 {
		t.Fatalf("%+v", got)
	}
}
