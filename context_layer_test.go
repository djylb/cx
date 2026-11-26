package main

import (
	"encoding/json"
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
	req := jobSubmission{Host: "local"}
	if err := attachContext(&req, "p", false, ""); err != nil {
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
	none := jobSubmission{Host: "local"}
	if err := attachContext(&none, "other", false, ""); err != nil {
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

func TestContextLayerSelection(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	t.Setenv("CX_CONTEXT_DIR", "")
	for rel, body := range map[string]string{"context/MEMORY.md": "shared", "projects/p/context/p.md": "project", "hosts/h/context/MEMORY.md": "gpu: none"} {
		p := filepath.Join(cfg, "cx", rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for layers, want := range map[string]int{"none": 0, "project": 1, "shared": 1, "host": 1, "shared,project,host": 3, "": 3} {
		req := jobSubmission{Host: "h"}
		if err := attachContext(&req, "p", false, layers); err != nil || len(req.Context) != want {
			t.Fatalf("%q: %v %v", layers, req.Context, err)
		}
		hostHint := strings.Contains(req.Preamble, "$CX_CONTEXT/host/MEMORY.md")
		if hostHint != (layers == "" || strings.Contains(layers, "host")) || (hostHint && string(req.Context["host/MEMORY.md"]) != "gpu: none") {
			t.Fatalf("%q: host layer %v %q", layers, req.Context, req.Preamble)
		}
		if layers == "none" && strings.Contains(req.Preamble, "CX_CONTEXT") {
			t.Fatalf("none still mentions context: %q", req.Preamble)
		}
	}
	other := jobSubmission{Host: "other"}
	if err := attachContext(&other, "p", false, ""); err != nil || len(other.Context) != 2 {
		t.Fatalf("other host got notes: %v %v", other.Context, err)
	}
	bad := jobSubmission{Host: "h"}
	if err := attachContext(&bad, "p", false, "everything"); err == nil {
		t.Fatal("invalid layer accepted")
	}
}

func TestOutboxBoard(t *testing.T) {
	data := []byte(`{"source_job":"h:p/a","to":"#board","time":"2026-01-01T00:00:01Z","text":"a1"}
{"source_job":"h:p/b","to":"parent","time":"2026-01-01T00:00:02Z","text":"b0"}
{"source_job":"h:p/b","to":"#board","time":"2026-01-01T00:00:03Z","text":"b1"}
{"source_job":"h:p/a","to":"#board","time":"2026-01-01T00:00:04Z","text":"a2"}
`)
	rows, err := appendOutboxRows(nil, data, "#board")
	if err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	got := latestPerJob(rows)
	if len(got) != 2 || !strings.Contains(string(got[0].raw), "b1") || !strings.Contains(string(got[1].raw), "a2") {
		t.Fatalf("%+v", got)
	}
}

func TestContextMirrorPut(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	own := filepath.Join(cfg, "cx", "projects", "mine", "context", "MEMORY.md")
	stale := filepath.Join(cfg, "cx", "context", "old.md")
	for _, p := range []string{own, stale} {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m := contextMirror{Source: "src", Roots: []string{"context", "hosts/h/context"}, Files: map[string][]byte{
		"context/MEMORY.md": []byte("shared"), "hosts/h/context/MEMORY.md": []byte("host"),
	}}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = nodeContextPut(string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(cfg, "cx", "context", "MEMORY.md")); string(b) != "shared" {
		t.Fatalf("shared %q", b)
	}
	if _, err = os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file kept: %v", err)
	}
	if b, _ := os.ReadFile(own); string(b) != "x" {
		t.Fatal("receiver's own project layer touched")
	}
	roots, err := contextRoots(filepath.Join(cfg, "cx"))
	if err != nil || strings.Join(roots, ",") != "context,hosts/h/context,projects/mine/context" {
		t.Fatalf("%v %v", roots, err)
	}
	layer := map[string][]byte{}
	if err := readContextLayer(filepath.Join(cfg, "cx", "context"), layer); err != nil {
		t.Fatal(err)
	}
	if _, ok := layer[mirrorMarker]; !ok {
		t.Fatal("missing mirror marker")
	}
	if err := nodeContextPut(`{"roots":["../x"],"files":{}}`); err == nil {
		t.Fatal("escaping root accepted")
	}
}

func TestMirrorDigest(t *testing.T) {
	a := contextMirror{Roots: []string{"context"}, Files: map[string][]byte{"context/a": []byte("1"), "context/b": []byte("2")}}
	b := contextMirror{Roots: []string{"context"}, Files: map[string][]byte{"context/b": []byte("2"), "context/a": []byte("1")}}
	if mirrorDigest(a) != mirrorDigest(b) {
		t.Fatal("digest depends on map order")
	}
	b.Files["context/b"] = []byte("3")
	if mirrorDigest(a) == mirrorDigest(b) {
		t.Fatal("content change not detected")
	}
	c := contextMirror{Roots: []string{"context", "hosts/h/context"}, Files: a.Files}
	if mirrorDigest(a) == mirrorDigest(c) {
		t.Fatal("removed root not detected")
	}
}

func TestPsmuxLaunchTarget(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CX_PSMUX", "psmux")
	t.Setenv("CXRUN_TMUX_SOCKET", "sock")
	t.Setenv("CX_MUX_SESSION", "")
	args := psmuxLaunch(root, "cx-1").launchArgs("cx.exe", "node")
	if strings.Join(args, " ") != "-L sock new-session -d -s cx-1 -- cx.exe node" {
		t.Fatal(args)
	}
	if err := os.WriteFile(filepath.Join(root, hostConfigName), []byte(`{"mux_session":"development"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args = psmuxLaunch(root, "cx-1").launchArgs("cx.exe", "node")
	if strings.Join(args, " ") != "-L sock new-window -d -t development -n cx-1 -- cx.exe node" {
		t.Fatal(args)
	}
}

func TestEnvFileExpansion(t *testing.T) {
	t.Setenv("CX_TEST_BASE", "/base")
	env, err := parseEnvFile("TOOLS=/opt/t\nPATH=${TOOLS}/bin:${CX_TEST_BASE}\nLIT=a$b\n")
	if err != nil {
		t.Fatal(err)
	}
	if env["PATH"] != "/opt/t/bin:/base" || env["LIT"] != "a$b" {
		t.Fatalf("%v", env)
	}
}

func TestWaitBaselineAcceptsEmptyStateCursor(t *testing.T) {
	if err := validateWaitBaseline(map[string]waitCursor{"old/job": {}, "new/job": {StateCursor: "1:0:1"}}); err != nil {
		t.Fatal(err)
	}
	if err := validateWaitBaseline(map[string]waitCursor{"bad/job": {StateCursor: "x"}}); err == nil {
		t.Fatal("malformed cursor accepted")
	}
}
