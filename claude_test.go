package main

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestClaudeDeferredInput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	h := newRuns(t, "")
	path := filepath.Join(filepath.Dir(h.work), "claude")
	fixture := `#!/bin/sh
read -r msg
case "$msg" in
  *task*) while [ ! -f release-first ]; do sleep .02; done ;;
  *follow*) case "$*" in *"--resume fixture-session"*) ;; *) exit 2 ;; esac ;;
  *) exit 3 ;;
esac
echo '{"type":"result","session_id":"fixture-session","result":"answer"}'
`
	if err := os.WriteFile(path, []byte(fixture), 0700); err != nil {
		t.Fatal(err)
	}
	h.withClock("conversation")
	done := h.start("conversation", "--engine", "claude", "--web-search", "--idle-exit", "1s")
	h.await("conversation", func(s status) bool { return s.TurnStatus == "inProgress" })
	if err := h.ctl("say", "conversation", "follow"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.work, "release-first"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.idleExit("conversation", done, func(s status) bool { return s.Finished == 2 }); err != nil {
		t.Fatal(err)
	}
	s := h.await("conversation", func(s status) bool { return s.State == "done" })
	if s.Finished != 2 || s.CompletedStatus != "completed" || s.Thread != "fixture-session" {
		t.Fatalf("unexpected result: %+v", s)
	}
}

func flagValue(args []string, name string) string {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func TestClaudeArgsMapping(t *testing.T) {
	c := config{Engine: "claude", Sandbox: "read-only", WebSearch: true}
	args := c.claudeArgs("", "")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--restricted") || !strings.Contains(flagValue(args, "--tools"), "WebFetch") ||
		strings.Contains(flagValue(args, "--tools"), "Bash") || strings.Contains(joined, "Write") || strings.Contains(joined, "--mcp-config") {
		t.Fatal(args)
	}
	root := filepath.Join(t.TempDir(), "cache")
	c = config{Engine: "claude", Sandbox: "workspace-write", Network: true, Effort: "high", Roots: []string{root}}
	args = c.claudeArgs("id", "/tmp/tools.json")
	allowed := flagValue(args, "--allowedTools")
	pattern := "//" + strings.TrimPrefix(filepath.ToSlash(root), "/") + "/**"
	for _, want := range []string{"Write(./**)", "Edit(./**)", "Bash", "WebFetch", "Edit(" + pattern + ")", "mcp__" + jobToolServerName} {
		if !strings.Contains(allowed, want) {
			t.Fatalf("missing %s in %v", want, args)
		}
	}
	if flagValue(args, "--resume") != "id" || flagValue(args, "--effort") != "high" || flagValue(args, "--add-dir") != root ||
		flagValue(args, "--mcp-config") != "/tmp/tools.json" || !strings.Contains(flagValue(args, "--tools"), "Bash") || strings.Contains(allowed, "WebSearch") {
		t.Fatal(args)
	}
	if c.driver().capabilities().Steer || !c.driver().capabilities().Resume {
		t.Fatal("invalid Claude capabilities")
	}
	c.Sandbox = dangerFullAccess
	args = c.claudeArgs("", "")
	if flagValue(args, "--permission-mode") != "bypassPermissions" || flagValue(args, "--tools") != "" {
		t.Fatal(args)
	}
	r := runner{c: config{WebSearch: true, Effort: "medium"}}
	cfg := r.threadParams()["config"].(fields)
	if cfg["web_search"] != "live" || cfg["model_reasoning_effort"] != "medium" {
		t.Fatal(cfg)
	}
}

func TestSnapshotMemberRoots(t *testing.T) {
	for _, engine := range []string{"codex", "claude"} {
		t.Run(engine, func(t *testing.T) { testSnapshotMemberRoots(t, engine) })
	}
}

func testSnapshotMemberRoots(t *testing.T, engine string) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	h := newRuns(t, "manual")
	checkout := filepath.Join(h.root, ".work", "p", "member-roots")
	tree := filepath.Join(checkout, "work", "tree")
	main := filepath.Join(tree, "app", "subdir")
	library := filepath.Join(tree, "lib")
	cache := t.TempDir()
	for _, dir := range []string{main, library, filepath.Join(checkout, "input")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	m := Manifest{Version: 1, Main: "app/subdir"}
	for _, name := range []string{"app", "lib"} {
		m.Repositories = append(m.Repositories, SnapshotRepo{Path: name, OID: strings.Repeat("a", 40), RepoID: snapSum([]byte(name))})
	}
	if err := writeJSON(filepath.Join(checkout, "input", "manifest.json"), m); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(h.work, "engine-arguments")
	if engine == "claude" {
		t.Setenv("TEST_CLAUDE_ARGUMENTS", capture)
		fixture := `#!/bin/sh
printf '%s\n' "$@" > "$TEST_CLAUDE_ARGUMENTS"
echo '{"type":"result","session_id":"fixture-session","result":"answer"}'
`
		if err := os.WriteFile(filepath.Join(filepath.Dir(h.work), "claude"), []byte(fixture), 0700); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Setenv("FIXTURE_PACKAGE_CACHE_RECORD", capture)
	}
	h.work = main
	h.withClock("p/member-roots")
	h.start("p/member-roots", "--engine", engine, "--writable-root", cache)
	state := h.await("p/member-roots", func(s status) bool {
		return s.Turn != "" || s.Finished == 1 || isTerminal(s.State)
	})
	if isTerminal(state.State) && state.State != "done" {
		t.Fatalf("runner failed: %s", state.Error)
	}
	if engine == "claude" && state.Finished == 0 {
		h.await("p/member-roots", func(s status) bool { return s.Finished == 1 })
	}
	var roots []string
	if engine == "claude" {
		b, err := os.ReadFile(capture)
		if err != nil {
			t.Fatal(err)
		}
		args := strings.Split(string(b), "\n")
		for i, arg := range args {
			if arg == "--add-dir" && i+1 < len(args) {
				roots = append(roots, args[i+1])
			}
		}
	} else {
		var record packageCacheRecord
		if err := readJSON(capture, &record); err != nil {
			t.Fatal(err)
		}
		roots = record.WritableRoots
	}
	for _, root := range []string{filepath.Join(tree, "app"), library, cache} {
		physical, err := filepath.EvalSymlinks(root)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(roots, root) && !slices.Contains(roots, physical) {
			t.Errorf("missing %s from %v", root, roots)
		}
	}
	physicalTree, err := filepath.EvalSymlinks(tree)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(roots, tree) || slices.Contains(roots, physicalTree) {
		t.Fatalf("exposed checkout parent: %v", roots)
	}
}
