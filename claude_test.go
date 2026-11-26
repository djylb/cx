package main

import (
	"os"
	"path/filepath"
	"runtime"
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
