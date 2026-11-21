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
  *task*) sleep 0.8 ;;
  *follow*) case "$*" in *"--resume fixture-session"*) ;; *) exit 2 ;; esac ;;
  *) exit 3 ;;
esac
echo '{"type":"result","session_id":"fixture-session","result":"answer"}'
`
	if err := os.WriteFile(path, []byte(fixture), 0700); err != nil {
		t.Fatal(err)
	}
	done := h.start("conversation", "--engine", "claude", "--web-search", "--idle-exit", "1s")
	h.await("conversation", func(s status) bool { return s.TurnStatus == "inProgress" })
	if err := h.ctl("say", "conversation", "follow"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s := h.await("conversation", func(s status) bool { return s.State == "done" })
	if s.Finished != 2 || s.CompletedStatus != "completed" || s.Thread != "fixture-session" {
		t.Fatalf("unexpected result: %+v", s)
	}
}

func TestDriverCapabilitiesAndPermissions(t *testing.T) {
	c := config{Engine: "claude", Sandbox: "read-only", WebSearch: true}
	args := strings.Join(c.claudeArgs(""), " ")
	if !strings.Contains(args, "--restricted") || !strings.Contains(args, "WebFetch") || strings.Contains(args, "Write") {
		t.Fatal(args)
	}
	c.Sandbox = "workspace-write"
	args = strings.Join(c.claudeArgs("id"), " ")
	if !strings.Contains(args, "Write(./**)") || !strings.Contains(args, "--resume id") {
		t.Fatal(args)
	}
	if c.driver().capabilities().Steer || !c.driver().capabilities().Resume {
		t.Fatal("invalid Claude capabilities")
	}
	c.Sandbox = dangerFullAccess
	if !strings.Contains(strings.Join(c.claudeArgs(""), " "), "bypassPermissions") {
		t.Fatal("missing full access")
	}
	r := runner{c: config{WebSearch: true, Effort: "medium"}}
	cfg := r.threadParams()["config"].(fields)
	if cfg["web_search"] != "live" || cfg["model_reasoning_effort"] != "medium" {
		t.Fatal(cfg)
	}
}
