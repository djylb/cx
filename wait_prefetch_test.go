package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// After `cx wait` reports a completed turn, `cx result` without --wait must read the
// local cache rather than start a background download (exit 12).
func TestWaitPrefetchesResult(t *testing.T) {
	f := newControllerFixture(t)
	_, src, prompt := runnerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	t.Setenv("CX_POLICY_DIR", f.home)
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("FIXMODE", "sealed-patches")
	t.Setenv("FIXPROMPT", "Fixture\n\ntask")
	if err := os.WriteFile(filepath.Join(f.home, "preamble.md"), []byte("Fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"GIT_AUTHOR_NAME", "fixture"}, {"GIT_COMMITTER_NAME", "fixture"}, {"GIT_AUTHOR_EMAIL", "fixture@example.invalid"}, {"GIT_COMMITTER_EMAIL", "fixture@example.invalid"}} {
		t.Setenv(kv[0], kv[1])
	}
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("base\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "--quiet"}, {"add", "."}, {"commit", "-qm", "base"}} {
		cliOutput(t, 0, nil, "git", append([]string{"-C", src}, args...)...)
	}
	const id = "local:p/prefetch"
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "prefetch", prompt, "--sync", "--idle-exit", "15s")
	defer cliOutput(t, 0, nil, f.cx, "close", id)
	cliOutput(t, 0, nil, f.cx, "wait", id, "--timeout", "10s")
	out, diag := cliStreams(t, 0, nil, f.cx, "result", id)
	if strings.Contains(string(out), "collecting") || !strings.Contains(diag, "last.md") {
		t.Fatalf("result after wait was not cached: %s %s", out, diag)
	}
}
