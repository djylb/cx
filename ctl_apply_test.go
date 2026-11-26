package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestApplySealedTurns(t *testing.T) {
	f := newControllerFixture(t)
	root, src, prompt := runnerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	t.Setenv("FIXMODE", "sealed-patches")
	t.Setenv("CX_POLICY_DIR", f.home)
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("FIXPROMPT", "Fixture\n"+strings.TrimRight(jobToolsNote, "\n")+"\n\ntask")
	if err := os.WriteFile(filepath.Join(f.home, "preamble.md"), []byte("Fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_AUTHOR_NAME", "fixture")
	t.Setenv("GIT_COMMITTER_NAME", "fixture")
	t.Setenv("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
	t.Setenv("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
	git := func(args ...string) []byte {
		t.Helper()
		return cliOutput(t, 0, nil, "git", append([]string{"-C", src}, args...)...)
	}
	read := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(src, name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	git("init", "--quiet")
	for _, name := range []string{"a", "undo"} {
		if err := os.WriteFile(filepath.Join(src, name), []byte("base\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	git("add", ".")
	git("commit", "-qm", "base")
	const id = "local:p/rounds"
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "rounds", prompt, "--sync", "--idle-exit", "2m")
	defer cliOutput(t, 0, nil, f.cx, "close", id)
	cliOutput(t, 0, nil, f.cx, "wait", id, "--timeout", "2m")
	sealed := filepath.Join(root, ".work", "p", "rounds", "result", "turn-1")
	ctx, cancel := fixtureContext(t)
	defer cancel()
	for {
		ok, err := fileExists(filepath.Join(sealed, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			break
		}
		if err = pause(ctx, 10*time.Millisecond); err != nil {
			t.Fatal("execution node did not seal turn 1 before collection")
		}
	}

	patch := cliOutput(t, 0, nil, f.cx, "patch", id, "--turn", "1", "--full")
	first := cliOutput(t, 0, nil, f.cx, "patch", id, "--turn", "1", "--full", "--incremental")
	if !bytes.Equal(first, patch) || !bytes.Contains(first, []byte("+round 1\n")) {
		t.Fatalf("first incremental patch: %s\ncumulative: %s", first, patch)
	}
	cliOutput(t, 0, nil, f.cx, "apply", id)
	git("commit", "-qm", "first turn")
	before := read(".git/index")
	// Use the low-level node entry that bypasses the controller's say preflight.
	cliOutput(t, 0, nil, f.cx, "node", "say", "--job", "p/rounds", "second round")
	cliOutput(t, 0, nil, f.cx, "wait", id, "--timeout", "2m")
	second := cliOutput(t, 0, nil, f.cx, "patch", id, "--turn", "2", "--incremental", "--full")
	for _, want := range []string{"-round 1\n+round 2\n", "-round 1\n+base\n", "+++ b/new"} {
		if !bytes.Contains(second, []byte(want)) {
			t.Fatalf("incremental patch is missing %q: %s", want, second)
		}
	}

	checkConflict := func(args ...string) {
		t.Helper()
		out, diag := cliStreams(t, 1, nil, f.cx, append([]string{"apply", id}, args...)...)
		for _, want := range []string{"/a", "--since-turn", "cx rebase"} {
			if !strings.Contains(diag, want) {
				t.Fatalf("missing %q in conflict diagnostics: %s", want, diag)
			}
		}
		if bytes.Contains(out, []byte("ready for review")) {
			t.Fatalf("misleading conflict output: %s", out)
		}
	}
	checkConflict()
	if !bytes.Equal(read(".git/index"), before) || string(read("a")) != "round 1\n" || string(read("undo")) != "round 1\n" {
		t.Fatal("default conflicting apply changed index or worktree")
	}
	if _, err := os.Stat(filepath.Join(src, "new")); !os.IsNotExist(err) {
		t.Fatalf("default conflicting apply wrote an unrelated patch hunk: %v", err)
	}
	checkConflict("--allow-conflicts")
	if a := read("a"); !bytes.Contains(a, []byte("<<<<<<<")) || !bytes.Contains(a, []byte("=======")) || !bytes.Contains(a, []byte(">>>>>>>")) {
		t.Fatalf("expected Git conflict markers: %s", a)
	}
	if len(git("ls-files", "--unmerged", "a")) == 0 || string(read("new")) != "round 2\n" {
		t.Fatal("opt-in merge failed to write unmerged index or clean file")
	}
	git("reset", "--hard", "HEAD")
	cliOutput(t, 0, nil, f.cx, "apply", id, "--since-turn", "2")
	if string(read("a")) != "round 2\n" || string(read("undo")) != "base\n" || string(read("new")) != "round 2\n" {
		t.Fatal("second turn application differs from sealed workspace")
	}
	if len(git("ls-files", "--unmerged")) != 0 {
		t.Fatal("second turn left conflicts")
	}

	if err := os.Rename(sealed, sealed+".fixture-backup"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CX_HOME", t.TempDir())
	_, diag := cliStreams(t, 44, nil, f.cx, "patch", id, "--turn", "2", "--incremental")
	if !strings.Contains(diag, "Sealed turn 1 is unavailable") {
		t.Fatalf("missing prior turn diagnostic: %s", diag)
	}
}
