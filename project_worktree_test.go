package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Another worktree of a member repository is not that member.
func TestResolveProjectMatchesWorktreePath(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	main := filepath.Join(base, "main")
	other := filepath.Join(base, "other")
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=f", "GIT_AUTHOR_EMAIL=f@example.invalid", "GIT_COMMITTER_NAME=f", "GIT_COMMITTER_EMAIL=f@example.invalid")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	if err := os.MkdirAll(main, 0o700); err != nil {
		t.Fatal(err)
	}
	run(main, "init", "-q")
	run(main, "commit", "-q", "--allow-empty", "-m", "base")
	run(main, "worktree", "add", "-q", other)
	dir, err := projectDirectory()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	def := "[[repositories]]\npath = " + `"` + filepath.ToSlash(main) + `"` + "\n"
	if err = os.WriteFile(filepath.Join(dir, "demo.toml"), []byte(def), 0o600); err != nil {
		t.Fatal(err)
	}
	if name, _, resolveErr := resolveProject(ctx, main, ""); resolveErr != nil || name != "demo" {
		t.Fatalf("member: %q (%v)", name, resolveErr)
	}
	if name, _, resolveErr := resolveProject(ctx, other, ""); resolveErr != nil || name == "demo" {
		t.Fatalf("other worktree: %q (%v)", name, resolveErr)
	}
}
