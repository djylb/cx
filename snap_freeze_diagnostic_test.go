package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestFreezeIndexDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		stage, mode, want, advice string
	}{
		{"1", "100644", "unresolved merge", "git merge --abort"},
		{"0", "160000", "submodule", "another worktree"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			ctx := snapTestCtx(t)
			repo := t.TempDir()
			resolvedRepo, err := filepath.EvalSymlinks(repo)
			if err != nil {
				t.Fatal(err)
			}
			git := snapGit{ctx: ctx, dir: repo}
			if _, err = git.get("init", "-q"); err != nil {
				t.Fatal(err)
			}
			oid, err := git.input([]byte("fixture"), "hash-object", "-w", "--stdin")
			if err != nil {
				t.Fatal(err)
			}
			record := tc.mode + " " + strings.TrimSpace(string(oid)) + " " + tc.stage + "\tentry\n"
			if _, err = git.input([]byte(record), "update-index", "--index-info"); err != nil {
				t.Fatal(err)
			}
			_, _, err = Freeze(ctx, repo, FreezeOptions{})
			for _, want := range []string{resolvedRepo, `"entry"`, tc.want, tc.advice} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Freeze error = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestSnapshotCLINestedGitDiagnostics(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("LAN_SNAPSHOT_ID", "")
	t.Setenv("CX_PROJECT", "")
	root := filepath.Join(f.home, "source")
	first := filepath.Join(root, "a")
	second := filepath.Join(root, "z")
	for _, dir := range []string{first, second} {
		cliOutput(t, 0, nil, "git", "init", "--quiet", dir)
	}
	_, diag := cliStreams(t, 2, nil, f.cx, "snapshot", root)
	if !strings.Contains(diag, `nested Git metadata: "a/.git"`) || strings.Contains(diag, "z/.git") {
		t.Fatalf("snapshot diagnostic: %s", diag)
	}
	cliOutput(t, 0, nil, f.cx, "project", "init", "mixed", "--repo", first, second)
	_, diag = cliStreams(t, 2, nil, f.cx, "snapshot", root, "--project", "mixed")
	for _, want := range []string{"DIR must be a project member repository", first, second} {
		if !strings.Contains(diag, want) {
			t.Fatalf("project snapshot diagnostic is missing %q: %s", want, diag)
		}
	}
}
