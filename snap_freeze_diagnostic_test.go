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
