package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func snapTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, c := fixtureContext(t)
	t.Cleanup(c)
	t.Setenv("CX_SNAPSHOT_STORE", filepath.Join(t.TempDir(), "store"))
	t.Setenv("GOWORK", "off")
	return ctx
}
func snapWriteTest(t *testing.T, p, s string) {
	t.Helper()
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte(s), 0600); e != nil {
		t.Fatal(e)
	}
}

func snapWorkspaceFixture(t *testing.T, ctx context.Context) string {
	t.Helper()
	root := t.TempDir()
	app, lib := filepath.Join(root, "app"), filepath.Join(root, "library")
	git := func(repo string, args ...string) {
		t.Helper()
		if _, err := (snapGit{ctx: ctx, dir: repo}).get(args...); err != nil {
			t.Fatal(err)
		}
	}
	snapWriteTest(t, filepath.Join(app, "go.mod"), "module fixture.test/app\n\ngo 1.23\n\nrequire fixture.test/lib v0.0.0\n")
	snapWriteTest(t, filepath.Join(app, ".gitignore"), "go.work*\ninternal/web/dist\nignored\n")
	snapWriteTest(t, filepath.Join(app, "state"), "base\n")
	snapWriteTest(t, filepath.Join(app, "removed"), "")
	snapWriteTest(t, filepath.Join(lib, "go.mod"), "module fixture.test/lib\n\ngo 1.23\n")
	snapWriteTest(t, filepath.Join(lib, "lib.go"), "package lib\nconst Value = \"base\"\n")
	for _, repo := range []string{app, lib} {
		git(repo, "init", "-q")
		git(repo, "add", ".")
		git(repo, "-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "baseline")
	}
	if err := os.Remove(filepath.Join(app, "removed")); err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(app, "state"), "staged version\n")
	snapWriteTest(t, filepath.Join(app, "staged"), "staged only\n")
	git(app, "add", "state", "staged")
	snapWriteTest(t, filepath.Join(app, "state"), "working version\n")
	snapWriteTest(t, filepath.Join(app, "new file-中"), "untracked\n")
	snapWriteTest(t, filepath.Join(app, "ignored"), "")
	snapWriteTest(t, filepath.Join(app, "internal", "web", "dist", "generated"), "generated\n")
	snapWriteTest(t, filepath.Join(lib, "lib.go"), "package lib\nconst Value = \"dirty-lib\"\n")
	snapWriteTest(t, filepath.Join(app, "go.work"), "go 1.23\nuse (\n .\n ../library\n)\n")
	return app
}

func TestSnapWorkspace(t *testing.T) {
	ctx := snapTestCtx(t)
	app := snapWorkspaceFixture(t, ctx)
	t.Setenv("GOWORK", filepath.Join(app, "go.work"))
	sid, m, e := Freeze(ctx, app, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Repositories) != 2 || m.Main != "app" ||
		m.Repositories[0].Path != "app" || m.Repositories[1].Path != "library" {
		t.Fatalf("wrong workspace layout: %+v", m)
	}
	hub := endpoint{State: t.TempDir()}
	if e = Publish(ctx, hub, sid, nil); e != nil {
		t.Fatal(e)
	}
	job := t.TempDir()
	unpublished := filepath.Join(job, "work", "tree")
	snapWriteTest(t, filepath.Join(unpublished, "unpublished"), "partial materialization")
	start := time.Now()
	p, e := Materialize(ctx, job, m, filepath.Join(hub.State, "snapshot"))
	if e != nil {
		t.Fatal(e)
	}
	if _, err := os.Stat(filepath.Join(unpublished, "unpublished")); !os.IsNotExist(err) {
		t.Fatalf("materialization left unpublished work: %v", err)
	}
	t.Logf("two-repository checkout including review.diff: %v", time.Since(start))
	if p.ReviewDiff == nil {
		t.Fatal("missing review")
	}
	for _, file := range []struct{ path, content string }{
		{"app/state", "working version\n"},
		{"app/staged", "staged only\n"},
		{"app/new file-中", "untracked\n"},
		{"library/lib.go", "package lib\nconst Value = \"dirty-lib\"\n"},
	} {
		b, err := os.ReadFile(filepath.Join(unpublished, file.path))
		if err != nil || string(b) != file.content {
			t.Errorf("checkout %s: got %q (%v), want %q", file.path, b, err, file.content)
		}
	}
	for _, name := range []string{"ignored", "removed"} {
		if _, err := os.Stat(filepath.Join(p.Source, name)); !os.IsNotExist(err) {
			t.Errorf("checkout contains %s: %v", name, err)
		}
	}
	const wantWork = "go 1.23\n\nuse (\n\t./tree/app\n\t./tree/library\n)\n"
	if b, err := os.ReadFile(p.Env["GOWORK"]); err != nil || string(b) != wantWork {
		t.Fatalf("materialized go.work: got %q (%v), want %q", b, err, wantWork)
	}
	snapWriteTest(t, filepath.Join(p.Source, "state"), "job edit\n")
	snapWriteTest(t, filepath.Join(p.Source, "artifacts", "capture"), "result")
	result, e := Collect(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	if len(result.Repositories) != 1 || len(result.Artifacts) != 1 {
		t.Fatalf("result %+v", result)
	}
	patch := filepath.Join(result.Dir, result.Repositories[0].Patch)
	g := snapGit{ctx: ctx, dir: app}
	if names, err := g.get("apply", "--numstat", patch); err != nil || names != "1\t1\tstate" {
		t.Fatalf("result patch changes: %q (%v)", names, err)
	}
	if _, e = g.get("-c", "core.autocrlf=false", "apply", patch); e != nil {
		t.Fatal(e)
	}
	if b, err := os.ReadFile(filepath.Join(app, "state")); err != nil || string(b) != "job edit\n" {
		t.Fatalf("applied result: got %q (%v), want job edit", b, err)
	}
}

func TestSnapProjectWorkspaceUnion(t *testing.T) {
	ctx := snapTestCtx(t)
	app := snapWorkspaceFixture(t, ctx)
	t.Setenv("GOWORK", filepath.Join(app, "go.work"))
	tools := filepath.Join(filepath.Dir(app), "tools")
	snapWriteTest(t, filepath.Join(tools, "tool"), "fixture")
	if _, e := (snapGit{ctx: ctx, dir: tools}).get("init", "-q"); e != nil {
		t.Fatal(e)
	}
	// The workspace also discovers app. The project-only repository is kept,
	// and app's include is still applied when the Go entry is deduplicated.
	_, m, e := Freeze(ctx, app, FreezeOptions{Repositories: []projectRepository{
		{Path: tools},
		{Path: app, Include: []string{"ignored"}},
	}})
	if e != nil {
		t.Fatal(e)
	}
	if m.Main != "app" || len(m.Repositories) != 3 || m.Repositories[0].Path != "app" ||
		m.Repositories[1].Path != "library" || m.Repositories[2].Path != "tools" ||
		len(m.Repositories[0].Include) != 1 || m.Repositories[0].Include[0] != "ignored" ||
		m.Workspace == nil || !strings.Contains(m.Workspace.Text, "./tree/library") {
		t.Fatalf("wrong project workspace union: %+v", m)
	}
}

func TestSnapManifestV1Encoding(t *testing.T) {
	m := Manifest{Version: 1, Main: "app", Repositories: []SnapshotRepo{{Path: "app", OID: "object", RepoID: "repository"}}}
	const original = `{"main":"app","repositories":[{"head":null,"oid":"object","path":"app","repo_id":"repository"}],"version":1,"workspace":null}`
	if b, e := snapJSON(m); e != nil || string(b) != original {
		t.Fatalf("v1 snapshot encoding: %s (%v)", b, e)
	}
}

func TestSnapDirectoryIncrement(t *testing.T) {
	ctx := snapTestCtx(t)
	if runtime.GOOS == "windows" {
		// Include Git lock paths beyond MAX_PATH even with a short TEMP.
		store := os.Getenv("CX_SNAPSHOT_STORE")
		const minStoreLength = 135
		if padding := minStoreLength - len(store) - 1; padding > 0 {
			t.Setenv("CX_SNAPSHOT_STORE", filepath.Join(store, strings.Repeat("s", padding)))
		}
	}
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "first"), strings.Repeat("a", 4096))
	hub := endpoint{State: t.TempDir()}
	for _, v := range []string{"one", "two"} {
		snapWriteTest(t, filepath.Join(src, "other"), v)
		id, m, e := Freeze(ctx, src, FreezeOptions{})
		if e != nil {
			t.Fatal(e)
		}
		if e = Publish(ctx, hub, id, nil); e != nil {
			t.Fatal(e)
		}
		p, e := Materialize(ctx, t.TempDir(), m, filepath.Join(hub.State, "snapshot"))
		if e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(filepath.Join(p.Source, "other"))
		if e != nil || string(b) != v {
			t.Fatal("wrong worktree")
		}
	}
	snapWriteTest(t, filepath.Join(src, ".env"), "no")
	if _, _, e := Freeze(ctx, src, FreezeOptions{}); e == nil {
		t.Fatal("accepted untracked sensitive file")
	}
}
