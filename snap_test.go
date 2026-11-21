package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func snapTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, c := context.WithTimeout(context.Background(), 23*time.Second)
	t.Cleanup(c)
	t.Setenv("CX_SNAPSHOT_STORE", filepath.Join(t.TempDir(), "store"))
	t.Setenv("PYTHONDONTWRITEBYTECODE", "1")
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

func TestSnapPythonWorkspace(t *testing.T) {
	ctx := snapTestCtx(t)
	app := snapWorkspaceFixture(t, ctx)
	t.Setenv("GOWORK", filepath.Join(app, "go.work"))
	sid, m, e := Freeze(ctx, app, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Repositories) != 2 {
		t.Fatalf("expected two workspace repositories: %+v", m.Repositories)
	}
	if reference, ok := pythonReference(t, filepath.Join("snapshot", "freeze.py")); ok {
		t.Setenv("PYTHONPATH", filepath.Dir(reference))
		cmd := exec.CommandContext(ctx, "python3", "-c", `from pathlib import Path
import freeze,sys
freeze.STORE=Path(sys.argv[2])
print(freeze.freeze(Path(sys.argv[1])))`, app, t.TempDir())
		var diag bytes.Buffer
		cmd.Stderr = &diag
		b, err := cmd.Output()
		if err != nil || string(sid) != strings.TrimSpace(string(b)) {
			t.Fatalf("workspace differential: %v Python=%s Go=%s %s", err, b, sid, diag.String())
		}
	} else {
		t.Log("snapshot/freeze.py unavailable: skipping workspace snapshot ID comparison")
	}
	hub := endpoint{State: t.TempDir()}
	if e = Publish(ctx, hub, sid, nil); e != nil {
		t.Fatal(e)
	}
	job := t.TempDir()
	start := time.Now()
	p, e := Materialize(ctx, job, m, filepath.Join(hub.State, "snapshot"))
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("two-repository checkout including review.diff: %v", time.Since(start))
	if p.ReviewDiff == nil {
		t.Fatal("missing review")
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
	if _, e = g.get("apply", "--check", patch); e != nil {
		t.Fatal(e)
	}
	goPatch, e := os.ReadFile(patch)
	if e != nil || len(goPatch) == 0 {
		t.Fatalf("missing Go patch: %v", e)
	}
	reference, ok := pythonReference(t, "cx-executor.py")
	if !ok {
		t.Log("cx-executor.py unavailable: skipping workspace result patch comparison")
		return
	}
	script := `import importlib.util,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location("executor",sys.argv[1])
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
repo,oid,tmp=Path(sys.argv[2]),sys.argv[3],Path(sys.argv[4])
sys.stdout.buffer.write(mod.repository_patch(repo,{"oid":oid},tmp,repo/"artifacts")[0])`
	cmd := exec.CommandContext(ctx, "python3", "-c", script, reference, p.Source, result.Repositories[0].SnapshotOID, filepath.Join(job, "py-index"))
	var diag bytes.Buffer
	cmd.Stderr = &diag
	b, e := cmd.Output()
	if e != nil {
		t.Fatalf("reference patch: %v %s", e, diag.String())
	}
	if !bytes.Equal(b, goPatch) {
		t.Fatal("Go and Python result patches differ")
	}
}
func TestSnapDirectoryIncrement(t *testing.T) {
	ctx := snapTestCtx(t)
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

func TestSnapEmbeddedDenylist(t *testing.T) {
	if strings.TrimSpace(snapDeny) == "" {
		t.Fatal("embedded snapshot policy is empty")
	}
	patterns := strings.Split(snapDeny, "\n")
	for _, pattern := range []string{"*.pem", "*.key", ".env*", ".ssh", ".aws", "credentials", "token.json"} {
		if !slices.Contains(patterns, pattern) {
			t.Errorf("embedded snapshot policy is missing %q", pattern)
		}
	}
	reference, ok := pythonReference(t, filepath.Join("snapshot", "denylist"))
	if !ok {
		t.Log("snapshot/denylist unavailable: skipping embedded policy comparison")
		return
	}
	data, e := os.ReadFile(reference)
	if e != nil {
		t.Fatal(e)
	}
	if string(data) != snapDeny {
		t.Fatal("embedded snapshot policy differs from snapshot/denylist")
	}
}
