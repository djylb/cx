package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRebaseQueuesActiveTurnAndReplacesPendingRequest(t *testing.T) {
	f := newControllerFixture(t)
	const id = "p/busy"
	s := status{Job: "busy", Project: "p", RunID: "fixture", Engine: "codex", State: "running",
		Finished: 1, Turn: "second", TurnStatus: "inProgress", LastCompleted: "first"}
	r := waitFixture(t, os.Getenv("CXRUN_STATE"), id, s)
	ctx := snapTestCtx(t)
	source := t.TempDir()
	snapWriteTest(t, filepath.Join(source, "source.txt"), "original")
	original, _, err := Freeze(ctx, source, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	script := `#!/bin/sh
read -r input
printf '%s\n' '{"type":"result","session_id":"fixture","result":"STATUS: DONE"}'
`
	if err = os.WriteFile(filepath.Join(f.home, "claude"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.s.State = "failed"
		if saveErr := r.saveTerminal(); saveErr != nil {
			t.Error(saveErr)
		}
	}()
	origin, err := originFile("local", id, s.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Snapshot: original, Source: source, JobOptions: []string{"--engine", "claude", "--network", "--idle-exit", "1s"}}); err != nil {
		t.Fatal(err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "rebase", "local:"+id, "first request", "--host", "local", "--profile", "first-account"); !strings.Contains(string(out), "queued") {
		t.Fatalf("initial rebase: %s", out)
	}
	pending, err := resultDest("local", id, s.RunID, "2")
	if err != nil {
		t.Fatal(err)
	}
	var request rebaseRequest
	if err = readJSON(pending+".rebase.json", &request); err != nil ||
		request.Host != "local" || request.Profile != "first-account" {
		t.Fatalf("pending destination = %+v, %v", request, err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "rebase", "--session", "replacement", "local:"+id, "second request"); !strings.Contains(string(out), "replaced") {
		t.Fatalf("replacement rebase: %s", out)
	}
	const child = "p/busy-r1"
	dir, err := submissionDir("local", child)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, "input.json")
	if _, err = os.Stat(input); !os.IsNotExist(err) {
		t.Fatalf("submitted before turn was sealed: %v", err)
	}
	snapWriteTest(t, filepath.Join(source, "source.txt"), "updated")
	fresh, _, err := Freeze(ctx, source, FreezeOptions{})
	if err != nil || fresh == original {
		t.Fatalf("fresh snapshot: %s, %v", fresh, err)
	}
	if err = r.publishTurnReport("second", []byte("STATUS: DONE")); err != nil {
		t.Fatal(err)
	}
	r.s.State, r.s.TurnStatus, r.s.LastCompleted, r.s.Finished = "waiting", "completed", "second", 2
	if err = r.save(); err != nil {
		t.Fatal(err)
	}
	// Observe the child registration before joining detached fixture workers.
	// Polling only synchronizes across processes; the fixture controls sealing.
	for {
		dir, pathErr := resultDest("local", id, s.RunID, "2")
		if pathErr != nil {
			t.Fatal(pathErr)
		}
		request = rebaseRequest{}
		if err = readJSON(dir+".rebase.json", &request); err != nil {
			t.Fatal(err)
		}
		if request.Failure != "" || request.Host != "" || request.Profile != "" {
			t.Fatalf("replaced request = %+v", request)
		}
		progress, receiptErr := readSubmissionProgress(filepath.Dir(input))
		if receiptErr != nil && !os.IsNotExist(receiptErr) {
			t.Fatal(receiptErr)
		}
		if progress.State == "submitted" {
			break
		}
		if err = pause(ctx, 20*time.Millisecond); err != nil {
			log, _ := os.ReadFile(dir + ".rebase.log")
			t.Fatalf("waiting for rebase submission: %v, log: %s", err, log)
		}
	}
	var q queuedJob
	if err = readJSON(input, &q); err != nil {
		logPath, pathErr := resultDest("local", id, s.RunID, "2")
		if pathErr != nil {
			t.Fatal(pathErr)
		}
		log, _ := os.ReadFile(logPath + ".rebase.log")
		t.Fatalf("read child submission: %v, worker log: %s", err, log)
	}
	if out := cliOutput(t, 0, nil, f.cx, "rebase", "local:"+id, "late request"); !strings.Contains(string(out), child) {
		t.Fatalf("completed rebase receipt: %s", out)
	}

	next, err := submissionDir("local", "p/busy-r2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(next, "input.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected extra rebase submission: %v", err)
	}
	if !strings.HasPrefix(q.Request.Prompt, "second request\n") ||
		!strings.Contains(q.Request.Prompt, "$CX_CONTEXT/rebase/result.json") ||
		q.Request.Session != "replacement" || q.Request.Snapshot != fresh {
		t.Fatalf("child prompt = %q, session = %q, snapshot = %q", q.Request.Prompt, q.Request.Session, q.Request.Snapshot)
	}
	if got := string(q.Request.Context["rebase/last.md"]); got != "STATUS: DONE" {
		t.Fatalf("attached report = %q", got)
	}

}

func TestRebaseWorkerRetainsRequestOnObservationFailure(t *testing.T) {
	f := newControllerFixture(t)
	const id = "p/stopped"
	s := status{Job: "stopped", Project: "p", RunID: "fixture", State: "failed"}
	waitFixture(t, os.Getenv("CXRUN_STATE"), id, s)
	dest, err := resultDest("local", id, s.RunID, "1")
	if err != nil {
		t.Fatal(err)
	}
	slot := dest + ".rebase.json"
	if err = writeJSON(slot, rebaseRequest{Text: "keep this"}); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 44, nil, f.cx, "node", "rebase-worker", "local", id, s.RunID, "1")
	var req struct {
		Text    string
		Failure string
	}
	if err = readJSON(slot, &req); err != nil || req.Text != "keep this" || !strings.Contains(req.Failure, "not sealed") {
		t.Fatalf("retained request = %+v, %v", req, err)
	}
}

func TestRebaseTransfersSealedResultToNode(t *testing.T) {
	f := newControllerFixture(t)
	ctx := snapTestCtx(t)
	const id, host, child = "p/remote-parent", "remote", "p/remote-parent-r1"
	node := t.TempDir()
	config := filepath.Join(f.home, ".config", "cx", "hosts.toml")
	snapWriteTest(t, config, fmt.Sprintf("[hosts.remote]\nssh = %q\ncx = %q\nstate = %q\nplatform = %q\n", "fixture", f.cx, node, runtime.GOOS+"/"+runtime.GOARCH))
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	blockDispatch := filepath.Join(f.home, "block-dispatch")
	snapWriteTest(t, blockDispatch, "reject registration")
	ssh := `#!/bin/sh
while [ "$1" != -- ]; do shift; done
shift 2
export CX_SNAPSHOT_STORE=` + shellQuote(filepath.Join(node, "store")) + `
case "$1" in
  *' node wire')
    request=$(cat)
    case "$request" in
      *'"start-installed-task"'*)
        if test -f ` + shellQuote(blockDispatch) + `; then
          printf 'cx-wire: ready\nfixture registration rejected\n' >&2
          exit 42
        fi
        ;;
    esac
    printf '%s' "$request" | sh -c "$1"
    exit $?
    ;;
esac
exec sh -c "$1"
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(ssh), 0700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
read -r input
printf '%s\n' '{"type":"result","session_id":"fixture","result":"STATUS: DONE"}'
`
	if err := os.WriteFile(filepath.Join(f.home, "claude"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	s := status{Job: "remote-parent", Project: "p", RunID: "fixture", Engine: "claude",
		State: "waiting", Finished: 1, Turn: "first", LastCompleted: "first", TurnStatus: "completed"}
	r := waitFixture(t, node, id, s)
	sealed := filepath.Join(r.dir, "result", "turn-1")
	const patch = "sample.patch"
	patchData := strings.Repeat("patch content\n", 12000)
	snapWriteTest(t, filepath.Join(sealed, patch), patchData)
	snapWriteTest(t, filepath.Join(sealed, "last.md"), "previous report")
	if err := writeJSON(filepath.Join(sealed, "result.json"), turnResult{Turn: 1, TurnID: "first", Repositories: []turnPatch{{SnapshotPatch: SnapshotPatch{Patch: patch}}}}); err != nil {
		t.Fatal(err)
	}
	source := t.TempDir()
	snapWriteTest(t, filepath.Join(source, "source.txt"), "fresh source")
	original, _, err := Freeze(ctx, source, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	origin, err := originFile(host, id, s.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Snapshot: original, Source: source, JobOptions: []string{"--engine", "claude", "--network", "--idle-exit", "1s"}}); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 8, nil, f.cx, "rebase", host+":"+id, "repair")
	dest, err := resultDest(host, id, s.RunID, "1")
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Text, Child, Failure string
		Submitted            bool
	}
	if err = readJSON(dest+".rebase.json", &request); err != nil || request.Text != "repair" || request.Child != child || request.Failure == "" || request.Submitted {
		t.Fatalf("request after failed dispatch = %+v, %v", request, err)
	}
	queued, err := submissionDir(host, child)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(queued, "input.json")
	before, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(blockDispatch); err != nil {
		t.Fatal(err)
	}
	r.s.Finished, r.s.Turn, r.s.LastCompleted = 2, "second", "second"
	if err = r.save(); err != nil {
		t.Fatal(err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "rebase", "--turn", "1", host+":"+id, "ignored after claim"); strings.TrimSpace(string(out)) != child {
		t.Fatalf("rebase receipt: %s", out)
	}
	var completed rebaseRequest
	if err = readJSON(dest+".rebase.json", &completed); err != nil || !completed.Submitted || completed.Failure != "" {
		t.Fatalf("completed request = %+v, %v", completed, err)
	}
	if after, readErr := os.ReadFile(input); readErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("retry changed submission input: %v", readErr)
	}
	work, err := jobDir(filepath.Join(node, ".work"), child)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(work, "context", "rebase")
	var m turnResult
	if err = readJSON(filepath.Join(dir, "result.json"), &m); err != nil || len(m.Repositories) != 1 || m.Repositories[0].Patch != patch {
		t.Fatalf("node result manifest = %+v, %v", m, err)
	}
	if b, readErr := os.ReadFile(filepath.Join(dir, m.Repositories[0].Patch)); readErr != nil || string(b) != patchData {
		t.Fatalf("node patch = %d bytes, %v", len(b), readErr)
	}
	if b, readErr := os.ReadFile(filepath.Join(dir, "last.md")); readErr != nil || string(b) != "previous report" {
		t.Fatalf("node report = %q, %v", b, readErr)
	}
	job, err := jobDir(node, child)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = awaitFixtureStatus(ctx, job, func(s status) bool { return isTerminal(s.State) }); err != nil {
		t.Fatalf("waiting for child to exit: %v", err)
	}
}

func TestRebaseReturnsExistingClaimAtSealBoundary(t *testing.T) {
	f := newControllerFixture(t)
	const id, child = "p/sealed", "p/sealed-r1"
	s := status{Job: "sealed", Project: "p", RunID: "fixture", Engine: "claude",
		State: "waiting", Finished: 1, Turn: "first", LastCompleted: "first", TurnStatus: "completed"}
	waitFixture(t, os.Getenv("CXRUN_STATE"), id, s)
	origin, err := originFile("local", id, s.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Snapshot: "fixture"}); err != nil {
		t.Fatal(err)
	}
	dest, err := resultDest("local", id, s.RunID, "1")
	if err != nil {
		t.Fatal(err)
	}
	// Hold the worker lease at the point after claiming, before submitting.
	slot := dest + ".rebase.json"
	lease, err := lockPath(slot+".worker", os.O_CREATE)
	if err != nil || lease == nil {
		t.Fatalf("fixture worker lease = %v, %v", lease, err)
	}
	defer closeBestEffort(lease)
	if err = writeJSON(slot, fields{"Text": "original", "Child": child}); err != nil {
		t.Fatal(err)
	}
	if out := cliOutput(t, 0, nil, f.cx, "rebase", "local:"+id, "late request"); strings.TrimSpace(string(out)) != child {
		t.Fatalf("existing claim: %s", out)
	}
	var request struct{ Text, Child string }
	if err = readJSON(slot, &request); err != nil || request.Text != "original" || request.Child != child {
		t.Fatalf("claim changed = %+v, %v", request, err)
	}
}

func TestRebaseCarriesCachedChangesToAnotherHost(t *testing.T) {
	f := newControllerFixture(t)
	ctx := snapTestCtx(t)
	const host, id, child, generation = "offline", "p/carry", "p/carry-r1", "fixture"
	config := filepath.Join(f.home, ".config", "cx", "hosts.toml")
	snapWriteTest(t, config, "[hosts.offline]\nssh = \"fixture\"\ncx = \"cx\"\nstate = \"offline\"\n")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	writeCredentialFixture(t, filepath.Join(f.home, "ssh"), "#!/bin/sh\nprintf 'cx-wire: ready\\n' >&2\nexit 9\n", 0700)
	writeCredentialFixture(t, filepath.Join(os.Getenv("CXRUN_STATE"), "host.json"),
		`{"min_free_bytes":0,"profiles":{"target":{"engine_homes":{"claude":"target-home"}}}}`, 0600)
	writeCredentialFixture(t, filepath.Join(f.home, "claude"), `#!/bin/sh
set -eu
test "${CLAUDE_CONFIG_DIR##*/}" = target-home
test "$(head -n 1 sample)" = 'source change'
test "$(tail -n 1 sample)" = 'job change'
read -r input
printf '%s\n' '{"type":"result","session_id":"fixture","result":"STATUS: DONE"}'
`, 0700)
	source := t.TempDir()
	path := filepath.Join(source, "sample")
	const initial = "first\n2\n3\n4\n5\n6\n7\nlast\n"
	snapWriteTest(t, path, initial)
	g := snapGit{ctx: ctx, dir: source}
	for _, args := range [][]string{{"init", "-q"}, {"add", "sample"}, {"-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "fixture"}} {
		if _, gitErr := g.get(args...); gitErr != nil {
			t.Fatal(gitErr)
		}
	}
	original, manifest, err := Freeze(ctx, source, FreezeOptions{History: "HEAD"})
	if err != nil {
		t.Fatal(err)
	}
	origin, err := originFile(host, id, generation)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Snapshot: original, Source: source, History: "HEAD",
		JobOptions: []string{"--engine", "claude", "--network", "--profile", "old-account", "--idle-exit", "1s"}}); err != nil {
		t.Fatal(err)
	}
	dest, err := resultDest(host, id, generation, "2")
	if err != nil {
		t.Fatal(err)
	}
	cached := resultVariant(dest, true)
	repo := manifest.Repositories[0]
	const patch = "diff --git a/sample b/sample\n--- a/sample\n+++ b/sample\n@@ -6,3 +6,3 @@\n 6\n 7\n-last\n+job change\n"
	snapWriteTest(t, filepath.Join(cached, "sample.patch"), patch)
	snapWriteTest(t, filepath.Join(cached, "last.md"), "previous report")
	if err = writeJSON(filepath.Join(cached, "result.json"), turnResult{
		Turn: 2, TurnID: "second", Repositories: []turnPatch{{SnapshotPatch: SnapshotPatch{
			Path: repo.Path, RepoID: repo.RepoID, SnapshotOID: repo.OID, Patch: "sample.patch",
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	marker, seen, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = publishResultMarker(ctx, marker, seen, generation, 2); err != nil {
		t.Fatal(err)
	}
	args := []string{"rebase", host + ":" + id, "continue the work", "--host", "local", "--profile", "target"}

	conflicting := strings.Replace(initial, "last", "source change", 1)
	snapWriteTest(t, path, conflicting)
	_, diagnostic := cliStreams(t, 1, nil, f.cx, args...)
	if !strings.Contains(diagnostic, "conflicting files: sample") {
		t.Fatalf("missing conflict diagnostic: %s", diagnostic)
	}
	queued, err := submissionDir("local", child)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(queued, "input.json")
	if _, statErr := os.Stat(input); !os.IsNotExist(statErr) {
		t.Fatalf("unexpected submission after conflict: %v", statErr)
	}
	if data, readErr := os.ReadFile(path); readErr != nil || string(data) != conflicting {
		t.Fatalf("source after conflict = %q, %v", data, readErr)
	}

	updated := strings.Replace(initial, "first", "source change", 1)
	snapWriteTest(t, path, updated)
	index := filepath.Join(source, ".git", "index")
	before, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	out, diagnostic := cliStreams(t, 0, nil, f.cx, append(args, "--turn", "2")...)
	if strings.TrimSpace(string(out)) != child || !strings.Contains(diagnostic, "using cached turn 2") {
		t.Fatalf("offline rebase: %s %s", out, diagnostic)
	}
	var q queuedJob
	if err = readJSON(input, &q); err != nil {
		t.Fatal(err)
	}
	if q.Request.Host != "local" || canonicalPath(q.Origin.Source) != canonicalPath(source) || q.Origin.History != "HEAD" ||
		!strings.Contains(q.Request.Prompt, "checkout already includes") ||
		string(q.Request.Context["rebase/last.md"]) != "previous report" ||
		string(q.Request.Context["rebase/sample.patch"]) != patch {
		t.Fatalf("unexpected child submission: %+v", q)
	}
	merged, err := loadSnapshot(q.Request.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	store, err := snapStore()
	if err != nil {
		t.Fatal(err)
	}
	row := merged.Repositories[0]
	content, err := (snapGit{ctx: ctx, dir: row.bare(store)}).get("show", row.OID+":sample")
	if err != nil || content != strings.TrimSpace(strings.Replace(updated, "last", "job change", 1)) ||
		row.Base != repo.Base || row.RepoID != repo.RepoID {
		t.Fatalf("snapshot content = %q, repository = %+v, error = %v", content, row, err)
	}
	if data, readErr := os.ReadFile(path); readErr != nil || string(data) != updated {
		t.Fatalf("source after dispatch = %q, %v", data, readErr)
	}
	if data, readErr := os.ReadFile(index); readErr != nil || !bytes.Equal(data, before) {
		t.Fatalf("source index changed: %v", readErr)
	}
	job, err := jobDir(os.Getenv("CXRUN_STATE"), child)
	if err != nil {
		t.Fatal(err)
	}
	s, err := awaitFixtureStatus(ctx, job, func(s status) bool { return isTerminal(s.State) })
	if err != nil || s.Profile != "target" || s.State != "done" {
		t.Fatalf("child status = %+v, %v", s, err)
	}
}
