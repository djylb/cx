package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

// After `cx wait` reports a completed turn, `cx result` without --wait must read the
// local cache.
func TestWaitPrefetchesResult(t *testing.T) {
	f := newControllerFixture(t)
	_, src, prompt := runnerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	t.Setenv("CX_POLICY_DIR", f.home)
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("FIXMODE", "sealed-patches")
	t.Setenv("FIXPROMPT", "Fixture\n"+jobToolsNote+strings.TrimRight(sandboxNote, "\n")+"\n\ntask")
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
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "prefetch", prompt, "--sync", "--idle-exit", "2m")
	defer cliOutput(t, 0, nil, f.cx, "close", id)
	cliOutput(t, 0, nil, f.cx, "wait", id, "--timeout", "2m")
	out, diag := cliStreams(t, 0, nil, f.cx, "result", id)
	if strings.Contains(string(out), "collecting") || !strings.Contains(diag, "last.md") {
		t.Fatalf("result after wait was not cached: %s %s", out, diag)
	}
}

func TestWaitPrefetchesSelectedTurn(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	before := status{RunID: "run", Finished: 1, LastCompleted: "first", Turn: "second", TurnStatus: "inProgress"}
	now := status{RunID: "run", Job: "prefetch", State: "waiting", Finished: 3, LastCompleted: "third", CompletedStatus: "completed"}
	want := observedResult{RunID: "run", Number: 2, TurnID: "second"}
	obs := waitResult(before, now, true, &turnCompletion{observedResult: want, Outcome: "completed"})
	if obs.Result == nil || *obs.Result != want {
		t.Fatalf("wait selected %+v, want %+v", obs.Result, want)
	}
	fixtureArchive := writePrefetchArchive(t, ctx, want)
	waitReply, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}
	setupPrefetchSSH(t, f, `
case "$request" in
  *'"observe-one"'*)
    printf '%s\n' 'job=prefetch status=waiting completed_turn=second outcome=completed' `+shellQuote(string(waitReply))+`
    ;;
  *'"result-download","p/prefetch","run","2","0"'*)
    printf '%s\n' "$(wc -c < `+shellQuote(fixtureArchive)+` | tr -d ' ')"
    cat `+shellQuote(fixtureArchive)+`
    ;;
  *) exit 44 ;;
esac
`)
	out := cliOutput(t, 0, nil, f.cx, "wait", "remote:p/prefetch", "--timeout", "2m")
	if !bytes.Contains(out, []byte("completed_turn=second")) || bytes.Contains(out, []byte("run_id")) {
		t.Fatalf("wait returned %s", out)
	}
	out = cliOutput(t, 0, nil, f.cx, "result", "remote:p/prefetch", "--turn", "2", "--full")
	if !bytes.Contains(out, []byte("report for second")) {
		t.Fatalf("result returned %s", out)
	}
	assertPrefetchRequests(t, f.home)
}

func TestWaitPrefetchUnavailableAndSlow(t *testing.T) {
	for _, scenario := range []struct {
		name, action string
	}{
		{"unavailable", "exit 44"},
		{"silent", "exec sleep 120"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newControllerFixture(t)
			setupPrefetchSSH(t, f, `
case "$request" in
  *'"observe-one"'*)
    printf '%s\n' 'job=prefetch status=waiting completed_turn=second outcome=completed' \
      '{"job":"prefetch","status":"waiting","result":{"run_id":"run","turn":2,"turn_id":"second"}}'
    ;;
  *'"result-download","p/prefetch","run","2","0"'*) `+scenario.action+` ;;
  *) exit 44 ;;
esac
`)
			ctx, cancel := fixtureContext(t)
			defer cancel()
			clock := timeshim.NewFake(time.Now())
			h := endpoint{SSH: "fixture", CX: "cx"}
			done := make(chan error, 1)
			go func() { done <- waitOne(clockContext(ctx, clock), h, "remote", "p/prefetch", time.Minute) }()
			if scenario.name == "silent" {
				if err := awaitFixtureCondition(ctx, func() (bool, error) {
					data, err := os.ReadFile(filepath.Join(f.home, "requests"))
					if os.IsNotExist(err) {
						return false, nil
					}
					return bytes.Contains(data, []byte("result-download")), err
				}); err != nil {
					t.Fatal(err)
				}
				if err := clock.WaitDeadline(ctx, clock.Now().Add(prefetchLimit)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(prefetchLimit)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			assertPrefetchRequests(t, f.home)
		})
	}
}

func setupPrefetchSSH(t *testing.T, f controllerFixture, script string) {
	t.Helper()
	config := filepath.Join(f.home, ".config", "cx")
	if err := os.MkdirAll(config, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "hosts.toml"), []byte("[hosts.remote]\nssh='fixture'\ncx='cx'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	head := "#!/bin/sh\nIFS= read -r request || :\nprintf '%s\\n' \"$request\" >> " + shellQuote(filepath.Join(f.home, "requests")) + "\nprintf 'cx-wire: ready\\n' >&2\n"
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(head+script), 0700); err != nil {
		t.Fatal(err)
	}
}

func assertPrefetchRequests(t *testing.T, home string) {
	t.Helper()
	requests, err := os.ReadFile(filepath.Join(home, "requests"))
	lines := strings.Split(strings.TrimSpace(string(requests)), "\n")
	if err != nil || len(lines) != 2 || !strings.Contains(lines[0], `"observe-one"`) ||
		!strings.Contains(lines[1], `"result-download","p/prefetch","run","2","0"`) {
		t.Fatalf("expected one observation and one selected download, got %s (%v)", requests, err)
	}
}

func writePrefetchArchive(t *testing.T, ctx context.Context, result observedResult) string {
	t.Helper()
	dir := t.TempDir()
	if err := writeJSON(filepath.Join(dir, "result.json"), turnResult{Turn: result.Number, TurnID: result.TurnID}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "last.md"), []byte("report for "+result.TurnID+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	archive, err := resultArchive(ctx, dir, dir, status{}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { removeBestEffort(archive) })
	return archive
}

func TestResultMarkerRejectsLatePublication(t *testing.T) {
	t.Setenv("CX_HOME", t.TempDir())
	ctx, cancel := fixtureContext(t)
	defer cancel()
	const host, id = "local", "p/prefetch"
	marker, before, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(seen resultReference, gen string, n int64) {
		t.Helper()
		if publishErr := publishResultMarker(ctx, marker, seen, gen, n); publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	check := func(want resultReference) {
		t.Helper()
		got, readErr := readResultReference(marker)
		if readErr != nil || got != want {
			t.Fatalf("marker = %+v, want %+v (%v)", got, want, readErr)
		}
	}
	publish(before, "old", 3)
	publish(before, "old", 2)
	check(resultReference{Generation: "old", Turn: 3})
	_, old, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	publish(old, "new", 1)
	publish(old, "old", 4)
	check(resultReference{Generation: "new", Turn: 1})
	_, current, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = forgetResult(host, id); err != nil {
		t.Fatal(err)
	}
	publish(current, "new", 2)
	check(resultReference{Epoch: 1})
	_, epoch, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	publish(epoch, "new", 2)
	check(resultReference{Generation: "new", Turn: 2, Epoch: 1})
}

func TestWaitPrefetchKeepsNewerGeneration(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	const host, id = "remote", "p/prefetch"
	target := observedResult{RunID: "old", Number: 2, TurnID: "second"}
	archive := writePrefetchArchive(t, ctx, target)
	entered, release := filepath.Join(f.home, "entered"), filepath.Join(f.home, "release")
	setupPrefetchSSH(t, f, `
case "$request" in
  *'"observe-one"'*)
    printf '%s\n' 'job=prefetch status=waiting completed_turn=second outcome=completed' \
      '{"job":"prefetch","status":"waiting","result":{"run_id":"old","turn":2,"turn_id":"second"}}'
    ;;
  *'"result-download","p/prefetch","old","2","0"'*)
    touch `+shellQuote(entered)+`
    while [ ! -f `+shellQuote(release)+` ]; do sleep .01; done
    printf '%s\n' "$(wc -c < `+shellQuote(archive)+` | tr -d ' ')"
    cat `+shellQuote(archive)+`
    ;;
  *) exit 44 ;;
esac
`)
	clock := timeshim.NewFake(time.Now())
	h := endpoint{SSH: "fixture", CX: "cx"}
	done := make(chan error, 1)
	go func() { done <- waitOne(clockContext(ctx, clock), h, host, id, time.Minute) }()
	if err := awaitFixtureFile(ctx, entered); err != nil {
		t.Fatal("old prefetch did not start", err)
	}
	marker, seen, err := resultMarkerSnapshot(host, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = publishResultMarker(ctx, marker, seen, "new", 1); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	dest, err := resultDest(host, id, target.RunID, "2")
	if err != nil {
		t.Fatal(err)
	}
	if err = validateResultTurn(resultVariant(dest, false), target); err != nil {
		t.Fatalf("old download did not reach publication: %v", err)
	}
	if now, err := readResultReference(marker); err != nil || now.Generation != "new" || now.Turn != 1 {
		t.Fatalf("old download changed the cache marker: %+v, %v", now, err)
	}
}

func TestCanceledResultUnpackKeepsDownload(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	target := observedResult{RunID: "run", Number: 2, TurnID: "second"}
	archive := writePrefetchArchive(t, ctx, target)
	dest := filepath.Join(t.TempDir(), "result")
	part := dest + ".part"
	data, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(part, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(dest+".total", len(data)); err != nil {
		t.Fatal(err)
	}
	installCtx, stop := context.WithCancel(ctx)
	defer stop()
	canceled := cancelDuringUnpack{Context: installCtx, dir: filepath.Dir(dest), cancel: stop}
	if err = installResultTurn(canceled, part, dest, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled unpack: %v", err)
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("canceled unpack published destination: %v", err)
	}
	if err = fetchResultTurn(ctx, endpoint{}, "p/prefetch", dest, false, target); err != nil {
		t.Fatalf("complete download should be reusable locally: %v", err)
	}
	if _, err = os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("installed archive left its partial file: %v", err)
	}
}

// Cancel at the first read after extraction has opened its output file.
type cancelDuringUnpack struct {
	context.Context
	cancel context.CancelFunc
	dir    string
}

func (c cancelDuringUnpack) Err() error {
	reports, _ := filepath.Glob(filepath.Join(c.dir, ".unpack-*", "last.md"))
	if len(reports) > 0 {
		c.cancel()
	}
	return c.Context.Err()
}
