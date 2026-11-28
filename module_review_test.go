package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/djylb/timeshim"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func reviewWork(name, prefix string) moduleWork {
	return moduleWork{Modules: []string{name}, Definitions: []moduleDefinition{{Name: name, Paths: []string{prefix}}}}
}

func TestModulePathEquivalence(t *testing.T) {
	current := reviewWork("active", "SRC/API/**")
	for _, platform := range []string{"linux/amd64", "windows/amd64", "darwin/arm64"} {
		fold := foldTargetPaths(platform)
		conflict, _ := moduleClaimRelation(current, "", []moduleClaim{{Name: "peer", Paths: []string{"src/api/child"}}}, fold)
		if (len(conflict) != 0) != fold {
			t.Fatalf("%s conflict = %v", platform, conflict)
		}
		files := hotspotFiles([]string{"src/API/GEN.GO"}, []string{"src/api/*.go"}, fold)
		if (len(files) != 0) != fold {
			t.Fatalf("%s hotspots = %v", platform, files)
		}
	}
	if paths := commandReadPaths(`cat src\file.go "docs\with spaces.md"`, true); !slices.Equal(paths, []string{`src\file.go`, `docs\with spaces.md`}) {
		t.Fatalf("Windows command reads = %q", paths)
	}
}

func TestWindowsModulePathEquivalence(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("uses a native Windows checkout")
	}
	root := t.TempDir()
	work := filepath.Join(root, "checkout")
	file := filepath.Join(work, "Src", "File.go")
	snapWriteTest(t, file, "package source\n")
	alternative := filepath.Join(work, "src", "file.GO")
	if _, err := os.Stat(alternative); err != nil {
		t.Fatalf("Windows checkout path is not case equivalent: %v", err)
	}
	r := runner{root: root, c: config{Dir: work}}
	r.recordReadPaths("turn", work, commandReadPaths(`cat src\file.GO`, true), "parsed")
	r.recordReadPaths("turn", work, []string{file}, "structured")
	if !slices.Equal(r.reads.paths, []readObservation{{Path: "src/file.GO", Source: "structured"}}) {
		t.Fatalf("Windows path variants were not merged: %+v", r.reads.paths)
	}
}

func TestModuleOutputBudget(t *testing.T) {
	var out bytes.Buffer
	bounded := budgetOutput{dst: &out, left: 4}
	if _, err := io.WriteString(&bounded, "abcdef"); err != nil || !bounded.truncated || out.String() != "abcd" {
		t.Fatalf("bounded stream %q, truncated=%t, err=%v", out.String(), bounded.truncated, err)
	}
	if _, err := decodeModuleTable(strings.NewReader("{} "), 2); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("overflow after valid JSON: %v", err)
	}
	if _, err := decodeModuleTable(strings.NewReader("{}"), 2); err != nil {
		t.Fatal(err)
	}
}

func TestModuleRestartChecksClaimsCLI(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := filepath.Join(f.home, "state")
	work := reviewWork("api", "api")
	for _, fixture := range []struct{ name, state string }{{"ended", "done"}, {"holder", "waiting"}} {
		dir := filepath.Join(root, "p", fixture.name)
		run := fixture.name + "-run"
		cfg := config{Work: &work, Dir: f.home, Engine: "codex", RunID: run}
		s := status{Project: "p", Job: fixture.name, State: fixture.state, Engine: "codex", RunID: run}
		if err := registerFixtureJob(ctx, filepath.Dir(dir), dir, cfg, s, (*stateWriter).sync); err != nil {
			t.Fatal(err)
		}
	}
	lease, err := lockPath(filepath.Join(root, "p", "holder", "owner.lock"), os.O_CREATE)
	if err != nil || lease == nil {
		t.Fatalf("holder lease: %v", err)
	}
	defer closeBestEffort(lease)
	for _, args := range [][]string{
		{"resume", "local:p/ended"},
		{"say", "local:p/ended", "continue"},
		{"fork", "local:p/ended", "--job", "child"},
	} {
		_, diagnostic := cliStreams(t, 15, nil, f.cx, args...)
		if !strings.Contains(diagnostic, "local:p/holder") {
			t.Fatalf("%v missing claim holder: %s", args, diagnostic)
		}
	}
	_, diagnostic := cliStreams(t, 1, nil, f.cx, "say", "local:p/ended", "continue", "--module-override", "coordinated restart")
	if !strings.Contains(diagnostic, "coordinated restart") || !strings.Contains(diagnostic, "no saved thread") {
		t.Fatalf("restart override: %s", diagnostic)
	}
}

func TestModuleTableModesAndUncheckedCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, "config"))
	snapWriteTest(t, filepath.Join(f.home, ".config", "cx", "hosts.toml"), "[hosts.unchecked]\nssh = \"unchecked\"\nplatform = \"linux/amd64\"\n")
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	src := filepath.Join(f.home, "src")
	snapWriteTest(t, filepath.Join(src, "modules.json"), `{"modules":[{"name":"api","paths":["api"]}]}`)
	project := filepath.Join(f.home, "config", "cx", "projects", "p.toml")
	snapWriteTest(t, project, "modules_cmd = [\"sh\", \"-c\", \"exit 19\"]\n")
	cliOutput(t, 0, nil, f.cx, "submit", "local", src, "plain", "--sync", "--cmd", "--", "sh", "-c", "exit 0")
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/plain", "--timeout", "10s")
	cliOutput(t, 2, nil, f.cx, "submit", "local", src, "explicit", "--module", "api", "--cmd", "--", "sh", "-c", "exit 0")
	snapWriteTest(t, project, "modules_file = \"modules.json\"\n")
	out, diagnostic := cliStreams(t, 0, nil, f.cx, "run", "local", src, "explicit", "--module", "api", "--json", "--sync", "--cmd", "--", "sh", "-c", "exit 0")
	var submission struct {
		Job             string
		ClaimsUnchecked []string `json:"claims_unchecked"`
	}
	if err := json.Unmarshal(out, &submission); err != nil || submission.Job != "p/explicit" ||
		!slices.Equal(submission.ClaimsUnchecked, []string{"unchecked"}) || !strings.Contains(diagnostic, "claims_unchecked: unchecked") {
		t.Fatalf("unchecked submission: %s, stderr %q (%v)", out, diagnostic, err)
	}
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/explicit", "--timeout", "10s")
}

func TestFirstTurnEmptyStructuredUsesPreamble(t *testing.T) {
	h := newRuns(t, "complete")
	t.Setenv("FIXPROMPT", "context only")
	snapWriteTest(t, h.prompt, "")
	preamble := filepath.Join(t.TempDir(), "preamble")
	snapWriteTest(t, preamble, "context only")
	if err := h.finishFirstTurn("job", "--preamble-file", preamble, "--input-json", "[]"); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshModuleScopeUsesNewTable(t *testing.T) {
	f := newControllerFixture(t)
	ctx := snapTestCtx(t)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, "config"))
	src := filepath.Join(t.TempDir(), "repo")
	snapWriteTest(t, filepath.Join(src, "modules.json"), `{"modules":[{"name":"api","paths":["old/**"]}],"hotspots":["old.txt"]}`)
	cliOutput(t, 0, nil, "git", "-C", src, "init", "-q")
	cliOutput(t, 0, nil, "git", "-C", src, "add", ".")
	cliOutput(t, 0, nil, "git", "-C", src, "-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "baseline")
	snapWriteTest(t, filepath.Join(f.home, "config", "cx", "projects", "p.toml"), "modules_file = \"modules.json\"\n")
	id, manifest, err := Freeze(ctx, src, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const job = "p/refresh"
	initial := status{Project: "p", Job: "refresh", RunID: "fixture", Engine: "codex", State: "running", Turn: "first", TurnStatus: "inProgress"}
	r := waitFixture(t, os.Getenv("CXRUN_STATE"), job, initial)
	r.log, err = os.Create(filepath.Join(r.dir, "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(r.log)
	work := filepath.Join(os.Getenv("CXRUN_STATE"), ".work", job)
	prepared, err := Materialize(ctx, work, manifest, "")
	if err != nil {
		t.Fatal(err)
	}
	r.c.Dir = prepared.Source
	req := jobSubmission{ID: job, Host: "local", Snapshot: id}
	if err = prepareModuleWork(ctx, &req, projectDefinition{ModulesFile: "modules.json"}, ctlOptions{modules: []string{"api"}}); err != nil {
		t.Fatal(err)
	}
	r.c.Work = &req.moduleWork
	if err = r.stateLog.Append("config", r.c); err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(filepath.Join(work, "request.json"), req); err != nil {
		t.Fatal(err)
	}
	origin, err := originFile("local", job, initial.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(origin, submissionOrigin{Source: src, Snapshot: id, ModuleOptions: []string{"--module", "api"}}); err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(src, "modules.json"), `{"modules":[{"name":"api","paths":["new/**"],"accept":[["new-check"]]}],"hotspots":["new.txt"]}`)
	cliOutput(t, 0, nil, f.cx, "refresh", "local:"+job)
	if err = r.finish(turn{ID: "first", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if question, refreshErr := r.refreshBeforeTurn(); question || refreshErr != nil {
		t.Fatalf("refresh activation: question=%t, err=%v", question, refreshErr)
	}
	if r.c.Work == nil || len(r.c.Work.Definitions) != 1 ||
		!slices.Equal(r.c.Work.Definitions[0].Paths, []string{"new/**"}) ||
		!slices.Equal(r.c.Work.Hotspots, []string{"new.txt"}) {
		t.Fatalf("effective config: %+v", r.c.Work)
	}
	for _, expected := range []struct {
		n       int
		hotspot string
	}{{1, "old.txt"}, {2, "new.txt"}} {
		if expected.n == 2 {
			snapWriteTest(t, filepath.Join(prepared.Source, "new.txt"), "new hotspot\n")
			if err = r.finish(turn{ID: "second", Status: "completed"}); err != nil {
				t.Fatal(err)
			}
		}
		folder, err := sealTurn(ctx, r.dir, r.s, int64(expected.n))
		if err != nil {
			t.Fatal(err)
		}
		var turn turnResult
		if err = readJSON(filepath.Join(folder, "result.json"), &turn); err != nil || !slices.Equal(turn.Hotspots, []string{expected.hotspot}) {
			t.Fatalf("turn %d hotspots %v (%v)", expected.n, turn.Hotspots, err)
		}
	}
}

func TestModuleCommandCloseBeforeMainCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, "config"))
	ready := filepath.Join(f.home, "context-ready")
	main := filepath.Join(f.home, "main-started")
	t.Setenv("FIXTURE_CONTEXT_READY", ready)
	t.Setenv("FIXTURE_MAIN_STARTED", main)
	src := filepath.Join(f.home, "repo")
	snapWriteTest(t, filepath.Join(src, "modules.json"), `{"modules":[{"name":"api","context":["sh","-c","touch \"$FIXTURE_CONTEXT_READY\"; exec sleep 120"]}]}`)
	snapWriteTest(t, filepath.Join(f.home, "config", "cx", "projects", "p.toml"), "modules_file = \"modules.json\"\n")
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "closing", "--module", "api", "--sync", "--cmd", "--", "sh", "-c", `touch "$FIXTURE_MAIN_STARTED"`)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	if err := awaitFixtureFile(ctx, ready); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 0, nil, f.cx, "close", "local:p/closing")
	if _, err := os.Stat(main); !os.IsNotExist(err) {
		t.Fatalf("main command started after close: %v", err)
	}
	in, err := readInboxState(filepath.Join(f.home, "state", "p", "closing"))
	if err != nil || in.acks[1].Result != "delivered" {
		t.Fatalf("close acknowledgement %v (%v)", in, err)
	}
}

func TestAcceptancePendingDoesNotBlockNextTurnCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("FIXMODE", "sealed-patches")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CX_POLICY_DIR", f.home)
	snapWriteTest(t, filepath.Join(f.home, "preamble.md"), "")
	if err := os.Symlink(f.cx, filepath.Join(f.home, "codex")); err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(f.home, "repo")
	snapWriteTest(t, filepath.Join(repo, "a"), "base\n")
	snapWriteTest(t, filepath.Join(repo, "undo"), "base\n")
	prompt := filepath.Join(f.home, "prompt")
	snapWriteTest(t, prompt, "task")
	started, release := filepath.Join(f.home, "check.started"), filepath.Join(f.home, "check.release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	check := fmt.Sprintf("printf ready > '%s'; while test ! -f '%s'; do sleep 0.01; done; test -f a", started, release)
	cliOutput(t, 0, nil, f.cx, "run", "local", repo, "independent", prompt, "--sync", "--no-tools", "--context", "none", "--accept", check)
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Second)
	defer cancel()
	awaitFixture := func(path string) {
		t.Helper()
		if err := awaitFixtureCondition(ctx, func() (bool, error) {
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				return false, nil
			}
			return err == nil, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	awaitFixture(started)
	result := func(turn, status string) resultOutput {
		t.Helper()
		out := cliOutput(t, 0, nil, f.cx, "result", "p/independent", "--turn", turn, "--wait", "--timeout", "12s", "--json")
		var result resultOutput
		if err := json.Unmarshal(out, &result); err != nil || result.AcceptanceStatus != status {
			t.Fatalf("result %s = %s (%v)", turn, out, err)
		}
		return result
	}
	first := result("1", "pending")
	cliOutput(t, 0, nil, f.cx, "say", "p/independent", "second round")
	result("2", "pending")
	snapWriteTest(t, release, "go")
	nodeResult := filepath.Join(f.home, "state", ".work", "p", "independent", "result", "turn-1", acceptanceFilename)
	awaitFixture(nodeResult)
	loaded := result("1", "passed")
	if loaded.PatchManifest != first.PatchManifest || len(loaded.Acceptance) != 1 {
		t.Fatalf("acceptance did not update the cached result independently: %+v", loaded)
	}
	cliOutput(t, 0, nil, f.cx, "close", "p/independent")
}

func TestOutboxReplaysPublishedTurnAttachments(t *testing.T) {
	f := newControllerFixture(t)
	s := status{Project: "p", Job: "replay", Engine: "codex", RunID: "job-run", State: "waiting", Finished: 1, LastCompleted: "engine-turn", CompletedStatus: "completed"}
	r := waitFixture(t, filepath.Join(f.home, "state"), "p/replay", s)
	warning := &contextWarning{Turn: 1, Percent: 80, Threshold: 60}
	if err := r.publishAgentTurn(turn{ID: s.LastCompleted, Status: "completed"}, []byte("STATUS: DONE"), nil, warning, true); err != nil {
		t.Fatal(err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	s.State = "done"
	if err := r.stateLog.Final(s); err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(r.dir, "result", "turn-1")
	snapWriteTest(t, filepath.Join(folder, acceptanceFilename), `{"status":"passed"}`)
	for range 2 {
		out := cliOutput(t, 0, nil, f.cx, "node", "outbox", "--job", "p/replay")
		if strings.Count(string(out), `"type":"acceptance"`) != 1 || strings.Count(string(out), `"type":"context_warning"`) != 1 {
			t.Fatalf("outbox replay = %s", out)
		}
	}
}

func TestManagedDrainDeadlineAfterLeaderExit(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	clock := timeshim.NewFake(time.Unix(1000, 0))
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(writer)
	defer closeBestEffort(reader)
	pipe := &commandPipe{read: reader, done: make(chan error, 1)}
	go func() {
		_, copyErr := io.Copy(io.Discard, reader)
		pipe.done <- copyErr
	}()
	exited := make(chan processWait, 1)
	exited <- processWait{}
	child := &managedCommand{done: exited, pipes: []*commandPipe{pipe}}
	if waitErr, stopErr := child.wait(ctx, clock, nil); waitErr != nil || stopErr != nil {
		t.Fatalf("leader: %v, %v", waitErr, stopErr)
	}
	drained := make(chan error, 1)
	go func() { drained <- child.finish() }()
	if err = clock.WaitDeadline(ctx, clock.Now().Add(outputDrainLimit)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(outputDrainLimit)
	select {
	case err = <-drained:
		if managedOutputStatus(err) != "truncated" || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("open inherited pipe: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestActiveResumeKeepsReceiptAndRejectsStaleIdentity(t *testing.T) {
	f := newControllerFixture(t)
	root := filepath.Join(f.home, "state")
	s := status{Project: "p", Job: "restart", Engine: "codex", RunID: "running", State: "running"}
	r := waitFixture(t, root, "p/restart", s)
	identity, _, _, err := captureJobIdentity(r.dir)
	if err != nil {
		t.Fatal(err)
	}
	binding := jobClaimInfo{RunID: identity.run, Directory: identity.key, ActiveOnly: true, Baseline: "previous baseline"}
	args := []string{"resume", "local", "p/restart", "", "", "", ""}
	var receipt bytes.Buffer
	if err := restartNode(t.Context(), args, nil, &receipt, binding); err != nil {
		t.Fatal(err)
	}
	var observed status
	if err := json.Unmarshal(receipt.Bytes(), &observed); err != nil || observed.RunID != s.RunID || observed.State != s.State {
		t.Fatalf("active resume receipt = %s, %v", receipt.Bytes(), err)
	}
	binding.Directory = "previous directory"
	if err := restartNode(t.Context(), args, nil, io.Discard, binding); !errors.Is(err, errJobReplaced) {
		t.Fatalf("stale directory resumed: %v", err)
	}
}
