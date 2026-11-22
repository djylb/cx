package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTerminalCandidateFailureDoesNotPublish(t *testing.T) {
	for _, stage := range []string{"sync", "close", "directory"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			w, release := openTestLogWriter(t, dir)
			defer release()
			if err := w.Append("status", "running"); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected terminal failure")
			active := w.file
			switch stage {
			case "sync":
				w.syncFn = func(f *os.File) error {
					if f != active {
						return fault
					}
					return syncJobFile(f)
				}
			case "close":
				w.closeFn = func(f *os.File) error {
					err := f.Close()
					if f != active {
						return errors.Join(err, fault)
					}
					return err
				}
			case "directory":
				w.syncDirFn = func(string) error { return fault }
			}
			if err := w.Final("done"); !errors.Is(err, fault) {
				t.Fatalf("Final = %v", err)
			}
			record, _, err := latestState(dir)
			if err != nil || record.Final || string(record.Value) != `"running"` {
				t.Fatal(record, err)
			}
		})
	}
}

func TestRotationKeepsOldCheckpointOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	w, release := openTestLogWriter(t, dir)
	defer release()
	w.SetRotateBytes(1)
	if err := w.Append("status", "first"); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate("config", "second"); err != nil {
		t.Fatal(err)
	}
	if err := w.Append("status", "still second"); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected namespace failure")
	w.syncDirFn = func(string) error { return fault }
	if err := w.Rotate("config", "third"); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	if _, err := os.Stat(segmentPath(w.dir, 1)); err != nil {
		t.Fatalf("unflushed snapshot retired its predecessor: %v", err)
	}
}

func TestWorkerMailboxAndCloseRejectRecreatedJob(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "job")
	c := config{RunID: "same-id", Engine: "cmd", Argv: []string{"unexpected"}}
	s := status{Job: "job", Engine: "cmd", State: "done"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	prior, err := deliverClose(t.Context(), dir, markerOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := jobDirectoryKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	if renameErr := os.Rename(dir, filepath.Join(root, "old")); renameErr != nil {
		t.Fatal(renameErr)
	}
	s.State = "queued"
	if registerErr := register(root, dir, c, s); registerErr != nil {
		t.Fatal(registerErr)
	}
	if runErr := run(t.Context(), []string{"_command", "--state", root, "--job", "job",
		"--expect-run", c.RunID, "--expect-dir", key}); !errors.Is(runErr, errJobReplaced) {
		t.Fatalf("stale command worker: %v", runErr)
	}
	record, err := newRecord("msg", inboxMessage{Text: "old"})
	if err != nil {
		t.Fatal(err)
	}
	record.ID = "old"
	if _, err = appendJobMailbox(t.Context(), dir, "inbox", &prior.id, record, nil); !errors.Is(err, errJobReplaced) {
		t.Fatalf("stale mailbox write: %v", err)
	}
	oldSnapshot := prior.status
	oldSnapshot.final = false
	if _, projectErr := projectJobStatus(t.Context(), dir, &oldSnapshot, prior.id); !errors.Is(projectErr, errJobReplaced) {
		t.Fatalf("stale observer projected the new job: %v", projectErr)
	}
	s, cursor, err := loadJobStatus(dir)
	if err != nil || !errors.Is(prior.check(dir, s, cursor), errJobReplaced) || s.PID != 0 {
		t.Fatal(s, cursor, err)
	}
}

func TestCloseIntentIsAcknowledgedWithTerminalCommit(t *testing.T) {
	dir := fixture(t, "running")
	req, err := publishControl(t.Context(), dir, "close", markerOptions{ID: "close"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, s: status{Engine: "codex", State: "running"}}
	startFixtureWriter(t, &r)
	if recoverErr := r.recoverInbox(turn{}); recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if _, ok, takeErr := r.takeMarker("close"); takeErr != nil || !ok {
		t.Fatal(ok, takeErr)
	}
	in, err := readInboxState(dir)
	if err != nil || in.claims[req.Seq].Action != "close" || in.acks[req.Seq].Result != "" {
		t.Fatal(in, err)
	}
	if closeErr := r.stateLog.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	restarted := runner{dir: dir, s: r.s}
	startFixtureWriter(t, &restarted)
	if recoverErr := restarted.recoverInbox(turn{}); recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if _, ok, takeErr := restarted.takeMarker("close"); takeErr != nil || !ok {
		t.Fatal(ok, takeErr)
	}
	restarted.s.State = "done"
	restarted.closeCompleted = true
	if terminalErr := restarted.saveTerminal(); terminalErr != nil {
		t.Fatal(terminalErr)
	}
	in, err = readInboxState(dir)
	if err != nil || in.acks[req.Seq].Result != "delivered" {
		t.Fatal(in, err)
	}
}

func TestOutboxRetryReusesDeadlineAndRequest(t *testing.T) {
	dir := fixture(t, "running")
	id, _, _, err := captureJobIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := agentMail{Type: "ask", To: "parent", Question: "choose", Blocking: true, Timeout: time.Second}
	second := first
	if err := appendMail(t.Context(), dir, id, "op/retry", &first); err != nil {
		t.Fatal(err)
	}
	outbox := filepath.Join(dir, "outbox")
	record, readErr := findMailboxID(outbox, filepath.Join(outbox, ".ids", snapSum([]byte("op/retry"))), "op/retry")
	if readErr != nil || !strings.HasPrefix(string(record.Value), `{"timeout_ns":1000000000,"deadline":`) {
		t.Fatalf("outbox retry requires the existing JSON field order: %s (%v)", record.Value, readErr)
	}
	if err := os.RemoveAll(filepath.Join(dir, "outbox", ".ids")); err != nil {
		t.Fatal(err)
	}
	if err := appendMail(t.Context(), dir, id, "op/retry", &second); err != nil {
		t.Fatal(err)
	}
	if first.Seq != second.Seq || !first.Deadline.Equal(*second.Deadline) || first.Request != second.Request {
		t.Fatal(first, second)
	}
	second.Question = "changed"
	if err := appendMail(t.Context(), dir, id, "op/retry", &second); err == nil {
		t.Fatal("operation ID reused with changed input")
	}
}

func TestTerminalAfterDamagedSegmentRemainsObservable(t *testing.T) {
	dir := fixture(t, "running")
	r := runner{dir: dir, s: status{Job: "j", State: "done"}}
	startFixtureWriter(t, &r)
	if err := r.stateLog.Append("status", status{Job: "j", State: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(r.stateLog.file, "damaged frame"); err != nil {
		t.Fatal(err)
	}
	if err := r.stateLog.Final(r.s); err != nil {
		t.Fatal(err)
	}
	confirmation, err := deliverClose(t.Context(), dir, markerOptions{}, nil)
	if err != nil || confirmation == nil || !confirmation.status.final {
		t.Fatal(confirmation, err)
	}
}

func TestReportRejectsExtraBytes(t *testing.T) {
	dir := t.TempDir()
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	if err := r.publishTurnReport("test", []byte("body")); err != nil {
		t.Fatal(err)
	}
	f, err := openJobFile(filepath.Join(dir, "turns", "test.md"), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("trailing")
	if writeErr := errors.Join(err, f.Close()); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, err := readTurnReport(t.Context(), dir, "test"); err == nil {
		t.Fatal("report reader accepted a trailing byte")
	}
}

func TestResumeRefusesOlderDamagedConsumptionCheckpoint(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "agent")
	c := config{RunID: "r", Engine: "codex", Dir: root}
	s := status{Job: "agent", RunID: c.RunID, Engine: c.Engine, State: "waiting"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	f, err := openJobFile(segmentPath(filepath.Join(dir, "state"), 1), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("unreadable consumption checkpoint\n")
	if writeErr := errors.Join(err, f.Close()); writeErr != nil {
		t.Fatal(writeErr)
	}
	r := runner{dir: dir, c: c, s: s}
	startFixtureWriter(t, &r)
	if err := r.stateLog.Append("status", s); err != nil {
		t.Fatal(err)
	}
	if err := r.stateLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), []string{"resume", "--state", root, "--job", "agent"}); !errors.Is(err, errLogDamage) {
		t.Fatalf("resume accepted the damaged checkpoint: %v", err)
	}
}

func TestFollowResetsAllPositionsOnDirectoryReplacement(t *testing.T) {
	dir := fixture(t, "running")
	pos := tailPosition{Mode: "messages", RunID: "test", Next: 600, Since: 19, Finished: 8, LastCompleted: "old",
		StateCursor: (stateCursor{Seg: 1, Off: 1 << 20, Seq: 20}).String()}
	key, err := jobDirectoryKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos.DirectoryID = key
	// Preserve RunID deliberately so the directory check has to detect it.
	root := t.TempDir()
	next := filepath.Join(root, "next")
	c := config{RunID: "test", Engine: "codex"}
	if registerErr := register(root, next, c, status{Job: "next", Engine: c.Engine, State: "running"}); registerErr != nil {
		t.Fatal(registerErr)
	}
	obs, err := pos.openObservation(next)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(obs)
	if pos.Next != 0 || pos.Since != 0 || pos.Finished != 0 || pos.LastCompleted != "" ||
		pos.DirectoryID == key || pos.Mode != "messages" {
		t.Fatal(pos)
	}
	if obs.tail.Cursor() != obs.cursor {
		t.Fatalf("tail still holds an old cursor: %v, want %v", obs.tail.Cursor(), obs.cursor)
	}
	updated := status{Job: "next", RunID: "test", Engine: "codex", State: "waiting", Finished: 1, LastCompleted: "new"}
	w := runner{dir: next, s: updated, c: c}
	startFixtureWriter(t, &w)
	if saveErr := w.save(); saveErr != nil {
		t.Fatal(saveErr)
	}
	if readErr := pos.readState(obs); readErr != nil || obs.s.LastCompleted != "new" {
		t.Fatalf("new directory's status was skipped: %+v (%v)", obs.s, readErr)
	}
	f, err := openJobFile(filepath.Join(next, "progress.log"), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintln(f, "1\t2026-11-22T00:00:00Z\tturn/completed\tnew checkout output")
	if writeErr := errors.Join(err, f.Close()); writeErr != nil {
		t.Fatal(writeErr)
	}
	w.s.State = "done"
	if terminalErr := w.saveTerminal(); terminalErr != nil {
		t.Fatal(terminalErr)
	}
	old := tailPosition{RunID: "test", DirectoryID: key, Next: 600, Mode: "seq", Raw: true,
		StateCursor: (stateCursor{Seg: 1, Off: 1 << 20, Seq: 20}).String()}
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CXRUN_STATE", root)
	out := cliOutput(t, 0, nil, exe, "node", "follow", "next", string(raw), "2s")
	dec := json.NewDecoder(strings.NewReader(string(out)))
	var frame tailFrame
	if err := dec.Decode(&frame); err != nil || !strings.Contains(string(frame.Data), "new checkout output") {
		t.Fatalf("new directory's progress was skipped: %s (%v)", out, err)
	}
}

func TestRegistrationDoesNotReuseOldJobFiles(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "job")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"owner.lock", "progress.log", "status.json"} {
		if err := writeJobBytes(filepath.Join(dir, name), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := register(root, dir, config{}, status{Job: "job", State: "queued"}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("legacy files were accepted as an interrupted registration: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state")); !os.IsNotExist(err) {
		t.Fatalf("registration wrote into the old job directory: %v", err)
	}
}
