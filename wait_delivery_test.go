package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func waitFixture(t *testing.T, root, id string, s status) *runner {
	t.Helper()
	dir, err := jobDir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(dir)
	if err = os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	c := config{Engine: s.Engine, RunID: s.RunID}
	if err = register(parent, dir, c, s); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatalf("hold fixture owner: %v, %v", owner, err)
	}
	t.Cleanup(func() { closeBestEffort(owner) })
	r := &runner{dir: dir, c: c, s: s, ctx: t.Context()}
	startFixtureWriter(t, r)
	return r
}

func TestWaitAnyWithdrawalBetweenCallsAndTurnsBeforeTerminal(t *testing.T) {
	root := t.TempDir()
	r := waitFixture(t, root, "j", status{Job: "j", RunID: "mixed", Engine: "codex", State: "waiting",
		Finished: 1, Turn: "one", LastCompleted: "one", CompletedStatus: "completed"})
	ctx, cancel := fixtureContext(t)
	defer cancel()
	baseline := make(map[string]waitCursor)
	seen := make(map[string]bool)
	expect := func(event, outcome string, code int, finished int64) {
		t.Helper()
		row, err := pollWaitAny(ctx, root, "", "", baseline)
		if err != nil || row == nil || row.Event != event || row.Outcome != outcome || row.Code != code || row.Finished != finished ||
			row.EventID == "" || seen[row.EventID] {
			t.Fatalf("wanted %s/%s (%d, %d), got %+v (%v)", event, outcome, code, finished, row, err)
		}
		seen[row.EventID] = true
	}
	expect("completion", "completed", 0, 1)
	before := baseline["j"]
	message, err := newRecord("msg", inboxMessage{Text: "withdraw between observations"})
	if err != nil {
		t.Fatal(err)
	}
	message.ID = "withdraw-between-calls"
	message, err = appendJobMailbox(ctx, r.dir, "inbox", nil, message, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.stateLog.Append("inbox_ack", inboxAck{Seq: message.Seq, ID: message.ID, Result: "withdrawn"}); err != nil {
		t.Fatal(err)
	}
	// The withdrawal has no selected completion when it is delivered.
	expect("withdrawal", "interrupted", 1, 1)
	if after := baseline["j"]; after.StateCursor != before.StateCursor || after.Finished != before.Finished {
		t.Fatalf("withdrawal confirmed another event: before %+v after %+v", before, after)
	}
	for _, turn := range []struct{ id, outcome string }{{"two", "completed"}, {"three", "failed"}} {
		r.s.Finished++
		r.s.Turn, r.s.LastCompleted, r.s.CompletedStatus = turn.id, turn.id, turn.outcome
		r.s.TurnStatus = turn.outcome
		if err = r.save(); err != nil {
			t.Fatal(err)
		}
	}
	r.s.State, r.s.CompletedStatus = "stalled", "interrupted"
	if err = r.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	expect("completion", "completed", 0, 2)
	if baseline["j"].Terminal != "" {
		t.Fatalf("completion confirmed terminal state: %+v", baseline["j"])
	}
	expect("completion", "failed", 1, 3)
	expect("terminal", "", 3, 3)
	assertNoWaitAnyEvent(t, ctx, root, "", "", baseline)
}

func TestWaitAnyReportsTerminalFailureWithDamagedOutbox(t *testing.T) {
	root := t.TempDir()
	r := waitFixture(t, root, "j", status{Job: "j", RunID: "damage", Engine: "codex", State: "waiting"})
	ctx, cancel := fixtureContext(t)
	defer cancel()
	id, _, _, err := captureJobIdentity(r.dir)
	if err != nil {
		t.Fatal(err)
	}
	mail := agentMail{Type: "send", To: "parent", Text: "unconfirmed notification"}
	if mailErr := appendMail(ctx, r.dir, id, "before-damage", &mail); mailErr != nil {
		t.Fatal(mailErr)
	}
	f, err := openJobFile(segmentPath(filepath.Join(r.dir, "outbox"), 1), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(writeFrame(f.Write, []byte("invalid complete frame\n")), f.Close()); err != nil {
		t.Fatal(err)
	}
	r.s.State = "failed"
	if terminalErr := r.saveTerminal(); !errors.Is(terminalErr, errLogDamage) {
		t.Fatalf("expected failure diagnostic, got %v", terminalErr)
	}
	baseline := make(map[string]waitCursor)
	row, err := pollWaitAny(ctx, root, "", "", baseline)
	if err != nil || row == nil || row.Event != "terminal" || row.State != "failed" || row.Code != 1 ||
		row.OutboxSeq != 0 || baseline["j"].OutboxSeq != 0 || baseline["j"].OutboxCursor != "" {
		t.Fatalf("terminal result or mail acknowledgement incorrect: %+v (%v), %+v", row, err, baseline)
	}
}

type waitWriterFunc func([]byte) (int, error)

func (f waitWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestWaitAnyRetriesFailedOutputAndCursorWithStableID(t *testing.T) {
	for _, mode := range []string{"without cursor", "stdout with cursor", "persist cursor"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			r := waitFixture(t, root, "j", status{Job: "j", RunID: "deliver", Engine: "cmd", State: "queued"})
			if err := r.setQueueReason("test admission condition"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := fixtureContext(t)
			defer cancel()
			var file string
			if mode != "without cursor" {
				file = filepath.Join(t.TempDir(), "cursor.json")
			}
			var first waitReport
			out := waitWriterFunc(func(p []byte) (int, error) {
				if err := json.Unmarshal(p, &first); err != nil {
					return 0, err
				}
				if mode == "persist cursor" {
					return len(p), os.Mkdir(file, 0700)
				}
				return 0, io.ErrClosedPipe
			})
			err := waitAny(ctx, root, "", "", file, out)
			if mode == "persist cursor" {
				if err == nil || !strings.Contains(err.Error(), "write wait baseline") {
					t.Fatalf("wanted cursor persistence failure, got %v", err)
				}
				if err = os.Remove(file); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("wanted stdout failure, got %v", err)
			}
			var output bytes.Buffer
			if err = waitAny(ctx, root, "", "", file, &output); !errors.Is(err, ctlExit(exitQueued)) {
				t.Fatalf("failed event did not replay: %s (%v)", &output, err)
			}
			var second waitReport
			if err = json.Unmarshal(output.Bytes(), &second); err != nil || first.EventID == "" || first != second {
				t.Fatalf("notice changed across retry: %+v vs %+v (%v)", first, second, err)
			}
		})
	}
}

func TestWaitAnySinceFileLockCoversOutput(t *testing.T) {
	root := t.TempDir()
	r := waitFixture(t, root, "j", status{Job: "j", RunID: "serialize", Engine: "cmd", State: "queued"})
	if err := r.setQueueReason("test admission condition"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "cursor.json")
	ctx, cancel := fixtureContext(t)
	defer cancel()
	enter, release := make(chan struct{}), make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- waitAny(ctx, root, "", "", file, waitWriterFunc(func(p []byte) (int, error) {
			close(enter)
			select {
			case <-release:
				return len(p), nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}))
	}()
	select {
	case <-enter:
	case err := <-done:
		t.Fatalf("wait stopped before stdout: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	lock, err := lockPath(file+".lock", os.O_CREATE)
	if err != nil || lock != nil {
		if lock != nil {
			closeBestEffort(lock)
		}
		t.Fatalf("cursor lock released before stdout and commit: %v", err)
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if !errors.Is(err, ctlExit(exitQueued)) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestWaitNoticeDoesNotMarkReplacementWithSameRun(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	c := config{Engine: "cmd", RunID: "same"}
	s := status{Engine: c.Engine, RunID: c.RunID, Job: "j", State: "queued"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	identity, _, _, err := captureJobIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = deliverNotice(t.Context(), dir, identity, queueEventID(s), true, func() error {
		if renameErr := os.Rename(dir, dir+"-detached"); renameErr != nil {
			return renameErr
		}
		return register(root, dir, c, s)
	})
	if !errors.Is(err, errJobReplaced) {
		t.Fatalf("acknowledgement should reject the replacement: %v", err)
	}
	if _, err = os.Lstat(filepath.Join(dir, ".notices")); !os.IsNotExist(err) {
		t.Fatalf("notice touched the replacement directory: %v", err)
	}
}

func TestWaitThirteenMachineContract(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	t.Run("command exit", func(t *testing.T) {
		cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "code-thirteen", "--", "sh", "-c", "exit 13")
		body := cliOutput(t, 13, nil, f.cx, "wait", "local:p/code-thirteen", "--timeout", "2m")
		if json.Valid(body) || !bytes.Contains(body, []byte("exit_code=13")) {
			t.Fatalf("command exit should retain normal terminal output: %s", body)
		}
	})
	t.Run("admission", func(t *testing.T) {
		r := waitFixture(t, root, "q/queue-thirteen", status{Project: "q", Job: "queue-thirteen", RunID: "thirteen", Engine: "cmd", State: "queued"})
		if err := r.setQueueReason("test admission hold"); err != nil {
			t.Fatal(err)
		}
		// The node wire successfully sends an observed notice, but the controller
		// cannot deliver it. This failure must leave the node's marker uncommitted.
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		closeBestEffort(reader)
		ctx, cancel := fixtureContext(t)
		defer cancel()
		cmd := exec.CommandContext(ctx, f.cx, "wait", "local:q/queue-thirteen", "--timeout", "2m")
		cmd.Stdout = writer
		err = cmd.Run()
		closeBestEffort(writer)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() == exitQueued || ctx.Err() != nil {
			t.Fatalf("closed controller stdout should fail before acknowledgement: %v", err)
		}
		one := cliOutput(t, 13, nil, f.cx, "wait", "local:q/queue-thirteen", "--timeout", "2m")
		anyJob := cliOutput(t, 13, nil, f.cx, "wait", "--any", "--project", "q", "--timeout", "2m")
		for _, body := range [][]byte{one, anyJob} {
			var row waitObservation
			if err := json.Unmarshal(body, &row); err != nil || row.State != "queued" || row.QueueReason != r.s.QueueReason ||
				row.EventID != queueEventID(r.s) || row.NoticeKey != "" {
				t.Fatalf("invalid admission notification: %s (%v)", body, err)
			}
		}
		cliOutput(t, 124, nil, f.cx, "wait", "local:q/queue-thirteen", "--timeout", "150ms")
	})
}

func TestWaitStalledExitAndNodeMarker(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	r := waitFixture(t, root, "p/stall-marker", status{Project: "p", Job: "stall-marker", RunID: "r", Engine: "codex", State: "running"})
	cliOutput(t, 0, nil, f.cx, "node", "interrupt", "--job", "p/stall-marker", "--cmd-id", "cli-interrupt")
	inbox := filepath.Join(r.dir, "inbox")
	record, err := findMailboxID(inbox, filepath.Join(inbox, ".ids", snapSum([]byte("cli-interrupt"))), "cli-interrupt")
	if err != nil || record.Kind != "interrupt" {
		t.Fatalf("node marker was not published: %+v (%v)", record, err)
	}
	r.s.State = "stalled"
	if err = r.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	if body := cliOutput(t, 3, nil, f.cx, "wait", "local:p/stall-marker", "--timeout", "2m"); !bytes.Contains(body, []byte("status=stalled")) {
		t.Fatalf("unexpected stalled diagnostic: %s", body)
	}
}
