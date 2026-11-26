package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryAndRotationAfterFirstSegmentReclaimed(t *testing.T) {
	dir := fixture(t, "running")
	s, _, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, s: s}
	startFixtureWriter(t, &r)
	appendRPC := func(id string) stateRecord {
		t.Helper()
		rec, recordErr := newRecord("rpc", fields{"method": "thread/read"})
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		rec.ID = id
		msg, recordErr := appendInbox(t.Context(), dir, rec, nil)
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		return msg
	}
	delivered, claimed := appendRPC("completed"), appendRPC("claimed")
	if refreshErr := r.refreshInbox(); refreshErr != nil {
		t.Fatal(refreshErr)
	}
	if receiptErr := r.commitReceipt(receiptRecord{Op: delivered.ID, Status: "ok"}); receiptErr != nil {
		t.Fatal(receiptErr)
	}
	if ackErr := r.ackInbox(delivered, "delivered"); ackErr != nil {
		t.Fatal(ackErr)
	}
	if claimErr := r.claimInbox(claimed, "deliver"); claimErr != nil {
		t.Fatal(claimErr)
	}
	if reportErr := r.publishTurnReport("first", []byte("retained report")); reportErr != nil {
		t.Fatal(reportErr)
	}
	r.stateLog.SetRotateBytes(1)
	for range 3 {
		if saveErr := r.save(); saveErr != nil {
			t.Fatal(saveErr)
		}
	}
	if _, statErr := os.Stat(segmentPath(r.stateLog.dir, 1)); !os.IsNotExist(statErr) {
		t.Fatalf("first segment was not reclaimed: %v", statErr)
	}
	if closeErr := errors.Join(r.inbox.tail.Close(), r.stateLog.Close()); closeErr != nil {
		t.Fatal(closeErr)
	}
	restarted := runner{dir: dir, s: s}
	startFixtureWriter(t, &restarted)
	if recoveryErr := restarted.stateLog.RecoveryError(); recoveryErr != nil {
		t.Fatal(recoveryErr)
	}
	if recoverErr := restarted.recoverRPC(); recoverErr != nil {
		t.Fatal(recoverErr)
	}
	defer closeBestEffort(restarted.inbox.tail)
	if refreshErr := restarted.refreshInbox(); refreshErr != nil {
		t.Fatal(refreshErr)
	}
	if len(restarted.inbox.queue) != 0 || restarted.inbox.acks[claimed.Seq].Result != "uncertain" {
		t.Fatalf("recovery exposed old RPCs for replay: %+v", restarted.inbox.queue)
	}
	restarted.stateLog.SetRotateBytes(1)
	for range 3 {
		if saveErr := restarted.save(); saveErr != nil {
			t.Fatal(saveErr)
		}
	}
	in, err := readInboxState(dir)
	if err != nil || in.acks[delivered.Seq].ID != delivered.ID || in.receipts[delivered.ID].Status != "ok" ||
		in.acks[claimed.Seq].ID != claimed.ID || in.receipts[claimed.ID].Status != "uncertain" {
		t.Fatalf("second rotation lost the durable outcomes: %+v %v", in, err)
	}
	reports, err := turnReportCheckpoints(dir)
	if err != nil || len(reports) != 1 {
		t.Fatalf("second rotation lost the report reference: %+v %v", reports, err)
	}
}

func TestStateTailFirstReadCrossesGapBetweenRetainedSegments(t *testing.T) {
	dir := t.TempDir()
	for i, seg := range []uint64{2, 4, 5} {
		f := testLogSegment(t, dir, seg)
		testLogWrite(t, f, testLogFrame(t, uint64(i+2), "status", seg))
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	testLogNext(t, tail, 2, 3, 4)
}

func TestRotationPowerLossKeepsPredecessorThroughReservedSeal(t *testing.T) {
	dir := t.TempDir()
	w, closeWriter := openTestLogWriter(t, dir)
	if err := w.Append("config", "last durable config"); err != nil {
		t.Fatal(err)
	}
	old := segmentPath(w.dir, 1)
	durable := w.size
	w.syncFn = func(f *os.File) error {
		if f == w.file {
			info, err := f.Stat()
			if err != nil {
				return err
			}
			durable = uint64(info.Size())
		}
		return syncJobFile(f)
	}
	for range 3 {
		if err := w.Append("status", "buffered update"); err != nil {
			t.Fatal(err)
		}
	}
	w.SetRotateBytes(1)
	powerLoss := errors.New("power cut after checkpoint publication")
	w.appendFn = func(f *os.File, data []byte) (int, error) {
		if f == w.file {
			return 0, powerLoss
		}
		return f.Write(data)
	}
	if err := w.Rotate("config", "checkpoint"); !errors.Is(err, powerLoss) {
		t.Fatalf("simulation missed the cut: %v", err)
	}
	closeWriter()
	if err := os.Truncate(old, int64(durable)); err != nil {
		t.Fatal(err)
	}
	testLogLatest(t, dir, "status", 7, 2)
	next, _ := openTestLogWriter(t, dir)
	if err := next.RecoveryError(); err != nil {
		t.Fatalf("published checkpoint lost the predecessor's buffered prefix: %v", err)
	}
}

func TestQueuedGenerationSurvivesLostReasonFrame(t *testing.T) {
	dir := fixture(t, "queued")
	s, _, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, s: s}
	startFixtureWriter(t, &r)
	var durable int64
	r.stateLog.syncFn = func(f *os.File) error {
		info, statErr := f.Stat()
		if statErr == nil {
			durable = info.Size()
		}
		return errors.Join(statErr, syncJobFile(f))
	}
	if queueErr := r.setQueueReason("disk"); queueErr != nil {
		t.Fatal(queueErr)
	}
	observed, _, err := loadJobStatus(dir)
	if err != nil || observed.QueueReason != "disk" || observed.QueueGeneration == 0 {
		t.Fatalf("queue reason never became visible: %+v %v", observed, err)
	}
	old := segmentPath(r.stateLog.dir, r.stateLog.seg)
	if closeErr := r.stateLog.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if truncateErr := os.Truncate(old, durable); truncateErr != nil {
		t.Fatal(truncateErr)
	}
	s, _, err = loadJobStatus(dir)
	if err != nil || s.QueueReason != "" || s.QueueGeneration != observed.QueueGeneration {
		t.Fatalf("power loss erased the acknowledged generation: %+v %v", s, err)
	}
	restarted := runner{dir: dir, s: s}
	startFixtureWriter(t, &restarted)
	if err := restarted.setQueueReason("disk"); err != nil || restarted.s.QueueGeneration <= observed.QueueGeneration {
		t.Fatalf("new block reused an acknowledged generation: %+v %v", restarted.s, err)
	}
}

func TestOutboxDamageAfterCheckpointBlocksPollAndTerminal(t *testing.T) {
	dir := fixture(t, "running")
	id, s, _, err := captureJobIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, s: s}
	startFixtureWriter(t, &r)
	r.log, err = openJobFile(filepath.Join(dir, "progress.log"), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(r.log)
	addAsk := func(name string) agentMail {
		t.Helper()
		m := agentMail{Type: "ask", To: "parent", Question: name, Blocking: true, Timeout: time.Hour}
		if mailErr := appendMail(t.Context(), dir, id, name, &m); mailErr != nil {
			t.Fatal(mailErr)
		}
		return m
	}
	addAsk("confirmed")
	if pollErr := r.pollAgentMail(); pollErr != nil {
		t.Fatal(pollErr)
	}
	at := r.inbox.outbox
	addAsk("damaged")
	path := filepath.Join(dir, "outbox")
	f, err := openJobFile(segmentPath(path, 1), jobFileLockExisting)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("!"), int64(at.Off))
	if updateErr := errors.Join(err, f.Close()); updateErr != nil {
		t.Fatal(updateErr)
	}
	later := agentMail{Type: "ask", Seq: 3, RunID: s.RunID, Request: "ask-3", Blocking: true}
	if writeErr := writeJobBytes(segmentPath(path, 2), testLogFrame(t, 3, "ask", later)); writeErr != nil {
		t.Fatal(writeErr)
	}
	for range 2 {
		if pollErr := r.pollAgentMail(); !errors.Is(pollErr, errLogDamage) || r.inbox.outbox != at {
			t.Fatalf("damaged unacknowledged suffix was crossed: %v, %v", r.inbox.outbox, pollErr)
		}
	}
	r.s.State = "done"
	if terminalErr := r.saveTerminal(); !errors.Is(terminalErr, errLogDamage) {
		t.Fatalf("published success after losing an ask: %v", terminalErr)
	}
	terminal, _, err := loadJobStatus(dir)
	if err != nil || !terminal.final || terminal.State != "failed" {
		t.Fatalf("terminal did not surface mailbox damage: %+v %v", terminal, err)
	}
}

func TestMailboxTailWaitsForIncompleteFrame(t *testing.T) {
	dir := t.TempDir()
	frame := testLogFrame(t, 1, "ask", agentMail{Seq: 1, Type: "ask"})
	path := filepath.Join(dir, "outbox")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := createSegment(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(f)
	testLogWrite(t, f, bytes.TrimSuffix(frame, []byte("\n")))
	tail, err := openOutboxTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	testLogNext(t, tail)
	testLogWrite(t, f, []byte("\n"))
	testLogNext(t, tail, 1)
}

func TestCaptureJobIdentityRejectsSameRunReplacementAfterStatusRead(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	s := status{Job: "j", RunID: "same", State: "running"}
	if err := writeFixtureStatus(dir, s); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := captureJobIdentityWithStatus(dir, func(path string) (status, stateCursor, error) {
		read, cursor, err := loadJobStatus(path)
		if err != nil {
			return read, cursor, err
		}
		if err := os.Rename(dir, filepath.Join(root, "detached")); err != nil {
			t.Fatal(err)
		}
		if err := writeFixtureStatus(dir, s); err != nil {
			t.Fatal(err)
		}
		return read, cursor, nil
	})
	if !errors.Is(err, errJobReplaced) {
		t.Fatalf("paired old state with the replacement directory: %v", err)
	}
}

func TestRegistrationWaitsForOwnerProbe(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	gate, err := lockUntil(t.Context(), ownerProbePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(gate)
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), os.O_CREATE)
	if err != nil || owner == nil {
		t.Fatalf("probe could not acquire owner: %v", err)
	}
	defer closeBestEffort(owner)
	err = expireLockWait(t, func(ctx context.Context) error {
		return registerFixtureJob(ctx, root, dir, config{}, status{Job: "j", State: "queued"}, (*stateWriter).sync)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("register treated a read-side probe as a live owner: %v", err)
	}
	if err := errors.Join(owner.Close(), gate.Close()); err != nil {
		t.Fatal(err)
	}
	if err := registerFixtureJob(t.Context(), root, dir, config{}, status{Job: "j", State: "queued"}, (*stateWriter).sync); err != nil {
		t.Fatal(err)
	}
}

func TestCommandAdmissionCoordinatesWithManagedWorkspace(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	cwd := filepath.Join(root, ".work", "p", "workspace", "work", "tree")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	gate, err := jobOperationLock(t.Context(), root, "p/workspace")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(gate)
	err = expireLockWait(t, func(ctx context.Context) error {
		return nodeControl(ctx, []string{"submit-command", "another/use", cwd, "--", "not-started"})
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("command crossed the workspace cleanup lock: %v", err)
	}
	dir := filepath.Join(root, "another", "use")
	if _, _, err := loadJobStatus(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a command was registered while workspace cleanup held the lock: %v", err)
	}
}

func TestDetachedCleanupChecksLiveWorkspaceUserBeforeRetry(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	base := filepath.Join(root, ".work", "p", "workspace")
	cwd := filepath.Join(base, "work", "tree")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "consumer")
	c := config{Dir: cwd, RunID: "live"}
	s := status{Job: "consumer", State: "running", Engine: "cmd", PID: os.Getpid()}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	owner, err := openJobOwner(t.Context(), dir, jobFileLockExisting)
	if err != nil || owner == nil {
		t.Fatalf("cannot hold workspace consumer: %v", err)
	}
	defer closeBestEffort(owner)
	if err := cleanNodeJob(t.Context(), "p/workspace"); err == nil {
		t.Fatal("detach retry removed a workspace with a live consumer")
	}
	if _, err := os.Stat(cwd); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cleanNodeJob(t.Context(), "p/workspace"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cwd); !os.IsNotExist(err) {
		t.Fatalf("workspace was not removed after use ended: %v", err)
	}
}

func TestNodeRejectsAnOlderUnmarkedStateRootBeforeRegistering(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "old")
	s := status{Job: "old", RunID: "old-run", State: "running"}
	if err := writeFixtureStatus(dir, s); err != nil {
		t.Fatal(err)
	}
	w, err := openStateLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if logErr := errors.Join(w.Append(outboxSeenKind, 3), w.Close()); logErr != nil {
		t.Fatal(logErr)
	}
	t.Setenv("CXRUN_STATE", root)
	err = run(t.Context(), []string{"start", "--job", "new", "--dir", root, "--cmd", "--", "not-started"})
	if !errors.Is(err, errStateFormat) {
		t.Fatalf("registered against a legacy checkpoint format: %v", err)
	}
	for _, path := range []string{filepath.Join(root, "new"), filepath.Join(root, nodeStateFormatFile)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("legacy root was mutated before validation: %s: %v", path, err)
		}
	}
}
