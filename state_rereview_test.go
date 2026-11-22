package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

func appendFaultFrame(t *testing.T, file *os.File, record stateRecord) {
	t.Helper()
	b, err := encodeFrame(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(file.Write, b); err != nil {
		t.Fatal(err)
	}
}

func TestFinalDirectoryBarrierCanBeRetriedOnSameTail(t *testing.T) {
	dir := t.TempDir()
	w, release := openTestLogWriter(t, dir)
	defer release()
	if err := w.Append("status", "running"); err != nil {
		t.Fatal(err)
	}
	_, start, err := latestState(dir)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := openStateTail(dir, start)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	fault := errors.New("injected post-link namespace failure")
	var calls int
	w.syncDirFn = func(path string) error {
		calls++
		if calls == 2 {
			return fault
		}
		return syncJobDir(path)
	}
	receipt, err := newRecord("receipt", receiptRecord{Op: "close", Status: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Final("done", receipt); !errors.Is(err, fault) {
		t.Fatalf("final did not report post-link error: %v", err)
	}
	tail.syncDir = func(string) error { return fault }
	if records, err := tail.Next(); !errors.Is(err, fault) || len(records) != 0 || tail.Cursor() != start {
		t.Fatalf("unconfirmed batch escaped: %v %v %v", records, tail.Cursor(), err)
	}
	tail.syncDir = syncJobDir
	records, err := tail.Next()
	if err != nil || len(records) != 2 || records[0].Kind != "receipt" || !records[1].Final {
		t.Fatalf("Tail retry lost the transaction: %v %v", records, err)
	}
	record, cursor, err := latestState(dir)
	if err != nil || !record.Final || cursor != tail.Cursor() {
		t.Fatalf("Latest could not confirm the same candidate: %v %v %v", record, cursor, err)
	}
}

func TestRecoveryRejectsCrossSegmentSequenceReuse(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "agent")
	c := config{RunID: "r", Engine: "codex", Dir: root}
	s := status{Job: "agent", RunID: c.RunID, Engine: c.Engine, State: "waiting"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	_, base, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := createSegment(filepath.Join(dir, "state"), base.Seg+1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := newRecord("inbox_claim", inboxClaim{Seq: 1, ID: "side-effect", Action: "deliver"})
	if err != nil {
		t.Fatal(err)
	}
	// CRC and segment-internal sequencing are valid. Only the shared
	// boundary detects that Tail would skip the claim.
	claim.Seq = base.Seq
	appendFaultFrame(t, f, claim)
	next, err := newRecord("status", s)
	if err != nil {
		t.Fatal(err)
	}
	next.Seq = base.Seq + 1
	appendFaultFrame(t, f, next)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	if err := run(ctx, []string{"resume", "--state", root, "--job", s.Job}); !errors.Is(err, errLogDamage) {
		t.Fatalf("resume reached the engine after losing a valid claim: %v", err)
	}
}

func TestRegistrationConfigBarrierPrecedesMutableFiles(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "agent")
	c := config{RunID: "retry", Engine: "codex", Dir: root}
	s := status{Job: "agent", Engine: c.Engine, State: "queued"}
	fault := errors.New("config flush failed")
	err := registerJob(t.Context(), root, dir, c, s, func(w *stateWriter) error {
		record, err := jobConfigRecord(dir)
		if err != nil || record.Seq == 0 {
			t.Fatalf("barrier ran before config append: %v %v", record, err)
		}
		for _, name := range []string{"progress.log", "events.jsonl", "handoff.lock"} {
			if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
				t.Fatalf("stream %s already existed at config barrier: %v", name, err)
			}
		}
		return fault
	})
	if !errors.Is(err, fault) {
		t.Fatalf("registration ignored failed config barrier: %v", err)
	}
	// Model loss of unflushed config bytes. The directory remains retryable.
	if err := os.RemoveAll(filepath.Join(dir, "state")); err != nil {
		t.Fatal(err)
	}
	if err := register(root, dir, c, s); err != nil {
		t.Fatalf("retry of unfinished registration: %v", err)
	}
}

func TestCloseWaitUsesLatestPastDamageAfterPublication(t *testing.T) {
	dir := fixture(t, "running")
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer closeBestEffort(owner)
	_, start, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type answer struct {
		confirmation *closeConfirmation
		err          error
	}
	done := make(chan answer, 1)
	go func() {
		got, err := deliverClose(ctx, dir, markerOptions{ID: "close-with-damage"}, nil)
		done <- answer{got, err}
	}()
	for {
		record, err := findMailboxID(filepath.Join(dir, "inbox"), filepath.Join(dir, "inbox", ".ids", snapSum([]byte("close-with-damage"))), "close-with-damage")
		if err != nil {
			t.Fatal(err)
		}
		if record.Seq != 0 {
			break
		}
		if err := pause(ctx, 5*time.Millisecond); err != nil {
			t.Fatal("close did not publish its request")
		}
	}
	f, err := openJobFile(segmentPath(filepath.Join(dir, "state"), start.Seg), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "damaged frame"); err != nil {
		t.Fatal(err)
	}
	final, err := newRecord("status", status{Job: "j", Engine: "codex", State: "done"})
	if err != nil {
		t.Fatal(err)
	}
	final.Final, final.Seq = true, start.Seq+2
	appendFaultFrame(t, f, final)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.confirmation == nil || got.confirmation.cursor.Seq != final.Seq {
			t.Fatalf("close did not find the later final: %+v %v", got.confirmation, got.err)
		}
	case <-ctx.Done():
		t.Fatal("close stayed on the damaged Tail")
	}
}

func TestWaitAnySeparatelyDeliversWithdrawalAndCompletion(t *testing.T) {
	t.Run("initial cursor", func(t *testing.T) { testWaitAnyWithdrawalAndCompletion(t, false) })
	t.Run("saved cursor", func(t *testing.T) { testWaitAnyWithdrawalAndCompletion(t, true) })
}

func testWaitAnyWithdrawalAndCompletion(t *testing.T, saved bool) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	c := config{RunID: "r", Engine: "codex"}
	s := status{Job: "j", RunID: c.RunID, Engine: c.Engine, State: "waiting", Finished: 1,
		LastCompleted: "first", CompletedStatus: "completed"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer closeBestEffort(owner)
	_, start, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := newRecord("msg", inboxMessage{Text: "withdraw this"})
	if err != nil {
		t.Fatal(err)
	}
	msg.ID = "withdraw"
	msg, err = appendJobMailbox(t.Context(), dir, "inbox", nil, msg, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, c: c, s: s}
	startFixtureWriter(t, &r)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticks := make(chan time.Time)
	baseline := make(map[string]waitCursor)
	if saved {
		baseline["j"] = waitCursor{RunID: s.RunID, Finished: 1, StateCursor: start.String()}
	}
	before := baseline["j"].Finished
	type answer struct {
		report *waitReport
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		row, err := pollWaitAnyTicks(ctx, root, "", "", baseline, ticks)
		done <- answer{row, err}
	}()
	tick := func() {
		t.Helper()
		select {
		case ticks <- time.Now():
		case got := <-done:
			t.Fatalf("unexpected event before matching ACK: %+v (%v)", got.report, got.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	tick()
	if err := r.stateLog.Append("inbox_ack", inboxAck{Seq: msg.Seq, ID: "wrong-id", Result: "withdrawn"}); err != nil {
		t.Fatal(err)
	}
	tick()
	tick() // the observer processed the wrong ID and returned to the clock.
	ack, err := newRecord("inbox_ack", inboxAck{Seq: msg.Seq, ID: msg.ID, Result: "withdrawn"})
	if err != nil {
		t.Fatal(err)
	}
	r.s.Finished, r.s.LastCompleted, r.s.CompletedStatus = 2, "other", "failed"
	finished, err := newRecord("status", r.s)
	if err != nil {
		t.Fatal(err)
	}
	// Publish the ACK and unrelated completion as a single visible batch.
	if err := r.stateLog.commit(ack, finished); err != nil {
		t.Fatal(err)
	}
	var got answer
	select {
	case got = <-done:
	case ticks <- time.Now():
		select {
		case got = <-done:
		case <-ctx.Done():
			t.Fatal("wait-any did not observe a matching ACK")
		}
	case <-ctx.Done():
		t.Fatal("wait-any did not observe a matching ACK")
	}
	if got.err != nil || got.report == nil || got.report.Outcome != "interrupted" ||
		baseline["j"].Finished != before || baseline["j"].StateCursor != start.String() {
		t.Fatalf("wrong withdrawal: %+v %v, baseline %+v", got.report, got.err, baseline)
	}
	row, err := pollWaitAnyTicks(ctx, root, "", "", baseline, ticks)
	if err != nil || row == nil || row.Outcome != "failed" || row.Finished != 2 || baseline["j"].Finished != 2 {
		t.Fatalf("unrelated completion was lost: %+v (%v), baseline %+v", row, err, baseline)
	}
}

func TestCloseFailureIsUncertainInTerminalReceipt(t *testing.T) {
	dir := fixture(t, "running")
	req, err := publishControl(t.Context(), dir, "close", markerOptions{ID: "fail-close"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir, s: status{Job: "j", Engine: "codex"}}
	startFixtureWriter(t, &r)
	fault := errors.New("injected process reclaim failure")
	err = r.serveAgent(func() error {
		if _, ok, err := r.takeMarker("close"); err != nil || !ok {
			t.Fatalf("close intent not acquired: %v %v", ok, err)
		}
		return fault
	})
	if !errors.Is(err, fault) {
		t.Fatal(err)
	}
	in, err := readInboxState(dir)
	if err != nil || in.acks[req.Seq].ID != req.ID || in.acks[req.Seq].Result != "uncertain" {
		t.Fatalf("close failure got a successful delivery: %+v %v", in, err)
	}
}

func TestWorkspaceUserIgnoresLostOwnerWithoutTakingRegistry(t *testing.T) {
	root := t.TempDir()
	work, dir := filepath.Join(root, "workspace"), filepath.Join(root, "consumer")
	c := config{RunID: "r", Engine: "codex", Dir: filepath.Join(work, "source")}
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := register(root, dir, c, status{Job: "consumer", State: "waiting", Engine: c.Engine}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	registry, err := registryLock(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(registry)
	if got, err := workspaceUser(ctx, root, work, ""); err != nil || got != "" {
		t.Fatalf("lost owner blocked cleanup: %q %v", got, err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer closeBestEffort(owner)
	if got, err := workspaceUser(ctx, root, work, ""); err != nil || got != "consumer" {
		t.Fatalf("active checkout user was ignored: %q %v", got, err)
	}
}

func TestDetachedFlatJobDoesNotReapProjectTrash(t *testing.T) {
	root := t.TempDir()
	flat, project := filepath.Join(root, "p"), filepath.Join(root, "project-fixture")
	for _, path := range []string{flat, project} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := detachCleanJob(root, flat, status{Job: "p"}); err != nil {
		t.Fatal(err)
	}
	if err := detachCleanJob(root, project, status{Project: "p", Job: "j"}); err != nil {
		t.Fatal(err)
	}
	if err := reapDetachedJobs(root, "", "p"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(trashJobDir(root, "p", "j"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("flat job removed project trash: %v %v", entries, err)
	}
}

func TestEmptyMailboxTailDoesNotRelistEveryPoll(t *testing.T) {
	tail, err := openOutboxTail(t.TempDir(), stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	var listings int
	tail.segments = func(string) ([]uint64, error) {
		listings++
		return nil, nil
	}
	for range 20 {
		if err := readMailTail(tail, 0, func(agentMail) error { return errors.New("unexpected message") }); err != nil {
			t.Fatal(err)
		}
		tail.nextScan = time.Now().Add(time.Hour)
	}
	if listings != 1 {
		t.Fatalf("enumerated an empty mailbox %d times", listings)
	}
}

func TestClaudeExitGraceMeasuresReadsNotBackpressure(t *testing.T) {
	t.Run("backpressure", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer closeBestEffort(reader)
		defer closeBestEffort(writer)
		const grace = 20 * time.Millisecond
		pipe := &claudeOutput{file: reader, grace: grace}
		if _, err := fmt.Fprintln(writer, `{"type":"first"}`); err != nil {
			t.Fatal(err)
		}
		event := make(chan string)
		unblock := make(chan struct{})
		done := make(chan error, 1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		go func() {
			done <- decodeClaudeOutput(pipe, func(f claudeFrame) bool {
				select {
				case event <- f.event.Type:
				case <-ctx.Done():
					return false
				}
				if f.event.Type == "first" {
					select {
					case <-unblock:
					case <-ctx.Done():
						return false
					}
				}
				return true
			})
		}()
		select {
		case got := <-event:
			if got != "first" {
				t.Fatal(got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		pipe.childExited()
		if err := pause(ctx, 4*grace); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(writer, `{"type":"second"}`); err != nil {
			t.Fatalf("pipe closed during backpressure: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		close(unblock)
		select {
		case got := <-event:
			if got != "second" {
				t.Fatal(got)
			}
		case err := <-done:
			t.Fatalf("decoder stopped before the second event: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case err := <-done:
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
	t.Run("inherited-writer", func(t *testing.T) {
		reader, inherited, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer closeBestEffort(reader)
		defer closeBestEffort(inherited)
		pipe := &claudeOutput{file: reader, grace: 20 * time.Millisecond}
		pipe.childExited()
		done := make(chan error, 1)
		go func() { done <- decodeClaudeOutput(pipe, func(claudeFrame) bool { return true }) }()
		select {
		case err := <-done:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("idle inherited stdout did not end cleanly: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("inherited stdout held the decoder indefinitely")
		}
	})
}

func TestTerminalScansOnlyOutboxSuffixAfterRestart(t *testing.T) {
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
	ask := agentMail{Type: "ask", To: "parent", Question: "pending?", Blocking: true, Timeout: time.Hour}
	if err := appendMail(t.Context(), dir, id, "ask", &ask); err != nil {
		t.Fatal(err)
	}
	if err := r.pollAgentMail(); err != nil {
		t.Fatal(err)
	}
	if err := r.stateLog.Close(); err != nil {
		t.Fatal(err)
	}
	closeBestEffort(r.inbox.outboxTail)
	late := agentMail{Type: "ask", To: "parent", Question: "late?", Blocking: true, Timeout: time.Hour}
	if err := appendMail(t.Context(), dir, id, "late", &late); err != nil {
		t.Fatal(err)
	}
	// Force a full-history reader to stop at the prefix. The persisted
	// cursor, pending question and later CRC-valid frame remain intact.
	outbox := segmentPath(filepath.Join(dir, "outbox"), 1)
	f, err := openJobFile(outbox, jobFileLockExisting)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("!"), 0)
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	restarted := runner{dir: dir, s: s}
	restarted.s.State = "done"
	startFixtureWriter(t, &restarted)
	if err := restarted.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	in, err := readInboxState(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []string{ask.Request, late.Request} {
		receipt := in.receipts["closed/"+request]
		if receipt.AgentReply == nil || receipt.AgentReply.Request != request || !json.Valid(receipt.AgentReply.Error) {
			t.Fatalf("no terminal receipt for %s: %+v", request, receipt)
		}
	}
}

func TestOrphanPublicationBarriersCoverOlderReportReference(t *testing.T) {
	dir := fixture(t, "running")
	r := runner{dir: dir, s: status{Job: "j", Engine: "codex", State: "waiting", Turn: "first",
		LastCompleted: "first", Finished: 1, CompletedStatus: "completed"}}
	startFixtureWriter(t, &r)
	if err := r.publishTurnReport("first", []byte("retained report")); err != nil {
		t.Fatal(err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	predecessor := segmentPath(r.stateLog.dir, r.stateLog.seg)
	if err := r.stateLog.Close(); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("older report segment did not flush")
	prior, _, err := loadJobStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectJobStatusWithBarrier(t.Context(), dir, &prior, func(w *stateWriter) error {
		w.syncFn = func(file *os.File) error {
			if file.Name() == predecessor {
				return fault
			}
			return syncJobFile(file)
		}
		return w.syncHistory()
	})
	if !errors.Is(err, fault) {
		t.Fatalf("orphan final bypassed the predecessor barrier: %v", err)
	}
	current, _, err := loadJobStatus(dir)
	if err != nil || current.final {
		t.Fatalf("orphan was confirmed despite I/O failure: %+v (%v)", current, err)
	}
	if changed, err := projectJobStatus(t.Context(), dir, &current); err != nil || !changed || !current.final {
		t.Fatalf("orphan did not recover: %+v (%v)", current, err)
	}
	b, err := readTurnReport(t.Context(), dir, current.LastCompleted)
	if err != nil || string(b) != "retained report" {
		t.Fatalf("final cannot resolve its retained report: %q %v", b, err)
	}
}

func TestPreparedReferenceWaitsForCheckoutAndLinkedMetadata(t *testing.T) {
	dir := t.TempDir()
	tree := filepath.Join(dir, "work", "tree")
	admin := filepath.Join(t.TempDir(), "source.git", "worktrees", "tree")
	for _, path := range []string{tree, admin, filepath.Join(dir, "input")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		filepath.Join(dir, "input", "manifest.json"): "snapshot inputs",
		filepath.Join(tree, ".git"):                  "gitdir: " + admin,
		filepath.Join(admin, "gitdir"):               filepath.Join(tree, ".git"),
		filepath.Join(admin, "HEAD"):                 "head",
		filepath.Join(tree, "payload"):               "worktree contents",
		filepath.Join(dir, "work", "go.work"):        "go 1.26",
		filepath.Join(dir, "review.diff"):            "review",
	}
	for path, b := range files {
		if err := writeJobBytes(path, []byte(b)); err != nil {
			t.Fatal(err)
		}
	}
	w, err := openStateLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(w)
	record, err := newRecord("blob", blobRef{Role: preparedBlobRole, SHA256: "dummy"})
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("linked worktree metadata failed to sync")
	w.syncFn = func(f *os.File) error {
		if f.Name() == filepath.Join(admin, "HEAD") {
			return fault
		}
		return syncJobFile(f)
	}
	if err := w.commit(record); !errors.Is(err, fault) {
		t.Fatalf("prepared reference ignored checkout durability failure: %v", err)
	}
	r, _, err := latestStateKind(dir, "blob")
	if err != nil || r.Seq != 0 {
		t.Fatalf("invalid prepared reference became visible: %+v %v", r, err)
	}
	seen := make(map[string]bool)
	ns := make(map[string]bool)
	w.syncFn = func(f *os.File) error {
		seen[f.Name()] = true
		return syncJobFile(f)
	}
	w.syncDirFn = func(path string) error {
		ns[path] = true
		return syncJobDir(path)
	}
	if err := w.commit(record); err != nil {
		t.Fatal(err)
	}
	for path := range files {
		if !seen[path] {
			t.Errorf("did not sync %s before committing prepared reference", path)
		}
	}
	for _, path := range []string{dir, filepath.Join(dir, "input"), filepath.Join(dir, "work"), tree, admin, filepath.Dir(admin)} {
		if !ns[path] {
			t.Errorf("did not sync checkout directory %s", path)
		}
	}
}

func TestWaitSelectsCorrectedTurnFromRetainedCheckpoint(t *testing.T) {
	dir := fixture(t, "running")
	o, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(o)
	o.floor = &status{Finished: 1}
	r := runner{dir: dir, s: status{Job: "j", Engine: "codex", State: "waiting"}}
	startFixtureWriter(t, &r)
	r.stateLog.SetRotateBytes(1)
	for _, turn := range []struct {
		n       int64
		id, end string
	}{{2, "target", "completed"}, {2, "target", "failed"}, {3, "later", "completed"}, {4, "latest", "completed"}} {
		r.s.Finished, r.s.LastCompleted, r.s.CompletedStatus = turn.n, turn.id, turn.end
		if err := r.save(); err != nil {
			t.Fatal(err)
		}
	}
	segments, err := logSegments(r.stateLog.dir)
	if err != nil || len(segments) != 2 || segments[0] <= 2 {
		t.Fatalf("the checkpoint test did not reclaim history: %v %v", segments, err)
	}
	if err := o.read(); err != nil {
		t.Fatal(err)
	}
	if o.selected == nil || o.selected.Number != 2 || o.selected.TurnID != "target" || o.selected.Outcome != "failed" {
		t.Fatalf("compaction lost the first turn or its correction: %+v", o.selected)
	}
}

func TestWorkerWaitsForOwnerProbeGate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "worker")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c := config{RunID: "fixture-probe", Engine: "cmd", Argv: []string{exe, "fixture-command", "argv", "check", "stderr"}}
	s := status{Job: "worker", Engine: c.Engine, State: "queued"}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	gate, err := lockPath(ownerProbePath(dir), 0)
	if err != nil || gate == nil {
		t.Fatal(gate, err)
	}
	defer closeBestEffort(gate)
	probe, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || probe == nil {
		t.Fatal(probe, err)
	}
	defer closeBestEffort(probe)
	f, err := openJobFile(filepath.Join(dir, "owner.lock"), jobFileLockExisting)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(f)
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := tryAcquireJobOwner(canceled, dir, f); !errors.Is(err, context.Canceled) {
		t.Fatalf("worker reported owner busy while only the probe held it: %v", err)
	}
	key, err := jobDirectoryKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"_command", "--state", root, "--job", s.Job,
			"--expect-run", c.RunID, "--expect-dir", key})
	}()
	for {
		guard, err := lockPath(filepath.Join(root, ".admit", "registry.lock"), 0)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err == nil && guard == nil {
			break
		}
		if guard != nil {
			closeBestEffort(guard)
		}
		select {
		case err := <-done:
			t.Fatalf("worker exited during ownership probe: %v", err)
		default:
		}
		if err := pause(ctx, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(probe.Close(), gate.Close()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got, _, err := loadJobStatus(dir)
	if err != nil || !got.final || got.ExitCode == nil || *got.ExitCode != 37 {
		t.Fatalf("worker failed to take ownership: %+v %v", got, err)
	}
}

func TestResumeWaitsForTerminalOwner(t *testing.T) {
	h := newRuns(t, "complete")
	const job = "terminal"
	if err := <-h.start(job); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(h.root, job)
	before, _, err := loadJobStatus(dir)
	if err != nil || !before.final {
		t.Fatalf("expected a terminal job: %+v %v", before, err)
	}

	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer closeBestEffort(owner)
	gate, err := lockUntil(h.ctx, ownerProbePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		defer close(result)
		result <- h.ctl("resume", job, "--launch-id", "terminal-handoff")
	}()
	defer func() {
		closeBestEffort(gate)
		closeBestEffort(owner)
		<-result
	}()

	// Force the first ownership attempt while the predecessor still holds
	// its lock. The resume worker holds the registry while blocked at the gate.
	for {
		registry, err := lockPath(filepath.Join(h.root, ".admit", "registry.lock"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if registry == nil {
			break
		}
		closeBestEffort(registry)
		if err := pause(h.ctx, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err := registryLock(h.ctx, h.root)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(owner.Close(), registry.Close()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	after, _, err := loadJobStatus(dir)
	if err != nil || !after.final || after.Generation != before.Generation+1 || after.LaunchID != "terminal-handoff" {
		t.Fatalf("resume did not publish the next generation: %+v %v", after, err)
	}
}

func TestRevertEmptyHistoryKeepsHistoricalReportAcrossResume(t *testing.T) {
	h := newRuns(t, "revert-empty")
	const job = "reverted"
	done := h.start(job, "--idle-exit", "30s")
	s := h.await(job, func(s status) bool { return s.State == "waiting" && s.LastCompleted == "first" })
	dir := filepath.Join(h.root, job)
	archive := filepath.Join(dir, "result", "turn-1")
	if err := os.MkdirAll(archive, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeJobBytes(filepath.Join(archive, "result.json"), []byte(`{"turn_id":"first"}`)); err != nil {
		t.Fatal(err)
	}
	const history = "historical report"
	if err := writeJobBytes(filepath.Join(archive, "last.md"), []byte(history)); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	show := func(turn, want string) {
		t.Helper()
		var got struct{ Report string }
		reply := cliOutput(t, 0, nil, exe, "node", "show", job, turn, "true")
		if err := json.Unmarshal(reply, &got); err != nil || got.Report != want {
			t.Fatalf("show %q returned %s (%v), want %q", turn, reply, err, want)
		}
	}
	show("", history)
	if err := h.ctl("undo", job, "--before", "first", "--op-id", "empty-history"); err != nil {
		t.Fatal(err)
	}
	show("", "")
	show("1", history)
	if err := h.ctl("close", job); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	resumed := make(chan error, 1)
	go func() { resumed <- h.ctl("resume", job) }()
	h.await(job, func(next status) bool {
		return next.Generation > s.Generation && next.State == "waiting" && next.Finished == 1 &&
			next.Turn == "" && next.LastCompleted == ""
	})
	show("", "")
	show("1", history)
	if err := h.ctl("close", job); err != nil {
		t.Fatal(err)
	}
	if err := <-resumed; err != nil {
		t.Fatal(err)
	}
}

func TestDedupClaimRetiresSupersededAttempts(t *testing.T) {
	root := t.TempDir()
	s := status{Job: "unused"}
	for i := range 12 {
		c := config{RunID: fmt.Sprintf("attempt-%d", i), DedupKey: "same-key"}
		if err := claimKey(root, c, s); err != nil {
			t.Fatal(err)
		}
		dir, receipt, err := dedupReceipt(root, c.DedupKey)
		if err != nil || receipt.RunID != c.RunID {
			t.Fatalf("claim %d: %+v %v", i, receipt, err)
		}
		segments, err := logSegments(filepath.Join(dir, "state"))
		if err != nil || len(segments) != 1 || segments[0] != uint64(i+1) {
			t.Fatalf("obsolete claims remain on the retry path: %v %v", segments, err)
		}
	}
}

func TestLiveJobProjectionDoesNotWaitForRegistry(t *testing.T) {
	dir := fixture(t, "running")
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	guard, err := registryLock(ctx, filepath.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(guard)
	f, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || f == nil {
		t.Fatal(f, err)
	}
	defer closeBestEffort(f)
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := projectJobStatus(ctx, dir, &s, identity); err != nil || changed {
		t.Fatalf("ordinary live polling waited on registry: %v %v", changed, err)
	}
}

func TestCleanupRetriesDetachedDirectoryAfterReapFailure(t *testing.T) {
	if runtime.GOOS == "windows" || os.Getuid() == 0 {
		t.Skip("requires permissions to block recursive deletion")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "job")
	if err := register(root, dir, config{RunID: "first", Engine: "codex"}, status{Job: "job", Engine: "codex", State: "done"}); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(trashJobDir(root, "", "job"), "stale")
	if err := os.MkdirAll(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeJobBytes(filepath.Join(stale, "blocked"), nil); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stale, 0500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(stale, 0700) }()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	args := []string{"clean", "--state", root, "--job", "job"}
	if err := run(ctx, args); err == nil {
		t.Fatal("clean ignored recursive deletion failure")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("failed recursive deletion left a live registration: %v", err)
	}
	// No registry lock is left behind, and retry discovers the detached job.
	guard, err := registryLock(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	closeBestEffort(guard)
	if err := os.Chmod(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, args); err != nil {
		t.Fatalf("trash could not be retried by job ID: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Dir(stale)); err != nil || len(entries) != 0 {
		t.Fatalf("cleanup did not reap detached job: %v %v", entries, err)
	}
}

func TestMaterializeRetriesUnpublishedRegisteredWorktree(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "file.txt"), "from the snapshot")
	_, manifest, err := Freeze(ctx, src, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	prepared, err := Materialize(ctx, dir, manifest, "")
	if err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(prepared.Source, "unpublished.txt"), "partial attempt")
	if err := os.RemoveAll(filepath.Join(dir, "state")); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(t.TempDir(), "git-trace.json")
	t.Setenv("GIT_TRACE2_EVENT", trace)
	recovered, err := Materialize(ctx, dir, manifest, "")
	if err != nil {
		t.Fatalf("could not reclaim and retry the registered worktree: %v", err)
	}
	traceFile, err := os.Open(trace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(traceFile)
	dec := json.NewDecoder(traceFile)
	var removal bool
	for {
		var event struct {
			Event string
			Argv  []string
		}
		if err := dec.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		at := slices.Index(event.Argv, "worktree")
		if event.Event == "start" && at >= 0 && len(event.Argv) > at+1 && event.Argv[at+1] == "remove" {
			removal = true
			break
		}
	}
	if !removal {
		t.Fatal("retry skipped removal of the unpublished Git registration")
	}
	if _, err := os.Stat(filepath.Join(recovered.Source, "unpublished.txt")); !os.IsNotExist(err) {
		t.Fatalf("retried checkout reused unpublished bytes: %v", err)
	}
	git := snapGit{ctx: ctx, dir: recovered.Source}
	head, err := git.get("rev-parse", "HEAD")
	if err != nil || head != recovered.SnapshotOID {
		t.Fatalf("retried checkout was not registered with Git: %q (%v)", head, err)
	}
}

func TestIndexedRetryDoesNotReplayOlderMailboxFrames(t *testing.T) {
	dir := fixture(t, "running")
	first, err := newRecord("msg", inboxMessage{Text: "old"})
	if err != nil {
		t.Fatal(err)
	}
	first.ID = "earlier"
	if _, err := appendJobMailbox(t.Context(), dir, "inbox", nil, first, nil); err != nil {
		t.Fatal(err)
	}
	request, err := newRecord("msg", inboxMessage{Text: "same"})
	if err != nil {
		t.Fatal(err)
	}
	request.ID = "stable"
	published, err := appendJobMailbox(t.Context(), dir, "inbox", nil, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := segmentPath(filepath.Join(dir, "inbox"), 1)
	f, err := openJobFile(path, jobFileLockExisting)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("!"), 0)
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	retry, err := appendJobMailbox(t.Context(), dir, "inbox", nil, request, nil)
	if err != nil || retry.Seq != published.Seq {
		t.Fatalf("indexed retry parsed the damaged prefix: %d (%v), want %d", retry.Seq, err, published.Seq)
	}
}

func TestArchiveFailureLeavesOriginalJobRegistered(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "job")
	c := config{RunID: "archived-run", DedupKey: "archive-key", Engine: "codex"}
	s := status{Job: "job", RunID: c.RunID, Engine: "codex", State: "done", CompletedStatus: "completed"}
	if err := claimKey(root, c, s); err != nil {
		t.Fatal(err)
	}
	if err := register(root, dir, c, s); err != nil {
		t.Fatal(err)
	}
	dedup, _, err := dedupReceipt(root, c.DedupKey)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	guard, err := registryLock(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		closeBestEffort(guard)
		t.Fatal(owner, err)
	}
	fault := errors.New("archive namespace flush failed after link")
	err = archiveAndDetachJob(ctx, root, dir, owner, c, s, func(archive, bound string) error {
		entries, err := os.ReadDir(archive)
		if err != nil || len(entries) != 1 || bound != root {
			t.Errorf("archive had no link when its barrier ran: %v %v", entries, err)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("job was detached before the archive barrier: %v", err)
		}
		return fault
	})
	closeBestEffort(guard)
	if !errors.Is(err, fault) {
		t.Fatalf("clean ignored a failed terminal archive barrier: %v", err)
	}
	if got, _, err := loadJobStatus(dir); err != nil || !got.final || got.RunID != c.RunID {
		t.Fatalf("job was detached without a durable archive: %+v (%v)", got, err)
	}
	if err := run(ctx, []string{"clean", "--state", root, "--job", s.Job}); err != nil {
		t.Fatal(err)
	}
	if got, err := archivedDedupStatus(dedup, receipt{RunID: c.RunID}); err != nil || !got.final {
		t.Fatalf("archive missing after retry: %+v (%v)", got, err)
	}
}

func TestWaitStartBindsMessageWithDifferentExistingACKID(t *testing.T) {
	dir := fixture(t, "waiting")
	msg, err := newRecord("msg", inboxMessage{Text: "keep queued"})
	if err != nil {
		t.Fatal(err)
	}
	msg.ID = "correct"
	msg, err = appendJobMailbox(t.Context(), dir, "inbox", nil, msg, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	if err := r.stateLog.Append("inbox_ack", inboxAck{Seq: msg.Seq, ID: "wrong", Result: "withdrawn"}); err != nil {
		t.Fatal(err)
	}
	o, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(o)
	if busy, err := waitStart(o); err != nil || !busy || o.pending[msg.Seq] != msg.ID {
		t.Fatalf("prior ACK for a different ID hid the queued message: %v %v %+v", busy, err, o.pending)
	}
	if err := r.refreshInbox(); !errors.Is(err, errLogDamage) {
		t.Fatalf("runner recovered a mismatched consumption log: %v", err)
	}
}

func TestInboxClaimDoesNotOutliveMissingMessageSegment(t *testing.T) {
	dir := fixture(t, "waiting")
	msg, err := newRecord("msg", inboxMessage{Text: "not yet applied"})
	if err != nil {
		t.Fatal(err)
	}
	msg.ID = "durable"
	if _, err := appendJobMailbox(t.Context(), dir, "inbox", nil, msg, nil); err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	if err := r.refreshInbox(); err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(r.inbox.tail)
	if len(r.inbox.queue) != 1 {
		t.Fatal(r.inbox.queue)
	}
	item := r.inbox.queue[0]
	// Renaming the fixture models loss of the source namespace while the
	// runner still has its Open handle and has parsed the message.
	path := segmentPath(filepath.Join(dir, "inbox"), item.position.Seg)
	if err := os.Rename(path, path+".lost"); err != nil {
		t.Fatal(err)
	}
	if err := r.claimInbox(item, "deliver"); err == nil {
		t.Fatal("runner persisted a claim without its input frame")
	}
	in, err := readInboxState(dir)
	if err != nil || in.claims[item.Seq].Seq != 0 {
		t.Fatalf("unsafe claim entered the state log: %v %v", in, err)
	}
}
