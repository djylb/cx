package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRunnerRotationKeepsMailboxAndReports(t *testing.T) {
	dir := t.TempDir()
	r := runner{dir: dir, s: status{State: "waiting", LastCompleted: "first"}}
	startFixtureWriter(t, &r)
	if err := r.refreshInbox(); err != nil {
		t.Fatal(err)
	}
	// A process lost ownership midway through writing its previous report.
	first := filepath.Join(dir, "turns", "first.md")
	if err := os.MkdirAll(filepath.Dir(first), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first, []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.publishTurnReport("first", []byte("first report")); err != nil {
		t.Fatal(err)
	}
	if err := r.publishTurnReport("second", []byte("second report")); err != nil {
		t.Fatal(err)
	}
	if err := r.ackInbox(stateRecord{Seq: 4, ID: "handled"}, "delivered"); err != nil {
		t.Fatal(err)
	}
	if err := r.claimInbox(stateRecord{Seq: 5, ID: "waiting"}, "deliver"); err != nil {
		t.Fatal(err)
	}
	if err := r.commitReceipt(receiptRecord{Op: "answer", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := r.stateLog.Append(outboxSeenKind, stateCursor{Seg: 1, Off: 80, Seq: 8}); err != nil {
		t.Fatal(err)
	}
	r.stateLog.SetRotateBytes(1)
	for range 3 {
		if err := r.save(); err != nil {
			t.Fatal(err)
		}
	}
	segments, err := logSegments(filepath.Join(dir, "state"))
	if err != nil || len(segments) != 2 || segments[0] != 3 {
		t.Fatal(segments, err)
	}
	for _, id := range []string{"first", "second"} {
		report, err := readTurnReport(t.Context(), dir, id)
		if err != nil || string(report) != id+" report" {
			t.Fatalf("turn %s: %q, %v", id, report, err)
		}
	}
	if data, err := os.ReadFile(first); err != nil || string(data) != "incomplete" {
		t.Fatalf("unpublished bytes changed: %q, %v", data, err)
	}
	in, err := readInboxState(dir)
	if err != nil || in.acks[4].Result != "delivered" || in.claims[5].ID != "waiting" ||
		in.receipts["answer"].Status != "completed" || in.outbox.Seq != 8 {
		t.Fatal(in, err)
	}
}

func TestWaitTracksWithdrawnInboxSequence(t *testing.T) {
	dir := fixture(t, "waiting")
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	if err := publishMessage(t.Context(), dir, "target", inboxMessage{Text: "queued"}); err != nil {
		t.Fatal(err)
	}
	observer, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(observer)
	if busy, err := waitStart(observer); err != nil || !busy {
		t.Fatal(busy, err)
	}
	if err := r.refreshInbox(); err != nil {
		t.Fatal(err)
	}
	if err := r.ackInbox(stateRecord{Seq: 2, ID: "unrelated"}, "withdrawn"); err != nil {
		t.Fatal(err)
	}
	if err := observer.read(); err != nil || observer.withdrawn {
		t.Fatal(observer.withdrawn, err)
	}
	if err := r.ackInbox(stateRecord{Seq: 1, ID: "target"}, "withdrawn"); err != nil {
		t.Fatal(err)
	}
	if err := observer.read(); err != nil || !observer.withdrawn {
		t.Fatal(observer.withdrawn, err)
	}
}

func TestDedupPublishesNewClaimAfterFailure(t *testing.T) {
	h := commands(t)
	dedup := []string{"--dedup-key", "retry"}
	h.start("failed", dedup, "argv", "failed", "")
	h.cli(1, "wait", "--job", "p/failed", "--timeout", "3s")
	_, first, err := dedupReceipt(h.root, "retry")
	if err != nil || first.Job != "failed" || first.RunID == "" {
		t.Fatal(first, err)
	}
	h.start("successful", dedup, "package-cache-env")
	h.cli(0, "wait", "--job", "p/successful", "--timeout", "3s")
	h.cli(0, "clean", "--job", "p/failed")
	h.cli(0, "clean", "--job", "p/successful")
	dir, second, err := dedupReceipt(h.root, "retry")
	if err != nil || second.Job != "successful" || second.RunID == first.RunID {
		t.Fatal(second, err)
	}
	s, err := archivedDedupStatus(dir, second)
	if err != nil || !s.final || s.RunID != second.RunID {
		t.Fatal(s, err)
	}
	if err := claimKey(h.root, config{DedupKey: "retry"}, status{}); !errors.Is(err, errDuplicate) {
		t.Fatal(err)
	}
}
