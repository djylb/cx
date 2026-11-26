package main

import (
	"path/filepath"
	"testing"
	"time"
)

// A follow-up waiting for a slot is pending input, not idleness: the runner must not
// idle-exit and strand the message in the inbox.
func TestQueuedFollowupOutlivesIdleExit(t *testing.T) {
	const idle = 300 * time.Millisecond
	h := newRuns(t, "question")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	clock := h.withClock("a")
	a := h.start("a", "--interactive", "--idle-exit", idle.String())

	// The first turn explicitly waits for input, so the idle clock cannot
	// end the runner while the test takes the only slot.
	h.await("a", func(s status) bool { return s.State == "waiting_input" && s.Finished == 1 })
	slot, err := lockUntil(h.ctx, filepath.Join(h.root, ".admit", "slot-0.lock"))
	if err != nil || slot == nil {
		t.Fatalf("reserve the released slot: %v", err)
	}
	defer func() { _ = slot.Close() }()
	if err := h.ctl("say", "a", "second round"); err != nil {
		t.Fatal(err)
	}
	h.await("a", func(s status) bool { return s.State == "queued" })
	// A host policy refresh after the clock jump must still be handled.
	setTestHost(t, h.root, map[string]int{"agent": 1}, "exit 1")
	clock.Advance(time.Second + 3*idle)
	h.await("a", func(s status) bool { return s.State == "queued" && s.QueueReason == "host load check did not pass" })
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	clock.Advance(time.Second)
	if err := slot.Close(); err != nil {
		t.Fatal(err)
	}
	clockAfter := func(s status) bool { return s.LastCompleted == "second" && s.Finished == 2 }
	if err := h.idleExit("a", a, clockAfter); err != nil {
		t.Fatal(err)
	}
}
