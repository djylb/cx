package main

import (
	"errors"
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
	a := h.start("a", "--interactive", "--idle-exit", idle.String())
	dir := filepath.Join(h.root, "a")
	await := func(match func(status) bool) {
		t.Helper()
		for {
			var s status
			if err := projected(h.ctx, dir, &s); err == nil {
				if match(s) {
					return
				}
			} else if !errors.Is(err, errNotRegistered) {
				t.Fatal(err)
			}
			if err := pause(h.ctx, 10*time.Millisecond); err != nil {
				t.Fatalf("last status %+v: %v", s, err)
			}
		}
	}
	// The first turn explicitly waits for input, so the idle clock cannot
	// end the runner while the test takes the only slot.
	await(func(s status) bool { return s.State == "waiting_input" && s.Finished == 1 })
	slot, err := lockPath(filepath.Join(h.root, ".admit", "slot-0.lock"), 0)
	if err != nil || slot == nil {
		t.Fatalf("reserve the released slot: %v", err)
	}
	defer func() { _ = slot.Close() }()
	if err := h.ctl("say", "a", "second round"); err != nil {
		t.Fatal(err)
	}
	await(func(s status) bool { return s.State == "queued" })
	if err := pause(h.ctx, 3*idle); err != nil {
		t.Fatal(err)
	}
	var s status
	if err := projected(h.ctx, dir, &s); err != nil || s.State != "queued" {
		t.Fatalf("queued follow-up idle-exited: %+v %v", s, err)
	}
	if err := slot.Close(); err != nil {
		t.Fatal(err)
	}
	await(func(s status) bool { return s.LastCompleted == "second" && s.Finished == 2 })
	select {
	case err := <-a:
		if err != nil {
			t.Fatal(err)
		}
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
}
