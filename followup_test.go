package main

import (
	"path/filepath"
	"testing"
	"time"
)

// A follow-up waiting for a slot is pending input, not idleness: the runner must not
// idle-exit and strand the message in the inbox.
func TestQueuedFollowupOutlivesIdleExit(t *testing.T) {
	h := newRuns(t, "manual")
	setTestHost(t, h.root, map[string]int{"codex": 1}, "")
	a := h.start("a", "--idle-exit", "300ms")
	h.await("a", func(s status) bool { return s.State == "running" && s.Turn == "first" })
	if err := h.ctl("say", "a", "completed"); err != nil {
		t.Fatal(err)
	}
	h.await("a", func(s status) bool { return s.State == "waiting" && s.Finished == 1 })
	b := h.start("b", "--idle-exit", "1m")
	h.await("b", func(s status) bool { return s.State == "running" && s.Turn == "first" })
	if err := h.ctl("say", "a", "second round"); err != nil {
		t.Fatal(err)
	}
	h.await("a", func(s status) bool { return s.State == "queued" })
	time.Sleep(900 * time.Millisecond) // well past a's idle limit while queued
	var s status
	if err := readJSON(filepath.Join(h.root, "a", "status.json"), &s); err != nil || s.State != "queued" {
		t.Fatalf("queued follow-up idle-exited: %+v %v", s, err)
	}
	if err := h.ctl("interrupt", "b"); err != nil {
		t.Fatal(err)
	}
	h.await("a", func(s status) bool { return s.Turn == "second" && s.State == "running" })
	for id, end := range map[string]<-chan error{"a": a, "b": b} {
		if err := h.ctl("close", id); err != nil {
			t.Fatal(err)
		}
		if err := <-end; err != nil {
			t.Fatal(id, err)
		}
	}
}
