package main

import (
	"bytes"
	"errors"
	"testing"
)

func TestDefaultResultRejectsCachedAndSealedTurnAfterEmptyRevert(t *testing.T) {
	f := newControllerFixture(t)
	h := newRuns(t, "revert-empty")
	const job, addr = "p/reverted-result", "local:p/reverted-result"
	done := h.start(job, "--idle-exit", "2m")
	h.await(job, func(s status) bool { return s.State == "waiting" && s.LastCompleted == "first" })
	before := cliOutput(t, 0, nil, f.cx, "result", addr, "--wait", "--full")
	if err := h.ctl("undo", job, "--before", "first", "--op-id", "empty-result-history"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{}, {"--refresh", "--wait"}} {
		if out := cliOutput(t, 44, nil, f.cx, append([]string{"result", addr}, args...)...); len(out) != 0 {
			t.Fatalf("default result displayed reverted history: %s", out)
		}
	}
	if err := ctlResult(t.Context(), "local", job, "agent", ctlOptions{wait: true}); !errors.Is(err, ctlExit(44)) {
		t.Fatalf("agent result advanced to reverted history: %v", err)
	}
	historical := cliOutput(t, 0, nil, f.cx, "result", addr, "--turn", "1", "--refresh", "--wait", "--full")
	if !bytes.Equal(before, historical) {
		t.Fatalf("explicit history changed: old %s new %s", before, historical)
	}
	if err := h.ctl("close", job); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
