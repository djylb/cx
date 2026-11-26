package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Closing a job whose finished turn was already reported must not wake `wait --any`.
func TestWaitAnyIgnoresQuietClose(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	parent := filepath.Join(root, "p")
	dir := filepath.Join(parent, "a")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	first := status{Project: "p", Job: "a", State: "waiting", RunID: "r", Finished: 1, CompletedStatus: "completed", LastCompleted: "t1"}
	if err := register(parent, dir, config{RunID: "r"}, first); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatal(owner, err)
	}
	defer func() { _ = owner.Close() }()
	reg := func(s status) {
		t.Helper()
		if err := writeFixtureStatus(dir, s); err != nil {
			t.Fatal(err)
		}
	}
	since := filepath.Join(t.TempDir(), "cursor.json")
	ctx, cancel := fixtureContext(t)
	defer cancel()
	var out bytes.Buffer
	if err := waitAny(ctx, root, []string{"p"}, "", since, true, &out); err != nil || out.Len() == 0 {
		t.Fatal("first completion was not reported", err)
	}
	first.State = "done"
	reg(first)
	var baseline map[string]waitCursor
	if err := readJSON(since, &baseline); err != nil {
		t.Fatal(err)
	}
	assertNoWaitAnyEvent(t, ctx, root, "p", "", baseline)
}
