package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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
		if err := writeJSON(filepath.Join(dir, "status.json"), s); err != nil {
			t.Fatal(err)
		}
	}
	since := filepath.Join(t.TempDir(), "cursor.json")
	run := func(timeout time.Duration) string {
		t.Helper()
		var out bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_ = waitAny(ctx, root, "p", "", since, &out)
		return out.String()
	}
	if run(2*time.Second) == "" {
		t.Fatal("first completion was not reported")
	}
	first.State = "done"
	reg(first)
	if got := run(300 * time.Millisecond); got != "" {
		t.Fatalf("quiet close woke the waiter: %s", got)
	}
}
