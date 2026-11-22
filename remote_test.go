package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitAndStatusBeforeRegistration(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "j")
	statusArgs := []string{"status", "--state", root, "--job", "j"}
	waitArgs := []string{"wait", "--state", root, "--job", "j", "--timeout"}
	ctx := context.Background()
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := openJobFile(filepath.Join(dir, "owner.lock"), jobFileLock)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	unpublished := runner{dir: dir}
	if err := unpublished.openState(); err != nil {
		t.Fatal(err)
	}
	if err := unpublished.stateLog.Close(); err != nil {
		t.Fatal(err)
	}
	var row status
	output := fixtureCLI(t, 11, statusArgs...)
	if err := json.Unmarshal([]byte(output), &row); err != nil || row.State != "not_registered" || row.Job != "j" {
		t.Fatalf("status before registration: %q: %v", output, err)
	}
	output = fixtureCLI(t, 124, append(waitArgs, "30ms")...)
	if !strings.Contains(output, "status=not_registered wait_timeout=true") {
		t.Fatalf("registration deadline: %q", output)
	}
	registered := make(chan error, 1)
	timer := time.AfterFunc(40*time.Millisecond, func() {
		registered <- register(root, dir, config{}, status{Job: "j", State: "done"})
	})
	defer timer.Stop()
	if err := run(ctx, append(waitArgs, "2s")); err != nil {
		t.Fatalf("wait through registration: %v", err)
	}
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, statusArgs); err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	if err := r.stateLog.Append("status", "invalid status payload"); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, append(waitArgs, "2s")); err == nil || errors.Is(err, errObserveTimeout) {
		t.Fatalf("broken status should fail immediately: %v", err)
	}
}

func fixture(t *testing.T, state string) string {
	t.Helper()
	dir := t.TempDir()
	if err := register(filepath.Dir(dir), dir, config{Engine: "codex"}, status{Job: "j", Engine: "codex", State: state}); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A retried say with the same ID delivers once, including after the job ends.
func TestSayNameIsIdempotent(t *testing.T) {
	dir := fixture(t, "waiting")
	t.Setenv("CXRUN_MSG_NAME", "0000000000000000001-0123456789abcdef.msg")
	if err := control(t.Context(), "say", dir, []string{"hello"}); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, status{Job: "j", State: "done", Engine: "codex"}); err != nil {
		t.Fatal(err)
	}
	if err := control(t.Context(), "say", dir, []string{"hello"}); err != nil {
		t.Fatalf("retry after end: %v", err)
	}
	tail, err := inboxTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	if records, err := tail.Next(); err != nil || len(records) != 1 {
		t.Fatalf("inbox changed: %v %v", records, err)
	}
	t.Setenv("CXRUN_MSG_NAME", "bad")
	if err := control(t.Context(), "say", dir, []string{"x"}); err == nil {
		t.Fatal("bad name accepted")
	}
}

// log prints only complete lines from the offset and reports a drained terminal job through errDrained.
func TestLogOffsetAndDrain(t *testing.T) {
	dir := fixture(t, "running")
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), []byte("a\nb\npart"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := logFrom(dir, []string{"2"}); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, status{Job: "j", State: "done", Engine: "codex"}); err != nil {
		t.Fatal(err)
	}
	if err := logFrom(dir, []string{"4"}); err != nil { // partial line pending: not drained
		t.Fatal(err)
	}
	log, err := openJobFile(filepath.Join(dir, "progress.log"), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	_, err = log.WriteString("\n")
	if err = errors.Join(err, log.Close()); err != nil {
		t.Fatal(err)
	}
	if err := logFrom(dir, []string{"4"}); !errors.Is(err, errDrained) {
		t.Fatalf("want errDrained, got %v", err)
	}
}
