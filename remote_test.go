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
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, append(waitArgs, "2s")); err == nil || errors.Is(err, errObserveTimeout) {
		t.Fatalf("broken status should fail immediately: %v", err)
	}
}

func fixture(t *testing.T, state string) string {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{"inbox", "inbox/pending", "inbox/done", "inbox/uncertain"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(`{"job":"j","status":"`+state+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A retried say with the same CXRUN_MSG_NAME delivers once, also after the message was archived and the job ended.
func TestSayNameIsIdempotent(t *testing.T) {
	dir := fixture(t, "waiting")
	t.Setenv("CXRUN_MSG_NAME", "0000000000000000001-0123456789abcdef.msg")
	if err := control(context.Background(), "say", dir, []string{"hello"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "inbox", "0000000000000000001-0123456789abcdef.msg"), filepath.Join(dir, "inbox/done", "0000000000000000001-0123456789abcdef.msg")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(`{"job":"j","status":"done"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := control(context.Background(), "say", dir, []string{"hello"}); err != nil {
		t.Fatalf("retry after archive and end: %v", err)
	}
	if e, _ := os.ReadDir(filepath.Join(dir, "inbox")); len(e) != 3 { // pending, done, uncertain
		t.Fatalf("inbox changed: %v", e)
	}
	t.Setenv("CXRUN_MSG_NAME", "bad")
	if err := control(context.Background(), "say", dir, []string{"x"}); err == nil {
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
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(`{"job":"j","status":"done"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := logFrom(dir, []string{"4"}); err != nil { // partial line pending: not drained
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), []byte("a\nb\npart\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := logFrom(dir, []string{"4"}); !errors.Is(err, errDrained) {
		t.Fatalf("want errDrained, got %v", err)
	}
}
