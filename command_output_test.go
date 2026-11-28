package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandLogPrefixExcludesAbandonedWrite(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{commandOutputStream, commandOutputCursor} {
		snapWriteTest(t, filepath.Join(dir, name), "")
	}
	out, err := openCommandOutputWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = out.Write([]byte("committed")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	blocked, resume := make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	go func() {
		_, blockedErr := out.write([]byte("late"), func(p []byte) (int, error) {
			n, writeErr := out.stream.Write(p)
			close(blocked)
			<-resume
			return n, writeErr
		})
		written <- blockedErr
	}()
	resumed := false
	defer func() {
		if !resumed {
			close(resume)
		}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkPrefix := func(s status) {
		t.Helper()
		var log strings.Builder
		readErr := writeJobLog(&log, dir, 0, s, nil)
		if readErr != nil && !errors.Is(readErr, errDrained) || log.String() != "committed" {
			t.Fatalf("visible output = %q, %v", log.String(), readErr)
		}
	}
	checkPrefix(status{Engine: "cmd"})
	size, err := out.seal(ctx, dir, true)
	if err != nil || size != int64(len("committed")) {
		t.Fatalf("sealed length = %d, %v", size, err)
	}
	close(resume)
	resumed = true
	select {
	case err := <-written:
		if !errors.Is(err, errOutputTruncated) {
			t.Fatalf("late write completed without truncation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkPrefix(status{Engine: "cmd", final: true})
	stage := t.TempDir()
	if name, _, err := copyResultLog(ctx, dir, stage, commandOutputName, "1", true, io.Discard); err != nil || name != commandOutputName {
		t.Fatalf("archive copy = %q, %v", name, err)
	}
	if contents, err := os.ReadFile(filepath.Join(stage, commandOutputName)); err != nil || string(contents) != "committed" {
		t.Fatalf("archive output = %q, %v", contents, err)
	}
}
