//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFreshRunnerKeepsRegistrationOwner(t *testing.T) {
	root := t.TempDir()
	setTestHost(t, root, nil, "")
	prompt := filepath.Join(t.TempDir(), "prompt")
	if err := os.WriteFile(prompt, []byte("task"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	register := func(ctx context.Context, parent, dir string, c config, s status, syncConfig func(*stateWriter) error) (*os.File, error) {
		owner, err := registerJob(ctx, parent, dir, c, s, syncConfig)
		if err != nil {
			return nil, err
		}
		// Keep the same open file description alive as a forked child
		// would before exec. A fresh open cannot take this flock.
		dup, err := unix.FcntlInt(owner.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			return nil, errors.Join(err, owner.Close())
		}
		t.Cleanup(func() { _ = unix.Close(dup) })
		// A queued close should reach a terminal record without an engine.
		turn := ""
		if _, err := publishControl(ctx, dir, "close", markerOptions{Turn: &turn}, nil); err != nil {
			return nil, errors.Join(err, owner.Close())
		}
		return owner, nil
	}
	if err := runNodeWithRegistration(ctx, []string{"start", "--state", root, "--job", "p/j", "--dir", filepath.Dir(prompt), "--prompt-file", prompt}, false, register); err != nil {
		t.Fatal(err)
	}
	s, _, err := loadJobStatus(filepath.Join(root, "p", "j"))
	if err != nil || !s.final || s.State != "done" || s.CompletedStatus != "interrupted" || s.PID != 0 {
		t.Fatalf("expected a terminal queued close, got %+v: %v", s, err)
	}
}
