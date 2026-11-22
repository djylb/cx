//go:build darwin || linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A remote call that gets no reply at all retires the shared SSH master, so a master
// wedged on a stalled link does not swallow every later invocation.
func TestSilentInvocationRetiresMaster(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", t.TempDir())
	var retired []string
	old := retireMaster
	retireMaster = func(host string) { retired = append(retired, host) }
	defer func() { retireMaster = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	h := endpoint{SSH: "worker", CX: "cx", Platform: "linux/amd64"}
	if _, err := h.call(ctx, time.Second, "status", "--job", "p/j"); err == nil {
		t.Fatal("silent call succeeded")
	}
	if len(retired) != 1 || retired[0] != "worker" {
		t.Fatalf("retired %v", retired)
	}

	// A canceled caller says nothing about the link.
	retired = nil
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	_, _ = h.call(ctx, time.Second, "status", "--job", "p/j")
	if len(retired) != 0 {
		t.Fatalf("retired on cancel: %v", retired)
	}
}
