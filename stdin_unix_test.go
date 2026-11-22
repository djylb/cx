//go:build darwin || linux

package main

import (
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// A watchdog that closes standard input must end a read blocked on a silent pipe.
func TestInterruptibleStdinUnblocksRead(t *testing.T) {
	if os.Getenv("CX_TEST_STDIN_CHILD") == "1" {
		in := interruptibleStdin()
		time.AfterFunc(100*time.Millisecond, func() { _ = in.Close() })
		_, _ = io.ReadAll(in)
		os.Exit(0)
	}
	var fds [2]int
	// syscall.Pipe leaves both ends blocking, like the pipe sshd gives a remote command.
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	r, w := os.NewFile(uintptr(fds[0]), "r"), os.NewFile(uintptr(fds[1]), "w")
	defer func() { _ = w.Close() }()
	cmd := exec.Command(os.Args[0], "-test.run=^TestInterruptibleStdinUnblocksRead$")
	cmd.Env = append(os.Environ(), "CX_TEST_STDIN_CHILD=1")
	cmd.Stdin = r
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("read on a silent stdin pipe was not interrupted")
	}
}
