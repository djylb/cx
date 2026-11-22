//go:build darwin || linux

package main

import (
	"os"
	"syscall"
)

// interruptibleStdin returns standard input as a pollable file. A blocking pipe read is
// not interrupted by Close, so idle and cancellation watchdogs would otherwise leave the
// process parked on a silent SSH pipe indefinitely.
func interruptibleStdin() *os.File {
	if syscall.SetNonblock(0, true) != nil {
		return os.Stdin
	}
	if f := os.NewFile(0, "/dev/stdin"); f != nil {
		return f
	}
	return os.Stdin
}
