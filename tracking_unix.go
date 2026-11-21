//go:build linux || darwin

package main

import (
	"bytes"
	"syscall"
)

func marked(env []byte, run string) bool {
	if run == "" {
		return false
	}
	want := []byte("CXRUN_RUN=" + run)
	for len(env) > 0 {
		entry, rest, _ := bytes.Cut(env, []byte{0})
		if bytes.Equal(entry, want) {
			return true
		}
		env = rest
	}
	return false
}
func unixSignal(force bool) syscall.Signal {
	if force {
		return syscall.SIGKILL
	}
	return syscall.SIGTERM
}
