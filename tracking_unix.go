//go:build linux || darwin

package main

import (
	"bytes"
	"strings"
	"syscall"
)

func reclaimTrackedJob(_ string) error { return nil }

func marked(env []byte, run string) bool {
	if run == "" {
		return false
	}
	key := "CXRUN_RUN="
	if strings.ContainsRune(run, '/') {
		key = "CXRUN_PROCESS_SCOPE="
	}
	want := []byte(key + run)
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
