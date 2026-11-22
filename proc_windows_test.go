//go:build windows

package main

import (
	"syscall"
	"testing"
	"unsafe"
)

func TestAppServerInOwnedJobObject(t *testing.T) {
	root, work, _ := runnerFixture(t)
	s, err := openServer(config{Dir: work}, root, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.tree.kill()
		close(s.stop)
		closeBestEffort(s.in)
		_ = waitProcess(s.cmd, 5e9)
		s.tree.cleanup()
	}()
	var member int32
	ok, _, err := syscall.NewLazyDLL("kernel32.dll").NewProc("IsProcessInJob").Call(uintptr(s.tree.process), uintptr(s.tree.job), uintptr(unsafe.Pointer(&member)))
	if s.tree.job == 0 || ok == 0 || member == 0 {
		t.Fatal("app-server outside owned job", err)
	}
}
