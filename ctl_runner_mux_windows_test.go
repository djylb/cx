package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestMuxWorkerStartsWithDeleteDeniedDescriptor(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "worker.json")
	log := filepath.Join(root, "worker.log")
	spec := muxLaunch{Env: map[string]string{"CXRUN_STATE": root}, Log: log, Args: []string{"host"}}
	if err := writeJSON(path, spec); err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(path))
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = windows.CloseHandle(h) }()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "node", "runner-worker", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("worker required descriptor deletion: %s (%v)", out, err)
	}
	var settings hostConfig
	if err := readJSON(log, &settings); err != nil {
		t.Fatalf("worker did not reach node entry point: %v", err)
	}
}
