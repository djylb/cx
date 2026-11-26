package main

import (
	"context"
	"crypto/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
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
	ctx, cancel := fixtureContext(t)
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

func TestWindowsPsmuxReapsExactJobWindow(t *testing.T) {
	program := os.Getenv("CX_TEST_PSMUX")
	if program == "" {
		t.Skip("set CX_TEST_PSMUX to a psmux executable")
	}
	h := commands(t)
	socket, session := "cx-test-"+rand.Text(), "bootstrap"
	t.Setenv("CX_PSMUX", "")
	t.Setenv("CXRUN_TMUX_SOCKET", "")
	t.Setenv("CX_MUX_SESSION", "")
	cfg, err := readHostConfig(h.root)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Psmux, cfg.MuxSocket, cfg.MuxSession = program, socket, session
	if err := writeJSON(filepath.Join(h.root, hostConfigName), cfg); err != nil {
		t.Fatal(err)
	}
	mux := func(ctx context.Context, argv ...string) (string, error) {
		b, err := exec.CommandContext(ctx, program, append([]string{"-L", socket}, argv...)...).CombinedOutput()
		return string(b), err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = mux(ctx, "kill-server")
	})
	for _, args := range [][]string{
		{"new-session", "-d", "-s", session, "--", h.exe, "fixture-command", "sleep"},
		{"set-option", "-g", "remain-on-exit", "on"},
	} {
		if out, err := mux(h.ctx, args...); err != nil {
			t.Fatalf("%v: %s (%v)", args, out, err)
		}
	}
	list := func() ([]string, error) {
		out, err := mux(h.ctx, "list-windows", "-a", "-F", "#{window_name}")
		return strings.Fields(out), err
	}
	bootstrap, err := list()
	if err != nil || len(bootstrap) != 1 {
		t.Fatalf("bootstrap windows = %v, %v", bootstrap, err)
	}
	cliOutput(t, 0, nil, h.exe, "node", "submit-command", "p/mux", h.work, "--",
		h.exe, "fixture-command", "after-parent")
	h.running("mux")
	run := h.status("mux")
	if run.Mux == nil || run.Mux.Session != session || run.Mux.Socket != socket {
		t.Fatalf("wrong launch target: %+v", run.Mux)
	}
	if windows, err := list(); err != nil || !slices.Contains(windows, run.Mux.Name) {
		t.Fatalf("running window missing: %v, %v", windows, err)
	}
	if err := os.WriteFile(filepath.Join(h.work, "parent-gone"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "wait", "--job", "p/mux")
	if err := awaitFixtureCondition(h.ctx, func() (bool, error) {
		names, err := list()
		return slices.Equal(names, bootstrap), err
	}); err != nil {
		t.Fatal("terminal job window did not close", err)
	}
	if err := run.Mux.close(h.ctx); err != nil {
		t.Fatal("repeat close", err)
	}
	if names, err := list(); err != nil || !slices.Equal(names, bootstrap) {
		t.Fatalf("repeat close affected bootstrap: %v, %v", names, err)
	}

	// Simulate a worker that died after recording its pane and final status.
	stale := muxWindow{Program: program, Socket: socket, Session: session, Name: "cx-stale"}
	for _, name := range []string{stale.Name, stale.Name + "-other"} {
		if out, err := mux(h.ctx, "new-window", "-d", "-t", session, "-n", name, "--", h.exe, "fixture-command", "sleep"); err != nil {
			t.Fatalf("create %s: %s (%v)", name, out, err)
		}
	}
	dir := filepath.Join(h.root, "p", "stale")
	runID := rand.Text()
	c := config{Project: "p", Engine: "cmd", RunID: runID}
	s := status{Project: "p", Job: "stale", Engine: "cmd", State: "done", RunID: runID, Mux: &stale}
	if err := register(filepath.Dir(dir), dir, c, s); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "clean", "--job", "p/stale")
	if names, err := list(); err != nil || !slices.Equal(names, append(bootstrap, stale.Name+"-other")) {
		t.Fatalf("clean affected the wrong windows: %v, %v", names, err)
	}
}
