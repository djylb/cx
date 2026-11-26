package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestControllerHelp(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_TEST_COMMAND", filepath.Join(f.home, "missing-test-tool"))
	// A help invocation must bypass configuration loading and attachment reads.
	config := filepath.Join(f.home, ".config", "cx")
	if err := os.MkdirAll(config, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "hosts.toml"), []byte("["), 0600); err != nil {
		t.Fatal(err)
	}
	root := cliOutput(t, 0, nil, f.cx, "--help")
	for name := range ctlCommands {
		args := strings.Split(name, "-")
		t.Run(name, func(t *testing.T) {
			if len(args) == 1 && !bytes.Contains(root, []byte("\n  "+name+" ")) {
				t.Errorf("%s missing from top-level help", name)
			}
			for _, flag := range []string{"--help", "-h"} {
				out := cliOutput(t, 0, nil, f.cx, append(args, flag)...)
				if !bytes.Contains(out, []byte("Usage: cx "+strings.Join(args, " "))) ||
					!bytes.Contains(out, []byte("Exit codes:")) {
					t.Errorf("incomplete help: %s", out)
				}
			}
		})
	}
	cliOutput(t, 0, nil, f.cx, "-h")
	cliOutput(t, 0, nil, f.cx, "say", "missing", "@"+filepath.Join(f.home, "missing"), "--help")
	cliOutput(t, 0, nil, f.cx, "run", "local", "--help", "--", "unused")
	for _, args := range [][]string{
		{"--unknown"}, {"exec", "local", "--unknown"}, {"say", "missing", "--unknown=1"},
		{"fork", "missing", "--unknown"}, {"resume", "missing", "--unknown"},
		{"hosts", "--unknown"}, {"outbox", "--unknown"}, {"version", "--unknown"},
		{"chat", "read", "#room", "--unknown"}, {"project", "init", "p", "--unknown"},
		{"node", "--unknown"},
	} {
		ctx, cancel := fixtureContext(t)
		out, err := exec.CommandContext(ctx, f.cx, args...).CombinedOutput()
		cancel()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 || !bytes.Contains(bytes.ToLower(out), []byte("usage: cx")) {
			t.Errorf("%v: expected usage and exit 2, got %v, %s", args, err, out)
		}
	}
}
