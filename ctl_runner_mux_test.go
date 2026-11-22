package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestNodeWireAndDirectStateRoot(t *testing.T) {
	f := newControllerFixture(t)
	defaultRoot := filepath.Join(f.home, defaultStateDir)
	selected := filepath.Join(f.home, "selected")
	t.Setenv("CXRUN_STATE", "")
	for root, socket := range map[string]string{defaultRoot: "default-socket", selected: "selected-socket"} {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(root, hostConfigName), fields{"mux_socket": socket}); err != nil {
			t.Fatal(err)
		}
	}
	request, err := json.Marshal(invocation{State: selected, Args: []string{"host"}, Budget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(request)
	for _, args := range [][]string{{"wire", encoded}, {"watch-wire", encoded}, {"host", "--state=" + selected}} {
		var got hostConfig
		reply := cliOutput(t, 0, nil, f.cx, append([]string{"node"}, args...)...)
		if decodeErr := json.Unmarshal(reply, &got); decodeErr != nil || got.MuxSocket != "selected-socket" {
			t.Fatalf("%v: unexpected host settings: %s (%v)", args[:1], reply, decodeErr)
		}
	}
}

func TestMuxRunnerCarriesStateAcrossDaemon(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture stands in for the psmux daemon")
	}
	oldRoot, root := t.TempDir(), t.TempDir()
	t.Setenv("CXRUN_STATE", oldRoot)
	t.Setenv("CX_PSMUX", "")
	t.Setenv("CXRUN_TMUX_SOCKET", "")
	mux := filepath.Join(root, "fixture-mux")
	// Simulate a long-lived daemon with its own CXRUN_STATE, not the
	// environment of the client that made this launch request.
	script := "#!/bin/sh\n" +
		"test \"$1 $2\" = '-L selected-socket' || exit 20\n" +
		"shift 7\nexec \"$@\"\n"
	if err := os.WriteFile(mux, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(oldRoot, hostConfigName), fields{
		"psmux": filepath.Join(oldRoot, "missing-mux"), "mux_socket": "old-socket",
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(root, hostConfigName), fields{"psmux": mux, "mux_socket": "selected-socket"}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(root, "status.json"), status{State: "done"}); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = launchMuxRunner(ctx, root, exe, root, root, []string{"host"}, nil, false); err != nil {
		t.Fatal(err)
	}
	var got hostConfig
	if err = readJSON(filepath.Join(root, "launch.log"), &got); err != nil || got.MuxSocket != "selected-socket" {
		t.Fatalf("worker selected the wrong root: %q (%v)", got.MuxSocket, err)
	}
	t.Setenv("CXRUN_TMUX_SOCKET", "explicit-socket")
	if program, socket := psmuxSettings(root); program != mux || socket != "explicit-socket" {
		t.Fatal(program, socket)
	}
}
