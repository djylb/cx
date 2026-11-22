package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func setTestHost(t *testing.T, root string, slots map[string]int, load string) {
	t.Helper()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	// Fixture jobs must be admitted independently of the test volume's free space.
	// Tests exercising disk admission set their own minimum.
	host, err := readHostConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	if slots != nil {
		host.Slots = slots
	}
	var minFree uint64
	host.MinFreeBytes = &minFree
	host.LoadCmd = load
	if err := writeJSON(filepath.Join(root, hostConfigName), host); err != nil {
		t.Fatal(err)
	}
}

func TestHostCLI(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	t.Setenv("CXRUN_STATE", root)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	platformSlots := 3
	switch runtime.GOOS {
	case "linux":
		platformSlots = 10
	case "windows":
		platformSlots = 6
	}
	out := fixtureCLI(t, 0, "host")
	var effective hostConfig
	if err := json.Unmarshal([]byte(out), &effective); err != nil {
		t.Fatal(err, out)
	}
	if len(effective.Slots) != 3 || effective.Slots["codex"] != platformSlots || effective.Slots["test"] != platformSlots ||
		effective.Slots["system"] != 1 || effective.LoadCmd != "" {
		t.Fatal(out)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("host must not create the state root: %v", err)
	}

	override := t.TempDir()
	setTestHost(t, override, map[string]int{"codex": 2, "custom": 4}, "exit 1")
	out = fixtureCLI(t, 0, "host", "--state", override)
	effective = hostConfig{}
	if err := json.Unmarshal([]byte(out), &effective); err != nil {
		t.Fatal(err, out)
	}
	if len(effective.Slots) != 4 || effective.Slots["codex"] != 2 || effective.Slots["test"] != platformSlots ||
		effective.Slots["system"] != 1 || effective.Slots["custom"] != 4 || effective.LoadCmd != "exit 1" {
		t.Fatal(out)
	}
}

func TestInvalidHostConfig(t *testing.T) {
	for _, value := range []string{`{`, `null`, `{"slots":null}`, `{"slots":{"test":0}}`, `{"slots":{"test":-1}}`, `{"slots":{"test":"1"}}`, `{"slots":{"../test":1}}`} {
		t.Run(value, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "host.json"), []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readHostConfig(root); err == nil || !strings.Contains(err.Error(), "host.json") {
				t.Fatalf("invalid host config: %v", err)
			}
		})
	}
}

func TestStartRejectsOldAdmissionFlagsAndHostName(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	for _, option := range []string{"--slots", "--load-cmd"} {
		err := run(context.Background(), []string{"start", "--job", "job", option, "1"})
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("%s: %v", option, err)
		}
	}
	for _, job := range []string{"host.json", "HOST.JSON/job"} {
		err := run(context.Background(), []string{"start", "--job", job})
		if err == nil || !strings.Contains(err.Error(), "reserved at the state root") {
			t.Fatalf("%s: %v", job, err)
		}
	}
}
