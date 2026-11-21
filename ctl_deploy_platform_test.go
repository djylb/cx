package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

func TestPlatformParsesWindowsQuotedEcho(t *testing.T) {
	for in, want := range map[string]string{
		"windows AMD64\"\r\n": "windows/amd64",
		"Linux x86_64\n":      "linux/amd64",
		"Darwin arm64":        "darwin/arm64",
	} {
		if got, err := platform(in); err != nil || got != want {
			t.Fatalf("platform(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestWindowsMissingVersionProbe(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing 'cx'.exe")
	command := windowsNodeCommand(missing, []string{"node", "wire"}, true)
	args := strings.Fields(command)
	raw, err := base64.StdEncoding.DecodeString(args[len(args)-1])
	if err != nil || len(raw)%2 != 0 {
		t.Fatalf("invalid PowerShell encoding: %v", err)
	}
	var words []uint16
	for i := 0; i < len(raw); i += 2 {
		words = append(words, binary.LittleEndian.Uint16(raw[i:]))
	}
	script := string(utf16.Decode(words))
	if len(args) != 5 || args[1] != "-NoProfile" || args[2] != "-NonInteractive" || !strings.HasPrefix(script, "$ProgressPreference='SilentlyContinue';") {
		t.Fatalf("noninteractive, quiet PowerShell required: %s", command)
	}
	if !strings.Contains(script, psQuote(missing)) || !strings.Contains(script, "NativeErrorCode -in 2,3) {exit 127};throw") {
		t.Fatalf("missing file handling: %s", script)
	}
	if runtime.GOOS == "windows" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != missingNodeBinary || len(out) != 0 {
			t.Fatalf("missing executable: %q, %v", out, err)
		}
	}
}

func TestFirstInstallProbes(t *testing.T) {
	f := newControllerFixture(t)
	stub := `#!/usr/bin/env python3
import base64, os, sys
line = sys.argv[-1]
if line.startswith("powershell "):
    exe, *options, encoded = line.split()
    script = base64.b64decode(encoded).decode("utf-16le")
    if options != ["-NoProfile", "-NonInteractive", "-EncodedCommand"] or not script.startswith("$ProgressPreference='SilentlyContinue';"):
        print("#< CLIXML Preparing modules for first use", file=sys.stderr)
    sys.exit(127)
os.execl("/bin/sh", "sh", "-c", line)
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, plat := range []string{"linux/amd64", "windows/amd64"} {
		h := endpoint{SSH: "fixture", Platform: plat, CX: filepath.Join(f.home, "missing 'cx'")}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cmd, err := h.command(ctx, []string{"version"})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		b, err := cmd.CombinedOutput()
		cancel()
		var ex *exec.ExitError
		if !errors.As(err, &ex) || ex.ExitCode() != missingNodeBinary || len(b) != 0 {
			t.Fatalf("%s probe: %q, %v", plat, b, err)
		}
	}
}

func TestExplicitInstallPathsSurviveDetection(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(f.home, ".config", "cx")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := "[hosts.fixed]\nssh='fixture'\ncx='.local/bin/cx'\nstate='custom state'\n[hosts.automatic]\nssh='fixture'\n"
	if err := os.WriteFile(filepath.Join(dir, "hosts.toml"), []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	hs, err := loadEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	fixed, auto := hs["fixed"], hs["automatic"]
	fixed.setPlatform("windows/amd64")
	auto.setPlatform("windows/amd64")
	if fixed.CX != ".local/bin/cx" || fixed.State != "custom state" || auto.CX != ".local/bin/cx.exe" {
		t.Fatalf("fixed=%+v automatic=%+v", fixed, auto)
	}
}
