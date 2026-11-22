package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

type controllerFixture struct{ home, cx, python string }

func newControllerFixture(t *testing.T) controllerFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Unix OpenSSH shell fixture")
	}
	for _, key := range []string{"CX_SESSION", "CLAUDE_CODE_HOST_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"} {
		t.Setenv(key, "")
	}
	dir := t.TempDir()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"cx", "cxrun"} {
		if e = os.Symlink(exe, filepath.Join(dir, name)); e != nil {
			t.Fatal(e)
		}
	}
	py, _ := pythonReference(t, "cx-control.py")
	state := filepath.Join(dir, "state")
	setTestHost(t, state, nil, "")
	for k, v := range map[string]string{"HOME": dir, "CXRUN_STATE": state, "CX_HOME": dir, "CX_PROJECT": "p", "CX_HOSTS": "local", "CXRUN_BIN": filepath.Join(dir, "cxrun"), "PYTHONDONTWRITEBYTECODE": "1", "GORACE": "atexit_sleep_ms=1", "CX_CHAT_LOCAL_ROOT": ""} {
		t.Setenv(k, v)
	}
	return controllerFixture{dir, filepath.Join(dir, "cx"), py}
}
func cliOutput(t *testing.T, code int, stdin []byte, exe string, args ...string) []byte {
	t.Helper()
	stdout, _ := cliStreams(t, code, stdin, exe, args...)
	return stdout
}

func cliStreams(t *testing.T, code int, stdin []byte, exe string, args ...string) ([]byte, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Stdin = bytes.NewReader(stdin)
	var diag bytes.Buffer
	cmd.Stderr = &diag
	b, e := cmd.Output()
	actual := 0
	if e != nil {
		var exit *exec.ExitError
		if !errors.As(e, &exit) {
			t.Fatal(e)
		}
		actual = exit.ExitCode()
	}
	if len(args) > 0 && args[0] == "setup" && actual == 0 && diag.Len() != 0 {
		t.Fatalf("unexpected setup stderr: %s", diag.Bytes())
	}
	if actual != code {
		t.Fatalf("%s %v: expected %d, got %d: %s %s", exe, args, code, actual, b, diag.Bytes())
	}
	return b, diag.String()
}
func TestB1PythonContract(t *testing.T) {
	f := newControllerFixture(t)
	if f.python == "" {
		t.Log("cx-control.py unavailable: skipping controller output and exit-code comparisons")
	}
	a := []string{"exec", "local", "--tag", "sample", "--wait", "--", "/bin/echo", "a ; $x"}
	out := cliOutput(t, 0, nil, f.cx, a...)
	if string(out) != "a ; $x\n" {
		t.Fatalf("literal argv: %q", out)
	}
	cliOutput(t, 0, nil, f.cx, "host", "local", "--set", "slots.test=2")
	for _, tc := range []struct {
		args []string
		obj  bool
	}{
		{[]string{"ls", "--json"}, true}, {[]string{"status", "local:p/sample"}, true}, {[]string{"show", "local:p/sample", "--turn", "1"}, true}, {[]string{"host", "local"}, true},
		{[]string{"tail", "local:p/sample", "--raw"}, false}, {[]string{"tail", "local:p/sample", "--since", "0"}, false}, {[]string{"wait", "local:p/sample", "--timeout", "1s"}, false},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			got := cliOutput(t, 0, nil, f.cx, tc.args...)
			var a any
			if tc.obj {
				if err := json.Unmarshal(got, &a); err != nil {
					t.Fatalf("Go output %q: %v", got, err)
				}
			}
			if f.python == "" {
				return
			}
			want := cliOutput(t, 0, nil, "python3", append([]string{f.python}, tc.args...)...)
			if !tc.obj {
				if !bytes.Equal(got, want) {
					t.Fatalf("%q != %q", got, want)
				}
				return
			}
			var b any
			if json.Unmarshal(want, &b) != nil || !reflect.DeepEqual(a, b) {
				t.Fatalf("Go %s\nPython %s", got, want)
			}
		})
	}
	// Doctor and overview are fully offline. No real agent login is consulted.
	for _, tool := range []string{"codex", "claude"} {
		if e := os.WriteFile(filepath.Join(f.home, tool), []byte("#!/bin/sh\nexit 1\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, args := range [][]string{{"wait", "--any", "--project", "p", "--timeout", "2s"}, {"overview", "--project", "p", "--json"}} {
		got := cliOutput(t, 0, nil, f.cx, args...)
		var a, b fields
		if err := json.Unmarshal(got, &a); err != nil {
			t.Fatalf("Go output %q: %v", got, err)
		}
		if f.python == "" {
			continue
		}
		want := cliOutput(t, 0, nil, "python3", append([]string{f.python}, args...)...)
		if json.Unmarshal(want, &b) != nil {
			t.Fatalf("%s != %s", got, want)
		}
		if args[0] == "overview" {
			ga := a["local"].(map[string]any)
			gb := b["local"].(map[string]any)
			for _, k := range []string{"load1", "memory_available_bytes", "disk_free_bytes", "cpu_usage_percent"} {
				delete(ga, k)
				delete(gb, k)
			}
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%v: %s != %s", args, got, want)
		}
	}
	if b := cliOutput(t, 0, nil, f.cx, a...); len(b) != 0 {
		t.Fatalf("replayed acknowledged exec bytes: %q", b)
	}
	cliOutput(t, 1, nil, f.cx, "exec", "local", "--tag", "sample", "--", "/bin/echo", "changed")
	cliOutput(t, 2, nil, f.cx, "exec", "local", "--tag", "empty")
	cliOutput(t, 17, nil, f.cx, "exec", "local", "--wait", "--tag", "bad", "--", "/bin/sh", "-c", "exit 17")
	cliOutput(t, 17, nil, f.cx, "wait", "local:p/bad")
	cliOutput(t, 124, nil, f.cx, "wait", "--any", "--project", "missing", "--timeout", "30ms")
	if f.python != "" {
		cliOutput(t, 2, nil, "python3", f.python, "exec", "local", "--tag", "empty")
		cliOutput(t, 124, nil, "python3", f.python, "wait", "--any", "--project", "missing", "--timeout", "30ms")
	}
}
func TestB1ReceiveInterruptedMember(t *testing.T) {
	f := newControllerFixture(t)
	path := filepath.Join(f.home, "artifact")
	var wire bytes.Buffer
	z := gzip.NewWriter(&wire)
	if _, e := z.Write([]byte("prefix")); e != nil {
		t.Fatal(e)
	}
	if e := z.Close(); e != nil {
		t.Fatal(e)
	}
	cliOutput(t, 1, wire.Bytes()[:wire.Len()-4], f.cx, "node", "recv", path, "--offset", "0")
	if got := string(cliOutput(t, 0, nil, f.cx, "node", "recv", path, "--offset")); got != "6\n" {
		t.Fatal(got)
	}
	wire.Reset()
	z.Reset(&wire)
	_, _ = z.Write([]byte("suffix"))
	_ = z.Close()
	cliOutput(t, 0, wire.Bytes(), f.cx, "node", "recv", path, "--offset", "6")
	cliOutput(t, 1, wire.Bytes(), f.cx, "node", "recv", path, "--offset", "6")
	b, e := os.ReadFile(path)
	if e != nil || string(b) != "prefixsuffix" {
		t.Fatal(string(b), e)
	}
}
func TestB1RelaySetup(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(f.home, ".config", "cx")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	// Each alias has a separate home and cwd. The mock accepts only system SSH's
	// batch/timeout options, checks the jump alias, and never opens a socket.
	for _, name := range []string{"first", "second"} {
		p := filepath.Join(f.home, name)
		if e := os.MkdirAll(p, 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.Symlink(f.cx, filepath.Join(p, "cx")); e != nil {
			t.Fatal(e)
		}
	}
	cfg := fmt.Sprintf("[hosts.first]\nssh='first'\ncx='%s'\n[hosts.second]\nssh='second'\ncx='%s'\nvia='first'\n", filepath.Join(f.home, "first", "cx"), filepath.Join(f.home, "second", "cx"))
	if e := os.WriteFile(filepath.Join(dir, "hosts.toml"), []byte(cfg), 0600); e != nil {
		t.Fatal(e)
	}
	script := "#!/bin/sh\nset -eu\ntest \"$1\" = -oBatchMode=yes && test \"$2\" = -oConnectTimeout=90\ntest \"$3\" = -oControlMaster=auto && test \"$5\" = -oControlPersist=10m\nshift 5\nif test \"$1\" = -J; then test \"$2\" = first; shift 2; fi\ntest \"$1\" = --; shift\ncase \"$1\" in first|second);; *) exit 99;; esac\nif test \"$1\" = second; then test \"$PWD\" = \"$CX_FIXTURE/first\" || exit 98; fi\ncd \"$CX_FIXTURE/$1\"\nexport HOME=\"$PWD\" CXRUN_STATE=\"$PWD/state\"\nshift\n# Like ssh, join the remaining operands into one remote command line.\nexec /bin/sh -c \"$*\"\n"
	if e := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CX_FIXTURE", f.home)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	sftp := `#!/usr/bin/env python3
import os, sys, shlex, shutil
alias=sys.argv[-1]
assert alias == "second" and os.path.samefile(os.getcwd(),os.path.join(os.environ["CX_FIXTURE"], "first"))
os.chdir(os.path.join(os.environ["CX_FIXTURE"], alias))
for line in sys.stdin:
    op, *args=shlex.split(line)
    if op=="put": shutil.copyfile(*args)
    elif op=="chmod": os.chmod(args[1],int(args[0],8))
    elif op=="rm": os.remove(args[0])
    else: sys.exit(3)
`
	if e := os.WriteFile(filepath.Join(f.home, "sftp"), []byte(sftp), 0700); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(f.home, "second", "cx")
	if e := os.Remove(dest); e != nil {
		t.Fatal(e)
	}
	for _, tool := range []string{"claude", "codex"} {
		if e := os.WriteFile(filepath.Join(f.home, tool), []byte("#!/bin/sh\nexit 1\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	diag := cliOutput(t, 0, nil, f.cx, "doctor", "second", "--json")
	var report map[string]fields
	if e := json.Unmarshal(diag, &report); e != nil || report["second"]["node_available"] != false {
		t.Fatal(string(diag), e)
	}
	if _, e := os.Stat(dest); !os.IsNotExist(e) {
		t.Fatal("doctor installed cx")
	}
	cliOutput(t, 0, nil, f.cx, "setup", "second")
	cfgBytes, e := os.ReadFile(filepath.Join(dir, "hosts.toml"))
	if e != nil || !strings.Contains(string(cfgBytes), runtime.GOOS+"/"+runtime.GOARCH) {
		t.Fatal(string(cfgBytes), e)
	}
	before, e := os.Stat(dest)
	if e != nil {
		t.Fatal(e)
	}
	cliOutput(t, 0, nil, f.cx, "host", "second", "--set", "slots.test=4")
	cliOutput(t, 0, nil, f.cx, "setup", "second")
	after, e := os.Stat(dest)
	if e != nil || !os.SameFile(before, after) {
		t.Fatal("unchanged version replaced", e)
	}
	out := cliOutput(t, 0, nil, f.cx, "host", "second")
	if !bytes.Contains(out, []byte(`"test":4`)) {
		t.Fatal(string(out))
	}
	cache, e := filepath.Glob(filepath.Join(f.home, "first", ".cache", "cx", "bin", "*", "*", "cx"))
	if e != nil || len(cache) != 1 {
		t.Fatal(cache, e)
	}
}

type fixtureDownload func(*http.Request) (*http.Response, error)

func (fn fixtureDownload) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }
func TestB1ReleaseFallback(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("PATH", f.home)
	t.Setenv("CX_SETUP_BINARY", "")
	version := strings.Repeat("b", 64)
	plat := "linux/arm64"
	if runtime.GOOS+"/"+runtime.GOARCH == plat {
		plat = "windows/amd64"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, e := cacheArtifact(ctx, version, plat, releaseSource{}); e == nil {
		t.Fatal("missing release config accepted")
	}
	requests := 0
	old := http.DefaultClient
	defer func() { http.DefaultClient = old }()
	http.DefaultClient = &http.Client{Transport: fixtureDownload(func(r *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("release"))}, nil
	})}
	key := strings.ReplaceAll(plat, "/", "-")
	release := releaseSource{URL: "fixture:/{platform}/{version}", SHA256: map[string]string{key: strings.Repeat("0", 64)}}
	if _, e := cacheArtifact(ctx, version, plat, release); e == nil {
		t.Fatal("bad digest accepted")
	}
	path, _ := cachePath(version, plat)
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("failed release published", e)
	}
	release.SHA256[key] = fmt.Sprintf("%x", sha256.Sum256([]byte("release")))
	for i := 0; i < 2; i++ {
		if _, e := cacheArtifact(ctx, version, plat, release); e != nil {
			t.Fatal(e)
		}
	}
	if requests != 2 {
		t.Fatal("cache miss", requests)
	}
}

func TestWatchWireCancellation(t *testing.T) {
	f := newControllerFixture(t)
	// A local SSH stand-in acknowledges both the request and the cancellation EOF.
	mock := `#!/bin/sh
IFS= read -r request || exit 1
echo ready
cat >/dev/null
echo closed
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(mock), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h := endpoint{SSH: "fixture", CX: "cx"}
	cmd, err := h.command(ctx, []string{"wait-poll"})
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(stdout)
	ready, readErr := r.ReadString('\n')
	cancel()
	ack, ackErr := io.ReadAll(r)
	waitErr := cmd.Wait()
	if readErr != nil || ready != "ready\n" || ackErr != nil || string(ack) != "closed\n" {
		t.Fatalf("watch-wire handshake: %q (%v), cancellation: %q (%v), wait: %v", ready, readErr, ack, ackErr, waitErr)
	}
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatal(waitErr)
	}
}

func TestB1ConnectionAndProgressClocks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell clock fixture")
	}
	for _, tc := range []struct {
		name, script string
		expired      bool
	}{
		{"slow handshake", "echo proxy-banner >&2; sleep .6; echo connected; sleep .1; echo done", false},
		{"active transfer", "for i in 1 2 3 4 5; do echo data; sleep .1; done", false},
		{"silent node", "printf 'cx-wire: ready\n' >&2; exec sleep 3", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The idle budget leaves room for process exit and reaping on slow CI runners;
			// the handshake sleep still exceeds it, so only the connect clock can cover it.
			ctx, stop := context.WithTimeout(context.Background(), 4*time.Second)
			defer stop()
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", tc.script)
			e := runActive(cmd, stop, 2*time.Second, 400*time.Millisecond, io.Discard, io.Discard)
			if errors.Is(e, context.DeadlineExceeded) != tc.expired || !tc.expired && e != nil {
				t.Fatal(e)
			}
		})
	}
	for _, tc := range []struct {
		platform, diag string
		fallback       bool
	}{{"windows", "Bad configuration option: ControlMaster", true}, {"windows", "Host key verification failed", false}, {"linux", "Bad configuration option: ControlMaster", false}} {
		if unsupportedDiagnostic(tc.platform, tc.diag) != tc.fallback {
			t.Fatal(tc)
		}
	}
}

func TestB1RawCursorAndFullJSON(t *testing.T) {
	f := newControllerFixture(t)
	payload := strings.Repeat("字", 400) + "$x"
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "large", "--wait", "--", "/bin/echo", payload)
	for i, want := range []string{payload + "\n", ""} {
		got := cliOutput(t, 0, nil, f.cx, "tail", "local:p/large", "--raw", "--cursor", "consumer")
		if string(got) != want {
			t.Fatalf("cursor pass %d: %d bytes", i, len(got))
		}
	}
	for _, tc := range []struct {
		args []string
		full bool
	}{{[]string{"show", "local:p/large", "--json", "--max-bytes", "1"}, true}, {[]string{"show", "local:p/large"}, false}} {
		b := cliOutput(t, 0, nil, f.cx, tc.args...)
		var obj struct{ Report string }
		if e := json.Unmarshal(b, &obj); e != nil {
			t.Fatal(e)
		}
		if (obj.Report == payload+"\n") != tc.full {
			t.Fatal("incorrect report preview")
		}
	}
}
