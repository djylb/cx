package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func commandFixture(args []string) int {
	switch args[0] {
	case "argv":
		fmt.Fprint(os.Stdout, args[1])
		fmt.Fprint(os.Stderr, args[2])
		return 37
	case "silent":
		time.Sleep(30 * time.Second)
	case "after-parent":
		for i := 0; i < 150; i++ {
			if _, err := os.Stat("parent-gone"); err == nil {
				fmt.Print("survived")
				return 0
			}
			time.Sleep(50 * time.Millisecond)
		}
	case "detached-parent":
		child := exec.Command(os.Args[0], "fixture-command", "spawn-sleep")
		if err := child.Run(); err != nil {
			return 2
		}
		if len(args) > 1 {
			return 0
		}
		time.Sleep(25 * time.Second)
	case "sleep":
		time.Sleep(22 * time.Second)
		return 0
	case "spawn-sleep":
		child := exec.Command(os.Args[0], "fixture-command", "sleep")
		if runtime.GOOS == "linux" {
			child = exec.Command("/bin/sleep", "22")
		}
		detachWorker(child)
		if err := child.Start(); err != nil {
			return 2
		}
		if err := writeJSON("sleep.json", process{PID: child.Process.Pid, Start: processStart(child.Process.Pid)}); err != nil {
			return 2
		}
		return 0
	case "tree":
		exe, err := os.Executable()
		if err != nil {
			return 2
		}
		child := exec.Command(exe, "fixture-command", "grandchild")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err = child.Run(); err != nil {
			return 2
		}
	case "grandchild":
		signal.Ignore(syscall.SIGTERM)
		held, err := lockPath("grandchild.lock", os.O_CREATE)
		if err != nil || held == nil {
			return 2
		}
		defer held.Close()
		fmt.Println("grandchild ready")
		time.Sleep(30 * time.Second)
	}
	return 2
}

type commandHarness struct {
	root, work, exe string
	ctx             context.Context
	t               *testing.T
}

func commands(t *testing.T) *commandHarness {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	setTestHost(t, root, map[string]int{"codex": 1, "test": 1}, "")
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	return &commandHarness{root, t.TempDir(), exe, ctx, t}
}

// running blocks until job owns its slot (start returns before admission, so queue-order tests need this).
func (h *commandHarness) running(job string) {
	h.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(h.cli(0, "status", "--job", "p/"+job), `"status": "running"`); {
		if time.Now().After(deadline) {
			h.t.Fatalf("%s never started", job)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *commandHarness) cli(rc int, args ...string) string {
	h.t.Helper()
	cmd := exec.CommandContext(h.ctx, h.exe, append([]string{"fixture-cli"}, args...)...)
	b, err := cmd.CombinedOutput()
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != rc {
		h.t.Fatalf("%v: %s: %v", args, b, err)
	}
	return string(b)
}
func (h *commandHarness) start(id string, opts []string, helper ...string) {
	h.t.Helper()
	args := []string{"start", "--project", "p", "--job", id, "--dir", h.work, "--cmd"}
	args = append(args, opts...)
	args = append(args, "--", h.exe, "fixture-command")
	h.cli(0, append(args, helper...)...)
}
func (h *commandHarness) status(id string) status {
	h.t.Helper()
	var s status
	if err := readJSON(filepath.Join(h.root, "p", id, "status.json"), &s); err != nil {
		h.t.Fatal(err)
	}
	return s
}

func TestCommandArgvExitAndLog(t *testing.T) {
	h := commands(t)
	// A saturated Codex class must not delay a test command.
	g, err := registryLock(h.ctx, h.root)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	held, err := lockPath(filepath.Join(h.root, ".admit", "slot-0.lock"), os.O_CREATE)
	if err != nil || held == nil {
		t.Fatal(err)
	}
	defer held.Close()
	h.start("args", []string{"--session", "suite"}, "argv", "a $b * ;", "tail")
	if out := h.cli(1, "wait", "--job", "p/args"); !strings.Contains(out, "exit_code=37") {
		t.Fatal(out)
	}
	s := h.status("args")
	if s.ExitCode == nil || *s.ExitCode != 37 || s.State != "failed" || s.Session != "suite" {
		t.Fatal(s)
	}
	want := "a $b * ;tail"
	if got := h.cli(10, "log", "--job", "p/args", "2"); got != want[2:] {
		t.Fatal(got)
	}
	if got := h.cli(0, "tail", "--job", "p/args"); got != want {
		t.Fatal(got)
	}
	b, err := os.ReadFile(filepath.Join(h.root, "p", "args", "last.md"))
	if err != nil || string(b) != want {
		t.Fatal(string(b), err)
	}
}
func TestCommandAfterSubmitterExits(t *testing.T) {
	h := commands(t)
	h.start("detach", []string{"--timeout", "12s", "--dedup-key", "receipt"}, "after-parent")
	// cli has waited for the actual submitter to exit and for its stdout/stderr EOF.
	if out := h.cli(124, "wait", "--job", "p/detach", "--timeout", "80ms"); !strings.Contains(out, "wait_timeout=true") {
		t.Fatal(out)
	}
	if err := os.WriteFile(filepath.Join(h.work, "parent-gone"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "wait", "--job", "p/detach")
	if out := h.cli(10, "log", "--job", "p/detach"); out != "survived" {
		t.Fatal(out)
	}
	h.cli(0, "clean", "--job", "p/detach")
	out := h.cli(7, "start", "--job", "p/dup", "--cmd", "--dedup-key", "receipt", "--", h.exe)
	var s status
	if err := json.Unmarshal([]byte(out), &s); err != nil || s.Job != "detach" {
		t.Fatal(out, err)
	}
}
func TestCommandQuietWatchdog(t *testing.T) {
	h := commands(t)
	h.start("quiet", []string{"--quiet-timeout", "250ms"}, "silent")
	if out := h.cli(3, "wait", "--job", "p/quiet"); !strings.Contains(out, "status=stalled") {
		t.Fatal(out)
	}
}
func TestCloseReapsGrandchildAfterTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture ignores POSIX TERM")
	}
	h := commands(t)
	h.start("tree", []string{"--timeout", "2s"}, "tree")
	if out := h.cli(4, "wait", "--job", "p/tree"); !strings.Contains(out, "status=timeout") {
		t.Fatal(out)
	}
	if got := h.cli(10, "log", "--job", "p/tree"); got != "grandchild ready\n" {
		t.Fatal(got)
	}
	h.cli(0, "close", "--job", "p/tree")
	f, err := lockPath(filepath.Join(h.work, "grandchild.lock"), 0)
	if err != nil || f == nil {
		t.Fatal("grandchild still holds lock", err)
	}
	f.Close()
}

func TestCommandClassQueueAndClose(t *testing.T) {
	h := commands(t)
	h.start("holder", nil, "after-parent")
	h.running("holder") // start returns before admission: holder must own the only slot first
	h.start("queued", nil, "silent")
	if s := h.status("queued"); s.State != "queued" || s.CommandPID != 0 {
		t.Fatal(s)
	}
	h.start("deadline", []string{"--queue-timeout", "300ms"}, "silent")
	if out := h.cli(4, "wait", "--job", "p/deadline"); !strings.Contains(out, "status=queue_timeout") {
		t.Fatal(out)
	}
	if s := h.status("deadline"); s.State != "queue_timeout" || s.ExitCode != nil {
		t.Fatal(s)
	}
	h.cli(0, "close", "--job", "p/queued")
	h.cli(1, "wait", "--job", "p/queued")
	h.cli(0, "close", "--job", "p/holder")
	h.cli(1, "wait", "--job", "p/holder")
}
func TestExplicitCommandShell(t *testing.T) {
	h := commands(t)
	script := "printf 'from shell'"
	if runtime.GOOS == "windows" {
		script = "echo from shell"
	}
	h.cli(0, "start", "--job", "p/shell", "--dir", h.work, "--cmd-shell", "--", script)
	h.cli(0, "wait", "--job", "p/shell")
	if got := strings.TrimSpace(h.cli(10, "log", "--job", "p/shell")); got != "from shell" {
		t.Fatal(got)
	}
}

func TestWindowsCommandExitCode(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows unsigned 32-bit process exit code")
	}
	h := commands(t)
	h.cli(0, "start", "--job", "p/win", "--cmd-shell", "--", "exit /b -1073741819")
	if out := h.cli(1, "wait", "--job", "p/win"); !strings.Contains(out, "exit_code=3221225477") {
		t.Fatal(out)
	}
}

func TestCommandRuntimeStartsAfterAdmission(t *testing.T) {
	h := commands(t)
	h.start("holder", nil, "after-parent")
	h.running("holder") // start returns before admission: holder must own the only slot first
	h.start("late", []string{"--timeout", "500ms"}, "argv", "late", " start")
	if out := h.cli(124, "wait", "--job", "p/late", "--timeout", "750ms"); !strings.Contains(out, "status=queued wait_timeout=true") {
		t.Fatal(out)
	}
	if err := os.WriteFile(filepath.Join(h.work, "parent-gone"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if out := h.cli(1, "wait", "--job", "p/late"); !strings.Contains(out, "exit_code=37") {
		t.Fatal(out)
	}
}
