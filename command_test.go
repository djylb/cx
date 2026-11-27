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
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func commandFixture(args []string) int {
	switch args[0] {
	case "package-cache-env":
		if err := json.NewEncoder(os.Stdout).Encode(packageCacheFixtureEnv()); err != nil {
			return 2
		}
		return 0
	case "argv":
		if _, err := fmt.Fprint(os.Stdout, args[1]); err != nil {
			return 2
		}
		if _, err := fmt.Fprint(os.Stderr, args[2]); err != nil {
			return 2
		}
		return 37
	case "silent":
		time.Sleep(fixtureLimit)
	case "after-parent":
		ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
		defer cancel()
		if err := awaitFixtureFile(ctx, "parent-gone"); err != nil {
			return 2
		}
		if _, err := fmt.Print("survived"); err != nil {
			return 2
		}
		return 0
	case "detached-parent":
		child := exec.Command(os.Args[0], "fixture-command", "spawn-sleep")
		if len(args) > 2 && args[2] == "unmarked" {
			child.Env = slices.DeleteFunc(child.Environ(), func(entry string) bool {
				name, _, _ := strings.Cut(entry, "=")
				return strings.EqualFold(name, "CXRUN_RUN")
			})
		}
		if err := child.Run(); err != nil {
			return 2
		}
		if len(args) > 1 {
			switch args[1] {
			case "exit":
				return 0
			case "gated":
				ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
				defer cancel()
				if err := awaitFixtureFile(ctx, "allow-exit"); err != nil {
					return 2
				}
				return 0
			}
		}
		time.Sleep(fixtureLimit)
	case "sleep":
		time.Sleep(fixtureLimit)
		return 0
	case "spawn-sleep":
		child := exec.Command(os.Args[0], "fixture-command", "sleep")
		if runtime.GOOS == "linux" {
			child = exec.Command("/bin/sleep", "120")
		}
		detachWorker(child)
		if err := child.Start(); err != nil {
			return 2
		}
		if err := writeJSON("sleep.json", process{PID: child.Process.Pid, Start: processStart(child.Process.Pid)}); err != nil {
			return 2
		}
		return 0
	case "cache-parent":
		if err := launchCacheFixture(len(args) > 1); err != nil {
			diagnosticf("%v\n", err)
			return 2
		}
		return 0
	case "cache-child":
		if err := useCacheFixture(); err != nil {
			diagnosticf("%v\n", err)
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
		defer func() { _ = held.Close() }()
		if err := writeJSON("grandchild.json", os.Getpid()); err != nil {
			return 2
		}
		if _, err := fmt.Println("grandchild ready"); err != nil {
			return 2
		}
		time.Sleep(fixtureLimit)
	}
	return 2
}

type commandHarness struct {
	ctx             context.Context
	t               *testing.T
	root, work, exe string
}

func commands(t *testing.T) *commandHarness {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	setTestHost(t, root, map[string]int{"agent": 1, "test": 1}, "")
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	joinFixtureWorkers(t, root)
	ctx, cancel := fixtureContext(t)
	t.Cleanup(cancel)
	return &commandHarness{root: root, work: t.TempDir(), exe: exe, ctx: ctx, t: t}
}

// running blocks until job owns its slot (start returns before admission, so queue-order tests need this).
func (h *commandHarness) running(job string) {
	h.t.Helper()
	_, err := awaitFixtureStatus(h.ctx, filepath.Join(h.root, "p", job), func(s status) bool { return s.State == "running" })
	if err != nil {
		h.t.Fatalf("%s never started: %v", job, err)
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
	if err := loadJobStatusInto(filepath.Join(h.root, "p", id), &s); err != nil {
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
	if closeErr := g.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	held, err := lockPath(filepath.Join(h.root, ".admit", "slot-0.lock"), os.O_CREATE)
	if err != nil || held == nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
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
	b, err := readTurnReport(t.Context(), filepath.Join(h.root, "p", "args"), "1")
	if err != nil || string(b) != want {
		t.Fatal(string(b), err)
	}
}
func TestCommandAfterSubmitterExits(t *testing.T) {
	h := commands(t)
	h.start("detach", []string{"--timeout", "2m", "--dedup-key", "receipt"}, "after-parent")
	// cli has waited for the actual submitter to exit and for its stdout/stderr EOF.
	if runtime.GOOS != "windows" {
		barrier, err := lockPath(os.Getenv(fixtureWorkerLock), 0)
		if barrier != nil {
			closeBestEffort(barrier)
		}
		if err != nil || barrier != nil {
			t.Fatalf("detached worker must still hold the fixture lease: %v", err)
		}
	}
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
	if err := awaitFixtureOwnerReleased(h.ctx, filepath.Join(h.root, "p", "detach")); err != nil {
		t.Fatal(err)
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
	advance := h.clockJob("quiet")
	h.start("quiet", []string{"--quiet-timeout", "250ms"}, "silent")
	if runtime.GOOS == "windows" {
		// Get the worker's process handle before advancing the watchdog.
		// Terminal status clears the PID before the final cache scan finishes.
		pid := h.status("quiet").PID
		if pid <= 0 {
			t.Fatalf("invalid watchdog worker PID: %d", pid)
		}
		worker, err := os.FindProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			done := make(chan error, 1)
			go func() {
				_, err := worker.Wait()
				_ = worker.Release()
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("waiting for watchdog worker: %v", err)
				}
			case <-h.ctx.Done():
				t.Errorf("waiting for watchdog worker: %v", h.ctx.Err())
			}
		}()
	}
	advance(commandPollInterval, 250*time.Millisecond)
	if out := h.cli(3, "wait", "--job", "p/quiet"); !strings.Contains(out, "status=stalled") {
		t.Fatal(out)
	}
}
func TestCloseReapsGrandchildAfterTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture ignores POSIX TERM")
	}
	h := commands(t)
	advance := h.clockJob("tree")
	h.start("tree", []string{"--timeout", "2s"}, "tree")
	if err := awaitFixtureFile(h.ctx, filepath.Join(h.work, "grandchild.json")); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := readJSON(filepath.Join(h.work, "grandchild.json"), &pid); err != nil || pid <= 0 {
		t.Fatalf("grandchild PID: %d, %v", pid, err)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	lockPathname := filepath.Join(h.work, "grandchild.lock")
	t.Cleanup(func() {
		defer func() { _ = p.Release() }()
		lock, lockErr := lockPath(lockPathname, 0)
		if lockErr == nil && lock == nil {
			_ = p.Kill()
		}
		if lock != nil {
			closeBestEffort(lock)
		}
	})
	if waitErr := awaitFixtureCondition(h.ctx, func() (bool, error) {
		data, readErr := os.ReadFile(filepath.Join(h.root, "p", "tree", "output.log"))
		return strings.Contains(string(data), "grandchild ready\n"), readErr
	}); waitErr != nil {
		t.Fatal(waitErr)
	}
	advance(2*time.Second, 2*time.Second)
	if out := h.cli(4, "wait", "--job", "p/tree"); !strings.Contains(out, "status=timeout") {
		t.Fatal(out)
	}
	if got := h.cli(10, "log", "--job", "p/tree"); got != "grandchild ready\n" {
		t.Fatal(got)
	}
	h.cli(0, "close", "--job", "p/tree")
	f, err := lockPath(lockPathname, 0)
	if err != nil || f == nil {
		t.Fatal("grandchild still holds lock", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandClassQueueAndClose(t *testing.T) {
	h := commands(t)
	h.start("holder", nil, "after-parent")
	h.running("holder") // start returns before admission: holder must own the only slot first
	h.start("queued", nil, "silent")
	if s := h.status("queued"); s.State != "queued" || s.CommandPID != 0 {
		t.Fatal(s)
	}
	h.cli(0, "close", "--job", "p/queued")
	h.cli(1, "wait", "--job", "p/queued")
	h.cli(0, "close", "--job", "p/holder")
	h.cli(1, "wait", "--job", "p/holder")
	for _, name := range []string{"queued", "holder"} {
		in, err := readInboxState(filepath.Join(h.root, "p", name))
		if err != nil || in.acks[1].Result != "delivered" {
			t.Fatalf("%s did not acknowledge the completed close: %+v (%v)", name, in, err)
		}
	}
}
func TestCommandQueueDeadline(t *testing.T) {
	h := commands(t)
	pool := filepath.Join(h.root, ".admit", "classes", testSlotClass)
	if err := os.MkdirAll(pool, 0700); err != nil {
		t.Fatal(err)
	}
	slot, err := lockPath(filepath.Join(pool, "slot-0.lock"), os.O_CREATE)
	if err != nil || slot == nil {
		t.Fatalf("hold admission slot: %v", err)
	}
	defer closeBestEffort(slot)
	advance := h.clockJob("deadline")
	h.start("deadline", []string{"--queue-timeout", "300ms"}, "silent")
	advance(300*time.Millisecond, 300*time.Millisecond)
	if out := h.cli(4, "wait", "--job", "p/deadline"); !strings.Contains(out, "status=queue_timeout") {
		t.Fatal(out)
	}
	if s := h.status("deadline"); s.State != "queue_timeout" || s.ExitCode != nil {
		t.Fatal(s)
	}
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
	advance := h.clockJob("late")
	h.start("late", []string{"--timeout", "500ms"}, "argv", "late", " start")
	advance(0, time.Second)
	if s := h.status("late"); s.State != "queued" || s.CommandPID != 0 {
		t.Fatal(s)
	}
	if err := os.WriteFile(filepath.Join(h.work, "parent-gone"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if out := h.cli(1, "wait", "--job", "p/late"); !strings.Contains(out, "exit_code=37") {
		t.Fatal(out)
	}
}
