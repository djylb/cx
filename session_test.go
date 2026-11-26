package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestCurrentSessionPriority(t *testing.T) {
	keys := []string{"CX_SESSION", "CLAUDE_CODE_HOST_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "OPENCODE_SESSION_ID"}
	for _, key := range keys {
		t.Setenv(key, key)
	}
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "not-a-session-id")
	t.Setenv("CODEX_SESSION_ID", "not-a-session-id")
	for _, key := range keys {
		if got := currentSession(); got != key {
			t.Fatalf("session priority at %s: %q", key, got)
		}
		t.Setenv(key, "")
	}
	if got := currentSession(); got != "" {
		t.Fatalf("unknown session: %q", got)
	}
}

func TestJobEnvironmentUsesExecutionHost(t *testing.T) {
	cmd := exec.Command("unused")
	cmd.Env = []string{"CX_SESSION=old", "CLAUDE_CODE_CHILD_SESSION=old", "CODEX_SESSION_ID=old", "OPENCODE_SESSION_ID=old"}
	if runtime.GOOS == "windows" {
		cmd.Env = append(cmd.Env, "claude_code_session_id=old")
	}
	c := config{
		Host: "remote", Project: "p", RunID: "fixture",
		localEnv: []string{"CODEX_THREAD_ID=old", "KEEP=runtime"},
		Env:      map[string]string{"CLAUDE_CODE_HOST_SESSION_ID": "old", "KEEP": "explicit"},
	}
	c.mark(cmd, filepath.Join("work", "j"))
	want := []string{"KEEP=runtime", "KEEP=explicit", "CXRUN_JOB=p/j", "CXRUN_RUN=fixture", "CX_SESSION=remote:p/j"}
	// os/exec provides SYSTEMROOT for Windows child processes.
	got := slices.DeleteFunc(cmd.Env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return runtime.GOOS == "windows" && strings.EqualFold(key, "SYSTEMROOT")
	})
	if !slices.Equal(got, want) {
		t.Fatalf("job environment = %q, want %q", got, want)
	}
}

const assertJobSession = `test "$CX_SESSION" = "local:$CXRUN_JOB"
test -z "${CLAUDE_CODE_HOST_SESSION_ID+x}${CLAUDE_CODE_SESSION_ID+x}${CLAUDE_CODE_CHILD_SESSION+x}${CODEX_THREAD_ID+x}"
test -z "${CODEX_SESSION_ID+x}${OPENCODE_SESSION_ID+x}"
`

func TestNewSubmissionRequiresSession(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_SESSION", "")
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("CXRUN_MONITOR_TIMEOUT", "10s")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	work := t.TempDir()
	command := []string{"exec", "local", "--tag", "command", "--wait", "--", "/bin/sh", "-ec", assertJobSession}
	run := []string{"run", "local", work, "run", "--inplace", "--cmd", "--", "/bin/sh", "-ec", assertJobSession}
	agent := []string{"agent", "--host", "local", "--tag", "agent", "--dir", work, "--engine", "claude", "--idle-exit", "1s", "task"}
	fork := []string{"fork", "local:p/agent", "--job", "fork"}
	rebase := []string{"rebase", "local:p/agent", "follow up"}
	withFlags := func(args []string, flags ...string) []string {
		return append(append([]string{args[0]}, flags...), args[1:]...)
	}
	for _, args := range [][]string{command, run, agent, fork, rebase} {
		out, diag := cliStreams(t, 2, nil, f.cx, args...)
		if len(out) != 0 || !strings.Contains(diag, "set CX_SESSION or use --session NAME or --no-session") {
			t.Fatalf("cx %s: stdout %q, stderr %q", args[0], out, diag)
		}
		out, diag = cliStreams(t, 2, nil, f.cx, withFlags(args, "--session", "explicit", "--no-session")...)
		if len(out) != 0 || !strings.Contains(diag, "mutually exclusive") {
			t.Fatalf("cx %s accepted conflicting session flags: stdout %q, stderr %q", args[0], out, diag)
		}
	}
	cliOutput(t, 2, nil, f.cx, withFlags(command, "--session=")...)
	cliOutput(t, 2, nil, f.cx, withFlags(command, "--reuse")...)
	cliOutput(t, 0, nil, f.cx, withFlags(command, "--session", "explicit")...)
	// Reusing a completed job does not submit another job.
	if out := cliOutput(t, 0, nil, f.cx, withFlags(command, "--reuse")...); !strings.Contains(string(out), `"reused":true`) {
		t.Fatalf("sessionless reuse failed: %s", out)
	}
	t.Setenv("CX_SESSION", "detected")
	cliOutput(t, 0, nil, f.cx, withFlags(run, "--no-session")...)
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/run", "--timeout", "10s")

	claude := "#!/bin/sh\nset -eu\n" + assertJobSession + `read -r input
printf '%s\n' '{"type":"result","session_id":"fixture","result":"STATUS: DONE"}'
`
	if err := os.WriteFile(filepath.Join(f.home, "claude"), []byte(claude), 0700); err != nil {
		t.Fatal(err)
	}
	cliOutput(t, 0, nil, f.cx, withFlags(agent, "--session", "agent-explicit")...)
	cliOutput(t, 0, nil, f.cx, withFlags(rebase, "--no-session")...)
	cliOutput(t, 0, nil, f.cx, "wait", "local:p/agent-r1", "--timeout", "10s")
	for job, want := range map[string]string{"command": "explicit", "run": "", "agent": "agent-explicit", "agent-r1": ""} {
		checkSessionAssignment(t, f, job, want)
	}
}

func checkSessionAssignment(t *testing.T, f controllerFixture, job, want string) {
	t.Helper()
	var s status
	out := cliOutput(t, 0, nil, f.cx, "status", "local:p/"+job, "--json")
	if err := json.Unmarshal(out, &s); err != nil || s.Session != want {
		t.Fatalf("session for %s = %q, want %q (%s, %v)", job, s.Session, want, out, err)
	}
}

func TestNestedSubmissionUsesJobSession(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_SESSION", "manager")
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "claude-host")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "claude-child")
	t.Setenv("CODEX_THREAD_ID", "codex-thread")
	t.Setenv("CODEX_SESSION_ID", "codex-session")
	t.Setenv("OPENCODE_SESSION_ID", "opencode-session")
	t.Setenv("CXRUN_MONITOR_TIMEOUT", "10s")
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	t.Setenv("CX_POLICY_DIR", f.home)
	t.Setenv("FIXMODE", "complete")
	t.Setenv("FIXPROMPT", "task")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SESSION_FIXTURE_ENGINE", exe)
	wrapper := "#!/bin/sh\nset -eu\n" + assertJobSession + `
if [ "$CXRUN_JOB" = p/manager ]; then
	cx exec local --tag descendant --wait -- /bin/sh -ec '` + assertJobSession + `'
fi
exec "$SESSION_FIXTURE_ENGINE" "$@"
`
	for name, content := range map[string]string{"preamble.md": "", "stall-nudge.md": "", "codex": wrapper} {
		if err := os.WriteFile(filepath.Join(f.home, name), []byte(content), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cliOutput(t, 0, nil, f.cx, "agent", "--host", "local", "--tag", "manager", "--dir", t.TempDir(), "--inplace", "--no-tools", "--idle-exit", "20s", "task")
	t.Cleanup(func() { cliOutput(t, 0, nil, f.cx, "close", "local:p/manager") })
	for _, key := range []string{"CX_SESSION", "CLAUDE_CODE_HOST_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "OPENCODE_SESSION_ID"} {
		t.Setenv(key, "")
	}
	cliOutput(t, 0, nil, f.cx, "fork", "local:p/manager", "--job", "forked", "--through", "first", "--session", "fork-explicit")
	t.Cleanup(func() { cliOutput(t, 0, nil, f.cx, "close", "local:p/forked") })
	cliOutput(t, 0, nil, f.cx, "agent", "--continue", "local:p/manager", "second round")
	for job, want := range map[string]string{"manager": "manager", "descendant": "local:p/manager", "forked": "fork-explicit"} {
		checkSessionAssignment(t, f, job, want)
	}
}
