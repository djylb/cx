package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseEnvFile(t *testing.T) {
	got, err := parseEnvFile(" # comment\r\n\r\nA=\n B = 'literal # text' \nC=\"$HOME ${X} = hi\"\nC=$OTHER # literal\n")
	want := map[string]string{"A": "", "B": " 'literal # text' ", "C": "$OTHER # literal"}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("parse got %#v, %v", got, err)
	}
	if _, err := parseEnvFile("bad line containing confidential text"); err == nil ||
		err.Error() != "invalid env_file assignment on line 1" {
		t.Fatalf("invalid assignment diagnostic: %v", err)
	}
}

func TestCredentialFilterAcrossWrites(t *testing.T) {
	filter, err := newSecretFilter(map[string]string{"A": "secret-", "B": "secret-value", "C": "测试"})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := &filteredOutput{dst: &out, filter: filter}
	for _, part := range []string{"before sec", "ret-", "value after ", "测", "试"} {
		if _, err := w.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.flush(len(w.pending)); err != nil || out.String() != "before [REDACTED] after [REDACTED]" {
		t.Fatalf("got %q, %v", out.String(), err)
	}
}

func credentialCLI(t *testing.T, f controllerFixture, code int, args ...string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.cx, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != code {
		t.Fatalf("%v: wanted exit %d, got %v\n%s\n%s", args, code, err, out, stderr.Bytes())
	}
	return string(out), stderr.String()
}

func writeCredentialFixture(t *testing.T, path, data string, mode fs.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func assertJobHasNoCredential(t *testing.T, root string, credentials ...string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || !entry.Type().IsRegular() {
			return walkErr
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, value := range credentials {
			if bytes.Contains(b, []byte(value)) {
				return fmt.Errorf("credential reached %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCredentialProfilesCommandCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	root := os.Getenv("CXRUN_STATE")
	hostValue := "fixture-host-value-unique"
	profileValue := "fixture-profile-value-unique"
	writeCredentialFixture(t, filepath.Join(root, "host.env"), "AUTH_VALUE="+hostValue+"\nHOST_ONLY=host-side\n", 0600)
	writeCredentialFixture(t, filepath.Join(root, "second.env"), "AUTH_VALUE="+profileValue+"\nCODEX_HOME=wrong-value\n", 0644)
	writeCredentialFixture(t, filepath.Join(root, "host.json"), `{
		"min_free_bytes":0,"env_file":"host.env","default_profile":"first",
		"profiles":{"first":{"codex_home":"first-account"},"second":{"codex_home":"second-account","claude_config_dir":"second-claude","env_file":"second.env"},"missing":{"env_file":"absent.env"}}
	}`, 0600)
	for _, tc := range []struct {
		tag, profile, check, echo string
	}{
		{"defaults", "", `test "${CODEX_HOME##*/}" = first-account && test -n "$HOST_ONLY"`, hiddenValue},
		{"override", "second", `test "${CODEX_HOME##*/}" = second"-account" && test "${CLAUDE_CONFIG_DIR##*/}" = second-claude && test -z "${HOST_ONLY:-}"`, hiddenValue},
		{"skipped", "missing", `test -z "${AUTH_VALUE:-}" && test -z "${HOST_ONLY:-}"`, ""},
	} {
		var args []string
		if tc.profile != "" {
			args = []string{"--profile", tc.profile}
		}
		out, _ := credentialCLI(t, f, 0, append(append([]string{"exec", "local", "--tag", tc.tag, "--wait", "--timeout", "2s"}, args...), "--", "/bin/sh", "-ec", tc.check+`; printf '%s' "$AUTH_VALUE"`)...)
		if out != tc.echo {
			t.Fatalf("unexpected output for %s", tc.tag)
		}
		assertJobHasNoCredential(t, filepath.Join(root, "p", tc.tag), profileValue, hostValue, "wrong-value", "host-side")
		assertJobHasNoCredential(t, filepath.Join(root, ".work", "p", tc.tag), profileValue, hostValue, "wrong-value", "host-side")
	}
	if _, diag := credentialCLI(t, f, 1, "exec", "local", "--profile", "unknown", "--", "/usr/bin/true"); !strings.Contains(diag, "unknown profile") {
		t.Fatal(diag)
	}
	if _, diag := credentialCLI(t, f, 1, "run", "local", t.TempDir(), "reject", "--cmd", "--profile", "unknown", "--", "/usr/bin/true"); !strings.Contains(diag, "unknown profile") {
		t.Fatal(diag)
	}
	for _, args := range [][]string{
		{"ls", "--json"}, {"overview", "--json"}, {"show", "p/override", "--json"},
	} {
		out, _ := credentialCLI(t, f, 0, args...)
		if !strings.Contains(out, `"profile":"second"`) || strings.Contains(out, "second-account") {
			t.Fatalf("%v: %s", args, out)
		}
	}
	for _, args := range [][]string{{"ls"}, {"overview"}} {
		out, _ := credentialCLI(t, f, 0, args...)
		if !strings.Contains(out, "second") || strings.Contains(out, "second-account") {
			t.Fatalf("%v: %s", args, out)
		}
	}
	events := fixtureCLI(t, 0, "events", "--job", "p/override")
	if !strings.Contains(events, `"method":"env_file/loaded","params":{"keys":["AUTH_VALUE","CODEX_HOME"]}`) {
		t.Fatal(events)
	}
	writeCredentialFixture(t, filepath.Join(f.home, "codex"), `#!/bin/sh
case "${CODEX_HOME##*/}" in
first-account) echo 'Logged in'; exit 0;;
*) echo 'Not logged in'; exit 1;;
esac
`, 0700)
	writeCredentialFixture(t, filepath.Join(f.home, "claude"), "#!/bin/sh\nprintf '%s\\n' '{\"loggedIn\":false}'\n", 0700)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, _ := credentialCLI(t, f, 0, "doctor", "local", "--json")
	for _, want := range []string{`"first":{"claude_available":true,"codex_login_status":"logged_in"`,
		`"second":{"claude_available":true,"codex_login_status":"logged_out"`, `"mode":"0644"`,
		`env_file permissions are broader than 0600`, `env_file missing; runner skips it`} {
		if !strings.Contains(out, want) {
			t.Fatalf("doctor missing %q in %s", want, out)
		}
	}
	if strings.Contains(out, profileValue) || strings.Contains(out, hostValue) {
		t.Fatal("doctor disclosed a credential")
	}
	_, diag := credentialCLI(t, f, 0, "exec", "local", "--wait", "--env", "MY_API_KEY=mock-cli-input", "--", "/usr/bin/true")
	if !strings.Contains(diag, "MY_API_KEY 看起来是凭据键名，请改用执行端 env_file") || strings.Contains(diag, "mock-cli-input") {
		t.Fatal(diag)
	}
}

func TestCredentialProfilesAgentResumeCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	t.Setenv("FIXMODE", "credential")
	t.Setenv("CX_POLICY_DIR", f.home)
	root := os.Getenv("CXRUN_STATE")
	work := t.TempDir()
	prompt := filepath.Join(f.home, "prompt")
	writeCredentialFixture(t, prompt, "", 0600)
	writeCredentialFixture(t, filepath.Join(f.home, "preamble.md"), "", 0600)
	value := "fixture-mock-auth-codex"
	writeCredentialFixture(t, filepath.Join(root, "account.env"), "AUTH_VALUE="+value+"\n", 0600)
	hostJSON := filepath.Join(root, "host.json")
	hostConfig := func(defaultName string) string {
		return fmt.Sprintf(`{"min_free_bytes":0,"default_profile":%q,"profiles":{
			"first":{"codex_home":"first-account","env_file":"account.env"},"second":{"codex_home":"second-account"}
		}}`, defaultName)
	}
	writeCredentialFixture(t, hostJSON, hostConfig("first"), 0600)
	wrapper := `#!/bin/sh
set -eu
test "${CODEX_HOME##*/}" = first-account
test "$AUTH_VALUE" = "$CX_FIXTURE_AUTH"
printf '%s\n' "$AUTH_VALUE" >&2
exec "$CX_FIXTURE_CODEX" "$@"
`
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CX_FIXTURE_CODEX", exe)
	t.Setenv("CX_FIXTURE_AUTH", value)
	writeCredentialFixture(t, filepath.Join(f.home, "codex"), wrapper, 0700)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	input := filepath.Join(f.home, "input.json")
	writeCredentialFixture(t, input, `[{"type":"text","text":"task","nested":{"SomeToken":"mock-input-value","PASSWORD":"mock-input-value"}}]`, 0600)
	_, diag := credentialCLI(t, f, 0, "run", "local", work, "agent", prompt, "--inplace", "--idle-exit", "250ms", "--input-json", "@"+input)
	for _, key := range []string{"SomeToken", "PASSWORD"} {
		if !strings.Contains(diag, key+" 看起来是凭据键名，请改用执行端 env_file") || strings.Contains(diag, "mock-input-value") {
			t.Fatal(diag)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := &testRuns{t: t, ctx: ctx, root: root}
	id := "p/agent"
	h.await(id, func(s status) bool { return s.State == "done" && s.Finished == 1 && s.Profile == "first" })
	writeCredentialFixture(t, hostJSON, hostConfig("second"), 0600)
	_, diag = credentialCLI(t, f, 1, "resume", id, "--profile", "second")
	if !strings.Contains(diag, "resume uses the original profile") {
		t.Fatal(diag)
	}
	credentialCLI(t, f, 0, "resume", id, "--profile", "first")
	h.await(id, func(s status) bool { return s.State == "done" && s.Generation >= 2 && s.Profile == "first" })
	// A fork of the old job should also retain the first account despite the changed default.
	credentialCLI(t, f, 0, "fork", id, "--job", "child", "--through", "first")
	h.await("p/child", func(s status) bool { return s.Thread == "child" && s.Profile == "first" })
	h.await("p/child", func(s status) bool { return s.State == "done" })
	assertJobHasNoCredential(t, filepath.Join(root, "p"), value)
	assertJobHasNoCredential(t, filepath.Join(root, ".work", "p"), value)
}

func TestCredentialProfilesClaudeCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	t.Setenv("CX_CONTEXT_DIR", t.TempDir())
	root := os.Getenv("CXRUN_STATE")
	value := "fixture-mock-auth-claude"
	writeCredentialFixture(t, filepath.Join(root, "profile.env"), "AUTH_VALUE="+value+"\n", 0600)
	writeCredentialFixture(t, filepath.Join(root, "host.json"), `{
		"min_free_bytes":0,"profiles":{"claude-test":{"claude_config_dir":"claude-home","env_file":"profile.env"}}
	}`, 0600)
	fake := `#!/bin/sh
set -eu
test "${CLAUDE_CONFIG_DIR##*/}" = claude-home
test "$AUTH_VALUE" = "$CX_FIXTURE_AUTH"
read -r input
printf '%s\n' "$AUTH_VALUE" >&2
printf '{"type":"result","session_id":"fixture-session","result":"%s"}\n' "$AUTH_VALUE"
`
	t.Setenv("CX_FIXTURE_AUTH", value)
	writeCredentialFixture(t, filepath.Join(f.home, "claude"), fake, 0700)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, _ := credentialCLI(t, f, 0, "agent", "--host", "local", "--dir", t.TempDir(), "--tag", "claude", "--profile", "claude-test", "--engine", "claude", "--inplace", "--idle-exit", "250ms", "task")
	if strings.Contains(out, value) || !strings.Contains(out, hiddenValue) {
		t.Fatal("expected redacted Claude result")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h := &testRuns{t: t, ctx: ctx, root: root}
	h.await("p/claude", func(s status) bool { return s.State == "done" && s.Profile == "claude-test" })
	assertJobHasNoCredential(t, filepath.Join(root, "p", "claude"), value)
	assertJobHasNoCredential(t, filepath.Join(root, ".work", "p", "claude"), value)
}
