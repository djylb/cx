package main

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestEngineRegistry(t *testing.T) {
	e, err := lookupEngine("")
	if err != nil || e.name != defaultEngine {
		t.Fatal(e.name, err)
	}
	for _, name := range engineNames() {
		e, err := lookupEngine(name)
		if err != nil || e.name != name || e.binary == "" || e.homeEnv == "" || len(e.loginArgs) == 0 || e.drv == nil {
			t.Fatal(name, err)
		}
		c := config{Engine: name}
		if c.driver() != e.drv {
			t.Fatal("driver mismatch", name)
		}
	}
	if _, err = lookupEngine("other"); err == nil || !strings.Contains(err.Error(), "codex, claude") {
		t.Fatal(err)
	}
	var c config
	if d, ok := c.driver().(invalidDriver); !ok || !errors.Is(d.err, errMissingEngine) {
		t.Fatal("a job without an engine must not run")
	}
	if (&config{Argv: []string{"true"}}).driver() != (commandDriver{}) {
		t.Fatal("command driver")
	}
}

func TestEngineLoginState(t *testing.T) {
	codex, _ := lookupEngine("codex")
	claude, _ := lookupEngine("claude")
	exitErr := &exec.ExitError{}
	cases := []struct {
		err    error
		want   any
		out    string
		engine engineSpec
	}{
		{nil, "logged_in", "Logged in using ChatGPT", codex},
		{exitErr, "logged_out", "Not logged in", codex},
		{errors.New("timeout"), nil, "", codex},
		{nil, "logged_in", `{"loggedIn": true, "authMethod": "x"}`, claude},
		{exitErr, "logged_out", `{"loggedIn": false}`, claude},
		{nil, "logged_out", `{"other": 1}`, claude},
	}
	for _, c := range cases {
		if got := c.engine.loginState([]byte(c.out), c.err); got != c.want {
			t.Fatalf("%s %q: got %v want %v", c.engine.name, c.out, got, c.want)
		}
	}
}

func TestEngineOptionChecks(t *testing.T) {
	claude, _ := lookupEngine("claude")
	codex, _ := lookupEngine("codex")
	text, _ := json.Marshal([]fields{{"type": "text", "text": "x"}})
	skill, _ := json.Marshal([]fields{{"type": "skill", "name": "x"}})
	cases := []struct {
		want     string
		o        options
		c        config
		approval bool
	}{
		{"", options{input: string(text)}, config{Sandbox: "workspace-write", Network: true, Effort: "high", JobTools: true, Roots: []string{"/r"}}, false},
		{`input type "skill" not supported by engine claude`, options{input: string(skill)}, config{Sandbox: "workspace-write", Network: true}, false},
		{"--approval-timeout not supported by engine claude", options{}, config{Sandbox: "workspace-write", Network: true}, true},
		{"fork not supported by engine claude", options{}, config{Sandbox: "workspace-write", Network: true, ForkThread: "t"}, false},
		{"network isolation for workspace-write", options{}, config{Sandbox: "workspace-write"}, false},
		{"", options{}, config{Sandbox: "read-only"}, false},
	}
	for i, tc := range cases {
		err := tc.c.checkEngineOptions(claude, tc.o, tc.approval)
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Fatalf("case %d: %v", i, err)
		}
		if err = tc.c.checkEngineOptions(codex, tc.o, tc.approval); err != nil {
			t.Fatalf("codex case %d: %v", i, err)
		}
	}
}

func TestJobCapabilityRejection(t *testing.T) {
	for _, tc := range []struct {
		engine, op, want string
	}{
		{"claude", "compact", "compact not supported by engine claude"},
		{"claude", "approve", "approve not supported by engine claude"},
		{"codex", "compact", ""},
		{"cmd", "requests", ""},
		{"", "rpc", errMissingEngine.Error()},
	} {
		dir := t.TempDir()
		if err := writeFixtureStatus(dir, status{Engine: tc.engine, State: "waiting"}); err != nil {
			t.Fatal(err)
		}
		err := checkJobCapability(dir, tc.op, commandNeeds[tc.op])
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Fatalf("%s %s: %v", tc.engine, tc.op, err)
		}
	}
	if got := strings.Join(enginesWith(commandNeeds["rpc"]), ","); got != "codex" {
		t.Fatal(got)
	}
}

func TestClaudeUsageAndRateLimits(t *testing.T) {
	var ev claudeEvent
	raw := `{"type":"result","is_error":true,"api_error_status":429,"result":"x","usage":{"input_tokens":10,"cache_creation_input_tokens":5,"cache_read_input_tokens":20,"output_tokens":7}}`
	if err := json.Unmarshal([]byte(raw), &ev); err != nil || !claudeRateLimited(ev) || ev.Usage == nil {
		t.Fatal(ev, err)
	}
	if claudeRateLimited(claudeEvent{Result: "compile failed"}) || !claudeRateLimited(claudeEvent{Result: "Claude usage limit reached; resets at 5pm"}) {
		t.Fatal("rate limit wording")
	}
	first, err := addUsage(nil, ev.Usage.counts())
	if err != nil {
		t.Fatal(err)
	}
	second, err := addUsage(first, tokenCounts{TotalTokens: 3, InputTokens: 2, OutputTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	var u struct{ Total, Last tokenCounts }
	if err = json.Unmarshal(second, &u); err != nil || u.Total.TotalTokens != 45 || u.Total.InputTokens != 37 || u.Total.CachedInputTokens != 20 || u.Last.TotalTokens != 3 {
		t.Fatal(string(second), err)
	}
	f := fields{}
	usage(f, second)
	if f["tokens"] == nil || *(f["tokens"].(*int64)) != 45 {
		t.Fatal(f)
	}
	if !(turnError{RateLimited: true}).rateLimited() {
		t.Fatal("adapter rate limit flag")
	}

	reset := int64(4102444800)
	var s rateLimitStatus
	s.update(claudeRateLimit{Status: "rejected", Type: "five_hour", ResetsAt: &reset}.snapshot(), 0)
	if !s.RateLimited || s.ResetAt == nil || *s.ResetAt != reset || s.LimitID != "claude" {
		t.Fatalf("%+v", s)
	}
	util := 0.42
	s = rateLimitStatus{}
	s.update(claudeRateLimit{Status: "allowed", Utilization: &util, ResetsAt: &reset}.snapshot(), 0)
	if s.RateLimited || s.Primary == nil || s.Primary.UsedPercent != 42 {
		t.Fatalf("%+v", s)
	}
}
