package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestContextWarningUsesMeasuredPercentage(t *testing.T) {
	r := runner{}
	for _, test := range []struct {
		telemetry string
		warn      bool
	}{
		{`{"last":{"totalTokens":5999},"modelContextWindow":10000}`, false},
		{`{"last":{"totalTokens":6000},"modelContextWindow":10000}`, true},
		{`{"last":{"totalTokens":6000}}`, false},
	} {
		if got := r.usageWarning(readUsage(json.RawMessage(test.telemetry))) != nil; got != test.warn {
			t.Errorf("warning for %s = %v, want %v", test.telemetry, got, test.warn)
		}
	}
}

func TestContextWarningAndUsageCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := filepath.Join(f.home, "config")
	t.Setenv("XDG_CONFIG_HOME", config)
	t.Setenv("FIXMODE", "usage-advisory")
	if err := os.Symlink(f.cx, filepath.Join(f.home, "codex")); err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(f.home, "policy")
	snapWriteTest(t, filepath.Join(policy, "preamble.md"), "")
	t.Setenv("CX_POLICY_DIR", policy)
	snapWriteTest(t, filepath.Join(config, "cx", "usage.toml"), "context_warn = 65\nusage_top = 1\nusage_turns = 1\nusage_context = 70\n")
	src := filepath.Join(f.home, "source")
	snapWriteTest(t, filepath.Join(src, "content"), "fixture\n")
	prompt := filepath.Join(f.home, "prompt")
	snapWriteTest(t, prompt, "task")
	for _, tag := range []string{"advised", "disabled"} {
		args := []string{"run", "local", src, tag, prompt, "--sync", "--context", "none", "--no-tools"}
		if tag == "disabled" {
			args = append(args, "--context-warn", "0", "--role", "reader")
		}
		cliOutput(t, 0, nil, f.cx, args...)
		id := "p/" + tag
		t.Cleanup(func() { cliOutput(t, 0, nil, f.cx, "close", id) })
		out := cliOutput(t, 0, nil, f.cx, "result", id, "--json", "--wait", "--timeout", "12s")
		var result resultOutput
		if err := json.Unmarshal(out, &result); err != nil {
			t.Fatal(err)
		}
		if result.Usage == nil || result.CumulativeUsage == nil || result.Usage.Turn == nil ||
			result.CumulativeUsage.Cached == nil || *result.CumulativeUsage.Cached != 50 ||
			result.CumulativeUsage.Fresh == nil || *result.CumulativeUsage.Fresh != 100 ||
			result.CumulativeUsage.Output == nil || *result.CumulativeUsage.Output != 50 ||
			result.Usage.Turn.Fresh == nil || *result.Usage.Turn.Fresh != 20 {
			t.Fatalf("usage breakdown: %s", out)
		}
		mail := cliOutput(t, 0, nil, f.cx, "outbox", id)
		if tag == "disabled" {
			if result.ContextWarning != nil || strings.Contains(string(mail), "context_warning") {
				t.Fatalf("disabled warning: %s / %s", out, mail)
			}
			continue
		}
		warning := result.ContextWarning
		if warning == nil || warning.Turn != 1 || warning.Percent != 70 || warning.Threshold != 65 ||
			!slices.Equal(warning.Actions, []string{"continue", "compact", "handoff"}) {
			t.Fatalf("warning: %s", out)
		}
		var note agentMail
		if err := json.Unmarshal(mail, &note); err != nil || note.Type != "context_warning" || note.To != "parent" ||
			note.ContextWarning == nil || note.ContextWarning.Percent != 70 {
			t.Fatalf("warning event: %s (%v)", mail, err)
		}
	}
	var report usageRanking
	out := cliOutput(t, 0, nil, f.cx, "ls", "--all", "--usage", "--json")
	if err := json.Unmarshal(out, &report); err != nil || len(report.Jobs) != 1 || len(report.Projects) != 1 ||
		len(report.Roles) != 1 || len(report.HighContextJobs) != 2 || report.Projects[0].Total == nil ||
		*report.Projects[0].Total != 400 {
		t.Fatalf("usage ranking: %s (%v)", out, err)
	}
	for _, tag := range []string{"advised", "disabled"} {
		line := cliOutput(t, 0, nil, f.cx, "ls", "p/"+tag, "--json")
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil || row["cached_tokens"] != float64(50) ||
			row["fresh_input_tokens"] != float64(100) || row["output_tokens"] != float64(50) {
			t.Fatalf("listing usage: %s (%v)", line, err)
		}
	}
}

func TestTransportHistoryRotatesWithOutageDuration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.log")
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.FixedZone("fixture", 8*60*60))
	for i := range 5 {
		at := start.Add(time.Duration(i) * time.Hour)
		for _, sample := range []struct {
			when   time.Time
			reason string
		}{
			{at, "wire silence"}, {at.Add(time.Second), "dial failed"}, {at.Add(3500 * time.Millisecond), ""},
		} {
			if err := recordTransportSample(path, "worker", sample.reason, sample.when, 250); err != nil {
				t.Fatal(err)
			}
		}
	}
	var events []transportEvent
	for _, name := range []string{path + ".1", path} {
		b, err := os.ReadFile(name)
		if err != nil || len(b) > 250 {
			t.Fatalf("transport log %q size=%d err=%v", name, len(b), err)
		}
		events = append(events, transportEvents(b)...)
	}
	if len(events) == 0 {
		t.Fatal("rotation lost every record")
	}
	latest := events[len(events)-1]
	if latest.Event != "reconnect" || latest.Reason != "wire silence" || latest.DisconnectedMS == nil ||
		*latest.DisconnectedMS != 3500 || latest.Time.Location() != time.UTC {
		t.Fatalf("recorded outage: %+v", latest)
	}
}

func TestDoctorReportsTransportDisconnectAndReconnectCLI(t *testing.T) {
	f := newControllerFixture(t)
	snapWriteTest(t, filepath.Join(f.home, ".config", "cx", "hosts.toml"), "[hosts.worker]\nssh = \"worker\"\nplatform = \"linux/amd64\"\n")
	ssh := `#!/bin/sh
if test "$FIX_TRANSPORT" = disconnected; then
  printf 'connection refused\n' >&2
  exit 255
fi
printf 'cx-wire: ready\n' >&2
printf '{"state_writable":true}\n'
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(ssh), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FIX_TRANSPORT", "disconnected")
	down := cliOutput(t, 1, nil, f.cx, "doctor", "worker", "--json")
	if !strings.Contains(string(down), "dial failed") {
		t.Fatalf("missing disconnect: %s", down)
	}
	t.Setenv("FIX_TRANSPORT", "connected")
	up := cliOutput(t, 0, nil, f.cx, "doctor", "worker", "--json")
	var reports map[string]struct {
		Recent []transportEvent `json:"recent_transport"`
	}
	if err := json.Unmarshal(up, &reports); err != nil {
		t.Fatal(err)
	}
	events := reports["worker"].Recent
	if len(events) != 2 || events[1].Event != "reconnect" || events[1].DisconnectedMS == nil {
		t.Fatalf("reconnect: %s", up)
	}
}

func TestEngineUsageKeepsCumulativeAndDoesNotCarryTurnSample(t *testing.T) {
	before := json.RawMessage(`{"last":{"inputTokens":9},"total":{"inputTokens":110,"cachedInputTokens":80,"outputTokens":6}}`)
	after, measured := retainEngineUsage(json.RawMessage(`{"total":{"outputTokens":9}}`), before, false)
	usage := readUsage(after)
	if usage == nil {
		t.Fatalf("no usage in %s", after)
	}
	if measured || usage.Turn != nil || usage.Total == nil || *usage.Total.Fresh != 30 || *usage.Total.Output != 9 {
		t.Fatalf("latest cumulative with no turn sample: %s, %t", after, measured)
	}
}
