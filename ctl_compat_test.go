package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestControllerCompatibilityOptions(t *testing.T) {
	opts, err := parseCtl("ls", []string{"--watch", "--interval", "2.5", "--role", "reviewer", "--all-hosts"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.watch || opts.interval != 2500*time.Millisecond || opts.role != "reviewer" {
		t.Fatalf("ls options: %+v", opts)
	}
	for _, interval := range []string{"0", "-1", "NaN", "Inf", "1e20"} {
		if _, parseErr := parseCtl("ls", []string{"--watch", "--interval", interval}); parseErr == nil {
			t.Errorf("accepted invalid watch interval %q", interval)
		}
	}
	opts, err = parseCtl("wait", []string{"--any", "--host", "local", "one", "--host", "two"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(opts.hosts, []string{"local", "one", "two"}) {
		t.Fatalf("wait hosts: %v", opts.hosts)
	}
	if _, err := parseCtl("tail", []string{"p/job", "--from-start", "--since", "2"}); err == nil {
		t.Error("accepted conflicting tail origins")
	}
	for _, tc := range []struct {
		history string
		args    []string
	}{
		{"HEAD", []string{"--with-history", "source"}},
		{"base..HEAD", []string{"--with-history=base..HEAD", "source"}},
		{"base..HEAD", []string{"--with-history", "base..HEAD", "source"}},
	} {
		opts, err := parseCtl("snapshot", tc.args)
		if err != nil {
			t.Errorf("snapshot options %q: %v", tc.args, err)
			continue
		}
		if opts.history != tc.history || !slices.Equal(opts.pos, []string{"source"}) {
			t.Errorf("snapshot options %q: history=%q, operands=%v", tc.args, opts.history, opts.pos)
		}
	}
}

func TestControllerCompatibilityViews(t *testing.T) {
	f := newControllerFixture(t)
	for _, prog := range []string{"codex", "claude"} {
		if err := os.WriteFile(filepath.Join(f.home, prog), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	log := []byte("line\x00\xff\n")
	for _, fixture := range []struct{ job, role, session, engine string }{
		{"review", "reviewer", "session-a", "cmd"},
		{"work", "worker", "session-b", "codex"},
	} {
		dir := filepath.Join(f.home, "state", "p", fixture.job)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		state := status{Job: fixture.job, Project: "p", Engine: fixture.engine, Session: fixture.session, State: "done"}
		if err := writeFixtureStatus(dir, state); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(dir, "request.json"), fields{"role": fixture.role}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, state.logName()), log, 0600); err != nil {
			t.Fatal(err)
		}
	}
	listing := cliOutput(t, 0, nil, f.cx, "ls", "--all", "--all-hosts", "--role", "reviewer")
	if !bytes.Contains(listing, []byte("p/review")) || bytes.Contains(listing, []byte("p/work")) {
		t.Fatalf("role-filtered listing: %s", listing)
	}
	addressed := cliOutput(t, 0, nil, f.cx, "ls", "--all", "p/review")
	if !bytes.Contains(addressed, []byte("p/review")) || bytes.Contains(addressed, []byte("p/work")) {
		t.Fatalf("addressed listing: %s", addressed)
	}
	overview := cliOutput(t, 0, nil, f.cx, "overview", "--all")
	if !bytes.Contains(overview, []byte("\n  \"session-a\"\n    p/review ")) ||
		!bytes.Contains(overview, []byte("\n  \"session-b\"\n    p/work ")) {
		t.Fatalf("session groups: %s", overview)
	}
	for _, job := range []string{"local:p/review", "local:p/work"} {
		args := []string{"tail", job, "--raw", "--cursor", "replay", "--timeout", "2m"}
		if got := cliOutput(t, 0, nil, f.cx, args...); !bytes.Equal(got, log) {
			t.Fatalf("%s initial tail: %q", job, got)
		}
		if got := cliOutput(t, 0, nil, f.cx, args...); len(got) != 0 {
			t.Fatalf("%s saved cursor tail: %q", job, got)
		}
		if got := cliOutput(t, 0, nil, f.cx, append(args, "--from-start")...); !bytes.Equal(got, log) {
			t.Fatalf("%s replayed tail: %q", job, got)
		}
	}
}

func TestControllerListSubmissionReceipts(t *testing.T) {
	f := newControllerFixture(t)
	for _, fixture := range []struct{ job, state, phase string }{
		{"staging", "submitting", "snapshot"},
		{"failed", "submit_failed", ""},
		{"stopped", "submitting", ""},
		{"registered", "submit_failed", ""},
	} {
		dir, err := submissionDir("local", "p/"+fixture.job)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		progress := submissionProgress{
			State: fixture.state, Phase: fixture.phase, Host: "local", Project: "p",
			Job: fixture.job, Role: "worker", Session: "suite",
		}
		if err = writeJSON(filepath.Join(dir, "submission.json"), progress); err != nil {
			t.Fatal(err)
		}
		if fixture.job == "staging" {
			lock, lockErr := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
			if lockErr != nil || lock == nil {
				t.Fatal(lock, lockErr)
			}
			t.Cleanup(func() { closeBestEffort(lock) })
		}
	}
	dir := filepath.Join(f.home, "state", "p", "registered")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, status{
		Job: "registered", Project: "p", State: "done", Session: "suite",
	}); err != nil {
		t.Fatal(err)
	}
	listing := cliOutput(t, 0, nil, f.cx, "ls", "--json", "--session", "suite", "--role", "worker")
	got := make(map[string]submissionProgress)
	for _, line := range bytes.Split(bytes.TrimSpace(listing), []byte("\n")) {
		var row submissionProgress
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if _, exists := got[row.Job]; exists {
			t.Fatalf("duplicate row for %s: %s", row.Job, listing)
		}
		got[row.Job] = row
	}
	if len(got) != 4 || got["staging"].State != "submitting" || got["staging"].Phase != "snapshot" ||
		got["failed"].State != "submit_failed" || got["stopped"].State != "submit_failed" ||
		got["registered"].State != "done" {
		t.Fatalf("unexpected submission views: %s", listing)
	}
}
