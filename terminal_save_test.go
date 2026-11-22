package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTerminalSaveSurvivesTransientRefusal(t *testing.T) {
	dir := t.TempDir()
	failures := 3
	orig := writeStatus
	writeStatus = func(path string, v any, durable bool) error {
		if !durable {
			t.Error("terminal state written without a flush")
		}
		if failures > 0 {
			failures--
			return errors.New("access is denied")
		}
		return writeJSON(path, v)
	}
	t.Cleanup(func() { writeStatus = orig })
	code := int64(0)
	r := runner{dir: dir, s: status{Job: "j", State: "done", ExitCode: &code}}
	if err := r.saveTerminal(); err != nil || failures != 0 {
		t.Fatal(err, failures)
	}
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil || s.State != "done" || s.ExitCode == nil || *s.ExitCode != 0 {
		t.Fatal(s, err)
	}
}

func TestTerminalSaveGivesUpAndRecordsCause(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	orig := writeStatus
	writeStatus = func(string, any, bool) error { return errors.New("access is denied") }
	budget := terminalSaveBudget
	terminalSaveBudget = 200 * time.Millisecond
	t.Cleanup(func() { writeStatus, terminalSaveBudget = orig, budget })
	start := time.Now()
	r := runner{dir: dir, s: status{State: "done"}}
	err := r.saveTerminal()
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatal(err, time.Since(start))
	}
	appendProgress(dir, "runner/error", err.Error())
	b, e := os.ReadFile(filepath.Join(dir, "progress.log"))
	if e != nil || !strings.Contains(string(b), "\trunner/error\taccess is denied") {
		t.Fatal(string(b), e)
	}
}
