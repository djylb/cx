package main

import (
	"context"
	"errors"
	"path/filepath"
	"time"
)

var terminalCodes = map[string]int{"done": 0, "failed": 1, "stalled": 3, "timeout": 4, "queue_timeout": 4, "orphaned": 5}

type stateFailure string

func (e stateFailure) Error() string { return string(e) }

var errObserveTimeout = errors.New("observation timed out")
var errNotRegistered = errors.New("job not registered")

func outcomeError(state, outcome string) error {
	code, failure := outcomeResult(state, outcome)
	if code != 0 {
		return stateFailure(failure)
	}
	return nil
}

func outcomeResult(state, outcome string) (code int, failure string) {
	if code = terminalCodes[state]; code != 0 {
		return code, state
	}
	if outcome == "failed" || outcome == "interrupted" {
		return terminalCodes["failed"], "failed"
	}
	return 0, ""
}

// Serialize publication, clean and worker handoff before probing ownership.
func projected(ctx context.Context, dir string, s *status) error {
	path := filepath.Join(dir, "status.json")
	if err := readJSON(path, s); err != nil || isTerminal(s.State) {
		return err
	}
	root := filepath.Dir(dir)
	if s.Project != "" {
		root = filepath.Dir(root)
	}
	g, err := registryLock(ctx, root)
	if err != nil {
		return err
	}
	defer g.Close()
	f, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil {
		return err
	}
	if f != nil {
		defer f.Close()
	}
	*s = status{}
	if err = readJSON(path, s); err != nil || f == nil || isTerminal(s.State) {
		return err
	}
	s.State = "orphaned"
	s.PID = 0
	s.ServerPID = 0
	s.CommandPID = 0
	s.Approvals = nil
	s.LastEvent = time.Now().UTC()
	r := runner{dir: dir, s: *s}
	if err = r.localEvent("orphaned", "owner.lock released"); err != nil {
		return err
	}
	return r.save()
}
