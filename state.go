package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

func unregisteredJob(dir string) error {
	return fmt.Errorf("%s: %w: %w", dir, errNotRegistered, os.ErrNotExist)
}

func jobStatusRecord(dir string) (stateRecord, stateCursor, error) {
	record, cursor, err := latestState(dir)
	if err == nil && record.Seq == 0 {
		err = unregisteredJob(dir)
	}
	return record, cursor, err
}

func decodeJobStatus(record stateRecord, dst *status) error {
	next := status{final: record.Final}
	if err := json.Unmarshal(record.Value, &next); err != nil {
		return err
	}
	*dst = next
	return nil
}

func readJobStatus(dir string, s *status) (stateRecord, error) {
	record, _, err := jobStatusRecord(dir)
	if err == nil {
		err = decodeJobStatus(record, s)
	}
	return record, err
}

func jobConfigRecord(dir string) (stateRecord, error) {
	record, _, err := latestStateKind(dir, "config")
	if err == nil && record.Seq == 0 {
		err = unregisteredJob(dir)
	}
	return record, err
}

func readJobConfig(dir string, c *config) error {
	record, err := jobConfigRecord(dir)
	if err != nil {
		return err
	}
	*c = config{}
	return json.Unmarshal(record.Value, c)
}

func projected(ctx context.Context, dir string, s *status) error {
	id, current, _, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	*s = current
	_, err = projectJobStatus(ctx, dir, s, id)
	return err
}

// The probe gate is held only around the nonblocking ownership test. A
// worker under the registry lock waits for the gate before trying owner.lock,
// so it cannot mistake a read-side probe for a running worker.
func ownerProbePath(dir string) string { return filepath.Join(dir, "owner-probe.lock") }

func probeJobOwner(dir string) (free bool, err error) {
	probe, err := lockPath(ownerProbePath(dir), os.O_CREATE)
	if err != nil || probe == nil {
		return false, err
	}
	defer closeBestEffort(probe)
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		return false, err
	}
	return true, owner.Close()
}

func tryAcquireJobOwner(ctx context.Context, dir string, owner *os.File) (bool, error) {
	probe, err := lockUntil(ctx, ownerProbePath(dir))
	if err != nil {
		return false, err
	}
	defer closeBestEffort(probe)
	return tryLock(owner)
}

func openJobOwner(ctx context.Context, dir string, mode jobFileMode) (*os.File, error) {
	path := filepath.Join(dir, "owner.lock")
	owner, err := openJobFile(path, mode)
	if err != nil {
		return nil, err
	}
	owned, err := tryAcquireJobOwner(ctx, dir, owner)
	if err != nil || !owned {
		return nil, errors.Join(err, owner.Close())
	}
	if err := checkJobHandle(owner, path); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func commandHandoffPending(dir string, s status) (bool, error) {
	if s.Engine != "cmd" || s.PID != 0 {
		return false, nil
	}
	handoff, err := lockPath(filepath.Join(dir, "handoff.lock"), 0)
	if err != nil || handoff == nil {
		return handoff == nil, err
	}
	return false, handoff.Close()
}

// Serialize publication, clean and worker handoff before probing ownership.
func projectJobStatus(ctx context.Context, dir string, s *status, bound ...jobIdentity) (bool, error) {
	return projectJobStatusWithBarrier(ctx, dir, s, (*stateWriter).syncHistory, bound...)
}

func projectJobStatusWithBarrier(ctx context.Context, dir string, s *status, barrier func(*stateWriter) error, bound ...jobIdentity) (bool, error) {
	if s.final {
		return false, nil
	}
	if s.State == "" {
		return false, unregisteredJob(dir)
	}
	identity := jobIdentity{run: s.RunID}
	if len(bound) != 0 && bound[0].key != "" {
		identity = bound[0]
		if err := identity.check(dir, *s); err != nil {
			return false, err
		}
	} else {
		var err error
		identity.key, err = jobDirectoryKey(dir)
		if err != nil {
			return false, err
		}
	}
	path := filepath.Join(dir, "owner.lock")
	free, err := probeJobOwner(dir)
	if err != nil || !free {
		return false, err
	}
	root := filepath.Dir(dir)
	if s.Project != "" {
		root = filepath.Dir(root)
	}
	guard, err := registryLock(ctx, root)
	if err != nil {
		return false, err
	}
	defer closeBestEffort(guard)
	owner, err := openJobFile(path, jobFileLockExisting)
	if err != nil {
		return false, err
	}
	defer closeBestEffort(owner)
	free, err = tryAcquireJobOwner(ctx, dir, owner)
	if err != nil || !free {
		return false, err
	}
	if handleErr := checkJobHandle(owner, path); handleErr != nil {
		return false, handleErr
	}
	if _, err = readJobStatus(dir, s); err != nil {
		return false, err
	}
	if identityErr := identity.check(dir, *s); identityErr != nil {
		return false, identityErr
	}
	if s.final {
		return true, nil
	}
	if pending, handoffErr := commandHandoffPending(dir, *s); handoffErr != nil || pending {
		return false, handoffErr
	}
	// The owner and pinned directory protect recovery from reassignment.
	// Filesystem barriers and mailbox recovery do not require the registry.
	if closeErr := guard.Close(); closeErr != nil {
		return false, closeErr
	}
	r := runner{dir: dir, s: *s, root: root, ctx: ctx}
	if err = readJobConfig(dir, &r.c); err != nil {
		return false, err
	}
	r.s.State = "orphaned"
	r.s.PID = 0
	r.s.EnginePID = 0
	r.s.CommandPID = 0
	r.s.Approvals = nil
	r.s.LastEvent = time.Now().UTC()
	if err = r.openState(); err != nil {
		return false, err
	}
	if err = barrier(r.stateLog); err != nil {
		return false, errors.Join(err, r.stateLog.Close())
	}
	if err = r.saveTerminal(); err != nil {
		return false, err
	}
	*s = r.s
	s.final = true
	return true, r.localEvent("orphaned", "owner.lock released")
}
