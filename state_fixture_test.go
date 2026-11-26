package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func register(parent, dir string, c config, s status) error {
	return registerFixtureJob(context.Background(), parent, dir, c, s, (*stateWriter).sync)
}

func registerFixtureJob(ctx context.Context, parent, dir string, c config, s status, syncConfig func(*stateWriter) error) error {
	owner, err := registerJob(ctx, parent, dir, c, s, syncConfig)
	if err != nil {
		return err
	}
	return owner.Close()
}

func writeFixtureStatus(dir string, s status, report ...string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	owner, err := openJobFile(filepath.Join(dir, "owner.lock"), jobFileLock)
	if err != nil {
		return err
	}
	if err = owner.Close(); err != nil {
		return err
	}
	if mailboxErr := initMailboxLocks(dir); mailboxErr != nil {
		return mailboxErr
	}
	var c config
	if err = readJobConfig(dir, &c); err != nil && !errors.Is(err, errNotRegistered) {
		return err
	}
	c.Engine, c.RunID = s.Engine, s.RunID
	r := runner{dir: dir, s: s, c: c}
	if err := r.openState(); err != nil {
		return err
	}
	defer closeBestEffort(r.stateLog)
	if len(report) != 0 {
		if err := r.publishTurnReport(s.LastCompleted, []byte(report[0])); err != nil {
			return err
		}
	}
	if isTerminal(s.State) {
		return r.saveTerminal()
	}
	return r.save()
}

func startFixtureWriter(t *testing.T, r *runner) {
	t.Helper()
	if err := r.openState(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeBestEffort(r.stateLog) })
}

func fileExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}
