package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

const fixtureLimit = 2 * time.Minute

func fixtureContext(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(t.Context()), fixtureLimit)
}

func clockContext(ctx context.Context, c *timeshim.Fake) context.Context {
	return context.WithValue(ctx, clockKey{}, c)
}

func runnerClockContext(ctx context.Context, c *timeshim.Fake) context.Context {
	return context.WithValue(ctx, runnerClockKey{}, c)
}

// Await current or newly published state. Cross-process writers do not share
// Go channels with the test, so probe the append-only tail at a fixed cadence.
func awaitFixtureStatus(ctx context.Context, dir string, match func(status) bool) (status, error) {
	s, cursor, err := loadJobStatus(dir)
	if err == nil && match(s) {
		return s, nil
	}
	if err != nil && !errors.Is(err, errNotRegistered) {
		return s, err
	}
	tail, err := openStateTail(dir, cursor)
	if err != nil {
		return s, err
	}
	defer closeBestEffort(tail)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		rows, err := tail.Next()
		if err != nil {
			return s, err
		}
		for _, row := range rows {
			if row.Kind != "status" {
				continue
			}
			if err = decodeJobStatus(row, &s); err != nil {
				return s, err
			}
			if match(s) {
				return s, nil
			}
		}
		select {
		case <-ctx.Done():
			return s, ctx.Err()
		case <-tick.C:
		}
	}
}

func (h *testRuns) withClock(id string) *timeshim.Fake {
	c := timeshim.NewFake(time.Now())
	h.clocks[id] = c
	return c
}

func (h *testRuns) finishFirstTurn(id string, options ...string) error {
	h.t.Helper()
	h.withClock(id)
	done := h.start(id, options...)
	return h.idleExit(id, done, func(s status) bool { return s.State == "waiting" && s.Finished == 1 })
}

func (h *testRuns) idleExit(id string, done <-chan error, after func(status) bool) error {
	h.t.Helper()
	h.await(id, after)
	h.clocks[id].Advance(3 * time.Minute)
	select {
	case err := <-done:
		return err
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

func (h *testRuns) awaitRecord(id string, match func(stateRecord) bool) {
	h.t.Helper()
	tail, err := openStateTail(filepath.Join(h.root, id), stateCursor{})
	if err != nil {
		h.t.Fatal(err)
	}
	defer closeBestEffort(tail)
	for {
		rows, err := tail.Next()
		if err != nil {
			h.t.Fatal(err)
		}
		for _, row := range rows {
			if match(row) {
				return
			}
		}
		if err := pause(h.ctx, 10*time.Millisecond); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *testRuns) stallUntilRestart(id string, clock *timeshim.Fake) {
	h.t.Helper()
	clock.Advance(time.Second)
	h.await(id, func(s status) bool { return s.StallCount == 1 && s.LastAction == "steer" })
	clock.Advance(time.Second)
	h.await(id, func(s status) bool { return s.Turn == "continued" && s.StallCount == 2 })
	clock.Advance(time.Second)
	h.await(id, func(s status) bool {
		return s.StallCount == 3 && s.LastAction == (codexDriver{}).restartAction() &&
			s.Turn == "continued" && s.TurnStatus == "inProgress"
	})
	// The restart can publish its action before the new turn has started.
	// Round-trip with the replacement server before moving past its last event.
	if err := h.ctl("rpc", id, "fixture/noop"); err != nil {
		h.t.Fatal(err)
	}
}

func awaitFixtureFile(ctx context.Context, path string) error {
	return awaitFixtureCondition(ctx, func() (bool, error) { return fileExists(path) })
}

func awaitFixtureOwnerReleased(ctx context.Context, dir string) error {
	return awaitFixtureCondition(ctx, func() (bool, error) {
		owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
		if err != nil || owner == nil {
			return false, err
		}
		return true, owner.Close()
	})
}

func awaitRemovedFixtureFile(ctx context.Context, path string) error {
	return awaitFixtureCondition(ctx, func() (bool, error) {
		exists, err := fileExists(path)
		return !exists, err
	})
}

func awaitFixtureCondition(ctx context.Context, check func() (bool, error)) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready, err := check(); ready || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func advanceFixture(t *testing.T, ctx context.Context, clock *timeshim.Fake) {
	t.Helper()
	if err := clock.WaitPending(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clock.AdvanceToNext()
}

func expireLockWait(t *testing.T, attempt func(context.Context) error) error {
	t.Helper()
	ctx, stop := fixtureContext(t)
	defer stop()
	clock := timeshim.NewFake(time.Now())
	bounded, cancel := timeshim.WithTimeout(clockContext(ctx, clock), clock, time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- attempt(bounded) }()
	if err := clock.WaitDeadline(ctx, clock.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	return <-done
}

// commandFixtureMain drives a real detached node using a test-only file
// handshake. The CLI and the command payload are still separate processes.
func commandFixtureMain() {
	job := os.Getenv("CX_TEST_CLOCK_JOB")
	at := slices.Index(os.Args, "--job") + 1
	if job == "" || at == 0 || at >= len(os.Args) || os.Args[at] != job {
		main()
		return
	}
	dir := os.Getenv("CX_TEST_CLOCK_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
	defer cancel()
	clock := timeshim.NewFake(time.Now())
	go func() {
		request := filepath.Join(dir, "advance")
		if err := awaitFixtureFile(ctx, request); err != nil {
			return
		}
		var step struct{ After, By time.Duration }
		if err := readJSON(request, &step); err != nil {
			diagnosticf("fixture clock: %v\n", err)
			return
		}
		if step.After != 0 {
			if err := clock.WaitDeadline(ctx, clock.Now().Add(step.After)); err != nil {
				return
			}
		}
		clock.Advance(step.By)
		if err := os.WriteFile(filepath.Join(dir, "advanced"), nil, 0600); err != nil {
			diagnosticf("fixture clock: %v\n", err)
		}
	}()
	mainContext(runnerClockContext(ctx, clock))
}

func (h *commandHarness) clockJob(job string) func(time.Duration, time.Duration) {
	h.t.Helper()
	dir := h.t.TempDir()
	h.t.Setenv("CX_TEST_CLOCK_JOB", job)
	h.t.Setenv("CX_TEST_CLOCK_DIR", dir)
	return func(after, by time.Duration) {
		h.t.Helper()
		if err := writeJSON(filepath.Join(dir, "advance"), struct{ After, By time.Duration }{after, by}); err != nil {
			h.t.Fatal(err)
		}
		if err := awaitFixtureFile(h.ctx, filepath.Join(dir, "advanced")); err != nil {
			h.t.Fatal(err)
		}
	}
}

type fixtureWaitAnswer struct {
	report *waitReport
	err    error
}

// startFixtureWaitAny completes the first scan before returning the tick
// channel. A result on that scan is exposed as an unexpected early return.
func startFixtureWaitAny(t *testing.T, ctx context.Context, root, project, session string, baseline map[string]waitCursor) (chan time.Time, <-chan fixtureWaitAnswer) {
	t.Helper()
	var projects []string
	if project != "" {
		projects = []string{project}
	}
	ticks := make(chan time.Time)
	done := make(chan fixtureWaitAnswer, 1)
	go func() {
		// Use an unbuffered tick until the first scan reaches its select.
		report, err := selectWaitAny(ctx, root, projects, session, baseline, ticks, func(*waitReport) error { return nil })
		done <- fixtureWaitAnswer{report, err}
	}()
	select {
	case early := <-done:
		t.Fatalf("wait returned before the fixture event: %+v (%v)", early.report, early.err)
	case ticks <- time.Time{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return ticks, done
}

func assertNoWaitAnyEvent(t *testing.T, ctx context.Context, root, project, session string, baseline map[string]waitCursor) {
	t.Helper()
	clock := timeshim.NewFake(time.Now())
	bounded, cancel := timeshim.WithTimeout(ctx, clock, time.Second)
	defer cancel()
	_, done := startFixtureWaitAny(t, bounded, root, project, session, baseline)
	clock.AdvanceToNext()
	select {
	case answer := <-done:
		if answer.report != nil || !errors.Is(answer.err, errObserveTimeout) {
			t.Fatalf("unexpected wait event: %+v (%v)", answer.report, answer.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
