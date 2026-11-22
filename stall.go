package main

import (
	"errors"
	"fmt"
	"time"
)

func (c config) nudgeText(fallback string) string {
	if c.StallNudge != "" {
		return c.StallNudge
	}
	return fallback
}

// errStalled ends the runner after the last escalation step failed (status "stalled").
var errStalled = errors.New("stalled: no progress after steer, interrupt and app-server restart")

func isTerminal(state string) bool {
	_, ok := terminalCodes[state]
	return ok
}

// watchdog escalates when a running turn produces no app-server event for --stall (longer while a command item
// is in progress): 1 steer, 2 interrupt + new turn, 3 kill the app-server tree, resume the thread + new turn,
// 4 stop with status stalled. Progress after an action resets the count. Every step is logged and in status.json.
func (r *runner) watchdog() error {
	if r.c.Stall <= 0 || !r.active() || len(r.s.Approvals) > 0 {
		return nil
	}
	limit := r.c.Stall
	if len(r.cmdOpen) > 0 && limit < r.c.CmdStall {
		limit = r.c.CmdStall
	}
	ref := r.s.LastEvent
	for _, t := range []time.Time{r.turnSince, r.lastAction} {
		if t.After(ref) {
			ref = t
		}
	}
	if time.Since(ref) < limit {
		return nil
	}
	r.s.StallCount++
	var act string
	var err error
	switch r.s.StallCount {
	case 1:
		if !r.c.driver().capabilities().Steer {
			r.pending = r.c.nudgeText(defaultStallNudge)
			act = "queue next turn"
			break
		}
		act = "steer"
		err = r.input(r.c.nudgeText(defaultStallNudge), fmt.Sprintf("stall-%d", time.Now().UnixNano()), true)
	case 2:
		act = "interrupt, then continue"
		r.pending = r.c.nudgeText(defaultContinue)
		err = r.interrupt()
	case 3:
		act = "restart app-server, resume thread, continue"
		if r.s.Restarts >= r.c.RestartLimit {
			return errStalled
		}
		r.s.Restarts++
		if err = r.save(); err != nil {
			return err
		}
		r.pending = r.c.nudgeText(defaultContinue)
		if r.claude != nil {
			r.claude.interrupt()
		} else {
			err = r.restartServer()
		}
		if err != nil {
			return errors.Join(errStalled, err)
		}
	default:
		r.s.LastAction = "stalled"
		_ = r.note("stall", "step 4: giving up")
		return errStalled
	}
	r.lastAction = time.Now()
	r.s.LastAction = act
	if err != nil {
		if errors.Is(err, errServerClosed) || r.ctx.Err() != nil {
			return err
		}
		_ = r.note("stall", fmt.Sprintf("step %d %s failed: %v", r.s.StallCount, act, err))
	} else if e := r.note("stall", fmt.Sprintf("step %d: %s", r.s.StallCount, act)); e != nil {
		return e
	}
	return r.save()
}

// restartServer replaces the app-server process (also after it died) and resumes the same thread.
func (r *runner) restartServer() error {
	if r.follow != nil {
		r.cancelFollow()
		if r.budget != nil {
			r.resumeRuntime()
		}
	}
	old := r.srv
	_ = old.cmd.Process.Kill()
	close(old.stop)
	_ = old.in.Close()
	old.tree.cleanup()
	done := make(chan struct{})
	go func() {
		_ = old.wait() // The old server was killed. Only reap completion matters.
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		r.srv = nil
		return errReapDeadline
	}
	r.srv = nil
	r.s.Approvals = nil
	r.cmdOpen = map[string]time.Time{}
	srv, err := openServer(r.c, r.root, r.dir)
	if err != nil {
		return err
	}
	r.srv = srv
	r.s.ServerPID = srv.cmd.Process.Pid
	if err = r.save(); err != nil {
		return err
	}

	if err = r.initializeThread(); err != nil {
		return err
	}
	if r.pending != "" && r.active() {
		text := r.pending
		r.pending = ""
		return r.input(text, fmt.Sprintf("continue-%d", time.Now().UnixNano()), true)
	}
	return nil
}

// continuePending starts the queued "continue" turn once the interrupted one has ended.
func (r *runner) continuePending() (bool, error) {
	if r.pending == "" || r.active() {
		return false, nil
	}
	text := r.pending
	r.pending = ""
	return true, r.input(text, fmt.Sprintf("continue-%d", time.Now().UnixNano()), false)
}
