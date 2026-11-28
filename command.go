package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/djylb/timeshim"
)

// The worker has no handles to the submitter's terminal or SSH pipes.
// processWait carries the outcome for normal exit. After a forced stop,
// receivers only need to know that the process has been reaped.
type processWait struct{ err error }

var startDetachedWorker = func(cmd *exec.Cmd) error {
	detachWorker(cmd)
	return cmd.Start()
}

func launchCommand(root, project, job, runID, directory string) (<-chan error, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "_command", "--state", root, "--project", project, "--job", job, "--expect-run", runID, "--expect-dir", directory)
	if err = startDetachedWorker(cmd); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return done, nil
}

const (
	outputTailBytes     = 64 * 1024
	commandPollInterval = 50 * time.Millisecond
)

func (r *runner) publishOutputTail() error {
	f, length, err := openCommandOutput(r.dir, nil)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	size := min(length, outputTailBytes)
	b, err := io.ReadAll(io.NewSectionReader(f, length-size, size))
	if err == nil && int64(len(b)) != size {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	const turnID = "1"
	if err := r.publishTurnReport(turnID, b); err != nil {
		return err
	}
	r.s.Turn, r.s.LastCompleted = turnID, turnID
	return nil
}

func (r *runner) serveCommand() (err error) {
	var cmd *exec.Cmd
	var abandoned bool
	// Publish terminal status only after output and progress are flushed.
	defer func() {
		r.s.CommandPID = 0
		r.s.PID = 0
		r.s.Finished = 1
		r.s.LastEvent = time.Now().UTC()
		if r.s.ExitCode != nil && *r.s.ExitCode != 0 {
			r.s.FailedCommand = &failedCommand{brief(cmd.String()), *r.s.ExitCode}
		}
		if err != nil {
			r.s.Error = err.Error()
			r.s.State = "failed"
		}
		if r.s.State == "done" {
			r.s.CompletedStatus = "completed"
		} else if r.s.CompletedStatus == "" {
			r.s.CompletedStatus = "failed"
		}
		if saveErr := r.saveTerminal(); saveErr != nil {
			err = errors.Join(err, saveErr)
			// The pane's stderr is not retained; leave the cause where status readers look.
			appendProgress(r.dir, "runner/error", r.c.secret.text(err.Error()))
		}
	}()
	r.log, err = openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
	if err != nil {
		return err
	}
	defer closeBestEffort(r.log)
	out, err := openCommandOutputWriter(r.dir)
	if err != nil {
		return err
	}
	defer func() {
		_, sealErr := out.seal(context.WithoutCancel(r.baseContext()), r.dir, abandoned)
		if sealErr != nil {
			r.s.OutputDrain = "truncated"
		}
		err = errors.Join(err, sealErr, r.publishOutputTail())
	}()
	if r.c.Work != nil && r.c.Work.hasModuleContext() {
		r.modulePreamble()
	}
	if r.closeRequest != 0 {
		r.closeCompleted = true
		r.s.State, r.s.CompletedStatus = "failed", "interrupted"
		return r.note("command/stop", "interrupted before start")
	}
	if _, close, closeErr := r.takeMarker("close"); closeErr != nil {
		return closeErr
	} else if close {
		r.closeCompleted = true
		r.s.State, r.s.CompletedStatus = "failed", "interrupted"
		return r.note("command/stop", "interrupted before start")
	}
	cmd = commandArgv(r.c.Argv, r.c.Shell)
	cmd.Dir = r.c.Dir
	output, _ := r.c.secret.output(cmd, out)
	cmd.Stdout, cmd.Stderr = output, output
	r.c.mark(cmd, r.dir)
	child, err := startManagedCommand(cmd, &r.c, false)
	if err != nil {
		return err
	}
	finished := false
	defer func() {
		if !finished {
			outputErr := child.finish()
			abandoned = errors.Is(outputErr, errOutputTruncated)
			r.s.OutputDrain = managedOutputStatus(outputErr)
			err = errors.Join(err, outputErr)
		}
	}()
	stopRuntime := r.startRuntime()
	defer stopRuntime()
	r.s.State = "running"
	r.s.CommandPID = cmd.Process.Pid
	r.s.LastEvent = time.Now().UTC()
	if err = r.note("command/start", fmt.Sprintf("pid=%d", r.s.CommandPID)); err != nil {
		return err
	}
	if err = r.save(); err != nil {
		return err
	}
	clock := timeshim.Or(r.clock)
	lastOutput := clock.Now()
	var size int64
	poll := func() error {
		if end := out.length(); end != size {
			size = end
			lastOutput = clock.Now()
		}
		if _, ok, e := r.takeMarker("close"); e != nil {
			return e
		} else if ok {
			return errQueuedClose
		}
		if r.c.Quiet > 0 && timeshim.Since(clock, lastOutput) >= r.c.Quiet {
			return errStalled
		}
		return nil
	}
	waitErr, stopErr := child.wait(r.ctx, r.clock, poll)
	outputErr := child.finish()
	finished = true
	r.s.ExitCode = child.exitCode()
	r.s.OutputDrain = managedOutputStatus(outputErr)
	abandoned = errors.Is(outputErr, errOutputTruncated)
	if errors.Is(outputErr, errQueuedClose) {
		r.closeCompleted = true
	}
	if errors.Is(outputErr, errManagedReap) {
		defer func() { err = errors.Join(err, outputErr) }()
	}
	if stopErr == nil {
		if r.s.ExitCode == nil {
			return waitErr
		}
		r.s.State = "done"
		if waitErr != nil {
			r.s.State = "failed"
			r.s.Error = waitErr.Error()
		}
		return r.note("command/exit", fmt.Sprintf("exit_code=%d", *r.s.ExitCode))
	}
	switch {
	case errors.Is(stopErr, context.DeadlineExceeded):
		r.s.State, r.s.CompletedStatus = "timeout", "timeout"
	case errors.Is(stopErr, errStalled):
		r.s.State, r.s.CompletedStatus = "stalled", "stalled"
	case errors.Is(stopErr, errQueuedClose), errors.Is(stopErr, context.Canceled):
		if errors.Is(stopErr, errQueuedClose) {
			if runtime.GOOS != "windows" {
				if reclaimErr := r.reclaim(); reclaimErr != nil {
					return reclaimErr
				}
			}
			r.closeCompleted = true
		}
		r.s.State, r.s.CompletedStatus = "failed", "interrupted"
	default:
		return stopErr
	}
	return r.note("command/exit", r.s.CompletedStatus)
}

var errReapDeadline = errors.New("process reap deadline")

func waitProcess(cmd *exec.Cmd, limit time.Duration, clock timeshim.Clock) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-timeshim.After(timeshim.Or(clock), limit):
		return errReapDeadline
	}
}

func commandArgv(argv []string, shell bool) *exec.Cmd {
	if shell {
		return shellCommand(argv[0])
	}
	return exec.Command(argv[0], argv[1:]...)
}

// Auxiliary commands have no job of their own. Their processes are scoped
// to a separate group so stopping a check cannot stop the agent engine.
func runBoundedCommand(ctx context.Context, clock timeshim.Clock, limit time.Duration, cmd *exec.Cmd, poll func() error, config *config) (int64, error) {
	clock = timeshim.Or(clock)
	ctx, cancel := timeshim.WithTimeout(ctx, clock, limit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	child, err := startManagedCommand(cmd, config, true)
	if err != nil {
		return -1, err
	}
	waitErr, stopErr := child.wait(ctx, clock, poll)
	err = errors.Join(waitErr, stopErr, child.finish())
	if errors.Is(stopErr, context.DeadlineExceeded) {
		return 124, err
	}
	if stopErr == nil {
		if code := child.exitCode(); code != nil {
			return *code, err
		}
	}
	return -1, err
}

type commandTail []byte

func (t *commandTail) Write(p []byte) (int, error) {
	if len(p) >= outputTailBytes {
		*t = append((*t)[:0], p[len(p)-outputTailBytes:]...)
	} else {
		if discard := len(*t) + len(p) - outputTailBytes; discard > 0 {
			*t = append((*t)[:0], (*t)[discard:]...)
		}
		*t = append(*t, p...)
	}
	return len(p), nil
}
