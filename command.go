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
	"syscall"
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
	f, err := openJobFile(filepath.Join(r.dir, "output.log"), jobFileRead)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := min(fi.Size(), outputTailBytes)
	b, err := io.ReadAll(io.NewSectionReader(f, fi.Size()-size, size))
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
	out, err := openJobFile(filepath.Join(r.dir, "output.log"), jobFileAppend)
	if err != nil {
		return err
	}
	defer closeBestEffort(out)
	defer func() { err = errors.Join(err, r.publishOutputTail()) }()
	if r.c.Shell {
		cmd = shellCommand(r.c.Argv[0])
	} else {
		cmd = exec.Command(r.c.Argv[0], r.c.Argv[1:]...)
	}
	cmd.Dir = r.c.Dir
	output, flush := r.c.secret.output(cmd, out)
	cmd.Stdout, cmd.Stderr = output, output
	defer func() { err = errors.Join(err, flush()) }()
	r.c.mark(cmd, r.dir)
	tree, err := r.c.startCachedCommand(cmd)
	if err != nil {
		return err
	}
	stopRuntime := r.startRuntime()
	defer stopRuntime()
	done := make(chan processWait, 1)
	go func() { done <- processWait{cmd.Wait()} }()
	var exited bool
	defer func() {
		if !exited {
			_ = cmd.Process.Kill()
		}
		err = errors.Join(err, tree.cleanup())
		if !exited {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				err = errors.Join(err, errReapDeadline)
			}
		}
	}()
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
	ticker := timeshim.NewTicker(clock, commandPollInterval)
	defer ticker.Stop()
	var size int64
	for {
		var reason string
		var closing bool
		select {
		case waited := <-done:
			err = waited.err
			exited = true
			if cmd.ProcessState == nil {
				return err
			}
			if errors.Is(err, exec.ErrWaitDelay) {
				err = nil
			}
			code := commandExitCode(cmd.ProcessState)
			r.s.ExitCode = &code
			if err == nil {
				r.s.State = "done"
			} else {
				r.s.State = "failed"
				r.s.Error = err.Error()
			}
			return r.note("command/exit", fmt.Sprintf("exit_code=%d", code))
		case <-r.ctx.Done():
			reason = "timeout"
			if !errors.Is(r.ctx.Err(), context.DeadlineExceeded) {
				reason = "interrupted"
			}
		case <-ticker.C:
			fi, e := out.Stat()
			if e != nil {
				return e
			}
			if fi.Size() != size {
				size = fi.Size()
				lastOutput = clock.Now()
			}
			if _, ok, e := r.takeMarker("close"); e != nil {
				return e
			} else if ok {
				reason = "interrupted"
				closing = true
				if runtime.GOOS != "windows" {
					if err = r.reclaim(); err != nil {
						return err
					}
				}
			} else if r.c.Quiet > 0 && timeshim.Since(clock, lastOutput) >= r.c.Quiet {
				reason = "stalled"
			} else {
				continue
			}
		}
		if err = r.note("command/stop", reason); err != nil {
			return err
		}
		if runtime.GOOS == "windows" {
			if err = tree.cleanup(); err != nil {
				return err
			}
		} else {
			// The Unix process group remains available for explicit close or clean.
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				return errReapDeadline
			}
		}
		exited = true
		r.closeCompleted = closing
		if cmd.ProcessState != nil {
			code := commandExitCode(cmd.ProcessState)
			r.s.ExitCode = &code
		}
		r.s.State = reason
		r.s.CompletedStatus = reason
		if reason == "interrupted" {
			r.s.State = "failed"
		}
		return r.note("command/exit", reason)
	}
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
