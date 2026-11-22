package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// The worker has no handles to the submitter's terminal or SSH pipes.
// processWait carries the outcome for normal exit. After a forced stop,
// receivers only need to know that the process has been reaped.
type processWait struct{ err error }

func launchCommand(root, project, job string) (<-chan error, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "_command", "--state", root, "--project", project, "--job", job)
	detachWorker(cmd)
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return done, nil
}

const outputTailBytes = 64 * 1024

func saveOutputTail(dir string, f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size > outputTailBytes {
		size = outputTailBytes
	}
	b, err := io.ReadAll(io.NewSectionReader(f, fi.Size()-size, size))
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, "last.md"), b)
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
		err = errors.Join(err, r.save())
	}()
	r.log, err = os.OpenFile(filepath.Join(r.dir, "progress.log"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer closeBestEffort(r.log)
	out, err := os.OpenFile(filepath.Join(r.dir, "output.log"), os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer closeBestEffort(out)
	defer func() { err = errors.Join(err, saveOutputTail(r.dir, out)) }()
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
	tree, err := startCommand(cmd, r.c.RunID)
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
		tree.cleanup()
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
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var size int64
	lastOutput := time.Now()
	for {
		var reason string
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
				lastOutput = time.Now()
			}
			if _, ok, e := r.takeMarker("close"); e != nil {
				return e
			} else if ok {
				reason = "interrupted"
				if err = r.reclaim(); err != nil {
					return err
				}
			} else if r.c.Quiet > 0 && time.Since(lastOutput) >= r.c.Quiet {
				reason = "stalled"
			} else {
				continue
			}
		}
		if err = r.note("command/stop", reason+"; graceful signal, 10s grace"); err != nil {
			return err
		}
		// Deadlines stop the foreground payload only. Background processes
		// remain available for inspection until an explicit close or clean.
		_ = cmd.Process.Signal(syscall.SIGTERM)
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

func waitProcess(cmd *exec.Cmd, limit time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		return errReapDeadline
	}
}
