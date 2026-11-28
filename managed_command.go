package main

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/djylb/timeshim"
)

type commandPipe struct {
	read, write *os.File
	dst         io.Writer
	done        chan error
	aborted     atomic.Bool
}

func (p *commandPipe) Write(data []byte) (int, error) {
	if p.aborted.Load() {
		return 0, errOutputTruncated
	}
	return p.dst.Write(data)
}

func (p *commandPipe) copy() {
	_, err := io.Copy(p, p.read)
	if filter, ok := p.dst.(*filteredOutput); ok && !p.aborted.Load() {
		err = errors.Join(err, filter.finish())
	}
	p.done <- err
}

type managedCommand struct {
	cmd       *exec.Cmd
	tree      commandTree
	done      <-chan processWait
	pipes     []*commandPipe
	scoped    bool
	run       string
	drain     context.Context
	clock     timeshim.Clock
	poll      func() error
	reaped    bool
	processMu sync.Mutex
	waited    bool
}

func startManagedCommand(cmd *exec.Cmd, c *config, scoped bool) (*managedCommand, error) {
	m := &managedCommand{cmd: cmd, scoped: scoped}
	if c != nil {
		m.run = c.RunID
	}
	if scoped {
		m.run += "/" + rand.Text()
		env := cmd.Environ()
		cmd.Env = append(env, "CXRUN_PROCESS_SCOPE="+m.run)
	}
	pipe := func(dst io.Writer) (*os.File, error) {
		if dst == nil {
			return nil, nil
		}
		if file, ok := dst.(*os.File); ok {
			return file, nil
		}
		rd, wr, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		m.pipes = append(m.pipes, &commandPipe{read: rd, write: wr, dst: dst, done: make(chan error, 1)})
		return wr, nil
	}
	out, err := pipe(cmd.Stdout)
	if err != nil {
		return nil, err
	}
	errout := out
	if cmd.Stderr != cmd.Stdout {
		errout, err = pipe(cmd.Stderr)
	}
	if err == nil {
		cmd.WaitDelay = 0
		cmd.Stdout, cmd.Stderr = nil, nil
		if out != nil {
			cmd.Stdout = out
		}
		if errout != nil {
			cmd.Stderr = errout
		}
		if c == nil {
			m.tree, err = startCommand(cmd, "")
		} else {
			m.tree, err = c.startScopedCommand(cmd, !scoped)
		}
	}
	for _, p := range m.pipes {
		closeBestEffort(p.write)
		if err != nil {
			closeBestEffort(p.read)
		} else {
			go p.copy()
		}
	}
	if err != nil {
		return nil, err
	}
	m.done = m.commandDone()
	return m, nil
}

func (m *managedCommand) commandDone() <-chan processWait {
	done := make(chan processWait, 1)
	go func() {
		if !m.scoped || runtime.GOOS == "windows" {
			done <- processWait{m.cmd.Wait()}
			return
		}
		// Keep the process-group leader unreaped until the group is closed.
		// The kernel still reserves its PID at the exit notification.
		if waitScopedExit(m.cmd.Process.Pid) {
			m.processMu.Lock()
			killTree(m.cmd)
			err := m.cmd.Wait()
			m.waited = true
			m.processMu.Unlock()
			done <- processWait{err}
			return
		}
		// On platforms where notification is unavailable, cancellation
		// still signals the live process, but never signals a reaped PID.
		m.processMu.Lock()
		m.waited = true
		m.processMu.Unlock()
		done <- processWait{m.cmd.Wait()}
	}()
	return done
}

func (m *managedCommand) stop() error {
	if m.reaped {
		return nil
	}
	select {
	case <-m.done:
		m.reaped = true
		return nil
	default:
	}
	var err error
	switch {
	case runtime.GOOS == "windows":
		err = m.tree.cleanup()
	case m.scoped:
		m.processMu.Lock()
		if !m.waited {
			killTree(m.cmd)
		} else {
			_ = m.cmd.Process.Kill()
		}
		m.processMu.Unlock()
	default:
		_ = m.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-m.done:
			m.reaped = true
			return nil
		case <-time.After(10 * time.Second):
			_ = m.cmd.Process.Kill()
		}
	}
	select {
	case <-m.done:
		m.reaped = true
	case <-time.After(5 * time.Second):
		err = errors.Join(err, errReapDeadline)
	}
	return err
}

var errOutputTruncated = errors.New("truncated")
var errManagedReap = errors.New("reap_failed")

const outputDrainLimit = 2 * time.Second

func (m *managedCommand) finish() error {
	reapErr := errors.Join(m.stop(), m.tree.cleanup())
	if m.scoped && runtime.GOOS != "windows" { // main job processes stay until close or clean
		reapErr = errors.Join(reapErr, reapManagedProcesses(m.run))
	}
	var err error
	if reapErr != nil {
		err = errors.Join(errManagedReap, reapErr)
		for _, p := range m.pipes {
			closeBestEffort(p.read)
		}
	}
	if len(m.pipes) == 0 {
		return err
	}
	parent := m.drain
	if parent == nil {
		parent = context.Background()
	}
	clock := timeshim.Or(m.clock)
	ctx, cancel := timeshim.WithTimeout(parent, clock, outputDrainLimit)
	defer cancel()
	var ticks <-chan time.Time
	if m.poll != nil {
		ticker := timeshim.NewTicker(clock, commandPollInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	drain := func(p *commandPipe) (error, bool) {
		for {
			select {
			case readErr := <-p.done:
				return readErr, true
			default:
			}
			select {
			case readErr := <-p.done:
				return readErr, true
			case <-ctx.Done():
				return ctx.Err(), false
			case <-ticks:
				if pollErr := m.poll(); pollErr != nil {
					cancel()
					return pollErr, false
				}
			}
		}
	}
	for _, p := range m.pipes {
		readErr, done := drain(p)
		if !done {
			// A regular file or caller-provided writer can be blocked in a
			// non-cancellable write. Abandon its private copier at the deadline.
			p.aborted.Store(true)
		}
		closeBestEffort(p.read)
		if readErr != nil {
			err = errors.Join(err, errOutputTruncated, readErr)
		}
	}

	return err
}

func (m *managedCommand) wait(ctx context.Context, clock timeshim.Clock, poll func() error) (waitErr, stopErr error) {
	m.drain, m.clock, m.poll = ctx, clock, poll
	var ticks <-chan time.Time
	if poll != nil {
		ticker := timeshim.NewTicker(timeshim.Or(clock), commandPollInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case result := <-m.done:
			m.reaped = true
			return result.err, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticks:
			if err := poll(); err != nil {
				return nil, err
			}
		}
	}
}

func (m *managedCommand) exitCode() *int64 {
	if !m.reaped || m.cmd.ProcessState == nil {
		return nil
	}
	code := commandExitCode(m.cmd.ProcessState)
	return &code
}

func managedOutputStatus(err error) string {
	switch {
	case errors.Is(err, errManagedReap):
		return "reap_failed"
	case errors.Is(err, errOutputTruncated):
		return "truncated"
	default:
		return "drained"
	}
}
