package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

// A nil handle denotes contention. The caller owns every non-nil handle.
func lockPath(path string, flags int) (*os.File, error) {
	return lockFile(path, flags, tryLock)
}

func lockFile(path string, flags int, lock func(*os.File) (bool, error)) (*os.File, error) {
	f, err := os.OpenFile(path, flags|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	locked, err := lock(f)
	if !locked {
		f.Close()
		return nil, err
	}
	return f, nil
}

func registryLock(ctx context.Context, root string) (*os.File, error) {
	dir := filepath.Join(root, ".admit")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return lockUntil(ctx, filepath.Join(dir, "registry.lock"))
}
func lockUntil(ctx context.Context, path string) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		f, err := lockPath(path, os.O_CREATE)
		if err != nil || f != nil {
			return f, err
		}
		if err = pause(ctx, 20*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

type queueTicket struct{ file *os.File }

func (r *runner) pool() string {
	base := filepath.Join(r.root, ".admit")
	if r.c.SlotClass == "" || r.c.SlotClass == codexSlotClass {
		return base
	}
	return filepath.Join(base, "classes", r.c.SlotClass)
}
func (r *runner) newTicket() (*queueTicket, error) {
	dir := filepath.Join(r.pool(), "queue")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%019d-%s-%s", time.Now().UnixNano(), r.s.Project, r.s.Job))
	f, err := lockPath(path, os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, err
	}
	return &queueTicket{f}, nil
}

func (t *queueTicket) release() {
	if t.file != nil {
		path := t.file.Name()
		t.file.Close()
		os.Remove(path)
		t.file = nil
	}
}

// Called under the registry lock, so a newly created ticket already has its owner.
func (t *queueTicket) first() (bool, error) {
	dir := filepath.Dir(t.file.Name())
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if path == t.file.Name() {
			return true, nil
		}
		f, err := lockPath(path, 0)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if f == nil {
			return false, nil
		}
		f.Close()
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, errors.New("queue ticket disappeared")
}
func loadOK(ctx context.Context, command string) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd.exe", "/d", "/s", "/c", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}
	ownGroup(cmd)
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error { killTree(cmd); return nil }
	return cmd.Run() == nil
}

// The running budget spans the app-server lifetime (including restarts)
// but is suspended each time a follow-up turn needs a new FIFO ticket.
type runtimeBudget struct {
	parent    context.Context
	remaining time.Duration
	deadline  time.Time
	cancel    context.CancelFunc
}

func (r *runner) startRuntime() func() {
	if r.c.Timeout == 0 {
		return func() {}
	}
	r.budget = &runtimeBudget{parent: r.ctx, remaining: r.c.Timeout}
	r.resumeRuntime()
	return func() { r.budget.cancel() }
}
func (r *runner) resumeRuntime() {
	b := r.budget
	b.deadline = time.Now().Add(b.remaining)
	r.ctx, b.cancel = context.WithDeadline(b.parent, b.deadline)
}
func (r *runner) pauseRuntime() error {
	b := r.budget
	b.remaining = time.Until(b.deadline)
	if b.remaining <= 0 {
		return context.DeadlineExceeded
	}
	b.cancel()
	r.ctx = b.parent
	return nil
}

var errQueueTimeout = fmt.Errorf("queue_timeout: %w", context.DeadlineExceeded)
var errQueuedClose = errors.New("closed while queued")

func (r *runner) releaseSlot() {
	if r.slot != nil {
		r.slot.Close()
		r.slot = nil
	}
}

func (r *runner) acquireSlot() error {
	if r.slot != nil {
		return nil
	}
	return r.admit(nil)
}

func (r *runner) admit(t *queueTicket) (err error) {
	if t != nil {
		defer t.release()
	}
	if r.budget != nil {
		if err = r.pauseRuntime(); err != nil {
			return err
		}
		defer func() {
			if err == nil {
				r.resumeRuntime()
			}
		}()
	}
	ctx := r.ctx
	if r.c.QueueTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, r.c.QueueTimeout, errQueueTimeout)
		defer cancel()
		defer func() {
			if errors.Is(context.Cause(ctx), errQueueTimeout) {
				r.releaseSlot()
				err = errQueueTimeout
			}
		}()
	}

	r.s.State = "queued"
	if err := r.save(); err != nil {
		return err
	}
	return r.reserve(ctx, t, true, func(reason string) error {
		return r.setQueueReason(reason)
	})
}
func (r *runner) setQueueReason(reason string) error {
	if r.s.QueueReason == reason {
		return nil
	}
	r.s.QueueReason = reason
	return r.save()
}

func (r *runner) reserve(ctx context.Context, t *queueTicket, controls bool, report func(string) error) error {
	host, err := readHostConfig(r.root)
	if err != nil {
		return err
	}
	slots := host.slotCount(r.c.SlotClass)
	if t == nil {
		guard, err := registryLock(ctx, r.root)
		if err != nil {
			return err
		}
		t, err = r.newTicket()
		guard.Close()
		if err != nil {
			return err
		}
		defer t.release()
	}
	var checked, diskChecked, nextMaintenance time.Time
	allowed := host.LoadCmd == ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if controls {
			if _, ok, e := r.takeMarker("close"); e != nil {
				return e
			} else if ok {
				if err := r.reclaim(); err != nil {
					return err
				}
				return errQueuedClose
			}
		}
		if host.LoadCmd != "" && time.Since(checked) >= 15*time.Second {
			allowed = loadOK(ctx, host.LoadCmd)
			checked = time.Now()
			continue
		}
		if time.Since(diskChecked) >= time.Second {
			policy, err := readHostConfig(r.root)
			if err != nil {
				return err
			}
			host.MinFreeBytes, host.CacheMaxBytes = policy.MinFreeBytes, policy.CacheMaxBytes
			_, _, warning := host.diskStatus(r.root)
			if err := report(warning); err != nil {
				return err
			}
			diskChecked = time.Now()
		}
		if allowed && !time.Now().Before(nextMaintenance) {
			guard, err := registryLock(ctx, r.root)
			if err != nil {
				return err
			}
			first, err := t.first()
			var slot *os.File
			if first && err == nil {
				for i := 0; i < slots; i++ {
					slot, err = lockPath(filepath.Join(r.pool(), fmt.Sprintf("slot-%d.lock", i)), os.O_CREATE)
					if slot != nil || err != nil {
						break
					}
				}
			}
			guard.Close()
			if err != nil {
				return err
			}
			if slot != nil {
				reason, err := maintainBuildCache(ctx, r.root, host)
				if err == nil {
					err = report(reason)
				}
				if reason != "" || err != nil {
					slot.Close()
					if err != nil {
						return err
					}
					nextMaintenance = time.Now().Add(time.Second)
				} else {
					r.slot = slot
					return nil
				}
			}
		}
		if err := pause(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

type slotResult struct {
	slot *os.File
	err  error
}
type followAdmission struct {
	done      chan slotResult
	cancel    context.CancelFunc
	abandoned chan struct{}
	stopped   chan struct{}
	reasons   chan string
}

func (r *runner) cancelFollow() {
	if r.follow != nil {
		r.follow.cancel()
		close(r.follow.abandoned)
		select {
		case <-r.follow.stopped:
		case <-time.After(12 * time.Second):
		}
		r.follow = nil
	}
}
func (r *runner) followSlot() (bool, error) {
	if r.slot != nil {
		return true, nil
	}
	if r.follow == nil {
		if r.budget != nil {
			if err := r.pauseRuntime(); err != nil {
				return false, err
			}
		}
		ctx, cancel := context.WithCancel(r.ctx)
		if r.c.QueueTimeout > 0 {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeoutCause(ctx, r.c.QueueTimeout, errQueueTimeout)
			parentCancel := cancel
			cancel = func() { stop(); parentCancel() }
		}
		q := &followAdmission{done: make(chan slotResult), cancel: cancel, abandoned: make(chan struct{}), stopped: make(chan struct{}), reasons: make(chan string, 1)}
		r.follow = q
		// The admission worker owns the ticket and slot until the result is collected.
		worker := runner{root: r.root, c: r.c, s: status{Job: r.s.Job, Project: r.s.Project}}
		go func() {
			defer close(q.stopped)
			err := worker.reserve(ctx, nil, false, func(reason string) error {
				select {
				case <-q.reasons:
				default:
				}
				q.reasons <- reason
				return nil
			})
			if cause := context.Cause(ctx); cause != nil {
				worker.releaseSlot()
				err = cause
			}
			select {
			case q.done <- slotResult{worker.slot, err}:
			case <-q.abandoned:
				worker.releaseSlot()
			}
		}()
		r.s.State = "queued"
		return false, r.save()
	}
	select {
	case result := <-r.follow.done:
		r.follow.cancel()
		r.follow = nil
		r.slot = result.slot
		if r.budget != nil {
			r.resumeRuntime()
		}
		if result.err != nil {
			return false, result.err
		}
		return true, r.setQueueReason("")
	default:
		select {
		case reason := <-r.follow.reasons:
			return false, r.setQueueReason(reason)
		default:
			return false, nil
		}
	}
}
