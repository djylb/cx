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

	"github.com/djylb/timeshim"
)

// A nil handle denotes contention. The caller owns every non-nil handle.
func lockPath(path string, flags int) (*os.File, error) {
	return lockFile(path, flags, tryLock)
}

func lockFile(path string, flags int, lock func(*os.File) (bool, error)) (*os.File, error) {
	mode := jobFileLockExisting
	if flags&os.O_EXCL != 0 {
		mode = jobFileNew
	} else if flags&os.O_CREATE != 0 {
		mode = jobFileLock
	}
	f, err := openJobFile(path, mode)
	if err != nil {
		return nil, err
	}
	locked, err := lock(f)
	if !locked {
		_ = f.Close()
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
	clock := timeshim.Or(contextClock(ctx))
	ctx, cancel := timeshim.WithTimeout(ctx, clock, 5*time.Second)
	defer cancel()
	for {
		f, err := lockPath(path, os.O_CREATE)
		if err != nil || f != nil {
			return f, err
		}
		if err = timeshim.Sleep(ctx, clock, 20*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

type queueTicket struct{ file *os.File }

func (r *runner) pool() string {
	base := filepath.Join(r.root, ".admit")
	if r.c.SlotClass == "" || r.c.SlotClass == agentSlotClass {
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
		_ = t.file.Close()
		_ = os.Remove(path)
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
		_ = f.Close()
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, errors.New("queue ticket disappeared")
}
func loadOK(ctx context.Context, command string, clock timeshim.Clock) bool {
	ctx, cancel := timeshim.WithTimeout(ctx, clock, 10*time.Second)
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
	cancel    context.CancelFunc
	deadline  time.Time
	remaining time.Duration
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
	b.deadline = timeshim.Or(r.clock).Now().Add(b.remaining)
	r.ctx, b.cancel = timeshim.WithDeadline(b.parent, timeshim.Or(r.clock), b.deadline)
}
func (r *runner) pauseRuntime() error {
	b := r.budget
	b.remaining = b.deadline.Sub(timeshim.Or(r.clock).Now())
	if b.remaining <= 0 {
		return context.DeadlineExceeded
	}
	b.cancel()
	r.ctx = b.parent
	return nil
}

var errQueueTimeout = fmt.Errorf("queue_timeout: %w", context.DeadlineExceeded)
var errQueuedClose = errors.New("closed while queued")

const (
	slotQueueGrace       = 2 * time.Second
	loadInterval         = 15 * time.Second
	diskRecoveryInterval = 15 * time.Second
)

func (r *runner) releaseSlot() {
	if r.slot != nil {
		_ = r.slot.Close()
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
		ctx, cancel = timeshim.WithTimeoutCause(ctx, timeshim.Or(r.clock), r.c.QueueTimeout, errQueueTimeout)
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
	if reason != "" {
		if r.s.QueueGeneration == ^uint64(0) {
			return errors.New("queue generation exhausted")
		}
		// Persist the reservation before any observer can acknowledge this
		// reason. Recovery keeps the generation even if the next frame is lost.
		reserved := r.s
		reserved.QueueReason = ""
		reserved.QueueGeneration++
		if err := r.stateLog.Append("status", reserved); err != nil {
			return err
		}
		if err := r.stateLog.sync(); err != nil {
			return err
		}
		r.s.QueueGeneration++
	}
	r.s.QueueReason = reason
	return r.save()
}

func (r *runner) reserve(ctx context.Context, t *queueTicket, controls bool, report func(string) error) error {
	clock := timeshim.Or(r.clock)
	host, err := readHostConfig(r.root)
	if err != nil {
		return err
	}
	if t == nil {
		guard, err := registryLock(ctx, r.root)
		if err != nil {
			return err
		}
		t, err = r.newTicket()
		_ = guard.Close()
		if err != nil {
			return err
		}
		defer t.release()
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	defer stopMaintenance()
	var maintenanceDone chan struct{}
	var loadChecked, diskChecked, slotBlockedSince time.Time
	var diskReason, slotReason string
	allowed := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if controls {
			if _, ok, e := r.takeMarker("close"); e != nil {
				return e
			} else if ok {
				if runtime.GOOS != "windows" {
					if err := r.reclaim(); err != nil {
						return err
					}
				}
				r.closeCompleted = true
				return errQueuedClose
			}
		}
		if clock.Now().Sub(diskChecked) >= time.Second {
			policy, err := readHostConfig(r.root)
			if err != nil {
				return err
			}
			if policy.LoadCmd != host.LoadCmd {
				loadChecked = time.Time{}
			}
			host = policy
			disk := host.diskStatus(r.root)
			select {
			case <-maintenanceDone:
				maintenanceDone = nil
			default:
			}
			if disk.belowMinimum() && maintenanceDone == nil {
				maintenanceDone = make(chan struct{})
				go func(policy *hostConfig, done chan struct{}) {
					defer close(done)
					scanCtx, cancel := timeshim.WithTimeout(maintenanceCtx, clock, transferLimit)
					defer cancel()
					if err := maintainCaches(scanCtx, r.root, policy, false); err != nil && scanCtx.Err() == nil {
						diagnosticf("cache maintenance: %v\n", err)
					}
				}(host, maintenanceDone)
			}
			diskReason = disk.queueReason()
			diskChecked = clock.Now()
		}
		reason := diskReason
		if reason == "" && host.LoadCmd != "" {
			if clock.Now().Sub(loadChecked) >= loadInterval {
				allowed = loadOK(ctx, host.LoadCmd, clock)
				loadChecked = clock.Now()
			}
			if !allowed {
				reason = "host load check did not pass"
			}
		}
		if reason != "" {
			slotBlockedSince, slotReason = time.Time{}, ""
		} else {
			slot, blocked, err := r.trySlot(ctx, t, host.slotCount(r.c.SlotClass))
			if err != nil {
				return err
			}
			if slot != nil {
				diskReason = host.diskStatus(r.root).queueReason()
				diskChecked = clock.Now()
				if diskReason == "" {
					if err = report(""); err == nil {
						r.slot = slot
						return nil
					}
				}
				closeBestEffort(slot)
				if err != nil {
					return err
				}
				reason = diskReason
				slotBlockedSince, slotReason = time.Time{}, ""
			} else {
				if slotReason != blocked {
					slotBlockedSince, slotReason = clock.Now(), blocked
				}
				if clock.Now().Sub(slotBlockedSince) >= slotQueueGrace {
					reason = slotReason
				}
			}
		}
		if err := report(reason); err != nil {
			return err
		}
		if err := timeshim.Sleep(ctx, timeshim.Or(contextClock(ctx)), 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func (r *runner) trySlot(ctx context.Context, ticket *queueTicket, slots int) (*os.File, string, error) {
	guard, err := registryLock(ctx, r.root)
	if err != nil {
		return nil, "", err
	}
	defer closeBestEffort(guard)
	first, err := ticket.first()
	if err != nil {
		return nil, "", err
	}
	if !first {
		return nil, "waiting for earlier FIFO tickets", nil
	}
	for i := 0; i < slots; i++ {
		slot, err := lockPath(filepath.Join(r.pool(), fmt.Sprintf("slot-%d.lock", i)), os.O_CREATE)
		if slot != nil || err != nil {
			return slot, "", err
		}
	}
	return nil, "all slots in the admission class are occupied", nil
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
			ctx, stop = timeshim.WithTimeoutCause(ctx, timeshim.Or(r.clock), r.c.QueueTimeout, errQueueTimeout)
			parentCancel := cancel
			cancel = func() { stop(); parentCancel() }
		}
		q := &followAdmission{done: make(chan slotResult), cancel: cancel, abandoned: make(chan struct{}), stopped: make(chan struct{}), reasons: make(chan string, 1)}
		r.follow = q
		// The admission worker owns the ticket and slot until the result is collected.
		worker := runner{root: r.root, c: r.c, clock: r.clock, s: status{Job: r.s.Job, Project: r.s.Project}}
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
