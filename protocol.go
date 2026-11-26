package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/djylb/timeshim"
)

type fields = map[string]any
type rpcError struct {
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
	Code    int             `json:"code"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("RPC %d: %s", e.Code, e.Message) }

type message struct {
	Error          *rpcError
	Method         string
	ID             json.RawMessage
	Params, Result json.RawMessage
}

var errServerClosed = errors.New("app-server transport closed")

type frame struct {
	err error
	raw []byte
}
type server struct {
	clock           timeshim.Clock
	cmd             *exec.Cmd
	finishLog       func() error
	in              io.WriteCloser
	frames          chan frame
	stop            chan struct{}
	tree            commandTree
	writes          chan writeRequest
	expired         map[string]bool
	broken          bool
	writeTimeout    time.Duration
	responseTimeout time.Duration
	next            int
}

func openServer(c config, root, dir string) (*server, error) {
	args := []string{"app-server", "--listen", "stdio://"}
	if c.JobTools {
		exe, toolArgs, e := jobToolServer(c, root, dir)
		if e != nil {
			return nil, e
		}
		binary, _ := json.Marshal(exe)
		argv, _ := json.Marshal(toolArgs)
		prefix := "mcp_servers." + jobToolServerName + "."
		args = append(args,
			"-c", prefix+"command="+string(binary),
			"-c", prefix+"args="+string(argv),
			"-c", fmt.Sprintf("%stool_timeout_sec=%d", prefix, int(jobToolTimeout/time.Second)),
			"-c", prefix+`default_tools_approval_mode="approve"`,
		)
	}
	cmd := exec.Command("codex", args...)
	cmd.Dir = c.Dir

	log, err := openJobFile(filepath.Join(dir, "server.stderr.log"), jobFileAppendCreate)
	if err != nil {
		return nil, err
	}
	keepLog := false
	defer func() {
		if !keepLog {
			_ = log.Close()
		}
	}()
	stderr, flush := c.secret.output(cmd, log)
	cmd.Stderr = stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		_ = in.Close()
		return nil, err
	}
	c.mark(cmd, dir)
	tree, err := c.startCachedCommand(cmd)
	if err != nil {
		_ = in.Close()
		_ = out.Close()
		return nil, err
	}
	s := &server{tree: tree, writes: make(chan writeRequest, 16), expired: make(map[string]bool), writeTimeout: 10 * time.Second, cmd: cmd, in: in, frames: make(chan frame, 32), stop: make(chan struct{})}
	if c.secret != nil {
		keepLog = true
		s.finishLog = func() error { return errors.Join(flush(), log.Close()) }
	}
	go s.writer()
	go func() {
		defer close(s.frames)
		br := bufio.NewReader(out)
		for {
			line, err := br.ReadBytes('\n')
			if len(bytes.TrimSpace(line)) > 0 {
				select {
				case s.frames <- frame{raw: line}:
				case <-s.stop:
					return
				}
			}
			if err != nil {
				select {
				case s.frames <- frame{err: err}:
				case <-s.stop:
				}
				return
			}
		}
	}()
	return s, nil
}

// Read the final notifications through EOF before Wait closes the stdout pipe.
func (s *server) wait() error {
	err := s.cmd.Wait()
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil
	}
	if s.finishLog != nil {
		err = errors.Join(err, s.finishLog())
	}
	return err
}

func (r *runner) shutdown() (result error) {
	s := r.srv
	// The Windows driver owns a Job Object and terminal publication also
	// reclaims processes that kept the run marker outside that object.
	closing := runtime.GOOS != "windows" && !r.closing.IsZero()
	defer func() { result = errors.Join(result, s.tree.cleanup()) }()
	if closing {
		defer func() {
			if closing {
				result = errors.Join(result, r.reclaim())
			}
		}()
	}
	if s.broken && !closing {
		_ = s.cmd.Process.Kill()
	}
	_ = s.in.Close()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var drainErr error
drain:
	for {
		select {
		case f, ok := <-s.frames:
			if !ok || errors.Is(f.err, io.EOF) {
				break drain
			}
			if _, err := r.read(f); err != nil {
				drainErr = err
				break drain
			}
		case <-timer.C:
			break drain
		}
	}
	drainErr = errors.Join(drainErr, r.flushTurnFailure())
	close(s.stop)
	done := make(chan processWait, 1)
	go func() { done <- processWait{s.wait()} }()
	select {
	case waited := <-done:
		return errors.Join(drainErr, waited.err)
	case <-time.After(3 * time.Second):
	}
	if closing {
		closing = false
		if err := r.reclaim(); err != nil {
			return errors.Join(drainErr, err)
		}
	}
	_ = s.cmd.Process.Kill()
	drainErr = errors.Join(drainErr, s.tree.cleanup())
	select {
	case <-done:
		return errors.Join(drainErr, r.note("shutdown", "app-server exceeded EOF deadline; killed owned PID"))
	case <-time.After(5 * time.Second):
		return errors.Join(drainErr, errors.New("app-server did not exit after kill"))
	}
}

type writeRequest struct {
	done chan error
	data []byte
}

func (s *server) writer() {
	for {
		select {
		case <-s.stop:
			return
		case w := <-s.writes:
			_, err := s.in.Write(w.data)
			w.done <- err
			if err != nil {
				return
			}
		}
	}
}

// Only the runner calls send. A timed-out stream may contain a partial frame.
func (s *server) send(v fields) error {
	if s.broken {
		return errServerClosed
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w := writeRequest{data: append(b, '\n'), done: make(chan error, 1)}
	timer := timeshim.NewTimer(timeshim.Or(s.clock), s.writeTimeout)
	defer timer.Stop()
	select {
	case s.writes <- w:
		select {
		case err = <-w.done:
			if err == nil {
				return nil
			}
		case <-timer.C:
			err = errors.New("stdin write deadline")
		}
	case <-timer.C:
		err = errors.New("stdin queue deadline")
	}
	s.broken = true
	return errors.Join(errServerClosed, err)
}
func (r *runner) read(f frame) (message, error) {
	var m message
	if f.err != nil || len(f.raw) == 0 {
		if err := r.flushTurnFailure(); err != nil {
			return m, err
		}
		return m, fmt.Errorf("%w: %v", errServerClosed, f.err)
	}
	if err := json.Unmarshal(f.raw, &m); err != nil {
		return m, fmt.Errorf("invalid app-server JSON: %w", err)
	}
	if m.Method != "" {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(f.raw, &record); err != nil {
			return m, err
		}
		if r.c.secret != nil && len(m.Params) != 0 {
			filtered, err := r.c.secret.jsonOutput(m.Params)
			if err != nil {
				return m, err
			}
			record["params"] = filtered
		}
		if err := r.appendEvent(r.events, record); err != nil {
			return m, err
		}
		if m.Method == "thread/compacted" || m.Method == "item/completed" {
			var compact event
			if json.Unmarshal(m.Params, &compact) == nil && compact.ThreadID == r.s.Thread && (m.Method == "thread/compacted" || compact.Item.Type == "contextCompaction") {
				if err := json.Unmarshal(record["seq"], &r.s.CompactSeq); err != nil {
					return m, err
				}
			}
		}
		r.s.LastEvent = timeshim.Or(r.clock).Now().UTC()
		r.dirty = true
		if len(m.ID) > 0 {
			return m, r.request(m, record["params"])
		}
		return m, r.notify(m)
	}
	if r.srv.expired[string(m.ID)] {
		delete(r.srv.expired, string(m.ID))
		return message{Method: "rpc/late"}, r.localEvent("rpc/late", string(m.ID))
	}
	return m, nil
}

// Pump notifications and server requests even when they precede the RPC response.
func (r *runner) call(method string, p any, result any) error {
	r.srv.next++
	id := r.srv.next
	if err := r.srv.send(fields{"id": id, "method": method, "params": p}); err != nil {
		return err
	}
	// Approval may legitimately outlive a normal RPC, but never extend the absolute deadline.
	limit := r.srv.responseTimeout
	if limit == 0 {
		limit = 60 * time.Second
	}
	clock := timeshim.Or(r.srv.clock)
	deadline := clock.Now().Add(limit + r.c.Approval)
	timer := timeshim.NewTimer(clock, limit)
	defer timer.Stop()
	tick := time.NewTicker(statePollInterval)
	defer tick.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-timer.C:
			if remaining := timeshim.Until(clock, deadline); remaining > 0 && len(r.s.Approvals) > 0 {
				timer.Reset(remaining)
				continue
			}
			r.srv.expired[strconv.Itoa(id)] = true
			return fmt.Errorf("%s timed out: %w", method, errRPCUncertain)
		case <-tick.C:
			if err := r.flushState(); err != nil {
				return err
			}
			if err := r.flushExpiredTurnFailure(); err != nil {
				return err
			}
			if len(r.s.Approvals) > 0 {
				if err := r.rpcFiles(true); err != nil {
					return err
				}
				if err := r.approvalFiles(); err != nil {
					return err
				}
			}
		case f := <-r.srv.frames:
			m, err := r.read(f)
			if err != nil {
				return errors.Join(errRPCUncertain, err)
			}
			if m.Method != "" {
				continue
			}
			if string(m.ID) != strconv.Itoa(id) {
				return fmt.Errorf("%w: %s: unexpected response id %s", errRPCUncertain, method, m.ID)
			}
			if m.Error != nil {
				data, filterErr := r.c.secret.jsonOutput(m.Error.Data)
				if filterErr != nil {
					return filterErr
				}
				m.Error.Data = data
				m.Error.Message = r.c.secret.text(m.Error.Message)
				return fmt.Errorf("%s: %w", method, m.Error)
			}
			if result != nil {
				return json.Unmarshal(m.Result, result)
			}
			return nil
		}
	}
}

type runner struct {
	idleSince, turnSince, closing time.Time
	lastAction                    time.Time
	lastStateWrite                time.Time

	ctx                       context.Context
	clock                     timeshim.Clock
	claude                    *claudeSession
	budget                    *runtimeBudget
	slot                      *os.File
	targetLease, packageLease *os.File
	events, log               *os.File
	stateLog                  *stateWriter
	inbox                     *inboxState
	srv                       *server
	turnFailure               *pendingTurnFailure
	cmdOpen                   map[string]time.Time // command items in progress
	follow                    *followAdmission

	root, dir string
	lastText  string
	pending   string // a turn to start once the current one ended (stall recovery)

	c              config
	s              status
	closeRequest   uint64
	closeCompleted bool
	dirty          bool
}

const (
	statePollInterval    = 100 * time.Millisecond
	statePublishInterval = 150 * time.Millisecond
)

// The caller holds owner.lock until the writer is closed.
func (r *runner) openState() error {
	w, err := openStateLog(r.dir)
	if err != nil {
		return err
	}
	if err = w.RecoveryError(); err != nil {
		appendProgress(r.dir, "state/recovery", err.Error())
	}
	if err = w.Append("config", r.c); err != nil {
		return errors.Join(err, w.Close())
	}
	r.stateLog = w
	return nil
}

func (r *runner) saveTerminal() (result error) {
	defer closeBestEffort(r.stateLog)
	r.s.QueueReason = ""
	defer func() {
		if r.inbox != nil {
			closeBestEffort(r.inbox.tail)
			closeBestEffort(r.inbox.outboxTail)
		}
	}()
	if runtime.GOOS == "windows" && r.root != "" && r.c.RunID != "" {
		// The drivers have finished logging before calling saveTerminal.
		reaper := runner{root: r.root, dir: r.dir, c: r.c, s: r.s, ctx: r.ctx}
		if err := reaper.reclaim(); err != nil {
			r.closeCompleted = false
			if r.s.Error == "" {
				r.s.Error = err.Error()
			} else {
				r.s.Error += "\n" + err.Error()
			}
			if r.s.State != "orphaned" {
				r.s.State = "failed"
			}
			defer func() { result = errors.Join(result, err) }()
		}
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// Cleanup must still run after the runtime deadline or a canceled caller.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	unlock, err := lockJobMailboxes(ctx, r.dir)
	if err != nil {
		return err
	}
	defer unlock()
	var outcomes []stateRecord
	if err = r.stateLog.RecoveryError(); err == nil {
		outcomes, err = r.terminalMailboxOutcomes()
	}
	if !errors.Is(err, errLogDamage) {
		if err != nil {
			return err
		}
		return r.stateLog.Final(r.s, outcomes...)
	}
	// A damaged transcript cannot establish an individual result safely.
	// Publish the job-level failure without deducing or replaying operations.
	r.s.Error = err.Error()
	if r.s.State == "orphaned" {
		return r.stateLog.Final(r.s)
	}
	r.s.State = "failed"
	return errors.Join(err, r.stateLog.Final(r.s))
}

func (r *runner) failStart(err error) error {
	r.s.State = "failed"
	if errors.Is(err, errQueueTimeout) {
		r.s.State = "queue_timeout"
	}
	r.s.PID = 0
	r.s.Error = err.Error()
	return errors.Join(err, r.saveTerminal())
}

func (r *runner) flushState() error {
	if !r.stateLog.NeedsRotate() && (!r.dirty || time.Since(r.lastStateWrite) < statePublishInterval) {
		return nil
	}
	return r.save()
}

// save publishes transitions immediately. Pollers use flushState for telemetry.
func (r *runner) save() error {
	if err := r.stateLog.Append("status", r.s); err != nil {
		return err
	}
	r.dirty = false
	r.lastStateWrite = time.Now()
	if !r.stateLog.NeedsRotate() {
		return nil
	}
	if err := validateSegments(filepath.Join(r.dir, "state"), false); err != nil {
		return err
	}
	// Runtime preparation adds machine-local cache paths to r.c.
	c, err := jobConfigRecord(r.dir)
	if err != nil {
		return err
	}
	checkpoints, err := mailboxCheckpoints(r.dir)
	if err != nil {
		return err
	}
	reports, err := turnReportCheckpoints(r.dir)
	if err != nil {
		return err
	}
	completions, err := completionCheckpoints(r.dir)
	if err != nil {
		return err
	}
	checkpoints = append(checkpoints, reports...)
	return r.stateLog.Rotate(c.Value, r.s, append(checkpoints, completions...)...)
}
func (r *runner) active() bool { return r.s.TurnStatus == "inProgress" }
func (r *runner) deriveState() {
	if r.s.InputKind == "server_request" || r.s.InputKind == agentAsk {
		r.s.InputKind = ""
	}
	for _, a := range r.s.Approvals {
		if inputMethod(a.Method) {
			r.s.State = "waiting_input"
			r.s.InputKind = "server_request"
			if a.Method == agentAsk {
				r.s.InputKind = agentAsk
			}
			return
		}
	}
	switch {
	case r.s.InputKind == "next_turn":
		r.s.State = "waiting_input"
	case len(r.s.Approvals) > 0:
		r.s.State = "waiting_approval"
	case r.active():
		r.s.State = "running"
	case r.follow != nil:
		r.s.State = "queued"
	default:
		r.s.State = "waiting"
	}
}
func (r *runner) serveAgent(session func() error) (err error) {
	defer r.cancelFollow()
	r.s.PID = os.Getpid()
	r.s.EnginePID = 0
	r.s.Error = ""
	r.s.StallCount = 0
	r.s.LastAction = ""
	r.s.State = "running"
	r.s.Approvals = nil
	defer func() {
		if r.inbox != nil {
			// Finish receipts for RPCs that lost the engine before the job
			// terminates, including initial fork and transport errors.
			err = errors.Join(err, r.recoverRPC())
		}
		r.s.PID = 0
		r.s.EnginePID = 0
		if r.s.InputKind == "server_request" || r.s.InputKind == agentAsk {
			r.s.InputKind = ""
		}
		r.s.Approvals = nil
		if err != nil {
			r.s.State = "failed"
			if errors.Is(err, errQueueTimeout) {
				r.s.State = "queue_timeout"
			} else if errors.Is(err, context.DeadlineExceeded) {
				r.s.State = "timeout"
			} else if errors.Is(err, errStalled) {
				r.s.State = "stalled"
			}
			r.s.Error = err.Error()
		} else {
			r.s.State = "done"
		}
		err = errors.Join(err, r.saveTerminal())
	}()
	if err = r.save(); err != nil {
		return err
	}
	r.log, err = openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
	if err != nil {
		return err
	}
	defer closeBestEffort(r.log)
	defer func() {
		if err != nil {
			err = errors.Join(err, r.note("failed", err.Error()))
		} else {
			err = r.note("done", "agent exited")
		}
	}()
	r.events, err = openJobFile(filepath.Join(r.dir, "events.jsonl"), jobFileAppend)
	if err != nil {
		return err
	}
	defer closeBestEffort(r.events)
	err = session()
	if errors.Is(err, errQueuedClose) {
		r.s.CompletedStatus = "interrupted"
		err = nil
	}
	r.closeCompleted = err == nil && r.closeRequest != 0
	return err
}

func (r *runner) serveCodex() (err error) {
	r.srv, err = openServer(r.c, r.root, r.dir)
	if err != nil {
		return err
	}
	stopRuntime := r.startRuntime()
	defer stopRuntime()
	defer func() {
		if r.srv != nil {
			err = errors.Join(err, r.shutdown())
		}
	}()
	r.s.EnginePID = r.srv.cmd.Process.Pid
	if err = r.save(); err != nil {
		return err
	}
	err = r.initializeThread()
	tick := time.NewTicker(statePollInterval)
	defer tick.Stop()
	for {
		if err != nil {
			if errors.Is(err, errQueuedClose) {
				r.s.CompletedStatus = "interrupted"
				return nil
			}
			if errors.Is(err, errServerClosed) && !r.active() && r.slot == nil && r.follow == nil && r.s.CompletedStatus == "failed" {
				return nil
			}
			if !errors.Is(err, errServerClosed) || r.s.EngineRestarts >= r.c.RestartLimit || !r.closing.IsZero() {
				return err
			}
			r.s.Restarts++
			r.s.EngineRestarts++
			r.s.StallCount = 3
			r.s.LastAction = "resume after app-server exit"
			if noteErr := r.note("restart", r.s.LastAction); noteErr != nil {
				return noteErr
			}
			unfinished := r.active() || r.s.Finished == 0
			if unfinished && r.s.Thread != "" {
				r.pending = r.c.nudgeText(defaultContinue)
			}
			if err = r.save(); err != nil {
				return err
			}
			if err = r.restartServer(); err != nil {
				continue
			}

			r.lastAction = timeshim.Or(r.clock).Now()
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case f := <-r.srv.frames:
			_, err = r.read(f)
		case <-tick.C:
			var stop bool
			stop, err = r.pollAgent()
			if stop {
				return err
			}
		}
	}
}

func (r *runner) initializeThread() error {
	r.s.Generation++
	if err := r.recoverRPC(); err != nil {
		return err
	}
	if err := r.save(); err != nil {
		return err
	}
	if err := r.initialize(); err != nil {
		return err
	}
	return r.connectThread()
}

func (r *runner) initialize() error {
	if err := r.call("initialize", fields{"clientInfo": fields{"name": "cxrun", "version": "1"}, "capabilities": fields{"experimentalApi": true}}, nil); err != nil {
		return err
	}
	return r.srv.send(fields{"method": "initialized"})
}
