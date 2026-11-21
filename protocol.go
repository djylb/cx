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
	"strconv"
	"time"
)

type fields = map[string]any
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("RPC %d: %s", e.Code, e.Message) }

type message struct {
	ID             json.RawMessage
	Method         string
	Params, Result json.RawMessage
	Error          *rpcError
}

var errServerClosed = errors.New("app-server transport closed")

type frame struct {
	raw []byte
	err error
}
type server struct {
	cmd             *exec.Cmd
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

func openServer(c config, dir string) (*server, error) {
	args := []string{"app-server", "--listen", "stdio://"}
	if c.JobTools {
		exe, e := os.Executable()
		if e != nil {
			return nil, e
		}
		root, e := stateRoot()
		if e != nil {
			return nil, e
		}
		id := filepath.Base(dir)
		if c.Project != "" {
			id = c.Project + "/" + id
		}
		binary, _ := json.Marshal(exe)
		argv, _ := json.Marshal([]string{"node", "mcp-tools", "--state", root, "--job", id})
		args = append(args, "-c", "mcp_servers.cxrun.command="+string(binary), "-c", "mcp_servers.cxrun.args="+string(argv), "-c", "mcp_servers.cxrun.tool_timeout_sec=150")
	}
	cmd := exec.Command("codex", args...)
	cmd.Dir = c.Dir

	log, err := os.OpenFile(filepath.Join(dir, "server.stderr.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	cmd.Stderr = log
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		in.Close()
		return nil, err
	}
	c.mark(cmd, dir)
	tree, err := startCommand(cmd, c.RunID)
	if err != nil {
		in.Close()
		out.Close()
		return nil, err
	}
	s := &server{tree: tree, writes: make(chan writeRequest, 16), expired: make(map[string]bool), writeTimeout: 10 * time.Second, cmd: cmd, in: in, frames: make(chan frame, 32), stop: make(chan struct{})}
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
func (r *runner) shutdown() (result error) {
	s := r.srv
	defer s.tree.cleanup()
	closing := !r.closing.IsZero()
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
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case err := <-done:
		return errors.Join(drainErr, err)
	case <-time.After(3 * time.Second):
	}
	if closing {
		closing = false
		if err := r.reclaim(); err != nil {
			return errors.Join(drainErr, err)
		}
	}
	_ = s.cmd.Process.Kill()
	select {
	case <-done:
		return errors.Join(drainErr, r.note("shutdown", "app-server exceeded EOF deadline; killed owned PID"))
	case <-time.After(5 * time.Second):
		return errors.Join(drainErr, errors.New("app-server did not exit after kill"))
	}
}

type writeRequest struct {
	data []byte
	done chan error
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
	w := writeRequest{append(b, '\n'), make(chan error, 1)}
	timer := time.NewTimer(s.writeTimeout)
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
		r.s.LastEvent = time.Now().UTC()
		r.dirty = true
		if len(m.ID) > 0 {
			return m, r.request(m)
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
	deadline := time.Now().Add(limit + r.c.Approval)
	timer := time.NewTimer(limit)
	defer timer.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-timer.C:
			if remaining := time.Until(deadline); remaining > 0 && len(r.s.Approvals) > 0 {
				timer.Reset(remaining)
				continue
			}
			r.srv.expired[strconv.Itoa(id)] = true
			return fmt.Errorf("%s timed out: %w", method, errRPCUncertain)
		case <-tick.C:
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
	claude     *claudeSession
	budget     *runtimeBudget
	ctx        context.Context
	c          config
	s          status
	root, dir  string
	slot       *os.File
	cacheLease *os.File

	events, log                   *os.File
	srv                           *server
	dirty                         bool
	lastText, failureReport       string
	turnFailure                   *pendingTurnFailure
	idleSince, turnSince, closing time.Time
	lastAction                    time.Time
	pending                       string               // a turn to start once the current one ended (stall recovery)
	cmdOpen                       map[string]time.Time // command items in progress
	follow                        *followAdmission
}

func (r *runner) save() error {
	r.dirty = false
	var approvals []approval
	for _, a := range r.s.Approvals {
		if approvalMethod(a.Method) {
			approvals = append(approvals, a)
		}
	}
	return writeJSON(filepath.Join(r.dir, "status.json"), struct {
		status
		Legacy []approval `json:"waiting_approval,omitempty"`
	}{r.s, approvals})
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
	r.s.ServerPID = 0
	r.s.Error = ""
	r.s.StallCount = 0
	r.s.LastAction = ""
	r.s.State = "running"
	r.s.Approvals = nil
	defer func() {
		r.s.PID = 0
		r.s.ServerPID = 0
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
		err = errors.Join(err, r.save())
	}()
	if err = r.save(); err != nil {
		return err
	}
	r.log, err = os.OpenFile(filepath.Join(r.dir, "progress.log"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer r.log.Close()
	defer func() {
		if err != nil {
			err = errors.Join(err, r.note("failed", err.Error()))
		} else {
			err = r.note("done", "agent exited")
		}
	}()
	r.events, err = os.OpenFile(filepath.Join(r.dir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer r.events.Close()
	return session()
}

func (r *runner) serveCodex() (err error) {
	r.srv, err = openServer(r.c, r.dir)
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
	r.s.ServerPID = r.srv.cmd.Process.Pid
	if err = r.save(); err != nil {
		return err
	}
	err = r.initializeThread()
	tick := time.NewTicker(250 * time.Millisecond)
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
			if !errors.Is(err, errServerClosed) || r.s.Restarts >= r.c.RestartLimit || !r.closing.IsZero() {
				return err
			}
			r.s.Restarts++
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

			r.lastAction = time.Now()
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
