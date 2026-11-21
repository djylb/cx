package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// pendingInput reports input that will start a new turn: any undelivered inbox message, or a message taken
// for delivery after the last recorded event (a steer delivered during an earlier turn can linger in
// pending/ and must not make an idle job look busy).
func pendingInput(dir string, since time.Time) (bool, error) {
	for _, sub := range []string{"inbox", "inbox/pending"} {
		entries, err := os.ReadDir(filepath.Join(dir, sub))
		if err != nil {
			return false, err
		}
		for _, e := range entries {
			if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), ".msg") {
				continue
			}
			if sub == "inbox" {
				return true, nil
			}
			if info, err := e.Info(); err == nil && info.ModTime().After(since) {
				return true, nil
			}
		}
	}
	return false, nil
}

// waitStart observes a job once to select the floor for this invocation.
func waitStart(dir string, s status) (status, bool, error) {
	if s.Engine == "cmd" || isTerminal(s.State) {
		return s, false, nil
	}
	if s.TurnStatus == "inProgress" || s.State == "queued" {
		return s, true, nil
	}
	pending, err := pendingInput(dir, s.LastEvent)
	if pending || err != nil {
		return s, pending, err
	}
	// The inbox can be drained between the status read and the scan.
	// Delivery publishes the new turn (or its completion) before archiving.
	var now status
	err = readJSON(filepath.Join(dir, "status.json"), &now)
	busy := !isTerminal(now.State) && (now.TurnStatus == "inProgress" || now.State == "queued")
	return now, busy, err
}

func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// A single 30-second observation interval can contain multiple complete turns.
// finish writes this same completion record for live events and restored history.
func waitOutcome(dir string, before, now status) (id, outcome string, err error) {
	if now.Finished <= before.Finished+1 {
		return now.LastCompleted, now.CompletedStatus, nil
	}
	f, err := os.Open(filepath.Join(dir, "progress.log"))
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	after := before.LastCompleted == ""
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		line := lines.Text()
		first, rest, _ := strings.Cut(line, "\t")
		if _, e := strconv.ParseUint(first, 10, 64); e == nil {
			line = rest
		}
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 || parts[1] != "turn/completed" {
			continue
		}
		turn := strings.Fields(parts[2])
		if len(turn) < 2 {
			continue
		}
		if turn[0] == before.LastCompleted {
			after = true
			continue
		}
		if after && (before.TurnStatus != "inProgress" || before.Turn == turn[0]) {
			return turn[0], turn[1], nil
		}
	}
	if err = lines.Err(); err != nil {
		return "", "", err
	}
	if before.TurnStatus == "inProgress" {
		// Recovery can replace the original turn without recording its completion.
		return now.LastCompleted, now.CompletedStatus, nil
	}
	return "", "", errors.New("completion record missing for wait target")
}

func observe(ctx context.Context, op, dir string) error {
	if op == "wait" {
		_, err := waitJob(ctx, dir)
		return err
	}
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	log, err := os.Open(filepath.Join(dir, s.logName()))
	if err != nil {
		return err
	}
	defer log.Close()
	for {
		if err = projected(ctx, dir, &s); err != nil {
			return err
		}
		if _, err = io.Copy(os.Stdout, log); err != nil {
			return err
		}
		if isTerminal(s.State) {
			return nil
		}
		if err = pause(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
}

func waitJob(ctx context.Context, dir string) (status, error) {
	var s status
	for {
		err := projected(ctx, dir, &s)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return s, waitTimeout(s, dir)
			}
			return s, err
		}
		if err = pause(ctx, 200*time.Millisecond); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return s, waitTimeout(s, dir)
			}
			return s, err
		}
	}
	initial, busy, err := waitStart(dir, s)
	if err != nil {
		return s, err
	}
	s = initial
	for {
		advanced := s.Finished > initial.Finished
		terminal := isTerminal(s.State)
		question := s.State == "waiting_input"
		if question && (!busy || advanced || s.InputKind != "next_turn") {
			fresh, err := claimQuestion(dir, s)
			if err != nil {
				return s, err
			}
			if fresh {
				fmt.Printf("job=%s status=waiting_input input_kind=%s\n", s.Job, s.InputKind)
				return s, errQuestion
			}
		}
		if terminal || advanced || (!question && !busy && (s.State == "waiting" || s.Finished > 0)) {
			if busy && terminal && !advanced {
				s.LastCompleted = ""
				if s.CompletedStatus == initial.CompletedStatus {
					s.CompletedStatus = ""
				}
			}
			if s.Engine == "cmd" || terminalCodes[s.State] != 0 {
				fmt.Printf("job=%s status=%s", s.Job, s.State)
				if s.ExitCode != nil {
					fmt.Printf(" exit_code=%d", *s.ExitCode)
				}
				fmt.Println()
				return s, outcomeError(s.State, s.CompletedStatus)
			}
			if busy && advanced {
				s.LastCompleted, s.CompletedStatus, err = waitOutcome(dir, initial, s)
				if err != nil {
					return s, err
				}
			}
			fmt.Printf("job=%s status=%s completed_turn=%s outcome=%s\n", s.Job, s.State, s.LastCompleted, s.CompletedStatus)
			return s, outcomeError(s.State, s.CompletedStatus)
		}
		if err = pause(ctx, 200*time.Millisecond); err == nil {
			s = status{}
			err = projected(ctx, dir, &s)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			if s.Job == "" {
				s = initial
			}
			return s, waitTimeout(s, dir)
		}
		if err != nil {
			return s, err
		}
	}
}

func waitTimeout(s status, dir string) error {
	if s.Job == "" {
		fmt.Printf("job=%s status=not_registered wait_timeout=true\n", filepath.Base(dir))
	} else {
		fmt.Printf("job=%s status=%s wait_timeout=true\n", s.Job, s.State)
	}
	return errors.Join(context.DeadlineExceeded, errObserveTimeout)
}

func monitorContext(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc, error) {
	if limit == 0 {
		limit = defaultMonitorTimeout
		if raw := os.Getenv("CXRUN_MONITOR_TIMEOUT"); raw != "" {
			var err error
			limit, err = time.ParseDuration(raw)
			if err != nil || limit <= 0 {
				return nil, nil, errors.New("invalid CXRUN_MONITOR_TIMEOUT")
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	return ctx, cancel, nil
}

func control(ctx context.Context, op, dir string, args []string) error {
	if op != "say" && op != "log" && len(args) != 0 {
		return fmt.Errorf("%s: unexpected arguments", op)
	}
	switch op {
	case "status":
		var s status
		if err := projected(ctx, dir, &s); err != nil {
			var pathErr *os.PathError
			if errors.Is(err, os.ErrNotExist) && errors.As(err, &pathErr) &&
				pathErr.Path == filepath.Join(dir, "status.json") {
				row := map[string]string{"job": filepath.Base(dir), "status": "not_registered"}
				if err := json.NewEncoder(os.Stdout).Encode(row); err != nil {
					return err
				}
				return errNotRegistered
			}
			return err
		}
		return printStatus(dir)
	case "wait", "tail":
		ctx, cancel, err := monitorContext(ctx, 0)
		if err != nil {
			return err
		}
		defer cancel()
		return observe(ctx, op, dir)
	case "log":
		return logFrom(dir, args)
	case "close", "interrupt", "approve", "deny":
		return publishMarker(dir, op, markerOptions{}, args)
	case "say":
	default:
		return fmt.Errorf("unknown operation %q", op)
	}

	// A retried delivery (lost reply) names the message itself: an already known name is a success.
	if v := os.Getenv("CXRUN_MSG_NAME"); v != "" {
		if !msgName.MatchString(v) {
			return errors.New("invalid CXRUN_MSG_NAME")
		}
		for _, sub := range []string{"", "pending", "done", "uncertain"} {
			if ok, err := fileExists(filepath.Join(dir, "inbox", sub, v)); err != nil {
				return err
			} else if ok {
				fmt.Println(v)
				return nil
			}
		}
	}
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	if s.Engine == "cmd" {
		return fmt.Errorf("%s is unsupported for command jobs", op)
	}
	if isTerminal(s.State) {
		return fmt.Errorf("job is %s; resume it first", s.State)
	}
	var name string
	var b []byte
	if len(args) != 1 {
		return errors.New("say requires text or @file")
	}
	if strings.HasPrefix(args[0], "@") {
		var err error
		b, err = os.ReadFile(args[0][1:])
		if err != nil {
			return err
		}
	} else {
		b = []byte(args[0])
	}
	if strings.TrimSpace(string(b)) == "" {
		return errors.New("empty message")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	name = fmt.Sprintf("%019d-%s.msg", time.Now().UnixNano(), hex.EncodeToString(nonce[:]))
	if v := os.Getenv("CXRUN_MSG_NAME"); v != "" {
		name = v
	}
	if err := atomicWrite(filepath.Join(dir, "inbox", name), b); err != nil {
		return err
	}
	fmt.Println(name)
	return nil
}

var msgName = regexp.MustCompile(`^[0-9]{19}-[0-9a-f]{16}\.msg$`)

// errDrained: the job reached a terminal state and logFrom printed everything; main maps it to exit code 10.
var errDrained = errors.New("log drained")

// logFrom prints the complete lines of progress.log from a byte offset (args[0], default 0), so that a remote
// monitor can resume after a dropped connection. The state is read before the log: when it is terminal and the log
// ends with a newline, nothing more will come (exit code 10).
func logFrom(dir string, args []string) error { return writeLogFrom(os.Stdout, dir, args) }
func writeLogFrom(out io.Writer, dir string, args []string) error {
	var off int64
	if len(args) > 1 {
		return errors.New("log: one optional offset")
	}
	if len(args) == 1 {
		n, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil || n < 0 {
			return errors.New("log: invalid offset")
		}
		off = n
	}
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(dir, s.logName()))
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	if s.Engine == "cmd" {
		if _, err = io.Copy(out, f); err != nil {
			return err
		}
		if isTerminal(s.State) {
			return errDrained
		}
		return nil
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	end := bytes.LastIndexByte(b, '\n') + 1
	if _, err = out.Write(b[:end]); err != nil {
		return err
	}
	if isTerminal(s.State) && end == len(b) {
		return errDrained
	}
	return nil
}

func (s status) logName() string {
	if s.Engine == "cmd" {
		return "output.log"
	}
	return "progress.log"
}

func logSince(dir, raw string) error { return writeLogSince(os.Stdout, dir, raw) }
func writeLogSince(out io.Writer, dir, raw string) error {
	since, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return err
	}
	var s status
	if err = readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(dir, "progress.log"))
	if err != nil {
		return err
	}
	defer f.Close()
	// seq is the one-based byte position of a record. A positive cursor
	// points into the last excluded line, including for non-record cursors.
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	pos := fi.Size()
	if since > 0 && since <= uint64(pos) {
		pos = int64(since - 1)
	}
	if since > 0 {
		if _, err = f.Seek(pos, io.SeekStart); err != nil {
			return err
		}
	}
	r := bufio.NewReader(f)
	if since > 0 && pos < fi.Size() {
		if _, err = r.ReadString('\n'); err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			return nil
		}
	}
	for {
		line, e := r.ReadString('\n')
		if e == io.EOF {
			if len(line) == 0 && isTerminal(s.State) {
				return errDrained
			}
			return nil
		}
		if e != nil {
			return e
		}
		first, _, _ := strings.Cut(line, "\t")
		seq, e := strconv.ParseUint(first, 10, 64)
		if e == nil && seq > since {
			if _, e = io.WriteString(out, line); e != nil {
				return e
			}
		}
	}
}
