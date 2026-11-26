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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

func loadJobStatus(dir string) (s status, cursor stateCursor, err error) {
	record, cursor, err := jobStatusRecord(dir)
	if err == nil {
		err = decodeJobStatus(record, &s)
	}
	return s, cursor, err
}

func loadJobStatusInto(dir string, dst *status) error {
	_, err := readJobStatus(dir, dst)
	return err
}

func readSharedFile(path string) ([]byte, error) {
	f, err := openJobFile(path, jobFileRead)
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(f)
	return io.ReadAll(f)
}

type jobStateObserver struct {
	tail                 *stateTail
	pending              map[uint64]string
	floor                *status
	selected             *turnCompletion
	withdrawal           *inboxAck
	identity             jobIdentity
	dir                  string
	deliveredWithdrawals []uint64
	s                    status
	cursor               stateCursor
	withdrawn            bool
	watchWithdrawals     bool
}

func (o *jobStateObserver) Close() error { return o.tail.Close() }

// Seed the view once. A reconnect can still consume the tail from its saved
// position. The seed prevents intermediate records from regressing the view.
func openJobObservation(dir string, from stateCursor, runID string) (*jobStateObserver, error) {
	identity, s, cur, err := captureJobIdentity(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if from == (stateCursor{}) || runID != "" && runID != s.RunID || runID == "" && from.Seg > cur.Seg {
		from = cur
	}
	tail, err := openStateTail(dir, from)
	if err != nil {
		return nil, err
	}
	return &jobStateObserver{dir: dir, s: s, cursor: cur, tail: tail, identity: identity}, nil
}

func (o *jobStateObserver) read() error {
	identity := o.identity
	if identity.key == "" {
		key, err := jobDirectoryKey(o.dir)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		identity.key = key
	} else if err := identity.checkDir(o.dir); err != nil {
		return err
	}
	records, err := o.tail.Next()
	if err != nil {
		return err
	}
	if err := identity.checkDir(o.dir); err != nil {
		return err
	}
	for _, record := range records {
		if record.Kind != "inbox_ack" || len(o.pending) == 0 && !o.watchWithdrawals {
			continue
		}
		var ack inboxAck
		if err := json.Unmarshal(record.Value, &ack); err != nil {
			return err
		}
		if id := o.pending[ack.Seq]; id != "" && id == ack.ID {
			if ack.Result != "withdrawn" {
				o.pending = nil
			} else {
				delete(o.pending, ack.Seq)
				o.withdrawn = len(o.pending) == 0
				if o.withdrawn && !o.watchWithdrawals {
					o.withdrawal = &ack
				}
			}
		}
		if o.watchWithdrawals && ack.Result == "withdrawn" && !slices.Contains(o.deliveredWithdrawals, ack.Seq) &&
			(o.withdrawal == nil || ack.Seq < o.withdrawal.Seq) {
			inbox := filepath.Join(o.dir, "inbox")
			message, err := findMailboxID(inbox, filepath.Join(inbox, ".ids", snapSum([]byte(ack.ID))), ack.ID)
			if err != nil {
				return err
			}
			if message.Kind == "msg" && message.Seq == ack.Seq {
				o.withdrawal = &ack
			}
		}
	}
	cur := o.tail.Cursor()
	if cur.Seq < o.cursor.Seq {
		// Latest can seed a verified status past damage in a saved segment.
		// Resume at that anchor when the tail could not reach it.
		tail, err := openStateTail(o.dir, o.cursor)
		if err != nil {
			return err
		}
		closeBestEffort(o.tail)
		o.tail = tail
		return nil
	}
	for _, record := range records {
		var completion turnCompletion
		switch record.Kind {
		case "status":
			var s status
			if err := decodeJobStatus(record, &s); err != nil {
				return err
			}
			if record.Seq > o.cursor.Seq {
				if o.identity.key != "" && o.identity.run != s.RunID {
					return errJobReplaced
				}
				identity.run = s.RunID
				o.s, o.identity = s, identity
			}
			completion = completedTurn(s)
		case "turn_completion":
			if err := json.Unmarshal(record.Value, &completion); err != nil {
				return err
			}
		default:
			continue
		}
		if o.floor != nil && completion.RunID == o.floor.RunID && completion.Number > o.floor.Finished &&
			completion.TurnID != "" && (o.selected == nil || completion.Number < o.selected.Number ||
			completion.Number == o.selected.Number && completion.TurnID == o.selected.TurnID) {
			selected := completion
			o.selected = &selected
		}
	}
	o.cursor = cur
	return nil
}

func (o *jobStateObserver) poll(ctx context.Context) error {
	if err := o.read(); err != nil {
		return err
	}
	return o.project(ctx)
}

func (o *jobStateObserver) project(ctx context.Context) error {
	refreshed, err := projectJobStatus(ctx, o.dir, &o.s, o.identity)
	if err != nil || !refreshed {
		return err
	}
	return o.read()
}

// waitStart observes a job once to select the floor for this invocation.
func waitStart(observer *jobStateObserver) (bool, error) {
	s := observer.s
	if s.Engine == "cmd" || isTerminal(s.State) {
		return false, nil
	}
	if s.TurnStatus == "inProgress" || s.State == "queued" && s.Finished == 0 {
		return true, nil
	}
	pending, err := pendingInboxMessages(observer.dir)
	if err != nil {
		return false, err
	}
	if len(pending) != 0 {
		observer.pending = make(map[uint64]string, len(pending))
		for _, message := range pending {
			observer.pending[message.Seq] = message.ID
		}
		return true, nil
	}
	// The inbox can be drained between the status read and the scan.
	// Delivery publishes the new turn (or its completion) before acknowledging.
	err = observer.read()
	now := observer.s
	busy := !isTerminal(now.State) && (now.TurnStatus == "inProgress" || now.State == "queued")
	return busy, err
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

func progressRecord(line string) (kind, detail string) {
	fields := strings.SplitN(line, "\t", 4)
	if len(fields) != 4 {
		return "", ""
	}
	if _, err := strconv.ParseUint(fields[0], 10, 64); err != nil {
		return "", ""
	}
	return fields[2], strings.TrimRight(fields[3], "\r\n")
}

func openObservedFile(dir, name string, identity *jobIdentity) (*os.File, error) {
	f, err := openJobFile(filepath.Join(dir, name), jobFileRead)
	if identity != nil {
		if replacement := identity.checkDir(dir); replacement != nil {
			if f != nil {
				closeBestEffort(f)
			}
			return nil, replacement
		}
	}
	return f, err
}

func observe(ctx context.Context, op, dir string) error {
	if op == "wait" {
		_, err := waitJob(ctx, dir, true)
		return err
	}
	observer, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		return err
	}
	defer closeBestEffort(observer)
	if err = observer.poll(ctx); err != nil {
		return err
	}
	log, err := openObservedFile(dir, observer.s.logName(), &observer.identity)
	if err != nil {
		return err
	}
	defer closeBestEffort(log)
	for {
		if _, err = io.Copy(os.Stdout, log); err != nil {
			return err
		}
		if observer.s.final {
			return nil
		}
		if err = pause(ctx, 500*time.Millisecond); err != nil {
			return err
		}
		if err = observer.poll(ctx); err != nil {
			return err
		}
	}
}

type observedResult struct {
	RunID  string `json:"run_id"`
	TurnID string `json:"turn_id"`
	Number int64  `json:"turn"`
}

// The status describes the job at the end of the observation. Result identifies
// the selected completion, which can precede that status's latest completion.
type waitObservation struct {
	Result    *observedResult `json:"result,omitempty"`
	EventID   string          `json:"event_id,omitempty"`
	NoticeKey string          `json:"notice_key,omitempty"`
	status
}

func waitJob(ctx context.Context, dir string, acknowledge bool) (waitObservation, error) {
	clock := timeshim.Or(contextClock(ctx))
	observer, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		return waitObservation{}, err
	}
	defer closeBestEffort(observer)
	for {
		err = observer.poll(ctx)
		if err == nil {
			break
		}
		s := observer.s
		if !errors.Is(err, os.ErrNotExist) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return waitObservation{}, waitTimeout(s, dir)
			}
			return waitObservation{}, err
		}
		if err = timeshim.Sleep(ctx, clock, 200*time.Millisecond); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return waitObservation{}, waitTimeout(s, dir)
			}
			return waitObservation{}, err
		}
	}
	busy, err := waitStart(observer)
	s := observer.s
	if err != nil {
		return waitObservation{}, err
	}
	initial := s
	observer.floor = &initial
	for {
		if replacedRun(initial.RunID, s.RunID) {
			return waitObservation{}, errors.New("job generation changed during wait")
		}
		if observer.withdrawn {
			fmt.Printf("job=%s status=%s outcome=interrupted\n", s.Job, s.State)
			return waitObservation{status: s}, outcomeError(s.State, "interrupted")
		}
		advanced := s.Finished > initial.Finished
		if !advanced && s.State == "queued" && s.QueueReason != "" {
			observation := waitObservation{status: s, EventID: queueEventID(s)}
			if !acknowledge {
				observation.NoticeKey = observer.identity.key
			}
			fresh, noticeErr := deliverNotice(ctx, dir, observer.identity, observation.EventID, acknowledge, func() error {
				return json.NewEncoder(os.Stdout).Encode(observation)
			})
			if noticeErr != nil {
				return waitObservation{}, noticeErr
			}
			if fresh {
				return observation, ctlExit(exitQueued)
			}
		}
		terminal := s.final
		question := s.State == "waiting_input"
		if question && (!busy || advanced || s.InputKind != "next_turn") {
			observation := waitObservation{status: s, EventID: questionKey(s)}
			if !acknowledge {
				observation.NoticeKey = observer.identity.key
			}
			fresh, claimErr := deliverNotice(ctx, dir, observer.identity, observation.EventID, acknowledge, func() error {
				if !acknowledge {
					return json.NewEncoder(os.Stdout).Encode(observation)
				}
				_, printErr := fmt.Printf("job=%s status=waiting_input input_kind=%s\n", s.Job, s.InputKind)
				return printErr
			})
			if claimErr != nil {
				return waitObservation{}, claimErr
			}
			if fresh {
				return observation, errQuestion
			}
		}
		if terminal || advanced || (!question && !busy && (s.State == "waiting" || s.State != "queued" && s.Finished > 0)) {
			observation := waitResult(initial, s, busy, observer.selected)
			s = observation.status
			if s.Engine == "cmd" || terminalCodes[s.State] != 0 {
				fmt.Printf("job=%s status=%s", s.Job, s.State)
				if s.ExitCode != nil {
					fmt.Printf(" exit_code=%d", *s.ExitCode)
				}
				fmt.Println()
				return observation, outcomeError(s.State, s.CompletedStatus)
			}
			fmt.Printf("job=%s status=%s completed_turn=%s outcome=%s\n", s.Job, s.State, s.LastCompleted, s.CompletedStatus)
			return observation, outcomeError(s.State, s.CompletedStatus)
		}
		if err = timeshim.Sleep(ctx, clock, 200*time.Millisecond); err == nil {
			err = observer.poll(ctx)
			s = observer.s
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			if s.Job == "" {
				s = initial
			}
			return waitObservation{}, waitTimeout(s, dir)
		}
		if err != nil {
			return waitObservation{}, err
		}
	}
}

func waitResult(initial, s status, busy bool, selected *turnCompletion) waitObservation {
	if busy && s.final && s.Finished <= initial.Finished {
		s.LastCompleted = ""
		if s.CompletedStatus == initial.CompletedStatus {
			s.CompletedStatus = ""
		}
		return waitObservation{status: s}
	}
	result := observedResult{RunID: s.RunID, Number: s.Finished, TurnID: s.LastCompleted}
	if busy && selected != nil {
		result = selected.observedResult
		s.LastCompleted, s.CompletedStatus = selected.TurnID, selected.Outcome
	}
	observation := waitObservation{status: s}
	if result.RunID != "" && (result.TurnID != "" || result.Number == 0 && readyResult(s)) {
		observation.Result = &result
	}
	return observation
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
	ctx, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), limit)
	return ctx, cancel, nil
}

func control(ctx context.Context, op, dir string, args []string) error {
	if op != "say" && op != "log" && len(args) != 0 {
		return fmt.Errorf("%s: unexpected arguments", op)
	}
	switch op {
	case "status":
		var s status
		err := projected(ctx, dir, &s)
		if errors.Is(err, os.ErrNotExist) {
			row := map[string]string{"job": filepath.Base(dir), "status": "not_registered"}
			if encodeErr := json.NewEncoder(os.Stdout).Encode(row); encodeErr != nil {
				return encodeErr
			}
			return errNotRegistered
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(s)
	case "wait", "tail":
		waitCtx, cancel, err := monitorContext(ctx, 0)
		if err != nil {
			return err
		}
		defer cancel()
		return observe(waitCtx, op, dir)
	case "log":
		return logFrom(dir, args)
	case "say":
	default:
		return fmt.Errorf("unknown operation %q", op)
	}

	var name string
	var b []byte
	if len(args) != 1 {
		return errors.New("say requires text or @file")
	}
	if strings.HasPrefix(args[0], "@") {
		var err error
		b, err = readSharedFile(args[0][1:])
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
		if !msgName.MatchString(v) {
			return errors.New("invalid CXRUN_MSG_NAME")
		}
		name = v
	}
	if err := publishMessage(ctx, dir, name, inboxMessage{Text: string(b)}); err != nil {
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
	if err := loadJobStatusInto(dir, &s); err != nil {
		return err
	}
	return writeJobLog(out, dir, off, s, nil)
}

func writeJobLog(out io.Writer, dir string, off int64, s status, identity *jobIdentity) error {
	f, err := openObservedFile(dir, s.logName(), identity)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	if _, err = f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	if s.Engine == "cmd" {
		if _, err = io.Copy(out, f); err != nil {
			return err
		}
		if s.final {
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
	if s.final && end == len(b) {
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
	if err = loadJobStatusInto(dir, &s); err != nil {
		return err
	}
	return writeJobProgress(out, dir, since, s, nil)
}

func writeJobProgress(out io.Writer, dir string, since uint64, s status, identity *jobIdentity) error {
	f, err := openObservedFile(dir, "progress.log", identity)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
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
			if len(line) == 0 && s.final {
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
