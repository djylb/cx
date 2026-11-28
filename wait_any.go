package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type waitCursor struct {
	DirKey       string   `json:"dir_key"`
	EventID      string   `json:"event_id,omitempty"`
	StateCursor  string   `json:"state_cursor"`
	OutboxCursor string   `json:"outbox_cursor,omitempty"`
	Question     string   `json:"question,omitempty"`
	RunID        string   `json:"run_id,omitempty"`
	Terminal     string   `json:"terminal,omitempty"`
	Withdrawals  []uint64 `json:"withdrawals,omitempty"`

	// Notification acknowledgements can lag the state tail while a job is
	// busy. OutboxSeq belongs to the independent outbox stream.
	OutboxSeq       uint64 `json:"outbox_seq,omitempty"`
	Finished        int64  `json:"completed_turns"`
	QueueGeneration uint64 `json:"queue_generation,omitempty"`
}

type waitReport struct {
	ResetAt     *int64 `json:"reset_at"`
	Host        string `json:"host,omitempty"`
	Project     string `json:"project"`
	Job         string `json:"job"`
	State       string `json:"status"`
	QueueReason string `json:"queue_reason,omitempty"`
	Outcome     string `json:"outcome"`
	Event       string `json:"event"`
	EventID     string `json:"event_id"`
	Next        string `json:"next"`
	OutboxSeq   uint64 `json:"outbox_seq,omitempty"`
	Finished    int64  `json:"completed_turns"`
	Turn        int64  `json:"turn"`
	Code        int    `json:"code"`
	RateLimited bool   `json:"rate_limited"`
}

type waitPollRequest struct {
	Cursor    map[string]waitCursor
	Session   string
	Projects  []string
	Jobs      []string
	Limit     time.Duration
	FromStart bool
}

type waitPollResponse struct {
	Row    *waitReport
	Cursor json.RawMessage
}

const exitQueued = 13

type waitFloor struct {
	runID    string
	finished int64
	busy     bool
}

func decodeWaitBaseline(b []byte) (map[string]waitCursor, error) {
	baseline := make(map[string]waitCursor)
	if len(bytes.TrimSpace(b)) > 0 {
		if err := json.Unmarshal(b, &baseline); err != nil {
			return nil, fmt.Errorf("read wait baseline: %w", err)
		}
	}
	if baseline == nil {
		return nil, errors.New("wait baseline must be a JSON object")
	}
	return baseline, validateWaitBaseline(baseline)
}

func validateWaitBaseline(baseline map[string]waitCursor) error {
	for id, before := range baseline {
		// An empty state cursor means the job had no readable state log when the
		// baseline was taken; polling treats it as "no position" and resumes.
		if before.StateCursor == "" {
			continue
		}
		if _, err := parseCursor(before.StateCursor); err != nil {
			return fmt.Errorf("wait baseline for %s: %w", id, err)
		}
		if before.OutboxCursor != "" {
			if _, err := parseCursor(before.OutboxCursor); err != nil {
				return fmt.Errorf("wait outbox baseline for %s: %w", id, err)
			}
		}
	}
	return nil
}

func validateWaitProjects(projects []string) error {
	for _, p := range projects {
		if _, err := jobDir("", p+"/wait"); err != nil {
			return fmt.Errorf("invalid project %q", p)
		}
	}
	return nil
}

func validateWaitJob(id string) error {
	if !strings.Contains(id, "/") {
		return fmt.Errorf("expected PROJECT/JOB, got %q", id)
	}
	_, err := jobDir("", id)
	return err
}

func readWaitCursorFile(path string) ([]byte, error) {
	b, err := readSharedFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// Compare, deliver and advance under the same lock. A stale candidate must
// be selected again from the current cursor before it can be delivered.
func updateWaitCursor(ctx context.Context, path string, before, next []byte, output func() error) (current []byte, advanced bool, err error) {
	lock, err := lockUntil(ctx, path+".update")
	if err != nil {
		return nil, false, err
	}
	defer closeBestEffort(lock)
	current, err = readWaitCursorFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("write wait baseline: %w", err)
	}
	if (current == nil) != (before == nil) || !bytes.Equal(current, before) {
		return current, false, nil
	}
	if err = ctx.Err(); err != nil {
		return nil, false, err
	}
	if output != nil {
		if err = output(); err != nil {
			return nil, false, err
		}
	}
	if err = atomicWrite(path, next); err != nil {
		return nil, false, fmt.Errorf("write wait baseline: %w", err)
	}
	return next, true, nil
}

// Serialize metadata publication, inspection and removal with the lock probe.
func registerWaitObserver(ctx context.Context, path, label, alternative string) (func(), error) {
	guard, err := lockUntil(ctx, path+".register")
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(guard)

	observer, err := lockPath(path, os.O_CREATE)
	if err != nil {
		return nil, err
	}
	record := path + ".owner"
	if observer == nil {
		owner, readErr := readSharedFile(record)
		if readErr != nil {
			diagnosticf("cx wait --any: another process is waiting on %s (cannot read observer record: %v). Stop it or %s.\n", label, readErr, alternative)
		} else {
			diagnosticf("cx wait --any: another process is waiting on %s (%s). Stop it by PID or %s.\n", label, owner, alternative)
		}
		return nil, ctlExit(2)
	}

	started := timeshim.Or(contextClock(ctx)).Now().UTC()
	owner := fmt.Appendf(nil, "PID %d, started %s", os.Getpid(), started.Format(time.RFC3339Nano))
	if err = atomicWrite(record, owner); err != nil {
		closeBestEffort(observer)
		return nil, fmt.Errorf("write wait observer for %s: %w", label, err)
	}
	return func() {
		cleanupGuard, cleanupErr := lockUntil(context.Background(), path+".register")
		if cleanupErr == nil {
			defer closeBestEffort(cleanupGuard)
			_ = os.Remove(record)
		}
		closeBestEffort(observer)
	}, nil
}

func waitJobID(project, job string) string {
	if project != "" {
		return project + "/" + job
	}
	return job
}

func (r *waitReport) setNext(host string) {
	if r.Turn > 0 {
		r.Next = fmt.Sprintf("cx result %s:%s/%s --turn %d", host, r.Project, r.Job, r.Turn)
	}
}

func initialWaitBaseline(ctx context.Context, root, session string, fromStart bool) (map[string]waitCursor, error) {
	for ctx.Err() == nil {
		baseline, err := seedWaitBaseline(ctx, root, session, fromStart)
		if !errors.Is(err, errJobReplaced) {
			return baseline, err
		}
		// A replacement during the scan invalidates the initial snapshot.
	}
	return nil, ctx.Err()
}

func seedWaitBaseline(ctx context.Context, root, session string, fromStart bool) (map[string]waitCursor, error) {
	baseline := make(map[string]waitCursor)
	err := walkJobDirs(ctx, root, nil, func(dir, project, job string) (bool, bool, error) {
		observer, err := openJobObservation(dir, stateCursor{}, "")
		if err != nil {
			return false, false, err
		}
		defer closeBestEffort(observer)
		s := observer.s
		if s.State == "" {
			return false, false, nil
		}
		if session != "" && s.Session != session {
			return true, false, nil
		}
		id := waitJobID(project, job)
		if fromStart {
			baseline[id] = waitCursor{DirKey: observer.identity.key, RunID: s.RunID, StateCursor: (stateCursor{}).String()}
			return true, false, nil
		}
		if err = observer.poll(ctx); errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		} else if err != nil {
			return true, false, err
		}
		s = observer.s
		if session != "" && s.Session != session {
			return true, false, nil
		}
		cursor := waitCursor{DirKey: observer.identity.key, RunID: s.RunID, StateCursor: observer.cursor.String(), Finished: completedResultCount(s), QueueGeneration: s.QueueGeneration}
		if s.State == "waiting_input" {
			cursor.Question = questionKey(s)
		}
		if s.final {
			cursor.Terminal = waitTerminalKey(s)
		}
		if s.Engine != "cmd" {
			tail, tailErr := openOutboxTail(dir, stateCursor{})
			if tailErr != nil {
				return true, false, tailErr
			}
			defer closeBestEffort(tail)
			tailErr = readMailTail(tail, 0, func(m agentMail) error {
				if m.RunID != s.RunID {
					return errJobReplaced
				}
				cursor.OutboxSeq = m.Seq
				return nil
			})
			if errors.Is(tailErr, errJobReplaced) {
				return true, false, nil
			}
			if tailErr != nil && (!s.final || !errors.Is(tailErr, errLogDamage)) {
				return true, false, tailErr
			}
			cursor.OutboxCursor = tail.Cursor().String()
		}
		if err = observer.identity.checkDir(dir); err != nil {
			return true, false, err
		}
		baseline[id] = cursor
		return true, false, nil
	})
	return baseline, err
}

func waitAny(ctx context.Context, root string, projects []string, session, sinceFile string, fromStart bool, out io.Writer) (err error) {
	defer func() {
		if errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = errObserveTimeout
		}
	}()
	if sinceFile != "" {
		release, lockErr := registerWaitObserver(ctx, sinceFile+".lock", fmt.Sprintf("cursor file %q", sinceFile),
			"use --since-file PATH for an independent cursor")
		if lockErr != nil {
			return lockErr
		}
		defer release()
	}
	emit := func(row *waitReport) error {
		row.setNext("local")
		return json.NewEncoder(out).Encode(row)
	}
	baseline, saved, err := loadWaitBaseline(ctx, root, session, sinceFile, fromStart)
	if err != nil {
		return err
	}
	tick := timeshim.NewTicker(timeshim.Or(contextClock(ctx)), time.Second)
	defer tick.Stop()
	for {
		report, err := selectWaitAny(ctx, root, projects, session, baseline, tick.C, emit)
		if err != nil {
			return err
		}
		if sinceFile != "" {
			next, marshalErr := json.Marshal(baseline)
			if marshalErr != nil {
				return marshalErr
			}
			var advanced bool
			_, advanced, err = updateWaitCursor(ctx, sinceFile, saved, next, func() error { return emit(report) })
			if err != nil {
				return err
			}
			if !advanced {
				baseline, saved, err = loadWaitBaseline(ctx, root, session, sinceFile, fromStart)
				if err != nil {
					return err
				}
				continue
			}
		} else if baseline != nil {
			if err = emit(report); err != nil {
				return err
			}
		}
		switch report.Code {
		case 6:
			return errQuestion
		case exitQueued:
			return ctlExit(exitQueued)
		}
		return nil
	}
}

func loadWaitBaseline(ctx context.Context, root, session, sinceFile string, fromStart bool) (map[string]waitCursor, []byte, error) {
	var saved []byte
	if sinceFile != "" {
		var err error
		saved, err = readWaitCursorFile(sinceFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read wait baseline: %w", err)
		}
	}
	if saved == nil && (sinceFile != "" || fromStart) {
		baseline, err := initialWaitBaseline(ctx, root, session, fromStart)
		if err != nil || sinceFile == "" {
			return baseline, nil, err
		}
		b, err := json.Marshal(baseline)
		if err != nil {
			return nil, nil, err
		}
		saved, _, err = updateWaitCursor(ctx, sinceFile, nil, b, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	if saved == nil {
		return nil, nil, nil
	}
	baseline, err := decodeWaitBaseline(saved)
	return baseline, saved, err
}

func pollWaitAny(ctx context.Context, root string, projects []string, session string, baseline map[string]waitCursor, onlyJobs ...string) (*waitReport, error) {
	tick := timeshim.NewTicker(timeshim.Or(contextClock(ctx)), time.Second)
	defer tick.Stop()
	return selectWaitAny(ctx, root, projects, session, baseline, tick.C, nil, onlyJobs...)
}

func selectWaitAny(ctx context.Context, root string, projects []string, session string, baseline map[string]waitCursor, ticks <-chan time.Time, output func(*waitReport) error, onlyJobs ...string) (*waitReport, error) {
	// A floor belongs to this observation call. A job that loses the race
	// to another result must retain its original persisted cursor.
	type observedJob struct {
		*jobStateObserver
		mail    *stateTail
		key     string
		floor   waitFloor
		anchor  stateCursor
		seen    uint64
		outbox  uint64
		started bool
	}
	jobs := make(map[string]*observedJob)
	defer func() {
		for _, job := range jobs {
			closeBestEffort(job.jobStateObserver)
			closeBestEffort(job.mail)
		}
	}()
	for scan := uint64(1); ctx.Err() == nil; scan++ {
		var result *waitReport
		err := walkJobDirs(ctx, root, projects, func(dir, p, j string) (bool, bool, error) {
			id := waitJobID(p, j)
			if len(onlyJobs) > 0 && !slices.Contains(onlyJobs, id) {
				return false, false, nil
			}
			key, err := jobDirectoryKey(dir)
			if errors.Is(err, os.ErrNotExist) {
				return false, false, nil
			}
			if err != nil {
				return false, false, err
			}
			before := baseline[id]
			if before.DirKey != key {
				before = waitCursor{DirKey: key}
			}
			job := jobs[dir]
			if job != nil && key != job.key {
				closeBestEffort(job.jobStateObserver)
				closeBestEffort(job.mail)
				delete(jobs, dir)
				job = nil
			}
			if job == nil {
				var from *stateCursor
				if before.StateCursor != "" {
					cursor, parseErr := parseCursor(before.StateCursor)
					if parseErr != nil {
						return false, false, parseErr
					}
					from = &cursor
				}
				observer, openErr := openJobObservationFrom(dir, from, before.RunID)
				if openErr != nil {
					return false, false, openErr
				}
				if observer.identity.key != key {
					closeBestEffort(observer)
					return false, false, nil
				}
				if baseline != nil {
					// Snapshots after a seed can still carry earlier completions.
					finished := max(completedResultCount(observer.s), before.Finished)
					if !replacedRun(before.RunID, observer.s.RunID) && from != nil && *from == observer.tail.Cursor() {
						finished = before.Finished
						observer.watchWithdrawals = true
						observer.deliveredWithdrawals = before.Withdrawals
					}
					observer.floor = &status{RunID: observer.s.RunID, Finished: finished}
				}
				job = &observedJob{jobStateObserver: observer, key: key, anchor: observer.tail.Cursor()}
				jobs[dir] = job
			}
			job.seen = scan
			if readErr := job.read(); readErr != nil {
				return false, false, readErr
			}
			if job.s.State == "" {
				return false, false, nil
			}
			if len(projects) > 0 && p == "" || session != "" && job.s.Session != session {
				return true, false, nil
			}
			if projectErr := job.project(ctx); errors.Is(projectErr, os.ErrNotExist) {
				return false, false, nil
			} else if projectErr != nil {
				return true, false, projectErr
			}
			s := job.s
			if !job.started || replacedRun(job.floor.runID, s.RunID) {
				busy, startErr := waitStart(job.jobStateObserver)
				if errors.Is(startErr, os.ErrNotExist) {
					return false, false, nil
				}
				if startErr != nil {
					return true, false, startErr
				}
				s = job.s
				job.floor = waitFloor{s.RunID, completedResultCount(s), busy}
				job.started = true
				job.outbox = 0
				var from stateCursor
				if before.RunID == s.RunID && before.OutboxCursor != "" {
					var cursorErr error
					from, cursorErr = parseCursor(before.OutboxCursor)
					if cursorErr != nil {
						return true, false, cursorErr
					}
				}
				if s.Engine != "cmd" {
					closeBestEffort(job.mail)
					var tailErr error
					job.mail, tailErr = openOutboxTail(dir, from)
					if tailErr != nil {
						return true, false, tailErr
					}
				}
			}
			if replacedRun(before.RunID, s.RunID) {
				// Mail, queue and input acknowledgements belong to the run.
				// Completion and terminal acknowledgements belong to the job.
				before = waitCursor{DirKey: key, Finished: before.Finished, Terminal: before.Terminal, EventID: before.EventID}
			}
			job.outbox = max(job.outbox, before.OutboxSeq)
			if job.mail != nil {
				err = readMailTail(job.mail, job.outbox, func(m agentMail) error {
					if m.RunID != s.RunID {
						return errJobReplaced
					}
					job.outbox = m.Seq
					return nil
				})
				if err != nil {
					if !s.final || !errors.Is(err, errLogDamage) {
						return true, false, err
					}
					// A published job-level failure remains observable even when
					// the damaged mailbox cannot establish an outbox event.
					job.outbox = before.OutboxSeq
				}
			}
			if err := job.identity.checkDir(dir); err != nil {
				return true, false, err
			}
			for {
				report, next := waitEvent(s, before, job.anchor, job.cursor, job.outbox, job.floor, job.withdrawal, job.selected)
				if report == nil {
					return true, false, nil
				}
				if baseline != nil {
					if report.Event == "outbox" {
						next.OutboxCursor = job.mail.Cursor().String()
					}
					baseline[id] = next
				} else if output != nil {
					if report.Event == "queue" || report.Event == "question" || report.Event == "outbox" {
						fresh, err := deliverNotice(ctx, dir, job.identity, report.EventID, true, func() error { return output(report) })
						if err != nil {
							return true, false, err
						}
						if !fresh {
							before = next
							if report.Event == "question" && s.InputKind == "next_turn" {
								// The report asking for input already represents this turn for
								// consumers without a separate completion cursor.
								before.Finished = max(before.Finished, s.Finished)
							}
							continue
						}
					} else if err := output(report); err != nil {
						return true, false, err
					}
				}
				result = report
				return true, true, nil
			}
		})
		if err != nil {
			if ctx.Err() == nil {
				return nil, err
			}
			break
		}
		if result != nil {
			return result, nil
		}
		for dir, job := range jobs {
			if job.seen != scan {
				closeBestEffort(job.jobStateObserver)
				closeBestEffort(job.mail)
				delete(jobs, dir)
			}
		}
		select {
		case <-ctx.Done():
		case <-ticks:
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, errObserveTimeout
	}
	return nil, ctx.Err()
}

func waitEvent(s status, before waitCursor, anchor, cursor stateCursor, outbox uint64, floor waitFloor, withdrawal *inboxAck, selected *turnCompletion) (*waitReport, waitCursor) {
	next := before
	next.RunID = s.RunID
	next.StateCursor = anchor.String()
	report := &waitReport{Project: s.Project, Job: s.Job, State: s.State, Finished: s.Finished}
	switch {
	case withdrawal != nil:
		next.Withdrawals = append(next.Withdrawals, withdrawal.Seq)
		report.Event = "withdrawal"
		report.State = "withdrawn"
		report.EventID = waitEventID(s.RunID, report.Event, withdrawal.ID)
		report.Code, report.Outcome, report.Finished = 1, "interrupted", before.Finished
		return report, next
	case s.State == "queued" && s.QueueReason != "" && s.QueueGeneration != before.QueueGeneration:
		next.QueueGeneration = s.QueueGeneration
		report.Event = "queue"
		report.EventID = queueEventID(s)
		report.Code, report.QueueReason = exitQueued, s.QueueReason
		return report, next
	}
	blocked := floor.busy && completedResultCount(s) <= floor.finished
	if s.State == "waiting_input" && (!blocked || s.InputKind != "next_turn") {
		key := questionKey(s)
		if key != before.Question {
			next.Question = key
			report.Event, report.EventID, report.Code = "question", key, 6
			if s.InputKind == "next_turn" && s.LastCompleted != "" {
				report.Turn = s.Finished
			}
			return report, next
		}
	}
	if s.Engine == "cmd" {
		// The command's completion and process exit are one terminal event.
		selected = nil
	} else if selected == nil && !blocked && s.Finished > before.Finished && (s.LastCompleted != "" || s.Engine == "") {
		completion := completedTurn(s)
		selected = &completion
	}
	if selected != nil {
		next.Finished = selected.Number
		if selected.Number == s.Finished {
			next.StateCursor = cursor.String()
		}
		report.Event = "completion"
		if s.State == "queued" || s.final {
			report.State = "completed"
		}
		report.EventID = waitEventID(s.RunID, report.Event, fmt.Sprintf("%d/%s", selected.Number, selected.TurnID))
		report.Finished, report.Outcome = selected.Number, selected.Outcome
		report.Turn, report.RateLimited = selected.Number, selected.RateLimited
		if report.RateLimited {
			report.ResetAt = selected.ResetAt
		}
		report.Code, _ = outcomeResult("", selected.Outcome)
		return report, next
	}
	if s.final {
		key := waitTerminalKey(s)
		// Closing after the already observed turns carries no new result.
		quietClose := s.Engine != "cmd" && s.State == "done" && s.Finished > 0 && s.Finished <= max(before.Finished, floor.finished)
		if key != before.Terminal && !quietClose {
			next.Terminal = key
			next.StateCursor = cursor.String()
			report.Event = "terminal"
			report.EventID = waitEventID(s.RunID, report.Event, key)
			if s.Engine == "cmd" {
				next.Finished = s.Finished
				report.Outcome = s.CompletedStatus
				if s.LastCompleted != "" {
					report.Turn = s.Finished
				}
			}
			report.Code, _ = outcomeResult(s.State, report.Outcome)
			return report, next
		}
	}
	if outbox > before.OutboxSeq {
		next.OutboxSeq = outbox
		report.Event = "outbox"
		if s.State == "queued" {
			report.State = "outbox"
		}
		report.EventID = waitEventID(s.RunID, report.Event, strconv.FormatUint(outbox, 10))
		report.OutboxSeq = outbox
		return report, next
	}
	return nil, before
}

func waitTerminalKey(s status) string {
	return fmt.Sprintf("%s/%d", s.State, s.Restarts)
}

func replacedRun(before, now string) bool {
	return before != "" && now != "" && before != now
}

var errQuestion = errors.New("waiting for input")

func questionKey(s status) string {
	var key strings.Builder
	key.WriteString(s.RunID + "/" + s.InputKind + "/" + s.LastCompleted)
	if s.InputKind == "server_request" || s.InputKind == agentAsk {
		for _, a := range s.Approvals {
			if inputMethod(a.Method) {
				_, _ = fmt.Fprintf(&key, "/%d/%s/%d", a.Generation, a.ID, a.Requested.UnixNano())
				break
			}
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(key.String())))
}
func waitEventID(runID, kind, identity string) string {
	return fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%q/%q/%q", runID, kind, identity)))
}

func queueEventID(s status) string {
	return waitEventID(s.RunID, "queue", strconv.FormatUint(s.QueueGeneration, 10))
}

func confirmWaitNotice(ctx context.Context, dir string, identity jobIdentity, eventID string) error {
	if _, err := hex.DecodeString(eventID); len(eventID) != sha256.Size*2 || err != nil {
		return errors.New("invalid wait event identity")
	}
	_, err := deliverNotice(ctx, dir, identity, eventID, true, func() error { return nil })
	return err
}

// Serialize emitters outside the job directory. A failed output leaves the
// notification available. Check identity under the inbox lock before looking
// at the marker and before writing it, since detach takes that same lock.
func deliverNotice(ctx context.Context, dir string, identity jobIdentity, eventID string, acknowledge bool, output func() error) (bool, error) {
	guard, err := lockUntil(ctx, mailboxLockPath(dir, "notices"))
	if err != nil {
		return false, err
	}
	defer closeBestEffort(guard)
	folder := filepath.Join(dir, ".notices")
	path := filepath.Join(folder, eventID)
	underInbox := func(fn func() error) error {
		inbox, lockErr := lockMailbox(ctx, mailboxLockPath(dir, "inbox"))
		if lockErr != nil {
			return lockErr
		}
		defer closeBestEffort(inbox)
		s, _, statusErr := loadJobStatus(dir)
		if statusErr != nil {
			return statusErr
		}
		if identityErr := identity.check(dir, s); identityErr != nil {
			return identityErr
		}
		return fn()
	}
	var exists bool
	err = underInbox(func() error {
		_, statErr := os.Lstat(path)
		exists = statErr == nil
		if os.IsNotExist(statErr) {
			return nil
		}
		return statErr
	})
	if err != nil || exists {
		return false, err
	}
	if outputErr := output(); outputErr != nil {
		return false, outputErr
	}
	if !acknowledge {
		return true, nil
	}
	err = underInbox(func() error {
		if mkdirErr := os.Mkdir(folder, 0700); mkdirErr != nil && !os.IsExist(mkdirErr) {
			return mkdirErr
		}
		if writeErr := writeJobBytes(path, nil); writeErr != nil {
			return writeErr
		}
		return syncJobTree(folder, dir)
	})
	return true, err
}
