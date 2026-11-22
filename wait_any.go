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
	"strconv"
	"time"
)

type waitCursor struct {
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
	Project     string `json:"project"`
	Job         string `json:"job"`
	State       string `json:"status"`
	QueueReason string `json:"queue_reason,omitempty"`
	Outcome     string `json:"outcome"`
	Event       string `json:"event"`
	EventID     string `json:"event_id"`
	OutboxSeq   uint64 `json:"outbox_seq,omitempty"`
	Finished    int64  `json:"completed_turns"`
	Code        int    `json:"code"`
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
	for id, before := range baseline {
		if _, err := parseCursor(before.StateCursor); err != nil {
			return nil, fmt.Errorf("wait baseline for %s: %w", id, err)
		}
	}
	return baseline, nil
}

func waitAny(ctx context.Context, root, project, session, sinceFile string, out io.Writer) error {
	var baseline map[string]waitCursor
	if sinceFile != "" {
		lock, err := lockUntil(ctx, sinceFile+".lock")
		if err != nil {
			return err
		}
		defer closeBestEffort(lock)
		b, err := readSharedFile(sinceFile)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read wait baseline: %w", err)
		}
		baseline, err = decodeWaitBaseline(b)
		if err != nil {
			return err
		}
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	report, err := selectWaitAny(ctx, root, project, session, baseline, tick.C, json.NewEncoder(out).Encode)
	if err != nil {
		return err
	}
	if sinceFile != "" {
		if err = json.NewEncoder(out).Encode(report); err != nil {
			return err
		}
		if err = writeJSON(sinceFile, baseline); err != nil {
			return fmt.Errorf("write wait baseline: %w", err)
		}
	}
	if report.Code == 6 {
		return errQuestion
	}
	if report.Code == exitQueued {
		return ctlExit(exitQueued)
	}
	return nil
}

func pollWaitAny(ctx context.Context, root, project, session string, baseline map[string]waitCursor) (*waitReport, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	return pollWaitAnyTicks(ctx, root, project, session, baseline, tick.C)
}

func pollWaitAnyTicks(ctx context.Context, root, project, session string, baseline map[string]waitCursor, ticks <-chan time.Time) (*waitReport, error) {
	return selectWaitAny(ctx, root, project, session, baseline, ticks, nil)
}

func selectWaitAny(ctx context.Context, root, project, session string, baseline map[string]waitCursor, ticks <-chan time.Time, output func(any) error) (*waitReport, error) {
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
		err := walkJobDirs(ctx, root, project, func(dir, p, j string) (bool, bool, error) {
			id := j
			if p != "" {
				id = p + "/" + j
			}
			key, err := jobDirectoryKey(dir)
			if errors.Is(err, os.ErrNotExist) {
				return false, false, nil
			}
			if err != nil {
				return false, false, err
			}
			before := baseline[id]
			job := jobs[dir]
			if job != nil && key != job.key {
				closeBestEffort(job.jobStateObserver)
				closeBestEffort(job.mail)
				delete(jobs, dir)
				job = nil
			}
			if job == nil {
				var from stateCursor
				if before.StateCursor != "" {
					from, err = parseCursor(before.StateCursor)
					if err != nil {
						return false, false, err
					}
				}
				observer, openErr := openJobObservation(dir, from, before.RunID)
				if openErr != nil {
					return false, false, openErr
				}
				if baseline != nil && (before.RunID == "" || before.RunID == observer.s.RunID) {
					observer.floor = &status{RunID: observer.s.RunID, Finished: before.Finished}
					observer.watchWithdrawals = before.StateCursor != ""
					observer.deliveredWithdrawals = before.Withdrawals
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
			if (project != "" && p != project) || (session != "" && job.s.Session != session) {
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
				job.floor = waitFloor{s.RunID, s.Finished, busy}
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
				before = waitCursor{}
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
	blocked := floor.busy && s.Finished <= floor.finished
	if s.State == "waiting_input" && (!blocked || s.InputKind != "next_turn") {
		key := questionKey(s)
		if key != before.Question {
			next.Question = key
			report.Event, report.EventID, report.Code = "question", key, 6
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
		report.Code, _ = outcomeResult("", selected.Outcome)
		return report, next
	}
	if s.final {
		key := fmt.Sprintf("%s/%d", s.State, s.Restarts)
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

func replacedRun(before, now string) bool {
	return before != "" && now != "" && before != now
}

var errQuestion = errors.New("waiting for input")

func questionKey(s status) string {
	key := s.RunID + "/" + s.InputKind + "/" + s.LastCompleted
	if s.InputKind == "server_request" || s.InputKind == agentAsk {
		for _, a := range s.Approvals {
			if inputMethod(a.Method) {
				key += fmt.Sprintf("/%d/%s/%d", a.Generation, a.ID, a.Requested.UnixNano())
				break
			}
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
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
