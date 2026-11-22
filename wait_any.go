package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type waitCursor struct {
	Question  string `json:"question,omitempty"`
	RunID     string `json:"run_id,omitempty"`
	Terminal  string `json:"terminal,omitempty"`
	OutboxSeq uint64 `json:"outbox_seq,omitempty"`
	Finished  int64  `json:"completed_turns"`
}

type waitReport struct {
	Project   string `json:"project"`
	Job       string `json:"job"`
	State     string `json:"status"`
	Outcome   string `json:"outcome"`
	OutboxSeq uint64 `json:"outbox_seq,omitempty"`
	Finished  int64  `json:"completed_turns"`
	Code      int    `json:"code"`
}

func waitAny(ctx context.Context, root, project, session, sinceFile string, out io.Writer) error {
	baseline := make(map[string]waitCursor)
	if sinceFile != "" {
		b, err := readStateFile(sinceFile)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("read wait baseline: %w", err)
		}
		if len(bytes.TrimSpace(b)) > 0 {
			if err = json.Unmarshal(b, &baseline); err != nil {
				return fmt.Errorf("read wait baseline: %w", err)
			}
		}
		if baseline == nil {
			return errors.New("wait baseline must be a JSON object")
		}
	}
	// These floors apply only to this call. Losing a poll to another job
	// must not advance that job's persisted cursor.
	type start struct {
		runID   string
		outcome string
		floor   int64
		busy    bool
	}
	starts := make(map[string]start)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for ctx.Err() == nil {
		var result *waitReport
		var key string
		var cursor waitCursor
		err := scanJobs(ctx, root, project, session, func(s status) (bool, error) {
			id := s.Job
			if s.Project != "" {
				id = s.Project + "/" + id
			}
			dir := filepath.Join(root, s.Project, s.Job)
			target, seen := starts[id]
			if !seen || replacedRun(target.runID, s.RunID) {
				var busy bool
				var err error
				s, busy, err = waitStart(dir, s)
				if os.IsNotExist(err) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				target = start{runID: s.RunID, floor: s.Finished, busy: busy, outcome: s.CompletedStatus}
				starts[id] = target
			}
			before := baseline[id]
			if replacedRun(before.RunID, s.RunID) {
				before = waitCursor{}
			}
			now := waitCursor{OutboxSeq: s.OutboxSeq, RunID: s.RunID, Finished: before.Finished}
			if isTerminal(s.State) {
				now.Terminal = s.State
			}
			terminal := now.Terminal != "" && now.Terminal != before.Terminal
			blocked := target.busy && s.Finished <= target.floor
			// An explicit close of a job whose turns were already reported (or predate this wait) ends it
			// without new information: not an event. Failure-like terminal states still wake the waiter.
			if terminal && now.Terminal == "done" && s.Finished > 0 && s.Finished <= max(before.Finished, target.floor) {
				terminal = false
			}
			question := s.State == "waiting_input" && (!blocked || s.InputKind != "next_turn")
			mail := now.OutboxSeq > before.OutboxSeq
			if mail && sinceFile == "" {
				var err error
				mail, err = claimNotice(dir, fmt.Sprintf("outbox-%d", now.OutboxSeq))
				if err != nil {
					return false, err
				}
			}
			if question {
				now.Question = questionKey(s)
				if sinceFile == "" {
					fresh, err := claimQuestion(dir, s)
					if err != nil || (!fresh && !mail) {
						return false, err
					}
				} else if now.Question == before.Question && !mail {
					return false, nil
				}
			}
			completion := !blocked && s.Finished > before.Finished
			if !mail && !question && !completion && !terminal {
				return false, nil
			}
			if completion {
				now.Finished = s.Finished
			}
			outcome := s.CompletedStatus
			if blocked && (!terminal || outcome == target.outcome) {
				outcome = ""
			}
			code := 0
			if completion || terminal {
				code, _ = outcomeResult(s.State, outcome)
			}
			if question {
				code = 6
			}
			result = &waitReport{OutboxSeq: s.OutboxSeq, Project: s.Project, Job: s.Job, State: s.State, Outcome: outcome, Finished: s.Finished, Code: code}
			key, cursor = id, now
			return true, nil
		})
		if err != nil {
			if ctx.Err() == nil {
				return err
			}
			break
		}
		if result != nil {
			if err := json.NewEncoder(out).Encode(result); err != nil {
				return err
			}
			if sinceFile != "" {
				baseline[key] = cursor
				if err := writeJSON(sinceFile, baseline); err != nil {
					return fmt.Errorf("write wait baseline: %w", err)
				}
			}
			if result.Code == 6 {
				return errQuestion
			}
			return nil
		}
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errObserveTimeout
	}
	return ctx.Err()
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
func claimQuestion(dir string, s status) (bool, error) {
	return claimNotice(dir, "question-"+questionKey(s))
}
func claimNotice(dir, name string) (bool, error) {
	f, err := os.OpenFile(filepath.Join(dir, "inbox", "done", name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, f.Close()
}
