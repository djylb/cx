package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const agentAsk = "agent_ask"

type agentMail struct {
	Deadline   *time.Time `json:"deadline,omitempty"`
	Time       time.Time  `json:"time"`
	Type       string     `json:"type"`
	To         string     `json:"to"`
	Text       string     `json:"text,omitempty"`
	Question   string     `json:"question,omitempty"`
	Request    string     `json:"request_id,omitempty"`
	RunID      string     `json:"run_id"`
	Options    []string   `json:"options,omitempty"`
	Seq        uint64     `json:"seq"`
	Generation uint64     `json:"generation"`
	Blocking   bool       `json:"blocking,omitempty"`
}

func mailPath(dir string, seq uint64) string {
	return filepath.Join(dir, "outbox", "messages", fmt.Sprintf("%020d.json", seq))
}

// The append lock covers sequence allocation and atomic publication. The log itself is the counter.
func appendMail(ctx context.Context, dir string, m *agentMail) error {
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	if isTerminal(s.State) || s.Engine == "cmd" {
		return errors.New("messaging requires a live agent job")
	}
	folder := filepath.Dir(mailPath(dir, 0))
	if err := os.MkdirAll(folder, 0700); err != nil {
		return err
	}
	lock, err := lockUntil(ctx, filepath.Join(folder, "append.lock"))
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	// The runner's cursor is a lower bound; unpublished gaps cannot exist under the append lock.
	m.Seq = s.OutboxSeq + 1
	for {
		exists, err := fileExists(mailPath(dir, m.Seq))
		if err != nil {
			return err
		}
		if !exists {
			break
		}
		m.Seq++
	}
	m.Generation = s.Generation
	m.RunID = s.RunID
	m.Time = time.Now().UTC()
	if m.Type == "ask" {
		m.Request = fmt.Sprintf("ask-%d", m.Seq)
	}
	return writeJSON(mailPath(dir, m.Seq), m)
}

func readMail(dir string, since uint64, visit func(agentMail) error) error {
	for seq := since + 1; seq > 0; seq++ {
		var m agentMail
		err := readJSON(mailPath(dir, seq), &m)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if m.Seq != seq {
			return errors.New("invalid outbox sequence")
		}
		if err = visit(m); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) pollAgentMail() error {
	return readMail(r.dir, r.s.OutboxSeq, func(m agentMail) error {
		if m.Blocking && m.RunID == r.s.RunID && m.Generation == r.s.Generation && m.Deadline != nil && time.Now().Before(*m.Deadline) {
			id, _ := json.Marshal(m.Request)
			r.s.Approvals = append(r.s.Approvals, approval{Generation: m.Generation, ID: id, Method: agentAsk, Summary: m.Question, Requested: m.Time, Deadline: *m.Deadline})
			r.deriveState()
		}
		r.s.OutboxSeq = m.Seq
		if err := r.note("agent/"+m.Type, fmt.Sprintf("seq=%d %s", m.Seq, brief(m.Question))); err != nil {
			return err
		}
		return r.save()
	})
}

func answerPath(dir string, id json.RawMessage) (string, error) {
	var request string
	if err := json.Unmarshal(id, &request); err != nil {
		return "", err
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(request, "ask-"), 10, 64)
	if err != nil || request != fmt.Sprintf("ask-%d", n) {
		return "", errors.New("invalid agent request")
	}
	return filepath.Join(dir, "inbox", "done", request+".answer"), nil
}

// Include archived messages because the runner may already have steered the turn.
func agentInbox(dir string) (fields, error) {
	messages := fields{}
	for _, sub := range []string{"", "pending", "done", "uncertain"} {
		folder := filepath.Join(dir, "inbox", sub)
		entries, err := os.ReadDir(folder)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), ".msg") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(folder, e.Name()))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			messages[e.Name()] = string(b)
		}
	}
	return messages, nil
}
