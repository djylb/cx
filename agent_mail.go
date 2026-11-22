package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

const agentAsk = "agent_ask"
const outboxSeenKind = "outbox_seen"
const outboxRequestKind = "outbox_request"

type pendingAgentAsk struct {
	Request    string `json:"request_id"`
	Generation uint64 `json:"generation"`
}

type agentMail struct {
	Deadline   *time.Time    `json:"deadline,omitempty"`
	Time       time.Time     `json:"time"`
	Type       string        `json:"type"`
	To         string        `json:"to"`
	Text       string        `json:"text,omitempty"`
	Question   string        `json:"question,omitempty"`
	Request    string        `json:"request_id,omitempty"`
	RunID      string        `json:"run_id"`
	Options    []string      `json:"options,omitempty"`
	Timeout    time.Duration `json:"timeout_ns,omitempty"`
	Seq        uint64        `json:"seq"`
	Generation uint64        `json:"generation"`
	Blocking   bool          `json:"blocking,omitempty"`
}

func appendMail(ctx context.Context, dir string, identity jobIdentity, operation string, m *agentMail) error {
	_, err := appendJobMailbox(ctx, dir, "outbox", &identity, stateRecord{Kind: m.Type, ID: operation},
		func(record *stateRecord, previous stateRecord, s status) error {
			if s.Engine == "cmd" {
				return errors.New("messaging requires an agent job")
			}
			if previous.Seq != 0 {
				var original agentMail
				if err := json.Unmarshal(previous.Value, &original); err != nil {
					return err
				}
				m.Seq, m.Time, m.Deadline = original.Seq, original.Time, original.Deadline
				m.Request, m.Generation, m.RunID = original.Request, original.Generation, original.RunID
			} else {
				m.Seq, m.Time = record.Seq, record.Time
				m.Generation, m.RunID = s.Generation, s.RunID
				if m.Type == "ask" {
					m.Request = fmt.Sprintf("ask-%d", m.Seq)
				}
				if m.Blocking {
					deadline := m.Time.Add(m.Timeout)
					m.Deadline = &deadline
				}
			}
			var err error
			// Outbox retries compare payload bytes. Preserve the existing field
			// order with timeout_ns first in the on-disk JSON.
			type mailFields agentMail
			wire := struct {
				Timeout *time.Duration `json:"timeout_ns,omitempty"`
				*mailFields
			}{mailFields: (*mailFields)(m)}
			if m.Timeout != 0 {
				wire.Timeout = &m.Timeout
			}
			record.Value, err = json.Marshal(wire)
			return err
		})
	return err
}

func openOutboxTail(dir string, from stateCursor) (*stateTail, error) {
	return openMailboxTail(filepath.Join(dir, "outbox"), from)
}

func readMail(dir string, since uint64, visit func(agentMail) error) error {
	tail, err := openOutboxTail(dir, stateCursor{})
	if err != nil {
		return err
	}
	defer closeBestEffort(tail)
	return readMailTail(tail, since, visit)
}

func readMailTail(tail *stateTail, since uint64, visit func(agentMail) error) error {
	records, err := tail.Next()
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Seq <= since {
			continue
		}
		var m agentMail
		if err := json.Unmarshal(record.Value, &m); err != nil {
			return fmt.Errorf("%w: invalid outbox record %d: %w", errLogDamage, record.Seq, err)
		}
		if m.Seq != record.Seq || m.Type != record.Kind {
			return fmt.Errorf("%w: invalid outbox record %d", errLogDamage, record.Seq)
		}
		if err := visit(m); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) pollAgentMail() error {
	if r.inbox == nil {
		if err := r.refreshInbox(); err != nil {
			return err
		}
	}
	in := r.inbox
	if in.outboxTail == nil {
		tail, err := openOutboxTail(r.dir, in.outbox)
		if err != nil {
			return err
		}
		in.outboxTail = tail
	}
	var arrived []agentMail
	if err := readMailTail(in.outboxTail, in.outbox.Seq, func(m agentMail) error {
		if m.RunID != r.s.RunID {
			return errJobReplaced
		}
		arrived = append(arrived, m)
		return nil
	}); err != nil || len(arrived) == 0 {
		return err
	}
	at := in.outboxTail.Cursor()
	// The checkpoint can outlive the current owner. Persist the source
	// frames before publishing a durable state that can skip them.
	if err := syncMailboxPrefix(filepath.Join(r.dir, "outbox"), in.outbox, at); err != nil {
		return err
	}
	for _, m := range arrived {
		if m.Blocking && !in.answered[m.Request] {
			ask := pendingAgentAsk{Request: m.Request, Generation: m.Generation}
			if err := r.stateLog.Append(outboxRequestKind, ask); err != nil {
				return err
			}
			in.pendingAsks[m.Request] = ask
			if m.Generation == r.s.Generation && m.Deadline != nil && time.Now().Before(*m.Deadline) {
				id, _ := json.Marshal(m.Request)
				r.s.Approvals = append(r.s.Approvals, approval{Generation: m.Generation, ID: id, Method: agentAsk, Summary: m.Question, Requested: m.Time, Deadline: *m.Deadline})
				r.deriveState()
			}
		}
		if err := r.note("agent/"+m.Type, fmt.Sprintf("seq=%d %s", m.Seq, brief(m.Question))); err != nil {
			return err
		}
		if err := r.save(); err != nil {
			return err
		}
	}
	if err := r.stateLog.Append(outboxSeenKind, at); err != nil {
		return err
	}
	in.outbox = at
	return nil
}

func waitAgentAnswer(ctx context.Context, dir string, identity jobIdentity, request string) (fields, error) {
	receipt, err := waitReceipt(ctx, dir, identity, func(record receiptRecord) bool {
		return record.AgentReply != nil && record.AgentReply.Request == request
	})
	if err != nil {
		return nil, err
	}
	return fields{"answer": receipt.AgentReply.Answer, "error": receipt.AgentReply.Error}, nil
}

// Expose bodies only after delivery is acknowledged. Queued messages can
// still be withdrawn and must never be visible to the agent.
func agentInbox(dir string) (fields, error) {
	tail, err := inboxTail(dir, stateCursor{})
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	records, err := tail.Next()
	if err != nil {
		return nil, err
	}
	in, err := readInboxState(dir)
	if err != nil {
		return nil, err
	}
	messages := fields{}
	for _, record := range records {
		ack := in.acks[record.Seq]
		if record.Kind != "msg" || ack.ID != record.ID || ack.Result != "delivered" {
			continue
		}
		var msg inboxMessage
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			return nil, err
		}
		if len(msg.Input) > 0 {
			body, err := json.Marshal(msg.Input)
			if err != nil {
				return nil, err
			}
			msg.Text = string(body)
		}
		messages[record.ID] = msg.Text
	}
	return messages, nil
}
