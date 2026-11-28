package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/djylb/timeshim"
)

type markerOptions struct {
	Turn *string
	ID   string
}

type controlMarker struct {
	ID      string          `json:"id"`
	Turn    *string         `json:"turn_id"`
	Request json.RawMessage `json:"request_id,omitempty"`
}

func isMarker(op string) bool {
	return op == "close" || op == "interrupt" || op == "admit" || op == "approve" || op == "deny"
}

func targetTurn(s status) string {
	if s.TurnStatus == "inProgress" {
		return s.Turn
	}
	return ""
}

type closeConfirmation struct {
	id     jobIdentity
	status status
	cursor stateCursor
}

func (c *closeConfirmation) check(dir string, s status, pos stateCursor) error {
	if err := c.id.check(dir, s); err != nil {
		return err
	}
	if !s.final || c.cursor != pos {
		return errJobReplaced
	}
	return nil
}

func deliverClose(ctx context.Context, dir string, opt markerOptions, args []string) (*closeConfirmation, error) {
	id, s, start, err := captureJobIdentity(dir)
	if err != nil {
		return nil, err
	}
	if s.final {
		return &closeConfirmation{id, s, start}, nil
	}
	record, err := publishControlFor(ctx, dir, &id, "close", opt, args)
	if err != nil {
		return nil, err
	}
	tail, err := openStateTail(dir, start)
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	clock := timeshim.Or(contextClock(ctx))
	ctx, cancel := timeshim.WithTimeout(ctx, clock, 60*time.Second)
	defer cancel()
	var probe time.Time
	for {
		if err := id.checkDir(dir); err != nil {
			return nil, err
		}
		records, err := tail.Next()
		if err != nil {
			return nil, err
		}
		var withdrawn bool
		for _, item := range records {
			if item.Final {
				var terminal status
				if err := decodeJobStatus(item, &terminal); err != nil {
					return nil, err
				}
				if err := id.check(dir, terminal); err != nil {
					return nil, err
				}
				return &closeConfirmation{id, terminal, item.position}, nil
			}
			if item.Kind == "inbox_ack" {
				var ack inboxAck
				if err := json.Unmarshal(item.Value, &ack); err != nil {
					return nil, err
				}
				if ack.Seq == record.Seq && ack.ID == record.ID && ack.Result == "withdrawn" {
					withdrawn = true
				}
			}
		}
		if withdrawn {
			return nil, nil
		}
		if clock.Now().After(probe) {
			if _, err := projectJobStatus(ctx, dir, &s, id); err != nil {
				return nil, err
			}
			s, cursor, err := loadJobStatus(dir)
			if err != nil {
				return nil, err
			}
			if err := id.check(dir, s); err != nil {
				return nil, err
			}
			if s.final && cursor.Seq >= start.Seq {
				return &closeConfirmation{id, s, cursor}, nil
			}
			probe = clock.Now().Add(time.Second)
		}
		if err := timeshim.Sleep(ctx, clock, 50*time.Millisecond); err != nil {
			return nil, fmt.Errorf("close completion pending: %w", err)
		}
	}
}

func newMarkerID() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}

func publishMarker(dir, op string, opt markerOptions, args []string) error {
	_, err := publishControl(context.Background(), dir, op, opt, args)
	return err
}

var errAlreadyClosed = errors.New("job already closed")

func publishControl(ctx context.Context, dir, op string, opt markerOptions, args []string) (stateRecord, error) {
	return publishControlFor(ctx, dir, nil, op, opt, args)
}

func publishControlFor(ctx context.Context, dir string, expected *jobIdentity, op string, opt markerOptions, args []string) (stateRecord, error) {
	if !isMarker(op) {
		return stateRecord{}, fmt.Errorf("invalid control operation %q", op)
	}
	if opt.ID == "" {
		var err error
		opt.ID, err = newMarkerID()
		if err != nil {
			return stateRecord{}, err
		}
	}
	m := controlMarker{ID: opt.ID, Turn: opt.Turn}
	if len(args) > 0 {
		if len(args) != 1 || (op != "approve" && op != "deny") || !json.Valid([]byte(args[0])) {
			return stateRecord{}, errors.New("unexpected control arguments")
		}
		m.Request = json.RawMessage(args[0])
	}
	if m.Turn == nil {
		if turn, ok := os.LookupEnv("CXRUN_TURN_ID"); ok {
			m.Turn = &turn
		}
	}
	record, err := appendJobMailbox(ctx, dir, "inbox", expected, stateRecord{Kind: op, ID: m.ID}, func(record *stateRecord, previous stateRecord, s status) error {
		if previous.Seq != 0 {
			if previous.Kind != op {
				return fmt.Errorf("log ID %q already has a different record", m.ID)
			}
			var original controlMarker
			if err := json.Unmarshal(previous.Value, &original); err != nil {
				return err
			}
			if m.Turn == nil {
				m.Turn = original.Turn
			}
		} else {
			if s.Engine == "cmd" && op != "close" && op != "admit" {
				return errors.New("command only accepts close or admit")
			}
			if op == "admit" && s.State != "queued" {
				// A waiver left in the inbox would silently apply to a later admission.
				return errors.New("job is not queued")
			}
			if isTerminal(s.State) {
				if op == "close" {
					return errAlreadyClosed
				}
				return fmt.Errorf("job is %s", s.State)
			}
			if m.Turn == nil {
				turn := targetTurn(s)
				m.Turn = &turn
			}
		}
		var err error
		record.Value, err = json.Marshal(m)
		return err
	})
	if errors.Is(err, errAlreadyClosed) {
		err = nil
	}
	return record, err
}

func (r *runner) marker(record stateRecord) (controlMarker, bool, error) {
	var m controlMarker
	if err := json.Unmarshal(record.Value, &m); err != nil {
		return m, false, err
	}
	return m, m.Turn != nil && *m.Turn == targetTurn(r.s), nil
}

func (r *runner) takeMarker(op string) (controlMarker, bool, error) {
	if err := r.refreshInbox(); err != nil {
		return controlMarker{}, false, err
	}
	if op == "close" {
		// All runners check for close, including during admission. Publish
		// pending session changes before acting on that terminal control.
		if err := r.consumeAdoptions(); err != nil {
			return controlMarker{}, false, err
		}
	}
	for _, record := range r.inbox.queue {
		if record.Kind != op {
			continue
		}
		m, valid, err := r.marker(record)
		if err != nil {
			return m, false, err
		}
		if op == "close" && (valid || r.inbox.claims[record.Seq].Action == "close") {
			if r.closeRequest == 0 {
				if claimErr := r.claimInbox(record, "close"); claimErr != nil {
					return m, false, claimErr
				}
				r.closeRequest = record.Seq
			}
			return m, true, nil
		}
		result := "delivered"
		if !valid {
			result = "withdrawn"
		}
		if err = r.ackInbox(record, result); err != nil {
			return m, false, err
		}
		if valid {
			return m, true, nil
		}
	}
	return controlMarker{}, false, nil
}

func (r *runner) processInterrupt(record stateRecord) error {
	_, valid, err := r.marker(record)
	if err != nil {
		return err
	}
	if !valid {
		return r.ackInbox(record, "withdrawn")
	}
	if !r.active() {
		if targets := r.interruptTargets(record); len(targets) > 0 {
			if err := r.claimInbox(record, "withdraw"); err != nil {
				return err
			}
			return r.withdrawQueued(record, targets)
		}
	} else if err := r.claimInbox(record, "deliver"); err != nil {
		return err
	}
	if err := r.interrupt(); err != nil {
		return err
	}
	return r.ackInbox(record, "delivered")
}
