package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

const adoptLimit = 30 * time.Second
const adoptKind = "adopt"

func ctlAdopt(ctx context.Context, h endpoint, id, session string) error {
	// An interrupted call may have reached the job. Avoid unbound replay
	// across a replacement at the same job address.
	r, err := h.invoke(ctx, adoptLimit+2*time.Second, false, "adopt-job", id, session)
	if err != nil {
		return err
	}
	return r.output()
}

func adoptJob(ctx context.Context, root, dir, session string) error {
	if strings.TrimSpace(session) == "" {
		return errors.New("adopt requires a session")
	}
	id, _, start, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	op, err := newMarkerID()
	if err != nil {
		return err
	}
	request, err := newRecord(adoptKind, session)
	if err != nil {
		return err
	}
	request.ID = op
	tail, err := openStateTail(dir, start)
	if err != nil {
		return err
	}
	defer closeBestEffort(tail)

	clock := timeshim.Or(contextClock(ctx))
	ctx, cancel := timeshim.WithTimeout(ctx, clock, adoptLimit)
	defer cancel()
	for ctx.Err() == nil {
		if err := id.checkDir(dir); err != nil {
			return err
		}
		if request.Seq != 0 {
			records, readErr := tail.Next()
			if readErr != nil {
				return readErr
			}
			for _, item := range records {
				if item.Kind != "inbox_ack" {
					continue
				}
				var ack inboxAck
				if err := json.Unmarshal(item.Value, &ack); err != nil {
					return err
				}
				if ack.Seq == request.Seq && ack.ID == request.ID {
					return adoptAckResult(ack)
				}
			}
		}
		handled, adoptErr := adoptWithOwner(ctx, root, dir, id, request, session)
		if handled || adoptErr != nil {
			return adoptErr
		}
		if request.Seq == 0 {
			published, publishErr := appendJobMailbox(ctx, dir, "inbox", &id, request, nil)
			if publishErr != nil && !errors.Is(publishErr, errAlreadyClosed) {
				return publishErr
			}
			if publishErr == nil {
				request = published
			}
		}
		if timeshim.Sleep(ctx, clock, 50*time.Millisecond) != nil {
			break
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errObserveTimeout
	}
	return ctx.Err()
}

func adoptAckResult(ack inboxAck) error {
	if ack.Result != "delivered" {
		return fmt.Errorf("adoption %s", ack.Result)
	}
	return nil
}

func adoptWithOwner(ctx context.Context, root, dir string, id jobIdentity, request stateRecord, session string) (bool, error) {
	free, err := probeJobOwner(dir)
	if err != nil || !free {
		return false, err
	}
	guard, err := registryLock(ctx, root)
	if err != nil {
		return false, err
	}
	defer closeBestEffort(guard)
	owner, err := openJobOwner(ctx, dir, jobFileLockExisting)
	if err != nil || owner == nil {
		return false, err
	}
	defer closeBestEffort(owner)
	// The runner may have published its final snapshot while the call
	// attempted to acquire ownership. Only use the state read under the lock.
	s, _, err := loadJobStatus(dir)
	if err != nil {
		return false, err
	}
	if identityErr := id.check(dir, s); identityErr != nil {
		return false, identityErr
	}
	if closeErr := guard.Close(); closeErr != nil {
		return false, closeErr
	}

	if s.final {
		if request.Seq != 0 {
			in, readErr := readInboxState(dir)
			if readErr != nil {
				return true, readErr
			}
			if ack := in.acks[request.Seq]; ack.ID == request.ID {
				return true, adoptAckResult(ack)
			}
			return true, errors.New("job ended without an adoption receipt")
		}
		if s.Session == session {
			return true, nil
		}
	}
	writer, err := openStateLog(dir)
	if err != nil {
		return true, err
	}
	defer closeBestEffort(writer)
	if recoveryErr := writer.RecoveryError(); recoveryErr != nil {
		return true, recoveryErr
	}
	if historyErr := writer.syncHistory(); historyErr != nil {
		return true, historyErr
	}
	if s.final {
		s.Session = session
		return true, writer.Final(s)
	}
	r := runner{root: root, dir: dir, s: s, stateLog: writer, ctx: ctx}
	if err = r.refreshInbox(); err != nil {
		return true, err
	}
	defer closeBestEffort(r.inbox.tail)
	if err = r.consumeAdoptions(); err != nil {
		return true, err
	}
	if request.Seq != 0 {
		ack := r.inbox.acks[request.Seq]
		if ack.ID != request.ID {
			return true, errors.New("adoption request lost from inbox")
		}
		return true, adoptAckResult(ack)
	}
	return true, r.saveAdoption(session)
}

func (r *runner) consumeAdoptions() error {
	for _, record := range r.inbox.queue {
		if record.Kind != adoptKind {
			continue
		}
		if err := r.syncInboxThrough(record.position); err != nil {
			return err
		}
		var session string
		if err := json.Unmarshal(record.Value, &session); err != nil {
			return err
		}
		if err := r.saveAdoption(session); err != nil {
			return err
		}
		if err := r.ackInbox(record, "delivered"); err != nil {
			return err
		}
		if err := r.stateLog.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) saveAdoption(session string) error {
	if r.s.Session == session {
		return nil
	}
	r.s.Session = session
	if err := r.save(); err != nil {
		return err
	}
	return r.stateLog.sync()
}
