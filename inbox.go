package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"time"
)

func inputMethod(m string) bool {
	return m == agentAsk || m == "item/tool/requestUserInput" || m == "mcpServer/elicitation/request"
}
func approvalMethod(m string) bool {
	switch m {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "execCommandApproval", "applyPatchApproval":
		return true
	}
	return false
}

type approval struct {
	Requested time.Time `json:"requested_at"`
	Deadline  time.Time `json:"deadline"`
	Summary   string    `json:"summary"`
	Method    string    `json:"method"`
	Reason    string    `json:"reason,omitempty"`
	Questions []struct {
		ID string `json:"id"`
	} `json:"questions,omitempty"`
	ID          json.RawMessage `json:"id"`
	Permissions json.RawMessage `json:"permissions,omitempty"`
	Generation  uint64          `json:"generation"`
}

func (r *runner) request(m message, capturedParams json.RawMessage) error {
	provider := m.Method == "item/tool/call" || m.Method == "attestation/generate" || m.Method == "account/chatgptAuthTokens/refresh"
	switch {
	case provider || inputMethod(m.Method) || approvalMethod(m.Method):
		var p struct {
			Reason      string
			Permissions json.RawMessage
		}
		if err := json.Unmarshal(m.Params, &p); err != nil {
			return err
		}
		now := time.Now().UTC()
		a := approval{ID: m.ID, Method: m.Method, Reason: r.c.secret.text(p.Reason), Requested: now, Deadline: now.Add(r.c.Approval), Permissions: p.Permissions}
		a.Generation = r.s.Generation
		a.Summary = brief(string(capturedParams))
		var q struct {
			Questions []struct {
				ID string `json:"id"`
			}
		}
		if err := json.Unmarshal(m.Params, &q); err != nil {
			return err
		}
		a.Questions = q.Questions
		r.s.Approvals = append(r.s.Approvals, a)
		r.deriveState()
		if err := r.writeProgress("server/request", fmt.Sprintf("%s id=%s %s", m.Method, m.ID, a.Reason)); err != nil {
			return err
		}
		if err := r.save(); err != nil {
			return err
		}
		if provider {
			return r.requestError(a, "host provider unavailable")
		}
		return nil
	default:
		if err := r.writeProgress("server/request", m.Method+" unsupported"); err != nil {
			return err
		}
		return r.srv.send(fields{"id": m.ID, "error": fields{"code": -32601, "message": "unsupported by unattended cxrun"}})
	}
}
func (r *runner) requestError(a approval, msg string) error {
	if err := r.srv.send(fields{"id": a.ID, "error": fields{"code": -32001, "message": msg}}); err != nil {
		return err
	}
	return r.removeApproval(a.ID)
}
func (r *runner) removeApproval(id json.RawMessage) error {
	for i, a := range r.s.Approvals {
		if bytes.Equal(a.ID, id) {
			r.s.Approvals = append(r.s.Approvals[:i], r.s.Approvals[i+1:]...)
			r.lastAction = time.Now()
			r.deriveState()
			return r.save()
		}
	}
	return nil
}
func (r *runner) decide(a approval, allow bool, reason string) error {
	var result fields
	switch a.Method {
	case "item/permissions/requestApproval":
		result = fields{"permissions": fields{}, "scope": "turn"}
		if allow && len(a.Permissions) > 0 {
			result["permissions"] = a.Permissions
		}
	case "execCommandApproval", "applyPatchApproval":
		result = fields{"decision": fields{"denied": fields{"rejection": "user"}}}
		if allow {
			result["decision"] = "approved"
		}
	default:
		result = fields{"decision": "decline"}
		if allow {
			result["decision"] = "accept"
		}
	}
	if err := r.srv.send(fields{"id": a.ID, "result": result}); err != nil {
		return err
	}
	if err := r.writeProgress("approval", fmt.Sprintf("id=%s allow=%t %s", a.ID, allow, r.c.secret.text(reason))); err != nil {
		return err
	}
	return r.removeApproval(a.ID)
}

type inboxMessage struct {
	Text  string            `json:"text,omitempty"`
	Input []json.RawMessage `json:"input,omitempty"`
}

type inboxAck struct {
	Seq    uint64 `json:"seq"`
	ID     string `json:"id"`
	Result string `json:"result"`
}

// A claim is written before a side effect so recovery can distinguish a
// queued message from an operation that may have reached the engine.
type inboxClaim struct {
	Seq        uint64 `json:"seq"`
	ID         string `json:"id"`
	Action     string `json:"action"`
	Generation uint64 `json:"generation"`
}

type inboxState struct {
	tail        *stateTail
	inboxSynced stateCursor
	outbox      stateCursor
	outboxTail  *stateTail
	pendingAsks map[string]pendingAgentAsk
	answered    map[string]bool
	queue       []stateRecord
	first       map[string]uint64
	acks        map[uint64]inboxAck
	claims      map[uint64]inboxClaim
	receipts    map[string]receiptRecord
}

func inboxTail(dir string, from stateCursor) (*stateTail, error) {
	return openMailboxTail(filepath.Join(dir, "inbox"), from)
}

func readInboxState(dir string) (*inboxState, error) {
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	records, err := tail.Next()
	if err != nil {
		return nil, err
	}
	in := &inboxState{
		first: make(map[string]uint64), acks: make(map[uint64]inboxAck),
		claims: make(map[uint64]inboxClaim), receipts: make(map[string]receiptRecord),
		pendingAsks: make(map[string]pendingAgentAsk), answered: make(map[string]bool),
	}
	for _, record := range records {
		switch record.Kind {
		case outboxSeenKind:
			if err := json.Unmarshal(record.Value, &in.outbox); err != nil {
				return nil, err
			}
		case outboxRequestKind:
			var ask pendingAgentAsk
			if err := json.Unmarshal(record.Value, &ask); err != nil {
				return nil, err
			}
			if !in.answered[ask.Request] {
				in.pendingAsks[ask.Request] = ask
			}
		case "inbox_ack":
			var ack inboxAck
			if err := json.Unmarshal(record.Value, &ack); err != nil {
				return nil, err
			}
			in.acks[ack.Seq] = ack
			delete(in.claims, ack.Seq)
		case "inbox_claim":
			var claim inboxClaim
			if err := json.Unmarshal(record.Value, &claim); err != nil {
				return nil, err
			}
			in.applyClaim(claim)
		case "receipt":
			var receipt receiptRecord
			if err := json.Unmarshal(record.Value, &receipt); err != nil {
				return nil, err
			}
			in.applyReceipt(receipt)
		}
	}
	return in, nil
}

// mailboxCheckpoints retains outstanding delivery intents and published
// outcomes when Rotate reclaims state segments. The runner calls this under
// owner.lock and passes the records alongside its other rotation checkpoints.
func mailboxCheckpoints(dir string) ([]stateRecord, error) {
	in, err := readInboxState(dir)
	if err != nil {
		return nil, err
	}
	records := make([]stateRecord, 0, len(in.acks)+len(in.claims)+len(in.receipts))
	appendValue := func(kind string, value any) error {
		record, err := newRecord(kind, value)
		if err == nil {
			records = append(records, record)
		}
		return err
	}
	if in.outbox.Seq != 0 {
		if err := appendValue(outboxSeenKind, in.outbox); err != nil {
			return nil, err
		}
	}
	for _, id := range slices.Sorted(maps.Keys(in.pendingAsks)) {
		if err := appendValue(outboxRequestKind, in.pendingAsks[id]); err != nil {
			return nil, err
		}
	}
	for _, seq := range slices.Sorted(maps.Keys(in.acks)) {
		if err := appendValue("inbox_ack", in.acks[seq]); err != nil {
			return nil, err
		}
	}
	for _, seq := range slices.Sorted(maps.Keys(in.claims)) {
		if err := appendValue("inbox_claim", in.claims[seq]); err != nil {
			return nil, err
		}
	}
	for _, op := range slices.Sorted(maps.Keys(in.receipts)) {
		if err := appendValue("receipt", in.receipts[op]); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (in *inboxState) applyReceipt(receipt receiptRecord) {
	in.receipts[receipt.Op] = receipt
	if receipt.AgentReply != nil {
		request := receipt.AgentReply.Request
		in.answered[request] = true
		delete(in.pendingAsks, request)
	}
	if receipt.Op == initialForkOp {
		delete(in.claims, 0)
	}
}

func (in *inboxState) applyClaim(claim inboxClaim) {
	if claim.Action == "release" {
		delete(in.claims, claim.Seq)
	} else if _, acked := in.acks[claim.Seq]; !acked {
		in.claims[claim.Seq] = claim
	}
}

func (r *runner) refreshInbox() error {
	if r.inbox == nil {
		if err := validateSegments(filepath.Join(r.dir, "inbox"), true); err != nil {
			return err
		}
		in, err := readInboxState(r.dir)
		if err != nil {
			return err
		}
		r.inbox = in
	}
	in := r.inbox
	if in.tail == nil {
		var err error
		in.tail, err = inboxTail(r.dir, stateCursor{})
		if err != nil {
			return err
		}
	}
	records, err := in.tail.Next()
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.ID == "" {
			return fmt.Errorf("inbox record %d has no ID", record.Seq)
		}
		if ack, ok := in.acks[record.Seq]; ok && ack.ID != record.ID {
			return fmt.Errorf("%w: inbox ACK ID mismatch at %d", errLogDamage, record.Seq)
		}
		if claim, ok := in.claims[record.Seq]; ok && claim.ID != record.ID {
			return fmt.Errorf("%w: inbox claim ID mismatch at %d", errLogDamage, record.Seq)
		}
		if _, seen := in.first[record.ID]; !seen {
			in.first[record.ID] = record.Seq
		}
		in.queue = append(in.queue, record)
	}
	for _, record := range in.queue {
		if _, acked := in.acks[record.Seq]; !acked && in.first[record.ID] != record.Seq {
			if err := r.ackInbox(record, "duplicate"); err != nil {
				return err
			}
		}
	}
	in.queue = slices.DeleteFunc(in.queue, func(record stateRecord) bool {
		_, acked := in.acks[record.Seq]
		return acked
	})
	return nil
}

// The mailbox locks are held through publication of the segment containing
// these outcomes and the terminal status.
func (r *runner) terminalMailboxOutcomes() ([]stateRecord, error) {
	if err := r.refreshInbox(); err != nil {
		return nil, err
	}
	if n := len(r.inbox.queue); n != 0 {
		if err := r.syncInboxThrough(r.inbox.queue[n-1].position); err != nil {
			return nil, err
		}
	}
	var records []stateRecord
	appendValue := func(kind string, v any) error {
		frame, err := newRecord(kind, v)
		if err == nil {
			records = append(records, frame)
		}
		return err
	}
	for _, item := range r.inbox.queue {
		result := "withdrawn"
		if _, claimed := r.inbox.claims[item.Seq]; claimed {
			result = "uncertain"
		}
		if item.Kind == "close" && item.Seq == r.closeRequest && r.closeCompleted {
			result = "delivered"
		}
		if item.Kind == "rpc" {
			if receipt, ok := r.inbox.receipts[item.ID]; ok {
				result = receipt.inboxResult()
			} else {
				failure := receiptRecord{Op: item.ID, Generation: r.s.Generation, Status: "error",
					Error: &rpcError{Code: -32000, Message: "job ended before operation"}}
				if result == "uncertain" {
					failure = uncertainReceipt(r.inbox.claims[item.Seq])
				}
				if err := appendValue("receipt", failure); err != nil {
					return nil, err
				}
			}
		}
		if err := appendValue("inbox_ack", inboxAck{Seq: item.Seq, ID: item.ID, Result: result}); err != nil {
			return nil, err
		}
	}
	in := r.inbox
	tail, err := openOutboxTail(r.dir, in.outbox)
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	// The durable state cursor covers acknowledged observations. Only frames
	// added since then need to be read while the mailboxes are closed.
	if err := readMailTail(tail, in.outbox.Seq, func(mail agentMail) error {
		if mail.RunID != r.s.RunID {
			return errJobReplaced
		}
		if mail.Blocking && !in.answered[mail.Request] {
			in.pendingAsks[mail.Request] = pendingAgentAsk{Request: mail.Request, Generation: mail.Generation}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := syncMailboxPrefix(filepath.Join(r.dir, "outbox"), in.outbox, tail.Cursor()); err != nil {
		return nil, err
	}
	for _, id := range slices.Sorted(maps.Keys(in.pendingAsks)) {
		ask := in.pendingAsks[id]
		if err := appendValue("receipt", receiptRecord{Op: "closed/" + id, Status: "error",
			Generation: ask.Generation, AgentReply: &agentReply{Request: id,
				Error: json.RawMessage(`"job ended before answer"`)}}); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// A persisted claim or ACK must not outlive its message. Flush the batch
// already observed by this runner so one barrier covers multiple outcomes.
func (r *runner) syncInboxThrough(through stateCursor) error {
	in := r.inbox
	if through.Seq <= in.inboxSynced.Seq {
		return nil
	}
	tail := in.tail.Cursor()
	if err := syncMailboxPrefix(filepath.Join(r.dir, "inbox"), in.inboxSynced, tail); err != nil {
		return err
	}
	in.inboxSynced = tail
	return nil
}

func (r *runner) claimInbox(record stateRecord, action string) error {
	if err := r.syncInboxThrough(record.position); err != nil {
		return err
	}
	claim := inboxClaim{Seq: record.Seq, ID: record.ID, Action: action, Generation: r.s.Generation}
	if err := r.stateLog.Append("inbox_claim", claim); err != nil {
		return err
	}
	if err := r.stateLog.sync(); err != nil {
		return err
	}
	r.inbox.applyClaim(claim)
	return nil
}

func (r *runner) ackInbox(record stateRecord, result string) error {
	if err := r.syncInboxThrough(record.position); err != nil {
		return err
	}
	ack := inboxAck{Seq: record.Seq, ID: record.ID, Result: result}
	if err := r.stateLog.Append("inbox_ack", ack); err != nil {
		return err
	}
	r.inbox.acks[record.Seq] = ack
	delete(r.inbox.claims, record.Seq)
	return nil
}

func publishMessage(ctx context.Context, dir, id string, msg inboxMessage) error {
	record, err := newRecord("msg", msg)
	if err != nil {
		return err
	}
	record.ID = id
	_, err = appendInbox(ctx, dir, record, func(_ *stateRecord, previous stateRecord, s status) error {
		if previous.Seq != 0 {
			return nil
		}
		eng, err := liveAgentEngine(s)
		if err != nil || len(msg.Input) == 0 {
			return err
		}
		return eng.checkInput(msg.Input)
	})
	return err
}

func appendInbox(ctx context.Context, dir string, record stateRecord, prepare func(*stateRecord, stateRecord, status) error) (stateRecord, error) {
	return appendJobMailbox(ctx, dir, "inbox", nil, record, prepare)
}

// pendingInboxMessages supplies the message IDs and sequence numbers that a
// wait observer can bind to, then match against subsequent inbox_ack records.
func pendingInboxMessages(dir string) ([]stateRecord, error) {
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
	return slices.DeleteFunc(records, func(record stateRecord) bool {
		ack := in.acks[record.Seq]
		return record.Kind != "msg" || ack.ID == record.ID && ack.Seq == record.Seq
	}), nil
}

func (r *runner) approvalFiles() error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	// A denial takes precedence when both decisions are waiting.
	for _, kind := range []string{"deny", "approve"} {
		for _, record := range r.inbox.queue {
			if record.Kind != kind {
				continue
			}
			if _, acked := r.inbox.acks[record.Seq]; acked {
				continue
			}
			m, valid, err := r.marker(record)
			if err != nil {
				return err
			}
			matched := false
			if valid {
				for _, a := range r.s.Approvals {
					id := string(m.Request)
					if !approvalMethod(a.Method) || (id != "" && id != string(a.ID)) ||
						record.Time.Before(a.Requested) || (id == "" && len(r.s.Approvals) != 1) {
						continue
					}
					if time.Now().Before(a.Deadline) {
						if err = r.claimInbox(record, "deliver"); err != nil {
							return err
						}
						if err = r.decide(a, record.Kind == "approve", "inbox/"+record.Kind); err != nil {
							return err
						}
						matched = true
					}
					break
				}
			}
			result := "delivered"
			if !matched {
				result = "withdrawn"
				if err := r.note("approval", "ignored stale or ambiguous "+record.Kind); err != nil {
					return err
				}
			}
			if err := r.ackInbox(record, result); err != nil {
				return err
			}
		}
	}
	for _, a := range append([]approval(nil), r.s.Approvals...) {
		if !time.Now().Before(a.Deadline) {
			if a.Method == agentAsk {
				if err := r.removeApproval(a.ID); err != nil {
					return err
				}
				continue
			}
			if inputMethod(a.Method) {
				if err := r.requestError(a, "input deadline exceeded"); err != nil {
					return err
				}
				continue
			}
			if err := r.decide(a, false, "timeout"); err != nil {
				return err
			}
		}
	}
	return nil
}
func (r *runner) recoverInbox(t turn) error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	for _, record := range r.inbox.queue {
		claim, ok := r.inbox.claims[record.Seq]
		if !ok || record.Kind == "rpc" || record.Kind == "close" {
			continue
		}
		if claim.Action == "withdraw" {
			if err := r.withdrawQueued(record, r.interruptTargets(record)); err != nil {
				return err
			}
			continue
		}
		result := "uncertain"
		if record.Kind == "msg" {
			for _, item := range t.Items {
				if item.Type == "userMessage" && item.ClientID == record.ID {
					result = "delivered"
					break
				}
			}
		}
		if err := r.ackInbox(record, result); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) deliver(record stateRecord) error {
	if r.active() && !r.c.driver().capabilities().Steer {
		return nil
	}
	var msg inboxMessage
	if err := json.Unmarshal(record.Value, &msg); err != nil {
		return err
	}
	if !r.active() {
		ready, err := r.followSlot()
		if !ready || err != nil {
			return err
		}
	}
	// Serialize the start claim with enqueue. An interrupt already published
	// while this follow-up was queued gets to withdraw the input first.
	lock, err := lockMailbox(r.ctx, mailboxLockPath(r.dir, "inbox"))
	if err != nil {
		return err
	}
	claimed, err := func() (bool, error) {
		defer closeBestEffort(lock)
		if err := r.refreshInbox(); err != nil {
			return false, err
		}
		if !r.active() {
			for _, pending := range r.inbox.queue {
				if pending.Kind != "interrupt" || pending.Seq <= record.Seq {
					continue
				}
				_, valid, err := r.marker(pending)
				if err != nil || valid {
					return false, err
				}
			}
		}
		return true, r.claimInbox(record, "deliver")
	}()
	if !claimed || err != nil {
		return err
	}
	steer := r.active()
	if len(msg.Input) > 0 {
		err = r.sendInput(msg.Input, record.ID, steer)
	} else {
		err = r.input(msg.Text, record.ID, steer)
	}
	var rejection *rpcError
	if steer && errors.As(err, &rejection) {
		// The completion event can arrive on either side of a rejected steer reply.
		// Only an observed end of the expected turn permits sending it as a new turn.
		if r.active() {
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
		settle:
			for r.active() {
				select {
				case f := <-r.srv.frames:
					if _, readErr := r.read(f); readErr != nil {
						return errors.Join(err, readErr, r.claimInbox(record, "release"))
					}
				case <-r.ctx.Done():
					return errors.Join(r.ctx.Err(), r.claimInbox(record, "release"))
				case <-timer.C:
					break settle
				}
			}
		}
		if !r.active() {
			return r.claimInbox(record, "release")
		}
	}
	if errors.As(err, &rejection) {
		return errors.Join(err, r.claimInbox(record, "release"))
	}
	if err != nil {
		return err
	}
	return r.ackInbox(record, "delivered")
}

// The interrupt sequence bounds its target set across retries and recovery.
// An earlier claim already handed to the engine is outside that set.
func (r *runner) interruptTargets(interrupt stateRecord) []stateRecord {
	var targets []stateRecord
	for _, record := range r.inbox.queue {
		if record.Seq >= interrupt.Seq {
			break
		}
		_, acked := r.inbox.acks[record.Seq]
		_, claimed := r.inbox.claims[record.Seq]
		if record.Kind == "msg" && !acked && !claimed {
			targets = append(targets, record)
		}
	}
	return targets
}

func (r *runner) withdrawQueued(interrupt stateRecord, targets []stateRecord) error {
	if !r.active() {
		queued := r.follow != nil || r.s.State == "queued"
		if r.follow != nil {
			r.cancelFollow()
			if r.budget != nil {
				r.resumeRuntime()
			}
		}
		if queued {
			r.releaseSlot()
		}
		r.s.QueueReason = ""
		r.deriveState()
		r.idleSince = time.Now()
		if err := r.save(); err != nil {
			return err
		}
	}
	if err := r.interrupt(); err != nil {
		return err
	}
	for _, record := range targets {
		if err := r.ackInbox(record, "withdrawn"); err != nil {
			return err
		}
	}
	return r.ackInbox(interrupt, "delivered")
}

func (r *runner) interrupt() error {
	if !r.active() {
		return r.note("turn/interrupt", "idle")
	}
	if err := r.c.driver().interrupt(r); err != nil {
		return err
	}
	return r.writeProgress("turn/interrupt", r.s.Turn)
}
func (r *runner) pollAgent() (bool, error) {
	if err := r.flushExpiredTurnFailure(); err != nil {
		return false, err
	}
	stop, err := r.poll()
	if errors.Is(err, errQueuedClose) {
		r.s.CompletedStatus = "interrupted"
		return true, nil
	}
	if !stop && err == nil {
		err = r.flushState()
	}
	return stop, err
}

func (r *runner) poll() (bool, error) {
	if err := r.pollAgentMail(); err != nil {
		return false, err
	}
	if err := r.approvalFiles(); err != nil {
		return false, err
	}
	_, closing, err := r.takeMarker("close")
	if err != nil {
		return false, err
	}
	if closing || !r.closing.IsZero() {
		if r.closing.IsZero() {
			r.closing = time.Now()
			if err = r.interrupt(); err != nil {
				return false, err
			}
		}
		if !r.active() {
			return true, nil
		}
		if time.Since(r.closing) > 10*time.Second {
			return false, errors.New("close: turn did not acknowledge interrupt")
		}
		return false, nil
	}
	if r.active() && time.Since(r.turnSince) > r.c.TurnLimit {
		return false, fmt.Errorf("turn %s exceeded %s", r.s.Turn, r.c.TurnLimit)
	}
	if err = r.rpcFiles(false); err != nil {
		return false, err
	}
	if err = r.watchdog(); err != nil {
		return false, err
	}
	if err = r.refreshInbox(); err != nil {
		return false, err
	}
	for _, record := range r.inbox.queue {
		if record.Kind == "interrupt" {
			return false, r.processInterrupt(record)
		}
	}
	if started, continueErr := r.continuePending(); started || continueErr != nil {
		return false, continueErr
	}
	for _, record := range r.inbox.queue {
		if record.Kind == "msg" {
			return false, r.deliver(record)
		}
	}
	return r.s.State != "waiting_input" && !r.active() && time.Since(r.idleSince) >= r.c.Idle, nil
}
