package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
func (r *runner) mail(name string) string { return filepath.Join(r.dir, "inbox", name) }
func (r *runner) archive(path string) error {
	name := filepath.Base(path)
	if !strings.HasSuffix(name, ".msg") {
		name = fmt.Sprintf("%d-%s", time.Now().UnixNano(), name)
	}
	return os.Rename(path, r.mail(filepath.Join("done", name)))
}
func (r *runner) approvalFiles() error {
	for _, name := range []string{"deny", "approve"} {
		m, ok, err := r.takeMarker(name)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		id := string(m.Request)
		matched := false
		for _, a := range r.s.Approvals {
			if !approvalMethod(a.Method) {
				continue
			}
			if id != "" && id != string(a.ID) {
				continue
			}
			if m.published.Before(a.Requested) || (id == "" && len(r.s.Approvals) != 1) {
				continue
			}
			if time.Now().Before(a.Deadline) {
				if err = r.decide(a, name == "approve", "inbox/"+name); err != nil {
					return err
				}
				matched = true
			}
			break
		}
		if !matched {
			if err = r.note("approval", "ignored stale or ambiguous "+name); err != nil {
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
	entries, err := os.ReadDir(r.mail("pending"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		path := r.mail(filepath.Join("pending", name))
		found := false
		for _, i := range t.Items {
			if i.Type == "userMessage" && i.ClientID == name {
				found = true
				break
			}
		}
		if found {
			if err = r.archive(path); err != nil {
				return err
			}
			continue
		}
		// The remote side may have committed before the transport died. Never blindly replay side effects.
		if err = os.MkdirAll(r.mail("uncertain"), 0700); err != nil {
			return err
		}
		if err = os.Rename(path, r.mail(filepath.Join("uncertain", name))); err != nil {
			return err
		}
		if err = r.note("inbox/uncertain", name+" acknowledgement unavailable after restart"); err != nil {
			return err
		}
	}
	return nil
}
func (r *runner) deliver(name string) error {
	if r.active() && !r.c.driver().capabilities().Steer {
		return nil
	}
	path := r.mail(name)
	b, err := readStateFile(path)
	if err != nil {
		return err
	}
	// A remote publisher retries by republishing the same immutable name and bytes.
	for _, sub := range []string{"done", "uncertain", "withdrawn"} {
		previous, duplicateErr := readStateFile(r.mail(filepath.Join(sub, name)))
		if os.IsNotExist(duplicateErr) {
			continue
		}
		if duplicateErr != nil {
			return duplicateErr
		}
		if !bytes.Equal(b, previous) {
			return fmt.Errorf("message name reused with different contents: %s", name)
		}
		if duplicateErr = r.note("inbox/duplicate", sub+"/"+name); duplicateErr != nil {
			return duplicateErr
		}
		return os.Remove(path)
	}
	// Keep unsent messages in inbox while waiting for admission, including on crash.
	if !r.active() {
		ready, slotErr := r.followSlot()
		if !ready || slotErr != nil {
			return slotErr
		}
	}
	pending := r.mail(filepath.Join("pending", name))
	if err = os.Rename(path, pending); err != nil {
		return err
	}
	steer := r.active()
	if strings.HasSuffix(name, ".input.msg") {
		var input []json.RawMessage
		if err = json.Unmarshal(b, &input); err == nil {
			err = r.sendInput(input, name, steer)
		}
	} else {
		err = r.input(string(b), name, steer)
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
						return errors.Join(err, readErr, os.Rename(pending, path))
					}
				case <-r.ctx.Done():
					return errors.Join(r.ctx.Err(), os.Rename(pending, path))
				case <-timer.C:
					break settle
				}
			}
		}
		if !r.active() {
			return os.Rename(pending, path)
		}
	}
	if errors.As(err, &rejection) {
		return errors.Join(err, os.Rename(pending, path))
	}
	if err != nil {
		return err
	}
	return r.archive(pending)
}

// withdrawQueued cancels a follow-up that is still waiting for admission: interrupting an
// idle job drops the queued input (kept under inbox/withdrawn) and returns it to waiting.
func (r *runner) withdrawQueued() error {
	if r.follow == nil || r.active() {
		return nil
	}
	r.cancelFollow()
	entries, err := os.ReadDir(r.mail(""))
	if err != nil {
		return err
	}
	if err = os.MkdirAll(r.mail("withdrawn"), 0700); err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".msg") {
			continue
		}
		if err = os.Rename(r.mail(entry.Name()), r.mail(filepath.Join("withdrawn", entry.Name()))); err != nil {
			return err
		}
		if err = r.note("inbox/withdrawn", entry.Name()); err != nil {
			return err
		}
	}
	r.deriveState()
	r.idleSince = time.Now()
	return r.save()
}
func (r *runner) interrupt() error {
	if !r.active() {
		return r.note("turn/interrupt", "idle")
	}
	if r.claude != nil {
		r.claude.interrupt()
		return r.writeProgress("turn/interrupt", r.s.Turn)
	}
	err := r.call("turn/interrupt", fields{"threadId": r.s.Thread, "turnId": r.s.Turn}, nil)
	var rejected *rpcError
	if err != nil && (!errors.As(err, &rejected) || r.active()) {
		return err
	}
	return r.writeProgress("turn/interrupt", r.s.Turn)
}
func fileExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
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
	if !stop && err == nil && r.dirty {
		err = r.save()
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
	if started, continueErr := r.continuePending(); started || continueErr != nil {
		return false, continueErr
	}
	_, exists, err := r.takeMarker("interrupt")
	if err != nil {
		return false, err
	}
	if exists {
		if err = r.withdrawQueued(); err != nil {
			return false, err
		}
		if err = r.interrupt(); err != nil {
			return false, err
		}
		return false, nil
	}
	// A follow-up waiting for admission stays in the inbox; it is pending input, so idle
	// exit must not end the runner (queue-timeout bounds the wait instead).
	entries, err := os.ReadDir(r.mail(""))
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".msg") {
			return false, r.deliver(entry.Name())
		}
	}
	return r.s.State != "waiting_input" && !r.active() && time.Since(r.idleSince) >= r.c.Idle, nil
}
