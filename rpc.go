package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/djylb/timeshim"
)

type envelope struct {
	Kind       string          `json:"kind"`
	Method     string          `json:"method,omitempty"`
	Params     json.RawMessage `json:"params,omitempty"`
	Request    json.RawMessage `json:"request_id,omitempty"`
	Question   string          `json:"question_id,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      json.RawMessage `json:"error,omitempty"`
	Version    int             `json:"version"`
	Generation uint64          `json:"generation,omitempty"`
}
type rpcReceipt struct {
	Error      *rpcError       `json:"error,omitempty"`
	Op         string          `json:"op_id"`
	State      string          `json:"status"`
	Result     json.RawMessage `json:"result,omitempty"`
	Generation uint64          `json:"generation"`
}

// receiptRecord is the state-log payload. rpcReceipt preserves the CLI
// response fields used by controllers.
type receiptRecord struct {
	Error      *rpcError       `json:"error,omitempty"`
	AgentReply *agentReply     `json:"agent_reply,omitempty"`
	Op         string          `json:"op"`
	Status     string          `json:"status"`
	Answer     json.RawMessage `json:"answer,omitempty"`
	Generation uint64          `json:"generation"`
}

type agentReply struct {
	Request string          `json:"request"`
	Answer  json.RawMessage `json:"answer"`
	Error   json.RawMessage `json:"error"`
}

func (r *runner) commitReceipt(receipt receiptRecord) error {
	if err := r.stateLog.Append("receipt", receipt); err != nil {
		return err
	}
	r.inbox.applyReceipt(receipt)
	return nil
}

func (r *runner) recoverRPC() error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	if claim, ok := r.inbox.claims[0]; ok {
		if err := r.commitReceipt(uncertainReceipt(claim)); err != nil {
			return err
		}
	}
	for _, record := range r.inbox.queue {
		if record.Kind != "rpc" {
			continue
		}
		claim, claimed := r.inbox.claims[record.Seq]
		receipt, done := r.inbox.receipts[record.ID]
		if !claimed && !done {
			continue
		}
		if !done {
			receipt = uncertainReceipt(claim)
			if err := r.commitReceipt(receipt); err != nil {
				return err
			}
		}
		if err := r.ackInbox(record, receipt.inboxResult()); err != nil {
			return err
		}
	}
	return nil
}

func uncertainReceipt(claim inboxClaim) receiptRecord {
	return receiptRecord{Op: claim.ID, Generation: claim.Generation, Status: "uncertain",
		Error: &rpcError{Code: -32002, Message: "connection ended before receipt"}}
}

func (receipt receiptRecord) inboxResult() string {
	if receipt.Status == "uncertain" {
		return "uncertain"
	}
	return "delivered"
}

// Nested calls may pump replies, but never start another client call.
func (r *runner) rpcFiles(repliesOnly bool) error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	for _, record := range r.inbox.queue {
		if record.Kind != "rpc" {
			continue
		}
		if _, claimed := r.inbox.claims[record.Seq]; claimed {
			continue
		}
		if receipt, done := r.inbox.receipts[record.ID]; done {
			return r.ackInbox(record, receipt.inboxResult())
		}
		var env envelope
		parseErr := json.Unmarshal(record.Value, &env)
		if repliesOnly && env.Kind != "reply" {
			continue
		}
		if err := r.claimInbox(record, "deliver"); err != nil {
			return err
		}
		receipt, transportErr, err := r.finishRPC(record.ID, env, parseErr)
		if err != nil {
			return err
		}
		return errors.Join(transportErr, r.ackInbox(record, receipt.inboxResult()))
	}
	return nil
}

// finishRPC returns a committed receipt when receiptErr is nil. A separate transport
// error lets the caller acknowledge the receipt before handling a disconnect.
func (r *runner) finishRPC(id string, env envelope, parseErr error) (receipt receiptRecord, transportErr, receiptErr error) {
	receipt = receiptRecord{Op: id, Generation: r.s.Generation, Status: "completed"}
	if parseErr == nil {
		receipt.Answer, parseErr = r.executeRPC(id, env, &receipt.AgentReply)
	}
	if len(receipt.Answer) != 0 {
		filtered, err := r.c.secret.jsonOutput(receipt.Answer)
		if err != nil {
			return receiptRecord{}, nil, err
		}
		receipt.Answer = filtered
	}
	if parseErr != nil {
		receipt.AgentReply = nil
		receipt.Status = "error"
		var remote *rpcError
		if errors.As(parseErr, &remote) {
			receipt.Error = remote
		} else {
			receipt.Error = &rpcError{Code: -32000, Message: parseErr.Error()}
		}
		if errors.Is(parseErr, errServerClosed) || errors.Is(parseErr, errRPCUncertain) || r.ctx.Err() != nil {
			receipt.Status = "uncertain"
		}
	}
	if err := r.commitReceipt(receipt); err != nil {
		return receiptRecord{}, nil, err
	}
	if errors.Is(parseErr, errServerClosed) {
		return receipt, parseErr, nil
	}
	return receipt, nil, nil
}

const detachedReview = "cxrun/reviewDetached"

var errRPCUncertain = errors.New("RPC outcome uncertain")

func (r *runner) executeRPC(id string, e envelope, reply **agentReply) (json.RawMessage, error) {
	if !r.c.driver().capabilities().RPC {
		return nil, fmt.Errorf("engine RPC not supported by engine %s", r.c.Engine)
	}
	if e.Version != 1 {
		return nil, errors.New("unsupported envelope version")
	}
	switch e.Kind {
	case "reply":
		if e.Generation != r.s.Generation {
			return nil, errors.New("stale connection generation")
		}
		if (e.Result == nil) == (e.Error == nil) {
			return nil, errors.New("reply requires exactly one result or error")
		}
		for _, a := range r.s.Approvals {
			if bytes.Equal(a.ID, e.Request) && timeshim.Or(r.clock).Now().Before(a.Deadline) {
				if a.Method == agentAsk {
					var request string
					if err := json.Unmarshal(a.ID, &request); err != nil {
						return nil, err
					}
					*reply = &agentReply{Request: request, Answer: e.Result, Error: e.Error}
					return json.RawMessage("null"), r.removeApproval(a.ID)
				}
				p := fields{"id": a.ID}
				if e.Error != nil {
					p["error"] = e.Error
				} else {
					p["result"] = e.Result
				}
				if err := r.srv.send(p); err != nil {
					return nil, err
				}
				return json.RawMessage("null"), r.removeApproval(a.ID)
			}
		}
		return nil, errors.New("request already resolved or expired")
	case "call":
		if e.Method == "" {
			return nil, errors.New("missing RPC method")
		}
		var after int64
		if e.Method == "thread/compact/start" {
			fi, err := r.events.Stat()
			if err != nil {
				return nil, err
			}
			after = fi.Size()
		}
		var result json.RawMessage
		method := e.Method
		if method == detachedReview {
			var params map[string]json.RawMessage
			if err := json.Unmarshal(e.Params, &params); err != nil || params["target"] == nil {
				return nil, errors.New("detached review requires a target")
			}
			var child struct{ Thread struct{ ID string } }
			if err := r.call("thread/start", r.threadParams(), &child); err != nil {
				return nil, err
			}
			if child.Thread.ID == "" {
				return nil, fmt.Errorf("%w: thread/start omitted id", errRPCUncertain)
			}
			r.s.ReviewThread, r.s.ReviewTurn = child.Thread.ID, ""
			if err := r.save(); err != nil {
				return nil, errors.Join(errRPCUncertain, err)
			}
			params["threadId"], _ = json.Marshal(child.Thread.ID)
			params["delivery"] = json.RawMessage(`"inline"`)
			e.Params, _ = json.Marshal(params)
			method = "review/start"
		}
		if err := r.call(method, e.Params, &result); err != nil {
			return nil, err
		}
		var target struct{ ThreadID string }
		_ = json.Unmarshal(e.Params, &target)
		switch method {
		case "thread/revert":
			if target.ThreadID == r.s.Thread {
				if err := r.refreshHistory(); err != nil {
					return result, fmt.Errorf("%w: revert accepted, history refresh: %w", errRPCUncertain, err)
				}
			}
		case "review/start":
			var review struct {
				ReviewThreadID string
				Turn           turn
			}
			if err := json.Unmarshal(result, &review); err != nil {
				return result, err
			}
			r.s.ReviewThread = review.ReviewThreadID
			r.s.ReviewTurn = review.Turn.ID
		case "thread/compact/start":
			if target.ThreadID == r.s.Thread {
				r.s.CompactOp = id
				r.s.CompactAfter = after
			}
		}
		if err := r.save(); err != nil {
			return result, errors.Join(errRPCUncertain, err)
		}
		return result, nil
	default:
		return nil, errors.New("unknown envelope kind")
	}
}
func (r *runner) refreshHistory() error {
	var page struct{ Data []turn }
	p := fields{"threadId": r.s.Thread, "sortDirection": "desc", "limit": 1, "itemsView": "full"}
	if err := r.call("thread/turns/list", p, &page); err != nil {
		return err
	}
	r.lastText = ""
	if len(page.Data) > 0 {
		return r.finish(page.Data[0])
	}
	r.clearCurrentTurn()
	return nil
}

func (r *runner) clearCurrentTurn() {
	r.s.Turn = ""
	r.s.LastCompleted = ""
	r.s.CompletedStatus = ""
	r.s.CompletedRateLimited = false
	r.s.TurnStatus = ""
	r.s.InputKind = ""
	r.s.turnSummary = turnSummary{}
	r.deriveState()
	r.idleSince = timeshim.Or(r.clock).Now()
}

func (r *runner) hasUncertainRevert() bool {
	inbox := filepath.Join(r.dir, "inbox")
	for id, receipt := range r.inbox.receipts {
		if receipt.Status != "uncertain" {
			continue
		}
		frame, err := findMailboxID(inbox, filepath.Join(inbox, ".ids", snapSum([]byte(id))), id)
		var env envelope
		if err == nil && frame.Kind == "rpc" && json.Unmarshal(frame.Value, &env) == nil && env.Method == "thread/revert" {
			var target struct{ ThreadID string }
			if json.Unmarshal(env.Params, &target) == nil && target.ThreadID == r.s.Thread {
				return true
			}
		}
	}
	return false
}

const initialForkOp = "initial-fork"

// Registration holds the source thread. The child owns the call and its receipt,
// so reconnecting after a lost response cannot create a second fork.
func (r *runner) fork(params fields, dst any) error {
	if err := r.refreshInbox(); err != nil {
		return err
	}
	receipt, ok := r.inbox.receipts[initialForkOp]
	if !ok {
		raw, err := json.Marshal(params)
		if err != nil {
			return err
		}
		e := envelope{Version: 1, Kind: "call", Method: "thread/fork", Params: raw}
		if err = r.claimInbox(stateRecord{ID: initialForkOp}, "deliver"); err != nil {
			return err
		}
		var transportErr error
		receipt, transportErr, err = r.finishRPC(initialForkOp, e, nil)
		if err != nil {
			return err
		}
		if transportErr != nil {
			return transportErr
		}
	}
	if receipt.Error != nil {
		return receipt.Error
	}
	return json.Unmarshal(receipt.Answer, dst)
}
