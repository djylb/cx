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

type envelope struct {
	Kind       string          `json:"kind"`
	Method     string          `json:"method,omitempty"`
	Params     json.RawMessage `json:"params,omitempty"`
	Request    json.RawMessage `json:"request_id,omitempty"`
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

func (r *runner) receiptPath(id string) string { return filepath.Join(r.dir, "outbox", id+".json") }
func (r *runner) commitReceipt(name string, res rpcReceipt) error {
	if err := writeJSON(r.receiptPath(res.Op), res); err != nil {
		return err
	}
	sub := "done"
	if res.State == "uncertain" {
		sub = "uncertain"
	}
	if err := os.MkdirAll(r.mail(sub), 0700); err != nil {
		return err
	}
	return os.Rename(r.mail(filepath.Join("pending", name)), r.mail(filepath.Join(sub, name)))
}
func (r *runner) recoverRPC() error {
	if err := os.MkdirAll(filepath.Join(r.dir, "outbox"), 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(r.mail("pending"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		id, ok := strings.CutSuffix(name, ".rpc")
		if !ok {
			continue
		}
		var res rpcReceipt
		if err := readJSON(r.receiptPath(id), &res); os.IsNotExist(err) {
			res = rpcReceipt{Op: id, Generation: r.s.Generation - 1, State: "uncertain", Error: &rpcError{Code: -32002, Message: "connection ended before receipt"}}
		} else if err != nil {
			return err
		}
		if err := r.commitReceipt(name, res); err != nil {
			return err
		}
	}
	return nil
}

// Nested calls may pump replies, but never start another client call.
func (r *runner) rpcFiles(repliesOnly bool) error {
	entries, err := os.ReadDir(r.mail(""))
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		id, ok := strings.CutSuffix(name, ".rpc")
		if !ok || !e.Type().IsRegular() {
			continue
		}
		if _, err := jobPath(r.dir, id); err != nil {
			continue
		}
		path := r.mail(name)
		var env envelope
		parseErr := readJSON(path, &env)
		if repliesOnly && env.Kind != "reply" {
			continue
		}
		if seen, err := fileExists(r.receiptPath(id)); err != nil {
			return err
		} else if seen {
			return os.Remove(path)
		}
		pending := r.mail(filepath.Join("pending", name))
		if busy, err := fileExists(pending); err != nil {
			return err
		} else if busy {
			continue
		}
		if err := os.Rename(path, pending); err != nil {
			return err
		}
		return r.finishRPC(name, id, env, parseErr)
	}
	return nil
}

func (r *runner) finishRPC(name, id string, env envelope, parseErr error) error {
	res := rpcReceipt{Op: id, Generation: r.s.Generation, State: "completed"}
	if parseErr == nil {
		res.Result, parseErr = r.executeRPC(id, env)
	}
	if len(res.Result) != 0 {
		filtered, err := r.c.secret.jsonOutput(res.Result)
		if err != nil {
			return err
		}
		res.Result = filtered
	}
	if parseErr != nil {
		res.State = "error"
		var remote *rpcError
		if errors.As(parseErr, &remote) {
			res.Error = remote
		} else {
			res.Error = &rpcError{Code: -32000, Message: parseErr.Error()}
		}
		if errors.Is(parseErr, errServerClosed) || errors.Is(parseErr, errRPCUncertain) || r.ctx.Err() != nil {
			res.State = "uncertain"
		}
	}
	if err := r.commitReceipt(name, res); err != nil {
		return err
	}
	if errors.Is(parseErr, errServerClosed) {
		return parseErr
	}
	return nil
}

const detachedReview = "cxrun/reviewDetached"

var errRPCUncertain = errors.New("RPC outcome uncertain")

func (r *runner) executeRPC(id string, e envelope) (json.RawMessage, error) {
	if r.claude != nil {
		return nil, errors.New("app-server RPC unavailable with Claude")
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
			if bytes.Equal(a.ID, e.Request) && time.Now().Before(a.Deadline) {
				if a.Method == agentAsk {
					path, err := answerPath(r.dir, a.ID)
					if err != nil {
						return nil, err
					}
					if err = writeJSON(path, fields{"answer": e.Result, "error": e.Error}); err != nil {
						return nil, err
					}
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
	r.s.Turn = ""
	r.s.LastCompleted = ""
	r.s.CompletedStatus = ""
	r.s.TurnStatus = ""
	r.s.InputKind = ""
	r.s.turnSummary = turnSummary{}
	r.deriveState()
	r.idleSince = time.Now()
	return atomicWrite(filepath.Join(r.dir, "last.md"), nil)
}

// Registration holds the source thread. The child owns the call and its receipt,
// so reconnecting after a lost response cannot create a second fork.
func (r *runner) fork(params fields, dst any) error {
	const id = "initial-fork"
	var res rpcReceipt
	if err := readJSON(r.receiptPath(id), &res); os.IsNotExist(err) {
		raw, marshalErr := json.Marshal(params)
		if marshalErr != nil {
			return marshalErr
		}
		e := envelope{Version: 1, Kind: "call", Method: "thread/fork", Params: raw}
		name := id + ".rpc"
		if writeErr := writeJSON(r.mail(filepath.Join("pending", name)), e); writeErr != nil {
			return writeErr
		}
		if finishErr := r.finishRPC(name, id, e, nil); finishErr != nil {
			return finishErr
		}
		if readErr := readJSON(r.receiptPath(id), &res); readErr != nil {
			return readErr
		}
	} else if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return json.Unmarshal(res.Result, dst)
}
