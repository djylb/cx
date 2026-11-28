package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type options struct {
	opID, params, method                           string
	request, result, rpcErr, answers               string
	scope, model, effort, target, delivery, before string
	thread, turn, input, since, through            string
	images                                         []string
	generation                                     uint64
}

func (o *options) flags(fs *flag.FlagSet, op string) {
	for _, f := range []struct {
		p    *string
		name string
		ops  string
	}{
		{&o.opID, "op-id", "rpc review compact undo settings mcp threads history usage limits models reply say"},
		{&o.params, "params", "rpc mcp threads history limits usage models"},
		{&o.request, "request", "reply say"}, {&o.result, "result", "reply"}, {&o.rpcErr, "error", "reply"}, {&o.answers, "answers", "say"},
		{&o.target, "target", "review"}, {&o.delivery, "delivery", "review"}, {&o.before, "before", "undo"}, {&o.scope, "scope", "settings"}, {&o.model, "model", "settings"}, {&o.effort, "effort", "settings"},
		{&o.method, "method", "events"}, {&o.thread, "thread", "events plan diff history"}, {&o.turn, "turn", "events plan diff history"}, {&o.since, "since", "events plan diff"},
		{&o.input, "input-json", "start say"}, {&o.through, "through", "fork"},
	} {
		if strings.Contains(" "+f.ops+" ", " "+op+" ") {
			fs.StringVar(f.p, f.name, "", f.name)
		}
	}
	if op == "start" || op == "say" {
		fs.Func("image", "attach an image file (repeatable)", func(v string) error { o.images = append(o.images, v); return nil })
	}
	if op == "reply" || op == "say" {
		fs.Uint64Var(&o.generation, "generation", 0, "expected connection generation")
	}
}
func rawArg(v string) (json.RawMessage, error) {
	b := []byte(v)
	var err error
	if strings.HasPrefix(v, "@") {
		b, err = readSharedFile(v[1:])
		if err != nil {
			return nil, err
		}
	}
	if !json.Valid(b) {
		return nil, errors.New("expected JSON or @file")
	}
	return b, nil
}
func (o *options) inputs(text string) ([]json.RawMessage, error) {
	parts, err := o.inputParts(text)
	if err == nil && len(parts) == 0 {
		err = errors.New("empty input")
	}
	return parts, err
}

func (o *options) inputParts(text string) ([]json.RawMessage, error) {
	var a []json.RawMessage
	if o.input != "" {
		b, err := rawArg(o.input)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &a); err != nil || a == nil {
			return nil, errors.New("expected UserInput array")
		}
	}
	if text != "" {
		b, _ := json.Marshal(fields{"type": "text", "text": text})
		a = append([]json.RawMessage{b}, a...)
	}
	for _, image := range o.images {
		path, err := filepath.Abs(image)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(fields{"type": "localImage", "path": path})
		a = append(a, b)
	}
	return a, nil
}

var aliases = map[string]string{
	"compact":        "thread/compact/start",
	"undo":           "thread/revert",
	"review":         "review/start",
	"limits":         "account/rateLimits/read",
	"usage":          "account/usage/read",
	"models":         "model/list",
	"threads":        "thread/list",
	"history":        "thread/read",
	"mcp list":       "mcpServerStatus/list",
	"mcp reload":     "config/mcpServer/reload",
	"mcp login":      "mcpServer/oauth/login",
	"mcp resource":   "mcpServer/resource/read",
	"mcp call":       "mcpServer/tool/call",
	"history turns":  "thread/turns/list",
	"history items":  "thread/items/list",
	"threads loaded": "thread/loaded/list",
	"history read":   "thread/read",
}

func (o *options) dispatch(ctx context.Context, dir, op string, args []string, expected *jobIdentity) (bool, error) {
	if op == "events" || op == "diff" || op == "plan" {
		return true, o.events(dir, op)
	}
	if op != "say" && op != "reply" && op != "requests" && op != "rpc" && op != "settings" && op != "mcp" && aliases[op] == "" {
		return false, nil
	}
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		return true, err
	}
	if expected != nil && identity != *expected {
		return true, errJobReplaced
	}
	if op == "requests" {
		if s.Approvals == nil {
			s.Approvals = []approval{}
		}
		return true, json.NewEncoder(os.Stdout).Encode(s.Approvals)
	}
	var original *envelope
	if (op == "reply" || op == "say") && o.opID != "" {
		original, err = publishedReply(dir, o.opID)
		if err != nil {
			return true, err
		}
	}
	if op == "say" && original == nil && o.request == "" && o.answers == "" && s.InputKind != "server_request" {
		return false, nil
	}
	e := envelope{Version: 1, Kind: "call"}
	if op == "reply" || op == "say" {
		e.Kind = "reply"
		e.Generation = o.generation
		var matched *approval
		if original != nil {
			if !matchesRequest(original.Request, o.request) {
				return true, errors.New("op-id reused with a different request")
			}
			e.Method, e.Request, e.Question = original.Method, original.Request, original.Question
			if e.Generation == 0 {
				e.Generation = original.Generation
			}
		} else {
			if e.Generation == 0 {
				e.Generation = s.Generation
			}
			for i, a := range s.Approvals {
				if (op == "say" && !inputMethod(a.Method)) || !matchesRequest(a.ID, o.request) {
					continue
				}
				if matched != nil {
					return true, errors.New("ambiguous request ID")
				}
				matched = &s.Approvals[i]
			}
			if matched == nil {
				return true, errors.New("no matching pending request")
			}
			e.Method, e.Request = matched.Method, matched.ID
			if matched.Method == "item/tool/requestUserInput" && len(matched.Questions) == 1 {
				e.Question = matched.Questions[0].ID
			}
		}
		var err error
		switch {
		case e.Method == agentAsk && op == "say" && len(args) == 1 && o.answers == "":
			text := args[0]
			if strings.HasPrefix(text, "@") {
				text, err = readInputText(text[1:])
			}
			e.Result, _ = json.Marshal(text)
		case o.rpcErr != "":
			if o.result != "" {
				return true, errors.New("result and error are exclusive")
			}
			e.Error, err = rawArg(o.rpcErr)
		case o.result != "":
			e.Result, err = rawArg(o.result)
		case o.answers != "":
			e.Result, err = rawArg(o.answers)
		case op == "say" && len(args) == 1 && e.Method == "item/tool/requestUserInput" && e.Question != "":
			text := args[0]
			if strings.HasPrefix(text, "@") {
				text, err = readInputText(text[1:])
			}
			e.Result, _ = json.Marshal(fields{"answers": fields{e.Question: fields{"answers": []string{text}}}})
		default:
			return true, errors.New("supply an explicit reply payload")
		}
		if err != nil {
			return true, err
		}
	} else {
		key := op
		if op == "rpc" {
			if len(args) != 1 {
				return true, errors.New("rpc requires METHOD")
			}
			e.Method = args[0]
			e.Params = json.RawMessage("{}")
			if o.params != "" {
				var err error
				e.Params, err = rawArg(o.params)
				if err != nil {
					return true, err
				}
			}
			return true, o.publish(ctx, dir, identity, e)
		}
		if len(args) > 0 {
			key += " " + strings.Join(args, " ")
		}
		e.Method = aliases[key]
		p := map[string]json.RawMessage{}
		set := func(k, v string) { p[k], _ = json.Marshal(v) }
		if o.params != "" {
			b, err := rawArg(o.params)
			if err != nil {
				return true, err
			}
			if err = json.Unmarshal(b, &p); err != nil || p == nil {
				return true, errors.New("params must be an object")
			}
		}
		switch op {
		case "history", "compact", "review", "undo", "settings":
			if _, ok := p["threadId"]; !ok {
				set("threadId", s.Thread)
			}
			if o.thread != "" {
				set("threadId", o.thread)
			}
			if o.turn != "" {
				set("turnId", o.turn)
			}
		}
		switch op {
		case "settings":
			if (o.scope != "turn" && o.scope != "thread") || (o.model == "" && o.effort == "") {
				return true, errors.New("settings requires scope and model or effort")
			}
			e.Method = o.scope + "/settings/update"
			if o.scope == "turn" {
				set("turnId", s.Turn)
			}
			if o.model != "" {
				set("model", o.model)
			}
			if o.effort != "" {
				set("effort", o.effort)
			}
		case "undo":
			if o.before == "" {
				return true, errors.New("undo requires --before")
			}
			set("beforeTurnId", o.before)
		case "review":
			v, err := rawArg(o.target)
			if err != nil {
				return true, err
			}
			p["target"] = v
			switch o.delivery {
			case "detached":
				e.Method = detachedReview
			case "", "inline":
				set("delivery", "inline")
			default:
				return true, errors.New("review delivery must be inline or detached")
			}
		}
		if e.Method == "" {
			return true, fmt.Errorf("unknown operation %s", key)
		}
		e.Params, _ = json.Marshal(p)
	}
	return true, o.publish(ctx, dir, identity, e)
}
func matchesRequest(id json.RawMessage, option string) bool {
	var decoded string
	return option == "" || string(id) == option || (json.Unmarshal(id, &decoded) == nil && decoded == option)
}

func publishedReply(dir, id string) (*envelope, error) {
	inbox := filepath.Join(dir, "inbox")
	record, err := findMailboxID(inbox, filepath.Join(inbox, ".ids", snapSum([]byte(id))), id)
	if err != nil || record.Kind != "rpc" {
		return nil, err
	}
	var original envelope
	if err := json.Unmarshal(record.Value, &original); err != nil {
		return nil, err
	}
	if original.Kind != "reply" {
		return nil, nil
	}
	return &original, nil
}

func (o *options) publish(ctx context.Context, dir string, identity jobIdentity, e envelope) error {
	id := o.opID
	if id == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return err
		}
		id = hex.EncodeToString(b[:])
	}
	if _, err := jobPath(dir, id); err != nil {
		return err
	}
	if id == initialForkOp {
		return errors.New("operation ID is reserved")
	}
	ctx, cancel := context.WithTimeout(ctx, rpcLimit)
	defer cancel()
	record, err := newRecord("rpc", e)
	if err != nil {
		return err
	}
	record.ID = id
	if _, err = appendJobMailbox(ctx, dir, "inbox", &identity, record, func(_ *stateRecord, previous stateRecord, s status) error {
		if previous.Seq != 0 {
			return nil
		}

		eng, engineErr := liveAgentEngine(s)
		if engineErr != nil {
			return engineErr
		}
		if !eng.caps().RPC {
			return eng.unsupported("engine RPC")
		}
		return nil
	}); err != nil {
		return err
	}
	receipt, err := waitReceipt(ctx, dir, identity, func(record receiptRecord) bool { return record.Op == id })
	if err != nil {
		return fmt.Errorf("RPC receipt %s: %w", id, err)
	}
	res := rpcReceipt{Op: receipt.Op, State: receipt.Status, Result: receipt.Answer,
		Error: receipt.Error, Generation: receipt.Generation}
	if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func waitReceipt(ctx context.Context, dir string, identity jobIdentity, match func(receiptRecord) bool) (receiptRecord, error) {
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		return receiptRecord{}, err
	}
	defer closeBestEffort(tail)
	for {
		if err := identity.checkDir(dir); err != nil {
			return receiptRecord{}, err
		}
		records, err := tail.Next()
		if err != nil {
			return receiptRecord{}, err
		}
		if err := identity.checkDir(dir); err != nil {
			return receiptRecord{}, err
		}
		for _, record := range records {
			if record.Kind == "receipt" {
				var receipt receiptRecord
				if err := json.Unmarshal(record.Value, &receipt); err != nil {
					return receiptRecord{}, err
				}
				if match(receipt) {
					return receipt, nil
				}
			}
		}
		if len(records) != 0 && records[len(records)-1].Final {
			return receiptRecord{}, errors.New("job ended before receipt")
		}
		if err := pause(ctx, 60*time.Millisecond); err != nil {
			return receiptRecord{}, err
		}
	}
}

func (o *options) events(dir, op string) error {
	var since uint64
	if o.since != "" {
		var err error
		since, err = strconv.ParseUint(o.since, 10, 64)
		if err != nil {
			return err
		}
	}
	if op != "events" {
		o.method = "turn/" + op + "/updated"
		if o.thread == "" {
			s, _, err := loadJobStatus(dir)
			if err != nil {
				return err
			}
			o.thread = s.Thread
		}
	}
	f, err := openJobFile(filepath.Join(dir, "events.jsonl"), jobFileRead)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	br := bufio.NewReader(f)
	var last json.RawMessage
	for {
		line, err := br.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var v struct {
			Method string
			Params struct {
				ThreadID, TurnID string
				Turn             struct{ ID string }
			}
			Seq uint64
		}
		if err = json.Unmarshal(line, &v); err != nil {
			return err
		}
		if v.Params.TurnID == "" {
			v.Params.TurnID = v.Params.Turn.ID
		}
		if v.Seq <= since || o.method != "" && v.Method != o.method || o.thread != "" && v.Params.ThreadID != o.thread || o.turn != "" && v.Params.TurnID != o.turn {
			continue
		}
		if op == "events" {
			if _, err = os.Stdout.Write(line); err != nil {
				return err
			}
		} else {
			last = line
		}
	}
	if op != "events" {
		return json.NewEncoder(os.Stdout).Encode(fields{"observed": last != nil, "event": last})
	}
	return nil
}
