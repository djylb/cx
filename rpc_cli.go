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
	thread, turn, input, since, image, through     string
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
		{&o.input, "input-json", "start say"}, {&o.image, "image", "start say"}, {&o.through, "through", "fork"},
	} {
		if strings.Contains(" "+f.ops+" ", " "+op+" ") {
			fs.StringVar(f.p, f.name, "", f.name)
		}
	}
	if op == "reply" || op == "say" {
		fs.Uint64Var(&o.generation, "generation", 0, "expected connection generation")
	}
}
func rawArg(v string) (json.RawMessage, error) {
	b := []byte(v)
	var err error
	if strings.HasPrefix(v, "@") {
		b, err = os.ReadFile(v[1:])
		if err != nil {
			return nil, err
		}
	}
	if !json.Valid(b) {
		return nil, errors.New("expected JSON or @file")
	}
	return b, nil
}
func (o options) inputs(text string) ([]json.RawMessage, error) {
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
	if o.image != "" {
		path, err := filepath.Abs(o.image)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(fields{"type": "localImage", "path": path})
		a = append(a, b)
	}
	if len(a) == 0 {
		return nil, errors.New("empty input")
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

func (o options) dispatch(ctx context.Context, dir, op string, args []string) (bool, error) {
	if op == "events" || op == "diff" || op == "plan" {
		return true, o.events(dir, op)
	}
	if op != "say" && op != "reply" && op != "requests" && op != "rpc" && op != "settings" && op != "mcp" && aliases[op] == "" {
		return false, nil
	}
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return true, err
	}
	if op == "requests" {
		if s.Approvals == nil {
			s.Approvals = []approval{}
		}
		return true, json.NewEncoder(os.Stdout).Encode(s.Approvals)
	}
	if op == "say" && o.request == "" && o.answers == "" && s.InputKind != "server_request" {
		return false, nil
	}
	e := envelope{Version: 1, Kind: "call"}
	if op == "reply" || op == "say" {
		e.Kind = "reply"
		e.Generation = o.generation
		if e.Generation == 0 {
			e.Generation = s.Generation
		}
		var matched *approval
		for i, a := range s.Approvals {
			if op == "say" && !inputMethod(a.Method) {
				continue
			}
			raw := string(a.ID)
			var str string
			if o.request != "" && raw != o.request && (json.Unmarshal(a.ID, &str) != nil || str != o.request) {
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
		e.Request = matched.ID
		var err error
		switch {
		case matched.Method == agentAsk && op == "say" && len(args) == 1 && o.answers == "":
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
		case op == "say" && len(args) == 1 && matched.Method == "item/tool/requestUserInput" && len(matched.Questions) == 1:
			text := args[0]
			if strings.HasPrefix(text, "@") {
				text, err = readInputText(text[1:])
			}
			e.Result, _ = json.Marshal(fields{"answers": fields{matched.Questions[0].ID: fields{"answers": []string{text}}}})
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
			return true, o.publish(ctx, dir, s, e)
		} else {
			if len(args) > 0 {
				key += " " + strings.Join(args, " ")
			}
			e.Method = aliases[key]
		}
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
	return true, o.publish(ctx, dir, s, e)
}
func (o options) publish(ctx context.Context, dir string, s status, e envelope) error {
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
	path := filepath.Join(dir, "inbox", id+".rpc")
	out := filepath.Join(dir, "outbox", id+".json")
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	lock, err := lockUntil(ctx, filepath.Join(dir, "inbox", ".rpc-publish.lock"))
	if err != nil {
		return err
	}
	err = func() error {
		defer closeBestEffort(lock)
		for _, sub := range []string{"", "pending", "done", "uncertain"} {
			var old envelope
			readErr := readJSON(filepath.Join(dir, "inbox", sub, id+".rpc"), &old)
			if os.IsNotExist(readErr) {
				continue
			}
			if readErr != nil {
				return readErr
			}
			a, _ := json.Marshal(old)
			b, _ := json.Marshal(e)
			if string(a) != string(b) {
				return errors.New("op-id reused with different payload")
			}
			return nil
		}
		if isTerminal(s.State) || s.Engine == "cmd" {
			return errors.New("RPC requires a live Codex job")
		}
		return writeJSON(path, e)
	}()
	if err != nil {
		return err
	}
	for {
		var res rpcReceipt
		if err = readJSON(out, &res); err == nil {
			if err = json.NewEncoder(os.Stdout).Encode(res); err != nil {
				return err
			}
			if res.Error != nil {
				return res.Error
			}
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		if err = pause(ctx, 60*time.Millisecond); err != nil {
			return fmt.Errorf("rpcReceipt %s: %w", id, err)
		}
	}
}
func (o options) events(dir, op string) error {
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
			var s status
			if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
				return err
			}
			o.thread = s.Thread
		}
	}
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
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

func structuredSay(dir string, o options, args []string) error {
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	if s.Engine == "cmd" || isTerminal(s.State) {
		return errors.New("input requires a live Codex job")
	}
	text := ""
	if len(args) > 1 {
		return errors.New("expected at most one text argument")
	}
	if len(args) == 1 {
		text = args[0]
		if strings.HasPrefix(text, "@") {
			var err error
			text, err = readInputText(text[1:])
			if err != nil {
				return err
			}
		}
	}
	input, err := o.inputs(text)
	if err != nil {
		return err
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return err
	}
	name := fmt.Sprintf("%019d-%x.input.msg", time.Now().UnixNano(), nonce)
	if v := os.Getenv("CXRUN_MSG_NAME"); v != "" {
		if !msgName.MatchString(v) {
			return errors.New("invalid CXRUN_MSG_NAME")
		}
		name = strings.TrimSuffix(v, ".msg") + ".input.msg"
	}
	if err = writeJSON(filepath.Join(dir, "inbox", name), input); err != nil {
		return err
	}
	fmt.Println(name)
	return nil
}
