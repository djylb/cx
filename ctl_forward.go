package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
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

func isThinControl(op string) bool {
	switch op {
	case "requests", "reply", "approve", "deny", "review", "undo", "compact", "settings", "mcp", "events", "plan", "diff", "limits", "usage", "models", "threads", "history", "rpc", "interrupt", "close", "say":
		return true
	}
	return false
}

func ctlForward(ctx context.Context, op string, args []string) error {
	var argv []string
	var o ctlOptions
	switch op {
	case "outbox":
		var e error
		o, e = parseCtl(op, args)
		if e != nil {
			return ctlUsage(op, e)
		}
		if o.any && len(o.pos) != 0 || !o.any && (len(o.pos) != 1 || o.project != "") {
			return ctlUsage(op, errors.New("outbox needs JOB or --any [--project P]"))
		}
	case "hosts":
		if len(args) > 1 || len(args) == 1 && strings.HasPrefix(args[0], "-") {
			return ctlUsage(op, errors.New("hosts accepts at most one host"))
		}
	default:
		if len(args) == 0 || strings.HasPrefix(args[0], "-") {
			return ctlUsage(op, fmt.Errorf("%s requires a job", op))
		}
		var e error
		argv, e = controlOperands(op, args[1:])
		if e != nil {
			return ctlUsage(op, e)
		}
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	if op == "hosts" {
		names := hs.names()
		if len(args) == 1 {
			names = args
		}
		// Query hosts concurrently with a per-host bound so one slow link cannot hold the others.
		type factsReply struct {
			err error
			r   reply
		}
		replies := make([]chan factsReply, len(names))
		for i, name := range names {
			h, hostErr := hs.get(name)
			if hostErr != nil {
				return hostErr
			}
			replies[i] = make(chan factsReply, 1)
			go func() {
				hostCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				r, err := h.call(hostCtx, 30*time.Second, "facts")
				if err == nil && r.code != 0 {
					err = r.output()
				}
				replies[i] <- factsReply{err, r}
			}()
		}
		var failed error
		for i, name := range names {
			v := <-replies[i]
			if v.err != nil {
				failed = errors.Join(failed, fmt.Errorf("%s: %w", name, v.err))
				if hostErr := emitRows([]byte(fmt.Sprintf(`{"error":%q}`, v.err.Error())), "host", name); hostErr != nil {
					return hostErr
				}
				continue
			}
			if hostErr := emitRows(v.r.data, "host", name); hostErr != nil {
				return hostErr
			}
		}
		if failed != nil {
			return failed
		}
		return nil
	}
	if op == "outbox" {
		return ctlOutbox(ctx, hs, o)
	}
	name, id, e := resolveJob(ctx, args[0])
	if e != nil {
		return e
	}
	h, e := hs.get(name)
	if e != nil {
		return e
	}
	if op == "say" {
		if e = forgetResult(name, id); e != nil {
			return e
		}
	}
	r, e := h.invoke(ctx, 185*time.Second, false, append([]string{"control-action", op, id}, argv...)...)
	if e != nil {
		return e
	}
	return r.output()
}

func controlFlags(op string) *flag.FlagSet {
	f := cliFlags(op)
	var o options
	o.flags(f, op)
	if isMarker(op) {
		f.String("cmd-id", "", "idempotency key (defaults to CXRUN_CMD_ID)")
		f.String("turn-id", "", "target turn (defaults to the active turn)")
	}
	return f
}

// File operands belong to the invoking controller. The wire already carries
// arbitrary UTF-8 arguments on stdin, including values with embedded newlines.
func controlOperands(op string, args []string) ([]string, error) {
	type operand struct{ key, value string }
	var operands []operand
	literal := false
	f := controlFlags(op)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !literal && isHelp(arg) {
			return nil, flag.ErrHelp
		}
		if !literal && arg == "--" {
			literal = true
			continue
		}
		if literal || !strings.HasPrefix(arg, "-") {
			operands = append(operands, operand{value: arg})
			continue
		}
		key, value, set := strings.Cut(arg, "=")
		key = strings.TrimLeft(key, "-")
		if f.Lookup(key) == nil {
			return nil, fmt.Errorf("unknown flag %s", arg)
		}
		if !set {
			if i+1 == len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			i++
			value = args[i]
		}
		operands = append(operands, operand{key, value})
	}
	var flags, pos []string
	for _, arg := range operands {
		key, value := arg.key, arg.value
		if key == "" {
			switch {
			case op != "say":
				flags = append(flags, value)
			case strings.HasPrefix(value, "@"):
				b, e := readInputText(value[1:])
				if e != nil {
					return nil, e
				}
				flags = append(flags, "--cx-text", b)
			default:
				pos = append(pos, value)
			}
			continue
		}
		switch key {
		case "params", "result", "error", "answers", "target", "input-json":
			b, e := rawArg(value)
			if e != nil {
				return nil, e
			}
			if key == "input-json" {
				warnInputCredentials(b)
			}
			value = string(b)
		case "image":
			b, e := os.ReadFile(value)
			if e != nil {
				return nil, e
			}
			flags = append(flags, "--cx-image", filepath.Ext(value), base64.StdEncoding.EncodeToString(b))
			continue
		}
		flags = append(flags, "--"+key+"="+value)
	}
	if op == "say" && len(pos) > 0 {
		flags = append(flags, "--")
		flags = append(flags, pos...)
	}
	return flags, nil
}

func nodeAction(ctx context.Context, a []string) error {
	if len(a) < 2 || !isThinControl(a[0]) {
		return errors.New("invalid control action")
	}
	op, id := a[0], a[1]
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	if op == "say" {
		var s status
		if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
			return e
		}
		if readyResult(s) {
			if _, e = sealTurn(ctx, dir, s, s.Finished); e != nil {
				return e
			}
		}
	}
	if op == "interrupt" {
		var s status
		if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
			return e
		}
		if s.Engine == "cmd" {
			op = "close"
		}
	}
	argv := []string{op, "--job", id}
	var textPath string
	for i := 2; i < len(a); i++ {
		switch a[i] {
		case "--":
			argv = append(argv, a[i:]...)
			i = len(a)
		case "--cx-image", "--cx-text":
			if op != "say" || i+1 >= len(a) {
				return errors.New("invalid attachment")
			}
			if _, statErr := os.Stat(filepath.Join(dir, "config.json")); statErr != nil {
				return statErr
			}
			folder := filepath.Join(dir, "attachments")
			if e = os.MkdirAll(folder, 0700); e != nil {
				return e
			}
			data := []byte(a[i+1])
			suffix := ".txt"
			isText := a[i] == "--cx-text"
			i++
			if !isText {
				suffix = string(data)
				if len(suffix) > 13 || strings.ContainsAny(suffix, "/\\:") {
					suffix = ""
				}
				if i+1 >= len(a) {
					return errors.New("missing image data")
				}
				i++
				data, e = base64.StdEncoding.DecodeString(a[i])
				if e != nil {
					return e
				}
			}
			path := filepath.Join(folder, fmt.Sprintf("%x%s", sha256.Sum256(data), suffix))
			if e = atomicWrite(path, data); e != nil {
				return e
			}
			if isText {
				textPath = path
			} else {
				argv = append(argv, "--image", path)
			}
		default:
			argv = append(argv, a[i])
		}
	}
	if textPath != "" {
		argv = append(argv, "--", "@"+textPath)
	}
	return run(ctx, argv)
}

func emitRows(data []byte, key, value string) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	enc := json.NewEncoder(os.Stdout)
	for {
		var row map[string]json.RawMessage
		e := dec.Decode(&row)
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if row == nil {
			return errors.New("expected object")
		}
		row[key], _ = json.Marshal(value)
		if e = enc.Encode(row); e != nil {
			return e
		}
	}
}

func ctlOutbox(ctx context.Context, hs endpoints, o ctlOptions) error {
	if o.since == "" {
		o.since = "0"
	}
	if _, e := strconv.ParseUint(o.since, 10, 64); e != nil {
		return e
	}
	if o.any && len(o.pos) == 0 {
		var failed bool
		for _, name := range hs.names() {
			h, e := hs.get(name)
			if e == nil {
				var r reply
				r, e = h.call(ctx, 30*time.Second, "outbox-all", name, o.project, o.since)
				if e == nil {
					e = r.output()
				}
			}
			if e != nil {
				diagnosticf("%v %v\n", name, e)
				failed = true
			}
		}
		if failed {
			return ctlExit(1)
		}
		return nil
	}
	name, id, e := resolveJob(ctx, o.pos[0])
	if e != nil {
		return e
	}
	h, e := hs.get(name)
	if e != nil {
		return e
	}
	r, e := h.call(ctx, 30*time.Second, "outbox", "--job", id, "--since", o.since)
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	return emitRows(r.data, "source_job", o.pos[0])
}

func nodeOutboxAll(ctx context.Context, a []string) error {
	if len(a) != 3 {
		return errors.New("outbox-all HOST PROJECT SINCE")
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	since, e := strconv.ParseUint(a[2], 10, 64)
	if e != nil {
		return e
	}
	return scanJobs(ctx, root, a[1], "", func(s status) (bool, error) {
		id := s.Job
		if s.Project != "" {
			id = s.Project + "/" + id
		}
		dir, e := jobDir(root, id)
		if e != nil {
			return false, e
		}
		return false, readMail(dir, since, func(m agentMail) error {
			return json.NewEncoder(os.Stdout).Encode(struct {
				Source string `json:"source_job"`
				agentMail
			}{Source: a[0] + ":" + id, agentMail: m})
		})
	})
}
