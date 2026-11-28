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
	"sort"
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
		for _, name := range names {
			if _, err := hs.get(name); err != nil {
				return err
			}
		}
		hostsCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		type factsReply struct {
			err   error
			data  []byte
			index int
		}
		replies := make(chan factsReply, len(names))
		for i, name := range names {
			go func() {
				hostCtx, cancel := context.WithTimeout(hostsCtx, 60*time.Second)
				defer cancel()
				r, err := hs[name].call(hostCtx, 30*time.Second, "facts")
				if err == nil && r.code != 0 {
					err = ctlExit(r.code)
				}
				replies <- factsReply{index: i, data: r.data, err: err}
			}()
		}
		enc := json.NewEncoder(os.Stdout)
		failures := make([]error, len(names))
		for range names {
			v := <-replies
			name := names[v.index]
			var row map[string]json.RawMessage
			if v.err == nil {
				v.err = json.Unmarshal(v.data, &row)
				if v.err == nil && row == nil {
					v.err = errors.New("expected host facts object")
				}
			}
			if v.err != nil {
				failures[v.index] = fmt.Errorf("%s: %w", name, v.err)
				if err := enc.Encode(fields{"host": name, "error": v.err.Error()}); err != nil {
					return err
				}
				continue
			}
			row["host"], _ = json.Marshal(name)
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
		return errors.Join(failures...)
	}
	if op == "outbox" {
		return ctlOutbox(ctx, hs, o)
	}
	name, id, e := resolveJob(ctx, args[0])
	if e != nil {
		return e
	}
	// A job still being submitted, or whose submission failed, has no node to
	// forward to: report it the way the other job commands do.
	if handled, observeErr := observeSubmission(ctx, name, id, op, 0); handled || observeErr != nil {
		return observeErr
	}
	h, e := hs.get(name)
	if e != nil {
		return e
	}
	action := []string{"control-action", op, id}
	if op == "say" {
		var override string
		argv, override, e = sayClaimOperands(argv)
		if e != nil {
			return ctlUsage(op, e)
		}
		if e = forgetResult(name, id); e != nil {
			return e
		}
		action = append(action, name)
		address := claimAddress{Host: name, Job: id}
		r, err := withRestartClaim(ctx, hs, h, address, address, override, func(info jobClaimInfo) (reply, error) {
			return h.invoke(ctx, controlCallIdle, false, append(append(action, info.wire()), argv...)...)
		})
		if err != nil {
			return err
		}
		return r.output()
	}
	r, e := h.invoke(ctx, controlCallIdle, false, append(action, argv...)...)
	if e != nil {
		return e
	}
	return r.output()
}

func controlFlags(op string) *flag.FlagSet {
	f := cliFlags(op)
	var o options
	o.flags(f, op)
	if op == "say" {
		f.String("module-override", "", "reason for overriding an overlapping module claim")
	}
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
	var flags []string
	var hasText bool
	for _, arg := range operands {
		key, value := arg.key, arg.value
		if key == "" {
			if op != "say" {
				flags = append(flags, value)
				continue
			}
			if hasText {
				return nil, errors.New("expected at most one text argument")
			}
			hasText = true
			if path, file := strings.CutPrefix(value, "@"); file {
				var readErr error
				value, readErr = readInputText(path)
				if readErr != nil {
					return nil, readErr
				}
			}
			flags = append(flags, "--cx-text", value)
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
			b, e := readSharedFile(value)
			if e != nil {
				return nil, e
			}
			flags = append(flags, "--cx-image", filepath.Ext(value), base64.StdEncoding.EncodeToString(b))
			continue
		}
		flags = append(flags, "--"+key+"="+value)
	}
	return flags, nil
}

func sayClaimOperands(args []string) ([]string, string, error) {
	var out []string
	var override string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if value, ok := strings.CutPrefix(arg, "--module-override="); ok {
			if strings.TrimSpace(value) == "" {
				return nil, "", errors.New("--module-override requires a reason")
			}
			override = value
			continue
		}
		n := 1
		switch arg {
		case "--cx-text":
			n = 2
		case "--cx-image":
			n = 3
		}
		out = append(out, args[i:i+n]...)
		i += n - 1
	}
	return out, override, nil
}

func nodeAction(ctx context.Context, a []string, binding ...jobClaimInfo) error {
	if len(a) < 2 || !isThinControl(a[0]) {
		return errors.New("invalid control action")
	}
	op, id := a[0], a[1]
	operands := a[2:]
	var host string
	if op == "say" {
		if len(operands) == 0 {
			return errors.New("missing execution host")
		}
		host, operands = operands[0], operands[1:]
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	if op == "say" {
		if _, e = checkNodeStateFormat(root); e != nil {
			return e
		}
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	if op == "interrupt" {
		var s status
		if e = loadJobStatusInto(dir, &s); e != nil {
			return e
		}
		if s.Engine == "cmd" {
			op = "close"
		}
	}
	var staging *os.File
	if op == "say" && len(binding) != 0 {
		staging, e = jobOperationLock(ctx, root, id)
		if e != nil {
			return e
		}
		defer func() { closeBestEffort(staging) }()
		if e = (jobIdentity{run: binding[0].RunID, key: binding[0].Directory}).checkDir(dir); e != nil {
			return e
		}
	}
	var argv []string
	var textPath string
	for i := 0; i < len(operands); i++ {
		switch operands[i] {
		case "--":
			argv = append(argv, operands[i:]...)
			i = len(operands)
		case "--cx-image", "--cx-text":
			if op != "say" || i+1 >= len(operands) {
				return errors.New("invalid attachment")
			}
			folder := filepath.Join(dir, "attachments")
			if e = os.MkdirAll(folder, 0700); e != nil {
				return e
			}
			data := []byte(operands[i+1])
			suffix := ".txt"
			isText := operands[i] == "--cx-text"
			i++
			if !isText {
				suffix = safeExt(string(data))
				if i+1 >= len(operands) {
					return errors.New("missing image data")
				}
				i++
				data, e = base64.StdEncoding.DecodeString(operands[i])
				if e != nil {
					return e
				}
			}
			path := filepath.Join(folder, fmt.Sprintf("%x%s", sha256.Sum256(data), suffix))
			if e = snapImmutable(path, data); e != nil {
				return e
			}
			if isText {
				textPath = path
			} else {
				argv = append(argv, "--image", path)
			}
		default:
			argv = append(argv, operands[i])
		}
	}
	if op == "say" {
		f := cliFlags(op)
		var opts options
		opts.flags(f, op)
		if e = f.Parse(argv); e != nil {
			return e
		}
		if f.NArg() != 0 {
			return errors.New("invalid say text attachment")
		}
		var text []string
		if textPath != "" {
			text = []string{"@" + textPath}
		}
		closeBestEffort(staging)
		staging = nil
		return say(ctx, dir, host, id, opts, text, binding...)
	}
	return run(ctx, append([]string{op, "--job", id}, argv...))
}

func say(ctx context.Context, dir, host, id string, opts options, args []string, binding ...jobClaimInfo) error {
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	if len(binding) != 0 && (binding[0].RunID != identity.run || binding[0].Directory != identity.key) {
		return errJobReplaced
	}
	if host != "" {
		if _, err = projectJobStatus(ctx, dir, &s, identity); err != nil {
			return err
		}
	}
	if handled, dispatchErr := opts.dispatch(ctx, dir, "say", args, &identity); handled {
		return dispatchErr
	}
	name, msg, err := prepareMessage(opts, args)
	if err != nil {
		return err
	}
	if host != "" {
		if isTerminal(s.State) {
			if len(msg.Input) != 0 && s.Engine != "cmd" {
				eng, engineErr := lookupEngine(s.Engine)
				if engineErr != nil {
					return engineErr
				}
				if engineErr = eng.checkInput(msg.Input); engineErr != nil {
					return engineErr
				}
			}
			if err = restartNode(ctx, []string{"resume", host, id, "", "", "", ""}, &identity, io.Discard, binding...); err != nil {
				return err
			}
		} else if readyResult(s) {
			if _, err = sealTurn(ctx, dir, s, s.Finished); err != nil {
				return err
			}
		}
	}
	if err = publishMessage(ctx, dir, name, msg, identity); err != nil {
		return err
	}
	fmt.Println(name)
	return nil
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
		var board []outboxRow
		for _, name := range hs.names() {
			h, e := hs.get(name)
			if e == nil {
				var r reply
				r, e = h.call(ctx, 30*time.Second, "outbox-all", name, o.project, o.since)
				if e == nil && r.code == 0 && (o.to != "" || o.latest) {
					board, e = appendOutboxRows(board, r.data, o.to)
				} else if e == nil {
					e = r.output()
				}
			}
			if e != nil {
				diagnosticf("%v %v\n", name, e)
				failed = true
			}
		}
		if o.latest {
			board = latestPerJob(board)
		}
		enc := json.NewEncoder(os.Stdout)
		for _, row := range board {
			if e := enc.Encode(row.raw); e != nil {
				return e
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
		identity, _, _, err := captureJobIdentity(dir)
		if err != nil {
			return false, err
		}
		if replayErr := replayTurnEvents(ctx, dir, s, identity, false); replayErr != nil {
			diagnosticf("cx: supplement outbox for %s: %v\n", id, replayErr)
		}
		return false, readMail(dir, since, func(m agentMail) error {
			return json.NewEncoder(os.Stdout).Encode(struct {
				Source string `json:"source_job"`
				agentMail
			}{Source: a[0] + ":" + id, agentMail: m})
		})
	})
}

// outboxRow is one outbox line kept verbatim; only the fields used for selection are decoded.
type outboxRow struct {
	Time   time.Time `json:"time"`
	Source string    `json:"source_job"`
	To     string    `json:"to"`
	raw    json.RawMessage
}

func appendOutboxRows(rows []outboxRow, data []byte, to string) ([]outboxRow, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var raw json.RawMessage
		if e := dec.Decode(&raw); e != nil {
			return rows, e
		}
		var row outboxRow
		if e := json.Unmarshal(raw, &row); e != nil {
			return rows, e
		}
		if to == "" || row.To == to {
			row.raw = raw
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// latestPerJob keeps each job's newest message, oldest first, so progress notes read as a board.
func latestPerJob(rows []outboxRow) []outboxRow {
	last := map[string]int{}
	for i, r := range rows {
		if j, ok := last[r.Source]; !ok || !rows[j].Time.After(r.Time) {
			last[r.Source] = i
		}
	}
	out := make([]outboxRow, 0, len(last))
	for i := range rows {
		if last[rows[i].Source] == i {
			out = append(out, rows[i])
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].Time.Before(out[b].Time) })
	return out
}
