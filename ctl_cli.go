package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type ctlOptions struct {
	contextWarn, usageTop, usageTurns, usageContext                                 string
	modules, accept                                                                 []string
	moduleOverride, changeset, acceptTimeout                                        string
	waitJobsByHost                                                                  map[string][]string
	env                                                                             map[string]string
	project, session, tag, cwd, dir, host                                           string
	child, through                                                                  string
	engine, model, effort, sandbox, role, parent                                    string
	batch, snapshot, history, previous, input, profile                              string
	quiet, timeout, idle, turnLimit, approval, stall, queue                         string
	turn, sinceTurn, cursor, since, kind, mention, repo, script, shell, context, to string
	hosts, projects, channels, sets, argv, pos, images, own                         []string
	interval                                                                        time.Duration
	max                                                                             int
	cleanBase, all, explicitSession, noSession                                      bool
	full, json, log, path, watch, any, wait, raw, refresh, fromStart                bool
	inplace, interactive, web, hard, network, noNetwork                             bool
	command, reuse, sync, force, noTools, messages, latest, now                     bool
	incremental, allowConflicts, brief, usage, active, noAccept, reads              bool
}

func ctlFlags(op string, o *ctlOptions) *flag.FlagSet {
	f := cliFlags(op)
	cmd := ctlCommands[op]
	bools := map[string]*bool{
		"usage": &o.usage, "active": &o.active,
		"no-accept": &o.noAccept, "reads": &o.reads,
		"incremental": &o.incremental, "allow-conflicts": &o.allowConflicts,
		"all": &o.all, "clean-base": &o.cleanBase,
		"no-session": &o.noSession,
		"refresh":    &o.refresh, "cmd": &o.command, "reuse": &o.reuse,
		"inplace": &o.inplace, "interactive": &o.interactive, "web-search": &o.web,
		"hard": &o.hard, "network": &o.network, "no-network": &o.noNetwork,
		"force": &o.force, "sync": &o.sync, "no-tools": &o.noTools, "now": &o.now,
		"brief": &o.brief, "full": &o.full, "json": &o.json, "log": &o.log, "path": &o.path, "watch": &o.watch,
		"messages": &o.messages, "any": &o.any, "wait": &o.wait, "raw": &o.raw, "from-start": &o.fromStart, "latest": &o.latest,
	}
	stringsByName := map[string]*string{
		"context-warn": &o.contextWarn, "usage-top": &o.usageTop, "usage-turns": &o.usageTurns, "usage-context": &o.usageContext,
		"module-override": &o.moduleOverride, "changeset": &o.changeset, "accept-timeout": &o.acceptTimeout,
		"type": &o.kind, "mention": &o.mention, "repo": &o.repo,
		"engine": &o.engine, "model": &o.model, "effort": &o.effort,
		"sandbox": &o.sandbox, "role": &o.role, "parent": &o.parent,
		"batch": &o.batch, "snapshot": &o.snapshot, "with-history": &o.history,
		"profile": &o.profile, "dir": &o.dir, "continue": &o.previous, "input-json": &o.input,
		"idle-exit": &o.idle, "turn-timeout": &o.turnLimit,
		"approval-timeout": &o.approval, "stall": &o.stall, "queue-timeout": &o.queue,
		"script": &o.script, "shell": &o.shell, "project": &o.project,
		"session": &o.session, "tag": &o.tag, "cwd": &o.cwd,
		"job": &o.child, "through": &o.through,
		"timeout": &o.timeout, "quiet-timeout": &o.quiet, "turn": &o.turn,
		"cursor": &o.cursor, "since": &o.since, "since-turn": &o.sinceTurn, "context": &o.context, "to": &o.to,
	}
	const defaultPreviewBytes = 8192
	o.max = defaultPreviewBytes
	for k := range strings.FieldsSeq(cmd.flags) {
		if op == "wait" && k == "project" {
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.projects = append(o.projects, v); return nil })
			continue
		}
		if op == "wait" && k == "job" {
			f.Func(k, ctlFlagHelp(op, k), func(v string) error {
				host, id, ok := strings.Cut(v, ":")
				if !ok || host == "" || validateWaitJob(id) != nil {
					return fmt.Errorf("expected HOST:PROJECT/JOB, got %q", v)
				}
				if o.waitJobsByHost == nil {
					o.waitJobsByHost = make(map[string][]string)
				}
				o.waitJobsByHost[host] = append(o.waitJobsByHost[host], id)
				return nil
			})
			continue
		}
		if p := bools[k]; p != nil {
			f.BoolVar(p, k, false, ctlFlagHelp(op, k))
			continue
		}
		if p := stringsByName[k]; p != nil {
			f.StringVar(p, k, "", ctlFlagHelp(op, k))
			continue
		}
		switch k {
		case "max-bytes":
			f.IntVar(&o.max, k, defaultPreviewBytes, "preview budget in bytes")
		case "all-hosts":
			f.Bool(k, false, ctlFlagHelp(op, k))
		case "interval":
			o.interval = time.Second
			f.Func(k, ctlFlagHelp(op, k), func(v string) error {
				seconds, err := strconv.ParseFloat(v, 64)
				nanos := seconds * float64(time.Second)
				if err != nil || !(nanos >= 1 && nanos < float64(math.MaxInt64)) {
					return errors.New("interval must be positive and fit in a duration")
				}
				o.interval = time.Duration(nanos)
				return nil
			})
		case "host":
			if op == "rebase" {
				f.StringVar(&o.host, k, "", "execution host for the follow-up")
				continue
			}
			if op == "agent" {
				f.StringVar(&o.host, k, "auto", "execution host (auto uses CX_DEFAULT_HOST, project configuration, or local)")
				continue
			}
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.hosts = append(o.hosts, v); return nil })
		case "chat":
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.channels = append(o.channels, v); return nil })
		case "env":
			f.Func(k, ctlFlagHelp(op, k), func(v string) error {
				if err := addCommandEnv(&o.env, v); err != nil {
					return err
				}
				key, _, _ := strings.Cut(v, "=")
				if sensitiveKey(key) {
					warnCredentialKey(key)
				}
				return nil
			})
		case "set":
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.sets = append(o.sets, v); return nil })
		case "module", "accept":
			target := &o.modules
			if k == "accept" {
				target = &o.accept
			}
			f.Func(k, ctlFlagHelp(op, k), func(v string) error {
				if strings.TrimSpace(v) == "" {
					return fmt.Errorf("--%s requires a non-empty value", k)
				}
				*target = append(*target, v)
				return nil
			})
		case "own":
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.own = append(o.own, v); return nil })
		case "image":
			f.Func(k, ctlFlagHelp(op, k), func(v string) error { o.images = append(o.images, v); return nil })
		default:
			panic("unregistered flag " + k)
		}
	}
	return f
}

func parseCtl(op string, args []string) (ctlOptions, error) {
	var o ctlOptions
	f := ctlFlags(op, &o)
	hasSeparator := false
	if i := slices.Index(args, "--"); i >= 0 {
		hasSeparator = true
		o.argv = args[i+1:]
		args = args[:i]
	}
	var flags []string
	for i := 0; i < len(args); i++ {
		v := args[i]
		if isHelp(v) {
			return o, flag.ErrHelp
		}
		if v == "-" || !strings.HasPrefix(v, "-") {
			o.pos = append(o.pos, v)
			continue
		}
		key, _, assigned := strings.Cut(strings.TrimLeft(v, "-"), "=")
		opt := f.Lookup(key)
		if opt == nil {
			return o, fmt.Errorf("unknown flag %s", v)
		}
		if key == "with-history" && !assigned {
			next := ""
			if i+1 < len(args) {
				next = args[i+1]
			}
			if next != "HEAD" && (strings.HasPrefix(next, "-") || !strings.HasSuffix(next, "..HEAD")) {
				flags = append(flags, "--with-history=HEAD")
				continue
			}
		}
		flags = append(flags, v)
		if _, ok := opt.Value.(interface{ IsBoolFlag() bool }); !ok && !assigned {
			i++
			if i == len(args) {
				return o, fmt.Errorf("%s needs a value", v)
			}
			flags = append(flags, args[i])
			if key == "chat" || key == "env" || (key == "host" || key == "project") && op == "wait" {
				for i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
					i++
					flags = append(flags, "--"+key, args[i])
				}
			}
		}
	}
	if hasSeparator && op != "exec" && op != "run" && op != "submit" {
		return o, errors.New("unexpected argument separator")
	}
	e := f.Parse(flags)
	f.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "host":
			if op == "rebase" && o.host == "" {
				e = errors.New("--host requires a host name")
			}
		case "session":
			o.explicitSession = true
		case "module-override", "changeset", "accept-timeout", "context-warn", "usage-top", "usage-turns", "usage-context":
			if strings.TrimSpace(fl.Value.String()) == "" {
				e = fmt.Errorf("--%s requires a non-empty value", fl.Name)
			}
		case "since-turn":
			if n, err := strconv.ParseInt(o.sinceTurn, 10, 64); err != nil || n < 1 {
				e = errors.New("--since-turn requires a positive turn number")
			}
		}
	})
	if o.sinceTurn != "" && o.incremental {
		return o, errors.New("--since-turn and --incremental are mutually exclusive")
	}
	if op != "patch" && o.sinceTurn != "" && o.turn != "" {
		return o, errors.New("--since-turn and --turn are mutually exclusive")
	}
	if o.active && (o.explicitSession || len(o.pos) != 0 || o.changeset != "") {
		return o, errors.New("--active lists all hosts and sessions and cannot be combined with a job, --session, or --changeset")
	}
	if o.all && o.explicitSession {
		return o, errors.New("--all and --session are mutually exclusive")
	}
	if o.noSession && o.explicitSession {
		return o, errors.New("--session and --no-session are mutually exclusive")
	}
	if o.explicitSession && strings.TrimSpace(o.session) == "" && (op == "wait" || f.Lookup("no-session") != nil) {
		return o, errors.New("--session requires a non-empty name")
	}
	if op == "agent" && o.previous != "" && (o.explicitSession || o.noSession) {
		return o, errors.New("session options apply to new jobs")
	}
	if o.raw && o.messages {
		return o, errors.New("--raw and --messages are mutually exclusive")
	}
	if o.fromStart && o.since != "" {
		return o, errors.New("--from-start and --since are mutually exclusive")
	}
	if op == "result" {
		if o.all && (!o.reads || o.turn != "") {
			return o, errors.New("--all requires --reads without --turn")
		}
		var outputModes int
		for _, enabled := range []bool{o.full, o.json, o.log, o.path, o.reads} {
			if enabled {
				outputModes++
			}
		}
		if outputModes > 1 {
			return o, errors.New("--full, --json, --log, --path and --reads are mutually exclusive")
		}
	}
	if o.max < 0 {
		return o, errors.New("negative output budget")
	}
	return o, e
}
func defaultProject(ctx context.Context) (string, error) {
	return projectAt(ctx, ".")
}
func projectAt(ctx context.Context, dir string) (string, error) {
	name, _, e := resolveProject(ctx, dir, "")
	return name, e
}
func controller(ctx context.Context, a []string) error {
	if len(a) == 1 && isHelp(a[0]) {
		printCtlHelp(os.Stdout, "")
		return nil
	}
	if len(a) == 0 {
		return ctlUsage("", errors.New("command required"))
	}
	if a[0] == "--max-bytes" && len(a) >= 3 {
		a = append(append([]string{}, a[2:]...), "--max-bytes", a[1])
	} else if strings.HasPrefix(a[0], "--max-bytes=") && len(a) > 1 {
		a = append(append([]string{}, a[1:]...), a[0])
	}
	op := a[0]
	if _, ok := ctlCommands[op]; !ok || strings.HasPrefix(op, "chat-") || strings.HasPrefix(op, "project-") {
		return ctlUsage("", fmt.Errorf("unknown cx command %q", op))
	}
	if len(a) > 1 && isHelp(a[1]) {
		return ctlUsage(op, flag.ErrHelp)
	}
	if op == "chat" {
		if len(a) < 2 {
			return ctlUsage(op, errors.New("chat requires an action"))
		}
		action := "chat-" + a[1]
		if _, ok := ctlCommands[action]; !ok {
			return ctlUsage(op, fmt.Errorf("unknown chat action %q", a[1]))
		}
		o, e := parseCtl(action, a[2:])
		if e != nil {
			return ctlUsage(action, e)
		}
		hs, e := loadEndpoints()
		if e != nil {
			return e
		}
		return ctlChat(ctx, hs, a[1], o)
	}
	if op == "test" || op == "gate" {
		if op == "gate" && (len(a) < 2 || strings.HasPrefix(a[1], "-")) {
			return ctlUsage(op, errors.New("gate requires DIR"))
		}
		if op == "gate" && len(a) > 2 && isHelp(a[2]) {
			return ctlUsage(op, flag.ErrHelp)
		}
		return legacyGate(ctx, a)
	}
	if op == "project" {
		return ctlProject(ctx, a[1:])
	}
	if op == "context" {
		return ctlContext(ctx, a[1:])
	}
	if op == "version" {
		if len(a) != 1 {
			return ctlUsage(op, errors.New("version takes no arguments"))
		}
		return printVersion()
	}
	if isThinControl(op) || op == "hosts" || op == "outbox" {
		return ctlForward(ctx, op, a[1:])
	}
	o, e := parseCtl(op, a[1:])
	if e != nil {
		return ctlUsage(op, e)
	}
	if op == "adopt" {
		if !o.explicitSession {
			o.session = currentSession()
		}
		if strings.TrimSpace(o.session) == "" {
			return ctlUsage(op, errors.New("current session unknown; set CX_SESSION or use --session NAME"))
		}
	}
	if op == "fork" || op == "resume" {
		return ctlRestart(ctx, op, o)
	}
	if op == "wait" && !o.any && (o.all || o.explicitSession || len(o.hosts) > 0 || len(o.projects) > 0 || len(o.waitJobsByHost) > 0 || o.cursor != "" || len(o.channels) > 0 || o.fromStart) {
		return ctlUsage(op, errors.New("wait filters require --any"))
	}
	if op == "wait" && o.any {
		if err := validateWaitProjects(o.projects); err != nil {
			return ctlUsage(op, err)
		}
	}
	if (op == "ls" || op == "overview" || op == "wait" && o.any) && !o.all && !o.active && !o.explicitSession {
		o.session = currentSession()
		if o.session == "" {
			if op == "wait" {
				return ctlUsage(op, errors.New("current session unknown; set CX_SESSION or use --session or --all"))
			}
			diagnosticf("cx %s: current session unknown, showing jobs from all sessions\n", op)
		}
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	switch {
	case op == "mirror":
		return mirror(ctx, hs, o)
	case op == "run" || op == "submit" || op == "agent" || op == "snapshot":
		return ctlSubmit(ctx, hs, op, o)
	case op == "doctor" || op == "ls" || op == "overview":
		return displayHosts(ctx, hs, op, o)
	case op == "wait" && o.any:
		return ctlWaitAny(ctx, hs, o)
	}
	if op == "rebase" {
		if len(o.pos) != 2 {
			return ctlUsage(op, errors.New("rebase requires JOB and TEXT"))
		}
		if o.profile != "" && o.host == "" {
			return ctlUsage(op, errors.New("--profile requires --host"))
		}
		if o.host != "" {
			if _, e = hs.get(o.host); e != nil {
				return ctlUsage(op, e)
			}
		}
		o.session, e = o.submissionSession()
		if e != nil {
			return ctlUsage(op, e)
		}
		o.dir = o.pos[1]
		o.pos = o.pos[:1]
	}
	if (op == "apply" || op == "refresh") && len(o.pos) == 2 {
		o.dir = o.pos[1]
		o.pos = o.pos[:1]
	}
	if len(o.pos) != 1 {
		return ctlUsage(op, fmt.Errorf("%s requires one host or job", op))
	}
	name, id := o.pos[0], ""
	if op != "exec" && op != "setup" && op != "host" {
		name, id, e = resolveJob(ctx, name)
		if e != nil {
			return e
		}
	}
	h, e := hs.get(name)
	if e != nil {
		return e
	}
	if id != "" && op != "clean" && (op != "result" || !o.wait) {
		var d time.Duration
		switch op {
		case "tail", "wait":
			d, e = observation(o.timeout)
			if e != nil {
				return e
			}
		case "adopt":
			d = adoptLimit
		}
		started := time.Now()
		if handled, observeErr := observeSubmission(ctx, name, id, op, d); handled || observeErr != nil {
			return observeErr
		}
		if op == "wait" || op == "tail" {
			remaining := d - time.Since(started)
			if remaining <= 0 {
				return ctlExit(124)
			}
			o.timeout = remaining.String()
		}
	}
	switch op {
	case "adopt":
		return ctlAdopt(ctx, h, id, o.session)
	case "clean":
		return cleanControllerJob(ctx, h, name, id)
	case "refresh":
		return ctlRefresh(ctx, hs, h, name, id, o)
	case "rebase":
		return ctlRebase(ctx, h, name, id, o)
	case "result", "patch", "apply":
		return ctlResult(ctx, name, id, op, o)
	case "setup":
		return retryTransfer(ctx, func() error { return deployOnce(ctx, hs, name, true) })
	case "host":
		r, callErr := h.call(ctx, 30*time.Second, append([]string{"host-config"}, o.sets...)...)
		if callErr != nil {
			return callErr
		}
		return r.output()
	case "exec":
		if o.script != "" && len(o.argv) > 0 || o.script == "" && o.shell != "" {
			return ctlUsage(op, errors.New("exec uses --script FILE [--shell SHELL] or -- ARGV"))
		}
		if o.script == "" && (len(o.argv) == 0 || o.argv[0] == "") {
			return ctlUsage(op, errors.New("exec requires -- ARGV"))
		}
		if o.project == "" {
			o.project, e = defaultProject(ctx)
			if e != nil {
				return e
			}
		}
		if o.tag == "" {
			var nonce [4]byte
			if _, e = rand.Read(nonce[:]); e != nil {
				return e
			}
			o.tag = fmt.Sprintf("%s-%x", time.Now().Format("20060102-150405"), nonce)
		}
		id = o.project + "/" + o.tag
		if _, e = jobDir("", id); e != nil {
			return e
		}
		if o.reuse {
			q := reuseQuery{Host: name, ID: id, Dir: o.cwd, Argv: o.argv, Shell: o.shell, Env: o.env, Profile: o.profile}
			if o.script != "" {
				q.ScriptHash, e = fileHash(o.script)
				if e != nil {
					return e
				}
			}
			found, reuseErr := lookupReuse(ctx, h, q)
			if reuseErr != nil || found {
				return reuseErr
			}
		}
		o.session, e = o.submissionSession()
		if e != nil {
			return ctlUsage(op, e)
		}
		args := []string{"submit-command", id, o.cwd, "--job-host", name}
		for k, v := range o.env {
			args = append(args, "--env", k+"="+v)
		}
		for _, kv := range [][2]string{{"--profile", o.profile}, {"--session", o.session}, {"--queue-timeout", o.queue}, {"--quiet-timeout", o.quiet}, {"--timeout", o.timeout}} {
			if kv[1] != "" {
				args = append(args, kv[0], kv[1])
			}
		}
		if o.now {
			args = append(args, "--now")
		}
		var r reply
		if o.script != "" {
			if h.SSH != "" && h.Platform == "" {
				if e = deploy(ctx, hs, name); e != nil {
					return e
				}
				h = hs[name]
			}
			r, e = submitScript(ctx, h, args, o.script, o.shell)
		} else {
			r, e = submitCommand(ctx, hs, name, append(append(args, "--"), o.argv...))
		}
		h = hs[name]
		if e != nil {
			return e
		}
		if r.code != 0 {
			return r.output()
		}
		dst := os.Stdout
		if o.wait {
			dst = os.Stderr
		}
		if e = forgetResult(name, id); e != nil {
			return e
		}
		if e = rememberRoute(id, name); e != nil {
			return e
		}
		if _, e = fmt.Fprintln(dst, id); e != nil {
			return e
		}
		if !o.wait {
			return nil
		}
		var ack status
		if e = json.Unmarshal(r.data, &ack); e != nil {
			return e
		}
		return follow(ctx, h, name, id, ctlOptions{raw: true, wait: true, cursor: "exec-" + ack.RunID})
	case "tail":
		return follow(ctx, h, name, id, o)
	case "status":
		r, e := h.call(ctx, 30*time.Second, "status", "--job", id)
		if e != nil {
			return e
		}
		return r.output()
	case "show":
		r, e := h.call(ctx, 30*time.Second, "show", id, o.turn, fmt.Sprint(o.json || o.full))
		if e != nil {
			return e
		}
		if r.code != 0 {
			return r.output()
		}
		return r.output()
	case "wait":
		limit, e := observation(o.timeout)
		if e != nil {
			return e
		}
		return waitOne(ctx, h, name, id, limit)
	}
	return errors.New("invalid command")
}
func waitOne(ctx context.Context, h endpoint, host, id string, limit time.Duration) error {
	clock := timeshim.Or(contextClock(ctx))
	deadline := clock.Now().Add(limit)
	marker, seen, cacheErr := resultMarkerSnapshot(host, id)
	r, e := h.call(ctx, limit+2*time.Second, "observe-one", id, limit.String())
	if e != nil {
		return e
	}
	if r.code == exitQueued {
		return printQueuedWait(ctx, h, id, r)
	}
	if r.code == 6 {
		var notice waitObservation
		if e = json.Unmarshal(r.data, &notice); e != nil {
			return e
		}
		if _, e = fmt.Printf("job=%s status=waiting_input input_kind=%s\n", notice.Job, notice.InputKind); e != nil {
			return e
		}
		return acknowledgeWait(ctx, h, id, notice, r.code)
	}
	// The node appends the observation to the ordinary wait diagnostic.
	first, last, ok := bytes.Cut(r.data, []byte("\n{"))
	if ok {
		var s waitObservation
		if e = json.Unmarshal(append([]byte("{"), last...), &s); e != nil {
			return e
		}
		if cacheErr == nil && s.Result != nil {
			defer func() {
				prefetchCtx, cancel := timeshim.WithDeadline(ctx, clock, deadline)
				defer cancel()
				prefetchResult(prefetchCtx, h, host, id, marker, seen, *s.Result)
			}()
		}
		if isTerminal(s.State) {
			fmt.Printf("job=%s status=%s completed_turn=%s outcome=%s", s.Job, s.State, s.LastCompleted, s.CompletedStatus)
			if s.ExitCode != nil {
				fmt.Printf(" exit_code=%d", *s.ExitCode)
			}
			fmt.Println()
			return resultCode(s.status, r.code)
		}
		r.data = append(first, '\n')
	}
	return r.output()
}

func printQueuedWait(ctx context.Context, h endpoint, id string, r reply) error {
	var notice waitObservation
	if err := json.Unmarshal(r.data, &notice); err != nil || notice.State != "queued" {
		return r.output()
	}
	row := notice
	row.NoticeKey = ""
	if err := json.NewEncoder(os.Stdout).Encode(row); err != nil {
		return err
	}
	return acknowledgeWait(ctx, h, id, notice, r.code)
}

func acknowledgeWait(ctx context.Context, h endpoint, id string, notice waitObservation, code int) error {
	r, err := h.call(ctx, 5*time.Second, "wait-ack", id, notice.RunID, notice.NoticeKey, notice.EventID)
	if err != nil {
		return err
	}
	if r.code != 0 {
		return r.output()
	}
	return ctlExit(code)
}
func observation(s string) (time.Duration, error) {
	if s == "" {
		s = os.Getenv("CXRUN_MONITOR_TIMEOUT")
	}
	if s == "" {
		return defaultMonitorTimeout, nil
	}
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 {
		return 0, errors.New("invalid observation timeout")
	}
	return d, nil
}
func resultCode(s status, fallback int) error {
	if s.Engine == "cmd" {
		if (s.State == "done" || s.State == "failed") && s.ExitCode != nil && *s.ExitCode >= 0 {
			fallback = int(*s.ExitCode)
		} else if code, ok := terminalCodes[s.State]; ok {
			fallback = code
		}
	}
	if fallback != 0 {
		return ctlExit(fallback)
	}
	return nil
}
func preview(b []byte, o ctlOptions) error {
	if o.json || o.watch || o.full || len(b) <= o.max {
		_, e := os.Stdout.Write(b)
		return e
	}
	f, e := os.CreateTemp("", "cx-output-*")
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	diagnosticf("%v\n", f.Name())
	n := o.max
	for n > 0 && n < len(b) && b[n]&0xc0 == 0x80 {
		n--
	}
	_, e = os.Stdout.Write(b[:n])
	return e
}
func cursorFile(parts ...string) (string, error) {
	home, e := controllerCacheRoot()
	if e != nil {
		return "", e
	}
	p := filepath.Join(home, "cursors")
	for _, part := range parts {
		p, e = jobPath(p, part)
		if e != nil {
			return "", e
		}
	}
	return p, os.MkdirAll(filepath.Dir(p), 0700)
}

var keyEvent = regexp.MustCompile("^(item/(started|completed) commandExecution|command/(start|exit|stop)|turn/(plan/updated|steer|start|completed|interrupt)|item/completed plan|error|stall|agent/(ask|send)|server/request|failed|restart)$")

func resolveJob(ctx context.Context, address string) (name, id string, e error) {
	name = address
	var ok bool
	name, id, ok = strings.Cut(name, ":")
	if !ok {
		id = name
		name = "local"
	}
	if !strings.Contains(id, "/") {
		p, err := defaultProject(ctx)
		if err != nil {
			return "", "", err
		}
		id = p + "/" + id
	}
	id = strings.TrimPrefix(id, "/")
	if _, e = jobDir("", id); e != nil {
		return "", "", e
	}
	if !strings.Contains(address, ":") {
		name, e = lookupRoute(ctx, id)
	}
	return name, id, e
}
