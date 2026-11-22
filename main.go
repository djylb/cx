package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	defaultSandbox          = "read-only"
	defaultNetwork          = false
	defaultRestartLimit     = 3
	defaultCmdStall         = 30 * time.Minute
	defaultQuiet            = 15 * time.Minute
	defaultIdle             = 30 * time.Minute
	defaultApproval         = 2 * time.Minute
	defaultTurnLimit        = 6 * time.Hour
	defaultStall            = 10 * time.Minute
	defaultMonitorTimeout   = 24 * time.Hour
	defaultRuntimeLimit     = 0
	defaultQueueLimit       = 0
	defaultLinuxSlots       = 10
	defaultWindowsSlots     = 6
	defaultOtherSlots       = 3
	defaultSystemSlots      = 1
	codexSlotClass          = "codex"
	testSlotClass           = "test"
	appServerApprovalPolicy = "never"
	defaultStallNudge       = "No progress detected. Report status, abandon any stuck command, and continue the task."
	defaultContinue         = "Continue the unfinished task."
)

const dangerFullAccess = "danger-full-access"

type config struct {
	secret                              *secretFilter
	Env                                 map[string]string `json:"env,omitempty"`
	Engine                              string            `json:"engine,omitempty"`
	ForkThread                          string            `json:"fork_thread,omitempty"`
	Through                             string            `json:"through,omitempty"`
	RunID                               string            `json:"run_id,omitempty"`
	SlotClass                           string            `json:"slot_class,omitempty"`
	Project                             string            `json:"project,omitempty"`
	Session                             string            `json:"session,omitempty"`
	DedupKey                            string            `json:"dedup_key,omitempty"`
	StallNudge                          string            `json:"stall_nudge,omitempty"`
	Dir, Model, Effort, Sandbox, Prompt string
	localEnv                            []string
	Input                               []json.RawMessage `json:"input,omitempty"`
	Argv                                []string          `json:"argv,omitempty"`
	Roots                               []string          `json:"roots,omitempty"` // extra writable roots (workspace-write), e.g. a Go build cache
	RestartLimit                        int               `json:"restart_limit"`
	CmdStall                            time.Duration     `json:"cmd_stall"`
	QueueTimeout                        time.Duration     `json:"queue_timeout,omitempty"`
	Timeout                             time.Duration     `json:"timeout,omitempty"`
	Quiet                               time.Duration     `json:"quiet_timeout,omitempty"`
	Idle, Approval, TurnLimit, Stall    time.Duration
	JobTools                            bool `json:"job_tools,omitempty"`
	WebSearch                           bool `json:"web_search,omitempty"`
	Interactive                         bool `json:"interactive,omitempty"`
	Shell                               bool `json:"cmd_shell,omitempty"`
	Network                             bool
}

func (c config) privileged() bool {
	return len(c.Argv) == 0 && c.Sandbox == dangerFullAccess
}

type status struct {
	LastEvent time.Time `json:"last_event_time"`
	ExitCode  *int64    `json:"exit_code,omitempty"`
	rateLimitStatus
	turnSummary

	Project     string `json:"project,omitempty"`
	Session     string `json:"session,omitempty"`
	Job         string `json:"job"`
	State       string `json:"status"`
	QueueReason string `json:"queue_reason,omitempty"`
	Profile     string `json:"profile,omitempty"`
	Engine      string `json:"engine,omitempty"`
	RunID       string `json:"run_id,omitempty"`

	Thread          string `json:"thread_id"`
	Turn            string `json:"turn_id"`
	TurnStatus      string `json:"turn_status"`
	LastCompleted   string `json:"last_completed_turn_id,omitempty"`
	CompletedStatus string `json:"last_completed_status,omitempty"`
	InputKind       string `json:"input_kind,omitempty"`
	ReviewThread    string `json:"review_thread_id,omitempty"`
	ReviewTurn      string `json:"review_turn_id,omitempty"`
	CompactOp       string `json:"compact_op,omitempty"`
	Error           string `json:"error,omitempty"`
	LastAction      string `json:"last_action,omitempty"`

	Usage     json.RawMessage `json:"token_usage"`
	Approvals []approval      `json:"pending_requests,omitempty"`

	OutboxSeq    uint64 `json:"outbox_seq,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
	CompactAfter int64  `json:"compact_after_seq,omitempty"`
	CompactSeq   int64  `json:"compact_event_seq,omitempty"`
	RunnerStart  uint64 `json:"runner_start,omitempty"`
	Restarts     int    `json:"restarts,omitempty"`
	CommandPID   int    `json:"command_pid,omitempty"`
	PID          int    `json:"pid,omitempty"`
	ServerPID    int    `json:"server_pid,omitempty"`
	Finished     int64  `json:"completed_turns"`
	StallCount   int    `json:"stall_count,omitempty"`
	Privileged   bool   `json:"privileged,omitempty"`
}

// closeBestEffort releases resources whose operation has already completed.
// Writers that rely on Close to commit data must check its result separately.
func closeBestEffort(c io.Closer) { _ = c.Close() }

func removeBestEffort(path string)     { _ = os.Remove(path) }
func removeTreeBestEffort(path string) { _ = os.RemoveAll(path) }

func atomicWrite(path string, b []byte) error {
	return atomicWriteSync(path, b, (*os.File).Sync)
}
func atomicWriteSync(path string, b []byte, syncFile func(*os.File) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	defer removeBestEffort(f.Name())
	_, err = f.Write(b)
	if err == nil {
		err = syncFile(f)
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = replaceFile(f.Name(), path); err != nil || runtime.GOOS == "windows" {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer closeBestEffort(dir)
	return syncFile(dir)
}
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(path, append(b, '\n'))
}
func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func readInputText(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("%s: expected UTF-8 text", path)
	}
	return string(b), nil
}

const defaultStateDir = ".local/share/cx/state"

func stateRoot() (string, error) {
	if s := os.Getenv("CXRUN_STATE"); s != "" {
		return filepath.Abs(s)
	}
	home, err := os.UserHomeDir()
	return filepath.Join(home, defaultStateDir), err
}

var jobName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$`)
var deviceName = regexp.MustCompile(`(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\.|$)`)

func jobPath(root, job string) (string, error) {
	if !jobName.MatchString(job) || deviceName.MatchString(job) || strings.HasSuffix(job, ".") {
		return "", fmt.Errorf("invalid job id %q", job)
	}
	return filepath.Join(root, job), nil
}
func printStatus(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "status.json"))
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}

// Publish config, mailbox and initial status together. Concurrent starters see a complete job.
func register(root, dir string, c config, s status) error {
	s.RunID = c.RunID
	s.Privileged = c.privileged()
	stage, err := os.MkdirTemp(root, ".register-*")
	if err != nil {
		return err
	}
	defer removeTreeBestEffort(stage)
	for _, p := range []string{"inbox/done", "inbox/pending"} {
		if err = os.MkdirAll(filepath.Join(stage, p), 0700); err != nil {
			return err
		}
	}
	if err = writeJSON(filepath.Join(stage, "config.json"), c); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(stage, "status.json"), s); err != nil {
		return err
	}
	files := []string{"progress.log", "owner.lock", "events.jsonl"}
	if len(c.Argv) > 0 {
		files = append(files, "output.log", "last.md")
	}
	for _, name := range files {
		if err = os.WriteFile(filepath.Join(stage, name), nil, 0600); err != nil {
			return err
		}
	}
	return os.Rename(stage, dir)
}

func run(ctx context.Context, args []string) error { return runNode(ctx, args, false) }
func runNode(ctx context.Context, args []string, directSubmit bool) error {
	if len(args) == 0 {
		return errors.New("usage: cxrun host [--state PATH] or cxrun start|resume|status|say|interrupt|close|wait|tail --job ID [options]")
	}
	op := args[0]
	if op == "states" && len(args) == 1 {
		return json.NewEncoder(os.Stdout).Encode(terminalCodes)
	}
	fs := flag.NewFlagSet(op, flag.ContinueOnError)
	root, err := stateRoot()
	if err != nil {
		return err
	}
	fs.StringVar(&root, "state", root, "state root")
	if op == "facts" || op == "notes" {
		return hostInfoCLI(ctx, op, fs, &root, args[1:])
	}
	if op == "host" {
		if parseErr := fs.Parse(args[1:]); parseErr != nil {
			return parseErr
		}
		if fs.NArg() != 0 {
			return errors.New("host: unexpected arguments")
		}
		host, hostErr := readHostConfig(root)
		if hostErr != nil {
			return hostErr
		}
		return json.NewEncoder(os.Stdout).Encode(host)
	}
	job := fs.String("job", "", "job ID")
	var c config
	var since string
	var outSince uint64
	if op == "outbox" {
		fs.Uint64Var(&outSince, "since", 0, "exclusive message sequence")
	}
	if op == "log" {
		fs.StringVar(&since, "since", "", "exclusive progress sequence")
	}
	var marker markerOptions
	if isMarker(op) {
		fs.StringVar(&marker.ID, "cmd-id", os.Getenv("CXRUN_CMD_ID"), "idempotency key")
		fs.Func("turn-id", "target turn", func(v string) error { marker.Turn = &v; return nil })
	}
	project := fs.String("project", "", "project namespace")
	session := fs.String("session", "", "session label")
	var limit time.Duration
	var anyJob bool
	var sinceFile, listFields string
	if op == "wait" {
		fs.DurationVar(&limit, "timeout", 0, "wait deadline")
		fs.BoolVar(&anyJob, "any", false, "wait for any matching job")
		fs.StringVar(&sinceFile, "since-file", "", "consumer baseline file for --any")
	}
	if op == "list" {
		fs.StringVar(&listFields, "fields", "", "brief for compact status")
	}
	var force, command bool
	var requestedProfile string
	if op == "start" || op == "resume" || op == "fork" {
		fs.StringVar(&requestedProfile, "profile", "", "execution host profile")
	}
	newProfile := op == "start"

	var prompt, preamblePath, stallNudgePath string
	if op == "start" {
		fs.BoolVar(&command, "cmd", false, "start a command")
		fs.Func("env", "command environment NAME=VALUE", func(v string) error { return addCommandEnv(&c.Env, v) })
		fs.BoolVar(&c.Shell, "cmd-shell", false, "execute one explicit shell script")
		fs.StringVar(&c.SlotClass, "slot-class", "", "admission class")
		fs.DurationVar(&c.Timeout, "timeout", defaultRuntimeLimit, "runtime limit excluding queue time")
		fs.DurationVar(&c.QueueTimeout, "queue-timeout", defaultQueueLimit, "maximum wait per admission")
		fs.DurationVar(&c.Quiet, "quiet-timeout", defaultQuiet, "command output inactivity limit")
		fs.StringVar(&c.DedupKey, "dedup-key", "", "deduplication key")
		fs.BoolVar(&force, "force", false, "skip duplicate key check")
		fs.StringVar(&c.Dir, "dir", "", "working directory")
		fs.StringVar(&prompt, "prompt-file", "", "UTF-8 prompt file")
		fs.StringVar(&preamblePath, "preamble-file", os.Getenv("CXRUN_PREAMBLE_FILE"), "UTF-8 first-turn preamble file")
		fs.StringVar(&stallNudgePath, "stall-nudge-file", "", "UTF-8 stall and recovery message file")
		fs.StringVar(&c.Engine, "engine", "", "codex or claude")
		fs.BoolVar(&c.WebSearch, "web-search", false, "enable live web search")
		fs.StringVar(&c.Model, "model", "", "model (engine default)")
		fs.StringVar(&c.Effort, "effort", "", "reasoning effort")
		fs.StringVar(&c.Sandbox, "sandbox", defaultSandbox, "read-only, workspace-write, or danger-full-access")
		fs.BoolVar(&c.Network, "network", defaultNetwork, "sandbox network access")
		fs.Func("writable-root", "extra writable directory for workspace-write (repeatable)", func(v string) error {
			c.Roots = append(c.Roots, v)
			return nil
		})
		fs.DurationVar(&c.Idle, "idle-exit", defaultIdle, "idle lifetime")
		fs.DurationVar(&c.Approval, "approval-timeout", defaultApproval, "approval deadline")
		fs.DurationVar(&c.TurnLimit, "turn-timeout", defaultTurnLimit, "turn deadline")
		fs.IntVar(&c.RestartLimit, "max-restarts", defaultRestartLimit, "app-server restart budget per job")
		fs.DurationVar(&c.CmdStall, "cmd-stall", defaultCmdStall, "command silence window")
		fs.DurationVar(&c.Stall, "stall", defaultStall, "no app-server event for this long: steer, interrupt, restart, stalled (0 disables)")
	}
	var opts options
	opts.flags(fs, op)
	if op == "start" {
		fs.BoolVar(&c.JobTools, "job-tools", false, "configure job-scoped Codex message tools")
		fs.BoolVar(&c.Interactive, "interactive", false, "recognize STATUS: QUESTION")
	}
	var prefix []string
	rest := args[1:]
	if err = fs.Parse(rest); err != nil {
		return err
	}
	switch op {
	case "rpc", "fork", "mcp", "history", "threads":
		if rest = fs.Args(); len(rest) > 0 {
			prefix = append(prefix, rest[0])
			if err = fs.Parse(rest[1:]); err != nil {
				return err
			}
		}
	}
	if p, j, ok := strings.Cut(*job, "/"); ok {
		if p == "" || (*project != "" && *project != p) {
			return errors.New("invalid qualified job")
		}
		*project, *job = p, j
	}
	if op == "start" {
		top := *project
		if top == "" {
			top = *job
		}
		if strings.EqualFold(top, hostConfigName) || strings.EqualFold(top, "cache") {
			return fmt.Errorf("%s is reserved at the state root", top)
		}
	}
	parent := root
	if *project != "" {
		parent, err = jobPath(root, *project)
		if err != nil || len(*project) > 48 {
			return errors.New("invalid project")
		}
	}
	if op == "list" {
		if *job != "" || fs.NArg() != 0 {
			return errors.New("list accepts filters only")
		}
		if listFields != "" && listFields != "brief" {
			return errors.New("list: --fields must be brief")
		}
		return listJobs(ctx, root, *project, *session, listFields == "brief")
	}
	if limit < 0 {
		return errors.New("negative timeout")
	}
	if anyJob {
		if *job != "" || fs.NArg() != 0 {
			return errors.New("wait --any accepts filters only")
		}
		waitCtx, cancel, monitorErr := monitorContext(ctx, limit)
		if monitorErr != nil {
			return monitorErr
		}
		defer cancel()
		return waitAny(waitCtx, root, *project, *session, sinceFile, os.Stdout)
	}
	if sinceFile != "" {
		return errors.New("--since-file requires --any")
	}
	dir, err := jobPath(parent, *job)
	if err != nil {
		return err
	}
	pos := append(append([]string{}, prefix...), fs.Args()...)
	if op == "mcp-tools" || op == "outbox" {
		if len(pos) != 0 {
			return errors.New("unexpected arguments")
		}
		if op == "mcp-tools" {
			stopRead := context.AfterFunc(ctx, func() { _ = os.Stdin.Close() })
			defer stopRead()
			return serveTools(ctx, dir, os.Stdin, os.Stdout)
		}
		enc := json.NewEncoder(os.Stdout)
		return readMail(dir, outSince, func(m agentMail) error { return enc.Encode(m) })
	}
	profile := requestedProfile
	if op == "fork" {
		if len(pos) != 1 {
			return errors.New("fork SOURCE --job NEW")
		}
		sourceParent := parent
		source := pos[0]
		if proj, name, ok := strings.Cut(source, "/"); ok {
			sourceParent, err = jobPath(root, proj)
			if err != nil {
				return err
			}
			source = name
		}
		src, sourceErr := jobPath(sourceParent, source)
		if sourceErr != nil {
			return sourceErr
		}
		if sourceErr = readJSON(filepath.Join(src, "config.json"), &c); sourceErr != nil {
			return sourceErr
		}
		var from status
		if sourceErr = readJSON(filepath.Join(src, "status.json"), &from); sourceErr != nil {
			return sourceErr
		}
		if from.Thread == "" {
			return errors.New("source has no thread")
		}
		if !c.driver().capabilities().Fork {
			return errors.New("driver does not support fork")
		}
		if profile == "" {
			profile = from.Profile
		}
		c.ForkThread = from.Thread
		c.Through = opts.through
		c.DedupKey = ""
		op = "start"
	}
	if handled, dispatchErr := opts.dispatch(ctx, dir, op, pos); handled {
		return dispatchErr
	}
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		defer cancel()
		if fs.NArg() != 0 {
			return errors.New("wait: unexpected arguments")
		}
		return observe(ctx, op, dir)
	}
	terminalClose := op == "close"
	if terminalClose {
		terminalClose, err = deliverClose(ctx, dir, marker, fs.Args())
		if err != nil || !terminalClose {
			return err
		}
	}
	if !terminalClose && op != "start" && op != "resume" && op != "clean" && op != "_command" {
		if since != "" {
			if fs.NArg() != 0 {
				return errors.New("since and offset are exclusive")
			}
			return logSince(dir, since)
		}
		if op == "ps" {
			if fs.NArg() != 0 {
				return errors.New("ps: unexpected arguments")
			}
			return printProcesses(dir)
		}
		if isMarker(op) {
			return publishMarker(dir, op, marker, fs.Args())
		}
		if op == "say" && (opts.input != "" || opts.image != "") {
			return structuredSay(dir, opts, fs.Args())
		}
		return control(ctx, op, dir, fs.Args())
	}
	if fs.NArg() != 0 && c.ForkThread == "" && !command && !c.Shell {
		return errors.New("unexpected positional arguments")
	}
	var guard *os.File
	if op != "_command" {
		guard, err = registryLock(ctx, root)
	}
	if err != nil {
		return err
	}
	defer func() {
		if guard != nil {
			_ = guard.Close()
		}
	}()
	if *project != "" {
		if fi, e := os.Stat(filepath.Join(parent, "config.json")); (e != nil && !os.IsNotExist(e)) || (e == nil && fi.Mode().IsRegular()) {
			return fmt.Errorf("project collides with flat job or inaccessible: %s", *project)
		}
	}
	c.Project, c.Session = *project, *session
	s := status{Job: *job, Project: *project, Session: *session, State: "queued", LastEvent: time.Now().UTC()}
	if op == "start" {
		s.Profile, _, err = selectedProfile(root, profile, newProfile)
		if err != nil {
			return err
		}
		if _, err = os.Stat(dir); err == nil {
			if directSubmit {
				if err = checkSubmission(dir, c, s.Profile, fs.Args()); err != nil {
					return err
				}
			}
			return printStatus(dir)
		} else if !os.IsNotExist(err) {
			return err
		}
		if c.Engine != "" && c.Engine != "codex" && c.Engine != "claude" {
			return errors.New("unknown engine")
		}
		if c.Engine == "claude" {
			s.Engine = c.Engine
		}
		if command || c.Shell {
			if c.Engine != "" || c.WebSearch {
				return errors.New("agent flags on command job")
			}
			c.Argv = fs.Args()
			if len(c.Argv) == 0 || c.Argv[0] == "" || (c.Shell && len(c.Argv) != 1) {
				return errors.New("supply command argv, or one script for --cmd-shell")
			}
			if c.Quiet < 0 {
				return errors.New("negative command deadline")
			}
			if c.Dir == "" {
				c.Dir = "."
			}
			if c.SlotClass == "" {
				c.SlotClass = testSlotClass
			}
			s.Engine = "cmd"
		} else {
			c.Quiet = 0
		}
		if c.SlotClass != "" {
			if _, err = jobPath(root, c.SlotClass); err != nil {
				return err
			}
		}
		if c.Timeout < 0 || c.QueueTimeout < 0 {
			return errors.New("negative runtime or queue timeout")
		}
		switch c.Sandbox {
		case "read-only", "workspace-write", dangerFullAccess:
		default:
			return errors.New("unsupported --sandbox")
		}
		if c.Dir == "" || c.Idle <= 0 || c.Approval <= 0 || c.TurnLimit <= 0 || c.Stall < 0 || c.CmdStall < 0 || c.RestartLimit < 0 {
			return errors.New("supply --dir and positive timeouts")
		}
		c.Dir, err = filepath.Abs(c.Dir)
		if err != nil {
			return err
		}
		info, startErr := os.Stat(c.Dir)
		if startErr != nil {
			return startErr
		}
		if !info.IsDir() {
			return errors.New("--dir must be a directory")
		}
		if len(c.Argv) == 0 {
			if prompt != "" {
				c.Prompt, startErr = readInputText(prompt)
			}
			if startErr != nil {
				return startErr
			}
			if c.ForkThread == "" && opts.input == "" && opts.image == "" && strings.TrimSpace(c.Prompt) == "" {
				return errors.New("empty prompt")
			}
			if preamblePath != "" {
				prefix, preambleErr := readInputText(preamblePath)
				if preambleErr != nil {
					return preambleErr
				}
				if strings.TrimSpace(prefix) != "" {
					c.Prompt = strings.TrimRight(prefix, "\r\n") + "\n\n" + c.Prompt
				}
			}
			if opts.input != "" || opts.image != "" {
				c.Input, startErr = opts.inputs(c.Prompt)
				if startErr != nil {
					return startErr
				}
			}
			if stallNudgePath != "" {
				if c.StallNudge, startErr = readInputText(stallNudgePath); startErr != nil {
					return startErr
				}
				if strings.TrimSpace(c.StallNudge) == "" {
					return errors.New("empty stall nudge")
				}
			}
		}
		if startErr = os.MkdirAll(parent, 0700); startErr != nil {
			return startErr
		}
		if force {
			c.DedupKey = ""
		}
		if c.DedupKey != "" {
			if startErr = claimKey(root, c, s); startErr != nil {
				return startErr
			}
		}
		if startErr = c.newRunID(); startErr != nil {
			return startErr
		}
		if startErr = register(parent, dir, c, s); startErr != nil {
			if _, e := os.Stat(dir); e == nil {
				return printStatus(dir)
			}
			return startErr
		}
		if len(c.Argv) > 0 {
			var done <-chan error
			if directSubmit && runtime.GOOS == "windows" {
				startErr = launchMuxCommand(ctx, root, *project, *job, c.RunID)
			} else {
				done, startErr = launchCommand(root, *project, *job)
			}
			if startErr != nil {
				if directSubmit && runtime.GOOS == "windows" && (errors.Is(startErr, context.DeadlineExceeded) || errors.Is(startErr, context.Canceled)) {
					return fmt.Errorf("psmux launch acknowledgement unavailable: %w", startErr)
				}
				return errors.Join(startErr, os.RemoveAll(dir))
			}
			ready, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			for {
				if startErr = readJSON(filepath.Join(dir, "status.json"), &s); startErr != nil {
					return startErr
				}
				if s.PID != 0 || isTerminal(s.State) {
					return printStatus(dir)
				}
				select {
				case workerErr := <-done:
					return errors.Join(errors.New("worker exited before acquiring ownership"), workerErr)
				case <-ready.Done():
					return fmt.Errorf("worker registration acknowledgement: %w", ready.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
	}
	if op == "resume" {
		if err = readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
			return err
		}
		if err = checkResumeProfile(root, s.Profile, requestedProfile); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, "owner.lock"), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	owned, err := tryLock(f)
	if err != nil {
		return err
	}
	if !owned && terminalClose {
		_ = guard.Close()
		guard = nil
		// Terminal status is published just before the runner releases ownership.
		deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for !owned {
			if err = pause(deadline, 20*time.Millisecond); err != nil {
				return err
			}
			owned, err = tryLock(f)
			if err != nil {
				return err
			}
		}
	}
	if !owned && op == "clean" {
		return errors.New("job has a live owner")
	}
	if !owned {
		return printStatus(dir)
	}
	if err = readJSON(filepath.Join(dir, "config.json"), &c); err != nil {
		return err
	}
	if err = readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	if op == "_command" {
		if len(c.Argv) == 0 {
			return errors.New("expected command job")
		}
		if s.State != "queued" {
			return printStatus(dir)
		}
		s.PID = os.Getpid()
		if err = writeJSON(filepath.Join(dir, "status.json"), s); err != nil {
			return err
		}
		guard, err = registryLock(ctx, root)
		if err != nil {
			return err
		}
	}
	r := runner{root: root, dir: dir, c: c, s: s, ctx: ctx}
	if op == "clean" || terminalClose {
		if !isTerminal(s.State) {
			return errors.New("job is not terminal")
		}
		// Keep owner.lock throughout the grace period; other jobs can register.
		if guard != nil {
			_ = guard.Close()
			guard = nil
		}
		if err = r.reclaim(); err != nil {
			return err
		}
		if terminalClose {
			return r.releaseClosedWorkspace()
		}
		guard, err = registryLock(ctx, root)
		if err != nil {
			return err
		}
		if err = archiveReceipt(root, c, r.s); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		// Detach the complete directory before recursive deletion, so a
		// partial filesystem failure cannot leave a half-registered job.
		trash, tempErr := os.MkdirTemp(parent, ".clean-*")
		if tempErr != nil {
			return tempErr
		}
		if tempErr = os.Rename(dir, filepath.Join(trash, "job")); tempErr != nil {
			_ = os.Remove(trash)
			return tempErr
		}
		return os.RemoveAll(trash)
	}

	if len(c.Argv) > 0 && op != "_command" {
		return errors.New("command jobs cannot be replayed by resume")
	}

	r.s.RunnerStart = processStart(os.Getpid())
	if r.c.RunID == "" {
		if err = r.c.newRunID(); err != nil {
			return err
		}
		if err = writeJSON(filepath.Join(dir, "config.json"), r.c); err != nil {
			return err
		}
	}
	r.s.RunID = r.c.RunID
	defer r.releaseSlot()
	// Register the first ticket under the publication lock to preserve start order.
	var ticket *queueTicket
	if s.Thread == "" || r.active() {
		ticket, err = r.newTicket()
		if err != nil {
			r.s.State = "failed"
			r.s.PID = 0
			r.s.Error = err.Error()
			return errors.Join(err, r.save())
		}
	}
	_ = guard.Close()
	guard = nil
	if ticket != nil {
		r.s.PID = os.Getpid()
		r.s.ServerPID = 0
		if err = r.admit(ticket); err != nil {
			r.s.PID = 0
			if errors.Is(err, errQueuedClose) {
				r.s.State = "done"
				r.s.CompletedStatus = "interrupted"
				return r.save()
			}
			r.s.State = "failed"
			if errors.Is(err, errQueueTimeout) {
				r.s.State = "queue_timeout"
			}
			r.s.Error = err.Error()
			return errors.Join(err, r.save())
		}
	}
	if err = r.prepareBuildCache(); err != nil {
		r.s.State = "failed"
		r.s.PID = 0
		r.s.Error = err.Error()
		return errors.Join(err, r.save())
	}
	defer r.releaseBuildCache()
	if err = r.loadCredentials(); err == nil {
		err = r.preparePackageCaches()
	}
	if err != nil {
		r.s.State = "failed"
		r.s.PID = 0
		r.s.Error = err.Error()
		return errors.Join(err, r.save())
	}
	return r.c.driver().run(&r)
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "_command" {
		ignoreHangup()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dispatch(ctx, os.Args[1:]); err != nil {
		var code ctlExit
		if errors.As(err, &code) {
			os.Exit(int(code))
		}
		switch {
		case errors.Is(err, errQuestion):
			os.Exit(6)
		case errors.Is(err, errDuplicate):
			os.Exit(7)
		case errors.Is(err, errStalled):
			os.Exit(terminalCodes["stalled"])
		case errors.Is(err, errObserveTimeout):
			os.Exit(124)
		case errors.Is(err, errNotRegistered):
			os.Exit(11)
		}
		var result stateFailure
		if errors.As(err, &result) {
			os.Exit(terminalCodes[string(result)])
		}
		if errors.Is(err, errDrained) {
			os.Exit(10)
		}
		fmt.Fprintln(os.Stderr, "cxrun:", err)
		os.Exit(1)
	}
}

func (c *config) newRunID() error {
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	c.RunID = hex.EncodeToString(nonce[:])
	return err
}
