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

	"github.com/djylb/timeshim"
)

const (
	defaultSandbox          = "workspace-write"
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
	agentSlotClass          = "agent"
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
	cacheUse                            *cacheUse
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

func (c *config) privileged() bool {
	return len(c.Argv) == 0 && c.Sandbox == dangerFullAccess
}

type status struct {
	LastEvent time.Time `json:"last_event_time"`
	ExitCode  *int64    `json:"exit_code,omitempty"`

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
	LaunchID        string `json:"launch_id,omitempty"`

	rateLimitStatus
	turnSummary
	Usage     json.RawMessage `json:"token_usage"`
	Approvals []approval      `json:"pending_requests,omitempty"`

	QueueGeneration uint64 `json:"queue_generation,omitempty"`
	Generation      uint64 `json:"generation,omitempty"`
	CompactAfter    int64  `json:"compact_after_seq,omitempty"`
	CompactSeq      int64  `json:"compact_event_seq,omitempty"`
	RunnerStart     uint64 `json:"runner_start,omitempty"`
	Restarts        int    `json:"restarts,omitempty"`
	EngineRestarts  int    `json:"engine_restarts,omitempty"`
	CommandPID      int    `json:"command_pid,omitempty"`
	PID             int    `json:"pid,omitempty"`
	EnginePID       int    `json:"engine_pid,omitempty"`
	Finished        int64  `json:"completed_turns"`
	StallCount      int    `json:"stall_count,omitempty"`
	Privileged      bool   `json:"privileged,omitempty"`
	final           bool
}

// diagnosticf writes advisory text or reports an existing failure. A broken
// stderr cannot report its own write error, and must not change the command result.
func diagnosticf(format string, args ...any) { _, _ = fmt.Fprintf(os.Stderr, format, args...) }

// closeBestEffort releases resources whose operation has already completed.
// Writers that rely on Close to commit data must check its result separately.
func closeBestEffort(c io.Closer) { _ = c.Close() }

func removeBestEffort(path string)     { _ = os.Remove(path) }
func removeTreeBestEffort(path string) { _ = os.RemoveAll(path) }

// atomicWrite replaces private files. Shared job state uses the state log.
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
	if err = os.Rename(f.Name(), path); err != nil || runtime.GOOS == "windows" {
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
	b, err := readSharedFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func readInputText(path string) (string, error) {
	b, err := readSharedFile(path)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("%s: expected UTF-8 text", path)
	}
	return string(b), nil
}

// Default state roots: under the home directory on Unix, under %LOCALAPPDATA% on Windows.
const (
	defaultStateDir        = ".local/share/cx/state"
	defaultWindowsStateDir = `cx\state`
)

func stateRoot() (string, error) {
	if s := os.Getenv("CXRUN_STATE"); s != "" {
		return filepath.Abs(s)
	}
	if runtime.GOOS == "windows" {
		local := os.Getenv("LOCALAPPDATA")
		if local == "" {
			return "", errors.New("LOCALAPPDATA is unset; set CXRUN_STATE")
		}
		return filepath.Join(local, defaultWindowsStateDir), nil
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

func trashJobDir(root, project, job string) string {
	if project == "" {
		return filepath.Join(root, ".trash", "flat", job)
	}
	return filepath.Join(root, ".trash", "projects", project, job)
}

// The registry lock covers detachment only. Trash paths are never reused.
func detachCleanJob(root, dir string, s status) error {
	trash := trashJobDir(root, s.Project, s.Job)
	if err := os.MkdirAll(trash, 0700); err != nil {
		return err
	}
	if err := os.Rename(dir, filepath.Join(trash, rand.Text())); err != nil {
		return err
	}
	return errors.Join(syncJobDir(filepath.Dir(dir)), syncJobTree(trash, root))
}

func reapDetachedJobs(root, project, job string) error {
	trash := trashJobDir(root, project, job)
	entries, err := os.ReadDir(trash)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err = errors.Join(err, os.RemoveAll(filepath.Join(trash, entry.Name())))
	}
	return err
}

func printStatus(dir string) error {
	record, _, err := jobStatusRecord(dir)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(record.Value, '\n'))
	return err
}

// The caller holds the registry lock. The first status record publishes the job.
// On success the caller keeps owner.lock for worker startup or command handoff.
func registerJob(ctx context.Context, parent, dir string, c config, s status, syncConfig func(*stateWriter) error) (*os.File, error) {
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			return nil, readErr
		}
		for _, entry := range entries {
			switch entry.Name() {
			case "owner.lock", "owner-probe.lock", "state":
				continue
			case "output.log", "progress.log", "events.jsonl", "handoff.lock":
				// A staged registration has published its config before any
				// mutable streams are created.
				if cfg, err := jobConfigRecord(dir); err == nil && cfg.Seq != 0 {
					continue
				}
			}
			return nil, fmt.Errorf("%w: retire incompatible or populated job directory %s", os.ErrExist, dir)
		}
	}

	owner, err := openJobOwner(ctx, dir, jobFileLock)
	if err != nil {
		return nil, err
	}
	if owner == nil {
		return nil, &os.PathError{Op: "register", Path: dir, Err: os.ErrExist}
	}
	if err := publishRegistration(parent, dir, c, s, syncConfig); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func publishRegistration(parent, dir string, c config, s status, syncConfig func(*stateWriter) error) error {
	if record, _, err := latestState(dir); err != nil {
		return err
	} else if record.Seq != 0 {
		return &os.PathError{Op: "register", Path: dir, Err: os.ErrExist}
	}
	s.RunID = c.RunID
	s.Privileged = c.privileged()
	r := runner{dir: dir, c: c, s: s}
	if err := r.openState(); err != nil {
		return err
	}
	defer closeBestEffort(r.stateLog)
	// The durable config distinguishes an incomplete registration from a
	// legacy directory even if later stream creation is interrupted.
	if err := syncConfig(r.stateLog); err != nil {
		return err
	}
	files := []string{"progress.log", "events.jsonl", "handoff.lock", "owner-probe.lock"}
	if len(c.Argv) > 0 {
		files = append(files, "output.log")
	}
	for _, name := range files {
		f, err := openJobFile(filepath.Join(dir, name), jobFileNew)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	if err := initMailboxLocks(dir); err != nil {
		return err
	}
	// Persist the directory chain before the registration can yield an owner.
	root := parent
	if s.Project != "" {
		root = filepath.Dir(parent)
	}
	if err := syncJobTree(dir, root); err != nil {
		return err
	}
	if isTerminal(s.State) {
		return r.saveTerminal()
	}
	return errors.Join(r.save(), r.stateLog.Close())
}

// The submitter holds the registry lock. A worker that acquired owner.lock
// before the launch acknowledgement failed remains responsible for its state.
func recordLaunchFailure(ctx context.Context, dir string, c config, launchErr error) error {
	owner, err := openJobFile(filepath.Join(dir, "owner.lock"), jobFileLock)
	if err != nil {
		return errors.Join(launchErr, err)
	}
	defer closeBestEffort(owner)
	owned, err := tryAcquireJobOwner(ctx, dir, owner)
	if err != nil || !owned {
		return errors.Join(launchErr, err)
	}
	r := runner{dir: dir, c: c}
	record, err := readJobStatus(dir, &r.s)
	if err != nil || record.Final || r.s.PID != 0 || r.s.RunID != c.RunID {
		return errors.Join(launchErr, err)
	}
	if err := r.openState(); err != nil {
		return errors.Join(launchErr, err)
	}
	return r.failStart(launchErr)
}

func run(ctx context.Context, args []string) error { return runNode(ctx, args, false) }
func runNode(ctx context.Context, args []string, directSubmit bool) error {
	return runNodeWithRegistration(ctx, args, directSubmit, registerJob)
}

func runNodeWithRegistration(ctx context.Context, args []string, directSubmit bool, register func(context.Context, string, string, config, status, func(*stateWriter) error) (*os.File, error)) error {
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
	var expectedRun, expectedDir, expectedWorkspace, launchID string
	if op == "_command" || op == "resume" || op == "fork" || op == "mcp-tools" {
		fs.StringVar(&expectedRun, "expect-run", "", "expected job run for worker handoff")
	}
	if op == "_command" || op == "resume" || op == "fork" {
		fs.StringVar(&expectedDir, "expect-dir", "", "expected directory identity for worker handoff")
	}
	if op == "start" || op == "fork" || op == "resume" {
		fs.StringVar(&launchID, "launch-id", "", "runner handoff identifier")
		fs.StringVar(&expectedWorkspace, "expect-workspace", "", "expected working directory identity for runner handoff")
	}
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
	var force, command, network, noNetwork bool
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
		fs.StringVar(&c.Engine, "engine", "", "agent engine: "+strings.Join(engineNames(), " or ")+" (default "+defaultEngine+")")
		fs.BoolVar(&c.WebSearch, "web-search", false, "enable live web search")
		fs.StringVar(&c.Model, "model", "", "model (engine default)")
		fs.StringVar(&c.Effort, "effort", "", "reasoning effort")
		fs.StringVar(&c.Sandbox, "sandbox", defaultSandbox, "read-only, workspace-write, or danger-full-access")
		fs.BoolVar(&network, "network", false, "allow sandbox network access (default for workspace-write)")
		fs.BoolVar(&noNetwork, "no-network", false, "deny sandbox network access (default for read-only)")
		fs.Func("writable-root", "extra writable directory for workspace-write (repeatable)", func(v string) error {
			c.Roots = append(c.Roots, v)
			return nil
		})
		fs.DurationVar(&c.Idle, "idle-exit", defaultIdle, "idle lifetime")
		fs.DurationVar(&c.Approval, "approval-timeout", defaultApproval, "approval deadline")
		fs.DurationVar(&c.TurnLimit, "turn-timeout", defaultTurnLimit, "turn deadline")
		fs.IntVar(&c.RestartLimit, "max-restarts", defaultRestartLimit, "engine restart budget per job")
		fs.DurationVar(&c.CmdStall, "cmd-stall", defaultCmdStall, "command silence window")
		fs.DurationVar(&c.Stall, "stall", defaultStall, "no engine event for this long: steer, interrupt, restart, stalled (0 disables)")
	}
	var opts options
	opts.flags(fs, op)
	if op == "start" {
		fs.BoolVar(&c.JobTools, "job-tools", false, "give the agent job-scoped messaging tools")
		fs.BoolVar(&c.Interactive, "interactive", false, "recognize STATUS: QUESTION")
	}
	var prefix []string
	rest := args[1:]
	if err = fs.Parse(rest); err != nil {
		return err
	}
	if _, formatErr := checkNodeStateFormat(root); formatErr != nil {
		return formatErr
	}
	explicitApproval := false
	fs.Visit(func(f *flag.Flag) { explicitApproval = explicitApproval || f.Name == "approval-timeout" })
	if network && noNetwork {
		return errors.New("--network and --no-network are exclusive")
	}
	if op == "start" {
		c.Network = network || !noNetwork && c.Sandbox == "workspace-write"
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
			return serveToolsFor(ctx, dir, expectedRun, commandInput{ctx: ctx}, os.Stdout)
		}
		enc := json.NewEncoder(os.Stdout)
		return readMail(dir, outSince, func(m agentMail) error { return enc.Encode(m) })
	}
	profile := requestedProfile
	var sourceID, sourceDir string
	var sourceIdentity jobIdentity
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
			sourceID = proj + "/" + name
		} else {
			sourceID = source
			if *project != "" {
				sourceID = *project + "/" + source
			}
		}
		src, sourceErr := jobPath(sourceParent, source)
		if sourceErr != nil {
			return sourceErr
		}
		id, from, _, sourceErr := captureJobIdentity(src)
		if sourceErr != nil {
			return sourceErr
		}
		if expectedRun != "" && expectedRun != id.run || expectedDir != "" && expectedDir != id.key {
			return errJobReplaced
		}
		sourceIdentity, sourceDir = id, src
		expectedRun, expectedDir = "", ""
		if sourceErr = readJobConfig(src, &c); sourceErr != nil {
			return sourceErr
		}
		if from.Thread == "" {
			return errors.New("source has no thread")
		}
		if !c.driver().capabilities().Fork {
			return fmt.Errorf("fork not supported by engine %s", c.Engine)
		}
		if profile == "" {
			profile = from.Profile
		}
		c.ForkThread = from.Thread
		c.Through = opts.through
		c.DedupKey = ""
		op = "start"
	}
	if need, ok := commandNeeds[op]; ok && op != "resume" {
		if err = checkJobCapability(dir, op, need); err != nil {
			return err
		}
	}
	if handled, dispatchErr := opts.dispatch(ctx, dir, op, pos); handled {
		return dispatchErr
	}
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), limit)
		defer cancel()
		if fs.NArg() != 0 {
			return errors.New("wait: unexpected arguments")
		}
		return observe(ctx, op, dir)
	}
	var closing *closeConfirmation
	if op == "close" {
		closing, err = deliverClose(ctx, dir, marker, fs.Args())
		if err != nil || closing == nil {
			return err
		}
		work, workErr := workDir(dir, closing.status)
		if workErr != nil {
			return workErr
		}
		if id := privateWorkspaceID(root, work); id != "" {
			lock, lockErr := jobOperationLock(ctx, root, id)
			if lockErr != nil {
				return lockErr
			}
			defer closeBestEffort(lock)
		}
	}
	terminalClose := closing != nil
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
		if op == "say" && (opts.input != "" || len(opts.images) > 0) {
			return structuredSay(dir, opts, fs.Args())
		}
		return control(ctx, op, dir, fs.Args())
	}
	if fs.NArg() != 0 && c.ForkThread == "" && !command && !c.Shell {
		return errors.New("unexpected positional arguments")
	}
	releaseWorkspace := func() {}
	var startingIdentity jobIdentity
	if (op == "start" || op == "resume") && (!directSubmit || launchID != "") {
		cwd := c.Dir
		jobs := []string{*job}
		if *project != "" {
			jobs[0] = *project + "/" + *job
		}
		if sourceID != "" {
			jobs = append(jobs, sourceID)
		}
		if op == "resume" {
			startingIdentity, _, _, err = captureJobIdentity(dir)
			if err != nil {
				return err
			}
			var saved config
			if configErr := readJobConfig(dir, &saved); configErr != nil {
				return configErr
			}
			cwd = saved.Dir
		} else if cwd == "" && (command || c.Shell) {
			cwd = "."
		}
		bound, workspaceErr := bindWorkspace(root, cwd)
		if workspaceErr != nil {
			return workspaceErr
		}
		if expectedWorkspace != "" && bound.key != expectedWorkspace {
			return errJobReplaced
		}
		releaseWorkspace, workspaceErr = bound.lock(ctx, root, jobs...)
		if workspaceErr != nil {
			return workspaceErr
		}
		defer releaseWorkspace()
		if sourceID != "" {
			if identityErr := sourceIdentity.checkDir(sourceDir); identityErr != nil {
				return identityErr
			}
		}
		if op == "start" && bound.path != "" {
			c.Dir = bound.path
		}
	}
	guard, err := registryLock(ctx, root)
	if err != nil {
		return err
	}
	defer func() {
		if guard != nil {
			_ = guard.Close()
		}
	}()
	var owner *os.File
	maintainCacheOnExit := false
	defer func() {
		if owner != nil {
			closeBestEffort(owner)
		}
		if maintainCacheOnExit {
			maintainReleasedCaches(ctx, root)
		}
	}()
	if op == "start" || op == "resume" {
		if formatErr := ensureNodeStateFormat(ctx, root); formatErr != nil {
			return formatErr
		}
	}
	if *project != "" {
		if fi, e := os.Stat(filepath.Join(parent, "owner.lock")); (e != nil && !os.IsNotExist(e)) || (e == nil && fi.Mode().IsRegular()) {
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
		if _, _, err = jobStatusRecord(dir); err == nil {
			if directSubmit {
				if err = checkSubmission(dir, c, s.Profile, fs.Args()); err != nil {
					return err
				}
			}
			return printStatus(dir)
		} else if !errors.Is(err, errNotRegistered) {
			return err
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
			eng, engineErr := lookupEngine(c.Engine)
			if engineErr != nil {
				return engineErr
			}
			c.Engine, s.Engine = eng.name, eng.name
			if engineErr = c.checkEngineOptions(eng, opts, explicitApproval); engineErr != nil {
				return engineErr
			}
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
			if c.ForkThread == "" && opts.input == "" && len(opts.images) == 0 && strings.TrimSpace(c.Prompt) == "" {
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
			if opts.input != "" || len(opts.images) > 0 {
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
		if force {
			c.DedupKey = ""
		}
		if startErr = c.newRunID(); startErr != nil {
			return startErr
		}
		if c.DedupKey != "" {
			if startErr = claimKey(root, c, s); startErr != nil {
				return startErr
			}
		}
		s.LaunchID = launchID
		// An in-process worker keeps the registration lock handle. On Unix,
		// a concurrent fork can keep a closed handle's flock alive until exec.
		if owner, startErr = register(ctx, parent, dir, c, s, (*stateWriter).sync); startErr != nil {
			return startErr
		}
		if len(c.Argv) > 0 {
			key, keyErr := jobDirectoryKey(dir)
			if keyErr != nil {
				return keyErr
			}
			handoff, handoffErr := lockPath(filepath.Join(dir, "handoff.lock"), 0)
			if handoffErr != nil {
				return handoffErr
			}
			if handoff == nil {
				return errors.New("command handoff already active")
			}
			defer closeBestEffort(handoff)
			if closeErr := owner.Close(); closeErr != nil {
				return closeErr
			}
			owner = nil
			_ = guard.Close()
			guard = nil
			var done <-chan error
			if directSubmit && runtime.GOOS == "windows" {
				startErr = launchMuxCommand(ctx, root, *project, *job, c.RunID, key)
			} else {
				done, startErr = launchCommand(root, *project, *job, c.RunID, key)
			}
			if startErr != nil {
				if directSubmit && runtime.GOOS == "windows" && (errors.Is(startErr, context.DeadlineExceeded) || errors.Is(startErr, context.Canceled)) {
					return fmt.Errorf("psmux launch acknowledgement unavailable: %w", startErr)
				}
				var lockErr error
				guard, lockErr = registryLock(ctx, root)
				if lockErr != nil {
					return errors.Join(startErr, lockErr)
				}
				return recordLaunchFailure(ctx, dir, c, startErr)
			}
			return awaitRunner(ctx, dir, c.RunID, "", 0, done, "")
		}
	}

	identity, current, cursor, err := captureJobIdentity(dir)
	if err != nil {
		if op == "clean" && errors.Is(err, os.ErrNotExist) {
			_ = guard.Close()
			guard = nil
			return reapDetachedJobs(root, *project, *job)
		}
		return err
	}
	if startingIdentity.key != "" && startingIdentity != identity {
		return errJobReplaced
	}
	if (op == "_command" && (expectedRun == "" || expectedDir == "")) || (expectedRun != "" && current.RunID != expectedRun) {
		return errJobReplaced
	}
	if expectedDir != "" {
		key, keyErr := jobDirectoryKey(dir)
		if keyErr != nil {
			return keyErr
		}
		if key != expectedDir {
			return errJobReplaced
		}
	}
	if closing != nil {
		if confirmErr := closing.check(dir, current, cursor); confirmErr != nil {
			return confirmErr
		}
	}
	s = current
	if op == "resume" {
		if err = checkResumeProfile(root, s.Profile, requestedProfile); err != nil {
			return err
		}
	}
	ownerPath := filepath.Join(dir, "owner.lock")
	owned := owner != nil
	if !owned {
		owner, err = openJobFile(ownerPath, jobFileLockExisting)
		if err != nil {
			return err
		}
		owned, err = tryAcquireJobOwner(ctx, dir, owner)
		if err != nil {
			return err
		}
	}
	resumeTerminal := op == "resume" && s.final
	if !owned && (terminalClose || resumeTerminal) {
		// The final status is visible before the previous runner releases
		// ownership. Let its teardown finish without blocking the registry.
		deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for !owned {
			if guard != nil {
				closeBestEffort(guard)
				guard = nil
			}
			if err = pause(deadline, 20*time.Millisecond); err != nil {
				return err
			}
			if resumeTerminal {
				// Resume publishes a new generation under the registry lock.
				guard, err = registryLock(deadline, root)
				if err != nil {
					return err
				}
			}
			owned, err = tryAcquireJobOwner(deadline, dir, owner)
			if err != nil {
				return err
			}
		}
	}
	if !owned {
		if op == "resume" && launchID == "" {
			return printStatus(dir)
		}
		return errJobOwnerBusy
	}
	if handleErr := checkJobHandle(owner, ownerPath); handleErr != nil {
		return handleErr
	}
	s, cursor, err = loadJobStatus(dir)
	if err != nil {
		return err
	}
	if identityErr := identity.check(dir, s); identityErr != nil {
		return identityErr
	}
	if resumeTerminal && !s.final {
		return errJobOwnerBusy
	}
	if closing != nil {
		if confirmErr := closing.check(dir, s, cursor); confirmErr != nil {
			return confirmErr
		}
	}
	if err = readJobConfig(dir, &c); err != nil {
		return err
	}
	if c.RunID != s.RunID {
		return errJobReplaced
	}
	if op == "_command" {
		if len(c.Argv) == 0 {
			return errors.New("expected command job")
		}
		if s.final || s.PID != 0 || s.State != "queued" {
			return errors.New("command handoff is no longer pending")
		}
	}

	r := runner{root: root, dir: dir, c: c, s: s, ctx: ctx, clock: runnerPolicyClock(ctx)}
	if op == "clean" || terminalClose {
		if !s.final {
			return errors.New("job is not terminal")
		}
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
		// Reacquire locks in registry -> owner order and pin the terminal
		// cursor that was actually reclaimed.
		if err = owner.Close(); err != nil {
			return err
		}
		owner = nil
		guard, err = registryLock(ctx, root)
		if err != nil {
			return err
		}
		confirmed, confirmedCursor, statusErr := loadJobStatus(dir)
		if statusErr != nil {
			return statusErr
		}
		if identityErr := identity.check(dir, confirmed); identityErr != nil {
			return identityErr
		}
		if !confirmed.final || confirmedCursor != cursor {
			return errJobReplaced
		}
		if detachErr := archiveAndDetachJob(ctx, root, dir, c, r.s, syncJobTree); detachErr != nil {
			return detachErr
		}
		_ = guard.Close()
		guard = nil
		return reapDetachedJobs(root, s.Project, s.Job)
	}

	if len(c.Argv) > 0 && op != "_command" {
		return errors.New("command jobs cannot be replayed by resume")
	}

	if op != "_command" {
		r.s.LaunchID = launchID
	}
	r.s.RunnerStart = processStart(os.Getpid())
	r.s.PID = os.Getpid()
	r.s.EnginePID = 0
	r.s.CommandPID = 0
	r.s.QueueReason = ""
	if op == "resume" {
		r.s.Restarts++
		r.s.Error = ""
		r.s.Approvals = nil
	}
	needsTicket := s.Thread == "" || r.active()
	if needsTicket {
		r.s.State = "queued"
	} else {
		r.s.State = "running"
	}
	if r.c.RunID == "" {
		if err = r.c.newRunID(); err != nil {
			return err
		}
	}
	r.s.RunID = r.c.RunID
	if err = r.openState(); err != nil {
		return err
	}
	defer closeBestEffort(r.stateLog)
	if damage := r.stateLog.RecoveryError(); damage != nil {
		return fmt.Errorf("cannot resume with damaged state checkpoints: %w", damage)
	}
	if err = r.save(); err != nil {
		return err
	}
	defer r.releaseSlot()
	// Register the first ticket under the publication lock to preserve start order.
	var ticket *queueTicket
	if needsTicket {
		ticket, err = r.newTicket()
		if err != nil {
			return r.failStart(err)
		}
	}
	_ = guard.Close()
	guard = nil
	releaseWorkspace()
	if ticket != nil {
		if err = r.admit(ticket); err != nil {
			if errors.Is(err, errQueuedClose) {
				r.s.PID = 0
				r.s.State = "done"
				r.s.CompletedStatus = "interrupted"
				return r.saveTerminal()
			}
			return r.failStart(err)
		}
	}
	if err = r.loadCredentials(); err == nil {
		err = r.prepareCaches()
	}
	if err != nil {
		return r.failStart(err)
	}
	maintainCacheOnExit = true
	defer r.releaseCaches()
	return r.c.driver().run(&r)
}
func main() { mainContext(context.Background()) }

func mainContext(parent context.Context) {
	if len(os.Args) > 1 && os.Args[1] == "_command" {
		ignoreHangup()
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dispatch(ctx, os.Args[1:]); err != nil {
		var code ctlExit
		if errors.As(err, &code) {
			var reason resultUnavailable
			if errors.As(err, &reason) {
				diagnosticf("cx: %s\n", reason)
			}
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
		case errors.Is(err, errStateHistoryBusy):
			diagnosticf("cxrun: %v\n", err)
			os.Exit(9)
		}
		var result stateFailure
		if errors.As(err, &result) {
			os.Exit(terminalCodes[string(result)])
		}
		if errors.Is(err, errDrained) {
			os.Exit(10)
		}
		diagnosticf("cxrun: %v\n", err)
		os.Exit(1)
	}
}

func (c *config) newRunID() error {
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	c.RunID = hex.EncodeToString(nonce[:])
	return err
}
