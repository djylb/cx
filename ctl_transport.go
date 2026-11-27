package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"

	"github.com/BurntSushi/toml"
)

type ctlExit int

func (e ctlExit) Error() string { return fmt.Sprintf("exit %d", e) }

type endpoint struct {
	relay    *endpoint
	Via      string `toml:"via"`
	SSH      string `toml:"ssh"`
	CX       string `toml:"cx"`
	State    string `toml:"state"`
	Platform string `toml:"platform"`
	// Manager marks a host that runs its own managing session: submissions to it
	// first bring its read-only copy of the context layers up to date.
	Manager   bool `toml:"manager"`
	defaultCX bool
}
type endpoints map[string]endpoint

func loadEndpoints() (endpoints, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	cfg := hostFile{Hosts: endpoints{"local": {Platform: runtime.GOOS + "/" + runtime.GOARCH}}}
	md, err := toml.DecodeFile(filepath.Join(home, ".config", "cx", "hosts.toml"), &cfg)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if keys := md.Undecoded(); len(keys) > 0 {
		return nil, fmt.Errorf("unknown host setting %s", keys[0])
	}
	for name, h := range cfg.Hosts {
		if _, err := jobPath("", name); err != nil {
			return nil, err
		}
		if name != "local" && (h.SSH == "" || strings.HasPrefix(h.SSH, "-") || strings.ContainsAny(h.SSH, " \r\n\t")) {
			return nil, fmt.Errorf("%s: invalid ssh alias", name)
		}
		if name == "local" && h.SSH != "" {
			return nil, errors.New("local uses direct execution")
		}
		if h.CX == "" {
			h.defaultCX = true
			h.CX = defaultBinary(h)
		}
		cfg.Hosts[name] = h
	}
	for n, h := range cfg.Hosts {
		resolved, e := cfg.Hosts.resolve(n, map[string]bool{})
		if e != nil {
			return nil, e
		}
		h.relay = resolved.relay
		cfg.Hosts[n] = h
	}
	return cfg.Hosts, nil
}
func (h endpoint) withPlatform(plat string) endpoint {
	h.Platform = plat
	if h.defaultCX {
		h.CX = defaultBinary(h)
	}
	return h
}

func defaultBinary(h endpoint) string {
	if h.SSH == "" {
		return "cx"
	}
	if strings.HasPrefix(h.Platform, "windows") {
		return ".local/bin/cx.exe"
	}
	return ".local/bin/cx"
}
func (hs endpoints) resolve(name string, seen map[string]bool) (endpoint, error) {
	h, ok := hs[name]
	if !ok || seen[name] {
		return endpoint{}, fmt.Errorf("invalid via chain at %s", name)
	}
	if h.Via != "" {
		seen[name] = true
		r, e := hs.resolve(h.Via, seen)
		if e != nil {
			return h, e
		}
		if r.SSH == "" || h.SSH == "" {
			return h, errors.New("via requires remote endpoints")
		}
		h.relay = &r
	}
	return h, nil
}
func (hs endpoints) names() []string {
	if s := os.Getenv("CX_HOSTS"); s != "" {
		return strings.Split(s, ",")
	}
	var names []string
	for n := range hs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
func (hs endpoints) get(n string) (endpoint, error) {
	h, ok := hs[n]
	if !ok {
		return h, fmt.Errorf("unknown host %q", n)
	}
	return h, nil
}

func dispatch(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "node" {
		if len(args) == 1 || strings.HasPrefix(args[1], "-") {
			if len(args) == 2 && isHelp(args[1]) {
				printCtlHelp(os.Stdout, "node")
				return nil
			}
			printCtlHelp(os.Stderr, "node")
			return ctlExit(2)
		}
		return nodeCLI(ctx, args[1:])
	}
	name := strings.TrimSuffix(strings.ToLower(filepath.Base(os.Args[0])), ".exe")
	if name == "cxrun" || name == "cxrun.bin" || strings.HasPrefix(name, "cxrun-") || len(args) > 0 && args[0] == "_command" {
		return nodeCLI(ctx, args)
	}
	err := controller(ctx, args)
	if err == nil {
		return nil
	}
	if code, ok := errors.AsType[ctlExit](err); ok {
		if code < 0 || code > 255 {
			return ctlExit(125)
		}
		return err
	}
	if cause, ok := errors.AsType[errSignal](context.Cause(ctx)); ok && errors.Is(err, context.Canceled) {
		err = cause
	}
	diagnosticf("cx: %v\n", err)
	if errors.Is(err, context.DeadlineExceeded) {
		return ctlExit(124)
	}
	if errors.Is(err, context.Canceled) {
		return ctlExit(130)
	}
	if sig, ok := errors.AsType[errSignal](err); ok {
		if s, isSys := sig.sig.(syscall.Signal); isSys {
			return ctlExit(128 + int(s))
		}
		return ctlExit(130)
	}
	return ctlExit(2)
}

type invocation struct {
	State  string
	Args   []string
	Budget time.Duration
}

func nodeCLI(ctx context.Context, args []string) error {
	if len(args) >= 1 && (args[0] == "wire" || args[0] == "watch-wire") {
		var b []byte
		var err error
		switch len(args) {
		case 1:
			if args[0] == "watch-wire" {
				input := bufio.NewReader(os.Stdin)
				b, err = input.ReadBytes('\n')
				if err == nil {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					defer cancel()
					go func() { _, _ = io.Copy(io.Discard, input); cancel() }()
				}
			} else {
				b, err = io.ReadAll(os.Stdin)
			}
		case 2:
			b, err = base64.RawURLEncoding.DecodeString(args[1])
		default:
			return errors.New("invalid wire arguments")
		}
		if err != nil {
			return err
		}
		var req invocation
		if err = json.Unmarshal(b, &req); err != nil {
			return err
		}
		if req.Budget > 0 {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeout(ctx, req.Budget)
			defer stop()
		}
		if req.State != "" {
			if err = os.Setenv("CXRUN_STATE", req.State); err != nil {
				return err
			}
		}
		args = req.Args
		if len(args) > 0 && !strings.HasPrefix(args[0], "forward") && args[0] != "platform-probe" {
			if _, err := io.WriteString(os.Stderr, wireReady); err != nil {
				return err
			}
		}
	}
	if len(args) > 0 {
		switch args[0] {
		case "channel":
			if len(args) == 2 {
				return channelNode(ctx, args[1])
			}
			return errors.New("invalid channel request")
		case "forward-message":
			if len(args) == 3 {
				return forwardMessage(args[1], args[2])
			}
			return errors.New("invalid message request")
		case "runner-worker":
			if len(args) == 2 {
				return runMuxWorker(ctx, args[1])
			}
			return errors.New("invalid mux worker arguments")
		case "seed-cache":
			if len(args) == 3 {
				return seedRelayCache(args[1], args[2])
			}
			return errors.New("invalid cache seed request")
		case "compiler-protocol":
			fmt.Println("1")
			return nil
		case "compiler-install":
			if len(args) == 4 {
				return centerInstall(ctx, args[1], args[2], args[3])
			}
			return errors.New("invalid compiler request")
		case "task-receive":
			return receiveTask(ctx, args[1:])
		case "start-installed-task":
			if len(args) == 3 {
				return startInstalledTask(ctx, args[1], args[2])
			}
			return errors.New("invalid task request")
		case "snap-receive":
			return ReceiveSnapshot(ctx, args[1:])
		case "submit-worker":
			if len(args) == 3 {
				return submissionWorker(ctx, args[1], args[2])
			}
			return errors.New("invalid submission worker arguments")
		case "continue-job":
			if len(args) == 4 {
				return continueNode(ctx, args[1], args[2], args[3], "")
			}
			if len(args) == 5 {
				return continueNode(ctx, args[1], args[2], args[3], args[4])
			}
			return errors.New("invalid continue arguments")
		case "context-put":
			if len(args) == 2 {
				return nodeContextPut(args[1])
			}
			return errors.New("invalid context push")
		case "mirror-refs":
			if len(args) == 3 {
				return mirrorRefs(ctx, args[1], args[2])
			}
			return errors.New("invalid mirror refs")
		case "find-reuse":
			if len(args) == 2 {
				return findReuse(ctx, args[1])
			}
			return errors.New("invalid reuse query")
		case "clean-job":
			if len(args) == 2 {
				return cleanNodeJob(ctx, args[1])
			}
			return errors.New("invalid clean arguments")
		case "refresh-job":
			return nodeRefresh(ctx, args[1:])
		case "rebase-worker":
			return rebaseWorker(ctx, args[1:])
		case "result-download":
			return nodeResult(ctx, args[1:])
		case "observe-result":
			return nodeObserveResult(ctx, args[1:])
		case "start-job":
			if len(args) == 2 {
				return startNodeJob(ctx, args[1])
			}
			return errors.New("invalid job request")
		case "restart-job":
			return restartNode(ctx, args[1:], nil, os.Stdout)
		case "submit-script":
			return receiveScript(ctx, args)
		case "control-action":
			return nodeAction(ctx, args[1:])
		case "outbox-all":
			return nodeOutboxAll(ctx, args[1:])
		case "version":
			return printVersion()
		case "init-host":
			root, e := stateRoot()
			if e != nil {
				return e
			}
			return hostSettings(ctx, root, nil, true)
		case "forward", "forward-stream", "platform-probe", "preinstall-doctor":
			return relayNode(ctx, args)
		case "cache-path", "cache-install", "relay-setup":
			return cacheNode(ctx, args)
		case "recv":
			return receive(ctx, args[1:])
		case "adopt-job", "follow", "observe-one", "wait-ack", "submit-if-version", "install", "host-config", "check-profile", "doctor", "show", "overview", "rows", "wait-poll", "submit-command":
			return nodeControl(ctx, args)
		}
	}
	return run(ctx, args)
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func psArgs(script string) []string {
	script = "$ProgressPreference='SilentlyContinue';" + script
	var b []byte
	for _, c := range utf16.Encode([]rune(script)) {
		b = append(b, byte(c), byte(c>>8))
	}
	return []string{"-NoProfile", "-NonInteractive", "-EncodedCommand", base64.StdEncoding.EncodeToString(b)}
}
func psCommand(script string) string {
	return "powershell " + strings.Join(psArgs(script), " ")
}
func psQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

const missingNodeBinary = 127

func windowsNodeCommand(binary string, argv []string, probe bool) string {
	script := "$ErrorActionPreference='Stop';"
	script += "$p=New-Object System.Diagnostics.ProcessStartInfo;$p.UseShellExecute=$false;$p.FileName=" + psQuote(binary) + ";$p.Arguments=" + psQuote(strings.Join(argv, " ")) + ";"
	if probe {
		script += "try {$c=[Diagnostics.Process]::Start($p)} catch {$err=$_.Exception.GetBaseException();if ($err -is [ComponentModel.Win32Exception] -and $err.NativeErrorCode -in 2,3) {exit " + strconv.Itoa(missingNodeBinary) + "};throw};"
	} else {
		script += "$c=[Diagnostics.Process]::Start($p);"
	}
	script += "$c.WaitForExit();exit $c.ExitCode"
	return psCommand(script)
}

func (h endpoint) command(ctx context.Context, args []string) (*exec.Cmd, error) {
	// These callers manage their own clocks around the returned command.
	cmd, e := h.transportCommand(ctx, args, 0)
	if e != nil {
		return nil, e
	}
	return cmd.Cmd, nil
}

type relayTarget struct {
	endpoint
	Idle time.Duration `json:"idle"`
}

func (h endpoint) transportCommand(ctx context.Context, args []string, idle time.Duration) (*transportProcess, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	stream := (len(args) == 4 && args[0] == "recv") || (len(args) > 0 && (args[0] == "forward-stream" || args[0] == "submit-script" || args[0] == "snap-receive" || args[0] == "task-receive"))
	if h.relay != nil {
		op := "forward"
		if stream {
			op = "forward-stream"
		}
		dest, e := json.Marshal(relayTarget{h, idle})
		if e != nil {
			return nil, e
		}
		return h.relay.transportCommand(ctx, append([]string{op, string(dest)}, args...), idle)
	}
	var budget time.Duration
	if end, ok := ctx.Deadline(); ok {
		budget = time.Until(end)
		if budget <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	b, err := json.Marshal(invocation{h.State, args, budget})
	if err != nil {
		return nil, err
	}
	argv := []string{"node", "wire"}
	watch := args
	for len(watch) > 2 && watch[0] == "forward" {
		watch = watch[2:]
	}
	live := len(watch) > 0 && (watch[0] == "adopt-job" || watch[0] == "channel" || watch[0] == "wait-poll" || watch[0] == "observe-one" || watch[0] == "observe-result" || watch[0] == "result-download" || watch[0] == "follow")
	if live {
		argv[1] = "watch-wire"
	}

	if stream {
		argv = append(argv, base64.RawURLEncoding.EncodeToString(b))
	}
	cmd := &transportProcess{ctx: ctx, connect: idle, idle: idle}
	if h.SSH == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		cmd.Cmd = exec.CommandContext(ctx, exe, argv...)
	} else {
		line := shellQuote(h.CX) + " " + strings.Join(argv, " ")
		probe := len(args) > 0 && (args[0] == "version" || args[0] == "submit-if-version" || args[0] == "doctor" || args[0] == "compiler-protocol")
		if strings.HasPrefix(h.Platform, "windows") {
			line = windowsNodeCommand(h.CX, argv, probe)
		} else if probe {
			binary := shellQuote(h.CX)
			line = "command -v " + binary + " >/dev/null 2>&1 || test -e " + binary + " || exit 127; " + line
		}
		opts, master, e := endpointSSHOptions(ctx, h.SSH)
		if e != nil {
			return nil, e
		}
		cmd.Cmd = exec.CommandContext(ctx, "ssh", append(opts, "--", h.SSH, line)...)
		cmd.master, cmd.connect = master, sshConnect
	}
	if live {
		wr, e := cmd.StdinPipe()
		if e != nil {
			return nil, e
		}
		// EOF cancels watch-wire on the node. Start and Wait own the child end.
		cmd.Cancel = wr.Close
		cmd.WaitDelay = time.Second
		go func() { _, _ = wr.Write(append(b, '\n')) }()
	} else if !stream {
		cmd.Stdin = bytes.NewReader(b)
	}
	return cmd, nil
}

// runIdle uses the caller's deadline and the transport's connection and
// activity clocks. Only the SSH multiplexing fallback can replay the command.
func (h endpoint) runIdle(ctx context.Context, out io.Writer, args ...string) (string, error) {
	idle, err := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if err != nil {
		return "", err
	}
	for {
		attempt, stop := context.WithCancel(ctx)
		cmd, startErr := h.transportCommand(attempt, args, idle)
		if startErr != nil {
			stop()
			return "", startErr
		}
		var diag bytes.Buffer
		err = cmd.run(ctx, stop, out, &diag)
		stop()
		if !errors.Is(err, errSSHMasterUnsupported) {
			return strings.TrimSpace(cleanDiag(diag.Bytes())), err
		}
	}
}

func (h endpoint) callIdle(ctx context.Context, step string, args ...string) (reply, error) {
	var out bytes.Buffer
	diag, err := h.runIdle(ctx, &out, args...)
	if err == nil {
		return reply{data: out.Bytes()}, nil
	}
	if code, ok := h.nodeExit(err); ok {
		if diag != "" {
			diagnosticf("%s: %s\n", step, diag)
		}
		return reply{data: out.Bytes(), code: code}, nil
	}
	err = idleCallError(ctx, step, diag, err)
	if errors.Is(err, ctlExit(9)) {
		diagnosticf("cx: %v\n", err)
	}
	return reply{}, err
}

func (h endpoint) nodeExit(err error) (int, bool) {
	var exit *exec.ExitError
	if errors.As(err, &exit) && (exit.ExitCode() != 255 || h.SSH == "") && exit.ExitCode() >= 0 {
		return exit.ExitCode(), true
	}
	return 0, false
}

func idleCallError(ctx context.Context, step, diag string, err error) error {
	if cause := ctx.Err(); cause != nil {
		return fmt.Errorf("%s: %w", step, cause)
	}
	if errors.Is(err, errWireSilence) {
		err = fmt.Errorf("no progress: %w", err)
	}
	if diag != "" {
		err = fmt.Errorf("%w: %s", err, diag)
	}
	return fmt.Errorf("%s: %w: %v", step, ctlExit(9), err)
}

type reply struct {
	data []byte
	code int
}

func (h endpoint) call(ctx context.Context, per time.Duration, args ...string) (reply, error) {
	return h.invoke(ctx, per, true, args...)
}
func (h endpoint) invoke(parent context.Context, per time.Duration, replay bool, args ...string) (reply, error) {
	return h.invokeWithDiagnostics(parent, per, replay, true, args...)
}

func (h endpoint) invokeWithDiagnostics(parent context.Context, per time.Duration, replay, diagnose bool, args ...string) (reply, error) {
	connect := h.connectionBudget(per)
	ctx, cancel := context.WithTimeout(parent, connect+3*per+15*time.Second)
	defer cancel()
	for i := 0; ; i++ {
		attempt, stop := context.WithCancel(ctx)
		cmd, err := h.transportCommand(attempt, args, per)
		if err != nil {
			stop()
			if cause := parent.Err(); cause != nil {
				return reply{}, errors.Join(ctlExit(9), cause)
			}
			return reply{}, err
		}
		var out, diag bytes.Buffer
		err = cmd.run(ctx, stop, &out, &diag)
		b := out.Bytes()
		stop()
		if errors.Is(err, errSSHMasterUnsupported) {
			i--
			continue
		}
		if err == nil {
			return reply{b, 0}, nil
		}
		if code, ok := h.nodeExit(err); ok {
			if diagnose && diag.Len() > 0 {
				diagnosticf("%s", cleanDiag(diag.Bytes()))
			}
			return reply{b, code}, nil
		}
		if cause := parent.Err(); cause != nil {
			return reply{}, errors.Join(ctlExit(9), cause)
		}
		if !replay || i == 3 || ctx.Err() != nil || h.SSH == "" {
			if !diagnose {
				return reply{}, fmt.Errorf("%w: %v %s", ctlExit(9), err, cleanDiag(diag.Bytes()))
			}
			diagnosticf("node invocation: %v %s\n", err, cleanDiag(diag.Bytes()))
			return reply{}, ctlExit(9)
		}
		if err = pause(ctx, time.Second<<i); err != nil {
			return reply{}, errors.Join(ctlExit(9), parent.Err())
		}
	}
}
func (r reply) output() error {
	if _, err := os.Stdout.Write(r.data); err != nil {
		return err
	}
	if r.code != 0 {
		return ctlExit(r.code)
	}
	return nil
}

func relayNode(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("missing relay destination")
	}
	var dest relayTarget
	if e := json.Unmarshal([]byte(args[1]), &dest); e != nil {
		return e
	}
	if args[0] == "preinstall-doctor" {
		f, e := preinstallDoctor(ctx, dest.endpoint)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(f)
	}
	if args[0] == "platform-probe" {
		p, e := detectPlatform(ctx, dest.endpoint)
		if e == nil {
			fmt.Println(p)
		}
		return e
	}
	// Each relay acknowledges and monitors its downstream command. Upstream
	// sees this link's heartbeat while a later hop connects or recovers.
	relayCtx, finish := context.WithCancel(ctx)
	defer finish()
	if dest.Idle > 0 {
		if _, e := io.WriteString(os.Stderr, wireReady); e != nil {
			return e
		}
		go func() {
			ticker := time.NewTicker(min(time.Second, max(time.Nanosecond, dest.Idle/3)))
			defer ticker.Stop()
			for {
				select {
				case <-relayCtx.Done():
					return
				case <-ticker.C:
					if _, e := io.WriteString(os.Stderr, wireProgress); e != nil {
						finish()
						return
					}
				}
			}
		}()
	}
	for {
		attempt, stop := context.WithCancel(relayCtx)
		cmd, e := dest.transportCommand(attempt, args[2:], dest.Idle)
		if e != nil {
			stop()
			return e
		}
		if args[0] == "forward-stream" {
			cmd.Stdin = os.Stdin
		}
		var diag bytes.Buffer
		if dest.Idle > 0 {
			e = cmd.run(relayCtx, stop, os.Stdout, &diag)
		} else {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.WaitDelay = time.Second
			e = cmd.Run()
		}
		stop()
		if errors.Is(e, errSSHMasterUnsupported) {
			continue
		}
		if s := cleanDiag(diag.Bytes()); s != "" {
			diagnosticf("%s", s)
		}
		if errors.Is(e, errWireSilence) {
			diagnosticf("cx: relay to %s: no progress: %v\n", dest.SSH, e)
			return ctlExit(255)
		}
		if ex, ok := errors.AsType[*exec.ExitError](e); ok {
			return ctlExit(ex.ExitCode())
		}
		return e
	}
}
