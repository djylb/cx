package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"
)

// defaultEngine is the single place that names the engine used when a job does not choose one.
const defaultEngine = "codex"

// capabilities describe what an engine adapter supports. Generic code consults them instead of
// comparing engine names, and submissions are checked against them before any work starts.
type capabilities struct {
	Steer, Fork, Resume, Approvals, StructuredEvents bool
	JobTools, Effort, Network, WritableRoots         bool
	// RPC covers the engine's own control surface (threads, history, settings, review, ...).
	RPC bool
	// NetworkIsolation means the sandbox can deny network access to commands the agent runs.
	NetworkIsolation bool
}

type driver interface {
	capabilities() capabilities
	run(*runner) error
	// restartAction names stall-recovery step 3 for this driver; restart performs it.
	restartAction() string
	restart(*runner) error
	interrupt(*runner) error
	// checkInput validates user input parts ({"type":"text"|"localImage"|"image",...}).
	checkInput([]json.RawMessage) error
}

// engineSpec is the authoritative description of one agent engine: its adapter, its
// capabilities and how hosts probe its CLI version and login state.
type engineSpec struct {
	drv    driver
	name   string
	binary string
	// homeEnv is the environment variable that selects the engine's account directory.
	homeEnv string
	// Login output is lowercased with whitespace removed before matching. loggedOut wins;
	// otherwise a zero exit means logged in when loggedIn is empty or matches.
	loggedIn, loggedOut    string
	versionArgs, loginArgs []string
}

var engines = []engineSpec{
	{name: "codex", binary: "codex", homeEnv: "CODEX_HOME", drv: codexDriver{},
		versionArgs: []string{"--version"}, loginArgs: []string{"login", "status"}, loggedOut: "notloggedin"},
	{name: "claude", binary: "claude", homeEnv: "CLAUDE_CONFIG_DIR", drv: claudeDriver{},
		versionArgs: []string{"--version"}, loginArgs: []string{"auth", "status", "--json"},
		loggedIn: `"loggedin":true`, loggedOut: `"loggedin":false`},
}

func engineNames() []string {
	names := make([]string, len(engines))
	for i, e := range engines {
		names[i] = e.name
	}
	return names
}

// lookupEngine resolves an engine name; the empty name selects defaultEngine.
func lookupEngine(name string) (engineSpec, error) {
	if name == "" {
		name = defaultEngine
	}
	i := slices.IndexFunc(engines, func(e engineSpec) bool { return e.name == name })
	if i < 0 {
		return engineSpec{}, fmt.Errorf("unknown engine %q (known: %s)", name, strings.Join(engineNames(), ", "))
	}
	return engines[i], nil
}

func (e engineSpec) caps() capabilities { return e.drv.capabilities() }

// unsupported reports an option the engine cannot honour, so callers fail instead of dropping it.
func (e engineSpec) unsupported(what string) error {
	return fmt.Errorf("%s not supported by engine %s", what, e.name)
}

// loginState classifies login output without retaining it: it can contain account details.
func (e engineSpec) loginState(out []byte, exitErr error) any {
	text := strings.ToLower(string(bytes.Join(bytes.Fields(out), nil)))
	switch {
	case e.loggedOut != "" && strings.Contains(text, e.loggedOut):
		return "logged_out"
	case exitErr == nil && (e.loggedIn == "" || strings.Contains(text, e.loggedIn)):
		return "logged_in"
	case exitErr == nil:
		return "logged_out"
	}
	if _, ok := errors.AsType[*exec.ExitError](exitErr); ok {
		return "logged_out"
	}
	return nil
}

func (e engineSpec) probeLogin(ctx context.Context, env []string) any {
	ctx, cancel := context.WithTimeout(ctx, engineLoginTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, e.binary, e.loginArgs...)
	if len(env) > 0 {
		cmd.Env = append(cmd.Environ(), env...)
	}
	cmd.WaitDelay = 200 * time.Millisecond
	b, err := cmd.CombinedOutput()
	if ctx.Err() != nil || errors.Is(err, exec.ErrNotFound) {
		return nil
	}
	return e.loginState(b, err)
}

// engineLoginTimeout bounds one login probe. Engine CLIs start slowly on loaded or remote nodes.
const engineLoginTimeout = 8 * time.Second

// engineFacts is the per-engine probe shared by host facts, doctor and credential doctor.
// Output: {"engines": {NAME: {"path", "version", "login_status"}}}.
func engineFacts(ctx context.Context, env []string, version bool) fields {
	type result struct {
		info fields
		name string
	}
	ch := make(chan result, len(engines))
	for _, e := range engines {
		go func() {
			info := fields{"path": nil, "login_status": nil}
			if version {
				info["version"] = nil
			}
			if p, err := exec.LookPath(e.binary); err == nil {
				info["path"] = p
				if version {
					info["version"] = stringOrNil(probe(ctx, append([]string{e.binary}, e.versionArgs...)...))
				}
				info["login_status"] = e.probeLogin(ctx, env)
			}
			ch <- result{info, e.name}
		}()
	}
	out := fields{}
	for range engines {
		r := <-ch
		out[r.name] = r.info
	}
	return out
}

// engineDoctorSh prints "engine\tNAME\tPATH\tLOGIN" for each installed engine with stock tools,
// applying the same login rule as loginState.
func engineDoctorSh() string {
	var b strings.Builder
	for _, e := range engines {
		args := make([]string, len(e.loginArgs))
		for i, a := range e.loginArgs {
			args[i] = shellQuote(a)
		}
		out, in := "", "*"
		if e.loggedOut != "" {
			out = "*" + shellQuote(e.loggedOut) + "*) s=logged_out ;; "
		}
		if e.loggedIn != "" {
			in = "*" + shellQuote(e.loggedIn) + "*"
		}
		if _, err := fmt.Fprintf(&b, "p=$(command -v %s); if [ -n \"$p\" ]; then o=$(\"$p\" %s 2>&1); rc=$?; o=$(printf %%s \"$o\" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]'); "+
			"case \"$o\" in %s*) if [ $rc = 0 ]; then case \"$o\" in %s) s=logged_in ;; *) s=logged_out ;; esac; else s=logged_out; fi ;; esac; "+
			"printf 'engine\\t%%s\\t%%s\\t%%s\\n' %s \"$p\" \"$s\"; fi\n",
			shellQuote(e.binary), strings.Join(args, " "), out, in, shellQuote(e.name)); err != nil {
			panic(err) // strings.Builder.Write always returns nil.
		}
	}
	return b.String()
}

// engineDoctorPS fills $engines with the same per-engine fields on Windows.
func engineDoctorPS() string {
	var b strings.Builder
	b.WriteString("$engines=@{};")
	for _, e := range engines {
		args := make([]string, len(e.loginArgs))
		for i, a := range e.loginArgs {
			args[i] = psQuote(a)
		}
		if _, err := fmt.Fprintf(&b, "$p=(Get-Command %s -ErrorAction SilentlyContinue).Source;$s=$null;if($p){$o=(((& $p %s 2>&1)|Out-String).ToLower() -replace '\\s','');$rc=$LASTEXITCODE;"+
			"if(%s -and $o.Contains(%s)){$s='logged_out'}elseif($rc -eq 0 -and (%s -eq '' -or $o.Contains(%s))){$s='logged_in'}else{$s='logged_out'}};"+
			"$engines[%s]=@{path=$p;login_status=$s};",
			psQuote(e.binary), strings.Join(args, " "), psQuote(e.loggedOut), psQuote(e.loggedOut), psQuote(e.loggedIn), psQuote(e.loggedIn), psQuote(e.name)); err != nil {
			panic(err) // strings.Builder.Write always returns nil.
		}
	}
	return b.String()
}

// commandNeeds names the capability an engine-specific agent command requires; help lists
// the engines providing it and the node rejects the command for other engines.
var commandNeeds = map[string]func(capabilities) bool{
	"compact": rpcCap, "history": rpcCap, "limits": rpcCap, "mcp": rpcCap, "models": rpcCap,
	"review": rpcCap, "rpc": rpcCap, "settings": rpcCap, "threads": rpcCap, "undo": rpcCap, "usage": rpcCap,
	"fork":    func(c capabilities) bool { return c.Fork },
	"approve": approvalCap, "deny": approvalCap, "reply": approvalCap, "requests": approvalCap,
	"resume": func(c capabilities) bool { return c.Resume },
}

// checkJobCapability rejects an engine-specific command for a job whose engine lacks it.
func checkJobCapability(dir, op string, need func(capabilities) bool) error {
	var s status
	if err := loadJobStatusInto(dir, &s); err != nil {
		return err
	}
	if s.Engine == "cmd" {
		return nil // command jobs answer these with their own (empty) results
	}
	e, err := lookupEngine(s.Engine)
	if err != nil || s.Engine == "" {
		return errors.Join(errMissingEngine, err)
	}
	if !need(e.caps()) {
		return e.unsupported(op)
	}
	return nil
}

func rpcCap(c capabilities) bool      { return c.RPC }
func approvalCap(c capabilities) bool { return c.Approvals }

func enginesWith(need func(capabilities) bool) []string {
	var names []string
	for _, e := range engines {
		if need(e.caps()) {
			names = append(names, e.name)
		}
	}
	return names
}

// checkEngineOptions rejects job options the engine cannot honour, before the job is registered.
func (c *config) checkEngineOptions(e engineSpec, o options, explicitApproval bool) error {
	cp := e.caps()
	switch {
	case c.Effort != "" && !cp.Effort:
		return e.unsupported("--effort")
	case len(c.Roots) > 0 && !cp.WritableRoots:
		return e.unsupported("--writable-root")
	case c.ForkThread != "" && !cp.Fork:
		return e.unsupported("fork")
	case explicitApproval && !cp.Approvals:
		return e.unsupported("--approval-timeout")
	case c.JobTools && !cp.JobTools:
		return e.unsupported("job tools")
	case c.Network && !cp.Network:
		return e.unsupported("--network")
	case !c.Network && c.Sandbox == "workspace-write" && !cp.NetworkIsolation:
		return e.unsupported("network isolation for workspace-write (pass --network)")
	}
	if o.input != "" || len(o.images) > 0 {
		parts, err := o.inputParts("")
		if err != nil {
			return err
		}
		if len(parts) != 0 {
			return e.checkInput(parts)
		}
	}
	return nil
}

// liveAgentEngine returns the engine of a running agent job.
func liveAgentEngine(s status) (engineSpec, error) {
	switch {
	case s.Engine == "cmd":
		return engineSpec{}, errors.New("command jobs have no agent engine")
	case isTerminal(s.State):
		return engineSpec{}, errors.New("job has ended; resume it first")
	case s.Engine == "":
		return engineSpec{}, errMissingEngine
	}
	return lookupEngine(s.Engine)
}

// checkInput validates user input parts before they are queued, so limits fail early.
func (e engineSpec) checkInput(parts []json.RawMessage) error { return e.drv.checkInput(parts) }
