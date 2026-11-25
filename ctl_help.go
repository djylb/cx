package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

type ctlCommand struct {
	usage, summary, flags string
}

const submitFlags = "profile engine model effort sandbox role parent snapshot with-history input-json image idle-exit turn-timeout approval-timeout stall queue-timeout inplace interactive web-search hard network no-network force sync no-tools clean-base"

var ctlCommands = map[string]ctlCommand{
	"agent":        {"[--host HOST] [--dir DIR] TEXT|@FILE|-\n  cx agent --continue JOB TEXT|@FILE|-", "Submit or continue an agent and read its report.", submitFlags + " dir host tag continue full"},
	"apply":        {"JOB [DIR]", "Check and apply a collected patch.", "turn since-turn allow-conflicts repo max-bytes"},
	"approve":      {"JOB", "Approve a pending request.", ""},
	"chat":         {"post|read|wait|forward ...", "Post, read, wait for, or forward project messages.", ""},
	"chat-forward": {"JOB SEQ CHANNEL", "Forward an outbox message to a channel.", "max-bytes"},
	"chat-post":    {"CHANNEL TEXT|@FILE", "Post a message to a channel.", "type max-bytes"},
	"chat-read":    {"CHANNEL", "Read channel messages.", "since cursor timeout max-bytes latest"},
	"chat-wait":    {"CHANNEL", "Wait for matching channel messages.", "since cursor timeout mention max-bytes"},
	"clean":        {"JOB", "Remove a terminal job's private state.", ""},
	"close":        {"JOB", "Close a job and release its private checkout when unused.", ""},
	"compact":      {"JOB", "Compact the agent thread.", ""},
	"deny":         {"JOB", "Deny a pending request.", ""},
	"diff":         {"JOB", "Read recorded diff events.", ""},
	"doctor":       {"[HOST ...]", "Check execution hosts.", "json"},
	"events":       {"JOB", "Read recorded events.", ""},
	"exec":         {"HOST [options] -- ARGV...\n  cx exec HOST --script FILE [--shell SHELL] [options]", "Submit a command job.", "project tag cwd timeout quiet-timeout queue-timeout wait session script shell reuse env profile"},
	"fork":         {"JOB --job CHILD [--through TURN] [--profile NAME]", "Fork an agent thread on its current host.", ""},
	"gate":         {"DIR [GATE-ARGS ...]", "Run the source directory's scripts/gate.sh.", ""},
	"history":      {"JOB [read|turns|items]", "Read agent thread history.", ""},
	"host":         {"HOST", "Read or update node configuration.", "set"},
	"hosts":        {"[HOST]", "Print current execution host facts.", ""},
	"interrupt":    {"JOB", "Stop an active turn or command.", ""},
	"limits":       {"JOB", "Read the engine account's rate limits.", ""},
	"ls":           {"[JOB]", "List jobs.", "json watch interval project session role all all-hosts max-bytes"},
	"mcp":          {"JOB list|reload|login|resource|call", "Call an engine MCP operation.", ""},
	"mirror":       {"[DIR | --project PROJECT]", "Publish a source baseline.", "project"},
	"models":       {"JOB", "List the engine's available models.", ""},
	"node":         {"COMMAND [ARGS ...]", "Run an execution-node command.", ""},
	"outbox":       {"JOB [--since N]\n  cx outbox --any [--project PROJECT] [--since N]", "Read job messages after a sequence.", "any project since"},
	"overview":     {"", "Summarize jobs and project channels.", "json project session all max-bytes"},
	"patch":        {"JOB", "Show collected patch statistics or contents.", "turn incremental full repo max-bytes"},
	"plan":         {"JOB", "Read recorded plan events.", ""},
	"project":      {"ls | show NAME | init NAME --repo DIR [DIR ...] [--default-host HOST]", "List, inspect, or initialize projects.", ""},
	"project-init": {"NAME --repo DIR [DIR ...] [--default-host HOST]", "Initialize a project.", ""},
	"project-ls":   {"", "List configured projects.", ""},
	"project-show": {"NAME", "Print project configuration.", ""},
	"rebase":       {"JOB TEXT", "Submit a follow-up to resolve patch conflicts.", ""},
	"reply":        {"JOB [--request ID] (--result JSON|@FILE | --error JSON|@FILE)", "Reply to a pending engine request.", ""},
	"requests":     {"JOB", "List pending engine requests.", ""},
	"result":       {"JOB", "Collect a sealed turn's report, JSON metadata, or full log.", "turn full json log wait refresh max-bytes"},
	"resume":       {"JOB [--profile NAME]", "Restart a recoverable agent job.", ""},
	"review":       {"JOB --target JSON|@FILE", "Start an engine review.", ""},
	"rpc":          {"JOB METHOD", "Invoke an engine RPC method.", ""},
	"run":          {"HOST DIR TAG PROMPTFILE [options]\n  cx run HOST DIR TAG --cmd [options] -- ARGV...\n  cx run HOST DIR --batch FILE [options]", "Submit an agent or command job.", submitFlags + " batch cmd reuse timeout quiet-timeout"},
	"say":          {"JOB [TEXT|@FILE]", "Send input or a reply to a job.", ""},
	"settings":     {"JOB --scope turn|thread [--model MODEL] [--effort EFFORT]", "Update agent turn or thread settings.", ""},
	"setup":        {"HOST", "Install or update cx on an execution host.", ""},
	"show":         {"JOB", "Show a job or turn summary.", "full json turn max-bytes"},
	"snapshot":     {"DIR", "Freeze and publish a source snapshot.", "with-history"},
	"status":       {"JOB", "Print the current job status.", "json"},
	"tail":         {"JOB", "Follow job progress, agent messages or raw output.", "raw messages cursor since from-start timeout"},
	"test":         {"[TEST-ARGS ...]", "Run the executable configured by CX_TEST_COMMAND.", ""},
	"threads":      {"JOB [loaded]", "List agent threads.", ""},
	"undo":         {"JOB --before TURN", "Revert an agent thread to before a turn.", ""},
	"usage":        {"JOB", "Read the engine account's usage.", ""},
	"version":      {"", "Print the cx version.", ""},
	"wait":         {"JOB [--timeout DURATION]\n  cx wait --any [options]", "Wait for a turn, job, admission block, or matching message.", "any timeout host project session cursor chat all"},
}

func cliFlags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}

func isHelp(arg string) bool { return arg == "-h" || arg == "--help" }

func ctlUsage(op string, err error) error {
	if errors.Is(err, flag.ErrHelp) {
		printCtlHelp(os.Stdout, op)
		return nil
	}
	if op == "" {
		printCtlHelp(os.Stderr, op)
		return err
	}
	// A short usage line plus the error: the full help would bury the actual mistake.
	cmd := ctlCommands[op]
	_, _ = fmt.Fprintf(os.Stderr, "usage: cx %s %s\nRun cx %s --help for options.\n", strings.ReplaceAll(op, "-", " "), cmd.usage, strings.ReplaceAll(op, "-", " "))
	return err
}

func printCtlHelp(w io.Writer, op string) {
	if op == "" {
		_, _ = fmt.Fprintln(w, "Usage: cx COMMAND [ARGS ...]\n\nCommands:")
		printCtlCommands(w, "")
		_, _ = fmt.Fprintln(w, "\nUse cx COMMAND --help for details.\nExit codes: 0 help, 2 invalid command or arguments.")
		return
	}

	cmd := ctlCommands[op]
	_, _ = fmt.Fprintf(w, "Usage: cx %s", strings.ReplaceAll(op, "-", " "))
	if cmd.usage != "" {
		_, _ = fmt.Fprint(w, " ", cmd.usage)
	}
	if cmd.flags != "" && !strings.Contains(cmd.usage, "[options]") {
		_, _ = fmt.Fprint(w, " [options]")
	}
	summary := cmd.summary
	if needs, ok := commandNeeds[op]; ok {
		summary += "\nEngines: " + strings.Join(enginesWith(needs), ", ")
	}
	_, _ = fmt.Fprintf(w, "\n\n%s\n\nOptions:\n  -h, --help\n    \tShow this help and exit.\n", summary)

	var o ctlOptions
	f := ctlFlags(op, &o)
	switch {
	case isThinControl(op):
		f = controlFlags(op)
	case op == "fork" || op == "resume":
		f = restartFlags(op, new(string), new(string), new(string))
	}
	f.SetOutput(w)
	f.PrintDefaults()

	switch op {
	case "chat", "project":
		_, _ = fmt.Fprintln(w, "\nActions:")
		printCtlCommands(w, op+"-")
	case "project-init":
		_, _ = fmt.Fprintln(w, "  --repo DIR [DIR ...]\n    \tGit checkouts in the project (required).\n  --default-host HOST\n    \tExecution host (CX_DEFAULT_HOST, otherwise local).")
	case "node":
		_, _ = fmt.Fprintln(w, "\nCOMMAND and ARGS use the cxrun execution-node interface and wire protocol.")
	case "test", "gate":
		_, _ = fmt.Fprintln(w, "\nArguments are forwarded to the executable. LAN_SOURCE_MODE defaults to snapshot.")
	}
	if strings.Contains(cmd.usage, "JOB") {
		_, _ = fmt.Fprintln(w, "\nJOB accepts [HOST:]PROJECT/TAG or TAG in the current project.")
	}
	if cmd.flags != "" || isThinControl(op) || op == "fork" {
		_, _ = fmt.Fprintln(w, "Unspecified flags are off or unset unless a default is given.")
	}
	_, _ = fmt.Fprintln(w, "\nExit codes:")
	_, _ = fmt.Fprintln(w, "  0 success or help")
	_, _ = fmt.Fprintln(w, "  2 invalid arguments or controller error")
	switch op {
	case "wait", "tail", "exec":
		for _, state := range []string{"failed", "stalled", "timeout", "orphaned"} {
			name := state
			if state == "timeout" {
				name = "job timeout or queue timeout"
			}
			_, _ = fmt.Fprintf(w, "  %d %s\n", terminalCodes[state], name)
		}
		_, _ = fmt.Fprintln(w, "  6 waiting for input\n  8 submission failed\n  9 transport failure\n  11 job registration not found")
		if op == "wait" {
			_, _ = fmt.Fprintf(w, "  %d admission blocked (JSON with status=queued and queue_reason)\n", exitQueued)
			_, _ = fmt.Fprintln(w, "  Check stdout JSON status=queued to identify an admission block. A command can also exit 13.")
			_, _ = fmt.Fprintln(w, "  wait --any delivers at least once. Reuse a cursor and deduplicate retries by event_id.")
		}
		_, _ = fmt.Fprintln(w, "  124 observation timeout\n  130 observation canceled")
		_, _ = fmt.Fprintln(w, "  Command jobs can return the command's exit code.")
	case "agent":
		_, _ = fmt.Fprintf(w, "  1 failed turn\n  6 waiting for input\n  7 duplicate job\n  9 transport failure\n  %d admission blocked (JSON with status=queued and queue_reason)\n  44 result unavailable\n  124 observation timeout\n", exitQueued)
		_, _ = fmt.Fprintln(w, "  Check stdout JSON status=queued to identify an admission block.")
	case "run":
		_, _ = fmt.Fprintln(w, "  7 duplicate job\n  9 transport failure")
	case "result", "patch", "apply", "rebase":
		failure := "failed turn or operation"
		if op == "apply" {
			failure = "apply failed or had conflicts (even with --allow-conflicts)"
		}
		_, _ = fmt.Fprintf(w, "  1 %s\n  9 transport failure\n  44 result unavailable\n", failure)
		if op == "result" {
			_, _ = fmt.Fprintln(w, "  12 background download in progress")
		}
		if op == "apply" {
			_, _ = fmt.Fprintln(w, "  10 changes applied but Git status failed")
		}
	case "chat-wait":
		_, _ = fmt.Fprintln(w, "  124 observation timeout")
	case "test", "gate":
		_, _ = fmt.Fprintln(w, "  The executable's exit code is propagated.")
	case "node":
		_, _ = fmt.Fprintln(w, "  Node commands return their own status codes, including 1 for node errors. Use cx node states for job terminal codes.")
	case "version", "project", "project-ls", "project-init", "project-show", "chat":
	default:
		_, _ = fmt.Fprintln(w, "  Execution-node exit codes are propagated when applicable.")
	}
}

var ctlFlagDescriptions = map[string]string{
	"since-turn":      "apply changes from turn `N` through the latest sealed turn",
	"incremental":     "show changes since the preceding sealed turn",
	"allow-conflicts": "write merge results with conflicts and exit nonzero",
	"all":             "include every session", "all-hosts": "include all configured hosts (default)", "any": "select multiple jobs",
	"batch": "read tagged prompt paths from `FILE`", "chat": "watch `CHANNEL` (repeatable)",
	"clean-base": "require a clean Git baseline", "cmd": "run command arguments after --",
	"continue": "continue an existing `JOB`", "cwd": "remote working `DIR` (node default when unset)",
	"dir": "source `DIR` (defaults to the current directory)", "effort": "reasoning `LEVEL` (engine default, or high with --hard)",
	"engine": "agent `ENGINE`: " + strings.Join(engineNames(), " or ") + " (default " + defaultEngine + ")", "env": "set `NAME=VALUE` (repeatable)",
	"messages": "follow complete agent messages from the event log",
	"latest":   "print only the newest message from each author, oldest first (a status board)",
	"force":    "bypass duplicate detection", "from-start": "follow from the beginning, resetting any saved cursor",
	"full": "show full output", "hard": "use high reasoning effort",
	"host":  "limit to one or more `HOST` values (repeatable, defaults to all configured hosts)",
	"image": "attach image `FILE` (repeatable; png, jpeg, gif or webp)", "inplace": "run in the original directory on local",
	"input-json": "use structured input from `JSON|@FILE`", "interactive": "recognize input questions",
	"interval": "seconds between watch polls (defaults to 1)",
	"json":     "emit JSON", "mention": "wait for a mention of `NAME`",
	"model": "agent `MODEL` (engine default)", "network": "enable sandbox network access (enabled by default with workspace-write)",
	"no-network": "disable sandbox network access", "no-tools": "omit the job-scoped messaging tools",
	"parent":  "parent `JOB` or session (defaults to CX_PARENT, then current session)",
	"project": "select `PROJECT`", "raw": "show raw job output", "refresh": "refresh the cached result",
	"repo": "select repository `PATH`", "reuse": "reuse matching successful command output",
	"profile": "execution host credential profile `NAME`", "role": "agent `ROLE` (defaults to worker)", "sandbox": "sandbox `MODE`: read-only (review, research), workspace-write (default; edits inside the checkout), danger-full-access (needs the execution account's full access; recorded as privileged)",
	"script": "upload and run `FILE`", "session": "select `SESSION` (defaults to the detected session where applicable)",
	"set": "update a node setting with `NAME=VALUE` (repeatable)", "snapshot": "reuse snapshot `ID`",
	"sync": "wait for job registration", "tag": "job `TAG` (generated when omitted)",
	"turn": "select sealed turn `N` (defaults to latest)", "type": "message `TYPE` (defaults to discussion)",
	"wait": "wait for completion", "watch": "refresh the listing continuously", "web-search": "enable live web search",
	"with-history": "include Git history as `HEAD|A..HEAD` (omitted by default, HEAD when given without a value)",
}

func ctlFlagHelp(op, key string) string {
	switch key {
	case "log":
		return "stream the complete command log or the selected agent turn's progress log"
	case "role":
		if op == "ls" {
			return "filter jobs by `ROLE`"
		}
	case "idle-exit":
		return fmt.Sprintf("idle lifetime `DURATION` (defaults to %s)", defaultIdle)
	case "turn-timeout":
		return fmt.Sprintf("turn deadline `DURATION` (defaults to %s)", defaultTurnLimit)
	case "approval-timeout":
		return fmt.Sprintf("approval deadline `DURATION` (defaults to %s)", defaultApproval)
	case "stall":
		return fmt.Sprintf("agent stall `DURATION` (defaults to %s)", defaultStall)
	case "queue-timeout":
		return "admission deadline `DURATION` (defaults to no deadline)"
	case "quiet-timeout":
		return fmt.Sprintf("command output inactivity `DURATION` (defaults to %s)", defaultQuiet)
	case "timeout":
		if op == "exec" || op == "run" {
			return "command runtime `DURATION` (defaults to no deadline)"
		}
		return fmt.Sprintf("observation `DURATION` (defaults to CXRUN_MONITOR_TIMEOUT or %s)", defaultMonitorTimeout)
	case "cursor":
		if op == "wait" || op == "chat-wait" {
			return "consumer cursor `NAME` (defaults to default)"
		}
		return "persist progress under cursor `NAME` (unset by default)"
	case "since":
		if op == "tail" {
			return "start after sequence `N` (otherwise resume the cursor or start at the beginning)"
		}
		return "exclusive sequence `N` (defaults to 0 when no cursor applies)"
	case "shell":
		return "script `SHELL` (defaults to powershell on Windows and sh elsewhere)"
	}
	return ctlFlagDescriptions[key]
}

func printCtlCommands(w io.Writer, prefix string) {
	var names []string
	for name := range ctlCommands {
		if prefix == "" && !strings.Contains(name, "-") || prefix != "" && strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		_, _ = fmt.Fprintf(w, "  %-12s %s\n", strings.TrimPrefix(name, prefix), ctlCommands[name].summary)
	}
}
