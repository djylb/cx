# Manage cx jobs

Use cx as the execution layer. You choose what to send, where it should run, which engine and permissions it needs, and when to accept the result. Keep host aliases, credentials, and user-specific paths in local configuration. See the [installation guide](agent-install.md) for setup.

Run `cx --help` to list commands and `cx COMMAND --help` for usage, options, defaults, and exit codes. The short form is `-h`. Help works without loading host configuration or contacting an execution host.

## Dispatch, observe, steer, collect

1. Write a focused prompt with the goal, allowed changes, validation, and expected report. Ask for a brief summary beginning with `STATUS: DONE`, `STATUS: PARTIAL`, or `STATUS: BLOCKED`, and a `## Pending decisions` section if choices need review.
2. Use `cx run HOST DIR TAG PROMPTFILE` for background dispatch. It freezes the source, prints `project/tag`, and submits in the background. Add `--sync` to wait for registration. Use `cx agent --host HOST --dir DIR TEXT` to submit, wait, and read the report in one call. `agent` also accepts `@FILE` or `-` for stdin. Put the managing session's shell call in the background if it supports completion notifications.
3. Keep the job ID. Run `cx wait JOB --timeout 1m`, then use `cx show JOB` for a turn summary or `cx tail JOB --timeout 30s --cursor manager` for live progress. Use `status` for current state. If a background submission fails, `show` includes submission diagnostics. A wait timeout only ends the current observation.
4. Send a correction with `cx say JOB 'TEXT'` or `cx say JOB @FILE`. A running Codex turn can receive steering; an idle agent starts another turn. A running Claude Code turn receives the message on a subsequent turn. `cx agent --continue JOB TEXT` sends a follow-up and waits for the report.
5. Collect with `cx result JOB --wait`. Without `--wait`, cx can start or rejoin a background download and return 12 while collection proceeds. Use `--turn N` to select a sealed turn, or `--refresh` after activity from another controller. If the job makes deliverable files, write them into `artifacts/` in the job's working directory.
6. Inspect `cx patch JOB` for change statistics and `cx patch JOB --full --repo PATH` for one repository's patch. Run `cx apply JOB` only after review. It checks the selected patches before writing them to the original working trees. Use `--repo PATH` to select one repository. If a patch conflicts, `cx rebase JOB 'Explain the conflict here'` creates a related job on a fresh snapshot. Accept and commit through your normal Git workflow.
7. `cx interrupt JOB` stops the active turn. `cx close JOB` ends the agent session and releases its private workspace when the last user of that checkout closes. Collect the desired results, then `cx clean JOB` when the terminal job's private state can be removed. Use `cx resume JOB` to reconnect a recoverable Codex thread whose runner died and whose checkout still exists.

Separate the command's exit status, the job state, and the agent's report status when deciding whether a task is complete. The [README](../README.md#exit-codes) lists observation and collection codes.

### Choose source and target explicitly

`run` and `agent` default to an isolated snapshot. A Git snapshot can include current uncommitted changes. Use `--clean-base` to reject a dirty Git baseline. History is omitted by default. Use `--with-history` or `--with-history=A..HEAD` when the task needs history. `cx snapshot DIR` prints an ID you can pass with `--snapshot ID` to reuse frozen inputs. Non-Git directories also work. `--inplace` uses the original directory, requires `local`, and does not produce a snapshot patch.

Keep project context under `~/.config/cx/projects/PROJECT/context/` or set `CX_CONTEXT_DIR`. Context is copied into the job and made available as `$CX_CONTEXT`. Set `CX_POLICY_DIR` to use your own `preamble.md`, `interactive.md`, or `stall-nudge.md` templates. Check what you send to the execution account, configured relay, and model service, including prompts, attachments and uncommitted files.

Use `HOST:project/tag` for a known job when the controller has no local route, or when multiple hosts use the same job ID. `cx hosts [HOST]` gives current host facts. Use `cx project init NAME --repo DIR [DIR …]` to group multiple repositories. The project name is resolved from `CX_PROJECT`, configured Git membership, or the source directory.

## Watch a group with one wait

```sh
cx ls --project PROJECT
cx overview --project PROJECT
cx wait --any --project PROJECT --host worker --cursor manager --timeout 1m
```

`wait --any` polls the selected hosts and returns one JSON event and its code. It can report a completed turn, a terminal job, a request for input, or new outbox activity. Use another wait after you handle that event. Use distinct cursor names for independent consumers or independently filtered streams. `--host` is repeatable. When a channel center is configured, `--chat CHANNEL` can add channel messages to the same wait.

cx labels submissions with the first available value from `CX_SESSION`, `CLAUDE_CODE_HOST_SESSION_ID`, `CLAUDE_CODE_SESSION_ID`, and `CODEX_THREAD_ID`. `ls`, `overview`, and `wait --any` filter to that session by default. Use `--session NAME` to select one or `--all` to see every session. If no session is detected, the CLI reports that it is looking across sessions.

## Run commands and scripts

Use `exec` for a command that needs no source snapshot. It returns a job ID by default. `--wait` streams merged command output to stdout, prints the job ID to stderr, and returns the process exit code on normal exit. Use `--cwd` for an existing directory on the execution host. By default each job gets a private empty working directory. Use `--env KEY=VALUE` to supply explicit variables. Use `run HOST DIR TAG --cmd -- ARGV…` for a command that needs a source snapshot.

```sh
cx exec worker --wait --timeout 20s -- git --version
cx exec worker --wait --timeout 20s --script ./check.sh --shell sh
```

Arguments after `--` keep their boundaries. Use an explicit shell for pipes or redirection, or upload a local script with `--script FILE`. The script is streamed to a private job directory on the host. Shell choices are `sh`, `powershell`, `pwsh`, and `cmd`; the default is `sh` on Unix and `powershell` on Windows. The selected interpreter must be installed on the execution host. Use `--queue-timeout` for admission, `--timeout` for runtime, `--quiet-timeout` for command silence, and `wait --timeout` for the observation deadline.

Command jobs run as the execution account. Verify the target and working directory, especially when using a shared or explicit `--cwd`.

## Interactive and Codex controls

Use `run` or `agent` with `--interactive` when you can answer questions. A prompt can instruct the agent to use `STATUS: QUESTION` for a decision it needs. A waiting job can return code 6. Read the report or use `cx requests JOB` to inspect pending Codex requests. For an idle turn asking in its report, reply with `cx say JOB TEXT`. For a pending Codex request, use its ID with `cx say JOB --request ID TEXT`, `cx say JOB --request ID --answers @FILE`, or `cx reply JOB --request ID --result @FILE` according to the request type. `approve` and `deny` handle approval requests.

Codex job-scoped messaging can deliver questions and messages through its `ask`, `send`, and `inbox` tools. Use `cx outbox JOB --since N` to read records after a known sequence. The managing session decides what to answer or forward. Use `--no-tools` at submission to omit these tools.

The Codex controls expose engine operations directly:

- `cx fork JOB --job CHILD [--through TURN]` forks the thread on the same host and shares its checkout. Use a separate snapshot job when two agents need independent writable directories.
- `cx review JOB --target '{"type":"uncommittedChanges"}'` starts a Codex review. Use `cx plan JOB`, `cx diff JOB`, `cx history JOB`, or `cx usage JOB` to inspect available thread data.
- `events`, `limits`, `threads`, `models`, `undo`, `compact`, `settings`, `mcp`, and `rpc` expose node observations or Codex operations. Use raw RPC only when you know the engine's method and parameter schema. Claude Code uses a separate engine and supports text turns and session continuation.

## Keep context small

- Read `show` or the summary from `result` first. Use `--full` for the full report or detailed view when needed. Use `patch` for stats and `--full --repo PATH` for the exact patch.
- Follow `tail` with a named cursor. Request `--raw` only for diagnostics. With direct commands, `tail --raw --since N` uses an output byte offset.
- Continue a thread with `say` when the same context is useful. Give reviewers the patch and the relevant files. Reuse a snapshot for comparable jobs. Command reuse via `--reuse` is opt-in and should be used only when the recorded inputs determine the outcome. cx does not freeze an explicit `exec --cwd` directory.

## Sandbox and execution environment

- **Nested sandboxes.** A managing AI's sandbox can block SSH, runtime state, or launching a second sandbox locally. Test from the actual managing session. Check which layer denied access before choosing another execution host or requesting the necessary permissions. `danger-full-access` gives the agent the execution account's access and is recorded as privileged.
- **Linux root and bubblewrap.** When provisioning a root execution account from a tar archive, avoid preserving a foreign owner (for GNU tar, extract with `--no-same-owner`). Check that state, checkout and required mount points are owned and accessible by the execution account. Codex's Linux sandbox can depend on bubblewrap (`bwrap`) and usable namespaces. Test a tiny sandboxed turn as that account. Confine any ownership repair to the private deployment directory.
- **Windows.** Run the agent in psmux's `cx` namespace by default inside a logged-in Windows session. Keep the account logged in and validate the agent under that session's PATH and Windows sandbox configuration.
- **Claude sessions.** The restricted Claude modes require a CLI supporting `--restricted`. The agent must be able to write its own session files to resume. A surrounding sandbox can block those writes even when workspace edits are allowed. Verify the session directory under the actual execution account before attempting continuation. Restricted mode limits available tools, so run build or test commands as separate command jobs when needed.
- **Writable caches and loopback.** Give the toolchain writable caches and ensure the sandbox policy permits the writes as well as filesystem permissions. Check Go's `GOCACHE` and `GOMODCACHE` if Go tests fail. For Codex submissions, `workspace-write` enables sandbox network access by default, including loopback, and `--no-network` disables it. Read-only jobs can opt in with `--network`. Check loopback explicitly when tests bind to localhost. `--web-search` enables the engine's web tools independently of command networking.
- **Non-interactive PATH.** SSH commands and script modes can run without the user's interactive profile. Check tool versions through `cx exec`. Put version manager shims or real tool directories on the execution PATH and install the pinned toolchains before dispatch. Even when a shim resolves, check that the selected toolchain and caches are usable from that environment.
- **Shared build output and disk.** cx shares a per-project Cargo target cache on each execution node. Admission can wait when the state volume falls below its free-space threshold. Check `cx host HOST`, `cx doctor HOST --json`, and the job's queue reason when builds accumulate. Coordinate shared caches across jobs that use different toolchains. Put files intended for delivery in `artifacts/` and avoid packaging disposable build and dependency trees.
