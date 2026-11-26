# Manage cx jobs

Start with the [installation guide](agent-install.md). Use `cx --help` and `cx COMMAND --help` for syntax, and the [reference](reference.md) for defaults, host policy, engine capabilities, and exit codes.

## Choose inputs and dispatch

Write a prompt with the goal, allowed changes, and validation. For work you plan to apply as a patch, ask the agent to leave changes uncommitted. Ask for a short report starting with `STATUS: DONE`, `STATUS: PARTIAL`, or `STATUS: BLOCKED`, with unresolved choices under `Pending decisions`.

Before dispatching, set `CX_SESSION` or use a supported session environment. cx recognizes Claude Code, Codex, and OpenCode from their session variables. You can override the detected value with `--session NAME` or explicitly create an ungrouped job with `--no-session`. Submissions exit 2 when no session is available and neither flag is set. Inside a job, cx sets `CX_SESSION` to that job's own `HOST:PROJECT/JOB` so its child jobs belong to its session.

- `cx run HOST DIR TAG PROMPTFILE` snapshots the source and prints `project/tag`. Add `--sync` to wait for registration. `cx agent --host HOST --dir DIR TEXT` submits, waits, and prints the report. It also takes `@FILE` or `-` for stdin and prints the job ID to stderr.
- Choose `--engine`, `--sandbox`, `--queue-timeout`, and `--turn-timeout` for the task. Use `--profile NAME` to select a [configured execution account](agent-install.md#configure-execution-credentials). Keep secrets in that account's login or environment file.
- Snapshots can include uncommitted work. Use `--clean-base` to require a clean Git source, `--with-history` to include commit history, or `--snapshot ID` to reuse the ID from `cx snapshot DIR`. Non-Git directories work too. For several repositories, register them with `cx project init NAME --repo DIR [DIR …]`.
- Include needed files in the snapshot, attach them, or inline their contents in the prompt. Review inputs before sending work to an execution host or model service. Write deliverable files to `artifacts/` in the job checkout. `--inplace` uses the original directory on `local`; use snapshots for the patch workflow.
- For another controller, or to disambiguate a job, use `HOST:project/tag`.

For commands, use `exec` for a private working directory or pass `--cwd` for an existing directory on the execution host. Use `run HOST DIR TAG --cmd -- ARGV…` to run against a source snapshot. With `exec --wait`, stdout carries merged command output, stderr carries the job ID, and normal process exit codes pass through.

```sh
cx exec local --wait --queue-timeout 10s --timeout 5s -- git --version
script=$(mktemp)
printf '%s\n' 'git --version' > "$script"
cx exec local --wait --queue-timeout 10s --timeout 5s --script "$script" --shell sh
```

Scripts default to `sh` on Unix and `powershell` on Windows. Choose an installed interpreter with `--shell`.

## Observe and follow up

Keep exactly one watcher per session. After dispatching, run this in the background (a background task, not a shell loop):

```sh
cx wait --any --timeout 55m
```

It watches every job this session dispatched, on every host, and returns one JSON line as soon as any job finishes a turn, asks a question, is rate-limited, fails, is blocked from admission, or sends a message. A turn event carries the completed `turn`, the `next` result command, and `rate_limited` to tell a quota stop from a normal finish. Then:

1. Handle the event: `cx show JOB` for the report, `cx result JOB` to collect it, `cx say JOB TEXT` to continue. A `say` starts a new turn, and its completion arrives as a new event.
2. Start the same command again. Its cursor remembers what was delivered, so nothing repeats or goes missing. On exit 124 nothing happened: start it again.

A new cursor starts from the current state, so jobs that finished before it existed are not reported: when you first start watching (or switch cursors), check `cx ls --fields brief` once, or add `--from-start` to replay retained events. Narrow the watch with `--project P` or `--host H` (both repeatable). A disconnected host is retried on its own while the others keep being watched. Do not poll `cx status` in a loop, wait on each job separately, or use `cx result --wait` to watch jobs.

**Sessions.** Jobs belong to the session that dispatched them, detected from `CX_SESSION`, then Claude Code, Codex and OpenCode session variables. Where none is set, dispatching and `wait --any` stop with exit 2: set `CX_SESSION`, pass `--session NAME`, or dispatch with `--no-session` on purpose. `cx adopt JOB [--session NAME]` moves an existing job into a session. Agents dispatching their own jobs get `CX_SESSION=HOST:PROJECT/JOB`.

Other controls: `cx interrupt JOB` stops an active turn; `cx resume JOB` restarts a recoverable agent job. For interactive turns, answer a question with `cx say JOB TEXT`; for engine requests, use `cx requests JOB`, then `say --request`, `reply`, `approve` or `deny`. Read a job's own messages with `cx outbox JOB`. Use `cx tail JOB --messages --cursor manager` for full agent messages.

## Share memory and progress

- **Memory.** The dispatching controller sends three context layers with every agent job as `$CX_CONTEXT`: shared `~/.config/cx/context/`, project `~/.config/cx/projects/PROJECT/context/` (wins on name clashes) and host `~/.config/cx/hosts/HOST/context/` (as `host/`). Keep each `MEMORY.md` a short index; agents read it first and open linked files only when relevant. Update them whenever a durable rule or host fact is learned. Only the manager edits them; agents and sub-managers send proposals to the parent, and the manager decides what to keep.
- **Sub-managers.** A manager on another host dispatches from its own `~/.config/cx/`. Mark that host `manager = true` in `hosts.toml`: each submission to it first refreshes its read-only copy of these layers when they changed (`cx context push HOST` forces it). Its own extra project layers are left untouched.
- **Opting out.** Use `--context project`, `--context shared,host` or `--context none` on `run` and `agent` to start a job without some or all layers.
- **Progress board.** Status, plan step and report outcome need no agent effort: `cx ls --fields brief`. For extra notes, ask agents to `send` one line to `#board` when they start, finish or block, and read `cx outbox --any --to '#board' --latest` (newest line per job).

## Work while the manager is away

Jobs run on their hosts without the controller and keep their state on the node. An agent that needs an answer ends its turn with `STATUS: QUESTION`. The job keeps its thread and continues when the manager replies with `cx say`. After reconnecting, reuse your `cx wait --any` cursor or run `cx ls` to see current job states.

## Collect, review, and clean up

1. Use `cx result JOB --wait` for the report, `--json` for metadata and artifact paths, or `--log` for the full log. With `--wait`, cx waits for the turn active when it first observes the job, or collects the latest sealed turn if the job is idle. Use `--turn N` to select a specific turn, including a future turn. The observation deadline is set by `--timeout`, `CXRUN_MONITOR_TIMEOUT`, or the 24-hour default. Exit 124 means the wait timed out. Exit 44 reports why the result is unavailable on stderr. Without `--wait`, exit 12 means collection is underway. For a full report, add `--full`.
2. Inspect `cx patch JOB` for statistics and `cx patch JOB --full --repo PATH` for the selected repository's patch. Use `cx apply JOB` after review, then commit through the project's Git workflow.
3. By default, patches are cumulative from the dispatch snapshot. If turn 1 is already applied and committed, inspect turn 2 with `cx patch JOB --turn 2 --incremental`. Use `cx apply JOB --since-turn 2` to apply turn 2 through the latest sealed turn, using turn 1 as the base. Run incremental operations from the submitting controller.
4. On conflicts, `apply` reports the files and leaves the selected repositories untouched. Choose `--allow-conflicts` to write the merge with conflict markers and unmerged index entries, or `cx rebase JOB TEXT` to dispatch a follow-up against a fresh snapshot.
5. Use `cx close JOB` to end an agent session and release its private checkout once unused. Collect what you need before using `cx clean JOB` to remove terminal job state.

Judge task completion using the job state, exit code, and report together.
