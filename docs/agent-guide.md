# Manage cx jobs

Start with the [installation guide](agent-install.md). Use `cx --help` and `cx COMMAND --help` for syntax, and the [reference](reference.md) for defaults, host policy, engine capabilities, and exit codes.

## Choose inputs and dispatch

Write a prompt with the goal, allowed changes, and validation. For work you plan to apply as a patch, ask the agent to leave changes uncommitted. Ask for a short report starting with `STATUS: DONE`, `STATUS: PARTIAL`, or `STATUS: BLOCKED`, with unresolved choices under `Pending decisions`.

Before dispatching, set `CX_SESSION` or use a supported session environment. cx recognizes Claude Code, Codex, and OpenCode from their session variables. You can override the detected value with `--session NAME` or explicitly create an ungrouped job with `--no-session`. Submissions exit 2 when no session is available and neither flag is set. Inside a job, cx sets `CX_SESSION` to that job's own `HOST:PROJECT/JOB` so its child jobs belong to its session.

- `cx run HOST DIR TAG PROMPTFILE` snapshots the source and prints `project/tag`. Add `--sync` to wait for registration. `cx agent --host HOST --dir DIR TEXT` submits, waits, and prints the report. It also takes `@FILE` or `-` for stdin and prints the job ID to stderr.
- Choose `--engine`, `--sandbox`, `--queue-timeout`, and `--turn-timeout` for the task. Work that needs host privileges, such as network namespaces, TUN devices, the host process table or local sockets, needs `--sandbox danger-full-access`; command jobs (`cx exec`, `run --cmd`) run without the agent sandbox. Use `--profile NAME` to select a [configured execution account](agent-install.md#configure-execution-credentials). Keep secrets in that account's login or environment file.
- Snapshots can include uncommitted work. Use `--clean-base` to require a clean Git source, `--with-history` to include commit history, or `--snapshot ID` to reuse the ID from `cx snapshot DIR`. Non-Git directories work too.
- For a new snapshot-backed agent, repeat `--own PATH` with `cx run` or `cx agent` to limit workspace writes to existing files or directories. Paths are relative to the main repository root. Use `../MEMBER/PATH` for a sibling snapshot repository. Managed caches and job temporary space remain writable.
- Include needed files in the snapshot, attach them, or inline their contents in the prompt. Review inputs before sending work to an execution host or model service. Write deliverable files to `artifacts/` in the job checkout. `--inplace` uses the original directory on `local`; use snapshots for the patch workflow.
- For another controller, or to disambiguate a job, use `HOST:project/tag`. A job ID without a host resolves through this controller's routes, then through cached listings and a short query of every configured host. One match is used and remembered. Several matches are an error that lists the qualified IDs.
- A snapshot of a repository with local changes uses a frozen worktree: committed content, uncommitted changes to tracked files, and untracked files that `.gitignore` does not exclude. Submission names the untracked files it includes. Ignore or move drafts the job should not see. Ignored files enter only through `--include` or the project's `include`.
- Jobs cannot dispatch to other hosts: execution hosts have no controller host configuration, so `cx exec OTHER` inside a job reports an unknown host. The manager dispatches cross-host steps. Do not ask a job to run something on another host.

For commands, use `exec` for a private working directory or pass `--cwd` for an existing directory on the execution host. Use `run HOST DIR TAG --cmd -- ARGV…` to run against a source snapshot. With `exec --wait`, stdout carries merged command output, stderr carries the job ID, and normal process exit codes pass through.

```sh
cx exec local --wait --queue-timeout 10s --timeout 5s -- git --version
script=$(mktemp)
printf '%s\n' 'git --version' > "$script"
cx exec local --wait --queue-timeout 10s --timeout 5s --script "$script" --shell sh
```

Scripts default to `sh` on Unix and `powershell` on Windows. Choose an installed interpreter with `--shell`.

### Mount several repositories

Put every repository the job needs in the same project:

```sh
cx project init demo --repo ./app ./library --include 'app:.cargo/config.toml'
cx project show demo
cx run local ./app check prompt.txt --project demo
```

With no explicit selection, cx can infer the project from the source repository. The job starts in the selected member's repository root with the project layout preserved, so it can access `../library`. With `workspace-write`, one job can update the library and its consumers together. Go workspace discovery adds other referenced repositories to the snapshot.

Submission warns when the job references a repository the project does not mount: a local `replace` target in `go.mod`, a `use` entry in `go.work`, or an absolute path in the prompt that lies in another repository. The job is still submitted; add the repository to the project if the job must change it.

Repeat `--include REPO:PATH` for each input file or quoted glob. These entries are stored as the repository's `include` list in the project definition. Use them for local configuration needed by the job. Each entry must match a file, and job edits to the matched files are excluded from collected patches. Keep credentials out of these files.

Go builds and checks share writable caches across jobs on the same node. Use `--env KEY=PATH` to override `GOCACHE`, `GOMODCACHE`, `STATICCHECK_CACHE`, or `GOLANGCI_LINT_CACHE` for one job. Use `cx hosts` to check whether caches in use exceed the node's configured budget.

## Work by module (按模块工作)

Configure the project's `modules_file` to supply module names, path scopes, optional context and acceptance commands, and direct dependencies. See the [module configuration reference](reference.md#module-configuration). With no module table, use repository-relative path prefixes such as `--module internal/api`.

```sh
cx run local ./app update-api prompt.txt --module api --changeset release-7
cx ls --active --module api --brief
cx result app/update-api
cx result app/update-api --reads
cx result app/update-api --reads --all
cx apply app/update-api
cx ls --all --changeset release-7 --json
```

Repeat `--module` to claim several modules. An active writer with the same module or an overlapping path scope can block submission. Coordinate with the listed job or pass `--module-override "reason"` to continue and record the decision. Active writers for direct dependencies also produce a warning. Overlapping scopes still conflict. Claims are checked against reachable nodes at dispatch. Concurrent submissions can both pass, and pending asynchronous submissions become visible when registered. Check `claims_unchecked` in `cx run --json`. Fork and rebase report an active source so you can close it before starting a second writer. Refresh checks the new scope. For a job that only inspects the checkout, use `--sandbox read-only`.

The first prompt lists the claims, direct dependencies and generated context at `$CX_CONTEXT/modules/NAME.md`. A refreshed prompt gives the path for its new baseline. If a context command fails, the prompt points to the progress log and the job continues. Include the supplied `Changeset: ID` trailer when committing. After applying, use `ls --changeset ID --json` on the applying controller to look up applied turns, target HEADs at apply time, and result trees. Find final commits by their changeset trailers.

Acceptance runs after the patch seals, in a disposable checkout. Continue the job while checks are pending. Inspect `acceptance` in `cx result JOB --json` or read the acceptance event in `cx outbox JOB`.

The runner freezes the delivery and runs acceptance in a private copy. The result records the acceptance phase and any checks completed. Add `--accept 'COMMAND'` to supply a check for this job, `--accept-timeout 10m` to change its per-command deadline, or `--no-accept` to skip checks. Review failures in `cx result JOB --json`. Use `--reads` to view best-effort paths from the selected turn.

## Observe and follow up

Keep exactly one watcher per session. After dispatching, run this in the background (a background task, not a shell loop):

```sh
cx wait --any --timeout 55m
```

To wait for particular jobs, name each one: `cx wait --any --job linux:p/a1 --job linux:p/a2 --timeout 55m`. Run it in the background without a shell loop or output filter. It exits after the first event among them.

It watches every job this session dispatched, on every host, and returns one JSON line as soon as any job finishes a turn, asks a question, is rate-limited, fails, is blocked from admission, or sends a message. A turn event carries the completed `turn`, the `next` result command, and `rate_limited` to tell a quota stop from a normal finish. For a quota stop, use `reset_at` (the blocking quota reset recorded at turn completion, in Unix seconds, or `null` if unknown) to decide when to continue. Then:

1. Handle the event: `cx show JOB` for the report, `cx result JOB` to collect it, `cx say JOB TEXT` to continue. A `say` starts a new turn when the agent is idle and resumes an ended job with a saved thread and an available workspace before delivering the message. The brief phase `completed` identifies a `done` job that `say` cannot resume. Its completion arrives as a new event. Use `cx say JOB @FILE` for long messages that exceed the local command-line limit.
2. Start the same command again. It resumes from its saved cursor. On exit 124, start it again.

A new cursor starts from the current state, so jobs that finished before it existed are not reported: when you first start watching (or switch cursors), check `cx ls --brief` once, or add `--from-start` to replay retained events. Narrow the watch with `--project P`, `--host H`, or `--job HOST:PROJECT/JOB`. Each flag is repeatable. A job must match every filter supplied. Changing `--job` values keeps the same session cursor. Cursors are kept per session, so watchers of different sessions never share events. A disconnected host is retried on its own while the others keep being watched. Do not poll `cx status` in a loop, wait on each job separately, or use `cx result --wait` to watch jobs.

**Sessions.** Jobs belong to the session that dispatched them, detected from `CX_SESSION`, then Claude Code, Codex and OpenCode session variables. Where none is set, dispatching and `wait --any` stop with exit 2: set `CX_SESSION`, pass `--session NAME`, or dispatch with `--no-session` on purpose. `cx adopt JOB [--session NAME]` moves an existing job into a session. Agents dispatching their own jobs get `CX_SESSION=HOST:PROJECT/JOB`.

Other controls: `cx interrupt JOB` stops an active turn; `cx resume JOB` restarts a recoverable agent job. For interactive turns, answer a question with `cx say JOB TEXT`; for engine requests, use `cx requests JOB`, then `say --request`, `reply`, `approve` or `deny`. Read a job's own messages with `cx outbox JOB`. Use `cx tail JOB --messages --cursor manager` for full agent messages.

## Share memory and progress

- **Memory.** The dispatching controller sends three context layers with every agent job as `$CX_CONTEXT`: shared `~/.config/cx/context/`, project `~/.config/cx/projects/PROJECT/context/` (wins on name clashes) and host `~/.config/cx/hosts/HOST/context/` (as `host/`). Keep each `MEMORY.md` a short index; agents read it first and open linked files only when relevant. Update them whenever a durable rule or host fact is learned. Only the manager edits them; agents and sub-managers send proposals to the parent, and the manager decides what to keep.
- **Sub-managers.** A manager on another host dispatches from its own `~/.config/cx/`. Mark that host `manager = true` in `hosts.toml`: each submission to it first refreshes its read-only copy of these layers when they changed (`cx context push HOST` forces it). Its own extra project layers are left untouched.
- **Opting out.** Use `--context project`, `--context shared,host` or `--context none` on `run` and `agent` to start a job without some or all layers.
- **Progress board.** Use `cx ls --brief` for the current phase, turn counts, applied turns by repository, and plan step. For a rate-limited job, use `cx ls JOB --brief --json` to read its known `rate_limit.reset_at`. For extra notes, ask agents to `send` one line to `#board` when they start, finish or block, and read `cx outbox --any --to '#board' --latest` (newest line per job).

## Watch context and usage

Use `cx ls --usage` to see token use by project, job and role. The view flags jobs with several turns and high context use. Set `--usage-top`, `--usage-turns` and `--usage-context` to tune it. The listing and sealed results show cached input, fresh input and output separately.

When a turn reaches the context advisory threshold, `cx outbox JOB` and `cx result JOB` offer three choices. Continue, compact in place with `cx compact JOB` and continue the same job (recommended), or ask for a handoff summary and start a new job only when the task changes. Set `--context-warn N` on a new job to change the threshold. Use 0 to disable the advisory. See the [usage reference](reference.md#context-and-token-usage) for controller defaults.

Use `cx doctor HOST --json` to see the controller’s recent disconnects and reconnects alongside the current node status.

## Run many jobs at once

- Split work along file boundaries: give each set of files to one job, and keep each job to one change that can be collected on its own. Start a separate job for new findings instead of queueing them into a long-running one.
- Name the paths each job owns and the paths it must not touch in its own prompt, or restrict its writable paths with `--own PATH`.
- Keep one background `cx wait --any` per session and handle every event line. Restart it when your job list changes; narrow it with `--job` rather than filtering its output.
- For multi-turn jobs, plain `cx apply JOB` applies only the turns after the last one you applied. When jobs conflict on the same files, redo one on the new base with `cx rebase` instead of merging by hand.
- Bring inputs that Git ignores into snapshots with the project `--include` list.
- When a job stops on a usage limit, check `rate_limited` and `reset_at` in its event; `cx rebase JOB TEXT --host H` continues its work on another host or account.
- Collect a batch of similar jobs together once all of them have finished.
- A job waits for an admission slot, earlier queued jobs, and the host load check. Submit with `--now` to start at once, or run `cx admit JOB` for a job that is already queued. Both ignore the slot limit, the queue order and the load check; the disk minimum still applies. Use them for urgent or short work, since they add load beyond the configured slots.

## Work while the manager is away

Jobs run on their hosts without the controller and keep their state on the node. An agent that needs an answer ends its turn with `STATUS: QUESTION`. The job keeps its thread and continues when the manager replies with `cx say`. After reconnecting, reuse your `cx wait --any` cursor or run `cx ls --active --brief` to see running, queued, submitting, and waiting jobs across all hosts and sessions. Add `--json` for structured output. Changing host, project, or job filters keeps the same session cursor. A second wait on that cursor exits with code 2 and reports the active observer’s PID and start time. Stop it by PID or use `--cursor NAME` for an independent cursor.

When a host is unreachable, `cx ls` shows its cached jobs with the time of the last successful observation. In `cx ls --json`, check `stale` and `as_of` before acting on a cached job status.

## Collect, review, and clean up

1. Use `cx result JOB` to collect and preview a sealed turn. Add `--full` for the full report, `--path` for its absolute local path on one line, `--json` for metadata and artifact paths, or `--log` for the full log. Use `--turn N` to select a turn. By default, cx selects the active turn or the latest sealed turn if the job is idle. Add `--wait` to wait for the selected turn to seal. The overall deadline covers status lookup, waiting, and collection. It defaults to `CXRUN_MONITOR_TIMEOUT` or 24 hours. Collection also has a per-turn limit of `CX_COLLECT_LIMIT` seconds (30 minutes by default). An explicit `--timeout` sets the overall deadline and overrides the collection limit. Result node calls time out after 120 seconds without progress by default. Set `CX_TRANSFER_IDLE` in seconds to change this window. Exit 124 indicates an observation deadline, including during status lookup without `--wait`. Exit 44 reports an unavailable result or a collection failure, including a download deadline, on stderr. `--path` can reuse a cached report and downloads the standard archive on a cache miss.
2. Inspect all changed repositories with `cx patch JOB`, or use `cx patch JOB --full --repo PATH` for a single repository. Use `cx apply JOB` to apply changes across all repositories or add `--repo PATH` to select one. Commit through the project's Git workflow.
3. By default, `cx patch` and `cx apply` start after the last applied turn for each repository in the job run, or at the selected result’s baseline if there is no apply record for that repository on that baseline. Patch headers show each starting turn. Use `--since-turn 1` for the cumulative patch or `--since-turn N` to start at a specific turn. Run incremental operations from the submitting controller.

   Use `cx refresh JOB [DIR]` to update a running snapshot job when the source advances. The default is the job's original source directory. Check the snapshot ID and activation time in `cx ls`. If the next result says `STATUS: QUESTION`, review the listed conflicts and reply with `cx say JOB TEXT`. The checkout keeps the conflict markers. After refresh, new patches are cumulative from the new snapshot until you apply a turn on that baseline. Earlier sealed results keep their original baselines and turn numbers.

4. If any selected repository has a conflict, `cx apply` reports the repository and files and leaves all selected repositories untouched. To resolve a single repository locally, select it with `--repo PATH --allow-conflicts`. Or run `cx rebase JOB TEXT` from the submitting controller to dispatch a follow-up against a fresh snapshot. If the current turn is active, cx queues the rebase until the turn is sealed and prints the controller log path. Repeat the command to replace pending text before the child is claimed. After a claim, use `--turn N` to retrieve the same child ID or retry a failed submission. The previous result is available to the child at `$CX_CONTEXT/rebase/result.json`.

   To continue after a rate limit or host outage, run `cx rebase JOB TEXT --host H [--profile P]`. The child starts from the current source plus the sealed turn’s cumulative changes. Merge conflicts stop dispatch without editing the source checkout. If the old host is unreachable, use a result already collected by the controller, optionally selecting it with `--turn N`. Omit `--profile` to use the destination’s default.
5. Run `cx ls` on the controller that ran `apply` to see the highest applied turn for each repository on the current baseline. The table shows it in the APPLIED TURN column, and JSON shows it in `applied_turn`.
6. Use `cx close JOB` to end an agent session and release its private checkout once unused. Closing or cleaning a job also removes its private temporary directory. Collect what you need before using `cx clean JOB` to remove terminal job state.

Judge task completion using the job state, exit code, and report together.
