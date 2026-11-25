# Manage cx jobs

Start with the [installation guide](agent-install.md). Use `cx --help` and `cx COMMAND --help` for syntax, and the [reference](reference.md) for defaults, host policy, engine capabilities, and exit codes.

## Choose inputs and dispatch

Write a prompt with the goal, allowed changes, and validation. For work you plan to apply as a patch, ask the agent to leave changes uncommitted. Ask for a short report starting with `STATUS: DONE`, `STATUS: PARTIAL`, or `STATUS: BLOCKED`, with unresolved choices under `Pending decisions`.

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

1. Run `cx wait JOB --timeout 20s` in the background if your session supports it. When it returns, use `cx show JOB` for the turn report, `cx status JOB` for current state, or `cx tail JOB --cursor manager --timeout 20s` to follow progress. For submission failures, start with `show`.
2. On exit 124, wait again for the same job. On exit 13 with stdout JSON containing `status: "queued"`, read `queue_reason` and continue observing. On exit 6, check the report or the engine's pending requests.
3. Use `cx say JOB TEXT` or `cx say JOB @FILE` to follow up. Input starts a new turn when the job is idle. It can steer an active Codex turn and queues for Claude Code's next turn. `cx agent --continue JOB TEXT` sends the input and waits for a report. Use `cx interrupt JOB` to stop an active turn, or `cx resume JOB` to restart a recoverable agent job.
4. For interactive turns, submit with `--interactive`. Reply to a question in the report with `cx say JOB TEXT`. For pending engine requests, use `cx requests JOB`, then `say --request`, `reply`, `approve`, or `deny` as appropriate. For the job's own messages, read `cx outbox JOB --since N` and answer or forward them.

Use `cx tail JOB --messages --cursor manager` for complete agent messages or `--raw` for diagnostic output. Cursor names persist progress within the selected stream. Use `--from-start` to replay.

### Watch several jobs

```sh
cx ls --project demo
cx overview --project demo
cx wait --any --project demo --host local --cursor manager --timeout 20s
```

The wait returns one JSON event. Reuse the cursor for the same stream and deduplicate retries by `event_id`. Use separate cursors for independently filtered streams. With a configured channel center, add `--chat CHANNEL` to watch messages too.

`ls`, `overview`, and `wait --any` use the current [session](reference.md#environment-variables) when detected. Use `--session NAME` or `--all` to change the selection.

## Share memory and progress

- **Memory.** The dispatching controller sends three context layers with every agent job as `$CX_CONTEXT`: shared `~/.config/cx/context/`, project `~/.config/cx/projects/PROJECT/context/` (wins on name clashes) and host `~/.config/cx/hosts/HOST/context/` (as `host/`). Keep each `MEMORY.md` a short index; agents read it first and open linked files only when relevant. Only the manager edits these files; agents propose changes by sending them to the parent.
- **Sub-managers.** A manager running on another host dispatches from its own `~/.config/cx/`. Before handing work to it, run `cx context push HOST` to copy this controller's layers there; the copy is read-only (marked `.mirror.json`), so changes go back through the parent and are pushed again. Its own extra project layers are left untouched.
- **Opting out.** Use `--context project`, `--context shared,host` or `--context none` on `run` and `agent` to start a job without some or all layers.
- **Progress board.** Status, plan step and report outcome need no agent effort: `cx ls --fields brief`. For extra notes, ask agents to `send` one line to `#board` when they start, finish or block, and read `cx outbox --any --to '#board' --latest` (newest line per job).

## Work while the manager is away

Jobs run on their hosts without the controller: a dispatched job needs no connection back to finish, and its state stays on the node. An agent that needs an answer ends its turn with `STATUS: QUESTION`; the job keeps its thread and continues when the manager replies with `cx say`. After reconnecting, `cx wait --any` (or `cx ls`) reports what finished or is waiting.

## Collect, review, and clean up

1. Use `cx result JOB --wait` for the report, `--json` for metadata and artifact paths, or `--log` for the full log. Choose a historical sealed turn with `--turn N`. Without `--wait`, exit 12 means collection is underway. For a full report, add `--full`.
2. Inspect `cx patch JOB` for statistics and `cx patch JOB --full --repo PATH` for the selected repository's patch. Use `cx apply JOB` after review, then commit through the project's Git workflow.
3. By default, patches are cumulative from the dispatch snapshot. If turn 1 is already applied and committed, inspect turn 2 with `cx patch JOB --turn 2 --incremental`. Use `cx apply JOB --since-turn 2` to apply turn 2 through the latest sealed turn, using turn 1 as the base. Run incremental operations from the submitting controller.
4. On conflicts, `apply` reports the files and leaves the selected repositories untouched. Choose `--allow-conflicts` to write the merge with conflict markers and unmerged index entries, or `cx rebase JOB TEXT` to dispatch a follow-up against a fresh snapshot.
5. Use `cx close JOB` to end an agent session and release its private checkout once unused. Collect what you need before using `cx clean JOB` to remove terminal job state.

Judge task completion using the job state, exit code, and report together.
