# cx reference

Use `cx --help`, `cx COMMAND --help`, and `cx node --help` for command discovery. See the [manager guide](agent-guide.md) for job workflows and the [installation guide](agent-install.md) for setup.

## Host configuration

The controller reads `~/.config/cx/hosts.toml`. The built-in target `local` runs under the controller account.

| Setting | Meaning |
| --- | --- |
| `hosts.<target>.ssh` | OpenSSH alias for a remote target. The user's SSH configuration supplies authentication and host-key policy. |
| `hosts.<target>.cx` | Node binary path. Remote default is `.local/bin/cx` under the execution account's home, with `.exe` on Windows. |
| `hosts.<target>.state` | Optional node state root. Use a private local volume. |
| `hosts.<target>.via` | Configured relay target. Its SSH configuration resolves the next hop. |
| `hosts.<target>.platform` | Target OS and architecture as `GOOS/GOARCH`, detected and recorded by setup. |

`cx setup HOST` installs or reuses the node binary and initializes the state root, preserving an existing `host.json`. Local setup uses the same default install path under the controller account's home. Keep the checkout and Go available for a cross-build, or provide a matching binary with `CX_SETUP_BINARY`. A Linux target or relay with Go can compile the node. Linux upgrade paths can use rsync for binary deltas.

Review the execution host's `host.json` with `cx host HOST`. Update an individual setting with `--set`. For example, `cx host local --set slots.agent=2` changes agent concurrency. Edit named profiles in the JSON file on the execution host.

| `host.json` key | Meaning and default |
| --- | --- |
| `slots.agent`, `slots.test` | Positive limits for concurrent agent turns and command jobs, respectively. Each defaults to 10 on Linux, 6 on Windows, and 3 elsewhere. Direct node starts can select a category with `--slot-class`. |
| `slots.system` | System category limit, default 1. Unspecified custom categories use the platform default. |
| `load_cmd` | Optional admission check for all categories. It runs under `sh` on Unix or `cmd.exe` on Windows. Exit 0 allows admission. |
| `min_free_bytes` | Free space required on the state volume. Default is the larger of 20 GB and 5% of the volume. Set 0 to disable the free-space threshold. |
| `cache_max_bytes` | Budget for cx-managed build and package caches. Defaults to the smaller of 50 GB and 10% of the state volume. Set an explicit byte count to override it. |
| `env_file` | Private UTF-8 environment file for jobs. Use `KEY=VALUE`; blank lines and full-line comments are allowed. `${NAME}` expands to an earlier value in the file or the runner's environment (for example `PATH=C:\tools\bin;${PATH}`); everything else is literal. The last assignment for a key wins, and a job's own `--env` overrides it. |
| `default_profile`, `profiles` | Default account name for new jobs and the map of named accounts. `run`, `agent`, and `exec` accept `--profile NAME`; continuation and resume keep the recorded profile. Fork inherits it by default. |
| `profiles.<name>.engine_homes` | Maps `codex` to its `CODEX_HOME` directory and `claude` to its `CLAUDE_CONFIG_DIR` directory. |
| `profiles.<name>.env_file` | Uses the profile's file in place of the host file. An omitted value inherits the host file. Relative profile and environment-file paths are resolved from the node state root. |
| `psmux`, `mux_socket` | Windows psmux executable and server namespace. Defaults are `psmux` on PATH and `cx`. |
| `mux_session` | Existing psmux session started from the signed-in desktop; jobs open as its windows. Unset starts each job in its own session. |

Use `cx hosts [HOST]` or `cx node facts` for host facts. `engines.<name>` reports `path`, `version`, and `login_status` (`logged_in`, `logged_out`, or null). After setup, `cx doctor HOST --json` also reports state and cache checks and profile login probes. Use a smoke turn to verify credentials supplied through `env_file`.

## Projects and multi-repository snapshots

Put repositories that a job needs to mount together in the same project. `cx project init NAME --repo DIR [DIR ...]` writes their primary checkouts to `~/.config/cx/projects/NAME.toml`. Use `cx project show NAME` to inspect the configuration.

`cx run`, `cx agent`, and `cx snapshot` accept `--project NAME`. Without an explicit selection, cx uses `CX_PROJECT` or matches the source Git repository to a project. When the source belongs to a multi-repository project, cx snapshots all its members relative to their common parent. The source repository root becomes the job working directory, so sibling repositories are available at paths such as `../library`. With `workspace-write`, the job can edit every snapshot member. A source outside the selected project uses its ordinary snapshot layout. Go workspace discovery also runs and adds any additional repositories.

For local inputs such as an ignored build configuration, add `include = [".cargo/config.toml"]` to the appropriate `[[repositories]]` entry. At initialization, repeat `--include REPO:PATH` for each path or glob. `REPO` can be a checkout path or a unique repository directory name. Quote globs to pass them to cx and use paths relative to the repository root with `/` separators. Each entry must match a file. cx reports missing entries and stops the snapshot. Matched files travel with the snapshot and are excluded from collected patches. Keep credentials out of `include`.

Use `cx patch JOB` to inspect all changed repositories and `cx apply JOB` to apply their changes together. Add `--repo PATH` to select one repository. `PATH` is relative to the snapshot's common parent. Both commands start after the last applied turn for each repository in the job run. They use the dispatch snapshot when there is no apply record. Patch headers show each starting turn. Pass `--since-turn 1` for the cumulative patch. Run `cx ls` on the controller that ran `apply` to see the latest applied turn per repository.

Run `cx rebase JOB TEXT` from the submitting controller to dispatch a follow-up against a fresh snapshot. For an active turn, cx queues the request until the turn is sealed and prints the controller log path. A new request for that turn replaces the pending request until cx claims a child job. After the claim, repeat the command with `--turn N` to retrieve the same child ID or retry a failed submission. The child receives the sealed report and patches under `$CX_CONTEXT/rebase/`.

Use `cx rebase JOB TEXT --host H [--profile P]` to continue on another host with the sealed turn’s cumulative changes merged into the current source snapshot. The source checkout stays untouched. Merge conflicts stop dispatch and list the affected files. Omit `--profile` to use the destination’s default. If the source host is unreachable, cx can use a sealed result already collected by the controller. Specify `--turn N` to select a particular cached turn. The child’s patch is relative to its new snapshot.

Run `cx refresh JOB [DIR]` from the submitting controller to freeze a new baseline for a running snapshot job. `DIR` defaults to the original source directory. After the current turn seals, cx rebases the checkout before starting another turn. A conflict produces a `STATUS: QUESTION` result with the affected paths and leaves the merge state in the checkout. Use `cx say JOB TEXT` to give resolution instructions. If a follow-up was already queued, send it again after deciding how to resolve the conflict.

`cx ls` shows the current snapshot ID and its activation time. Sealed results keep their original baselines. New results use the refreshed snapshot, and the applied-turn tracker starts empty for that baseline. Turn numbers continue to increase.


## Node state

The default state root is `~/.local/share/cx/state` on Unix and `%LOCALAPPDATA%\cx\state` on Windows. Set `CXRUN_STATE` to override it. Direct node operations that accept `--state PATH` can select it per call.

| Location under the state root | Contents |
| --- | --- |
| `host.json` | Host settings and execution profiles. |
| `PROJECT/JOB/` | Project job state. A direct node job without a project uses `JOB/`. |
| `PROJECT/JOB/state/NNNNNN.jsonl` | Numbered append-only job state segments, including configuration, status, and receipts. cx can rotate these segments. |
| `PROJECT/JOB/inbox/NNNNNN.jsonl`, `PROJECT/JOB/outbox/NNNNNN.jsonl` | Numbered message and control streams. |
| `PROJECT/JOB/turns/`, `PROJECT/JOB/blobs/` | Immutable turn reports and referenced agent content when present. |
| `PROJECT/JOB/tmp/` | Job-private temporary directory, removed on close or clean. |
| `PROJECT/JOB/progress.log`, `events.jsonl`, `output.log` | Progress, structured events, and merged command output. `output.log` applies to command jobs. |
| `.work/PROJECT/JOB/` | Private snapshot workspace, with its own `state/` and `blobs/` for checkout metadata. |
| `snapshot/`, `cache/` | Snapshot store and shared build and package caches. |

cx shares Cargo build output at `cache/cargo-target/PROJECT/REPO` for jobs in the same project and source repository, and uses a job's own target directory when it supplies `--env CARGO_TARGET_DIR=PATH`.

cx provides shared, writable defaults for `GOCACHE`, `GOMODCACHE`, `GOPATH`, `STATICCHECK_CACHE`, and `GOLANGCI_LINT_CACHE` under `cache/packages/`. Use `--env KEY=PATH` to select a different directory for a job. cx checks the cache budget and free-space threshold while jobs run and reclaims unused entries in least-recently-used order. Look for `cache_warning` in `cx hosts` and `cx ls --json` when leased caches alone exceeded the budget at the last scan.

Use cx to query and manage state. For direct access on an execution host, use `cx node status --job PROJECT/JOB`, `cx node list`, or `cx node wait --job PROJECT/JOB --timeout 20s`. Node `list` and `wait --any` accept project and session filters. Run `cx result JOB` on the controller to collect the sealed turn and preview its report. Use `--path` to print the absolute path to the local report, `--json` for metadata and artifact paths, or `--log` to stream the log. Use `--wait` to wait for the selected turn to seal. The overall result deadline defaults to `CXRUN_MONITOR_TIMEOUT` or 24 hours. Each collected turn is also subject to `CX_COLLECT_LIMIT` (30 minutes by default). `--timeout DURATION` sets the overall deadline and overrides the per-turn collection limit. Result node calls use a progress-based idle timeout (`CX_TRANSFER_IDLE` in seconds, default 120). Observation deadlines exit 124, and collection failures, including download deadlines, exit 44. `--path` can reuse either cached archive and fetches the standard archive on a cache miss.

## List jobs

`cx ls` shows job status for the current session. Use `--all` for all sessions or `--session ID` to select one. When a host is unreachable, cx uses its last successful listing from `~/.cache/cx/ls/<host>.json` and shows the snapshot time. With `--json`, cached job rows include `host`, `stale: true`, and `as_of` in UTC. A one-shot listing exits with code 1 if any host query fails, even when cached rows are available.

## Engine capabilities

| Capability | Codex (`codex`, default) | Claude Code (`claude`) |
| --- | --- | --- |
| Continue a session, recover with `resume`, set effort, use job messaging tools | Yes | Yes |
| Steer a running turn | Yes | Messages queue for the next turn |
| Fork, engine approvals and requests, and engine RPC (for example `review`, `history`, `models`) | Yes | — |
| Attach images with `--image` or structured input | Yes | PNG, JPEG, GIF, or WebP. Local and embedded images are limited to 5 MiB and 8,000 pixels per side. Up to 100 images per message. |
| Add writable roots on `cx node start` | Yes | Yes |
| Turn off command networking for `workspace-write` | `--no-network` | — |

- `workspace-write` is the default sandbox mode and enables networking by default. In Codex, `read-only` defaults to network off and can opt in with `--network`. For Claude Code, use `read-only` to give only file-reading tools by default. Its `workspace-write` mode gives edit and shell tools and requires networking enabled. Use an execution account suited to the task's permissions.
- For a new snapshot-backed agent, repeat `--own PATH` with `cx run` or `cx agent` to limit workspace writes to existing files or directories. Paths are relative to the main repository root. Use `../MEMBER/PATH` for a sibling snapshot repository. Managed caches and job temporary space remain writable. Claude Code requires its native Bash sandbox for scoped jobs.
- `--web-search` enables live web search. Claude Code also enables WebFetch with this option or with `--network`. `danger-full-access` grants the execution account's access and is recorded as privileged.
- For jobs submitted through the controller, job messaging tools are enabled by default. Use `--no-tools` to omit them. A direct node start opts in with `--job-tools`.
- Check `cx COMMAND --help` for the engines supported by engine-specific commands. With Claude Code, keep its session directory writable so the job can continue.

## Deadlines

Pass durations such as `15s` or `2m` to deadline flags.

| Deadline | Default |
| --- | --- |
| `--queue-timeout` / command `--timeout` | No limit. Command runtime starts after admission. |
| `--quiet-timeout` | 15 minutes without command output. |
| `--turn-timeout` / `--idle-exit` | 6 hours per agent turn / 30 minutes idle. |
| `--approval-timeout` | 2 minutes on engines that support approvals. |
| `--stall` | 10 minutes without agent events; 0 disables stall handling. |
| Wait, follow, and `result --wait` observation | 24 hours, or `CXRUN_MONITOR_TIMEOUT` when set. An observation deadline leaves the job running. |

Direct agent starts can set `--cmd-stall` (30 minutes for a silent command within a turn) and `--max-restarts` (3 automatic engine restarts per job).

Admission checks the host's disk threshold, load check, and available slots. A queued notice includes `queue_reason` and `event_id`. Single-job `wait` and `agent` share notification acknowledgement; `wait --any` tracks deliveries by cursor.

## Wait for any job

```sh
cx wait --any --host local --host worker --project alpha --project beta --timeout 2m
cx wait --any --job local:alpha/build --job worker:alpha/test --timeout 2m
```

`cx wait --any` prints one JSON event. It watches the current session across configured hosts and projects. Repeat `--host`, `--project`, or `--job HOST:PROJECT/JOB` to select multiple values. Jobs must match every filter supplied. Set `--session NAME` to select a session explicitly or `--all` to watch every session. If cx cannot determine the current session, it exits 2 and asks for an explicit session or scope.

cx selects the first nonempty value among `CX_SESSION`, `CLAUDE_CODE_HOST_SESSION_ID`, `CLAUDE_CODE_SESSION_ID`, `CODEX_THREAD_ID`, and `OPENCODE_SESSION_ID`, in that order. This detects Claude Code, Codex (including desktop when it exports `CODEX_THREAD_ID`), and OpenCode. Set `CX_SESSION` when your tool does not export a recognized variable. When creating a job, use `--session NAME` to override detection or `--no-session` to explicitly leave it ungrouped. With neither flag and no detected session, submission exits 2. Inside an agent or command job, cx clears inherited session identifiers and sets `CX_SESSION` to the job's own `HOST:PROJECT/JOB`. A child submission therefore belongs to that job's session.

Use `cx ls --brief` for a compact progress table or add `--json` for machine-readable rows. For registered jobs, `phase` summarizes progress as `running`, `idle`, `needs_input`, `rate_limited`, `ended`, `completed`, `failed`, or `queued`. An `ended` job has a saved agent thread and an available workspace. A `completed` job finished with status `done` and cannot be resumed by `say`. Check `rate_limit.reset_at` for the known reset time in Unix seconds. Use `cx say JOB TEXT` to send a message, including to an ended agent job with a saved thread. Use `cx say JOB @FILE` for long messages that exceed the local command-line limit.

Use `cx adopt JOB [--session NAME]` to move an existing job into the given session or, by default, the current session. `wait --any`, `ls`, and `overview` use the new session after adoption. Cursors keep acknowledgements already recorded for the same job.

A named cursor (default `default`) shares a session baseline across host, project, and job filters. Before polling for events, the controller attempts to initialize each configured host with all jobs in scope. Each host's first successful baseline sets its own starting position, even if the host has no jobs yet. Unreachable hosts initialize on a later successful contact. The node form, `cx node wait --any --since-file PATH`, saves the same session-wide baseline for a single host. Baselines persist even if no event arrives. Use `--from-start` when initializing to replay retained events, or choose a different `--cursor NAME` for independent delivery. Use `event_id` to make downstream handling idempotent. Only one observer can hold a session cursor at a time. A second observer exits with code 2 and reports the current observer’s PID and start time. Stop that process by PID or use `--cursor NAME` for an independent cursor. For node waits, use a different `--since-file PATH`.

Job events include these fields.

| Field | Meaning |
| --- | --- |
| `turn` | Number of the completed turn associated with this event, or `0` when the event has no completed turn. |
| `next` | Suggested command such as `cx result worker:alpha/check --turn 2`, or an empty string when the event has no result. |
| `rate_limited` | Boolean set when a usage or rate limit ended the selected turn. The existing outcome and exit code still describe the turn result. |
| `reset_at` | Blocking quota window reset recorded when the turn completed, in Unix seconds, or `null` when unknown or inapplicable. |

When a host disconnects, cx reports the outage on stderr and backs off independently while it watches the other hosts. A previously initialized host resumes from its saved position when it reconnects. The command exits 124 when its deadline expires without an event, with a stderr message if every selected host is unavailable. `--chat CHANNEL` includes channel messages for the selected projects (or the current project when no project is specified).

## Environment variables

| Variable | Use |
| --- | --- |
| `CXRUN_STATE` | State root on the machine running cx. |
| `CXRUN_MONITOR_TIMEOUT` | Default observation duration, for example `20s`. |
| `CX_PSMUX`, `CXRUN_TMUX_SOCKET` | Override the Windows psmux executable and namespace. |
| `CX_PROJECT`, `XDG_CONFIG_HOME` | Select a project explicitly or relocate the projects configuration directory. Project membership and source directory supply the fallback name. |
| `CX_SESSION` | Explicit session label for submissions and observation. |
| `CX_PARENT` | Default parent job or session for a submission. Use `--parent` to override it. |
| `CX_DEFAULT_HOST`, `CX_HOSTS` | Automatic submission host override and comma-separated host selection for multi-host views and waits. |
| `CX_CONTEXT_DIR`, `CX_POLICY_DIR` | Override project context and policy template directories. Context merges the shared `context/` next to the projects directory with `PROJECT/context/` (project wins); the job gets its copy at `$CX_CONTEXT`, and a `MEMORY.md` there is named in the preamble.  Host notes come from `hosts/HOST/context/` beside it, under `host/`; `--context` selects layers. |
| `CX_SNAPSHOT_STORE`, `CX_HOME` | Override the local snapshot store and the base directory for collected results and the apply log, respectively. |
| `CX_MODULE`, `CX_SETUP_BINARY` | Source module directory (the directory containing `go.mod` in the public checkout) or a prebuilt node binary for setup. |
| `CXRUN_PREAMBLE_FILE` | Default first-turn preamble file for direct `cx node start`. |

Transfer and controller operation limits use seconds without a unit suffix.

| Variable | Default |
| --- | --- |
| `CX_SUBMIT_LIMIT` | 90 minutes for submission. |
| `CX_TRANSFER_LIMIT` | 6 hours for transfer and deployment. |
| `CX_COLLECT_LIMIT` | 30 minutes for result collection. |
| `CX_RPC_LIMIT` | 3 minutes for engine RPC. |
| `CX_COMPILER_INSTALL_LIMIT` | 6 minutes for compiler installation on a build host. |
| `CX_TRANSFER_IDLE`, `CX_PUBLISH_IDLE` | 120 seconds of inactivity during transfer or snapshot publication, respectively. |
| `CX_PUBLISH_RETRY_WINDOW` | 30 minutes for retrying publication after connection failures; 0 disables retries. |

## Exit codes

Read `cx COMMAND --help` for the codes relevant to an operation. `cx node states` prints the terminal state map. Controller `wait JOB` and `exec --wait` also propagate a command process's normal exit code; use job state and `exit_code` to distinguish it.

| Code | Meaning |
| --- | --- |
| 0 | Success, completed turn, or help. |
| 1 | Failed operation, failed or interrupted turn, or patch conflict. |
| 2 | Controller usage, configuration, or other controller error. |
| 3 / 4 / 5 | Stalled, runtime or queue timeout, or orphaned runner. |
| 6 | Waiting for input. |
| 7 | Duplicate submission. |
| 8 / 9 | Background submission failure or transport failure. |
| 10 | `apply` wrote changes and could not read Git status. For direct `cx node log` with a byte offset, the terminal log is drained. |
| 11 | Node observation did not find a registered job. |
| 12 | Result download in progress. Retry or use `result --wait`. |
| 13 | Admission notice when stdout is JSON with `status: "queued"`. The job remains queued. |
| 44 | Result or required sealed turn unavailable. The reason is printed to stderr. |
| 124 / 130 | Observation timed out or was canceled. |
