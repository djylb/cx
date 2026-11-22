# cx

[简体中文](docs/README.zh-CN.md)

cx lets any AI session with shell access dispatch an agent or command to a local machine or an SSH host you configure. Jobs run in the background on the execution host. The managing session can check progress, send instructions to an agent, and collect reports, patches, and files.

One Go binary serves as both the controller (`cx`) and the execution node (`cx node`). You or your managing AI choose the host, model, sandbox, and retry strategy. Applying a patch is an explicit step, and you decide when to commit.

## Features

- Run Codex or Claude Code agents, direct commands, or uploaded scripts.
- Freeze a source directory into a snapshot for an isolated checkout. Reuse the snapshot to run multiple jobs against the same inputs.
- Reconnect to running jobs, follow logs with cursors, and watch a group of jobs with one background wait.
- Retrieve completed turns, inspect per-repository patches, and collect files from `artifacts/`.

## Requirements

| Where | Requirements |
| --- | --- |
| Controller | A `cx` binary for your OS and architecture, Git for Git snapshots and patch workflows, and an OpenSSH client for remote jobs. |
| Remote host | Linux, macOS, or Windows with OpenSSH Server and an account usable from non-interactive SSH. Git is needed for Git checkouts. |
| Execution host for agent jobs | A signed-in Codex CLI with `app-server` support or a Claude Code CLI compatible with the selected mode. Windows command and agent jobs use psmux in a logged-in session. The current Go launcher detaches workers directly on macOS and Linux. The Unix node can also be hosted under tmux for manual operation. |
| Building from source | Go 1.26 or later. See the [installation guide](docs/agent-install.md) for cross-platform deployment and the optional rsync upgrade path. |

## Installation prompt

Copy this into the AI session you want to use as your manager. It needs no other context:

> Install and set up cx for me from https://github.com/djylb/cx. Clone the repository with `git clone https://github.com/djylb/cx` (or update an existing clone), then read `docs/agent-install.md` in it and follow it step by step. Detect my operating system and check which dependencies and credentials are already available. Ask me for missing information, including which execution hosts and accounts to use. Get my consent before installing tools. Put real connection details only in my user configuration. Install the cx skill or guide reference for this AI as the guide describes. Run the documented short smoke checks, then summarize the setup and show me how to dispatch and monitor a job with `docs/agent-guide.md`.

## Quick start

These examples use a POSIX shell and a disposable directory. No remote host or model login is required for the first example.

```sh
work=$(mktemp -d)
cd "$work"
git init -q -b main
printf 'Demo\n' > README.md
git add README.md
git -c user.name=Example -c user.email=example@example.invalid commit -qm 'Initialize demo'

export CX_PROJECT=cx-demo
cx setup local
job=$(cx run local . check --cmd --queue-timeout 30s --timeout 20s -- git status --short)
cx wait "$job" --timeout 45s
cx show "$job"
cx clean "$job"
```

For an agent job, use the same demo checkout and a signed-in Codex CLI. Save the prompt outside the source directory.

```sh
prompt=$(mktemp)
printf '%s\n' 'Add one sentence to README.md. Report STATUS: DONE if finished.' > "$prompt"
job=$(cx run local . edit "$prompt" --queue-timeout 30s --turn-timeout 2m)
cx wait "$job" --timeout 30s
cx show "$job"
```

If the wait returns 124, repeat it to observe the same job. While the job is active, use `cx tail "$job" --timeout 30s` from another terminal to follow it. Send a correction with `cx say "$job" 'Keep the sentence under 15 words.'`. On an idle agent, `say` opens another turn, so wait for that turn before collecting it.

```sh
cx result "$job" --wait
cx patch "$job"
# After reviewing the patch, apply it to the source checkout.
cx apply "$job"
cx close "$job"
# Check that the job has reached a terminal state before cleaning.
cx status "$job"
cx clean "$job"
```

For a configured SSH host, replace `local` with its cx alias. Snapshot jobs may send source files, uncommitted changes, prompts, attachments, and project context to the execution host and any configured relay. Agent input goes to the selected model service. Choose the source and execution account accordingly.

## Commands

| Command | Use |
| --- | --- |
| `doctor [HOST]`, `setup HOST`, `host HOST`, `hosts [HOST]` | Check dependencies, deploy the node, inspect host policy, or read host facts. |
| `run HOST DIR TAG PROMPTFILE`, `agent --host HOST --dir DIR TEXT` | Submit a background agent job, or submit and wait for a report. |
| `exec HOST -- ARGV…`, `exec HOST --script FILE` | Run a command or script on the host, with optional `--wait`. |
| `snapshot DIR`, `project init NAME --repo DIR` | Freeze inputs for reuse or register project membership. |
| `wait JOB`, `wait --any`, `tail JOB`, `show JOB` | Wait for an event, stream progress, or inspect a turn. |
| `status JOB`, `ls`, `overview` | Inspect job state or survey jobs across configured hosts. |
| `say JOB TEXT`, `interrupt JOB`, `resume JOB`, `close JOB` | Send input or manage an agent's lifecycle. |
| `result JOB`, `patch JOB`, `apply JOB`, `rebase JOB TEXT`, `clean JOB` | Retrieve, inspect, apply, retry against fresh inputs, or clean up. |

Use `HOST:project/tag` to address a known job directly from another controller. See the [agent guide](docs/agent-guide.md) for command syntax, session grouping, and engine controls. Every command accepts `-h`/`--help`.

## Exit codes

These codes describe the observation or operation. A command process that exits normally supplies its own exit code to `wait JOB` and `exec --wait`. Inspect the job state and its `exit_code` when the number is ambiguous.

| Code | Meaning |
| --- | --- |
| 0 | Operation succeeded or the observed turn completed. |
| 1 | Agent turn failed or was interrupted. |
| 2 | Controller error, for example invalid syntax or configuration. |
| 3 | Job stalled, including a command's quiet timeout. |
| 4 | Job runtime or queue timeout. |
| 5 | Job orphaned after its runner stopped. |
| 6 | Agent waiting for input. |
| 8 | Background submission failed. Inspect `show JOB`. |
| 9 | Transport failure while getting the result or observing the job. Retry observation. |
| 10 | `apply` wrote changes, then failed to read Git status. Check the worktree before retrying. |
| 11 | A node observation could not find a registered job. |
| 12 | Result collection is in progress. Retry or use `result --wait`. |
| 13 | `wait` or `agent` observed an admission block. Stdout is JSON with `status: "queued"` and `queue_reason`. |
| 44 | The requested result is unavailable. Check the job and turn. |
| 124 | Observation timed out. The job keeps running. |

An agent's report can begin with `STATUS: DONE`, `STATUS: PARTIAL`, or `STATUS: BLOCKED`. Read that line and the reported evidence when deciding whether the task was delivered.

## License

Apache License 2.0. See [LICENSE](LICENSE).
