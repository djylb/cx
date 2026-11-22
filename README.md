# cx

[简体中文](docs/README.zh-CN.md)

cx dispatches Codex, Claude Code, commands, and scripts to the local machine or a configured SSH host. Jobs run in the background. A managing AI or a person can follow progress, send instructions, and collect reports, Git patches, and artifacts. One Go binary provides both the controller (`cx`) and the execution node (`cx node`).

## Install

Build with Go 1.26 or later. Git is required for Git snapshots and patches, and Git 2.38 or later is required on the controller for three-way patch merges. For remote execution, use OpenSSH with non-interactive authentication and SFTP. Windows execution needs psmux in a logged-in session. Agent jobs need the selected engine CLI and credentials on the execution host.

Read the [installation guide](docs/agent-install.md), or give your managing AI this prompt.

> Clone https://github.com/djylb/cx and follow `docs/agent-install.md`. Check the prerequisites, ask me which execution hosts and accounts to use, and get my approval before installing system tools. Run the smoke checks and show me how to dispatch a job using `docs/agent-guide.md`.

## Run a local job

With `cx` on PATH, run this in a POSIX shell. The command needs neither SSH nor model credentials.

```sh
cx setup local
job=$(cx exec local --project demo --queue-timeout 10s --timeout 5s -- git --version)
cx wait "$job" --timeout 20s
cx result "$job" --log --wait
cx clean "$job"
```

For an agent job, use a signed-in Codex CLI. This disposable Git checkout lets you inspect and apply the agent's patch.

```sh
work=$(mktemp -d)
cd "$work"
git init -q
printf 'Demo\n' > README.md
git add README.md
git -c user.name='Demo Author' -c user.email=demo@example.invalid commit -qm 'Initialize demo'
prompt=$(mktemp)
printf '%s\n' 'Add one sentence to README.md and leave the change uncommitted.' > "$prompt"
job=$(cx run local . demo/edit "$prompt" --queue-timeout 10s --turn-timeout 15s)
cx wait "$job" --timeout 20s
cx show "$job"
```

If the wait expires, wait again for the same job. After a completed turn, inspect the patch, apply it if accepted, then close and clean the job.

```sh
cx patch "$job" --full
cx apply "$job"
cx close "$job"
cx clean "$job"
```

## Find a command

Run `cx --help` and `cx COMMAND --help`. See the [manager guide](docs/agent-guide.md) for dispatch, follow-up, and result collection, or the [reference](docs/reference.md) for defaults, host settings, engine capabilities, and exit codes.

## License

[Apache License 2.0](LICENSE)
