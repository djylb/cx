# Install cx with a managing AI

## Build and install the controller

Confirm the controller and execution platforms with the user. Get approval before installing system tools or changing services. Keep connection details and credentials in user configuration outside the checkout.

```sh
git clone https://github.com/djylb/cx
cd cx
go build -o cx .
```

On Windows, use `go build -o cx.exe .`. Move the binary to a user-owned directory on PATH and run `cx version` to print its SHA-256. Alternatively, `go install github.com/djylb/cx@latest` installs into `GOBIN` or the Go binary directory. Keep the checkout for cross-platform setup.

## Register and set up execution hosts

Use the built-in `local` target for execution under the controller account. For a remote target, reuse an alias from the user's OpenSSH configuration. Verify the host key and confirm batch-mode SSH and SFTP work under the intended execution account. Add the alias to `~/.config/cx/hosts.toml`; replace both uppercase placeholders.

```toml
[hosts.TARGET]
ssh = "SSH_ALIAS"
```

Set `target=local` as below, or assign the configured target key to `target`.

```sh
target=local
cx setup "$target"
cx doctor "$target" --json
cx host "$target"
```

Check Git for Git jobs, free space, a writable local state volume, and the selected engine's CLI and login. For Windows, enable OpenSSH Server with SFTP and PowerShell, and keep the execution account signed into the desktop. Start psmux from that session if needed.

```sh
psmux -L cx new-session -d -s cx-bootstrap
psmux -L cx list-sessions
```

Then set `cx host TARGET --set mux_session=cx-bootstrap` so jobs open as windows of that desktop session. Jobs started from an SSH session otherwise run without a desktop, where sandboxed GUI-linked programs (Git, Node, PowerShell) fail to start. Check `psmux_session` and `mux_session_present` in `cx doctor` and verify the engine CLI is on the session's PATH. For binary and state paths, cross-platform setup, and host policy, see the [reference](reference.md#host-configuration).

## Configure execution credentials

Sign in under the account that runs the node. For a command-only host, skip agent login. Prepare Codex with `CODEX_HOME` or Claude Code with `CLAUDE_CONFIG_DIR` set to the intended account directory. [Codex authentication](https://developers.openai.com/codex/cli/reference#codex-login) · [Claude Code authentication](https://code.claude.com/docs/en/authentication)

To select separate engine accounts or supply credentials from a private environment file, merge a profile into `host.json` on the execution host. The paths below are relative to the state root.

```json
{
  "default_profile": "review",
  "profiles": {
    "review": {
      "engine_homes": {"codex": "accounts/codex", "claude": "accounts/claude"},
      "env_file": "credentials/review.env"
    }
  }
}
```

Create the file with access restricted to the execution account (`0600` on Unix). Use one UTF-8 `KEY=VALUE` per line. Leave values unquoted unless the quotes are part of the value. After setup, check `cx doctor "$target" --json` for engine login and profile configuration. For API keys or tokens supplied by the environment file, verify access with the agent smoke check.

## Run the smoke checks

Use the same `target` as above. These examples use a POSIX shell on the controller. From PowerShell, set the equivalent environment variable and use a fresh directory for the agent check.

```sh
cx exec "$target" --project smoke --wait --queue-timeout 10s --timeout 5s -- git --version
```

The command prints its job ID to stderr. For an agent host, run a short read-only turn using the configured engine. For Claude Code, replace `codex` with `claude` and select a low-cost available model with `--model`.

```sh
demo=$(mktemp -d)
CXRUN_MONITOR_TIMEOUT=20s cx agent --host "$target" --dir "$demo" --engine codex \
  --sandbox read-only --queue-timeout 10s --turn-timeout 10s \
  'Reply with exactly one line: STATUS: DONE'
```

Use the job IDs to inspect results. Close the agent and clean both jobs after they reach a terminal state. See the [manager guide](agent-guide.md#collect-review-and-clean-up).

## Give the manager its instructions

For Claude Code, copy [SKILL.md](../skills/cx/SKILL.md) to `~/.claude/skills/cx/SKILL.md` or the project's `.claude/skills/cx/SKILL.md`. Tell the manager where the cx checkout is. For Codex, give the session the [manager guide](agent-guide.md) or reference it from the project's `AGENTS.md`. Report the installed version, selected targets and engines, and smoke results.
