# Install cx with a managing AI

Run these steps with the user who will operate cx. Ask for missing deployment information, and get consent before installing system tools or changing system services. Keep authentication in the user's SSH agent, OS credential store, or agent CLI login. Keep connection details and real configuration out of the checkout.

## 1. Identify the controller and execution hosts

- Determine the OS and CPU architecture of the controller and each intended execution host. Ask whether the controller itself should execute jobs. The built-in host `local` uses the controller account directly.
- Check Git, the system `ssh` client, and access to an OpenSSH server with SFTP on each remote. Check the selected agent CLI under the execution account, including `codex login status` or `claude auth status --json`. A command-only host can skip model login.
- Check tmux if you plan to host a Unix node in a tmux session. The normal Go Unix launch path detaches workers directly. On Windows, check psmux on PATH and its logged-in session for both command and agent jobs.
- Check free disk space and writable state and cache directories. If building, check a working Go 1.26+ toolchain. Install missing dependencies with the platform's package manager after the user agrees. Ask the user to complete logins that require credentials or a browser.

## 2. Obtain the controller binary

Use one of these paths. Install the binary as `cx` (`cx.exe` on Windows) in a user-owned directory on PATH, then run `cx version` to print the installed binary's SHA-256.

1. **Release download.** If the maintainers publish release assets, pick the OS/architecture build and verify it against the published SHA-256 checksum before installing. Use the release location provided by the maintainer.
2. **Go install.** From `cxrun/` in a source checkout, run `go install .`. Go writes `cx` to `GOBIN` or to the Go binary directory when `GOBIN` is unset.
3. **Build from source.** From `cxrun/`, run `go build -o cx .` on Unix or `go build -o cx.exe .` on Windows. Move the result to the user-owned PATH directory.

The current module path is `cx`. Both Go commands above use the checked-out module. A versioned `go install` from a public module requires a published import path.

## 3. Configure remote hosts

Ask which hosts, accounts and SSH aliases to use. Reuse existing entries in `~/.ssh/config` where possible. If an entry is needed, merge a block like this into the user's file and replace the uppercase placeholders with the values the user provided:

```sshconfig
Host cx-worker
    HostName EXECUTION_HOST
    User EXECUTION_ACCOUNT
```

Verify the remote host key through a trusted channel on first connection. Once it is trusted and the key is available, test non-interactive authentication with `ssh -o BatchMode=yes -o ConnectTimeout=10 cx-worker git --version`. cx uses OpenSSH in batch mode and relies on the user's host key checking and authentication.

In the controller user's `~/.config/cx/hosts.toml`, add a logical name that refers to the SSH alias:

```toml
[hosts.worker]
ssh = "cx-worker"
```

Here `worker` is the cx host name and `cx-worker` is the OpenSSH alias. Omit this entry for a local-only deployment. Optional `cx` and `state` override the node binary path and node state root. By default, the node stores state in `~/.local/share/cx/state`. The default remote install location is `.local/bin/cx` under the SSH user's home, with `.exe` on Windows. `setup` records the detected platform in the user config. An optional `via` names a configured relay that resolves the next SSH hop through its own SSH configuration. Start with direct SSH unless the deployment needs a relay.

## 4. Diagnose and deploy

```sh
cx doctor --json
cx doctor worker --json
cx setup worker
cx doctor worker --json
cx host worker
```

Use `cx setup local` for local execution. `doctor` reports probes such as tool availability, login state, node availability, and writable paths. Some checks may be unknown. A missing node or state directory before first setup is expected. Review the actual fields and resolve prerequisites before the smoke checks.

`setup` installs or reuses the matching binary and initializes the state directory. An existing `host.json` is preserved. Review the slot and load policy with the user before changing it. For a different target platform, keep a checkout and Go available for cross-building. Set `CX_MODULE` to the `cxrun/` module directory if discovery fails. You can also point `CX_SETUP_BINARY` at a verified, matching target binary for a specific setup invocation.

For deployments with published artifacts, setup can use `[release]` in the user host file, with a `url` template containing `{version}` and `{platform}` and a `[release.sha256]` entry keyed by `goos-goarch` for each platform. SSH and gzip streams carry job and result data. Some Linux upgrade paths use rsync for binary deltas. If rsync is unavailable, use `CX_SETUP_BINARY` for the target to select the explicit artifact path.

### Configure credentials on the execution host

Use the same OS account that runs the node. Keep credentials and account settings outside the checkout and its snapshots.

**Codex.** Load `OPENAI_API_KEY` into the execution account's local shell from its secret store, then run `printenv OPENAI_API_KEY | codex login --with-api-key`. For ChatGPT sign-in on a headless host, use `codex login --device-auth`. Run the login and `codex login status` with `CODEX_HOME` set to the account directory you intend to use. [Codex CLI authentication](https://developers.openai.com/codex/cli/reference#codex-login)

**Claude Code.** Put `ANTHROPIC_API_KEY` in a private executor `env_file`, or configure a local `apiKeyHelper` in the account's Claude settings. For an account that supports `claude setup-token`, store its returned token locally as `CLAUDE_CODE_OAUTH_TOKEN` in the executor's `env_file`. Set `CLAUDE_CONFIG_DIR` to the desired account directory when preparing its settings or authentication. [Claude Code authentication](https://code.claude.com/docs/en/authentication)

On the execution host, merge credential paths and named profiles into `host.json` under its state root. The example uses paths relative to that root. Absolute executor paths also work.

```json
{
  "env_file": "credentials/host.env",
  "default_profile": "account-a",
  "profiles": {
    "account-a": {
      "codex_home": "accounts/a/codex",
      "claude_config_dir": "accounts/a/claude"
    },
    "account-b": {
      "codex_home": "accounts/b/codex",
      "claude_config_dir": "accounts/b/claude",
      "env_file": "credentials/account-b.env"
    }
  }
}
```

Create the credential files privately on the executor. Use mode `0600` on Unix and restrict access to the executor account on Windows. The UTF-8 format is one `KEY=VALUE` per line. Blank lines and lines beginning with `#` after optional whitespace are ignored. Characters after the first `=`, including quotes, spaces, inline `#` and dollar signs, are literal. A profile's `env_file` replaces the host file selection. A profile with no file uses the host file. Only the child process environment receives the file's values.

Use `cx doctor HOST --json` after setup to check each profile's Codex login, Claude CLI availability and selected file. A missing file is skipped. Unix permission warnings are advisory. The Codex probe checks the account's `CODEX_HOME`; it does not verify API credentials supplied by an environment file.

### Windows execution

- Enable OpenSSH Server and verify SSH and SFTP as the intended Windows account. Keep PowerShell available to the SSH login.
- Sign in to the Windows desktop with that account. From the logged-in session, start or reuse a psmux server in the `cx` namespace. For example, if it has no sessions, run `psmux -L cx new-session -d -s cx-bootstrap` in the desktop terminal. Check `psmux -L cx list-sessions` and the `psmux_session` field from the controller. Keep the Windows account logged in while jobs run. If you reuse a different psmux namespace, set `CXRUN_TMUX_SOCKET` in the execution account's SSH environment to match it.
- Verify the agent CLI is visible in the psmux environment. If command execution works but agent startup hangs or fails, check the desktop session and sandbox diagnostics before changing permissions.

## 5. Run two short smoke checks

Replace `worker` with the configured cx host name, or use `local`. Keep the work disposable and use short deadlines. These examples use a POSIX shell on the controller. On Windows, create the temporary directory with PowerShell and pass its path to the same cx options.

```sh
cx exec worker --project cx-smoke --wait --queue-timeout 15s --timeout 10s -- git --version
```

Check the output and process exit code. The job ID goes to stderr in this mode. Use it to inspect or clean up the command job.

For the agent check, use a fresh empty directory on the controller (`"$demo"` in this example). Run one short, read-only turn with the engine already set up on the execution host.

```sh
demo=$(mktemp -d)
CXRUN_MONITOR_TIMEOUT=1m cx agent --host worker --dir "$demo" --engine codex --sandbox read-only \
  --queue-timeout 15s --turn-timeout 30s --idle-exit 30s \
  'Do not use tools. Reply with exactly one line: STATUS: DONE'
```

Use `--engine claude` for Claude Code and select a low-cost available model with `--model`. Check the reported status and the job ID. If the agent check fails, inspect `cx show JOB` and the reported startup error. Close the agent when finished and clean the smoke jobs once their output is no longer needed.

## 6. Install the manager instructions

For Claude Code, copy the template [skills/cx/SKILL.md](../skills/cx/SKILL.md) to the user's `~/.claude/skills/cx/SKILL.md`, or to `.claude/skills/cx/SKILL.md` for one project. In the installed copy, provide the local checkout location for `docs/agent-guide.md` and this guide, or pass that location when invoking the skill. For Codex, give the session the same guide or add a reference to it in that project's `AGENTS.md`.

Report the controller version, logical hosts, selected engines, smoke results, and any checks still awaiting the user's action. Hand off to the [daily agent guide](agent-guide.md).
