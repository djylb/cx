---
name: cx
description: Dispatch, monitor, steer, and collect cx agent or command jobs on local and configured SSH hosts.
---

# Manage cx jobs

Read the cx checkout's `docs/agent-guide.md` before dispatch. If the checkout location is unknown, ask the user for it. For setup, follow `docs/agent-install.md`.

Choose the host, sandbox, and time limits for the task. Keep the returned job ID and use a background `cx wait` when the session supports it. Use `cx show` and `cx tail` for evidence, `cx say` to steer, and `cx result` to collect. Review `cx patch` before an explicit `cx apply`.
