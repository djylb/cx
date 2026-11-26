---
name: cx
description: Dispatch, monitor, steer, and collect cx agent or command jobs on local and configured SSH hosts.
---

# Manage cx jobs

Read `docs/agent-guide.md` in the cx checkout. Use `docs/agent-install.md` to set up cx and `docs/reference.md` for defaults and engine capabilities.

Choose the execution host, inputs, engine, sandbox, and deadlines. Keep one background `cx wait --any --timeout 55m` per session: it returns one event for any of your jobs; handle it (`cx show`, `cx result`, `cx say`), then start it again. Never poll `cx status` in loops or wait per job. Review the patch before applying it. For multi-turn changes, follow the guide's instructions for applying only the turns you still need.
Keep the context layers current: record durable rules and host facts as you learn them, and judge memory proposals that agents send.
