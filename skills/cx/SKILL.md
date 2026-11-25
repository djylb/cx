---
name: cx
description: Dispatch, monitor, steer, and collect cx agent or command jobs on local and configured SSH hosts.
---

# Manage cx jobs

Read `docs/agent-guide.md` in the cx checkout. Use `docs/agent-install.md` to set up cx and `docs/reference.md` for defaults and engine capabilities.

Choose the execution host, inputs, engine, sandbox, and deadlines. Save the job ID, then use `cx wait` and `cx show` to observe the turn, `cx say` to follow up, and `cx result` to collect it. Review the patch before applying it. For multi-turn changes, follow the guide's instructions for applying only the turns you still need.
Keep the context layers current: record durable rules and host facts as you learn them, and judge memory proposals that agents send.
