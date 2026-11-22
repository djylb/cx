# cx

[English](../README.md)

cx 将 Codex、Claude Code、命令和脚本派发到本机或已配置的 SSH 主机。作业在后台运行，管理 AI 或用户可以跟踪进度、补充指令，并回收报告、Git 补丁和交付物。控制端（`cx`）和执行端（`cx node`）使用同一个 Go 二进制。

## 安装

源码构建需要 Go 1.26 或更高版本。Git 快照和补丁需要 Git，控制端执行三方合并需要 Git 2.38 或更高版本。远端执行需要支持非交互认证和 SFTP 的 OpenSSH。Windows 执行端需要登录会话内的 psmux。代理作业需要在执行端准备所选引擎的 CLI 和凭据。

阅读[安装指南](agent-install.md)，也可以把以下提示词交给管理 AI。

> 克隆 https://github.com/djylb/cx，按 `docs/agent-install.md` 操作。检查依赖，问我使用哪些执行主机和账号，安装系统工具前征得我的同意。完成冒烟检查后，按 `docs/agent-guide.md` 演示如何派发作业。

## 运行本机作业

将 `cx` 加入 PATH 后，在 POSIX shell 中运行以下命令。此示例无需 SSH 或模型凭据。

```sh
cx setup local
job=$(cx exec local --project demo --queue-timeout 10s --timeout 5s -- git --version)
cx wait "$job" --timeout 20s
cx result "$job" --log --wait
cx clean "$job"
```

代理作业需要已登录的 Codex CLI。以下临时 Git 工作区可用于检查和应用代理生成的补丁。

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

等待超时后，可继续等待同一个作业。一轮完成后，先审查补丁，确认接受后再应用，最后关闭并清理作业。

```sh
cx patch "$job" --full
cx apply "$job"
cx close "$job"
cx clean "$job"
```

## 查找命令

运行 `cx --help` 和 `cx COMMAND --help`。[管理指南](agent-guide.md)介绍派发、跟进和回收流程，[参考文档](reference.md)列出默认值、主机配置、引擎能力和退出码。

## 许可证

[Apache License 2.0](../LICENSE)
