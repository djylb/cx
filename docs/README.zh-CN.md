# cx

[English](../README.md)

cx 让任何能运行 shell 命令的 AI 会话把代理或命令任务派到本机或已配置的 SSH 主机。作业在执行机后台运行。管理会话可以随时查看进度、给代理补充指令，并回收报告、补丁和文件。

控制端（`cx`）和执行端（`cx node`）使用同一个 Go 二进制。主机、模型、沙箱和重试由用户或管理 AI 决定。工作区补丁需显式应用，提交仍由调用方按项目流程完成。

## 特性

- 支持 Codex、Claude Code、直接命令和上传脚本。
- 将源码冻结为快照，在独立副本执行。同一快照可以重复派发。
- 断线后可以重新观察作业，按游标续读日志，或用一个后台等待观察一组作业。
- 按轮回收报告、逐仓补丁和 `artifacts/` 下的交付物。

## 要求

| 位置 | 要求 |
| --- | --- |
| 控制端 | 与操作系统及架构匹配的 `cx`。Git 快照和补丁需要 Git，远端作业需要 OpenSSH 客户端。 |
| 远端执行机 | Linux、macOS 或 Windows，已启用 OpenSSH Server，并有支持非交互 SSH 的账号。Git 检出需要 Git。 |
| 执行环境 | 代理作业需要已登录且支持 `app-server` 的 Codex CLI，或与所用模式兼容的 Claude Code CLI。Windows 的命令和代理作业需要登录会话内的 psmux。macOS 和 Linux 的 Go 控制端直接分离后台进程，也可手动在 tmux 中托管节点。 |
| 源码构建 | Go 1.26 或更新版本。跨平台部署和可选的 rsync 升级路径见[安装指南](agent-install.md)。 |

## 安装提示词

把下面这段话交给你选定的管理 AI：

> 请读取 cx 源码副本里的 `docs/agent-install.md`，按步骤帮我部署 cx。先检测操作系统和现有依赖，缺信息就问我，包括执行主机和登录账号。安装工具前请征得我的同意。实际连接信息只写进我的用户配置。完成文档中的短冒烟检查后，汇总部署结果并演示如何派发和观察作业。

## 快速上手

以下示例使用 POSIX shell 和临时目录。第一个示例只运行本机命令，无需远端主机或模型登录。

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

在同一个示例目录里，使用已登录的 Codex CLI 提交代理任务。提示词放在源码目录外。

```sh
prompt=$(mktemp)
printf '%s\n' '给 README.md 增加一句话。完成时报告 STATUS: DONE。' > "$prompt"
job=$(cx run local . edit "$prompt" --queue-timeout 30s --turn-timeout 2m)
cx wait "$job" --timeout 30s
cx show "$job"
```

如果 wait 返回 124，可再次等待同一个作业。作业运行时，可以在另一个终端执行 `cx tail "$job" --timeout 30s`，也可以用 `cx say "$job" '请把新增内容控制在一句话。'` 补充指令。如果作业已经空闲，`say` 会打开新一轮，等该轮完成后再收结果。

```sh
cx result "$job" --wait
cx patch "$job"
# 审查后再应用到原始工作树。
cx apply "$job"
cx close "$job"
# 确认作业进入终态后再清理。
cx status "$job"
cx clean "$job"
```

把 `local` 换成已配置的 cx 主机别名，即可派发到 SSH 主机。源码快照、未提交文件改动、提示词、附件和项目上下文可能发往执行机和配置的中转机，代理输入会发送给所选模型服务。请据此选择输入和执行账号。

## 命令一览

| 命令 | 用途 |
| --- | --- |
| `doctor [HOST]`、`setup HOST`、`host HOST`、`hosts [HOST]` | 检查依赖、部署节点、查看准入配置或主机事实。 |
| `run HOST DIR TAG PROMPTFILE`、`agent --host HOST --dir DIR TEXT` | 后台提交，或提交后等待报告。 |
| `exec HOST -- ARGV…`、`exec HOST --script FILE` | 执行命令或脚本，可加 `--wait`。 |
| `snapshot DIR`、`project init NAME --repo DIR` | 冻结输入以复用，或登记项目成员。 |
| `wait JOB`、`wait --any`、`tail JOB`、`show JOB` | 等待事件、追踪进度或查看指定轮次。 |
| `status JOB`、`ls`、`overview` | 查询作业状态，或浏览各主机作业。 |
| `say JOB TEXT`、`interrupt JOB`、`resume JOB`、`close JOB` | 插话、打断、恢复或关闭代理。 |
| `result JOB`、`patch JOB`、`apply JOB`、`rebase JOB TEXT`、`clean JOB` | 回收结果、检查补丁、应用变更、用新快照重派或清理作业。 |

从另一控制端操作作业时，可以用 `HOST:project/tag` 直接寻址。语法、会话分组和 Codex 控制命令见[管理者指南](agent-guide.md)。当前控制端没有通用的 `--help` 输出。

## 退出码

下表描述观察或操作的结果。命令进程正常退出时，`wait JOB` 和 `exec --wait` 透传进程退出码。遇到同一个数字可能有两种含义时，检查作业状态和 `exit_code`。

| 退出码 | 含义 |
| --- | --- |
| 0 | 操作成功或观察到本轮完成。 |
| 1 | 代理本轮失败或被中断。 |
| 2 | 控制端报错，例如参数或配置错误。 |
| 3 | 作业卡住，包括命令无输出超时。 |
| 4 | 作业运行或排队超时。 |
| 5 | 运行器消失，作业变为孤儿。 |
| 6 | 代理等待输入。 |
| 8 | 后台提交失败，查看 `show JOB`。 |
| 9 | 观察或回收时发生传输错误，可重新观察。 |
| 10 | `apply` 已写入变更，但读取 Git 状态失败。重试前检查工作树。 |
| 11 | 节点观察时未找到已登记的作业。 |
| 12 | 结果仍在回收。可重试或加 `result --wait`。 |
| 44 | 所请求的结果尚不可用。检查作业和轮次。 |
| 124 | 本次观察超时，作业继续运行。 |

代理报告首行可以是 `STATUS: DONE`、`STATUS: PARTIAL` 或 `STATUS: BLOCKED`。判断是否交付时，结合报告和实际验证结果。

## 许可证

Apache License 2.0，见 [LICENSE](LICENSE)。
