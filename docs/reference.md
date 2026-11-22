# cxrun

`cxrun` 在执行主机上运行代理作业或命令作业。代理引擎由注册表（`engine.go`）统一描述：Codex 引擎持有一个 `codex app-server` 子进程，通过 stdio JSON-RPC 维护线程；Claude Code 引擎每轮运行一次 `claude` CLI，以 session 续接。关闭 Monitor、SSH 断线，或控制端 Mac 进入睡眠后，远端执行主机上的 tmux/psmux 会话可以继续运行。执行主机本身需要保持运行，模型推理需要该主机能访问服务。

## 执行端 CLI

Codex 模式的 `start` 是前台 runner。控制端可将它放入 tmux/psmux，然后通过 `status` 确认登记。下面的命令共用同一个私有状态根，示例中的路径和标识由调用方提供。

```bash
export CXRUN_STATE="$STATE"
cxrun host
cxrun start --project tools --job review --session sprint --dir "$DIR" \
  --prompt-file "$FILE" --preamble-file "$PREAMBLE" --dedup-key "$KEY"
cxrun status --job tools/review
cxrun wait --project tools --job review --timeout 20m
cxrun wait --any --project tools --session sprint --since-file "$CURSOR" --timeout 20m
cxrun list --project tools --session sprint --fields brief
cxrun ps --job tools/review
cxrun log --job tools/review 0
cxrun close --job tools/review
cxrun clean --job tools/review
```

| 选项或命令 | 执行端行为 |
| --- | --- |
| `--project P`、`--session S` | project 最长 48 个字符，命名规则与 job 相同。项目作业在 `STATE/P/JOB/`，所有单作业命令也接受 `--job P/JOB`。session 是筛选标签，保存到 config 和 status。省略 project 的单名作业沿用 `STATE/JOB/`，旧 runner 持锁时重复 start/resume 返回原状态。 |
| `host [--state PATH]` | 输出当前生效的主机配置 JSON，含各槽位类别及负载检查命令。 |
| `--preamble-file PATH` | 读取 UTF-8 前言，以空行与首轮提示词分隔。省略时读取 `CXRUN_PREAMBLE_FILE` 指定的文件。两者均未指定、显式路径为空或文件内容全为空白时，直接发送提示词。后续轮使用各自的输入。 |
| `--stall-nudge-file PATH` | 读取非空的 UTF-8 文案，供卡住时提醒和恢复时继续任务使用。省略时，提醒使用 `No progress detected. Report status, abandon any stuck command, and continue the task.`，续跑使用 `Continue the unfinished task.`。 |
| `start --timeout DUR`、`--queue-timeout DUR` | 两种引擎共用。运行期限从命令进程或 app-server 成功启动时计时。每次等待 FIFO 准入单独计时，含负载、磁盘及缓存检查；获槽后继续消耗运行预算。两项默认均为 0，表示不限。空闲 app-server 计入运行时间，stall 重启沿用剩余预算。运行超时进入 `timeout`，排队超时进入 `queue_timeout`。 |
| `--dedup-key K`、`--force` | 使用可作文件名的标识（例如十六进制 SHA-256）。同 JOB 重试打印已有状态，退出 0。其他 JOB 命中进行中或已成功的回执时打印原状态，退出 7。失败或中断后可重新提交。force 跳过去重，同 JOB 仍幂等。 |
| `--stall DUR` | 默认 10 分钟，0 关闭。活动命令的静默窗口取 stall 与 `--cmd-stall` 的较大值，后者默认 30 分钟。连续静默先 steer，再 interrupt 后续跑，再按 PID 杀掉 app-server 进程树、恢复原线程并继续；仍静默则进入 stalled。意外退出和 stall 重启共用 `--max-restarts N`，默认每个作业 3 次，0 禁止自动重启。自动恢复已用次数在状态的 `engine_restarts` 中，`restarts` 同时计入显式 resume。动作见 progress.log 及状态的 stall_count 和 last_action。 |
| `status --job JOB` | 输出状态 JSON。状态日志中尚无有效 `status` 帧时，输出 `{"job":"…","status":"not_registered"}` 并退出 11。状态读取或解码错误会报告异常。 |
| `wait --timeout DUR` | 在同一个观察期限内等待登记、本轮完成或 runner 终止。退出 0 表示 completed，1 表示 failed/interrupted，3 表示 stalled，4 表示 timeout 或 queue_timeout，5 表示 orphaned，6 表示待回答。124 专用于本次观察超时，打印 wait_timeout=true。登记前超时还会打印 status=not_registered。默认观察期限 24 小时，可用 CXRUN_MONITOR_TIMEOUT 设置。 |
| `wait --any [--project P] [--session S] [--since-file F] [--timeout DUR]` | 每秒检查状态根，返回首个匹配的新事件。准入阻塞退出 13，待答问题退出 6。完成或终态时，节点命令退出 0，作业结果记在 JSON 的 `code` 字段，控制端用该字段作为退出码。观察超时退出 124 并保持 stdout 为空。默认观察期限与单作业 wait 相同。 |
| `states` | 输出 JSON 对象，键为终态名，值为 wait 的退出码。已结束轮次的 failed/interrupted 结果返回 1。 |
| `list [--project P] [--session S] [--fields brief]` | 每行一个作业 JSON，可组合筛选。省略筛选时包含旧扁平作业，省略 fields 时保留完整状态字段。 |
| `clean --job P/JOB` | 仅删除已经释放 owner.lock 的 由 `states` 列出的终态作业状态目录。保留 `.dedup/K` 回执，成功作业清理后再次命中仍退出 7。控制端的 `cx clean` 一并回收私有工作目录。 |
| `log --job P/JOB --since SEQ` | 输出 progress.log 中 seq 严格大于 SEQ 的完整行。0 从首个带 seq 的记录开始。命令作业也可用此入口读取生命周期日志。 |
| `log --job P/JOB [OFFSET]` | OFFSET 为字节偏移。Codex 作业输出完整日志行，命令作业输出原始字节；终态且已读尽时退出 10。 |

### 主机准入配置

在执行端状态根放置 `host.json`。状态根由 `--state PATH`、`CXRUN_STATE` 或默认位置依次确定：Unix 为 `~/.local/share/cx/state`，Windows 为 `%LOCALAPPDATA%\cx\state`。

```json
{"slots":{"agent":3,"test":2,"system":1},"load_cmd":""}
```

- 槽位数必须是正整数。缺少文件或类别时，`agent`、`test` 及未配置的自定义类别在 Linux、Windows、其他平台上分别默认 10、6、3。`system` 默认 1。可用 `--slot-class` 选择类别。状态根保留 `host.json` 和 `cache` 名称，项目内的作业仍可使用这些名称。
- `load_cmd` 适用于所有类别。省略或为空时直接进行槽位检查。非空时 Unix 用 sh，Windows 用 cmd.exe，以退出码 0 表示允许准入。排队时立即检查，随后每 15 秒复查，单次上限 10 秒。
- 每次准入读取配置，包括跟进轮。排队时定期刷新配置，负载命令变化后重新检查。持有的槽位随该轮结束或进程退出而释放。遇到无法读取或格式错误的配置，`cxrun host` 和新一轮准入都会报错。更新文件时可在同目录写入临时文件并原子替换。
- 各类别有独立的 FIFO 票据和槽位锁。代理默认类别 `agent` 使用 `.admit/queue/` 和 `.admit/slot-N.lock`，其他类别位于 `.admit/classes/类别/`。等待时显示 `queued`，可用 `close` 取消。空闲时 `say` 开新轮并重新排队，活动轮直接 steer。stall 中断后续跑及引擎重启沿用原槽位。跟进轮排队时仍处理控制消息、协议帧和空闲期限。
- `min_free_bytes` 约束状态根所在卷的可用空间，默认取 20 GB 和该卷容量 5% 中的较大值。可显式设为 0。低于阈值时作业保持 `queued`，并请求主机回收可再生缓存。多个作业共享回收扫描，排队期间继续检查可用空间和 close 请求。逐项删除后复测实际可用空间。`status` 的 `queue_reason` 说明阻塞条件，`doctor` 和 `overview` 的 `disk_warning` 提供当前字节数。满足水位后按原队列继续准入。
- 作业进程继承原环境变量（包括 `GOCACHE`），执行端设置 `CARGO_TARGET_DIR=<state>/cache/cargo-target/<project>`，并加入代理 `workspace-write` 的可写目录。同项目复用 Cargo 构建产物，无项目名的作业使用自己的缓存目录。`cache_max_bytes`（默认 50 GB）合计各 Cargo target、共享 npm/pnpm/yarn/pip 缓存，以及 Cargo 下载的 registry 和 Git 缓存中的普通文件逻辑大小。运行器释放租约后和磁盘低水位时触发主机清理，按目录及内容记录的最近使用时间从旧到新淘汰。清理限于 cx 准备并登记且身份匹配的目录，跳过包含挂载边界的目录。项目 Cargo target 和共享包缓存的租约保护运行器及其仍存活的后台进程。作业工作目录、用户自定义缓存路径以及 Cargo 凭据、配置和已安装工具均在回收范围之外。
- 执行端在 `<state>/cache/packages/` 下共享 npm、pnpm、Yarn、pip 和 Cargo 注册表缓存。作业收到 `npm_config_cache`、`PNPM_STORE_DIR`（以及 pnpm 的 `npm_config_store_dir`）、`YARN_CACHE_FOLDER`、`PIP_CACHE_DIR` 与 `CARGO_HOME`，这些目录也加入 Codex `workspace-write` 的可写目录。命令作业仍可通过 `--env` 显式覆盖变量。
- 从旧版升级时，把 `--slots` 和 `--load-cmd` 的设置迁到主机文件，并从调用方删除这两个参数。旧作业快照中的 `slots` 和 `load_cmd` 不再参与准入。

### 执行账户与凭据

- 在执行端 `host.json` 配置可选的 `env_file`、`default_profile` 和 `profiles`。每个 profile 支持 `engine_homes`（引擎名到账户目录，按引擎导出为 `CODEX_HOME`、`CLAUDE_CONFIG_DIR`）和 `env_file`，相对路径以执行端状态根为基准。配置示例见 [安装指南](../docs/agent-install.md#configure-credentials-on-the-execution-host)。
- 新作业使用 `--profile NAME` 或主机默认账户。未知 profile 报错。作业状态只保存已选账户名。续跑和续轮沿用该账户，显式指定的名称必须一致。fork 默认继承，也可为新作业显式选择账户。
- profile 提供的 `env_file` 替换主机文件，省略时使用主机文件。文件缺失时跳过；格式错误或其他读取错误会使作业启动失败。文件应在工作区和快照外，Unix 建议权限 `0600`。doctor 报告文件存在状态和 Unix 权限，并按 profile 检查 Codex 登录状态和 Claude CLI 可用性。
- runner 在准入后读取 UTF-8 环境文件。支持空行和整行 `#` 注释，首个等号后的值按字面使用，重复键取最后一项。合并顺序为继承环境、显式环境、所选环境文件、账户目录及 runner 的作业标记。每次新启动 runner 时读取当前文件。子进程只收到本机注入的值，加载事件只记录键名。cx 会过滤自己捕获的输出中的字面凭据值。作业及其工具生成的文件由作业控制，保持凭据远离这些文件。
- 控制端 `--env` 和 `--input-json` 遇到疑似凭据键名只告警，调用仍继续。密钥统一交给执行端本地环境文件或 CLI 登录。

### 作业默认值

| 设置 | 默认值 | 覆盖方式 |
| --- | --- | --- |
| 模型、推理力度 | 引擎配置 | `--model`、`--effort` |
| 沙箱与网络 | `workspace-write`；网络在 `workspace-write` 下默认开启，其他沙箱默认关闭 | `--sandbox`、`--network`、`--no-network` |
| 运行、排队期限 | 均为 0（不限） | `start --timeout`、`--queue-timeout` |
| 空闲、单轮、审批期限 | 30m、6h、2m | `--idle-exit`、`--turn-timeout`、`--approval-timeout` |
| stall、活动命令静默窗口、命令作业输出静默窗口 | 10m、30m、15m | `--stall`、`--cmd-stall`、`--quiet-timeout` |
| app-server 重启预算 | 3 次 | `--max-restarts` |
| 观察期限 | 24h | `CXRUN_MONITOR_TIMEOUT`；`wait --timeout` 优先 |

提示词与文案文件在执行端提交时读取。合并后的首轮提示词及自定义提醒保存在作业快照中，恢复时可直接使用。

### 批量观察

`wait --any` 的每行结果固定含 `project`、`job`、`status`、`outcome`、`completed_turns` 和 `code`。旧扁平作业的 `project` 为 `""`。没有轮次结果时 `outcome` 为 `""`。用 `code` 判断作业结果，它沿用单作业 wait 的码表。若两次观察之间完成多轮，返回最新快照及累计完成数。首次观察某个作业时，如果轮次正在进行、正在排队，或说话消息已进入收件箱等待开轮，本次调用只用后续完成唤醒。空闲作业仍可立即返回最近一次未报告的完成。作业终态、待答问题和 outbox 消息也可独立唤醒。

每个消费者在执行端为同一状态根维护独立的 `--since-file`，并串行调用该文件。文件所在目录由调用方创建。省略文件或使用缺失、空白或 `{}` 文件时，从空基线开始，现有已完成轮次或终态可以立即返回。每次成功输出后，原子更新已报告作业的游标，键为 `P/JOB`（旧扁平作业为 `JOB`），值记录 `completed_turns`、已报告的 `terminal` 及可用的 `run_id`。未匹配及未报告的作业游标保持原样。单独交付问题或 outbox 消息时，被本次等待跳过的旧完成也不会推进完成游标。超时不写文件。进程若在输出和游标提交之间中断，下次可能重复交付。省略文件时每次调用都从空基线开始。

`list --fields brief` 只含 `project`、`job`、`session`、可选的 `profile`、`status`、`turn_id`、`completed_turns`、`plan_step`、`changed_files`、`failed_command`、`last_event_time` 和 `rate_limit`。其中 `plan_step` 是最新计划中首个 `inProgress` 步骤，无进行项时为空。 `changed_files` 来自本轮最新的整轮 diff，重命名算一个文件，无 diff 时为 0。 `failed_command` 为最近一次非零退出的命令，含 `command`（单行摘要）和 `exit_code`，无记录时为 null。 `rate_limit` 含 `rate_limited`，已知时附 Unix 秒 `reset_at`、`primary`、`secondary` 和 `limit_id`。每个已知窗口含 `used_percent` 及可用的 `reset_at`。完整 list 保留这些信息及原有的状态字段。

## Codex 0.171 控制协议

控制端通过作业目录提交操作。写入同目录临时文件，关闭后原子改名为 `inbox/<op-id>.rpc`。操作 ID 使用作业名相同的字符和长度约束，在作业内唯一。CLI 默认随机生成 ID，调用方可用 `--op-id` 固定同一次调用的标识。runner 在每个 stdio 连接内分配出站数字 ID。两种信封示例分别如下。

```json
{"version":1,"kind":"call","method":"thread/loaded/list","params":{}}
```

```json
{"version":1,"kind":"reply","generation":3,"request_id":42,"result":{"decision":"acceptForSession"}}
```

reply 必须携带收到请求时的代次和原始 JSON ID，并在 `result`、`error` 中选一项。服务端定义结果结构和所有决策选项。错误使用 JSON-RPC 的 code、message 和可选 data。runner 处理调用过程中仍可交付 reply。完成后在状态日志追加 `receipt`，随后按信封序号追加 `inbox_ack`。回执含 `op_id`、`generation`、`status` 以及 `result` 或 `error`。状态为 completed、error 或 uncertain。后者表示请求可能已执行。消费方应按原 ID 读取回执并核查历史。CLI 等待回执超时后，执行端仍可能完成操作。

### 提问、审批和输入

`requests --job JOB` 读取当前 pending_requests 列表。每项含 method、generation、原始 id、requested_at、deadline 和 summary。完整参数在 events.jsonl。五类审批支持任意协议决定，approve/deny 保留原有标记契约。工具调用、认证刷新和 attestation 在登记请求后立即返回提供者未配置错误（-32001），已处理请求从列表移除。requestUserInput 和 MCP elicitation 等待回复时显示 waiting_input 和 input_kind=server_request，暂停 stall 升级，继续计算作业总时限和请求期限。

```bash
cxrun reply --job JOB --generation 3 --request 42 --result @decision.json
cxrun reply --job JOB --generation 3 --request 42 --error @error.json
cxrun say --job JOB --request 42 --answers @result.json
cxrun say --job JOB --request 42 "一个问题的文本回答"
cxrun start --job JOB --dir "$DIR" --interactive --input-json @inputs.json
cxrun say --job JOB --image "$IMAGE" "请检查这张图"
```

`--answers` 接受完整的响应对象；文本简写适用于恰有一个问题的 requestUserInput。异步控制端应携带观测到的 `--generation`。JSON 输入使用原始 UserInput 数组，可与文本和执行端本地图片组合。普通 say 沿用有序消息队列、steer 和下一轮槽位分配。

交互作业的已完成轮次若最终消息首行精确等于 `STATUS: QUESTION`，runner 发布 waiting_input 和 input_kind=next_turn 并释放槽位。此时 say 启动下一轮。wait 和不带游标的 wait --any 在作业目录原子领取问题，首个观察者收到退出码 6。带 --since-file 的消费者按各自游标领取。等待下一轮回答期间仍有作业总时限。

### 客户端命令

以下命令均支持执行端的 --state 和 --project。参数文件路径在执行端解析。thin RPC 命令将回执 JSON 打到标准输出。

| 调用 | 执行语义 |
|---|---|
| `rpc METHOD --job JOB --params @json` | 发送原始方法与 JSON 参数，包括实验方法。 |
| `fork SOURCE --job NEW --through TURN` | 从源作业读取线程，前台运行新作业，使用 thread/fork 的 lastTurnId（含该轮）。省略 through 即从当前历史分叉。新作业的 initial-fork 回执用于断线恢复。 |
| `review --job JOB --target @target.json --delivery inline\|detached` | target 为 ReviewTarget 对象。默认 inline。0.171 仅支持 inline 线协议，detached 通过一条 cxrun/reviewDetached 信封创建独立线程并在其上启动 review。状态保留 review_thread_id 和 review_turn_id，可用 history 查询。 |
| `undo --job JOB --before TURN` | thread/revert 的 beforeTurnId，随后从 thread/turns/list 同步最近轮次及对应的报告引用。 |
| `compact --job JOB` | thread/compact/start 回执表示受理。compact_op 和 compact_after_seq 标记最近一次受理及发送前事件位置。compact_event_seq 大于该位置表示之后观察到本线程的压缩完成通知；上游通知只标识线程和轮次。 |
| `settings --job JOB --scope turn\|thread --model M --effort E` | 发送 turn/settings/update 或 thread/settings/update。model、effort 可只给其中一项。启动配置提供初值，后续轮次沿用服务端设置。 |
| `mcp list\|reload\|login\|resource\|call --job JOB --params @json` | 依次对应 mcpServerStatus/list、config/mcpServer/reload、mcpServer/oauth/login、mcpServer/resource/read、mcpServer/tool/call。 |
| `limits\|usage\|models --job JOB [--params @json]` | account/rateLimits/read、account/usage/read、model/list。 |
| `threads [loaded] --job JOB [--params @json]` | thread/list 或 thread/loaded/list。 |
| `history [read\|turns\|items] --job JOB [--thread ID] [--turn ID] [--params @json]` | 默认 thread/read，也可读取 thread/turns/list 或 thread/items/list。分页 cursor 和返回值原样透传。 |
| `events --job JOB [--since SEQ] [--method M] [--thread ID] [--turn ID]` | 读取事件日志，输出 seq 大于游标且符合所有筛选条件的完整 JSON 行。 |
| `plan\|diff --job JOB [--thread ID] [--turn ID] [--since SEQ]` | 默认当前线程。输出最近一条完整更新的 observed 和 event；空计划或空 diff 仍为 observed=true。 |

本地协议冒烟可在 cxrun 目录运行 `timeout 30 env CXRUN_REAL_SMOKE=1 go test -count=1 -timeout 25s -run '^TestLocalHandshake$'`。测试在临时目录启动独立 app-server，握手后读取已加载线程列表。

## 命令作业

在执行主机上提交命令时，所有 cxrun 参数放在 `--` 前。分隔符后的每项都是一个独立参数，保留空格、空参数和 shell 元字符。默认工作目录为提交时的当前目录，命令使用执行账户的本机权限和环境。

```bash
cxrun start --cmd --job tools/unit --dir "$DIR" --session ci \
  --slot-class test --timeout 30m --quiet-timeout 5m -- go test -race ./...
cxrun wait --job tools/unit --timeout 35m
cxrun log --job tools/unit 1024
cxrun close --job tools/unit
```

需要管道、重定向等 shell 语法时，显式使用 `--cmd-shell -- '一段脚本'`，仅传一个脚本参数。Unix 使用 `/bin/sh -c`，Windows 使用 `cmd.exe /d /s /c`。直接执行模式不做字符串拼接。

- 提交成功返回登记状态。命令 runner 独立运行，Unix 使用新会话、忽略 SIGHUP，并将标准流与提交端分离；Windows 从登录会话的 psmux 启动 cxrun，子进程留在该控制台。命令另有自己的进程组。用 `close` 取消已排队或运行中的命令。
- 命令默认槽位类别为 `test`，代理默认为 `agent`。所有引擎共用主机配置、负载检查、project、session、去重和 clean。
- `--quiet-timeout` 从进程启动或最近一次输出计算，默认 15m，0 关闭。运行中命中期限或收到 close 时，close 按下文的进程回收规则处理；超时只停止前台负载。清理时间可能延后终态发布。
- 状态的 `engine` 为 `cmd`，运行时有 `command_pid`，进程退出后有 `exit_code`。Windows 以无符号 32 位值记录；Unix 因信号退出时记为 -1。尚未启动的作业没有进程退出码。`progress.log` 记录生命周期；`output.log` 按原始字节合并 stdout/stderr。`tail` 跟随输出，`log OFFSET` 从字节偏移读取，包括没有换行的末尾，终态读尽返回 10。轮次报告保存最后 64 KiB，较短输出完整保留。
- `wait` 成功返回 0；命令失败或取消返回 1，已取得的退出码以 `exit_code=N` 打印。无输出超时返回 3，输出含 `status=stalled`。总时长超时返回 4，输出含 `status=timeout`；仅本次观察超时返回 124，输出含 `wait_timeout=true`。命令为一次性执行，重跑请使用新 JOB。

### 查看和回收后台进程

`cxrun ps --project tools --job review` 输出一个 JSON 对象。`processes` 中每项含 `pid`、`ppid`、`start_time`、`command_line`、`run_marked` 和 `in_runner_tree`。后两项分别表示是否读到本次 run 标记，以及能否沿父链追溯到持有该作业的 runner。启动时间保留内核精度，Linux 使用自开机以来的时钟滴答，macOS 使用 Unix 纳秒，Windows 使用 FILETIME。读取受限时，查看 `unreadable` 计数、单项的 `metadata_unreadable` 和顶层 `warning`。

登记时生成配置记录中的随机 `run_id`，恢复和 stall 重启沿用该值。两种引擎都向负载注入 `CXRUN_RUN` 和 `CXRUN_JOB=<project>/<job>`，旧扁平命名空间使用 `/job`。Unix 合并进程树和完整环境项匹配，因此 `setsid`、`nohup` 和 shell 的 `&` 不会清除标记。Linux 读取 procfs；macOS 使用 sysctl，受限时尝试枚举当前 UID。macOS 会隐藏部分系统程序的环境。进程在首次观察前已脱离父链且标记不可读或已清除时，Unix 扫描无法归属它。Windows 在放行子进程之前加入命名 Job Object，`ps` 同时识别对象成员和带标记的进程。最后一个 Job Object 句柄关闭后，存活的后台进程仍可通过 `CXRUN_RUN` 标记保留缓存。

自然结束或重启后，后台进程可继续运行。用 `ps` 检查，再用 `close` 回收；`clean` 在删除终态作业目录之前也执行回收。Unix 先向仍带本作业 `CXRUN_RUN` 的 PID 发 SIGTERM，Windows 向匹配成员的顶层窗口请求关闭。10 秒后对仍存活的匹配进程强杀，每次发送前核对启动时间。Linux 支持时用 pidfd，Windows 使用进程句柄。progress.log 记录回收 PID、阶段和系统调用结果。旧作业恢复时补建标记；旧负载缺少标记时不参与回收。

## 本机使用

从 Go 模块目录构建控制程序。以下示例使用已登录的 Codex CLI，`$DIR` 指向要处理的工作目录，`$FILE` 指向提示词文件。在单独的终端运行观察命令。

```sh
go build -trimpath -o cx .
./cx run local "$DIR" review "$FILE" --sync
./cx ls --json
./cx wait "$project/review" --timeout 5m
./cx close "$project/review"
```

节点和控制端的单作业 `wait` 在轮次结束或 runner 结束时返回。调用时若有进行中或排队的轮次，或者 `say` 已投递但新轮尚未开始，等待该轮或之后的轮次完成。调用时若作业已经空闲，立即返回最近一次完成。轮次失败时 `wait` 和 `wait --any` 均返回 1，中断、runner 失败和观察超时也返回非零退出码。app-server 的不可重试错误或失败轮次在 `last_completed_status` 和 `turn_status` 中均记为 `failed`。可重试的尝试错误仍可由该轮后续成功完成。模型正常提交 `STATUS: BLOCKED` 也属于已完成的协议轮次。自动化应再通过 `cx result` 读取报告首行判断任务交付结果。

`wait JOB`、`wait --any` 和 `agent` 观察到准入阻塞时退出 13，stdout 输出含 `status: "queued"` 和 `queue_reason` 的 JSON 对象。低于磁盘水位、无法探测磁盘空间或负载检查未通过时，节点即时发布原因。相同 FIFO 或名额等待持续两秒才发布原因。每次出现新原因、或原因解除后再次出现，均可通知一次。单作业等待和 `agent` 共享认领，带游标的 `wait --any` 按游标单独去重。作业继续排队，后续等待继续观察。

`interrupt` 中断当前轮，runner 继续接收消息。`close` 优先于尚未发送的普通消息；它中断活动轮，等待结束通知，然后关闭 stdio 并回收子进程。需要让当前轮自然结束时，先 `wait` 再 `close`。空闲退出由 `--idle-exit` 控制，默认 30 分钟。`--turn-timeout` 默认 6 小时。每个连接由单一写协程按队列发送，写入超过 10 秒即废弃连接并按预算恢复。迟到的已超时请求应答记入 progress.log。单次 RPC 等待 60 秒，存在待审批请求时最多额外等待一个审批期限。

### 强制退出后恢复

用同一执行主机、同一状态根、同一 Codex 登录环境及 `CODEX_HOME` 在 tmux/psmux 里启动以下前台节点命令，由它管理子进程的寿命。

```text
cxrun start --job JOB --dir DIR --model M --effort E --sandbox workspace-write --prompt-file FILE --state STATE
cxrun resume --job JOB --state STATE
```

登记方先持有 `owner.lock`，再在新段发布配置和初始状态。崩溃会释放锁，重复 `resume` 只允许一个写入者。status、wait 和 list 在登记锁内检查 owner.lock。若锁空闲且没有 final，则新开段追加并刷盘 orphaned 终态。命令 worker 在取锁前崩溃也适用。恢复时，`thread/resume` 用原线程 ID 读取 Codex 历史，重建最后一轮状态。若旧轮被中断，Claude 用 `say` 明确交代接下来要做的事；历史上下文仍属于原线程。

## 文件契约

状态目录应放在执行主机的私有本地文件系统，并置于任务仓库之外。这里包含提示词、命令、模型输出和原始事件。Unix 下新建目录权限为 0700，文件为 0600；Windows 部署需为运行账户配置目录 ACL。

| 作业目录下的路径 | 含义 |
| --- | --- |
| `state/*.jsonl`、`owner.lock` | 持锁方追加配置和完整状态快照，终态记录带 `final:true`。状态包括轮次、请求、用量、限流、完成计数及批量观察字段。 |
| `events.jsonl` | 每行一个 JSON 对象，保留未知协议字段、delta 与原始请求 ID。 |
| `progress.log` | 按制表符分隔 seq、UTC 时间、类型与摘要。seq 为该行起始字节位置加一，跨 resume 递增。 |
| `turns/*.md` | 报告发布后内容固定。协议失败时报告以 `STATUS: FAILED` 开头。结果包提供 `last.md`。 |
| `inbox/*.jsonl` | 依序追加消息、控制标记与 RPC，状态日志记录消费意图、结果和回执。 |
| `outbox/*.jsonl` | 代理向外发送消息和问答。以消息序号为游标。 |
| `server.stderr.log`、`output.log` | 引擎诊断及命令输出。 |

mailbox 的锁在作业目录同级的 `.mail-locks/`。终态与仍未处理的操作结果在同一段发布。读者成功同步链接所在目录后才确认终态。关闭和回收按 run、目录身份和终态游标确认。已认领但进程回收失败的 close 记为 `uncertain`。清理失败时，使用相同作业 ID 重试，可继续删除已移到 `.trash/` 的目录。

MCP 的 `send` 和 `ask` 支持可选的 `operation_id`。在同一个 run 内重连重试时，使用相同 ID 和参数。阻塞问答沿用初次发布的请求号和期限。

整批切换时保留旧状态根用于退役，使用新状态根重新提交，并清除旧控制端的 wait 和 follow 游标。

用 `cx doctor HOST --json` 检查 `state_local`。Windows 节点会给本地作业根设置禁止内容索引属性，并报告 `state_not_content_indexed`。受控 checkout 在自己的状态日志记录准备数据的 blob 引用。

`interrupt`、`close`、`approve` 和 `deny` 按消息 ID 去重。指定 `--cmd-id` / `--turn-id` 或 `CXRUN_CMD_ID` / `CXRUN_TURN_ID` 时，重试沿用原值。活动轮用轮次 ID，空闲、初始排队和命令作业用空串。恢复后若 ack 为 `uncertain`，先核对历史和副作用，再判断是否用新 ID 重发。

`account/rateLimits/updated` 更新账户额度窗口，`thread/tokenUsage/updated` 更新轮次 token 用量。额度耗尽的窗口、协议报告的额度拦截和轮次限流错误均可标记 `rate_limited`，该字段反映最近观测。存在耗尽窗口时，`reset_at` 取可确定的最晚重置时刻，其余情况显示已知窗口的最近重置时刻。限流错误在 `error` 通知或 `turn/completed` 的错误字段出现时，以 `turn/rate_limited` 写入原始事件和进度日志，记录轮次、来源及是否重试。槽位和并发控制继续沿用原规则。

默认始终请求 `approvalPolicy=never`。实际收到命令、文件、权限或旧版审批时，状态切换为 `waiting_approval` 并显示请求列表。请求出现后再提交决策，使用列表中 `id` 的 JSON 表示（数值保持数值，字符串保留双引号）。只有一个待处理请求时可省略 request_id。并发请求需指定 ID。默认 2 分钟后拒绝并继续，可用 `--approval-timeout` 调整。批准权限请求只返回本次请求的权限，作用域为当前轮。

```bash
# TURN 和 CMD_ID 由调用方固定，同一次投递重试沿用
cxrun deny --job review-01 --turn-id "$TURN" --cmd-id "$CMD_ID" 42
```

## 验证与版本边界

```bash
(cd cxrun && CLAUDE_CODE_HOST_SESSION_ID=local_x timeout 30 go test -race -count=1 -timeout 25s -run TestFirstInstallProbes .)
(cd cxrun && timeout 60 go vet ./...)
(cd cxrun && timeout 120 staticcheck ./...)
(cd cxrun && timeout 10 gofmt -l .)
(cd cxrun && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 timeout 90 go build -trimpath -o /tmp/cxrun-linux .)
(cd cxrun && CGO_ENABLED=0 GOOS=windows GOARCH=amd64 timeout 90 go build -trimpath -o /tmp/cxrun.exe .)
```

`runner_test.go` 以独立 stdio 子进程模拟服务端，走正式入口验证登记和恢复互斥、运行中插话、空闲开新轮、批准/拒绝/超时、命令退出码、三文件落盘、diff、最后消息、中断与空闲退出。

app-server 是实验接口。升级时先在临时目录运行 `timeout 30 codex app-server generate-json-schema --out DIR`，核对初始化、线程恢复与分页历史、turn、item、原始请求 ID 和各类审批返回值，再跑协议测试和真实模型冒烟。Linux 和 Windows 本次覆盖交叉编译，真实文件系统及终端管理器的行为需在对应主机验收。


### 作业消息工具

控制端为**每个作业**生成以下 Codex `config.toml` 条目。将示例中的可执行文件和状态根替换为执行端绝对路径，P、J 替换为本作业的项目和作业名。配置的有效范围应限于该作业，避免并行作业共用收件地址。

```toml
[mcp_servers.cxrun]
command = "/absolute/path/to/cxrun"
args = ["mcp-tools", "--state", "/absolute/path/to/state", "--project", "P", "--job", "J"]
tool_timeout_sec = 150
```

- `ask(question, options?, blocking=false, timeout?)` 默认发给 parent 并立即返回投递记录。blocking 模式的 timeout 单位为秒，默认 120 秒。客户端的 tool_timeout_sec 应大于期望等待时间。挂起时状态为 `waiting_input`、`input_kind=agent_ask`，wait 返回 6。问题文本也进入 progress.log。管理端用 `cxrun say --state S --project P --job J --request ask-3 "答复内容"` 答复，工具结果的 JSON 中含 answer。超时返回工具错误，runner 清除本次等待。
- `send(to, text)` 记录目的地（parent、project/job 或 #channel）和正文。控制端负责转发至目标作业的 say 或频道日志。
- `inbox()` 返回本作业收到的消息，键为投递文件名，值为原始正文。它也包含已交给轮次的历史消息，重复读取可按键去重。
- `cxrun outbox --state S --project P --job J --since 2` 输出 seq 大于 2 的记录，每行一个 JSON。序号从 1 开始，在本作业内递增。`outbox/*.jsonl` 存放追加发布的记录，字段含 seq、type、time（UTC）、to，send 带 text，ask 带 question、request_id。管理端在处理成功后保存游标。`wait --any --since-file FILE` 在 runner 观察到新消息时也会唤醒，输出 outbox_seq。一次唤醒可以覆盖多条消息。

### 主机信息与笔记指纹

`cxrun facts --state S` 只读探测执行端并输出一个 JSON 对象。容量字段以字节计，load1 为一分钟平均负载，Windows 对应 cpu_usage_percent。macOS 通过空闲与 inactive 页数估计可用内存。version_managers 记录工具目录是否存在，rustup 对应 Cargo 的 bin 目录。engines 按引擎名给出 path、version 和 login_status（logged_in、logged_out 或 null）。探测失败或平台缺少相应指标时字段为 null。

`cxrun notes fingerprint --dir D` 输出含 fingerprint 的 JSON 对象。SHA-256 输入包括目录顶层的版本声明文件、package.json 中的 engines、PATH 中相关可执行文件的路径及 mtime，以及 OS 和 Codex 版本。仅对输入计算指纹，版本选择、安装和笔记更新由代理处理。

### 执行驱动

`start --engine NAME` 选择代理引擎，省略时使用注册表的默认引擎（codex）；登记时总是把解析后的引擎名写入 config 和 status，未写引擎名的作业不能运行。`--cmd` 选择命令驱动。引擎不支持的选项（如 Claude 的 `--approval-timeout`、fork、`workspace-write` 下的 `--no-network`）在登记前报 `... not supported by engine NAME`。准入、收件箱和逐轮结果沿用原文件契约。

Claude Code 需要支持 `--restricted` 的 CLI。只读作业提供 Read、Glob、Grep；工作区写作业额外提供当前目录与 `--writable-root`（`--add-dir`）内的 Edit、Write 以及 Bash；网络开启时提供 WebFetch，`--web-search` 再加 WebSearch；`--effort` 透传为 `--effort`；作业工具通过 `--mcp-config` 注入并允许 `mcp__cxrun`。全权限作业使用 bypassPermissions。Codex 将 thread 配置的 web_search 设为 live。图片输入两种引擎共用同一输入模型（`localImage` 路径或 `image` URL 部分，`--image` 可重复）：Claude 把它们转为 stream-json 的 image 块（按内容识别 png/jpeg/gif/webp 并 base64 编码；`https` URL 直接引用），超出 Claude API 限制（单张 5 MB、单边 8000 像素、每条消息 100 张）时报错，不做压缩。Claude 的用量（`usage.total/last`）与限速（result 的 429/限额文案、rate_limit_event）记入同一组状态字段。Claude 每轮使用 stream-json，后续轮以保存的 session ID 续接，因此执行账户必须可持久化 Claude 会话。插话顺延到下一轮。Claude 不支持 fork、审批、插话和引擎 RPC，相应命令按能力拒绝。

## 单一二进制 cx（B1）

在本模块执行 `go build -o cx .`，Windows 使用 `cx.exe`。用 `cx --help` 列出所有控制命令，用 `cx COMMAND --help`（或 `-h`）查看用法、选项、默认值和退出码。帮助不会加载主机配置或联系执行端。执行端的原有参数放在 `cx node` 后面。同一程序命名为 `cxrun` 或 `cxrun.exe` 时仍接受原有节点命令。Python 和 shell 入口继续可用于回退和后续批次。

主机表位于用户自己的 `~/.config/cx/hosts.toml`。内置逻辑名 `local` 使用本机执行。每个远端使用独立的 `[hosts.<逻辑名>]` 表，`ssh` 填系统 ssh_config 中的别名；可选 `cx` 填安装路径，`state` 填执行端状态根，`via` 填中转机逻辑名。中转机通过自己的 ssh_config 解析下一跳。未指定安装路径时使用远端用户目录下的 `.local/bin/cx`（Windows 带 .exe）。`platform` 由 setup 或首次远端 exec 探测并写入，格式为 GOOS/GOARCH。凭据由系统 OpenSSH 管理。

`cx setup HOST` 根据控制程序内容哈希选择 `~/.cache/cx/bin/<hash>/<goos>-<goarch>/` 内的产物。已匹配的节点直接复用。同平台还可复用当前程序，跨平台先查本机缓存。中心编译成功后，其产物摘要会按控制程序哈希和目标平台保存，供后续核对远端版本。需要重新编译时才查找模块源码，使用 `CGO_ENABLED=0`，可用 `CX_MODULE` 指定源码目录。也可用 `CX_SETUP_BINARY` 指定对应目标的预编译产物。缺少 Go 或模块源码时，配置的下载源取自 `[release]` 的 `url`，支持 `{version}` 和 `{platform}`，校验值取自 `[release.sha256]` 中以 `<goos>-<goarch>` 命名的键。首次探测时，未安装的程序被静默识别。新节点首次安装通过系统 SFTP 投递临时程序，升级通过节点的 gzip 接收器按未压缩字节续传。远端校验 SHA-256 后在目标目录内替换；同内容复用已有文件。已有 host.json 按原样保留。

传输保留系统的主机密钥校验并启用 BatchMode。OpenSSH 默认使用 ControlMaster=auto 和 ControlPersist=10m，控制套接字名为 cm-%C，放在用户私有目录。建连预算为每跳 90 秒，节点确认后改按输出活动计时，同时受操作的总期限约束。Windows 客户端缺少复用支持时使用普通连接，doctor 报告复用状态。

```sh
cx doctor local --json
cx exec local --tag check --wait -- git --version
cx status "$host:$project/$job"
cx wait --any --project "$project" --cursor monitor --timeout 5m
cx tail "$host:$project/$job" --raw --cursor monitor
cx show "$host:$project/$job" --turn 1 --json
cx ls --json --watch
cx overview --project "$project" --json
```

exec 默认输出 project/job，带 --wait 时作业号写到 stderr，stdout 是原始命令输出。普通退出的命令保留其退出码，作业自身的时限、等待超时和连接失败沿用节点与控制端的退出码表。单作业可直接使用 HOST:project/job，简写从控制端的用户路由缓存解析。当前项目按 CX_PROJECT、项目成员的 Git common-dir、Git 根目录名顺序确定。指定 --json 可取得完整数据，人读列表超出 --max-bytes 时完整输出的路径写到 stderr。不同消费者应使用各自的 --cursor；wait --any 只提交实际交付的主机游标。

## B2 快照包内 API

```go
func Freeze(ctx context.Context, src string, opts FreezeOptions) (SnapshotID, Manifest, error)
func Publish(ctx context.Context, hub endpoint, sid SnapshotID, progress func(int64)) error
func Materialize(ctx context.Context, job string, manifest Manifest, target string) (PreparedSnapshot, error)
func Collect(ctx context.Context, job string) (SnapshotResult, error)
```

`FreezeOptions{History: ""}` 使用合成基线，`HEAD` 和 `A..HEAD` 选择真实历史。对象库根目录由 `CX_SNAPSHOT_STORE` 指定，默认在节点 state 下的 snapshot 目录。SnapshotID 是清单原始字节的 SHA-256。target 是执行端对象库根目录，传空值使用默认。job 是本地作业目录。PreparedSnapshot.Source 和 Env 用于启动进程；SnapshotResult.Dir 包含本次独立收集的文件，由调用方负责清理。

B3 已接入节点分派。`nodeCLI` 的 `snap-receive` 参数交给 `ReceiveSnapshot(ctx, args[1:])`，`endpoint.command` 将它列为流式命令（含 forward-stream 中转）。正常发布在同一条连接上读取已收偏移、发送剩余归档、等待导入回执。

冻结和发布都沿用 v1 清单、`repos/<store_id>.git` 和 `refs/snapshots/<oid>`。读取原有对象库时可把 `CX_SNAPSHOT_STORE` 指向其根目录。导入先刷盘 Git 对象、引用和目录，再发布清单。已发布的快照复用前校验 Git 引用及可达对象，缺失时重新导入。控制端解析 Go 模块或工作区时调用本机 `go mod edit`、`go work edit`。上下文目录的所有普通文件会随任务包发送。显式请求 Git 历史时，可达历史对象也会发送。测试逐项对照二进制内嵌的敏感名单与 `snapshot/denylist`。

发布归档留在控制端直到收到导入成功回执；节点按归档 SHA-256 保留已收前缀，按清单 SHA-256 发布。检出使用私有 bare 的 detached worktree。收集补丁使用临时索引和临时对象目录，主工作目录的 `artifacts/` 单独回收。原 Python 路径仍可读写现有清单，保留作迁移回退。


## B3 控制命令

`cx run HOST DIR TAG PROMPTFILE` 冻结输入，记录作业号，再由后台进程发布并登记。用 `--sync` 等待登记，用 `cx show JOB --json` 查看提交阶段、已传字节或失败原因。`cx agent --continue JOB TEXT` 接续同一会话。执行端通过 `Materialize` 准备工作树，按轮调用 `Collect` 封存报告、逐仓补丁与 artifacts。`result` 启动后台回收时返回 12，`result --wait` 等到回收完成。失败轮次的 `result` 同步读取报告，输出后返回 1。传输断开后按本地已落盘字节继续下载。

`cx result JOB` 输出摘要时，在 stderr 提示 `last.md` 的绝对路径。加 `--full` 输出完整报告。回收中退出 12 时，`--json` 在 stdout 输出包含 `job`、`status: "collecting"`、`turn`、`bytes` 和 `total` 的 JSON 对象，未知总大小为 `null`。同一退出码下，`--log` 的 stdout 为空，回收进度在 stderr。

### 多轮补丁与应用

`cx patch JOB [--turn N]` 和 `cx apply JOB [--turn N]` 默认使用相对派发快照的累计补丁。已应用并提交第 1 轮时，可用 `cx patch JOB --turn 2 --incremental` 检查第 2 轮相对第 1 轮的变化，再用 `cx apply JOB --since-turn 2` 应用第 2 轮至最新完成轮次的变化。三方合并以第 1 轮封存的树为基线。`--incremental` 选第 1 轮时，输出与第 1 轮累计补丁相同。`--since-turn` 与 `--turn` 互斥。

Go 节点封存已完成的快照轮次，后续轮开始前确保上一轮已封存。控制端用派发快照和两端的累计补丁还原轮次树。增量操作使用派发控制端保存的快照。旧作业缺少前置封存时会指出所缺轮次。控制端三方合并需要 Git 2.38 或更新版本。

| 退出码 | `cx apply` 的含义 |
| --- | --- |
| 0 | 应用成功或补丁为空。提交由调用方决定。 |
| 1 | 应用失败或存在冲突。默认任何冲突都会阻止所有选中仓库的写入。`--allow-conflicts` 显式允许写入合并结果及文本冲突标记，冲突文件保留未合并索引条目，仍退出 1。 |
| 9 | 传输失败。 |
| 10 | 已写入，但读取 Git 状态失败。 |
| 44 | 结果不可用，包括增量所需的旧轮次未封存。 |

发生冲突时会列出文件，提示按已应用的轮次使用 `--since-turn`，或用 `cx rebase JOB TEXT` 以新快照重新派发。

`cx tail JOB --messages` 在一次执行端调用内跟随 `events.jsonl`，通过 gzip 流返回完整的 `agentMessage`，每条消息以 `--- turn N, <time>` 开头。旧事件缺少时间戳时显示 `unknown`。`--cursor NAME` 独立保存消息流的字节偏移，重连后续读。`--since N` 跳过序号不大于 N 的事件，`--from-start` 从头重放。消息模式要求控制端和执行端都支持 `--messages`。

`cx close` 先回收作业进程并封存可收集的最新结果。私有检出副本在最后一个使用它的作业关闭后，通过 `git worktree remove` 和 `git worktree prune` 回收。报告、补丁和 artifacts 仍可读取，继续在已回收的快照上工作需要重新提交。终态作业也可直接用 `cx clean` 回收检出副本和私有作业数据。显式指定的工作目录由调用方管理。

任务包与快照发布共用带偏移确认的 gzip 流。环境变量以秒为单位。`CX_TRANSFER_IDLE` 和 `CX_PUBLISH_IDLE` 默认 120 秒，有 I/O 进度就延续期限。`CX_PUBLISH_RETRY_WINDOW` 默认 1800 秒，设为 0 可禁用连接故障重试。发布方按目标和快照保留待确认归档，接收方按内容摘要保留已收前缀。

配置了 Linux 中心的拓扑中，setup 与远端提交优先将模块源码快照发布到中心，在那里按目标平台编译和缓存二进制，再沿已配置的路由安装。新中心首次可用源码归档引导安装。中心缺少 Go 时，升级使用旧二进制作为 rsync 增量传输的基础。`CX_SETUP_BINARY` 可指定预构建产物。

`patch` 默认显示统计，`--full` 显示正文，`--repo` 选择仓库。`apply` 对所有选中仓库完成预检后才安装补丁，在项目输出目录记录 `apply-log.jsonl`。项目和聊天沿用项目 TOML 与 Git 日志契约。`test` 调用 CX_TEST_COMMAND 指定的可执行文件，`gate` 调用项目的 scripts/gate.sh。默认提交到 local，可用 CX_DEFAULT_HOST 覆盖。CX_POLICY_DIR 可指定前言和提醒模板目录。Windows 从 PATH 查找 psmux，套接字默认 cx，可用 CXRUN_TMUX_SOCKET 选择已有套接字。旧部署可在 hosts.toml 显式填写原 state 和 cx 路径继续使用，例如将 state 指向原 ~/.cxrun 的实际绝对路径。读取旧 Python 路由时用 CX_HOME 指定旧部署目录。新作业按 CX_SESSION、CLAUDE_CODE_HOST_SESSION_ID、CLAUDE_CODE_SESSION_ID、CODEX_THREAD_ID 的顺序识别提交会话。`ls`、`overview` 和 `wait --any` 默认选取当前会话，`--all` 显示所有会话。B3 从 `cx` 入口使用，旧 Python 和 shell 入口保留作回退。全部批次验收并经维护者确认后再清理旧入口。

开发副本的端到端回归使用临时状态目录、代理替身和本地 SSH 替身。在模块目录执行：

```sh
timeout 30 go test -race -count=1 -timeout 25s -run '^(TestB3SnapshotResult|TestB3ResumeResultStream|TestSnapNodeWire)$' .
timeout 30 go test -race -count=1 -timeout 25s -run '^(TestB3LinuxCenterSetup|TestB3CenterWithoutGoUsesDelta|TestB3RetryWindow)$' .
```
