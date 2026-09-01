# Operations 控制面强制规则

本文件适用于 `a02_Data_Collection_Operations` 整棵目录树。上级规则见
[02_Futures_Lakehouse/AGENTS.md](../AGENTS.md)，操作者入口与双命令启动方法见
[README.md](README.md)。若规则冲突，必须先消除冲突，不得选择性执行。

## 正式职责边界

- 本目录只承载已经确认的采集控制面、人工维护入口、不可执行历史快照、运行证据和纯控制面测试；业务转换、API 调用和正式湖提交仍由
  `a01_Data_Collection` 各业务入口负责。
- 根级 `run_daily_update.py` 是唯一日常正式入口。它的默认 manifest 固定为 b01—b04 的 18 个阶段，按既定全局顺序运行；`--groups` 只筛选组，
  `--skip-optional-quality` 只允许跳过 b01/c07。b01/c08 永远不得进入任何 operations batch。
- 日常 CLI 必须显式提供全新的 `--run-root`。不得恢复 `--mode`、sample、测试湖、日期截断、`--start-stage` 或 `--require-monitor` 等草稿控制项。
- 仓单性能窗口和中位耗时上限必须成对提供，只转发给 b02/c03，不得改变其他阶段。
- `b02_Manual_Maintenance` 目前只有 c03 合约日历 `--full --write` 这一项正式人工维护。不得把其他历史脚本自行提升为可执行维护入口。
- `b03_Archived_One_Off_Batches` 只保存用户确认的逐字节历史快照。快照使用 `.snapshot` 扩展名，默认不可执行；完整性只以
  `snapshot_manifest.json` 中的字节数和 SHA-256 为准。

## 人工授权、worker 与 monitor

- 每个预计超过 10 分钟或写正式湖的批次，都必须获得用户在当前交互中对边界清楚的单批授权。这里的代码不构成未来批次、定时任务、常驻服务或自动恢复授权。
- 正式批次必须按 [README.md](README.md) 的双命令方式启动：一个独立、持续可见的 Terminal 运行 `watch_batch.ps1`，另一个 hidden detached 进程运行正式 Python CLI。
- worker 启动前和运行中都以 `monitor.pid` 门禁存活的可见 monitor。monitor 缺失或退出时，worker 必须停止当前递归业务子进程树、停止后续阶段并保留证据。
- 正式人工中断只通过当前 run root 下的 `interrupt.request` 文件提出。worker 在等待 monitor、阶段启动前、运行循环、阶段之间和成功发布前都必须检查；发现请求后递归停树、停止后续、保留 request，并发布 `interrupted/130` 后正常释放锁。关闭 monitor 仍是监控故障，只能得到 `failed/1`，不得冒充人工中断。
- Codex 只允许在启动后做一次有界健康检查，确认 worker、业务 child、心跳和可见 monitor 正常后必须结束回合；不得以 sleep、tail 或持续轮询维持 Agent 回合。
- `watch_batch.ps1 -Once` 是严格只读渲染：不得创建 run root、不得新建或覆盖 `monitor.pid`、不得改写任何现场。它必须兼容缺少新协议字段的 legacy status。

## 固定执行语义

- `background_worker.py` 的 `StageSpec` 必须保持 immutable；`run_batch` 只接受调用者显式构造的不可变业务阶段 tuple。不得在 worker 内引入隐式业务 manifest、阶段跳转或业务重试。
- 每个批次在业务阶段前自动且只执行一次以下三个 preflight，顺序不可更改：

  1. `02_Futures_Lakehouse/verify_runtime.py`
  2. `a02_Data_Collection_Operations/verify_operations_runtime.py`
  3. `a01_Data_Collection/b00_sync_notebook_exports.py --check`

- 正式主机必须启用 Windows `LongPathsEnabled=1`，且仓库必须配置
  `core.longpaths=true`。operations runtime 的超过 260 字符路径 probe 未通过时，preflight 必须在任何业务阶段前终止批次。

- 每个 preflight 和业务阶段都只启动一次子进程。普通非零、配额停止、中断、monitor 消失或状态发布失败均不得重试业务 API、阶段或事务。
- worker 必须在运行中用 psutil 跟踪完整 descendants，并在任何非成功路径递归 terminate/kill；已退出的直接父进程不得导致遗留 child 被漏掉。
- 状态协议固定包含 `protocol_version`、`operation_name`、`mode: "formal"`、`phase` 以及既有运行字段。正常终态只有
  `succeeded`、`failed`、`quota_stopped`、`interrupted`，对应进程退出码固定为 0、1、3、130。
- `status.json` 使用同目录临时文件、flush/fsync、`os.replace` 原子发布；只允许对替换时的 Windows WinError 5/32 做短时有界重试。发布最终失败必须停止 child 并保留全局锁。
- PowerShell monitor 读取 `status.json` 必须用 `FileShare.ReadWrite | FileShare.Delete`，读取后立即释放句柄。

## 全局锁与运行历史

- 全部正式 operations 共享唯一锁目录
  `run_history/.active_formal_run`。只有目录本身的原子 `mkdir` 获取锁，owner 固定写入
  `run_history/.active_formal_run/owner.json`。
- 不得自动判断或清理“陈旧锁”。只有 worker 成功发布正常终态且确认递归 child 树全部停止后，才允许删除自己持有的锁。异常崩溃、状态最终发布失败、owner 不匹配或 child 未停必须保留锁等待人工核查。
- 期望终态发布后若锁释放失败，必须先原子留下 `control_failure.json`（原终态、owner 与释放错误），再把可见 status/failure 降级为 `failed/1`。二次 status 发布失败也不得删除 control failure 或锁。
- 崩溃锁只能在核对 owner、确认 worker/全部 child 已死亡，并检查 status、failure、阶段日志与事务现场后，由用户在当前人工处置中明确决定是否清除；任何实现不得自动判 stale。
- 未取得锁的并发 loser 只能在自己的全新 run root 发布可见的 `failed` 终态和 failure 证据；不得修改、删除或释放 active lock 及其 owner。
- 每个新批次使用 `run_history` 下唯一且全新的目录；禁止覆盖、续跑或复用既有 status。`legacy_imports` 是已迁入的旧现场，只读保留；不得作为新 `--run-root`，也不得移动或改写。
- manifest、status、failure 和阶段日志都是单批证据。失败或配额停止后只报告并等待下一次用户决定，不得自动续跑。

## 验证边界

- `verify_operations_runtime.py` 只能在本目录临时路径测试超过 260 字符且包含中文/空格的 Python 原子替换、PowerShell 共享读取和 PyArrow Parquet round-trip；不得读写业务湖。
- `tests` 只能运行纯控制面、临时脚本、临时路径、只读 manifest 和 runtime probe；禁止调用业务 API或写正式湖。
- 使用项目标准解释器执行：

  ```powershell
  E:\anaconda3\envs\latitude\python.exe -m unittest discover -s E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\tests -v
  E:\anaconda3\envs\latitude\python.exe E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\verify_operations_runtime.py
  ```
