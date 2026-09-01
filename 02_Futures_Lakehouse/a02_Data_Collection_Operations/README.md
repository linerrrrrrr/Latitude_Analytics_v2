# 数据采集 Operations

这里是正式采集的人工控制面，不是业务实现、调度器或自动恢复服务。强制规则见
[AGENTS.md](AGENTS.md)，业务入口与湖仓契约见
[a01_Data_Collection/README.md](../a01_Data_Collection/README.md)。

## 正式入口

日常入口是根级 [run_daily_update.py](run_daily_update.py)。默认按下列固定顺序执行 18 个阶段：

1. `b01/c01_trade_calendar`
2. `b01/c02_futures_variety_calendar`
3. `b01/c03_futures_contract_calendar`
4. `b01/c04_futures_bar_calendar`
5. `b01/c05_futures_daily`
6. `b01/c06_futures_minute`
7. `b01/c07_suspected_session_reconciliation`
8. `b02/c01_exchange_report_calendar`
9. `b02/c01a_position_rank_special_case_calibration`
10. `b02/c02_futures_holding_reports`
11. `b02/c03_warehouse_receipt`
12. `b03/c01_external_market_calendar`
13. `b03/c02_domestic_spot_basis`
14. `b03/c03_overseas_futures`
15. `b03/c04_external_index`
16. `b04/c01_macro_release_calendar`
17. `b04/c02_interest_rate`
18. `b04/c03_macro_release`

`--groups b01 b02 b03 b04` 可选择非空组组合，但仍保持上面的全局顺序。
`--skip-optional-quality` 只跳过 b01/c07。b01/c08 不在任何 operations manifest 中。

恢复 b02/c03 仓单时，可成对增加：

```powershell
--warehouse-performance-window-size 50 --warehouse-performance-max-median-seconds 15.4
```

两个参数必须同时出现，且只传给仓单阶段。日常入口没有 mode、sample、测试湖、日期范围、起始阶段或可选 monitor 开关。

人工全量维护目前只有
[b02_Manual_Maintenance/run_c03_contract_calendar_full_update.py](b02_Manual_Maintenance/run_c03_contract_calendar_full_update.py)，它固定调用 c03 合约日历的 `--full --write`。

## 一个正式批次的双命令启动

每次先取得用户对当前单批的明确授权，并选择 `run_history` 下从未使用过的批次目录。下面的 `$runRoot` 必须在两个 Terminal 中完全相同；示例名称仅用于展示，禁止复用已有目录。

Terminal 1 保持可见，先启动 monitor：

```powershell
$runRoot = 'E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\run_history\daily-YYYYMMDDTHHMMSSZ-unique'
pwsh.exe -NoProfile -ExecutionPolicy Bypass -File 'E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\watch_batch.ps1' -RunRoot $runRoot
```

Terminal 2 用 hidden detached 进程启动 worker；命令返回不表示批次完成：

```powershell
$runRoot = 'E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\run_history\daily-YYYYMMDDTHHMMSSZ-unique'
Start-Process `
    -FilePath 'E:\anaconda3\envs\latitude\python.exe' `
    -ArgumentList @(
        'E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\run_daily_update.py',
        '--run-root',
        $runRoot
    ) `
    -WorkingDirectory 'E:\Latitude_Analytics_v2' `
    -WindowStyle Hidden `
    -PassThru
```

若是唯一允许的 c03 人工全量维护，只把 `-ArgumentList` 中的 Python 脚本换成
`b02_Manual_Maintenance\run_c03_contract_calendar_full_update.py`，仍必须使用新的 run root 和同一个可见 monitor。

启动后可做一次只读、有界健康快照：

```powershell
pwsh.exe -NoProfile -ExecutionPolicy Bypass -File 'E:\Latitude_Analytics_v2\02_Futures_Lakehouse\a02_Data_Collection_Operations\watch_batch.ps1' -RunRoot $runRoot -Once
```

`-Once` 不创建目录，也不新建或覆盖 `monitor.pid`。确认 worker、child、心跳与独立可见 monitor 正常后，Codex 必须结束回合，不持续 tail 或轮询。

需要人工停止当前批次时，保持 monitor 开启，在另一个 Terminal 为同一个 run root 创建一次中断请求：

```powershell
$requestPath = Join-Path $runRoot 'interrupt.request'
if (Test-Path -LiteralPath $requestPath) { throw 'interrupt.request 已存在，禁止覆盖证据。' }
[System.IO.File]::WriteAllText(
    $requestPath,
    ('requested_at=' + [DateTimeOffset]::UtcNow.ToString('o') + "`n"),
    [System.Text.UTF8Encoding]::new($false)
)
```

worker 会递归停止当前 child 树、停止后续阶段、保留 `interrupt.request`，并发布
`interrupted/130` 后正常释放锁。直接关闭 monitor 是监控故障，结果固定为
`failed/1`，不是人工中断；不要用 `Stop-Process` 绕过控制面。

## 自动 preflight 与终止语义

每个正式 batch 在业务阶段前自动运行三项检查：湖仓 Python runtime、operations Windows I/O runtime、Notebook 导出同步检查。三项各有独立日志，任一失败都会阻止业务阶段。

正式 Windows 主机必须设置 `LongPathsEnabled=1`，本仓库必须设置
`core.longpaths=true`。operations runtime 会在超过 260 字符且包含中文/空格的路径上实测控制面 I/O；probe 失败会在任何业务阶段前终止批次。

worker 不做任何业务重试，也不跳阶段或自动恢复。普通失败、quota stop、中断、monitor 消失或状态发布故障都会递归停止当前 child 树，并停止后续阶段。终态和进程退出码为：

| 终态 | 退出码 |
| --- | ---: |
| `succeeded` | 0 |
| `failed` | 1 |
| `quota_stopped` | 3 |
| `interrupted` | 130 |

## 锁、状态和历史证据

所有正式入口共用原子目录锁
`run_history/.active_formal_run/owner.json`。worker 只有在成功发布终态且确认 child 树停止后才释放锁；状态发布最终失败或异常崩溃会故意保留锁，禁止自动删锁或猜测续跑。
未取得锁的并发批次会在自己的 run root 发布可见的 `failed` 终态，但不会改写或释放 active owner；操作者应根据该失败和 owner 现场人工核查。
若期望终态已发布但锁释放失败，run root 会先保存 `control_failure.json`，再把可见 status/failure 降级为 `failed/1`；原终态、owner 和释放错误保留在 control failure 中，锁不删除。

新状态协议保留 `mode: "formal"`，并增加 `protocol_version`、`operation_name` 和
`phase`。`watch_batch.ps1` 同时可只读显示缺少这些字段的旧状态。每个 run root 中的 manifest、status、failure 和 logs 都是不可覆盖的现场证据。

`run_history/legacy_imports` 是旧运行现场的只读导入区，不得用于新批次。
其索引 [legacy_import_index.json](run_history/legacy_import_index.json) 冻结记录 11 个 runs、125 个 files、2,770,744 bytes；历史 JSON 或日志中内嵌的旧绝对路径属于证据原文，不做地址重写。
`b03_Archived_One_Off_Batches` 内的 6 项 `.snapshot` 是不可直接执行的历史字节快照，完整性见
[snapshot_manifest.json](b03_Archived_One_Off_Batches/snapshot_manifest.json)。它们不属于当前正式入口。

本目录不创建计划任务、不运行常驻守护进程、不自动重试、不自动断点恢复，也不授权未来批次。

若崩溃留下 `.active_formal_run`，必须先核对 `owner.json`，确认其中 worker 及其全部 child 已死亡，再检查 status、failure、阶段日志和事务现场。只有用户在当前人工处置中明确决定后才可清除该锁；任何代码或 Agent 都不得自动判定 stale 或自动删锁。
