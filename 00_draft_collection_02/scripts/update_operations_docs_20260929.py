"""Synchronize the authorized operations-console migration and its indexes."""
import pathlib
import re

root = pathlib.Path.cwd().resolve()
root_rules = (root / "AGENTS.md").read_text(encoding="utf-8")
index_text = root_rules.split("# 变量命名", 1)[0]
checked = []
for target in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", index_text):
    path = root / target
    if path.is_file():
        content = path.read_text(encoding="utf-8-sig")
        checked.append((target, len(content), bool(re.search(r"operations|worker|monitor|skip-optional-quality", content))))
print("Indexed sources checked:", len(checked))

def replace(relative, before, after):
    path = root / relative
    text = path.read_text(encoding="utf-8")
    if before not in text:
        raise ValueError(f"Expected text missing: {relative}: {before[:80]}")
    path.write_text(text.replace(before, after), encoding="utf-8", newline="\n")

replace("AGENTS.md", "后台 worker 必须同时配有独立、用户可见且不依赖 LLM 的 Terminal 窗口或 pane", "后台 worker 必须同时配有独立、用户可见且不依赖 LLM 的 Terminal 窗口、pane，或由所属工作流规范明确规定的独立总控台窗口")
replace("AGENTS.md", "等待用户再次调用 Codex 后再决定如何继续。", "等待操作者核查后显式决定是否启动新的有界批次。")
replace("AGENTS.md", "默认 worker 中 a01 只执行 b01—b07；`--skip-optional-quality` 只跳过 b07。b08 永远不在任何默认 worker manifest 中。", "operations 总控台的日常快捷选择包含这 18 个阶段，a01 为 b01—b07；允许操作者单独选择任意正式环节并配置其原 CLI 参数。b08 不进入日常快捷选择，必须人工单独勾选并显式设置 `--confirm-full-quality`，提交时另选 `--write`。")
for relative in ("AGENTS.md", "02_Futures_Lakehouse/AGENTS.md", "03_Futures_Database/AGENTS.md"):
    path = root / relative
    text = path.read_text(encoding="utf-8")
    text = text.replace("18 个默认日常阶段的人工启动、detached worker、可见 monitor", "19 个正式环节的人工选择、18 项日常快捷选择、detached worker、可见总控台")
    text = text.replace("正式 worker、monitor 与有界单批运行的操作入口和状态证据说明", "交互总控台、原参数透传、只读参考快照与有界单批运行说明")
    path.write_text(text, encoding="utf-8", newline="\n")

replace("02_Futures_Lakehouse/AGENTS.md", "默认 worker 中 a01 固定执行 b01—b07；`--skip-optional-quality` 只跳过 b07，b08 从所有默认 worker manifest 退出，只能人工显式 `--confirm-full-quality --write`。", "operations 总控台的日常快捷选择中 a01 为 b01—b07；允许单独选择任意正式环节并按原 CLI 配置。b08 不被日常快捷选择选中，只能人工勾选并显式确认 `--confirm-full-quality`，提交时另选 `--write`。")
replace("02_Futures_Lakehouse/AGENTS.md", "同时必须开启独立可见且无 LLM 的 Terminal monitor", "同时必须开启独立可见且无 LLM 的监控窗口；operations 使用 `console.py` 桌面总控台，其他工作流沿用所属规范的 Terminal monitor")
replace("02_Futures_Lakehouse/AGENTS.md", "采集 worker 与 PowerShell monitor 共享", "采集 worker 与总控台或 PowerShell monitor 共享")
replace("03_Futures_Database/AGENTS.md", "18 阶段默认日常 worker 中 a01 固定为 b01—b07，`--skip-optional-quality` 只删除 b07 且不得恢复未选 `--groups`；b08 不得出现在默认、全量或生产 worker manifest 中，只能由操作员显式执行。", "operations 总控台的日常快捷选择为 18 个阶段，其中 a01 为 b01—b07；操作者可以单独选择任意正式环节并配置其原 CLI 参数。b08 不进入日常快捷选择，必须人工勾选并显式设置 `--confirm-full-quality`，提交时另选 `--write`；允许这个经人工确认的显式批次交给 detached worker。总控台不改变各业务入口的写入范围和数据契约。")

readme_path = root / "02_Futures_Lakehouse/README.md"
readme = readme_path.read_text(encoding="utf-8")
tree_start = readme.index("   ├─ run_daily_update.py")
tree_end = readme.index("```", tree_start)
readme = readme[:tree_start] + """   ├─ console.py                         # 唯一交互入口；选择、原参数、监控、历史
   ├─ runtime/                           # worker、原 CLI 读取/调用、Windows I/O 检查
   ├─ tests/                             # 纯控制面测试
   ├─ run_history/                       # 新旧批次现场；原有历史原位保留
   ├─ archived_batches/                  # 已有不可执行历史快照
   └─ referance/                         # 重构前全目录只读 ZIP 与摘要清单
""" + readme[tree_end:]
readme = readme.replace("统一生产 worker 的 18 阶段 manifest 中，a01 固定为 b01—b07，`--skip-optional-quality` 只移除 b07，", "总控台的日常快捷选择包含 18 个阶段，a01 为 b01—b07；操作者可逐项取消选择，")
readme = readme.replace("不进入任何默认 worker，必须显式 `--confirm-full-quality --write`。", "不进入日常快捷选择；总控台可人工勾选并显式确认 `--confirm-full-quality`，提交时另选 `--write`。")
readme_path.write_text(readme, encoding="utf-8", newline="\n")

operations = root / "02_Futures_Lakehouse/operations"
(operations / "README.md").write_text("""# 数据采集总控台

唯一交互入口是 [console.py](console.py)。它以独立桌面窗口提供环节选择、原 CLI 参数表单、命令预览、监控及历史查看。
执行内核在 [runtime/background_worker.py](runtime/background_worker.py)，业务采集仍由 a01—a04 原文件负责。
强制规则见 [AGENTS.md](AGENTS.md)，项目规则见 [湖仓 AGENTS](../AGENTS.md)，业务影响见 [湖仓 README](../README.md)。

## 打开与使用

```powershell
Set-Location -LiteralPath 'E:\\Latitude_Analytics_v2'
& 'E:\\anaconda3\\envs\\latitude\\pythonw.exe' 'E:\\Latitude_Analytics_v2\\02_Futures_Lakehouse\\operations\\console.py'
```

打开窗口只读取源码参数和历史证据，不调用业务 API、不读写正式湖。若需要查看启动错误，可用同环境 `python.exe` 替代 `pythonw.exe`。

1. 点击环节查看参数和原文件说明，点击选择列或按空格勾选。19 个正式环节均可单独选择；多选仍按 a01—a04 和组内文件顺序执行。不会自动追加上游。
2. 逐个配置原文件参数。空白字段沿用原 CLI 默认值，布尔参数由勾选控制；多值参数用英文逗号分隔。参数类型、范围、默认值、必选条件直接读取原 Click 声明；业务源码不被导入或执行。动态回调/动态默认值不能静态读取时明确报错。
3. “选中日常 18 环节”只改变选择，不自动加入 `--write`，也不覆盖已编辑参数。b08 必须单独勾选并确认原参数 `--confirm-full-quality`；提交需另选 `--write`。各文件原有 `--full`、`--force`、配额和仓单性能参数均在其表单内。
4. “预览本批命令”展示逐项命令、有效参数和写入意图。此步只做 CLI 类型解析；成对日期、正式湖范围、仓单参数配对及上游就绪条件由原入口检查。只读业务运行仍可能请求来源 API。
5. 在预览窗口点击“启动本批次”。总控台自动生成新批次目录，后台独立运行；此操作只授权当前显示的批次。参数与源码摘要随 request 固定，运行中编辑表单不改变本批。
6. 在“运行监控”查看三个 preflight、各业务阶段、耗时、心跳、最近输出和日志。任何失败、配额停止或监控失效都会停止后续阶段。阶段数量不代表总耗时百分比。
7. “请求中断当前批次”写入一次 `interrupt.request`。窗口保持可见直到后台停止；点击窗口关闭会询问是否请求中断并在停止后退出。进程被强行关闭或界面心跳超过 12 秒失效则作为监控故障停止，保留现场。

## 目录与设计

根目录保留一个业务操作入口 `console.py` 和两份说明，三个内部运行脚本集中在 `runtime/`。
原 `run_daily_update.py` 改造为总控台，原人工 b03 专用启动器和 PowerShell monitor 已由总控台交互替代。
没有总控台业务参数层；原参数名称和默认值仍归业务文件所有。控制面自动生成批次身份、目录和心跳，维护不可变执行清单。

## 监控与状态

- `request.json`：操作者确认的环节、原始参数、有效参数及业务脚本 SHA-256；不重复定义业务规则。
- `command_manifest.json`：三个 preflight 与实际业务子进程命令。
- `status.json`：原子发布的当前状态，兼容既有字段；新增 `stage_history`、`last_output_at`、`progress_at`，分别记录阶段结果、最近输出和进度更新时间。
- `monitor.pid` / `monitor.json`：可见窗口进程与 UI 刷新心跳；worker 同时核对进程创建时间，防止仅凭复用 PID 判断监控健康。
- `bootstrap.log`、`logs/`、`failure.json`、`control_failure.json`：启动日志、阶段日志与失败证据。
- `interrupt.request`：人工中断证据，重复点击不覆盖。

界面关闭后无需 Codex 回合参与运行。非正常关闭会使监控失效，worker 停止子进程；系统或 worker 崩溃保留锁，等待人工核查。
结束批次耗时固定为 `finished_at - started_at`；旧状态没有阶段明细时显示“旧记录未记录”，不编造完成记录。
`mode: formal` 保留为旧控制协议兼容字段，不表示每个阶段写入正式湖；实际写入意图和目标以 request 中原业务参数为准。
历史查看只读，不能重用旧目录、覆盖旧状态或自动恢复。

## 保留的执行边界

每批自动且仅一次按顺序执行：`a00_01_verify_runtime.py`、`runtime/verify_operations_runtime.py`、`a00_02_sync_notebook_exports.py --check`。
启动后每个业务脚本的源码摘要再次核对；选择或预览后源码改变则停止，要求重新创建批次。
共用 `run_history/.active_formal_run/owner.json` 原子目录锁。成功发布终态且全部 child 停止后才释放锁；禁止自动清理陈旧锁。
终态为 `succeeded/0`、`failed/1`、`quota_stopped/3`、`interrupted/130`。
Windows 状态读取使用 `ReadWrite | Delete` 共享并立即关闭；发布采用同目录临时文件、fsync 和原子替换，仅对 WinError 5/32 短时有界重试。
不创建定时任务，不自动重试业务，不自动续跑，不授权未来批次。

## 只读参考和原有历史

重构前完整目录已保存到 [referance/snapshot_manifest.json](referance/snapshot_manifest.json) 指向的 ZIP。
包含全部 154 个原文件（3,109,798 bytes），包括忽略的历史、缓存和未提交修改；清单保留逐文件 SHA-256、字节数和原修改时间，ZIP 与清单设置 Windows 只读属性。
该 ZIP 仅本地保存，沿用 `.gitignore` 的压缩备份忽略规则；不要提交其中的运行证据。不得修改、执行或覆盖参考快照。
原 `run_history/legacy_imports`、其索引和 `archived_batches` 保持原位与原字节；历史记录中的旧绝对路径属于证据，不重写。

## 验证

```powershell
& 'E:\\anaconda3\\envs\\latitude\\python.exe' -m unittest discover -s 'E:\\Latitude_Analytics_v2\\02_Futures_Lakehouse\\operations\\tests' -v
& 'E:\\anaconda3\\envs\\latitude\\python.exe' 'E:\\Latitude_Analytics_v2\\02_Futures_Lakehouse\\operations\\runtime\\verify_operations_runtime.py'
```

测试只使用控制面临时脚本和路径，不调用业务 API、不写正式湖。界面测试不触发生产批次。
""", encoding="utf-8", newline="\n")

(operations / "AGENTS.md").write_text("""# Operations 控制面强制规则

本文件适用于 operations 整棵目录树。上级规则见 [湖仓 AGENTS](../AGENTS.md) 与 [根规则](../../AGENTS.md)，操作与目录说明见 [README.md](README.md)。
本轮经用户确认从固定日常 CLI 改为任意正式环节的交互选择；下述规则替代旧双命令、仅 b03 维护和禁止 b08 worker 的限制。

## 职责与唯一参数来源

- `console.py` 是唯一人工交互入口；`runtime/` 只负责源码 CLI 读取/调用、后台监督和运行环境检查。采集、转换、Schema、日期/湖路径写入规则和数据提交仍归 a01—a04 原入口。
- 只列出 a01—a04 直接子级、具有同名 Notebook 的 b*.py 正式入口；19 个入口均可人工单独选择，多选按业务编号顺序。不得运行任意外部脚本或自动补选未授权上游。
- 表单直接从原文件的 Click 声明读取名称、类型、默认值、required 与 multiple；只静态解析常量及允许的 Click 类型构造，不导入业务模块。动态声明必须明确拒绝，禁止浏览界面时调用业务 API。
- 总控台不再定义 `--groups`、`--skip-optional-quality`、`--warehouse-*`、业务日期或湖路径转发参数。原始 `--write` 默认值必须保留，不能暗中追加；所有原业务约束继续生效。
- 日常快捷选择仅勾选原 18 阶段，不改变参数；b08 永远不被该快捷选择选中。b08 由操作者单独勾选并显式启用 `--confirm-full-quality`，写入需另选 `--write`；允许由本次明确选择的后台批次执行。
- `--full`、`--force` 以及其他原 CLI 参数均在各自环节表单中选择。不得复制业务数据规则到控制面或默认创建全量维护批次。

## 人工启动、后台执行和可见监控

- 操作者在命令预览中点击“启动本批次”是对显示的单批明确授权；Agent 仍须获得当前交互中的具体批次授权，不能因实现或测试请求启动采集。没有未来批次、计划任务、常驻守护、自动恢复或业务重试授权。
- 每批使用 run_history 下全新唯一目录。request 固定原始参数、有效参数和脚本摘要，worker 核对后构造 immutable StageSpec tuple；子进程执行前再核对源码摘要。运行中表单修改仅影响未来新批次。
- 可见桌面总控台代替原 Terminal monitor，并与 hidden detached worker 分离。UI 每秒刷新状态和 monitor.json；worker 核对 PID、进程创建时间和 12 秒心跳新鲜度。缺失、退出或心跳失效必须递归停止 child，停止后续并保留现场。
- 点击中断只创建当前批次 interrupt.request，不覆盖原请求。worker 在等待监控、阶段前后、运行循环和成功发布前检查，停止后发布 interrupted/130。主动关闭窗口先请求中断并等待后台停止；强行退出或冻结是 failed/1 监控故障。
- Codex 对实际授权的长批次只做一次有界健康检查，随后结束回合；不得 sleep、tail 或轮询维持 Agent。GUI 自身事件循环与状态刷新不依赖 LLM。
- 历史查看严格只读，不覆盖 monitor.pid、不改写 status、不附着接管旧 worker。未知旧字段可降级显示，禁止根据阶段序号编造完成记录。

## 执行与 Windows I/O

- worker 无隐式业务 manifest。每批业务前按顺序各运行一次：a00_01_verify_runtime.py、runtime/verify_operations_runtime.py、a00_02_sync_notebook_exports.py --check。标准解释器为 latitude。
- 保留 LongPathsEnabled=1、core.longpaths=true 和超过 260 字符路径 probe。preflight 不通过则不能执行业务。
- 每个 preflight 和业务阶段仅启动一次；非零、配额、中断、监控或发布故障不得重试 API、阶段或事务。psutil 跟踪全部 descendants，在非成功路径递归 terminate/kill，避免父进程退出后遗漏后代。
- 状态保留 protocol_version、operation_name、mode、phase 与既有字段；mode=formal 仅表示兼容旧控制协议，实际是否提交和目标湖来自原业务参数。终态固定 succeeded/0、failed/1、quota_stopped/3、interrupted/130。
- status 和 monitor 状态使用同目录临时文件、flush/fsync、原子替换；仅对替换 WinError 5/32 短时有界重试，发布最终失败必须停止业务并保留锁。读取采用 FileShare.ReadWrite | FileShare.Delete（数值 7），立即释放句柄，禁止独占轮询读取。
- 新状态记录阶段结果、最近输出及最近进度时间。worker 心跳与业务推进分别呈现，不用阶段数量冒充时间百分比。终态耗时固定使用结束时间。

## 锁与证据

- 全部控制批次（包括只读业务运行）共享 run_history/.active_formal_run，以目录原子 mkdir 获取锁，owner.json 记录拥有者；禁止自动判定或清除 stale 锁。
- 只有成功发布正常终态且确认全部 child 已停止，才释放自己的锁。崩溃、状态发布失败、owner 不匹配或 child 未停必须留锁。清锁需操作者核对 owner、进程、status、failure、日志和事务现场后明确决定。
- 终态后释放锁失败先写 control_failure.json，再降级 failed/1；二次发布失败也不得删锁和证据。并发 loser 只可写自己的新目录失败证据，不得触碰 active owner。
- request、manifest、status、failure、控制故障和日志均为单批证据，不覆盖复用、不自动续跑。legacy_imports 和原历史索引不可改写或迁移。
- referance 是本次用户要求的完整只读参考；ZIP 和 snapshot_manifest.json 已核验并设置只读属性。不得修改、覆盖、删除或运行其中内容；ZIP 沿用 Git 本地备份忽略规则。archived_batches 原六项快照及摘要保持原字节。

## 验证范围

- runtime/verify_operations_runtime.py 仅在 operations 临时路径测试 Windows 原子替换、共享读取和 PyArrow round-trip，不触及业务湖。
- tests 仅运行临时控制面脚本、参数静态解析、UI 控件和临时路径测试；禁止业务 API 和正式湖写入。命令见 README。
- 改变这些规则时，同步检查根目录规范索引、湖仓 AGENTS/README、数据库 AGENTS 与受影响 Notebook 说明；Notebook 文本变化仍需用默认 PythonExporter 导出，不直接改写导出的 .py。
""", encoding="utf-8", newline="\n")

replace("AGENTS.md", "- [00_draft_collection_02](00_draft_collection_02)", "- [02_Futures_Lakehouse/operations/referance/snapshot_manifest.json](02_Futures_Lakehouse/operations/referance/snapshot_manifest.json)：本次 operations 重构前全目录只读 ZIP 的逐文件摘要清单；冻结证据，不是新的运行规范。\n- [00_draft_collection_02](00_draft_collection_02)")
print("Operations documentation synchronized.")
