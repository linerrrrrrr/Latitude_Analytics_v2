"""a03/b04 第 1—4 项：解释、流程图、main 日志；保留业务 AST 和注释。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb')
path = ROOT/RELATIVE
original_bytes = path.read_bytes()
notebook = nbformat.read(path, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id:c for c in notebook.cells}
assert not any('a03-b04-flow-' in c.id for c in notebook.cells)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b04-docs-logs-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse', '03_Futures_Database', 'config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, indent=2), encoding='utf8')
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    destination = snapshot/relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, destination)

descriptions = {
'00': '''# b04 外部指数日表

消费外部市场日历中 `external_index/INDICATOR_ID` 的 required 指标—日期格点，按连续待办段请求 Eastmoney `RPT_INDUSTRY_INDEX`，生产 `fact_external_index_daily`；正式事实复读后回写日历，已退出 required 集合的旧事实另行清退。

| 上下游 | 本环节职责与依赖 |
| --- | --- |
| a01/b01 → a03/b01 | 自然日历与实体有效期形成完整外部市场日历；本入口消费 required 格点，不自行扩展日期宇宙。 |
| `config/futures_lakehouse/external_market_entities.py` | 19 个来源指标 ID、项目代码、中文名、分类和有效期的唯一配置来源。 |
| `config/data_contracts.py` | 外部市场日历与外部指数事实的字段、主键、分区和质量契约。 |
| 事实输出 | 主键为 `index_code/observation_date`，按 `index_category/year/month` 完整叶提交。 |
| 日历回写 | 按 `dataset_name/year/month` 完整叶提交；多个指数分类共用同月日历，须保留此前提交的其他指标状态。 |
| operations 与读取 Demo | 默认人工批次第 15 阶段，在 a03/b03 后执行；数据库 Demo 按分类、年月只读展示事实。 |

本入口与 a03/b02、b03 共用日历，但不读取它们的 raw 或事实。生产边界见湖仓 `AGENTS.md`、`README.md` 与数据库 `AGENTS.md`。''',
'01': '''## 更新集合、清退与写入边界

每次同时计算两个集合：当前 required 格点中尚未由事实和日历共同证明完整的格点进入 API 待办；正式事实中不属于当前 required 范围的格点进入无 API 清退。空事实表使用同一规则自然形成采集范围，不采用最大日期截断，也没有 `--full` 或独立日期水位文件。

| 情形 | 当前行为 |
| --- | --- |
| 正式事实计数与日历完成状态、审计字段一致 | 已完成，跳过。 |
| required 格点尚不完整 | 进入采集；当前没有仅修复陈旧日历的独立无 API 分支。 |
| 旧事实已退出 required 集合 | 不请求 API，从旧事实自身的分类—年月完整叶删除；纯清退不创建 HTTP 会话。 |
| 完整分页未返回某个精确待办日期 | 正式计数确认 0 后写 `empty_confirmed + warning`，不造占位事实。 |
| 不带 `--write` | 计算计划；有采集待办仍请求、归一化和验证，清退只预览，不写事实或日历。 |
| 成对显式日期 | 只缩小本次范围；可只读，写入必须使用不同于正式湖的临时湖。 |

连续段以同指标的上游 required 日期顺序定义；周末不人为拆段，夹在待办之间的已完成 required 日期会拆段。范围响应中的非待办日期仍参与分页和来源验收，但不得覆盖已完成事实。

事实和日历先后独立提交。当前日历失败不会撤销此前成功事实；再次运行依现有完整性判定重新生成待办，不能描述成 b03 的无 API 修复。此前成功分区保留，业务循环不自动重跑失败分区；HTTP 适配器现有 `Retry(total=3)` 可能产生额外请求尝试。''',
'02': '''## 初始化与共享依赖

按项目三个标记文件定位根目录，导入两张权威 Schema、共享实体映射、项目设置以及 HTTP、DataFrame 和 Arrow 库。本单元格不创建 HTTP 会话、不请求来源、不写湖。''',
'04': '''## Schema 契约与有界本地样例

交互内核且不存在 `__file__` 时，按上游日历、输出事实的顺序展示权威 Schema。已经传入 `lake_root`，显式选择样例会执行有界本地读取；不会请求 Eastmoney 或写入数据。普通脚本运行跳过展示。''',
'06': '''## 表身份、分区和来源字段

表名、主键及 Hive 分区从两张 Schema metadata 各读取一次。日历使用来源 `INDICATOR_ID` 定位请求实体，事实用稳定项目 `index_code` 作为业务身份；对应名称、分类和有效期均由共享配置提供。

`EASTMONEY_FIELDS` 只请求 `INDICATOR_ID/INDICATOR_VALUE/REPORT_DATE`。当前每页 500 行、总页数上限 10000；两个 Hive partitioning 采用权威字段类型，状态枚举服务于现有业务校验。''',
'08': '''## 当前 Dataset 精确读取

`open_exact_dataset()` 枚举 Parquet 文件、打开 Dataset，再按权威列序补回 Hive 字段并比较完整 Schema/metadata。当前检查的是 Dataset 汇总 Schema，未逐 fragment 检查；提交后仍从表根打开再过滤当前叶。

这里只描述现存读取实现。物理契约与描述性 metadata 的区分、上游可信语义及重复全表检查的收缩留到第 7—9 项，本轮不改变读取或验收范围。''',
'10': '''## 同指标的连续 required 待办段

`pending_request_ranges()` 按某指标上游 required 日期的顺序切分精确待办集合。已完成 required 日期隔开两个待办段，普通周末不拆段。返回每段首尾日期与精确待办日期集合，并确认各段并集覆盖全部待办。''',
'12': '''## 已完成、采集待办与无 API 清退

`external_index_reconciliation()` 选择本数据集当前 required 格点及可选日期范围，按来源指标—日期计算事实计数。日历完成标记、0/1 计数、成功或确认空状态、质量、批次和时间共同满足时才扣除；其余 required 格点全部进入采集待办。

正式事实中不再属于该 required 集合的格点进入清退计划，沿用旧事实自身的 `index_category/year/month` 定位，不能按新配置重算旧分区。范围模式只清退范围内旧事实。当前不存在独立的日历状态修复集合，不能照搬境外期货 b03 的三类规划。''',
'14': '''## 事实叶分区过滤表达式

`fact_partition_expression()` 按 Schema 分区顺序构造当前分类—年月的 Arrow 过滤条件。当前 staging 与正式表根 Dataset 均用它选取完整事实叶；不在此处决定采集、清退或写入。''',
'16': '''## 日历完成与失败状态生成

事实正式复读并核对 0/1 计数后，`apply_calendar_completion()` 生成 `success + passed` 或 `empty_confirmed + warning`，补齐批次及审计时间。`apply_calendar_failure()` 只更新失败请求段的精确待办格点，保留已有正式事实计数，标为未完成并记录原因。

两者当前都遍历传入日历、完整验收并返回内存结果，尚未持久化。请求段失败时不部分提交当前事实叶；此前成功事实和日历叶保留。不同指数分类回写同月日历时，后续处理使用前面已更新的内存状态。''',
'18': '''## CLI：分区推进、计数核对与批次日志

`main()` 先执行参数和正式湖写入门禁，再读取并对账。将采集与清退计划按事实分类—年月合并处理；只有存在采集待办才创建 HTTP 会话。每个指标按连续 required 待办段分页、归一化，范围内非待办日期不参与覆盖。

写入时合并完整事实叶、提交并正式复读，核对 API 预期与正式 0/1 计数，以及清退格点正式计数为 0。存在采集格点才生成并提交日历；纯清退不回写完成状态。当前仍逐分区重建内存事实，并在全部分区结束后重新读取两张正式表求差。

日志统一使用 `function/phase/status/elapsed_s`，耗时为本次 main 累计值，起止用 `=` 分隔线。保留 operations 可识别前缀；API 成功只表示查询及归一化完成，标记 `persisted=false`。分区完成只在当前事实、必要的日历和计数核对均成功后报告；失败记录阶段、分区、指标和日期段，并保留原异常类型与原因链。

本轮统一入口日志。函数自行报告页级和行级进度、生成与提交日志归位留到第 5—6 项；重复校验、共享事务与执行入口仍保留现有实现。''',
'20': '''## 当前 Notebook 与脚本入口

Notebook 目前用 `args=[]` 和 `standalone_mode=False` 避免读取内核参数；默认不写湖，但存在采集待办时仍请求 Eastmoney。纯清退只读计划不创建 HTTP 会话，直接运行 `.py` 时由 Click 读取终端参数。

当前入口仅判断 `ipykernel` 是否已加载，在内核中导入同名模块也可能触发业务运行。显式 `notebook_args`、`__file__` 保护和末尾手动终端命令格留到第 11 项统一；本轮保留原执行单元格。''',
}
for suffix, source in descriptions.items():
    cells['b03-c04-'+suffix].source = source

splits = {
'b03-c04-09': [
    ('a03-b04-calendar-validation', '# 日历消费者只检查', '## 当前日历业务验收\n\n检查日历主键、状态、年月、原因、0/1 计数、required 关系、完成与审计时间。当前读取、状态生成和提交复读均调用此完整验收；本轮保留全部调用。'),
    ('a03-b04-fact-validation', 'def validate_external_index_frame(', '## 当前事实业务验收\n\n检查事实主键、来源身份、共享指标代码/名称/分类映射、观测日期、年月、有限指数值和审计时间。当前先契约转换，再执行业务检查，按主键排序返回。'),
    ('a03-b04-read-fact', '# 空事实目录是合法的全量起点', '## 可选正式事实读取\n\n目录不存在或没有 Parquet 时返回权威空表；否则精确打开、物化并完整验证事实。启动与写入批末都会调用；本轮不改变读取范围。'),
],
'b03-c04-11': [
    ('a03-b04-session', 'def create_eastmoney_session(', '## HTTP 会话与现有重试策略\n\n`create_eastmoney_session()` 创建带 GET 适配器的会话，沿用 `Retry(total=3)`，覆盖连接、读取及配置的限流/服务端错误。业务分页循环自身不重跑失败范围；页数不是实际 HTTP 尝试数。main 在 finally 中关闭会话。'),
    ('a03-b04-query', 'def query_eastmoney_indicator_range(', '## Eastmoney 完整分页查询\n\n一个指标、一个连续待办段共用请求日期范围。首页冻结 pages/count；逐页检查元数据、页长、累计条数与上限。仅首页合法空响应可返回空集合，后续页意外空或页数/总数漂移不能当成功。HTTP、JSON 与来源错误沿现有规则分类并抛出。\n\n函数返回完整原始记录列表；来源 ID、日期、跨页键和数值由紧随其后的归一化验收。任何部分页都不能推进事实提交。'),
    ('a03-b04-normalize', 'def normalize_external_index_response(', '## 分页响应归一化与精确待办筛选\n\n逐条检查必需字段、请求指标 ID、日期范围、跨页指标—日期唯一性和有限数值；空值、布尔值、非法数值继续失败。先验证完整范围响应，再仅保留精确待办日期，避免覆盖已完整事实。映射项目代码/名称/分类并形成权威事实，最后完整验收返回；空响应不生成占位行。'),
],
'b03-c04-13': [
    ('a03-b04-merge', '# 触达指标—日期整体替换', '## 完整事实叶合并\n\n从旧事实选择当前分类—年月叶，保留未触达格点，删除本次采集和清退触达的旧行，再拼接本批响应。确认空和清退没有新行；全空结果使用权威空表。当前返回前仍完整验收，仅表示内存结果。'),
],
'b03-c04-15': [
    ('a03-b04-fact-commit', '# 每次只提交一个完整指数分类', '## 当前事实叶提交与恢复\n\n完整叶验收并确认分区范围后写 staging 与零行标记，完整复读并逐值比较。正式表缺少根标记时先安装标记，再备份当前旧叶；非空安装新叶，空结果不安装以表达删除。正式复读与待提交完整内容一致后返回，日历尚未回写。\n\n当前使用手写恢复，处理当前事实叶和本次新增标记；此前成功叶保留。恢复完整清理 staging、备份和隔离目录，恢复不完整保留现场。共享事务接入在后续第 10 项完成，本图只表达现有顺序。'),
],
'b03-c04-17': [
    ('a03-b04-calendar-commit', '# 日历按 dataset—年—月完整提交', '## 当前日历完整叶提交与恢复\n\n精确触达格点须与日历主键一一对应，按 dataset—年月选择完整叶，保留同月其他指标及此前分类的已提交状态。逐叶写 staging、完整验收并逐值比较，再备份、安装和正式逐值复读。\n\n当前手写恢复只处理正在提交的日历叶，此前成功事实及日历叶保留；已有日历根标记不替换。所有触达叶完成后返回行数，不写独立日期水位。日历失败不撤销已成功事实，也不保证下一次运行免 API。'),
],
}
extras = {}
for cell_id, entries in splits.items():
    source = cells[cell_id].source
    offsets = [source.index(marker) for _, marker, _ in entries]
    cells[cell_id].source = source[:offsets[0]].rstrip()
    extras[cell_id] = []
    for index, (new_id, _, description) in enumerate(entries):
        end = offsets[index+1] if index+1<len(offsets) else len(source)
        extras[cell_id] += [nbformat.v4.new_markdown_cell(description, id=new_id+'-text'),
                           nbformat.v4.new_code_cell(source[offsets[index]:end].strip(), id=new_id)]

cells['b03-c04-03'].source = cells['b03-c04-03'].source.replace('import sys\n', 'import sys\nimport time\n')
main = cells['b03-c04-19'].source
tree = ast.parse(main)
function = next(n for n in tree.body if isinstance(n, ast.FunctionDef))
events = sorted([n for n in ast.walk(function) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and ast.unparse(n.value.func)=='click.echo'], key=lambda n:n.lineno)
assert len(events)==8
messages = [
    'planning_progress: table={TABLE_NAME}; mode={mode}; lake_root={resolved_lake_root}; write={str(write).lower()}; function=main; dataset={DATASET_NAME}; phase=read; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'reconciliation_plan: complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; obsolete_fact_grid_count={len(obsolete_df)}; function=main; dataset={DATASET_NAME}; phase=plan; status=completed; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'planning_progress: outcome=up_to_date; pending_grid_count=0; obsolete_fact_grid_count=0; function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}',
    'planning_progress: reconciliation_partitions={len(partition_groups)}; function=main; dataset={DATASET_NAME}; phase=partition_plan; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'partition_start: {group_number}/{len(partition_groups)}; key={partition_key}; partition_number={group_number}; total_partitions={len(partition_groups)}; fetch_grids={len(pending_group_df)}; obsolete_grids={len(obsolete_group_df)}; processed_grids={processed_grid_count}; total_grids={len(work_df)}; function=main; dataset={DATASET_NAME}; phase=partition; status=started; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'api_success: indicator={source_indicator_id}; range={range_start}/{range_end}; pending_grids={len(range_pending_dates)}; rows={len(range_df)}; function=main; dataset={DATASET_NAME}; phase=source_result; status=completed; normalized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'partition_committed: key={partition_key}; calendar_rows={calendar_rows}; grids={len(group_df)}; obsolete_removed={len(obsolete_grids)}; processed_grids={processed_grid_count}; total_grids={len(work_df)}; function=main; dataset={DATASET_NAME}; phase=partition; status=completed; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}',
    'finished: grids={processed_grid_count}; rows={total_rows}; obsolete_grids={obsolete_grid_count}; write={str(write).lower()}; function=main; dataset={DATASET_NAME}; phase=run; status=completed; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}',
]
lines = main.splitlines(keepends=True)
for node, message in reversed(list(zip(events, messages, strict=True))):
    lines[node.lineno-1:node.end_lineno] = [' '*node.col_offset+'click.echo(f"'+message.replace('\\n','\n').replace('\n','\\n')+'")\n']
main = ''.join(lines)

def insert_before(prefix, addition):
    global main
    found = [n for n in ast.walk(ast.parse(main)) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
    assert len(found)==1, (prefix,len(found))
    node = found[0]
    lines = main.splitlines(keepends=True)
    lines[node.lineno-1:node.lineno-1] = [textwrap.indent(addition+'\n', ' '*node.col_offset)]
    main = ''.join(lines)

for prefix, phase in (
    ('calendar_dataset =', 'read_calendar'), ('fact_df = read_optional_fact', 'read_fact'),
    ('pending_df, obsolete_df, complete_count =', 'plan'), ('session = create_eastmoney_session', 'session'),
    ('request_ranges =', 'request_ranges'), ('response_rows = query_eastmoney', 'fetch'),
    ('range_df = normalize_external', 'normalize'), ('calendar_df = apply_calendar_failure', 'failure_state'),
    ('incoming_df =', 'aggregate'), ('complete_df = full_fact_partition', 'merge'),
    ('committed_partition_df =', 'fact_commit'), ('partition_counts =', 'fact_evidence'),
    ('calendar_df = apply_calendar_completion', 'calendar_state'), ('final_calendar_dataset =', 'final_read_calendar'),
    ('final_fact_df =', 'final_read_fact'), ('remaining_df, remaining_obsolete_df, _ =', 'final_reconcile'),
):
    insert_before(prefix, f'log_phase = "{phase}"')
insert_before('commit_calendar_partitions(calendar_df, failed_grids', 'log_phase = "failure_calendar_commit"')
insert_before('calendar_rows = commit_calendar_partitions', 'log_phase = "calendar_commit"')
insert_before('updated_at = datetime.now', 'log_partition = partition_key\nlog_indicator = "none"\nlog_range = "none"\nlog_phase = "partition"')
insert_before('entity = INDEX_ENTITY_BY_SOURCE_ID', 'log_indicator = source_indicator_id')
insert_before('failed_grids =', 'log_range = f"{range_start}/{range_end}"')
insert_before('message = str(error)', 'log_failed_phase = log_phase')
insert_before('raise click.ClickException(message)', 'log_phase = log_failed_phase')
function = next(n for n in ast.parse(main).body if isinstance(n, ast.FunctionDef))
lines = main.splitlines(keepends=True)
header = ''.join(lines[:function.body[0].lineno-1])
body = ''.join(lines[function.body[0].lineno-1:])
cells['b03-c04-19'].source = header+'''    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_partition = "none"
    log_indicator = "none"
    log_range = "none"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n外部指数日表 / External index daily\\n"
        f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
'''+textwrap.indent(body, '    ')+'''    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; "
            f"indicator={log_indicator}; range={log_range}; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
        raise
'''

def flow(suffix, title, body, overall=False):
    return nbformat.v4.new_markdown_cell(
        ('## 总流程：' if overall else '### 局部流程：')+title+'\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        'flowchart TD\n'+textwrap.dedent(body).strip()+'\n```', id='a03-b04-flow-'+suffix)

graph_specs = {
'b03-c04-03': ('03', '初始化', '''A["按三个标记定位项目根"] --> B["导入 Schema、共享实体、设置与库"]
B --> C["定义依赖；不创建会话、不请求、不写湖"]'''),
'b03-c04-05': ('05', 'Schema 和本地样例', '''A{"交互内核且没有 __file__？"} -->|是| B["展示日历、事实权威 Schema"]
B --> C["用户显式选样例时有界读取本地湖"]
A -->|否| D["跳过展示"]'''),
'b03-c04-07': ('07', '表身份与来源映射', '''A["Schema metadata"] --> B["表名、主键、分区与 Hive 类型"]
C["共享 19 个指数配置"] --> D["来源 ID 到项目身份映射"]
B --> E["固定来源字段、分页上限和状态枚举"]
D --> E'''),
'b03-c04-09': ('09', '当前精确读取', '''A["枚举 Parquet；不存在则报错"] --> B["打开 Dataset；按权威列序重建 Schema"]
B --> C["比较汇总字段、类型、nullable 和完整 metadata"]
C --> D["返回 Dataset；尚未物化数据行"]'''),
'a03-b04-calendar-validation': ('calendar-validation', '当前日历验收', '''A["输入日历；契约转换"] --> B["主键、枚举、原因、年月与指标配置"]
B --> C["required、完成状态与 0/1 计数关系"]
C --> D["批次、完成和质检时间；排序返回"]'''),
'a03-b04-fact-validation': ('fact-validation', '当前事实验收', '''A["输入事实；契约转换"] --> B["主键与共享代码、名称、分类映射"]
B --> C["来源、日期、年月与审计时间"]
C --> D["指数值有限；按主键排序返回"]'''),
'a03-b04-read-fact': ('read-fact', '可选正式事实', '''A{"存在正式 Parquet？"} -->|否| B["返回权威空表"]
A -->|是| C["精确打开；读取完整事实"]
C --> D["完整业务验收；返回事实"]'''),
'b03-c04-11': ('11', '连续 required 待办段', '''A["某指标的精确待办日期"] --> B["读取首尾范围内的上游 required 日期序列"]
B --> C["连续待办合段；遇已完成 required 日期拆段"]
C --> D["各段并集必须覆盖全部待办"]
D --> E["返回日期范围与每段精确集合"]'''),
'a03-b04-session': ('session', 'HTTP 会话', '''A["确有采集待办"] --> B["创建 Session；配置 GET Retry total=3"]
B --> C["挂载适配器与请求头；返回会话"]
C --> D["main 的 finally 关闭会话"]'''),
'a03-b04-query': ('query', '当前完整分页', '''A["指标与日期段；从首页开始"] --> B["GET 当前页；HTTP 与 JSON 检查"]
B --> C{"合法首页空响应？"}
C -->|是| Z["返回空记录列表"]
C -->|否| D["冻结并核对 pages、count 和页长"]
D --> E["累计完整页记录"]
E --> F{"已到最后一页？"}
F -->|否| G["页码递增"]
G --> B
F -->|是| H["核对累计条数；返回原始记录"]
B -. 失败 .-> R["按既有分类抛错；不返回部分成功"]
D -. 失败 .-> R'''),
'a03-b04-normalize': ('normalize', '来源归一化', '''A["完整分页记录"] --> B["字段、请求指标、日期范围和跨页唯一性"]
B --> C["指数值非空、非布尔且有限"]
C --> D["只保留精确待办日期；其余不覆盖"]
D --> E["映射项目身份；构造事实或权威空表"]
E --> F["完整业务验收；返回内存结果"]'''),
'b03-c04-13': ('13', '采集与清退对账', '''A["required 日历格点与正式事实；可选范围"] --> B["逐格点核对状态和正式 0/1 计数"]
B --> C{"共同证明完整？"}
C -->|是| D["已完成跳过"]
C -->|否| E["采集待办；没有独立状态修复分支"]
A --> F["正式事实减去当前 required 集合"]
F --> G["清退计划；保留旧事实原分区坐标"]
D --> H["返回待采、清退和完成数"]
E --> H
G --> H'''),
'a03-b04-merge': ('merge', '完整事实叶合并', '''A["当前分类年月旧事实叶"] --> B["保留未触达格点；删除采集与清退触达旧行"]
B --> C["拼接本批事实；空结果使用权威空表"]
C --> D["完整业务验收；返回内存叶"]'''),
'b03-c04-15': ('15', '事实叶过滤条件', '''A["权威分区列和当前叶坐标"] --> B["构造分类、年、月相等条件"]
B --> C["组合 Arrow 表达式；供现有复读使用"]'''),
'a03-b04-fact-commit': ('fact-commit', '当前事实叶提交', '''A["完整叶验收；确认分区范围"] --> B["写 staging 和零行标记；完整逐值复读"]
B --> C["必要时安装标记；备份当前旧叶"]
C --> D{"新叶非空？"}
D -->|是| E["安装新叶"]
D -->|否| F["不装新叶；表达删除"]
E --> G["从正式表根过滤当前叶；完整逐值验收"]
F --> G
G --> H["清理临时目录；返回正式叶"]
C -. 失败 .-> R["现有手写恢复当前叶与新增标记"]
E -. 失败 .-> R
G -. 失败 .-> R
R --> S["恢复完整则清理；恢复不全保留现场；抛错"]
B -. 失败 .-> T["清理 staging；抛错"]'''),
'b03-c04-17': ('17', '日历状态生成', '''A{"正式 0/1 计数或请求失败？"} -->|正式计数| B["1 记 success；0 记 empty_confirmed 和 warning"]
A -->|失败| C["仅失败段精确格点；保留计数；记未完成"]
B --> D["补齐状态与审计字段；保留其他指标状态"]
C --> D
D --> E["当前完整日历验收；返回内存状态"]'''),
'a03-b04-calendar-commit': ('calendar-commit', '当前日历逐叶提交', '''A["触达格点与日历一一对应"] --> B["选取完整 dataset 年月叶；保留其他指标"]
B --> C["完整验收；staging 写入和逐值复读"]
C --> D["备份旧叶；安装新叶；正式逐值验收"]
D --> E{"还有日历叶？"}
E -->|是| B
E -->|否| F["返回触达行数；无独立水位"]
D -. 失败 .-> R["手写恢复当前叶；此前成功事实与日历叶保留"]
R --> S["恢复完整则清理；恢复不全保留现场；抛错"]'''),
'b03-c04-19': ('19', 'CLI 分区推进', '''A["参数门禁；读取与对账"] --> B{"有采集或清退计划？"}
B -->|否| Z["完整；结束日志"]
B -->|是| C["仅有采集才建会话；按分类年月循环"]
C --> D["当前叶按指标与连续段查询、归一化"]
D --> E{"启用 write？"}
E -->|否| F["汇总内存结果与清退预览"]
E -->|是| G["合并并提交事实叶；正式计数核对"]
G --> H["有采集格点才生成并提交日历"]
H --> I["更新内存事实；报告当前叶完成"]
I --> J{"还有事实叶？"}
F --> J
J -->|是| D
J -->|否| K["finally 关闭会话；write 时执行现有批末复验"]
K --> L["结果和结束日志"]
D -. 请求或转换失败 .-> R["write 时回写失败段；原异常继续传播"]
G -. 失败 .-> S["按现有提交边界恢复；停止本批"]
H -. 失败 .-> S
R --> T["finally 关闭会话；失败日志保留原因链"]
S --> T'''),
'b03-c04-21': ('21', '当前执行入口', '''A{"已加载 ipykernel？"} -->|是| B["args 为空；standalone_mode=False"]
B --> C["默认不写；有采集待办仍请求"]
A -->|否| D{"直接运行脚本？"}
D -->|是| E["Click 读取终端参数"]
D -->|否| F["不执行入口"]'''),
}
overview = flow('overview', 'required 格点采集与旧事实清退', '''A["参数门禁；读取上游日历和已有事实"] --> B["按格点共同判定完成；求采集与清退集合"]
B --> C{"存在本批工作？"}
C -->|否| Z["报告完整；结束"]
C -->|是| D["合并分类年月计划；有采集才建 HTTP 会话"]
D --> E["当前叶：采集指标按 required 连续段完整分页"]
E --> F["来源归一化；仅接纳精确待办日期"]
F --> G{"启用 write？"}
G -->|否| H["内存汇总；清退只预览"]
G -->|是| I["完整叶合并；采集替换、失效清退、未触达保留"]
I --> J["提交并复读事实；核对 0/1 与清退计数"]
J --> K["有采集格点才提交对应日历状态"]
K --> L["报告当前叶完成；此前成功叶保留"]
L --> M{"还有事实叶？"}
H --> M
M -->|是| E
M -->|否| N["关闭会话；write 时正式全表复验"]
N --> Z
E -. 失败 .-> R["按既有边界回写失败或恢复；关闭会话并抛错"]
F -. 失败 .-> R
J -. 失败 .-> R
K -. 失败 .-> R''', overall=True)
new_cells = []
for cell in notebook.cells:
    for item in [cell, *extras.get(cell.id, [])]:
        if item.cell_type=='code':
            suffix, title, body = graph_specs[item.id]
            new_cells.append(flow(suffix, title, body))
        new_cells.append(item)
        if item.id=='b03-c04-01': new_cells.append(overview)
notebook.cells = new_cells
assert before.metadata==notebook.metadata
for old in before.cells:
    assert {k:v for k,v in old.items() if k!='source'}=={k:v for k,v in cells[old.id].items() if k!='source'}
ast.parse('\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code'))
nbformat.validate(notebook)
assert path.read_bytes()==original_bytes
path.write_text(nbformat.writes(notebook)+'\n', encoding='utf8', newline='\n')

# 复用既有隔离导出与本机离线渲染检查入口，仅替换本次目标名称。
for source_name, target_name in (
    ('export_a03_b03_isolated_20260928.py', 'export_a03_b04_isolated_20260928.py'),
    ('render_a03_b03_flowcharts_20260928.cjs', 'render_a03_b04_flowcharts_20260928.cjs'),
):
    source = (ROOT/'00_draft_collection_02/scripts'/source_name).read_text(encoding='utf8')
    source = source.replace('b03_overseas_futures', 'b04_external_index').replace('a03_b03', 'a03_b04')
    (ROOT/'00_draft_collection_02/scripts'/target_name).write_text(source, encoding='utf8', newline='\n')
print(json.dumps({'snapshot':str(snapshot), 'code_cells':len(graph_specs), 'flowcharts':len(graph_specs)+1}, ensure_ascii=False))
