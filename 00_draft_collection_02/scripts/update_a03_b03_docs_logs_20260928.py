"""a03/b03 第 1—4 项：说明、分块流程图和现有入口日志；不改变业务算法。"""
import ast
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb')
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b03-docs-logs-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse', '03_Futures_Database', 'config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, ensure_ascii=False, indent=2), encoding='utf8')
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    destination = snapshot/relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, destination)
path = ROOT/RELATIVE
original_bytes = path.read_bytes()
notebook = json.loads(original_bytes)
cells = {c['id']:c for c in notebook['cells']}

def source(cell_id):
    return ''.join(cells[cell_id]['source'])

def put(cell_id, value):
    cells[cell_id]['source'] = value.strip('\n').splitlines(keepends=True)

def markdown(cell_id, value):
    return {'cell_type':'markdown', 'id':cell_id, 'metadata':{}, 'source':value.strip('\n').splitlines(keepends=True)}

def code_cell(cell_id, value):
    return {'cell_type':'code', 'id':cell_id, 'metadata':{}, 'execution_count':None, 'outputs':[], 'source':value.strip('\n').splitlines(keepends=True)}

def flow(cell_id, title, body):
    return markdown('a03-b03-flow-'+cell_id, title+'\n\n```mermaid\n%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'+textwrap.dedent(body).strip()+'\n```')

put('b03-c03-00', '''# b03 境外期货日线

从外部市场日历选择 `overseas_futures/ALL` 的 required 日期，逐日查询 JQData `finance.FUT_GLOBAL_DAILY`，生产 `fact_overseas_futures_daily`，并在事实正式复读后回写日历完成状态。

| 上下游 | 本环节使用或产生的内容 |
| --- | --- |
| a01/b01 → a03/b01 | 自然日历和共享实体配置生成外部市场日历；本入口只消费其 required 日期，不自行扩展日期宇宙。 |
| `config/futures_lakehouse/external_market_entities.py` | 境外期货请求实体 `ALL` 的唯一配置来源；一次日期查询返回当日全部来源品种。 |
| `config/jqdata_connection.py` | 共享认证与连接边界；仅存在 API 待办时调用。 |
| `config/data_contracts.py` | 外部市场日历与境外期货事实的字段、类型、主键和分区权威。 |
| 事实输出与日历回写 | 事实按 `year/month` 完整叶提交；日历按 `dataset_name/year/month` 完整叶回写，保留未触达行。 |
| operations 与读取 Demo | 默认人工批次在 a03/b01、b02 后运行本入口；数据库 Demo 按契约只读消费境外期货事实。 |

本入口与 a03/b02 生意社原文归档、a03/b04 外部指数共用日历，但不消费它们的 raw 或事实。运行边界见湖仓 `README.md`、`AGENTS.md` 与数据库 `AGENTS.md`。
''')
put('b03-c03-01', '''## 更新集合、来源质量与写入边界

每次按「上游当前 required 日期 − 事实与日历状态共同证明完整的日期」求差，分成 API 待办与无 API 日历修复。当前不使用最大日期截断，也没有 `--full` 或独立日期水位文件。空事实表由同一差集自然形成待办。

| 情形 | 当前处理 |
| --- | --- |
| 正式事实与日历状态、数量、质量原因一致 | 已完成，跳过请求。 |
| 非空事实已正式提交，日历状态或原因陈旧 | 从事实复算计数和 OHLC 结论，只修复日历，不认证 JQData。 |
| 正式事实为零行，已有确认空审计凭证 | 依日历证据判定完成或修复；零行本身不能证明曾查询成功。 |
| 缺少上述事实或确认空证据 | 每个待办日期查询一次；不自动重试业务查询。 |
| 不带 `--write` | 计算计划；有 API 待办时仍查询、归一化和校验，但不提交事实或状态。 |
| 成对显式日期 | 缩小检查范围；只读可用，写入必须指定非正式临时湖。 |

查询条件为 `day == snapshot_date`，因此 API 事实日必须等于请求日；返回达到 5000 行上限时拒绝提交。成功空响应记为 `empty_confirmed + warning`，不生成占位事实行。

可空数值的 `None`、`pd.NA`、Pandas `NaN` 在来源归一化时转为 Arrow null；非空非有限数、非数值文本、负成交量和负振幅继续失败。有限 OHLC 跨列异常保留来源原值，并将对应日期记为 `success + warning`。

正式湖由 `settings.futures_lake_root` 唯一定位，禁止用显式日期写正式湖。事实提交与日历提交是先后两个边界：日历提交失败不撤销已提交事实，后续运行通过无 API 状态修复补齐。
''')
put('b03-c03-02', '''## 初始化与共享依赖

按项目三个标记文件定位根目录，加载两张权威 Schema、共享实体、JQData 连接和项目设置。本格只定义依赖，不认证、不查询、不写湖。
''')
put('b03-c03-04', '''## Schema 契约与有界本地样例

交互内核且没有 `__file__` 时，按上游日历、输出事实的顺序展示权威字段说明。传入 `lake_root` 后，用户显式选择样例会进行有界本地读取；不会认证 JQData、请求来源或修改数据。普通脚本运行跳过展示。
''')
put('b03-c03-06', '''## 表身份、分区与来源字段

表名、主键和 Hive 分区从两张 Schema metadata 各读取一次。事实主键为 `instrument_code/trading_date`，分区为 `year/month`；日历主键为数据集—实体—观测日期，分区为 `dataset_name/year/month`。

`JQDATA_FIELDS` 固定显式查询字段，`SOURCE` 记录来源身份；`ALL` 表示一次日期查询返回整表，不是逐品种请求名单。状态枚举用于已有业务校验，两个 Hive partitioning 使用各自权威字段类型。
''')
put('b03-c03-08', '''## 当前 Dataset 兼容与精确读取

`open_compatible_dataset()` 检查表根、汇总 Schema 和各 Parquet fragment 的字段、类型、nullable，再返回 metadata 是否精确一致。`open_exact_dataset()` 进一步要求当前 metadata；现用于日历、staging 和正式验收。Hive 分区字段按权威列序补回。

当前事实读取仍将 metadata 不一致交给旧的整表升级分支。这是本轮保留的存量实现；数据库规范已要求纯描述性 metadata 更新不重写历史文件，相关读取和迁移逻辑在后续第 7—9 项一起收缩。本轮流程图表达当前执行路径。
''')
put('b03-c03-10', '''## JQData 单日查询

`query_overseas_futures_grid()` 使用显式字段和 `day == snapshot_date` 查询一次，不分页、不重试。权限或表不存在等异常标为永久错误，其他查询异常及返回 `None` 标为可重试错误；这个分类不意味着本次运行自动重试。

函数返回来源 DataFrame，字段、日期和数值验收由紧随其后的归一化完成。日志中的 API 成功只在查询与归一化均成功后报告，仍不表示事实已提交。
''')
put('b03-c03-12', '''## 已完成、无 API 修复与 API 待办

`plan_overseas_futures_grids()` 只选择 `overseas_futures/ALL` required 日期，应用可选范围后，从正式事实计算逐日期数量和 OHLC warning。`calendar_completion_result()` 统一首次回写、状态修复和完整性判断使用的状态与原因文本。

日历完成标记、事实计数、质量状态与原因、批次和时间全部一致才判为完整。非空正式事实足以进入无 API 状态修复；零事实日期必须已有 `empty_confirmed`、缺失标记、零计数、完成批次与时间，才具有可修复的确认空证据。其余日期进入 API 待办。

三类集合互相区分；不能把「有日历行」「当前 required」「已完成」混为一谈，也不能用固定品种数或最大日期替代当前格点判定。
''')
put('b03-c03-14', '''## 当前 metadata 整根升级分支

`upgrade_fact_metadata()` 是现存的物理兼容表迁移路径：完整事实验收后写 staging 根及零行标记，精确复读，再备份旧根、安装新根并逐值验收。它不调用 API；任一安装或验收失败尝试恢复旧根。

当前完整恢复后清理 staging、备份和隔离目录，恢复失败则保留现场。此处只解释存量行为，不将描述性 metadata 整表重写提升为新规则；后续收缩时需同时处理调用方、旧测试和本段说明。
''')
put('b03-c03-16', '''## 日历完成与失败状态生成

`apply_calendar_completion()` 依据正式事实计数与 warning 生成成功或确认空状态，补齐批次、完成和质检时间。`apply_calendar_failure()` 保留已有正式事实计数，将失败日期设为未完成并记录原因。

这两个函数返回内存日历，尚未写入。当前仍遍历传入日历并在返回前完整验收；提交成功才构成持久完成凭证。请求或转换失败时只回写失败日期，当前尚未提交的月份事实不提交，此前成功月份保留。
''')
put('b03-c03-18', '''## CLI：日期规划、月份推进与运行日志

`main()` 先检查日期参数和正式写入边界，再读取日历与事实、处理存量 metadata 分支、生成三类日期集合。纯状态修复不认证 JQData；有 API 待办才建立连接，并按年月处理日期。

每个月先汇总所有成功响应，再保留未触达事实、整体替换触达日期。写入时先提交事实，复算计数与 warning，核对来源 warning 后生成并提交日历；不写入时只累计本次内存结果。当前仍保留状态修复后的重新规划及批末正式全表复核，后续再收缩重复工作。

日志统一使用 `function/phase/status/elapsed_s`，`elapsed_s` 为本次 main 的累计耗时，起止用 `=` 分隔线。保留 operations 可识别的计划、月份开始、API 结果和提交前缀。内存结果标记 `persisted=false`，月份提交日志只在事实和对应日历提交均返回后发出。失败日志标明阶段、月份和日期，并按原异常类型与异常链抛出。

本轮统一现有入口日志；函数内部自行报告细粒度进度及日志归属在后续第 5—6 项落实。
''')
put('b03-c03-20', '''## 当前 Notebook 与脚本入口

Notebook 当前显式使用 `args=[]`、`standalone_mode=False`，避免读取内核参数；默认不写湖，但有待办时仍认证并查询 JQData。直接运行 `.py` 时由 Click 读取终端参数。

当前入口仅检查 `ipykernel` 是否加载，在内核中导入同名模块也可能触发运行。显式 `notebook_args`、`__file__` 导入保护和末尾手动终端命令格留待第 11 项统一；本轮不修改执行入口。
''')

# 仅沿已有顶层函数边界拆分单元格；原注释随对应函数保留。
splits = {
    'b03-c03-09': [
        ('a03-b03-calendar-validation', '# 日历消费者只检查', '## 当前日历业务验收\n\n检查主键、状态枚举、年月、原因、计数、required 关系与完成审计时间，按主键排序返回。当前在读取、状态生成和提交复读中重复使用；本轮保留全部检查。'),
        ('a03-b03-fact-validation', '# 事实生产者严格校验', '## 当前事实业务验收\n\n检查来源身份、业务主键、请求日与事实日、年月和审计时间。来源归一化之外的事实只接纳真实 null，残留 NaN/Inf 仍失败；成交量与振幅非负。有限 OHLC 关系异常留给独立 warning 计算，不改写价格。'),
        ('a03-b03-quality', '# 有限 OHLC 跨列异常不改变', '## OHLC 来源质量旁证\n\n`ohlc_relation_warning_map()` 按请求日期汇总 high/low/open/close 关系异常，记录代码、名称、来源 ID 和原值。无异常返回空映射；有异常不修改事实，不把日期转为采集失败。'),
        ('a03-b03-read-fact', '# 空事实目录是合法的全量起点', '## 可选事实读取\n\n事实目录不存在或没有 Parquet 时返回权威空表；否则精确打开、物化并完整验证事实。当前批末还会调用此函数重新读取全表，本轮不改变读取范围。'),
    ],
    'b03-c03-11': [
        ('a03-b03-normalize', 'def normalize_overseas_futures_response(', '## 来源响应归一化\n\n空 DataFrame 返回权威空事实。非空响应检查必需字段、5000 行上限、请求日期、来源 ID 和代码，再转换文本与数值。来源可空数值缺失标记归一为 null，非空非法文本和 Inf 失败，最后按事实契约验收。'),
    ],
    'b03-c03-13': [
        ('a03-b03-merge', '# 触达请求日期整体替换', '## 完整月份事实合并\n\n`full_fact_partition()` 选择当前年月旧事实，删除本次触达日期的旧行，再拼接本次全部响应；未触达日期原样保留。确认空日期没有新事实行，因此会清除该日期旧事实；结果为空时返回权威空表。返回前仍执行完整业务验收。'),
    ],
    'b03-c03-15': [
        ('a03-b03-fact-commit', '# 每次只提交一个完整年—月事实叶分区', '## 完整事实叶提交与当前恢复\n\n当前叶验收后写 staging 和零行标记，逐值复读；正式表缺少根标记时先安装标记，再备份旧叶、安装新叶。空叶结果以删除旧事实叶表达。正式复读必须与待提交完整内容一致，成功后才允许生成日历完成状态。\n\n当前仍使用手写回滚，恢复范围是当前事实叶及本次新建标记；此前成功叶保留。完整恢复后清理临时目录，恢复失败保留现场。共享模块留待第 10 项接入。'),
    ],
    'b03-c03-17': [
        ('a03-b03-calendar-commit', '# 日历按 dataset—年—月完整提交', '## 日历完整叶提交与当前恢复\n\n按触达日期确定 `dataset_name/year/month`，选择并校验完整叶后写 staging；同叶未触达行保留。逐叶备份、安装、正式逐值复读，每叶独立成功，此前成功事实和日历叶不会因后续叶失败撤销。\n\n现有实现仍从表根打开 Dataset 并过滤目标叶，重复业务验收与手写恢复均保留在本轮范围外；全部触达叶成功后返回日历行数。根级已有标记不替换。'),
    ],
}
extra_cells = {}
for original_id, entries in splits.items():
    original_source = source(original_id)
    offsets = [original_source.index(marker) for _, marker, _ in entries]
    put(original_id, original_source[:offsets[0]])
    added = []
    for index, (new_id, _, description) in enumerate(entries):
        end = offsets[index+1] if index+1<len(offsets) else len(original_source)
        added += [markdown(new_id+'-text', description), code_cell(new_id, original_source[offsets[index]:end])]
    extra_cells[original_id] = added

put('b03-c03-03', source('b03-c03-03').replace('import sys\n', 'import sys\nimport time\n'))
main_source = source('b03-c03-19')
node = next(n for n in ast.parse(main_source).body if isinstance(n, ast.FunctionDef))
lines = main_source.splitlines(keepends=True)
body = ''.join(lines[node.body[0].lineno-1:])

# 改写现有日志文本；保留监控前缀和既有测试使用的事件摘要。
logs = [
    ('planning_progress: table={TABLE_NAME}; mode={mode}; lake_root={resolved_lake_root}; calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; write={str(write).lower()}; ', 'read', 'completed'),
    ('planning_progress: metadata_upgrade_plan: scope=full_table; rows={len(fact_df)}; api_requests=0; write={str(write).lower()}; ', 'metadata_plan', 'completed'),
    ('planning_progress: metadata_upgraded: rows={len(fact_df)}; full_root_swap=true; api_requests=0; persisted=true; ', 'metadata_commit', 'completed'),
    ('planning_progress: metadata_upgrade_preview_only: api_requests=0; persisted=false; ', 'metadata_plan', 'completed'),
    ('reconciliation_plan: complete_grid_count={complete_count}; state_repair_count={len(state_repair_df)}; api_pending_grid_count={len(pending_df)}; ', 'plan', 'completed'),
    ('planning_progress: outcome=up_to_date; api_pending_grid_count=0; persisted=false; ', 'run', 'completed'),
    ('state_repair_plan: grids={len(repair_dates)}; write={str(write).lower()}; api_requests=0; ', 'repair_plan', 'completed'),
    ('planning_progress: state_repaired: grids={len(repair_dates)}; calendar_rows={repaired_calendar_rows}; api_requests=0; persisted=true; ', 'repair', 'completed'),
    ('planning_progress: state_repair_preview_only: api_requests=0; persisted=false; ', 'run', 'completed'),
    ('planning_progress: outcome=state_repaired; api_requests=0; persisted=true; date_watermark=none; ', 'run', 'completed'),
    ('planning_progress: pending_partitions={len(partition_groups)}; pending_grids={len(pending_df)}; ', 'partition_plan', 'completed'),
    ('partition_start: {group_number}/{len(partition_groups)}; partition_number={group_number}; total_partitions={len(partition_groups)}; key={partition_key}; grids={len(group_df)}; processed_grids={processed_grid_count}; total_grids={len(pending_df)}; ', 'partition', 'started'),
    ('api_success: date={observation_date}; rows={len(grid_df)}; key={partition_key}; persisted=false; ', 'fetch_and_normalize', 'completed'),
    ('planning_progress: api_quality_warning: date={observation_date}; reason={api_quality_warning_by_date[observation_date]}; persisted=false; ', 'source_quality', 'completed'),
    ('partition_committed: key={partition_key}; calendar_rows={calendar_rows}; grids={len(group_df)}; processed_grids={processed_grid_count}; total_grids={len(pending_df)}; persisted=true; ', 'partition', 'completed'),
    ("finished: grids={processed_grid_count}; rows={total_rows}; write={str(write).lower()}; outcome={'written' if write else 'readonly'}; date_watermark=none; ", 'run', 'completed'),
]
echoes = sorted((n for n in ast.walk(ast.parse(textwrap.dedent(body))) if isinstance(n, ast.Call) and ast.unparse(n.func)=='click.echo'), key=lambda n:n.lineno)
assert len(echoes)==len(logs)
body_lines = body.splitlines(keepends=True)
for echo, (content, phase, status) in reversed(list(zip(echoes, logs, strict=True))):
    indent = body_lines[echo.lineno-1][:len(body_lines[echo.lineno-1])-len(body_lines[echo.lineno-1].lstrip())]
    tail = f'function=main; dataset={{DATASET_NAME}}; phase={phase}; status={status}; elapsed_s={{time.perf_counter() - log_started_at:.3f}}'
    if phase=='run': tail += '\\n{log_boundary}'
    replacement = indent+'click.echo(\n'+indent+'    f"'+content+'"\n'+indent+'    f"'+tail+'"\n'+indent+')\n'
    body_lines[echo.lineno-1:echo.end_lineno]=[replacement]
body = ''.join(body_lines)
for marker, phase in (
    ('    calendar_dataset = open_exact_dataset(', 'read_calendar'),
    ('    fact_metadata_upgrade_required = False', 'read_fact'),
    ('            fact_df = upgrade_fact_metadata(', 'metadata_commit'),
    ('    pending_df, state_repair_df, complete_count = plan_overseas_futures_grids(', 'plan'),
    ('            fact_counts = grid_count_map(fact_df)', 'repair_generate'),
    ('            repaired_calendar_rows = commit_calendar_partitions(', 'repair_commit'),
    ('            repaired_calendar_dataset = open_exact_dataset(', 'repair_verify'),
    ('    jqdata = authenticate_jqdata(', 'authenticate'),
    ('                raw_df = query_overseas_futures_grid(', 'fetch'),
    ('                grid_df = normalize_overseas_futures_response(', 'normalize'),
    ('                grid_quality_warning = ohlc_relation_warning_map(', 'source_quality'),
    ('                    calendar_df = apply_calendar_failure(', 'failure_state'),
    ('                    commit_calendar_partitions(', 'failure_calendar_commit'),
    ('        incoming_df = (', 'aggregate'),
    ('        complete_df = full_fact_partition(', 'merge'),
    ('        committed_partition_df = commit_complete_fact_partition(', 'fact_commit'),
    ('        partition_counts = grid_count_map(', 'fact_evidence'),
    ('        calendar_df = apply_calendar_completion(', 'calendar_state'),
    ('        calendar_rows = commit_calendar_partitions(', 'calendar_commit'),
    ('        fact_df = pd.concat([', 'advance_memory'),
    ('        final_calendar_dataset = open_exact_dataset(', 'final_verify'),
):
    assert body.count('\n'+marker)==1, marker
    indent=marker[:len(marker)-len(marker.lstrip())]
    body=body.replace('\n'+marker, '\n'+indent+f'log_phase = "{phase}"\n'+marker)
body=body.replace('        partition_key = tuple(raw_partition_key)', '        partition_key = tuple(raw_partition_key)\n        log_partition = partition_key\n        log_date = "none"')
body=body.replace('        for observation_date in group_df["observation_date"].tolist():', '        for observation_date in group_df["observation_date"].tolist():\n            log_date = observation_date')
body=body.replace('            except Exception as error:\n', '            except Exception as error:\n                log_failed_phase = log_phase\n')
body=body.replace('                raise click.ClickException(message) from error', '                log_phase = log_failed_phase\n                raise click.ClickException(message) from error')
prefix='''    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_partition = "none"
    log_date = "none"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n境外期货日线 / Overseas futures daily\\n"
        f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
'''
suffix='''    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; key={log_partition}; date={log_date}; error={type(log_error).__name__}; "
            f"write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
        raise
'''
put('b03-c03-19', ''.join(lines[:node.body[0].lineno-1])+prefix+textwrap.indent(body, '    ')+suffix)

diagrams = {}
def diagram(code_id, title, body):
    diagrams[code_id] = flow(code_id.replace('b03-c03-', '').replace('a03-b03-', ''), '### 局部流程：'+title, body)

diagram('b03-c03-03','初始化','''flowchart TD
    A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
    B -->|是| C["配置导入路径；加载权威契约和共享连接"]
    B -->|否| D["抛错停止"]
    C --> E["不认证；不查询；不写湖"]''')
diagram('b03-c03-05','契约和样例展示','''flowchart TD
    A{"交互内核且无 __file__？"} -->|否| B["跳过"]
    A -->|是| C["展示日历与事实权威 Schema"]
    C --> D["用户选择时读取有界本地样例"]
    D --> E["不认证 JQData；不回写"]''')
diagram('b03-c03-07','表身份和查询字段','''flowchart TD
    A["两张权威 Schema"] --> B["读取表名、主键、分区；构造 Hive partitioning"]
    B --> C["共享实体 ALL；显式来源字段；状态枚举"]''')
diagram('b03-c03-09','当前契约读取','''flowchart TD
    A["打开表根；重建 Hive 字段"] --> B["核对汇总与各 fragment 物理结构"]
    B --> C{"物理兼容？"}
    C -->|否| D["拒绝读取"]
    C -->|是| E["比较当前 metadata"]
    E --> F{"调用方要求精确？"}
    F -->|否| G["返回 Dataset 与精确标记"]
    F -->|是| H{"metadata 一致？"}
    H -->|是| I["返回 Dataset"]
    H -->|否| D''')
diagram('a03-b03-calendar-validation','日历验收','''flowchart TD
    A["契约转换；检查主键"] --> B["枚举、原因、年月与计数"]
    B --> C["required、完成、缺失与结果状态自洽"]
    C --> D["批次与审计时间有效"]
    D --> E["按主键排序返回"]''')
diagram('a03-b03-fact-validation','事实验收','''flowchart TD
    A["归一化后的事实"] --> B["拒绝残留 NaN 和 Inf；契约转换"]
    B --> C["检查主键、来源 ID、代码、日期和年月"]
    C --> D["可空数值有限；成交量与振幅非负"]
    D --> E["按主键排序返回；不拒绝有限 OHLC 关系异常"]''')
diagram('a03-b03-quality','OHLC 旁证','''flowchart TD
    A["逐行读取有限 OHLC"] --> B["比较 high、low、open、close"]
    B --> C{"存在关系异常？"}
    C -->|是| D["按日期汇总代码、来源 ID 与原值明细"]
    C -->|否| E["该行不增加 warning"]
    D --> F["返回日期到原因映射；不修改事实"]
    E --> F''')
diagram('a03-b03-read-fact','可选事实读取','''flowchart TD
    A{"存在 Parquet？"} -->|否| B["返回权威空事实"]
    A -->|是| C["精确打开 Dataset；物化全部事实"]
    C --> D["现有完整业务验收；返回事实"]''')
diagram('b03-c03-11','单日查询','''flowchart TD
    A["显式字段；day 等于请求日期"] --> B["一次 finance.run_query"]
    B --> C{"正常返回且非 None？"}
    C -->|是| D["返回来源响应；尚未归一化或提交"]
    C -->|否| E["分类请求错误并抛出；不自动重试"]''')
diagram('a03-b03-normalize','来源归一化','''flowchart TD
    A["确认 DataFrame"] --> B{"响应为空？"}
    B -->|是| C["返回权威空事实"]
    B -->|否| D["必需字段；少于 5000 行；请求日期一致"]
    D --> E["ID 与代码非空；名称不得为空白"]
    E --> F["来源缺失转 null；拒绝非法文本和非有限值"]
    F --> G["构造事实字段；完整验收返回"]''')
diagram('b03-c03-13','日期集合规划','''flowchart TD
    A["选择 ALL required 日期及可选范围"] --> B["正式事实计数和 OHLC warning"]
    B --> C{"日历状态与事实证据一致？"}
    C -->|是| D["已完成"]
    C -->|否| E{"非空正式事实或已有确认空审计？"}
    E -->|是| F["无 API 日历修复"]
    E -->|否| G["API 待办"]
    D --> H["返回三个集合的结果"]
    F --> H
    G --> H''')
diagram('a03-b03-merge','完整月份合并','''flowchart TD
    A["选择当前年月旧事实"] --> B["保留未触达日期"]
    B --> C["拼接本次响应；触达日期整体替换"]
    C --> D["确认空不造占位行；必要时构造权威空表"]
    D --> E["完整叶验收；返回内存结果"]''')
diagram('b03-c03-15','存量 metadata 整根升级','''flowchart TD
    A["完整事实验收；准备路径"] --> B["写 staging 根与零行标记"]
    B --> C["staging 精确逐值复读"]
    C --> D["备份旧根；安装新根"]
    D --> E["正式完整事实精确逐值复读"]
    E --> F["清理临时路径；返回正式事实"]
    D -. 失败 .-> R["按原有移动标记恢复旧根"]
    E -. 失败 .-> R
    R --> S["完整恢复则清理；恢复失败保留现场并抛错"]
    B -. 失败 .-> T["清理 staging；重抛"]
    C -. 失败 .-> T''')
diagram('a03-b03-fact-commit','当前事实叶提交','''flowchart TD
    A["完整叶验收；确认分区边界"] --> B["写 staging 与零行标记；逐值复读"]
    B --> C["必要时安装新标记；备份旧叶"]
    C --> D{"完整叶非空？"}
    D -->|是| E["安装当前新叶"]
    D -->|否| F["不安装新叶；表达旧事实删除"]
    E --> G["正式过滤当前叶；完整内容一致"]
    F --> G
    G --> H["返回正式叶；日历尚未回写"]
    C -. 失败 .-> R["现有恢复分支处理当前叶与新标记"]
    E -. 失败 .-> R
    G -. 失败 .-> R
    R --> S["恢复成功清理；恢复失败保留现场并抛错"]
    B -. 失败 .-> T["清理 staging；重抛"]''')
diagram('b03-c03-17','日历内存状态','''flowchart TD
    A{"事实复读完成或请求失败？"} -->|完成| B["正计数记 success；零计数记 empty_confirmed"]
    B --> C["设置质量原因、批次和完成时间"]
    A -->|失败| D["保留当前事实计数；记错误且未完成"]
    C --> E["完整日历验收；返回内存状态"]
    D --> E
    E --> F["尚未提交日历"]''')
diagram('a03-b03-calendar-commit','当前日历逐叶提交','''flowchart TD
    A["触达日期定位完整日历叶"] --> B["完整叶验收；staging 写入与逐值复读"]
    B --> C["备份当前旧叶；安装新叶"]
    C --> D["从正式表根过滤当前叶；逐值复读"]
    D --> E["清理临时路径；处理下一叶"]
    E --> F["全部触达叶完成后返回行数"]
    C -. 失败 .-> R["现有恢复分支处理当前叶；此前成功叶保留"]
    D -. 失败 .-> R
    R --> S["恢复成功清理；恢复失败保留现场并抛错"]
    B -. 失败 .-> T["清理 staging；重抛"]''')
diagram('b03-c03-19','CLI 与月份推进','''flowchart TD
    A["开始日志；参数门禁；读取与规划"] --> B["有修复集合则按 write 决定是否提交"]
    B --> C{"存在 API 待办？"}
    C -->|否| D["完成或修复预览；结束日志"]
    C -->|是| N["认证 JQData"]
    N --> P{"还有待办月份？"}
    P -->|是| E["当前月逐日查询、转换与质量旁证"]
    E --> F{"启用 write？"}
    F -->|否| G["累计内存结果；下一月"]
    G --> P
    F -->|是| H["合并完整叶；提交并正式复读事实"]
    H --> V["正式计数与 warning；核对来源旁证"]
    V --> I["生成并提交日历；报告月份完成"]
    I --> J["更新内存事实；下一月"]
    J --> P
    P -->|否| Q{"启用 write？"}
    Q -->|是| K["现有批末正式复核"]
    Q -->|否| L["最终结果与结束日志"]
    K --> L
    E -. 请求或转换失败 .-> R["write 时仅提交失败日期状态；停止本批"]
    H -. 失败 .-> S["按当前提交边界处理；停止本批"]
    I -. 失败 .-> S
    R --> T["统一失败日志；保留异常链"]
    S --> T''')
diagram('b03-c03-21','当前执行入口','''flowchart TD
    A{"已加载 ipykernel？"} -->|是| B["args 为空；调用 main；不读取内核参数"]
    B --> C["默认不写；有 API 待办仍查询"]
    A -->|否| D{"直接执行脚本？"}
    D -->|是| E["Click 读取终端参数"]
    D -->|否| F["不运行入口"]''')
overview=flow('overview','## 总流程：日期证据、事实与日历', '''flowchart TD
    A["外部市场日历；正式境外事实"] --> B["参数门禁与读取；现存 metadata 分支"]
    B --> C["按 ALL required 日期规划"]
    C --> D["已完成：跳过"]
    C --> E["事实或确认空证据完整：只修复日历"]
    C --> F["证据缺失：API 待办"]
    E --> G{"启用 write？"}
    G -->|是| H["提交日历修复；不认证 JQData"]
    G -->|否| I["仅报告修复计划"]
    F --> J["认证；按月逐日查询与归一化"]
    J --> K{"启用 write？"}
    K -->|否| L["仅内存结果"]
    K -->|是| M["合并当前月完整叶；提交并复读事实"]
    M --> N["依据正式计数和 OHLC 结论回写日历"]
    N --> O["逐月推进；现有批末复核"]
    D --> P["统一结束或失败日志"]
    H --> P
    I --> P
    L --> P
    O --> P''')
new_cells=[]
for cell in notebook['cells']:
    if cell['cell_type']=='code': new_cells.append(diagrams[cell['id']])
    new_cells.append(cell)
    if cell['id']=='b03-c03-01': new_cells.append(overview)
    for added in extra_cells.get(cell['id'], []):
        if added['cell_type']=='code': new_cells.append(diagrams[added['id']])
        new_cells.append(added)
notebook['cells']=new_cells
ast.parse('\n\n'.join(''.join(c['source']) for c in new_cells if c['cell_type']=='code'))
assert path.read_bytes()==original_bytes, 'Notebook 在准备期间发生变化'
path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1)+'\n', encoding='utf8', newline='\n')
print(json.dumps({'snapshot':str(snapshot),'code_cells':sum(c['cell_type']=='code' for c in new_cells),'flowcharts':len(diagrams)+1},ensure_ascii=False))
