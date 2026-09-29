"""a04/b01 第 1—4 项：说明、流程图和 main 日志；不改变业务分支。"""
import ast
import copy
import json
import pathlib
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
path = ROOT / RELATIVE
original_bytes = path.read_bytes()
notebook = nbformat.read(path, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
assert not any(cell.id.startswith('a04-b01-flow-') for cell in notebook.cells)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a04-b01-docs-logs-before-'))
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    destination = snapshot / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, destination)

descriptions = {
    '3f34971c': '''# b01 宏观发布日历

本入口生产 `dim_macro_release_calendar`，没有直接上游维度表，不读取 a01 交易日历，也不调用 Tushare 或 Eastmoney。它按共享配置生成理论观测日/报告期、项目可用日和调度状态，为 a04/b02、b03 提供事实采集格点。

| 依赖或下游 | 与本环节的关系 |
| --- | --- |
| `.env` / `config.settings` | 提供统一历史起点与正式湖根目录；默认终点为北京时间当前日。 |
| `config/futures_lakehouse/macro_release_entities.py` | 25 个系列、频率及版本化可用日规则的唯一配置来源；8 个 SHIBOR 期限、17 个宏观系列。 |
| `config/data_contracts.py` | 日历字段、主键、分区和业务契约的唯一来源。 |
| 现有正式日历 | 提供既有格点及 b02/b03 已回写的采集、计数、质量和审计状态。 |
| a04/b02_interest_rate | 消费 `interest_rate` required 格点，生产 SHIBOR 事实，正式复读后回写日历。 |
| a04/b03_macro_release | 消费 `macro_release` required 格点，生产 CPI/PPI/PMI/GDP 事实，正式复读后回写日历。 |
| operations / 数据库读取 Demo | 默认日常阶段先执行本入口、再执行 b02/b03；Demo 只读展示正式表。 |

本入口只提交日历，不直接读取或删除两张下游事实表。下游仍将正式事实和日历状态共同用于完整性判定；已有事实完整但日历陈旧时，下游可以无 API 修复。''',
    'ae2ed8cd': '''## 日期含义、自动范围与写入边界

| 日期或模式 | 当前语义 |
| --- | --- |
| SHIBOR `report_date` | 普通周一至周五观测日，不按交易所节假日日历过滤。 |
| 宏观 `report_date` | CPI/PPI/PMI 使用月末，GDP 使用季末；不是来源 API 的实际发布日期。 |
| `expected_available_date` | 项目版本化规则推定的最早可用日；已到北京时间当前日才置为 required。 |
| 默认自动范围 | 从统一起点到北京时间当前日生成完整理论表，比较新增、内部缺口、撤销和政策变化。不是 a01 的尾部增量模式。 |
| 成对显式日期 | 与统一起点、当前日取交集后生成；范围外旧日历原样参与完整叶合并。可只读，写入必须选择非正式湖。 |
| 不带 `--write` | 读取本地日历、生成并校验计划；不提交，也不请求 API。 |
| 带 `--write` | 普通更新替换或删除变化完整叶；新表、完整期望为空或现有 metadata 迁移分支使用整根交换。 |

可用日规则来自共享配置：SHIBOR 当日；CPI/PPI 次月 9 日并向后顺延周末；PMI 月末，2 月使用 3 月 4 日；GDP 季末后第 16 日。required 表示应采，并不证明来源已返回数值或事实已完成。

当前 `main()` 先判断旧表能否通过当前策略校验。通过时，仅三个政策字段逐值相同的格点继承 `STATE_COLUMNS`，包括原 `updated_at`；新增或可用日到达等政策变化使用新初始化状态。旧策略不通过且 metadata 也旧时，自动模式不继承旧表状态、完整重新生成；显式日期拒绝旧策略迁移。当前 metadata 下的策略错误仍报错。

现有代码尚把任意 metadata 差异都纳入迁移判断，未区分纯描述性差异；这与项目“纯描述性 metadata 变化不得重写历史”的约束存在待处理差异。本轮只整理呈现和日志，不将这一现存分支描述为已经收缩。

相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../03_Futures_Database/AGENTS.md)。''',
    '30cc4c56': '''## Schema 契约与有界本地样例

交互内核且不存在 `__file__` 时，只展示本表的权威 Schema，因为它没有直接上游维度表。调用已传入 `lake_root`，显式选择样例会执行有界本地读取；不会调用 API 或写入湖仓。字段、表名、主键和分区不在展示单元格重复定义。''',
    'eccd9a3b': '''## 表身份、政策字段与可继承状态

表名、主键和 Hive 分区从具名 Schema metadata 各读取一次。主键为数据集类型—系列—报告/观测日，完整叶按 `dataset_name/year/month` 定位。

`POLICY_COLUMNS` 是可用日、是否 required 和版本化中文原因；`STATE_COLUMNS` 是下游采集完成、结果、计数、质量、批次及时间。`requirement_reason_text()` 只生成带配置/规则版本的原因文本，不决定落盘；版本文字变化也会使政策字段比较不同。''',
    'a2e386f4': '''## Hive 字段与完整 Schema 重建

`reconstructed_schema()` 按权威列序从 Dataset 取字段，把目录补出的 Hive 字段与文件字段组合后保留 Dataset metadata。缺少权威字段立即报错；本函数不读取记录，也不证明业务数据完整。后续函数分别判断物理兼容和 metadata 精确一致。''',
    '2dc99ad7': '''## 日历自身的业务验收

`validate_macro_calendar_table()` 先按权威 Arrow 契约转换，检查主键唯一，再逐行核对数据集枚举、年月、0/1 事实计数、完成状态和审计时间。严格模式还核对共享系列、理论频率、版本化可用日和原因文本，以及 pending、成功、确认空、失败、not_required 各自的状态组合。

`allow_legacy_policy=True` 仅放宽旧系列/规则与相关策略状态检查，仍保留物理契约、主键、基础完成关系等要求；只用于现有表的迁移识别。新生成结果、staging 和正式复读使用严格模式。返回按主键排序的 Pandas 表。''',
    '05826f25': '''## 理论格点生成与下游状态继承

`build_expected_calendar()` 将传入的可继承旧行按主键建立映射，再按 25 个系列的普通工作日、月末或季末频率展开日期。每行计算项目可用日和 required，初始化 pending 或 not_required 状态。

只有旧行存在且三个政策字段逐值相同，才逐项继承完整下游状态和原更新时间；其余行保留新状态。函数最后执行完整业务验收并返回内存结果，尚未落盘。`main()` 在旧策略整体不合法时传入空继承表；不能把这一分支描述成只重置个别格点。''',
    '1bece26c': '''## 完整内容摘要

`table_digest()` 将全部权威字段按主键排序，经 Arrow 行重建为稳定表示，再写 Arrow IPC 流并计算 SHA-256。摘要覆盖政策、下游状态和审计字段，不只是主键或行数；它用于比较期望、旧表和复读结果，不承担新的业务规则。''',
    '62335bcd': '''## 当前完整叶提交与手写恢复

`commit_partitions()` 先校验完整期望并计算摘要，写入本批 staging 零行契约标记和变化叶，再检查 staging 总行数、迁移时的完整摘要及逐叶内容。当前仍保留多次业务校验和转换，这一轮没有删除它们。

普通路径按本批全部变化叶依次备份、安装或删除，未触达叶与既有根标记保持原样；新表、完整期望为空或强制迁移走整根交换。正式整表重新打开、业务验收并比较总行数和完整摘要后才返回提交行数。本次没有独立日期水位文件。

当前恢复仍由本函数手写：异常时按记录倒序恢复已处理叶，整根交换则按两个移动标记恢复；恢复完整后清理 staging、备份及失败隔离目录，恢复不完整保留现场并报错。当前循环中某处恢复报错会停止余下恢复，尚未接入共享模块的逐项继续恢复能力。后续事务接入必须保持“本次全部变化叶共同恢复”的边界。''',
    '2dcd351e': '''## CLI：范围、继承、完整比较与提交

`main()` 先检查成对日期、日期顺序及正式湖写入门禁，再确定北京时间可见日和生成范围。读取现有日历时区分物理不兼容、metadata 旧、当前策略有效或无效；显式范围保留范围外旧行，自动模式生成完整理论水位。

计划统计新增、撤销、政策字段变化和变化分区；`policy_resets` 只统计交集主键的三个政策字段差异，不等于旧策略迁移时全部状态初始化的行数。没有变化且正式表存在则结束；否则只读输出计划，或调用提交函数并等待正式复读完成。

本轮日志统一为事件前缀与 `table / function / phase / status / elapsed_s` 字段，批次首尾使用 `=` 分隔。阶段日志暂由 `main()` 报告范围、读取、生成、比较和提交；后续第 5—6 项再将函数内进度与完成日志归位。计划/生成完成用 `persisted=false`，提交成功才用 `persisted=true`；失败保留原异常。''',
    'dfa42b3a': '''## 当前 Notebook 与脚本入口

Notebook 入口以 `args=[]`、`standalone_mode=False` 调用 Click，默认生成正式湖只读计划，不调用 API。普通终端直接运行 `.py` 时读取命令行参数，只有显式 `--write` 才提交。

当前执行格只检查是否加载 `ipykernel`，尚未加入 `__file__` 保护和显式 `notebook_args`，因此内核中导入同名模块也可能触发入口；这与上方 Schema 浏览格的保护条件不同。入口改写属于后续第 11 项，本轮流程图如实反映当前条件。''',
}
for cell_id, source in descriptions.items():
    cells[cell_id].source = source.strip() + '\n'

# 只按既有独立函数边界拆展示单元格，不抽取函数、不改函数体。
extras = {}
split_specs = {
    'a4a99db8': [
        ('physical_schema_matches', 'physical', '物理字段兼容性', '逐项比较字段名顺序、类型及 nullable，不比较 metadata；返回布尔值，不读取数据记录。'),
        ('dataset_has_exact_schema_metadata', 'metadata', 'Dataset 与 fragment metadata', '先比较重建后的 Dataset Schema/metadata，再按去除 Hive 列的文件 Schema 检查所有 fragment；任一不同返回 False。'),
        ('open_compatible_dataset', 'compatible', '打开物理兼容的日历', '确认存在 Parquet 后打开 Hive Dataset，检查完整 Dataset 与每个 fragment 的物理结构；返回 Dataset 以及 metadata 是否精确一致，不物化记录。'),
        ('open_exact_dataset', 'exact', '打开精确契约的日历', '复用物理兼容读取；metadata 不精确时拒绝，用于当前 staging 和正式提交复读。'),
        ('partition_expression', 'partition', '完整叶过滤表达式', '按权威分区顺序组合相等条件；供 staging 逐叶复读使用。空键报错，不在此决定生成或提交范围。'),
    ],
    'db0b5fa0': [
        ('changed_partition_keys', 'changed', '识别完整叶差异', '取期望与现有分区键并集，逐叶比较行数和完整内容摘要；新增、删除、行数相同但值不同均进入计划。当前逐叶仍构造整表布尔掩码，后续第 7—9 项再收缩，不把它描述成已完成分组复用。'),
    ],
}
for cell_id, specs in split_specs.items():
    cell = cells[cell_id]
    lines = cell.source.splitlines(keepends=True)
    functions = {node.name: node for node in ast.parse(cell.source).body}
    starts = [functions[name].lineno - 1 for name, *_ in specs]
    segments = [0, *starts, len(lines)]
    cell.source = ''.join(lines[segments[0]:segments[1]]).rstrip() + '\n'
    additions = []
    for index, (_, suffix, title, explanation) in enumerate(specs, 1):
        additions.append(nbformat.v4.new_markdown_cell('## ' + title + '\n\n' + explanation + '\n', id='a04-b01-doc-' + suffix))
        new_cell = nbformat.v4.new_code_cell(''.join(lines[segments[index]:segments[index + 1]]).rstrip() + '\n', id='a04-b01-code-' + suffix)
        cells[new_cell.id] = new_cell
        additions.append(new_cell)
    extras[cell_id] = additions

cells['b5ed1a92'].source = cells['b5ed1a92'].source.replace('import sys\n', 'import sys\nimport time\n', 1)
main_source = cells['cd6a9f44'].source

def replace_once(old, new):
    global main_source
    assert main_source.count(old) == 1, old
    main_source = main_source.replace(old, new, 1)

def log(phase, status, fields='', event='planning_progress'):
    return (
        '    click.echo(\n'
        f'        f"{event}: table={{TABLE_NAME}}; function=main; phase={phase}; status={status}; "\n'
        + (f'        f"{fields}; "\n' if fields else '')
        + '        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"\n'
        '    )\n'
    )

replace_once('    silver_root = resolved_lake_root / "silver"',
    log('parameters', 'completed', 'mode={\'explicit\' if has_explicit_dates else \'automatic\'}; lake_root={resolved_lake_root}; generation_start={generation_start_date}; generation_end={generation_end_date}; visible_date={visible_date}')
    + '    log_phase = "read_existing"\n' + log('read_existing', 'started')
    + '    silver_root = resolved_lake_root / "silver"')
replace_once('    if has_explicit_dates:\n        in_scope_mask = (',
    log('read_existing', 'completed', 'rows={len(existing_df)}; metadata_upgrade_required={str(metadata_upgrade_required).lower()}; existing_policy_is_current={str(existing_policy_is_current).lower()}')
    + '    log_phase = "scope_existing"\n    if has_explicit_dates:\n        in_scope_mask = (')
replace_once('    expected_scope_df = build_expected_calendar(',
    '    log_phase = "generate_expected"\n' + log('generate_expected', 'started', 'series={len(MACRO_RELEASE_SERIES)}; inheritance_rows={len(inheritance_existing_df)}')
    + '    expected_scope_df = build_expected_calendar(')
replace_once('    existing_rows_by_key = {',
    log('generate_expected', 'completed', 'scope_rows={len(expected_scope_df)}; full_rows={len(expected_full_df)}; persisted=false')
    + '    log_phase = "reconcile"\n' + log('reconcile', 'started')
    + '    existing_rows_by_key = {')
replace_once('        f"table={TABLE_NAME}; mode={mode}; "',
    '        f"planning_progress: table={TABLE_NAME}; function=main; phase=reconcile; status=completed; mode={mode}; "')
replace_once('        f"lake_root={resolved_lake_root}; write={str(write).lower()}"',
    '        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "\n'
    '        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace_once('        "reconciliation_plan: "',
    '        f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; persisted=false; "')
replace_once('        f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}"',
    '        f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}; "\n'
    '        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace_once('        click.echo("宏观发布日历已经与当前理论水位和下游状态完整一致。")',
    textwrap.indent(log('plan', 'skipped', 'reason=already_current; rows={len(expected_full_df)}; persisted=false; message=宏观发布日历已经与当前理论水位和下游状态完整一致。', 'up_to_date'), '    ').rstrip()
    + '\n' + textwrap.indent(log('run', 'completed', 'write={str(write).lower()}; changed_partitions=0; persisted=false'), '    ').rstrip())
replace_once('        committed_rows = commit_partitions(',
    '        log_phase = "commit"\n' + textwrap.indent(log('commit', 'started', 'partitions={len(partition_keys)}; full_root_swap={str(full_root_swap_used).lower()}'), '    ')
    + '        committed_rows = commit_partitions(')
replace_once('            f"committed: rows={committed_rows}; "',
    '            f"committed: table={TABLE_NAME}; function=main; phase=commit; status=completed; rows={committed_rows}; persisted=true; "')
replace_once('            f"full_root_swap={str(full_root_swap_used).lower()}"',
    '            f"full_root_swap={str(full_root_swap_used).lower()}; "\n'
    '            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace_once('        click.echo("dry_run: 未写入数据湖。")',
    textwrap.indent(log('commit', 'skipped', 'reason=dry_run; write=false; persisted=false', 'dry_run'), '    ').rstrip())
main_source = main_source.rstrip() + '\n' + log('run', 'completed', 'write={str(write).lower()}; changed_partitions={len(partition_keys)}; persisted={str(write).lower()}')
function = next(node for node in ast.parse(main_source).body if isinstance(node, ast.FunctionDef))
lines = main_source.splitlines(keepends=True)
start = function.body[0].lineno - 1
prefix, body = ''.join(lines[:start]), ''.join(lines[start:])
cells['cd6a9f44'].source = (
    prefix
    + '    log_started_at = time.perf_counter()\n    log_phase = "parameters"\n'
    + '    click.echo("=" * 80)\n'
    + log('run', 'started', 'write={str(write).lower()}')
    + '    try:\n' + textwrap.indent(body, '    ')
    + '    except Exception as error:\n'
    + '        click.echo(\n'
    + '            f"planning_progress: table={TABLE_NAME}; function=main; phase={log_phase}; status=failed; "\n'
    + '            f"error={type(error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"\n'
    + '        )\n        raise\n'
    + '    finally:\n        click.echo("=" * 80)\n'
)

graphs = {
    'b5ed1a92': ('初始化与配置', '''A["当前工作目录向上搜索三个项目标记"] --> B{"找到项目根？"}
B -->|否| X["报错停止"]
B -->|是| C["设置导入路径；加载权威 Schema、配置、设置"]
C --> D["仅初始化依赖；不请求 API、不写湖"]'''),
    '8c267047': ('契约和样例浏览', '''A{"交互内核且没有 __file__？"} -->|否| Z["跳过展示"]
A -->|是| B["展示本表权威 Schema"]
B --> C["显式选择样例时有界读取本地数据"]'''),
    '9d9f0315': ('表身份和状态边界', '''A["权威 Schema metadata"] --> B["各读取一次表名、主键、分区"]
B --> C["定义政策字段、继承字段及状态枚举"]
C --> D["构造 Hive partitioning"]
E["系列、可用日、required"] --> F["原因文本包含配置与规则版本"]'''),
    'a4a99db8': ('重建 Schema', '''A["Dataset Schema；权威字段顺序"] --> B["依次取出文件和 Hive 字段"]
B --> C["组合字段并保留 Dataset metadata"]
B -. 缺字段 .-> X["抛出 TypeError"]'''),
    'a04-b01-code-physical': ('比较物理契约', '''A["实际 Schema 与期望 Schema"] --> B{"字段名及顺序一致？"}
B -->|否| F["返回 False"]
B -->|是| C{"类型和 nullable 逐项一致？"}
C -->|否| F
C -->|是| T["返回 True；不比较 metadata"]'''),
    'a04-b01-code-metadata': ('检查精确 metadata', '''A["重建 Dataset Schema"] --> B{"含 metadata 的比较一致？"}
B -->|否| F["返回 False"]
B -->|是| C["权威 Schema 去除 Hive 列得到文件 Schema"]
C --> D["逐 fragment 精确比较；返回是否全部一致"]'''),
    'a04-b01-code-compatible': ('物理兼容读取', '''A["路径中存在 Parquet？"] --> B["打开 Hive Dataset"]
B --> C["检查 Dataset 和所有 fragment 物理结构"]
C --> D["检查精确 metadata"]
D --> E["返回 Dataset 和一致性标志；未物化数据"]
A -. 无文件 .-> X["报错停止"]
C -. 不兼容 .-> X'''),
    'a04-b01-code-exact': ('精确读取', '''A["调用物理兼容读取"] --> B{"metadata 精确一致？"}
B -->|是| C["返回 Dataset"]
B -->|否| X["拒绝读取；抛出 TypeError"]'''),
    'a04-b01-code-partition': ('构造叶过滤条件', '''A["分区键与权威分区列"] --> B["逐列构造相等条件并取交集"]
B --> C{"表达式非空？"}
C -->|是| D["返回 Arrow Expression"]
C -->|否| X["报错停止"]'''),
    'ebbfdc80': ('日历业务验收', '''A["Arrow 契约转换；检查主键唯一"] --> B["逐行核对枚举、年月、计数和审计时间"]
B --> C["核对完成布尔值与状态、0/1 数量关系"]
C --> D{"允许旧策略？"}
D -->|否| E["校验系列、频率、可用日、原因和详细状态组合"]
D -->|是| F["保留基础校验；跳过当前策略要求"]
E --> G["按主键排序返回 Pandas"]
F --> G'''),
    'd9a85e0d': ('生成理论日历', '''A["旧行按主键建映射；初始化期望行"] --> B["按系列频率生成普通工作日、月末或季末"]
B --> C["计算项目可用日及 required；初始化状态"]
C --> D{"旧行存在且三个政策字段相同？"}
D -->|是| E["继承全部状态字段和旧更新时间"]
D -->|否| F["保留新初始化状态"]
E --> G["收集各行；空范围得到权威空表"]
F --> G
G --> H["完整业务验收；返回内存结果"]'''),
    'db0b5fa0': ('完整内容摘要', '''A["全部权威列按主键排序"] --> B["Pandas 转 Arrow；按行重建稳定表示"]
B --> C["写 Arrow IPC 流"]
C --> D["返回完整内容 SHA-256"]'''),
    'a04-b01-code-changed': ('变化完整叶', '''A["期望和已有分区键并集"] --> B["按键构造两表掩码；选取完整叶"]
B --> C{"行数相同且完整摘要相同？"}
C -->|是| D["跳过该叶"]
C -->|否| E["记录变化键；包括旧叶删除"]
D --> F["遍历全部键；返回变化列表"]
E --> F'''),
    'f4c3582d': ('当前提交和恢复', '''A["完整期望验收与摘要"] --> B["staging 写零行标记和变化叶"]
B --> C["staging 总量、逐叶和迁移完整摘要验收"]
C --> D{"整根交换？"}
D -->|是| E["按移动标记备份旧根、安装新根"]
D -->|否| F["本批变化叶依次备份、安装或显式删除"]
E --> G["正式整表验收；总行数和完整摘要相同"]
F --> G
G --> H["清理临时目录；返回提交行数"]
E -. 失败 .-> R["现有手写恢复；整根或全部已处理叶"]
F -. 失败 .-> R
G -. 失败 .-> R
R --> S["恢复完整则清理；不完整保留现场并抛错"]
B -. 失败 .-> T["清理 staging；抛错"]
C -. 失败 .-> T'''),
    'cd6a9f44': ('主流程和日志', '''A["批次开始；参数与正式湖写入门禁"] --> B["读取旧表；识别旧 metadata 和旧策略"]
B --> C["区分范围内外；选择可继承旧行"]
C --> D["生成理论格点；显式范围合并范围外旧行"]
D --> E["完整内容比较；统计变化叶和格点"]
E --> F{"已有表且无变化或迁移？"}
F -->|是| Z["up_to_date；结束日志"]
F -->|否| G{"启用 write？"}
G -->|否| H["dry_run；计划未落盘"]
G -->|是| I["提交并正式复读；committed"]
H --> Z
I --> Z
B -. 异常 .-> X["记录当前阶段；继续抛出原异常；结束分隔线"]
D -. 异常 .-> X
I -. 异常 .-> X'''),
    '4a5b544e': ('当前执行入口', '''A{"已加载 ipykernel？"} -->|是| B["main.main：args 为空；standalone_mode=False"]
B --> C["默认只读计划；不调用 API"]
A -->|否| D{"直接运行脚本？"}
D -->|是| E["Click 读取终端参数"]
D -->|否| F["不执行入口"]'''),
}

def flow(suffix, title, body, overall=False):
    return nbformat.v4.new_markdown_cell(
        ('## 总流程：' if overall else '### 流程：') + title + '\n\n```mermaid\nflowchart TD\n' + body + '\n```\n',
        id='a04-b01-flow-' + suffix,
    )

overview = flow('overview', '理论格点、下游状态与正式提交', '''A["统一起点、北京时间当前日、共享系列配置"] --> B["读取现有日历；确认兼容及继承边界"]
B --> C["生成完整理论格点和项目可用日"]
C --> D["政策相同继承状态；其余按规则初始化"]
D --> E["完整表比较：新增、内部缺口、撤销、政策变化"]
E --> F{"需要提交变化或初始化？"}
F -->|否| Z["已经一致；结束"]
F -->|是| G{"启用 write？"}
G -->|否| H["只读计划；不写湖"]
G -->|是| I["staging 写入与复读；安装变化叶或整根"]
I --> J["正式整表验收；成功才报告落盘"]
I -. 失败 .-> R["现有手写恢复本批；抛错"]
J -. 失败 .-> R
J --> K["下游 b02/b03 消费 required 格点并生产事实"]
K -. 正式事实复读后回写 .-> L["日历完成、计数、质量及审计状态"]
L -. 下一次运行读取 .-> B''', overall=True)
new_cells = []
for cell in notebook.cells:
    for item in [cell, *extras.get(cell.id, [])]:
        if item.id == 'b5ed1a92':
            new_cells.append(nbformat.v4.new_markdown_cell('## 初始化与依赖\n\n沿用项目标记文件搜索定位根目录，导入权威 Schema、宏观配置和项目设置；仅初始化，不执行采集或写入。\n', id='a04-b01-doc-imports'))
        if item.cell_type == 'code':
            title, graph = graphs[item.id]
            new_cells.append(flow(item.id, title, graph))
        new_cells.append(item)
        if item.id == 'ae2ed8cd':
            new_cells.append(overview)
notebook.cells = new_cells
assert before.metadata == notebook.metadata
for old in before.cells:
    assert {key: value for key, value in old.items() if key != 'source'} == {key: value for key, value in cells[old.id].items() if key != 'source'}
ast.parse('\n\n'.join(cell.source for cell in notebook.cells if cell.cell_type == 'code'))
nbformat.validate(notebook)
assert path.read_bytes() == original_bytes
path.write_text(nbformat.writes(notebook) + '\n', encoding='utf8', newline='\n')

for old_name, new_name in (
    ('export_a03_b04_isolated_20260928.py', 'export_a04_b01_isolated_20260928.py'),
    ('render_a03_b04_flowcharts_20260928.cjs', 'render_a04_b01_flowcharts_20260928.cjs'),
):
    helper = (ROOT / '00_draft_collection_02/scripts' / old_name).read_text(encoding='utf8')
    helper = helper.replace('a03_External_Market_Data/b04_external_index', 'a04_Macro_And_Interest_Rates/b01_macro_release_calendar').replace('a03_b04', 'a04_b01')
    (ROOT / '00_draft_collection_02/scripts' / new_name).write_text(helper, encoding='utf8', newline='\n')
print(json.dumps({'snapshot': str(snapshot), 'code_cells': len(graphs), 'flowcharts': len(graphs) + 1}, ensure_ascii=False))
