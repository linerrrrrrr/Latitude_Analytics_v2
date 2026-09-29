"""整理 b08 说明和 Mermaid，统一入口日志；验证业务 AST 和原注释不变。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b08_full_minute_quality.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}

descriptions = {
    'c08-title': '''# b08 分钟主键缺失审计

本环节只读取本地 silver，以理论分钟主键减去已提交的分钟事实主键，生成 `fact_futures_missing_bar`，并回写 `dim_futures_bar_calendar` 的缺失计数和检查时间。不调用外部 API。

| 上下游 | 数据职责与本环节的关系 |
| --- | --- |
| b03 → b04 | 合约 Session 结构经 b04 形成行情日历；b08 直接读取 b04 的 Session 起止、交易日和理论条数，不另读合约日历。 |
| b06 | 在完整日历上维护当前采集要求和完成状态，生产 `fact_futures_minute`；b08 信任正式提交的事实主键、Session 归属、范围和行情质量。 |
| b07 | 将日线与分钟聚合比较结果写入行情日历；b08 原样保留已有旁证，不调用 b07，也不读取日线事实。 |
| b08 当前输出 | 缺失明细整表快照，以及触达日历完整叶中的五个计数、标志和时间字段。 |
| 后续读取 | 数据库读取示例及研究消费者按权威 Schema 读取缺失明细；b04 在结构不变时继承这些日历状态。 |

计算范围为全部 `bar_frequency=1m AND is_fetch_required=true` 的 Session，运行前要求这些 Session 全部已完成 b06。计算式为：

`required 且已完成的理论分钟主键 − 正式分钟事实的 contract_code、bar_at 主键 = 缺失分钟主键`

理论时间采用 `(session_start_at, session_end_at]` 的分钟结束时刻语义。分钟事实仅投影 `contract_code`、`bar_at`；事实中的 OHLC 等行情值由 b06 负责，缺失审计保留已有质量结论。

运行语义见湖仓根目录 `README.md` 与 `03_Futures_Database/AGENTS.md`；字段、主键、分区及状态含义以 `config/data_contracts.py` 为准，下方浏览器直接展示该契约。''',
    'c08-boundary': '''## 人工全量范围、就绪前提与写入边界

`--confirm-full-quality` 是必需的人工全量确认；本入口不接受日期、月份、合约等范围裁剪。所有 required 的 1m Session 必须已完成 b06，否则在创建 staging 之前失败，不跳过未完成行继续生成缺失结论。

| 方式 | 计算与暂存 | 正式写入 |
| --- | --- | --- |
| `--confirm-full-quality` | 从指定湖全范围读取，按分区计算并在系统临时目录写入、复读 staging。 | 不提交缺失表或日历；退出时清理临时目录。 |
| `--confirm-full-quality --write` | 执行同样的全量审计，在该湖 silver 下准备本批 staging。 | 缺失明细整表根与全部触达日历叶共同提交、共同回滚。 |

正式湖来自 `settings.futures_lake_root`；`--lake-root` 可用于独立临时湖。b08 是人工入口，不进入任何 operations batch，也不由 `--skip-optional-quality` 控制；该选项只跳过 b07。

日历只更新 `actual_bar_count`、`is_data_missing`、`missing_bar_count`、`missing_checked_at`、`updated_at`。调度状态、拉取要求、完成凭证、质量状态/原因/检查时间以及全部 b07 旁证均保留。缺失计数减少也不会自动把历史 `warning` 改成 `passed`。

即使缺失为零，也生成带零行 `schema.parquet` 的缺失表，用于替换旧缺失快照；未选中行在触达日历叶内保留原值。没有 required Session 时仍读取上游契约并生成零行缺失表，不写日历叶。b08 不推进独立日期水位。''',
    'c08-schema-heading': '''## Schema 契约与有界样例

仅在交互式 Notebook 中展示行情日历、分钟事实、缺失明细三张直接相关表。共享浏览器从 `config/data_contracts.py` 读取权威 Schema，可按用户选择有界读取本地样例；此格不启动全量审计或提交，脚本运行和普通模块导入跳过展示。''',
    'c08-metadata-heading': '''## 表身份、算法键与分区

稳定表名、主键和 Hive 分区从三张具名 Schema 的 metadata 初始化；`MINUTE_KEY_COLUMNS` 只定义求差需要的两列投影，`SESSION_MATCH_COLUMNS` 表达合约日内的 Session 身份，不另定义表契约。

| 数据 | 分区与本环节用途 |
| --- | --- |
| 行情日历 | `bar_frequency/exchange_code/year/month`；外层按 1m 交易所月读取完整叶，修改后保留完整叶提交。 |
| 分钟事实 | `exchange_code/underlying_code/year/month`；按候选品种及归属交易年月读取分钟主键。 |
| 缺失明细 | `bar_frequency/exchange_code/underlying_code/year/month`；只生成 1m，归属交易日与 Session 属性继承理论格点。 |

Hive partitioning 使用对应权威字段的类型。跨自然日的夜盘仍按理论 Session 的归属交易日与年月组织，不能按 `bar_at` 的自然日另分区。''',
    'c08-dataset-heading': '''## 数据集读取、分区发现与确定性内容摘要

`open_exact_dataset()` 确认存在 Parquet 后打开 Dataset，检查重建的逻辑 Schema 和全部 fragment 的物理字段、类型、nullable 与表身份。描述性 metadata 差异不阻塞历史读取。返回 Dataset 后，调用方再按分区物化所需列。

`parquet_file_schema()` 从权威 Schema 排除 Hive 分区列，得到文件字段；`partition_expression()` 将分区等值条件以 AND 连接。`discover_partition_keys()` 根据目录发现叶键，跳过根级 `schema.parquet`，检查目录层级和列顺序，并将年、月解释为整数。

`table_digest()` 先固定权威类型、metadata 与列顺序，再按主键排序，对全部逻辑内容生成 SHA-256。摘要单独记录 null 有效位，并归一 null 槽位、NaN payload 和正负零的物理表示，避免逻辑值相同却产生不同摘要。

输入检查不重新承担 b06 行情质量验证；b08 自己的 staging 和正式输出继续保留逐 fragment 契约及完整内容摘要验收。此处说明当前实现，不将后续计划中的重复检查收缩提前视为已完成。''',
    'c08-output-heading': '''## 本环节产出与回写字段的验收

`validate_missing_output()` 将缺失明细固定为权威 Arrow 契约，检查非空结果的主键唯一性与目标分区归属；零行结果保留契约后返回。

`validate_calendar_audit_output()` 将完整日历叶转换为权威类型并检查分区归属；针对 required 的 1m 行，检查已完成状态、实际/缺失条数非负、两者相加等于理论条数、缺失标志一致，以及两个回写时间均等于本批 `detected_at`。

这里验证本环节的输出和直接计算前提，不重新检查分钟行情数值，也不重新判定已有质量或旁证。当前生成和 staging 复读仍调用对应输出 validator；正式缺失叶再次校验，正式日历叶通过契约与完整内容摘要验收。''',
    'c08-build-heading': '''## 按分区生成缺失明细与日历 staging

`build_full_audit_staging()` 先打开行情日历和分钟事实，发现 1m 日历叶，再用窄列完成一次全局就绪检查。所有 required Session 已完成后才创建两个 staging 目录，并写缺失表零行契约标记。

外层按交易所月读取完整日历叶；选出 required 行，叶内再次检查完成状态，再按品种分组。事实侧严格只读取该品种月的 `contract_code`、`bar_at`。Polars 用 `datetime_ranges + explode` 展开 `(start, end]` 的理论分钟，再以 `anti join` 求差，并核对展开数量与理论条数一致。

缺失点按原日历行索引汇总；实际条数按“理论条数 − 缺失条数”得到，表示理论范围内已存在的主键数，不是整个事实分区的总行数。日历只回写五个计数、时间字段。非空缺失结果逐品种月写入并复读 staging；零缺失不写业务叶，只保留表根契约标记。

当前交易所月全部品种处理完毕后，校验、写入并复读更新后的完整日历叶。各叶使用内容摘要比对；末尾再检查缺失表总行数及两个 staging 数据集契约。结果包含叶摘要和累计 Session、理论键、实际键、缺失键数量，尚未提交正式数据。

无 required 行的日历叶跳过回写；不会把全历史分钟值或所有缺失明细合成一个内存大表。''',
    'c08-commit-heading': '''## 缺失表根与日历叶的协调提交

`commit_full_audit()` 接收已验证的两个 staging 目录及摘要。先确认正式目标、staging、backup 和隔离路径均位于该湖 silver 内，再备份并替换整个缺失表根，随后按顺序备份、安装全部触达日历叶。缺失表根中的零行契约标记随整根安装；日历根级标记保持原样。

备份保留到两张表正式验收全部完成。缺失表检查物理/身份契约、总行数、分区集合、逐叶主键与内容摘要；日历检查数据集契约及全部触达叶的完整内容摘要。当前验收仍会打开正式日历表根，不能把它描述为已经只打开触达叶。

安装或验收失败时，按本地移动记录倒序隔离新日历叶、恢复旧叶，再恢复原缺失表。完整恢复后抛出原异常并清理临时目录；恢复本身失败时停止后续恢复、保留 staging/backup/隔离现场，并报告回滚异常。此处仍是 b08 自身实现，尚未接入共享事务模块。

只有全部验收成功并完成既有清理后才返回缺失行数和日历分区数。两表共同回滚不表示对外部读者原子可见，也不提供并发写入协调或进程终止后的自动恢复。''',
    'c08-cli-heading': '''## CLI 调度、汇总与运行日志

`main()` 检查人工确认参数，准备湖路径、本批 ID 与统一检测时间；根据 `--write` 选择 silver 内暂存路径或系统临时目录，再调用全量生成、输出汇总，并决定是否协调提交。只读退出会清理系统临时目录；写入模式在尚未尝试提交时发生异常，由入口清理两个 staging 目录。进入提交阶段后的恢复和清理由提交函数按现有逻辑处理。

日志对齐 b01、b02 的 88 个 `=` 运行边界，采用 `table/function/phase/status` 和 `elapsed_s`。`missing_audit_scope` 表达全量范围；`missing_audit_total:` 汇总 Session、理论键、实际键、缺失键与日历叶数量；`planning_progress:` 表达生成、跳过提交及运行状态，`committed:` 仅在协调提交返回后报告持久结果。只读结果记 `persisted=false`；普通异常继续向上抛出，不输出正常结束日志。

本轮日志起止与汇总仍由入口报告，读取、生成及提交函数的细分进度尚未迁入函数。此代码格仍保留 Click 命令定义和原有 `if __name__ == "__main__": main()`；直接运行 `.py` 使用命令行参数，模块导入不启动业务，Notebook 的独立执行单元格尚未对齐。''',
}
for cell_id, source in descriptions.items():
    cells[cell_id].source = source


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(
        title + '\n\n```mermaid\n'
        + '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        + textwrap.dedent(body).strip() + '\n```', id=cell_id,
    )


overview = flow('b08-flow-overview', '## 总流程：理论分钟主键求差与两表提交', '''
    flowchart TD
        A["人工启动；必须 confirm-full-quality"] --> B["打开行情日历、分钟事实；发现 1m 日历叶"]
        B --> C{"全部 required Session 已完成 b06？"}
        C -->|否| X["抛错；尚未创建 staging"]
        C -->|是| D["创建暂存目录；写缺失表零行契约标记"]
        D --> E["按交易所月、品种月读取日历与分钟两列主键"]
        E --> F["展开理论分钟并求差；回写五个日历字段"]
        F --> G["逐叶写入、复读 staging；验收摘要与总量"]
        G --> H{"write？"}
        H -->|否| I["汇总只读结果；清理临时目录后结束"]
        H -->|是| J["备份并安装缺失表根及全部触达日历叶"]
        J --> K["正式复读两表；检查契约、总量、分区及摘要"]
        K --> L["清理后报告整批提交；结束"]
        J -. 失败 .-> R["倒序恢复日历叶和缺失表；抛错"]
        K -. 失败 .-> R
        R --> S["恢复不完整保留现场；无成功日志"]
''')

flow_specs = {
    'c08-imports': ('environment', '运行环境与权威依赖', '''
        flowchart TD
            A["导入标准库"] --> B["从当前目录向上查找项目标记"]
            B --> C{"找到项目根？"}
            C -->|否| X["抛错停止"]
            C -->|是| D["加入项目根与湖仓模块路径"]
            D --> E["导入 DataFrame 库、三张 Schema、转换函数及 settings"]
    '''),
    'c08-schema-browser': ('schema', 'Schema 与有界样例展示', '''
        flowchart TD
            A{"交互式 Notebook？"} -->|否| B["跳过展示"]
            A -->|是| C["调用共享 Schema 浏览器"]
            C --> D["展示日历、分钟事实和缺失明细契约"]
            D --> E["可选有界本地样例；不启动全量审计"]
    '''),
    'c08-metadata': ('constants', '表身份、算法键与分区', '''
        flowchart TD
            A["从权威 Schema metadata 读取表名、主键与分区"] --> B["定义分钟两列投影和 Session 身份列"]
            B --> C["用权威字段类型构造三张表的 Hive partitioning"]
            C --> D["供按交易所月、品种月读取和写入使用"]
    '''),
    'c08-dataset-helpers': ('read', '数据集读取、分区发现与摘要', '''
        flowchart TD
            A["open_exact_dataset：发现 Parquet 并打开"] --> B["重建逻辑 Schema；检查稳定身份和所有 fragment"]
            B --> C["返回 Dataset；调用方按分区条件物化"]
            D["discover_partition_keys：遍历 Parquet 路径"] --> E["跳过根标记；检查目录结构并解析叶键"]
            F["table_digest：固定权威类型和 metadata"] --> G["按主键排序；归一 null、NaN 和正负零表示"]
            G --> H["摘要覆盖完整内容与有效位"]
            B -. 不兼容 .-> X["抛错"]
            E -. 非法路径 .-> X
    '''),
    'c08-output-validation': ('validate', '自身输出的验收', '''
        flowchart TD
            A["缺失明细 Arrow 输入"] --> B["固定契约；非空时检查主键和分区"]
            B --> C["返回已校验缺失表"]
            D["完整日历叶输入"] --> E["转换契约并检查分区；选择 required 1m 行"]
            E --> F["检查完成状态、非负计数及实际加缺失等于理论"]
            F --> G["检查缺失标志和本批两个回写时间"]
            G --> H["返回已校验日历表；质量与旁证保持原值"]
            B -. 不满足 .-> X["抛错"]
            F -. 不满足 .-> X
            G -. 不满足 .-> X
    '''),
    'c08-build': ('build', '分区求差、状态回写与暂存验收', '''
        flowchart TD
            A["打开上游；发现 1m 日历叶；检查全局完成状态"] --> B["创建 staging；写缺失表零行标记"]
            B --> C["读取当前完整日历叶；选择 required 行"]
            C --> D{"有选中 Session？"}
            D -->|否| N{"还有日历叶？"}
            D -->|是| E["按品种读取分钟两列键；展开理论分钟并核对数量"]
            E --> F["anti join 求缺失；按日历行汇总计数"]
            F --> G["更新五个字段；其余日历状态保留"]
            G --> H{"本品种月有缺失？"}
            H -->|是| I["校验、暂存缺失叶；复读并比较内容摘要"]
            H -->|否| J["累计本组 Session 和主键数量"]
            I --> J
            J --> K{"还有品种？"}
            K -->|是| E
            K -->|否| L["校验、暂存完整日历叶；复读并比较内容摘要"]
            L --> N
            N -->|是| C
            N -->|否| O["验收 staging 总量和契约；返回摘要与统计"]
    '''),
    'c08-commit': ('commit', '两表协调安装与失败恢复', '''
        flowchart TD
            A["确认目标和临时路径位于 silver 内"] --> B["备份并替换整个缺失表根"]
            B --> C["依次备份并安装全部触达日历叶"]
            C --> D["正式缺失表：契约、总量、分区集合及逐叶内容摘要"]
            D --> E["正式日历：数据集契约与触达叶内容摘要"]
            E --> F["全部验收通过；清理临时目录后返回提交数量"]
            B -. 失败 .-> R["倒序隔离新日历叶并恢复旧叶；再恢复缺失表"]
            C -. 失败 .-> R
            D -. 失败 .-> R
            E -. 失败 .-> R
            R --> S{"恢复完整？"}
            S -->|是| T["清理 staging、backup、隔离目录；抛出原异常"]
            S -->|否| U["保留恢复现场；抛出回滚异常"]
    '''),
    'c08-cli': ('main', '入口调度、运行边界与清理', '''
        flowchart TD
            A["检查人工确认参数；准备运行路径和批次时间"] --> B["运行开始日志；报告全量范围"]
            B --> C["报告生成开始；调用 build_full_audit_staging"]
            C --> D["汇总理论、实际、缺失键及 Session 数"]
            D --> E{"write？"}
            E -->|否| F["报告跳过提交；只读结果未落盘"]
            E -->|是| G["报告提交开始；调用 commit_full_audit"]
            G --> H["提交返回后报告 persisted=true"]
            F --> I["执行既有 finally 清理；报告正常结束"]
            H --> I
            C -. 异常 .-> X["按现有边界清理或保留现场；异常向上抛出"]
            G -. 异常 .-> X
            X --> Y["不报告正常结束"]
    '''),
}

cells['c08-imports'].source = cells['c08-imports'].source.replace('import tempfile\n', 'import tempfile\nimport time\n')
source = cells['c08-cli'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace_once('    temporary_directory = None', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_outcome = None

    temporary_directory = None''')
replace_once('''    click.echo(
        "missing_audit_scope=all_required_completed_1m_sessions; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        "minute_columns=contract_code,bar_at"
    )''', '''    click.echo(
        f"{log_boundary}\\n分钟主键缺失审计开始 / Missing audit run started\\n"
        "function=main()\\n"
        f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=run; status=started; "
        f"missing_audit_scope=all_required_completed_1m_sessions; run_id={run_id}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        f"minute_columns=contract_code,bar_at; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
    )''')
replace_once('''    try:
        audit_result = build_full_audit_staging(''', '''    try:
        log_build_started_at = time.perf_counter()
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=build_audit; status=started; "
            f"run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        audit_result = build_full_audit_staging(''')
replace_once('''            "missing_audit_total: "''', '''            f"missing_audit_total: table={MISSING_TABLE_NAME}; function=main; phase=build_audit; status=completed; "''')
replace_once('''            f"calendar_partitions={len(audit_result['calendar_digests'])}"''', '''            f"calendar_partitions={len(audit_result['calendar_digests'])}; "
            f"missing_partitions={len(audit_result['missing_digests'])}; persisted=false; "
            f"elapsed_s={time.perf_counter() - log_build_started_at:.3f}"''')
replace_once('''                "write=false; committed_missing_rows=0; "
                "committed_calendar_partitions=0"
            )
            return''', '''                f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=commit; status=skipped; "
                "write=false; committed_missing_rows=0; committed_calendar_partitions=0; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_outcome = "read_only"
            return''')
replace_once('''        commit_attempted = True''', '''        log_commit_started_at = time.perf_counter()
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=commit; status=started; "
            f"calendar_table={CALENDAR_TABLE_NAME}; missing_rows={audit_result['total_missing']}; "
            f"calendar_partitions={len(audit_result['calendar_digests'])}; run_id={run_id}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        commit_attempted = True''')
replace_once('''            "write=true; "
            f"committed_missing_rows={committed_missing_rows}; "
            f"committed_calendar_partitions={committed_calendar_partitions}"
        )''', '''            f"committed: table={MISSING_TABLE_NAME}; function=main; phase=commit; status=completed; "
            f"calendar_table={CALENDAR_TABLE_NAME}; write=true; persisted=true; run_id={run_id}; "
            f"committed_missing_rows={committed_missing_rows}; "
            f"committed_calendar_partitions={committed_calendar_partitions}; "
            f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"
        )
        log_outcome = "committed"''')
replace_once('''            shutil.rmtree(calendar_staging_path, ignore_errors=True)
''', '''            shutil.rmtree(calendar_staging_path, ignore_errors=True)
        if log_outcome is not None:
            click.echo(
                f"{log_boundary}\\n分钟主键缺失审计结束 / Missing audit run ended\\n"
                "function=main()\\n"
                f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=run; status=completed; "
                f"outcome={log_outcome}; write={str(write).lower()}; run_id={run_id}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
            )
''')
cells['c08-cli'].source = source

updated_cells = []
for cell in notebook.cells:
    if cell.id in flow_specs:
        suffix, title, body = flow_specs[cell.id]
        updated_cells.append(flow(f'b08-flow-{suffix}', f'### 局部流程：{title}', body))
    updated_cells.append(cell)
    if cell.id == 'c08-boundary':
        updated_cells.append(overview)
notebook.cells = updated_cells


class RemoveLogging(ast.NodeTransformer):
    def visit_Import(self, node):
        if len(node.names) == 1 and node.names[0].name == 'time':
            return None
        return node

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        if not node.body and not node.orelse:
            assert all(name.id.startswith('log_') for name in ast.walk(node.test) if isinstance(name, ast.Name))
            return None
        return node


old_code = [c for c in before.cells if c.cell_type == 'code']
new_code = [c for c in notebook.cells if c.cell_type == 'code']
assert [c.id for c in old_code] == [c.id for c in new_code]
for old, new in zip(old_code, new_code, strict=True):
    if old.id not in ('c08-imports', 'c08-cli'):
        assert old == new, old.id
    for key in ('metadata', 'outputs', 'execution_count'):
        assert old[key] == new[key], (old.id, key)
    assert ast.dump(RemoveLogging().visit(ast.parse(old.source))) == ast.dump(RemoveLogging().visit(ast.parse(new.source))), old.id
    comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
    assert comments(old.source) == comments(new.source), old.id
    compile(new.source, str(PATH), 'exec')
assert len(new_code) == len(flow_specs)
assert notebook.metadata == before.metadata
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print(f'Updated {PATH.name}: {len(descriptions)} explanatory cells; {len(flow_specs)+1} diagrams; business AST, comments and code cell state unchanged.')
