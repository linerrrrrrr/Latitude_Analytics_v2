"""整理 a02/b01 说明、流程图和主流程日志；保留业务 AST、注释与单元格状态。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.ipynb"
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}

descriptions = {
    "147b61ed": """# b01 期货交易所报告采集日历

本环节读取 `dim_futures_variety_calendar`，为每个交易所—品种—交易日展开三类报告格点，生成 `dim_futures_exchange_report_calendar`。整个环节只处理本地 silver，不调用外部 API。

| 上下游 | 与本环节的关系 |
| --- | --- |
| a01/b02 品种日历 | 提供完整的品种交易日宇宙，决定当前有效报告格点。 |
| 共享事实采集政策 | `config/futures_lakehouse/futures_fact_collection_policy.py` 的白名单决定当前是否应采；白名单外格点仍保留。当前三类报告没有额外覆盖排除。 |
| a02/b02 成交持仓报告 | 消费 `position_rank`、`member_position` 两类格点，生产相应事实并回写采集、计数和质量状态。 |
| a02/b03 仓单 | 消费 `warehouse_receipt` 格点，生产仓单事实并回写状态。 |
| a02/b01a 特殊案例校准 | 位于本入口与 b02 之间，准备已配置案例的 raw 证据；不修改报告日历。b02 回写的校准 `success + warning` 由本入口继承。 |

`is_fetch_required` 表示当前采集义务，`is_fetch_completed` 表示持久完成凭证。政策排除只停止后续采集、清零当前缺失；已完成格点的批次、条数与质量证据继续保留，重新纳入时仍有效。

字段、主键、分区及状态解释以 `config/data_contracts.py` 为准，下方浏览器直接展示该契约。运行语义见湖仓根目录 `README.md` 和 `03_Futures_Database/AGENTS.md`。""",
    "93522332": """## 自动更新与写入边界

默认从完整上游生成期望报告日历，与现有报告日历逐分区比较；无差异时直接结束。空湖、尾部新增、内部缺口、上游格点撤销以及白名单或规则变化均由同一条路径处理。

| 模式 | 计算范围 | 写入边界 |
| --- | --- | --- |
| 默认自动模式 | 当前上游全部品种交易日及其三类报告格点。 | 不带 `--write` 只读；带 `--write` 提交存在差异的完整叶分区。 |
| 成对 `--start-date`、`--end-date` | 闭区间内的上游与现有报告格点；合并时保留范围外原有行。 | 可只读检查；写入只能指定与正式湖不同的 `--lake-root`。 |

正式湖根目录由 `settings.futures_lake_root` 读取。显式日期必须成对且起始日不晚于结束日；本入口没有 `--full` 参数，也不单独维护日期水位文件。

白名单缩小仍保留理论格点和已完成证据；上游品种交易日撤销则会移除相应报告格点。两种变化的含义不同。""",
    "96f73c42": """## 初始化与权威 Schema

按项目标记文件查找根目录，加载 `latitude` 环境中的 Pandas、PyArrow、Click，以及品种日历和报告日历两张具名 Schema、事实白名单和项目设置。此格只准备依赖，不展开日历或提交数据。""",
    "e53acac5": """## Schema 契约与有界样例

仅在交互式 Notebook 中展示：上游品种日历在前，本环节报告日历在后。共享浏览器读取权威 Schema；由于传入了 `lake_root`，还可按用户选择有界读取本地样例。浏览器不调用 API、不写入数据，也不启动本环节更新；普通脚本运行或模块导入跳过该展示。""",
    "db90e05b": """## 表身份、分区与状态列

两张表的表名、主键与 Hive 分区顺序在初始化时从各自 Schema metadata 读取一次，供路径、排序和校验复用。报告类型从 `dataset_name` 字段枚举读取。

| 类别 | 用途 |
| --- | --- |
| `POLICY_COLUMNS` | `is_fetch_required` 与中文原因；判断当前政策是否变化。 |
| `STATE_COLUMNS` | 完成凭证、采集结果、缺失标志、条数、质量与审计时间；在生成期望行时按状态继承规则处理。 |
| 物理契约与身份 | 字段类型、nullable、表名、主键及分区标识；描述性 metadata 以当前 Schema 为说明权威。 |

报告日历以 `dataset_name/exchange_code/year/month` 组织完整叶；上游分区和两张表的 Hive 字段类型均使用各自权威 Schema。""",
    "dbf59f85": """## 契约化读取与当前校验路径

`open_exact_dataset()` 确认路径下存在 Parquet，打开带 Hive 分区的 Dataset，补回分区字段后比较字段类型、nullable 与表身份。它返回 Dataset，具体列和日期过滤由调用方在 `to_table()` 时给出。描述性 metadata 的变化不要求改写历史文件。

| 函数 | 当前执行内容 |
| --- | --- |
| `partition_expression()` | 按权威分区顺序构造各字段等值条件，以 AND 连接。 |
| `validate_upstream_table()` | 转为品种日历契约，检查主键唯一和交易日与年月一致，再按主键排序。这里描述现有实现；上游业务语义的权威责任仍属于 a01/b02。 |
| `validate_report_calendar_table()` | 转为报告日历契约，检查主键、枚举、原因、年月、非负条数，以及采集义务、完成凭证、缺失和质量时间之间的关系。 |

完成状态只允许 `success` 或 `empty_confirmed` 且具有批次与完成时间；缺失标志只允许出现在当前应采、确认空且实际条数为零的格点。政策排除的已完成行保留历史状态，未完成免采行使用 `not_required / not_applicable`。""",
    "8a21e2d0": """## 生成期望报告格点与继承下游状态

`build_expected_calendar()` 将上游每个品种交易日展开为三类报告，按共享白名单设置当前采集义务与原因。新格点初始化为待采或免采状态；既有格点按主键匹配。

| 既有格点条件 | 状态处理 |
| --- | --- |
| 采集义务未变，或已有完成凭证 | 继承 `STATE_COLUMNS`；其中当前免采时清除当前缺失，应采且 `empty_confirmed` 时恢复缺失标志。 |
| 采集义务改变且从未完成 | 使用新义务对应的初始状态。 |
| 两个政策字段均未变 | 继续使用原 `updated_at`；新增或政策变化采用本批时间。 |

输出经过报告日历业务校验后返回。上游没有行时返回契约化空表；显式日期模式的范围外原有行由 `main()` 随后合并。""",
    "0bbe59a0": """## 内容摘要与完整叶差异计划

`table_digest()` 固定权威列顺序、类型、metadata 和主键排序，从 Python 标量重建 Arrow 缓冲区，再对 IPC 字节计算 SHA-256，消除不同 fragment 切片布局带来的摘要差异。

`changed_partition_keys()` 遍历期望与现有分区键的并集，比较每个完整叶的内容摘要。新增、内容变化和只存在于旧表的分区均进入提交计划；后者用于删除上游已撤销的叶。内容一致的叶不进入计划。

政策未变的行保留原 `updated_at`，使同一输入再次运行能够得到无差异结果。此计划表达日历需要同步的叶数，不能作为下游报告事实的 API 待办数量。""",
    "20aadb37": """## 完整叶 staging、正式复读与失败恢复

`commit_partitions()` 接收完整期望表与变更分区键。当前先验证完整期望表并计算摘要，在同一 silver 根目录下准备 staging、backup 和 failed 路径。

1. staging 写入零行 `schema.parquet` 和变更叶的完整期望内容；复读 staging 总行数，再逐叶核对业务约束、行数与内容摘要。
2. 期望全空时，整表替换为可读的零行数据集；其余情况逐叶备份旧目录、安装新叶，期望为空的旧叶只备份而不安装。
3. 正式安装后复读完整报告日历，检查业务约束、总行数与完整期望内容摘要，全部通过后返回写入的变更行数。
4. staging 准备失败时清理 staging 并抛出异常。安装或正式复读失败时，整表或已记录叶按原逻辑恢复；逐叶按安装逆序恢复。

当前实现恢复成功后清理 staging、backup、failed；恢复本身失败则保留三者并抛出带原始提交异常的错误。路径移动与恢复仍由本函数本地执行。多叶安装期间不保证其他读取者看到同一批次的原子快照。""",
    "637e05c1": """## CLI：读取、生成、差异计划与提交

`main()` 检查参数和正式写入边界，读取上游品种日历与现有报告日历。目标表不存在时使用契约化空表；显式日期只限制上游读取及参与重建的旧行，范围外旧行合并回完整期望表，因此同月其他日期也会保留。

生成期望表后计算变更叶和格点统计。`complete_report_grids` 表示与期望行逐值一致的日历格点数，不代表事实采集完成数；`removed_grids` 表示范围内已退出上游有效集合的旧格点数。

日志使用 88 个 `=` 的运行边界、中文阶段说明与 `table/function/phase/status` 字段，并以 `elapsed_s` 表示本次主流程累计秒数。读取、生成、规划和提交的阶段日志当前由 `main()` 输出；`reconciliation_plan:` 保留格点与分区统计，供现有 monitor 识别。正式复读通过后才输出提交成功；只读或无差异明确标记跳过提交。异常继续向上抛出，不输出正常完成日志。""",
    "abe75249": """## Notebook 与脚本运行入口

当前交互入口用 `main.main(args=[], standalone_mode=False)` 显式传入空参数，默认对正式湖执行只读自动计划，避开内核的 `-f` 参数。直接运行同名 Python 脚本时由 `main()` 读取 CLI 参数；添加 `--write` 才提交。

当前入口按 `ipykernel` 是否已加载选择分支，Notebook 内导入同名模块也可能进入只读计划。此处流程图描述当前判断，执行入口的进一步对齐留在对应改动项。""",
}

for cell_id, source in descriptions.items():
    cells[cell_id].source = source


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(
        title + '\n\n```mermaid\n'
        + '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        + textwrap.dedent(body).strip() + '\n```',
        id=cell_id,
    )


overview = flow("a02-b01-flow-overview", "## 总流程：报告日历同步", '''
    flowchart TD
        A["检查参数与正式写入边界"] --> B["读取品种日历和现有报告日历"]
        B --> C["逐品种日展开三类报告；应用白名单"]
        C --> D["继承完成和质量证据；合并范围外旧行"]
        D --> E["比较完整叶摘要；统计日历格点差异"]
        E --> F{"有变更分区？"}
        F -->|否| G["无差异结束"]
        F -->|是| H{"启用 --write？"}
        H -->|否| I["只读计划结束"]
        H -->|是| J["准备并复读 staging"]
        J --> K["整表置空或逐叶替换、删除"]
        K --> L["正式整表业务校验与摘要核对"]
        L -->|通过| M["清理临时路径；报告提交完成"]
        J -->|准备失败| N["清理 staging；抛出异常"]
        K -->|失败| O["恢复已移动目标；抛出异常"]
        L -->|失败| O
''')
flow_specs = {
    "bdd11053": ("init", "初始化", '''
        flowchart TD
            A["当前目录及其父目录"] --> B{"匹配项目标记文件？"}
            B -->|是| C["加入项目根与湖仓支撑模块路径"]
            B -->|否| D["抛出未找到项目根目录"]
            C --> E["导入库、两张 Schema、白名单与设置"]
    '''),
    "1472db9c": ("schema", "契约与有界样例展示", '''
        flowchart TD
            A{"交互内核且没有 __file__？"} -->|是| B["展示品种日历和报告日历 Schema"]
            B --> C["按选择有界读取本地样例"]
            A -->|否| D["跳过展示"]
    '''),
    "94323996": ("metadata", "表身份与分区初始化", '''
        flowchart TD
            A["两张权威 Schema"] --> B["读取表名、主键与分区 metadata"]
            A --> C["读取报告类型枚举"]
            B --> D["构造两张表的 Hive partitioning"]
            C --> E["政策列与状态列用于后续继承"]
            D --> F["后续路径、排序、校验复用"]
            E --> F
    '''),
    "5d05b12f": ("validation", "读取与当前校验路径", '''
        flowchart TD
            A["open_exact_dataset：发现 Parquet"] --> B["打开 Dataset；重建逻辑 Schema"]
            B --> C["检查物理字段和表身份"]
            C --> D["调用方按列及过滤条件物化"]
            D --> E{"数据对象？"}
            E -->|品种日历| F["契约转换；主键与年月检查"]
            E -->|报告日历| G["契约转换；主键与枚举检查"]
            G --> H["应采、完成、缺失、质量与时间关系"]
            F --> I["按对应主键排序后返回"]
            H --> I
    '''),
    "38ff53e0": ("build", "生成与状态继承", '''
        flowchart TD
            A["现有行按主键索引"] --> B["遍历上游品种日和三类报告"]
            B --> C["依据白名单设置义务及初始状态"]
            C --> D{"既有格点且义务未变或已完成？"}
            D -->|是| E["继承状态；按当前义务调整缺失标志"]
            D -->|否| F["保留初始状态"]
            E --> G{"既有两个政策字段都未变？"}
            F --> G
            G -->|是| H["保留原 updated_at"]
            G -->|否| I["使用本批 updated_at"]
            H --> J["汇集期望行；零行保留契约"]
            I --> J
            J --> K["校验报告日历后返回"]
    '''),
    "b14ee43f": ("diff", "内容摘要与分区差异", '''
        flowchart TD
            A["期望与现有分区键取并集"] --> B["逐键筛选两边完整叶"]
            B --> C["非空叶按主键排序并固定 Arrow 表示"]
            C --> D["IPC 字节计算 SHA-256；空叶记 None"]
            D --> E{"两边摘要一致？"}
            E -->|是| F["此叶保持原样"]
            E -->|否| G["加入变更键；含待删除的旧叶"]
            F --> H["遍历结束后返回变更键"]
            G --> H
    '''),
    "6f1d04d3": ("commit", "暂存、安装、复读与恢复", '''
        flowchart TD
            A{"变更键为空？"} -->|是| B["返回 0"]
            A -->|否| C["完整期望表校验与摘要；准备路径"]
            C --> D["写入 staging 标记和变更完整叶"]
            D --> E["staging 总行数与逐叶内容验收"]
            D -->|失败| X["清理 staging；抛出异常"]
            E -->|失败| X
            E -->|通过| F{"完整期望表为空？"}
            F -->|是| G["备份旧表根；安装零行表根"]
            F -->|否| H["逐叶备份；安装新叶或删除旧叶"]
            G --> I["正式整表业务、行数和摘要验收"]
            H --> I
            I -->|通过| J["清理临时路径；返回变更行数"]
            G -->|失败| K["尝试恢复旧表根或倒序恢复已记录叶"]
            H -->|失败| K
            I -->|失败| K
            K --> L{"恢复完成？"}
            L -->|是| M["清理临时路径；重抛提交错误"]
            L -->|否| N["保留 staging、backup、failed；抛出恢复错误"]
    '''),
    "40229d27": ("cli", "主流程与运行分支", '''
        flowchart TD
            A["校验成对日期、先后关系和写入目标"] --> B["读取上游；目标缺失时用空表"]
            B --> C["划分显式范围内外；生成范围内期望"]
            C --> D["合回范围外旧行；计算变更键与统计"]
            D --> E["输出 reconciliation_plan"]
            E --> F{"有差异且启用 --write？"}
            F -->|是| G["调用 commit_partitions；成功后报告提交"]
            F -->|否| H["报告无差异或只读跳过提交"]
            G --> I["输出正常运行完成边界"]
            H --> I
    '''),
    "03fe6466": ("entry", "当前 Notebook 与脚本入口", '''
        flowchart TD
            A{"ipykernel 已加载？"} -->|是| B["main.main；args 为空；standalone_mode=False"]
            B --> C["正式湖只读自动计划"]
            A -->|否| D{"__name__ 为 __main__？"}
            D -->|是| E["main 读取命令行参数"]
            D -->|否| F["普通模块导入不执行入口"]
    '''),
}

cells["bdd11053"].source = cells["bdd11053"].source.replace("import sys\n", "import sys\nimport time\n", 1)
source = cells["40229d27"].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new, 1)


replace_once('''    # 上游日历是本表有效水位的唯一业务来源；本入口不请求外部 API。''', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n交易所报告日历同步开始 / Report calendar run started\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={'explicit' if has_explicit_dates else 'automatic'}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
    )
    click.echo(
        "读取品种日历与现有报告日历 / Read calendar inputs\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=read_inputs; status=started; "
        f"upstream_table={UPSTREAM_TABLE_NAME}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )

    # 上游日历是本表有效水位的唯一业务来源；本入口不请求外部 API。''')
replace_once('''    if has_explicit_dates:
        in_scope_mask''', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=read_inputs; status=completed; "
        f"upstream_rows={len(upstream_df)}; existing_rows={len(existing_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )

    if has_explicit_dates:
        in_scope_mask''')
replace_once('''    updated_at = datetime.now(timezone.utc)''', '''    click.echo(
        "生成期望报告格点 / Build expected report calendar\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=generate; status=started; "
        f"upstream_rows={len(upstream_df)}; scoped_existing_rows={len(scoped_existing_df)}; "
        f"outside_scope_rows={len(outside_scope_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    updated_at = datetime.now(timezone.utc)''')
replace_once('''    partition_keys = changed_partition_keys(''', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=generate; status=completed; "
        f"expected_scope_rows={len(expected_scope_df)}; expected_full_rows={len(expected_full_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    click.echo(
        "比较完整叶并规划差异 / Plan calendar reconciliation\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=plan; status=started; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    partition_keys = changed_partition_keys(''')
replace_once('''    click.echo(
        f"table={TABLE_NAME}; mode={run_mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
''', '')
replace_once('''        "reconciliation_plan: "''', '''        f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; "
        f"mode={run_mode}; write={str(write).lower()}; "''')
replace_once('''        f"changed_partitions={len(partition_keys)}"''', '''        f"changed_partitions={len(partition_keys)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"''')
replace_once('''        click.echo("报告日历已经与上游和当前事实采集政策一致。")''', '''        click.echo(
            "报告日历已经与上游和当前事实采集政策一致。\\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=skipped; "
            f"reason=no_changes; committed_rows=0; committed_partitions=0; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"{log_boundary}\\n交易所报告日历同步完成 / Report calendar run completed\\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome=no_changes; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )''')
replace_once('''    if write:
        committed_rows''', '''    if write:
        click.echo(
            "提交报告日历完整叶 / Commit report calendar partitions\\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=started; "
            f"planned_partitions={len(partition_keys)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        committed_rows''')
replace_once('''            f"committed_rows={committed_rows}; "
            f"committed_partitions={len(partition_keys)}"''', '''            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=completed; "
            f"write=true; committed_rows={committed_rows}; "
            f"committed_partitions={len(partition_keys)}; persisted=true; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"''')
replace_once('''            f"dry_run_rows={len(expected_scope_df)}; "
            f"planned_partitions={len(partition_keys)}"''', '''            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=skipped; "
            f"reason=read_only; write=false; dry_run_rows={len(expected_scope_df)}; "
            f"planned_partitions={len(partition_keys)}; persisted=false; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"''')
source += '''

    click.echo(
        f"{log_boundary}\\n交易所报告日历同步完成 / Report calendar run completed\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
        f"outcome={'committed' if write else 'read_only'}; write={str(write).lower()}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
    )'''
cells["40229d27"].source = source

updated_cells = []
for cell in notebook.cells:
    if cell.id in flow_specs:
        suffix, title, body = flow_specs[cell.id]
        updated_cells.append(flow(f"a02-b01-flow-{suffix}", f"### 局部流程：{title}", body))
    updated_cells.append(cell)
    if cell.id == "93522332":
        updated_cells.append(overview)
notebook.cells = updated_cells


class RemoveLogging(ast.NodeTransformer):
    def visit_Import(self, node):
        if len(node.names) == 1 and node.names[0].name == "time":
            return None
        return node

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == "click.echo":
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith("log_") for t in node.targets):
            return None
        return self.generic_visit(node)


old_code = [c for c in before.cells if c.cell_type == "code"]
new_code = [c for c in notebook.cells if c.cell_type == "code"]
assert [c.id for c in old_code] == [c.id for c in new_code]
for old, new in zip(old_code, new_code, strict=True):
    if old.id not in ("bdd11053", "40229d27"):
        assert old == new, old.id
    for key in ("metadata", "outputs", "execution_count"):
        assert old[key] == new[key], (old.id, key)
    assert ast.dump(RemoveLogging().visit(ast.parse(old.source))) == ast.dump(RemoveLogging().visit(ast.parse(new.source))), old.id
    comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
    assert comments(old.source) == comments(new.source), old.id
    compile(new.source, str(PATH), "exec")
assert len(new_code) == len(flow_specs)
assert notebook.metadata == before.metadata
nbformat.validate(notebook)
with PATH.open("w", encoding="utf-8", newline="\n") as handle:
    nbformat.write(notebook, handle)
print(f"Updated {PATH.name}: {len(descriptions)} explanatory cells; {len(flow_specs)+1} diagrams; business AST, comments and code cell state unchanged.")
