"""b04 第 1—4 项：说明、流程图及入口日志；不执行业务入口。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b04_futures_bar_calendar.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}

descriptions = {
    'c04-introduction': '''
        # b04_futures_bar_calendar

        生成 `dim_futures_bar_calendar`：每个固定月份合约—交易日一个 `1d` 格点，每个合约—交易日—Session 一个 `1m` 格点。本表读取 b03 正式合约日历，不调用外部 API；同时承载 b05—b08 回写的采集、缺失检查和质量证据。

        阅读顺序：初始化与契约 → 上游边界及本表校验 → 分区读取 → 理论结构生成 → 差异与状态继承 → 单叶提交 → 运行入口。Notebook 是唯一业务源，同名 `.py` 由默认 PythonExporter 生成。
    ''',
    'c04-imports-notes': '''
        ## 初始化、权威契约与字段分工

        从当前目录向上搜索项目标记，导入配置与 `config.data_contracts` 的具名 Schema；表名、主键和 Hive 分区从 metadata 读取。正式湖根目录在运行时通过 `settings.futures_lake_root` 取得，唯一配置来源是 `.env` 的 `FUTURES_LAKE_ROOT`。

        `UPSTREAM_STRUCTURE_COLUMNS` 是 b03 的 12 列投影；`STRUCTURAL_COLUMNS` 是 b04 的 13 个结构字段，包含本表主键。其余字段属于 `STATE_COLUMNS`：结构一致时继承，新增或结构变化时初始化。物理类型及表身份仍以权威 Schema 为准，不另外定义字段契约。执行初始化单元格不请求来源、不写湖。
    ''',
    'c04-schema-browser-heading': '''
        ## Schema 契约浏览

        交互式 Notebook 使用共享展示模块，依次呈现上游合约日历和当前行情日历的权威 Schema，并提供有界只读样例。导出的命令行脚本跳过展示；浏览不调用业务 API、不写湖，也不代替生产校验。
    ''',
    'c04-validation-notes': '''
        ## 上游边界、结构比较与本表业务校验

        正式路径信任 b03 已提交的主键、Session 连续性及派生语义，只读取构造所需投影。`validate_contract_structure()`、`validate_contract_input()` 为独立 DataFrame 调用和迁移工具保留上游输入校验；它们不用于主流程重新证明 b03 的完整业务语义。

        `validate_structural_frame()` 检查生成或比较的 b04 结构，包括主键、频率、合约与交易所对应、日期分区、日线保留编号和分钟 Session。`assess_structural_partition()` 比较缺失、变化及多余行；本地结构业务校验失败时记入 `quality_error` 和 dirty 计划，物理不兼容则在 Dataset 打开阶段直接失败。

        `validate_bar_calendar_frame()` 检查结构及完整状态：枚举、说明文本、采集完成凭证、条数、缺失标记和质量时间。正式写入路径在合并出 dirty 完整叶后执行一次完整业务校验；staging 与正式安装后只复核文件契约、主键和行数。独立 `build_fresh_partitions()` 也对它返回的完整结果负责。本节描述当前校验边界，本轮不删减校验或转换。
    ''',
    'c04-dataset-notes': '''
        ## Dataset 发现与按分区读取

        `open_contract_dataset()` 在启动时各打开一次上下游根 Dataset，检查逻辑 Schema、逐个 fragment 的物理字段及稳定表身份。上游不存在时报错，下游尚未建立时返回 `None`。字段、类型、nullable、表名、主键和分区必须兼容；描述性 metadata 差异不构成历史叶重写理由。

        `partition_keys_from_files()` 从真实 Parquet 路径发现 Hive 键，排除根目录 `schema.parquet`。`read_partition()` 使用分区和可选日期过滤，只投影指定 Schema 的列。计划阶段读取结构列；写入前使用后面的 `read_existing_partition_leaf()` 精确复读当前 dirty 叶的完整状态，不在分区提交循环重新打开表根。
    ''',
    'c04-build-notes': '''
        ## 从合约 Session 生成两种频率的结构

        `build_structural_partitions()` 一次处理一个交易所—年月的 b03 投影，同时生成两种频率。分钟按 Session 保留时段信息，将 `minute_count` 作为 `expected_bar_count`；日线按合约日去重，使用 `session_number=0`、空 Session 字段和 `expected_bar_count=1`。两份生成结果分别执行结构校验。

        `initial_state()` 为新建或结构变化的行提供初始状态：日线等待 b05 采集，分钟先保持未选择，等待 b06 评估采集范围。b04 不导入事实白名单，也不把初始化解释为事实已完成。

        主流程计划阶段只生成结构，写入阶段再生成 dirty 范围并补状态，避免同时保存全历史完整结果。`build_fresh_partitions()` 是供独立调用的完整结果入口，另含上游输入及完整输出校验；本轮保留这两条调用路径。
    ''',
    'c04-merge-notes': '''
        ## 结构差异与既有状态继承

        `merge_preserving_state()` 按主键对齐新旧行，再逐列比较结构；只有主键存在且结构完全相同的行，才继承全部 `STATE_COLUMNS`，包括 `updated_at`。新增或结构变化的行使用 fresh 初始状态；期望集合之外的行不进入合并结果，是否删除由入口的更新范围决定。

        `assess_partition()` 合并状态并返回结构审计结果。比较只看结构，不因更新时间或事实状态本身变化而生成写入计划。计划阶段不读取完整状态；进入 dirty 写入阶段后重新读取当前叶，以取得 b05—b08 最新回写值。完整状态的业务规则仍由提交前的完整叶校验负责。
    ''',
    'c04-commit-notes': '''
        ## 单叶暂存、安装、正式复读与失败恢复

        `commit_partition()` 检查输入分区和替换范围，合并显式日期以外的保留行，再对 dirty 完整叶执行一次业务校验。非空结果写入 staging，复读文件 Schema/metadata、主键与行数后，才备份旧叶并安装新叶；空结果移除该叶。自动和 full 模式由调用方传入完整目标叶，显式日期写入只允许用于非正式湖。

        当前安装、备份、隔离和恢复仍由本文件实现，一次调用只负责一个叶及相关 `schema.parquet`。标记不存在时创建零行契约标记；已存在但与当前文件 Schema/metadata 不同的零行标记会备份后替换。这个标记不保存 b03 那样的已处理日期水位。

        正式安装后只复读当前叶的物理契约、主键和行数。失败时按已有路径恢复标记与旧叶，失败新叶保留隔离证据，恢复不完整时保留备份；此前成功叶不参与回滚。staging、安装和恢复实现保持原样，本轮仅整理说明和入口日志。
    ''',
    'c04-cli-notes': '''
        ## 三种运行模式、差异计划与顺序提交

        `main()` 先检查参数，再发现 Dataset 和分区。自动模式读取每个“频率—交易所”的目标尾叶，取最大 `trading_date`，仅生成并追加严格晚于水位的上游结构；目标缺少某一频率时，该频率从全部上游建立。full 和显式日期模式比较对应范围的上下游结构，并把缺失、修订及多余行汇总为 dirty 计划。

        没有 dirty 叶时结束；没有 `--write` 时只输出计划。写入时按基础分区重新生成结构、补初始状态，再精确读取当前 dirty 叶并继承状态。重新比较已变为 clean 的叶会跳过；其余逐叶提交，异常停止后续工作。自动尾部合并保留水位以内的历史行，显式日期提交保留日期范围外行。

        日志使用与 b01、b02 一致的 `=` 运行边界及 `table/phase/status` 字段。发现、计划和提交阶段报告数量与耗时；正常结束区分无需更新、只读计划和写入完成。计数与日志仍由当前 `main()` 负责，函数内部进度归位留待后续。异常沿原调用链抛出，不输出运行完成日志。

        当前代码单元格仍包含 `if __name__ == "__main__": main()`。直接运行 `.py` 使用命令行参数，普通导入不启动业务；Notebook 中若 `__name__` 为 `__main__`，执行本格也会调用 Click 并读取内核参数。本轮不改写执行单元格，实际运行可使用同名导出脚本。
    ''',
}

for cell_id, source in descriptions.items():
    cells[cell_id].source = textwrap.dedent(source).strip()

FLOW_INIT = '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%'


def markdown(cell_id, source):
    cell = nbformat.v4.new_markdown_cell(textwrap.dedent(source).strip())
    cell.id = cell_id
    return cell


def flow(cell_id, title, explanation, body):
    return markdown(cell_id, f'{title}\n\n{explanation}\n\n```mermaid\n{FLOW_INIT}\n{textwrap.dedent(body).strip()}\n```')


overview = flow('b04-flow-overview', '## 总流程：从合约日历到行情日历',
    '下图描述入口调用后的实际顺序。b04 只读本地 b03，不调用外部 API；各检查异常均向调用方抛出。', '''
    flowchart TD
        A["进入 main；检查日期、full 和正式写入门禁"] --> B["打开上下游 Dataset；发现分区并检查物理契约"]
        B --> C["选择自动尾部、显式日期或全历史范围"]
        C --> D["按月读取 b03 结构；生成 1d 与 1m 期望格点"]
        D --> E["比较结构；汇总 dirty 计划"]
        E --> F{"有 dirty 叶？"}
        F -->|否| G["记录无需更新；结束"]
        F -->|是| H{"启用 write？"}
        H -->|否| I["记录只读计划完成；结束"]
        H -->|是| J["重新生成结构及初始状态；复读当前完整叶"]
        J --> K["继承结构相同行的状态；重新判断差异"]
        K --> L{"仍为 dirty？"}
        L -->|否：跳过 clean| M["继续处理其余计划"]
        L -->|是| N["按模式保留旧行；单叶校验、暂存、安装及验收"]
        N -. 失败 .-> O["停止后续；按原路径恢复当前叶；保留此前成功叶"]
        N --> M
        M --> P["本批全部处理成功；记录完成与耗时"]
    ''')

scope = markdown('b04-update-contract', '''
    ## 更新范围、状态职责与正式湖写入边界

    | 模式 | 比较范围 | 写入边界 |
    | --- | --- | --- |
    | 默认尾部更新 | 按频率—交易所，只处理目标最大交易日之后的 b03 新结构 | `--write` 提交 dirty 完整叶，保留既有历史行 |
    | 成对显式日期 | 指定闭区间内的上下游结构 | 可只读检查；写入必须指定不同于正式湖的临时湖，保留区间外行 |
    | `--full` | 当前全部上游结构与全部本地分区 | `--full --write` 允许正式全历史维护，只提交差异叶 |

    日期必须成对，且与 `--full` 互斥。空目标由相同入口自然建表；内部缺口、旧日 Session 修订和目标孤儿叶由历史维护路径处理，默认尾部不扫描它们的业务内容。

    日线每个合约日一行，分钟每个 Session 一行；主键和 `bar_frequency/exchange_code/year/month` 分区来自权威 Schema。clean 叶不读取完整状态、不改变 `updated_at`。结构相同的行保留 b05—b08 回写值，新建或变化行初始化；b06 负责分钟事实白名单选择。
''')

flows = {
    'c04-imports': flow('b04-flow-setup', '### 局部流程：初始化与字段配置',
        '本格准备依赖、权威 Schema 投影和分区配置，不执行数据生成或写入。', '''
        flowchart TD
            A["从当前目录向上搜索项目标记"] --> B{"找到项目根？"}
            B -->|否| C["抛出定位异常"]
            B -->|是| D["加入导入路径；加载配置与权威 Schema"]
            D --> E["读取表名、主键和分区 metadata"]
            E --> F["定义结构列、状态列、枚举和 Hive partitioning"]
        '''),
    'c04-schema-browser': flow('b04-flow-schema-browser', '### 局部流程：Schema 与样例浏览',
        '展示使用已有共享模块，不替代采集和提交校验。', '''
        flowchart TD
            A{"交互式 Notebook 且无 __file__？"} -->|否| B["跳过展示"]
            A -->|是| C["加载共享 Schema 浏览模块"]
            C --> D["依次展示 b03 与 b04 权威契约"]
            D --> E["按用户选择读取有界样例；不写入"]
        '''),
    'c04-validation': flow('b04-flow-validation', '### 局部流程：校验与结构差异判断',
        '以下是不同调用边界，不是每次均顺序执行的三轮校验。正式上游读取不调用独立 DataFrame 的上游业务校验。', '''
        flowchart TD
            A["后续按函数职责调用"] --> B["独立上游输入：转换并检查合约 Session 结构"]
            A --> C["b04 结构校验：主键、频率、日期及 Session"]
            A --> D["dirty 完整叶：结构及完整状态规则"]
            C --> E["比较期望结构与现有结构"]
            E --> F{"结构相同且无质量错误？"}
            F -->|是| G["返回 clean"]
            F -->|否| H["汇总缺失、变化、多余行及 quality_error"]
            H --> I["返回 dirty 审计结果"]
            D --> J["完成凭证、缺失计数、质量时间等必须一致"]
            B --> K["返回规范化输入；校验异常向外抛出"]
            J --> L["返回完整叶；校验异常向外抛出"]
        '''),
    'c04-dataset-helpers': flow('b04-flow-dataset', '### 局部流程：发现 Dataset 与读取投影',
        '根 Dataset 在入口各打开一次；后续读取按分区键和日期过滤，投影所需列。', '''
        flowchart TD
            A["寻找实际 Parquet 文件"] --> B{"数据存在？"}
            B -->|否| C{"required 上游？"}
            C -->|是| D["抛出上游不存在异常"]
            C -->|否| E["返回 None；按空目标规划"]
            B -->|是| F["打开 Dataset；检查逻辑及 fragment 物理契约"]
            F --> G["从文件路径发现 Hive 键；排除 schema.parquet"]
            G --> H["按分区和可选日期过滤；投影指定列"]
            H --> I["按指定 Schema 转成 DataFrame"]
        '''),
    'c04-build': flow('b04-flow-build', '### 局部流程：理论结构与初始状态',
        '主流程计划阶段只需要结构；写入阶段补初始状态。独立完整结果入口另外承担输入及输出校验。', '''
        flowchart TD
            A["取得 b03 本月结构投影；排序"] --> B["分钟：逐 Session 保留；分钟数映射到预期条数"]
            A --> C["日线：按合约日去重；编号 0、空时段、预期 1 条"]
            B --> D["分别校验两种频率结构；返回结果"]
            C --> D
            D --> E{"调用方需要完整状态？"}
            E -->|否| F["用于结构计划比较"]
            E -->|是| G["补 initial_state；日线待采集，分钟待 b06 评估"]
            G --> H["主流程进入状态继承；独立 helper 校验完整输出"]
        '''),
    'c04-merge': flow('b04-flow-merge', '### 局部流程：按结构继承状态',
        '是否保留范围外旧行由 main 与提交函数决定；此处只说明期望格点的状态合并。', '''
        flowchart TD
            A["固定新旧完整表 Schema"] --> B["按主键左连接新旧行"]
            B --> C{"旧键存在且所有结构列相同？"}
            C -->|是| D["继承全部旧状态，包括 updated_at"]
            C -->|否| E["保留 fresh 初始状态"]
            D --> F["形成期望完整结果；固定 Schema 并排序"]
            E --> F
            F --> G["比较期望与现有结构；返回结果及 dirty 审计"]
        '''),
    'c04-commit': flow('b04-flow-commit', '### 局部流程：单叶提交与本地恢复',
        '本图对应现有本地事务实现；尚未接入共享安装模块。完整业务校验与物理复读是不同边界。', '''
        flowchart TD
            A["检查分区与替换范围；保留日期范围外行"] --> B["完整叶业务校验；转换 Arrow"]
            B --> C{"结果有行？"}
            C -->|是| D["写 staging；复读文件契约、主键和行数"]
            C -->|否| E["准备移除目标叶"]
            D -. 暂存失败 .-> X["清理 staging；抛出异常"]
            D --> F["备份旧叶；安装新叶或删除空叶"]
            E --> F
            F --> G["必要时创建或替换零行 schema.parquet"]
            G --> H["仅复读正式当前叶；检查物理契约、主键和行数"]
            F -. 安装失败 .-> R["按原路径恢复 marker 与当前叶"]
            G -. 标记失败 .-> R
            H -. 验收失败 .-> R
            R --> S["保留隔离证据；恢复不完整则保留备份并抛错"]
            H --> T["清理临时目录；返回提交输入行数"]
        '''),
    'c04-cli': flow('b04-flow-main', '### 局部流程：范围选择与写入分支',
        '自动模式按频率—交易所确定尾部；三种模式均先形成计划。入口调用仍保留在本代码单元格末尾。', '''
        flowchart TD
            A["检查成对日期、full 互斥及正式写入门禁"] --> B{"选择模式"}
            B -->|自动| C["读取各频率—交易所尾叶；选择尾月及后续月份"]
            B -->|日期| D["选择日期覆盖的上下游月份；过滤闭区间"]
            B -->|full| E["选择上下游全部月份及两种频率"]
            C --> F["生成结构；自动仅保留水位后的新日期"]
            D --> F
            E --> F
            F --> G["比较并汇总计划；记录数量与耗时"]
            G --> H{"有 dirty 叶且启用 write？"}
            H -->|否| I["记录无需更新或只读完成；结束"]
            H -->|是| J["重建 dirty 范围；复读当前完整叶并继承状态"]
            J --> K{"重新比较仍有差异？"}
            K -->|否| L["跳过 clean 叶"]
            K -->|是| M["按模式保留旧行；提交当前叶"]
            M -. 失败 .-> N["异常抛出；停止后续，不报运行成功"]
            M --> O["报告分区进度；处理剩余计划"]
            L --> O
            O --> P["全部处理成功；报告本批结果与总耗时"]
        '''),
}

new_cells = []
for cell in notebook.cells:
    if cell.id in flows:
        new_cells.append(flows[cell.id])
    new_cells.append(cell)
    if cell.id == 'c04-introduction':
        new_cells.extend([overview, scope])
notebook.cells = new_cells

source = cells['c04-cli'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace_once('    # 根 Dataset 各打开一次；规划循环只做列投影和分区过滤。', '''    log_boundary = "=" * 88
    log_mode = "explicit" if has_explicit_dates else ("full" if full else "automatic_tail")
    click.echo(
        f"{log_boundary}\\n行情日历运行开始 / Bar calendar run started\\n"
        "function=main()\\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\\n{log_boundary}"
    )
    # 根 Dataset 各打开一次；规划循环只做列投影和分区过滤。''')
replace_once('    discovery_started_at = time.perf_counter()\n', '''    discovery_started_at = time.perf_counter()
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=discovery; status=started")
''')
replace_once('    upstream_base_keys = set(upstream_partition_keys)\n', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=discovery; status=completed; "
        f"upstream_files={upstream_file_count}; target_files={target_file_count}; "
        f"upstream_partitions={len(upstream_partition_keys)}; target_partitions={len(target_partition_keys)}; "
        f"elapsed_s={discovery_elapsed:.3f}"
    )
    upstream_base_keys = set(upstream_partition_keys)
''')
replace_once('    # 按交易所—年月读取一次上游，再分别规划 1d 和 1m 分区。', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=plan; status=started; "
        f"mode={log_mode}; completed=0; total={len(base_keys)}; "
        f"candidate_partitions={len(candidate_partition_keys)}"
    )
    # 按交易所—年月读取一次上游，再分别规划 1d 和 1m 分区。''')
replace_once('''                f"table={TABLE_NAME}; "
                f"base_partitions={base_number}/{len(base_keys)}; "
                f"key={base_key}"''', '''                f"table={TABLE_NAME}; phase=plan; status=running; "
                f"completed={base_number - 1}; total={len(base_keys)}; "
                f"base_partitions={base_number}/{len(base_keys)}; key={base_key}; "
                f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"''')
replace_once('f"{plan_name}: table={TABLE_NAME}; "', 'f"{plan_name}: table={TABLE_NAME}; phase=plan; status=completed; mode={mode}; "')
replace_once('f"planning_timing: table={TABLE_NAME}; "', '''f"planning_progress: table={TABLE_NAME}; phase=plan; status=completed; "
        f"completed={len(base_keys)}; total={len(base_keys)}; "''')
replace_once('f"total_seconds={total_planning_elapsed:.3f}"', 'f"total_seconds={total_planning_elapsed:.3f}; elapsed_s={structure_planning_elapsed:.3f}"')
replace_once('''            "partition_plan: "
            f"partition={plan['partition_key']}; "''', '''            f"partition_plan: table={TABLE_NAME}; phase=plan; status=completed; "
            f"partition={plan['partition_key']}; "''')
replace_once('''        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return''', '''        click.echo(f"up_to_date: table={TABLE_NAME}; phase=plan; status=completed; mode={mode}")
        click.echo(
            f"{log_boundary}\\n行情日历无需更新 / Bar calendar up to date\\n"
            "function=main()\\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write={str(write).lower()}; outcome=up_to_date; "
            f"elapsed_s={time.perf_counter() - run_started_at:.3f}\\n{log_boundary}"
        )
        return''')
replace_once('''    if not write:
        return''', '''    if not write:
        click.echo(
            f"{log_boundary}\\n行情日历只读计划完成 / Bar calendar read-only plan completed\\n"
            "function=main()\\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write=false; outcome=read_only; dirty_partitions={len(dirty_partition_plans)}; "
            f"elapsed_s={time.perf_counter() - run_started_at:.3f}\\n{log_boundary}"
        )
        return''')
replace_once('    commit_elapsed_total = 0.0\n', '''    commit_elapsed_total = 0.0
    log_processed_partitions = 0
    log_skipped_partitions = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=started; "
        f"completed=0; total={len(dirty_partition_plans)}"
    )
''')
replace_once('''                click.echo(
                    f"commit_skipped_clean: partition={partition_key}"
                )''', '''                log_processed_partitions += 1
                log_skipped_partitions += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "
                    f"partition={partition_key}; outcome=skipped_clean; "
                    f"completed={log_processed_partitions}; total={len(dirty_partition_plans)}; "
                    f"elapsed_s={time.perf_counter() - run_started_at:.3f}"
                )''')
replace_once('''            commit_started_at = time.perf_counter()
            committed_partition_rows = commit_partition(''', '''            commit_started_at = time.perf_counter()
            click.echo(
                f"partition_start: table={TABLE_NAME}; phase=commit; status=started; "
                f"partition={partition_key}; completed={log_processed_partitions}; "
                f"total={len(dirty_partition_plans)}; rows={len(partition_commit_df)}"
            )
            committed_partition_rows = commit_partition(''')
replace_once('''            commit_elapsed_total += commit_elapsed
            click.echo(
                "commit_timing: "
                f"partition={partition_key}; "
                f"rows={committed_partition_rows}; "
                f"seconds={commit_elapsed:.3f}"
            )''', '''            commit_elapsed_total += commit_elapsed
            log_processed_partitions += 1
            click.echo(
                f"partition_committed: table={TABLE_NAME}; phase=commit; status=completed; "
                f"partition={partition_key}; completed={log_processed_partitions}; "
                f"total={len(dirty_partition_plans)}; rows={committed_partition_rows}; "
                f"elapsed_s={commit_elapsed:.3f}"
            )''')
replace_once('''        f"committed: mode={commit_mode}; "''', '''        f"committed: table={TABLE_NAME}; phase=commit_batch; status=completed; mode={commit_mode}; "
        f"completed={log_processed_partitions}; total={len(dirty_partition_plans)}; "
        f"skipped_clean_partitions={log_skipped_partitions}; "''')
replace_once('''        "remaining_pending=0"
    )''', '''        "remaining_pending=0"
    )
    click.echo(
        f"{log_boundary}\\n行情日历写入运行完成 / Bar calendar write run completed\\n"
        "function=main()\\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
        f"mode={mode}; write=true; outcome=committed; rows={committed_row_count}; "
        f"partitions={committed_partition_count}; skipped_clean_partitions={log_skipped_partitions}; "
        f"elapsed_s={time.perf_counter() - run_started_at:.3f}\\n{log_boundary}"
    )''')
cells['c04-cli'].source = source


class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            function = node.value.func
            if isinstance(function.value, ast.Name) and function.value.id == 'click' and function.attr == 'echo':
                return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(target, ast.Name) and target.id.startswith('log_') for target in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id.startswith('log_'):
            return None
        return self.generic_visit(node)


assert notebook.metadata == before.metadata
for original in before.cells:
    current = cells[original.id]
    assert original.metadata == current.metadata
    if original.cell_type != 'code':
        continue
    assert original.outputs == current.outputs and original.execution_count == current.execution_count
    comments = lambda s: [token.string for token in tokenize.generate_tokens(io.StringIO(s).readline)
                          if token.type == tokenize.COMMENT]
    assert comments(original.source) == comments(current.source), original.id
    a = ast.dump(RemoveLogging().visit(ast.parse(original.source)))
    b = ast.dump(RemoveLogging().visit(ast.parse(current.source)))
    assert a == b, f'非日志 AST 变化：{original.id}'
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook), encoding='utf-8', newline='\n')
print('Updated b04 Markdown, 9 diagrams and main logs; non-log AST, code comments and execution state unchanged.')
