"""本轮 b03 说明、流程图和日志修改；不执行业务入口。"""

import ast
import copy
import hashlib
import io
import json
import pathlib
import tempfile
import textwrap
import tokenize


ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b03_futures_contract_calendar.ipynb"
before_bytes = NOTEBOOK.read_bytes()
notebook = json.loads(before_bytes)
before = copy.deepcopy(notebook)
cells_by_id = {cell["id"]: cell for cell in notebook["cells"]}


def markdown(cell_id, source):
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": textwrap.dedent(source).strip().splitlines(keepends=True),
    }


descriptions = {
    "c03-intro": """
        # b03_futures_contract_calendar

        生成 `dim_futures_contract_calendar`：每个固定月份合约—交易日—Session 一行。从 b02 已提交的品种日历与 JQData 合约规则展开理论 Session；理论时段不等于当天已经确认实际开市。事实采集白名单不参与本表筛选。

        阅读顺序：初始化与契约 → 水位与物理检查 → Session 解析校验 → 上游与来源读取 → 逐月生成 → 单分区提交 → 三模式入口。函数定义单元格只注册函数，实际调用顺序见总流程。Notebook 是唯一业务源，同名 `.py` 由默认 PythonExporter 生成。
    """,
    "c03-update-contract": """
        ## 更新范围与正式湖写入边界

        | 模式 | 请求与比较范围 | `--write` 的提交范围 |
        | --- | --- | --- |
        | 默认尾部更新 | b02 中晚于 b03 自动水位的新增品种日；没有新增时在认证前结束 | 仅替换有差异分区内本批新增品种—日期键，保留其他旧行 |
        | 成对显式日期 | 指定闭区间内的上游格点与本地行，双向比较缺失、修订和多余行 | 仅提交有差异分区，替换该日期区间并保留区间外行 |
        | `--full` | b02 当前全部格点与本地整表，双向比较 | 仅提交有差异的完整分区 |

        自动水位取正式行最大交易日与表根 `schema.parquet` 的 `automatic_tail_processed_through` 较大值。即使新增候选全部没有有效 `trade_time`、未生成 Session，成功的自动 `--write` 仍记录本批已处理日期；只读运行不推进水位。没有新增日期时直接结束，不改写标记。

        默认模式信任已提交历史，不按历史合约数重新求差或清退旧行。历史内部缺口和规则修订由显式日期或 `--full` 检查。差异比较排除 `updated_at`，仅时间戳不同不触发提交。

        三种模式均可在带 `--write` 时更新正式湖；b03 的显式日期写入是已确认的例外。日期必须成对，且与 `--full` 互斥。不带 `--write` 仍会在存在请求格点时访问来源并形成计划。正式根目录来自 `.env` 的 `FUTURES_LAKE_ROOT`，运行时通过 `settings.futures_lake_root` 读取。
    """,
    "c03-api-contract": """
        ## 原始 API 与转换边界

        - `get_all_securities(["futures"], date=None)` 请求完整期货证券目录。代码位于 DataFrame 索引；按固定月份代码和上市区间筛选与本批上游格点重叠的合约，不请求连续合约信息。
        - `get_futures_info(codes, fields=["contract_multiplier", "tick_size", "trade_time"])` 每批最多请求 200 个合约，检查字段和请求合约覆盖。乘数与 tick 是合约级标量；`trade_time` 包含历史生效区间。
        - 前一交易日从传入的有序交易日序列取得。当前入口传入本批请求日期序列，因此本批首日通过一次 `get_trade_days(end_date=首日, count=2)` 补齐边界，其余日期使用序列中前一日。
        - 没有当日有效规则的候选合约日跳过，并累计数量与至多 10 条样例，供入口输出 warning；规则格式非法或同日命中多条规则则抛出异常。

        请求格点为空时，来源函数直接返回空目录、空日期映射及零 API 计数，不认证。来源异常向调用方抛出，不自动重试。
    """,
    "c03-table-layout": """
        ## 逻辑表、物理分区与提交边界

        主键为 `contract_code, trading_date, session_number`，Hive 分区为 `exchange_code/year/month`。Schema、表名、主键与分区定义来自 `config.data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA`。

        每次提交合并一个完整叶分区：按精确品种—日期键、日期区间或整叶确定替换范围。替换后为空时移除该叶；全表无数据文件且缺少标记时保留 0 行 `schema.parquet`。自动更新成功后另行原子写入带已处理日期的标记。

        分区逐个提交，失败时只尝试恢复当前分区；此前已成功分区保留。自动水位标记在本批所需分区全部成功后写入，不属于跨分区共同回滚事务。
    """,
    "c03-initialization-heading": """
        ## 初始化与依赖

        按项目标记定位仓库根目录，随后导入配置、DataFrame 库和权威 Schema。执行本单元格不认证、不请求来源、不写湖。
    """,
    "c03-schema-browser-heading": """
        ## Schema 契约浏览

        交互式 Notebook 使用共享展示模块读取权威 Schema，并按选择展示有界数据样例。直接运行导出脚本时跳过展示；展示不调用业务 API，也不替代生产校验。
    """,
    "c03-parser-validation-heading": """
        ## Session 解析与本表业务校验

        `parse_session_text()` 将时段文本解析为起止钟点。`validate_contract_calendar_frame()` 先按权威 Schema 转换，再检查主键、固定月份合约身份、上市及规则有效期、Session 序号、时间戳、分钟数、夜盘属性和派生字段。

        当前调用位置包括生成结果、计划中的本地比较范围、提交输入及合并后完整叶。staging 和正式安装后的复读检查物理契约、主键和行数，不再次运行这套 Session 业务规则。`business_rows_by_key()` 生成排除 `updated_at` 的主键到业务值映射，用于双向比较。
    """,
    "c03-source-heading": """
        ## 正式上游读取与 JQData 合约元数据

        `read_trusted_variety_calendar()` 检查 b02 数据集及物理契约，按尾部或成对日期过滤；b02 已证明的主键、日期、`active_contract_count` 和派生语义直接信任，不在此重做业务校验。

        `collect_source_data()` 对非空请求执行认证、目录筛选、分批合约信息请求和前一交易日映射，返回本批合约目录与 API 计数。当前来源汇总由 `main()` 在函数返回后输出；函数内部尚无逐批进度日志。
    """,
    "c03-build-heading": """
        ## 逐月展开当前有效 Session

        `build_contract_calendar_partition()` 一次接收一个交易所—年月的品种日格点。对每个格点筛选上市中的固定月份合约，选择当日唯一有效规则，再逐段生成北京时间的 Session。

        20:00 及以后的起点归前一交易日，06:00 前的起点归前一交易日的下一自然日，其余归当前交易日；结束钟点不晚于起点时，结束日期再加一天。生成后完成本表业务校验，并返回无有效规则的统计。

        当前入口先逐分区生成并比较，只保存差异计划；写入时再生成 dirty 分区。这样不同时保存全历史 Session。本单元格描述现有计算顺序，不改变重复生成策略。
    """,
    "c03-commit-heading": """
        ## 单分区暂存、提交与失败回滚

        `commit_partition()` 依次执行替换范围检查、输入业务校验、读取旧叶、按模式合并、完整叶业务校验，再写 staging 并复读。staging 通过后备份旧叶、安装新叶或删除空叶，最后在正式路径检查物理 Schema、表身份 metadata、主键与行数。

        staging 失败时清理暂存目录并抛出异常。安装或正式验收失败时尝试移走当前目标并恢复备份；存在隔离内容或回滚不完整时，异常说明相应保留路径。此前已成功分区不参与本次回滚。

        当前仍使用本文件内的备份、安装和回滚实现，尚未接入共享路径事务模块。根水位标记由自动入口在分区工作成功后单独写入。
    """,
    "c03-cli-heading": """
        ## 命令行入口与三模式差异计划

        `main()` 负责参数门禁、正式数据集检查、水位与上游范围选择、差异计划及顺序提交。自动模式水位同时考虑正式最大交易日和已处理日期标记；显式日期与 `--full` 双向比较相应范围。

        比较时，本地范围的业务校验若抛出 `TypeError` 或 `ValueError`，会记录为 `quality_error` 并纳入替换计划；其他异常按原调用链抛出。没有差异时不提交叶；只读模式只输出计划。自动写入即使没有 dirty 叶，也在本批存在新增请求日期时更新已处理标记。

        日志沿用 b01、b02 的 `=` 入口边界，使用 `table/phase/status`，在已有分区进度点报告 `completed/total`，正常结束报告累计 `elapsed_s`。进度行保留 monitor 可识别的行首前缀；完成日志只在对应操作成功后输出，异常不会输出运行成功。来源汇总和批次提交汇总仍由入口负责。

        当前单元格在 Notebook 中只定义入口，不自动运行；直接运行同名 `.py` 时读取命令行参数并调用 `main()`。本轮不改写执行入口。
    """,
}
for cell_id, description in descriptions.items():
    cells_by_id[cell_id]["source"] = markdown(cell_id, description)["source"]

config_heading = markdown("b03-config-heading", """
    ## 表配置、物理契约与自动水位标记

    从权威 Schema metadata 读取表名、主键和分区，配置物理检查与 Session 解析所需常量。物理检查固定字段、类型、nullable 和表身份 metadata，允许描述性 metadata 随权威配置更新。

    水位读取函数只读取 `schema.parquet` 的已处理日期；标记或该键不存在时返回 `None`。水位写入函数先在同目录写临时标记，复读 Schema 后原子替换正式标记，并清理临时文件。函数定义本身不执行这些 I/O。
""")

FLOW_INIT = '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%'


def flow(cell_id, title, note, graph):
    return markdown(cell_id, f"{title}\n\n{note}\n\n```mermaid\n{FLOW_INIT}\n{textwrap.dedent(graph).strip()}\n```")


overview = flow("b03-flow-overview", "## 总流程：从品种日历到合约 Session", "下图描述运行入口的实际调用顺序。矩形为操作，菱形为分支；未展开的校验异常向调用方抛出。单元格中的函数定义不会自行启动采集。", """
    flowchart TD
        A["运行脚本入口并检查参数"] --> B["检查现有表；选择自动、显式或 full 范围"]
        B --> C["读取所需 b02 品种日"]
        C --> D{"自动模式且无新增？"}
        D -->|是| E["零 API 结束"]
        D -->|否| F["采集合约目录、规则及前一交易日映射"]
        F --> G["逐分区生成 Session；双向比较并汇总计划"]
        G --> H{"启用 write？"}
        H -->|否| I["输出只读结果；不写分区或水位"]
        H -->|是| J{"存在 dirty 分区？"}
        J -->|是| K["逐个重新生成、合并、提交并正式验收"]
        K --> L{"本批分区均成功？"}
        L -->|否| M["停止后续；按失败阶段清理或恢复当前叶；保留此前成功叶"]
        L -->|是| N{"自动模式？"}
        J -->|否| N
        N -->|是| O["原子写入本批已处理日期标记"]
        N -->|否| P["输出运行完成"]
        O --> P
""")

flows = {
    "c03-initialization": [flow("b03-flow-setup", "### 局部流程：初始化", "本单元格只准备依赖，后续函数在被调用时才执行。", """
        flowchart TD
            A["从当前目录向上搜索项目标记"] --> B{"找到项目根目录？"}
            B -->|否| C["抛出未找到项目根目录异常"]
            B -->|是| D["加入项目根与湖仓目录导入路径"]
            D --> E["导入配置、表格库和权威 Schema"]
    """)],
    "d36703b192451764": [config_heading, flow("b03-flow-config", "### 局部流程：配置、物理检查与水位", "配置在单元格执行时加载；以下三个分支分别表示检查或水位函数被调用后的行为，彼此不是顺序调用。任一检查失败均抛出异常。", """
        flowchart TD
            A["读取 metadata；定义常量与函数"] --> B["后续按需调用"]
            B --> C["物理检查：字段、类型、nullable、身份 metadata"]
            C --> D["逐 fragment 检查去掉 Hive 分区列后的物理 Schema"]
            B --> E{"读取水位：标记及日期键存在？"}
            E -->|否| F["返回 None"]
            E -->|是| G["解码 ISO 日期并返回；非法日期抛错"]
            B --> H["写水位：构造含已处理日期的空表 Schema"]
            H --> I["写同目录临时标记并复读 Schema"]
            I --> J["原子替换 schema.parquet"]
            J --> K["清理临时标记；写入异常也执行清理"]
    """)],
    "c03-schema-browser": [flow("b03-flow-schema-browser", "### 局部流程：Schema 与样例浏览", "展示只读权威 Schema 和所选有界样例，不发起业务采集。", """
        flowchart TD
            A["执行展示单元格"] --> B{"交互式 Notebook 环境？"}
            B -->|是| C["共享展示模块读取 Schema"]
            C --> D["按选择浏览字段说明和有界数据样例"]
            B -->|否| E["跳过展示"]
    """)],
    "c03-parser-validation": [flow("b03-flow-validation", "### 局部流程：Session 解析、业务校验与比较映射", "三个函数各有入口。完整业务校验失败通常抛出异常；main 对本地比较范围的 TypeError/ValueError 另有纳入修订计划的处理。", """
        flowchart TD
            A["解析时段文本"] --> B["匹配时段格式；转换起止钟点"]
            B --> C["返回原始文本和钟点"]
            D["校验合约日历 DataFrame"] --> E["按权威 Schema 转换；检查主键"]
            E --> F{"空表？"}
            F -->|是| G["返回空表"]
            F -->|否| H["检查合约身份、来源及日期有效范围"]
            H --> I["检查 Session 时间、分钟数、夜盘及派生字段"]
            I --> J["检查每合约日的序号连续与起点唯一"]
            J --> K["按主键排序并返回"]
            L["生成业务比较映射"] --> M["按 Schema 转换；排除 updated_at"]
            M --> N["返回主键到业务字段元组的映射"]
    """)],
    "c03-source": [flow("b03-flow-source", "### 局部流程：上游读取与来源采集", "先由入口调用上游读取函数，再把请求格点传给来源采集函数。空请求不认证；同步 API 的内部进度未在当前函数中打印。", """
        flowchart TD
            A["读取 b02：检查范围参数与数据集存在"] --> B["检查逻辑及 fragment 物理契约"]
            B --> C["按尾部或日期区间过滤并返回品种日历"]
            C --> D{"采集输入为空？"}
            D -->|是| E["返回空目录、空映射、零 API 计数"]
            D -->|否| F["认证；请求完整合约目录"]
            F --> G["校验目录；保留覆盖本批格点的固定月份合约"]
            G --> H["每批至多 200 个合约请求信息"]
            H --> I["检查字段与返回合约覆盖；合并目录"]
            I --> J{"请求包含传入日期序列首日？"}
            J -->|是| K["一次 get_trade_days 补前一交易日"]
            J -->|否| L["使用日期序列的前一日"]
            K --> M["构建日期映射；返回目录与 API 汇总"]
            L --> M
    """)],
    "c03-build": [flow("b03-flow-build", "### 局部流程：逐分区生成 Session", "每次只处理一个交易所—年月。无有效规则计数后跳过；格式错误或规则重叠抛出异常。样例 warning 由入口汇总输出。", """
        flowchart TD
            A["遍历本分区品种日"] --> B["找到品种目录；筛选上市中的合约"]
            B --> C["遍历候选合约；检查 trade_time 规则"]
            C --> D{"当日有效规则数量？"}
            D -->|零或缺失| E["计入无有效规则统计；继续下一合约"]
            D -->|多条| F["抛出规则重叠异常"]
            D -->|唯一| G["逐 Session 解析起止钟点"]
            G --> H["依据前一交易日、夜盘和跨午夜规则确定日期"]
            H --> I["构造北京时间戳、分钟数与完整行"]
            I --> J["继续其余 Session、合约及品种日"]
            E --> J
            J --> K["汇总分区行或构造空表"]
            K --> L["完整业务校验；返回结果与审计统计"]
    """)],
    "c03-commit": [flow("b03-flow-commit", "### 局部流程：单分区提交与失败恢复", "一次调用只负责一个叶分区。当前实现的物理复读与本地回滚路径保持原样，自动批次水位由 main 另行推进。", """
        flowchart TD
            A["检查替换参数、输入业务规则与范围"] --> B["检查现有表物理契约；读取当前旧叶"]
            B --> C["按精确键、日期区间或整叶合并"]
            C --> D["校验完整叶；构造 Arrow 表"]
            D --> E{"完整叶有行？"}
            E -->|是| F["写 staging；复读 Schema、主键及行数"]
            E -->|否| G["准备删除该叶"]
            F --> H["备份旧叶；安装新叶或移除空叶"]
            G --> H
            F -. staging 失败 .-> X["清理 staging；抛出异常"]
            H --> I["必要时创建全表空标记；正式叶复读"]
            I --> J{"安装与验收成功？"}
            H -. 安装异常 .-> R["尝试移走当前目标并恢复备份"]
            J -->|否| R
            R --> S["按实际恢复结果保留隔离或备份；抛出异常"]
            J -->|是| T["清理临时目录；返回本次替换输入行数"]
    """)],
    "c03-cli": [flow("b03-flow-main", "### 局部流程：三模式计划、提交与入口", "在 Notebook 执行本单元格只定义 main；脚本直接启动时才自动调用。比较排除 updated_at；本地比较范围的业务错误记入修订计划。", """
        flowchart TD
            A["调用 main；检查日期成对及 full 互斥"] --> B["打开现有表并检查物理契约"]
            B --> C{"运行模式？"}
            C -->|自动| D["取正式最大日与标记较大值；读取上游尾部"]
            C -->|显式日期| E["读取上游指定日期；纳入同期本地分区"]
            C -->|full| F["读取全部上游；纳入全部本地分区"]
            D --> G{"上游尾部为空？"}
            G -->|是| H["up_to_date；零 API 结束"]
            G -->|否| I["采集来源；按交易所年月分组"]
            E --> I
            F --> I
            I --> J["逐分区生成期望行；读取相应本地范围"]
            J --> K["比较缺失、变化、多余行；本地质量错误纳入计划"]
            K --> L["汇总 dirty 计划与无有效规则 warning"]
            L --> M{"存在 dirty 计划？"}
            M -->|否| N["自动 write 时写已处理标记；输出无差异并结束"]
            M -->|是| O{"启用 write？"}
            O -->|否| P["输出只读完成；结束"]
            O -->|是| Q["逐 dirty 分区重新生成并调用 commit_partition"]
            Q --> R["全部成功后，自动模式写已处理标记"]
            R --> S["输出提交汇总与运行完成"]
    """)],
}

new_cells = []
for cell in notebook["cells"]:
    new_cells.extend(flows.get(cell["id"], []))
    new_cells.append(cell)
    if cell["id"] == "c03-intro":
        new_cells.append(overview)
notebook["cells"] = new_cells

initialization = "".join(cells_by_id["c03-initialization"]["source"])
initialization = initialization.replace(
    "from datetime import date, datetime, time, timedelta, timezone\n",
    "from datetime import date, datetime, time, timedelta, timezone\nfrom time import perf_counter\n",
)
cells_by_id["c03-initialization"]["source"] = initialization.splitlines(keepends=True)
cli = "".join(cells_by_id["c03-cli"]["source"])


def replace_once(old, new):
    global cli
    assert cli.count(old) == 1, (old[:120], cli.count(old))
    cli = cli.replace(old, new, 1)


def finished_log(indent, outcome, rows, partitions, title):
    return textwrap.indent(textwrap.dedent(f'''\
        click.echo(
            f"{{log_boundary}}\\n{title}\\n"
            "function=main()\\n"
            f"planning_progress: table={{TABLE_NAME}}; phase=run; status=completed; "
            f"mode={{mode}}; write={{str(write).lower()}}; outcome={outcome}; "
            f"rows={rows}; partitions={partitions}; "
            f"elapsed_s={{perf_counter() - run_started_at:.3f}}\\n{{log_boundary}}"
        )
    '''), " " * indent)


replace_once(
    '    target_path = (\n        resolved_lake_root / "silver" / TABLE_NAME\n    )',
    '''    run_started_at = perf_counter()
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n运行入口开始 / Run entry started\\n"
        "function=main()\\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=started; "
        f"mode={'explicit' if has_explicit_dates else 'full' if full_refresh else 'automatic'}; "
        f"write={str(write).lower()}; lake_root={resolved_lake_root}\\n{log_boundary}"
    )

    target_path = (
        resolved_lake_root / "silver" / TABLE_NAME
    )''',
)
replace_once(
    "    # 默认水位同时信任正式行与成功自动批次 marker，不再按全历史合约数求差。",
    '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=completed; "
        f"dataset_exists={str(existing_contract_calendar_dataset is not None).lower()}; "
        f"elapsed_s={perf_counter() - run_started_at:.3f}"
    )

    # 默认水位同时信任正式行与成功自动批次 marker，不再按全历史合约数求差。''',
)
replace_once(
    'f"up_to_date: table={TABLE_NAME}; mode=automatic_tail; "',
    'f"up_to_date: table={TABLE_NAME}; phase=run; status=completed; mode=automatic_tail; "',
)
replace_once(
    '            return\n\n    # 对尾部/显式范围',
    finished_log(12, "up_to_date", "0", "0", "合约日历检查完成，无待更新数据 / Contract calendar is up to date")
    + '            return\n\n    # 对尾部/显式范围',
)
replace_once(
    'f"source_ready: mode={mode}; "',
    'f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; "\n'
    '        f"outcome=source_ready; mode={mode}; "',
)
replace_once(
    '"planning_progress: "\n                f"table={TABLE_NAME}; "\n                f"partitions={partition_number}/"\n                f"{len(partition_keys)}; "\n                f"key={partition_key}"',
    'f"planning_progress: table={TABLE_NAME}; phase=reconcile; status=running; "\n'
    '                f"completed={partition_number - 1}; total={len(partition_keys)}; "\n'
    '                f"partition={partition_key}; elapsed_s={perf_counter() - run_started_at:.3f}"',
)
replace_once(
    '        f"{plan_name}: table={TABLE_NAME}; "',
    '        f"{log_boundary}\\n分区差异计划已生成 / Partition reconciliation plan created\\n"\n'
    '        "function=main()\\n"\n'
    '        f"reconciliation_plan: table={TABLE_NAME}; phase=reconcile; status=completed; "\n'
    '        f"plan={plan_name}; mode={mode}; completed={len(partition_keys)}; total={len(partition_keys)}; "',
)
replace_once(
    '        f"{len(dirty_partition_plans)}"\n    )\n    if no_valid_rule_samples:',
    '        f"{len(dirty_partition_plans)}; elapsed_s={perf_counter() - run_started_at:.3f}\\n{log_boundary}"\n'
    '    )\n    if no_valid_rule_samples:',
)
replace_once(
    '            f"{no_valid_rule_samples}"',
    '            f"{no_valid_rule_samples}; table={TABLE_NAME}; phase=reconcile; status=warning"',
)
replace_once(
    '            "partition_plan: "\n            "partition="',
    '            f"partition_plan: table={TABLE_NAME}; phase=reconcile; status=planned; "\n'
    '            "partition="',
)
replace_once(
    '                "watermark_committed: automatic_tail_processed_through="',
    '                f"planning_progress: table={TABLE_NAME}; phase=watermark; status=completed; "\n'
    '                "outcome=watermark_committed; automatic_tail_processed_through="',
)
replace_once(
    '            f"source_unchanged: table={TABLE_NAME}; "\n            f"mode={mode}"',
    '            f"up_to_date: table={TABLE_NAME}; phase=reconcile; status=completed; "\n'
    '            f"outcome=source_unchanged; mode={mode}"',
)
replace_once(
    '        return\n    if not write:\n        return\n\n    committed_row_count = 0',
    finished_log(8, "source_unchanged", "0", "0", "来源比较完成，无分区差异 / Source comparison completed, no partition changes")
    + '        return\n    if not write:\n'
    + finished_log(8, "dry_run", "{expected_session_count}", "{len(dirty_partition_plans)}", "差异计划只读运行完成 / Reconciliation dry run completed")
    + '        return\n\n    committed_row_count = 0',
)
replace_once(
    '        f"committed: mode={mode}; "',
    '        f"committed: table={TABLE_NAME}; phase=commit; status=completed; mode={mode}; "',
)
replace_once(
    '        f"partitions={len(dirty_partition_plans)}"\n    )\n\n\nif "ipykernel"',
    '        f"partitions={len(dirty_partition_plans)}; elapsed_s={perf_counter() - run_started_at:.3f}"\n    )\n'
    + finished_log(4, "committed", "{committed_row_count}", "{len(dirty_partition_plans)}", "合约日历写入运行完成 / Contract calendar write run completed")
    + '\n\nif "ipykernel"',
)
cells_by_id["c03-cli"]["source"] = cli.splitlines(keepends=True)


class WithoutLogs(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == "click.echo":
            return None
        return self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module == "time" and [alias.name for alias in node.names] == ["perf_counter"]:
            return None
        return node

    def visit_Assign(self, node):
        if all(isinstance(target, ast.Name) and target.id in {"run_started_at", "log_boundary"} for target in node.targets):
            return None
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        return node if node.body else None


def code(nb):
    return "\n\n".join("".join(cell["source"]) for cell in nb["cells"] if cell["cell_type"] == "code")


def comments(source):
    return [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline) if token.type == tokenize.COMMENT]


before_code, after_code = code(before), code(notebook)
assert ast.dump(WithoutLogs().visit(ast.parse(before_code))) == ast.dump(WithoutLogs().visit(ast.parse(after_code)))
assert comments(before_code) == comments(after_code)
for cell in before["cells"]:
    if cell["cell_type"] == "code":
        current_cell = cells_by_id[cell["id"]]
        for key in cell.keys() - {"source"}:
            assert cell[key] == current_cell[key], (cell["id"], key)
        if cell["id"] not in {"c03-initialization", "c03-cli"}:
            assert cell["source"] == current_cell["source"], cell["id"]
assert before["metadata"] == notebook["metadata"]

snapshot_root = pathlib.Path(tempfile.mkdtemp(prefix="b03-doc-log-before-"))
(snapshot_root / NOTEBOOK.name).write_bytes(before_bytes)
(snapshot_root / NOTEBOOK.with_suffix(".py").name).write_bytes(NOTEBOOK.with_suffix(".py").read_bytes())
workflow_root = ROOT / "02_Futures_Lakehouse"
other_hashes = {
    str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
    for directory in workflow_root.glob("a0[1-4]_*")
    for path in directory.glob("*.py")
    if path != NOTEBOOK.with_suffix(".py")
}
(snapshot_root / "other_script_hashes.json").write_text(json.dumps(other_hashes, indent=2), encoding="utf-8")
NOTEBOOK.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
print(json.dumps({"snapshot_root": str(snapshot_root), "cells": len(notebook["cells"]), "flowcharts": 1 + len(flows), "business_ast_unchanged": True, "code_comments_unchanged": True}, ensure_ascii=False))
