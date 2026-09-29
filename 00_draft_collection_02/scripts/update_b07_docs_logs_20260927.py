"""本轮 b07 说明、流程图和日志样式迁移；不改变业务 AST。"""
from __future__ import annotations

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat


ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b07_suspected_session_reconciliation.ipynb"
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}

descriptions = {
    "d5f0fbb7": """
        # b07 疑似休市 Session 定向校对

        本环节读取本地 silver，以同一合约日的其他完整 Session 重聚合结果核对 JQData 日线，只向 `dim_futures_bar_calendar` 回写旁证、质量原因和审计时间。全部读取来自本地 Parquet，不调用行情 API。

        | 上下游 | 数据职责与本环节的关系 |
        | --- | --- |
        | b03 合约日历 | `dim_futures_contract_calendar` 提供合约日的 `tick_size`；Session 结构经 b04 进入行情日历。 |
        | b04 行情日历 | 提供理论 Session、理论条数及状态载体；结构不变时保留下游状态，结构修订时按 b04 规则重建安全初态。 |
        | b05 日线事实 | `fact_futures_daily` 提供同合约日唯一日线及 `has_market_data`，供旁证比较。 |
        | b06 分钟事实与日历回写 | `fact_futures_minute` 提供其他 Session 的正式分钟值；完成的零行 Session 在日历中形成 `formal_empty_session` 候选。 |
        | b07 当前输出 | 更新完整日历叶中的候选行，保留未触达行；不修改日线或分钟事实。 |
        | b08 人工全量缺失检查 | 独立核对理论与事实分钟主键，保留 b07 旁证与质量原因，不调用本环节。 |

        四类比较全部一致才记录 `reconciled`。它表示强旁证，不能确认交易所休市：当前 Session 仍是 `suspected_closed`，`is_fetch_required` 与完成/缺失计数保持原值，质量仍为 `warning`。证据不足或比较不一致时记录 `inferred + warning`。

        运行语义见湖仓根目录 `README.md` 与 `03_Futures_Database/AGENTS.md`；字段、主键、分区和状态含义以 `config/data_contracts.py` 为准。下方 Schema 浏览器直接展示该可执行契约。
    """,
    "5d698893": """
        ## 候选范围、运行方式与写入边界

        默认候选为 `bar_frequency=1m`、`is_fetch_required=true`、`schedule_status=suspected_closed`，且 `evidence_source=fact_futures_minute:formal_empty_session` 的行。校对并提交后证据来源变为 `c07:daily_vs_other_sessions:rule=c07-v3`，因此既有 b07 证据不会在日常重复校对；只读运行没有持久回写，下次仍可选中同一候选。

        | 方式 | 候选范围 | 写入规则 |
        | --- | --- | --- |
        | 默认运行 | b06 尚未被 b07 覆盖的新空 Session 证据 | 不带 `--write` 只计算与预览；带 `--write` 回写完整日历叶。 |
        | 成对日期或合约范围 | 默认候选与指定范围的交集；日期和合约同时提供时也取交集 | 可只读检查；定向写正式湖必须同时带 `--force`。 |
        | 有界 `--force` | 指定范围内仍 required 且疑似休市的 Session，包含已有旁证 | 无论只读或写入、正式或临时湖，都必须给出成对日期或至少一个非空 `--contract-code`。 |

        日期必须成对且起始不晚于结束；合约可重复指定，入口去除空白、转大写并去重。`--write` 只控制提交。正式湖来自 `settings.futures_lake_root`，`--lake-root` 可指定独立临时湖。b07 没有独立日期水位文件，增量边界由日历证据来源表达。

        默认 worker 在 b06 后执行本环节，`--skip-optional-quality` 可跳过 b07；b08 仍是人工独立入口。无候选时仅完成行情日历候选扫描，不打开合约、日线、分钟三张依赖表。
    """,
    "c07-schema-browser-heading": """
        ## Schema 契约与有界样例

        仅在交互式 Notebook 中展示四张直接依赖表的权威 Schema。浏览器从 `config/data_contracts.py` 读取定义；样例按用户选择有界读取本地湖。此单元格不执行旁证校对或数据提交，脚本运行时跳过展示。
    """,
    "2b0257d7": """
        ## 表身份、分区和旁证规则

        表名、主键与分区列均从四张具名 Schema 的 metadata 初始化；日线和分钟事实的分区列必须相同。行情日历按 `bar_frequency/exchange_code/year/month`，合约日历按 `exchange_code/year/month`，事实表按 `exchange_code/underlying_code/year/month` 定向读取。

        持久标识 `c07-v3` 和 `c07:daily_vs_other_sessions:` 保持稳定。它们标识旁证规则与来源，不是输入指纹；日常不会据此重新证明全历史。

        | 比较项 | 当前容差 |
        | --- | --- |
        | open / high / low / close | 每项绝对误差不超过 `tick_size / 2`，相对容差为 0。 |
        | volume、open_interest | 各自绝对容差 `1e-6`，相对容差为 0。 |
        | money | `np.isclose`：绝对容差 `0.01`，相对容差 `1e-6`。 |

        每次保存七项日线原值、七项分钟重聚合值和四类比较布尔值；证据不足时保留可取得的原值，比较布尔值留空。
    """,
    "2a85dca5": """
        ## 待提交完整日历叶的业务校验

        `validate_calendar_frame()` 在提交前校验每个 dirty 完整叶一次：先固定 Arrow 契约，再检查主键、状态枚举、分区派生值、Session 结构、完成凭证、缺失计数和旁证一致性，最后按主键排序返回。`reconciled` 必须仍属疑似休市，且四类比较均为真。

        这项校验由 `commit_calendar_partitions()` 调用；只读校对完成转换与旁证判断，不会调用提交前完整叶校验。staging 和正式安装后的复读检查文件契约、主键唯一性及行数，不再重复完整业务校验。
    """,
    "782170ec": """
        ## 数据集读取、物理契约与分区过滤

        `open_exact_dataset()` 确认表存在，以 Hive 分区打开并检查逻辑 Schema；已有 `schema.parquet` 时检查其文件契约，不创建标记。`reconstructed_schema()` 按权威字段顺序重建逻辑 Schema，`physically_and_identity_compatible()` 比较字段、类型、nullable 以及表名/主键/分区身份，允许描述性 metadata 差异。

        `partition_expression()` 把各分区列的等值条件以 AND 连接。每次物化前，`validate_read_fragments()` 只检查过滤条件可能读取的文件；随后由调用方读取数据。候选过滤减少返回行，但仍可能检查多个历史文件，不代表候选扫描完全没有历史文件 I/O。

        有候选后，入口按日历叶读取完整日历、对应合约月及候选品种月的日线/分钟事实。当前入口还保留候选与定向输入的主键检查；这里描述现状，本轮不调整检查或转换次数。
    """,
    "d9a63ecb": """
        ## 完整日历叶内的逐候选旁证计算

        `reconcile_partition()` 复制完整日历叶，只更新传入主键集合中的候选。每个候选先清空旧旁证，再检查当前零行完成状态、当日唯一正 `tick_size`、唯一有效日线，以及其他 Session 的可用性。另有疑似 Session 时无法归因；权威确认休市的其他 Session 可以排除，其余必须都是 required、scheduled 且完整的 Session。

        保留分钟按 `bar_at` 排序：开盘取首条、收盘取末条、最高/最低取极值、成交量和成交额求和、持仓量取最后一个非空值。`invalid_ohlc_mask()` 识别有限 OHLC 跨列异常；这类原值由上游保留，但本环节不能据此升级为 `reconciled`。直接使用的非空非有限证据值会抛错。

        | 判断 | 旁证与质量 |
        | --- | --- |
        | 证据不足、其他 Session 不完整或 OHLC 关系异常 | 保存可取得的原值，比较布尔值留空，记录 `inferred + warning` 及原因。 |
        | 证据完备，四类比较全部一致 | 保存比较结果，记录 `reconciled + warning`。 |
        | 证据完备，但至少一类比较不一致 | 保存真/假比较结果，记录 `inferred + warning` 及不一致项。 |

        函数返回更新后的完整叶、已处理候选子集和保留兼容性的固定第三返回值 0。候选的调度状态、拉取要求与完成/缺失计数不变；未触达行保持原值。`changed_candidates` 表示本次写入内存旁证和审计时间的候选数，尚不代表已落盘。
    """,
    "e492cfe6": """
        ## 完整叶暂存、安装与本批共同回滚

        `commit_calendar_partitions()` 接受触达叶的全部行。空映射直接返回 0；非空时，各叶先做一次完整业务校验并确认分区归属，再合并写入 staging。staging 检查当前完整 Schema/metadata，以及各叶主键唯一性和行数。

        所有暂存叶通过后，按分区顺序备份旧叶、安装新叶，并仅打开刚安装的正式叶复读物理/身份契约、主键和行数。返回的 `committed_calendar_rows` 是提交完整叶的总行数，包含同叶保留行，不能当作候选数量。当前实现不写独立日期水位，也不修改根级 `schema.parquet`。

        安装或正式复读失败时，按实际移动记录倒序恢复本次调用已经触达的所有叶，包括本批前面已安装的叶。恢复成功则清理临时目录并抛出原异常；恢复不完整则保留备份和隔离目录，异常给出现场路径。staging 写入或验收失败发生在正式替换前，只清理 staging 并抛错。
    """,
    "a006bfaa": """
        ## CLI 调度、结果预览与运行日志

        `main()` 依次完成参数门禁、候选扫描、依赖表定向读取、逐叶校对、最多 20 行预览，再按 `--write` 决定是否提交完整日历叶。无候选直接结束；只读计算完成与正式提交成功使用不同结果状态。

        日志沿用 b01、b02 的 88 个 `=` 运行边界，使用 `table/function/phase/status` 和秒数 `elapsed_s`。`reconciliation_plan:` 报告候选及旁证数量，`planning_progress:` 报告阶段与运行结果，`committed:` 只在提交返回后输出；这些前缀均由现有 worker 识别。`reconciled` 是 `warning` 的子集，两项计数不能相加。

        本轮统一入口现有日志及运行边界，函数级进度归位留待后续步骤。普通异常继续向上抛出，不输出成功结束日志。此单元格仍保留 Click 定义和原有 `main()` 执行方式；最后执行单元格的对齐尚未在本轮实施。
    """,
}
for cell_id, description in descriptions.items():
    cells[cell_id].source = textwrap.dedent(description).strip()


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(
        id=cell_id,
        source=(title + '\n\n```mermaid\n'
                '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
                + textwrap.dedent(body).strip() + '\n```'),
    )


overview = flow("b07-flow-overview", "## 总流程：从空 Session 到持久旁证", """
    flowchart TD
        A["启动；检查日期、合约及 force 写入范围"] --> B["扫描行情日历中的 required 疑似 Session"]
        B --> C{"是否 force？"}
        C -->|否| D["仅保留 b06 formal_empty_session 新证据"]
        C -->|是| E["在显式范围内包含已有旁证"]
        D --> F{"有候选？"}
        E --> F
        F -->|否| Z["无候选结束；不开其他三张表"]
        F -->|是| G["按日历叶读取完整日历及相关合约、日线、分钟叶"]
        G --> H["排除当前疑似 Session；聚合其他完整 Session"]
        H --> I["与日线比较；生成旁证、warning 和审计时间"]
        I --> J["汇总并预览；调度与完成状态保留"]
        J --> K{"启用 write？"}
        K -->|否| L["只读结束；旁证未持久化"]
        K -->|是| M["完整 dirty 叶校验；staging 写入及复读"]
        M --> N["逐叶备份、安装、正式叶复读"]
        N --> O["整批成功；持久旁证供后续读取"]
        N -. 安装或复读失败 .-> P["倒序恢复本批已移动叶；抛错"]
""")

flow_specs = {
    "b8d9e4f3": ("environment", "环境与权威依赖", """
        flowchart TD
            A["导入标准库"] --> B["从当前目录向上查找项目标记"]
            B --> C{"找到项目根？"}
            C -->|否| X["抛错停止"]
            C -->|是| D["加入项目根与湖仓模块路径"]
            D --> E["导入 DataFrame 库、四张 Schema、转换函数和 settings"]
    """),
    "c07-schema-browser": ("schema", "Schema 与样例展示", """
        flowchart TD
            A{"交互式 Notebook？"} -->|否| B["跳过展示"]
            A -->|是| C["调用共享 Schema 浏览器"]
            C --> D["展示四张权威 Schema；可选有界本地样例"]
    """),
    "e0a86c30": ("constants", "表身份与比较规则", """
        flowchart TD
            A["从 Schema metadata 读取表名、主键、分区列"] --> B{"日线与分钟分区一致？"}
            B -->|否| X["抛错停止"]
            B -->|是| C["构造四张表的 Hive partitioning"]
            C --> D["定义 c07-v3、四类比较容差和旁证列"]
            D --> E["定义调度、证据和质量状态集合"]
    """),
    "7d33cf49": ("validate", "待提交叶业务校验", """
        flowchart TD
            A["按权威 Schema 转换完整日历叶"] --> B["检查主键唯一性"]
            B --> C["逐行检查枚举、原因、分区派生值和 Session 结构"]
            C --> D["检查完成凭证、缺失计数和质量时间"]
            D --> E["reconciled 必须疑似休市且四类比较为真"]
            E --> F["按主键排序返回"]
            B -. 不满足约束 .-> X["抛错停止提交"]
            C -. 不满足约束 .-> X
            D -. 不满足约束 .-> X
            E -. 不满足约束 .-> X
    """),
    "595a964d": ("read", "按调用边界读取数据", """
        flowchart TD
            A["open_exact_dataset：确认存在并打开 Dataset"] --> B["重建逻辑 Schema；检查物理字段和稳定身份"]
            B --> C["已有 schema.parquet 时检查文件契约"]
            C --> D["返回 Dataset；暂不物化业务行"]
            D --> E["调用方构造候选条件或 partition_expression 等值条件"]
            E --> F["validate_read_fragments：检查可能读取的文件契约"]
            F --> G["调用方按过滤条件执行 to_table"]
            B -. 不兼容 .-> X["抛错"]
            C -. 不兼容 .-> X
            F -. 不兼容 .-> X
    """),
    "ea42f696": ("reconcile", "逐候选生成旁证", """
        flowchart TD
            A["复制完整日历叶；选出候选主键"] --> B["逐候选取同合约日数据；清空旧旁证"]
            B --> C["检查零行完成状态、tick_size 和有效日线"]
            C --> D["排除当前 Session 和其他权威休市 Session"]
            D --> E["检查其余 Session 状态、完整条数及 OHLC 关系"]
            E --> F["聚合分钟；保存可取得的七项日线与聚合值"]
            F -. 非空非有限证据值 .-> X["抛错停止"]
            F --> G{"证据与前提完备？"}
            G -->|否| H["inferred + warning；比较值留空"]
            G -->|是| I["按容差比较 OHLC、成交量、金额、持仓量"]
            I --> J{"四类全部一致？"}
            J -->|是| K["reconciled + warning"]
            J -->|否| L["inferred + warning；记录不一致项"]
            H --> M["更新证据来源与审计时间；保留调度和完成状态"]
            K --> M
            L --> M
            M --> N{"还有候选？"}
            N -->|是| B
            N -->|否| O["契约转换与排序；返回完整叶、候选子集、0"]
    """),
    "96342530": ("commit", "暂存、安装与共同恢复", """
        flowchart TD
            A{"有待提交分区？"} -->|否| B["返回 0"]
            A -->|是| C["每个完整叶业务校验一次；确认分区归属"]
            C --> D["合并写 staging；检查 Schema、主键唯一性和行数"]
            D -. 失败 .-> E["清理 staging；抛错"]
            D --> F["逐叶备份旧目录；登记移动记录；安装新叶"]
            F --> G["仅复读当前正式叶的契约、主键和行数"]
            G --> H{"所有叶完成？"}
            H -->|否| F
            H -->|是| I["清理临时目录；返回完整叶总行数"]
            F -. 失败 .-> R["按移动记录倒序隔离新叶、恢复旧叶"]
            G -. 失败 .-> R
            R --> S{"恢复完整？"}
            S -->|是| T["清理临时目录；抛出原异常"]
            S -->|否| U["保留备份与隔离目录；报告路径并抛错"]
    """),
    "57c9061e": ("main", "入口调度与日志", """
        flowchart TD
            A["Click 参数门禁；运行开始日志"] --> B["只打开行情日历并过滤候选"]
            B --> C{"有候选？"}
            C -->|否| D["报告 0 候选；结束日志"]
            C -->|是| E["打开三张依赖表；按日历叶读取相关分区"]
            E --> F["调用 reconcile_partition；汇集完整叶和候选结果"]
            F --> G["报告旁证计数；预览最多 20 行"]
            G --> H{"write？"}
            H -->|否| I["报告跳过提交；只读结束日志"]
            H -->|是| J["提交开始日志；调用 commit_calendar_partitions"]
            J --> K["提交成功日志；运行结束日志"]
            J -. 异常 .-> L["异常向上抛出；无成功结束日志"]
    """),
}

cells["b8d9e4f3"].source = cells["b8d9e4f3"].source.replace("import sys\n", "import sys\nimport time\n")
source = cells["57c9061e"].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace_once('    silver_root = resolved_lake_root / "silver"', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_mode = "force" if force else "new_candidates"
    click.echo(
        f"{log_boundary}\\n疑似休市校对开始 / Reconciliation run started\\n"
        "function=main()\\n"
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}; "
        f"contract_codes={requested_contract_codes}\\n{log_boundary}"
    )
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; "
        "phase=candidate_scan; status=started"
    )
    silver_root = resolved_lake_root / "silver"''')


def run_end(outcome, indent):
    return textwrap.indent('''click.echo(
    f"{log_boundary}\\n疑似休市校对结束 / Reconciliation run ended\\n"
    "function=main()\\n"
    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=completed; "
    f"outcome=OUTCOME; mode={log_mode}; write={str(write).lower()}; "
    f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
)'''.replace("OUTCOME", outcome), " " * indent)


replace_once('''            "selected_candidates=0; changed_candidates=0; "
            "committed_partitions=0"''', '''            f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; "
            "phase=candidate_scan; status=completed; selected_candidates=0; changed_candidates=0; "
            "committed_partitions=0"
'''.rstrip())
replace_once('''        return

    # 3. 有候选时再打开三张直接依赖表；要求物理结构和稳定表身份兼容。''', run_end("no_candidates", 8) + '''
        return

    click.echo(
        f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; "
        f"phase=candidate_scan; status=completed; selected_candidates={len(candidate_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    log_reconcile_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; "
        "phase=reconcile; status=started"
    )
    # 3. 有候选时再打开三张直接依赖表；要求物理结构和稳定表身份兼容。''')
replace_once('''        f"selected_candidates={len(candidate_df)}; "''', '''        f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; phase=reconcile; status=completed; "
        f"selected_candidates={len(candidate_df)}; "''')
replace_once('''        f"force={force}"''', '''        f"force={str(force).lower()}; persisted=false; "
        f"elapsed_s={time.perf_counter() - log_reconcile_started_at:.3f}"''')
replace_once('''        click.echo(
            changed_df.loc[:, preview_columns]''', '''        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=preview; "
            f"status=completed; rows={min(20, len(changed_df))}; total_candidates={len(changed_df)}"
        )
        click.echo(
            changed_df.loc[:, preview_columns]''')
replace_once('''        click.echo("write=false; committed_partitions=0")
        return''', '''        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; "
            "status=skipped; write=false; committed_partitions=0"
        )
''' + run_end("read_only", 8) + '''
        return''')
replace_once('''    committed_rows = commit_calendar_partitions(''', '''    log_commit_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; status=started; "
        f"partitions={len(partition_frames)}; changed_candidates={len(changed_df)}"
    )
    committed_rows = commit_calendar_partitions(''')
replace_once('''        f"write=true; committed_partitions={len(partition_frames)}; "
        f"committed_calendar_rows={committed_rows}"''', '''        f"committed: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; status=completed; "
        f"write=true; persisted=true; committed_partitions={len(partition_frames)}; "
        f"committed_calendar_rows={committed_rows}; "
        f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"''')
replace_once('\n\n\nif __name__ == "__main__":', '\n' + run_end("committed", 4) + '\n\n\nif __name__ == "__main__":')
cells["57c9061e"].source = source

updated_cells = []
for cell in notebook.cells:
    if cell.id in flow_specs:
        suffix, title, body = flow_specs[cell.id]
        updated_cells.append(flow(f"b07-flow-{suffix}", f"### 局部流程：{title}", body))
    updated_cells.append(cell)
    if cell.id == "5d698893":
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
    if old.id not in ("b8d9e4f3", "57c9061e"):
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
print(f"Updated {PATH.name}: {len(descriptions)} explanations; {len(flow_specs) + 1} diagrams; business AST, comments and code cell state unchanged.")
