#!/usr/bin/env python
# coding: utf-8

# # b03 JQData 期货仓单日报
# 
# 从交易所报告日历选择未完成的仓单格点，每个交易所—品种—交易日串行查询一次 JQData `finance.FUT_WAREHOUSE_RECEIPT`。保存逐仓库数量、来源计量单位及较昨日变化，按完整品种月事实叶提交，再回写报告日历。
# 
# | 上下游 | 与本环节的关系 |
# | --- | --- |
# | a02/b01 报告日历 | 提供 `warehouse_receipt` 候选格点、当前采集义务和持久完成快照；本环节只回写仓单状态。 |
# | 共享事实采集政策 | 白名单由上游日历生产者应用；这里消费其 required 标记，不再定义另一份名单。 |
# | a02/b01a、a02/b02 | 运维顺序上位于本环节之前；仓单不读取其特殊案例 raw 或成交持仓事实，也不修改两类排名日历行。 |
# | JQData 共享连接 | 只有存在待办才认证；认证和 Windows TUN 出口由 `config/jqdata_connection.py` 承担。 |
# | `fact_futures_warehouse_receipt_daily` | 仓单事实输出；粒度为日期—交易所—品种—仓库，标准读取 Demo 和后续研究按权威 Schema 消费。 |
# | operations | 按人工选定批次启动本入口，识别进度与分区耗时日志；本入口不启动未来批次。 |
# 
# 字段、主键和分区来自 `config/data_contracts.py`；运行语义见湖仓根目录 `README.md`、`AGENTS.md` 与 `03_Futures_Database/AGENTS.md`。本入口没有独立日期水位文件。

# ## 自动更新与写入边界
# 
# 待办直接定义为：`dataset_name=warehouse_receipt AND is_fetch_required=true AND is_fetch_completed=false`。正式完成快照可信，启动时不从 clean 事实历史重新证明或修复完成状态；事实为空也不会自动推翻已完成日历。
# 
# | 运行方式 | 行为 |
# | --- | --- |
# | 默认不带参数 | 读取日历快照形成全部待办；有待办则认证、请求和转换，不提交事实或状态。 |
# | `--write` | 按品种月份合并完整事实叶并提交，再生成和提交仓单日历状态。 |
# | 成对日期 | 只过滤当前未完成待办，不重新审计已完成历史；只读可用，写入必须选择非正式测试湖。 |
# | 没有待办 | 依据日历快照报告无需采集并退出，不认证 JQData。 |
# | 可选性能门槛 | 两参数必须成对，仅允许无显式日期的自动正式 `--write`；不默认启用。 |
# 
# `--start-date` 与 `--end-date` 必须同时提供；没有 `--full`。正式湖根目录来自 `settings.futures_lake_root`。空湖必须先由上游 b01 提供报告日历，仓单事实由同一待办路径自然建表。
# 
# 事实叶先提交，日历叶随后逐个提交；每个当前叶通过共享模块独立安装和恢复，事实与日历不共同回滚。日历失败时已提交事实保留，未完成状态仍会形成下一次人工启动的待办。查询或转换失败时，带 `--write` 会尝试回写该格点的失败状态，然后停止；不会自动重试。

# ## 总流程：仓单采集与状态回写
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["参数、正式写入与性能门槛边界"] --> B["读取 required 仓单日历快照"]
#     B --> C{"存在未完成格点？"}
#     C -->|否| D["无需采集；不认证"]
#     C -->|是| E["认证；按交易所、品种、年月分组"]
#     E --> F["逐待办日请求一次；归一化来源"]
#     F --> G{"启用 --write？"}
#     G -->|否| H["只读汇总；进入下一分区"]
#     G -->|是| I["读当前叶；复核待办；生成完整事实叶"]
#     I --> J["事实单叶共享事务；正式物理契约与摘要复读"]
#     J --> K["生成 success 或 empty_confirmed；保留单位缺失 warning"]
#     K --> L["日历逐叶共享事务；正式摘要复读"]
#     L --> M["记录成功分区总耗时；必要时检查性能门槛"]
#     M --> N{"门槛允许继续？"}
#     N -->|是| O["后续分区或运行结束"]
#     H --> O
#     N -->|否| P["停止；已提交分区保留"]
#     F -. 失败 .-> Q{"启用 --write？"}
#     Q -->|是| R["读取当前叶并回写该格点失败状态；仍未完成"]
#     Q -->|否| P
#     R --> P
#     J -. 提交失败 .-> P
#     L -. 提交失败 .-> P
# ```

# ## 初始化与共享依赖
# 
# 按 `.env.template` 的三个项目标记定位根目录，导入两张权威 Schema、统一转换入口、JQData 共享认证、项目设置和湖仓共享路径事务。本格只加载依赖，不认证、不请求或写入。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
#     B -->|是| C["加入项目和湖仓模块路径；加载契约与配置"]
#     B -->|否| D["抛错"]
#     C --> E["仅加载定义；不请求或写入"]
# ```

# In[ ]:


from __future__ import annotations

import hashlib
import math
import pathlib
import shutil
import statistics
import sys
import time
import uuid
from datetime import date, datetime, timezone
from types import ModuleType

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 仅在交互内核且没有 `__file__` 时调用共享浏览器，依次展示报告日历和仓单事实。字段解释来自权威 Schema；传入 `lake_root` 后支持显式查看有界本地样例，不能把本格描述成完全不读取数据湖。
# 
# 展示不认证 JQData、不调用来源 API、不写文件；普通脚本执行跳过展示。

# ### 局部流程：Schema 与样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且无 __file__？"} -->|否| B["跳过展示"]
#     A -->|是| C["展示两张表的权威 Schema"]
#     C --> D["用户显式选择时读取有界本地样例"]
#     D --> E["不请求来源或写入文件"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、Hive 分区与来源约定
# 
# 表名、主键和分区列各从 Schema metadata 读取一次，Hive partitioning 在此建立。事实叶为 `exchange_code/underlying_code/year/month`；日历叶为 `dataset_name/exchange_code/year/month`，同一叶可包含其他品种，回写时必须保留。
# 
# `CALENDAR_PLANNING_COLUMNS` 限定启动规划投影，`JQDATA_FIELDS` 固定请求字段。交易所别名仅用于复核来源，输出身份仍继承日历；数量、单位和变化均保留来源语义。

# ### 局部流程：契约常量
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两张权威 Schema"] --> B["一次读取表名、主键、分区列"]
#     B --> C["构建 Hive partitioning"]
#     C --> D["固定规划列、来源列、来源标签与交易所别名"]
# ```

# In[ ]:


# 两张表的物理契约只从权威 Schema metadata 各读取一次。
CALENDAR_TABLE_NAME = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货交易所报告采集日历维度表。
CALENDAR_PRIMARY_KEY = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 报告类型—交易所—品种—交易日格点。
CALENDAR_PARTITION_COLUMNS = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 报告日历 Hive 叶分区顺序。

TABLE_NAME = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货逐仓库日频仓单事实表。
PRIMARY_KEY = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 日期—交易所—品种—仓库业务主键。
PARTITION_COLUMNS = FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 仓单事实 Hive 叶分区顺序。

GRID_COLUMNS = [
    "exchange_code",  # 项目/JQData 标准交易所代码。
    "underlying_code",  # 期货品种代码。
    "trading_date",  # 仓单报告归属交易日。
]
CALENDAR_PLANNING_COLUMNS = [
    *GRID_COLUMNS,
    "is_fetch_completed",
    "year",
    "month",
]
SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]

# 显式选择业务转换和来源复核需要的列；不依赖整表隐式返回顺序。
JQDATA_FIELDS = [
    "day",  # 来源报告日期。
    "exchange",  # 来源交易所代码，仅用于复核日历。
    "underlying_code",  # 来源品种代码。
    "warehouse_name",  # 仓库或统计地点名称。
    "warehouse_receipt_number",  # 当日仓单数量。
    "unit",  # 来源计量单位。
    "warehouse_receipt_number_increase",  # 相对昨日增减。
]
SOURCE = "JQData_finance_FUT_WAREHOUSE_RECEIPT"

# 响应可能使用项目后缀或交易所常用简称；输出始终继承上游项目代码。
JQDATA_RESPONSE_EXCHANGES = {
    "CCFX": {"CCFX", "CFFEX"},
    "XDCE": {"XDCE", "DCE"},
    "XZCE": {"XZCE", "CZCE", "ZCE"},
    "XSGE": {"XSGE", "SHFE"},
    "XINE": {"XINE", "INE", "SHFE"},
    "GFEX": {"GFEX"},
}

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
FACT_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 零行标记与启动日历物理契约
# 
# 根级 `schema.parquet` 必须是零行且满足物理字段、类型、nullable 和表名/主键/分区身份 metadata。一次读取文件 metadata，同时取得行数和 Arrow Schema，不再为同一标记重复打开 footer。日历根标记必须存在；事实表可以尚未创建，但已经有内容的根目录不能缺少标记。
# 
# `open_planning_calendar_dataset()` 在启动时核对日历 Dataset 和各 fragment 的物理契约；随后 `main()` 只读取 required 仓单的规划列。描述性 metadata 以当前配置为准，不要求历史文件重写；这里不重新验证上游完整业务语义。
# 
# 标记读取和日历打开函数自行报告开始、空表分支、物理契约核对及完成。fragment 进度沿既有循环累计，至多每 2 秒报告一次，不另扫目录统计总数。日历规划列的实际读取仍由 `main()` 报告。

# ### 局部流程：启动物理契约
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查根级零行标记"] --> B{"标记存在且契约兼容？"}
#     B -->|是| C["打开日历 Dataset；重建 Hive 字段核对身份"]
#     B -->|否| D["日历或已有内容报错；允许未创建的事实表"]
#     C --> E["沿原 fragment 循环核对契约并报告进度"]
#     E --> F["函数报告打开完成；main 读取并报告规划行数"]
# ```

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威字段顺序重建后再比较整表 metadata。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少权威 Schema 字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


def parquet_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    return pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    if actual_schema.names != expected_schema.names:
        return False
    if any(
        actual_field.type != expected_field.type
        or actual_field.nullable != expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    ):
        return False
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    return all(
        actual_metadata.get(key) == expected_metadata.get(key)
        and expected_metadata.get(key) is not None
        for key in SCHEMA_IDENTITY_METADATA_KEYS
    )


def validate_table_marker(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
    *,
    required: bool,
) -> bool:
    log_started_at = time.perf_counter()
    log_phase = 'read_marker'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    click.echo(
        f"planning_progress: table={log_table_name}; function=validate_table_marker; phase=read_marker; status=started; "
        f"path={table_path}; label={label}; required={str(required).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        marker_path = table_path / "schema.parquet"
        if not marker_path.is_file():
            has_existing_content = (
                table_path.is_dir()
                and next(table_path.iterdir(), None) is not None
            )
            if required or has_existing_content:
                raise FileNotFoundError(f"{label}缺少根级 schema.parquet：{table_path}")
            click.echo(
                f"planning_progress: table={log_table_name}; function=validate_table_marker; phase=read_marker; status=completed; "
                f"path={table_path}; outcome=absent_allowed; marker_present=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return False

        log_phase = "read_marker_metadata"
        marker_metadata = pq.read_metadata(marker_path)
        expected_file_schema = parquet_file_schema(schema, partition_columns)
        if (
            marker_metadata.num_rows != 0
            or not physically_and_identity_compatible(
                marker_metadata.schema.to_arrow_schema(),
                expected_file_schema,
            )
        ):
            raise TypeError(f"{label}根级 schema.parquet 与权威物理契约不一致。")
        click.echo(
            f"planning_progress: table={log_table_name}; function=validate_table_marker; phase=read_marker; status=completed; "
            f"path={marker_path}; rows=0; marker_present=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return True
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=validate_table_marker; phase=read_marker; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def open_planning_calendar_dataset(
    calendar_path: pathlib.Path,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'read_dataset'
    log_checked_fragments = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=open_planning_calendar_dataset; phase=read_dataset; status=started; "
        f"path={calendar_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        validate_table_marker(
            calendar_path,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "正式报告日历",
            required=True,
        )
        log_phase = "open_dataset"
        calendar_dataset = ds.dataset(
            calendar_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
        )
        if not physically_and_identity_compatible(
            reconstructed_schema(
                calendar_dataset,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ),
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ):
            raise TypeError("正式报告日历物理结构或表身份与权威契约不一致。")

        expected_file_schema = parquet_file_schema(
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
        )
        log_phase = "check_fragments"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=open_planning_calendar_dataset; phase=read_dataset; status=running; "
            f"checked_fragments=0; path={calendar_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for fragment in calendar_dataset.get_fragments():
            if not physically_and_identity_compatible(
                fragment.physical_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    "正式报告日历包含不兼容 Parquet fragment："
                    f"{fragment.path}"
                )
            log_checked_fragments += 1
            if time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=open_planning_calendar_dataset; phase=read_dataset; status=running; "
                    f"checked_fragments={log_checked_fragments}; path={calendar_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=open_planning_calendar_dataset; phase=read_dataset; status=completed; "
            f"checked_fragments={log_checked_fragments}; path={calendar_path}; outcome=opened; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return calendar_dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=open_planning_calendar_dataset; phase=read_dataset; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise




# ## dirty 输出叶的业务校验
# 
# 日历 validator 检查仓单身份、主键、状态、计数、审计字段和年月；事实 validator 检查主键、仓库名称、来源、数量、变化和更新时间。输入先按权威 Schema 转换，再按主键排序返回。
# 
# 它们分别由完整事实合并、日历完成或失败状态生成调用；clean 读取以及 staging/正式安装不重新调用这些完整业务规则。

# ### 局部流程：dirty 叶业务校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["生成的完整 dirty 叶"] --> B["按 Schema 转换；检查主键唯一性"]
#     B --> C{"输出类型？"}
#     C -->|事实| D["仓库、来源、年月、数量、变化、审计时间"]
#     C -->|日历| E["仓单身份、状态一致性、计数、审计时间"]
#     D --> F["按主键排序；返回验证结果"]
#     E --> F
# ```

# In[ ]:


def validate_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    normalized = arrow_to_pandas(
        checked,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    if normalized.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}报告日历主键不唯一。")

    fetch_statuses = {
        "pending",
        "success",
        "empty_confirmed",
        "retryable_error",
        "permanent_error",
        "not_required",
    }
    quality_statuses = {
        "pending",
        "passed",
        "warning",
        "failed",
        "not_applicable",
    }
    now_utc = datetime.now(timezone.utc)

    for row in checked.to_pylist():
        if row["dataset_name"] != "warehouse_receipt":
            raise ValueError(f"{context}包含非仓单报告日历行。")
        if row["fetch_result_status"] not in fetch_statuses:
            raise ValueError(f"{context}采集结果状态不在允许枚举中。")
        if row["quality_status"] not in quality_statuses:
            raise ValueError(f"{context}质量状态不在允许枚举中。")
        if not str(row["requirement_reason"]).strip() or not str(row["quality_reason"]).strip():
            raise ValueError(f"{context}报告日历中文原因不得为空。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}报告日历年月分区与交易日不一致。")
        if row["expected_record_count"] < 0 or row["actual_record_count"] < 0:
            raise ValueError(f"{context}报告日历记录数不得为负。")

        if not row["is_fetch_required"]:
            if row["is_data_missing"]:
                raise ValueError(f"{context}当前无需采集格点不得标记数据缺失。")
            if not row["is_fetch_completed"]:
                if row["fetch_result_status"] != "not_required":
                    raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_required。")
                if row["expected_record_count"] != 0 or row["actual_record_count"] != 0:
                    raise ValueError(f"{context}无完成凭证的免采集格点记录数必须为 0。")
                if row["quality_status"] != "not_applicable":
                    raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_applicable。")
                if any(
                    row[name] is not None
                    for name in [
                        "fetch_run_id",
                        "fetch_completed_at",
                        "quality_checked_at",
                    ]
                ):
                    raise ValueError(f"{context}无完成凭证的免采集格点不得保留运行审计值。")
        elif row["fetch_result_status"] == "not_required":
            raise ValueError(f"{context}需采集格点不得标为 not_required。")

        completed_status = row["fetch_result_status"] in {"success", "empty_confirmed"}
        if row["is_fetch_completed"] != completed_status:
            raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成格点缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        if row["is_data_missing"] and (
            not row["is_fetch_required"]
            or row["fetch_result_status"] != "empty_confirmed"
            or row["actual_record_count"] != 0
        ):
            raise ValueError(f"{context}缺失状态不能由当前结果复算。")
        if row["quality_status"] in {"passed", "warning", "failed"} and row["quality_checked_at"] is None:
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


def validate_warehouse_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names],
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(
        checked,
        FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    )
    if normalized.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}仓单事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        warehouse_name = str(row["warehouse_name"]).strip()
        if not warehouse_name or warehouse_name.lower() in {"nan", "none"}:
            raise ValueError(f"{context}仓库名称不得为空。")
        if row["source"] != SOURCE:
            raise ValueError(f"{context}仓单事实来源不一致。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}仓单年月分区与交易日不一致。")
        if not math.isfinite(row["warehouse_receipt_number"]) or row["warehouse_receipt_number"] < 0:
            raise ValueError(f"{context}仓单数量必须为有限非负数。")

        unit = row["warehouse_receipt_unit"]
        if unit is not None and not str(unit).strip():
            raise ValueError(f"{context}非空仓单单位不得为空白。")
        change = row["warehouse_receipt_number_change"]
        if change is not None and not math.isfinite(change):
            raise ValueError(f"{context}仓单数量变化必须为有限数。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}仓单 updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(PRIMARY_KEY).reset_index(drop=True)




# ## 当前叶读取与主键摘要
# 
# `read_partition_leaf()` 只枚举指定叶的 Parquet 文件，核对物理契约、补回 Hive 列并检查分区范围，返回当前完整叶。不存在的叶返回权威空表，存在但无 Parquet 的叶报错。
# 
# `read_leaf_primary_key_summary()` 在 staging 和正式路径只投影主键与分区列；检查分区范围，并对排序后的主键计算 SHA-256 与行数。主键唯一性由 dirty 叶完整业务校验保证；预期、staging 与正式摘要保留全部主键及其重复次数，行数和摘要一致即可证明安装没有增删或重复主键，不再额外转成 Pandas 重查唯一性。摘要证明行数和主键集合一致，不表示逐值比较所有业务列。
# 
# 两个读取函数自行报告叶缺失、待检查文件数、已检查文件数、列读取与结果行数；进度计数附着在原文件循环中，至多每 2 秒报告一次。摘要读取仍只投影主键和分区，不为日志追加业务列或第二次读取。

# ### 局部流程：完整叶和摘要读取
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["按分区键定位当前叶"] --> B{"叶存在？"}
#     B -->|否| C["返回权威空表或空主键摘要"]
#     B -->|是| D["只枚举当前叶；沿文件循环核对并报告进度"]
#     D --> E{"读取目的？"}
#     E -->|合并输入| F["读取完整叶；核对范围并转换"]
#     E -->|安装验收| G["投影主键和分区；核对范围"]
#     G --> H["排序主键；函数报告摘要完成和行数"]
# ```

# In[ ]:


def partition_relative_path(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    if len(partition_columns) != len(partition_key):
        raise ValueError("分区键数量与权威分区列不一致。")
    return pathlib.Path(*[
        f"{column}={value}"
        for column, value in zip(
            partition_columns,
            partition_key,
            strict=True,
        )
    ])


def read_partition_leaf(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    label: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'read_leaf'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    log_checked_files = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=started; "
        f"key={partition_key}; path={table_path}; label={label}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        leaf_path = table_path / partition_relative_path(
            partition_columns,
            partition_key,
        )
        if not leaf_path.is_dir():
            click.echo(
                f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
                f"key={partition_key}; path={leaf_path}; rows=0; outcome=absent_leaf; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(schema)
        parquet_files = list(leaf_path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"{label}叶目录不含 Parquet 文件：{leaf_path}")

        expected_file_schema = parquet_file_schema(schema, partition_columns)
        log_phase = "check_files"
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=running; "
            f"key={partition_key}; checked_files=0/{len(parquet_files)}; path={leaf_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for parquet_path in parquet_files:
            if not physically_and_identity_compatible(
                pq.read_schema(parquet_path),
                expected_file_schema,
            ):
                raise TypeError(f"{label}叶包含不兼容文件：{parquet_path}")
            log_checked_files += 1
            if time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=running; "
                    f"key={partition_key}; checked_files={log_checked_files}/{len(parquet_files)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        log_phase = "open_leaf"
        leaf_dataset = ds.dataset(
            leaf_path,
            format="parquet",
            partitioning=partitioning,
            partition_base_dir=str(table_path),
        )
        if not physically_and_identity_compatible(
            reconstructed_schema(leaf_dataset, schema),
            schema,
        ):
            raise TypeError(f"{label}叶物理结构或表身份与权威契约不一致。")
        log_phase = "read_columns"
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=running; "
            f"key={partition_key}; columns={len(schema.names)}; path={leaf_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        leaf_table = leaf_dataset.to_table(columns=schema.names)
        if len(leaf_table):
            actual_partition_keys = set(
                leaf_table.select(partition_columns)
                .to_pandas()
                .itertuples(index=False, name=None)
            )
            if actual_partition_keys != {partition_key}:
                raise ValueError(f"{label}叶内容越出指定 Hive 分区。")
        partition_df = arrow_to_pandas(leaf_table, schema)
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
            f"key={partition_key}; rows={len(partition_df)}; checked_files={log_checked_files}/{len(parquet_files)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_partition_leaf; phase=read_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def primary_key_summary(
    table: pa.Table,
    primary_key: list[str],
) -> tuple[int, str]:
    primary_key_table = table.select(primary_key)
    sorted_primary_key_table = primary_key_table.sort_by([
        (column, "ascending")
        for column in primary_key
    ]).replace_schema_metadata(None)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, sorted_primary_key_table.schema) as writer:
        writer.write_table(sorted_primary_key_table)
    digest = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
    return len(primary_key_table), digest


def read_leaf_primary_key_summary(
    table_path: pathlib.Path,
    schema: pa.Schema,
    primary_key: list[str],
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    label: str,
) -> tuple[int, str]:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'read_leaf_summary'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    log_checked_files = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=started; "
        f"key={partition_key}; path={table_path}; label={label}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        leaf_path = table_path / partition_relative_path(
            partition_columns,
            partition_key,
        )
        if not leaf_path.exists():
            empty_table = pa.Table.from_batches([], schema=schema)
            empty_summary = primary_key_summary(empty_table, primary_key)
            click.echo(
                f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=completed; "
                f"key={partition_key}; path={leaf_path}; rows=0; outcome=absent_leaf; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_summary
        parquet_files = list(leaf_path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"{label}叶目录不含 Parquet 文件：{leaf_path}")

        expected_file_schema = parquet_file_schema(schema, partition_columns)
        log_phase = "check_files"
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=running; "
            f"key={partition_key}; checked_files=0/{len(parquet_files)}; path={leaf_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for parquet_path in parquet_files:
            if not physically_and_identity_compatible(
                pq.read_schema(parquet_path),
                expected_file_schema,
            ):
                raise TypeError(f"{label}叶包含不兼容文件：{parquet_path}")
            log_checked_files += 1
            if time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=running; "
                    f"key={partition_key}; checked_files={log_checked_files}/{len(parquet_files)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        log_phase = "open_leaf"
        leaf_dataset = ds.dataset(
            leaf_path,
            format="parquet",
            partitioning=partitioning,
            partition_base_dir=str(table_path),
        )
        if not physically_and_identity_compatible(
            reconstructed_schema(leaf_dataset, schema),
            schema,
        ):
            raise TypeError(f"{label}叶物理结构或表身份与权威契约不一致。")
        summary_columns = list(dict.fromkeys([
            *primary_key,
            *partition_columns,
        ]))
        log_phase = "read_key_columns"
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=running; "
            f"key={partition_key}; columns={len(summary_columns)}; path={leaf_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        summary_table = leaf_dataset.to_table(columns=summary_columns)
        if len(summary_table):
            actual_partition_keys = set(
                summary_table.select(partition_columns)
                .to_pandas()
                .itertuples(index=False, name=None)
            )
            if actual_partition_keys != {partition_key}:
                raise ValueError(f"{label}叶主键越出指定 Hive 分区。")
        log_phase = "key_summary"
        leaf_summary = primary_key_summary(
            summary_table,
            primary_key,
        )
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=completed; "
            f"key={partition_key}; rows={leaf_summary[0]}; checked_files={log_checked_files}/{len(parquet_files)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return leaf_summary
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_leaf_primary_key_summary; phase=read_leaf_summary; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## JQData 仓单响应归一化
# 
# `normalize_warehouse_response()` 要求 DataFrame；空响应直接返回权威空表，包括当前允许的零行零列响应。非空响应必须包含约定列、少于 5000 行，并精确属于待办日期、品种和允许的交易所别名；单日触顶直接失败，不分页或拆日。
# 
# 逐行保留仓库名称、有限非负数量、可空来源单位和可空有限数量变化。单位空白或来源缺失归一为 null，后续在日历记录 warning；变化保留正负号。补入日历身份、年月和本批更新时间后转换为权威 Arrow/Pandas 表示。
# 
# 这里只做来源验收和契约转换。完整事实主键及业务校验放在合并后的 dirty 叶；只读路径不读取旧事实或提交状态。
# 
# 生成函数自行报告来源验收、行归一化和契约转换。沿原行循环每 1000 行检查 2 秒进度间隔；最终 `api_success:` 表示该日响应已生成事实，仍为 `persisted=false`。空响应同样由本函数报告，查询收到响应不提前代表业务验收通过。

# ### 局部流程：来源响应归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["来源必须为 DataFrame"] --> B{"空响应？"}
#     B -->|是| C["返回权威空表"]
#     B -->|否| D["列齐全；少于 5000 行；精确格点匹配"]
#     D --> E["逐行验收仓库、数量、单位与变化；报告进度"]
#     E --> F["单位缺失转 null；补日历身份和年月"]
#     F --> G["契约转换；函数报告 api_success；persisted=false"]
# ```

# In[ ]:


def normalize_warehouse_response(
    raw_df: pd.DataFrame,
    grid: dict[str, object],
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'generate_fact'
    log_visited_rows = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=started; "
        f"grid={grid}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not isinstance(raw_df, pd.DataFrame):
            raise TypeError("schema_error: JQData 仓单查询未返回 DataFrame。")
        if raw_df.empty:
            click.echo(
                f"api_success: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=completed; "
                f"grid={grid}; rows=0; outcome=empty_response; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)

        log_phase = "validate_source"
        missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
        if missing_columns:
            raise ValueError(f"schema_error: JQData 仓单表缺列 {sorted(missing_columns)}。")
        if len(raw_df) >= 5000:
            raise ValueError("schema_error: 单格点响应达到 run_query 上限，不能证明结果完整。")

        response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
        response_df["day"] = pd.to_datetime(response_df["day"], errors="coerce").dt.date
        if response_df["day"].isna().any() or not response_df["day"].eq(grid["trading_date"]).all():
            raise ValueError("schema_error: JQData 仓单响应包含请求交易日之外的行。")

        response_underlying = response_df["underlying_code"].astype(str).str.strip().str.upper()
        if not response_underlying.eq(grid["underlying_code"]).all():
            raise ValueError("schema_error: JQData 仓单响应品种与待办日历不一致。")
        response_exchange_values = response_df["exchange"]
        response_exchange_codes = (
            response_exchange_values.astype(str).str.strip().str.upper()
        )
        allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(
            grid["exchange_code"],
            {grid["exchange_code"]},
        )
        if (
            response_exchange_values.isna().any()
            or response_exchange_codes.eq("").any()
            or not response_exchange_codes.isin(allowed_exchanges).all()
        ):
            raise ValueError(
                "schema_error: JQData 仓单响应交易所与待办日历不一致；"
                f"actual={sorted(set(response_exchange_codes))}。"
            )

        log_phase = "normalize_rows"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=running; "
            f"grid={grid}; source_rows={len(response_df)}; visited_rows=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        rows = []
        for source_row in response_df.to_dict("records"):
            raw_warehouse_name = source_row["warehouse_name"]
            if pd.isna(raw_warehouse_name):
                raise ValueError("schema_error: JQData warehouse_name 为空。")
            warehouse_name = str(raw_warehouse_name).strip()
            if not warehouse_name or warehouse_name.lower() in {"nan", "none"}:
                raise ValueError("schema_error: JQData warehouse_name 为空。")

            quantity = pd.to_numeric(
                source_row["warehouse_receipt_number"],
                errors="coerce",
            )
            if pd.isna(quantity) or not math.isfinite(float(quantity)) or quantity < 0:
                raise ValueError("schema_error: JQData 仓单数量必须非空、有限且非负。")

            raw_unit = source_row["unit"]
            if pd.isna(raw_unit):
                unit = None
            else:
                unit_text = str(raw_unit).strip()
                unit = unit_text if unit_text and unit_text.lower() not in {"nan", "none"} else None

            raw_change = source_row["warehouse_receipt_number_increase"]
            if pd.isna(raw_change):
                quantity_change = None
            else:
                numeric_change = pd.to_numeric(raw_change, errors="coerce")
                if pd.isna(numeric_change) or not math.isfinite(float(numeric_change)):
                    raise ValueError("schema_error: JQData 仓单数量变化必须为有限数。")
                quantity_change = float(numeric_change)

            rows.append({
                "trading_date": grid["trading_date"],
                "exchange_code": grid["exchange_code"],
                "underlying_code": grid["underlying_code"],
                "warehouse_name": warehouse_name,
                "warehouse_receipt_number": float(quantity),
                "warehouse_receipt_unit": unit,
                "warehouse_receipt_number_change": quantity_change,
                "source": SOURCE,
                "updated_at": updated_at,
                "year": grid["trading_date"].year,
                "month": grid["trading_date"].month,
            })
            log_visited_rows += 1
            if log_visited_rows % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=running; "
                    f"grid={grid}; visited_rows={log_visited_rows}/{len(response_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        frame = pd.DataFrame(
            rows,
            columns=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
        )
        log_phase = "convert_contract"
        warehouse_table = pandas_to_arrow(
            frame,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        )
        warehouse_df = arrow_to_pandas(
            warehouse_table,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        )
        click.echo(
            f"api_success: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=completed; "
            f"grid={grid}; rows={len(warehouse_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return warehouse_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=normalize_warehouse_response; phase=generate_fact; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 可信完成快照与待办规划
# 
# `pending_report_grids()` 接收已在读取时过滤为 required 仓单的规划列，以 `is_fetch_completed` 区分完成和待办，再按交易所—品种—年月—日期排序。它不扫描事实历史、不按历史事实计数修复日历。
# 
# `grid_count_map()` 只用于当前完整叶的格点计数：成功回写取本次已验收事实的计数，采集失败回写取当前正式叶的计数；不参与启动时的全历史完成证明。
# 
# 待办函数拥有 `reconciliation_plan:`，报告规划行数、可信完成数与待办数；不为日志重新计算事实完成证据。

# ### 局部流程：快照待办
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["已过滤的 required 仓单规划行"] --> B{"is_fetch_completed？"}
#     B -->|是| C["信任完成凭证；计入完成数"]
#     B -->|否| D["加入待办；按交易所、品种、年月、日期排序"]
#     C --> E["函数报告 reconciliation_plan；不扫描事实历史"]
#     D --> E
# ```

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[tuple[object, ...], int]:
    if frame.empty:
        return {}
    counts = frame.groupby(GRID_COLUMNS, dropna=False).size()
    return {tuple(key): int(value) for key, value in counts.items()}


def pending_report_grids(
    calendar_planning_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    log_started_at = time.perf_counter()
    log_phase = 'plan'
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=started; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        completed_mask = calendar_planning_df["is_fetch_completed"].eq(True)
        complete_count = int(completed_mask.sum())
        pending_df = calendar_planning_df.loc[
            ~completed_mask,
            [*GRID_COLUMNS, "year", "month"],
        ].copy()
        if not pending_df.empty:
            pending_df = pending_df.sort_values([
                "exchange_code",
                "underlying_code",
                "year",
                "month",
                "trading_date",
            ]).reset_index(drop=True)
        click.echo(
            f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=completed; "
            f"planning_rows={len(calendar_planning_df)}; complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; basis=calendar_completion_snapshot; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return pending_df, complete_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 完整事实叶合并与提交前待办复核
# 
# `full_fact_partition()` 保留旧叶中未触达日期，拼接本次事实，执行一次完整业务校验；空结果保留权威结构。`ensure_pending_calendar_grids()` 从刚读取的当前日历叶确认本次格点唯一、required 且仍未完成。
# 
# 这些操作只针对当前品种月份及其对应日历叶，不重新打开或物化整张历史事实表。
# 
# 待办复核函数沿原格点循环报告检查数量；完整叶生成函数自行报告保留行、新行、验证后完整叶行数和失败阶段，生成结果明确标为 `persisted=false`。

# ### 局部流程：待办复核与完整叶生成
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["读取当前日历叶与旧事实叶"] --> B["确认待办唯一、required、未完成"]
#     B --> C["旧事实保留未触达日期；拼接新事实"]
#     C --> D["完整 dirty 叶执行一次业务校验"]
#     D --> E["函数报告完整叶生成完成；persisted=false"]
# ```

# In[ ]:


def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = 'generate_complete_leaf'
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate_complete_leaf; status=started; "
        f"persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        retained_df = existing_df.loc[
            ~existing_df["trading_date"].isin(touched_dates),
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
        ]
        complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
        if complete_df.empty:
            complete_df = empty_pandas(FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA)
        log_phase = "validate_dirty_fact"
        validated_fact_partition_df = validate_warehouse_frame(complete_df, "合并后的完整仓单分区")
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate_complete_leaf; status=completed; "
            f"retained_rows={len(retained_df)}; new_rows={len(incoming_df)}; rows={len(validated_fact_partition_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_fact_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate_complete_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def ensure_pending_calendar_grids(
    calendar_partition_df: pd.DataFrame,
    grid_records: list[dict[str, object]],
) -> None:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'check_pending'
    log_checked_grids = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=ensure_pending_calendar_grids; phase=check_pending; status=started; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        for grid in grid_records:
            grid_mask = calendar_partition_df["dataset_name"].eq("warehouse_receipt")
            for column in GRID_COLUMNS:
                grid_mask &= calendar_partition_df[column].eq(grid[column])
            calendar_grid_df = calendar_partition_df.loc[grid_mask]
            if len(calendar_grid_df) != 1:
                raise ValueError(f"报告日历叶缺少唯一仓单格点：{grid}。")
            calendar_grid = calendar_grid_df.iloc[0]
            if (
                not bool(calendar_grid["is_fetch_required"])
                or bool(calendar_grid["is_fetch_completed"])
            ):
                raise ValueError(f"仓单格点提交前已不再属于未完成待办：{grid}。")
            log_checked_grids += 1
            if time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=ensure_pending_calendar_grids; phase=check_pending; status=running; "
                    f"checked_grids={log_checked_grids}/{len(grid_records)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=ensure_pending_calendar_grids; phase=check_pending; status=completed; "
            f"checked_grids={log_checked_grids}/{len(grid_records)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=ensure_pending_calendar_grids; phase=check_pending; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 可选性能门槛
# 
# 两个性能参数必须同时提供，且只用于无显式日期的自动正式 `--write`。窗口不足时不判断；样本是成功完成事实与日历提交的分区总耗时，仅在首个完整窗口形成时检查一次。
# 
# 已记录的恢复配置为 `--performance-window-size 50 --performance-max-median-seconds 15.4`：首 50 个成功分区中位数不超过 15.4 秒为通过；超限则在该分区完成后停止，保留已成功提交，不处理第 51 个分区。默认日常运行不开启此门槛。

# ### 局部流程：首个成功窗口的性能判断
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前事实和日历均已成功提交"] --> B{"开启门槛且恰好达到首个窗口？"}
#     B -->|否| C["继续后续分区"]
#     B -->|是| D["计算首窗口分区总耗时中位数"]
#     D --> E{"不超过上限？"}
#     E -->|是| C
#     E -->|否| F["报告门槛失败并停止；已提交分区保留"]
# ```

# In[ ]:


def performance_gate_result(
    partition_seconds: list[float],
    window_size: int,
    max_median_seconds: float,
) -> tuple[bool, float] | None:
    if len(partition_seconds) < window_size:
        return None
    median_seconds = float(statistics.median(
        partition_seconds[:window_size]
    ))
    return median_seconds <= max_median_seconds, median_seconds


# ## 当前事实叶 staging、共享安装与恢复
# 
# `commit_complete_partition()` 接收已完整验证的 dirty 事实叶，检查分区范围并生成预期行数/主键摘要。staging 写入零行标记和非空叶，通过物理契约与摘要复读后进入 `StagedPathTransaction`：本次新建的根标记与当前事实叶属于同一事务，已有标记保持原样；空结果通过 `staged_path=None` 显式删除旧叶。
# 
# 正式标记、叶物理契约与主键摘要仍在本函数内检查，不重复完整业务 validator 或逐值读取全部业务列。验收在事务内完成，成功退出事务后才报告 `partition_committed: ... scope=leaf`。
# 
# 共享模块按实际成功的移动记录恢复：首次备份失败保留原目标，安装或正式验收失败时隔离已安装的新叶并恢复旧叶；本次新建标记回退时移除。恢复完整才清理旧备份，恢复不完整保留备份并抛错，staging 均清理。本函数只尝试移除当前目标的空父目录，不再递归扫描或删除表根。
# 
# 共享模块报告恢复结果，本函数报告失败阶段。事务仅覆盖当前叶及必要的新标记；此前成功叶保留，不提供跨表共同回滚、进程终止后的自动恢复或并发写入协调。
# 

# ### 局部流程：当前事实叶安装与共享恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["已验证事实叶；检查范围并计算主键摘要"] --> B["写 staging 标记与非空叶；物理契约和摘要复读"]
#     B --> C["进入共享事务：当前叶及必要的新标记"]
#     C --> D["安装新标记；备份旧叶；安装或显式删除叶"]
#     D --> E["本函数复读正式标记、叶物理契约与摘要"]
#     E --> F["成功退出事务；报告当前事实叶已提交"]
#     D -. 失败 .-> R["按实际移动倒序恢复；已安装新叶隔离留存"]
#     E -. 失败 .-> R
#     R --> S{"恢复完整？"}
#     S -->|是| T["清理备份；移除空父目录；抛错"]
#     S -->|否| U["保留备份和失败数据；抛错"]
#     B -. 失败 .-> V["清理 staging；正式数据未改动"]
#     C -. 进入失败 .-> V
#     T --> W["staging 清理；此前成功叶保留"]
#     U --> W
# ```
# 

# In[ ]:


def commit_complete_partition(
    validated_fact_partition_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> int:
    log_started_at = time.perf_counter()
    log_phase = 'commit'
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=commit; status=started; "
        f"key={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        complete_df = validated_fact_partition_df.loc[
            :,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names,
        ]
        if not complete_df.empty:
            actual_keys = set(
                complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
            )
            if actual_keys != {partition_key}:
                raise ValueError("待提交仓单内容越出指定 Hive 叶分区。")
        complete_table = pandas_to_arrow(
            complete_df,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
        )
        log_phase = "expected_summary"
        expected_summary = primary_key_summary(
            complete_table,
            PRIMARY_KEY,
        )

        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / TABLE_NAME
        validate_table_marker(
            target_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "正式仓单事实",
            required=False,
        )
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
        backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
        quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

        for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
        silver_root.mkdir(parents=True, exist_ok=True)
        try:
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=staging_write; status=started; "
                f"key={partition_key}; rows={len(complete_table)}; path={staging_path}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)

            file_schema = pa.schema(
                [
                    field
                    for field in FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
                    if field.name not in PARTITION_COLUMNS
                ],
                metadata=FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=file_schema),
                staging_path / "schema.parquet",
            )
            validate_table_marker(
                staging_path,
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                PARTITION_COLUMNS,
                "仓单 staging",
                required=True,
            )
            if len(complete_table):
                ds.write_dataset(
                    complete_table,
                    staging_path,
                    format="parquet",
                    partitioning=FACT_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

            log_phase = "staging_verify"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=staging_verify; status=started; "
                f"key={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_summary = read_leaf_primary_key_summary(
                staging_path,
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                PRIMARY_KEY,
                PARTITION_COLUMNS,
                FACT_PARTITIONING,
                partition_key,
                "仓单 staging",
            )
            if staged_summary != expected_summary:
                raise ValueError("仓单 staging 行数或主键摘要检查失败。")

        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        relative_path = pathlib.Path(*[
            f"{column}={value}"
            for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True)
        ])
        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        target_marker_path = target_path / "schema.parquet"
        staging_marker_path = staging_path / "schema.parquet"

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=install; status=started; "
                f"key={partition_key}; path={destination_path}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={TABLE_NAME}; function=commit_complete_partition; "
                    f"key={partition_key}; run_id={run_id}; scope=leaf"
                ),
            ) as transaction:
                if not target_marker_path.exists():
                    transaction.replace(
                        target_path=target_marker_path,
                        staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if len(complete_table) else None,
                )

                log_phase = "formal_verify"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=formal_verify; status=started; "
                    f"key={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                validate_table_marker(
                    target_path,
                    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                    PARTITION_COLUMNS,
                    "正式仓单事实",
                    required=True,
                )
                committed_summary = read_leaf_primary_key_summary(
                    target_path,
                    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                    PRIMARY_KEY,
                    PARTITION_COLUMNS,
                    FACT_PARTITIONING,
                    partition_key,
                    "正式仓单事实",
                )
                if committed_summary != expected_summary:
                    raise ValueError("正式仓单行数或主键摘要检查失败。")
        except Exception:
            cleanup_path = destination_path.parent
            while cleanup_path.is_relative_to(target_path):
                try:
                    cleanup_path.rmdir()
                except FileNotFoundError:
                    pass
                except OSError:
                    break
                if cleanup_path == target_path:
                    break
                cleanup_path = cleanup_path.parent
            raise
        finally:
            shutil.rmtree(staging_path, ignore_errors=True)

        click.echo(
            f"partition_committed: table={TABLE_NAME}; function=commit_complete_partition; phase=commit; status=completed; "
            f"key={partition_key}; rows={len(complete_table)}; persisted=true; scope=leaf; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return len(complete_table)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_complete_partition; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 仓单日历的完成与失败状态生成
# 
# `apply_calendar_completion()` 只更新当前格点：有事实行写 `success`，无行写 `empty_confirmed + warning`，有缺失单位的非空事实写 `success + warning`。同时记录条数、批次与时间；其他品种和日期保持原样。计数和单位缺失来自已验收的内存完整叶，其正式路径已通过物理契约及主键摘要检查。
# 
# `apply_calendar_failure()` 在查询或转换失败时保留未完成状态，记录 `retryable_error` 或 `permanent_error`、失败原因、当前正式事实计数和质量失败时间，完成时间保持空。两条生成路径各在返回前完整验证自己的 dirty 日历叶；返回值仍只是内存结果。
# 
# 两条状态生成函数沿原日历行循环报告已访问行数，返回前分别标明 `fetch_completed=true` 或 `false`、`persisted=false` 和 `date_watermark=none`；内存生成日志不代表日历状态已经落盘。

# ### 局部流程：完成与失败状态生成
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"当前结果？"} -->|事实已提交| B{"当前格点有事实行？"}
#     B -->|否| C["empty_confirmed；completed=true；warning"]
#     B -->|是| D["success；completed=true；单位缺失则 warning"]
#     A -->|查询或转换失败| E["retryable/permanent_error；completed=false；quality=failed"]
#     C --> F["更新本次格点计数、批次与时间；保留其他行"]
#     D --> F
#     E --> F
#     F --> G["完整验收；函数报告生成结果；persisted=false"]
# ```

# In[ ]:


def apply_calendar_completion(
    calendar_partition_df: pd.DataFrame,
    grid_results: dict[tuple[object, ...], tuple[int, int]],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'generate_calendar_completion'
    log_visited_rows = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_completion; status=started; "
        f"persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        updated_df = calendar_partition_df.copy()
        updated_grid_keys = set()

        for index, row in updated_df.iterrows():
            log_visited_rows += 1
            if log_visited_rows % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_completion; status=running; "
                    f"visited_rows={log_visited_rows}/{len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            if row["dataset_name"] != "warehouse_receipt":
                continue
            grid_key = tuple(row[column] for column in GRID_COLUMNS)
            result = grid_results.get(grid_key)
            if result is None:
                continue
            updated_grid_keys.add(grid_key)

            actual_count, missing_unit_count = result
            has_rows = actual_count > 0
            has_unit_warning = missing_unit_count > 0
            updated_df.at[index, "is_fetch_completed"] = True
            updated_df.at[index, "fetch_result_status"] = "success" if has_rows else "empty_confirmed"
            updated_df.at[index, "is_data_missing"] = not has_rows
            updated_df.at[index, "expected_record_count"] = 1
            updated_df.at[index, "actual_record_count"] = actual_count
            updated_df.at[index, "quality_status"] = (
                "warning"
                if not has_rows or has_unit_warning
                else "passed"
            )
            if not has_rows:
                quality_reason = "JQData 查询成功但没有仓单记录，已按确认空记录。"
            elif has_unit_warning:
                quality_reason = (
                    f"正式仓单事实复读 {actual_count} 行，其中 {missing_unit_count} 行来源未提供计量单位。"
                )
            else:
                quality_reason = f"JQData 仓单响应已转换并从正式事实表复读 {actual_count} 行。"
            updated_df.at[index, "quality_reason"] = quality_reason
            updated_df.at[index, "fetch_run_id"] = fetch_run_id
            updated_df.at[index, "fetch_completed_at"] = completed_at
            updated_df.at[index, "quality_checked_at"] = completed_at
            updated_df.at[index, "updated_at"] = completed_at

        if updated_grid_keys != set(grid_results):
            missing_grid_keys = sorted(set(grid_results) - updated_grid_keys)
            raise ValueError(f"报告日历叶缺少待完成格点：{missing_grid_keys}。")
        log_phase = "validate_dirty_calendar"
        validated_calendar_partition_df = validate_calendar_frame(updated_df, "仓单完成状态回写后的")
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_completion; status=completed; "
            f"updated_grids={len(updated_grid_keys)}; fetch_completed=true; rows={len(validated_calendar_partition_df)}; visited_rows={log_visited_rows}; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_completion; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def apply_calendar_failure(
    calendar_partition_df: pd.DataFrame,
    grid_key: tuple[object, ...],
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_count: int,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'generate_calendar_failure'
    log_visited_rows = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_calendar_failure; status=started; "
        f"persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if fetch_status not in {"retryable_error", "permanent_error"}:
            raise ValueError("失败状态不在允许枚举中。")
        updated_df = calendar_partition_df.copy()
        updated = False

        for index, row in updated_df.iterrows():
            log_visited_rows += 1
            if log_visited_rows % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = time.perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_calendar_failure; status=running; "
                    f"visited_rows={log_visited_rows}/{len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            row_grid_key = tuple(row[column] for column in GRID_COLUMNS)
            if row["dataset_name"] != "warehouse_receipt" or row_grid_key != grid_key:
                continue
            updated = True
            updated_df.at[index, "is_fetch_completed"] = False
            updated_df.at[index, "fetch_result_status"] = fetch_status
            updated_df.at[index, "is_data_missing"] = False
            updated_df.at[index, "expected_record_count"] = 1
            updated_df.at[index, "actual_record_count"] = current_fact_count
            updated_df.at[index, "quality_status"] = "failed"
            updated_df.at[index, "quality_reason"] = failure_reason
            updated_df.at[index, "fetch_run_id"] = fetch_run_id
            updated_df.at[index, "fetch_completed_at"] = None
            updated_df.at[index, "quality_checked_at"] = failed_at
            updated_df.at[index, "updated_at"] = failed_at

        if not updated:
            raise ValueError(f"报告日历叶缺少待记录失败的格点：{grid_key}。")
        log_phase = "validate_dirty_calendar"
        validated_calendar_partition_df = validate_calendar_frame(updated_df, "仓单失败状态回写后的")
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_calendar_failure; status=completed; "
            f"grid={grid_key}; fetch_status={fetch_status}; fetch_completed=false; rows={len(validated_calendar_partition_df)}; visited_rows={log_visited_rows}; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_calendar_failure; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 仓单日历逐叶提交
# 
# 只选择本次触达的仓单日历叶，保留叶内其他品种和日期；输入来自已完整验证的完成或失败状态生成函数。先按分区键对输入日历分组一次，目录、物理 Schema 和不变根标记检查放在分区循环前；逐叶直接取得完整组，不再对整份输入重复构造分区筛选。每个叶写 staging，复读物理契约与行数/主键摘要，再各自进入 `StagedPathTransaction` 安装；正式物理契约与摘要验收仍在事务内由本函数执行，不重复完整业务 validator。
# 
# 每个日历叶独立恢复，正式根级日历标记只验证、不替换。安装或验收失败按实际移动恢复当前叶，已安装的新叶隔离留存；恢复不完整保留旧备份，staging 清理。日历失败不撤销此前已提交的事实或日历叶。
# 
# 成功退出当前叶事务后才输出 `partition_committed: ... scope=leaf`，全部触达叶成功后才输出 `phase=calendar_state; persisted=true`。失败状态写入也走同一提交函数，因此“状态已落盘”不能推断 `is_fetch_completed=true`；本入口没有独立日期水位文件。
# 

# ### 局部流程：日历逐叶安装与共享恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["一次分组并准备目录、物理 Schema；检查根标记"] --> B["逐叶取得完整组；保留其他品种和日期"]
#     B --> C["写 staging；物理契约与主键摘要复读"]
#     C --> D["进入当前叶共享事务；安装并正式摘要验收"]
#     D --> E["成功退出事务；报告当前叶提交"]
#     E --> F{"还有日历叶？"}
#     F -->|是| B
#     F -->|否| G["报告日历状态已落盘；date_watermark=none"]
#     D -. 失败 .-> H["按实际移动恢复当前叶；新叶隔离留存"]
#     H --> I["恢复不完整保留旧备份；抛错停止"]
#     I --> J["staging 清理；此前成功事实和日历叶保留"]
#     C -. 失败 .-> K["清理 staging；当前正式叶未改动"]
# ```
# 

# In[ ]:


def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_grid_keys: set[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = 'commit_calendar'
    log_committed_partitions = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=started; "
        f"batch_state=pending; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not touched_grid_keys:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=skipped; "
                f"reason=no_grids; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        touched_mask = (
            calendar_df["dataset_name"].eq("warehouse_receipt")
            & calendar_df[GRID_COLUMNS].apply(tuple, axis=1).isin(touched_grid_keys)
        )
        touched_df = calendar_df.loc[touched_mask]
        partition_keys = set(
            touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None)
        )

        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=running; "
            f"partitions=0/{len(partition_keys)}; touched_rows={len(touched_df)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
        calendar_partition_groups = calendar_df.groupby(
            CALENDAR_PARTITION_COLUMNS,
            sort=False,
        )
        file_schema = pa.schema(
            [
                field
                for field in FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
                if field.name not in CALENDAR_PARTITION_COLUMNS
            ],
            metadata=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata,
        )
        if partition_keys:
            validate_table_marker(
                target_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                "正式报告日历",
                required=True,
            )

        for partition_key in sorted(partition_keys):
            log_phase = "prepare_leaf"
            complete_df = calendar_partition_groups.get_group(partition_key).loc[
                :,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
            ]

            # 日历和事实使用同一套完整叶分区提交语义，但保持各自独立 Schema。
            complete_table = pandas_to_arrow(
                complete_df,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            )
            log_phase = "expected_summary"
            expected_summary = primary_key_summary(
                complete_table,
                CALENDAR_PRIMARY_KEY,
            )
            run_id = uuid.uuid4().hex
            staging_path = silver_root / f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
            backup_path = silver_root / f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
            quarantine_path = silver_root / f".{CALENDAR_TABLE_NAME}.failed-{run_id}"
            relative_path = pathlib.Path(*[
                f"{column}={value}"
                for column, value in zip(
                    CALENDAR_PARTITION_COLUMNS,
                    partition_key,
                    strict=True,
                )
            ])

            for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
                if not managed_path.resolve().is_relative_to(silver_root):
                    raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
            try:
                log_phase = "staging_write"
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_write; status=started; "
                    f"key={partition_key}; rows={len(complete_table)}; path={staging_path}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                staging_path.mkdir(parents=True, exist_ok=False)
                pq.write_table(
                    pa.Table.from_batches([], schema=file_schema),
                    staging_path / "schema.parquet",
                )
                validate_table_marker(
                    staging_path,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                    CALENDAR_PARTITION_COLUMNS,
                    "报告日历 staging",
                    required=True,
                )
                ds.write_dataset(
                    complete_table,
                    staging_path,
                    format="parquet",
                    partitioning=CALENDAR_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
                log_phase = "staging_verify"
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_verify; status=started; "
                    f"key={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                staged_summary = read_leaf_primary_key_summary(
                    staging_path,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                    CALENDAR_PRIMARY_KEY,
                    CALENDAR_PARTITION_COLUMNS,
                    CALENDAR_PARTITIONING,
                    partition_key,
                    "报告日历 staging",
                )
                if staged_summary != expected_summary:
                    raise ValueError("报告日历 staging 行数或主键摘要检查失败。")

            except Exception:
                shutil.rmtree(staging_path, ignore_errors=True)
                raise

            source_path = staging_path / relative_path
            destination_path = target_path / relative_path
            try:
                log_phase = "install"
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=install; status=started; "
                    f"key={partition_key}; path={destination_path}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                with StagedPathTransaction(
                    root_path=target_path,
                    staging_dir=staging_path,
                    backup_dir=backup_path,
                    quarantine_dir=quarantine_path,
                    log_context=(
                        f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; "
                        f"key={partition_key}; run_id={run_id}; scope=leaf"
                    ),
                ) as transaction:
                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path,
                    )

                    log_phase = "formal_verify"
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=formal_verify; status=started; "
                        f"key={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    committed_summary = read_leaf_primary_key_summary(
                        target_path,
                        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                        CALENDAR_PRIMARY_KEY,
                        CALENDAR_PARTITION_COLUMNS,
                        CALENDAR_PARTITIONING,
                        partition_key,
                        "正式报告日历",
                    )
                    if committed_summary != expected_summary:
                        raise ValueError("正式报告日历行数或主键摘要检查失败。")
            except Exception:
                cleanup_path = destination_path.parent
                while cleanup_path.is_relative_to(target_path):
                    try:
                        cleanup_path.rmdir()
                    except FileNotFoundError:
                        pass
                    except OSError:
                        break
                    if cleanup_path == target_path:
                        break
                    cleanup_path = cleanup_path.parent
                raise
            finally:
                shutil.rmtree(staging_path, ignore_errors=True)

            log_committed_partitions += 1
            click.echo(
                f"partition_committed: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
                f"key={partition_key}; rows={len(complete_table)}; persisted=true; scope=leaf; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=running; "
                f"partitions={log_committed_partitions}/{len(partition_keys)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=calendar_state; status=completed; "
            f"partitions={log_committed_partitions}; rows={len(touched_df)}; persisted=true; date_watermark=none; outcome=state_committed; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return len(touched_df)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## JQData 单格点查询边界
# 
# `query_warehouse_grid()` 显式选择来源列，按一个品种和交易日调用一次 `finance.run_query()`；来源交易所由后续响应归一化复核。权限或缺表异常标记为 `permanent_error`，其他查询异常及返回 `None` 标记为 `retryable_error`，原异常链保留。
# 
# 错误分类不代表程序会自动重试。查询或转换失败均交由 `main()` 的既有失败状态回写分支处理，然后停止批次。
# 
# 查询函数拥有请求起止和单次调用日志，返回前的 `api_result:` 只表示收到响应，标明 `business_validated=false`。来源验收成功由归一化函数随后报告，任何异常仍不自动重试。

# ### 局部流程：单日查询
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["显式来源列；品种和交易日过滤"] --> B["函数报告请求开始；run_query 一次"]
#     B --> C{"异常或 None？"}
#     C -->|否| D["函数报告收到响应；交给归一化函数验收"]
#     C -->|是| E["权限/缺表分类为 permanent；其他为 retryable"]
#     E --> F["保留异常链并抛出；不自动重试"]
# ```

# In[ ]:


def query_warehouse_grid(
    jqdata: ModuleType,
    grid: dict[str, object],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = 'query'
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=query_warehouse_grid; phase=query; status=started; "
        f"grid={grid}; source_calls=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        table = jqdata.finance.FUT_WAREHOUSE_RECEIPT
        query_object = jqdata.query(*[
            getattr(table, field_name)
            for field_name in JQDATA_FIELDS
        ]).filter(
            table.day == grid["trading_date"],
            table.underlying_code == grid["underlying_code"],
        )

        try:
            log_phase = "source_request"
            click.echo(
                f"request_batch: table={TABLE_NAME}; function=query_warehouse_grid; phase=query; status=running; "
                f"grid={grid}; source_calls=1; response_pending=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            raw_df = jqdata.finance.run_query(query_object)
        except Exception as error:
            message = str(error)
            permanent_markers = ["无权限", "permission", "no table", "不存在"]
            error_type = (
                "permanent_error"
                if any(marker.lower() in message.lower() for marker in permanent_markers)
                else "retryable_error"
            )
            raise RuntimeError(
                f"{error_type}: JQData FUT_WAREHOUSE_RECEIPT 查询失败；grid={grid}。"
            ) from error

        if raw_df is None:
            raise RuntimeError(
                f"retryable_error: JQData 仓单查询返回 None；grid={grid}。"
            )
        click.echo(
            f"api_result: table={TABLE_NAME}; function=query_warehouse_grid; phase=query; status=completed; "
            f"grid={grid}; source_calls=1; outcome=response_received; business_validated=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return raw_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=query_warehouse_grid; phase=query; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：月份调度、失败停止与耗时汇总
# 
# `main()` 检查日期、正式写入及性能参数，读取日历窄列完成快照；无待办直接结束。存在待办才认证，依次处理每个品种月份：逐日请求和转换、拼接本次来源事实；只读到此结束当前月份。
# 
# 写入时读取当前事实叶和仓单日历叶，复核待办身份，保留未触达日期并生成 dirty 完整事实叶。事实安装及摘要复读成功后，才生成并提交日历完成状态。查询或转换异常时，只在启用 `--write` 后尝试提交该格点失败状态，再抛错；若回写本身失败，同样停止。
# 
# 当前 `main()` 保留原有 API、事实、日历、分区总耗时计时位置及性能门槛。运行日志统一使用 88 个 `=`、`function/phase/status/elapsed_s`，并保留 monitor 使用的前缀；`partition_committed:` 由事实和日历提交函数分别在对应叶成功后发出，`persisted=false` 标明内存或只读结果。无待办日志只表述可信快照无待办，不声称重新核验了全部事实。
# 
# `main()` 只保留运行边界、自己实际进行的日历窄列读取和月份汇总、原有分区计时及性能门槛，不重复函数内部的生成或提交完成日志。原有计时起止位置和性能窗口条件保持不变；日历状态生成与落盘日志分别由所属函数发出。异常仍按原类型和异常链抛出。
# 各日响应已经由归一化函数按权威 Schema 转换，并固定为请求格点。月度汇总直接拼接这些同类型结果，不再往返转换或重复检查格点子集；日历结果计数复用已有 `group_records`。事实合并后的 dirty 叶业务校验、staging/正式摘要验收及事务边界均保留。启动只读取一次日历规划投影，逐分区仍仅读取当前事实叶与对应日历叶，不扫描 clean 事实历史。
# 

# ### 局部流程：月份调度与运行日志
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["运行开始；参数检查与窄列规划"] --> B{"有待办？"}
#     B -->|否| C["依据完成快照报告无需采集"]
#     B -->|是| D["整批认证一次"]
#     D --> P["当前品种月开始日志"]
#     P --> E["逐日查询与归一化；函数自行报告"]
#     E --> X["直接拼接已契约化日响应"]
#     X --> F{"启用 --write？"}
#     F -->|否| G["只读检查与分区耗时；不提交"]
#     F -->|是| H["读取当前叶；复核待办；合并并提交事实"]
#     H --> I["生成并提交日历；main 汇总原有分区耗时"]
#     I --> J["必要时检查首个完整性能窗口"]
#     J -->|超限| K["失败停止；已提交保留"]
#     J -->|继续| L{"还有分区？"}
#     G --> L
#     L -->|是| P
#     L -->|否| M["运行完成汇总"]
#     E -. 查询或转换失败 .-> N["带 write 时尝试回写失败状态；随后抛错"]
#     N --> K
#     H -. 失败 .-> K
#     I -. 失败 .-> K
# ```

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "--performance-window-size",
    type=click.IntRange(min=1),
)
@click.option(
    "--performance-max-median-seconds",
    type=click.FloatRange(min=0.0, min_open=True),
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    performance_window_size: int | None,
    performance_max_median_seconds: float | None,
    write: bool,
) -> None:
    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n仓单日报 / Warehouse receipts\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
        formal_lake_root = settings.futures_lake_root.resolve()
        resolved_lake_root = (lake_root or formal_lake_root).resolve()
        has_explicit_dates = start_date is not None or end_date is not None

        if (start_date is None) != (end_date is None):
            raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
        has_performance_gate = performance_window_size is not None
        if has_performance_gate != (performance_max_median_seconds is not None):
            raise click.UsageError(
                "--performance-window-size 与 "
                "--performance-max-median-seconds 必须同时提供。"
            )
        if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
            raise click.UsageError(
                "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
                "请移除日期参数使用自动更新，或改用非正式测试湖。"
            )
        if has_performance_gate and (
            not write
            or has_explicit_dates
            or resolved_lake_root != formal_lake_root
        ):
            raise click.UsageError(
                "性能门槛只允许用于无显式日期的正式湖 --write 自动更新。"
            )

        requested_start = start_date.date() if start_date is not None else None
        requested_end = end_date.date() if end_date is not None else None
        if requested_start is not None and requested_start > requested_end:
            raise click.BadParameter("起始日期不得晚于结束日期。")

        silver_root = resolved_lake_root / "silver"
        calendar_path = silver_root / CALENDAR_TABLE_NAME
        fact_path = silver_root / TABLE_NAME

        log_phase = "planning"
        planning_started_at = time.perf_counter()
        calendar_dataset = open_planning_calendar_dataset(calendar_path)
        calendar_filter = (
            (ds.field("dataset_name") == "warehouse_receipt")
            & (ds.field("is_fetch_required") == True)
        )
        if requested_start is not None:
            calendar_filter &= ds.field("trading_date") >= pa.scalar(
                requested_start,
                type=pa.date32(),
            )
            calendar_filter &= ds.field("trading_date") <= pa.scalar(
                requested_end,
                type=pa.date32(),
            )
        log_phase = "read_calendar_plan"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=read_calendar_plan; status=started; "
            f"columns={len(CALENDAR_PLANNING_COLUMNS)}; required_only=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        calendar_planning_df = calendar_dataset.to_table(
            columns=CALENDAR_PLANNING_COLUMNS,
            filter=calendar_filter,
        ).to_pandas()
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=read_calendar_plan; status=completed; "
            f"rows={len(calendar_planning_df)}; required_only=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        validate_table_marker(
            fact_path,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "正式仓单事实",
            required=False,
        )

        pending_df, complete_count = pending_report_grids(calendar_planning_df)
        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"{log_boundary}\nplanning_progress: table={TABLE_NAME}; function=main; phase=planning; status=completed; mode={mode}; planning_seconds={time.perf_counter() - planning_started_at:.3f}; "
            f"lake_root={resolved_lake_root}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        if pending_df.empty:
            click.echo(
                f"up_to_date: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome=up_to_date; "
                f"basis=calendar_completion_snapshot; pending_grids=0; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )
            return

        # 只有确实存在 API 待办时才认证，纯完整性检查不会消耗供应商连接。
        log_phase = "authenticate"
        jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
        partition_groups = pending_df.groupby(
            ["exchange_code", "underlying_code", "year", "month"],
            sort=True,
        )
        partition_count = partition_groups.ngroups
        click.echo(f"partition_plan: function=main; phase=plan; status=completed; pending_partitions={partition_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}")

        total_rows = 0
        processed_grid_count = 0
        successful_partition_seconds: list[float] = []

        for group_number, (raw_partition_key, group_df) in enumerate(
            partition_groups,
            start=1,
        ):
            log_phase = "partition"
            partition_key = tuple(raw_partition_key)
            partition_started_at = time.perf_counter()
            updated_at = datetime.now(timezone.utc)
            batch_id = uuid.uuid4().hex
            frames = []

            click.echo(
                f"partition_start: {group_number}/{partition_count}; "
                f"function=main; phase=partition; status=started; key={partition_key}; grids={len(group_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            group_records = group_df[GRID_COLUMNS].to_dict("records")
            calendar_partition_values = {
                "dataset_name": "warehouse_receipt",
                "exchange_code": partition_key[0],
                "year": partition_key[2],
                "month": partition_key[3],
            }
            calendar_partition_key = tuple(
                calendar_partition_values[column]
                for column in CALENDAR_PARTITION_COLUMNS
            )
            api_started_at = time.perf_counter()
            for grid in group_records:
                grid_key = tuple(grid[column] for column in GRID_COLUMNS)
                try:
                    log_phase = "query"
                    raw_df = query_warehouse_grid(jqdata, grid)
                    log_phase = "normalize"
                    grid_df = normalize_warehouse_response(raw_df, grid, updated_at)
                except Exception as error:
                    log_source_failure_phase = log_phase
                    message = str(error)
                    if write:
                        log_phase = "record_failure"
                        fetch_status = (
                            "retryable_error"
                            if message.startswith("retryable_error:")
                            else "permanent_error"
                        )
                        existing_fact_partition_df = read_partition_leaf(
                            fact_path,
                            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                            PARTITION_COLUMNS,
                            FACT_PARTITIONING,
                            partition_key,
                            "正式仓单事实",
                        )
                        calendar_partition_df = read_partition_leaf(
                            calendar_path,
                            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                            CALENDAR_PARTITION_COLUMNS,
                            CALENDAR_PARTITIONING,
                            calendar_partition_key,
                            "正式报告日历",
                        )
                        ensure_pending_calendar_grids(
                            calendar_partition_df,
                            [grid],
                        )
                        current_fact_count = grid_count_map(
                            existing_fact_partition_df
                        ).get(grid_key, 0)
                        failed_calendar_partition_df = apply_calendar_failure(
                            calendar_partition_df,
                            grid_key,
                            fetch_status,
                            f"仓单采集失败：{message}",
                            batch_id,
                            datetime.now(timezone.utc),
                            current_fact_count,
                        )
                        commit_calendar_partitions(
                            failed_calendar_partition_df,
                            {grid_key},
                            resolved_lake_root,
                        )
                    log_phase = log_source_failure_phase
                    raise click.ClickException(message) from error
                frames.append(grid_df)
            api_seconds = time.perf_counter() - api_started_at

            log_phase = "assemble_facts"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=assemble_month; status=started; "
                f"key={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            incoming_df = pd.concat(frames, ignore_index=True)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=assemble_month; status=completed; "
                f"key={partition_key}; rows={len(incoming_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if not write:
                total_rows += len(incoming_df)
                processed_grid_count += len(group_df)
                partition_seconds = time.perf_counter() - partition_started_at
                click.echo(
                    f"partition_checked: function=main; phase=partition; status=completed; outcome=read_only; key={partition_key}; grids={len(group_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                click.echo(
                    f"partition_timing: function=main; phase=partition; status=completed; key={partition_key}; grids={len(group_df)}; "
                    f"api_seconds={api_seconds:.3f}; fact_seconds=0.000; "
                    f"calendar_seconds=0.000; partition_seconds={partition_seconds:.3f}"
                )
                continue

            log_phase = "read_current_leaves"
            fact_started_at = time.perf_counter()
            existing_fact_partition_df = read_partition_leaf(
                fact_path,
                FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
                PARTITION_COLUMNS,
                FACT_PARTITIONING,
                partition_key,
                "正式仓单事实",
            )
            calendar_partition_df = read_partition_leaf(
                calendar_path,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                CALENDAR_PARTITIONING,
                calendar_partition_key,
                "正式报告日历",
            )
            ensure_pending_calendar_grids(calendar_partition_df, group_records)

            touched_dates = set(group_df["trading_date"].tolist())
            log_phase = "merge_fact"
            complete_df = full_fact_partition(
                existing_fact_partition_df,
                incoming_df,
                touched_dates,
            )
            log_phase = "commit_fact"
            commit_complete_partition(
                complete_df,
                resolved_lake_root,
                partition_key,
            )
            fact_seconds = time.perf_counter() - fact_started_at

            partition_counts = grid_count_map(complete_df)
            if complete_df.empty:
                missing_unit_counts = {}
            else:
                missing_unit_series = (
                    complete_df.loc[
                        complete_df["warehouse_receipt_unit"].isna()
                    ]
                    .groupby(GRID_COLUMNS, dropna=False)
                    .size()
                )
                missing_unit_counts = {
                    tuple(key): int(value)
                    for key, value in missing_unit_series.items()
                }
            grid_results = {}
            for grid in group_records:
                grid_key = tuple(grid[column] for column in GRID_COLUMNS)
                grid_results[grid_key] = (
                    partition_counts.get(grid_key, 0),
                    missing_unit_counts.get(grid_key, 0),
                )
            calendar_started_at = time.perf_counter()
            completed_at = datetime.now(timezone.utc)
            log_phase = "generate_calendar_state"
            completed_calendar_partition_df = apply_calendar_completion(
                calendar_partition_df,
                grid_results,
                batch_id,
                completed_at,
            )
            log_phase = "commit_calendar"
            calendar_rows = commit_calendar_partitions(
                completed_calendar_partition_df,
                set(grid_results),
                resolved_lake_root,
            )
            calendar_seconds = time.perf_counter() - calendar_started_at

            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            partition_seconds = time.perf_counter() - partition_started_at
            click.echo(
                f"partition_timing: function=main; phase=partition; status=completed; key={partition_key}; grids={len(group_df)}; "
                f"api_seconds={api_seconds:.3f}; fact_seconds={fact_seconds:.3f}; "
                f"calendar_seconds={calendar_seconds:.3f}; "
                f"partition_seconds={partition_seconds:.3f}; calendar_rows={calendar_rows}"
            )
            successful_partition_seconds.append(partition_seconds)
            if (
                has_performance_gate
                and len(successful_partition_seconds) == performance_window_size
            ):
                log_phase = "performance_gate"
                gate_result = performance_gate_result(
                    successful_partition_seconds,
                    performance_window_size,
                    performance_max_median_seconds,
                )
                if gate_result is None:
                    raise RuntimeError("性能门槛样本计数状态不一致。")
                gate_passed, median_seconds = gate_result
                if not gate_passed:
                    message = (
                        "performance_gate_failed: "
                        f"samples={performance_window_size}; "
                        f"median_seconds={median_seconds:.3f}; "
                        f"max_median_seconds={performance_max_median_seconds:.3f}"
                    )
                    click.echo(f"{message}; function=main; phase=performance_gate; status=failed; elapsed_s={time.perf_counter() - log_started_at:.3f}")
                    raise click.ClickException(message)
                click.echo(
                    "performance_gate_passed: "
                    f"samples={performance_window_size}; "
                    f"median_seconds={median_seconds:.3f}; "
                    f"max_median_seconds={performance_max_median_seconds:.3f}; function=main; phase=performance_gate; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        click.echo(
            f"finished: function=main; phase=run; status=completed; outcome={'committed' if write else 'read_only'}; grids={processed_grid_count}; rows={total_rows}; "
            f"write={str(write).lower()}; "
            f"performance_samples={len(successful_partition_seconds)}; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        raise


# ## Notebook 与脚本运行入口
# 
# Notebook 使用显式 `notebook_args = []` 和 `standalone_mode=False`，避免读取内核的 `-f` 参数。默认不带 `--write`；有待办仍会认证和请求，只读不代表不联网。只有交互内核且没有 `__file__` 时才走 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时使用命令行参数。
# 
# 最后一个代码单元格仅保存终端命令注释，实际提交由操作者在终端显式运行 `--write`。自动模式按上游待办推进；成对日期只允许只读检查或非正式测试湖写入，本入口没有 `--full`。
# 

# ### 局部流程：Notebook 与脚本执行入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且无 __file__？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
#     B --> C["默认自动只读；有待办仍会认证和请求"]
#     A -->|否| D{"直接运行脚本？"}
#     D -->|是| E["main 读取命令行参数"]
#     D -->|否| F["模块导入：不执行入口"]
# ```
# 

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认执行正式湖自动 dry-run；带写入的测试必须显式使用非正式湖。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b03_warehouse_receipt",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()


# ### 局部流程：终端手动运行
# 
# 下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["在终端激活 latitude_env_v2"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["按上游待办采集并逐叶提交"]
# ```
# 

# In[ ]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a02_Futures_Exchange_Reports\b03_warehouse_receipt.py --write

