#!/usr/bin/env python
# coding: utf-8

# # b02 JQData 期货会员成交持仓排名
# 
# 从报告日历选择待办交易所—品种—交易日，串行合并同一品种月份的请求日期，读取 JQData `finance.FUT_MEMBER_POSITION_RANK`。一份长表响应同时生成逐会员排名与明确参与者类型两张事实表，再回写各自报告日历状态。
# 
# | 上下游 | 本环节的关系 |
# | --- | --- |
# | a02/b01 报告日历 | 提供 `position_rank`、`member_position` 成对的 required 格点；本环节回写完成状态、条数和质量证据。 |
# | a02/b01a 与特殊案例配置 | 仅当完整坏载荷指纹命中冻结案例时，读取其正式 raw 三文件证据并应用指定校准。 |
# | JQData 共享连接 | 只在存在待办时认证；认证和 Windows TUN 出口交给 `config/jqdata_connection.py`。 |
# | `fact_futures_position_rank_daily` | 逐来源合约—会员宽行，保存三类榜单的名次、指标及变化；未上榜指标允许为空。 |
# | `fact_futures_member_position_daily` | 只接纳明确参与者类型汇总标签；不能从普通会员名称猜测类型。 |
# | a02/b03 仓单 | 与本环节共享报告日历，但消费 `warehouse_receipt`；本环节保留其行和状态。 |
# 
# 三张 silver 表的字段、主键与分区来自 `config/data_contracts.py`；特殊案例唯一来源为 `config/futures_lakehouse/futures_position_rank_special_cases.py`。本环节没有独立日期水位文件。业务文本权威见湖仓根目录 `README.md`、`AGENTS.md` 和 `03_Futures_Database/AGENTS.md`。

# ## 自动更新与写入边界
# 
# 待办定义为：`当前 required 成对报告格点 − 两张事实计数与两类日历状态共同证明完整的格点`。仅有事实行或仅有日历完成标记均不够；已完成的 `warning` 可以构成完成证据，不因质量不是 `passed` 而重拉。
# 
# | 运行方式 | 行为 |
# | --- | --- |
# | 默认不带参数 | 计算全部自动待办；有待办才认证、请求和验收，不写数据湖。 |
# | `--write` | 每个品种月份先提交两张事实，再回写两类报告日历完整叶。 |
# | 成对日期 | 仅限制检查范围；只读可用，带写入时必须显式选择不同于正式湖的临时湖。 |
# | 没有待办 | 报告已完整并退出，不认证 JQData。 |
# 
# `--start-date` 与 `--end-date` 必须同时提供；没有 `--full`。`--lake-root` 默认来自 `settings.futures_lake_root`。空事实表由同一差集入口自然得到待办。
# 
# 当前按叶依次提交，不是整个月份共同回滚：第二张事实或后一个日历叶失败时，此前成功叶保留；因共同完成证据仍不齐，该格点会留在下次人工启动的待办中。来源请求不自动重试。

# ## 总流程：两张事实与成对报告日历
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查参数；读取 required 日历与两张事实格点计数"] --> B["比较成对完成证据；形成自动待办"]
#     B --> C{"存在待办？"}
#     C -->|否| D["已完整；不认证、不请求"]
#     C -->|是| E["认证；按交易所、品种、年月串行处理"]
#     E --> F["查询待办日；触顶按日期二分；单日触顶失败"]
#     F --> G["逐日冻结案例校准；归一化为两张事实与 warning"]
#     G --> H{"启用 --write？"}
#     H -->|否| I["仅验收；不落盘"]
#     H -->|是| J["读取当前完整叶；保留未触达数据并合并"]
#     J --> K["排名事实叶提交与复读 → 类型事实叶提交与复读"]
#     K --> L["按正式计数生成状态；两类日历叶依次提交"]
#     L --> M["该月份提交完成；累计进度"]
#     I --> N{"还有月份？"}
#     M --> N
#     N -->|是| F
#     N -->|否| O["汇总运行；无独立日期水位"]
#     F -. 异常 .-> X["停止后续处理；此前成功叶保留；不自动重试"]
#     G -. 异常 .-> X
#     J -. 异常 .-> X
#     K -. 异常 .-> X
#     L -. 异常 .-> X
# ```

# ## 初始化与共享依赖
# 
# 按 `.env.template` 的标记文件约定定位项目根，导入三张权威 Schema、转换函数、共享 JQData 连接、项目设置和冻结特殊案例配置。本格只加载定义，不认证、不请求或写入。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前目录与父目录"] --> B{"找到三个项目标记？"}
#     B -->|是| C["加入项目与湖仓模块路径；导入依赖和配置"]
#     B -->|否| D["抛出根目录定位错误"]
#     C --> E["仅加载定义；不请求、不写入"]
# ```

# In[ ]:


from __future__ import annotations

import hashlib
import json
import pathlib
import re
import shutil
import sys
import uuid
from collections.abc import Callable
from datetime import date, datetime, timezone
from time import perf_counter
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
    FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    FUTURES_POSITION_RANK_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.jqdata_connection import authenticate_jqdata
from config.futures_lakehouse.futures_position_rank_special_cases import (
    POSITION_RANK_SPECIAL_CASES,
)
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## 三张表的 Schema 与有界样例
# 
# 仅在交互内核且没有 `__file__` 时调用共享 Schema 浏览器。依次展示报告日历、排名事实和参与者类型事实；字段说明来自权威 Schema。传入 `lake_root` 后可以显式浏览有界本地样例，因此不能把本格描述成完全不读取数据湖。
# 
# 展示不认证 JQData、不调用来源 API、不写入文件；普通脚本执行跳过展示。

# ### 局部流程：Schema 与样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有文件路径变量？"} -->|是| B["共享浏览器展示三张权威 Schema"]
#     B --> C["可显式查看有界本地样例；不调用 API 或写入"]
#     A -->|否| D["跳过展示"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        FUTURES_POSITION_RANK_DAILY_SCHEMA,
        FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区与来源约定
# 
# 表名、主键及分区列在初始化时从三张 Schema metadata 各读取一次；Hive partitioning 同样在此建立。事实叶为交易所—品种—年月，日历叶为报告类型—交易所—年月，因此日历叶中还包含其他品种，提交时必须保留。
# 
# `JQDATA_FIELDS` 固定来源列；交易所别名只用于来源匹配，参与者标签只接受明确映射。`RUN_QUERY_ROW_LIMIT=5000` 是响应截顶边界，`MAX_SOURCE_RANK=20` 是每个具体合约、每类榜单的名次边界，不能相互替代。

# ### 局部流程：契约与来源常量
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["三张权威 Schema metadata"] --> B["读取表名、主键、分区列；建立 Hive partitioning"]
#     B --> C["固定来源字段、交易所别名与参与者标签"]
#     C --> D["区分响应 5000 行上限与榜单名次 20 上限"]
# ```

# In[ ]:


# 三张表的物理契约只从权威 Schema metadata 各读取一次。
CALENDAR_TABLE_NAME = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货交易所报告采集日历维度表。
CALENDAR_PRIMARY_KEY = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 报告类型—交易所—品种—交易日格点。
CALENDAR_PARTITION_COLUMNS = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 报告日历 Hive 叶分区顺序。

POSITION_TABLE_NAME = FUTURES_POSITION_RANK_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货会员每日成交持仓排名事实表。
POSITION_PRIMARY_KEY = FUTURES_POSITION_RANK_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 日期—交易所—品种—来源合约—会员。
POSITION_PARTITION_COLUMNS = FUTURES_POSITION_RANK_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 排名事实 Hive 叶分区顺序。

MEMBER_TABLE_NAME = FUTURES_MEMBER_POSITION_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货会员类型每日成交持仓事实表。
MEMBER_PRIMARY_KEY = FUTURES_MEMBER_POSITION_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 日期—交易所—品种—来源合约—参与者类型。
MEMBER_PARTITION_COLUMNS = FUTURES_MEMBER_POSITION_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 参与者类型事实 Hive 叶分区顺序。

GRID_COLUMNS = [
    "exchange_code",  # 项目/JQData 标准交易所代码。
    "underlying_code",  # 期货品种代码。
    "trading_date",  # 报告归属交易日。
]
DATASET_NAMES = [
    "position_rank",  # 逐会员排名事实。
    "member_position",  # 明确参与者类型汇总事实。
]

# JQData 表采用一类排名一行；这些是本入口唯一读取的来源列。
CALENDAR_COMPLETION_COLUMNS = [
    "dataset_name",
    *GRID_COLUMNS,
    "is_fetch_required",
    "is_fetch_completed",
    "fetch_result_status",
    "is_data_missing",
    "expected_record_count",
    "actual_record_count",
    "quality_status",
    "fetch_run_id",
    "fetch_completed_at",
    "quality_checked_at",
]

JQDATA_FIELDS = [
    "day",  # 交易日。
    "code",  # JQData 合约代码。
    "exchange",  # JQData 交易所代码。
    "underlying_code",  # 期货品种代码。
    "rank_type_ID",  # 排名类别编码，仅用于交叉校验类别稳定性。
    "rank_type",  # 排名类别中文名称。
    "rank",  # 本类别名次。
    "member_name",  # 会员简称或明确参与者类型汇总标签。
    "indicator",  # 本类别指标值。
    "indicator_increase",  # 本类别指标较前一日变化。
]

RUN_QUERY_ROW_LIMIT = 5000
MAX_SOURCE_RANK = 20
SOURCE_DUPLICATE_REASON_MARKER = "来源重复归一化："
SOURCE_SPECIAL_CASE_REASON_MARKER = "来源特殊校准："
SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]

POSITION_SOURCE = "JQData_FUT_MEMBER_POSITION_RANK"
MEMBER_SOURCE = "JQData_FUT_MEMBER_POSITION_RANK_participant_summary"

# 只有明确汇总标签才能进入参与者类型表；普通会员名称绝不据此猜测类型。
PARTICIPANT_LABELS = {
    "期货公司": "futures_company",
    "期货公司会员": "futures_company",
    "境外特殊经纪参与者": "futures_company",
    "期货公司会员/境外特殊经纪参与者": "futures_company",
    "非期货公司": "non_futures_company",
    "非期货公司会员": "non_futures_company",
    "境外特殊非经纪参与者": "non_futures_company",
    "非期货公司会员/境外特殊非经纪参与者": "non_futures_company",
}

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
POSITION_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_POSITION_RANK_DAILY_SCHEMA.field(name)
        for name in POSITION_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
MEMBER_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_MEMBER_POSITION_DAILY_SCHEMA.field(name)
        for name in MEMBER_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 数据集物理契约与表身份
# 
# `open_exact_dataset()` 发现表根 Parquet 文件、打开 Hive Dataset，并重建分区字段后核对物理类型、nullable 及表名/主键/分区身份。描述性 metadata 允许漂移，以当前配置说明为准。
# 
# 该入口用于启动时的完成计划；月份循环中的完整叶读取使用后面的 `read_complete_partition()`，不在每个月重新发现整张表。
# 
# `open_exact_dataset()` 自行报告发现文件、打开与物理契约核对的起止；缺失的可选事实表报告跳过。这里不为日志读取总行数。

# ### 局部流程：数据集打开与兼容性
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["启动时发现表根 Parquet"] --> B{"存在可读文件？"}
#     B -->|否| X["抛出缺失异常"]
#     B -->|是| C["打开 Hive Dataset；补回分区字段"]
#     C --> D{"物理字段与表身份兼容？"}
#     D -->|否| Y["抛出契约异常"]
#     D -->|是| E["返回 Dataset；允许描述性 metadata 漂移"]
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


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    log_started_at = perf_counter()
    log_phase = 'read_dataset'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=read_dataset; status=started; "
        f"path={table_path}; label={label}; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        # 正式输入、staging 与提交后输出都检查物理契约和表身份。
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else []
        )
        if not parquet_files:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        log_phase = "open_dataset"
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=read_dataset; status=running; "
            f"path={table_path}; discovered_files={len(parquet_files)}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
        )
        if not physically_and_identity_compatible(
            reconstructed_schema(dataset, schema),
            schema,
        ):
            raise TypeError(f"{label}物理结构或表身份与权威契约不一致。")

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=read_dataset; status=completed; "
            f"path={table_path}; discovered_files={len(parquet_files)}; outcome=opened; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=read_dataset; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 三张表的业务验收
# 
# 三个 validator 分别验证日历状态、逐会员排名及参与者类型事实，并按权威主键排序。当前实现包含 Schema 往返、主键、分区及逐行检查；本轮全部保留，重复验收的收缩留待第 7 项。

# ### 局部流程：三张表业务验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["输入 DataFrame"] --> B["按权威 Schema 转换；检查主键唯一"]
#     B --> C{"所属表？"}
#     C -->|日历| D["状态枚举、完成证据、计数、时间与分区"]
#     C -->|排名事实| E["来源、会员、名次与数量配对、合约及分区"]
#     C -->|类型事实| F["明确参与者类型、来源、数量及分区"]
#     D --> G["按主键排序返回；任一检查失败即抛错"]
#     E --> G
#     F --> G
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
        if row["dataset_name"] not in {"position_rank", "member_position", "warehouse_receipt"}:
            raise ValueError(f"{context}报告数据集名称不在权威枚举中。")
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


def validate_position_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_POSITION_RANK_DAILY_SCHEMA.names],
        FUTURES_POSITION_RANK_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(checked, FUTURES_POSITION_RANK_DAILY_SCHEMA)
    if normalized.duplicated(POSITION_PRIMARY_KEY).any():
        raise ValueError(f"{context}排名事实主键不唯一。")

    for row in checked.to_pylist():
        if not str(row["source_symbol"]).strip() or not str(row["member_name"]).strip():
            raise ValueError(f"{context}来源合约或会员名称不得为空。")
        if row["source"] != POSITION_SOURCE:
            raise ValueError(f"{context}排名事实来源不一致。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}排名事实年月分区与交易日不一致。")

        metric_pairs = [
            ("volume_rank", "volume"),
            ("long_position_rank", "long_position"),
            ("short_position_rank", "short_position"),
        ]
        if not any(row[value_name] is not None for _, value_name in metric_pairs):
            raise ValueError(f"{context}排名事实至少需要一个成交或持仓指标。")
        for rank_name, value_name in metric_pairs:
            rank_value = row[rank_name]
            metric_value = row[value_name]
            if (rank_value is None) != (metric_value is None):
                raise ValueError(f"{context}{rank_name} 与 {value_name} 必须同时存在或同时为空。")
            if rank_value is not None and rank_value <= 0:
                raise ValueError(f"{context}{rank_name} 必须大于零。")
            if metric_value is not None and metric_value < 0:
                raise ValueError(f"{context}{value_name} 不得为负。")

        contract_code = row["contract_code"]
        if contract_code is not None:
            code_body, separator, suffix = contract_code.partition(".")
            if not separator or suffix != row["exchange_code"] or not any(character.isdigit() for character in code_body):
                raise ValueError(f"{context}标准合约代码不合法。")

    return normalized.sort_values(POSITION_PRIMARY_KEY).reset_index(drop=True)


def validate_member_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = pandas_to_arrow(
        frame.loc[:, FUTURES_MEMBER_POSITION_DAILY_SCHEMA.names],
        FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(checked, FUTURES_MEMBER_POSITION_DAILY_SCHEMA)
    if normalized.duplicated(MEMBER_PRIMARY_KEY).any():
        raise ValueError(f"{context}参与者类型事实主键不唯一。")

    for row in checked.to_pylist():
        if row["participant_type"] not in {"futures_company", "non_futures_company"}:
            raise ValueError(f"{context}参与者类型不在允许枚举中。")
        if not str(row["source_symbol"]).strip():
            raise ValueError(f"{context}参与者类型事实来源合约不得为空。")
        if row["source"] != MEMBER_SOURCE:
            raise ValueError(f"{context}参与者类型事实来源不一致。")
        if row["trading_date"].year != row["year"] or row["trading_date"].month != row["month"]:
            raise ValueError(f"{context}参与者类型事实年月分区与交易日不一致。")

        measure_names = [
            "volume",
            "long_position",
            "short_position",
        ]
        if not any(row[name] is not None for name in measure_names):
            raise ValueError(f"{context}参与者类型事实至少需要一个成交或持仓指标。")
        if any(row[name] is not None and row[name] < 0 for name in measure_names):
            raise ValueError(f"{context}参与者类型数量指标不得为负。")

    return normalized.sort_values(MEMBER_PRIMARY_KEY).reset_index(drop=True)




# ## 启动窄列计数与当前完整叶读取
# 
# 启动阶段允许事实表不存在，并分批累计格点行数；写入阶段只枚举指定叶文件，检查根级零行标记、每个文件的物理契约，补回分区字段后完整验收。不存在的表或叶返回权威空表；缺失或非法标记按现有分支处理。
# 
# 窄列计数按现有 RecordBatch 累计批次与扫描行数，至多每 2 秒报告一次。完整叶读取按现有文件循环累计已读文件，并报告空表、读取、验收及失败阶段；不新增目录扫描或复读。

# ### 局部流程：窄列计数与完整叶读取
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"读取目的？"} -->|启动计划| B["可选事实表：缺失返回空；存在则核对契约"]
#     B --> C["函数沿现有批次报告扫描行数与格点计数"]
#     A -->|当前叶| D["检查根级零行标记；仅枚举目标叶文件"]
#     D --> E["核对每个物理文件；读取并补回分区列"]
#     E --> F["验收；函数报告文件数、行数及完成"]
# ```

# In[ ]:


def open_optional_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset | None:
    if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
        click.echo(
            f"planning_progress: function=open_optional_exact_dataset; phase=read_dataset; status=skipped; "
            f"path={table_path}; label={label}; reason=no_parquet; elapsed_s=0.000"
        )
        return None
    return open_exact_dataset(table_path, partitioning, schema, label)


def dataset_grid_count_map(
    dataset: ds.Dataset | None,
) -> dict[tuple[object, ...], int]:
    log_started_at = perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'count_grids'
    log_table_name = (dataset.schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8") if dataset is not None else "absent_facts"
    log_scanned_rows = 0
    log_batch_count = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=dataset_grid_count_map; phase=count_grids; status=started; "
        f"elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        if dataset is None:
            click.echo(
                f"planning_progress: table={log_table_name}; function=dataset_grid_count_map; phase=count_grids; status=completed; "
                f"batches=0; scanned_rows=0; grids=0; outcome=no_dataset; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            return {}

        counts_by_grid: dict[tuple[object, ...], int] = {}
        for record_batch in dataset.scanner(
            columns=GRID_COLUMNS,
            batch_size=65536,
        ).to_batches():
            log_phase = "count_batch"
            grid_df = record_batch.to_pandas()
            batch_counts = grid_df.groupby(GRID_COLUMNS, dropna=False).size()
            for grid_key, count in batch_counts.items():
                normalized_grid_key = tuple(grid_key)
                counts_by_grid[normalized_grid_key] = (
                    counts_by_grid.get(normalized_grid_key, 0) + int(count)
                )
            log_scanned_rows += record_batch.num_rows
            log_batch_count += 1
            if perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = perf_counter()
                click.echo(
                    f"planning_progress: table={log_table_name}; function=dataset_grid_count_map; phase=count_grids; status=running; "
                    f"batches={log_batch_count}; scanned_rows={log_scanned_rows}; grids={len(counts_by_grid)}; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={log_table_name}; function=dataset_grid_count_map; phase=count_grids; status=completed; "
            f"batches={log_batch_count}; scanned_rows={log_scanned_rows}; grids={len(counts_by_grid)}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return counts_by_grid
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=dataset_grid_count_map; phase=count_grids; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


def read_complete_partition(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
    validator: Callable[[pd.DataFrame, str], pd.DataFrame],
    label: str,
) -> pd.DataFrame:
    log_started_at = perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'read_leaf'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    log_read_files = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=started; "
        f"key={partition_key}; path={table_path}; label={label}; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        if len(partition_columns) != len(partition_key):
            raise ValueError(f"{label}分区键数量与权威分区列不一致。")
        if not table_path.is_dir():
            click.echo(
                f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=completed; "
                f"key={partition_key}; path={table_path}; rows=0; outcome=empty; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(schema)

        file_schema = pa.schema(
            [field for field in schema if field.name not in partition_columns],
            metadata=schema.metadata,
        )
        marker_path = table_path / "schema.parquet"
        if not marker_path.is_file():
            if next(table_path.iterdir(), None) is None:
                click.echo(
                    f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=completed; "
                    f"key={partition_key}; path={table_path}; rows=0; outcome=empty; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
                return empty_pandas(schema)
            raise FileNotFoundError(f"{label}缺少根级 schema.parquet。")
        log_phase = "read_marker"
        marker_file = pq.ParquetFile(marker_path)
        if (
            marker_file.metadata.num_rows != 0
            or not physically_and_identity_compatible(
                marker_file.schema_arrow,
                file_schema,
            )
        ):
            raise TypeError(f"{label}根级 schema.parquet 与权威物理契约不一致。")

        relative_path = pathlib.Path(*[
            f"{column}={value}"
            for column, value in zip(partition_columns, partition_key, strict=True)
        ])
        leaf_path = table_path / relative_path
        leaf_files = sorted(leaf_path.glob("*.parquet")) if leaf_path.is_dir() else []
        if not leaf_files:
            click.echo(
                f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=completed; "
                f"key={partition_key}; path={table_path}; rows=0; outcome=empty; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(schema)

        log_phase = "read_files"
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=running; "
            f"key={partition_key}; path={leaf_path}; files=0/{len(leaf_files)}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        physical_tables = []
        for leaf_file in leaf_files:
            if not physically_and_identity_compatible(
                pq.read_schema(leaf_file),
                file_schema,
            ):
                raise TypeError(f"{label}叶文件物理结构或表身份与权威契约不一致：{leaf_file}")
            physical_tables.append(pq.ParquetFile(leaf_file).read())
            log_read_files += 1
            if perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = perf_counter()
                click.echo(
                    f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=running; "
                    f"key={partition_key}; files={log_read_files}/{len(leaf_files)}; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
        physical_table = pa.concat_tables(physical_tables)
        partition_values = dict(zip(partition_columns, partition_key, strict=True))
        complete_table = pa.Table.from_arrays(
            [
                pa.array(
                    [partition_values[field.name]] * physical_table.num_rows,
                    type=field.type,
                )
                if field.name in partition_values
                else physical_table[field.name]
                for field in schema
            ],
            schema=schema,
        )
        log_phase = "validate_leaf"
        validated_partition_df = validator(
            arrow_to_pandas(complete_table, schema),
            f"{label}的完整叶",
        )
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=completed; "
            f"key={partition_key}; path={table_path}; files={log_read_files}/{len(leaf_files)}; rows={len(validated_partition_df)}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return validated_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=read_complete_partition; phase=read_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 已冻结特殊案例的显式校准
# 
# 只处理配置中的指定交易所、品种、交易日、来源合约和榜单类别。目标榜必须恰好有 20 行，再核对完整名次、会员、指标与变化。
# 
# | 目标榜载荷 | 处理 |
# | --- | --- |
# | 已等于冻结官方值 | 原样通过，不要求 raw 证据，也不新加校准 warning。 |
# | 精确等于冻结坏指纹 | 验收 b01a 正式 raw 的三文件集合、完整原文摘要、sidecar 和清单，然后整榜替换为冻结官方值。 |
# | 任何第三种载荷 | 硬失败，不推断、不局部修补。 |
# 
# 校准不修改其他榜单或未配置格点；返回包含案例 ID 和官方原文摘要的 warning，随后永久写入报告日历。这里不请求上期所来源、不创建 raw 证据。
# 
# 特殊案例函数自行报告匹配与校准结果、持久化前的 warning；正式 raw 验收函数沿三次既有读取报告 `verified_files=1/3` 至 `3/3`。校准完成表示内存响应已调整，仍为 `persisted=false`。

# ### 局部流程：冻结特殊案例
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"命中配置格点？"} -->|否| B["保持来源响应"]
#     A -->|是| C["定位目标榜；核对完整 20 行与类别"]
#     C --> D{"完整载荷等于哪一份冻结值？"}
#     D -->|官方值| B
#     D -->|坏指纹| E["验收 b01a 三文件正式 raw 证据"]
#     D -->|均不匹配| X["抛错；不猜测修复"]
#     E --> F["整榜替换为官方值；附案例与摘要 warning"]
#     F --> G["函数报告校准与 warning；返回内存响应"]
#     B --> G
# ```

# In[ ]:


def verify_special_case_artifacts(
    lake_root: pathlib.Path,
    special_case: dict[str, object],
) -> None:
    log_started_at = perf_counter()
    log_phase = 'verify_raw'
    click.echo(
        f"planning_progress: table={POSITION_TABLE_NAME}; function=verify_special_case_artifacts; phase=verify_raw; status=started; "
        f"case_id={special_case['case_id']}; verified_files=0/3; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        raw_root = lake_root.resolve() / "raw"
        artifact_path = raw_root / special_case["raw_relative_path"]
        if not artifact_path.resolve().is_relative_to(raw_root):
            raise ValueError("特殊案例证据路径越出 raw 根目录。")
        if not artifact_path.is_dir():
            raise FileNotFoundError(
                "缺少排名特殊案例正式证据；请先运行 "
                "a02/b01a_position_rank_special_case_calibration；"
                f"case_id={special_case['case_id']}。"
            )
        expected_filenames = {
            "response.dat",
            "response.sha256",
            "calibration.json",
        }
        actual_filenames = {path.name for path in artifact_path.iterdir()}
        if actual_filenames != expected_filenames:
            raise ValueError("排名特殊案例正式证据文件集合不一致。")

        log_phase = "response_hash"
        response_sha256 = hashlib.sha256(
            (artifact_path / "response.dat").read_bytes()
        ).hexdigest()
        if response_sha256 != special_case["official_response_sha256"]:
            raise ValueError("排名特殊案例正式原文 SHA-256 与冻结值不一致。")
        log_phase = "sidecar"
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=verify_special_case_artifacts; phase=verify_raw; status=running; "
            f"case_id={special_case['case_id']}; verified_files=1/3; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        sidecar_sha256 = (artifact_path / "response.sha256").read_text(
            encoding="ascii"
        ).strip()
        if sidecar_sha256 != response_sha256:
            raise ValueError("排名特殊案例摘要 sidecar 与正式原文不一致。")

        log_phase = "manifest"
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=verify_special_case_artifacts; phase=verify_raw; status=running; "
            f"case_id={special_case['case_id']}; verified_files=2/3; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        expected_manifest = {
            "case_id": special_case["case_id"],
            "trading_date": special_case["trading_date"].isoformat(),
            "exchange_code": special_case["exchange_code"],
            "underlying_code": special_case["underlying_code"],
            "source_symbol": special_case["source_symbol"],
            "rank_type_id": special_case["rank_type_id"],
            "rank_type": special_case["rank_type"],
            "official_url": special_case["official_url"],
            "official_response_sha256": response_sha256,
            "official_rows": [list(row) for row in special_case["official_rows"]],
        }
        actual_manifest = json.loads(
            (artifact_path / "calibration.json").read_text(encoding="utf-8")
        )
        if actual_manifest != expected_manifest:
            raise ValueError("排名特殊案例校准清单与冻结配置不一致。")
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=verify_special_case_artifacts; phase=verify_raw; status=completed; "
            f"case_id={special_case['case_id']}; verified_files=3/3; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=verify_special_case_artifacts; phase=verify_raw; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


def apply_position_rank_special_cases(
    raw_df: pd.DataFrame,
    grid: dict[str, object],
    lake_root: pathlib.Path,
) -> tuple[pd.DataFrame, list[str]]:
    log_started_at = perf_counter()
    log_phase = 'special_case'
    log_matched_cases = 0
    log_calibrated_cases = 0
    click.echo(
        f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=started; "
        f"grid={grid}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        calibrated_df = raw_df.copy()
        quality_warnings = []

        for special_case in POSITION_RANK_SPECIAL_CASES:
            case_grid = (
                special_case["exchange_code"],
                special_case["underlying_code"],
                special_case["trading_date"],
            )
            actual_grid = tuple(grid[column] for column in GRID_COLUMNS)
            if actual_grid != case_grid:
                continue

            log_phase = "match_payload"
            log_matched_cases += 1
            click.echo(
                f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=running; "
                f"case_id={special_case['case_id']}; grid={grid}; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            normalized_codes = calibrated_df["code"].astype(str).str.strip().str.upper()
            numeric_rank_type_ids = pd.to_numeric(
                calibrated_df["rank_type_ID"],
                errors="coerce",
            )
            case_mask = (
                normalized_codes.eq(special_case["source_symbol"])
                & numeric_rank_type_ids.eq(special_case["rank_type_id"])
            )
            case_df = calibrated_df.loc[case_mask].copy()
            if len(case_df) != 20:
                raise ValueError(
                    "schema_error: 已知排名特殊案例没有恰好返回 20 条目标记录；"
                    f"case_id={special_case['case_id']}, rows={len(case_df)}。"
                )
            normalized_rank_types = case_df["rank_type"].map(
                lambda value: re.sub(r"\s+", "", str(value)).casefold()
            )
            expected_rank_type = re.sub(
                r"\s+",
                "",
                special_case["rank_type"],
            ).casefold()
            if not normalized_rank_types.eq(expected_rank_type).all():
                raise ValueError("schema_error: 已知排名特殊案例类别文字发生变化。")

            ranks = pd.to_numeric(case_df["rank"], errors="coerce")
            indicators = pd.to_numeric(case_df["indicator"], errors="coerce")
            increases = pd.to_numeric(
                case_df["indicator_increase"],
                errors="coerce",
            )
            if (
                ranks.isna().any()
                or indicators.isna().any()
                or increases.isna().any()
                or not ranks.map(lambda value: float(value).is_integer()).all()
                or not indicators.map(lambda value: float(value).is_integer()).all()
                or not increases.map(lambda value: float(value).is_integer()).all()
            ):
                raise ValueError("schema_error: 已知排名特殊案例数值类型发生变化。")
            actual_rows = tuple(sorted(
                (
                    int(rank),
                    str(member_name).strip(),
                    int(indicator),
                    int(increase),
                )
                for rank, member_name, indicator, increase in zip(
                    ranks,
                    case_df["member_name"],
                    indicators,
                    increases,
                    strict=True,
                )
            ))
            if actual_rows == special_case["official_rows"]:
                click.echo(
                    f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=running; "
                    f"case_id={special_case['case_id']}; outcome=already_official; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
                continue
            if actual_rows != special_case["expected_jqdata_rows"]:
                raise ValueError(
                    "schema_error: 已知排名特殊案例既不匹配冻结坏指纹，也不匹配官方值；"
                    f"case_id={special_case['case_id']}。"
                )

            log_phase = "verify_raw"
            verify_special_case_artifacts(lake_root, special_case)
            log_phase = "calibrate"
            official_rows_by_rank = {
                row[0]: row for row in special_case["official_rows"]
            }
            for row_index in case_df.index:
                rank = int(pd.to_numeric(calibrated_df.at[row_index, "rank"]))
                _, member_name, indicator, increase = official_rows_by_rank[rank]
                calibrated_df.at[row_index, "member_name"] = member_name
                calibrated_df.at[row_index, "indicator"] = indicator
                calibrated_df.at[row_index, "indicator_increase"] = increase
            quality_warnings.append(
                f"{SOURCE_SPECIAL_CASE_REASON_MARKER}{special_case['case_id']}："
                "JQData 成交量 Top 20 与冻结坏指纹完全一致，已整体按上期所"
                "官方历史文件校准；"
                f"official_sha256={special_case['official_response_sha256']}"
            )
            log_calibrated_cases += 1
            click.echo(
                f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=running; "
                f"case_id={special_case['case_id']}; outcome=calibrated; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )

        for log_warning in quality_warnings:
            click.echo(
                f"source_quality_warning: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=warning; "
                f"grid={grid}; details={log_warning}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=completed; "
            f"grid={grid}; matched_cases={log_matched_cases}; calibrated_cases={log_calibrated_cases}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return calibrated_df, quality_warnings
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}; function=apply_position_rank_special_cases; phase=special_case; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源长表归一化与两张事实生成
# 
# `normalize_rank_response()` 核对来源类型、列、行数、日期、品种及交易所。按 `rank_type` 文字识别成交量、持买仓和持卖仓，`rank_type_ID` 只用于检查类别映射一致性；来源名次必须是 1—20 的整数。
# 
# 每行按来源合约及会员身份归入逐会员事实或明确参与者类型事实，保留来源合约代码；标准合约只在来源代码具有月份数字时生成。相应指标与变化透视为宽行，缺少的榜单指标保持空值。
# 
# 同一事实键的重复来源行，只有原始标签、类别 ID/文字、指标及变化全部一致才合并；逐会员名次取最小值，并返回重复来源 warning。任何业务值冲突都失败。最终两张输出各自通过业务 validator；空响应返回权威空表。
# 
# 生成函数自行报告输入处理、两张事实验收和输出条数；沿原行循环每 1000 行检查 2 秒进度间隔。`api_success:` 在本函数生成及验收成功后发出，标记 `persisted=false`；异常仍保留原类型及内容。

# ### 局部流程：来源归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["核对来源结构、请求范围及行数"] --> B["逐行核对类别、名次、指标、身份并报告进度"]
#     B --> C{"明确参与者类型标签？"}
#     C -->|否| D["合约与会员排名宽行"]
#     C -->|是| E["合约与参与者类型宽行"]
#     D --> F["重复键业务值一致才合并；冲突抛错"]
#     E --> F
#     F --> G["排名取最小值；记录来源重复 warning"]
#     G --> H["两表验收；函数报告生成完成、persisted=false"]
# ```

# In[ ]:


def metric_name_from_rank_type(rank_type: object) -> str:
    # 不猜测纯数字编码；业务含义必须由 JQData 同行 rank_type 文字证明。
    text = re.sub(r"[\s_\-/]+", "", str(rank_type)).lower()
    matches = []
    if "成交" in text or "volume" in text:
        matches.append("volume")
    if any(keyword in text for keyword in ["持买", "买持仓", "多头", "long"]):
        matches.append("long_position")
    if any(keyword in text for keyword in ["持卖", "卖持仓", "空头", "short"]):
        matches.append("short_position")

    if len(matches) != 1:
        raise ValueError(f"schema_error: 无法唯一识别 JQData rank_type={rank_type!r}。")
    return matches[0]


def normalize_source_code(
    raw_code: object,
    exchange_code: str,
    underlying_code: str,
) -> tuple[str, str | None]:
    source_symbol = str(raw_code).strip().upper()
    if not source_symbol or source_symbol in {"NAN", "NONE"}:
        raise ValueError("schema_error: JQData code 为空。")

    code_body, separator, suffix = source_symbol.partition(".")
    allowed_suffixes = JQDATA_RESPONSE_EXCHANGES.get(exchange_code, {exchange_code})
    if separator and suffix not in allowed_suffixes:
        raise ValueError(
            "schema_error: JQData code 后缀与报告日历交易所不一致；"
            f"code={source_symbol}, expected_one_of={sorted(allowed_suffixes)}。"
        )
    match = re.match(r"^(?P<underlying>[A-Z]+)", code_body)
    if match is None or match.group("underlying") != underlying_code:
        raise ValueError(
            "schema_error: JQData code 与报告日历品种不一致；"
            f"code={source_symbol}, expected={underlying_code}。"
        )

    contract_code = None
    if any(character.isdigit() for character in code_body):
        contract_code = f"{code_body}.{exchange_code}"
    return source_symbol, contract_code


def normalize_rank_response(
    raw_df: pd.DataFrame,
    grid: dict[str, object],
    updated_at: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, list[str]]]:
    log_started_at = perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'generate_facts'
    log_table_name = POSITION_TABLE_NAME + "+" + MEMBER_TABLE_NAME
    log_visited_rows = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=started; "
        f"grid={grid}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        if not isinstance(raw_df, pd.DataFrame):
            raise TypeError("schema_error: JQData 排名查询未返回 DataFrame。")
        if raw_df.empty:
            click.echo(
                f"api_success: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=completed; "
                f"grid={grid}; source_rows=0; position_rows=0; member_rows=0; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            return (
                empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA),
                empty_pandas(FUTURES_MEMBER_POSITION_DAILY_SCHEMA),
                {dataset_name: [] for dataset_name in DATASET_NAMES},
            )

        missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
        if missing_columns:
            raise ValueError(f"schema_error: JQData 排名表缺列 {sorted(missing_columns)}。")
        if len(raw_df) >= RUN_QUERY_ROW_LIMIT:
            raise ValueError("schema_error: 单格点响应达到 run_query 上限，不能证明结果完整。")

        response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
        response_df["day"] = pd.to_datetime(response_df["day"], errors="coerce").dt.date
        if response_df["day"].isna().any() or not response_df["day"].eq(grid["trading_date"]).all():
            raise ValueError("schema_error: JQData 排名响应包含请求交易日之外的行。")

        response_underlying = response_df["underlying_code"].astype(str).str.strip().str.upper()
        if not response_underlying.eq(grid["underlying_code"]).all():
            raise ValueError("schema_error: JQData 排名响应品种与待办日历不一致。")
        response_exchange_values = response_df["exchange"]
        response_exchanges = response_exchange_values.astype(str).str.strip().str.upper()
        allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(grid["exchange_code"], {grid["exchange_code"]})
        if (
            response_exchange_values.isna().any()
            or response_exchanges.eq("").any()
            or not response_exchanges.isin(allowed_exchanges).all()
        ):
            actual_exchange_values = sorted({
                "<null>" if pd.isna(value) else str(value).strip().upper()
                for value in response_exchange_values
            })
            raise ValueError(
                "schema_error: JQData 排名响应交易所与待办日历不一致；"
                f"actual={actual_exchange_values}。"
            )

        log_phase = "normalize_rows"
        log_source_rows = len(response_df)
        position_rows: dict[tuple[str, str], dict[str, object]] = {}
        member_rows: dict[tuple[str, str], dict[str, object]] = {}
        rank_type_ids: dict[int, str] = {}
        source_payload_by_metric_key: dict[
            tuple[str, str, str, str],
            tuple[str, int, str, float, float | None],
        ] = {}
        source_ranks_by_metric_key: dict[
            tuple[str, str, str, str], set[int]
        ] = {}
        source_duplicate_count_by_metric_key: dict[
            tuple[str, str, str, str], int
        ] = {}

        for source_row in response_df.to_dict("records"):
            log_visited_rows += 1
            if log_visited_rows % 1000 == 0 and perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = perf_counter()
                click.echo(
                    f"planning_progress: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=running; "
                    f"grid={grid}; visited_rows={log_visited_rows}/{log_source_rows}; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
            metric_name = metric_name_from_rank_type(source_row["rank_type"])
            rank_type_id = int(source_row["rank_type_ID"])
            previous_metric = rank_type_ids.setdefault(rank_type_id, metric_name)
            if previous_metric != metric_name:
                raise ValueError("schema_error: 同一 rank_type_ID 对应多个业务类别。")

            rank_value = pd.to_numeric(source_row["rank"], errors="coerce")
            indicator_value = pd.to_numeric(source_row["indicator"], errors="coerce")
            increase_value = pd.to_numeric(source_row["indicator_increase"], errors="coerce")
            if (
                isinstance(source_row["rank"], bool)
                or pd.isna(rank_value)
                or not 1 <= rank_value <= MAX_SOURCE_RANK
                or int(rank_value) != rank_value
            ):
                raise ValueError(
                    f"schema_error: JQData 排名名次必须是 1 至 {MAX_SOURCE_RANK} 的整数。"
                )
            if pd.isna(indicator_value) or indicator_value < 0:
                raise ValueError("schema_error: JQData 排名指标必须非空且非负。")

            member_name = str(source_row["member_name"]).strip()
            if not member_name or member_name in {"nan", "None"}:
                raise ValueError("schema_error: JQData member_name 为空。")
            source_symbol, contract_code = normalize_source_code(
                source_row["code"],
                grid["exchange_code"],
                grid["underlying_code"],
            )
            normalized_rank = int(rank_value)
            normalized_indicator = float(indicator_value)
            normalized_increase = (
                None if pd.isna(increase_value) else float(increase_value)
            )
            normalized_rank_type = re.sub(
                r"\s+", "", str(source_row["rank_type"])
            ).casefold()
            metric_change_name = f"{metric_name}_change"
            participant_type = PARTICIPANT_LABELS.get(member_name)

            if participant_type is None:
                key = (source_symbol, member_name)
                row = position_rows.setdefault(
                    key,
                    {
                        "trading_date": grid["trading_date"],
                        "exchange_code": grid["exchange_code"],
                        "underlying_code": grid["underlying_code"],
                        "source_symbol": source_symbol,
                        "contract_code": contract_code,
                        "member_name": member_name,
                        "volume_rank": None,
                        "volume": None,
                        "volume_change": None,
                        "long_position_rank": None,
                        "long_position": None,
                        "long_position_change": None,
                        "short_position_rank": None,
                        "short_position": None,
                        "short_position_change": None,
                        "source": POSITION_SOURCE,
                        "updated_at": updated_at,
                        "year": grid["trading_date"].year,
                        "month": grid["trading_date"].month,
                    },
                )
                dataset_name = "position_rank"
                source_identity = member_name
            else:
                key = (source_symbol, participant_type)
                row = member_rows.setdefault(
                    key,
                    {
                        "trading_date": grid["trading_date"],
                        "exchange_code": grid["exchange_code"],
                        "underlying_code": grid["underlying_code"],
                        "source_symbol": source_symbol,
                        "participant_type": participant_type,
                        "volume": None,
                        "volume_change": None,
                        "long_position": None,
                        "long_position_change": None,
                        "short_position": None,
                        "short_position_change": None,
                        "source": MEMBER_SOURCE,
                        "updated_at": updated_at,
                        "year": grid["trading_date"].year,
                        "month": grid["trading_date"].month,
                    },
                )
                dataset_name = "member_position"
                source_identity = participant_type

            metric_key = (
                dataset_name,
                source_symbol,
                source_identity,
                metric_name,
            )
            source_payload = (
                member_name,
                rank_type_id,
                normalized_rank_type,
                normalized_indicator,
                normalized_increase,
            )
            previous_payload = source_payload_by_metric_key.get(metric_key)
            if previous_payload is not None:
                if previous_payload != source_payload:
                    raise ValueError(
                        "schema_error: 同一来源事实键的重复行存在指标、变化、"
                        f"类别或原始参与者标签冲突；key={metric_key}。"
                    )
                source_ranks_by_metric_key[metric_key].add(normalized_rank)
                source_duplicate_count_by_metric_key[metric_key] += 1
                if dataset_name == "position_rank":
                    rank_name = f"{metric_name}_rank"
                    row[rank_name] = min(row[rank_name], normalized_rank)
                continue

            source_payload_by_metric_key[metric_key] = source_payload
            source_ranks_by_metric_key[metric_key] = {normalized_rank}
            source_duplicate_count_by_metric_key[metric_key] = 0
            if dataset_name == "position_rank":
                row[f"{metric_name}_rank"] = normalized_rank
            row[metric_name] = normalized_indicator
            row[metric_change_name] = normalized_increase

        quality_warnings_by_dataset = {
            dataset_name: [] for dataset_name in DATASET_NAMES
        }
        for metric_key, duplicate_count in source_duplicate_count_by_metric_key.items():
            if duplicate_count == 0:
                continue
            dataset_name, source_symbol, source_identity, metric_name = metric_key
            source_ranks = sorted(source_ranks_by_metric_key[metric_key])
            merge_rule = (
                f"取最小名次 {min(source_ranks)}"
                if dataset_name == "position_rank"
                else "按参与者类型事实粒度合并"
            )
            quality_warnings_by_dataset[dataset_name].append(
                f"{source_symbol}/{source_identity}/{metric_name} 来源重复 "
                f"{duplicate_count + 1} 行，原名次={source_ranks}，"
                f"指标与变化一致，{merge_rule}"
            )

        position_df = pd.DataFrame(
            position_rows.values(),
            columns=FUTURES_POSITION_RANK_DAILY_SCHEMA.names,
        ) if position_rows else empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA)
        member_df = pd.DataFrame(
            member_rows.values(),
            columns=FUTURES_MEMBER_POSITION_DAILY_SCHEMA.names,
        ) if member_rows else empty_pandas(FUTURES_MEMBER_POSITION_DAILY_SCHEMA)

        log_phase = "validate_facts"
        validated_position_df = validate_position_frame(position_df, "JQData 转换后的")
        validated_member_df = validate_member_frame(member_df, "JQData 转换后的")
        for log_dataset_name, log_warnings in quality_warnings_by_dataset.items():
            if log_warnings:
                click.echo(
                    f"source_quality_warning: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=warning; "
                    f"grid={grid}; dataset={log_dataset_name}; details={' | '.join(log_warnings)}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"api_success: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=completed; "
            f"grid={grid}; source_rows={len(response_df)}; position_rows={len(validated_position_df)}; member_rows={len(validated_member_df)}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return validated_position_df, validated_member_df, quality_warnings_by_dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=normalize_rank_response; phase=generate_facts; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完成证明与自动待办
# 
# 启动时只投影两张事实的格点列计数，再与两类 required 日历行比较。`pending_report_grids()` 要求每个交易所—品种—日同时具备成对日历行。
# 
# 非空事实需要 `success`、实际计数匹配、无缺失标记、完整审计字段以及 `passed` 或 `warning`；零行需要 `empty_confirmed + warning` 及匹配计数和缺失标记。只有两类都完整才扣除；其他格点进入按交易所—品种—年月排序的待办。
# 
# 这一启动扫描是当前 b02 的完成判定契约。本轮不将其改为仅信任 `is_fetch_completed`，也不删除事实计数。
# 
# `pending_report_grids()` 自行报告已比较格点、完整格点及待办数，并拥有 `reconciliation_plan:` 完成日志；不为日志重新扫描事实或日历。

# ### 局部流程：完成证明与待办
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前 required 日历；两张事实格点计数"] --> B["应用可选日期；要求两类日历成对"]
#     B --> C{"两类计数、状态和审计证据共同完整？"}
#     C -->|是| D["计入完整格点；不请求"]
#     C -->|否| E["加入待办；补年月并按分区、日期排序"]
#     D --> F["函数报告完整与待办数量；返回计划"]
#     E --> F
# ```

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[tuple[object, ...], int]:
    if frame.empty:
        return {}
    counts = frame.groupby(GRID_COLUMNS, dropna=False).size()
    return {tuple(key): int(value) for key, value in counts.items()}


def calendar_grid_is_complete(
    row: dict[str, object],
    actual_fact_count: int,
) -> bool:
    if not row["is_fetch_required"] or not row["is_fetch_completed"]:
        return False
    if row["actual_record_count"] != actual_fact_count:
        return False
    if not row["fetch_run_id"] or row["fetch_completed_at"] is None or row["quality_checked_at"] is None:
        return False

    if actual_fact_count > 0:
        return (
            row["fetch_result_status"] == "success"
            and not row["is_data_missing"]
            and row["expected_record_count"] == 1
            and row["quality_status"] in {"passed", "warning"}
        )
    return (
        row["fetch_result_status"] == "empty_confirmed"
        and row["is_data_missing"]
        and row["expected_record_count"] == 1
        and row["quality_status"] == "warning"
    )


def pending_report_grids(
    calendar_df: pd.DataFrame,
    position_counts: dict[tuple[object, ...], int],
    member_counts: dict[tuple[object, ...], int],
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, int]:
    log_started_at = perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'plan'
    log_checked_grids = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=started; "
        f"elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        relevant_mask = (
            calendar_df["dataset_name"].isin(DATASET_NAMES)
            & calendar_df["is_fetch_required"].eq(True)
        )
        if start_date is not None:
            relevant_mask &= calendar_df["trading_date"].ge(start_date)
            relevant_mask &= calendar_df["trading_date"].le(end_date)
        relevant_df = calendar_df.loc[relevant_mask].copy()

        rows_by_grid_dataset = {
            (
                row["exchange_code"],
                row["underlying_code"],
                row["trading_date"],
                row["dataset_name"],
            ): row
            for row in relevant_df.to_dict("records")
        }
        grid_keys = sorted({key[:3] for key in rows_by_grid_dataset})
        pending_rows = []
        complete_count = 0

        log_phase = "compare_completion"
        log_grid_count = len(grid_keys)
        for grid_key in grid_keys:
            position_row = rows_by_grid_dataset.get((*grid_key, "position_rank"))
            member_row = rows_by_grid_dataset.get((*grid_key, "member_position"))
            if position_row is None or member_row is None:
                raise ValueError(f"报告日历缺少成对的排名/参与者格点：{grid_key}")

            is_complete = (
                calendar_grid_is_complete(position_row, position_counts.get(grid_key, 0))
                and calendar_grid_is_complete(member_row, member_counts.get(grid_key, 0))
            )
            log_checked_grids += 1
            if log_checked_grids % 1000 == 0 and perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=running; "
                    f"checked_grids={log_checked_grids}/{log_grid_count}; pending_grids={len(pending_rows) + int(not is_complete)}; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
            if is_complete:
                complete_count += 1
                continue

            pending_rows.append(dict(zip(GRID_COLUMNS, grid_key, strict=True)))

        pending_df = pd.DataFrame(pending_rows, columns=GRID_COLUMNS)
        if not pending_df.empty:
            pending_df["year"] = pending_df["trading_date"].map(lambda value: value.year)
            pending_df["month"] = pending_df["trading_date"].map(lambda value: value.month)
            pending_df = pending_df.sort_values([
                "exchange_code",
                "underlying_code",
                "year",
                "month",
                "trading_date",
            ]).reset_index(drop=True)
        click.echo(
            f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=completed; "
            f"checked_grids={log_checked_grids}; complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return pending_df, complete_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=pending_report_grids; phase=plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 当前事实叶的保留与替换
# 
# 从已读取的旧完整叶中保留未触达日期，替换本次待办日期，再拼接新事实并完整验收。两张事实分别执行同一规则；空结果也保留权威结构，此函数只生成内存结果。
# 
# 函数自行报告合并开始、保留行数、新行数、完整叶行数与失败阶段；输出仍在内存，标记 `persisted=false`。

# ### 局部流程：完整事实叶合并
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前旧事实叶与本批新事实"] --> B["定位叶分区；移除本次触达日期的旧行"]
#     B --> C["保留其他日期；拼接本批行"]
#     C --> D["空结果使用权威空表；完整叶业务验收"]
#     D --> E["函数报告合并行数；返回内存完整叶"]
# ```

# In[ ]:


def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
    partition_columns: list[str],
    partition_key: tuple[object, ...],
    schema: pa.Schema,
    validator: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
    log_started_at = perf_counter()
    log_phase = 'merge'
    log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")
    click.echo(
        f"planning_progress: table={log_table_name}; function=full_fact_partition; phase=merge; status=started; "
        f"key={partition_key}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        partition_mask = pd.Series(True, index=existing_df.index)
        for column, value in zip(partition_columns, partition_key, strict=True):
            partition_mask &= existing_df[column].eq(value)
        existing_partition_df = existing_df.loc[partition_mask, schema.names]
        retained_df = existing_partition_df.loc[
            ~existing_partition_df["trading_date"].isin(touched_dates),
            schema.names,
        ]
        complete_df = pd.concat([retained_df, incoming_df.loc[:, schema.names]], ignore_index=True)
        if complete_df.empty:
            complete_df = empty_pandas(schema)
        log_phase = "validate_merged_leaf"
        validated_complete_df = validator(complete_df, "合并后的完整事实分区")
        click.echo(
            f"planning_progress: table={log_table_name}; function=full_fact_partition; phase=merge; status=completed; "
            f"key={partition_key}; retained_rows={len(retained_df)}; incoming_rows={len(incoming_df)}; complete_rows={len(validated_complete_df)}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return validated_complete_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=full_fact_partition; phase=merge; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 单叶 staging、共享安装与失败恢复
# 
# `commit_complete_partition()` 完整验证待提交叶并检查分区范围，转换为 Arrow，写入 staging 的零行根标记和非空叶。staging 完整复读并逐值一致后，进入 `StagedPathTransaction`，安装缺失的正式根标记并替换当前叶；空结果使用 `staged_path=None` 显式删除旧叶，保留可读零行根标记。已有标记保持原样。
# 
# 正式目标叶的完整业务校验和逐值比对仍在本函数内完成。只有验收通过且成功退出事务才返回并报告 `partition_committed: ... scope=leaf`。共享模块按实际成功的移动记录恢复旧叶，首次备份失败不会移走原目标；本次新建标记与当前叶共同恢复，标记无需隔离。
# 
# 安装或正式验收失败时，已安装的新叶隔离留存；恢复完整才清理旧备份，恢复不完整则保留备份并抛错。staging 清理，本函数仅尝试移除目标下的空父目录，不清理备份或失败数据。共享模块负责恢复日志，本函数报告失败阶段；该机制不提供进程终止后的自动恢复或并发写入协调。
# 
# 两张事实和各日历叶仍分别提交，后一个叶失败时此前成功叶保留；格点完成仍由两张正式事实计数和两类报告日历状态共同证明。本轮不改变查询、业务校验、更新范围及日历回写顺序，也没有独立日期水位文件。
# 

# ### 局部流程：单叶安装与共享恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["验收完整叶与分区范围；转换 Arrow"] --> B["写 staging 标记和非空叶；完整复读比对"]
#     B --> C["进入共享事务：当前叶及必要的新标记"]
#     C --> D["安装新标记；备份旧叶；安装或显式删除叶"]
#     D --> E["本函数完整复读正式叶；逐值比对"]
#     E --> F["成功退出事务；报告当前叶已提交"]
#     D -. 失败 .-> R["共享模块按实际移动倒序恢复；新叶隔离留存"]
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
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    table_name: str,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    validator: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
    log_started_at = perf_counter()
    log_phase = 'commit'
    click.echo(
        f"planning_progress: table={table_name}; function=commit_complete_partition; phase=commit; status=started; "
        f"key={partition_key}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        complete_df = validator(frame, "待提交完整分区")
        if not complete_df.empty:
            actual_keys = set(
                complete_df[partition_columns].itertuples(index=False, name=None)
            )
            if actual_keys != {partition_key}:
                raise ValueError("待提交内容越出指定 Hive 叶分区。")
        complete_table = pandas_to_arrow(complete_df.loc[:, schema.names], schema)

        log_phase = "prepare_paths"
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / table_name
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".{table_name}.staging-{run_id}"
        backup_path = silver_root / f".{table_name}.backup-{run_id}"
        quarantine_path = silver_root / f".{table_name}.failed-{run_id}"

        for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
        silver_root.mkdir(parents=True, exist_ok=True)
        try:
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=staging_write; status=started; "
                f"key={partition_key}; rows={len(complete_table)}; path={staging_path}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)

            file_schema = pa.schema(
                [field for field in schema if field.name not in partition_columns],
                metadata=schema.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=file_schema),
                staging_path / "schema.parquet",
            )
            if len(complete_table):
                ds.write_dataset(
                    complete_table,
                    staging_path,
                    format="parquet",
                    partitioning=partitioning,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

            log_phase = "staging_verify"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=staging_verify; status=started; "
                f"key={partition_key}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            staged_df = read_complete_partition(
                staging_path,
                schema,
                partition_columns,
                partition_key,
                validator,
                f"{table_name} staging",
            )
            if not pandas_to_arrow(staged_df.loc[:, schema.names], schema).equals(complete_table):
                raise ValueError("staging 完整分区内容检查失败。")

        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        relative_path = pathlib.Path(*[
            f"{column}={value}"
            for column, value in zip(partition_columns, partition_key, strict=True)
        ])
        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        target_marker_path = target_path / "schema.parquet"
        staging_marker_path = staging_path / "schema.parquet"

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=install; status=started; "
                f"key={partition_key}; path={destination_path}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={table_name}; function=commit_complete_partition; "
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
                    f"planning_progress: table={table_name}; function=commit_complete_partition; phase=formal_verify; status=started; "
                    f"key={partition_key}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
                committed_df = read_complete_partition(
                    target_path,
                    schema,
                    partition_columns,
                    partition_key,
                    validator,
                    f"正式 {table_name}",
                )
                if not pandas_to_arrow(committed_df.loc[:, schema.names], schema).equals(complete_table):
                    raise ValueError("正式完整分区内容检查失败。")
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
            f"partition_committed: table={table_name}; function=commit_complete_partition; phase=commit; status=completed; "
            f"key={partition_key}; rows={len(committed_df)}; persisted=true; scope=leaf; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_name}; function=commit_complete_partition; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 事实复读后的日历状态生成
# 
# 两张事实正式复读成功后，按格点计数生成 `success` 或 `empty_confirmed`，记录本批 ID、完成时间及质检时间。已有来源重复和特殊案例校准 warning 永久保留；未触达格点、其他品种和仓单状态保持原样。这里仅修改内存日历，不能视为已提交。
# 
# 状态生成函数报告已访问与已更新行数，返回前记 `persisted=false; date_watermark=none`；该日志不能替代日历提交成功。

# ### 局部流程：内存日历状态生成
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两张事实正式复读计数；当前日历叶"] --> B["只更新本次格点及两类报告"]
#     B --> C["保留已有重复来源与特殊校准 warning"]
#     C --> D{"对应事实有行？"}
#     D -->|是| E["success；有证据则 warning，否则 passed"]
#     D -->|否| F["empty_confirmed；缺失标记与 warning"]
#     E --> G["写入计数、批次、时间；验收内存日历"]
#     F --> G
#     G --> H["函数报告状态生成完成；persisted=false"]
# ```

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[object, ...], tuple[int, int]],
    quality_warnings_by_grid_dataset: dict[
        tuple[object, ...], list[str]
    ],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = perf_counter()
    log_last_progress_at = log_started_at
    log_phase = 'generate_calendar_state'
    log_visited_rows = 0
    log_updated_rows = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_state; status=started; "
        f"persisted=false; date_watermark=none; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        updated_df = calendar_df.copy()
        dataset_positions = {"position_rank": 0, "member_position": 1}

        for index, row in updated_df.iterrows():
            log_visited_rows += 1
            if log_visited_rows % 1000 == 0 and perf_counter() - log_last_progress_at >= 2.0:
                log_last_progress_at = perf_counter()
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_state; status=running; "
                    f"visited_rows={log_visited_rows}/{len(updated_df)}; updated_rows={log_updated_rows}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
            dataset_name = row["dataset_name"]
            if dataset_name not in dataset_positions:
                continue
            grid_key = tuple(row[column] for column in GRID_COLUMNS)
            counts = grid_counts.get(grid_key)
            if counts is None:
                continue

            actual_count = counts[dataset_positions[dataset_name]]
            has_rows = actual_count > 0
            current_quality_warnings = quality_warnings_by_grid_dataset.get(
                (*grid_key, dataset_name),
                [],
            )
            current_duplicate_details = "；".join(
                warning
                for warning in current_quality_warnings
                if not warning.startswith(SOURCE_SPECIAL_CASE_REASON_MARKER)
            )
            current_special_case_details = "；".join(
                warning.removeprefix(SOURCE_SPECIAL_CASE_REASON_MARKER)
                for warning in current_quality_warnings
                if warning.startswith(SOURCE_SPECIAL_CASE_REASON_MARKER)
            )
            existing_duplicate_details = ""
            existing_special_case_details = ""
            existing_quality_reason = str(row["quality_reason"])
            if (
                row["quality_status"] == "warning"
                and SOURCE_DUPLICATE_REASON_MARKER in existing_quality_reason
            ):
                existing_duplicate_details = existing_quality_reason.split(
                    SOURCE_DUPLICATE_REASON_MARKER,
                    maxsplit=1,
                )[1]
                if SOURCE_SPECIAL_CASE_REASON_MARKER in existing_duplicate_details:
                    existing_duplicate_details = existing_duplicate_details.split(
                        f"；{SOURCE_SPECIAL_CASE_REASON_MARKER}",
                        maxsplit=1,
                    )[0]
                existing_duplicate_details = existing_duplicate_details.removesuffix("。")
            if (
                row["quality_status"] == "warning"
                and SOURCE_SPECIAL_CASE_REASON_MARKER in existing_quality_reason
            ):
                existing_special_case_details = existing_quality_reason.split(
                    SOURCE_SPECIAL_CASE_REASON_MARKER,
                    maxsplit=1,
                )[1].removesuffix("。")
            duplicate_details = existing_duplicate_details
            if current_duplicate_details and current_duplicate_details not in duplicate_details:
                duplicate_details = "；".join(
                    detail
                    for detail in [duplicate_details, current_duplicate_details]
                    if detail
                )
            special_case_details = existing_special_case_details
            if (
                current_special_case_details
                and current_special_case_details not in special_case_details
            ):
                special_case_details = "；".join(
                    detail
                    for detail in [
                        special_case_details,
                        current_special_case_details,
                    ]
                    if detail
                )
            warning_segments = []
            if duplicate_details:
                warning_segments.append(
                    f"{SOURCE_DUPLICATE_REASON_MARKER}{duplicate_details}"
                )
            if special_case_details:
                warning_segments.append(
                    f"{SOURCE_SPECIAL_CASE_REASON_MARKER}{special_case_details}"
                )
            warning_details = "；".join(warning_segments)
            updated_df.at[index, "is_fetch_completed"] = True
            updated_df.at[index, "fetch_result_status"] = "success" if has_rows else "empty_confirmed"
            updated_df.at[index, "is_data_missing"] = not has_rows
            updated_df.at[index, "expected_record_count"] = 1
            updated_df.at[index, "actual_record_count"] = actual_count
            if has_rows and warning_details:
                updated_df.at[index, "quality_status"] = "warning"
                updated_df.at[index, "quality_reason"] = (
                    f"JQData 排名响应已转换并从正式事实表复读 {actual_count} 行；"
                    f"{warning_details}。"
                )
            elif has_rows:
                updated_df.at[index, "quality_status"] = "passed"
                updated_df.at[index, "quality_reason"] = (
                    f"JQData 排名响应已转换并从正式事实表复读 {actual_count} 行。"
                )
            else:
                updated_df.at[index, "quality_status"] = "warning"
                quality_reason = "JQData 查询成功但本数据集没有可落盘记录，已按确认空记录。"
                if warning_details:
                    quality_reason = (
                        quality_reason.removesuffix("。")
                        + f"；{warning_details}。"
                    )
                updated_df.at[index, "quality_reason"] = quality_reason
            updated_df.at[index, "fetch_run_id"] = fetch_run_id
            updated_df.at[index, "fetch_completed_at"] = completed_at
            updated_df.at[index, "quality_checked_at"] = completed_at
            updated_df.at[index, "updated_at"] = completed_at
            log_updated_rows += 1

        log_phase = "validate_calendar"
        validated_calendar_df = validate_calendar_frame(updated_df, "事实完成状态回写后的")
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_state; status=completed; "
            f"updated_rows={log_updated_rows}; grid_count={len(grid_counts)}; persisted=false; date_watermark=none; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_calendar_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 两类报告日历逐叶提交
# 
# 从本批完成格点找到两类日历叶，依次提交每个完整叶。叶中其他品种及日期一起保留；后一个日历叶失败不撤销前一个成功叶。所有触达叶通过正式复读后，调用方才报告该品种月份提交完成。
# 
# 日历提交函数自行累计成功叶数；全部触达叶通过后才报告 `phase=calendar_state; persisted=true; date_watermark=none`。中途失败不输出整组完成状态，已经成功叶的落盘日志仍有效。

# ### 局部流程：日历逐叶提交
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["按本次格点定位两类日历叶"] --> B["逐叶取完整内容；保留其他品种及日期"]
#     B --> C["业务验收；调用单叶提交及正式复读"]
#     C --> D{"还有叶？"}
#     D -->|是| B
#     D -->|否| E["函数报告日历状态已落盘；返回触达行数"]
#     C -. 失败 .-> F["停止；此前成功事实和日历叶保留"]
# ```

# In[ ]:


def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[object, ...], tuple[int, int]],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = perf_counter()
    log_phase = 'commit_calendar'
    log_committed_partitions = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=started; "
        f"date_watermark=none; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        if not grid_counts:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=skipped; "
                f"reason=no_grids; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            return 0
        grid_key_set = set(grid_counts)
        touched_mask = calendar_df[GRID_COLUMNS].apply(tuple, axis=1).isin(grid_key_set)
        touched_df = calendar_df.loc[
            touched_mask & calendar_df["dataset_name"].isin(DATASET_NAMES)
        ]
        partition_keys = set(
            touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None)
        )

        log_phase = "commit_leaves"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=running; "
            f"partitions=0/{len(partition_keys)}; touched_rows={len(touched_df)}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        for partition_key in sorted(partition_keys):
            partition_mask = pd.Series(True, index=calendar_df.index)
            for column, value in zip(CALENDAR_PARTITION_COLUMNS, partition_key, strict=True):
                partition_mask &= calendar_df[column].eq(value)
            complete_df = validate_calendar_frame(
                calendar_df.loc[
                    partition_mask,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
                ],
                "待提交的完整报告日历分区",
            )
            commit_complete_partition(
                complete_df,
                lake_root,
                CALENDAR_TABLE_NAME,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                CALENDAR_PARTITIONING,
                partition_key,
                validate_calendar_frame,
            )
            log_committed_partitions += 1
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=running; "
                f"partitions={log_committed_partitions}/{len(partition_keys)}; key={partition_key}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=calendar_state; status=completed; "
            f"partitions={log_committed_partitions}; touched_rows={len(touched_df)}; grid_count={len(grid_counts)}; persisted=true; date_watermark=none; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return len(touched_df)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit_calendar; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## JQData 月份待办查询与确定性二分
# 
# 只将当前交易所—品种—月份的待办日期放入 `day.in_(...)`，串行调用 `finance.run_query()`。每次响应核对类型、来源列、日期范围、品种和交易所；空响应也必须具有来源列。
# 
# 少于 5000 行时按请求日期拆回响应，未返回的日期对应空表。达到上限则丢弃本次截顶结果，把有序日期从中点分成两组继续查询；单日仍触顶则失败。该分段用于取得完整响应，不是对业务异常的自动重试。网络或权限异常直接抛出，不继续请求后续日期组。
# 
# 查询函数自行报告每次请求的日期组、请求次数、日期二分、已完成日期与已接纳行数。`query_batch_success:` 计时现在只覆盖本函数的查询、验收和按日拆分，不包含后续校准及两张事实生成。网络等待期间不虚构百分比或新增心跳线程。

# ### 局部流程：月份查询及日期二分
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["日期排序；拒绝空日期或重复日期"] --> B["报告日期组与请求次数；单次 run_query"]
#     B --> C["验收来源列、类型与请求范围"]
#     C --> D{"响应达到 5000 行？"}
#     D -->|否| E["按日期拆分；无返回的日期为空表"]
#     D -->|是| F{"日期组只有一天？"}
#     F -->|是| X["硬失败；不能证明完整性"]
#     F -->|否| G["丢弃截顶响应；有序日期二分入队"]
#     E --> H{"还有日期组？"}
#     G --> H
#     H -->|是| B
#     H -->|否| I["函数报告查询完成；返回按日响应和计数"]
#     B -. 请求异常 .-> Y["抛错；不重试"]
#     C -. 验收失败 .-> Y
# ```

# In[ ]:


def query_rank_date_batch(
    jqdata: ModuleType,
    exchange_code: str,
    underlying_code: str,
    pending_dates: list[date],
) -> tuple[dict[date, pd.DataFrame], int, int]:
    log_started_at = perf_counter()
    log_phase = 'query'
    log_table_name = POSITION_TABLE_NAME + "+" + MEMBER_TABLE_NAME
    log_completed_dates = 0
    log_accepted_rows = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=query_rank_date_batch; phase=query; status=started; "
        f"exchange={exchange_code}; underlying={underlying_code}; elapsed_s={perf_counter() - log_started_at:.3f}"
    )
    try:
        requested_dates = sorted(set(pending_dates))
        if not requested_dates:
            raise ValueError("JQData 排名批量查询日期不得为空。")
        if len(requested_dates) != len(pending_dates):
            raise ValueError("JQData 排名批量查询日期不得重复。")

        table = jqdata.finance.FUT_MEMBER_POSITION_RANK
        pending_date_groups = [requested_dates]
        raw_frames_by_date = {
            trading_date: pd.DataFrame(columns=JQDATA_FIELDS)
            for trading_date in requested_dates
        }
        source_call_count = 0
        split_count = 0

        while pending_date_groups:
            date_group = pending_date_groups.pop(0)
            query_object = jqdata.query(*[
                getattr(table, field_name)
                for field_name in JQDATA_FIELDS
            ]).filter(
                table.day.in_(date_group),
                table.underlying_code == underlying_code,
            )
            source_call_count += 1
            log_phase = "request"
            click.echo(
                f"request_batch: table={log_table_name}; function=query_rank_date_batch; phase=query; status=running; "
                f"exchange={exchange_code}; underlying={underlying_code}; request={source_call_count}; dates={date_group[0]}..{date_group[-1]}; request_dates={len(date_group)}; completed_dates={log_completed_dates}/{len(requested_dates)}; elapsed_s={perf_counter() - log_started_at:.3f}"
            )

            try:
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
                    f"{error_type}: JQData FUT_MEMBER_POSITION_RANK 查询失败；"
                    f"exchange={exchange_code}, underlying={underlying_code}, dates={date_group}。"
                ) from error

            log_phase = "validate_response"
            if raw_df is None:
                raise RuntimeError(
                    "retryable_error: JQData 排名批量查询返回 None；"
                    f"exchange={exchange_code}, underlying={underlying_code}, dates={date_group}。"
                )
            if not isinstance(raw_df, pd.DataFrame):
                raise TypeError("schema_error: JQData 排名批量查询未返回 DataFrame。")

            missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
            if missing_columns:
                raise ValueError(
                    f"schema_error: JQData 排名批量响应缺列 {sorted(missing_columns)}。"
                )
            if raw_df.empty:
                response_df = pd.DataFrame(columns=JQDATA_FIELDS)
            else:
                response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
                response_df["day"] = pd.to_datetime(
                    response_df["day"], errors="coerce"
                ).dt.date
                requested_date_set = set(date_group)
                response_dates = set(response_df["day"].dropna())
                if response_df["day"].isna().any() or not response_dates.issubset(
                    requested_date_set
                ):
                    raise ValueError(
                        "schema_error: JQData 排名批量响应包含请求交易日之外的行。"
                    )
                response_underlying = (
                    response_df["underlying_code"].astype(str).str.strip().str.upper()
                )
                if not response_underlying.eq(underlying_code).all():
                    raise ValueError(
                        "schema_error: JQData 排名批量响应品种与待办日历不一致。"
                    )
                response_exchange_values = response_df["exchange"]
                response_exchanges = response_exchange_values.astype(str).str.strip().str.upper()
                allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(
                    exchange_code,
                    {exchange_code},
                )
                if (
                    response_exchange_values.isna().any()
                    or response_exchanges.eq("").any()
                    or not response_exchanges.isin(allowed_exchanges).all()
                ):
                    actual_exchange_values = sorted({
                        "<null>" if pd.isna(value) else str(value).strip().upper()
                        for value in response_exchange_values
                    })
                    raise ValueError(
                        "schema_error: JQData 排名批量响应交易所与待办日历不一致；"
                        f"actual={actual_exchange_values}。"
                    )

            log_phase = "split_or_accept"
            if len(raw_df) >= RUN_QUERY_ROW_LIMIT:
                if len(date_group) == 1:
                    raise ValueError(
                        "schema_error: JQData 排名单日响应达到 5000 行上限，"
                        "不能证明 Top 20 榜单结果完整。"
                    )
                midpoint = len(date_group) // 2
                left_dates = date_group[:midpoint]
                right_dates = date_group[midpoint:]
                pending_date_groups[0:0] = [left_dates, right_dates]
                split_count += 1
                click.echo(
                    f"query_batch_split: table={log_table_name}; function=query_rank_date_batch; phase=query; status=split; "
                    f"exchange={exchange_code}; underlying={underlying_code}; dates={len(date_group)}; left={len(left_dates)}; right={len(right_dates)}; rows={len(raw_df)}; source_calls={source_call_count}; splits={split_count}; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
                continue

            for trading_date in date_group:
                raw_frames_by_date[trading_date] = response_df.loc[
                    response_df["day"].eq(trading_date),
                    JQDATA_FIELDS,
                ].reset_index(drop=True)
            log_completed_dates += len(date_group)
            log_accepted_rows += len(response_df)
            click.echo(
                f"fetch_progress: table={log_table_name}; function=query_rank_date_batch; phase=query; status=running; "
                f"exchange={exchange_code}; underlying={underlying_code}; completed_dates={log_completed_dates}/{len(requested_dates)}; source_calls={source_call_count}; splits={split_count}; accepted_rows={log_accepted_rows}; elapsed_s={perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"query_batch_success: table={log_table_name}; function=query_rank_date_batch; phase=query; status=completed; "
            f"exchange={exchange_code}; underlying={underlying_code}; dates={len(requested_dates)}; source_calls={source_call_count}; splits={split_count}; raw_rows={log_accepted_rows}; query_seconds={perf_counter() - log_started_at:.3f}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        return raw_frames_by_date, source_call_count, split_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=query_rank_date_batch; phase=query; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：计划、月份调度与完成汇总
# 
# `main()` 检查日期和正式湖写入边界，读取窄列完成证据、形成待办；无待办直接退出。存在待办才认证，随后依次处理品种月份：查询、逐日特殊案例校准与归一化、汇总两张事实，只读时到此结束当前月份。
# 
# 写入时先读两张旧事实叶与两类日历完整叶，确认当前待办格点仍 required，再合并和分别提交两张事实。只有两张事实正式复读成功，才按复读计数生成日历完成状态并逐叶提交；全部成功后输出当前月份的 `partition_timing:` 汇总。
# 
# 日志使用 88 个 `=` 的运行边界与 `table/function/phase/status` 字段；保留原监控前缀。只读月份使用 `persisted=false`，正式完成使用 `persisted=true`。`elapsed_s` 表达所属阶段或运行的累计耗时，原计数字段继续保留。`query_batch_success` 由查询函数发出，只覆盖查询、验收和按日拆分；生成、合并、单叶提交及日历状态各由负责函数报告。
# 
# `main()` 保留运行边界、实际仍在入口进行的日历窄列读取和月份拼接、只读结果与累计进度，不代报被调用函数的完成。异常报告失败阶段后继续抛出，不输出成功收尾；没有独立日期水位。

# ### 局部流程：月份调度
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["参数与写入边界；读取完成证据；形成待办"] --> B{"有待办？"}
#     B -->|否| C["报告 up_to_date；不认证"]
#     B -->|是| D["认证；逐品种月份请求、校准与归一化"]
#     D --> E{"启用写入？"}
#     E -->|否| F["报告只读验收；persisted=false"]
#     E -->|是| G["读当前完整叶；核对 required；合并事实"]
#     G --> H["排名事实 → 类型事实 → 两类日历叶"]
#     H --> I["报告月份累计进度；函数已报告各自提交"]
#     F --> J["继续下一月份；最后汇总运行"]
#     I --> J
#     D -. 异常 .-> X["报告当前阶段失败；抛错；不输出成功收尾"]
#     G -. 异常 .-> X
#     H -. 异常 .-> X
# ```

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    write: bool,
) -> None:
    log_started_at = perf_counter()
    log_boundary = "=" * 88
    log_phase = "arguments"
    click.echo(
        f"{log_boundary}\n成交持仓报告同步 / Holding reports run\n"
        f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
        formal_lake_root = settings.futures_lake_root.resolve()
        resolved_lake_root = (lake_root or formal_lake_root).resolve()
        has_explicit_dates = start_date is not None or end_date is not None

        if (start_date is None) != (end_date is None):
            raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
        if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
            raise click.UsageError(
                "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
                "请移除日期参数使用自动更新，或改用非正式测试湖。"
            )

        requested_start = start_date.date() if start_date is not None else None
        requested_end = end_date.date() if end_date is not None else None
        if requested_start is not None and requested_start > requested_end:
            raise click.BadParameter("起始日期不得晚于结束日期。")

        silver_root = resolved_lake_root / "silver"
        calendar_path = silver_root / CALENDAR_TABLE_NAME
        position_path = silver_root / POSITION_TABLE_NAME
        member_path = silver_root / MEMBER_TABLE_NAME

        log_phase = "plan"
        calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "正式报告日历",
        )
        calendar_filter = (
            ds.field("dataset_name").isin(DATASET_NAMES)
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
            f"columns={len(CALENDAR_COMPLETION_COLUMNS)}; required_only=true; elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        calendar_planning_df = calendar_dataset.to_table(
            columns=CALENDAR_COMPLETION_COLUMNS,
            filter=calendar_filter,
        ).to_pandas()
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=read_calendar_plan; status=completed; "
            f"rows={len(calendar_planning_df)}; required_only=true; elapsed_s={perf_counter() - log_started_at:.3f}"
        )

        position_dataset = open_optional_exact_dataset(
            position_path,
            POSITION_PARTITIONING,
            FUTURES_POSITION_RANK_DAILY_SCHEMA,
            "正式排名事实",
        )
        member_dataset = open_optional_exact_dataset(
            member_path,
            MEMBER_PARTITIONING,
            FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
            "正式参与者类型事实",
        )
        position_counts = dataset_grid_count_map(position_dataset)
        member_counts = dataset_grid_count_map(member_dataset)

        pending_df, complete_count = pending_report_grids(
            calendar_planning_df,
            position_counts,
            member_counts,
            requested_start,
            requested_end,
        )
        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=plan; status=completed; mode={mode}; "
            f"lake_root={resolved_lake_root}; write={str(write).lower()}"
        )
        if pending_df.empty:
            click.echo(
                "up_to_date: function=main; phase=plan; status=completed; pending_grids=0; "
                "两张持仓事实及报告日历状态已经完整一致。"
            )
            click.echo(
                f"{log_boundary}\n成交持仓报告运行完成 / Holding reports run completed\n"
                f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=completed; "
                f"outcome=up_to_date; write={str(write).lower()}; elapsed_s={perf_counter() - log_started_at:.3f}\n{log_boundary}"
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
        click.echo(f"partition_plan: function=main; phase=plan; status=completed; pending_partitions={partition_count}")

        total_position_rows = 0
        total_member_rows = 0
        total_source_calls = 0
        total_split_count = 0
        completed_grid_count = 0

        for group_number, (partition_key, group_df) in enumerate(partition_groups, start=1):
            partition_started_at = perf_counter()
            updated_at = datetime.now(timezone.utc)
            batch_id = uuid.uuid4().hex
            position_frames = []
            member_frames = []
            quality_warnings_by_grid_dataset: dict[
                tuple[object, ...], list[str]
            ] = {}

            click.echo(
                f"partition_start: function=main; phase=partition; status=started; partitions={group_number}/{partition_count}; "
                f"key={partition_key}; grids={len(group_df)}"
            )
            group_records = group_df[GRID_COLUMNS].to_dict("records")
            pending_dates = [grid["trading_date"] for grid in group_records]
            log_phase = "query"
            (
                raw_frames_by_date,
                source_call_count,
                split_count,
            ) = query_rank_date_batch(
                jqdata,
                partition_key[0],
                partition_key[1],
                pending_dates,
            )
            log_phase = "normalize"
            for grid in group_records:
                raw_df = raw_frames_by_date[grid["trading_date"]]
                calibrated_raw_df, special_case_warnings = (
                    apply_position_rank_special_cases(
                        raw_df,
                        grid,
                        resolved_lake_root,
                    )
                )
                (
                    position_grid_df,
                    member_grid_df,
                    quality_warnings_by_dataset,
                ) = normalize_rank_response(
                    calibrated_raw_df,
                    grid,
                    updated_at,
                )
                quality_warnings_by_dataset["position_rank"].extend(
                    special_case_warnings
                )
                position_frames.append(position_grid_df)
                member_frames.append(member_grid_df)
                grid_key = tuple(grid[column] for column in GRID_COLUMNS)
                for dataset_name, quality_warnings in (
                    quality_warnings_by_dataset.items()
                ):
                    if not quality_warnings:
                        continue
                    quality_warnings_by_grid_dataset[
                        (*grid_key, dataset_name)
                    ] = quality_warnings
            total_source_calls += source_call_count
            total_split_count += split_count

            log_phase = "generate"
            click.echo(
                f"planning_progress: table={POSITION_TABLE_NAME}; function=main; phase=assemble_month; status=started; "
                f"key={partition_key}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            incoming_position_df = (
                validate_position_frame(
                    pd.concat(position_frames, ignore_index=True),
                    "本分区 JQData 汇总后的",
                )
                if any(not frame.empty for frame in position_frames)
                else empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA)
            )
            click.echo(
                f"planning_progress: table={POSITION_TABLE_NAME}; function=main; phase=assemble_month; status=completed; "
                f"key={partition_key}; rows={len(incoming_position_df)}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            incoming_member_df = (
                validate_member_frame(
                    pd.concat(member_frames, ignore_index=True),
                    "本分区 JQData 汇总后的",
                )
                if any(not frame.empty for frame in member_frames)
                else empty_pandas(FUTURES_MEMBER_POSITION_DAILY_SCHEMA)
            )
            click.echo(
                f"planning_progress: table={MEMBER_TABLE_NAME}; function=main; phase=assemble_month; status=completed; "
                f"key={partition_key}; rows={len(incoming_member_df)}; persisted=false; elapsed_s={perf_counter() - log_started_at:.3f}"
            )

            if not write:
                total_position_rows += len(incoming_position_df)
                total_member_rows += len(incoming_member_df)
                completed_grid_count += len(group_df)
                click.echo(
                    f"partition_checked: function=main; phase=partition; status=completed; outcome=read_only; persisted=false; key={partition_key}; grids={len(group_df)}; "
                    f"elapsed_seconds={perf_counter() - partition_started_at:.3f}; elapsed_s={perf_counter() - partition_started_at:.3f}; "
                    f"cumulative_grids={completed_grid_count}/{len(pending_df)}; "
                    f"cumulative_source_calls={total_source_calls}; "
                    f"cumulative_splits={total_split_count}"
                )
                continue

            log_phase = "read_complete_leaves"
            touched_dates = set(group_df["trading_date"].tolist())
            existing_position_partition_df = read_complete_partition(
                position_path,
                FUTURES_POSITION_RANK_DAILY_SCHEMA,
                POSITION_PARTITION_COLUMNS,
                partition_key,
                validate_position_frame,
                "正式排名事实",
            )
            existing_member_partition_df = read_complete_partition(
                member_path,
                FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                MEMBER_PARTITION_COLUMNS,
                partition_key,
                validate_member_frame,
                "正式参与者类型事实",
            )

            fact_partition_values = dict(zip(
                POSITION_PARTITION_COLUMNS,
                partition_key,
                strict=True,
            ))
            calendar_partition_frames = []
            for dataset_name in DATASET_NAMES:
                calendar_partition_values = {
                    **fact_partition_values,
                    "dataset_name": dataset_name,
                }
                calendar_partition_key = tuple(
                    calendar_partition_values[column]
                    for column in CALENDAR_PARTITION_COLUMNS
                )
                calendar_partition_frames.append(read_complete_partition(
                    calendar_path,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                    CALENDAR_PARTITION_COLUMNS,
                    calendar_partition_key,
                    validate_calendar_frame,
                    f"正式报告日历 {dataset_name}",
                ))
            calendar_partition_df = validate_calendar_frame(
                pd.concat(calendar_partition_frames, ignore_index=True),
                "事实提交前复读的报告日历叶",
            )
            for grid in group_records:
                for dataset_name in DATASET_NAMES:
                    calendar_grid_mask = calendar_partition_df["dataset_name"].eq(
                        dataset_name
                    )
                    for column in GRID_COLUMNS:
                        calendar_grid_mask &= calendar_partition_df[column].eq(grid[column])
                    calendar_grid_df = calendar_partition_df.loc[calendar_grid_mask]
                    if len(calendar_grid_df) != 1:
                        raise ValueError(
                            "报告日历完整叶缺少唯一的成对待办格点；"
                            f"dataset={dataset_name}, grid={grid}。"
                        )
                    if not bool(calendar_grid_df.iloc[0]["is_fetch_required"]):
                        raise ValueError(
                            "报告日历格点在事实提交前已不再要求采集；"
                            f"dataset={dataset_name}, grid={grid}。"
                        )

            log_phase = "merge"
            complete_position_df = full_fact_partition(
                existing_position_partition_df,
                incoming_position_df,
                touched_dates,
                POSITION_PARTITION_COLUMNS,
                partition_key,
                FUTURES_POSITION_RANK_DAILY_SCHEMA,
                validate_position_frame,
            )
            complete_member_df = full_fact_partition(
                existing_member_partition_df,
                incoming_member_df,
                touched_dates,
                MEMBER_PARTITION_COLUMNS,
                partition_key,
                FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                validate_member_frame,
            )

            log_phase = "commit_position"
            committed_position_partition_df = commit_complete_partition(
                complete_position_df,
                resolved_lake_root,
                POSITION_TABLE_NAME,
                FUTURES_POSITION_RANK_DAILY_SCHEMA,
                POSITION_PARTITION_COLUMNS,
                POSITION_PARTITIONING,
                partition_key,
                validate_position_frame,
            )
            log_phase = "commit_member"
            committed_member_partition_df = commit_complete_partition(
                complete_member_df,
                resolved_lake_root,
                MEMBER_TABLE_NAME,
                FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                MEMBER_PARTITION_COLUMNS,
                MEMBER_PARTITIONING,
                partition_key,
                validate_member_frame,
            )

            position_partition_counts = grid_count_map(committed_position_partition_df)
            member_partition_counts = grid_count_map(committed_member_partition_df)
            grid_counts = {
                tuple(grid[column] for column in GRID_COLUMNS): (
                    position_partition_counts.get(tuple(grid[column] for column in GRID_COLUMNS), 0),
                    member_partition_counts.get(tuple(grid[column] for column in GRID_COLUMNS), 0),
                )
                for grid in group_records
            }
            completed_at = datetime.now(timezone.utc)
            log_phase = "calendar_completion"
            completed_calendar_partition_df = apply_calendar_completion(
                calendar_partition_df,
                grid_counts,
                quality_warnings_by_grid_dataset,
                batch_id,
                completed_at,
            )
            log_phase = "commit_calendar"
            calendar_rows = commit_calendar_partitions(
                completed_calendar_partition_df,
                grid_counts,
                resolved_lake_root,
            )

            total_position_rows += len(incoming_position_df)
            total_member_rows += len(incoming_member_df)
            completed_grid_count += len(group_df)
            click.echo(
                f"partition_timing: function=main; phase=partition; status=completed; persisted=true; key={partition_key}; "
                f"calendar_rows={calendar_rows}; grids={len(group_df)}; "
                f"landed_position_rows={len(incoming_position_df)}; "
                f"landed_member_rows={len(incoming_member_df)}; "
                f"elapsed_seconds={perf_counter() - partition_started_at:.3f}; elapsed_s={perf_counter() - partition_started_at:.3f}; "
                f"cumulative_grids={completed_grid_count}/{len(pending_df)}; "
                f"cumulative_source_calls={total_source_calls}; "
                f"cumulative_splits={total_split_count}"
            )

        click.echo(
            f"finished: function=main; phase=run; status=completed; outcome={'committed' if write else 'read_only'}; grids={completed_grid_count}; "
            f"position_rows={total_position_rows}; member_rows={total_member_rows}; "
            f"source_calls={total_source_calls}; splits={total_split_count}; "
            f"write={str(write).lower()}"
        )
        click.echo(
            f"{log_boundary}\n成交持仓报告运行完成 / Holding reports run completed\n"
            f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome={'committed' if write else 'read_only'}; write={str(write).lower()}; "
            f"elapsed_s={perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={perf_counter() - log_started_at:.3f}"
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
        prog_name="b02_futures_holding_reports",
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
#     A["在终端激活 latitude"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["按上游待办采集并逐叶提交"]
# ```
# 

# In[ ]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a02_Futures_Exchange_Reports\b02_futures_holding_reports.py --write

