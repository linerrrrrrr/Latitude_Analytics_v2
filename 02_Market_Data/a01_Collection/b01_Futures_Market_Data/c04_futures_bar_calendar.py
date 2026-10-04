#!/usr/bin/env python
# coding: utf-8

# # c04_futures_bar_calendar
# 
# 生成 `dim_futures_bar_calendar`：每个固定月份合约—交易日一个 `1d` 格点，每个合约—交易日—Session 一个 `1m` 格点。本表读取 c03 正式合约日历，不调用外部 API；同时承载 c05—c08 回写的采集、缺失检查和质量证据。
# 
# 阅读顺序：初始化与契约 → 上游边界及本表校验 → 分区读取 → 理论结构生成 → 差异与状态继承 → 单叶提交 → 运行入口。Notebook 是唯一业务源，同名 `.py` 由默认 PythonExporter 生成。
# 
# 函数定义与执行入口分开：依次运行定义单元格不会启动业务；最后的执行入口使用显式 Notebook 参数，或由同名脚本读取命令行参数。

# ## 总流程：从合约日历到行情日历
# 
# 下图描述入口调用后的实际顺序。c04 只读本地 c03，不调用外部 API；各检查异常均向调用方抛出。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["进入 main；检查日期、full 和正式写入门禁"] --> B["打开上下游 Dataset；发现分区并检查物理契约"]
#     B --> C["选择自动尾部、显式日期或全历史范围"]
#     C --> D["按月读取 c03 结构；生成 1d 与 1m 期望格点"]
#     D --> E["比较结构；汇总 dirty 计划"]
#     E --> F{"有 dirty 叶？"}
#     F -->|否| G["记录无需更新；结束"]
#     F -->|是| H{"启用 write？"}
#     H -->|否| I["记录只读计划完成；结束"]
#     H -->|是| J["重新生成结构及初始状态；复读当前完整叶"]
#     J --> K["继承结构相同行的状态；重新判断差异"]
#     K --> L{"仍为 dirty？"}
#     L -->|否：跳过 clean| M["继续处理其余计划"]
#     L -->|是| N["按模式保留旧行；单叶校验、暂存、安装及验收"]
#     N -. 失败 .-> O["停止后续；共享事务恢复当前叶及变更标记；保留此前成功叶"]
#     N --> M
#     M --> P["本批全部处理成功；记录完成与耗时"]
# ```

# ## 更新范围、状态职责与正式湖写入边界
# 
# | 模式 | 比较范围 | 写入边界 |
# | --- | --- | --- |
# | 默认尾部更新 | 按频率—交易所，只处理目标最大交易日之后的 c03 新结构 | `--write` 提交 dirty 完整叶，保留既有历史行 |
# | 成对显式日期 | 指定闭区间内的上下游结构 | 可只读检查；写入必须指定不同于正式湖的临时湖，保留区间外行 |
# | `--full` | 当前全部上游结构与全部本地分区 | `--full --write` 允许正式全历史维护，只提交差异叶 |
# 
# 日期必须成对，且与 `--full` 互斥。空目标由相同入口自然建表；内部缺口、旧日 Session 修订和目标孤儿叶由历史维护路径处理，默认尾部不扫描它们的业务内容。
# 
# 日线每个合约日一行，分钟每个 Session 一行；主键和 `bar_frequency/exchange_code/year/month` 分区来自权威 Schema。clean 叶不读取完整状态、不改变 `updated_at`。结构相同的行保留 c05—c08 回写值，新建或变化行初始化；c06 负责分钟事实白名单选择。

# ## 初始化、权威契约与字段分工
# 
# 从当前目录向上搜索项目标记，导入配置与 `config.data_contracts` 的具名 Schema；表名、主键和 Hive 分区从 metadata 读取。正式湖根目录在运行时通过 `settings.futures_lake_root` 取得，唯一配置来源是 `.env` 的 `FUTURES_LAKE_ROOT`。
# 
# `UPSTREAM_STRUCTURE_COLUMNS` 是 c03 的 12 列投影；`STRUCTURAL_COLUMNS` 是 c04 的 13 个结构字段，包含本表主键。其余字段属于 `STATE_COLUMNS`：结构一致时继承，新增或结构变化时初始化。物理类型及表身份仍以权威 Schema 为准，不另外定义字段契约。执行初始化单元格不请求来源、不写湖。
# 
# 频率按 `1d`、`1m` 固定顺序复用；主键投影 Schema 与去除 Hive 字段的物理文件 Schema 在初始化时由权威 Schema 派生一次，分区提交直接复用。

# ### 局部流程：初始化与字段配置
# 
# 本格准备依赖、权威 Schema 投影和分区配置，不执行数据生成或写入。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从当前目录向上搜索项目标记"] --> B{"找到项目根？"}
#     B -->|否| C["抛出定位异常"]
#     B -->|是| D["加入导入路径；加载配置与权威 Schema"]
#     D --> E["读取表名、主键和分区 metadata"]
#     E --> F["定义结构列、状态列、枚举和 Hive partitioning"]
# ```

# In[1]:


from __future__ import annotations

import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timezone


# 从任意子目录启动时，先按项目统一标记定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Market_Data/a01_Collection"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Schema、类型转换和校验入口全部取自根级可执行契约。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# 表名、主键和 Hive 分区只从权威 Schema metadata 读取。
TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
UPSTREAM_TABLE_NAME = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[b"primary_key"].decode(
    "utf-8"
).split(",")
UPSTREAM_PRIMARY_KEY = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
UPSTREAM_PARTITION_COLUMNS = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

STRUCTURAL_COLUMNS = [  # 由上游日历决定；结构变化时相应格点必须重建。
    *PRIMARY_KEY,  # 本表频率—合约—交易日—Session 主键。
    "exchange_code",  # 合约所属交易所。
    "underlying_code",  # 合约所属期货品种。
    "session_text",  # 上游原始 Session 文本；日线为空。
    "session_start_at",  # Session 开始时刻；日线为空。
    "session_end_at",  # Session 结束时刻；日线为空。
    "is_night_session",  # 是否夜盘 Session；日线为空。
    "expected_bar_count",  # 日线为 1；分钟为 Session 理论分钟数。
    "year",  # 交易年份分区值。
    "month",  # 交易月份分区值。
]
STATE_COLUMNS = [  # 由 b05～b08 回写且结构未变化时应保留的状态/证据字段。
    name
    for name in FUTURES_BAR_CALENDAR_SCHEMA.names
    if name not in STRUCTURAL_COLUMNS
]
STRUCTURAL_NON_KEY_COLUMNS = [
    name for name in STRUCTURAL_COLUMNS if name not in PRIMARY_KEY
]
UPSTREAM_STRUCTURE_COLUMNS = [
    *UPSTREAM_PRIMARY_KEY,
    "exchange_code",
    "underlying_code",
    "session_text",
    "session_start_at",
    "session_end_at",
    "is_night_session",
    "minute_count",
    "year",
    "month",
]
STRUCTURAL_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name) for name in STRUCTURAL_COLUMNS
])
UPSTREAM_STRUCTURE_SCHEMA = pa.schema([
    FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
    for name in UPSTREAM_STRUCTURE_COLUMNS
])

BAR_FREQUENCIES = ("1d", "1m")
PRIMARY_KEY_SCHEMA = pa.schema(
    [FUTURES_BAR_CALENDAR_SCHEMA.field(name) for name in PRIMARY_KEY],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)
PARQUET_FILE_SCHEMA = pa.schema(
    [
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in FUTURES_BAR_CALENDAR_SCHEMA.names
        if name not in PARTITION_COLUMNS
    ],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)
SCHEDULE_STATUSES = {
    "scheduled",
    "suspected_closed",
    "confirmed_closed",
}
EVIDENCE_LEVELS = {
    "contract_rule",
    "inferred",
    "reconciled",
    "authoritative",
}
QUALITY_STATUSES = {
    "pending",
    "passed",
    "warning",
    "failed",
    "not_applicable",
}

HIVE_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## Schema 契约浏览
# 
# 交互式 Notebook 使用共享展示模块，依次呈现上游合约日历和当前行情日历的权威 Schema，并提供有界只读样例。导出的命令行脚本跳过展示；浏览不调用业务 API、不写湖，也不代替生产校验。

# ### 局部流程：Schema 与样例浏览
# 
# 展示使用已有共享模块，不替代采集和提交校验。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"交互式 Notebook 且无 __file__？"} -->|否| B["跳过展示"]
#     A -->|是| C["加载共享 Schema 浏览模块"]
#     C --> D["依次展示 c03 与 c04 权威契约"]
#     D --> E["按用户选择读取有界样例；不写入"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        FUTURES_BAR_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 上游边界、结构比较与本表业务校验
# 
# 正式路径信任 c03 已提交的主键、Session 连续性及派生语义，只读取构造所需投影。`validate_contract_structure()`、`validate_contract_input()` 为独立 DataFrame 调用和迁移工具保留上游输入校验；它们不用于主流程重新证明 c03 的完整业务语义。
# 
# `validate_structural_frame()` 检查生成或比较的 c04 结构，包括主键、频率、合约与交易所对应、日期分区、日线保留编号和分钟 Session。`assess_structural_partition()` 比较缺失、变化及多余行；本地结构业务校验失败时记入 `quality_error` 和 dirty 计划，物理不兼容则在 Dataset 打开阶段直接失败。
# 
# `validate_bar_calendar_frame()` 检查结构及完整状态：枚举、说明文本、采集完成凭证、条数、缺失标记和质量时间。正式写入路径在合并出 dirty 完整叶后执行一次完整业务校验；staging 与正式安装后只复核文件契约、主键和行数。独立 `build_fresh_partitions()` 也对它返回的完整结果负责。完整输入校验直接指定完整 Schema，复用结构校验中的一次类型转换和排序，不再将同一份结构投影重复转换。比较函数要求期望结构已经由生成函数校验，复用其 Arrow 扩展类型；空值安全的相等比较直接使用 DataFrame，不再为比较转成两份 Arrow Table。现有结构的审计和 dirty 完整叶的最终校验继续保留。

# ### 局部流程：校验与结构差异判断
# 
# 以下是不同调用边界，不是每次均顺序执行的三轮校验。正式上游读取不调用独立 DataFrame 的上游业务校验。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["后续按函数职责调用"] --> B["独立上游输入：转换并检查合约 Session 结构"]
#     A --> C["c04 结构校验：主键、频率、日期及 Session"]
#     A --> D["dirty 完整叶：结构及完整状态规则"]
#     C --> E["复用已校验期望结构；检查现有结构并比较"]
#     E --> F{"结构相同且无质量错误？"}
#     F -->|是| G["返回 clean"]
#     F -->|否| H["汇总缺失、变化、多余行及 quality_error"]
#     H --> I["返回 dirty 审计结果"]
#     D --> J["完成凭证、缺失计数、质量时间等必须一致"]
#     B --> K["返回规范化输入；校验异常向外抛出"]
#     J --> L["返回完整叶；校验异常向外抛出"]
# ```

# In[3]:


# 兼容公开 helper 和迁移工具的任意 DataFrame 输入；正式 b03 Dataset 路径不重复调用。
def validate_contract_structure(
    frame: pd.DataFrame,
    context: str,
    *,
    schema: pa.Schema = UPSTREAM_STRUCTURE_SCHEMA,
) -> pd.DataFrame:
    """按所需投影或完整契约转换一次，再检查共用的 Session 结构规则。"""
    table = pandas_to_arrow(frame.loc[:, schema.names], schema)
    checked_df = arrow_to_pandas(table, schema)
    checked_df = checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)

    if checked_df.duplicated(UPSTREAM_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    duration_seconds = (
        checked_df["session_end_at"] - checked_df["session_start_at"]
    ).dt.total_seconds().to_numpy(dtype="float64", na_value=float("nan"))
    if checked_df["session_number"].le(0).any():
        raise ValueError(f"{context}session_number 必须大于 0。")
    if pd.isna(duration_seconds).any() or (duration_seconds <= 0).any():
        raise ValueError(f"{context}Session 起点必须严格早于终点。")
    if (duration_seconds % 60 != 0).any():
        raise ValueError(f"{context}Session 时间差不是整分钟。")
    if checked_df["minute_count"].ne(duration_seconds // 60).any():
        raise ValueError(f"{context}minute_count 与 Session 时间差不一致。")
    if checked_df["minute_count"].le(0).any():
        raise ValueError(f"{context}minute_count 必须大于 0。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(
        ".", n=1
    ).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(checked_df["exchange_code"].astype("string"))
    ).any():
        raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(checked_df["trading_date"].dt.month).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    group_columns = ["contract_code", "trading_date"]
    expected_session_numbers = checked_df.groupby(
        group_columns, sort=False
    ).cumcount().add(1)
    if checked_df["session_number"].ne(expected_session_numbers).any():
        raise ValueError(f"{context}同一合约日的 Session 编号不连续。")

    direct_columns = ["exchange_code", "underlying_code", "year", "month"]
    inconsistent_groups = checked_df.groupby(
        group_columns, sort=False
    )[direct_columns].nunique(dropna=False).gt(1).any(axis=1)
    if inconsistent_groups.any():
        raise ValueError(f"{context}同一合约日的直接属性不一致。")
    return checked_df


def validate_contract_input(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    return validate_contract_structure(
        frame, context, schema=FUTURES_CONTRACT_CALENDAR_SCHEMA
    )


# 结构快路径不读取 b05～b08 的状态字段。
def validate_structural_frame(
    frame: pd.DataFrame,
    context: str,
    *,
    schema: pa.Schema = STRUCTURAL_SCHEMA,
) -> pd.DataFrame:
    """按结构投影或完整契约转换一次；两种调用均执行全部结构规则。"""
    table = pandas_to_arrow(frame.loc[:, schema.names], schema)
    checked_df = arrow_to_pandas(table, schema)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df
    if not checked_df["bar_frequency"].isin(BAR_FREQUENCIES).all():
        raise ValueError(f"{context}bar_frequency 不在允许枚举中。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(
        ".", n=1
    ).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(checked_df["exchange_code"].astype("string"))
    ).any():
        raise ValueError(f"{context}exchange_code 与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(checked_df["trading_date"].dt.month).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")

    daily_df = checked_df.loc[checked_df["bar_frequency"].eq("1d")]
    minute_df = checked_df.loc[checked_df["bar_frequency"].eq("1m")]
    session_columns = [
        "session_text", "session_start_at", "session_end_at", "is_night_session"
    ]
    if (
        daily_df["session_number"].ne(0).any()
        or daily_df[session_columns].notna().to_numpy().any()
        or daily_df["expected_bar_count"].ne(1).any()
    ):
        raise ValueError(f"{context}日线 Session 结构不合法。")

    if not minute_df.empty:
        duration_seconds = (
            minute_df["session_end_at"] - minute_df["session_start_at"]
        ).dt.total_seconds().to_numpy(
            dtype="float64", na_value=float("nan")
        )
        if (
            minute_df["session_number"].le(0).any()
            or minute_df[session_columns].isna().to_numpy().any()
            or pd.isna(duration_seconds).any()
            or (duration_seconds <= 0).any()
            or (duration_seconds % 60 != 0).any()
            or minute_df["expected_bar_count"].ne(
                duration_seconds // 60
            ).any()
        ):
            raise ValueError(f"{context}分钟 Session 结构不合法。")
        expected_numbers = minute_df.groupby(
            ["contract_code", "trading_date"], sort=False
        ).cumcount().add(1)
        if minute_df["session_number"].ne(expected_numbers).any():
            raise ValueError(f"{context}分钟 Session 编号不连续。")
    return checked_df


# dirty 完整输出在 staging 前承担唯一一次完整状态校验；staging 与正式安装后只复核契约、主键和行数摘要。
def validate_bar_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked_df = validate_structural_frame(
        frame, context, schema=FUTURES_BAR_CALENDAR_SCHEMA
    )
    if checked_df.empty:
        return checked_df

    enum_rules = {
        "schedule_status": SCHEDULE_STATUSES,
        "evidence_level": EVIDENCE_LEVELS,
        "quality_status": QUALITY_STATUSES,
    }
    for column, allowed_values in enum_rules.items():
        if not checked_df[column].isin(allowed_values).all():
            raise ValueError(f"{context}{column} 不在允许枚举中。")

    text_fields = [
        "schedule_signal_reason",
        "evidence_source",
        "selection_reason",
        "quality_reason",
    ]
    if checked_df[text_fields].apply(
        lambda series: series.str.strip().eq("")
    ).to_numpy().any():
        raise ValueError(f"{context}中文状态说明字段不得为空。")
    if (
        checked_df["actual_bar_count"].lt(0).any()
        or checked_df["missing_bar_count"].lt(0).any()
    ):
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    confirmed_closed = checked_df["schedule_status"].eq("confirmed_closed")
    if (
        confirmed_closed
        & (
            checked_df["evidence_level"].ne("authoritative")
            | checked_df["is_fetch_required"]
        )
    ).any():
        raise ValueError(f"{context}确认休市必须具有权威证据且不得继续拉取。")

    completed = checked_df["is_fetch_completed"]
    missing_run_id = (
        checked_df["fetch_run_id"].isna()
        | checked_df["fetch_run_id"].eq("")
    )
    if (completed & (missing_run_id | checked_df["fetch_completed_at"].isna())).any():
        raise ValueError(f"{context}完成状态缺少运行批次或完成时间。")
    if (~completed & checked_df["fetch_completed_at"].notna()).any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    unchecked = checked_df["missing_checked_at"].isna()
    if (
        unchecked
        & (checked_df["is_data_missing"] | checked_df["missing_bar_count"].ne(0))
    ).any():
        raise ValueError(f"{context}未经缺失检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0).where(checked_df["is_fetch_required"], 0)
    checked_missing = ~unchecked
    if (
        checked_missing
        & checked_df["missing_bar_count"].ne(expected_missing)
    ).any():
        raise ValueError(f"{context}missing_bar_count 无法由条数复算。")
    if (
        checked_missing
        & checked_df["is_data_missing"].ne(expected_missing.gt(0))
    ).any():
        raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")
    if (
        checked_df["quality_status"].ne("pending")
        & checked_df["quality_checked_at"].isna()
    ).any():
        raise ValueError(f"{context}非 pending 质量状态缺少检查时间。")
    return checked_df


def assess_structural_partition(
    expected_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
    forced_quality_error: str | None,
) -> dict[str, object]:
    """期望结构由生成函数校验；此处检查现有结构并计算双向差异。"""
    expected_df = expected_frame.loc[:, STRUCTURAL_COLUMNS]
    business_quality_error = None
    try:
        existing_df = validate_structural_frame(existing_frame, "现有下游结构")
    except (TypeError, ValueError) as error:
        existing_row_count = len(existing_frame)
        existing_df = empty_pandas(STRUCTURAL_SCHEMA)
        business_quality_error = f"{type(error).__name__}: {error}"
    else:
        existing_row_count = len(existing_df)

    quality_messages = [
        message
        for message in [forced_quality_error, business_quality_error]
        if message is not None
    ]
    quality_error = "；".join(quality_messages) or None
    if expected_df.equals(existing_df) and quality_error is None:
        return {
            "expected_row_count": len(expected_df),
            "complete_count": len(expected_df),
            "missing_count": 0,
            "changed_count": 0,
            "extra_count": 0,
            "quality_error": None,
            "is_dirty": False,
        }

    joined_df = expected_df.merge(
        existing_df,
        how="outer",
        on=PRIMARY_KEY,
        suffixes=("__expected", "__existing"),
        indicator=True,
        validate="one_to_one",
    )
    missing_count = int(joined_df["_merge"].eq("left_only").sum())
    extra_count = int(joined_df["_merge"].eq("right_only").sum())
    common_df = joined_df.loc[joined_df["_merge"].eq("both")]
    same_structure = pd.Series(True, index=common_df.index, dtype=bool)
    for column in STRUCTURAL_NON_KEY_COLUMNS:
        expected_values = common_df[f"{column}__expected"]
        existing_values = common_df[f"{column}__existing"]
        same_values = expected_values.eq(existing_values) | (
            expected_values.isna() & existing_values.isna()
        )
        same_structure &= same_values.fillna(False).astype(bool)
    changed_count = int((~same_structure).sum())

    if forced_quality_error is not None and business_quality_error is None:
        changed_count = len(common_df)
    if business_quality_error is not None:
        extra_count = existing_row_count
    complete_count = (
        0
        if quality_error is not None
        else len(expected_df) - missing_count - changed_count
    )
    audit = {
        "expected_row_count": len(expected_df),
        "complete_count": complete_count,
        "missing_count": missing_count,
        "changed_count": changed_count,
        "extra_count": extra_count,
        "quality_error": quality_error,
    }
    audit["is_dirty"] = bool(
        missing_count or changed_count or extra_count or quality_error
    )
    return audit


# ## Dataset 发现与按分区读取
# 
# `open_contract_dataset()` 在启动时各打开一次上下游根 Dataset，检查逻辑 Schema、逐个 fragment 的物理字段及稳定表身份。上游不存在时报错，下游尚未建立时返回 `None`。字段、类型、nullable、表名、主键和分区必须兼容；描述性 metadata 差异不构成历史叶重写理由。
# 
# `partition_keys_from_files()` 从真实 Parquet 路径发现 Hive 键，排除根目录 `schema.parquet`。`read_partition()` 使用分区和可选日期过滤，只投影指定 Schema 的列。计划阶段读取结构列；写入前使用后面的 `read_existing_partition_leaf()` 精确复读当前 dirty 叶的完整状态，不在分区提交循环重新打开表根。
# 
# 读取进度由函数自己记录：Dataset 打开报告逐 fragment 检查进度，分区发现报告已见文件和分区数，两者复用原遍历，不为日志额外扫描目录；投影读取报告分区、列数、日期范围、返回行数和耗时。可选下游不存在、空结果和读取异常分别表达；异常记录阶段后原样抛出。

# ### 局部流程：发现 Dataset 与读取投影
# 
# 根 Dataset 在入口各打开一次；发现与读取函数自行报告进度和结果，异常记录后继续抛出。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["寻找实际 Parquet 文件"] --> B{"数据存在？"}
#     B -->|否| C{"required 上游？"}
#     C -->|是| D["抛出上游不存在异常"]
#     C -->|否| E["返回 None；按空目标规划"]
#     B -->|是| F["打开 Dataset；逐 fragment 检查并报告进度"]
#     F --> G["从文件路径发现 Hive 键；排除 schema.parquet"]
#     G --> H["按分区和可选日期过滤；投影指定列"]
#     H --> I["按 Schema 转成 DataFrame；报告行数与完成耗时"]
# ```

# In[4]:


# Dataset 会混入 Hive 分区字段；按契约顺序重建完整 Schema。
def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    """忽略描述 metadata；硬检查物理字段和稳定表身份。"""
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


# 统一打开上游或下游 Dataset；既有湖不比较描述性 metadata。
def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
    *,
    required: bool,
) -> ds.Dataset | None:
    log_started_at = time.perf_counter()
    log_phase = "file_discovery"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
        f"status=started; path={table_path}; label={label}; required={str(required).lower()}"
    )
    try:
        first_parquet = (
            next(table_path.rglob("*.parquet"), None)
            if table_path.is_dir()
            else None
        )
        if first_parquet is None:
            if required:
                raise FileNotFoundError(f"{label}不存在：{table_path}")
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
                f"status=completed; path={table_path}; outcome=absent_optional; checked_fragments=0; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return None

        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
        )
        log_phase = "logical_schema"
        actual_schema = reconstructed_schema(dataset, schema)
        if not physically_and_identity_compatible(actual_schema, schema):
            raise TypeError(f"{label} 物理结构或表身份与契约不一致。")
        expected_file_schema = pa.schema(
            [
                schema.field(name)
                for name in schema.names
                if name not in partition_columns
            ],
            metadata=schema.metadata,
        )
        log_phase = "fragment_schema"
        log_fragment_count = 0
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; "
            f"phase=fragment_schema; status=started; path={table_path}; checked_fragments=0"
        )
        for fragment in dataset.get_fragments():
            if not physically_and_identity_compatible(
                fragment.physical_schema, expected_file_schema
            ):
                raise TypeError(
                    f"{label} fragment 物理结构或表身份与契约不一致："
                    f"{fragment.path}"
                )
            log_fragment_count += 1
            if log_fragment_count == 1 or log_fragment_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; "
                    f"phase=fragment_schema; status=running; path={table_path}; "
                    f"checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
            f"status=completed; path={table_path}; outcome=ready; checked_fragments={log_fragment_count}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
            f"status=failed; path={table_path}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def cast_partition_value(column: str, raw_value: str) -> object:
    if column in {"year", "month"}:
        return int(raw_value)
    return raw_value


# 从实际 Parquet 文件路径发现完整 Hive 分区键，不能只看目录名。
def partition_keys_from_files(
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    log_started_at = time.perf_counter()
    log_file_count = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; "
        f"phase=partition_discovery; status=started; path={table_path}"
    )
    try:
        keys = set()
        if not table_path.is_dir():
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
                f"status=completed; path={table_path}; files_seen={log_file_count}; partitions={len(keys)}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return keys

        for parquet_path in table_path.rglob("*.parquet"):
            log_file_count += 1
            if parquet_path.name == "schema.parquet":
                continue

            relative_parts = parquet_path.relative_to(table_path).parts
            if len(relative_parts) != len(partition_columns) + 1:
                raise ValueError(f"数据集包含非法分区文件：{parquet_path}")

            values = []
            for column, directory_name in zip(
                partition_columns,
                relative_parts[:-1],
                strict=True,
            ):
                prefix = f"{column}="
                if not directory_name.startswith(prefix):
                    raise ValueError(f"数据集包含非法分区目录：{parquet_path}")

                raw_value = directory_name[len(prefix):]
                values.append(cast_partition_value(column, raw_value))

            keys.add(tuple(values))
            if log_file_count == 1 or log_file_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
                    f"status=running; path={table_path}; files_seen={log_file_count}; partitions={len(keys)}; "
                    f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
            f"status=completed; path={table_path}; files_seen={log_file_count}; partitions={len(keys)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return keys
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
            f"status=failed; path={table_path}; files_seen={log_file_count}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def partition_filter(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None

    for column, value in zip(
        partition_columns,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


# 按完整分区键读取；显式日期只在读取后进一步裁剪行。
def read_partition(
    dataset: ds.Dataset,
    schema: pa.Schema,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "filter"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=started; "
        f"partition={partition_key}; columns={len(schema.names)}; start_date={start_date}; end_date={end_date}"
    )
    try:
        expression = partition_filter(partition_columns, partition_key)

        if start_date is not None:
            expression &= ds.field("trading_date") >= start_date
            expression &= ds.field("trading_date") <= end_date

        log_phase = "scan"
        table = dataset.to_table(
            columns=schema.names,
            filter=expression,
        )
        log_phase = "schema_conversion"
        partition_df = arrow_to_pandas(table, schema)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=completed; "
            f"partition={partition_key}; rows={len(partition_df)}; columns={len(schema.names)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=failed; "
            f"partition={partition_key}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 从合约 Session 生成两种频率的结构
# 
# `build_structural_partitions()` 一次处理一个交易所—年月的 c03 投影，同时生成两种频率。分钟按 Session 保留时段信息，将 `minute_count` 作为 `expected_bar_count`；日线按合约日去重，使用 `session_number=0`、空 Session 字段和 `expected_bar_count=1`。两份生成结果分别执行结构校验。
# 
# `initial_state()` 为新建或结构变化的行提供初始状态：日线等待 c05 采集，分钟先保持未选择，等待 c06 评估采集范围。c04 不导入事实白名单，也不把初始化解释为事实已完成。
# 
# 主流程计划阶段只生成结构，写入阶段再生成 dirty 范围并补状态，避免同时保存全历史完整结果。`build_fresh_partitions()` 是供独立调用的完整结果入口，另含上游输入及完整输出校验；本轮保留这两条调用路径。
# 
# `build_structural_partitions()` 自行报告输入行数、分钟结构完成、日线结构完成及总耗时，进度单位为两种频率。独立 `build_fresh_partitions()` 另报告完整状态结果的逐频率进度；它调用结构生成函数产生的日志保留独立的 `function/phase`。空输入也完成校验并报告零行结果；异常只报告失败，不报告生成成功。

# ### 局部流程：理论结构与初始状态
# 
# 主流程计划阶段只需要结构；写入阶段补初始状态。独立完整结果入口另外承担输入及输出校验。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录生成开始；取得结构投影并排序"] --> B["分钟：逐 Session 保留；分钟数映射到预期条数"]
#     A --> C["日线：按合约日去重；编号 0、空时段、预期 1 条"]
#     B --> D["分别校验并报告频率进度；记录完成耗时"]
#     C --> D
#     D --> E{"调用方需要完整状态？"}
#     E -->|否| F["用于结构计划比较"]
#     E -->|是| G["补 initial_state；日线待采集，分钟待 c06 评估"]
#     G --> H["主流程进入状态继承；独立 helper 校验并报告完整结果"]
# ```

# In[5]:


# 新格点只初始化调度状态，不冒充事实已经完成。
def initial_state(
    frequency: str,
    updated_at: datetime,
) -> dict[str, object]:
    # 日线覆盖全部固定月份合约；分钟等待 b06 应用共享白名单。
    if frequency == "1d":
        is_fetch_required = True
        selection_reason = (
            "完整固定月份合约日线格点；等待 c05_futures_daily 采集。"
        )
        quality_reason = "等待日线事实提交与复读质检。"
    else:
        is_fetch_required = False
        selection_reason = (
            "分钟事实采集范围由 c06_futures_minute 维护；"
            "当前新建或结构变化格点等待其评估。"
        )
        quality_reason = "等待分钟采集政策评估及后续事实质检。"

    return {
        "schedule_status": "scheduled",
        "schedule_signal_reason": (
            "上游合约 Session 规则有效，结构阶段按计划开市初始化。"
        ),
        "evidence_level": "contract_rule",
        "evidence_source": "dim_futures_contract_calendar",
        "is_fetch_required": is_fetch_required,
        "selection_reason": selection_reason,
        "is_fetch_completed": False,
        "actual_bar_count": 0,
        "is_data_missing": False,
        "missing_bar_count": 0,
        "fetch_run_id": None,
        "fetch_completed_at": None,
        "missing_checked_at": None,
        "quality_status": "pending",
        "quality_reason": quality_reason,
        "daily_open": None,
        "daily_high": None,
        "daily_low": None,
        "daily_close": None,
        "daily_volume": None,
        "daily_money": None,
        "daily_open_interest": None,
        "aggregated_open": None,
        "aggregated_high": None,
        "aggregated_low": None,
        "aggregated_close": None,
        "aggregated_volume": None,
        "aggregated_money": None,
        "aggregated_open_interest": None,
        "ohlc_matches_daily": None,
        "volume_matches_daily": None,
        "money_matches_daily": None,
        "open_interest_matches_daily": None,
        "quality_checked_at": None,
        "updated_at": updated_at,
    }


# 正式路径信任 b03 已提交语义，只转换 12 列投影并验证生成的 13 列 b04 结构。
def build_structural_partitions(
    upstream_contract_structure_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    log_started_at = time.perf_counter()
    log_phase = "input_conversion"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
        f"status=started; completed=0; total=2; upstream_rows={len(upstream_contract_structure_df)}"
    )
    try:
        upstream_contract_structure_table = pandas_to_arrow(
            upstream_contract_structure_df.loc[:, UPSTREAM_STRUCTURE_COLUMNS],
            UPSTREAM_STRUCTURE_SCHEMA,
        )
        upstream_contract_structure_df = arrow_to_pandas(
            upstream_contract_structure_table, UPSTREAM_STRUCTURE_SCHEMA
        )
        upstream_contract_structure_df = upstream_contract_structure_df.sort_values(
            UPSTREAM_PRIMARY_KEY
        ).reset_index(drop=True)

        log_phase = "minute_structure"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=minute_structure; "
            "status=started; bar_frequency=1m"
        )
        minute_bar_structure_df = upstream_contract_structure_df.rename(
            columns={"minute_count": "expected_bar_count"}
        ).copy()
        minute_bar_structure_df["bar_frequency"] = "1m"
        minute_bar_structure_df = validate_structural_frame(
            minute_bar_structure_df.loc[:, STRUCTURAL_COLUMNS],
            "新生成分钟结构分区",
        )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
            f"status=running; completed=1; total=2; bar_frequency=1m; rows={len(minute_bar_structure_df)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "daily_structure"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=daily_structure; "
            "status=started; bar_frequency=1d"
        )
        daily_bar_structure_df = upstream_contract_structure_df.drop_duplicates(
            ["contract_code", "trading_date"],
            keep="first",
        ).copy()
        daily_bar_structure_df["bar_frequency"] = "1d"
        daily_bar_structure_df["session_number"] = 0
        daily_bar_structure_df["session_text"] = None
        daily_bar_structure_df["session_start_at"] = None
        daily_bar_structure_df["session_end_at"] = None
        daily_bar_structure_df["is_night_session"] = None
        daily_bar_structure_df["expected_bar_count"] = 1
        daily_bar_structure_df = validate_structural_frame(
            daily_bar_structure_df.loc[:, STRUCTURAL_COLUMNS],
            "新生成日线结构分区",
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
            f"status=completed; completed=2; total=2; daily_rows={len(daily_bar_structure_df)}; "
            f"minute_rows={len(minute_bar_structure_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return {
            "1d": daily_bar_structure_df,
            "1m": minute_bar_structure_df,
        }
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
            f"status=failed; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# 脏分区慢路径补齐安全初始状态，并继续保留公开 helper 语义。
def build_fresh_partitions(
    contract_frame: pd.DataFrame,
    updated_at: datetime,
) -> dict[str, pd.DataFrame]:
    log_started_at = time.perf_counter()
    log_phase = "input_validation"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
        f"status=started; upstream_rows={len(contract_frame)}"
    )
    try:
        contracts_df = validate_contract_input(
            contract_frame,
            "上游合约日历分区",
        )
        log_phase = "structure_build"
        structural_by_frequency = build_structural_partitions(contracts_df)
        fresh_by_frequency = {}

        for frequency, structural_df in structural_by_frequency.items():
            log_phase = "initial_state"
            fresh_df = structural_df.copy()
            for column, value in initial_state(frequency, updated_at).items():
                fresh_df[column] = value
            log_phase = "output_validation"
            fresh_by_frequency[frequency] = validate_bar_calendar_frame(
                fresh_df.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
                f"新生成 {frequency} 完整分区",
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
                f"status=running; bar_frequency={frequency}; completed={len(fresh_by_frequency)}; "
                f"total={len(structural_by_frequency)}; rows={len(fresh_by_frequency[frequency])}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
            f"status=completed; completed={len(fresh_by_frequency)}; total={len(structural_by_frequency)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return fresh_by_frequency
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
            f"status=failed; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 结构差异与既有状态继承
# 
# `merge_preserving_state()` 按主键对齐新旧行，再逐列比较结构；只有主键存在且结构完全相同的行，才继承全部 `STATE_COLUMNS`，包括 `updated_at`。新增或结构变化的行使用 fresh 初始状态；期望集合之外的行不进入合并结果，是否删除由入口的更新范围决定。
# 
# `assess_partition()` 将输入直接交给 `merge_preserving_state()`，由后者各规范化一次新旧完整表，再合并状态并返回结构审计结果。状态继承仅从已经固定类型的新旧列逐值选择，结果保留这些类型，不再整表往返转换。比较只看结构，不因更新时间或事实状态本身变化而生成写入计划。计划阶段不读取完整状态；进入 dirty 写入阶段后重新读取当前叶，以取得 c05—c08 最新回写值。完整状态的业务规则仍由提交前的完整叶校验负责。

# ### 局部流程：按结构继承状态
# 
# 是否保留范围外旧行由 main 与提交函数决定；此处只说明期望格点的状态合并。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["固定新旧完整表 Schema"] --> B["按主键左连接新旧行"]
#     B --> C{"旧键存在且所有结构列相同？"}
#     C -->|是| D["继承全部旧状态，包括 updated_at"]
#     C -->|否| E["保留 fresh 初始状态"]
#     D --> F["形成期望完整结果；保留列类型并排序"]
#     E --> F
#     F --> G["比较期望与现有结构；返回结果及 dirty 审计"]
# ```

# In[6]:


# 结构未变化时向量化继承下游事实与质检回写状态。
def merge_preserving_state(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
) -> pd.DataFrame:
    fresh_df = arrow_to_pandas(
        pandas_to_arrow(
            fresh_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    existing_df = arrow_to_pandas(
        pandas_to_arrow(
            existing_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    merged_df = fresh_df.merge(
        existing_df,
        how="left",
        on=PRIMARY_KEY,
        suffixes=("", "__existing"),
        indicator=True,
        validate="one_to_one",
    )
    same_structure = merged_df["_merge"].eq("both")
    for column in STRUCTURAL_NON_KEY_COLUMNS:
        fresh_values = merged_df[column]
        existing_values = merged_df[f"{column}__existing"]
        same_values = fresh_values.eq(existing_values) | (
            fresh_values.isna() & existing_values.isna()
        )
        same_structure &= same_values.fillna(False).astype(bool)

    # 新增或结构变化行保留 fresh 安全状态；只覆盖结构完全相同的行。
    for column in STATE_COLUMNS:
        merged_df[column] = merged_df[column].where(
            ~same_structure,
            merged_df[f"{column}__existing"],
        )

    return merged_df.loc[
        :, FUTURES_BAR_CALENDAR_SCHEMA.names
    ].sort_values(PRIMARY_KEY).reset_index(drop=True)


# 比较期望分区与现有分区，汇总新增、删除、修订和质量错误。
def assess_partition(
    fresh_frame: pd.DataFrame,
    existing_frame: pd.DataFrame,
    forced_quality_error: str | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    # 状态合并阶段只固定 Schema；完整业务规则由提交前唯一 validator 负责。
    desired_df = merge_preserving_state(fresh_frame, existing_frame)

    audit = assess_structural_partition(
        desired_df.loc[:, STRUCTURAL_COLUMNS],
        existing_frame.loc[:, STRUCTURAL_COLUMNS],
        forced_quality_error,
    )
    return desired_df, audit


# ## 单叶暂存、共享安装与失败恢复
# 
# `commit_partition()` 检查输入分区和替换范围，合并显式日期以外的保留行，再对 dirty 完整叶执行一次业务校验。非空结果写入 staging，复读文件 Schema/metadata、主键与行数后，再进入 `StagedPathTransaction` 安装；空结果以 `staged_path=None` 显式删除该叶，应有数据但来源路径缺失则报错。自动和 full 模式由调用方传入完整目标叶，显式日期写入只允许用于非正式湖。
# 
# 一次共享事务只负责当前叶及本次变更的 `schema.parquet`。标记不存在时新建；已存在但与当前文件 Schema/metadata 不同的零行标记会替换。新标记先写 staging，再由同一事务备份、安装并正式复读，确认文件契约和零行要求。已经匹配的标记保留原文件。这个标记不保存 c03 那样的已处理日期水位。
# 
# 正式叶仍只检查当前叶的物理契约、主键和行数。安装或验收失败时，共享模块按实际移动记录倒序恢复旧标记和旧叶；失败的新叶隔离，新标记直接移除；恢复不完整时保留备份。此前成功叶不参与回滚。staging 写入或复读失败由本函数清理；事务开始后的清理与恢复由共享模块负责。
# 
# 读取、合并校验、暂存、安装、标记与正式叶复读阶段仍由对应函数报告。共享模块报告恢复结果；本函数在事务退出后报告失败阶段及耗时，成功退出后才报告 `partition_committed`。业务合并、状态继承、水位选择和返回行数语义保持不变。

# ### 局部流程：单叶提交与共享恢复
# 
# 共享模块负责路径安装与恢复；业务校验、文件写入和正式验收留在 c04。当前叶与本次变更的契约标记共同回滚。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录开始；检查范围并保留区间外行"] --> B["完整叶业务校验；转换 Arrow"]
#     B --> C{"结果有行？"}
#     C -->|是| D["写 staging；复读文件契约、主键和行数"]
#     C -->|否| E["明确删除当前叶"]
#     D -. 暂存失败 .-> X["本函数清理 staging；抛错"]
#     D --> F["进入共享事务；备份并安装或删除当前叶"]
#     E --> F
#     F --> G["必要时暂存、安装并验收零行 schema.parquet"]
#     G --> H["仅复读正式当前叶；检查物理契约、主键和行数"]
#     F -. 安装失败 .-> R["共享事务倒序恢复实际移动的标记与当前叶"]
#     G -. 标记失败 .-> R
#     H -. 验收失败 .-> R
#     R --> S["隔离失败新叶；移除新标记；恢复不完整保留备份"]
#     S --> U["清理并记录恢复结果；本函数报告失败阶段并抛错"]
#     H --> T["成功退出并清理；记录完成，返回输入行数"]
# ```

# In[7]:


# 每个物理 Parquet 文件都必须带有去除 Hive 分区列后的精确文件 Schema。
def validate_output_parquet_files(table_path: pathlib.Path) -> None:
    parquet_files = list(table_path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"数据集没有 Parquet 文件：{table_path}")

    for parquet_path in parquet_files:
        actual_schema = pq.read_schema(parquet_path)
        if not actual_schema.equals(PARQUET_FILE_SCHEMA, check_metadata=True):
            raise TypeError(
                f"Parquet 文件 Schema/metadata 与契约不一致：{parquet_path}"
            )


def partition_relative_path(
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return pathlib.Path(
        *[
            f"{name}={value}"
            for name, value in zip(
                PARTITION_COLUMNS,
                partition_key,
                strict=True,
            )
        ]
    )


# dirty 慢路径只精确打开当前正式叶，避免重新扫描或打开根 Dataset。
def read_existing_partition_leaf(
    target_path: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "leaf_discovery"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
        f"status=started; partition={partition_key}; path={target_path}"
    )
    try:
        leaf_path = target_path / partition_relative_path(partition_key)
        parquet_files = list(leaf_path.glob("*.parquet"))
        if not parquet_files:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
                f"status=completed; partition={partition_key}; outcome=absent_leaf; rows=0; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(FUTURES_BAR_CALENDAR_SCHEMA)

        log_phase = "leaf_schema"
        leaf_dataset = ds.dataset(
            leaf_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
            partition_base_dir=str(target_path),
        )
        leaf_schema = reconstructed_schema(
            leaf_dataset, FUTURES_BAR_CALENDAR_SCHEMA
        )
        if not physically_and_identity_compatible(
            leaf_schema, FUTURES_BAR_CALENDAR_SCHEMA
        ):
            raise TypeError(
                f"正式叶 {partition_key} 物理结构或表身份与契约不兼容。"
            )
        log_phase = "leaf_read"
        existing_partition_df = read_partition(
            leaf_dataset,
            FUTURES_BAR_CALENDAR_SCHEMA,
            PARTITION_COLUMNS,
            partition_key,
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
            f"status=completed; partition={partition_key}; files={len(parquet_files)}; rows={len(existing_partition_df)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return existing_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
            f"status=failed; partition={partition_key}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def commit_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
    existing_partition_frame: pd.DataFrame | None = None,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "replacement_scope"
    click.echo(
        f"partition_start: table={TABLE_NAME}; function=commit_partition; phase=commit; status=started; "
        f"partition={partition_key}; completed=0; total=1; rows={len(frame)}; lake_root={lake_root}"
    )
    try:
        if (replace_start_date is None) != (replace_end_date is None):
            raise ValueError("替换起止日期必须同时提供。")
        if replace_start_date is not None and replace_start_date > replace_end_date:
            raise ValueError("替换起始日期不得晚于结束日期。")

        # 先固定输入 Schema；完整业务规则只在合并得到最终叶后校验一次。
        log_phase = "input_conversion"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=input_conversion; "
            f"status=started; partition={partition_key}"
        )
        incoming_table = pandas_to_arrow(
            frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        incoming_df = arrow_to_pandas(
            incoming_table, FUTURES_BAR_CALENDAR_SCHEMA
        )
        if not incoming_df.empty:
            incoming_keys = set(
                incoming_df[PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            )
            if incoming_keys != {partition_key}:
                raise ValueError("待提交数据越出指定 Hive 分区。")

            if replace_start_date is not None:
                outside_range = (
                    incoming_df["trading_date"].lt(replace_start_date)
                    | incoming_df["trading_date"].gt(replace_end_date)
                )
                if outside_range.any():
                    raise ValueError("待提交数据越出显式替换日期范围。")

        log_phase = "paths"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=paths; "
            f"status=started; partition={partition_key}"
        )
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / TABLE_NAME

        run_id = uuid.uuid4().hex[:12]
        staging_path = silver_root / f".c04s-{run_id}"
        backup_path = silver_root / f".c04b-{run_id}"
        quarantine_path = silver_root / f".c04q-{run_id}"

        silver_root.mkdir(parents=True, exist_ok=True)
        for managed_path in (
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ):
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

        log_phase = "merge"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=merge; "
            f"status=started; partition={partition_key}"
        )
        if replace_start_date is None:
            complete_partition_df = incoming_df
        else:
            if existing_partition_frame is None:
                existing_partition_frame = read_existing_partition_leaf(
                    target_path, partition_key
                )
            existing_table = pandas_to_arrow(
                existing_partition_frame.loc[
                    :, FUTURES_BAR_CALENDAR_SCHEMA.names
                ],
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            existing_df = arrow_to_pandas(
                existing_table, FUTURES_BAR_CALENDAR_SCHEMA
            )
            retained_df = existing_df.loc[
                (existing_df["trading_date"] < replace_start_date)
                | (existing_df["trading_date"] > replace_end_date),
                FUTURES_BAR_CALENDAR_SCHEMA.names,
            ]
            complete_partition_df = pd.concat(
                [retained_df, incoming_df],
                ignore_index=True,
            )

        log_phase = "dirty_validation"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=dirty_validation; "
            f"status=started; partition={partition_key}"
        )
        complete_partition_df = validate_bar_calendar_frame(
            complete_partition_df,
            "提交前 dirty 完整分区",
        )
        complete_partition_table = pandas_to_arrow(
            complete_partition_df,
            FUTURES_BAR_CALENDAR_SCHEMA,
        )

        log_phase = "staging_write"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=staging_write; "
            f"status=started; partition={partition_key}"
        )
        relative_path = partition_relative_path(partition_key)
        staging_path.mkdir(parents=True, exist_ok=False)

        # 先写 staging 并复核完整文件契约、主键和行数摘要，再接触正式目标。
        try:
            if len(complete_partition_table):
                ds.write_dataset(
                    complete_partition_table,
                    staging_path,
                    format="parquet",
                    partitioning=HIVE_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

                source_path = staging_path / relative_path
                log_phase = "staging_readback"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=staging_readback; "
                    f"status=started; partition={partition_key}"
                )
                validate_output_parquet_files(source_path)
                staged_dataset = ds.dataset(
                    source_path,
                    format="parquet",
                    partitioning=HIVE_PARTITIONING,
                    partition_base_dir=str(staging_path),
                )
                staged_schema = reconstructed_schema(
                    staged_dataset, FUTURES_BAR_CALENDAR_SCHEMA
                )
                if not staged_schema.equals(
                    FUTURES_BAR_CALENDAR_SCHEMA, check_metadata=True
                ):
                    raise TypeError("staging Schema/metadata 与契约不一致。")

                staged_primary_key_df = read_partition(
                    staged_dataset,
                    PRIMARY_KEY_SCHEMA,
                    PARTITION_COLUMNS,
                    partition_key,
                )
                if staged_primary_key_df.duplicated(PRIMARY_KEY).any():
                    raise ValueError("staging 分区主键不唯一。")
                if len(staged_primary_key_df) != len(complete_partition_table):
                    raise ValueError("staging 分区行数摘要不一致。")
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        marker_path = target_path / "schema.parquet"

        # 正式替换使用备份和隔离目录，异常时恢复原分区。
        log_phase = "prepare_install"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=prepare_install; "
            f"status=started; partition={partition_key}"
        )
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"table={TABLE_NAME}; partition={partition_key}; run_id={run_id}",
        ) as transaction:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=install; "
                f"status=started; partition={partition_key}"
            )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if len(complete_partition_table) else None,
            )

            log_phase = "schema_marker"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=schema_marker; "
                f"status=started; partition={partition_key}"
            )
            marker_needs_install = not marker_path.exists()
            if marker_path.exists():
                marker_schema = pq.read_schema(marker_path)
                if not marker_schema.equals(
                    PARQUET_FILE_SCHEMA,
                    check_metadata=True,
                ):
                    marker_metadata = pq.read_metadata(marker_path)
                    if marker_metadata.num_rows:
                        raise ValueError("schema.parquet 必须是 0 行契约标记。")
                    marker_needs_install = True

            if marker_needs_install:
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=PARQUET_FILE_SCHEMA),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path,
                    staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    if not marker_file.schema_arrow.equals(
                        PARQUET_FILE_SCHEMA, check_metadata=True
                    ) or marker_file.metadata.num_rows != 0:
                        raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")

            log_phase = "formal_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=formal_readback; "
                f"status=started; partition={partition_key}"
            )
            if destination_path.is_dir():
                committed_dataset = ds.dataset(
                    destination_path,
                    format="parquet",
                    partitioning=HIVE_PARTITIONING,
                    partition_base_dir=str(target_path),
                )
                committed_schema = reconstructed_schema(
                    committed_dataset, FUTURES_BAR_CALENDAR_SCHEMA
                )
                if not physically_and_identity_compatible(
                    committed_schema, FUTURES_BAR_CALENDAR_SCHEMA
                ):
                    raise TypeError("正式叶物理结构或表身份与契约不一致。")
                committed_primary_key_df = read_partition(
                    committed_dataset,
                    PRIMARY_KEY_SCHEMA,
                    PARTITION_COLUMNS,
                    partition_key,
                )
            else:
                committed_primary_key_df = empty_pandas(PRIMARY_KEY_SCHEMA)
            if committed_primary_key_df.duplicated(PRIMARY_KEY).any():
                raise ValueError("正式叶主键不唯一。")
            if len(committed_primary_key_df) != len(complete_partition_table):
                raise ValueError("正式叶行数与 staging 摘要不一致。")

        click.echo(
            f"partition_committed: table={TABLE_NAME}; function=commit_partition; phase=commit; status=completed; "
            f"partition={partition_key}; completed=1; total=1; rows={len(incoming_df)}; "
            f"replacement_rows={len(complete_partition_table)}; run_id={run_id}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return len(incoming_df)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=commit; status=failed; "
            f"partition={partition_key}; failed_phase={log_phase}; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 三种运行模式、差异计划与顺序提交
# 
# `main()` 先检查参数，再发现 Dataset 和分区。自动模式读取每个“频率—交易所”的目标尾叶，取最大 `trading_date`，仅生成并追加严格晚于水位的上游结构；目标缺少某一频率时，该频率从全部上游建立。full 和显式日期模式比较对应范围的上下游结构，并把缺失、修订及多余行汇总为 dirty 计划。
# 
# 没有 dirty 叶时结束；没有 `--write` 时只输出计划。写入前按两种频率各生成一次本批初始状态（共用 `run_updated_at`）；写入时按基础分区重新生成结构、填入相应初始状态，再精确读取当前 dirty 叶并继承状态。重新比较已变为 clean 的叶会跳过；其余逐叶提交，异常停止后续工作。自动尾部合并保留水位以内的历史行，显式日期提交保留日期范围外行。
# 
# 日志使用与 c01、c02 一致的 `=` 运行边界及 `table/phase/status` 字段。发现、计划和提交阶段报告数量与耗时；正常结束区分无需更新、只读计划和写入完成。读取、结构生成、完整结果生成和单叶提交日志由对应函数负责；`main()` 保留模式、计划、批次计数与累计耗时，不重复报告单叶提交起止。水位选择仍属于 `main()`，日志紧贴尾叶读取和最大交易日计算，分别报告各频率—交易所水位及总进度；full 与显式日期记录跳过自动水位选择。异常沿原调用链抛出，不输出运行完成日志。
# 
# 本格只定义 Click 入口；实际启动由后面的独立执行单元格负责。Notebook 显式传入参数，脚本使用命令行参数，普通导入不启动业务。
# 
# c04 没有独立水位写入。水位日志描述正式目标最大交易日的读取结果；提交函数中的 `schema_marker` 描述零行契约标记，两者不混称水位提交。

# ### 局部流程：范围选择与写入分支
# 
# 自动模式按频率—交易所确定尾部；三种模式均先形成计划。实际调用位于独立执行单元格。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查成对日期、full 互斥及正式写入门禁"] --> B{"选择模式"}
#     B -->|自动| C["逐频率—交易所读取尾叶并记录水位；选择尾部月份"]
#     B -->|日期| D["选择日期覆盖的上下游月份；过滤闭区间"]
#     B -->|full| E["选择上下游全部月份及两种频率"]
#     C --> F["生成结构；自动仅保留水位后的新日期"]
#     D --> F
#     E --> F
#     F --> G["比较并汇总计划；记录数量与耗时"]
#     G --> H{"有 dirty 叶且启用 write？"}
#     H -->|否| I["记录无需更新或只读完成；结束"]
#     H -->|是| J["复用本批初始状态；重建 dirty 范围并继承当前叶状态"]
#     J --> K{"重新比较仍有差异？"}
#     K -->|否| L["跳过 clean 叶"]
#     K -->|是| M["按模式保留旧行；提交当前叶"]
#     M -. 失败 .-> N["异常抛出；停止后续，不报运行成功"]
#     M --> O["报告批次进度；处理剩余计划"]
#     L --> O
#     O --> P["全部处理成功；报告本批结果与总耗时"]
# ```

# In[8]:


def month_is_relevant(
    partition_key: tuple[str, int, int],
    start_date: date,
    end_date: date,
) -> bool:
    _, year, month = partition_key
    return (
        (start_date.year, start_date.month)
        <= (year, month)
        <= (end_date.year, end_date.month)
    )


def default_tail_partition_keys(
    upstream_partition_keys: set[tuple[object, ...]],
    target_partition_keys: set[tuple[object, ...]],
) -> set[tuple[object, ...]]:
    # 每个频率—交易所只选目标尾月及随后上游月份。内部缺口、
    # 目标独有孤儿叶和旧月修订留给 --full。空目标频率自然选全部上游。
    upstream_months_by_exchange: dict[str, set[tuple[int, int]]] = {}
    for exchange_code, year, month in upstream_partition_keys:
        upstream_months_by_exchange.setdefault(exchange_code, set()).add(
            (year, month)
        )

    target_months_by_frequency_exchange: dict[
        tuple[str, str], set[tuple[int, int]]
    ] = {}
    for frequency, exchange_code, year, month in target_partition_keys:
        target_months_by_frequency_exchange.setdefault(
            (frequency, exchange_code), set()
        ).add((year, month))

    exchange_codes = sorted(upstream_months_by_exchange)
    selected_partition_keys: set[tuple[object, ...]] = set()
    for frequency in BAR_FREQUENCIES:
        for exchange_code in exchange_codes:
            upstream_months = upstream_months_by_exchange.get(
                exchange_code, set()
            )
            target_months = target_months_by_frequency_exchange.get(
                (frequency, exchange_code), set()
            )
            if not upstream_months:
                continue
            if not target_months:
                selected_months = upstream_months
            else:
                tail_boundary = max(target_months)
                selected_months = {
                    value
                    for value in upstream_months
                    if value >= tail_boundary
                }
            selected_partition_keys.update(
                (frequency, exchange_code, year, month)
                for year, month in selected_months
            )
    return selected_partition_keys


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--full", is_flag=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full: bool,
    write: bool,
) -> None:
    run_started_at = time.perf_counter()
    # 首先解析湖路径和显式日期，并执行正式写入门禁。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if full and has_explicit_dates:
        raise click.UsageError("--full 与显式日期范围互斥。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    automatic_tail_mode = not full and not has_explicit_dates
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    log_boundary = "=" * 88
    log_mode = "explicit" if has_explicit_dates else ("full" if full else "automatic_tail")
    click.echo(
        f"{log_boundary}\n行情日历运行开始 / Bar calendar run started\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\n{log_boundary}"
    )
    # 根 Dataset 各打开一次；规划循环只做列投影和分区过滤。
    discovery_started_at = time.perf_counter()
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=discovery; status=started")
    silver_root = resolved_lake_root / "silver"
    upstream_path = silver_root / UPSTREAM_TABLE_NAME
    target_path = silver_root / TABLE_NAME

    upstream_dataset = open_contract_dataset(
        upstream_path,
        UPSTREAM_PARTITIONING,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        UPSTREAM_PARTITION_COLUMNS,
        "上游合约 Session 日历",
        required=True,
    )
    target_dataset = open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        PARTITION_COLUMNS,
        "现有行情日历",
        required=False,
    )

    upstream_partition_keys = partition_keys_from_files(
        upstream_path,
        UPSTREAM_PARTITION_COLUMNS,
    )
    target_partition_keys = partition_keys_from_files(
        target_path,
        PARTITION_COLUMNS,
    )
    upstream_file_count = sum(
        pathlib.Path(path).name != "schema.parquet"
        for path in upstream_dataset.files
    )
    target_file_count = (
        sum(
            pathlib.Path(path).name != "schema.parquet"
            for path in target_dataset.files
        )
        if target_dataset is not None
        else 0
    )
    discovery_elapsed = time.perf_counter() - discovery_started_at

    invalid_frequencies = {
        key[0]
        for key in target_partition_keys
        if key[0] not in BAR_FREQUENCIES
    }
    if invalid_frequencies:
        raise ValueError(
            f"现有行情日历包含非法频率分区：{sorted(invalid_frequencies)}"
        )

    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=discovery; status=completed; "
        f"upstream_files={upstream_file_count}; target_files={target_file_count}; "
        f"upstream_partitions={len(upstream_partition_keys)}; target_partitions={len(target_partition_keys)}; "
        f"elapsed_s={discovery_elapsed:.3f}"
    )
    upstream_base_keys = set(upstream_partition_keys)
    target_base_keys = {
        (exchange_code, year, month)
        for _, exchange_code, year, month in target_partition_keys
    }
    log_watermark_started_at = time.perf_counter()
    log_watermark_count = 0
    if automatic_tail_mode:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=started; "
            "source=target_max_trading_date"
        )
    else:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=skipped; "
            f"mode={log_mode}; reason=explicit_scope"
        )
    try:
        automatic_tail_watermark_by_frequency_exchange: dict[
            tuple[str, str], date
        ] = {}
        target_tail_structure_row_count = 0
        if automatic_tail_mode and target_dataset is not None:
            target_partition_keys_by_frequency_exchange: dict[
                tuple[str, str], list[tuple[object, ...]]
            ] = {}
            for partition_key in target_partition_keys:
                frequency, exchange_code, _, _ = partition_key
                target_partition_keys_by_frequency_exchange.setdefault(
                    (frequency, exchange_code), []
                ).append(partition_key)
            for frequency_exchange, partition_keys in sorted(
                target_partition_keys_by_frequency_exchange.items()
            ):
                tail_partition_key = max(
                    partition_keys, key=lambda value: value[2:]
                )
                target_tail_structure_df = read_partition(
                    target_dataset,
                    STRUCTURAL_SCHEMA,
                    PARTITION_COLUMNS,
                    tail_partition_key,
                )
                target_tail_structure_row_count += len(
                    target_tail_structure_df
                )
                if not target_tail_structure_df.empty:
                    automatic_tail_watermark_by_frequency_exchange[
                        frequency_exchange
                    ] = max(target_tail_structure_df["trading_date"])
                log_watermark_count += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=watermark; status=running; "
                    f"frequency_exchange={frequency_exchange}; partition={tail_partition_key}; "
                    f"watermark={automatic_tail_watermark_by_frequency_exchange.get(frequency_exchange)}; "
                    f"completed={log_watermark_count}; total={len(target_partition_keys_by_frequency_exchange)}; "
                    f"rows={len(target_tail_structure_df)}; source=target_max_trading_date; "
                    f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
                )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=failed; "
            f"completed={log_watermark_count}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
        )
        raise
    if automatic_tail_mode:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=completed; "
            f"completed={log_watermark_count}; total={log_watermark_count}; "
            f"watermark_groups={len(automatic_tail_watermark_by_frequency_exchange)}; "
            f"rows={target_tail_structure_row_count}; source=target_max_trading_date; "
            f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
        )

    if has_explicit_dates:
        upstream_base_keys = {
            key
            for key in upstream_base_keys
            if month_is_relevant(
                key,
                requested_start_date,
                requested_end_date,
            )
        }
        target_base_keys = {
            key
            for key in target_base_keys
            if month_is_relevant(
                key,
                requested_start_date,
                requested_end_date,
            )
        }

    if full or has_explicit_dates:
        base_keys = sorted(upstream_base_keys | target_base_keys)
        candidate_partition_keys = {
            (frequency, *base_key)
            for base_key in base_keys
            for frequency in BAR_FREQUENCIES
        }
    else:
        candidate_partition_keys = default_tail_partition_keys(
            upstream_partition_keys, target_partition_keys
        )
        base_keys = sorted({
            (exchange_code, year, month)
            for _, exchange_code, year, month in candidate_partition_keys
        })
    run_updated_at = datetime.now(timezone.utc)

    planning_started_at = time.perf_counter()
    dirty_partition_plans = []
    evaluated_partition_count = 0
    clean_partition_count = 0
    upstream_source_row_count = 0
    target_structure_row_count = target_tail_structure_row_count
    upstream_grid_count = 0
    complete_grid_count = 0
    missing_grid_count = 0
    changed_grid_count = 0
    extra_grid_count = 0

    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=plan; status=started; "
        f"mode={log_mode}; completed=0; total={len(base_keys)}; "
        f"candidate_partitions={len(candidate_partition_keys)}"
    )
    # 按交易所—年月读取一次上游，再分别规划 1d 和 1m 分区。
    for base_number, base_key in enumerate(base_keys, start=1):
        if base_number == 1 or base_number % 25 == 0:
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; phase=plan; status=running; "
                f"completed={base_number - 1}; total={len(base_keys)}; "
                f"base_partitions={base_number}/{len(base_keys)}; key={base_key}; "
                f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
            )
        if base_key in upstream_partition_keys:
            upstream_contract_structure_df = read_partition(
                upstream_dataset,
                UPSTREAM_STRUCTURE_SCHEMA,
                UPSTREAM_PARTITION_COLUMNS,
                base_key,
                requested_start_date,
                requested_end_date,
            )
        else:
            upstream_contract_structure_df = empty_pandas(
                UPSTREAM_STRUCTURE_SCHEMA
            )
        upstream_source_row_count += len(upstream_contract_structure_df)
        expected_bar_structure_df_by_frequency = build_structural_partitions(
            upstream_contract_structure_df
        )

        for frequency in BAR_FREQUENCIES:
            partition_key = (frequency, *base_key)
            if partition_key not in candidate_partition_keys:
                continue
            expected_bar_structure_df = (
                expected_bar_structure_df_by_frequency[frequency]
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is not None:
                    expected_bar_structure_df = (
                        expected_bar_structure_df.loc[
                            expected_bar_structure_df["trading_date"].gt(
                                automatic_tail_watermark
                            )
                        ].reset_index(drop=True)
                    )
                existing_bar_structure_df = empty_pandas(
                    STRUCTURAL_SCHEMA
                )
            elif (
                target_dataset is not None
                and partition_key in target_partition_keys
            ):
                existing_bar_structure_df = read_partition(
                    target_dataset,
                    STRUCTURAL_SCHEMA,
                    PARTITION_COLUMNS,
                    partition_key,
                    requested_start_date,
                    requested_end_date,
                )
            else:
                existing_bar_structure_df = empty_pandas(STRUCTURAL_SCHEMA)
            target_structure_row_count += len(existing_bar_structure_df)
            structural_audit = assess_structural_partition(
                expected_bar_structure_df,
                existing_bar_structure_df,
                None,
            )

            evaluated_partition_count += 1
            upstream_grid_count += int(structural_audit["expected_row_count"])
            complete_grid_count += int(structural_audit["complete_count"])
            missing_grid_count += int(structural_audit["missing_count"])
            changed_grid_count += int(structural_audit["changed_count"])
            extra_grid_count += int(structural_audit["extra_count"])

            # 只有业务格点或质量状态真正变化时才加入写入计划。
            if structural_audit["is_dirty"]:
                dirty_partition_plans.append(
                    {
                        "partition_key": partition_key,
                        **structural_audit,
                    }
                )
            else:
                clean_partition_count += 1

    structure_planning_elapsed = time.perf_counter() - planning_started_at
    total_planning_elapsed = time.perf_counter() - run_started_at

    if has_explicit_dates:
        mode = "explicit"
        plan_name = "explicit_plan"
    elif full:
        mode = "full"
        plan_name = "full_plan"
    else:
        mode = "automatic_tail"
        plan_name = "auto_plan"

    click.echo(
        f"{plan_name}: table={TABLE_NAME}; phase=plan; status=completed; mode={mode}; "
        f"upstream_grid_count={upstream_grid_count}; "
        f"complete_grid_count={complete_grid_count}; "
        f"missing_grid_count={missing_grid_count}; "
        f"changed_grid_count={changed_grid_count}; "
        f"extra_grid_count={extra_grid_count}; "
        f"touched_partition_count={len(dirty_partition_plans)}; "
        f"clean_partition_count={clean_partition_count}; "
        f"evaluated_partition_count={evaluated_partition_count}"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=plan; status=completed; "
        f"completed={len(base_keys)}; total={len(base_keys)}; "
        f"upstream_files={upstream_file_count}; "
        f"target_files={target_file_count}; "
        f"upstream_rows={upstream_source_row_count}; "
        f"target_rows={target_structure_row_count}; "
        f"discovery_seconds={discovery_elapsed:.3f}; "
        f"structure_planning_seconds={structure_planning_elapsed:.3f}; "
        f"total_seconds={total_planning_elapsed:.3f}; elapsed_s={structure_planning_elapsed:.3f}"
    )

    for plan in dirty_partition_plans:
        click.echo(
            f"partition_plan: table={TABLE_NAME}; phase=plan; status=completed; "
            f"partition={plan['partition_key']}; "
            f"expected_rows={plan['expected_row_count']}; "
            f"missing={plan['missing_count']}; "
            f"changed={plan['changed_count']}; "
            f"extra={plan['extra_count']}; "
            f"quality_error={plan['quality_error']}"
        )

    if not dirty_partition_plans:
        click.echo(f"up_to_date: table={TABLE_NAME}; phase=plan; status=completed; mode={mode}")
        click.echo(
            f"{log_boundary}\n行情日历无需更新 / Bar calendar up to date\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write={str(write).lower()}; outcome=up_to_date; "
            f"elapsed_s={time.perf_counter() - run_started_at:.3f}\n{log_boundary}"
        )
        return
    # 不带 --write 时输出完整计划，但不创建 staging 或修改状态。
    if not write:
        click.echo(
            f"{log_boundary}\n行情日历只读计划完成 / Bar calendar read-only plan completed\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write=false; outcome=read_only; dirty_partitions={len(dirty_partition_plans)}; "
            f"elapsed_s={time.perf_counter() - run_started_at:.3f}\n{log_boundary}"
        )
        return

    planned_partition_keys = {
        plan["partition_key"]
        for plan in dirty_partition_plans
    }
    planned_base_keys = sorted(
        {
            (exchange_code, year, month)
            for _, exchange_code, year, month in planned_partition_keys
        }
    )

    committed_row_count = 0
    committed_partition_count = 0
    commit_elapsed_total = 0.0
    log_processed_partitions = 0
    log_skipped_partitions = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=started; "
        f"completed=0; total={len(dirty_partition_plans)}"
    )
    initial_state_by_frequency = {
        frequency: initial_state(frequency, run_updated_at)
        for frequency in BAR_FREQUENCIES
    }
    # 同一基础分区重新投影正式 b03 结构，逐分区构造完整 b04 目标并原子提交。
    for base_key in planned_base_keys:
        if base_key in upstream_partition_keys:
            upstream_contract_structure_df = read_partition(
                upstream_dataset,
                UPSTREAM_STRUCTURE_SCHEMA,
                UPSTREAM_PARTITION_COLUMNS,
                base_key,
                requested_start_date,
                requested_end_date,
            )
        else:
            upstream_contract_structure_df = empty_pandas(
                UPSTREAM_STRUCTURE_SCHEMA
            )
        expected_bar_structure_df_by_frequency = build_structural_partitions(
            upstream_contract_structure_df
        )
        fresh_bar_calendar_df_by_frequency = {}
        for frequency in BAR_FREQUENCIES:
            fresh_bar_calendar_df = (
                expected_bar_structure_df_by_frequency[frequency].copy()
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is not None:
                    fresh_bar_calendar_df = (
                        fresh_bar_calendar_df.loc[
                            fresh_bar_calendar_df["trading_date"].gt(
                                automatic_tail_watermark
                            )
                        ].reset_index(drop=True)
                    )
            for column, value in initial_state_by_frequency[frequency].items():
                fresh_bar_calendar_df[column] = value
            fresh_bar_calendar_df_by_frequency[frequency] = (
                fresh_bar_calendar_df.loc[
                    :, FUTURES_BAR_CALENDAR_SCHEMA.names
                ].copy()
            )

        for frequency in BAR_FREQUENCIES:
            partition_key = (frequency, *base_key)
            if partition_key not in planned_partition_keys:
                continue

            # 规划后可能有 b05～b08 更新状态；提交前精确复读当前完整叶。
            existing_complete_bar_calendar_df = (
                read_existing_partition_leaf(target_path, partition_key)
            )
            if automatic_tail_mode:
                exchange_code = str(base_key[0])
                automatic_tail_watermark = (
                    automatic_tail_watermark_by_frequency_exchange.get(
                        (frequency, exchange_code)
                    )
                )
                if automatic_tail_watermark is None:
                    existing_bar_calendar_df = empty_pandas(
                        FUTURES_BAR_CALENDAR_SCHEMA
                    )
                else:
                    existing_bar_calendar_df = (
                        existing_complete_bar_calendar_df.loc[
                            existing_complete_bar_calendar_df["trading_date"].gt(
                                automatic_tail_watermark
                            ),
                            FUTURES_BAR_CALENDAR_SCHEMA.names,
                        ]
                    )
            elif requested_start_date is None:
                existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df
                )
            else:
                existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df.loc[
                        existing_complete_bar_calendar_df["trading_date"].ge(
                            requested_start_date
                        )
                        & existing_complete_bar_calendar_df["trading_date"].le(
                            requested_end_date
                        ),
                        FUTURES_BAR_CALENDAR_SCHEMA.names,
                    ]
                )
            (
                desired_bar_calendar_df,
                current_structural_audit,
            ) = assess_partition(
                fresh_bar_calendar_df_by_frequency[frequency],
                existing_bar_calendar_df,
                None,
            )
            if not current_structural_audit["is_dirty"]:
                log_processed_partitions += 1
                log_skipped_partitions += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "
                    f"partition={partition_key}; outcome=skipped_clean; "
                    f"completed={log_processed_partitions}; total={len(dirty_partition_plans)}; "
                    f"elapsed_s={time.perf_counter() - run_started_at:.3f}"
                )
                continue
            if automatic_tail_mode:
                desired_primary_key_index = pd.MultiIndex.from_frame(
                    desired_bar_calendar_df[PRIMARY_KEY]
                )
                existing_primary_key_index = pd.MultiIndex.from_frame(
                    existing_complete_bar_calendar_df[PRIMARY_KEY]
                )
                retained_existing_bar_calendar_df = (
                    existing_complete_bar_calendar_df.loc[
                        ~existing_primary_key_index.isin(
                            desired_primary_key_index
                        ),
                        FUTURES_BAR_CALENDAR_SCHEMA.names,
                    ]
                )
                partition_commit_df = pd.concat(
                    [
                        retained_existing_bar_calendar_df,
                        desired_bar_calendar_df,
                    ],
                    ignore_index=True,
                )
            else:
                partition_commit_df = desired_bar_calendar_df
            commit_started_at = time.perf_counter()
            committed_partition_rows = commit_partition(
                partition_commit_df,
                resolved_lake_root,
                partition_key,
                requested_start_date,
                requested_end_date,
                existing_partition_frame=existing_complete_bar_calendar_df,
            )
            committed_row_count += committed_partition_rows
            committed_partition_count += 1
            commit_elapsed = time.perf_counter() - commit_started_at
            commit_elapsed_total += commit_elapsed
            log_processed_partitions += 1
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "
                f"partition={partition_key}; completed={log_processed_partitions}; "
                f"total={len(dirty_partition_plans)}; rows={committed_partition_rows}; "
                f"elapsed_s={time.perf_counter() - run_started_at:.3f}"
            )

    if has_explicit_dates:
        commit_mode = "explicit_non_formal"
    elif full:
        commit_mode = "full_history"
    else:
        commit_mode = "automatic_tail"
    click.echo(
        f"committed: table={TABLE_NAME}; phase=commit_batch; status=completed; mode={commit_mode}; "
        f"completed={log_processed_partitions}; total={len(dirty_partition_plans)}; "
        f"skipped_clean_partitions={log_skipped_partitions}; "
        f"rows={committed_row_count}; "
        f"partitions={committed_partition_count}; "
        f"commit_seconds={commit_elapsed_total:.3f}; "
        "remaining_pending=0"
    )
    click.echo(
        f"{log_boundary}\n行情日历写入运行完成 / Bar calendar write run completed\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
        f"mode={mode}; write=true; outcome=committed; rows={committed_row_count}; "
        f"partitions={committed_partition_count}; skipped_clean_partitions={log_skipped_partitions}; "
        f"elapsed_s={time.perf_counter() - run_started_at:.3f}\n{log_boundary}"
    )



# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数。当前单元格使用与 c01—c03 一致的显式日期只读示例（2026-08-01 至 2026-08-15）；执行只读取本地 c03/c04 并输出计划，不调用外部 API。脚本运行时使用命令行参数，模式与写入限制见开篇表格。在 Notebook 中导入同名 Python 模块不会触发入口。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会进入本地生成与规划流程。流程图本身不执行代码。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口单元格或运行脚本"] --> B{"Notebook 交互环境？"}
#     B -->|是| C["显式 notebook_args；不读取内核参数"]
#     C --> D["main.main：standalone_mode=False"]
#     B -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main：读取命令行参数"]
#     E -->|否| G["模块导入：不触发业务"]
#     D --> H["进入运行模式分支"]
#     F --> H
# ```

# In[9]:


# 用 Jupyter / IPython 运行时，内核进程的启动命令类似: ipykernel_launcher.py -f /path/to/connection.json
# sys.argv 里会包含: ['ipykernel_launcher.py', '-f', '/path/to/connection.json']
# parser.parse_args() 会读取 sys.argv，解析器会看到 -f 选项，但命令定义里没有 -f

# main()
# └── click.Command.__call__()
#     └── Command.main(args=None)
#         ├── args 为 None，因此读取 sys.argv[1:]
#         ├── Command.make_context(...)
#         ├── Command.parse_args(...)
#         │   └── 内部 parser.parse_args(args)
#         └── Command.invoke(...)
#             └── 调用 main(...) 函数体

if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 单元格内不写 --write
    main.main(
        args=notebook_args,
        prog_name="c04_futures_bar_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
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
#     C --> D["默认尾部更新并提交"]
# ```

# In[10]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Market_Data\a01_Collection\b01_Futures_Market_Data\c04_futures_bar_calendar.py --write

