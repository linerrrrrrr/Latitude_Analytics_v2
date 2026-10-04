#!/usr/bin/env python
# coding: utf-8

# # c06_futures_minute
# 
# 读取 c04 的 `dim_futures_bar_calendar`，从 JQData 采集一分钟行情并生成 `fact_futures_minute`；写入时先逐个提交分钟事实叶，再按日历叶集中回写采集完成、缺失和质量状态。
# 
# 日常按当前白名单评估全部历史 `1m` 格点，API 待办为 **当前需要采集且尚未完成** 的 Session。已完成的零行、部分缺失和 warning 是可信快照，日常不重拉，也不从全历史分钟事实重新证明完成。白名单缩小保留历史事实和完成证据；再次纳入的已完成格点不重复请求。
# 
# 阅读顺序：初始化与契约 → 输出校验 → Dataset 与精确叶读取 → 白名单与请求规划 → 分钟响应及 Session 对齐 → 完整叶提交 → 日历完成状态 → CLI。函数按定义顺序展开，总流程图按实际调用顺序阅读。Notebook 是唯一编辑源，同名 `.py` 由默认 PythonExporter 生成。

# ### 总流程：分钟采集与两阶段提交
# 
# 当前叶的安装或验收异常由共享事务恢复当前叶及本次新建标记，再抛出并停止后续处理；此前成功提交的事实叶或日历叶保留。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["参数检查；打开根 Dataset 并核对物理契约"] --> B["逐个 1m 叶投影窄列；形成政策变化与待办"]
#   B --> C{"有事实请求计划？"}
#   C -- 否 --> D["write 时提交政策变化；无 API"]
#   C -- 是 --> E["认证 JQData；遍历事实分区"]
#   E --> F{"剩余额度可覆盖当前分区并保留预留量？"}
#   F -- 否 --> G["停止后续请求；保留已处理结果"]
#   F -- 是 --> H["按合约日请求；Session 对齐和事实校验"]
#   H --> I{"write？"}
#   I -- 否 --> J["累计采集量；继续下一个分区"]
#   I -- 是 --> K["精确读当前事实叶；合并、校验与提交"]
#   K --> L["生成完成摘要；汇集到日历叶"]
#   J --> M{"还有事实分区？"}
#   L --> M
#   M -- 是 --> F
#   M -- 否 --> N{"write？"}
#   G --> N
#   N -- 否 --> O["输出只读结果；不回写日历"]
#   N -- 是 --> P["逐个日历叶合并政策与完成摘要并提交"]
#   D --> Q["报告运行结果与耗时"]
#   O --> Q
#   P --> Q
#   Q --> R["正常完成或 quota_stop"]
# ```

# ## 更新范围与写入边界
# 
# | 方式 | 处理范围 | 写入条件 |
# | --- | --- | --- |
# | 默认自动 | 全历史 `1m` 窄列评估，只请求当前 required 且未完成的 Session | `--write` 可提交正式湖 |
# | 成对 `--start-date/--end-date` | 只评估指定交易日期内的 Session | 正式湖仅可只读；写入必须指定非正式湖 |
# | 不带 `--write` | 仍采集待办并完成分钟转换与校验 | 不提交事实或日历状态 |
# 
# c06 不提供 `--full`。配额检查按事实分区进行，默认预留 5,000,000 条，可通过 `--quota-reserve` 调整；检查使用接口返回的剩余额度，接口未提供 `spare` 时现有逻辑不据此阻断请求。
# 
# 当前事务边界是“事实叶逐个提交，之后集中回写日历”。后续失败不会撤销此前成功事实叶；只有日历也成功提交，才持久保存对应 Session 的完成凭证。流程图用于解释，不执行采集。

# ## 项目定位与依赖
# 
# 从当前目录向上搜索 `.git`、`.env` 和 `config/settings.py`，先定位项目根，再导入配置、具名权威 Schema 和共享事实白名单。正式湖根目录使用 `settings.futures_lake_root`，唯一配置来源为 `.env` 的 `FUTURES_LAKE_ROOT`。
# 
# 初始化只建立依赖和定义，不认证 JQData、不采集、不写湖。

# ### 流程：定位项目与加载依赖
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["向上搜索三个项目标记"] --> B["加入项目与湖仓模块路径"] --> C["导入配置、Schema 和共享白名单"]
# ```

# In[1]:


from __future__ import annotations

# 标准库负责数值检查、路径管理、原子替换、时间和运行批次标识。
import math
import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timezone
from types import ModuleType
from typing import Callable


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

# 只使用项目统一规定的三个标记定位根目录。
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Market_Data/a01_Collection"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


# 第三方库分别承担 CLI、表格转换和 Arrow/Parquet 数据集读写。
import click
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# 所有字段、类型、metadata 和转换都来自中央可执行契约。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# 白名单来自根级配置；它只控制期货事实，不定义日历宇宙。
from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETIES_BY_EXCHANGE


# ## Schema 契约浏览
# 
# 交互式 Notebook 通过共享展示模块查看分钟事实及行情日历的权威 Schema，并按需查看有界只读样例；导出的命令行脚本跳过展示。字段说明来自 `config/data_contracts.py`，浏览不代替生产校验。

# ### 流程：交互式契约浏览
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A{"Notebook 交互环境？"} -- 是 --> B["展示分钟事实、行情日历契约与可选样例"]
#   A -- 否 --> C["脚本跳过展示"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、Session 键与规划常量
# 
# 表名、主键和 Hive 分区从具名 Schema metadata 各读取一次。事实按交易所—品种—年月分区；日历按频率—交易所—年月分区。两者的提交键分别使用自己的契约。
# 
# `SESSION_KEY` 用于分钟条数汇总与日历回写；`MINUTE_PLANNING_COLUMNS` 是政策和待办规划所需的窄列。JQData 字段、来源标识、质量枚举、配额预留与旧 c07 旁证列在此声明，不发起 I/O。

# ### 流程：建立表身份与规划常量
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["权威 Schema metadata"] --> B["读取表名、主键与分区"] --> C["声明 Session 键、API 字段与状态常量"] --> D["建立 Hive 分区与窄列 Schema"]
# ```

# In[3]:


TABLE_NAME = FUTURES_MINUTE_SCHEMA.metadata[b"table_name"].decode("utf-8")
CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_MINUTE_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
SESSION_KEY = [  # 汇总分钟条数和回写状态所用的 Session 业务键。
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # 分钟 bar 归属的期货交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_MINUTE_SCHEMA.metadata[b"partition_columns"].decode(
    "utf-8"
).split(",")
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

PRICE_FIELDS = [  # JQData 一分钟响应中参与行情质量检查的字段。
    "open",  # 分钟开盘价。
    "high",  # 分钟最高价。
    "low",  # 分钟最低价。
    "close",  # 分钟收盘价。
    "volume",  # 分钟成交量，单位为手。
    "money",  # 分钟成交额，单位为元。
    "open_interest",  # 分钟结束时持仓量，单位为手。
]
SOURCE_NAME = "JQData_get_price_1m_skip_paused_fq_none"
LEGACY_SOURCE_NAMES = {"JQData_get_price_1m", "JQData_get_price_1m_skip_paused"}
DEFAULT_QUOTA_RESERVE = 5_000_000

SELECTED_REASON = (
    "命中国内期货事实采集白名单；一分钟事实需要采集。"
)
EXCLUDED_REASON = (
    "未命中国内期货事实采集白名单；"
    "保留理论格点但不采集一分钟事实，既有历史分钟事实不自动删除。"
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

EVIDENCE_COLUMNS = [  # b06 产生新分钟状态时必须清空的旧 b07 校对证据。
    "daily_open",  # JQData 日线开盘价旁证。
    "daily_high",  # JQData 日线最高价旁证。
    "daily_low",  # JQData 日线最低价旁证。
    "daily_close",  # JQData 日线收盘价旁证。
    "daily_volume",  # JQData 日线成交量旁证。
    "daily_money",  # JQData 日线成交额旁证。
    "daily_open_interest",  # JQData 日线收盘持仓量旁证。
    "aggregated_open",  # 其他完整 Session 重聚合开盘价。
    "aggregated_high",  # 其他完整 Session 重聚合最高价。
    "aggregated_low",  # 其他完整 Session 重聚合最低价。
    "aggregated_close",  # 其他完整 Session 重聚合收盘价。
    "aggregated_volume",  # 其他完整 Session 重聚合成交量。
    "aggregated_money",  # 其他完整 Session 重聚合成交额。
    "aggregated_open_interest",  # 其他完整 Session 末条有效持仓量。
    "ohlc_matches_daily",  # 重聚合 OHLC 是否均匹配日线。
    "volume_matches_daily",  # 重聚合成交量是否匹配日线。
    "money_matches_daily",  # 重聚合成交额是否匹配日线。
    "open_interest_matches_daily",  # 重聚合末持仓是否匹配日线。
]

# 显式类型的 Hive 分区避免目录字符串推断改变契约。
HIVE_PARTITIONING = ds.partitioning(
    pa.schema([FUTURES_MINUTE_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in CALENDAR_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)

MINUTE_PLANNING_COLUMNS = [
    "bar_frequency",
    "contract_code",
    "exchange_code",
    "underlying_code",
    "trading_date",
    "session_number",
    "session_start_at",
    "session_end_at",
    "schedule_status",
    "is_fetch_required",
    "expected_bar_count",
    "is_fetch_completed",
    "year",
    "month",
]
MINUTE_PLANNING_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name)
    for name in MINUTE_PLANNING_COLUMNS
])

# 共享政策在本次模块加载期间固定；每个交易所的 Arrow 集合只构造一次。
SELECTED_UNDERLYINGS_BY_EXCHANGE = {
    exchange_code: pa.array(
        sorted(underlying_codes),
        type=FUTURES_BAR_CALENDAR_SCHEMA.field("underlying_code").type,
    )
    for exchange_code, underlying_codes in FUTURES_FACT_VARIETIES_BY_EXCHANGE.items()
}


# ## 待回写日历完整叶校验
# 
# `validate_calendar_state_frame()` 对本环节合并了政策与完成状态的 dirty 日历叶负责，检查 Schema、主键、结构与枚举、完成批次及时间、实际与缺失条数，以及质量说明和 c07 比较证据。
# 
# 校验器支持日历契约的两种频率，c06 主流程只回写 `1m` 叶。完整业务校验在 dirty 输出叶上执行一次；staging 和正式安装后仅检查物理契约、主键与行数。

# ### 流程：待提交日历完整叶校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["按权威 Schema 转换"] --> B["主键、枚举与日线分钟结构"] --> C["完成凭证、缺失计数与质量时间"] --> D["c07 比较证据一致性"] --> E["排序并返回完整叶；任一失败抛错"]
# ```

# In[4]:


def validate_calendar_state_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    calendar_table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(
        calendar_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if not checked_df["bar_frequency"].isin({"1d", "1m"}).all():
        raise ValueError(f"{context}bar_frequency 不在允许枚举中。")
    if not checked_df["schedule_status"].isin(SCHEDULE_STATUSES).all():
        raise ValueError(f"{context}schedule_status 不在允许枚举中。")
    if not checked_df["evidence_level"].isin(EVIDENCE_LEVELS).all():
        raise ValueError(f"{context}evidence_level 不在允许枚举中。")
    if not checked_df["quality_status"].isin(QUALITY_STATUSES).all():
        raise ValueError(f"{context}quality_status 不在允许枚举中。")

    text_fields = [
        "schedule_signal_reason",
        "evidence_source",
        "selection_reason",
        "quality_reason",
    ]
    empty_text = pd.Series(False, index=checked_df.index)
    for column in text_fields:
        text = checked_df[column].astype("string")
        empty_text |= text.isna() | text.str.strip().eq("")
    if empty_text.any():
        raise ValueError(f"{context}状态说明字段不得为空。")

    contract_exchange = checked_df["contract_code"].astype("string").str.rsplit(
        ".",
        n=1,
    ).str[-1]
    if not contract_exchange.eq(checked_df["exchange_code"]).all():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    trading_date = pd.to_datetime(checked_df["trading_date"])
    if (
        ~checked_df["year"].eq(trading_date.dt.year)
        | ~checked_df["month"].eq(trading_date.dt.month)
    ).any():
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")
    if (
        checked_df["actual_bar_count"].lt(0)
        | checked_df["missing_bar_count"].lt(0)
    ).any():
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    daily_mask = checked_df["bar_frequency"].eq("1d")
    minute_mask = ~daily_mask
    session_columns = [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
    ]
    if (
        ~checked_df.loc[daily_mask, "session_number"].eq(0)
    ).any() or checked_df.loc[daily_mask, session_columns].notna().any(axis=None):
        raise ValueError(f"{context}日线 Session 结构不合法。")
    if (~checked_df.loc[daily_mask, "expected_bar_count"].eq(1)).any():
        raise ValueError(f"{context}日线理论条数必须为 1。")
    if checked_df.loc[minute_mask, "session_number"].le(0).any():
        raise ValueError(f"{context}分钟 session_number 必须大于 0。")
    if (
        checked_df.loc[minute_mask, "session_start_at"]
        >= checked_df.loc[minute_mask, "session_end_at"]
    ).any():
        raise ValueError(f"{context}分钟 Session 起点必须早于终点。")

    confirmed_mask = checked_df["schedule_status"].eq("confirmed_closed")
    if (
        ~checked_df.loc[confirmed_mask, "evidence_level"].eq("authoritative")
    ).any() or checked_df.loc[confirmed_mask, "is_fetch_required"].any():
        raise ValueError(f"{context}确认休市状态缺少权威证据或仍要求拉取。")

    completed_mask = checked_df["is_fetch_completed"].eq(True)
    if (
        checked_df.loc[completed_mask, "fetch_run_id"].isna()
        | checked_df.loc[completed_mask, "fetch_run_id"]
        .astype("string")
        .str.strip()
        .eq("")
        | checked_df.loc[completed_mask, "fetch_completed_at"].isna()
    ).any():
        raise ValueError(f"{context}完成状态缺少批次或完成时间。")
    if checked_df.loc[~completed_mask, "fetch_completed_at"].notna().any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    checked_missing_mask = checked_df["missing_checked_at"].notna()
    if (
        checked_df.loc[~checked_missing_mask, "is_data_missing"].any()
        or checked_df.loc[~checked_missing_mask, "missing_bar_count"].ne(0).any()
    ):
        raise ValueError(f"{context}未经检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0)
    expected_missing = expected_missing.where(
        checked_df["is_fetch_required"],
        0,
    )
    if (
        checked_df.loc[checked_missing_mask, "missing_bar_count"]
        .ne(expected_missing.loc[checked_missing_mask])
        .any()
        or checked_df.loc[checked_missing_mask, "is_data_missing"]
        .ne(expected_missing.loc[checked_missing_mask].gt(0))
        .any()
    ):
        raise ValueError(f"{context}缺失状态无法由当前格点复算。")

    non_pending_mask = ~checked_df["quality_status"].eq("pending")
    if checked_df.loc[non_pending_mask, "quality_checked_at"].isna().any():
        raise ValueError(f"{context}非 pending 状态缺少质检时间。")

    comparison_columns = [
        "ohlc_matches_daily",
        "volume_matches_daily",
        "money_matches_daily",
        "open_interest_matches_daily",
    ]
    comparison_not_null = checked_df[comparison_columns].notna()
    if (comparison_not_null.any(axis=1) != comparison_not_null.all(axis=1)).any():
        raise ValueError(f"{context}b07 四项比较必须同时为空或同时非空。")
    reconciled_mask = checked_df["evidence_level"].eq("reconciled")
    if (
        ~comparison_not_null.loc[reconciled_mask].all(axis=1)
        | ~checked_df.loc[reconciled_mask, comparison_columns].eq(True).all(axis=1)
    ).any():
        raise ValueError(f"{context}reconciled 必须具有四项匹配证据。")

    return checked_df.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


# ## 分钟事实校验与 Session 边界
# 
# `invalid_ohlc_mask()` 识别有限价格之间的 high/low 关系异常；这类来源值保留，日历随后记 warning。`validate_minute_frame()` 在 Arrow 转换前拒绝非空 NaN/Inf，再检查 Schema、主键、合约身份、日期分区、来源和非负数量；真正的 `None`、`pd.NA` 按字段 nullable 契约处理。
# 
# 提供 Session 时，继续检查归属、业务属性、时间范围 `(session_start_at, session_end_at]`、整分钟格点和理论条数上界。来源转换结果与合并后的 dirty 完整事实叶分别承担自身校验，正式历史不参与全表业务复查。
# 
# 空分钟结果在函数前段已返回；提供 Session 时保留关联的 `many_to_one` 约束，移除关联前对同一键的重复检查。待替换旧分钟来自可信正式叶，待办 Session 来自 c04 唯一格点的筛选，因此 `rows_for_sessions()` 只选取所需键，不再重复证明上游主键或排序；合并后的 dirty 完整叶仍执行完整业务校验。

# ### 流程：分钟质量与可选 Session 校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["来源非空值有限性；转换 Arrow"] --> B["主键、合约身份、来源、年月与非负数量"]
#   B --> C{"提供 Session 且有分钟行？"}
#   C -- 是 --> D["匹配 Session；检查属性与右闭左开范围"]
#   D --> E["理论整分钟格点与条数上界"] --> F["返回排序后的校验结果"]
#   C -- 否 --> F
#   G["invalid_ohlc_mask：有限价格关系比较"] --> H["异常掩码；保留原值供 warning 使用"]
# ```

# In[5]:


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    # 空表也返回同索引布尔序列，便于调用方直接筛选和计数。
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)

    # 只比较实际存在的有限价格；NaN/Inf 仍由事实 validator 单独拒绝。
    comparable_max = frame[["open", "close", "low"]].max(
        axis=1,
        skipna=True,
    )
    comparable_min = frame[["open", "close", "high"]].min(
        axis=1,
        skipna=True,
    )
    invalid_mask = (
        frame["high"].notna()
        & comparable_max.notna()
        & frame["high"].lt(comparable_max)
    ) | (
        frame["low"].notna()
        & comparable_min.notna()
        & frame["low"].gt(comparable_min)
    )
    return invalid_mask.fillna(False).astype(bool)


def validate_minute_frame(
    frame: pd.DataFrame,
    context: str,
    sessions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    # 必须在 Arrow 转换前检查来源数值；否则 Pandas NaN 会被 Arrow 静默转成 null。
    # 真正的 Python None / pd.NA 仍按 Schema 的 nullable 语义处理，NaN/Inf 则明确拒绝。
    source_frame = frame.loc[:, FUTURES_MINUTE_SCHEMA.names]
    for column in PRICE_FIELDS:
        source_series = source_frame[column]
        if source_series.dtype == object:
            source_values = source_series.to_numpy(dtype=object, copy=False)
            explicit_null_mask = np.fromiter(
                (value is None or value is pd.NA for value in source_values),
                dtype=bool,
                count=len(source_values),
            )
            source_array = pa.array(
                source_values,
                mask=explicit_null_mask,
                type=pa.float64(),
                from_pandas=False,
            )
        else:
            source_array = pa.array(
                source_series,
                type=pa.float64(),
                from_pandas=False,
            )
        populated_values = source_array.drop_null().to_numpy(
            zero_copy_only=False
        )
        if (~np.isfinite(populated_values)).any():
            raise ValueError(f"{context}{column} 包含非有限数。")

    # Arrow 转换固定列顺序、类型、可空性和 metadata。
    minute_table = pandas_to_arrow(source_frame, FUTURES_MINUTE_SCHEMA)
    checked_df = arrow_to_pandas(minute_table, FUTURES_MINUTE_SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    contract_parts = checked_df["contract_code"].astype("string").str.split(
        ".",
        n=1,
        expand=True,
    )
    if not contract_parts[1].eq(checked_df["exchange_code"]).all():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    expected_underlying = contract_parts[0].str.extract(
        r"^([A-Za-z]+)",
        expand=False,
    ).str.upper()
    if not expected_underlying.eq(checked_df["underlying_code"]).all():
        raise ValueError(f"{context}品种代码无法由合约代码复算。")
    trading_date = pd.to_datetime(checked_df["trading_date"])
    if (
        ~checked_df["year"].eq(trading_date.dt.year)
        | ~checked_df["month"].eq(trading_date.dt.month)
    ).any():
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["session_number"].le(0).any():
        raise ValueError(f"{context}session_number 必须大于 0。")
    if not checked_df["source"].isin({SOURCE_NAME, *LEGACY_SOURCE_NAMES}).all():
        raise ValueError(f"{context}source 与 JQData 分钟契约不一致。")
    if checked_df[["volume", "money", "open_interest"]].lt(0).any(axis=None):
        raise ValueError(f"{context}数量和金额不得为负。")

    if sessions_df is not None:
        session_columns = [
            *SESSION_KEY,
            "exchange_code",
            "underlying_code",
            "session_start_at",
            "session_end_at",
            "expected_bar_count",
        ]
        session_boundaries_df = sessions_df.loc[:, session_columns].copy()
        matched_df = checked_df.merge(
            session_boundaries_df,
            on=SESSION_KEY,
            how="left",
            suffixes=("", "_session"),
            validate="many_to_one",
            indicator=True,
        )
        if matched_df["_merge"].ne("both").any():
            raise ValueError(f"{context}分钟行不属于待办 Session。")
        if (
            ~matched_df["exchange_code"].eq(matched_df["exchange_code_session"])
            | ~matched_df["underlying_code"].eq(
                matched_df["underlying_code_session"]
            )
        ).any():
            raise ValueError(f"{context}分钟行未继承 Session 业务属性。")
        if (
            ~matched_df["bar_at"].gt(matched_df["session_start_at"])
            | ~matched_df["bar_at"].le(matched_df["session_end_at"])
        ).any():
            raise ValueError(f"{context}bar_at 越出 (Session 开始,结束]。")
        elapsed_seconds = (
            matched_df["bar_at"] - matched_df["session_start_at"]
        ).dt.total_seconds().to_numpy(dtype="float64")
        minute_number = elapsed_seconds / 60
        expected_bar_count = matched_df["expected_bar_count"].to_numpy(
            dtype="int64"
        )
        if (
            (np.remainder(elapsed_seconds, 60) != 0)
            | (minute_number > expected_bar_count)
        ).any():
            raise ValueError(
                f"{context}bar_at 不是 Session 内的理论分钟格点。"
            )
        actual_counts = matched_df.groupby(
            SESSION_KEY, dropna=False
        ).size()
        expected_counts = session_boundaries_df.set_index(
            SESSION_KEY
        )["expected_bar_count"]
        if actual_counts.gt(expected_counts.reindex(actual_counts.index)).any():
            raise ValueError(
                f"{context}Session 实际分钟条数超过理论条数。"
            )

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# ### 流程：选取待替换 Session 的旧分钟
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A{"分钟或 Session 为空？"} -- 是 --> B["返回同结构空表"]
#   A -- 否 --> C["按 Session 键关联时间边界"] --> D["选取右闭左开范围内分钟"] --> E["返回待替换旧分钟"]
# ```

# In[6]:


def rows_for_sessions(
    frame: pd.DataFrame,
    sessions_df: pd.DataFrame,
) -> pd.DataFrame:
    # Session 为空时返回保留事实 Schema 的空表。
    if sessions_df.empty or frame.empty:
        return frame.iloc[0:0].copy()

    session_boundaries_df = sessions_df.loc[
        :,
        [*SESSION_KEY, "session_start_at", "session_end_at"],
    ]
    selected_df = frame.merge(
        session_boundaries_df,
        on=SESSION_KEY,
        how="inner",
    )
    selected_df = selected_df.loc[
        selected_df["bar_at"].gt(selected_df["session_start_at"])
        & selected_df["bar_at"].le(selected_df["session_end_at"]),
        FUTURES_MINUTE_SCHEMA.names,
    ].reset_index(drop=True)
    return selected_df


# ## Dataset 物理契约与精确叶读取
# 
# `reconstructed_schema()` 恢复 Dataset 的逻辑字段顺序；两个兼容性函数分别核对物理字段、类型、nullable 和表名、主键、分区身份 metadata。描述性 metadata 以当前权威契约为准，差异不触发历史重写。
# 
# `open_contract_dataset()` 在入口分别打开上游日历与现有分钟事实，检查 Schema 和 fragment 物理契约。规划只投影日历窄列，不读取历史事实行。分区发现从文件路径解析 Hive 键；提交时由 `read_complete_partition()` 定位当前完整叶，复原目录中的分区字段并转换为 Pandas，不重开事实表根。
# 
# `open_contract_dataset()`、`partition_keys_from_dataset()` 与 `read_complete_partition()` 分别报告根打开、分区发现与当前叶读取的起止和失败阶段；文件进度沿原遍历在首个及每 250 个报告，不额外列目录或扫描数据。缺叶、读到零行和异常分别表达。
# 
# 叶文件物理类型通过检查后，补回分区字段时直接复用非分区列，不再逐列做同类型 cast；最终仍使用统一 `arrow_to_pandas()` 完成契约转换。

# ### 流程：恢复 Dataset 逻辑 Schema
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["Dataset Schema 与分区 Schema"] --> B["按权威字段顺序取实际字段"] --> C["组成含 Dataset metadata 的逻辑 Schema"]
# ```

# In[7]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


# ### 流程：物理契约与表身份比较
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["实际与期望 Schema"] --> B["字段顺序、类型、nullable"]
#   A --> C["表名、主键、分区 metadata"]
#   B --> D["分别返回是否兼容；不比较描述性文字"]
#   C --> D
# ```

# In[8]:


def physically_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    if actual_schema.names != expected_schema.names:
        return False

    return all(
        actual_field.type == expected_field.type
        and actual_field.nullable == expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    )


def schema_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    identity_keys = (b"table_name", b"primary_key", b"partition_columns")
    return all(
        expected_metadata.get(key) is not None
        and actual_metadata.get(key) == expected_metadata[key]
        for key in identity_keys
    )


# ### 流程：打开根 Dataset 的物理门禁
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A{"目录存在 Parquet？"} -- 否 --> B{"上游必需？"}
#   B -- 是 --> C["缺失报错"]
#   B -- 否 --> D["返回 None"]
#   A -- 是 --> E["记录开始；打开 Dataset 并恢复 Schema"] --> F["核对契约；沿原遍历报告文件进度"] --> G["报告完成与文件数；返回 Dataset"]
# ```

# In[9]:


def open_contract_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
    *,
    required: bool,
) -> ds.Dataset | None:
    log_started_at = time.perf_counter()
    log_phase = "discovery"

    click.echo(
        f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=started; "
        f"path={table_path}; required={str(required).lower()}"
    )
    try:
        # 目录存在但没有 Parquet 文件，与下游表尚未创建具有相同含义。
        first_parquet_file = (
            next(table_path.rglob("*.parquet"), None)
            if table_path.is_dir()
            else None
        )
        if first_parquet_file is None:
            if required:
                raise FileNotFoundError(f"{label}不存在：{table_path}")
            click.echo(
                f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=completed; "
                f"outcome=absent_optional; checked_fragments=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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

        if not physically_compatible(actual_schema, schema):
            raise TypeError(f"{label}物理字段、类型或 nullable 与契约不一致。")
        if not schema_identity_compatible(actual_schema, schema):
            raise TypeError(f"{label}表名、主键或分区 metadata 与契约不一致。")

        expected_file_schema = parquet_file_schema(
            schema,
            partitioning.schema.names,
        )
        log_phase = "fragment_schema"
        log_fragment_count = 0
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=started; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for fragment in dataset.get_fragments():
            fragment_schema = fragment.physical_schema
            if not physically_compatible(fragment_schema, expected_file_schema):
                raise TypeError(
                    f"{label} Parquet fragment 物理字段、类型或 nullable 不兼容："
                    f"{fragment.path}"
                )
            if not schema_identity_compatible(fragment_schema, expected_file_schema):
                raise TypeError(
                    f"{label} Parquet fragment 表名、主键或分区 metadata 不兼容："
                    f"{fragment.path}"
                )
            log_fragment_count += 1
            if log_fragment_count == 1 or log_fragment_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=running; "
                    f"checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        # 描述性 metadata 只影响当前代码中的权威说明，不触发历史 Parquet 重写。
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=completed; "
            f"outcome=ready; checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 流程：分区发现与当前完整叶读取
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["根 Dataset 文件路径"] --> B["跳过零行标记；解析 Hive 目录与类型"] --> C["报告文件与分区数；返回键集合"]
#   D["表根、分区列与当前键"] --> E["记录读取开始；构造精确叶路径"] --> F{"存在叶文件？"}
#   F -- 否 --> G["记录缺叶；返回契约空表"]
#   F -- 是 --> H["检查各文件物理契约；读取当前叶"] --> I["补分区字段并转换；报告行数与耗时"]
# ```

# In[10]:


def cast_partition_value(column: str, raw_value: str) -> object:
    # year/month 在 Arrow 契约中是整数，其余分区字段保留字符串。
    if column in {"year", "month"}:
        return int(raw_value)
    return raw_value


def partition_keys_from_dataset(
    dataset: ds.Dataset,
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    log_started_at = time.perf_counter()
    log_phase = "partition_discovery"
    log_file_count = 0
    click.echo(
        f"planning_progress: table={table_path.name}; function=partition_keys_from_dataset; phase=partition_discovery; status=started; "
        f"path={table_path}"
    )
    try:
        keys = set()
        resolved_table_path = table_path.resolve()
        for file_name in dataset.files:
            parquet_path = pathlib.Path(file_name).resolve()
            if parquet_path.name == "schema.parquet":
                continue
            relative_parts = parquet_path.relative_to(resolved_table_path).parts
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
                values.append(
                    cast_partition_value(column, directory_name[len(prefix):])
                )
            keys.add(tuple(values))
            log_file_count += 1
            if log_file_count == 1 or log_file_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={table_path.name}; function=partition_keys_from_dataset; phase=partition_discovery; status=running; "
                    f"files={log_file_count}; partitions={len(keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={table_path.name}; function=partition_keys_from_dataset; phase=partition_discovery; status=completed; "
            f"files={log_file_count}; partitions={len(keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return keys
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=partition_keys_from_dataset; phase=partition_discovery; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def exact_partition_path(
    table_path: pathlib.Path,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return table_path.joinpath(
        *[
            f"{column}={value}"
            for column, value in zip(
                partition_columns,
                partition_key,
                strict=True,
            )
        ]
    )


def read_complete_partition(
    table_path: pathlib.Path,
    schema: pa.Schema,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "locate_leaf"

    click.echo(
        f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=leaf_read; status=started; "
        f"partition={partition_key}; path={table_path}"
    )
    try:
        partition_path = exact_partition_path(
            table_path,
            partition_columns,
            partition_key,
        )
        parquet_files = list(partition_path.rglob("*.parquet"))
        if not parquet_files:
            read_partition_df = empty_pandas(schema)
            click.echo(
                f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=leaf_read; status=completed; "
                f"partition={partition_key}; outcome=absent_leaf; rows=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return read_partition_df

        log_phase = "file_schema"
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=file_schema; status=started; "
            f"partition={partition_key}; files={len(parquet_files)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        expected_file_schema = parquet_file_schema(schema, partition_columns)
        for parquet_path in parquet_files:
            actual_file_schema = pq.read_schema(parquet_path)
            if not physically_compatible(actual_file_schema, expected_file_schema):
                raise TypeError(f"叶分区物理 Schema 不兼容：{parquet_path}")
            if not schema_identity_compatible(actual_file_schema, expected_file_schema):
                raise TypeError(f"叶分区表身份 metadata 不兼容：{parquet_path}")

        log_phase = "leaf_scan"
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=leaf_scan; status=started; "
            f"partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        file_table = ds.dataset(partition_path, format="parquet").to_table(
            columns=expected_file_schema.names,
        )
        partition_value_by_column = dict(
            zip(partition_columns, partition_key, strict=True)
        )
        logical_arrays = []
        for field in schema:
            if field.name in partition_value_by_column:
                logical_arrays.append(
                    pa.chunked_array(
                        [
                            pa.array(
                                [partition_value_by_column[field.name]] * len(file_table),
                                type=field.type,
                            )
                        ],
                        type=field.type,
                    )
                )
            else:
                logical_arrays.append(file_table[field.name])
        log_phase = "conversion"
        logical_table = pa.Table.from_arrays(logical_arrays, schema=schema)
        read_partition_df = arrow_to_pandas(logical_table, schema)
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=leaf_read; status=completed; "
            f"partition={partition_key}; outcome=read; rows={len(read_partition_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return read_partition_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_complete_partition; phase=leaf_read; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 白名单窄列规划与选择状态
# 
# `minute_policy_plan()` 按交易所取模块初始化时从共享配置生成的 Arrow 白名单集合，并排除 `confirmed_closed`，形成期望 required。政策 dirty 取 required 发生变化的格点；pending 取期望 required 且 `is_fetch_completed=false`，completed 取期望 required 且已有完成凭证。
# 
# 日常逐个读取日历 `1m` 分区的规划窄列，再按分钟事实分区组织待办。政策变更只定向回写对应日历完整叶；白名单缩小时保留事实、完成批次、质量和 c07 旁证，只停止未来采集并清零当前缺失。再次纳入的已完成格点不发起 API。
# 
# `minute_policy_plan()` 自行报告当前交易所和规划行数，明确完成依据为 `is_fetch_completed`。入口保留各分区的 dirty、pending、completed 累计，不为日志重复计算这些掩码。
# 
# 入口读取窄表后按 `MINUTE_PLANNING_SCHEMA` 转换一次，规划函数消费这一已转换结果；不再对同一表重复 cast，也不在每个日历分区重建同一交易所的白名单数组。

# ### 流程：白名单与可信完成凭证形成待办
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["已转换窄表；复用交易所白名单数组"] --> B["命中白名单且未确认休市：desired required"]
#   B --> C["与原 required 比较：政策 dirty"]
#   B --> D["尚未完成：pending"]
#   B --> E["已有凭证：completed；报告规划完成"]
# ```

# In[11]:


def minute_policy_plan(
    planning_table: pa.Table,
    exchange_code: str,
) -> tuple[pa.Array, pa.Array, pa.Array, pa.Array]:
    """用 Arrow compute 形成 required、dirty、pending、completed 掩码。"""
    log_started_at = time.perf_counter()
    log_phase = "whitelist"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=minute_policy_plan; phase=policy_plan; status=started; "
        f"exchange={exchange_code}; rows={len(planning_table)}"
    )
    try:
        selected_underlyings = SELECTED_UNDERLYINGS_BY_EXCHANGE.get(exchange_code)
        if selected_underlyings is not None:
            desired_required = pc.is_in(
                planning_table["underlying_code"],
                value_set=selected_underlyings,
            )
        else:
            desired_required = pa.array(
                np.zeros(len(planning_table), dtype=bool)
            )
        desired_required = pc.and_(
            desired_required,
            pc.invert(
                pc.equal(
                    planning_table["schedule_status"], "confirmed_closed"
                )
            ),
        )
        log_phase = "completion_evidence"
        completed_mask = pc.and_(
            desired_required, planning_table["is_fetch_completed"]
        )
        pending_mask = pc.and_(
            desired_required,
            pc.invert(planning_table["is_fetch_completed"]),
        )
        policy_changed_mask = pc.not_equal(
            planning_table["is_fetch_required"], desired_required
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=minute_policy_plan; phase=policy_plan; status=completed; "
            f"exchange={exchange_code}; rows={len(planning_table)}; completion_source=is_fetch_completed; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return (
            desired_required,
            policy_changed_mask,
            pending_mask,
            completed_mask,
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=minute_policy_plan; phase=policy_plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 配额门禁与合约日请求分组
# 
# `quota_spare()` 只使用 JQData 返回的 `spare`，不把总额度当作当前可用量。入口在每个事实分区的第一项行情请求前，以全部待办 Session 的理论分钟数检查剩余额度和 `--quota-reserve`；不足时停止后续分区，不请求当前分区。
# 
# `request_batches()` 按合约—交易日分组并按 Session 编号排序。`collect_partition()` 每组调用一次 `get_price(frequency="1m")`，时间覆盖该合约日待办 Session 的最早起点至最晚终点；不自动重试。
# 
# `quota_spare()` 报告额度读取及不可用结果；`request_batches()` 报告组批数量和进度。入口仍负责“当前分区是否可以开始”的配额决定，函数日志不增加 API 请求或重试。

# ### 流程：额度读取与合约日组批
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["quota_spare：请求剩余额度"] --> B["报告 spare 或不可用；入口检查预留量"]
#   C["request_batches：待办 Session"] --> D["按合约和交易日分组"] --> E["按 Session 排序；报告组批进度与完成"]
# ```

# In[12]:


def quota_spare(jqdata: ModuleType) -> int | None:
    log_started_at = time.perf_counter()
    log_phase = "quota_read"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_read; status=started; "
        f"source=get_query_count"
    )
    try:
        get_query_count = getattr(jqdata, "get_query_count", None)
        log_phase = "quota_api"
        if get_query_count is None:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_read; status=completed; "
                f"outcome=unavailable; spare=unknown; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return None

        # total 不是当前可消费量，只使用 spare。
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_api; status=started; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        quota = get_query_count()
        if isinstance(quota, dict) and quota.get("spare") is not None:
            spare = int(quota["spare"])
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_read; status=completed; "
                f"outcome=available; spare={spare}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return spare
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_read; status=completed; "
            f"outcome=unavailable; spare=unknown; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return None
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=quota_spare; phase=quota_read; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def request_batches(sessions_df: pd.DataFrame) -> list[pd.DataFrame]:
    log_started_at = time.perf_counter()
    log_phase = "request_plan"
    log_group_count = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=started; "
        f"sessions={len(sessions_df)}"
    )
    try:
        batches = []
        for _, batch_df in sessions_df.groupby(
            ["contract_code", "trading_date"],
            sort=True,
        ):
            batches.append(
                batch_df.sort_values("session_number").reset_index(drop=True)
            )
            log_group_count += 1
            if log_group_count == 1 or log_group_count % 100 == 0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=running; "
                    f"batches={log_group_count}; sessions={len(sessions_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=completed; "
            f"sessions={len(sessions_df)}; batches={len(batches)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return batches
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源响应归一化与分钟生成
# 
# `normalize_minute_response()` 将 JQData 的时间索引或时间列统一为 `bar_at`，处理单合约代码、北京时间和数值类型，拒绝未请求合约、重复分钟主键及非空非有限数。空 DataFrame 保留为空结果；API 返回 `None` 则报错。
# 
# `collect_partition()` 将每条分钟定位到待办 Session，继承交易日、Session 编号和分区身份。时间区间右闭左开；时间单位必须无损对齐，休盘间隔、Session 起点或其他未匹配的时间戳都会硬失败，不静默过滤。转换后的完整批次再检查理论分钟格点与数量边界。
# 
# 有限 OHLC 关系异常按原值保留，记录异常分钟与各 Session 异常计数；这些计数随后用于日历 warning，不直接改变开市证据。
# 
# `collect_partition()` 自行报告组批、逐请求、归一化、Session 对齐、来源质量与输出校验。接口返回非 None 仅记录请求完成；`api_success` 在整个分区的转换结果通过校验后才输出。空响应也有明确的请求进度；失败记录当前阶段并原样抛出。
# 
# 时间索引恢复成功后必有 time 列，移除其后的不可达补救分支。Session 起止与归一化后的 bar_at 已是时间类型，构造 DatetimeIndex 时不再先做重复解析；无损单位对齐保留。固定列清单在请求循环前准备，reset_index 后不再追加复制。来源非有限数检查、输出分钟/Session 校验均保留。

# ### 流程：分钟响应归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["要求 DataFrame"] --> B{"空响应？"}
#   B -- 是 --> C["返回规定列的空表"]
#   B -- 否 --> D["统一时间、合约与字段"] --> E["核对合约；归一北京时间"] --> F["拒绝非有限值；严格转换数值"] --> G["检查重复分钟主键；返回长表"]
# ```

# In[13]:


def normalize_minute_response(
    raw_df: pd.DataFrame,
    contract_code: str,
) -> pd.DataFrame:
    columns = ["contract_code", "bar_at", *PRICE_FIELDS]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: get_price 未返回 DataFrame。")
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized_df = raw_df.copy()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename_axis("time").reset_index()

    if "code" not in normalized_df.columns:
        normalized_df["code"] = contract_code

    required_columns = {"time", "code", *PRICE_FIELDS}
    missing_columns = required_columns - set(normalized_df.columns)
    if missing_columns:
        raise ValueError(
            f"schema_error: get_price 缺列 {sorted(missing_columns)}"
        )

    normalized_df = normalized_df.loc[
        :, ["code", "time", *PRICE_FIELDS]
    ].rename(columns={"code": "contract_code", "time": "bar_at"})
    normalized_df["contract_code"] = normalized_df["contract_code"].astype(str)
    if set(normalized_df["contract_code"]) - {contract_code}:
        raise ValueError("schema_error: get_price 返回未请求合约。")

    bar_at = pd.to_datetime(normalized_df["bar_at"], errors="raise")
    if bar_at.dt.tz is None:
        bar_at = bar_at.dt.tz_localize("Asia/Shanghai")
    else:
        bar_at = bar_at.dt.tz_convert("Asia/Shanghai")
    normalized_df["bar_at"] = bar_at

    # 在数值转换前区分真正的 None/pd.NA 与供应商返回的 NaN/Inf。
    # 前者转换为 Pandas nullable 浮点，后者是来源异常，必须立即失败。
    for column in PRICE_FIELDS:
        for value in normalized_df[column]:
            if value is None or value is pd.NA:
                continue
            try:
                is_finite = math.isfinite(float(value))
            except (TypeError, ValueError):
                # 非数值文本由下面 errors=raise 的转换给出明确契约错误。
                continue
            if not is_finite:
                raise ValueError(
                    f"schema_error: get_price {column} 含非有限数。"
                )
        normalized_df[column] = pd.to_numeric(
            normalized_df[column],
            errors="raise",
        ).astype("Float64")

    normalized_df = normalized_df.loc[:, columns]
    if normalized_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("schema_error: get_price 返回重复分钟主键。")
    return normalized_df


# ### 流程：单个事实分区的分钟采集
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录采集开始；待办按合约日组批"] --> B["记录请求进度；调用 get_price 1m"]
#   B --> C["None 抛错；报告 API 返回与归一化结果"] --> D{"响应有行？"}
#   D -- 否 --> H{"还有批次？"}
#   D -- 是 --> E["无损对齐时间单位；定位 Session，越界抛错"] --> F["继承业务属性；保留并记录 OHLC 异常"] --> G["积累分钟行与异常计数"] --> H
#   H -- 是 --> B
#   H -- 否 --> I["合并采集结果；验证分钟及 Session"] --> J["通过校验后记录 api_success；返回结果"]
# ```

# In[14]:


def collect_partition(
    jqdata: ModuleType,
    sessions_df: pd.DataFrame,
    updated_at: datetime,
) -> tuple[pd.DataFrame, int, dict[tuple[object, ...], int]]:
    log_started_at = time.perf_counter()
    log_phase = "request_plan"
    log_completed_batches = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=collect; status=started; "
        f"sessions={len(sessions_df)}; persisted=false"
    )
    try:
        collected_frames = []
        returned_rows = 0
        invalid_session_counts = {}

        fact_columns = FUTURES_MINUTE_SCHEMA.names
        inherited_columns = [
            "exchange_code",
            "underlying_code",
            "trading_date",
            "session_number",
            "year",
            "month",
        ]
        batches = request_batches(sessions_df)
        log_phase = "request_batches"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=request_plan; status=completed; "
            f"sessions={len(sessions_df)}; batches={len(batches)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for batch_number, batch_df in enumerate(batches, start=1):
            contract_code = str(batch_df["contract_code"].iloc[0])
            request_start = pd.Timestamp(batch_df["session_start_at"].min())
            request_end = pd.Timestamp(batch_df["session_end_at"].max())

            click.echo(
                "request_batch: "
                f"batch={batch_number}/{len(batches)}; "
                f"contract={contract_code}; "
                f"trading_date={batch_df['trading_date'].iloc[0]}; "
                f"sessions={len(batch_df)}; "
                f"start={request_start}; end={request_end}; "
                f"table={TABLE_NAME}; function=collect_partition; phase=api_request; status=started"
            )

            # JQData 接受无时区本地时间；业务时区含义仍由 Session 契约保存。
            log_phase = "get_price"
            log_api_started_at = time.perf_counter()
            raw_df = jqdata.get_price(
                contract_code,
                start_date=request_start.tz_localize(None),
                end_date=request_end.tz_localize(None),
                frequency="1m",
                fields=PRICE_FIELDS,
                skip_paused=True,
                fq=None,
                panel=False,
            )
            if raw_df is None:
                raise RuntimeError(
                    f"retryable_error: get_price 返回 None：{contract_code}"
                )

            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=api_request; status=completed; "
                f"batch={batch_number}/{len(batches)}; api=get_price; request_elapsed_s={time.perf_counter() - log_api_started_at:.3f}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_phase = "normalize_response"
            normalized_df = normalize_minute_response(raw_df, contract_code)
            returned_rows += len(normalized_df)

            log_completed_batches += 1
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=normalize_response; status=completed; "
                f"batch={batch_number}/{len(batches)}; rows={len(normalized_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if normalized_df.empty:
                continue
            log_phase = "session_alignment"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=session_alignment; status=started; "
                f"batch={batch_number}/{len(batches)}; rows={len(normalized_df)}; sessions={len(batch_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            session_intervals = pd.IntervalIndex.from_arrays(
                pd.DatetimeIndex(batch_df["session_start_at"]),
                pd.DatetimeIndex(batch_df["session_end_at"]),
                closed="right",
            )
            # Pandas 3.0 要求 IntervalIndex 与目标时间戳的单位完全一致。
            # 严格转换到 Session 单位，禁止把无法无损表示的亚微秒值静默截断。
            bar_at_index = pd.DatetimeIndex(
                normalized_df["bar_at"]
            ).as_unit(
                session_intervals.dtype.subtype.unit,
                round_ok=False,
            )
            matched_session_positions = session_intervals.get_indexer(
                bar_at_index
            )
            if (matched_session_positions < 0).any():
                unexpected_rows = normalized_df.loc[
                    matched_session_positions < 0,
                    ["contract_code", "bar_at"],
                ]
                raise ValueError(
                    "schema_error: get_price 返回越出请求 Session 边界的分钟："
                    f"{unexpected_rows.head(5).to_dict(orient='records')}"
                )

            matched_sessions_df = batch_df.iloc[
                matched_session_positions
            ].reset_index(drop=True)
            selected_df = normalized_df.reset_index(drop=True)
            for column in inherited_columns:
                selected_df[column] = matched_sessions_df[column].array
            selected_df["source"] = SOURCE_NAME
            selected_df["updated_at"] = updated_at

            log_phase = "source_quality"
            invalid_ohlc = invalid_ohlc_mask(selected_df)
            if invalid_ohlc.any():
                invalid_df = selected_df.loc[invalid_ohlc]
                grouped_invalid_counts = invalid_df.groupby(
                    SESSION_KEY,
                    dropna=False,
                ).size()
                for session_key, invalid_count in grouped_invalid_counts.items():
                    invalid_session_counts[tuple(session_key)] = (
                        invalid_session_counts.get(tuple(session_key), 0)
                        + int(invalid_count)
                    )
                for invalid_row in invalid_df.itertuples(index=False):
                    click.echo(
                        "preserved_invalid_ohlc_bar: "
                        f"contract={invalid_row.contract_code}; "
                        f"bar_at={invalid_row.bar_at}; "
                        f"ohlc={(invalid_row.open, invalid_row.high, invalid_row.low, invalid_row.close)}; "
                        f"table={TABLE_NAME}; function=collect_partition; phase=source_quality; status=warning"
                    )
            collected_frames.append(
                selected_df.loc[:, fact_columns]
            )

        log_phase = "validate_output"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=validate_output; status=started; "
            f"completed_batches={log_completed_batches}; returned_rows={returned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        collected_df = (
            pd.concat(collected_frames, ignore_index=True)
            if collected_frames
            else empty_pandas(FUTURES_MINUTE_SCHEMA)
        )
        validated_minute_df = validate_minute_frame(
            collected_df, "JQData 分钟转换结果", sessions_df,
        )
        click.echo(
            f"api_success: table={TABLE_NAME}; function=collect_partition; phase=collect; status=completed; "
            f"sessions={len(sessions_df)}; batches={len(batches)}; fact_rows={len(validated_minute_df)}; returned_rows={returned_rows}; invalid_ohlc_rows={sum(invalid_session_counts.values())}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_minute_df, returned_rows, invalid_session_counts
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_partition; phase=collect; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 单叶暂存、共享安装与失败恢复
# 
# `commit_complete_partition()` 接收事实或日历的完整目标叶，执行一次完整业务校验并转换为 Arrow。非空结果写入 staging；空结果仍写零行叶文件，不把空结果解释为删除整个分区。staging 只复读物理 Schema、身份 metadata、主键唯一性与行数，通过后进入共享 `StagedPathTransaction`。
# 
# 一次事务包含当前叶及必要的新建 `schema.parquet`。已有标记仍检查零行、物理契约与表身份，兼容时原样保留；描述性 metadata 差异不触发改写。缺失标记先写入 staging，再由同一事务安装并正式复读，确认零行与文件契约。随后只精确复读当前正式叶的物理契约、主键和行数。
# 
# 安装或正式验收失败时，共享模块按实际移动记录倒序恢复：移除本次新建标记、隔离失败新叶并恢复旧叶；恢复不完整时保留备份。第一次备份尚未成功的旧目标不会被当作新叶移走。staging 写入或复读失败由本函数清理，事务开始后的恢复与清理由共享模块负责。
# 
# 事实叶逐个提交，采集循环结束或额度停止后再逐叶提交日历；当前事务失败不撤销此前成功的事实叶或日历叶。共享模块使用同一文件系统内的路径替换，不提供跨表共同回滚、跨目录原子可见性、进程终止后的自动恢复或并发写入协调。c06 没有独立日期水位文件。
# 
# 本函数报告校验、暂存、安装、标记和正式复读进度，共享模块报告恢复结果；本函数在失败时记录阶段和耗时并继续抛错。只有正式验收通过、成功退出共享事务后才报告当前叶 `persisted=true`；事实叶成功尚不表示日历完成凭证已回写。

# ### 流程：构造 Parquet 文件 Schema
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["逻辑 Schema 与分区列"] --> B["移除目录承载的分区字段"] --> C["保留字段定义及完整 metadata"]
# ```

# In[15]:


def parquet_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    # Hive 分区值位于目录名中，数据文件只保存其余字段及完整 metadata。
    return pa.schema(
        [
            schema.field(name)
            for name in schema.names
            if name not in partition_columns
        ],
        metadata=schema.metadata,
    )


# ### 流程：构造精确分区过滤条件
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["分区列与对应值"] --> B["逐列等值条件以 AND 合并"] --> C["返回过滤表达式；空键报错"]
# ```

# In[16]:


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    # 用精确叶分区表达式限制 staging 与正式路径复读范围。
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


# ### 流程：单叶暂存、共享安装与恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录开始；完整叶业务校验与 Arrow 转换"] --> B["写 staging；空结果写零行叶"]
#   B --> C["复读文件契约、主键和行数"]
#   C -- 失败 --> X["本函数清理 staging 并抛错"]
#   C -- 通过 --> D["进入共享事务；备份并安装当前叶"]
#   D --> E["检查已有标记；必要时暂存、安装并验收新标记"]
#   E --> F["精确复读当前正式叶"]
#   D -. 失败 .-> R["共享模块倒序恢复实际移动的目标"]
#   E -. 失败 .-> R
#   F -- 失败 --> R
#   R --> S["移除新标记；隔离新叶并恢复旧叶；恢复不完整保留备份"]
#   S --> T["报告恢复结果与失败阶段；抛错"]
#   F -- 通过 --> G["成功退出并清理；报告当前叶已提交"]
# ```

# In[17]:


def commit_complete_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    table_name: str,
    schema: pa.Schema,
    partition_columns: list[str],
    partitioning: ds.Partitioning,
    partition_key: tuple[object, ...],
    validate_frame: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
    """校验一次 dirty 叶，并以轻量 staging/正式摘要完成可回滚替换。"""
    log_started_at = time.perf_counter()
    log_phase = "validate_dirty_leaf"

    click.echo(
        f"planning_progress: table={table_name}; function=commit_complete_partition; phase=commit_leaf; status=started; "
        f"partition={partition_key}; input_rows={len(frame)}"
    )
    try:
        complete_df = validate_frame(frame, "待提交完整分区")
        if not complete_df.empty:
            actual_partition_keys = set(
                complete_df[partition_columns].itertuples(index=False, name=None)
            )
            if actual_partition_keys != {partition_key}:
                raise ValueError("待提交数据越出指定 Hive 分区。")
        log_phase = "arrow_conversion"
        complete_table = pandas_to_arrow(complete_df, schema)

        log_phase = "prepare_paths"
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / table_name
        run_id = uuid.uuid4().hex[:12]
        staging_path = silver_root / f".s-{run_id}"
        backup_path = silver_root / f".b-{run_id}"
        quarantine_path = silver_root / f".q-{run_id}"
        silver_root.mkdir(parents=True, exist_ok=True)
        for managed_path in (target_path, staging_path, backup_path, quarantine_path):
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

        relative_path = pathlib.Path(
            *[
                f"{name}={value}"
                for name, value in zip(
                    partition_columns,
                    partition_key,
                    strict=True,
                )
            ]
        )
        expected_file_schema = parquet_file_schema(schema, partition_columns)
        if table_name == TABLE_NAME:
            primary_key = PRIMARY_KEY
        elif table_name == CALENDAR_TABLE_NAME:
            primary_key = CALENDAR_PRIMARY_KEY
        else:
            raise ValueError(f"b06 不支持提交未知表：{table_name}")
        non_partition_primary_key = [
            name for name in primary_key if name not in partition_columns
        ]
        log_phase = "staging_write"
        click.echo(
            f"planning_progress: table={table_name}; function=commit_complete_partition; phase=staging_write; status=started; "
            f"partition={partition_key}; rows={len(complete_table)}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        staging_path.mkdir(parents=True, exist_ok=False)

        try:
            if len(complete_table):
                ds.write_dataset(
                    complete_table,
                    staging_path,
                    format="parquet",
                    partitioning=partitioning,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
            else:
                empty_partition_path = staging_path / relative_path
                empty_partition_path.mkdir(parents=True, exist_ok=False)
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    empty_partition_path / "part-0.parquet",
                )

            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=staging_readback; status=started; "
                f"partition={partition_key}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_leaf_path = staging_path / relative_path
            staged_files = list(staged_leaf_path.rglob("*.parquet"))
            if not staged_files:
                raise FileNotFoundError(f"staging 缺少 {relative_path}。")
            staged_row_count = 0
            staged_key_frames = []
            for parquet_path in staged_files:
                staged_schema = pq.read_schema(parquet_path)
                if not physically_compatible(staged_schema, expected_file_schema):
                    raise TypeError("staging 物理 Schema 与契约不兼容。")
                if not schema_identity_compatible(staged_schema, expected_file_schema):
                    raise TypeError("staging 表身份 metadata 与契约不兼容。")
                metadata = pq.read_metadata(parquet_path)
                staged_row_count += metadata.num_rows
                if non_partition_primary_key and metadata.num_rows:
                    staged_key_frames.append(
                        pq.read_table(
                            parquet_path,
                            columns=non_partition_primary_key,
                        ).to_pandas(types_mapper=pd.ArrowDtype)
                    )
            if staged_row_count != len(complete_table):
                raise ValueError("staging 行数摘要与待提交分区不一致。")
            if staged_key_frames:
                staged_keys_df = pd.concat(staged_key_frames, ignore_index=True)
                if staged_keys_df.duplicated(non_partition_primary_key).any():
                    raise ValueError("staging 主键不唯一。")
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"planning_progress: table={table_name}; function=commit_complete_partition; phase=staging_readback; status=completed; "
            f"partition={partition_key}; files={len(staged_files)}; rows={staged_row_count}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "prepare_install"
        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        marker_path = target_path / "schema.parquet"
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=(
                f"table={table_name}; function=commit_complete_partition; "
                f"partition={partition_key}; run_id={run_id}"
            ),
        ) as transaction:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=install; status=started; "
                f"partition={partition_key}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path,
            )

            log_phase = "schema_marker"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=schema_marker; status=started; "
                f"partition={partition_key}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if marker_path.exists():
                marker_metadata = pq.read_metadata(marker_path)
                marker_schema = pq.read_schema(marker_path)
                if marker_metadata.num_rows:
                    raise ValueError("schema.parquet 必须是 0 行契约标记。")
                if not physically_compatible(marker_schema, expected_file_schema):
                    raise TypeError("schema.parquet 物理 Schema 与契约不兼容。")
                if not schema_identity_compatible(marker_schema, expected_file_schema):
                    raise TypeError("schema.parquet 表身份 metadata 与契约不兼容。")
            else:
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path,
                    staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    if (
                        marker_file.metadata.num_rows != 0
                        or not physically_compatible(marker_file.schema_arrow, expected_file_schema)
                        or not schema_identity_compatible(marker_file.schema_arrow, expected_file_schema)
                    ):
                        raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")

            log_phase = "formal_readback"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=formal_readback; status=started; "
                f"partition={partition_key}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            formal_files = list(destination_path.rglob("*.parquet"))
            if not formal_files:
                raise FileNotFoundError(f"正式叶缺少 Parquet 文件：{destination_path}")
            formal_row_count = 0
            formal_key_frames = []
            for parquet_path in formal_files:
                formal_schema = pq.read_schema(parquet_path)
                if not physically_compatible(formal_schema, expected_file_schema):
                    raise TypeError("正式叶物理 Schema 与契约不兼容。")
                if not schema_identity_compatible(formal_schema, expected_file_schema):
                    raise TypeError("正式叶表身份 metadata 与契约不兼容。")
                formal_metadata = pq.read_metadata(parquet_path)
                formal_row_count += formal_metadata.num_rows
                if non_partition_primary_key and formal_metadata.num_rows:
                    formal_key_frames.append(
                        pq.read_table(
                            parquet_path,
                            columns=non_partition_primary_key,
                        ).to_pandas(types_mapper=pd.ArrowDtype)
                    )
            if formal_row_count != len(complete_table):
                raise ValueError("正式叶行数摘要与 staging 不一致。")
            if formal_key_frames:
                formal_keys_df = pd.concat(formal_key_frames, ignore_index=True)
                if formal_keys_df.duplicated(non_partition_primary_key).any():
                    raise ValueError("正式叶主键不唯一。")
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=formal_readback; status=completed; "
                f"partition={partition_key}; files={len(formal_files)}; rows={formal_row_count}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"partition_committed: table={table_name}; function=commit_complete_partition; phase=commit_leaf; status=completed; "
            f"partition={partition_key}; rows={len(complete_df)}; run_id={run_id}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return complete_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_name}; function=commit_complete_partition; phase=commit_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 当前事实叶的 Session 替换
# 
# 入口精确读取当前 dirty 事实叶，`commit_fact_partition()` 用 `rows_for_sessions()` 找到真正待办 Session 的旧分钟，只替换这部分内容，保留同叶其他事实和白名单外历史。
# 
# 合并结果交给 `commit_complete_partition()` 完成单叶校验与安装。成功后返回本次采集的分钟，用于生成完成摘要；这里的事实提交成功尚不表示日历状态已回写。空响应也会清除该待办 Session 的旧分钟，并为后续零行完成状态提供依据。
# 
# `commit_fact_partition()` 报告保留、替换与合并的行数，合并结果带 `persisted=false`。单叶提交成功后报告事实提交完成，并明确 `calendar_state=pending`；入口只累计成功事实叶数量。
# 
# 采集函数已经按分钟主键排序；事实提交成功后直接返回该采集结果，不再次排序和重置索引。该返回值没有从正式湖重读行情值，完成摘要使用它及同一次采集的异常计数。

# ### 流程：待办 Session 替换与事实提交
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["当前事实叶与待办 Session"] --> B["保留非待办旧分钟"] --> C["拼接本次结果；报告 persisted=false"] --> D["校验并提交完整事实叶"] --> E["报告事实已提交；日历仍待回写"]
# ```

# In[18]:


def commit_fact_partition(
    collected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    pending_sessions_df: pd.DataFrame,
    partition_key: tuple[object, ...],
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "select_retained_rows"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_fact_partition; phase=fact_commit; status=started; "
        f"partition={partition_key}; new_rows={len(collected_df)}; sessions={len(pending_sessions_df)}"
    )
    try:
        # 只删除真正待办 Session 的旧分钟；未触达和白名单外历史数据继续保留。
        stale_df = rows_for_sessions(existing_df, pending_sessions_df)
        stale_index = pd.MultiIndex.from_frame(stale_df[PRIMARY_KEY])
        existing_index = pd.MultiIndex.from_frame(existing_df[PRIMARY_KEY])
        retained_df = existing_df.loc[
            ~existing_index.isin(stale_index),
            FUTURES_MINUTE_SCHEMA.names,
        ]

        log_phase = "merge_fact_leaf"
        desired_df = pd.concat(
            [
                retained_df,
                collected_df.loc[:, FUTURES_MINUTE_SCHEMA.names],
            ],
            ignore_index=True,
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_fact_partition; phase=merge_fact_leaf; status=completed; "
            f"partition={partition_key}; retained_rows={len(retained_df)}; new_rows={len(collected_df)}; complete_rows={len(desired_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "commit_fact_leaf"
        committed_df = commit_complete_partition(
            desired_df,
            lake_root,
            TABLE_NAME,
            FUTURES_MINUTE_SCHEMA,
            PARTITION_COLUMNS,
            HIVE_PARTITIONING,
            partition_key,
            validate_minute_frame,
        )

        # 正式安装只复读物理 Schema、主键与行数摘要；业务内容已经在 dirty 叶校验一次。
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_fact_partition; phase=fact_commit; status=completed; "
            f"partition={partition_key}; fact_rows={len(collected_df)}; complete_rows={len(committed_df)}; calendar_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return collected_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_fact_partition; phase=fact_commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完成摘要生成与日历集中回写
# 
# `build_calendar_completion_updates()` 在事实叶成功后，按 Session 统计本次分钟条数，复用采集阶段已经记录的 OHLC 异常计数，并按日历叶组织摘要。入口先汇集各成功事实叶的摘要，再调用 `commit_calendar_updates()`；生成摘要本身不写日历。
# 
# `commit_calendar_updates()` 对政策变化与完成摘要涉及的日历叶取并集，每叶精确读取一次，先应用白名单变化，再更新本批完成状态，最后执行一次完整叶业务校验和提交。未触达行保持原值；新完成状态清空本次 Session 的旧 c07 旁证，政策缩小本身保留既有旁证。
# 
# 零行 Session 记为已完成 warning，并产生 `suspected_closed` 信号交给 c07；部分缺失或有限 OHLC 异常也形成已完成 warning。OHLC 异常本身不改写开市证据。只有本次非空事实才取消对应的旧疑似休市信号。
# 
# c06 没有独立日期水位文件，下次待办依据日历中的 required 与 completed。配额停止仍进入后续日历回写，处理此前成功事实和政策变化；普通异常则直接停止，保留此前已提交的叶，不额外补写尚未提交的状态。
# 
# 完成摘要和日历叶生成分别由对应函数报告，内存结果均标记 `persisted=false`。`commit_calendar_updates()` 在对应日历叶的正式提交成功后，才为本次完成摘要报告 `phase=completion_state; persisted=true`。单纯政策回写不新增采集完成凭证，也不把零行契约标记当成日期水位。
# 
# 待办 Session 的唯一性由 c04 和规划筛选保证，生成摘要不再去重。完整摘要携带分区列并一次 groupby，循环直接取当前分组，移除每个分区与整批摘要重新 merge 的操作。日历回写仍保留一对一关联、覆盖检查和计数边界；移除紧邻关联之前的重复键检查，固定日历列清单在分区循环前准备。

# ### 流程：完成摘要与逐叶日历回写
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["成功事实分区与待办 Session"] --> B["统计实际条数；复用采集异常计数"] --> C["摘要一次分组；persisted=false；入口汇集"]
#   C --> D["合并政策变化与完成摘要的叶键"] --> E["精确读取当前日历完整叶"] --> F["应用白名单变化；保留既有完成证据"]
#   F --> G["写本次完成、缺失与质量状态"] --> H["零行形成疑似休市；清空本次旧 c07 旁证"] --> I["正式提交成功后报告完成凭证 persisted=true"] --> J{"还有日历叶？"}
#   J -- 是 --> E
#   J -- 否 --> K["返回政策行、完成 Session 和提交叶数量"]
# ```

# In[19]:


def build_calendar_completion_updates(
    sessions_df: pd.DataFrame,
    committed_requested_df: pd.DataFrame,
    invalid_session_counts: dict[tuple[object, ...], int],
) -> dict[tuple[object, ...], pd.DataFrame]:
    """把成功事实分区压成按 b04 叶组织的 Session 摘要。"""
    log_started_at = time.perf_counter()
    log_phase = "count_facts"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_calendar_completion_updates; phase=completion_summary; status=started; "
        f"sessions={len(sessions_df)}; fact_rows={len(committed_requested_df)}; persisted=false"
    )
    try:
        if sessions_df.empty:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_calendar_completion_updates; phase=completion_summary; status=completed; "
                f"sessions=0; partitions=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return {}

        fact_counts = (
            committed_requested_df.groupby(SESSION_KEY, dropna=False).size()
            if not committed_requested_df.empty
            else pd.Series(dtype="int64")
        )
        invalid_counts = pd.Series(invalid_session_counts, dtype="int64")

        log_phase = "build_session_summary"
        summary_partition_columns = [
            name for name in CALENDAR_PARTITION_COLUMNS if name != "bar_frequency"
        ]
        summary_columns = [*SESSION_KEY, "_actual_count", "_invalid_count"]
        session_summary_df = sessions_df.loc[
            :, [*SESSION_KEY, *summary_partition_columns]
        ].copy()
        session_index = pd.MultiIndex.from_frame(
            session_summary_df[SESSION_KEY]
        )
        session_summary_df["_actual_count"] = fact_counts.reindex(
            session_index, fill_value=0
        ).to_numpy(dtype="int64")
        session_summary_df["_invalid_count"] = invalid_counts.reindex(
            session_index, fill_value=0
        ).to_numpy(dtype="int64")

        log_phase = "group_calendar_leaves"
        updates_by_partition = {}
        for partition_values, partition_summary_df in session_summary_df.groupby(
            summary_partition_columns, sort=True
        ):
            exchange_code, year, month = partition_values
            partition_key = (
                "1m", exchange_code, int(year), int(month)
            )
            updates_by_partition[partition_key] = partition_summary_df.loc[
                :, summary_columns
            ].reset_index(drop=True)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_calendar_completion_updates; phase=completion_summary; status=completed; "
            f"sessions={len(session_summary_df)}; partitions={len(updates_by_partition)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updates_by_partition
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_calendar_completion_updates; phase=completion_summary; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def commit_calendar_updates(
    policy_updates_by_partition: dict[tuple[object, ...], pd.DataFrame],
    completion_updates_by_partition: dict[
        tuple[object, ...], pd.DataFrame
    ],
    lake_root: pathlib.Path,
    fetch_run_id: str,
    completed_at: datetime,
) -> tuple[int, int, int]:
    """每个 dirty b04 叶一次读取、一次完整校验和一次提交。"""
    log_started_at = time.perf_counter()
    log_phase = "plan_dirty_leaves"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=calendar_commit; status=started; "
        f"policy_leaves={len(policy_updates_by_partition)}; completion_leaves={len(completion_updates_by_partition)}; fetch_run_id={fetch_run_id}"
    )
    try:
        calendar_path = lake_root.resolve() / "silver" / CALENDAR_TABLE_NAME
        policy_row_count = 0
        completion_row_count = 0
        committed_partition_count = 0
        partition_keys = sorted(
            set(policy_updates_by_partition)
            | set(completion_updates_by_partition)
        )
        calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
        for partition_key in partition_keys:
            log_phase = "read_calendar_leaf"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=build_calendar_leaf; status=started; "
                f"partition={partition_key}; completed={committed_partition_count}; total={len(partition_keys)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            calendar_df = read_complete_partition(
                calendar_path,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                partition_key,
            )
            if calendar_df.empty:
                raise FileNotFoundError(f"日历回写目标叶不存在：{partition_key}")
            desired_df = calendar_df

            log_phase = "apply_policy"
            policy_updates_df = policy_updates_by_partition.get(partition_key)
            if policy_updates_df is not None and not policy_updates_df.empty:
                desired_df = desired_df.merge(
                    policy_updates_df,
                    on=CALENDAR_PRIMARY_KEY,
                    how="left",
                    validate="one_to_one",
                )
                update_mask = desired_df["_desired_required"].notna()
                if int(update_mask.sum()) != len(policy_updates_df):
                    raise ValueError(
                        f"政策回写主键无法完整匹配：{partition_key}"
                    )
                desired_required = desired_df["_desired_required"].fillna(
                    desired_df["is_fetch_required"]
                ).astype(bool)
                expansion_mask = update_mask & desired_required
                contraction_mask = update_mask & ~desired_required
                desired_df.loc[update_mask, "is_fetch_required"] = (
                    desired_required.loc[update_mask]
                )
                desired_df.loc[expansion_mask, "selection_reason"] = (
                    SELECTED_REASON
                )
                desired_df.loc[contraction_mask, "selection_reason"] = (
                    EXCLUDED_REASON
                )
                desired_df.loc[contraction_mask, "is_data_missing"] = False
                desired_df.loc[contraction_mask, "missing_bar_count"] = 0
                incomplete_expansion_mask = (
                    expansion_mask
                    & ~desired_df["is_fetch_completed"].eq(True)
                )
                completed_expansion_mask = (
                    expansion_mask & ~incomplete_expansion_mask
                )
                completed_expansion_missing = (
                    desired_df["expected_bar_count"]
                    - desired_df["actual_bar_count"]
                ).clip(lower=0)
                desired_df.loc[
                    completed_expansion_mask, "missing_bar_count"
                ] = completed_expansion_missing.loc[completed_expansion_mask]
                desired_df.loc[
                    completed_expansion_mask, "is_data_missing"
                ] = completed_expansion_missing.loc[
                    completed_expansion_mask
                ].gt(0)
                desired_df.loc[
                    incomplete_expansion_mask, "missing_checked_at"
                ] = None
                desired_df.loc[
                    incomplete_expansion_mask, "quality_status"
                ] = "pending"
                desired_df.loc[
                    incomplete_expansion_mask, "quality_reason"
                ] = "分钟采集政策已选择该格点，等待事实提交与复读。"
                desired_df.loc[
                    incomplete_expansion_mask, "quality_checked_at"
                ] = None
                desired_df.loc[update_mask, "updated_at"] = completed_at
                desired_df = desired_df.drop(columns=["_desired_required"])
                policy_row_count += len(policy_updates_df)

            log_phase = "apply_completion"
            completion_updates_df = completion_updates_by_partition.get(
                partition_key
            )
            if (
                completion_updates_df is not None
                and not completion_updates_df.empty
            ):
                desired_df = desired_df.merge(
                    completion_updates_df,
                    on=SESSION_KEY,
                    how="left",
                    validate="one_to_one",
                )
                requested_mask = desired_df["_actual_count"].notna()
                if int(requested_mask.sum()) != len(completion_updates_df):
                    raise ValueError(
                        f"待回写 Session 无法完整匹配正式日历：{partition_key}"
                    )
                if (
                    ~desired_df.loc[requested_mask, "bar_frequency"].eq("1m")
                    | ~desired_df.loc[requested_mask, "is_fetch_required"].eq(True)
                ).any():
                    raise ValueError("状态回写命中了非待采集分钟 Session。")

                actual_count = desired_df["_actual_count"].fillna(0).astype("int64")
                invalid_count = desired_df["_invalid_count"].fillna(0).astype("int64")
                expected_count = desired_df["expected_bar_count"].astype("int64")
                if (
                    actual_count.loc[requested_mask].gt(
                        expected_count.loc[requested_mask]
                    ).any()
                    or invalid_count.loc[requested_mask].gt(
                        actual_count.loc[requested_mask]
                    ).any()
                ):
                    raise ValueError(
                        "分钟实际条数或 OHLC 异常条数越出理论边界。"
                    )
                missing_count = expected_count - actual_count
                warning_mask = requested_mask & (
                    missing_count.gt(0) | invalid_count.gt(0)
                )
                passed_mask = requested_mask & ~warning_mask
                zero_mask = requested_mask & actual_count.eq(0)
                recovered_mask = (
                    requested_mask
                    & actual_count.gt(0)
                    & desired_df["schedule_status"].eq("suspected_closed")
                )
                desired_df.loc[zero_mask, "schedule_status"] = "suspected_closed"
                desired_df.loc[zero_mask, "schedule_signal_reason"] = (
                    "JQData 分钟请求成功且正式安装为 0 条，Session 疑似休市，"
                    "等待 b07 定向校对。"
                )
                desired_df.loc[zero_mask, "evidence_level"] = "inferred"
                desired_df.loc[zero_mask, "evidence_source"] = (
                    "fact_futures_minute:formal_empty_session"
                )
                desired_df.loc[recovered_mask, "schedule_status"] = "scheduled"
                desired_df.loc[recovered_mask, "schedule_signal_reason"] = (
                    "分钟事实正式安装非空，取消由空响应形成的疑似休市信号。"
                )
                desired_df.loc[recovered_mask, "evidence_level"] = "inferred"
                desired_df.loc[recovered_mask, "evidence_source"] = (
                    "fact_futures_minute:formal_nonempty_session"
                )
                for evidence_column in EVIDENCE_COLUMNS:
                    desired_df.loc[requested_mask, evidence_column] = None
                desired_df.loc[requested_mask, "is_fetch_completed"] = True
                desired_df.loc[requested_mask, "actual_bar_count"] = (
                    actual_count.loc[requested_mask]
                )
                desired_df.loc[requested_mask, "is_data_missing"] = (
                    missing_count.loc[requested_mask].gt(0)
                )
                desired_df.loc[requested_mask, "missing_bar_count"] = (
                    missing_count.loc[requested_mask]
                )
                desired_df.loc[requested_mask, "fetch_run_id"] = fetch_run_id
                desired_df.loc[requested_mask, "fetch_completed_at"] = completed_at
                desired_df.loc[requested_mask, "missing_checked_at"] = completed_at
                desired_df.loc[passed_mask, "quality_status"] = "passed"
                desired_df.loc[warning_mask, "quality_status"] = "warning"
                quality_reason = (
                    "JQData 分钟请求已完成并正式安装；理论 "
                    + expected_count.astype("string")
                    + " 条，实际 "
                    + actual_count.astype("string")
                    + " 条，缺失 "
                    + missing_count.astype("string")
                    + " 条。"
                )
                invalid_reason = (
                    "其中 "
                    + invalid_count.astype("string")
                    + " 条供应商原始 OHLC 跨列关系异常，原值已保留。"
                )
                quality_reason = quality_reason + invalid_reason.where(
                    invalid_count.gt(0), ""
                )
                desired_df.loc[requested_mask, "quality_reason"] = (
                    quality_reason.loc[requested_mask]
                )
                desired_df.loc[requested_mask, "quality_checked_at"] = completed_at
                desired_df.loc[requested_mask, "updated_at"] = completed_at
                desired_df = desired_df.drop(
                    columns=["_actual_count", "_invalid_count"]
                )
                completion_row_count += len(completion_updates_df)

            complete_df = desired_df.loc[
                :, calendar_columns
            ]
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=build_calendar_leaf; status=completed; "
                f"partition={partition_key}; rows={len(complete_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_phase = "commit_calendar_leaf"
            commit_complete_partition(
                complete_df,
                lake_root,
                CALENDAR_TABLE_NAME,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                CALENDAR_PARTITIONING,
                partition_key,
                validate_calendar_state_frame,
            )
            committed_partition_count += 1
            if completion_updates_df is not None and not completion_updates_df.empty:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=completion_state; status=completed; "
                    f"partition={partition_key}; sessions={len(completion_updates_df)}; source=is_fetch_completed; fetch_run_id={fetch_run_id}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=calendar_commit; status=running; "
                f"partition={partition_key}; committed_partitions={committed_partition_count}; total={len(partition_keys)}; policy_rows={policy_row_count}; completed_sessions={completion_row_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=calendar_commit; status=completed; "
            f"policy_rows={policy_row_count}; state_rows={completion_row_count}; partitions={committed_partition_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return policy_row_count, completion_row_count, committed_partition_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_updates; phase=calendar_commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：规划、调度与运行结果
# 
# `main()` 校验日期及正式湖写入边界，打开 Dataset 并形成政策变化和分钟待办。没有请求计划时不认证 JQData；若只有政策变化且启用写入，直接提交日历政策。存在待办时逐事实分区检查配额、调用采集并按 `--write` 决定是否提交，最后集中回写日历。
# 
# 日志使用与 c01、c02 一致的 88 个 `=` 运行边界及 `table/function/phase/status` 字段，区分规划、采集、事实提交、日历回写和最终结果。配额停止报告 `stopped`，不会显示为整批完成；普通异常继续抛出，不输出成功结束日志。读取、规划、采集、事实合并、完成摘要与提交由对应函数报告；入口只保留整批规划、分区调度、配额停止、累计进度和运行结果，不重复输出函数级起止。
# 
# 本单元格只定义 Click 命令。后面的独立执行单元格区分 Notebook 与脚本环境：Notebook 显式传入参数，避免读取内核的 `-f` 参数；直接运行 `.py` 使用命令行参数；普通模块导入不触发业务。
# 
# 正式湖没有逐分区重开表根或重复全表业务质检：根 Dataset 物理门禁各执行一次，日历窄列按当前分区过滤读取，提交只读取 dirty 完整叶。默认规划仍需遍历全历史理论格点以应用当前白名单；显式日期过滤表达式在规划循环前构造一次。

# ### 流程：入口分支与日志边界
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["参数检查；记录 run started"] --> B["规划窄列与请求分区；报告计划"] --> C{"存在请求计划？"}
#   C -- 否 --> D["可选政策写入；报告无需采集"]
#   C -- 是 --> E["按分区调度函数；累计采集与事实提交量"] --> F{"write？"}
#   F -- 否 --> G["只读采集汇总"]
#   F -- 是 --> H["调用日历回写函数；核对完成摘要数"]
#   G --> I{"配额停止？"}
#   H --> I
#   I -- 是 --> J["quota_stop；run stopped"]
#   I -- 否 --> K["run completed"]
#   D --> K
# ```

# In[20]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "--quota-reserve",
    type=click.IntRange(min=0),
    default=DEFAULT_QUOTA_RESERVE,
    show_default=True,
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    quota_reserve: int,
    write: bool,
) -> None:
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )
    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"{log_boundary}\n分钟运行开始 / Minute run started\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\n{log_boundary}"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=started"
    )
    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    target_path = silver_root / TABLE_NAME
    planning_started_at = time.perf_counter()

    # 根 Dataset 各打开一次，只做物理契约门禁；日常规划不读取任何事实行。
    calendar_dataset = open_contract_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        "上游行情日历",
        required=True,
    )
    open_contract_dataset(
        target_path,
        HIVE_PARTITIONING,
        FUTURES_MINUTE_SCHEMA,
        "现有分钟事实",
        required=False,
    )
    calendar_partition_keys = sorted(
        partition_key
        for partition_key in partition_keys_from_dataset(
            calendar_dataset,
            calendar_path,
            CALENDAR_PARTITION_COLUMNS,
        )
        if partition_key[0] == "1m"
    )
    run_updated_at = datetime.now(timezone.utc)
    fetch_run_id = (
        f"minute-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    policy_updates_by_partition: dict[
        tuple[object, ...],
        pd.DataFrame,
    ] = {}
    pending_frames_by_fact_partition: dict[
        tuple[object, ...],
        list[pd.DataFrame],
    ] = {}
    scanned_rows = 0
    selected_count = 0
    complete_count = 0
    pending_count = 0
    expected_pending_rows = 0
    click.echo(
        "planning_progress: "
        f"table={TABLE_NAME}; function=main; phase=planning; status=running; calendar_partitions={len(calendar_partition_keys)}; "
        f"calendar_files={len(calendar_dataset.files)}; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
    )

    requested_date_filter = None
    if requested_start_date is not None:
        requested_date_filter = (
            ds.field("trading_date") >= requested_start_date
        ) & (ds.field("trading_date") <= requested_end_date)
    for partition_number, calendar_partition_key in enumerate(
        calendar_partition_keys,
        start=1,
    ):
        calendar_filter = partition_expression(
            CALENDAR_PARTITION_COLUMNS, calendar_partition_key
        )
        if requested_date_filter is not None:
            calendar_filter &= requested_date_filter
        planning_table = calendar_dataset.to_table(
            columns=MINUTE_PLANNING_COLUMNS,
            filter=calendar_filter,
        ).cast(MINUTE_PLANNING_SCHEMA, safe=True)
        scanned_rows += len(planning_table)
        exchange_code = str(calendar_partition_key[1])
        (
            desired_required,
            policy_changed_mask,
            pending_mask,
            completed_mask,
        ) = minute_policy_plan(
            planning_table, exchange_code
        )

        selected_count += int(
            pc.sum(pc.cast(desired_required, pa.int64())).as_py() or 0
        )
        complete_count += int(
            pc.sum(pc.cast(completed_mask, pa.int64())).as_py() or 0
        )
        pending_count += int(
            pc.sum(pc.cast(pending_mask, pa.int64())).as_py() or 0
        )
        if bool(pc.any(policy_changed_mask).as_py()):
            policy_updates_df = planning_table.select(
                CALENDAR_PRIMARY_KEY
            ).filter(policy_changed_mask).to_pandas(
                types_mapper=pd.ArrowDtype
            )
            policy_updates_df["_desired_required"] = pc.filter(
                desired_required, policy_changed_mask
            ).to_numpy(zero_copy_only=False)
            policy_updates_by_partition[calendar_partition_key] = (
                policy_updates_df.reset_index(drop=True)
            )

        pending_table = planning_table.filter(pending_mask)
        pending_sessions_df = pending_table.to_pandas(
            types_mapper=pd.ArrowDtype
        )
        expected_pending_rows += int(
            pc.sum(pending_table["expected_bar_count"]).as_py() or 0
        )
        for partition_values, partition_sessions_df in pending_sessions_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        ):
            fact_partition_key = tuple(partition_values)
            pending_frames_by_fact_partition.setdefault(
                fact_partition_key,
                [],
            ).append(partition_sessions_df.reset_index(drop=True))

        if (
            partition_number == 1
            or partition_number % 25 == 0
            or partition_number == len(calendar_partition_keys)
        ):
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; function=main; phase=planning; status=running; "
                f"partitions={partition_number}/{len(calendar_partition_keys)}; "
                f"key={calendar_partition_key}; rows={scanned_rows}; "
                f"pending={pending_count}; "
                f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
            )

    plans = []
    for partition_key, partition_frames in sorted(
        pending_frames_by_fact_partition.items()
    ):
        pending_sessions_df = pd.concat(partition_frames, ignore_index=True)
        plans.append(
            {
                "partition_key": partition_key,
                "pending_sessions_df": pending_sessions_df,
                "expected_rows": int(
                    pending_sessions_df["expected_bar_count"].sum()
                ),
            }
        )
    policy_changed_count = sum(
        len(policy_updates_df)
        for policy_updates_df in policy_updates_by_partition.values()
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; function=main; phase=planning; status=completed; "
        f"policy_changed={policy_changed_count}; "
        f"selected_sessions={selected_count}; "
        f"complete_sessions={complete_count}; "
        f"pending_sessions={pending_count}; "
        f"pending_partitions={len(plans)}; "
        f"expected_pending_rows={expected_pending_rows}; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
    )

    if not plans:
        if write and policy_updates_by_partition:
            policy_rows, state_rows, calendar_partitions = (
                commit_calendar_updates(
                    policy_updates_by_partition,
                    {},
                    resolved_lake_root,
                    fetch_run_id,
                    run_updated_at,
                )
            )
        click.echo(
            f"up_to_date: table={TABLE_NAME}; mode={mode}; function=main; phase=collect; status=completed; "
            f"pending_sessions=0; policy_rows={policy_changed_count}; write={str(write).lower()}"
        )
        click.echo(
            f"{log_boundary}\n分钟运行结束 / Minute run ended\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome={'policy_committed' if write and policy_updates_by_partition else ('read_only' if policy_updates_by_partition else 'up_to_date')}; "
            f"mode={mode}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return

    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    log_processed_sessions = 0
    processed_partitions = 0
    completed_partitions = 0
    committed_rows = 0
    returned_rows = 0
    invalid_ohlc_rows = 0
    completed_session_count = 0
    quota_stop_message = None
    completion_update_frames_by_partition: dict[
        tuple[object, ...], list[pd.DataFrame]
    ] = {}
    for partition_number, plan in enumerate(plans, start=1):
        partition_key = plan["partition_key"]
        expected_rows = int(plan["expected_rows"])
        spare = quota_spare(jqdata)
        if spare is not None and spare - expected_rows < quota_reserve:
            quota_stop_message = (
                "quota_stop: "
                f"partition={partition_number}/{len(plans)}; "
                f"key={partition_key}; spare={spare}; "
                f"expected={expected_rows}; reserve={quota_reserve}; "
                f"completed_partitions={completed_partitions}; "
                f"table={TABLE_NAME}; function=main; phase=quota; status=stopped; "
                f"processed_sessions={log_processed_sessions}; planned_sessions={pending_count}; "
                "未请求或写入当前分区。"
            )
            break

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=collect_batch; status=running; "
            f"event=dispatch; partition={partition_number}/{len(plans)}; key={partition_key}; sessions={len(plan['pending_sessions_df'])}; expected_rows={expected_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        collected_df, partition_returned_rows, invalid_session_counts = (
            collect_partition(
                jqdata,
                plan["pending_sessions_df"],
                run_updated_at,
            )
        )
        returned_rows += partition_returned_rows
        processed_partitions += 1
        partition_invalid_rows = sum(invalid_session_counts.values())
        invalid_ohlc_rows += partition_invalid_rows
        log_processed_sessions += len(plan["pending_sessions_df"])
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=collect_batch; status=running; "
            f"processed_partitions={processed_partitions}; total_partitions={len(plans)}; processed_sessions={log_processed_sessions}; planned_sessions={pending_count}; returned_rows={returned_rows}; invalid_ohlc_rows={invalid_ohlc_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        if not write:
            continue

        existing_df = read_complete_partition(
            target_path,
            FUTURES_MINUTE_SCHEMA,
            PARTITION_COLUMNS,
            partition_key,
        )
        committed_requested_df = commit_fact_partition(
            collected_df,
            existing_df,
            plan["pending_sessions_df"],
            partition_key,
            resolved_lake_root,
        )
        partition_completion_updates = build_calendar_completion_updates(
            plan["pending_sessions_df"],
            committed_requested_df,
            invalid_session_counts,
        )
        for calendar_partition_key, completion_updates_df in (
            partition_completion_updates.items()
        ):
            completion_update_frames_by_partition.setdefault(
                calendar_partition_key, []
            ).append(completion_updates_df)
        completed_partitions += 1
        completed_session_count += len(plan["pending_sessions_df"])
        committed_rows += len(committed_requested_df)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_batch; status=running; "
            f"completed_fact_partitions={completed_partitions}; total_fact_partitions={len(plans)}; fact_rows={committed_rows}; pending_calendar_sessions={completed_session_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

    if not write:
        click.echo(
            f"api_result: table={TABLE_NAME}; function=main; phase=collect_summary; "
            f"status={'stopped' if quota_stop_message is not None else 'completed'}; partitions={processed_partitions}; "
            f"planned_partitions={len(plans)}; "
            f"planned_sessions={pending_count}; processed_sessions={log_processed_sessions}; returned_rows={returned_rows}; "
            f"invalid_ohlc_rows={invalid_ohlc_rows}; write=false"
        )
        if quota_stop_message is not None:
            click.echo(quota_stop_message)
        click.echo(
            f"{log_boundary}\n分钟运行结束 / Minute run ended\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
            f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
            f"outcome={'quota_stopped' if quota_stop_message is not None else 'read_only'}; mode={mode}; write={str(write).lower()}; "
            f"planned_sessions={pending_count}; processed_sessions={log_processed_sessions}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return

    completion_updates_by_partition = {
        partition_key: pd.concat(update_frames, ignore_index=True)
        for partition_key, update_frames in (
            completion_update_frames_by_partition.items()
        )
    }
    policy_rows, completed_sessions, calendar_partitions = (
        commit_calendar_updates(
            policy_updates_by_partition,
            completion_updates_by_partition,
            resolved_lake_root,
            fetch_run_id,
            run_updated_at,
        )
    )
    if completed_sessions != completed_session_count:
        raise RuntimeError("日历完成摘要行数与已提交 Session 数不一致。")
    if quota_stop_message is not None:
        click.echo(quota_stop_message)
    click.echo(
        f"committed: table={TABLE_NAME}; function=main; phase=commit_batch; status=completed; "
        f"mode={'explicit_non_formal' if has_explicit_dates else 'automatic_gap_fill'}; "
        f"quota_stopped={str(quota_stop_message is not None).lower()}"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_summary; status=completed; "
        f"completed_partitions={completed_partitions}; "
        f"completed_sessions={completed_sessions}; "
        f"fact_rows={committed_rows}; returned_rows={returned_rows}; "
        f"invalid_ohlc_rows={invalid_ohlc_rows}; "
        f"remaining_current_plan={pending_count - completed_sessions}"
    )
    click.echo(
        f"{log_boundary}\n分钟运行结束 / Minute run ended\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
        f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
        f"outcome={'quota_stopped' if quota_stop_message is not None else 'committed'}; mode={mode}; write={str(write).lower()}; "
        f"planned_sessions={pending_count}; processed_sessions={log_processed_sessions}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
    )


# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，与 c01—c05 保持一致。当前参数是 2026-08-01 至 2026-08-15 的成对日期只读示例，不带 `--write`；存在待办时仍会调用 JQData 并完成采集与校验，但不提交事实或回写日历。直接运行脚本使用命令行参数；在 Notebook 中导入同名 Python 模块不会触发业务。具体模式与正式湖写入限制见开篇表格。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会规划待办，并在存在请求批次时采集和校验。流程图本身不执行代码。
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

# In[21]:


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
        prog_name="c06_futures_minute",
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
#     C --> D["按白名单与完成凭证采集；事实逐叶提交后回写日历"]
# ```

# In[22]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Market_Data\a01_Collection\b01_Futures_Market_Data\c06_futures_minute.py --write

