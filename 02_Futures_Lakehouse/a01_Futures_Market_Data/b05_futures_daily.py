#!/usr/bin/env python
# coding: utf-8

# # b05_futures_daily
# 
# 读取 b04 的 `dim_futures_bar_calendar`，从 JQData 采集日线并生成 `fact_futures_daily`；写入时将事实叶和对应日历叶共同提交，回写采集完成、缺失和质量状态。
# 
# 默认按当前事实白名单评估全部历史日线格点，API 待办为 **当前需要采集且尚未完成** 的格点。正式完成状态是可信快照：已完成的空结果、缺失占位和 warning 不重新拉取，也不扫描全历史事实重新证明完成。
# 
# 阅读顺序：初始化与契约 → 输出校验 → Dataset 边界 → 白名单和请求计划 → 来源响应与事实生成 → 完整叶准备及协调提交 → 运行入口。函数按定义顺序展开，总流程图按实际调用顺序阅读。Notebook 是唯一编辑源，同名 `.py` 由默认 PythonExporter 生成。

# ## 总流程：日线待办到事实与完成状态
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["参数与正式湖写入边界"] --> B["读取 b04 日线规划窄列"]
#   B --> C["评估白名单：政策 dirty 与未完成 pending"]
#   C --> D{"存在 API 批次？"}
#   D -- 否 --> H["拼接已校验事实；保持主键排序"]
#   D -- 是 --> E["认证 JQData"] --> P["请求前检查本批配额"]
#   P -- 额度不足 --> Q["记录 quota_stop；停止后续请求"]
#   Q --> H
#   P -- 可请求 --> F["三次 API；归一化、派生与待办对齐"]
#   F --> G{"还有批次？"}
#   G -- 是 --> P
#   G -- 否 --> H
#   H --> I{"启用 write 且有变化？"}
#   I -- 否 --> J["只读结束或无需更新；保留配额停止结论"]
#   I -- 是 --> K["逐组准备事实与日历完整叶；业务校验"]
#   K --> L["staging → 共同安装 → 正式复读"]
#   L -- 成功 --> M["记录本组完成；继续下一组"]
#   M -- 全部组结束 --> N["输出已提交统计；有 quota_stop 则报告停止"]
#   M -- 还有组 --> K
#   L -- 失败 --> R["恢复当前组并抛错；此前成功组保留"]
# ```
# 
# 认证每次运行至多一次。图中未展开的读取、采集、生成和校验异常沿原调用链抛出，停止后续工作；只有进入安装阶段后的失败才涉及当前组恢复。

# ## 更新范围与写入边界
# 
# | 方式 | 处理范围 | 写入条件 |
# | --- | --- | --- |
# | 默认自动 | 全历史日线窄列评估；只采集当前 required 且未完成格点 | `--write` 可提交正式湖 |
# | 成对 `--start-date/--end-date` | 只评估指定日期的日线格点 | 正式湖仅可只读；写入必须指定非正式湖 |
# | 不带 `--write` | 仍采集待办并完成内存校验 | 不提交事实或日历状态 |
# 
# b05 不提供 `--full`。白名单来自共享配置，已完成证据不因零条或 warning 失效。配额不足停止后续请求；若启用写入，已完成批次和政策变化仍可提交，然后报告 `quota_stop`。业务异常不自动重试。

# ## 项目定位与依赖
# 
# 从当前目录向上搜索 `.git`、`.env` 和 `config/settings.py`，先定位项目根，再导入配置、具名权威 Schema 和共享事实采集白名单。正式湖根目录由 `settings.futures_lake_root` 读取，唯一配置来源是 `.env` 的 `FUTURES_LAKE_ROOT`。
# 
# 初始化只建立依赖与定义，不认证 JQData、不采集、不写湖。

# ### 流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["当前目录向上搜索项目标记"] --> B["导入配置与权威 Schema"] --> C["导入白名单与展示依赖"]
# ```

# In[1]:


from __future__ import annotations

# 标准库负责数值校验、路径管理、原子替换和运行批次标识。
import math
import numpy as np
import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from types import ModuleType


# 只使用项目统一规定的三个标记定位根目录，保证从仓库任意子目录运行都一致。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


# 第三方库分别承担 CLI、表格转换和 Arrow/Parquet 数据集读写。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# 业务代码只引用中央契约，不在 Notebook 中复制字段定义或正式湖路径。
from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
# 配置在根目录定位完成后导入，避免把正式湖路径或凭据复制到 Notebook。
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction
from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS


# ## Schema 契约浏览
# 
# 交互式 Notebook 通过共享展示模块查看本环节的权威 Schema 与有界只读样例；导出的命令行脚本跳过展示。字段含义、主键和分区以 `config/data_contracts.py` 为准，浏览不代替生产校验。

# ### 流程：只读契约浏览
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A{"交互式 Notebook？"} -- 是 --> B["展示权威 Schema 与有界样例"]
#   A -- 否 --> C["跳过展示"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、规划投影与请求常量
# 
# 表名、主键和 Hive 分区在初始化时从具名 Schema metadata 各读取一次。事实主键用于响应对齐和完整叶合并；API 待办依赖日历完成状态，不通过事实主键全表求差。
# 
# `CALENDAR_PLANNING_COLUMNS` 仅包含日线主键、白名单身份、休市状态、required、completed 和年月分区。JQData 字段、来源标识、质量枚举及请求上限在此声明，不发起 I/O。

# ### 流程：建立本环节常量
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["具名 Schema metadata"] --> B["表名、主键与分区"] --> C["规划投影、请求字段与限额"] --> D["构造 Hive partitioning"]
# ```

# In[3]:


# 表名、主键和 Hive 分区只从具名权威 Schema metadata 读取一次。
TABLE_NAME = FUTURES_DAILY_SCHEMA.metadata[b"table_name"].decode("utf-8")
CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
PRIMARY_KEY = FUTURES_DAILY_SCHEMA.metadata[b"primary_key"].decode(
    "utf-8"
).split(",")
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
PARTITION_COLUMNS = FUTURES_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")

# 日常规划只读取决定白名单变化和 API 待办所需的窄列。
CALENDAR_PLANNING_COLUMNS = [
    *CALENDAR_PRIMARY_KEY,
    "exchange_code",
    "underlying_code",
    "schedule_status",
    "is_fetch_required",
    "is_fetch_completed",
    "year",
    "month",
]
CALENDAR_PLANNING_SCHEMA = pa.schema([
    FUTURES_BAR_CALENDAR_SCHEMA.field(name)
    for name in CALENDAR_PLANNING_COLUMNS
])

PRICE_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "pre_close",
]
MARKET_VALUE_COLUMNS = [
    "previous_close",
    "previous_settlement",
    "open",
    "high",
    "low",
    "close",
    "settlement",
    "close_change_from_previous_settlement",
    "settlement_change_from_previous_settlement",
    "volume",
    "money",
    "open_interest",
    "open_interest_change",
]

SOURCE_NAME = "JQData_get_price_daily_get_extras_raw"
LEGACY_SOURCE_NAMES = {"JQData_get_price_1d_skip_paused"}
LEGACY_NULL_EXTRA_FIELDS = [
    "previous_close",
    "previous_settlement",
    "settlement",
    "close_change_from_previous_settlement",
    "settlement_change_from_previous_settlement",
    "open_interest_change",
]
SELECTED_REASON = "命中国内期货事实采集白名单；日线事实需要采集。"
EXCLUDED_REASON = (
    "未命中国内期货事实采集白名单；保留理论格点和既有事实，"
    "当前不要求采集日线事实。"
)

MAX_CONTRACTS_PER_REQUEST = 100
MAX_ESTIMATED_VALUES_PER_BATCH = 90_000
LOOKBACK_CALENDAR_DAYS = 45
DEFAULT_QUOTA_RESERVE = 5_000_000

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
        FUTURES_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 日线事实校验与 OHLC 质量判定
# 
# `close_mask()` 用于浮点派生关系比较；`invalid_ohlc_mask()` 识别有限价格之间的 high/low 关系异常。OHLC 关系异常保留来源原值，随后在日历中记 warning；非空 NaN/Inf、负成交量/成交额/持仓量仍拒绝通过。
# 
# `validate_daily_frame()` 检查 Schema、主键、来源、合约与日期分区、缺失占位及派生值。每个采集批调用它，并关闭前序行关系检查；请求组按交易所、年和合约拆分，批次主键范围互斥，入口只拼接并保持全局主键排序，不重复完整校验或 Pandas/Arrow 往返。写入前仍在合并后的 dirty 完整事实叶上执行完整业务校验。staging 与正式安装后只核对物理契约、主键唯一性和行数，不重复业务 validator。

# ### 流程：数值关系判断
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["待比较数值"] --> B["close_mask：容差比较"]
#   C["行情 OHLC"] --> D["invalid_ohlc_mask：high/low 关系"] --> E["返回异常掩码；不修改行情"]
# ```

# In[4]:


def close_mask(
    left: pd.Series,
    right: pd.Series,
) -> pd.Series:
    left_values = left.to_numpy(dtype="float64", na_value=np.nan)
    right_values = right.to_numpy(dtype="float64", na_value=np.nan)
    return pd.Series(
        np.isclose(
            left_values,
            right_values,
            rtol=1e-10,
            atol=1e-8,
            equal_nan=False,
        ),
        index=left.index,
    )


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    comparable_high = frame[["open", "close", "low"]].max(
        axis=1,
        skipna=True,
    )
    comparable_low = frame[["open", "close", "high"]].min(
        axis=1,
        skipna=True,
    )
    high_invalid = (
        frame["high"].notna()
        & comparable_high.notna()
        & frame["high"].lt(comparable_high)
    )
    low_invalid = (
        frame["low"].notna()
        & comparable_low.notna()
        & frame["low"].gt(comparable_low)
    )
    return (high_invalid | low_invalid).astype(bool)


# ### 流程：事实业务校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["转换至权威事实 Schema"] --> B["检查主键、身份、分区和来源"] --> C["检查占位、有限数、数量与派生值"]
#   C --> D{"启用前序行关系检查？"}
#   D -- 是 --> E["按合约日期检查前序关系"] --> F["返回已校验事实"]
#   D -- 否 --> F
# ```

# In[5]:


def validate_daily_frame(
    frame: pd.DataFrame,
    context: str,
    *,
    validate_predecessor_relationships: bool = True,
) -> pd.DataFrame:
    source_df = frame.loc[:, FUTURES_DAILY_SCHEMA.names]

    # Arrow 会把 Pandas NaN 转成 null，因此先在原输入中拒绝 NaN/Inf。
    for column in MARKET_VALUE_COLUMNS:
        explicit_nan = np.fromiter(
            (
                isinstance(value, (float, np.floating))
                and np.isnan(value)
                for value in source_df[column].array
            ),
            dtype=bool,
            count=len(source_df),
        )
        if explicit_nan.any():
            raise ValueError(f"{context}{column} 包含非有限数。")
        original_non_null = source_df[column].notna()
        numeric_values = pd.to_numeric(
            source_df[column], errors="coerce"
        ).to_numpy(dtype="float64", na_value=np.nan)
        if (original_non_null.to_numpy() & ~np.isfinite(numeric_values)).any():
            raise ValueError(f"{context}{column} 包含非有限数。")

    table = pandas_to_arrow(source_df, FUTURES_DAILY_SCHEMA)
    checked_df = arrow_to_pandas(table, FUTURES_DAILY_SCHEMA)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    contract_codes = checked_df["contract_code"].astype("string")
    contract_symbols = contract_codes.str.split(".", n=1, regex=False).str[0]
    contract_exchanges = contract_codes.str.rsplit(".", n=1).str[-1]
    derived_underlying = contract_symbols.str.extract(
        r"^([A-Za-z]+)", expand=False
    ).str.upper()
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(
            checked_df["exchange_code"].astype("string")
        )
    ).any():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    if checked_df["underlying_code"].astype("string").ne(
        derived_underlying
    ).any():
        raise ValueError(f"{context}品种代码无法由合约代码复算。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(
            checked_df["trading_date"].dt.month
        ).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if not checked_df["source"].isin(
        {SOURCE_NAME, *LEGACY_SOURCE_NAMES}
    ).all():
        raise ValueError(f"{context}source 与 JQData 日线契约不一致。")

    legacy_without_extras = checked_df["source"].isin(
        LEGACY_SOURCE_NAMES
    ) & checked_df[LEGACY_NULL_EXTRA_FIELDS].isna().all(axis=1)
    has_market_data = checked_df["has_market_data"].astype(bool)
    if (has_market_data & checked_df["close"].isna()).any():
        raise ValueError(f"{context}有效行情行必须具有 close。")
    if (
        ~has_market_data
        & checked_df[MARKET_VALUE_COLUMNS].notna().any(axis=1)
    ).any():
        raise ValueError(f"{context}缺失占位行的行情度量必须全部为空。")
    for column in ["volume", "money", "open_interest"]:
        if checked_df[column].dropna().lt(0).any():
            raise ValueError(f"{context}{column} 不得为负。")

    previous_settlement = checked_df["previous_settlement"]
    close_value = checked_df["close"]
    settlement = checked_df["settlement"]

    close_change_inputs = (
        ~legacy_without_extras
        & previous_settlement.notna()
        & close_value.notna()
    )
    expected_close_change = close_value - previous_settlement
    actual_close_change = checked_df[
        "close_change_from_previous_settlement"
    ]
    if (
        close_change_inputs
        & (
            actual_close_change.isna()
            | ~close_mask(actual_close_change, expected_close_change)
        )
    ).any():
        raise ValueError(f"{context}收盘价相对昨结涨跌额无法复算。")
    if (
        ~legacy_without_extras
        & ~close_change_inputs
        & actual_close_change.notna()
    ).any():
        raise ValueError(f"{context}收盘涨跌缺少必要输入。")

    settlement_change_inputs = (
        ~legacy_without_extras
        & previous_settlement.notna()
        & settlement.notna()
    )
    expected_settlement_change = settlement - previous_settlement
    actual_settlement_change = checked_df[
        "settlement_change_from_previous_settlement"
    ]
    if (
        settlement_change_inputs
        & (
            actual_settlement_change.isna()
            | ~close_mask(
                actual_settlement_change,
                expected_settlement_change,
            )
        )
    ).any():
        raise ValueError(f"{context}结算价相对昨结涨跌额无法复算。")
    if (
        ~legacy_without_extras
        & ~settlement_change_inputs
        & actual_settlement_change.notna()
    ).any():
        raise ValueError(f"{context}结算涨跌缺少必要输入。")

    # 只有合并后的 dirty 完整叶具有可比较的本地前序事实。API pending
    # 子集可能跨过已经完成的日期，不能把上一 pending 行误作上一交易日。
    if validate_predecessor_relationships:
        known_previous_settlement = checked_df.groupby(
            "contract_code", sort=False
        )["settlement"].transform(lambda series: series.ffill().shift())
        comparable_previous_settlement = (
            ~legacy_without_extras
            & known_previous_settlement.notna()
            & previous_settlement.notna()
        )
        if (
            comparable_previous_settlement
            & ~close_mask(previous_settlement, known_previous_settlement)
        ).any():
            raise ValueError(
                f"{context}previous_settlement 与前序事实不一致。"
            )

        known_previous_position = checked_df.groupby(
            "contract_code", sort=False
        )["open_interest"].transform(lambda series: series.ffill().shift())
        position_change_inputs = (
            ~legacy_without_extras
            & known_previous_position.notna()
            & checked_df["open_interest"].notna()
        )
        expected_position_change = (
            checked_df["open_interest"] - known_previous_position
        )
        actual_position_change = checked_df["open_interest_change"]
        if (
            position_change_inputs
            & (
                actual_position_change.isna()
                | ~close_mask(
                    actual_position_change,
                    expected_position_change,
                )
            )
        ).any():
            raise ValueError(f"{context}持仓变化无法由前序事实复算。")

    return checked_df


# ## 待回写日历完整叶校验
# 
# `validate_calendar_state_frame()` 对本环节准备提交的 b04 完整叶负责：检查结构、枚举、采集完成凭证、实际与缺失条数，以及质量说明和时间。它支持日历 Schema 中的两种频率，但 b05 主流程仅回写 `1d` 叶。
# 
# 这里校验的是合并了政策与本次完成状态的 dirty 输出叶；日常规划信任 b04 已提交的上游业务语义，不重复扫描全历史日历做业务质检。

# ### 流程：日历输出叶校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["转换至日历 Schema"] --> B["结构与枚举约束"] --> C["完成、缺失和质量约束"] --> D["返回完整日历叶"]
# ```

# In[6]:


def validate_calendar_state_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_BAR_CALENDAR_SCHEMA)
    checked_df = checked_df.sort_values(CALENDAR_PRIMARY_KEY).reset_index(
        drop=True
    )
    if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
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
        raise ValueError(f"{context}状态说明字段不得为空。")

    contract_codes = checked_df["contract_code"].astype("string")
    contract_exchanges = contract_codes.str.rsplit(".", n=1).str[-1]
    if (
        ~contract_codes.str.contains(".", regex=False)
        | contract_exchanges.ne(
            checked_df["exchange_code"].astype("string")
        )
    ).any():
        raise ValueError(f"{context}交易所与合约代码后缀不一致。")
    if (
        checked_df["year"].ne(checked_df["trading_date"].dt.year).any()
        or checked_df["month"].ne(
            checked_df["trading_date"].dt.month
        ).any()
    ):
        raise ValueError(f"{context}year/month 与 trading_date 不一致。")
    if checked_df["expected_bar_count"].le(0).any():
        raise ValueError(f"{context}expected_bar_count 必须大于 0。")
    if (
        checked_df["actual_bar_count"].lt(0).any()
        or checked_df["missing_bar_count"].lt(0).any()
    ):
        raise ValueError(f"{context}实际与缺失条数不得为负。")

    daily_df = checked_df.loc[checked_df["bar_frequency"].eq("1d")]
    minute_df = checked_df.loc[checked_df["bar_frequency"].eq("1m")]
    session_columns = [
        "session_text",
        "session_start_at",
        "session_end_at",
        "is_night_session",
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

    confirmed_closed = checked_df["schedule_status"].eq(
        "confirmed_closed"
    )
    if (
        confirmed_closed
        & (
            checked_df["evidence_level"].ne("authoritative")
            | checked_df["is_fetch_required"]
        )
    ).any():
        raise ValueError(f"{context}确认休市必须具有权威证据且不得拉取。")

    completed = checked_df["is_fetch_completed"]
    missing_run_id = (
        checked_df["fetch_run_id"].isna()
        | checked_df["fetch_run_id"].eq("")
    )
    if (completed & (
        missing_run_id | checked_df["fetch_completed_at"].isna()
    )).any():
        raise ValueError(f"{context}完成状态缺少运行批次或完成时间。")
    if (~completed & checked_df["fetch_completed_at"].notna()).any():
        raise ValueError(f"{context}未完成格点不得具有完成时间。")

    unchecked = checked_df["missing_checked_at"].isna()
    if (
        unchecked
        & (
            checked_df["is_data_missing"]
            | checked_df["missing_bar_count"].ne(0)
        )
    ).any():
        raise ValueError(f"{context}未经检查不得记录缺失。")
    expected_missing = (
        checked_df["expected_bar_count"] - checked_df["actual_bar_count"]
    ).clip(lower=0).where(checked_df["is_fetch_required"], 0)
    checked_missing = ~unchecked
    if (
        checked_missing
        & checked_df["missing_bar_count"].ne(expected_missing)
    ).any():
        raise ValueError(f"{context}missing_bar_count 无法复算。")
    if (
        checked_missing
        & checked_df["is_data_missing"].ne(expected_missing.gt(0))
    ).any():
        raise ValueError(f"{context}is_data_missing 与缺失条数不一致。")
    if (
        checked_df["quality_status"].ne("pending")
        & checked_df["quality_checked_at"].isna()
    ).any():
        raise ValueError(f"{context}非 pending 状态缺少检查时间。")
    return checked_df


# ## Dataset 的物理契约与稳定表身份
# 
# `reconstructed_schema()` 按权威字段顺序重建 Dataset 逻辑 Schema；`physically_and_identity_compatible()` 比较字段、类型、nullable，以及表名、主键和分区 metadata。描述性 metadata 以当前契约为准，其差异不触发历史数据重写。
# 
# `open_contract_dataset()` 打开上游根 Dataset，检查逻辑 Schema 和各 fragment 的物理字段；主流程随后用 Arrow filter 裁剪频率及可选日期，只读取规划窄列。写入前的事实和日历读取则使用后面的精确叶路径入口。
# 
# 读取日志由函数自行记录：根 Dataset 沿原有 fragment 遍历报告首个及每 250 个文件的检查进度，最后报告总数；不为日志额外列目录或扫描数据。异常记录失败阶段后原样抛出，可选下游不存在与必需上游缺失分别表达。

# ### 流程：恢复逻辑字段顺序
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["Dataset 逻辑 Schema"] --> B["按权威 Schema 顺序取字段"] --> C["保留 Dataset 表 metadata"]
# ```

# In[7]:


# 将目录分区字段与 Parquet 文件字段重新组合成可和权威契约比较的完整 Schema。
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
#   A["实际与权威 Schema"] --> B["字段名、类型、nullable"] --> C["表名、主键、分区 metadata"] --> D["返回兼容性；忽略描述差异"]
# ```

# In[8]:


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


# ### 流程：根 Dataset 打开
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A{"表路径有 Parquet？"} -- 否 --> B{"必需上游？"}
#   B -- 是 --> C["抛错"]
#   B -- 否 --> D["返回 None"]
#   A -- 是 --> E["记录打开开始；打开 Hive Dataset"] --> F["检查契约；沿原遍历报告文件进度"] --> G["记录完成并返回 Dataset"]
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
        first_parquet = (
            next(table_path.rglob("*.parquet"), None)
            if table_path.is_dir()
            else None
        )
        if first_parquet is None:
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
        if not physically_and_identity_compatible(actual_schema, schema):
            raise TypeError(f"{label} 物理结构或表身份与契约不一致。")

        expected_file_schema = parquet_file_schema(
            schema,
            partitioning.schema.names,
        )
        log_phase = "fragment_schema"
        log_fragment_count = 0
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=started"
        )
        for fragment in dataset.get_fragments():
            if not physically_and_identity_compatible(
                fragment.physical_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label} Parquet fragment 物理结构或表身份与契约不一致："
                    f"{fragment.path}"
                )
            log_fragment_count += 1
            if log_fragment_count == 1 or log_fragment_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=running; "
                    f"checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=completed; "
            f"outcome=ready; checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 窄列白名单规划与状态继承
# 
# `daily_policy_plan()` 在 Arrow 窄表上检查 `1d` 和 `session_number=0`，按共享白名单与 `confirmed_closed` 形成期望 required。政策 dirty 只取 required 布尔值发生变化的行；API pending 只取期望 required 且 `is_fetch_completed=false` 的行，中文说明变化本身不产生 dirty。
# 
# 白名单缩小时，`apply_policy_to_calendar_leaf()` 保留既有事实、完成批次、条数、质量及 b07 证据，只关闭 required 并清零当前缺失。重新纳入时，已有完成证据仍阻止 API；已完成且有缺失检查时间的行按现有条数恢复当前缺失计数。未触达行保留原值。
# 
# `plan_daily_policy()` 为 DataFrame 调用提供转换和可选日期裁剪；主流程直接调用 Arrow 版本，不经过这一入口。
# 
# `daily_policy_plan()` 自行报告政策规划起止及失败阶段，并明确完成依据为 `is_fetch_completed`；`request_batches()` 报告交易所—年份分组及每 100 个合约的组批进度。入口保留 dirty、pending、completed 和批次数量汇总，复用已有计数，不重复求差。

# ### 流程：政策计划与叶内应用
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录规划开始；日线规划窄表"] --> B["白名单且未确认休市 → 期望 required"]
#   B --> C["required 改变 → dirty"]
#   B --> D["required 且未完成 → pending"]
#   B --> E["required 且已完成 → completed；记录规划完成"]
#   C --> F["写入准备时按主键匹配当前日历叶"]
#   F --> G["更新 required/原因与当前缺失；保留完成和质量证据"]
# ```

# In[10]:


def daily_policy_plan(
    planning_table: pa.Table,
) -> tuple[pa.Table, pa.Array, pa.Array, pa.Array]:
    """用 Arrow compute 形成 dirty、pending 和 completed 精确掩码。"""
    log_started_at = time.perf_counter()
    log_phase = "input"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=daily_policy_plan; phase=policy_plan; status=started; "
        f"rows={len(planning_table)}; completion_source=is_fetch_completed"
    )
    try:
        planned_table = planning_table.cast(
            CALENDAR_PLANNING_SCHEMA, safe=True
        ).sort_by([(name, "ascending") for name in CALENDAR_PRIMARY_KEY])
        if bool(
            pc.any(pc.not_equal(planned_table["bar_frequency"], "1d")).as_py()
        ):
            raise ValueError("日线窄列规划只能包含 1d 格点。")
        if bool(
            pc.any(pc.not_equal(planned_table["session_number"], 0)).as_py()
        ):
            raise ValueError("日线窄列规划 session_number 必须为 0。")

        log_phase = "whitelist"
        desired_required = pa.array(
            np.zeros(len(planned_table), dtype=bool)
        )
        selected_underlyings_by_exchange: dict[str, list[str]] = {}
        for exchange_code, underlying_code in FUTURES_FACT_VARIETY_PAIRS:
            selected_underlyings_by_exchange.setdefault(
                exchange_code, []
            ).append(underlying_code)
        for exchange_code, underlying_codes in sorted(
            selected_underlyings_by_exchange.items()
        ):
            exchange_mask = pc.equal(
                planned_table["exchange_code"], exchange_code
            )
            underlying_mask = pc.is_in(
                planned_table["underlying_code"],
                value_set=pa.array(
                    sorted(underlying_codes),
                    type=planned_table["underlying_code"].type,
                ),
            )
            desired_required = pc.or_(
                desired_required,
                pc.and_(exchange_mask, underlying_mask),
            )
        log_phase = "completion_evidence"
        confirmed_closed = pc.equal(
            planned_table["schedule_status"], "confirmed_closed"
        )
        desired_required = pc.and_(
            desired_required, pc.invert(confirmed_closed)
        )
        policy_changed = pc.not_equal(
            planned_table["is_fetch_required"], desired_required
        )
        completion_evidence = planned_table["is_fetch_completed"]
        dirty_mask = policy_changed
        pending_mask = pc.and_(
            desired_required, pc.invert(completion_evidence)
        )
        completed_mask = pc.and_(desired_required, completion_evidence)
        desired_reason = pc.if_else(
            confirmed_closed,
            "权威证据确认休市；当前无需拉取日线事实。",
            pc.if_else(desired_required, SELECTED_REASON, EXCLUDED_REASON),
        )
        planned_table = planned_table.append_column(
            "desired_is_fetch_required", desired_required
        ).append_column("desired_selection_reason", desired_reason)
        planned_table = planned_table.append_column(
            "policy_changed", policy_changed
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=daily_policy_plan; phase=policy_plan; status=completed; "
            f"rows={len(planned_table)}; completion_source=is_fetch_completed; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return planned_table, dirty_mask, pending_mask, completed_mask
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=daily_policy_plan; phase=policy_plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def plan_daily_policy(
    planning_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    planning_table = pandas_to_arrow(
        planning_df.loc[:, CALENDAR_PLANNING_COLUMNS],
        CALENDAR_PLANNING_SCHEMA,
    )
    if start_date is not None:
        date_mask = pc.and_(
            pc.greater_equal(planning_table["trading_date"], start_date),
            pc.less_equal(planning_table["trading_date"], end_date),
        )
        planning_table = planning_table.filter(date_mask)
    planned_table, dirty_mask, pending_mask, completed_mask = (
        daily_policy_plan(planning_table)
    )
    return tuple(
        planned_table.filter(mask).to_pandas(types_mapper=pd.ArrowDtype)
        for mask in (dirty_mask, pending_mask, completed_mask)
    )


def apply_policy_to_calendar_leaf(
    calendar_df: pd.DataFrame,
    dirty_policy_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    if dirty_policy_df.empty:
        return calendar_df.copy()

    desired_df = calendar_df.copy()
    calendar_index = pd.MultiIndex.from_frame(
        desired_df[CALENDAR_PRIMARY_KEY]
    )
    update_df = dirty_policy_df.set_index(CALENDAR_PRIMARY_KEY)
    if update_df.index.has_duplicates:
        raise ValueError("白名单 dirty 规划主键不唯一。")
    missing_keys = update_df.index.difference(calendar_index)
    if len(missing_keys):
        raise ValueError(
            f"白名单 dirty 格点无法匹配日历叶：{missing_keys[:5].tolist()}"
        )

    matched = calendar_index.isin(update_df.index)
    matched_index = calendar_index[matched]
    desired_required = update_df.loc[
        matched_index, "desired_is_fetch_required"
    ].to_numpy(dtype=bool)
    policy_changed = update_df.loc[
        matched_index, "policy_changed"
    ].to_numpy(dtype=bool)
    matched_rows = desired_df.index[matched]

    desired_df.loc[
        matched_rows, "is_fetch_required"
    ] = desired_required
    reason_rows = matched_rows[policy_changed]
    reason_values = update_df.loc[
        matched_index[policy_changed],
        "desired_selection_reason",
    ].to_numpy()
    desired_df.loc[reason_rows, "selection_reason"] = reason_values

    required_rows = matched_rows[desired_required]
    required_completed = (
        desired_df.loc[required_rows, "is_fetch_completed"].astype(bool)
        & desired_df.loc[required_rows, "missing_checked_at"].notna()
    )
    required_missing = (
        desired_df.loc[required_rows, "expected_bar_count"].astype("int64")
        - desired_df.loc[required_rows, "actual_bar_count"].astype("int64")
    ).clip(lower=0).where(required_completed, 0)
    desired_df.loc[required_rows, "missing_bar_count"] = (
        required_missing.to_numpy()
    )
    desired_df.loc[required_rows, "is_data_missing"] = (
        required_missing.gt(0).to_numpy()
    )

    excluded_rows = matched_rows[~desired_required]
    desired_df.loc[excluded_rows, "is_data_missing"] = False
    desired_df.loc[excluded_rows, "missing_bar_count"] = 0
    desired_df.loc[matched_rows, "updated_at"] = updated_at
    return desired_df


# ## 请求分批与逐批配额检查
# 
# `request_batches()` 先按交易所和年份分组，再按合约顺序组批，通常每批至多 100 个合约、估计返回量至多 90,000；单合约即使超过估计量上限仍形成一批。请求起点向前回看 45 个自然日，为上一有效结算价和持仓量提供上下文，回看日期不增加输出键。
# 
# `quota_spare()` 只读取可用额度 spare；接口不存在或返回格式不支持时返回 `None`，不把未知额度当成零。入口在每批请求前检查预计消耗后是否仍满足 `--quota-reserve`（默认 5,000,000）。额度不足则停止后续请求；已采集结果仍进入汇总，并由 `--write` 决定是否与政策变化一起提交。
# 
# 回看 `timedelta` 在分组与合约循环前构造一次，所有批次复用同一长度。

# ### 流程：构造请求批次
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["pending 按交易所、年分组"] --> B["逐合约估算含回看期返回量；报告组批进度"]
#   B --> C{"已有合约且加入后超限？"}
#   C -- 是 --> D["保存上一批；当前合约开始新批"]
#   C -- 否 --> E["加入当前批"]
#   D --> F["继续合约；组尾保存剩余批次"]
#   E --> F
#   F --> G["记录完成与批次数；返回计划"]
# ```

# In[11]:


def request_batches(
    pending_df: pd.DataFrame,
) -> list[dict[str, object]]:
    log_started_at = time.perf_counter()
    log_phase = "request_plan"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=started; "
        f"pending={len(pending_df)}"
    )
    try:
        batches = []
        lookback_delta = timedelta(days=LOOKBACK_CALENDAR_DAYS)

        # 先按交易所和年份隔离请求，便于定位错误，也控制最长日期跨度。
        for (exchange_code, year), group_df in pending_df.groupby(
            ["exchange_code", "year"],
            sort=True,
        ):
            log_phase = "group_batches"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=running; "
                f"exchange={exchange_code}; year={year}; pending={len(group_df)}; batches={len(batches)}"
            )
            contract_codes = sorted(group_df["contract_code"].unique().tolist())
            current_codes: list[str] = []
            log_contract_count = 0

            # 逐个试放合约；超过任一安全边界时先落定上一批。
            for contract_code in contract_codes:
                tentative_codes = [*current_codes, contract_code]
                tentative_df = group_df.loc[
                    group_df["contract_code"].isin(tentative_codes)
                ]
                request_start = (
                    tentative_df["trading_date"].min()
                    - lookback_delta
                )
                request_end = tentative_df["trading_date"].max()
                natural_days = (request_end - request_start).days + 1
                estimated_values = natural_days * len(tentative_codes) * 3

                # 任一安全边界达到就结束当前批；单合约即使跨度大也必须能够执行。
                should_flush = current_codes and (
                    len(tentative_codes) > MAX_CONTRACTS_PER_REQUEST
                    or estimated_values > MAX_ESTIMATED_VALUES_PER_BATCH
                )
                if should_flush:
                    # 当前合约留给下一批，已经累计的合约先形成一个完整请求计划。
                    selected_df = group_df.loc[
                        group_df["contract_code"].isin(current_codes)
                    ].reset_index(drop=True)
                    start_value = (
                        selected_df["trading_date"].min()
                        - lookback_delta
                    )
                    end_value = selected_df["trading_date"].max()
                    batches.append(
                        {
                            "exchange_code": exchange_code,
                            "year": int(year),
                            "contract_codes": current_codes,
                            "pending_df": selected_df,
                            "request_start": start_value,
                            "request_end": end_value,
                            "estimated_values": (
                                (end_value - start_value).days + 1
                            )
                            * len(current_codes)
                            * 3,
                        }
                    )
                    current_codes = []

                current_codes.append(contract_code)
                log_contract_count += 1
                if log_contract_count == 1 or log_contract_count % 100 == 0 or log_contract_count == len(contract_codes):
                    click.echo(
                        f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=running; "
                        f"exchange={exchange_code}; year={year}; contracts={log_contract_count}/{len(contract_codes)}; "
                        f"finalized_batches={len(batches)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )

            if current_codes:
                # 分组末尾仍未触发阈值的合约组成最后一批。
                selected_df = group_df.loc[
                    group_df["contract_code"].isin(current_codes)
                ].reset_index(drop=True)
                start_value = (
                    selected_df["trading_date"].min()
                    - lookback_delta
                )
                end_value = selected_df["trading_date"].max()
                batches.append(
                    {
                        "exchange_code": exchange_code,
                        "year": int(year),
                        "contract_codes": current_codes,
                        "pending_df": selected_df,
                        "request_start": start_value,
                        "request_end": end_value,
                        "estimated_values": (
                            (end_value - start_value).days + 1
                        )
                        * len(current_codes)
                        * 3,
                    }
                )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=completed; "
            f"pending={len(pending_df)}; batches={len(batches)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return batches
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 流程：读取可用配额
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A{"SDK 有配额查询？"} -- 否 --> B["返回 None"]
#   A -- 是 --> C["查询额度"] --> D{"返回 dict 且 spare 非空？"}
#   D -- 是 --> E["返回整数 spare"]
#   D -- 否 --> B
# ```

# In[12]:


def quota_spare(jqdata: ModuleType) -> int | None:
    get_query_count = getattr(jqdata, "get_query_count", None)
    if get_query_count is None:
        return None

    # 旧版 SDK 可能没有配额查询；此时不凭空假定一个剩余额度。
    # 只读取 spare；total 不能代表当前仍可消费的返回量。
    quota = get_query_count()
    if isinstance(quota, dict) and quota.get("spare") is not None:
        return int(quota["spare"])
    return None


# ## 来源响应归一化
# 
# `normalize_price_response()` 将日线响应统一为合约—日期长表；`normalize_extra_response()` 将结算价或持仓量的日期×合约宽表转成长表。非空响应检查请求列、合约集合、重复键及数值边界；请求日期范围在 `collect_batch()` 中核对。
# 
# 空 DataFrame 是可接受的空结果；`None` 或不支持的响应类型抛出异常。主流程没有自动重试，异常停止后续工作。
# 
# 价格响应缺少 time 列时，仍通过 `rename_axis("time").reset_index()` 恢复；成功后必有 time 列，删除其后不可达的再次改名分支。

# ### 流程：日线响应归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["get_price 响应"] --> B{"合法 DataFrame？"}
#   B -- 否 --> X["抛错"]
#   B -- 是 --> C{"空响应？"}
#   C -- 是 --> D["返回空长表"]
#   C -- 否 --> E["整理日期与合约；检查列和请求合约"] --> F["检查数值与重复键；返回长表"]
# ```

# In[13]:


def normalize_price_response(
    raw_df: pd.DataFrame,
    contract_codes: list[str],
) -> pd.DataFrame:
    columns = ["contract_code", "trading_date", *PRICE_FIELDS]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: get_price 未返回 DataFrame。")
    # 空 DataFrame 是合法的明确空响应，保留固定列供日历左连接生成占位行。
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    # 单合约响应通常把时间放在索引，多合约 panel=False 则显式返回 code/time。
    normalized_df = raw_df.copy()
    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename_axis("time").reset_index()

    if "code" not in normalized_df.columns:
        if len(contract_codes) != 1:
            raise ValueError("schema_error: 多合约 get_price 缺少 code。")
        normalized_df["code"] = contract_codes[0]

    # 请求字段必须全部返回；缺列是响应契约错误，不应被当作无行情。
    required_columns = {"time", "code", *PRICE_FIELDS}
    missing_columns = required_columns - set(normalized_df.columns)
    if missing_columns:
        raise ValueError(
            f"schema_error: get_price 缺列 {sorted(missing_columns)}"
        )

    normalized_df = normalized_df.loc[
        :, ["code", "time", *PRICE_FIELDS]
    ].rename(columns={"code": "contract_code"})
    normalized_df["contract_code"] = normalized_df["contract_code"].astype(str)
    normalized_df["trading_date"] = pd.to_datetime(
        normalized_df["time"],
        errors="raise",
    ).dt.date

    unexpected_codes = set(normalized_df["contract_code"]) - set(contract_codes)
    if unexpected_codes:
        raise ValueError(
            f"schema_error: get_price 返回未请求合约 {sorted(unexpected_codes)}"
        )

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
    for column in ["volume", "money"]:
        if normalized_df[column].dropna().lt(0).any():
            raise ValueError(
                f"schema_error: get_price {column} 不得为负。"
            )

    normalized_df = normalized_df.loc[:, columns]
    if normalized_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("schema_error: get_price 返回重复合约日。")
    return normalized_df


# ### 流程：扩展字段响应归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["get_extras 响应"] --> B{"合法 DataFrame？"}
#   B -- 否 --> X["抛错"]
#   B -- 是 --> C{"空响应？"}
#   C -- 是 --> D["返回空长表"]
#   C -- 否 --> E["检查请求合约列；宽表转长表"] --> F["检查数值与重复键；返回长表"]
# ```

# In[14]:


def normalize_extra_response(
    raw_df: pd.DataFrame,
    contract_codes: list[str],
    value_column: str,
) -> pd.DataFrame:
    columns = ["contract_code", "trading_date", value_column]
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError(f"schema_error: {value_column} 未返回 DataFrame。")
    # 空 DataFrame 是合法的明确空响应，保留固定列供日历左连接生成占位行。
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    # get_extras 返回“日期 × 合约”的宽表；请求的每个合约都必须有一列。
    response_codes = set(map(str, raw_df.columns))
    missing_codes = set(contract_codes) - response_codes
    if missing_codes:
        raise ValueError(
            f"schema_error: {value_column} 缺少合约列 {sorted(missing_codes)}"
        )
    unexpected_codes = response_codes - set(contract_codes)
    if unexpected_codes:
        raise ValueError(
            f"schema_error: {value_column} 返回未请求合约 "
            f"{sorted(unexpected_codes)}"
        )

    normalized_df = raw_df.copy()
    normalized_df.columns = [str(column) for column in normalized_df.columns]
    # 转成长表后，三类响应都统一为 contract_code + trading_date 主键。
    normalized_df.index.name = "trading_date"
    normalized_df = normalized_df.reset_index().melt(
        id_vars="trading_date",
        value_vars=contract_codes,
        var_name="contract_code",
        value_name=value_column,
    )
    normalized_df["trading_date"] = pd.to_datetime(
        normalized_df["trading_date"],
        errors="raise",
    ).dt.date
    # get_extras 的宽表用浮点 NaN 表示某个合约日没有扩展值。
    # 该字段本身允许为空，所以 None、pd.NA 和 NaN 都归一为 Arrow null；Inf 仍是来源异常。
    for value in normalized_df[value_column]:
        if value is None or value is pd.NA or pd.isna(value):
            continue
        try:
            is_finite = math.isfinite(float(value))
        except (TypeError, ValueError):
            # 非数值文本由下面 errors=raise 的转换给出明确契约错误。
            continue
        if not is_finite:
            raise ValueError(
                f"schema_error: {value_column} 含非有限数。"
            )
    normalized_df[value_column] = pd.to_numeric(
        normalized_df[value_column],
        errors="raise",
    ).astype("Float64")
    if (
        value_column == "open_interest"
        and normalized_df[value_column].dropna().lt(0).any()
    ):
        raise ValueError(
            "schema_error: open_interest 不得为负。"
        )

    if normalized_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"schema_error: {value_column} 返回重复合约日。")
    return normalized_df.loc[:, columns]


# ## 一批来源采集、派生值计算与待办对齐
# 
# `collect_batch()` 顺序调用一次 `get_price(frequency="daily")` 和两次 `get_extras()`，分别取得价格与量额、结算价、持仓量。先在含回看期的扩展字段序列上计算上一有效值与变化，再将结果左连接到本批待办键。
# 
# 没有有效收盘价的待办保留一行 `has_market_data=false` 占位，行情指标全空；有效行情保留来源数值，有限 OHLC 关系异常不改值。函数完成本批业务校验后返回事实与返回量计数；`collect_batch()` 自行报告三次 API 的 0/3—3/3 进度、归一化、日期门禁、派生、待办对齐和输出校验，最后报告事实行数及耗时。`api_request` 完成仅表示接口已返回非 None，整批 `api_success` 只在输出校验通过后记录；异常记录实际阶段后原样抛出。入口只记录批次调度及累计完成量。
# 
# 三类归一化响应分别检查主键唯一性，外连接保持这一性质，待办键由 b04 及请求拆分保证唯一，因此三次 merge 不再重复执行 `validate="one_to_one"`。来源归一化仍检查包含回看期的全部响应；输出校验仍覆盖新生成的事实，不能用最终待办子集校验替代来源门禁。

# ### 流程：采集并生成一批事实
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录采集开始；三次 API 各报告进度"] --> B["报告归一化及请求日期门禁"]
#   B --> C["在回看序列上计算上一结算与持仓及变化"]
#   C --> D["左连接本批 pending 键"] --> E{"有有效收盘价？"}
#   E -- 是 --> F["保留来源行情与派生值"]
#   E -- 否 --> G["行情全空；标记缺失占位"]
#   F --> H["本批校验；暂不检查前序行关系"]
#   G --> H
#   H --> I["校验通过后记录 api_success；返回结果"]
# ```

# In[15]:


def collect_batch(
    jqdata: ModuleType,
    batch: dict[str, object],
    updated_at: datetime,
) -> tuple[pd.DataFrame, int]:
    log_started_at = time.perf_counter()
    log_phase = "batch_scope"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=collect; status=started; "
        f"contracts={len(batch['contract_codes'])}; pending={len(batch['pending_df'])}; start={batch['request_start']}; end={batch['request_end']}"
    )
    try:
        # 批次字典已经由 request_batches 固化；这里不重新扩大日期或合约范围。
        contract_codes = batch["contract_codes"]
        request_start = batch["request_start"]
        request_end = batch["request_end"]
        pending_df = batch["pending_df"]

        # 一批固定执行一次日线和两次扩展字段请求；任一 None 都视为可重试失败。
        log_phase = "get_price"
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=started; "
            f"api=get_price; completed=0; total=3; contracts={len(contract_codes)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raw_price_df = jqdata.get_price(
            contract_codes,
            start_date=request_start,
            end_date=request_end,
            frequency="daily",
            fields=PRICE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
            fill_paused=False,
            round=False,
        )
        if raw_price_df is None:
            raise RuntimeError("retryable_error: get_price 返回 None。")
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=completed; "
            f"api=get_price; completed=1; total=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        log_phase = "futures_sett_price"
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=started; "
            f"api=futures_sett_price; completed=1; total=3; contracts={len(contract_codes)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raw_settlement_df = jqdata.get_extras(
            "futures_sett_price",
            contract_codes,
            start_date=request_start,
            end_date=request_end,
            df=True,
        )
        if raw_settlement_df is None:
            raise RuntimeError("retryable_error: futures_sett_price 返回 None。")
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=completed; "
            f"api=futures_sett_price; completed=2; total=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        log_phase = "futures_positions"
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=started; "
            f"api=futures_positions; completed=2; total=3; contracts={len(contract_codes)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raw_position_df = jqdata.get_extras(
            "futures_positions",
            contract_codes,
            start_date=request_start,
            end_date=request_end,
            df=True,
        )
        if raw_position_df is None:
            raise RuntimeError("retryable_error: futures_positions 返回 None。")
        click.echo(
            f"fetch_progress: table={TABLE_NAME}; function=collect_batch; phase=api_request; status=completed; "
            f"api=futures_positions; completed=3; total=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        # 三类响应先各自标准化，再按合约—日期一对一合并。
        log_phase = "normalize_responses"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=normalize_responses; status=started; "
            f"pending={len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        price_df = normalize_price_response(raw_price_df, contract_codes)
        settlement_df = normalize_extra_response(
            raw_settlement_df,
            contract_codes,
            "settlement",
        )
        position_df = normalize_extra_response(
            raw_position_df,
            contract_codes,
            "open_interest",
        )

        # 所有来源日期都必须落在本次明确请求窗口内；越界响应是来源契约错误。
        log_phase = "response_dates"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=response_dates; status=started; "
            f"pending={len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for response_name, response_df in [
            ("get_price", price_df),
            ("futures_sett_price", settlement_df),
            ("futures_positions", position_df),
        ]:
            if (
                response_df["trading_date"].lt(request_start).any()
                or response_df["trading_date"].gt(request_end).any()
            ):
                raise ValueError(
                    f"schema_error: {response_name} 返回日期越出请求范围。"
                )

        # 结算价和持仓量允许某一侧缺值，所以使用外连接保留完整扩展字段日期轴。
        log_phase = "derive_values"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=derive_values; status=started; "
            f"pending={len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        extras_df = settlement_df.merge(
            position_df,
            on=PRIMARY_KEY,
            how="outer",
        ).sort_values(PRIMARY_KEY)

        # 先在包含 45 天回看窗口的完整序列上派生，再裁回真正待办键。
        # 因此本批首个待办日也能取得请求窗口内的上一有效值。
        if extras_df.empty:
            # 明确创建派生列，使全空批次仍具有稳定的合并结构。
            extras_df["previous_settlement"] = pd.Series(dtype="float64")
            extras_df["open_interest_change"] = pd.Series(dtype="float64")
        else:
            extras_df["previous_settlement"] = extras_df.groupby(
                "contract_code",
                sort=False,
            )["settlement"].transform(lambda values: values.ffill().shift())
            previous_position = extras_df.groupby(
                "contract_code",
                sort=False,
            )["open_interest"].transform(lambda values: values.ffill().shift())
            extras_df["open_interest_change"] = (
                extras_df["open_interest"] - previous_position
            )

        # 三类 API 结果统一到同一合约—日期轴；真正输出范围仍由下方日历左表决定。
        api_df = price_df.merge(
            extras_df,
            on=PRIMARY_KEY,
            how="outer",
        )

        base_columns = [
            "contract_code",
            "exchange_code",
            "underlying_code",
            "trading_date",
            "year",
            "month",
        ]
        # 日历是输出键的唯一驱动方：API 多返回的回看日期不会写入，少返回则形成占位。
        log_phase = "align_pending"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=align_pending; status=started; "
            f"pending={len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        daily_df = pending_df.loc[:, base_columns].merge(
            api_df,
            on=PRIMARY_KEY,
            how="left",
        )

        daily_df = daily_df.rename(columns={"pre_close": "previous_close"})
        daily_df["close_change_from_previous_settlement"] = (
            daily_df["close"] - daily_df["previous_settlement"]
        )
        daily_df["settlement_change_from_previous_settlement"] = (
            daily_df["settlement"] - daily_df["previous_settlement"]
        )
        daily_df["has_market_data"] = daily_df["close"].notna()
        daily_df["source"] = SOURCE_NAME
        daily_df["updated_at"] = updated_at

        # close 是“有行情”的最小判据；确认空结果必须清空全部行情度量，避免半行数据。
        missing_mask = ~daily_df["has_market_data"]
        daily_df[MARKET_VALUE_COLUMNS] = daily_df[
            MARKET_VALUE_COLUMNS
        ].astype("Float64")
        daily_df.loc[missing_mask, MARKET_VALUE_COLUMNS] = pd.NA

        # API 子集先执行逐行和请求边界门禁；跨日前序关系留到 dirty 完整叶。
        log_phase = "validate_output"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=validate_output; status=started; "
            f"pending={len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        checked_df = validate_daily_frame(
            daily_df.loc[:, FUTURES_DAILY_SCHEMA.names],
            "JQData 转换结果",
            validate_predecessor_relationships=False,
        )

        # 配额审计按实际返回的日线行和两类非空扩展值统计。
        returned_value_count = (
            len(price_df)
            + int(settlement_df["settlement"].notna().sum())
            + int(position_df["open_interest"].notna().sum())
        )
        click.echo(
            f"api_success: table={TABLE_NAME}; function=collect_batch; phase=collect; status=completed; "
            f"requests=3; fact_rows={len(checked_df)}; returned_values={returned_value_count}; "
            f"contracts={len(contract_codes)}; pending={len(pending_df)}; start={request_start}; end={request_end}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return checked_df, returned_value_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=collect_batch; phase=collect; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 文件 Schema 与精确完整叶读取
# 
# `parquet_file_schema()` 从文件 Schema 排除由 Hive 目录承载的分区列；`partition_relative_path()` 按分区字段顺序构造目标路径。`read_partition_leaf()` 直接打开这个叶目录，结合表根恢复 Hive 分区字段，检查物理契约后读取完整叶。
# 
# 不存在或没有 Parquet 的叶返回契约化空表，事实准备可据此建叶；日历准备要求 b04 叶已经存在，空表会报错。此处没有重新打开事实表根，也没有逐叶扫描全表。
# 
# `read_partition_leaf()` 自行报告目标分区、完整叶扫描、读取行数和耗时。缺叶、成功读到零行与读取异常分别表达；日志不引入额外叶读取。

# ### 流程：精确读取叶分区
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录读取开始；构造 Hive 叶路径"] --> B{"叶存在且有 Parquet？"}
#   B -- 否 --> C["记录缺叶；返回契约化空表"]
#   B -- 是 --> D["以表根恢复 Hive 列并打开叶 Dataset"] --> E["物理与逻辑契约检查"] --> F["读取并转换；报告行数及耗时"]
# ```

# In[16]:


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


def partition_relative_path(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> pathlib.Path:
    return pathlib.Path(*[
        f"{name}={value}"
        for name, value in zip(
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
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "leaf_discovery"

    click.echo(
        f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=started; "
        f"partition={partition_key}; path={table_path}"
    )
    try:
        relative_path = partition_relative_path(
            partition_columns,
            partition_key,
        )
        leaf_path = table_path / relative_path
        if not leaf_path.is_dir() or not next(leaf_path.glob("*.parquet"), None):
            click.echo(
                f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
                f"partition={partition_key}; outcome=absent_leaf; rows=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_pandas(schema)

        log_phase = "leaf_schema"
        leaf_dataset = ds.dataset(
            leaf_path,
            format="parquet",
            partitioning=partitioning,
            partition_base_dir=str(table_path),
        )
        expected_file_schema = parquet_file_schema(schema, partition_columns)
        for fragment in leaf_dataset.get_fragments():
            if not physically_and_identity_compatible(
                fragment.physical_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"正式叶 {partition_key} 包含不兼容 Parquet fragment："
                    f"{fragment.path}"
                )
        actual_schema = reconstructed_schema(leaf_dataset, schema)
        if not physically_and_identity_compatible(actual_schema, schema):
            raise TypeError(
                f"正式叶 {partition_key} 物理结构或表身份与契约不一致。"
            )
        log_phase = "leaf_scan"
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=leaf_scan; status=started; "
            f"partition={partition_key}; columns={len(schema.names)}"
        )
        leaf_table = leaf_dataset.to_table(columns=schema.names)
        log_phase = "schema_conversion"
        leaf_df = arrow_to_pandas(leaf_table, schema)
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
            f"partition={partition_key}; outcome=read; rows={len(leaf_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return leaf_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 事实与日历叶的协调提交
# 
# `commit_validated_leaf_group()` 接收已经过完整业务校验的叶规格，检查非空、分区范围和目标唯一性，再转换为 Arrow 并核对表根零行标记。写入 staging 后，仅复读文件物理契约、主键唯一性和行数；全部通过才进入共享 `StagedPathTransaction` 安装。
# 
# 一组包含一个 `1d—交易所—年月` 日历叶及其本次触达的全部事实叶，整个组共用一次事务。缺失的 `schema.parquet` 先写入 staging，再纳入同组安装，并正式复读确认物理契约及零行要求；已有兼容标记保持原样，描述性 metadata 差异不触发改写。
# 
# 正式安装后仍只精确复读本组叶的物理契约、主键和行数。安装或正式复读失败时，共享模块按实际移动记录逆序移除本次新建标记、隔离失败新叶并恢复旧叶；恢复不完整时保留备份。此前已经成功提交的其他组不回滚。这是进程内协调恢复，不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。
# 
# 规格、标记门禁、staging 写入及复读、安装和正式复读日志仍由本函数记录。staging 准备失败由本函数清理；事务内失败由共享模块恢复并报告结果，本函数随后报告失败阶段及耗时。只有整组正式验收通过并退出事务后才发布 `persisted=true` 和组完成日志；入口仅保留整批累计提交量。
# 
# 规格准备阶段为每个叶构造一次文件 Schema，后续零行标记检查、staging 复读、标记创建和正式复读均复用它；不改变这些不同 I/O 阶段各自的验收。准备阶段的 Pandas→Arrow 转换仍是实际写 Parquet 所需，不移除。

# ### 流程：单组暂存、共享安装与失败恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["记录提交开始；已校验叶规格"] --> B["检查目标；预备文件 Schema 并核对标记"]
#   B --> C["写 staging；复核物理契约、主键与行数"]
#   C -- 失败 --> D["清理 staging 并抛错"]
#   C -- 通过 --> E["共享事务安装整组叶；暂存、安装并验收新标记"]
#   E --> F["精确复读本组正式叶"]
#   F -- 通过 --> G["清理后报告组完成及状态已落盘"]
#   E -- 失败 --> H["共享模块逆序移除新标记、隔离新叶、恢复旧叶"]
#   F -- 失败 --> H
#   H --> I["记录恢复结果；保留现场并抛错"]
# ```

# In[17]:


def commit_validated_leaf_group(
    partition_specs: list[dict[str, object]],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "prepare_specs"
    log_installed_leaves = 0
    click.echo(
        f"partition_start: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=started; "
        f"leaves={len(partition_specs)}; lake_root={lake_root}"
    )
    try:
        if not partition_specs:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=completed; "
                f"outcome=no_specs; leaves=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        silver_root = lake_root.resolve() / "silver"
        run_id = uuid.uuid4().hex[:12]
        staging_root = silver_root / f".c05s-{run_id}"
        backup_root = silver_root / f".c05b-{run_id}"
        quarantine_root = silver_root / f".c05q-{run_id}"
        for path in (staging_root, backup_root, quarantine_root):
            if not path.resolve().is_relative_to(silver_root):
                raise ValueError(f"事务路径越出 silver 根目录：{path}")

        prepared_specs = []
        destinations = set()
        for spec in partition_specs:
            frame = spec["frame"]
            schema = spec["schema"]
            partition_columns = spec["partition_columns"]
            partition_key = spec["partition_key"]
            table_name = spec["table_name"]
            if frame.empty:
                raise ValueError("日常增量提交不得写空叶分区。")
            actual_keys = set(
                frame[partition_columns].itertuples(index=False, name=None)
            )
            if actual_keys != {partition_key}:
                raise ValueError("待提交数据越出指定 Hive 分区。")
            complete_table = pandas_to_arrow(frame, schema)
            relative_path = partition_relative_path(
                partition_columns,
                partition_key,
            )
            destination_key = (table_name, relative_path.as_posix())
            if destination_key in destinations:
                raise ValueError(f"事务包含重复目标叶：{destination_key}")
            destinations.add(destination_key)
            prepared_specs.append({
                **spec,
                "complete_table": complete_table,
                "relative_path": relative_path,
                "file_schema": parquet_file_schema(schema, partition_columns),
            })

        log_phase = "schema_marker"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=schema_marker; status=started; "
            f"run_id={run_id}; leaves={len(prepared_specs)}"
        )
        silver_root.mkdir(parents=True, exist_ok=True)
        checked_tables = set()
        for spec in prepared_specs:
            table_name = spec["table_name"]
            if table_name in checked_tables:
                continue
            checked_tables.add(table_name)
            target_table_path = silver_root / table_name
            marker_path = target_table_path / "schema.parquet"
            expected_marker_schema = spec["file_schema"]
            if marker_path.exists():
                marker_metadata = pq.read_metadata(marker_path)
                if marker_metadata.num_rows:
                    raise ValueError("schema.parquet 必须是 0 行契约标记。")
                if not physically_and_identity_compatible(
                    pq.read_schema(marker_path),
                    expected_marker_schema,
                ):
                    raise TypeError(
                        f"{table_name} schema.parquet 与权威契约不一致。"
                    )
            elif target_table_path.is_dir() and next(
                target_table_path.rglob("*.parquet"),
                None,
            ) is not None:
                raise FileNotFoundError(
                    f"{table_name} 已有数据但缺少 schema.parquet。"
                )

        log_phase = "staging_write"
        log_written_leaves = 0
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=staging_write; status=started; "
            f"run_id={run_id}; leaves={len(prepared_specs)}"
        )
        staging_root.mkdir(parents=True, exist_ok=False)
        try:
            for spec in prepared_specs:
                staging_table_path = staging_root / spec["table_name"]
                ds.write_dataset(
                    spec["complete_table"],
                    staging_table_path,
                    format="parquet",
                    partitioning=spec["partitioning"],
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
                log_written_leaves += 1
                click.echo(
                    f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=staging_write; status=running; "
                    f"partition={spec['partition_key']}; completed={log_written_leaves}; total={len(prepared_specs)}; "
                    f"run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

            # staging 只核对物理 Schema、主键唯一和行数，不重复业务 validator。
            for spec in prepared_specs:
                staging_table_path = staging_root / spec["table_name"]
                log_phase = "staging_readback"
                click.echo(
                    f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=staging_readback; status=started; "
                    f"partition={spec['partition_key']}; run_id={run_id}"
                )
                staging_leaf_path = staging_table_path / spec["relative_path"]
                parquet_files = list(staging_leaf_path.glob("*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(
                        f"staging 缺少叶分区：{staging_leaf_path}"
                    )
                expected_file_schema = spec["file_schema"]
                for parquet_path in parquet_files:
                    if not physically_and_identity_compatible(
                        pq.read_schema(parquet_path),
                        expected_file_schema,
                    ):
                        raise TypeError(
                            f"staging 文件 Schema 不一致：{parquet_path}"
                        )
                staged_dataset = ds.dataset(
                    staging_leaf_path,
                    format="parquet",
                    partitioning=spec["partitioning"],
                    partition_base_dir=str(staging_table_path),
                )
                staged_key_table = staged_dataset.to_table(
                    columns=spec["primary_key"]
                )
                if len(staged_key_table) != len(spec["complete_table"]):
                    raise ValueError("staging 行数与待提交完整叶不一致。")
                if staged_key_table.to_pandas().duplicated(
                    spec["primary_key"]
                ).any():
                    raise ValueError("staging 主键不唯一。")
        except Exception:
            shutil.rmtree(staging_root, ignore_errors=True)
            raise

        log_phase = "prepare_install"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=install; status=started; "
            f"run_id={run_id}; leaves={len(prepared_specs)}"
        )
        with StagedPathTransaction(
            root_path=silver_root,
            staging_dir=staging_root,
            backup_dir=backup_root,
            quarantine_dir=quarantine_root,
            log_context=f"table={TABLE_NAME}; function=commit_validated_leaf_group; run_id={run_id}",
        ) as transaction:
            # 同一日历叶涉及的事实叶与日历叶作为一个进程内协调事务安装。
            for spec in prepared_specs:
                table_name = spec["table_name"]
                target_table_path = silver_root / table_name
                log_phase = "install"
                click.echo(
                    f"planning_progress: table={table_name}; function=commit_validated_leaf_group; phase=install; status=running; "
                    f"partition={spec['partition_key']}; installed={log_installed_leaves}; total={len(prepared_specs)}; run_id={run_id}"
                )
                destination_path = target_table_path / spec["relative_path"]
                source_path = (
                    staging_root / table_name / spec["relative_path"]
                )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )

                marker_path = target_table_path / "schema.parquet"
                if not marker_path.exists():
                    log_phase = "schema_marker_install"
                    click.echo(
                        f"planning_progress: table={table_name}; function=commit_validated_leaf_group; phase=schema_marker_install; status=started; "
                        f"run_id={run_id}"
                    )
                    staged_marker_path = staging_root / table_name / "schema.parquet"
                    pq.write_table(
                        pa.Table.from_batches([], schema=spec["file_schema"]),
                        staged_marker_path,
                    )
                    transaction.replace(
                        target_path=marker_path,
                        staged_path=staged_marker_path,
                        quarantine_new=False,
                    )
                    with pq.ParquetFile(marker_path) as marker_file:
                        if marker_file.metadata.num_rows != 0 or not physically_and_identity_compatible(
                            marker_file.schema_arrow, spec["file_schema"]
                        ):
                            raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")
                log_installed_leaves += 1

            # 正式安装后只精确复读触达叶的物理契约、主键和行数摘要。
            log_phase = "formal_readback"
            log_checked_leaves = 0
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=formal_readback; status=started; "
                f"run_id={run_id}; leaves={len(prepared_specs)}"
            )
            for spec in prepared_specs:
                target_table_path = silver_root / spec["table_name"]
                destination_path = target_table_path / spec["relative_path"]
                parquet_files = list(destination_path.glob("*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(
                        f"正式叶缺少 Parquet 文件：{destination_path}"
                    )
                expected_file_schema = spec["file_schema"]
                for parquet_path in parquet_files:
                    if not physically_and_identity_compatible(
                        pq.read_schema(parquet_path),
                        expected_file_schema,
                    ):
                        raise TypeError(
                            f"正式叶文件 Schema 不一致：{parquet_path}"
                        )
                formal_dataset = ds.dataset(
                    destination_path,
                    format="parquet",
                    partitioning=spec["partitioning"],
                    partition_base_dir=str(target_table_path),
                )
                formal_key_table = formal_dataset.to_table(
                    columns=spec["primary_key"]
                )
                if len(formal_key_table) != len(spec["complete_table"]):
                    raise ValueError("正式叶行数与 staging 不一致。")
                if formal_key_table.to_pandas().duplicated(
                    spec["primary_key"]
                ).any():
                    raise ValueError("正式叶主键不唯一。")
                log_checked_leaves += 1
                click.echo(
                    f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=formal_readback; status=running; "
                    f"partition={spec['partition_key']}; checked={log_checked_leaves}; total={len(prepared_specs)}; run_id={run_id}"
                )

        for log_spec in prepared_specs:
            if log_spec["table_name"] == CALENDAR_TABLE_NAME:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_validated_leaf_group; phase=completion_state; "
                    f"status=completed; outcome=committed; persisted=true; partition={log_spec['partition_key']}; "
                    f"calendar_leaf_rows={len(log_spec['complete_table'])}; source=is_fetch_completed; run_id={run_id}"
                )
        click.echo(
            f"partition_committed: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=completed; "
            f"leaves={len(prepared_specs)}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return sum(len(spec["complete_table"]) for spec in prepared_specs)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 合并并校验 dirty 完整事实叶
# 
# `prepare_fact_leaf_specs()` 按事实 Hive 分区处理本批结果，精确读取旧叶，保留不在本批主键集合中的历史行，再合并新行。每个 dirty 完整叶执行一次完整 `validate_daily_frame()`，包括前序行关系检查，返回供协调提交使用的规格。
# 
# 没有新事实时返回空规格列表；纯白名单政策变化可以只提交日历叶。白名单缩小不会删除历史事实。
# 
# 生成日志属于 `prepare_fact_leaf_specs()`：逐叶报告读取、合并、业务校验后的准备进度及总耗时，空输入明确报告零叶。生成完成只表示内存规格已准备，`persisted=false`。
# 
# 事实 Schema 的列投影在分区循环前读取一次；读取、合并和完整业务校验仍逐 dirty 叶执行。

# ### 流程：准备完整事实叶
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#   A["记录生成开始；按事实分区处理"] --> B["精确读取旧叶"] --> C["保留其他键；合并新事实"] --> D["完整叶业务校验"] --> E["报告已准备叶数；返回内存规格"]
# ```

# In[18]:


def prepare_fact_leaf_specs(
    collected_df: pd.DataFrame,
    lake_root: pathlib.Path,
) -> list[dict[str, object]]:
    log_started_at = time.perf_counter()
    log_phase = "build_fact_leaves"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=started; "
        f"new_rows={len(collected_df)}; persisted=false"
    )
    try:
        if collected_df.empty:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=completed; "
                f"outcome=no_new_facts; leaves=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return []
        fact_path = lake_root.resolve() / "silver" / TABLE_NAME
        specs = []
        fact_columns = FUTURES_DAILY_SCHEMA.names
        for partition_values, incoming_df in collected_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        ):
            partition_key = tuple(partition_values)
            log_phase = "read_existing_leaf"
            existing_df = read_partition_leaf(
                fact_path,
                FUTURES_DAILY_SCHEMA,
                PARTITION_COLUMNS,
                HIVE_PARTITIONING,
                partition_key,
            )
            log_phase = "merge_leaf"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=merge_leaf; status=started; "
                f"partition={partition_key}; existing_rows={len(existing_df)}; new_rows={len(incoming_df)}"
            )
            incoming_keys = pd.MultiIndex.from_frame(incoming_df[PRIMARY_KEY])
            existing_keys = pd.MultiIndex.from_frame(existing_df[PRIMARY_KEY])
            retained_df = existing_df.loc[
                ~existing_keys.isin(incoming_keys),
                fact_columns,
            ]
            log_phase = "validate_complete_leaf"
            desired_df = validate_daily_frame(
                pd.concat(
                    [
                        retained_df,
                        incoming_df.loc[:, fact_columns],
                    ],
                    ignore_index=True,
                ),
                f"dirty 事实叶 {partition_key} ",
            )
            specs.append({
                "table_name": TABLE_NAME,
                "schema": FUTURES_DAILY_SCHEMA,
                "partition_columns": PARTITION_COLUMNS,
                "partitioning": HIVE_PARTITIONING,
                "primary_key": PRIMARY_KEY,
                "partition_key": partition_key,
                "frame": desired_df,
            })
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=running; "
                f"partition={partition_key}; prepared_leaves={len(specs)}; rows={len(desired_df)}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=completed; "
            f"leaves={len(specs)}; new_rows={len(collected_df)}; persisted=false; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return specs
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完成状态生成与 dirty 日历叶准备
# 
# `apply_completion_to_calendar_leaf()` 按本次事实生成完成凭证：正常行情为实际 1 条、passed；有限 OHLC 关系异常仍为实际 1 条，但记 warning；无有效收盘价为实际 0 条、缺失 1 条、warning。三者都记为已完成，日常不会因此重拉。
# 
# `prepare_calendar_leaf_spec()` 精确读取对应 b04 完整叶，先应用政策变化，再应用本次完成状态，最后执行一次完整状态校验。未触达行和无关证据保留原值。此时只形成内存结果，事实与日历随后在同组安装并共同验收；提交失败遵循上一节的恢复边界。
# 
# b05 没有独立日期水位文件；下次待办继续由日历中正式提交的 required 与 completed 决定。
# 
# 完成状态日志分为两层：`apply_completion_to_calendar_leaf()` 只报告内存状态已生成，带 `persisted=false`；`prepare_calendar_leaf_spec()` 报告完整叶准备及校验。只有 `commit_validated_leaf_group()` 完成本组所有正式复读并成功退出共享事务后，才报告该日历叶 `persisted=true`。这里的完成凭证不是最大日期水位，也不把零行 Schema 标记称为水位。

# ### 流程：准备完整日历叶
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["精确读取 b04 日历叶；不存在则报错"] --> B["应用 required 政策变化"]
#   B --> C["生成完成状态；记录 persisted=false"] --> D["主键匹配；更新命中行"]
#   D --> E["完整状态校验"] --> F["报告准备完成；等待与事实共同提交"]
# ```

# In[19]:


def apply_completion_to_calendar_leaf(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "derive_completion"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; status=started; "
        f"fact_rows={len(fact_df)}; calendar_rows={len(calendar_df)}; persisted=false"
    )
    try:
        if fact_df.empty:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; "
                f"status=completed; outcome=no_new_facts; updated_rows=0; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return calendar_df.copy()

        desired_df = calendar_df.copy()
        fact_status_df = fact_df.loc[
            :, [*PRIMARY_KEY, "has_market_data", "open", "high", "low", "close"]
        ].copy()
        invalid_mask = invalid_ohlc_mask(fact_df)
        has_market_data = fact_df["has_market_data"].astype(bool)
        fact_status_df["actual_bar_count"] = has_market_data.astype("int32")
        fact_status_df["missing_bar_count"] = (~has_market_data).astype("int32")
        fact_status_df["quality_status"] = np.where(
            ~has_market_data | invalid_mask,
            "warning",
            "passed",
        )
        fact_status_df["quality_reason"] = np.select(
            [
                ~has_market_data,
                invalid_mask,
            ],
            [
                "JQData 请求完成但没有有效收盘价；事实以缺失占位行正式提交。",
                "JQData 日线事实已正式提交；供应商原始 OHLC 跨列关系异常，原值已保留。",
            ],
            default="JQData 日线事实已正式提交，实际条数为 1。",
        )
        fact_status_df = fact_status_df.set_index(PRIMARY_KEY)

        log_phase = "match_calendar_keys"
        calendar_index = pd.MultiIndex.from_frame(desired_df[PRIMARY_KEY])
        if fact_status_df.index.has_duplicates:
            raise ValueError("完成状态输入事实主键不唯一。")
        missing_keys = fact_status_df.index.difference(calendar_index)
        if len(missing_keys):
            raise ValueError(
                f"事实行无法匹配日线日历：{missing_keys[:5].tolist()}"
            )
        matched = calendar_index.isin(fact_status_df.index)
        matched_rows = desired_df.index[matched]
        matched_index = calendar_index[matched]
        if not desired_df.loc[matched_rows, "is_fetch_required"].all():
            raise ValueError("完成状态回写命中了当前无需拉取的格点。")

        log_phase = "apply_completion"
        status_values = fact_status_df.loc[matched_index]
        desired_df.loc[matched_rows, "is_fetch_completed"] = True
        desired_df.loc[matched_rows, "actual_bar_count"] = status_values[
            "actual_bar_count"
        ].to_numpy()
        desired_df.loc[matched_rows, "is_data_missing"] = status_values[
            "missing_bar_count"
        ].gt(0).to_numpy()
        desired_df.loc[matched_rows, "missing_bar_count"] = status_values[
            "missing_bar_count"
        ].to_numpy()
        desired_df.loc[matched_rows, "fetch_run_id"] = fetch_run_id
        desired_df.loc[matched_rows, "fetch_completed_at"] = completed_at
        desired_df.loc[matched_rows, "missing_checked_at"] = completed_at
        desired_df.loc[matched_rows, "quality_status"] = status_values[
            "quality_status"
        ].to_numpy()
        desired_df.loc[matched_rows, "quality_reason"] = status_values[
            "quality_reason"
        ].to_numpy()
        desired_df.loc[matched_rows, "quality_checked_at"] = completed_at
        desired_df.loc[matched_rows, "updated_at"] = completed_at
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; "
            f"status=completed; outcome=prepared; updated_rows={len(matched_rows)}; persisted=false; "
            f"fetch_run_id={fetch_run_id}; actual_bars={int(has_market_data.sum())}; "
            f"confirmed_empty={int((~has_market_data).sum())}; invalid_ohlc_rows={int(invalid_mask.sum())}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return desired_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def prepare_calendar_leaf_spec(
    policy_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
    fetch_run_id: str,
    completed_at: datetime,
) -> dict[str, object]:
    log_started_at = time.perf_counter()
    log_phase = "read_existing_leaf"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=build_calendar_leaf; status=started; "
        f"partition={partition_key}; policy_rows={len(policy_df)}; fact_rows={len(fact_df)}; persisted=false"
    )
    try:
        calendar_path = lake_root.resolve() / "silver" / CALENDAR_TABLE_NAME
        existing_df = read_partition_leaf(
            calendar_path,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            CALENDAR_PARTITIONING,
            partition_key,
        )
        if existing_df.empty:
            raise FileNotFoundError(f"b04 日历叶不存在：{partition_key}")
        log_phase = "apply_policy"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=apply_policy; status=started; "
            f"partition={partition_key}"
        )
        desired_df = apply_policy_to_calendar_leaf(
            existing_df,
            policy_df,
            completed_at,
        )
        log_phase = "apply_completion"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=apply_completion; status=started; "
            f"partition={partition_key}"
        )
        desired_df = apply_completion_to_calendar_leaf(
            desired_df,
            fact_df,
            fetch_run_id,
            completed_at,
        )
        log_phase = "validate_complete_leaf"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=validate_complete_leaf; status=started; "
            f"partition={partition_key}"
        )
        validated_df = validate_calendar_state_frame(
            desired_df,
            f"dirty 日历叶 {partition_key} ",
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=build_calendar_leaf; status=completed; "
            f"partition={partition_key}; rows={len(validated_df)}; policy_rows={len(policy_df)}; fact_rows={len(fact_df)}; "
            f"persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return {
            "table_name": CALENDAR_TABLE_NAME,
            "schema": FUTURES_BAR_CALENDAR_SCHEMA,
            "partition_columns": CALENDAR_PARTITION_COLUMNS,
            "partitioning": CALENDAR_PARTITIONING,
            "primary_key": CALENDAR_PRIMARY_KEY,
            "partition_key": partition_key,
            "frame": validated_df,
        }
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=build_calendar_leaf; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 运行入口：规划、采集汇总与逐组提交
# 
# `main()` 校验参数，读取日历窄列并形成政策 dirty、API pending 和批次。只有存在请求批次时才认证 JQData；采集后拼接已校验结果，再根据 `--write` 输出只读结果或逐组准备完整叶并协调提交。没有政策变化和新事实时直接结束；每组失败都停止后续组。
# 
# 日志沿用 b01、b02 的 88 个 `=` 运行边界，使用 `table/function/phase/status` 字段，报告规划量、请求批次、提交组数和耗时。保留 `request_batch:`、`api_result:`、`committed:`、`quota_stop:` 等事件前缀，并使用监控可识别的 `planning_progress:` 表达运行状态。配额停止与全部完成分开表达；异常继续沿原调用链抛出，不输出成功结束日志。Dataset 与叶读取、政策及请求计划生成、单批采集、完整叶生成和协调提交日志均由对应函数负责；`main()` 保留整批规划、请求调度、配额停止、累计进度和运行结果，不重复报告单组提交起止。
# 
# 本单元格只定义 Click 命令，不启动业务。最后的独立执行单元格区分 Notebook 与脚本环境：Notebook 显式传入参数；直接运行 `.py` 读取命令行参数；普通模块导入不触发入口。
# 
# 进入提交循环前，政策与事实各建立一次按交易所—年月的行位置索引，循环中直接取当前分组，避免每个分区重新比较整批数据的三列。这里优化的是本次内存结果；正式湖没有逐分区全表扫描：上游根 Dataset 启动时检查并做一次窄列规划，提交链路只读取触达的完整叶。缺少零行标记时保留原有的 Parquet 存在性探测，不据此扫描全历史事实重建完成状态。

# ### 流程：入口分支与日志边界
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#   A["参数检查通过；记录 run started"] --> B["窄列规划与请求分批；报告计划"]
#   B --> C["逐批配额检查；调度采集并报告累计量"] --> D["拼接已校验批次；报告 api_result"]
#   D --> E{"write 且有政策或事实变化？"}
#   E -- 否 --> F["输出只读或无需更新结果"]
#   E -- 是 --> G["按预分组索引取叶；生成、提交并累计"]
#   F --> H{"发生配额停止？"}
#   G --> H
#   H -- 是 --> I["quota_stop；run stopped"]
#   H -- 否 --> J["run completed"]
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
        f"{log_boundary}\n日线运行开始 / Daily run started\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\n{log_boundary}"
    )
    run_updated_at = datetime.now(timezone.utc)
    fetch_run_id = (
        f"daily-{run_updated_at:%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME

    # 全历史日常规划只读取决定政策和完成状态的窄列，并用 Arrow filter 裁剪频率/显式日期。
    planning_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=started"
    )
    calendar_dataset = open_contract_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        "上游行情日历",
        required=True,
    )
    calendar_filter = ds.field("bar_frequency") == "1d"
    if requested_start_date is not None:
        calendar_filter &= ds.field("trading_date") >= requested_start_date
        calendar_filter &= ds.field("trading_date") <= requested_end_date
    calendar_file_count = sum(
        1 for _ in calendar_dataset.get_fragments(filter=calendar_filter)
    )
    click.echo(
        "planning_start: "
        f"table={TABLE_NAME}; calendar_files={calendar_file_count}; "
        f"planning_columns={len(CALENDAR_PLANNING_COLUMNS)}; "
        f"function=main; phase=planning; status=running; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
    )
    planning_table = calendar_dataset.to_table(
        columns=CALENDAR_PLANNING_COLUMNS,
        filter=calendar_filter,
    )
    planned_table, dirty_mask, pending_mask, completed_mask = (
        daily_policy_plan(planning_table)
    )
    dirty_policy_df = planned_table.filter(dirty_mask).to_pandas(
        types_mapper=pd.ArrowDtype
    )
    pending_df = planned_table.filter(pending_mask).to_pandas(
        types_mapper=pd.ArrowDtype
    )
    completed_required_count = int(
        pc.sum(pc.cast(completed_mask, pa.int64())).as_py() or 0
    )
    batches = request_batches(pending_df)
    estimated_values = sum(
        int(batch["estimated_values"])
        for batch in batches
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        f"planned_grid_count={len(planned_table)}; "
        f"policy_dirty_count={len(dirty_policy_df)}; "
        f"completed_required_count={completed_required_count}; "
        f"pending_grid_count={len(pending_df)}; "
        f"request_batch_count={len(batches)}; "
        f"estimated_values={estimated_values}; "
        f"planning_seconds={time.perf_counter() - planning_started_at:.3f}; "
        f"function=main; phase=planning; status=completed; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"
    )

    log_collect_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=collect_batch; status=started; "
        f"batches={len(batches)}; pending={len(pending_df)}"
    )
    collected_frames = []
    returned_value_count = 0
    completed_batch_count = 0
    completed_pending_count = 0
    quota_stop_message = None
    if batches:
        from config.jqdata_connection import authenticate_jqdata

        jqdata = authenticate_jqdata(
            settings.jqdata_id,
            settings.jqdata_secret,
        )
        for batch_number, batch in enumerate(batches, start=1):
            batch_estimated_values = int(batch["estimated_values"])
            spare = quota_spare(jqdata)
            if (
                spare is not None
                and spare - batch_estimated_values < quota_reserve
            ):
                remaining_pending_count = (
                    len(pending_df) - completed_pending_count
                )
                quota_stop_message = (
                    "quota_stop: "
                    f"batch={batch_number}/{len(batches)}; spare={spare}; "
                    f"expected={batch_estimated_values}; "
                    f"reserve={quota_reserve}; "
                    f"completed_batches={completed_batch_count}; "
                    f"remaining_pending={remaining_pending_count}; "
                    f"table={TABLE_NAME}; function=main; phase=collect_batch; status=stopped; "
                    f"elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"
                )
                break
            click.echo(
                "request_batch: "
                f"batch={batch_number}/{len(batches)}; "
                f"exchange={batch['exchange_code']}; "
                f"year={batch['year']}; "
                f"contracts={len(batch['contract_codes'])}; "
                f"pending={len(batch['pending_df'])}; "
                f"start={batch['request_start']}; "
                f"end={batch['request_end']}; "
                f"table={TABLE_NAME}; function=main; phase=collect_batch; status=running; event=dispatch"
            )
            batch_df, batch_returned_count = collect_batch(
                jqdata,
                batch,
                run_updated_at,
            )
            collected_frames.append(batch_df)
            returned_value_count += batch_returned_count
            completed_batch_count += 1
            completed_pending_count += len(batch["pending_df"])
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=collect_batch; status=running; "
                f"batch={batch_number}/{len(batches)}; fact_rows={len(batch_df)}; "
                f"returned_values={batch_returned_count}; completed_pending={completed_pending_count}; "
                f"pending_total={len(pending_df)}; elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"
            )

    remaining_pending_count = len(pending_df) - completed_pending_count

    # 各批已校验；请求按交易所、年和合约拆分，批次输出主键互斥。
    collected_df = (
        pd.concat(collected_frames, ignore_index=True)
        .sort_values(PRIMARY_KEY)
        .reset_index(drop=True)
        if collected_frames
        else empty_pandas(FUTURES_DAILY_SCHEMA)
    )
    invalid_ohlc_count = int(invalid_ohlc_mask(collected_df).sum())
    confirmed_empty_count = int(
        collected_df["has_market_data"].eq(False).sum()
    )
    click.echo(
        "api_result: "
        f"requests={completed_batch_count * 3}; "
        f"returned_values={returned_value_count}; "
        f"fact_rows={len(collected_df)}; "
        f"confirmed_empty={confirmed_empty_count}; "
        f"invalid_ohlc_rows={invalid_ohlc_count}; "
        f"table={TABLE_NAME}; function=main; phase=collect_batch; "
        f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
        f"remaining_pending={remaining_pending_count}; "
        f"elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"
    )

    if not write:
        if (
            quota_stop_message is None
            and dirty_policy_df.empty
            and collected_df.empty
        ):
            click.echo(
                f"up_to_date: table={TABLE_NAME}; mode={mode}; function=main; phase=run; status=completed"
            )
        else:
            click.echo(
                "preview_complete: "
                f"policy_rows={len(dirty_policy_df)}; "
                f"fact_rows={len(collected_df)}; write=false; "
                f"table={TABLE_NAME}; function=main; phase=preview; status=completed"
            )
        if quota_stop_message is not None:
            click.echo(quota_stop_message)
        click.echo(
            f"{log_boundary}\n日线运行结束 / Daily run ended\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
            f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
            f"outcome={'quota_stopped' if quota_stop_message is not None else ('up_to_date' if dirty_policy_df.empty and collected_df.empty else 'read_only')}; mode={mode}; write={str(write).lower()}; "
            f"remaining_pending={remaining_pending_count}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return
    if dirty_policy_df.empty and collected_df.empty:
        if quota_stop_message is None:
            click.echo(
                f"up_to_date: table={TABLE_NAME}; mode={mode}; function=main; phase=run; status=completed"
            )
        else:
            click.echo(quota_stop_message)
        click.echo(
            f"{log_boundary}\n日线运行结束 / Daily run ended\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
            f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
            f"outcome={'quota_stopped' if quota_stop_message is not None else 'up_to_date'}; mode={mode}; write={str(write).lower()}; "
            f"remaining_pending={remaining_pending_count}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return

    base_partition_columns = [
        name for name in CALENDAR_PARTITION_COLUMNS if name != "bar_frequency"
    ]
    policy_row_indices_by_partition = dirty_policy_df.groupby(
        base_partition_columns, sort=False
    ).indices
    fact_row_indices_by_partition = collected_df.groupby(
        base_partition_columns, sort=False
    ).indices
    dirty_calendar_keys = (
        set(policy_row_indices_by_partition) | set(fact_row_indices_by_partition)
    )

    log_commit_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_batch; status=started; "
        f"leaf_groups={len(dirty_calendar_keys)}; fact_rows={len(collected_df)}; policy_rows={len(dirty_policy_df)}"
    )
    committed_fact_rows = 0
    committed_calendar_rows = 0
    committed_leaf_groups = 0
    for base_key in sorted(dirty_calendar_keys):
        exchange_code, year, month = base_key
        leaf_policy_df = dirty_policy_df.iloc[
            policy_row_indices_by_partition.get(base_key, [])
        ].reset_index(drop=True)
        leaf_fact_df = collected_df.iloc[
            fact_row_indices_by_partition.get(base_key, [])
        ].reset_index(drop=True)

        fact_specs = prepare_fact_leaf_specs(
            leaf_fact_df,
            resolved_lake_root,
        )
        calendar_partition_key = (
            "1d",
            exchange_code,
            int(year),
            int(month),
        )
        calendar_spec = prepare_calendar_leaf_spec(
            leaf_policy_df,
            leaf_fact_df,
            resolved_lake_root,
            calendar_partition_key,
            fetch_run_id,
            run_updated_at,
        )
        commit_validated_leaf_group(
            [*fact_specs, calendar_spec],
            resolved_lake_root,
        )
        committed_fact_rows += len(leaf_fact_df)
        committed_calendar_rows += len(calendar_spec["frame"])
        committed_leaf_groups += 1
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_batch; status=running; "
            f"completed_groups={committed_leaf_groups}; total_groups={len(dirty_calendar_keys)}; "
            f"calendar_partition={calendar_partition_key}; fact_leaves={len(fact_specs)}; "
            f"new_fact_rows={len(leaf_fact_df)}; policy_rows={len(leaf_policy_df)}; "
            f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"
        )

    click.echo(
        f"committed: mode={mode}; "
        f"leaf_groups={committed_leaf_groups}; "
        f"fact_rows={committed_fact_rows}; "
        f"calendar_leaf_rows={committed_calendar_rows}; "
        f"remaining_current_plan={remaining_pending_count}; "
        f"table={TABLE_NAME}; function=main; phase=commit_batch; status=completed; "
        f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"
    )
    if quota_stop_message is not None:
        click.echo(quota_stop_message)
    click.echo(
        f"{log_boundary}\n日线运行结束 / Daily run ended\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
        f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
        f"outcome={'quota_stopped' if quota_stop_message is not None else 'committed'}; mode={mode}; write={str(write).lower()}; "
        f"remaining_pending={remaining_pending_count}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
    )


# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数。当前单元格使用与 b01—b04 一致的显式日期只读示例（2026-08-01 至 2026-08-15），不带 `--write`；存在未完成待办时仍会调用 JQData 采集并校验，但不提交事实或回写日历。脚本运行时使用命令行参数，模式与写入限制见开篇表格。在 Notebook 中导入同名 Python 模块不会触发入口。

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
        prog_name="b05_futures_daily",
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
#     A["在终端激活 latitude"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["按白名单与完成凭证规划；采集并逐组提交"]
# ```

# In[22]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a01_Futures_Market_Data\b05_futures_daily.py --write

