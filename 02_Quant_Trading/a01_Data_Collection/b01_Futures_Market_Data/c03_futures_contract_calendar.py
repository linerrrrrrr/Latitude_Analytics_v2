#!/usr/bin/env python
# coding: utf-8

# # c03_futures_contract_calendar
# 
# 目标表是 `dim_futures_contract_calendar`：每个固定月份合约—交易日—Session 一行。它从完整品种日历和 JQData 合约规则生成理论 Session，不断言 Session 当天一定实际开市。
# 
# 事实采集白名单不参与本表行筛选。Notebook 是唯一业务源；同名 Python 文件由默认 PythonExporter 生成。

# ## 自动更新与正式湖写入边界
# 
# 自动模式遵循：`当前有效 Session 格点 − 下游已经完整落盘且内容仍一致的 Session 格点 = 本次待更新格点`。空表自然得到全量；同一逻辑也识别尾部新增、历史内部缺口、Session 内容变化和已经失效的旧 Session。
# 
# 完整性比较不使用 `updated_at`，避免每次运行把审计时间变化误认为业务修订。一个月份只要存在缺失、变化、额外旧行或表级质量错误，就使用该月的完整当前结果重建。
# 
# 正式湖根目录只来自 `.env` 的 `FUTURES_LAKE_ROOT`。显式日期只允许只读检查，或者配合 `--write` 写入解析后明确不同于正式湖的临时/测试湖。`--write` 是唯一的“是否写入”语义。

# ## 原始 API 与转换边界
# 
# - `get_all_securities(["futures"], date=None)` 返回完整期货证券目录，合约代码位于 DataFrame 索引，使用 `start_date` 和 `end_date` 判断上市区间。
# - `get_futures_info(codes, fields=["contract_multiplier", "tick_size", "trade_time"])` 每批最多请求 200 个合约。乘数与 tick 是合约级标量；只有 `trade_time` 带历史生效区间。
# - `get_trade_days` 用于为夜盘确定前一交易日。代码不再依赖固定 15 天回看窗口。
# - 没有当日有效 `trade_time` 规则的候选合约日只计入审计，不伪造 Session；同日命中多条规则属于质量错误。

# ## 逻辑表与物理分区
# 
# 主键：`contract_code, trading_date, session_number`。
# 
# Hive 分区：`exchange_code/year/month`。自动模式始终生成完整月份后替换；显式日期写测试湖时，提交逻辑会保留同月请求范围之外的旧行。
# 
# 所有输入、staging 和正式路径都使用 `config.data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA`。空数据集保留 0 行 `schema.parquet`，使表仍可按权威 Schema 打开。

# ## 初始化与表配置

# In[ ]:


from __future__ import annotations

import pathlib
import re
import shutil
import sys
import uuid
from datetime import date, datetime, time, timedelta, timezone


# 从任意子目录启动时，先按项目统一标记定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Schema、类型转换和校验函数均来自根级可执行契约。
from config.data_contracts import (
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.settings import settings


# 这些常量集中说明本表、上游表以及物理分区边界。
SCHEMA = FUTURES_CONTRACT_CALENDAR_SCHEMA  # 合约 Session 日历的权威 Arrow Schema。
UPSTREAM_SCHEMA = FUTURES_VARIETY_CALENDAR_SCHEMA  # 上游品种交易日历的权威 Schema。
TABLE_NAME = "dim_futures_contract_calendar"  # 期货固定月份合约 Session 日历维度表。
UPSTREAM_TABLE_NAME = "dim_futures_variety_calendar"  # 上游期货品种交易日历维度表。
PARTITION_COLUMNS = [  # 本表和上游共同使用的 Hive 叶分区层级。
    "exchange_code",  # 交易所代码。
    "year",  # 交易年份；由 trading_date 复算。
    "month",  # 交易月份；由 trading_date 复算。
]
PRIMARY_KEY = [  # 唯一标识一个合约交易日内的一段 Session。
    "contract_code",  # JQData 标准固定月份合约代码。
    "trading_date",  # Session 归属的期货交易日。
    "session_number",  # 同一合约交易日内从 1 开始的 Session 顺序号。
]
BUSINESS_COLUMNS = [  # 判断业务内容是否变化；排除仅表示写入时刻的审计字段。
    name for name in SCHEMA.names if name != "updated_at"
]
INFO_FIELDS = [  # 从 JQData get_futures_info 请求的原始字段。
    "contract_multiplier",  # 合约乘数。
    "tick_size",  # 最小价格变动单位。
    "trade_time",  # 原始交易时间文本；后续拆为多个 Session。
]
INFO_BATCH_SIZE = 200
SOURCE_NAME = "JQData_get_all_securities+get_futures_info"

SESSION_PAIR = re.compile(
    r"^\s*(\d{1,2}:\d{2})\s*[-~—–至]\s*(\d{1,2}:\d{2})\s*$"
)
FIXED_CONTRACT = re.compile(
    r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})"
    r"\.(?P<exchange_code>[A-Z]+)$"
)
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})

HIVE_PARTITIONING = ds.partitioning(
    pa.schema([SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([UPSTREAM_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)


# ## Schema 契约交互浏览

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    ])


# ## Session 解析与本表质量校验
# 
# Session 解析是当前表独立且实质复杂的规则，因此保留为本文件内函数。表级校验在生成、staging 和正式路径复读时复用，保证同一套不变量不会漂移。

# In[ ]:


# 将 JQData 的单段交易时间文本严格解析为起止时刻。
def parse_session_text(session_value: object) -> tuple[str, time, time]:
    session_text = str(session_value).strip()
    match = SESSION_PAIR.fullmatch(session_text)
    if match is None:
        raise ValueError(f"无法完整解析 trade_time Session：{session_value!r}")

    def parse_clock(clock_text: str) -> time:
        hour_text, minute_text = clock_text.split(":", 1)
        return time(int(hour_text), int(minute_text))

    return session_text, parse_clock(match.group(1)), parse_clock(match.group(2))


# 对 c03 自己生产的完整业务语义做表级校验。
def validate_contract_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(frame.loc[:, SCHEMA.names], SCHEMA)
    checked_df = arrow_to_pandas(table, SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if checked_df.empty:
        return checked_df

    parsed_codes_df = (
        checked_df["contract_code"].astype("string").str.extract(FIXED_CONTRACT)
    )
    if parsed_codes_df.isna().any().any():
        raise ValueError(f"{context}包含非固定月份合约代码。")
    if parsed_codes_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES).any():
        raise ValueError(f"{context}包含连续或指数合约代码。")
    if not parsed_codes_df["exchange_code"].reset_index(drop=True).eq(
        checked_df["exchange_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}exchange_code 与合约后缀不一致。")
    if not parsed_codes_df["underlying_code"].reset_index(drop=True).eq(
        checked_df["underlying_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}underlying_code 与合约代码不一致。")

    if not checked_df["source"].eq(SOURCE_NAME).all():
        raise ValueError(f"{context}source 与数据契约不一致。")

    # 逐行验证合约存续期、规则有效期和 Session 时间边界。
    for row in checked_df.itertuples(index=False):
        if not row.list_date <= row.trading_date <= row.delist_date:
            raise ValueError(f"{context}{row.contract_code} 的交易日越出上市区间。")
        if not row.rule_effective_date <= row.trading_date <= row.rule_expiry_date:
            raise ValueError(f"{context}{row.contract_code} 的交易日越出规则区间。")
        if row.session_number <= 0:
            raise ValueError(f"{context}session_number 必须大于 0。")

        session_text, start_clock, end_clock = parse_session_text(row.session_text)
        start_at = pd.Timestamp(row.session_start_at)
        end_at = pd.Timestamp(row.session_end_at)
        if start_at.time() != start_clock or end_at.time() != end_clock:
            raise ValueError(f"{context}{session_text} 与 Session 时间戳不一致。")
        if start_at >= end_at:
            raise ValueError(f"{context}Session 起点必须严格早于终点。")

        duration_seconds = (end_at - start_at).total_seconds()
        if duration_seconds % 60 != 0:
            raise ValueError(f"{context}Session 时间差不是整分钟。")
        if row.minute_count != int(duration_seconds // 60) or row.minute_count <= 0:
            raise ValueError(f"{context}minute_count 与 Session 时间差不一致。")

        expected_night = start_clock >= time(20) or start_clock < time(6)
        if row.is_night_session != expected_night:
            raise ValueError(f"{context}is_night_session 与起始钟点不一致。")
        if row.spans_midnight != (start_at.date() != end_at.date()):
            raise ValueError(f"{context}spans_midnight 与时间戳日期不一致。")

        if not pd.isna(row.contract_multiplier) and row.contract_multiplier <= 0:
            raise ValueError(f"{context}非空 contract_multiplier 必须大于 0。")
        if not pd.isna(row.tick_size) and row.tick_size <= 0:
            raise ValueError(f"{context}非空 tick_size 必须大于 0。")
        if row.year != row.trading_date.year or row.month != row.trading_date.month:
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    # 同一合约日的 Session 编号必须连续，起点也不能重复。
    group_columns = ["contract_code", "trading_date"]
    for group_key, group_df in checked_df.groupby(group_columns, sort=False):
        session_numbers = sorted(int(number) for number in group_df["session_number"])
        expected_numbers = list(range(1, len(session_numbers) + 1))
        if session_numbers != expected_numbers:
            raise ValueError(f"{context}{group_key} 的 Session 编号不连续。")
        if group_df["session_start_at"].duplicated().any():
            raise ValueError(f"{context}{group_key} 包含重复 Session 起点。")

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 只比较业务列，避免 updated_at 造成无意义的分区重写。
def business_rows_by_key(frame: pd.DataFrame) -> dict[tuple[object, ...], tuple[object, ...]]:
    table = pandas_to_arrow(frame.loc[:, SCHEMA.names], SCHEMA)
    rows_by_key = {}
    for row in table.select(BUSINESS_COLUMNS).to_pylist():
        key = tuple(row[name] for name in PRIMARY_KEY)
        rows_by_key[key] = tuple(row[name] for name in BUSINESS_COLUMNS)
    return rows_by_key


# ## 正式上游读取与 JQData 合约元数据
# 
# c02 负责品种日历的完整业务语义。c03 只验证精确 Schema/metadata、上游主键和自身直接依赖的范围及活跃合约覆盖；若当前完整合约目录与 c02 的 `active_contract_count` 不一致，应先更新 c02，c03 不越过上游水位。

# In[ ]:


def collect_source_data(
    lake_root: pathlib.Path,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[date, date], dict[str, int]]:
    if (start_date is None) != (end_date is None):
        raise ValueError("起止日期必须同时提供。")
    if start_date is not None and start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    # 先验证正式上游的物理契约和直接消费边界，再决定是否调用 API。
    upstream_path = lake_root.resolve() / "silver" / UPSTREAM_TABLE_NAME
    if not upstream_path.is_dir():
        raise FileNotFoundError(f"缺少上游品种日历：{upstream_path}")

    upstream_dataset = ds.dataset(
        upstream_path,
        format="parquet",
        partitioning=UPSTREAM_PARTITIONING,
    )
    upstream_dataset_schema = pa.schema(
        [upstream_dataset.schema.field(name) for name in UPSTREAM_SCHEMA.names],
        metadata=upstream_dataset.schema.metadata,
    )
    if not upstream_dataset_schema.equals(UPSTREAM_SCHEMA, check_metadata=True):
        raise TypeError("上游品种日历 Schema/metadata 与契约不一致。")

    upstream_table = validate_arrow_table(
        upstream_dataset.to_table(columns=UPSTREAM_SCHEMA.names),
        UPSTREAM_SCHEMA,
    )
    upstream_df = arrow_to_pandas(upstream_table, UPSTREAM_SCHEMA)
    upstream_primary_key = ["exchange_code", "underlying_code", "trading_date"]
    if upstream_df.empty:
        raise ValueError("上游品种日历为空。")
    if upstream_df.duplicated(upstream_primary_key).any():
        raise ValueError("上游品种日历主键不唯一。")

    # 显式日期只缩小检查或非正式湖范围，且不得越出上游水位。
    upstream_min_date = min(upstream_df["trading_date"])
    upstream_max_date = max(upstream_df["trading_date"])
    if start_date is not None:
        if start_date < upstream_min_date or end_date > upstream_max_date:
            raise ValueError(
                "显式日期范围越出上游品种日历水位："
                f"{upstream_min_date} 至 {upstream_max_date}。"
            )
        selected_df = upstream_df.loc[
            (upstream_df["trading_date"] >= start_date)
            & (upstream_df["trading_date"] <= end_date)
        ].reset_index(drop=True)
    else:
        selected_df = upstream_df

    # 纯休市显式范围没有上游格点，无需认证或消耗 API 查询。
    if selected_df.empty:
        securities_df = pd.DataFrame(
            columns=[
                "contract_code",
                "start_date",
                "end_date",
                "underlying_code",
                "delivery_code",
                "exchange_code",
                *INFO_FIELDS,
            ]
        )
        return selected_df, securities_df, {}, {
            "api_call_count": 0,
            "contract_count": 0,
            "info_batch_count": 0,
        }

    # 只有确有上游格点时才通过共享连接边界认证 JQData。
    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)

    # date=None 是有意选择：维度表必须来自完整目录，而不是某日快照。
    securities_df = jqdata.get_all_securities(["futures"], date=None)
    if not isinstance(securities_df, pd.DataFrame):
        raise TypeError(
            "get_all_securities 应返回 pandas.DataFrame，实际为 "
            f"{type(securities_df).__name__}。"
        )

    # 将索引代码转成普通列，并只保留可解析的固定月份合约。
    securities_df = securities_df.rename_axis("contract_code").reset_index()
    required_columns = {"contract_code", "start_date", "end_date"}
    if not required_columns <= set(securities_df.columns):
        missing_columns = sorted(required_columns - set(securities_df.columns))
        raise ValueError(f"get_all_securities 缺少列：{missing_columns}")

    securities_df["contract_code"] = (
        securities_df["contract_code"].astype("string").str.upper()
    )
    parsed_codes_df = securities_df["contract_code"].str.extract(FIXED_CONTRACT)
    securities_df = pd.concat([securities_df, parsed_codes_df], axis=1)
    securities_df = securities_df.dropna(
        subset=["underlying_code", "delivery_code", "exchange_code"]
    )
    securities_df = securities_df.loc[
        ~securities_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES)
    ].copy()

    if securities_df.empty:
        raise ValueError("get_all_securities 未返回任何固定月份期货合约。")
    if securities_df.duplicated(["contract_code"]).any():
        duplicate_codes = sorted(
            securities_df.loc[
                securities_df.duplicated(["contract_code"], keep=False),
                "contract_code",
            ].unique()
        )
        raise ValueError(f"get_all_securities 合约代码重复：{duplicate_codes}")

    securities_df["start_date"] = pd.to_datetime(
        securities_df["start_date"], errors="raise"
    ).dt.date
    securities_df["end_date"] = pd.to_datetime(
        securities_df["end_date"], errors="raise"
    ).dt.date
    if securities_df[["start_date", "end_date"]].isna().any().any():
        raise ValueError("固定月份合约的 start_date/end_date 包含空值。")
    if (securities_df["start_date"] > securities_df["end_date"]).any():
        raise ValueError("固定月份合约存在 start_date 晚于 end_date 的记录。")

    # 只查询与所选上游品种日存在交集的固定合约；这不会应用事实白名单。
    candidate_mask = pd.Series(False, index=securities_df.index)
    group_bounds_df = (
        selected_df.groupby(["exchange_code", "underlying_code"])["trading_date"]
        .agg(["min", "max"])
        .reset_index()
    )
    for bounds in group_bounds_df.itertuples(index=False):
        candidate_mask |= (
            securities_df["exchange_code"].eq(bounds.exchange_code)
            & securities_df["underlying_code"].eq(bounds.underlying_code)
            & (securities_df["start_date"] <= bounds.max)
            & (securities_df["end_date"] >= bounds.min)
        )
    securities_df = securities_df.loc[candidate_mask].copy()
    contract_codes = sorted(securities_df["contract_code"].tolist())
    if not contract_codes:
        raise ValueError("完整合约目录无法覆盖任何上游品种日格点。")

    # get_futures_info 按固定批量查询，逐批检查响应类型和字段完整性。
    info_records = {}
    info_batch_count = 0
    for offset in range(0, len(contract_codes), INFO_BATCH_SIZE):
        batch_codes = contract_codes[offset : offset + INFO_BATCH_SIZE]
        batch_info = jqdata.get_futures_info(batch_codes, fields=INFO_FIELDS)
        info_batch_count += 1
        if not isinstance(batch_info, dict):
            raise TypeError(
                "get_futures_info 应返回 dict，实际为 "
                f"{type(batch_info).__name__}。"
            )

        for contract_code, record in batch_info.items():
            normalized_code = str(contract_code).upper()
            if normalized_code in info_records:
                raise ValueError(f"get_futures_info 重复返回 {normalized_code}。")
            if not isinstance(record, dict):
                raise TypeError(f"{normalized_code} 的合约信息不是 dict。")
            missing_fields = sorted(set(INFO_FIELDS) - set(record))
            if missing_fields:
                raise ValueError(
                    f"{normalized_code} 的 get_futures_info 缺少字段：{missing_fields}"
                )
            info_records[normalized_code] = {name: record[name] for name in INFO_FIELDS}

    missing_codes = sorted(set(contract_codes) - set(info_records))
    unexpected_codes = sorted(set(info_records) - set(contract_codes))
    if missing_codes:
        raise ValueError(
            f"get_futures_info 缺少 {len(missing_codes)} 个合约：{missing_codes[:10]}"
        )
    if unexpected_codes:
        raise ValueError(f"get_futures_info 返回未请求合约：{unexpected_codes[:10]}")

    info_df = pd.DataFrame(
        [
            {"contract_code": contract_code, **record}
            for contract_code, record in info_records.items()
        ]
    )
    info_df["contract_multiplier"] = pd.to_numeric(
        info_df["contract_multiplier"], errors="raise"
    )
    info_df["tick_size"] = pd.to_numeric(info_df["tick_size"], errors="raise")
    securities_df = securities_df.merge(
        info_df,
        on="contract_code",
        how="left",
        validate="one_to_one",
    )

    # 夜盘归属需要上一交易日，因此补取所选水位之前的一个交易日。
    trading_dates = sorted(selected_df["trading_date"].unique().tolist())
    first_window = sorted(
        {
            pd.Timestamp(value).date()
            for value in jqdata.get_trade_days(end_date=trading_dates[0], count=2)
        }
    )
    earlier_dates = [value for value in first_window if value < trading_dates[0]]
    if trading_dates[0] not in first_window or not earlier_dates:
        raise ValueError(f"无法确定 {trading_dates[0]} 的前一交易日。")
    first_previous_date = max(earlier_dates)

    calendar_dates = sorted(
        {
            pd.Timestamp(value).date()
            for value in jqdata.get_trade_days(
                start_date=first_previous_date,
                end_date=trading_dates[-1],
            )
        }
    )
    calendar_positions = {calendar_date: index for index, calendar_date in enumerate(calendar_dates)}
    previous_date_by_trading_date = {}
    for trading_date in trading_dates:
        position = calendar_positions.get(trading_date)
        if position is None or position == 0:
            raise ValueError(f"上游交易日 {trading_date} 不在 JQData 交易日序列中。")
        previous_date_by_trading_date[trading_date] = calendar_dates[position - 1]

    source_summary = {
        "api_call_count": 3 + info_batch_count,
        "contract_count": len(contract_codes),
        "info_batch_count": info_batch_count,
    }
    return selected_df, securities_df, previous_date_by_trading_date, source_summary


# ## 逐月展开当前有效 Session
# 
# 生成函数一次只接收一个交易所—年月的上游格点。这样既能完整比较规则修订，又不会把全历史合约 Session 同时放入内存。

# In[ ]:


def build_contract_calendar_partition(
    variety_partition_df: pd.DataFrame,
    securities_by_group: dict[tuple[str, str], pd.DataFrame],
    previous_date_by_trading_date: dict[date, date],
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    rows = []
    candidate_contract_day_count = 0
    no_valid_rule_contract_day_count = 0
    no_valid_rule_samples = []

    # 逐品种日匹配当日仍处于上市区间的固定月份合约。
    for item in variety_partition_df.itertuples(index=False):
        group_key = (str(item.exchange_code), str(item.underlying_code))
        group_df = securities_by_group.get(group_key)
        if group_df is None:
            raise ValueError(f"完整合约目录缺少上游品种：{group_key}")

        active_df = group_df.loc[
            (group_df["start_date"] <= item.trading_date)
            & (group_df["end_date"] >= item.trading_date)
        ]
        if len(active_df) != int(item.active_contract_count):
            raise ValueError(
                f"{group_key} 在 {item.trading_date} 的当前活跃合约数为 "
                f"{len(active_df)}，上游记录为 {item.active_contract_count}；"
                "请先更新 c02。"
            )

        # 每张合约根据交易日选择唯一生效的 trade_time 规则。
        for contract in active_df.itertuples(index=False):
            candidate_contract_day_count += 1
            trade_time_rules = contract.trade_time
            if trade_time_rules is None or trade_time_rules is pd.NA or trade_time_rules == []:
                no_valid_rule_contract_day_count += 1
                if len(no_valid_rule_samples) < 10:
                    no_valid_rule_samples.append((contract.contract_code, item.trading_date))
                continue
            if not isinstance(trade_time_rules, (list, tuple)):
                raise ValueError(
                    f"{contract.contract_code} 的 trade_time 不是规则列表。"
                )

            matching_rules = []
            for rule in trade_time_rules:
                if not isinstance(rule, (list, tuple)) or len(rule) < 3:
                    raise ValueError(
                        f"{contract.contract_code} 包含非法 trade_time 规则：{rule!r}"
                    )
                effective_date = pd.Timestamp(rule[0]).date()
                expiry_date = pd.Timestamp(rule[1]).date()
                if effective_date > expiry_date:
                    raise ValueError(
                        f"{contract.contract_code} 的规则生效日晚于失效日：{rule!r}"
                    )
                if effective_date <= item.trading_date <= expiry_date:
                    matching_rules.append((effective_date, expiry_date, rule[2:]))

            if not matching_rules:
                no_valid_rule_contract_day_count += 1
                if len(no_valid_rule_samples) < 10:
                    no_valid_rule_samples.append((contract.contract_code, item.trading_date))
                continue
            if len(matching_rules) > 1:
                raise ValueError(
                    f"{contract.contract_code} 在 {item.trading_date} 命中 "
                    f"{len(matching_rules)} 条 trade_time 规则。"
                )

            effective_date, expiry_date, session_values = matching_rules[0]
            previous_trading_date = previous_date_by_trading_date[item.trading_date]
            # 将规则中的每段 Session 展开为目标表的一行。
            for session_number, session_value in enumerate(session_values, start=1):
                session_text, start_clock, end_clock = parse_session_text(session_value)
                is_night_session = start_clock >= time(20) or start_clock < time(6)

                # 20:00 后开盘的夜盘从上一交易日自然日开始。
                if start_clock >= time(20):
                    start_day = previous_trading_date
                elif start_clock < time(6):
                    start_day = previous_trading_date + timedelta(days=1)
                else:
                    start_day = item.trading_date
                end_day = (
                    start_day + timedelta(days=1)
                    if end_clock <= start_clock
                    else start_day
                )

                session_start_at = pd.Timestamp(
                    datetime.combine(start_day, start_clock),
                    tz="Asia/Shanghai",
                )
                session_end_at = pd.Timestamp(
                    datetime.combine(end_day, end_clock),
                    tz="Asia/Shanghai",
                )
                minute_count = int(
                    (session_end_at - session_start_at).total_seconds() // 60
                )
                contract_multiplier = (
                    None
                    if pd.isna(contract.contract_multiplier)
                    else float(contract.contract_multiplier)
                )
                tick_size = (
                    None if pd.isna(contract.tick_size) else float(contract.tick_size)
                )

                rows.append(
                    {
                        "contract_code": contract.contract_code,
                        "exchange_code": item.exchange_code,
                        "underlying_code": item.underlying_code,
                        "trading_date": item.trading_date,
                        "list_date": contract.start_date,
                        "delist_date": contract.end_date,
                        "contract_multiplier": contract_multiplier,
                        "tick_size": tick_size,
                        "rule_effective_date": effective_date,
                        "rule_expiry_date": expiry_date,
                        "session_number": session_number,
                        "session_text": session_text,
                        "session_start_at": session_start_at,
                        "session_end_at": session_end_at,
                        "is_night_session": is_night_session,
                        "spans_midnight": start_day != end_day,
                        "minute_count": minute_count,
                        "source": SOURCE_NAME,
                        "updated_at": updated_at,
                        "year": item.trading_date.year,
                        "month": item.trading_date.month,
                    }
                )

    # 空分区也返回具有权威列顺序和类型的空表。
    if rows:
        result_df = pd.DataFrame(rows, columns=SCHEMA.names)
    else:
        result_df = empty_pandas(SCHEMA)
    result_df = validate_contract_calendar_frame(result_df, "当前期望分区")

    audit = {
        "candidate_contract_day_count": candidate_contract_day_count,
        "no_valid_rule_contract_day_count": no_valid_rule_contract_day_count,
        "no_valid_rule_samples": no_valid_rule_samples,
        "session_row_count": len(result_df),
    }
    return result_df, audit


# ## 单分区暂存、提交与失败回滚
# 
# 自动模式传入完整月份；显式日期模式先从旧分区保留范围外行。staging 通过复读前不触碰目标，目标复读失败则隔离新分区并恢复备份。

# In[ ]:


def commit_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[str, int, int],
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
) -> int:
    if (replace_start_date is None) != (replace_end_date is None):
        raise ValueError("替换起止日期必须同时提供。")
    if replace_start_date is not None and replace_start_date > replace_end_date:
        raise ValueError("替换起始日期不得晚于结束日期。")

    exchange_code, year, month = partition_key
    # 提交前先证明输入只属于目标交易所—年月分区。
    incoming_df = validate_contract_calendar_frame(frame, "待提交分区")
    if not incoming_df.empty:
        incoming_partition_keys = set(
            incoming_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if incoming_partition_keys != {(exchange_code, year, month)}:
            raise ValueError("待提交数据越出指定 Hive 分区。")
        if replace_start_date is not None and any(
            trading_date < replace_start_date or trading_date > replace_end_date
            for trading_date in incoming_df["trading_date"]
        ):
            raise ValueError("待提交数据越出显式替换日期范围。")

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"
    silver_root.mkdir(parents=True, exist_ok=True)

    for managed_path in (target_path, staging_path, backup_path, quarantine_path):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    target_dataset = None
    if target_path.is_dir() and any(target_path.rglob("*.parquet")):
        target_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        target_schema = pa.schema(
            [target_dataset.schema.field(name) for name in SCHEMA.names],
            metadata=target_dataset.schema.metadata,
        )
        if not target_schema.equals(SCHEMA, check_metadata=True):
            raise TypeError("现有正式数据集 Schema/metadata 与契约不一致。")

    existing_df = empty_pandas(SCHEMA)
    # 合并既有分区；显式范围只替换范围内行，自动模式替换完整分区。
    if target_dataset is not None:
        existing_table = target_dataset.to_table(
            columns=SCHEMA.names,
            filter=(ds.field("exchange_code") == exchange_code)
            & (ds.field("year") == year)
            & (ds.field("month") == month),
        )
        existing_df = arrow_to_pandas(existing_table, SCHEMA)

    if replace_start_date is None:
        complete_partition_df = incoming_df
    else:
        retained_df = existing_df.loc[
            (existing_df["trading_date"] < replace_start_date)
            | (existing_df["trading_date"] > replace_end_date),
            SCHEMA.names,
        ]
        complete_partition_df = pd.concat(
            [retained_df, incoming_df],
            ignore_index=True,
        )
    complete_partition_df = validate_contract_calendar_frame(
        complete_partition_df,
        "合并后完整分区",
    )
    complete_partition_table = pandas_to_arrow(complete_partition_df, SCHEMA)

    relative_path = pathlib.Path(
        f"exchange_code={exchange_code}",
        f"year={year}",
        f"month={month}",
    )
    staging_path.mkdir(parents=True, exist_ok=False)
    # 先写 staging 并复读，任何异常都不得触碰正式分区。
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
            staged_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
            )
            staged_schema = pa.schema(
                [staged_dataset.schema.field(name) for name in SCHEMA.names],
                metadata=staged_dataset.schema.metadata,
            )
            if not staged_schema.equals(SCHEMA, check_metadata=True):
                raise TypeError("staging Schema/metadata 与契约不一致。")
            staged_df = arrow_to_pandas(
                staged_dataset.to_table(columns=SCHEMA.names),
                SCHEMA,
            )
            staged_df = validate_contract_calendar_frame(staged_df, "staging 分区")
            if not pandas_to_arrow(staged_df, SCHEMA).equals(complete_partition_table):
                raise ValueError("staging 分区内容检查失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    marker_path = target_path / "schema.parquet"
    marker_created = False
    # 正式替换前备份旧分区；失败时从备份回滚。
    commit_succeeded = False

    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    saved_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if destination_path.exists():
            shutil.move(str(destination_path), str(saved_path))
        if source_path.is_dir():
            shutil.move(str(source_path), str(destination_path))
        elif len(complete_partition_table):
            raise FileNotFoundError(f"staging 缺少 {relative_path}。")

        data_files = [
            path
            for path in target_path.rglob("*.parquet")
            if path.name != "schema.parquet"
        ]
        if not data_files and not marker_path.exists():
            file_schema = pa.schema(
                [
                    SCHEMA.field(name)
                    for name in SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=SCHEMA.metadata,
            )
            pq.write_table(pa.Table.from_batches([], schema=file_schema), marker_path)
            marker_created = True

        committed_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        committed_schema = pa.schema(
            [committed_dataset.schema.field(name) for name in SCHEMA.names],
            metadata=committed_dataset.schema.metadata,
        )
        if not committed_schema.equals(SCHEMA, check_metadata=True):
            raise TypeError("正式数据集 Schema/metadata 与契约不一致。")

        committed_table = committed_dataset.to_table(
            columns=SCHEMA.names,
            filter=(ds.field("exchange_code") == exchange_code)
            & (ds.field("year") == year)
            & (ds.field("month") == month),
        )
        committed_df = validate_contract_calendar_frame(
            arrow_to_pandas(committed_table, SCHEMA),
            "正式复读分区",
        )
        if not pandas_to_arrow(committed_df, SCHEMA).equals(complete_partition_table):
            raise ValueError("正式复读分区内容与 staging 不一致。")
        commit_succeeded = True
    except Exception as commit_error:
        if marker_created and marker_path.exists():
            marker_path.unlink()

        rollback_errors = []
        try:
            if destination_path.exists():
                isolated_path = quarantine_path / relative_path
                isolated_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(isolated_path))
            if saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
        except Exception as rollback_error:
            rollback_errors.append(
                f"{type(rollback_error).__name__}: {rollback_error}"
            )

        if rollback_errors:
            raise RuntimeError(
                f"分区提交失败且回滚不完整；恢复副本保留在 {backup_path}；"
                f"回滚错误：{rollback_errors}"
            ) from commit_error
        if quarantine_path.exists() and any(quarantine_path.rglob("*")):
            raise RuntimeError(
                f"分区提交失败；旧分区已恢复，新分区隔离在 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)
        if commit_succeeded or not any(backup_path.rglob("*.parquet")):
            shutil.rmtree(backup_path, ignore_errors=True)
        if quarantine_path.exists() and not any(quarantine_path.rglob("*")):
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(incoming_df)


# ## 命令行入口与自动差集计划
# 
# 第一遍逐月生成当前结果并形成计划；只有带 `--write` 时才进行第二遍生成和提交。无写入运行不会创建 staging、修改正式分区或回写状态。

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
    # 解析正式/非正式湖和显式日期，先执行正式写入门禁。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    requested_start_date = start_date.date() if start_date is not None else None
    requested_end_date = end_date.date() if end_date is not None else None
    if requested_start_date is not None and requested_start_date > requested_end_date:
        raise click.BadParameter("起始日期不得晚于结束日期。")

    # 一次读取完整上游与 API 目录，后续按交易所—年月流式规划。
    variety_df, securities_df, previous_dates, source_summary = collect_source_data(
        resolved_lake_root,
        requested_start_date,
        requested_end_date,
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"api_success: mode={mode}; api_calls={source_summary['api_call_count']}; "
        f"contracts={source_summary['contract_count']}; "
        f"info_batches={source_summary['info_batch_count']}"
    )

    securities_by_group = {
        (str(exchange_code), str(underlying_code)): group_df.reset_index(drop=True)
        for (exchange_code, underlying_code), group_df in securities_df.groupby(
            ["exchange_code", "underlying_code"],
            sort=False,
        )
    }
    variety_by_partition = {
        (str(exchange_code), int(year), int(month)): group_df.reset_index(drop=True)
        for (exchange_code, year, month), group_df in variety_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        )
    }

    # 发现现有完整分区键；损坏分区留给逐分区质检标记为待重建。
    target_path = resolved_lake_root / "silver" / TABLE_NAME
    target_dataset = None
    existing_partition_keys = set()
    if target_path.is_dir() and any(target_path.rglob("*.parquet")):
        target_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        target_schema = pa.schema(
            [target_dataset.schema.field(name) for name in SCHEMA.names],
            metadata=target_dataset.schema.metadata,
        )
        if not target_schema.equals(SCHEMA, check_metadata=True):
            raise TypeError("现有合约日历 Schema/metadata 与契约不一致。")

        for parquet_path in target_path.rglob("*.parquet"):
            if parquet_path.name == "schema.parquet":
                continue
            relative_parts = parquet_path.relative_to(target_path).parts
            if (
                len(relative_parts) != 4
                or not relative_parts[0].startswith("exchange_code=")
                or not relative_parts[1].startswith("year=")
                or not relative_parts[2].startswith("month=")
            ):
                raise ValueError(f"正式表包含非法分区文件：{parquet_path}")
            existing_partition_keys.add(
                (
                    relative_parts[0].split("=", 1)[1],
                    int(relative_parts[1].split("=", 1)[1]),
                    int(relative_parts[2].split("=", 1)[1]),
                )
            )

    if has_explicit_dates:
        first_month = (requested_start_date.year, requested_start_date.month)
        last_month = (requested_end_date.year, requested_end_date.month)
        relevant_existing_keys = {
            key for key in existing_partition_keys if first_month <= key[1:] <= last_month
        }
    else:
        relevant_existing_keys = existing_partition_keys
    partition_keys = sorted(set(variety_by_partition) | relevant_existing_keys)

    run_updated_at = datetime.now(timezone.utc)
    dirty_partition_plans = []
    upstream_grid_count = 0
    complete_grid_count = 0
    missing_grid_count = 0
    changed_grid_count = 0
    extra_grid_count = 0
    no_valid_rule_count = 0
    no_valid_rule_samples = []

    # 当前上游分区与既有下游分区取并集，识别新增、修订和删除。
    for partition_number, partition_key in enumerate(
        partition_keys,
        start=1,
    ):
        if partition_number == 1 or partition_number % 25 == 0:
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; "
                f"partitions={partition_number}/{len(partition_keys)}; "
                f"key={partition_key}"
            )
        upstream_partition_df = variety_by_partition.get(partition_key)
        if upstream_partition_df is None:
            expected_df = empty_pandas(SCHEMA)
            audit = {
                "no_valid_rule_contract_day_count": 0,
                "no_valid_rule_samples": [],
            }
        else:
            expected_df, audit = build_contract_calendar_partition(
                upstream_partition_df,
                securities_by_group,
                previous_dates,
                run_updated_at,
            )

        existing_df = empty_pandas(SCHEMA)
        if target_dataset is not None and partition_key in existing_partition_keys:
            exchange_code, year, month = partition_key
            existing_table = target_dataset.to_table(
                columns=SCHEMA.names,
                filter=(ds.field("exchange_code") == exchange_code)
                & (ds.field("year") == year)
                & (ds.field("month") == month),
            )
            existing_df = arrow_to_pandas(existing_table, SCHEMA)

        comparison_existing_df = existing_df
        if has_explicit_dates:
            comparison_existing_df = existing_df.loc[
                (existing_df["trading_date"] >= requested_start_date)
                & (existing_df["trading_date"] <= requested_end_date)
            ].reset_index(drop=True)

        expected_rows = business_rows_by_key(expected_df)
        existing_quality_error = None
        try:
            checked_existing_df = validate_contract_calendar_frame(
                comparison_existing_df,
                f"现有分区 {partition_key} "
            )
            existing_rows = business_rows_by_key(checked_existing_df)
        except (TypeError, ValueError) as error:
            existing_quality_error = f"{type(error).__name__}: {error}"
            existing_rows = {}

        expected_keys = set(expected_rows)
        existing_keys = set(existing_rows)
        missing_keys = expected_keys - existing_keys
        extra_keys = existing_keys - expected_keys
        changed_keys = {
            key
            for key in expected_keys & existing_keys
            if expected_rows[key] != existing_rows[key]
        }
        if existing_quality_error is not None:
            missing_keys = expected_keys
            extra_count = len(comparison_existing_df)
        else:
            extra_count = len(extra_keys)

        # 业务行差异、既有质量错误或规则缺失都会触发分区计划。
        is_dirty = bool(
            missing_keys or changed_keys or extra_count or existing_quality_error
        )
        if is_dirty:
            dirty_partition_plans.append(
                {
                    "partition_key": partition_key,
                    "expected_row_count": len(expected_df),
                    "missing_count": len(missing_keys),
                    "changed_count": len(changed_keys),
                    "extra_count": extra_count,
                    "quality_error": existing_quality_error,
                }
            )

        upstream_grid_count += len(expected_df)
        complete_grid_count += len(expected_df) - len(missing_keys) - len(changed_keys)
        missing_grid_count += len(missing_keys)
        changed_grid_count += len(changed_keys)
        extra_grid_count += extra_count
        no_valid_rule_count += int(audit["no_valid_rule_contract_day_count"])
        for sample in audit["no_valid_rule_samples"]:
            if len(no_valid_rule_samples) < 10:
                no_valid_rule_samples.append(sample)

    plan_name = "explicit_plan" if has_explicit_dates else "auto_plan"
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; upstream_grid_count={upstream_grid_count}; "
        f"complete_grid_count={complete_grid_count}; missing_grid_count={missing_grid_count}; "
        f"changed_grid_count={changed_grid_count}; extra_grid_count={extra_grid_count}; "
        f"no_valid_rule_contract_day_count={no_valid_rule_count}; "
        f"touched_partition_count={len(dirty_partition_plans)}"
    )
    if no_valid_rule_samples:
        click.echo(f"no_valid_rule_samples: {no_valid_rule_samples}")
    for plan in dirty_partition_plans:
        click.echo(
            "partition_plan: "
            f"partition={plan['partition_key']}; expected_rows={plan['expected_row_count']}; "
            f"missing={plan['missing_count']}; changed={plan['changed_count']}; "
            f"extra={plan['extra_count']}; quality_error={plan['quality_error']}"
        )

    if not dirty_partition_plans:
        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return
    # 不带 --write 时只展示计划，不修改正式或测试湖。
    if not write:
        return

    committed_row_count = 0
    # 每个脏分区独立 staging、复读和提交，便于失败后安全续跑。
    for plan in dirty_partition_plans:
        partition_key = plan["partition_key"]
        upstream_partition_df = variety_by_partition.get(partition_key)
        if upstream_partition_df is None:
            expected_df = empty_pandas(SCHEMA)
        else:
            expected_df, _ = build_contract_calendar_partition(
                upstream_partition_df,
                securities_by_group,
                previous_dates,
                run_updated_at,
            )
        committed_row_count += commit_partition(
            expected_df,
            resolved_lake_root,
            partition_key,
            requested_start_date,
            requested_end_date,
        )

    commit_mode = "explicit_non_formal" if has_explicit_dates else "automatic_gap_fill"
    click.echo(
        f"committed: mode={commit_mode}; rows={committed_row_count}; "
        f"partitions={len(dirty_partition_plans)}; remaining_pending=0"
    )


if __name__ == "__main__":
    main()

