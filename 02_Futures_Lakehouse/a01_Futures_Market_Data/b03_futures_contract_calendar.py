#!/usr/bin/env python
# coding: utf-8

# # b03_futures_contract_calendar
# 
# 目标表是 `dim_futures_contract_calendar`：每个固定月份合约—交易日—Session 一行。它从完整品种日历和 JQData 合约规则生成理论 Session，不断言 Session 当天一定实际开市。
# 
# 事实采集白名单不参与本表行筛选。Notebook 是唯一业务源；同名 Python 文件由默认 PythonExporter 生成。

# ## 自动更新与正式湖写入边界
# 
# 默认模式信任已经正式提交的历史，只读取 b02 中晚于 b03 自动尾部水位的新增品种日；没有新增交易日时不认证、不调用 API。水位取正式行最大交易日与根 `schema.parquet` 中 `automatic_tail_processed_through` 的较大值，使整日候选均无有效 `trade_time` 时也能在成功 `--write` 后推进。默认模式不再对全历史合约数求差，也不清退旧行；无有效规则的候选合约日 warning 后跳过，专项覆盖统计留给显式或 `--full` 审计。
# 
# 同时提供 `--start-date/--end-date` 时定向拉取并双向比较指定日期范围；`--full` 强制拉取并双向比较完整当前水位。两种刷新模式都只提交存在业务差异的范围，比较不使用 `updated_at`。
# 
# 正式湖根目录只来自 `.env` 的 `FUTURES_LAKE_ROOT`。b03 是项目中经过确认的例外：默认、显式日期和 `--full` 都可在带 `--write` 时更新正式湖；不带 `--write` 只拉取、比较和输出计划。日期范围与 `--full` 互斥。

# ## 原始 API 与转换边界
# 
# - `get_all_securities(["futures"], date=None)` 返回完整期货证券目录，合约代码位于 DataFrame 索引，使用 `start_date` 和 `end_date` 判断上市区间。
# - `get_futures_info(codes, fields=["contract_multiplier", "tick_size", "trade_time"])` 每批最多请求 200 个合约。乘数与 tick 是合约级标量；只有 `trade_time` 带历史生效区间。
# - 前一交易日优先由可信品种日历的完整日期序列确定；只有最早水位边界需要调用一次 `get_trade_days`。
# - 没有当日有效 `trade_time` 规则的候选合约日只计入审计，不伪造 Session；同日命中多条规则属于质量错误。

# ## 逻辑表与物理分区
# 
# 主键：`contract_code, trading_date, session_number`。
# 
# Hive 分区：`exchange_code/year/month`。默认模式只替换本批尾部新增品种日，显式日期模式替换日期区间，`--full` 替换完整分区；三种模式都保留其替换范围之外的旧行。
# 
# 所有输入、staging 和正式路径都使用 `config.data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA`。空数据集保留 0 行 `schema.parquet`，使表仍可按权威 Schema 打开。

# ## 初始化与表配置

# In[ ]:


from __future__ import annotations

import os
import pathlib
from bisect import bisect_left
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


# In[ ]:


# 表名、主键和分区顺序只从权威 Schema metadata 读取一次。
TABLE_NAME = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
UPSTREAM_TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
PARTITION_COLUMNS = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
UPSTREAM_PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
PHYSICAL_METADATA_KEYS = (b"table_name", b"primary_key", b"partition_columns")
AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY = b"automatic_tail_processed_through"


def validate_compatible_dataset_schema(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    context: str,
) -> None:
    """只固定物理字段和路由 metadata；允许描述性 metadata 随代码更新。"""
    if actual_schema.names != expected_schema.names:
        raise TypeError(f"{context}字段及顺序与契约不一致。")
    for expected_field in expected_schema:
        actual_field = actual_schema.field(expected_field.name)
        if (
            actual_field.type != expected_field.type
            or actual_field.nullable != expected_field.nullable
        ):
            raise TypeError(f"{context}字段 {expected_field.name!r} 的类型或 nullable 与契约不一致。")
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    for metadata_key in PHYSICAL_METADATA_KEYS:
        if actual_metadata.get(metadata_key) != expected_metadata.get(metadata_key):
            raise TypeError(f"{context}{metadata_key.decode()} metadata 与契约不一致。")


def validate_dataset_fragment_schemas(
    dataset: ds.Dataset,
    expected_schema: pa.Schema,
    partition_columns: list[str],
    context: str,
) -> None:
    """逐个检查物理 Parquet Schema，防止 Dataset 逻辑 Schema 遮蔽后续文件漂移。"""
    expected_file_schema = pa.schema(
        [
            expected_schema.field(name)
            for name in expected_schema.names
            if name not in partition_columns
        ],
        metadata=expected_schema.metadata,
    )
    for fragment in dataset.get_fragments():
        validate_compatible_dataset_schema(
            fragment.physical_schema,
            expected_file_schema,
            f"{context}fragment {fragment.path} ",
        )


def read_automatic_tail_processed_through(target_path: pathlib.Path) -> date | None:
    marker_path = target_path / "schema.parquet"
    if not marker_path.is_file():
        return None
    encoded_watermark = (pq.read_schema(marker_path).metadata or {}).get(
        AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY
    )
    if encoded_watermark is None:
        return None
    try:
        return date.fromisoformat(encoded_watermark.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError(
            "schema.parquet 中 automatic_tail_processed_through 不是有效 ISO 日期。"
        ) from error


def write_automatic_tail_processed_through(
    lake_root: pathlib.Path,
    processed_through: date,
) -> None:
    target_path = lake_root.resolve() / "silver" / TABLE_NAME
    target_path.mkdir(parents=True, exist_ok=True)
    marker_path = target_path / "schema.parquet"
    temporary_marker_path = target_path / f".c03m-{uuid.uuid4().hex}.parquet"
    marker_metadata = dict(FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata or {})
    marker_metadata[AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY] = (
        processed_through.isoformat().encode("ascii")
    )
    marker_schema = pa.schema(
        [
            FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
            for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            if name not in PARTITION_COLUMNS
        ],
        metadata=marker_metadata,
    )
    try:
        pq.write_table(
            pa.Table.from_batches([], schema=marker_schema),
            temporary_marker_path,
        )
        if not pq.read_schema(temporary_marker_path).equals(
            marker_schema, check_metadata=True
        ):
            raise RuntimeError("b03 自动尾部水位 staging 复读失败。")
        os.replace(temporary_marker_path, marker_path)
    finally:
        temporary_marker_path.unlink(missing_ok=True)

VARIETY_DATE_COLUMNS = ["exchange_code", "underlying_code", "trading_date"]
BUSINESS_COLUMNS = [  # 判断业务内容是否变化；排除仅表示写入时刻的审计字段。
    name for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names if name != "updated_at"
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
    pa.schema([FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in UPSTREAM_PARTITION_COLUMNS]),
    flavor="hive",
)


# ## Schema 契约呈现

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


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


# 对 b03 自己生产的完整业务语义做表级校验。
def validate_contract_calendar_frame(
    contract_calendar_df: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    contract_calendar_table = pandas_to_arrow(
        contract_calendar_df.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    validated_contract_calendar_df = arrow_to_pandas(
        contract_calendar_table,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )

    if validated_contract_calendar_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if validated_contract_calendar_df.empty:
        return validated_contract_calendar_df

    parsed_codes_df = (
        validated_contract_calendar_df["contract_code"].astype("string").str.extract(FIXED_CONTRACT)
    )
    if parsed_codes_df.isna().any().any():
        raise ValueError(f"{context}包含非固定月份合约代码。")
    if parsed_codes_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES).any():
        raise ValueError(f"{context}包含连续或指数合约代码。")
    if not parsed_codes_df["exchange_code"].reset_index(drop=True).eq(
        validated_contract_calendar_df["exchange_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}exchange_code 与合约后缀不一致。")
    if not parsed_codes_df["underlying_code"].reset_index(drop=True).eq(
        validated_contract_calendar_df["underlying_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}underlying_code 与合约代码不一致。")

    if not validated_contract_calendar_df["source"].eq(SOURCE_NAME).all():
        raise ValueError(f"{context}source 与数据契约不一致。")

    # 逐行验证合约存续期、规则有效期和 Session 时间边界。
    for row in validated_contract_calendar_df.itertuples(index=False):
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
    for group_key, group_df in validated_contract_calendar_df.groupby(group_columns, sort=False):
        session_numbers = sorted(int(number) for number in group_df["session_number"])
        expected_numbers = list(range(1, len(session_numbers) + 1))
        if session_numbers != expected_numbers:
            raise ValueError(f"{context}{group_key} 的 Session 编号不连续。")
        if group_df["session_start_at"].duplicated().any():
            raise ValueError(f"{context}{group_key} 包含重复 Session 起点。")

    return validated_contract_calendar_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 只比较业务列，避免 updated_at 造成无意义的分区重写。
def business_rows_by_key(
    contract_calendar_df: pd.DataFrame,
) -> dict[tuple[object, ...], tuple[object, ...]]:
    contract_calendar_table = pandas_to_arrow(
        contract_calendar_df.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    rows_by_key = {}
    for row in contract_calendar_table.select(BUSINESS_COLUMNS).to_pylist():
        key = tuple(row[name] for name in PRIMARY_KEY)
        rows_by_key[key] = tuple(row[name] for name in BUSINESS_COLUMNS)
    return rows_by_key


# ## 正式上游读取与 JQData 合约元数据
# 
# b02 负责并已经证明品种日历的完整业务语义。b03 只确认 Dataset 存在及 Schema/metadata 物理兼容，直接信任其主键、日期、`active_contract_count` 和派生语义；JQData 原始响应与 b03 自己的输出仍由 b03 完整校验。

# In[ ]:


def read_trusted_variety_calendar(
    lake_root: pathlib.Path,
    *,
    start_date_exclusive: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    if start_date_exclusive is not None and (start_date is not None or end_date is not None):
        raise ValueError("尾部起点不能与闭区间同时提供。")
    if (start_date is None) != (end_date is None):
        raise ValueError("闭区间起止日期必须同时提供。")
    upstream_variety_calendar_path = (
        lake_root.resolve() / "silver" / UPSTREAM_TABLE_NAME
    )
    if not upstream_variety_calendar_path.is_dir():
        raise FileNotFoundError(
            f"缺少上游品种日历：{upstream_variety_calendar_path}"
        )

    upstream_variety_calendar_dataset = ds.dataset(
        upstream_variety_calendar_path,
        format="parquet",
        partitioning=UPSTREAM_PARTITIONING,
    )
    if set(upstream_variety_calendar_dataset.schema.names) != set(
        FUTURES_VARIETY_CALENDAR_SCHEMA.names
    ):
        raise TypeError("上游品种日历字段集合与契约不一致。")
    upstream_variety_calendar_schema = pa.schema(
        [
            upstream_variety_calendar_dataset.schema.field(name)
            for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
        ],
        metadata=upstream_variety_calendar_dataset.schema.metadata,
    )
    validate_compatible_dataset_schema(
        upstream_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "上游品种日历 "
    )
    validate_dataset_fragment_schemas(
        upstream_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, UPSTREAM_PARTITION_COLUMNS, "上游品种日历 "
    )

    upstream_filter = None
    if start_date_exclusive is not None:
        upstream_filter = ds.field("trading_date") > start_date_exclusive
    elif start_date is not None:
        upstream_filter = (ds.field("trading_date") >= start_date) & (
            ds.field("trading_date") <= end_date
        )

    upstream_variety_calendar_table = (
        upstream_variety_calendar_dataset.to_table(
            columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            filter=upstream_filter,
        )
    )
    return upstream_variety_calendar_table.to_pandas(
        types_mapper=pd.ArrowDtype
    )


def collect_source_data(
    requested_variety_calendar_df: pd.DataFrame,
    all_variety_trading_dates: list[date],
) -> tuple[pd.DataFrame, dict[date, date], dict[str, int]]:
    empty_contract_catalog_df = pd.DataFrame(
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
    if requested_variety_calendar_df.empty:
        return empty_contract_catalog_df, {}, {
            "api_call_count": 0,
            "contract_count": 0,
            "info_batch_count": 0,
            "boundary_trade_day_call_count": 0,
        }

    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)

    contract_catalog_df = jqdata.get_all_securities(["futures"], date=None)
    if not isinstance(contract_catalog_df, pd.DataFrame):
        raise TypeError(
            "get_all_securities 应返回 pandas.DataFrame，实际为 "
            f"{type(contract_catalog_df).__name__}。"
        )

    contract_catalog_df = (
        contract_catalog_df.rename_axis("contract_code").reset_index()
    )
    required_columns = {"contract_code", "start_date", "end_date"}
    if not required_columns <= set(contract_catalog_df.columns):
        missing_columns = sorted(
            required_columns - set(contract_catalog_df.columns)
        )
        raise ValueError(f"get_all_securities 缺少列：{missing_columns}")

    contract_catalog_df["contract_code"] = (
        contract_catalog_df["contract_code"].astype("string").str.upper()
    )
    parsed_contract_codes_df = (
        contract_catalog_df["contract_code"].str.extract(FIXED_CONTRACT)
    )
    contract_catalog_df = pd.concat(
        [contract_catalog_df, parsed_contract_codes_df],
        axis=1,
    )
    contract_catalog_df = contract_catalog_df.dropna(
        subset=["underlying_code", "delivery_code", "exchange_code"]
    )
    contract_catalog_df = contract_catalog_df.loc[
        ~contract_catalog_df["delivery_code"].isin(
            CONTINUOUS_DELIVERY_CODES
        )
    ].copy()

    if contract_catalog_df.empty:
        raise ValueError(
            "get_all_securities 未返回任何固定月份期货合约。"
        )
    if contract_catalog_df.duplicated(["contract_code"]).any():
        duplicate_contract_codes = sorted(
            contract_catalog_df.loc[
                contract_catalog_df.duplicated(
                    ["contract_code"],
                    keep=False,
                ),
                "contract_code",
            ].unique()
        )
        raise ValueError(
            "get_all_securities 合约代码重复："
            f"{duplicate_contract_codes}"
        )

    contract_catalog_df["start_date"] = pd.to_datetime(
        contract_catalog_df["start_date"],
        errors="raise",
    ).dt.date
    contract_catalog_df["end_date"] = pd.to_datetime(
        contract_catalog_df["end_date"],
        errors="raise",
    ).dt.date
    if contract_catalog_df[["start_date", "end_date"]].isna().any().any():
        raise ValueError(
            "固定月份合约的 start_date/end_date 包含空值。"
        )
    if (
        contract_catalog_df["start_date"]
        > contract_catalog_df["end_date"]
    ).any():
        raise ValueError(
            "固定月份合约存在 start_date 晚于 end_date 的记录。"
        )

    candidate_contract_mask = pd.Series(
        False,
        index=contract_catalog_df.index,
    )
    for (
        exchange_code,
        underlying_code,
    ), requested_group_df in requested_variety_calendar_df.groupby(
        ["exchange_code", "underlying_code"],
        sort=False,
    ):
        requested_trading_dates = sorted(
            requested_group_df["trading_date"].unique().tolist()
        )
        group_contract_mask = (
            contract_catalog_df["exchange_code"].eq(exchange_code)
            & contract_catalog_df["underlying_code"].eq(underlying_code)
        )
        for contract_row in contract_catalog_df.loc[
            group_contract_mask,
            ["start_date", "end_date"],
        ].itertuples():
            first_candidate_position = bisect_left(
                requested_trading_dates,
                contract_row.start_date,
            )
            if (
                first_candidate_position < len(requested_trading_dates)
                and requested_trading_dates[first_candidate_position]
                <= contract_row.end_date
            ):
                candidate_contract_mask.at[contract_row.Index] = True

    contract_catalog_df = contract_catalog_df.loc[
        candidate_contract_mask
    ].copy()
    requested_contract_codes = sorted(
        contract_catalog_df["contract_code"].tolist()
    )
    if not requested_contract_codes:
        raise ValueError(
            "完整合约目录无法覆盖任何请求的上游品种日格点。"
        )

    contract_info_by_code = {}
    info_batch_count = 0
    for offset in range(
        0,
        len(requested_contract_codes),
        INFO_BATCH_SIZE,
    ):
        batch_contract_codes = requested_contract_codes[
            offset : offset + INFO_BATCH_SIZE
        ]
        batch_contract_info = jqdata.get_futures_info(
            batch_contract_codes,
            fields=INFO_FIELDS,
        )
        info_batch_count += 1
        if not isinstance(batch_contract_info, dict):
            raise TypeError(
                "get_futures_info 应返回 dict，实际为 "
                f"{type(batch_contract_info).__name__}。"
            )

        for contract_code, contract_record in batch_contract_info.items():
            normalized_contract_code = str(contract_code).upper()
            if normalized_contract_code in contract_info_by_code:
                raise ValueError(
                    "get_futures_info 重复返回 "
                    f"{normalized_contract_code}。"
                )
            if not isinstance(contract_record, dict):
                raise TypeError(
                    f"{normalized_contract_code} 的合约信息不是 dict。"
                )
            missing_info_fields = sorted(
                set(INFO_FIELDS) - set(contract_record)
            )
            if missing_info_fields:
                raise ValueError(
                    f"{normalized_contract_code} 的 "
                    "get_futures_info 缺少字段："
                    f"{missing_info_fields}"
                )
            contract_info_by_code[normalized_contract_code] = {
                name: contract_record[name]
                for name in INFO_FIELDS
            }

    missing_contract_codes = sorted(
        set(requested_contract_codes) - set(contract_info_by_code)
    )
    unexpected_contract_codes = sorted(
        set(contract_info_by_code) - set(requested_contract_codes)
    )
    if missing_contract_codes:
        raise ValueError(
            "get_futures_info 缺少 "
            f"{len(missing_contract_codes)} 个合约："
            f"{missing_contract_codes[:10]}"
        )
    if unexpected_contract_codes:
        raise ValueError(
            "get_futures_info 返回未请求合约："
            f"{unexpected_contract_codes[:10]}"
        )

    contract_info_df = pd.DataFrame(
        [
            {
                "contract_code": contract_code,
                **contract_record,
            }
            for contract_code, contract_record
            in contract_info_by_code.items()
        ]
    )
    contract_info_df["contract_multiplier"] = pd.to_numeric(
        contract_info_df["contract_multiplier"],
        errors="raise",
    )
    contract_info_df["tick_size"] = pd.to_numeric(
        contract_info_df["tick_size"],
        errors="raise",
    )
    contract_catalog_df = contract_catalog_df.merge(
        contract_info_df,
        on="contract_code",
        how="left",
        validate="one_to_one",
    )

    requested_trading_dates = sorted(
        requested_variety_calendar_df["trading_date"].unique().tolist()
    )
    trading_date_position = {
        trading_date: position
        for position, trading_date
        in enumerate(all_variety_trading_dates)
    }
    boundary_previous_trading_date = None
    boundary_trade_day_call_count = 0
    if any(
        trading_date_position[trading_date] == 0
        for trading_date in requested_trading_dates
    ):
        earliest_trading_date = all_variety_trading_dates[0]
        boundary_window = sorted(
            {
                pd.Timestamp(value).date()
                for value in jqdata.get_trade_days(
                    end_date=earliest_trading_date,
                    count=2,
                )
            }
        )
        earlier_trading_dates = [
            value
            for value in boundary_window
            if value < earliest_trading_date
        ]
        if not earlier_trading_dates:
            raise ValueError(
                f"无法确定 {earliest_trading_date} 的前一交易日。"
            )
        boundary_previous_trading_date = max(
            earlier_trading_dates
        )
        boundary_trade_day_call_count = 1

    previous_trading_date_by_trading_date = {}
    for trading_date in requested_trading_dates:
        position = trading_date_position[trading_date]
        if position == 0:
            previous_trading_date_by_trading_date[trading_date] = (
                boundary_previous_trading_date
            )
        else:
            previous_trading_date_by_trading_date[trading_date] = (
                all_variety_trading_dates[position - 1]
            )

    source_summary = {
        "api_call_count": (
            1
            + info_batch_count
            + boundary_trade_day_call_count
        ),
        "contract_count": len(requested_contract_codes),
        "info_batch_count": info_batch_count,
        "boundary_trade_day_call_count": (
            boundary_trade_day_call_count
        ),
    }
    return (
        contract_catalog_df,
        previous_trading_date_by_trading_date,
        source_summary,
    )


# ## 逐月展开当前有效 Session
# 
# 生成函数一次只接收一个交易所—年月的上游格点。这样既能完整比较规则修订，又不会把全历史合约 Session 同时放入内存。

# In[ ]:


def build_contract_calendar_partition(
    variety_partition_df: pd.DataFrame,
    contract_catalog_by_group: dict[
        tuple[str, str],
        pd.DataFrame,
    ],
    previous_trading_date_by_trading_date: dict[date, date],
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    contract_session_rows = []
    candidate_contract_day_count = 0
    no_valid_rule_contract_day_count = 0
    no_valid_rule_samples = []

    for variety_day in variety_partition_df.itertuples(index=False):
        variety_key = (
            str(variety_day.exchange_code),
            str(variety_day.underlying_code),
        )
        variety_contract_catalog_df = (
            contract_catalog_by_group.get(variety_key)
        )
        if variety_contract_catalog_df is None:
            raise ValueError(
                f"完整合约目录缺少请求的上游品种：{variety_key}"
            )

        active_contract_catalog_df = (
            variety_contract_catalog_df.loc[
                (
                    variety_contract_catalog_df["start_date"]
                    <= variety_day.trading_date
                )
                & (
                    variety_contract_catalog_df["end_date"]
                    >= variety_day.trading_date
                )
            ]
        )

        for contract in active_contract_catalog_df.itertuples(
            index=False
        ):
            candidate_contract_day_count += 1
            trade_time_rules = contract.trade_time
            if (
                trade_time_rules is None
                or trade_time_rules is pd.NA
                or trade_time_rules == []
            ):
                no_valid_rule_contract_day_count += 1
                if len(no_valid_rule_samples) < 10:
                    no_valid_rule_samples.append(
                        (
                            contract.contract_code,
                            variety_day.trading_date,
                        )
                    )
                continue
            if not isinstance(trade_time_rules, (list, tuple)):
                raise ValueError(
                    f"{contract.contract_code} 的 "
                    "trade_time 不是规则列表。"
                )

            matching_trade_time_rules = []
            for trade_time_rule in trade_time_rules:
                if (
                    not isinstance(trade_time_rule, (list, tuple))
                    or len(trade_time_rule) < 3
                ):
                    raise ValueError(
                        f"{contract.contract_code} 包含非法 "
                        f"trade_time 规则：{trade_time_rule!r}"
                    )
                rule_effective_date = pd.Timestamp(
                    trade_time_rule[0]
                ).date()
                rule_expiry_date = pd.Timestamp(
                    trade_time_rule[1]
                ).date()
                if rule_effective_date > rule_expiry_date:
                    raise ValueError(
                        f"{contract.contract_code} 的规则生效日"
                        f"晚于失效日：{trade_time_rule!r}"
                    )
                if (
                    rule_effective_date
                    <= variety_day.trading_date
                    <= rule_expiry_date
                ):
                    matching_trade_time_rules.append(
                        (
                            rule_effective_date,
                            rule_expiry_date,
                            trade_time_rule[2:],
                        )
                    )

            if not matching_trade_time_rules:
                no_valid_rule_contract_day_count += 1
                if len(no_valid_rule_samples) < 10:
                    no_valid_rule_samples.append(
                        (
                            contract.contract_code,
                            variety_day.trading_date,
                        )
                    )
                continue
            if len(matching_trade_time_rules) > 1:
                raise ValueError(
                    f"{contract.contract_code} 在 "
                    f"{variety_day.trading_date} 命中 "
                    f"{len(matching_trade_time_rules)} 条 "
                    "trade_time 规则。"
                )

            (
                rule_effective_date,
                rule_expiry_date,
                session_values,
            ) = matching_trade_time_rules[0]
            previous_trading_date = (
                previous_trading_date_by_trading_date[
                    variety_day.trading_date
                ]
            )
            for session_number, session_value in enumerate(
                session_values,
                start=1,
            ):
                (
                    session_text,
                    start_clock,
                    end_clock,
                ) = parse_session_text(session_value)
                is_night_session = (
                    start_clock >= time(20)
                    or start_clock < time(6)
                )

                if start_clock >= time(20):
                    start_day = previous_trading_date
                elif start_clock < time(6):
                    start_day = (
                        previous_trading_date + timedelta(days=1)
                    )
                else:
                    start_day = variety_day.trading_date
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
                    (
                        session_end_at - session_start_at
                    ).total_seconds()
                    // 60
                )
                contract_multiplier = (
                    None
                    if pd.isna(contract.contract_multiplier)
                    else float(contract.contract_multiplier)
                )
                tick_size = (
                    None
                    if pd.isna(contract.tick_size)
                    else float(contract.tick_size)
                )

                contract_session_rows.append(
                    {
                        "contract_code": contract.contract_code,
                        "exchange_code": variety_day.exchange_code,
                        "underlying_code": (
                            variety_day.underlying_code
                        ),
                        "trading_date": variety_day.trading_date,
                        "list_date": contract.start_date,
                        "delist_date": contract.end_date,
                        "contract_multiplier": contract_multiplier,
                        "tick_size": tick_size,
                        "rule_effective_date": (
                            rule_effective_date
                        ),
                        "rule_expiry_date": rule_expiry_date,
                        "session_number": session_number,
                        "session_text": session_text,
                        "session_start_at": session_start_at,
                        "session_end_at": session_end_at,
                        "is_night_session": is_night_session,
                        "spans_midnight": start_day != end_day,
                        "minute_count": minute_count,
                        "source": SOURCE_NAME,
                        "updated_at": updated_at,
                        "year": variety_day.trading_date.year,
                        "month": variety_day.trading_date.month,
                    }
                )

    if contract_session_rows:
        new_contract_calendar_df = pd.DataFrame(
            contract_session_rows,
            columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
        )
    else:
        new_contract_calendar_df = empty_pandas(
            FUTURES_CONTRACT_CALENDAR_SCHEMA
        )
    new_contract_calendar_df = (
        validate_contract_calendar_frame(
            new_contract_calendar_df,
            "当前期望分区",
        )
    )

    audit = {
        "candidate_contract_day_count": (
            candidate_contract_day_count
        ),
        "no_valid_rule_contract_day_count": (
            no_valid_rule_contract_day_count
        ),
        "no_valid_rule_samples": no_valid_rule_samples,
        "session_row_count": len(new_contract_calendar_df),
    }
    return new_contract_calendar_df, audit


# ## 单分区暂存、提交与失败回滚
# 
# 默认模式只替换尾部新增品种—日期键，显式日期模式保留范围外行，`--full` 替换完整分区。完整脏叶在写 staging 前校验一次；staging 与正式精确叶只复读物理 Schema、表身份 metadata、主键和行数。

# In[ ]:


def commit_partition(
    replacement_contract_calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[str, int, int],
    replacement_variety_date_keys: (
        set[tuple[str, str, date]] | None
    ) = None,
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
) -> int:
    if (replace_start_date is None) != (replace_end_date is None):
        raise ValueError("替换起止日期必须同时提供。")
    if (
        replace_start_date is not None
        and replace_start_date > replace_end_date
    ):
        raise ValueError("替换起始日期不得晚于结束日期。")
    if (
        replacement_variety_date_keys is not None
        and replace_start_date is not None
    ):
        raise ValueError(
            "精确品种日替换与日期区间替换不能同时提供。"
        )

    exchange_code, year, month = partition_key
    replacement_contract_calendar_df = (
        validate_contract_calendar_frame(
            replacement_contract_calendar_df,
            "待提交分区",
        )
    )
    if not replacement_contract_calendar_df.empty:
        replacement_partition_keys = set(
            replacement_contract_calendar_df[
                PARTITION_COLUMNS
            ].itertuples(index=False, name=None)
        )
        if replacement_partition_keys != {
            (exchange_code, year, month)
        }:
            raise ValueError("待提交数据越出指定 Hive 分区。")

        if replacement_variety_date_keys is not None:
            replacement_row_variety_date_keys = set(
                replacement_contract_calendar_df[
                    VARIETY_DATE_COLUMNS
                ].itertuples(index=False, name=None)
            )
            if not replacement_row_variety_date_keys <= (
                replacement_variety_date_keys
            ):
                raise ValueError(
                    "待提交数据越出精确品种日替换范围。"
                )
        if (
            replace_start_date is not None
            and any(
                trading_date < replace_start_date
                or trading_date > replace_end_date
                for trading_date
                in replacement_contract_calendar_df[
                    "trading_date"
                ]
            )
        ):
            raise ValueError(
                "待提交数据越出显式替换日期范围。"
            )

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".c03s-{run_id}"
    backup_path = silver_root / f".c03b-{run_id}"
    quarantine_path = silver_root / f".c03q-{run_id}"
    silver_root.mkdir(parents=True, exist_ok=True)

    for managed_path in (
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ):
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(
                f"数据集路径越出 silver 根目录：{managed_path}"
            )

    existing_contract_calendar_dataset = None
    if (
        target_path.is_dir()
        and any(target_path.rglob("*.parquet"))
    ):
        existing_contract_calendar_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        existing_contract_calendar_schema = pa.schema(
            [
                existing_contract_calendar_dataset.schema.field(
                    name
                )
                for name
                in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            ],
            metadata=(
                existing_contract_calendar_dataset.schema.metadata
            ),
        )
        validate_compatible_dataset_schema(
            existing_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "现有正式数据集 "
        )
        validate_dataset_fragment_schemas(
            existing_contract_calendar_dataset, FUTURES_CONTRACT_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有正式数据集 "
        )

    existing_contract_calendar_df = empty_pandas(
        FUTURES_CONTRACT_CALENDAR_SCHEMA
    )
    if existing_contract_calendar_dataset is not None:
        existing_contract_calendar_table = (
            existing_contract_calendar_dataset.to_table(
                columns=(
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                ),
                filter=(
                    ds.field("exchange_code") == exchange_code
                )
                & (ds.field("year") == year)
                & (ds.field("month") == month),
            )
        )
        existing_contract_calendar_df = arrow_to_pandas(
            existing_contract_calendar_table,
            FUTURES_CONTRACT_CALENDAR_SCHEMA,
        )

    if replacement_variety_date_keys is not None:
        existing_variety_date_index = pd.MultiIndex.from_frame(
            existing_contract_calendar_df[
                VARIETY_DATE_COLUMNS
            ]
        )
        retained_contract_calendar_df = (
            existing_contract_calendar_df.loc[
                ~existing_variety_date_index.isin(
                    replacement_variety_date_keys
                ),
                FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
            ]
        )
        complete_partition_df = pd.concat(
            [
                retained_contract_calendar_df,
                replacement_contract_calendar_df,
            ],
            ignore_index=True,
        )
    elif replace_start_date is not None:
        retained_contract_calendar_df = (
            existing_contract_calendar_df.loc[
                (
                    existing_contract_calendar_df[
                        "trading_date"
                    ]
                    < replace_start_date
                )
                | (
                    existing_contract_calendar_df[
                        "trading_date"
                    ]
                    > replace_end_date
                ),
                FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
            ]
        )
        complete_partition_df = pd.concat(
            [
                retained_contract_calendar_df,
                replacement_contract_calendar_df,
            ],
            ignore_index=True,
        )
    else:
        complete_partition_df = (
            replacement_contract_calendar_df
        )

    complete_partition_df = validate_contract_calendar_frame(
        complete_partition_df,
        "合并后完整分区",
    )
    complete_partition_table = pandas_to_arrow(
        complete_partition_df,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )

    relative_path = pathlib.Path(
        f"exchange_code={exchange_code}",
        f"year={year}",
        f"month={month}",
    )
    staging_path.mkdir(parents=True, exist_ok=False)
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
            staged_contract_calendar_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
            )
            staged_contract_calendar_schema = pa.schema(
                [
                    staged_contract_calendar_dataset.schema.field(
                        name
                    )
                    for name
                    in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                ],
                metadata=(
                    staged_contract_calendar_dataset.schema.metadata
                ),
            )
            validate_compatible_dataset_schema(
                staged_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "staging "
            )
            staged_contract_calendar_table = validate_arrow_table(
                staged_contract_calendar_dataset.to_table(
                    columns=(
                        FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    )
                ),
                FUTURES_CONTRACT_CALENDAR_SCHEMA,
            )
            staged_contract_calendar_df = staged_contract_calendar_table.to_pandas()
            if staged_contract_calendar_df.duplicated(PRIMARY_KEY).any():
                raise ValueError("staging 主键不唯一。")
            if len(staged_contract_calendar_table) != len(complete_partition_table):
                raise ValueError("staging 分区行数检查失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    marker_path = target_path / "schema.parquet"
    marker_created = False
    commit_succeeded = False

    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    saved_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if destination_path.exists():
            shutil.move(
                str(destination_path),
                str(saved_path),
            )
        if source_path.is_dir():
            shutil.move(
                str(source_path),
                str(destination_path),
            )
        elif len(complete_partition_table):
            raise FileNotFoundError(
                f"staging 缺少 {relative_path}。"
            )

        data_files = []
        if not len(complete_partition_table):
            data_files = [
                path
                for path in target_path.rglob("*.parquet")
                if path.name != "schema.parquet"
            ]
        if not len(complete_partition_table) and not data_files and not marker_path.exists():
            physical_schema = pa.schema(
                [
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.field(
                        name
                    )
                    for name
                    in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=(
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata
                ),
            )
            pq.write_table(
                pa.Table.from_batches(
                    [],
                    schema=physical_schema,
                ),
                marker_path,
            )
            marker_created = True

        if destination_path.is_dir():
            committed_partition_dataset = ds.dataset(
                destination_path, format="parquet"
            )
            committed_partition_schema = pa.schema(
                [
                    committed_partition_dataset.schema.field(name)
                    for name in committed_partition_dataset.schema.names
                ],
                metadata=committed_partition_dataset.schema.metadata,
            )
            expected_partition_schema = pa.schema(
                [
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
                    for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata,
            )
            validate_compatible_dataset_schema(
                committed_partition_schema, expected_partition_schema,
                f"正式分区 {relative_path} ",
            )
            physical_primary_key = [
                name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS
            ]
            committed_primary_key_table = committed_partition_dataset.to_table(
                columns=physical_primary_key
            )
            if committed_primary_key_table.to_pandas().duplicated(
                physical_primary_key
            ).any():
                raise ValueError("正式分区主键不唯一。")
            committed_partition_row_count = len(committed_primary_key_table)
        else:
            committed_partition_row_count = 0
        if committed_partition_row_count != len(complete_partition_table):
            raise ValueError("正式分区行数检查失败。")
        commit_succeeded = True
    except Exception as commit_error:
        if marker_created and marker_path.exists():
            marker_path.unlink()

        rollback_errors = []
        try:
            if destination_path.exists():
                isolated_path = (
                    quarantine_path / relative_path
                )
                isolated_path.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                shutil.move(
                    str(destination_path),
                    str(isolated_path),
                )
            if saved_path.exists():
                destination_path.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                shutil.move(
                    str(saved_path),
                    str(destination_path),
                )
        except Exception as rollback_error:
            rollback_errors.append(
                f"{type(rollback_error).__name__}: "
                f"{rollback_error}"
            )

        if rollback_errors:
            raise RuntimeError(
                "分区提交失败且回滚不完整；恢复副本保留在 "
                f"{backup_path}；回滚错误：{rollback_errors}"
            ) from commit_error
        if (
            quarantine_path.exists()
            and any(quarantine_path.rglob("*"))
        ):
            raise RuntimeError(
                "分区提交失败；旧分区已恢复，新分区隔离在 "
                f"{quarantine_path}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)
        if (
            commit_succeeded
            or not any(backup_path.rglob("*.parquet"))
        ):
            shutil.rmtree(backup_path, ignore_errors=True)
        if (
            quarantine_path.exists()
            and not any(quarantine_path.rglob("*"))
        ):
            shutil.rmtree(
                quarantine_path,
                ignore_errors=True,
            )

    return len(replacement_contract_calendar_df)


# ## 命令行入口与三模式差异计划
# 
# 默认模式只读取 b02 中晚于目标最大交易日的尾部；显式日期和 `--full` 强制拉取相应范围。所有模式都先形成差异计划，只有带 `--write` 时才提交。

# In[ ]:


@click.command()
@click.option(
    "--lake-root",
    type=click.Path(path_type=pathlib.Path),
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--full",
    "full_refresh",
    is_flag=True,
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full_refresh: bool,
    write: bool,
) -> None:
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (
        lake_root or formal_lake_root
    ).resolve()

    has_explicit_dates = (
        start_date is not None or end_date is not None
    )
    if (start_date is None) != (end_date is None):
        raise click.UsageError(
            "--start-date 与 --end-date 必须同时提供。"
        )
    if full_refresh and has_explicit_dates:
        raise click.UsageError(
            "--full 与 --start-date/--end-date 不能同时提供。"
        )

    requested_start_date = (
        start_date.date()
        if start_date is not None
        else None
    )
    requested_end_date = (
        end_date.date()
        if end_date is not None
        else None
    )
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter(
            "起始日期不得晚于结束日期。"
        )

    target_path = (
        resolved_lake_root / "silver" / TABLE_NAME
    )
    existing_contract_calendar_dataset = None
    existing_partition_keys = set()
    if (
        target_path.is_dir()
        and any(target_path.rglob("*.parquet"))
    ):
        existing_contract_calendar_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        existing_contract_calendar_schema = pa.schema(
            [
                existing_contract_calendar_dataset.schema.field(
                    name
                )
                for name
                in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            ],
            metadata=(
                existing_contract_calendar_dataset.schema.metadata
            ),
        )
        validate_compatible_dataset_schema(
            existing_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "现有合约日历 "
        )
        validate_dataset_fragment_schemas(
            existing_contract_calendar_dataset, FUTURES_CONTRACT_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有合约日历 "
        )

        # 只有显式/full 双向维护需要发现目标孤儿分区；日常尾部不扫描全根。
        for parquet_path in (
            target_path.rglob("*.parquet")
            if has_explicit_dates or full_refresh
            else []
        ):
            if parquet_path.name == "schema.parquet":
                continue
            relative_parts = parquet_path.relative_to(
                target_path
            ).parts
            if (
                len(relative_parts) != 4
                or not relative_parts[0].startswith(
                    "exchange_code="
                )
                or not relative_parts[1].startswith("year=")
                or not relative_parts[2].startswith("month=")
            ):
                raise ValueError(
                    "正式表包含非法分区文件："
                    f"{parquet_path}"
                )
            existing_partition_keys.add(
                (
                    relative_parts[0].split("=", 1)[1],
                    int(
                        relative_parts[1].split(
                            "=",
                            1,
                        )[1]
                    ),
                    int(
                        relative_parts[2].split(
                            "=",
                            1,
                        )[1]
                    ),
                )
            )

    # 默认水位同时信任正式行与成功自动批次 marker，不再按全历史合约数求差。
    latest_contract_trading_date = None
    if not full_refresh and not has_explicit_dates:
        automatic_watermark_candidates = []
        if existing_contract_calendar_dataset is not None:
            existing_contract_trading_dates = existing_contract_calendar_dataset.to_table(
                columns=["trading_date"],
            ).column("trading_date").to_pylist()
            if existing_contract_trading_dates:
                automatic_watermark_candidates.append(max(existing_contract_trading_dates))
        marker_processed_through = read_automatic_tail_processed_through(target_path)
        if marker_processed_through is not None:
            automatic_watermark_candidates.append(marker_processed_through)
        latest_contract_trading_date = max(automatic_watermark_candidates, default=None)

    if has_explicit_dates:
        mode = "explicit"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root,
            start_date=requested_start_date,
            end_date=requested_end_date,
        )
    elif full_refresh:
        mode = "full"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root
        )
    else:
        mode = "automatic"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root,
            start_date_exclusive=latest_contract_trading_date,
        )
        if requested_variety_calendar_df.empty:
            click.echo(
                f"up_to_date: table={TABLE_NAME}; mode=automatic_tail; "
                f"latest_trading_date={latest_contract_trading_date}; api_calls=0"
            )
            return

    # 对尾部/显式范围，首日的前一交易日由 collect_source_data 的有界边界调用补齐。
    all_variety_trading_dates = sorted(
        requested_variety_calendar_df["trading_date"].drop_duplicates().tolist()
    )
    automatic_tail_processed_through = (
        max(all_variety_trading_dates) if mode == "automatic" else None
    )

    (
        requested_contract_catalog_df,
        previous_trading_date_by_trading_date,
        source_summary,
    ) = collect_source_data(
        requested_variety_calendar_df,
        all_variety_trading_dates,
    )
    click.echo(
        f"source_ready: mode={mode}; "
        f"api_calls={source_summary['api_call_count']}; "
        f"contracts={source_summary['contract_count']}; "
        f"info_batches="
        f"{source_summary['info_batch_count']}; "
        "boundary_trade_day_calls="
        f"{source_summary['boundary_trade_day_call_count']}"
    )

    contract_catalog_by_group = {
        (
            str(exchange_code),
            str(underlying_code),
        ): contract_group_df.reset_index(drop=True)
        for (
            exchange_code,
            underlying_code,
        ), contract_group_df
        in requested_contract_catalog_df.groupby(
            ["exchange_code", "underlying_code"],
            sort=False,
        )
    }
    requested_variety_by_partition = {
        (
            str(exchange_code),
            int(year),
            int(month),
        ): variety_partition_df.reset_index(
            drop=True
        )
        for (
            exchange_code,
            year,
            month,
        ), variety_partition_df
        in requested_variety_calendar_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        )
    }
    requested_keys_by_partition = {
        partition_key: {
            (
                str(row.exchange_code),
                str(row.underlying_code),
                row.trading_date,
            )
            for row
            in variety_partition_df.itertuples(index=False)
        }
        for partition_key, variety_partition_df
        in requested_variety_by_partition.items()
    }

    if mode == "automatic":
        partition_keys = sorted(
            requested_variety_by_partition
        )
    elif mode == "explicit":
        first_requested_month = (
            requested_start_date.year,
            requested_start_date.month,
        )
        last_requested_month = (
            requested_end_date.year,
            requested_end_date.month,
        )
        relevant_existing_partition_keys = {
            partition_key
            for partition_key in existing_partition_keys
            if (
                first_requested_month
                <= partition_key[1:]
                <= last_requested_month
            )
        }
        partition_keys = sorted(
            set(requested_variety_by_partition)
            | relevant_existing_partition_keys
        )
    else:
        partition_keys = sorted(
            set(requested_variety_by_partition)
            | existing_partition_keys
        )

    run_updated_at = datetime.now(timezone.utc)
    dirty_partition_plans = []
    expected_session_count = 0
    missing_session_count = 0
    changed_session_count = 0
    extra_session_count = 0
    no_valid_rule_contract_day_count = 0
    no_valid_rule_samples = []

    for partition_number, partition_key in enumerate(
        partition_keys,
        start=1,
    ):
        if (
            partition_number == 1
            or partition_number % 25 == 0
        ):
            click.echo(
                "planning_progress: "
                f"table={TABLE_NAME}; "
                f"partitions={partition_number}/"
                f"{len(partition_keys)}; "
                f"key={partition_key}"
            )

        requested_variety_partition_df = (
            requested_variety_by_partition.get(
                partition_key
            )
        )
        if requested_variety_partition_df is None:
            expected_contract_calendar_df = empty_pandas(
                FUTURES_CONTRACT_CALENDAR_SCHEMA
            )
            partition_audit = {
                "no_valid_rule_contract_day_count": 0,
                "no_valid_rule_samples": [],
            }
        else:
            (
                expected_contract_calendar_df,
                partition_audit,
            ) = build_contract_calendar_partition(
                requested_variety_partition_df,
                contract_catalog_by_group,
                previous_trading_date_by_trading_date,
                run_updated_at,
            )

        existing_contract_calendar_df = empty_pandas(
            FUTURES_CONTRACT_CALENDAR_SCHEMA
        )
        if (
            existing_contract_calendar_dataset is not None
            and (mode == "automatic" or partition_key in existing_partition_keys)
        ):
            exchange_code, year, month = partition_key
            existing_contract_calendar_table = (
                existing_contract_calendar_dataset.to_table(
                    columns=(
                        FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    ),
                    filter=(
                        ds.field("exchange_code")
                        == exchange_code
                    )
                    & (ds.field("year") == year)
                    & (ds.field("month") == month),
                )
            )
            existing_contract_calendar_df = (
                arrow_to_pandas(
                    existing_contract_calendar_table,
                    FUTURES_CONTRACT_CALENDAR_SCHEMA,
                )
            )

        if mode == "automatic":
            replacement_keys = (
                requested_keys_by_partition[
                    partition_key
                ]
            )
            existing_variety_date_index = (
                pd.MultiIndex.from_frame(
                    existing_contract_calendar_df[
                        VARIETY_DATE_COLUMNS
                    ]
                )
            )
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    existing_variety_date_index.isin(
                        replacement_keys
                    )
                ].reset_index(drop=True)
            )
        elif mode == "explicit":
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        >= requested_start_date
                    )
                    & (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        <= requested_end_date
                    )
                ].reset_index(drop=True)
            )
        else:
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df
            )

        expected_business_rows = business_rows_by_key(
            expected_contract_calendar_df
        )
        existing_quality_error = None
        try:
            validated_existing_contract_calendar_df = (
                validate_contract_calendar_frame(
                    comparison_existing_contract_calendar_df,
                    f"现有分区 {partition_key} ",
                )
            )
            existing_business_rows = business_rows_by_key(
                validated_existing_contract_calendar_df
            )
        except (TypeError, ValueError) as error:
            existing_quality_error = (
                f"{type(error).__name__}: {error}"
            )
            existing_business_rows = {}

        expected_primary_keys = set(
            expected_business_rows
        )
        existing_primary_keys = set(
            existing_business_rows
        )
        missing_primary_keys = (
            expected_primary_keys - existing_primary_keys
        )
        extra_primary_keys = (
            existing_primary_keys - expected_primary_keys
        )
        changed_primary_keys = {
            primary_key
            for primary_key
            in expected_primary_keys & existing_primary_keys
            if (
                expected_business_rows[primary_key]
                != existing_business_rows[primary_key]
            )
        }
        if existing_quality_error is not None:
            missing_primary_keys = expected_primary_keys
            partition_extra_count = len(
                comparison_existing_contract_calendar_df
            )
        else:
            partition_extra_count = len(
                extra_primary_keys
            )

        is_dirty = bool(
            missing_primary_keys
            or changed_primary_keys
            or partition_extra_count
            or existing_quality_error
        )
        if is_dirty:
            dirty_partition_plans.append(
                {
                    "partition_key": partition_key,
                    "expected_row_count": len(
                        expected_contract_calendar_df
                    ),
                    "missing_count": len(
                        missing_primary_keys
                    ),
                    "changed_count": len(
                        changed_primary_keys
                    ),
                    "extra_count": (
                        partition_extra_count
                    ),
                    "quality_error": (
                        existing_quality_error
                    ),
                }
            )

        expected_session_count += len(
            expected_contract_calendar_df
        )
        missing_session_count += len(
            missing_primary_keys
        )
        changed_session_count += len(
            changed_primary_keys
        )
        extra_session_count += partition_extra_count
        no_valid_rule_contract_day_count += int(
            partition_audit[
                "no_valid_rule_contract_day_count"
            ]
        )
        for sample in partition_audit[
            "no_valid_rule_samples"
        ]:
            if len(no_valid_rule_samples) < 10:
                no_valid_rule_samples.append(sample)

    plan_name = {
        "automatic": "auto_plan",
        "explicit": "explicit_plan",
        "full": "full_plan",
    }[mode]
    click.echo(
        f"{plan_name}: table={TABLE_NAME}; "
        "requested_variety_date_count="
        f"{len(requested_variety_calendar_df)}; "
        f"expected_session_count={expected_session_count}; "
        f"missing_session_count={missing_session_count}; "
        f"changed_session_count={changed_session_count}; "
        f"extra_session_count={extra_session_count}; "
        "no_valid_rule_contract_day_count="
        f"{no_valid_rule_contract_day_count}; "
        "touched_partition_count="
        f"{len(dirty_partition_plans)}"
    )
    if no_valid_rule_samples:
        click.echo(
            "warning: no_valid_trade_time_samples="
            f"{no_valid_rule_samples}"
        )
    for partition_plan in dirty_partition_plans:
        click.echo(
            "partition_plan: "
            "partition="
            f"{partition_plan['partition_key']}; "
            "expected_rows="
            f"{partition_plan['expected_row_count']}; "
            f"missing={partition_plan['missing_count']}; "
            f"changed={partition_plan['changed_count']}; "
            f"extra={partition_plan['extra_count']}; "
            "quality_error="
            f"{partition_plan['quality_error']}"
        )

    if not dirty_partition_plans:
        if mode == "automatic" and write:
            write_automatic_tail_processed_through(
                resolved_lake_root, automatic_tail_processed_through
            )
            click.echo(
                "watermark_committed: automatic_tail_processed_through="
                f"{automatic_tail_processed_through}"
            )
        click.echo(
            f"source_unchanged: table={TABLE_NAME}; "
            f"mode={mode}"
        )
        return
    if not write:
        return

    committed_row_count = 0
    for partition_plan in dirty_partition_plans:
        partition_key = partition_plan["partition_key"]
        requested_variety_partition_df = (
            requested_variety_by_partition.get(
                partition_key
            )
        )
        if requested_variety_partition_df is None:
            replacement_contract_calendar_df = (
                empty_pandas(
                    FUTURES_CONTRACT_CALENDAR_SCHEMA
                )
            )
        else:
            (
                replacement_contract_calendar_df,
                _,
            ) = build_contract_calendar_partition(
                requested_variety_partition_df,
                contract_catalog_by_group,
                previous_trading_date_by_trading_date,
                run_updated_at,
            )

        if mode == "automatic":
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
                replacement_variety_date_keys=(
                    requested_keys_by_partition[
                        partition_key
                    ]
                ),
            )
        elif mode == "explicit":
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
                replace_start_date=(
                    requested_start_date
                ),
                replace_end_date=requested_end_date,
            )
        else:
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
            )

    if mode == "automatic":
        write_automatic_tail_processed_through(
            resolved_lake_root, automatic_tail_processed_through
        )

    click.echo(
        f"committed: mode={mode}; "
        f"rows={committed_row_count}; "
        f"partitions={len(dirty_partition_plans)}"
    )


if "ipykernel" not in sys.modules and __name__ == "__main__":
    main()

