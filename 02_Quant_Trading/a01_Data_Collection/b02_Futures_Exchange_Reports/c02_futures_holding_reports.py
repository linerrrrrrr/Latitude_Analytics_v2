#!/usr/bin/env python
# coding: utf-8

# # c02 JQData 期货会员成交持仓排名
# 
# 目标表：`fact_futures_position_rank_daily` 与 `fact_futures_member_position_daily`。
# 
# 本入口只消费 `dim_futures_exchange_report_calendar` 中 `position_rank` 和 `member_position`
# 两类 required 格点。每个交易所—品种—交易日只查询一次 JQData
# `finance.FUT_MEMBER_POSITION_RANK`，再从同一长表响应同时生成两张事实表。

# ## 自动更新与写入边界
# 
# 自动模式遵循：
# 
# `上游当前 required 报告格点 − 两张事实与日历状态共同证明完整的格点 = 本次自动更新范围`
# 
# 空事实表只是下游完整格点集合为空。`--write` 只表示是否写入；显式日期只允许只读检查，
# 或写入与 `.env` 正式湖不同的临时/测试湖。事实正式复读成功前不回写报告日历。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

import pathlib
import re
import shutil
import sys
import uuid
from collections.abc import Callable
from datetime import date, datetime, timezone
from types import ModuleType

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
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

from config.data_contracts import (
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    FUTURES_POSITION_RANK_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings


# ## Schema 契约交互浏览
# 
# 只在交互式 Notebook 内核中展示统一的只读语义浏览界面。依赖顺序为报告日历、排名事实、
# 参与者类型事实；展示不会读取数据湖、认证 JQData 或产生写入。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        FUTURES_POSITION_RANK_DAILY_SCHEMA,
        FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与来源字段

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


# ## 契约化读取与三张表的业务质检
# 
# 报告日历已经由 c01 生成，本入口只重新检查自身直接依赖和将要回写的状态关系；两张事实表则执行
# 完整字段、主键、来源、排名、数量、日期和分区质检。

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


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    # 正式输入、staging 与提交后输出都执行同一精确物理契约检查。
    parquet_files = (
        list(table_path.rglob("*.parquet"))
        if table_path.is_dir()
        else []
    )
    if not parquet_files:
        raise FileNotFoundError(f"{label}不存在：{table_path}")

    dataset = ds.dataset(
        table_path,
        format="parquet",
        partitioning=partitioning,
    )
    if not reconstructed_schema(dataset, schema).equals(
        schema,
        check_metadata=True,
    ):
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")

    return dataset


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
            if row["fetch_result_status"] != "not_required" or row["is_fetch_completed"]:
                raise ValueError(f"{context}无需采集格点的执行状态不一致。")
            if row["quality_status"] != "not_applicable" or row["is_data_missing"]:
                raise ValueError(f"{context}无需采集格点的质量状态不一致。")
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


def read_optional_table(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    validator: Callable[[pd.DataFrame, str], pd.DataFrame],
    label: str,
) -> pd.DataFrame:
    if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
        return empty_pandas(schema)

    dataset = open_exact_dataset(table_path, partitioning, schema, label)
    table = dataset.to_table(columns=schema.names)
    return validator(arrow_to_pandas(table, schema), f"{label}的")


# ## JQData 长表响应归一化
# 
# `FUT_MEMBER_POSITION_RANK` 每一行只代表一种排名类别。本入口以 `rank_type` 的业务文字识别成交量、
# 持买仓和持卖仓，再按合约—会员透视；`rank_type_ID` 只用于检查同一响应中的类别编码没有自相矛盾。

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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: JQData 排名查询未返回 DataFrame。")
    if raw_df.empty:
        return (
            empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA),
            empty_pandas(FUTURES_MEMBER_POSITION_DAILY_SCHEMA),
        )

    missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
    if missing_columns:
        raise ValueError(f"schema_error: JQData 排名表缺列 {sorted(missing_columns)}。")
    if len(raw_df) >= 5000:
        raise ValueError("schema_error: 单格点响应达到 run_query 上限，不能证明结果完整。")

    response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
    response_df["day"] = pd.to_datetime(response_df["day"], errors="coerce").dt.date
    if response_df["day"].isna().any() or not response_df["day"].eq(grid["trading_date"]).all():
        raise ValueError("schema_error: JQData 排名响应包含请求交易日之外的行。")

    response_underlying = response_df["underlying_code"].astype(str).str.strip().str.upper()
    if not response_underlying.eq(grid["underlying_code"]).all():
        raise ValueError("schema_error: JQData 排名响应品种与待办日历不一致。")
    response_exchanges = set(
        response_df["exchange"].dropna().astype(str).str.strip().str.upper()
    )
    allowed_exchanges = JQDATA_RESPONSE_EXCHANGES.get(grid["exchange_code"], {grid["exchange_code"]})
    if not response_exchanges or not response_exchanges.issubset(allowed_exchanges):
        raise ValueError(
            "schema_error: JQData 排名响应交易所与待办日历不一致；"
            f"actual={sorted(response_exchanges)}。"
        )

    position_rows: dict[tuple[str, str], dict[str, object]] = {}
    member_rows: dict[tuple[str, str], dict[str, object]] = {}
    rank_type_ids: dict[int, str] = {}

    for source_row in response_df.to_dict("records"):
        metric_name = metric_name_from_rank_type(source_row["rank_type"])
        rank_type_id = int(source_row["rank_type_ID"])
        previous_metric = rank_type_ids.setdefault(rank_type_id, metric_name)
        if previous_metric != metric_name:
            raise ValueError("schema_error: 同一 rank_type_ID 对应多个业务类别。")

        rank_value = pd.to_numeric(source_row["rank"], errors="coerce")
        indicator_value = pd.to_numeric(source_row["indicator"], errors="coerce")
        increase_value = pd.to_numeric(source_row["indicator_increase"], errors="coerce")
        if pd.isna(rank_value) or int(rank_value) != rank_value or rank_value <= 0:
            raise ValueError("schema_error: JQData 排名名次必须是正整数。")
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
            if row[metric_name] is not None:
                raise ValueError("schema_error: 同一合约—会员—排名类别出现重复行。")
            row[f"{metric_name}_rank"] = int(rank_value)
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
            if row[metric_name] is not None:
                raise ValueError("schema_error: 同一合约—参与者类型—排名类别出现重复行。")

        row[metric_name] = float(indicator_value)
        row[metric_change_name] = None if pd.isna(increase_value) else float(increase_value)

    position_df = pd.DataFrame(
        position_rows.values(),
        columns=FUTURES_POSITION_RANK_DAILY_SCHEMA.names,
    ) if position_rows else empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA)
    member_df = pd.DataFrame(
        member_rows.values(),
        columns=FUTURES_MEMBER_POSITION_DAILY_SCHEMA.names,
    ) if member_rows else empty_pandas(FUTURES_MEMBER_POSITION_DAILY_SCHEMA)

    return (
        validate_position_frame(position_df, "JQData 转换后的"),
        validate_member_frame(member_df, "JQData 转换后的"),
    )


# ## 完整格点与自动待办
# 
# 报告明细行数无法事先精确预测，因此“事实目录中有若干行”不能单独证明一次可变长度 API 响应已经完整。
# 只有报告日历完成状态、正式事实复读计数和状态审计字段共同一致，格点才从自动待办中扣除。

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
            and row["quality_status"] == "passed"
        )
    return (
        row["fetch_result_status"] == "empty_confirmed"
        and row["is_data_missing"]
        and row["expected_record_count"] == 1
        and row["quality_status"] == "warning"
    )


def pending_report_grids(
    calendar_df: pd.DataFrame,
    position_df: pd.DataFrame,
    member_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, int]:
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
        for row in pandas_to_arrow(
            relevant_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ).to_pylist()
    }
    grid_keys = sorted({key[:3] for key in rows_by_grid_dataset})
    position_counts = grid_count_map(position_df)
    member_counts = grid_count_map(member_df)
    pending_rows = []
    complete_count = 0

    for grid_key in grid_keys:
        position_row = rows_by_grid_dataset.get((*grid_key, "position_rank"))
        member_row = rows_by_grid_dataset.get((*grid_key, "member_position"))
        if position_row is None or member_row is None:
            raise ValueError(f"报告日历缺少成对的排名/参与者格点：{grid_key}")

        is_complete = (
            calendar_grid_is_complete(position_row, position_counts.get(grid_key, 0))
            and calendar_grid_is_complete(member_row, member_counts.get(grid_key, 0))
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
    return pending_df, complete_count


def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
    partition_columns: list[str],
    partition_key: tuple[object, ...],
    schema: pa.Schema,
    validator: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
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
    return validator(complete_df, "合并后的完整事实分区")


# ## 完整叶分区 staging、正式复读与回滚
# 
# 每张事实表和每个报告日历叶分区都按“完整叶分区”提交：先写系统 staging 并复读，再备份旧叶目录、
# 替换、正式复读；失败时恢复旧分区。两张事实都正式复读后，才更新两类报告日历状态。

# In[ ]:


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None
    for column, value in zip(partition_columns, partition_key, strict=True):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition
    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


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
    complete_df = validator(frame, "待提交完整分区")
    if not complete_df.empty:
        actual_keys = set(
            complete_df[partition_columns].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交内容越出指定 Hive 叶分区。")
    complete_table = pandas_to_arrow(complete_df.loc[:, schema.names], schema)

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

        staged_dataset = open_exact_dataset(staging_path, partitioning, schema, f"{table_name} staging")
        staged_table = staged_dataset.to_table(
            columns=schema.names,
            filter=partition_expression(partition_columns, partition_key),
        )
        staged_df = validator(arrow_to_pandas(staged_table, schema), "staging 完整分区")
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
    saved_path = backup_path / relative_path
    target_marker_path = target_path / "schema.parquet"
    staging_marker_path = staging_path / "schema.parquet"
    target_had_partition = destination_path.exists()
    marker_created = False
    cleanup_recovery_paths = True

    try:
        target_path.mkdir(parents=True, exist_ok=True)
        if not target_marker_path.exists():
            shutil.move(str(staging_marker_path), str(target_marker_path))
            marker_created = True

        if target_had_partition:
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(destination_path), str(saved_path))
        if len(complete_table):
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))

        committed_dataset = open_exact_dataset(target_path, partitioning, schema, f"正式 {table_name}")
        committed_table = committed_dataset.to_table(
            columns=schema.names,
            filter=partition_expression(partition_columns, partition_key),
        )
        committed_df = validator(
            arrow_to_pandas(committed_table, schema),
            "正式路径复读的完整分区",
        )
        if not pandas_to_arrow(committed_df.loc[:, schema.names], schema).equals(complete_table):
            raise ValueError("正式完整分区内容检查失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if destination_path.exists():
                failed_path = quarantine_path / relative_path
                failed_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(failed_path))
            if target_had_partition and saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
            if marker_created and target_marker_path.exists():
                target_marker_path.unlink()
            if target_path.is_dir() and next(target_path.rglob("*.parquet"), None) is None:
                shutil.rmtree(target_path)
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                f"{table_name} 提交失败且回滚未完成；请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[object, ...], tuple[int, int]],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    updated_df = calendar_df.copy()
    dataset_positions = {"position_rank": 0, "member_position": 1}

    for index, row in updated_df.iterrows():
        dataset_name = row["dataset_name"]
        if dataset_name not in dataset_positions:
            continue
        grid_key = tuple(row[column] for column in GRID_COLUMNS)
        counts = grid_counts.get(grid_key)
        if counts is None:
            continue

        actual_count = counts[dataset_positions[dataset_name]]
        has_rows = actual_count > 0
        updated_df.at[index, "is_fetch_completed"] = True
        updated_df.at[index, "fetch_result_status"] = "success" if has_rows else "empty_confirmed"
        updated_df.at[index, "is_data_missing"] = not has_rows
        updated_df.at[index, "expected_record_count"] = 1
        updated_df.at[index, "actual_record_count"] = actual_count
        updated_df.at[index, "quality_status"] = "passed" if has_rows else "warning"
        updated_df.at[index, "quality_reason"] = (
            f"JQData 排名响应已转换并从正式事实表复读 {actual_count} 行。"
            if has_rows
            else "JQData 查询成功但本数据集没有可落盘记录，已按确认空记录。"
        )
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = completed_at
        updated_df.at[index, "quality_checked_at"] = completed_at
        updated_df.at[index, "updated_at"] = completed_at

    return validate_calendar_frame(updated_df, "事实完成状态回写后的")


def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[object, ...], tuple[int, int]],
    lake_root: pathlib.Path,
) -> int:
    if not grid_counts:
        return 0
    grid_key_set = set(grid_counts)
    touched_mask = calendar_df[GRID_COLUMNS].apply(tuple, axis=1).isin(grid_key_set)
    touched_df = calendar_df.loc[
        touched_mask & calendar_df["dataset_name"].isin(DATASET_NAMES)
    ]
    partition_keys = set(
        touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None)
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
    return len(touched_df)


# ## JQData 查询边界
# 
# 每个待办格点只执行一次显式字段查询。连接和 Windows TUN 出口由共享连接模块负责；业务入口只负责
# 构造 `FUT_MEMBER_POSITION_RANK` 查询、识别返回协议和区分权限/Schema 错误与可重试连接错误。

# In[ ]:


def query_rank_grid(
    jqdata: ModuleType,
    grid: dict[str, object],
) -> pd.DataFrame:
    table = jqdata.finance.FUT_MEMBER_POSITION_RANK
    query_object = jqdata.query(*[
        getattr(table, field_name)
        for field_name in JQDATA_FIELDS
    ]).filter(
        table.day == grid["trading_date"],
        table.underlying_code == grid["underlying_code"],
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
            f"{error_type}: JQData FUT_MEMBER_POSITION_RANK 查询失败；grid={grid}。"
        ) from error

    if raw_df is None:
        raise RuntimeError(
            f"retryable_error: JQData 排名查询返回 None；grid={grid}。"
        )
    return raw_df


# ## CLI：自动差集、分区进度与是否写入
# 
# 默认从正式报告日历自动寻找所有缺口。待办按事实叶分区顺序处理；带 `--write` 时每完成一个品种月份，
# 立即提交两张事实并回写两类日历状态，因此后续人工重启会从剩余格点继续。

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

    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        "正式报告日历",
    )
    calendar_df = validate_calendar_frame(
        arrow_to_pandas(
            calendar_dataset.to_table(
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
            ),
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        ),
        "正式路径读取的",
    )
    position_df = read_optional_table(
        position_path,
        POSITION_PARTITIONING,
        FUTURES_POSITION_RANK_DAILY_SCHEMA,
        validate_position_frame,
        "正式排名事实",
    )
    member_df = read_optional_table(
        member_path,
        MEMBER_PARTITIONING,
        FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
        validate_member_frame,
        "正式参与者类型事实",
    )

    pending_df, complete_count = pending_report_grids(
        calendar_df,
        position_df,
        member_df,
        requested_start,
        requested_end,
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"pending_grid_count={len(pending_df)}"
    )
    if pending_df.empty:
        click.echo("两张持仓事实及报告日历状态已经完整一致。")
        return

    # 只有确实存在 API 待办时才认证，纯完整性检查不会消耗供应商连接。
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    partition_groups = list(
        pending_df.groupby(
            ["exchange_code", "underlying_code", "year", "month"],
            sort=True,
        )
    )
    click.echo(f"pending_partitions={len(partition_groups)}")

    total_position_rows = 0
    total_member_rows = 0
    completed_grid_count = 0

    for group_number, (partition_key, group_df) in enumerate(partition_groups, start=1):
        updated_at = datetime.now(timezone.utc)
        batch_id = uuid.uuid4().hex
        position_frames = []
        member_frames = []

        click.echo(
            f"partition_start: {group_number}/{len(partition_groups)}; "
            f"key={partition_key}; grids={len(group_df)}"
        )
        for grid in group_df[GRID_COLUMNS].to_dict("records"):
            raw_df = query_rank_grid(jqdata, grid)
            position_grid_df, member_grid_df = normalize_rank_response(
                raw_df,
                grid,
                updated_at,
            )
            position_frames.append(position_grid_df)
            member_frames.append(member_grid_df)

        incoming_position_df = (
            validate_position_frame(
                pd.concat(position_frames, ignore_index=True),
                "本分区 JQData 汇总后的",
            )
            if any(not frame.empty for frame in position_frames)
            else empty_pandas(FUTURES_POSITION_RANK_DAILY_SCHEMA)
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
            f"api_success: key={partition_key}; "
            f"position_rows={len(incoming_position_df)}; "
            f"member_rows={len(incoming_member_df)}"
        )
        if not write:
            total_position_rows += len(incoming_position_df)
            total_member_rows += len(incoming_member_df)
            completed_grid_count += len(group_df)
            continue

        touched_dates = set(group_df["trading_date"].tolist())
        complete_position_df = full_fact_partition(
            position_df,
            incoming_position_df,
            touched_dates,
            POSITION_PARTITION_COLUMNS,
            partition_key,
            FUTURES_POSITION_RANK_DAILY_SCHEMA,
            validate_position_frame,
        )
        complete_member_df = full_fact_partition(
            member_df,
            incoming_member_df,
            touched_dates,
            MEMBER_PARTITION_COLUMNS,
            partition_key,
            FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
            validate_member_frame,
        )

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
            for grid in group_df[GRID_COLUMNS].to_dict("records")
        }
        completed_at = datetime.now(timezone.utc)
        calendar_df = apply_calendar_completion(
            calendar_df,
            grid_counts,
            batch_id,
            completed_at,
        )
        calendar_rows = commit_calendar_partitions(
            calendar_df,
            grid_counts,
            resolved_lake_root,
        )

        # 后续分区继续使用本次已提交的正式状态和事实内容。
        position_df = pd.concat([
            position_df.loc[
                ~(
                    position_df[POSITION_PARTITION_COLUMNS]
                    .apply(tuple, axis=1)
                    .isin({partition_key})
                )
            ],
            committed_position_partition_df,
        ], ignore_index=True)
        member_df = pd.concat([
            member_df.loc[
                ~(
                    member_df[MEMBER_PARTITION_COLUMNS]
                    .apply(tuple, axis=1)
                    .isin({partition_key})
                )
            ],
            committed_member_partition_df,
        ], ignore_index=True)

        total_position_rows += len(incoming_position_df)
        total_member_rows += len(incoming_member_df)
        completed_grid_count += len(group_df)
        click.echo(
            f"partition_committed: key={partition_key}; "
            f"calendar_rows={calendar_rows}; grids={len(group_df)}"
        )

    if write:
        # 最终复读三张正式表，确认最后一批提交后仍满足完整契约。
        final_calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            "最终正式报告日历",
        )
        validate_calendar_frame(
            arrow_to_pandas(
                final_calendar_dataset.to_table(
                    columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ),
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ),
            "最终正式",
        )

    click.echo(
        f"finished: grids={completed_grid_count}; "
        f"position_rows={total_position_rows}; member_rows={total_member_rows}; "
        f"write={str(write).lower()}"
    )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖自动 dry-run；带写入的测试必须显式使用非正式湖。
    main.main(
        args=[],
        prog_name="c02_futures_holding_reports",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

