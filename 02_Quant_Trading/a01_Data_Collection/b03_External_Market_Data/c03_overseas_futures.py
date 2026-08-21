#!/usr/bin/env python
# coding: utf-8

# # c03 境外期货日线
# 
# 目标表：`fact_overseas_futures_daily`。
# 
# 本入口只消费 `dim_external_market_calendar` 中 `dataset_name=overseas_futures`、
# `entity_code=ALL` 且 `is_fetch_required=true` 的日期格点。每个待办日期只查询一次 JQData
# `finance.FUT_GLOBAL_DAILY`，保存该日返回的全部境外期货品种行情。
# 

# ## 自动更新、确认空与写入边界
# 
# 自动模式遵循：
# 
# `上游当前 required 日期格点 − 事实与日历状态共同证明完整的格点 = 本次自动更新范围`
# 
# `snapshot_date` 是上游请求日期；JQData `day` 是事实交易日。由于查询条件就是
# `FUT_GLOBAL_DAILY.day == snapshot_date`，非空响应中的两者必须严格相等。查询成功且返回空表时记为
# `empty_confirmed`，不制造空品种事实行。
# 
# 有限 OHLC 值之间若出现 `high/low/open/close` 跨列关系异常，仍按 JQData 原值写入事实表，
# 不得静默修正价格，也不得把该日期当成采集失败；正式复读后在对应日历格点记录 `success + warning`
# 及具体来源异常。NaN、Inf、负成交量和负振幅仍属于禁止提交的硬错误。
# 
# `--write` 只表示是否提交；显式日期只允许只读检查，或写入与 `.env` 正式湖不同的临时/测试湖。
# 事实表经 staging 和正式路径复读成功后，才把对应外部市场日历格点标为完成。
# 

# ## 初始化与权威 Schema
# 

# In[ ]:


from __future__ import annotations

# 标准库负责数值检查、路径切换、分区回滚和批次审计。
import math
import pathlib
import shutil
import sys
import uuid
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

# 第三方库直接承担 CLI、DataFrame 与 Arrow 数据集操作。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# 项目配置只提供权威数据契约、请求实体、JQData 连接和正式湖路径。
from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    OVERSEAS_FUTURES_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.external_market_entities import OVERSEAS_FUTURES_ENTITY_CODE
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings


# ## Schema 契约交互浏览
# 
# 只在交互式 Notebook 内核中展示只读语义浏览界面。依赖顺序为外部市场日历、境外期货事实；
# 展示不会读取数据湖、认证 JQData 或产生写入。
# 

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与 JQData 来源字段
# 

# In[ ]:


# 表名、主键和 Hive 分区只从权威 Schema metadata 读取一次。
CALENDAR_TABLE_NAME = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 外部市场数据采集日历维度表。
CALENDAR_PRIMARY_KEY = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集—请求实体—观测日期格点。
CALENDAR_PARTITION_COLUMNS = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 外部市场日历 Hive 叶分区顺序。

TABLE_NAME = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 境外期货日线事实表。
PRIMARY_KEY = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 境外期货代码—API 交易日业务主键。
PARTITION_COLUMNS = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 境外期货事实 Hive 年—月叶分区顺序。

DATASET_NAME = "overseas_futures"  # 外部市场日历中的境外期货数据集代码。
ENTITY_CODE = OVERSEAS_FUTURES_ENTITY_CODE  # 单次日查询返回全表，对应实体 ALL。
GRID_COLUMNS = ["snapshot_date"]  # 来源请求和完成状态的最小日期格点。

SOURCE = "JQData_finance_FUT_GLOBAL_DAILY"
JQDATA_RESULT_ROWS_LIMIT = 5000

# 显式列顺序来自当前账户已核验的 FUT_GLOBAL_DAILY ORM 字段。
JQDATA_FIELDS = [
    "id",  # 财务库原始记录 ID。
    "code",  # 境外期货品种代码。
    "name",  # 境外期货品种名称。
    "day",  # API 事实交易日。
    "open",  # 开盘价。
    "close",  # 收盘价。
    "low",  # 最低价。
    "high",  # 最高价。
    "volume",  # 成交量。
    "change_pct",  # 来源涨跌幅百分数值。
    "amplitude",  # 来源振幅百分数值。
    "pre_close",  # 前收价。
]

FETCH_RESULT_STATUSES = {
    "pending",
    "success",
    "empty_confirmed",
    "retryable_error",
    "permanent_error",
    "not_required",
}
QUALITY_STATUSES = {
    "pending",
    "passed",
    "warning",
    "failed",
    "not_applicable",
}

# 两张表各自使用权威字段类型构造 Hive 分区，不重复声明类型。
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        EXTERNAL_MARKET_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
FACT_PARTITIONING = ds.partitioning(
    pa.schema([
        OVERSEAS_FUTURES_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与完整业务质检
# 
# 外部市场日历已经由 c01 生成，本入口只复核当前调度和状态回写直接依赖的边界。境外期货事实由本入口
# 完整检查 Schema/metadata、主键、来源、请求日与事实日、来源记录 ID、年月、有限数、成交量和振幅。
# 有限 OHLC 跨列关系异常作为来源质量 warning 保留，不篡改价格，也不拒绝整日事实。
# 

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


def physical_schema_matches(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    # metadata 升级只允许字段名、顺序、类型和 nullable 完全不变。
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


def dataset_has_exact_schema_metadata(
    dataset: ds.Dataset,
    schema: pa.Schema,
    partition_columns: list[str],
) -> bool:
    # Dataset 汇总 Schema 与每个 Parquet fragment 都必须携带当前 metadata。
    if not reconstructed_schema(dataset, schema).equals(
        schema,
        check_metadata=True,
    ):
        return False

    expected_file_schema = pa.schema(
        [field for field in schema if field.name not in partition_columns],
        metadata=schema.metadata,
    )
    return all(
        fragment.physical_schema.equals(
            expected_file_schema,
            check_metadata=True,
        )
        for fragment in dataset.get_fragments()
    )


def open_compatible_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> tuple[ds.Dataset, bool]:
    # 读取 metadata 过期表前，先逐 fragment 证明物理结构仍可无损迁移。
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
    if (
        len(dataset.schema.names) != len(schema.names)
        or set(dataset.schema.names) != set(schema.names)
        or not physical_schema_matches(
            reconstructed_schema(dataset, schema),
            schema,
        )
    ):
        raise TypeError(f"{label}物理字段、类型或 nullable 与权威契约不兼容。")

    expected_file_schema = pa.schema([
        field for field in schema if field.name not in partition_columns
    ])
    for fragment in dataset.get_fragments():
        if not physical_schema_matches(
            pa.schema(list(fragment.physical_schema)),
            expected_file_schema,
        ):
            raise TypeError(
                f"{label}存在物理结构不兼容的 Parquet fragment：{fragment.path}"
            )

    return dataset, dataset_has_exact_schema_metadata(
        dataset,
        schema,
        partition_columns,
    )


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    # staging、上游与提交后输出必须逐 fragment 精确匹配当前 metadata。
    dataset, is_exact = open_compatible_dataset(
        table_path,
        partitioning,
        schema,
        (
            CALENDAR_PARTITION_COLUMNS
            if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA
            else PARTITION_COLUMNS
        ),
        label,
    )
    if not is_exact:
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")

    return dataset


# 日历消费者只检查当前调度和状态回写直接依赖的关系。
def validate_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = validate_arrow_table(
        pandas_to_arrow(
            frame.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ),
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    normalized = arrow_to_pandas(checked, EXTERNAL_MARKET_CALENDAR_SCHEMA)
    if normalized.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}外部市场日历主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        if row["fetch_result_status"] not in FETCH_RESULT_STATUSES:
            raise ValueError(f"{context}采集结果状态不在允许枚举中。")
        if row["quality_status"] not in QUALITY_STATUSES:
            raise ValueError(f"{context}质量状态不在允许枚举中。")
        if not str(row["requirement_reason"]).strip():
            raise ValueError(f"{context}请求原因不得为空。")
        if not str(row["quality_reason"]).strip():
            raise ValueError(f"{context}质量原因不得为空。")
        if (
            row["observation_date"].year != row["year"]
            or row["observation_date"].month != row["month"]
        ):
            raise ValueError(f"{context}年月分区与观测日期不一致。")
        if row["actual_record_count"] < 0:
            raise ValueError(f"{context}实际记录数不得为负。")
        if row["dataset_name"] == DATASET_NAME and row["entity_code"] != ENTITY_CODE:
            raise ValueError(f"{context}境外期货请求实体必须为 ALL。")

        if not row["is_fetch_required"]:
            if row["fetch_result_status"] != "not_required":
                raise ValueError(f"{context}无需请求格点必须为 not_required。")
            if row["is_fetch_completed"] or row["is_data_missing"]:
                raise ValueError(f"{context}无需请求格点不得标记完成或缺失。")
            if row["actual_record_count"] != 0:
                raise ValueError(f"{context}无需请求格点的事实计数必须为零。")
            if row["quality_status"] != "not_applicable":
                raise ValueError(f"{context}无需请求格点必须为 not_applicable。")
        elif row["fetch_result_status"] == "not_required":
            raise ValueError(f"{context}需请求格点不得标为 not_required。")

        # 完成布尔值必须能完全由成功或确认空两种结果复算。
        completed_status = row["fetch_result_status"] in {
            "success",
            "empty_confirmed",
        }
        if row["is_fetch_completed"] != completed_status:
            raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
        if row["is_fetch_completed"]:
            if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                raise ValueError(f"{context}完成格点缺少批次或完成时间。")
        elif row["fetch_completed_at"] is not None:
            raise ValueError(f"{context}未完成格点不得具有完成时间。")

        if row["fetch_result_status"] == "success":
            if row["actual_record_count"] <= 0 or row["is_data_missing"]:
                raise ValueError(f"{context}success 必须有正式事实且不得标记缺失。")
        if row["fetch_result_status"] == "empty_confirmed":
            if row["actual_record_count"] != 0 or not row["is_data_missing"]:
                raise ValueError(f"{context}empty_confirmed 必须为零事实并标记缺失。")
        if row["is_data_missing"] and row["fetch_result_status"] != "empty_confirmed":
            raise ValueError(f"{context}数据缺失只能来自确认空响应。")
        if (
            row["quality_status"] in {"passed", "warning", "failed"}
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


# 事实生产者严格校验字段、日期和有限数；来源 OHLC 跨列异常另行形成 warning。
def validate_overseas_futures_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    # 必须在 Arrow 转换前检查来源数值；否则 Pandas NaN 会被 Arrow 静默转成 null。
    # 真正的 Python None / pd.NA 仍按 Schema 的 nullable 语义处理，NaN/Inf 则明确拒绝。
    source_frame = frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names]
    finite_source_columns = [
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "change_pct",
        "amplitude",
    ]
    for field_name in finite_source_columns:
        for value in source_frame[field_name]:
            if value is None or value is pd.NA:
                continue
            try:
                is_finite = math.isfinite(float(value))
            except (TypeError, ValueError):
                # 非数值类型交给紧随其后的权威 Arrow 类型转换给出契约错误。
                continue
            if not is_finite:
                raise ValueError(f"{context}{field_name} 非空时必须为有限数。")

    checked = validate_arrow_table(
        pandas_to_arrow(
            source_frame,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ),
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(checked, OVERSEAS_FUTURES_DAILY_SCHEMA)
    if normalized.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}境外期货事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        if row["snapshot_date"] != row["trading_date"]:
            raise ValueError(f"{context}请求日期与 API day 不一致。")
        if not str(row["source_instrument_id"]).strip():
            raise ValueError(f"{context}JQData 原始记录 ID 不得为空。")
        if not str(row["instrument_code"]).strip():
            raise ValueError(f"{context}境外期货代码不得为空。")
        if row["instrument_name"] is not None and not str(row["instrument_name"]).strip():
            raise ValueError(f"{context}非空境外期货名称不得为空白。")
        if row["source"] != SOURCE:
            raise ValueError(f"{context}事实来源不一致。")
        if (
            row["trading_date"].year != row["year"]
            or row["trading_date"].month != row["month"]
        ):
            raise ValueError(f"{context}年月分区与 API 交易日不一致。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

        # 境外期货价格可能出现零或负值；NaN/Inf 仍然禁止提交。
        for field_name in ["open", "high", "low", "close", "previous_close"]:
            value = row[field_name]
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{context}{field_name} 非空时必须为有限数。")

        # 有限 OHLC 的跨列关系异常来自供应商原始记录，不在这里改值或拒绝整日。
        # `ohlc_relation_warning_map()` 会按 snapshot_date 汇总异常并回写日历 warning。

        volume = row["volume"]
        if volume is not None and (
            not math.isfinite(volume)
            or volume < 0
        ):
            raise ValueError(f"{context}成交量非空时必须为有限非负数。")
        change_pct = row["change_pct"]
        if change_pct is not None and not math.isfinite(change_pct):
            raise ValueError(f"{context}涨跌幅非空时必须为有限数。")
        amplitude = row["amplitude"]
        if amplitude is not None and (
            not math.isfinite(amplitude)
            or amplitude < 0
        ):
            raise ValueError(f"{context}振幅非空时必须为有限非负数。")

    return normalized.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 有限 OHLC 跨列异常不改变事实值；这里仅生成按请求日期聚合的质量旁证。
def ohlc_relation_warning_map(
    frame: pd.DataFrame,
) -> dict[date, str]:
    if frame.empty:
        return {}

    checked_rows = pandas_to_arrow(
        frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names],
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    ).to_pylist()
    details_by_date: dict[date, list[str]] = {}

    for row in checked_rows:
        high = row["high"]
        low = row["low"]
        relation_issues = []

        if high is not None and low is not None and high < low:
            relation_issues.append(f"high={high} 低于 low={low}")
        for field_name in ["open", "close"]:
            value = row[field_name]
            if value is None:
                continue
            if high is not None and value > high:
                relation_issues.append(
                    f"{field_name}={value} 高于 high={high}"
                )
            if low is not None and value < low:
                relation_issues.append(
                    f"{field_name}={value} 低于 low={low}"
                )

        if not relation_issues:
            continue
        instrument_name = row["instrument_name"] or "无名称"
        detail = (
            f"{row['instrument_code']}（{instrument_name}，"
            f"source_id={row['source_instrument_id']}）："
            + "，".join(relation_issues)
        )
        details_by_date.setdefault(row["snapshot_date"], []).append(detail)

    return {
        snapshot_date: (
            f"JQData 来源存在 {len(details)} 行有限 OHLC 跨列关系异常；"
            "价格保持来源原值，未做修正。明细："
            + "；".join(details)
        )
        for snapshot_date, details in details_by_date.items()
    }


# 空事实目录是合法的全量起点；已有事实则必须精确符合新契约。
def read_optional_fact(table_path: pathlib.Path) -> pd.DataFrame:
    if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
        return empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)

    dataset = open_exact_dataset(
        table_path,
        FACT_PARTITIONING,
        OVERSEAS_FUTURES_DAILY_SCHEMA,
        "正式境外期货事实",
    )
    table = dataset.to_table(columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names)
    return validate_overseas_futures_frame(
        arrow_to_pandas(table, OVERSEAS_FUTURES_DAILY_SCHEMA),
        "正式路径读取的",
    )


# ## JQData 查询边界与严格归一化
# 
# 每个待办日期只查询一次显式字段列表。响应达到 JQData 财务库默认 5000 行上限时无法证明完整，必须失败；
# 来源非空文本不能被静默转成 null，API `day` 必须全部等于请求日期。
# 

# In[ ]:


def query_overseas_futures_grid(
    jqdata: ModuleType,
    snapshot_date: date,
) -> pd.DataFrame:
    table = jqdata.finance.FUT_GLOBAL_DAILY
    query_object = jqdata.query(*[
        getattr(table, field_name)
        for field_name in JQDATA_FIELDS
    ]).filter(table.day == snapshot_date)

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
            f"{error_type}: JQData FUT_GLOBAL_DAILY 查询失败；date={snapshot_date}。"
        ) from error

    if raw_df is None:
        raise RuntimeError(
            f"retryable_error: JQData FUT_GLOBAL_DAILY 返回 None；date={snapshot_date}。"
        )
    return raw_df


def normalize_overseas_futures_response(
    raw_df: pd.DataFrame,
    snapshot_date: date,
    updated_at: datetime,
) -> pd.DataFrame:
    if not isinstance(raw_df, pd.DataFrame):
        raise TypeError("schema_error: JQData 境外期货查询未返回 DataFrame。")
    if raw_df.empty:
        return empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)

    missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
    if missing_columns:
        raise ValueError(f"schema_error: JQData 境外期货表缺列 {sorted(missing_columns)}。")
    if len(raw_df) >= JQDATA_RESULT_ROWS_LIMIT:
        raise ValueError(
            "schema_error: 单日响应达到 finance.run_query 的 5000 行上限，"
            "不能证明结果完整。"
        )

    response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
    response_dates = pd.to_datetime(response_df["day"], errors="coerce").dt.date
    if response_dates.isna().any() or not response_dates.eq(snapshot_date).all():
        raise ValueError("schema_error: JQData day 与待办请求日期不一致。")

    # 来源定义为不可空的 ID 和代码必须在转换前检查，避免字符串化后出现伪值。
    if response_df["id"].isna().any():
        raise ValueError("schema_error: JQData id 含空值。")
    source_ids = response_df["id"].astype("string").str.strip()
    if source_ids.isna().any() or source_ids.eq("").any():
        raise ValueError("schema_error: JQData id 含空白值。")

    if response_df["code"].isna().any():
        raise ValueError("schema_error: JQData code 含空值。")
    instrument_codes = response_df["code"].astype("string").str.strip()
    if instrument_codes.isna().any() or instrument_codes.eq("").any():
        raise ValueError("schema_error: JQData code 含空白值。")

    instrument_names = response_df["name"].astype("string").str.strip()
    if instrument_names.dropna().eq("").any():
        raise ValueError("schema_error: JQData name 的非空值不得为空白。")

    # 只允许真正的来源 None/pd.NA 变成 Arrow null；NaN/Inf 和非数值文本必须失败。
    numeric_columns = [
        "open",
        "close",
        "low",
        "high",
        "volume",
        "change_pct",
        "amplitude",
        "pre_close",
    ]
    numeric_values = {}
    for field_name in numeric_columns:
        for value in response_df[field_name]:
            if value is None or value is pd.NA:
                continue
            try:
                is_finite = math.isfinite(float(value))
            except (TypeError, ValueError):
                # 非数值文本由下面的 invalid_mask 给出明确契约错误。
                continue
            if not is_finite:
                raise ValueError(
                    f"schema_error: JQData {field_name} 含非有限数。"
                )
        converted = pd.to_numeric(response_df[field_name], errors="coerce")
        invalid_mask = response_df[field_name].notna() & converted.isna()
        if invalid_mask.any():
            raise ValueError(
                f"schema_error: JQData {field_name} 含非空非数值文本。"
            )
        numeric_values[field_name] = converted.astype("Float64")

    frame = pd.DataFrame({
        "snapshot_date": snapshot_date,
        "trading_date": response_dates,
        "source_instrument_id": source_ids,
        "instrument_code": instrument_codes,
        "instrument_name": instrument_names,
        "open": numeric_values["open"],
        "high": numeric_values["high"],
        "low": numeric_values["low"],
        "close": numeric_values["close"],
        "volume": numeric_values["volume"],
        "change_pct": numeric_values["change_pct"],
        "amplitude": numeric_values["amplitude"],
        "previous_close": numeric_values["pre_close"],
        "source": SOURCE,
        "updated_at": updated_at,
        "year": snapshot_date.year,
        "month": snapshot_date.month,
    })
    return validate_overseas_futures_frame(
        frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names],
        "JQData 响应转换后的",
    )


# ## 完整格点与自动待办
# 
# 单次全表响应的行数随日期变化，不能用最大日期或固定品种数证明完成。只有外部市场日历完成状态、
# 正式事实按 `snapshot_date` 复读计数、OHLC warning 与审计字段逐项一致，日期格点才算完整。正式事实
# 已经存在但日历状态陈旧时只从事实无 API 修复；真正缺少事实证据的日期才进入 JQData 待办。
# 

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[date, int]:
    if frame.empty:
        return {}
    counts = frame.groupby("snapshot_date", dropna=False).size()
    return {key: int(value) for key, value in counts.items()}


# 同一完成结果只定义一次；首次回写、状态修复和完整性判断必须使用完全相同的状态与原因。
def calendar_completion_result(
    actual_fact_count: int,
    quality_warning_reason: str | None,
) -> tuple[str, bool, str, str]:
    if actual_fact_count < 0:
        raise ValueError("正式事实计数不得为负。")
    if quality_warning_reason is not None and actual_fact_count == 0:
        raise ValueError("零事实格点不得带有 OHLC 关系 warning。")

    if actual_fact_count > 0:
        quality_status = (
            "warning" if quality_warning_reason is not None else "passed"
        )
        quality_reason = (
            f"JQData FUT_GLOBAL_DAILY 响应已转换并从正式境外期货事实复读 {actual_fact_count} 行。"
            + (
                f" {quality_warning_reason}"
                if quality_warning_reason is not None
                else ""
            )
        )
        return "success", False, quality_status, quality_reason

    return (
        "empty_confirmed",
        True,
        "warning",
        "JQData FUT_GLOBAL_DAILY 查询成功且返回空表，正式事实复读为 0 行。",
    )


# 单个日期只有日历审计与正式事实逐项一致时才算完整，陈旧 warning 或 passed 都不能被接受。
def calendar_grid_is_complete(
    row: dict[str, object],
    actual_fact_count: int,
    quality_warning_reason: str | None,
) -> bool:
    if not row["is_fetch_required"] or not row["is_fetch_completed"]:
        return False
    if row["actual_record_count"] != actual_fact_count:
        return False
    if (
        not row["fetch_run_id"]
        or row["fetch_completed_at"] is None
        or row["quality_checked_at"] is None
    ):
        return False

    (
        expected_fetch_status,
        expected_is_missing,
        expected_quality_status,
        expected_quality_reason,
    ) = calendar_completion_result(
        actual_fact_count,
        quality_warning_reason,
    )
    return (
        row["fetch_result_status"] == expected_fetch_status
        and row["is_data_missing"] == expected_is_missing
        and row["quality_status"] == expected_quality_status
        and row["quality_reason"] == expected_quality_reason
    )


# 显式日期仅缩小检查集合；事实缺口和只需修复日历的格点必须分开规划。
def plan_overseas_futures_grids(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    relevant_mask = (
        calendar_df["dataset_name"].eq(DATASET_NAME)
        & calendar_df["entity_code"].eq(ENTITY_CODE)
        & calendar_df["is_fetch_required"].eq(True)
    )
    if start_date is not None:
        relevant_mask &= calendar_df["observation_date"].ge(start_date)
        relevant_mask &= calendar_df["observation_date"].le(end_date)
    relevant_df = calendar_df.loc[relevant_mask].copy()

    calendar_rows = pandas_to_arrow(
        relevant_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
    ).to_pylist()
    fact_counts = grid_count_map(fact_df)
    fact_warning_by_date = ohlc_relation_warning_map(fact_df)
    pending_rows = []
    state_repair_rows = []
    complete_count = 0

    for row in calendar_rows:
        observation_date = row["observation_date"]
        actual_fact_count = fact_counts.get(observation_date, 0)
        quality_warning_reason = fact_warning_by_date.get(observation_date)
        if calendar_grid_is_complete(
            row,
            actual_fact_count,
            quality_warning_reason,
        ):
            complete_count += 1
            continue

        grid = {
            "observation_date": observation_date,
            "year": observation_date.year,
            "month": observation_date.month,
        }

        # 非空正式事实本身足以证明该日 API 全表已经提交；日历陈旧只做无 API 状态修复。
        # 0 行没有事实文件可作证，只有既有完成审计明确为 empty_confirmed 时才可修复。
        empty_result_has_evidence = (
            actual_fact_count == 0
            and row["is_fetch_completed"]
            and row["fetch_result_status"] == "empty_confirmed"
            and row["is_data_missing"]
            and row["actual_record_count"] == 0
            and bool(row["fetch_run_id"])
            and row["fetch_completed_at"] is not None
        )
        if actual_fact_count > 0 or empty_result_has_evidence:
            state_repair_rows.append(grid)
        else:
            pending_rows.append(grid)

    pending_df = pd.DataFrame(
        pending_rows,
        columns=["observation_date", "year", "month"],
    )
    if not pending_df.empty:
        pending_df = pending_df.sort_values(
            ["year", "month", "observation_date"]
        ).reset_index(drop=True)

    state_repair_df = pd.DataFrame(
        state_repair_rows,
        columns=["observation_date", "year", "month"],
    )
    if not state_repair_df.empty:
        state_repair_df = state_repair_df.sort_values(
            ["year", "month", "observation_date"]
        ).reset_index(drop=True)
    return pending_df, state_repair_df, complete_count


# 触达请求日期整体替换，未触达日期在完整月份中原样保留。
def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    partition_mask = pd.Series(True, index=existing_df.index)
    for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
        partition_mask &= existing_df[column].eq(value)

    existing_partition_df = existing_df.loc[
        partition_mask,
        OVERSEAS_FUTURES_DAILY_SCHEMA.names,
    ]
    retained_df = existing_partition_df.loc[
        ~existing_partition_df["snapshot_date"].isin(touched_dates),
        OVERSEAS_FUTURES_DAILY_SCHEMA.names,
    ]
    complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
    if complete_df.empty:
        complete_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
    return validate_overseas_futures_frame(
        complete_df,
        "合并后的完整境外期货分区",
    )


# ## 完整事实叶分区 staging、正式复读与回滚
# 
# 每个年—月事实分区都按完整叶分区提交。触达日期被本次 JQData 结果替换；确认空日期会删除可能残留的
# 旧事实；未触达日期原样保留。staging 和正式路径都必须与待提交 Arrow 表逐值一致。
# 
# 若现有事实仅 metadata 过期而物理字段、顺序、类型与 nullable 完全兼容，则在正常差集之前无 API
# 重写完整事实、精确复读并整根 swap；任何失败都整根恢复旧事实，禁止形成混合 metadata。
# 

# In[ ]:


def fact_partition_expression(
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None
    for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition
    if expression is None:
        raise ValueError("事实分区键不得为空。")
    return expression


# 物理兼容但 metadata 过期的事实必须无 API 整表重写并整根原子替换。
def upgrade_fact_metadata(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    complete_df = validate_overseas_futures_frame(
        frame,
        "metadata 升级前的完整境外期货事实",
    )
    expected_table = pandas_to_arrow(
        complete_df.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names],
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    )

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

    for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    silver_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)
        file_schema = pa.schema(
            [
                field
                for field in OVERSEAS_FUTURES_DAILY_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=OVERSEAS_FUTURES_DAILY_SCHEMA.metadata,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=file_schema),
            staging_path / "schema.parquet",
        )
        if len(expected_table):
            ds.write_dataset(
                expected_table,
                staging_path,
                format="parquet",
                partitioning=FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        # staging 先证明所有 fragment 都是当前 metadata，再逐值比对完整事实。
        staged_dataset = open_exact_dataset(
            staging_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            "境外期货 metadata 升级 staging",
        )
        staged_df = validate_overseas_futures_frame(
            arrow_to_pandas(
                staged_dataset.to_table(
                    columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names
                ),
                OVERSEAS_FUTURES_DAILY_SCHEMA,
            ),
            "metadata 升级 staging 的完整境外期货事实",
        )
        if not pandas_to_arrow(
            staged_df,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ).equals(expected_table):
            raise ValueError("境外期货 metadata 升级 staging 逐值复读失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    target_had_existing = target_path.exists()
    cleanup_recovery_paths = True
    old_target_moved = False
    new_target_installed = False
    try:
        if target_had_existing:
            shutil.move(str(target_path), str(backup_path))
            old_target_moved = True
        shutil.move(str(staging_path), str(target_path))
        new_target_installed = True

        committed_dataset = open_exact_dataset(
            target_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            "metadata 升级后的正式境外期货事实",
        )
        committed_df = validate_overseas_futures_frame(
            arrow_to_pandas(
                committed_dataset.to_table(
                    columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names
                ),
                OVERSEAS_FUTURES_DAILY_SCHEMA,
            ),
            "metadata 升级后的正式境外期货事实",
        )
        if not pandas_to_arrow(
            committed_df,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ).equals(expected_table):
            raise ValueError("境外期货 metadata 升级正式复读逐值失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            # 旧根第一步移动失败时两个标志都为 False，绝不能再碰仍在原位的正式根。
            if (
                new_target_installed
                or old_target_moved
                or not target_had_existing
            ) and target_path.exists():
                shutil.move(str(target_path), str(quarantine_path))
            if old_target_moved and backup_path.exists():
                shutil.move(str(backup_path), str(target_path))
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                f"{TABLE_NAME} metadata 升级失败且自动回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


# 每次只提交一个完整年—月事实叶分区，并保留可回滚旧分区。
def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    complete_df = validate_overseas_futures_frame(frame, "待提交完整境外期货分区")
    if not complete_df.empty:
        actual_keys = set(
            complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交境外期货内容越出指定 Hive 叶分区。")
    complete_table = pandas_to_arrow(
        complete_df.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names],
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    )

    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

    for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")
    silver_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)

        file_schema = pa.schema(
            [
                field
                for field in OVERSEAS_FUTURES_DAILY_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=OVERSEAS_FUTURES_DAILY_SCHEMA.metadata,
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
                partitioning=FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

        # staging 复读必须逐值等于待提交 Arrow 表。
        staged_dataset = open_exact_dataset(
            staging_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            "境外期货 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names,
            filter=fact_partition_expression(partition_key),
        )
        staged_df = validate_overseas_futures_frame(
            arrow_to_pandas(staged_table, OVERSEAS_FUTURES_DAILY_SCHEMA),
            "staging 完整境外期货分区",
        )
        if not pandas_to_arrow(
            staged_df,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("境外期货 staging 完整分区内容检查失败。")
    except Exception:
        # staging 尚未进入正式替换；写入或复读异常只需清理本批目录。
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    relative_path = pathlib.Path(*[
        f"{column}={value}"
        for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True)
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

        # 正式路径复读成功是日历可以推进完成水位的前提。
        committed_dataset = open_exact_dataset(
            target_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            "正式境外期货事实",
        )
        committed_table = committed_dataset.to_table(
            columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names,
            filter=fact_partition_expression(partition_key),
        )
        committed_df = validate_overseas_futures_frame(
            arrow_to_pandas(
                committed_table,
                OVERSEAS_FUTURES_DAILY_SCHEMA,
            ),
            "正式路径复读的完整境外期货分区",
        )
        if not pandas_to_arrow(
            committed_df,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("正式境外期货完整分区内容检查失败。")
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
                f"{TABLE_NAME} 提交失败且回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


# ## 外部市场日历完成、失败状态与完整分区提交
# 
# 事实正式复读后才写 `success` 或 `empty_confirmed`。连接错误属于可重试错误；权限、字段、日期、
# 5000 行截断、NaN/Inf、负成交量或负振幅属于永久错误。有限 OHLC 跨列关系异常仍写 `success`，
# 同时把该请求日期标为 `warning` 并记录来源明细。失败格点保持未完成，后续自动运行继续补缺。
# 

# In[ ]:


# 只有本批已正式复读的日期才写入完成或确认空状态。
def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_results: dict[date, int],
    quality_warning_by_date: dict[date, str],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    unknown_warning_dates = set(quality_warning_by_date) - set(grid_results)
    if unknown_warning_dates:
        raise ValueError(
            "OHLC warning 日期必须属于本批正式复读格点："
            f"{sorted(unknown_warning_dates)}。"
        )
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        if (
            row["dataset_name"] != DATASET_NAME
            or row["entity_code"] != ENTITY_CODE
        ):
            continue
        observation_date = row["observation_date"]
        actual_count = grid_results.get(observation_date)
        if actual_count is None:
            continue

        warning_reason = quality_warning_by_date.get(observation_date)
        (
            fetch_result_status,
            is_data_missing,
            quality_status,
            quality_reason,
        ) = calendar_completion_result(actual_count, warning_reason)
        updated_df.at[index, "is_fetch_completed"] = True
        updated_df.at[index, "fetch_result_status"] = fetch_result_status
        updated_df.at[index, "is_data_missing"] = is_data_missing
        updated_df.at[index, "actual_record_count"] = actual_count
        updated_df.at[index, "quality_status"] = quality_status
        updated_df.at[index, "quality_reason"] = quality_reason
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = completed_at
        updated_df.at[index, "quality_checked_at"] = completed_at
        updated_df.at[index, "updated_at"] = completed_at

    return validate_calendar_frame(updated_df, "境外期货完成状态回写后的")


# 失败状态保留当前正式事实计数，但绝不把日期标为完成。
def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    observation_date: date,
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_count: int,
) -> pd.DataFrame:
    if fetch_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("失败状态不在允许枚举中。")
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        if not (
            row["dataset_name"] == DATASET_NAME
            and row["entity_code"] == ENTITY_CODE
            and row["observation_date"] == observation_date
        ):
            continue
        updated_df.at[index, "is_fetch_completed"] = False
        updated_df.at[index, "fetch_result_status"] = fetch_status
        updated_df.at[index, "is_data_missing"] = False
        updated_df.at[index, "actual_record_count"] = current_fact_count
        updated_df.at[index, "quality_status"] = "failed"
        updated_df.at[index, "quality_reason"] = failure_reason
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = None
        updated_df.at[index, "quality_checked_at"] = failed_at
        updated_df.at[index, "updated_at"] = failed_at

    return validate_calendar_frame(updated_df, "境外期货失败状态回写后的")


# 日历按 dataset—年—月完整提交，并保留同月其他请求实体。
def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_dates: set[date],
    lake_root: pathlib.Path,
) -> int:
    if not touched_dates:
        return 0

    touched_mask = (
        calendar_df["dataset_name"].eq(DATASET_NAME)
        & calendar_df["entity_code"].eq(ENTITY_CODE)
        & calendar_df["observation_date"].isin(touched_dates)
    )
    touched_df = calendar_df.loc[touched_mask]
    partition_keys = set(
        touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None)
    )

    for partition_key in sorted(partition_keys):
        partition_mask = pd.Series(True, index=calendar_df.index)
        for column, value in zip(
            CALENDAR_PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            partition_mask &= calendar_df[column].eq(value)
        complete_df = validate_calendar_frame(
            calendar_df.loc[
                partition_mask,
                EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
            ],
            "待提交的完整外部市场日历分区",
        )
        complete_table = pandas_to_arrow(
            complete_df,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        )

        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
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
            staging_path.mkdir(parents=True, exist_ok=False)

            file_schema = pa.schema(
                [
                    field
                    for field in EXTERNAL_MARKET_CALENDAR_SCHEMA
                    if field.name not in CALENDAR_PARTITION_COLUMNS
                ],
                metadata=EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=file_schema),
                staging_path / "schema.parquet",
            )
            ds.write_dataset(
                complete_table,
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            staged_dataset = open_exact_dataset(
                staging_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "外部市场日历 staging",
            )
            staged_table = staged_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                filter=(
                    (ds.field("dataset_name") == partition_key[0])
                    & (ds.field("year") == partition_key[1])
                    & (ds.field("month") == partition_key[2])
                ),
            )
            staged_df = validate_calendar_frame(
                arrow_to_pandas(staged_table, EXTERNAL_MARKET_CALENDAR_SCHEMA),
                "staging 完整外部市场日历分区",
            )
            if not pandas_to_arrow(
                staged_df,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ).equals(complete_table):
                raise ValueError("外部市场日历 staging 内容检查失败。")
        except Exception:
            # 尚未移动正式分区时失败，只需删除本次 staging。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        saved_path = backup_path / relative_path
        target_had_partition = destination_path.exists()
        cleanup_recovery_paths = True

        try:
            if target_had_partition:
                saved_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(saved_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))

            committed_dataset = open_exact_dataset(
                target_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "正式外部市场日历",
            )
            committed_table = committed_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                filter=(
                    (ds.field("dataset_name") == partition_key[0])
                    & (ds.field("year") == partition_key[1])
                    & (ds.field("month") == partition_key[2])
                ),
            )
            committed_df = validate_calendar_frame(
                arrow_to_pandas(
                    committed_table,
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "正式路径复读的完整外部市场日历分区",
            )
            if not pandas_to_arrow(
                committed_df,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ).equals(complete_table):
                raise ValueError("正式外部市场日历分区内容检查失败。")
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
            except Exception as rollback_error:
                rollback_errors.append(str(rollback_error))

            if rollback_errors:
                cleanup_recovery_paths = False
                raise RuntimeError(
                    "外部市场日历提交失败且回滚未完成；"
                    f"请检查 {backup_path} 与 {quarantine_path}。"
                ) from commit_error
            raise
        finally:
            if cleanup_recovery_paths:
                shutil.rmtree(staging_path, ignore_errors=True)
                shutil.rmtree(backup_path, ignore_errors=True)
                shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(touched_df)


# ## CLI：自动差集、JQData 查询、分区进度与是否写入
# 
# 默认从正式外部市场日历寻找全部缺口。待办按事实年—月叶分区顺序处理；带 `--write` 时每完成一个
# 月份就提交事实并回写日历。旧事实 metadata 升级先于差集且不认证 JQData；完成升级与正常差集后，
# 只有确有 API 待办时才认证 JQData。
# 

# In[ ]:


# CLI 先执行正式湖显式日期门禁，再读取日历或认证 JQData。
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
    fact_path = silver_root / TABLE_NAME

    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
        "正式外部市场日历",
    )
    calendar_df = validate_calendar_frame(
        arrow_to_pandas(
            calendar_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
            ),
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ),
        "正式路径读取的",
    )

    # 事实物理兼容但 metadata 过期时，先无 API 整根升级，再进入正常差集。
    fact_metadata_upgrade_required = False
    if fact_path.is_dir() and next(fact_path.rglob("*.parquet"), None):
        fact_dataset, fact_schema_is_exact = open_compatible_dataset(
            fact_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "正式境外期货事实",
        )
        fact_df = validate_overseas_futures_frame(
            arrow_to_pandas(
                fact_dataset.to_table(
                    columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names
                ),
                OVERSEAS_FUTURES_DAILY_SCHEMA,
            ),
            "正式路径读取的",
        )
        fact_metadata_upgrade_required = not fact_schema_is_exact
    else:
        fact_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)

    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    if fact_metadata_upgrade_required:
        click.echo(
            "metadata_upgrade_plan: scope=full_table; "
            f"rows={len(fact_df)}; api_requests=0; write={str(write).lower()}"
        )
        if write:
            fact_df = upgrade_fact_metadata(fact_df, resolved_lake_root)
            click.echo(
                f"metadata_upgraded: rows={len(fact_df)}; "
                "full_root_swap=true; api_requests=0"
            )
        else:
            click.echo("metadata_upgrade_preview_only: api_requests=0")

    # 正式事实和日历状态共同参与差集；已有完整事实只修复状态，不重复调用 API。
    pending_df, state_repair_df, complete_count = plan_overseas_futures_grids(
        calendar_df,
        fact_df,
        requested_start,
        requested_end,
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"state_repair_count={len(state_repair_df)}; "
        f"api_pending_grid_count={len(pending_df)}"
    )
    if pending_df.empty and state_repair_df.empty:
        click.echo("境外期货事实与外部市场日历状态已经完整一致。")
        return

    batch_id = uuid.uuid4().hex

    # 正式事实已完整但日历质量状态陈旧时，直接从事实复算并提交日历，不认证 JQData。
    if not state_repair_df.empty:
        repair_dates = set(state_repair_df["observation_date"].tolist())
        click.echo(
            f"state_repair_plan: grids={len(repair_dates)}; "
            f"write={str(write).lower()}; api_requests=0"
        )
        if write:
            fact_counts = grid_count_map(fact_df)
            fact_warning_by_date = ohlc_relation_warning_map(fact_df)
            repair_grid_results = {
                observation_date: fact_counts.get(observation_date, 0)
                for observation_date in repair_dates
            }
            repair_quality_warning_by_date = {
                observation_date: fact_warning_by_date[observation_date]
                for observation_date in repair_dates
                if observation_date in fact_warning_by_date
            }
            repaired_at = datetime.now(timezone.utc)
            calendar_df = apply_calendar_completion(
                calendar_df,
                repair_grid_results,
                repair_quality_warning_by_date,
                f"state-repair-{batch_id}",
                repaired_at,
            )
            repaired_calendar_rows = commit_calendar_partitions(
                calendar_df,
                repair_dates,
                resolved_lake_root,
            )

            # 从正式日历重新规划；状态修复不得残留，也不得把修复格点转成 API 待办。
            repaired_calendar_dataset = open_exact_dataset(
                calendar_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "状态修复后的正式外部市场日历",
            )
            calendar_df = validate_calendar_frame(
                arrow_to_pandas(
                    repaired_calendar_dataset.to_table(
                        columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                    ),
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "状态修复后的正式",
            )
            (
                pending_df,
                remaining_state_repair_df,
                _,
            ) = plan_overseas_futures_grids(
                calendar_df,
                fact_df,
                requested_start,
                requested_end,
            )
            if not remaining_state_repair_df.empty:
                raise RuntimeError("境外期货日历状态修复后仍存在陈旧格点。")
            if repair_dates & set(pending_df["observation_date"].tolist()):
                raise RuntimeError("已由正式事实修复的格点错误进入 API 待办。")
            click.echo(
                f"state_repaired: grids={len(repair_dates)}; "
                f"calendar_rows={repaired_calendar_rows}; api_requests=0"
            )
        elif pending_df.empty:
            click.echo("state_repair_preview_only: api_requests=0")
            return

    if pending_df.empty:
        click.echo("境外期货正式事实完整，日历状态已完成无 API 修复。")
        return

    # 只有真正缺少正式事实的日期才认证；纯状态修复永远不会消耗供应商连接。
    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    partition_groups = list(
        pending_df.groupby(["year", "month"], sort=True)
    )
    click.echo(f"pending_partitions={len(partition_groups)}")

    total_rows = 0
    processed_grid_count = 0

    # 按事实年—月顺序推进，已完成月份可在后续重启时直接扣除。
    for group_number, (raw_partition_key, group_df) in enumerate(
        partition_groups,
        start=1,
    ):
        partition_key = tuple(raw_partition_key)
        updated_at = datetime.now(timezone.utc)
        frames = []
        api_quality_warning_by_date: dict[date, str] = {}

        click.echo(
            f"partition_start: {group_number}/{len(partition_groups)}; "
            f"key={partition_key}; grids={len(group_df)}"
        )
        for observation_date in group_df["observation_date"].tolist():
            try:
                raw_df = query_overseas_futures_grid(jqdata, observation_date)
                grid_df = normalize_overseas_futures_response(
                    raw_df,
                    observation_date,
                    updated_at,
                )
                grid_quality_warning = ohlc_relation_warning_map(grid_df)
                api_quality_warning_by_date.update(grid_quality_warning)
            except Exception as error:
                message = str(error)
                if not message.startswith(("retryable_error:", "permanent_error:")):
                    message = f"permanent_error: {message}"

                # 请求或转换失败时只回写失败格点，不提交当前未完成月份事实。
                if write:
                    fetch_status = (
                        "retryable_error"
                        if message.startswith("retryable_error:")
                        else "permanent_error"
                    )
                    current_fact_count = grid_count_map(fact_df).get(
                        observation_date,
                        0,
                    )
                    calendar_df = apply_calendar_failure(
                        calendar_df,
                        observation_date,
                        fetch_status,
                        f"境外期货采集失败：{message}",
                        batch_id,
                        datetime.now(timezone.utc),
                        current_fact_count,
                    )
                    commit_calendar_partitions(
                        calendar_df,
                        {observation_date},
                        resolved_lake_root,
                    )
                raise click.ClickException(message) from error

            click.echo(
                f"api_success: date={observation_date}; rows={len(grid_df)}"
            )
            if observation_date in api_quality_warning_by_date:
                click.echo(
                    "api_quality_warning: "
                    + api_quality_warning_by_date[observation_date]
                )
            frames.append(grid_df)

        incoming_df = (
            validate_overseas_futures_frame(
                pd.concat(frames, ignore_index=True),
                "本分区 JQData 响应汇总后的",
            )
            if any(not frame.empty for frame in frames)
            else empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
        )
        if not write:
            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            continue

        touched_dates = set(group_df["observation_date"].tolist())
        complete_df = full_fact_partition(
            fact_df,
            incoming_df,
            touched_dates,
            partition_key,
        )
        committed_partition_df = commit_complete_fact_partition(
            complete_df,
            resolved_lake_root,
            partition_key,
        )

        partition_counts = grid_count_map(committed_partition_df)
        grid_results = {
            observation_date: partition_counts.get(observation_date, 0)
            for observation_date in touched_dates
        }
        committed_warning_map = ohlc_relation_warning_map(
            committed_partition_df
        )
        committed_quality_warning_by_date = {
            observation_date: committed_warning_map[observation_date]
            for observation_date in touched_dates
            if observation_date in committed_warning_map
        }
        expected_quality_warning_by_date = {
            observation_date: api_quality_warning_by_date[observation_date]
            for observation_date in touched_dates
            if observation_date in api_quality_warning_by_date
        }
        if committed_quality_warning_by_date != expected_quality_warning_by_date:
            raise RuntimeError(
                "JQData 响应与正式事实复读的 OHLC warning 不一致。"
            )
        completed_at = datetime.now(timezone.utc)
        calendar_df = apply_calendar_completion(
            calendar_df,
            grid_results,
            committed_quality_warning_by_date,
            batch_id,
            completed_at,
        )
        calendar_rows = commit_calendar_partitions(
            calendar_df,
            touched_dates,
            resolved_lake_root,
        )

        # 后续月份继续使用本次已经提交的正式事实内容。
        fact_df = pd.concat([
            fact_df.loc[
                ~(
                    fact_df[PARTITION_COLUMNS]
                    .apply(tuple, axis=1)
                    .isin({partition_key})
                )
            ],
            committed_partition_df,
        ], ignore_index=True)

        total_rows += len(incoming_df)
        processed_grid_count += len(group_df)
        click.echo(
            f"partition_committed: key={partition_key}; "
            f"calendar_rows={calendar_rows}; grids={len(group_df)}"
        )

    if write:
        # 全部分区结束后再次从正式路径求差，防止日历状态领先于事实。
        final_calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            "最终正式外部市场日历",
        )
        final_calendar_df = validate_calendar_frame(
            arrow_to_pandas(
                final_calendar_dataset.to_table(
                    columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                ),
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ),
            "最终正式",
        )
        final_fact_df = read_optional_fact(fact_path)
        remaining_df, remaining_state_repair_df, _ = plan_overseas_futures_grids(
            final_calendar_df,
            final_fact_df,
            requested_start,
            requested_end,
        )
        if not remaining_df.empty or not remaining_state_repair_df.empty:
            raise RuntimeError("境外期货提交后仍存在事实缺口或陈旧日历状态。")

    click.echo(
        f"finished: grids={processed_grid_count}; rows={total_rows}; "
        f"write={str(write).lower()}"
    )


# ## Notebook 与脚本运行入口
# 

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖自动 dry-run；测试写入必须显式使用非正式湖。
    main.main(
        args=[],
        prog_name="c03_overseas_futures",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

