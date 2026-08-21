#!/usr/bin/env python
# coding: utf-8

# # c03_macro_release
# 
# 目标表：`fact_macro_release`（中国宏观指标发布事实表）。
# 
# 本入口读取 `dim_macro_release_calendar` 中 `macro_release` 的 required 格点，以
# “上游有效格点 − 正式事实与日历状态共同证明完整的格点”自动求差，然后按年月和
# Eastmoney 报告名分页请求 CPI、PPI、PMI、GDP。来源宽表只按共享原列与数值偏移转换精确待办的系列—报告期格点。
# 
# 正式事实完整但日历状态陈旧时，只从正式事实复读并修复日历，不创建 Eastmoney 会话。

# ## 自动更新与写入边界
# 
# - 空事实表只是“下游完整格点为空”，与少量内部缺口使用同一流程。
# - 每个报告—年月窗口冻结并核对 `pages`、`count`、页长和累计行数；任何部分分页都不提交。
# - 完整响应中某个精确系列值缺失，才把该格点记为 `empty_confirmed`；结构、重复、越界或非法数值不是确认空。
# - Eastmoney 月度/季度 `REPORT_DATE` 使用当月 1 日编码；只按共享频率归一到月末/季末项目报告期，不把它当成发布日期。
# - `available_date` 只取上游日历的 `expected_available_date`。
# - `--write` 是唯一写入语义；显式日期只允许只读或非正式测试湖写入。
# - 完整年月事实叶先经 staging 逐值复读，再替换正式叶；事实正式复读成功后才回写日历。
# - Notebook 是权威源，同名 `.py` 只能由项目统一 PythonExporter 入口生成。

# In[ ]:


from __future__ import annotations

import hashlib
import math
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone


# 从任意子目录运行时，先按项目唯一约定定位根目录。
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
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.data_contracts import (
    MACRO_RELEASE_CALENDAR_SCHEMA,
    MACRO_RELEASE_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.macro_release_entities import MACRO_RELEASE_SERIES
from config.settings import settings


# ## Schema 契约交互浏览
# 
# 按依赖顺序展示上游宏观发布日历和当前宏观事实表。浏览界面不读取湖仓、
# 不调用 API，也不定义第二份表名、字段、主键或分区语义。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
        MACRO_RELEASE_SCHEMA,
    ])


# ## 表名、主键、分区、共享系列与 Eastmoney 边界
# 
# 表名、主键和 Hive 分区只从权威 Schema metadata 解码一次。17 个宏观系列、四个报告名及原列
# 只来自共享宏观配置；Notebook 不复制业务映射。

# In[ ]:


# 上游日历表名、主键和分区是状态读取与完整叶回写的稳定边界。
CALENDAR_TABLE_NAME = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 利率观测与宏观发布日历表。
CALENDAR_PRIMARY_KEY = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集类型—系列—报告/观测日唯一标识。
CALENDAR_PARTITION_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 数据集类型—年—月完整叶分区。

# 当前事实表名、主键和分区是自动求差与完整分区提交的稳定边界。
TABLE_NAME = MACRO_RELEASE_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 中国宏观指标发布事实表。
PRIMARY_KEY = MACRO_RELEASE_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 宏观系列—报告期唯一标识。
PARTITION_COLUMNS = MACRO_RELEASE_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 报告期年—月完整事实叶分区。

DATASET_NAME = "macro_release"  # 上游日历中的宏观发布数据集类型。
EASTMONEY_ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_PAGE_SIZE = 500
EASTMONEY_MAX_PAGES = 10_000

MACRO_SERIES = tuple(
    series
    for series in MACRO_RELEASE_SERIES
    if series.dataset_name == DATASET_NAME
)
SERIES_BY_CODE = {
    series.series_code: series
    for series in MACRO_SERIES
}

_series_by_report: dict[str, list[object]] = {}
for configured_series in MACRO_SERIES:
    _series_by_report.setdefault(
        configured_series.source_api,
        [],
    ).append(configured_series)
SERIES_BY_REPORT = {
    report_name: tuple(configured_series)
    for report_name, configured_series in _series_by_report.items()
}
FIELDS_BY_REPORT = {
    report_name: [
        "REPORT_DATE",
        *[series.source_column for series in configured_series],
    ]
    for report_name, configured_series in SERIES_BY_REPORT.items()
}

if len(MACRO_SERIES) != 17 or len(SERIES_BY_CODE) != 17:
    raise ValueError("共享宏观配置必须恰好提供 17 个唯一宏观系列。")
if set(SERIES_BY_REPORT) != {
    "RPT_ECONOMY_CPI",
    "RPT_ECONOMY_PPI",
    "RPT_ECONOMY_PMI",
    "RPT_ECONOMY_GDP",
}:
    raise ValueError("宏观系列必须恰好来自 CPI、PPI、PMI、GDP 四个报告。")
for report_name, configured_series in SERIES_BY_REPORT.items():
    source_columns = [series.source_column for series in configured_series]
    if len(source_columns) != len(set(source_columns)):
        raise ValueError(f"{report_name} 的共享来源列存在重复。")
if SERIES_BY_CODE["PPI_YOY"].source_column != "BASE_SAME":
    raise ValueError("PPI 同比必须读取 Eastmoney BASE_SAME 原列。")
if {
    series.series_code
    for series in MACRO_SERIES
    if series.source_value_offset == -100.0
} != {
    "CPI_NATIONAL_YTD",
    "CPI_CITY_YTD",
    "CPI_RURAL_YTD",
    "PPI_YTD",
}:
    raise ValueError("CPI/PPI 累计同比的 100 基准偏移配置不完整。")

API_SUCCESS_REASON = (
    "Eastmoney 宏观报告完整分页并通过结构、日期、唯一性与有限数校验；"
    "正式事实复读 1 行。"
)
API_EMPTY_REASON = (
    "Eastmoney 宏观报告完整分页；对应精确系列—报告期未返回有效值，"
    "正式事实复读 0 行。"
)
STATE_REPAIR_REASON = (
    "正式宏观发布事实已经完整复读 1 行；未调用 Eastmoney，"
    "按正式事实修复宏观发布日历状态。"
)

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        MACRO_RELEASE_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
FACT_PARTITIONING = ds.partitioning(
    pa.schema([
        MACRO_RELEASE_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化 Dataset 读取
# 
# 当前日历必须精确匹配权威 Schema/metadata。宏观事实只在无日期自动模式下允许读取
# “字段、类型、nullable 完全兼容但 metadata 过期”的旧表，随后整根 staging 升级；
# 普通生产读取、staging 和正式复读始终要求逐 fragment 精确匹配。

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威顺序重建后比较整表 metadata。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少权威 Schema 字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


def physical_schema_matches(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    # metadata 升级不得掩盖字段、顺序、类型或 nullable 的物理损坏。
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
    if not physical_schema_matches(
        reconstructed_schema(dataset, schema),
        schema,
    ):
        raise TypeError(
            f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。"
        )

    expected_file_schema = pa.schema([
        field
        for field in schema
        if field.name not in partition_columns
    ])
    for fragment in dataset.get_fragments():
        if not physical_schema_matches(
            pa.schema(list(fragment.physical_schema)),
            expected_file_schema,
        ):
            raise TypeError(
                f"{label}存在物理结构不兼容的 fragment：{fragment.path}"
            )

    is_exact = dataset_has_exact_schema_metadata(
        dataset,
        schema,
        partition_columns,
    )
    return dataset, is_exact


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> ds.Dataset:
    dataset, is_exact = open_compatible_dataset(
        table_path,
        partitioning,
        schema,
        partition_columns,
        label,
    )
    if not is_exact:
        raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")
    return dataset


def partition_expression(
    columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None
    for column, value in zip(columns, partition_key, strict=True):
        condition = ds.field(column) == value
        expression = condition if expression is None else expression & condition

    if expression is None:
        raise ValueError("Hive 分区键不得为空。")
    return expression


# ## 上游日历与宏观事实质量校验
# 
# 消费者只检查当前计算直接依赖的上游字段、主键和状态边界，不复制 c01 的完整可用日规则。
# 事实生产者对系列—报告—原列映射、来源、有限值、可用日、分区与审计时间承担完整验证责任。

# In[ ]:


def validate_macro_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    checked = validate_arrow_table(table, MACRO_RELEASE_CALENDAR_SCHEMA)
    frame = arrow_to_pandas(checked, MACRO_RELEASE_CALENDAR_SCHEMA)

    if frame.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}宏观发布日历主键不唯一。")
    if not frame.empty and not frame["dataset_name"].eq(DATASET_NAME).all():
        raise ValueError(f"{context}只允许包含 macro_release 日历行。")

    expected_series_codes = set(SERIES_BY_CODE)
    now_utc = datetime.now(timezone.utc)

    for row in checked.to_pylist():
        if row["series_code"] not in expected_series_codes:
            raise ValueError(f"{context}宏观系列未命中共享配置。")
        if row["actual_record_count"] not in {0, 1}:
            raise ValueError(f"{context}日历正式事实计数只允许 0 或 1。")
        if (
            row["report_date"].year != row["year"]
            or row["report_date"].month != row["month"]
        ):
            raise ValueError(f"{context}日历 year/month 与报告期不一致。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}日历 updated_at 不得晚于当前 UTC 时间。")

        status = row["fetch_result_status"]
        completed = status in {"success", "empty_confirmed"}
        if row["is_fetch_completed"] != completed:
            raise ValueError(f"{context}日历完成布尔值与结果状态不一致。")
        if status == "success" and (
            row["actual_record_count"] != 1
            or row["is_data_missing"]
        ):
            raise ValueError(f"{context}success 日历格点必须对应正式事实 1 行。")
        if status == "empty_confirmed" and (
            row["actual_record_count"] != 0
            or not row["is_data_missing"]
        ):
            raise ValueError(f"{context}empty_confirmed 日历格点必须对应 0 行。")
        if status not in {"success", "empty_confirmed"} and (
            row["actual_record_count"] != 0
            or row["is_data_missing"]
        ):
            raise ValueError(f"{context}未完成日历格点不得声明事实或缺失结论。")

    return frame.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


def validate_macro_release_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    ordered_input_df = frame.loc[:, MACRO_RELEASE_SCHEMA.names].copy()

    # 当前生产器不写空值事实；完整响应缺值由 0 行事实和日历 empty_confirmed 表达。
    if ordered_input_df["value"].isna().any():
        raise ValueError(f"{context}宏观事实 value 不得为空。")
    if ordered_input_df["value"].map(
        lambda value: isinstance(value, (bool, np.bool_))
    ).any():
        raise ValueError(f"{context}宏观事实 value 不得为布尔值。")
    if not ordered_input_df.empty:
        try:
            numeric_values = pd.to_numeric(
                ordered_input_df["value"],
                errors="raise",
            ).astype("float64")
        except Exception as error:
            raise ValueError(f"{context}宏观事实 value 必须为数值。") from error
        if not np.isfinite(numeric_values.to_numpy()).all():
            raise ValueError(f"{context}宏观事实 value 必须为有限数。")
        ordered_input_df["value"] = numeric_values

    table = pandas_to_arrow(ordered_input_df, MACRO_RELEASE_SCHEMA)
    checked = validate_arrow_table(table, MACRO_RELEASE_SCHEMA)
    checked_df = arrow_to_pandas(checked, MACRO_RELEASE_SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}宏观事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        configured_series = SERIES_BY_CODE.get(row["series_code"])
        if configured_series is None:
            raise ValueError(f"{context}宏观系列未命中共享配置。")
        expected_source = f"Eastmoney_{configured_series.source_api}"
        if row["source"] != expected_source:
            raise ValueError(
                f"{context}{row['series_code']} 来源必须为 {expected_source}。"
            )
        if not math.isfinite(row["value"]):
            raise ValueError(f"{context}宏观事实 value 必须为有限数。")
        if row["available_date"] < row["report_date"]:
            raise ValueError(f"{context}可用日不得早于报告期。")
        if (
            row["report_date"].year != row["year"]
            or row["report_date"].month != row["month"]
        ):
            raise ValueError(f"{context}事实 year/month 与报告期不一致。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}事实 updated_at 不得晚于当前 UTC 时间。")

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


def read_macro_calendar(table_path: pathlib.Path) -> pd.DataFrame:
    dataset = open_exact_dataset(
        table_path,
        CALENDAR_PARTITIONING,
        MACRO_RELEASE_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
        "正式宏观发布日历",
    )
    table = dataset.to_table(
        columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
        filter=ds.field("dataset_name") == DATASET_NAME,
    )
    return validate_macro_calendar_table(table, "正式上游")


def read_optional_fact(
    table_path: pathlib.Path,
) -> tuple[pd.DataFrame, bool]:
    if (
        not table_path.is_dir()
        or next(table_path.rglob("*.parquet"), None) is None
    ):
        return empty_pandas(MACRO_RELEASE_SCHEMA), True

    dataset, metadata_is_exact = open_compatible_dataset(
        table_path,
        FACT_PARTITIONING,
        MACRO_RELEASE_SCHEMA,
        PARTITION_COLUMNS,
        "现有正式宏观发布事实",
    )
    source_table = dataset.to_table(columns=MACRO_RELEASE_SCHEMA.names)

    # 物理兼容旧表只在内存中重新附着当前 metadata，随后必须整根升级。
    current_table = pa.Table.from_arrays(
        [source_table[name].combine_chunks() for name in MACRO_RELEASE_SCHEMA.names],
        schema=MACRO_RELEASE_SCHEMA,
    )
    current_df = validate_macro_release_frame(
        arrow_to_pandas(current_table, MACRO_RELEASE_SCHEMA),
        "现有正式",
    )
    return current_df, metadata_is_exact


def table_digest(
    frame: pd.DataFrame,
    schema: pa.Schema,
    primary_key: list[str],
) -> str:
    ordered_df = frame.sort_values(primary_key).reset_index(drop=True)
    source_table = pandas_to_arrow(
        ordered_df.loc[:, schema.names],
        schema,
    )
    stable_table = pa.Table.from_pylist(
        source_table.to_pylist(),
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(stable_table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


# ## 自动求差与无 API 状态修复
# 
# 正式事实存在且质量合格时，不因日历状态陈旧而重拉 API。只有没有正式事实、且日历也没有
# 完整 `empty_confirmed` 证据的 required 格点进入 API 待办。事实不得越出当前 required 上游水位，
# 且事实中的 `available_date` 必须逐值等于上游日历的保守可用日。

# In[ ]:


def fact_grid_count_map(
    fact_df: pd.DataFrame,
) -> dict[tuple[str, date], int]:
    if fact_df.empty:
        return {}

    counts = fact_df.groupby(PRIMARY_KEY, observed=True).size()
    return {
        (str(series_code), report_date): int(count)
        for (series_code, report_date), count in counts.items()
    }


def calendar_grid_is_complete(
    row: pd.Series,
    fact_count: int,
) -> bool:
    if not row["is_fetch_required"]:
        return False
    if row["actual_record_count"] != fact_count:
        return False
    if (
        pd.isna(row["fetch_run_id"])
        or not str(row["fetch_run_id"]).strip()
        or pd.isna(row["fetch_completed_at"])
        or pd.isna(row["quality_checked_at"])
    ):
        return False

    if fact_count == 1:
        return (
            row["is_fetch_completed"]
            and row["fetch_result_status"] == "success"
            and not row["is_data_missing"]
            and row["quality_status"] == "passed"
            and row["quality_reason"]
            in {API_SUCCESS_REASON, STATE_REPAIR_REASON}
        )
    if fact_count == 0:
        return (
            row["is_fetch_completed"]
            and row["fetch_result_status"] == "empty_confirmed"
            and row["is_data_missing"]
            and row["quality_status"] == "warning"
            and row["quality_reason"] == API_EMPTY_REASON
        )
    raise ValueError("单个宏观发布格点正式事实计数只允许 0 或 1。")


def plan_macro_release_grids(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    required_df = calendar_df.loc[
        calendar_df["is_fetch_required"]
    ].copy()
    if start_date is not None:
        required_df = required_df.loc[
            required_df["report_date"].ge(start_date)
            & required_df["report_date"].le(end_date)
        ].copy()

    all_required_df = calendar_df.loc[
        calendar_df["is_fetch_required"]
    ].copy()
    required_by_key = {
        (row.series_code, row.report_date): row
        for row in all_required_df.itertuples(index=False)
    }
    all_required_keys = set(required_by_key)
    fact_keys = set(fact_df[PRIMARY_KEY].itertuples(index=False, name=None))
    outside_watermark = fact_keys - all_required_keys
    if outside_watermark:
        sample = sorted(outside_watermark)[:10]
        raise ValueError(
            "正式宏观发布事实存在越出当前 required 上游水位的格点；"
            f"不得静默删除或忽略：{sample}"
        )

    for fact_row in fact_df.itertuples(index=False):
        key = (fact_row.series_code, fact_row.report_date)
        calendar_row = required_by_key[key]
        if fact_row.available_date != calendar_row.expected_available_date:
            raise ValueError(
                f"正式宏观事实 {key} 的 available_date 与上游日历不一致。"
            )

    fact_counts = fact_grid_count_map(fact_df)
    pending_rows = []
    repair_rows = []
    complete_count = 0

    for _, row in required_df.sort_values(
        ["year", "month", "report_date", "series_code"]
    ).iterrows():
        key = (row["series_code"], row["report_date"])
        fact_count = fact_counts.get(key, 0)
        if fact_count not in {0, 1}:
            raise ValueError(f"宏观发布格点 {key} 的正式事实多于 1 行。")

        if calendar_grid_is_complete(row, fact_count):
            complete_count += 1
        elif fact_count == 1:
            repair_rows.append(row.to_dict())
        else:
            pending_rows.append(row.to_dict())

    columns = list(calendar_df.columns)
    pending_df = pd.DataFrame(pending_rows, columns=columns)
    repair_df = pd.DataFrame(repair_rows, columns=columns)
    return pending_df, repair_df, complete_count


# ## Eastmoney 严格分页与精确格点转换
# 
# 每个报告—年月待办集合使用同一日期范围分页请求。首页冻结 `pages` 和 `count`，后续页必须保持一致；
# 页长、累计行数、日期范围和跨页来源日期唯一性必须完整。Eastmoney 来源日期使用报告月 1 日，
# 转换时按共享频率归一到项目月末/季末报告期；范围内非待办格点只参与来源校验，不覆盖正式事实。

# In[ ]:


class MacroReleaseRequestError(RuntimeError):
    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def create_eastmoney_session() -> requests.Session:
    session = requests.Session()
    retry_policy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "Latitude-Analytics/1.0",
    })
    return session


def query_eastmoney_report_range(
    session: requests.Session,
    report_name: str,
    range_start: date,
    range_end: date,
) -> list[dict[str, object]]:
    fields = FIELDS_BY_REPORT.get(report_name)
    if fields is None:
        raise MacroReleaseRequestError(
            "permanent_error",
            f"未知 Eastmoney 宏观报告：{report_name}",
        )

    page_number = 1
    expected_pages = None
    expected_count = None
    response_rows: list[dict[str, object]] = []

    while True:
        date_filter = (
            f"(REPORT_DATE>='{range_start.isoformat()}')"
            f"(REPORT_DATE<='{range_end.isoformat()}')"
        )
        params = {
            "reportName": report_name,
            "columns": ",".join(fields),
            "filter": date_filter,
            "pageNumber": page_number,
            "pageSize": EASTMONEY_PAGE_SIZE,
            "sortColumns": "REPORT_DATE",
            "sortTypes": "1",
            "source": "WEB",
            "client": "WEB",
        }

        try:
            response = session.get(
                EASTMONEY_ENDPOINT,
                params=params,
                timeout=(10, 45),
            )
            response.raise_for_status()
        except requests.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
                else None
            )
            status = (
                "retryable_error"
                if status_code in {408, 429}
                or (status_code is not None and status_code >= 500)
                else "permanent_error"
            )
            raise MacroReleaseRequestError(
                status,
                "Eastmoney HTTP 请求失败；"
                f"report={report_name}; page={page_number}; status={status_code}。",
            ) from error
        except requests.RequestException as error:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney 请求失败；"
                f"report={report_name}; page={page_number}。",
            ) from error

        try:
            payload = response.json()
        except ValueError as error:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney 未返回有效 JSON；"
                f"report={report_name}; page={page_number}。",
            ) from error
        if not isinstance(payload, dict):
            raise MacroReleaseRequestError(
                "permanent_error",
                "Eastmoney JSON 顶层必须为对象。",
            )

        if payload.get("success") is not True:
            code_value = payload.get("code")
            message = str(payload.get("message") or "").strip()
            if str(code_value) == "9201" and "返回数据为空" in message:
                if page_number != 1:
                    raise MacroReleaseRequestError(
                        "retryable_error",
                        "Eastmoney 后续分页意外返回空响应。",
                    )
                return []

            retryable_markers = [
                "频繁",
                "稍后",
                "繁忙",
                "超时",
                "timeout",
                "temporarily",
            ]
            status = (
                "retryable_error"
                if any(
                    marker.casefold() in message.casefold()
                    for marker in retryable_markers
                )
                else "permanent_error"
            )
            raise MacroReleaseRequestError(
                status,
                "Eastmoney 返回失败；"
                f"report={report_name}; page={page_number}; "
                f"code={code_value}; message={message}。",
            )

        result = payload.get("result")
        if not isinstance(result, dict):
            raise MacroReleaseRequestError(
                "permanent_error",
                "Eastmoney result 必须为对象。",
            )
        data_rows = result.get("data")
        if not isinstance(data_rows, list):
            raise MacroReleaseRequestError(
                "permanent_error",
                "Eastmoney result.data 必须为列表。",
            )

        integer_metadata = []
        for metadata_name in ["pages", "count"]:
            raw_value = result.get(metadata_name)
            if isinstance(raw_value, bool):
                raise MacroReleaseRequestError(
                    "permanent_error",
                    f"Eastmoney {metadata_name} 类型非法。",
                )
            if isinstance(raw_value, int):
                integer_metadata.append(raw_value)
                continue
            if isinstance(raw_value, str) and raw_value.strip().isdigit():
                integer_metadata.append(int(raw_value.strip()))
                continue
            raise MacroReleaseRequestError(
                "permanent_error",
                f"Eastmoney {metadata_name} 必须为整数。",
            )
        page_count, record_count = integer_metadata
        if page_count < 0 or record_count < 0:
            raise MacroReleaseRequestError(
                "permanent_error",
                "Eastmoney pages/count 不得为负。",
            )

        if record_count == 0:
            if page_number != 1 or page_count not in {0, 1} or data_rows:
                raise MacroReleaseRequestError(
                    "permanent_error",
                    "Eastmoney 空响应分页元数据不一致。",
                )
            return []

        calculated_pages = (
            record_count + EASTMONEY_PAGE_SIZE - 1
        ) // EASTMONEY_PAGE_SIZE
        if page_count != calculated_pages:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney pages 与 count 不一致。",
            )
        if page_count > EASTMONEY_MAX_PAGES:
            raise MacroReleaseRequestError(
                "permanent_error",
                "Eastmoney 总页数超过安全上限。",
            )

        if expected_pages is None:
            expected_pages = page_count
            expected_count = record_count
        elif page_count != expected_pages or record_count != expected_count:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney 分页元数据在请求期间漂移。",
            )

        expected_page_rows = (
            EASTMONEY_PAGE_SIZE
            if page_number < expected_pages
            else expected_count - EASTMONEY_PAGE_SIZE * (expected_pages - 1)
        )
        if len(data_rows) != expected_page_rows:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney 当前页行数与分页元数据不一致。",
            )
        response_rows.extend(data_rows)

        if page_number == expected_pages:
            break
        page_number += 1

    if len(response_rows) != expected_count:
        raise MacroReleaseRequestError(
            "retryable_error",
            "Eastmoney 累计行数与 count 不一致。",
        )
    return response_rows


def normalize_macro_release_response(
    response_rows: list[dict[str, object]],
    report_name: str,
    pending_df: pd.DataFrame,
    request_start_date: date,
    request_end_date: date,
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[tuple[str, date], int]]:
    configured_series = SERIES_BY_REPORT.get(report_name)
    if configured_series is None:
        raise ValueError(f"未知 Eastmoney 宏观报告：{report_name}")
    required_fields = set(FIELDS_BY_REPORT[report_name])

    values_by_date: dict[date, dict[str, float | None]] = {}
    for item in response_rows:
        if not isinstance(item, dict):
            raise ValueError("Eastmoney data 元素必须为对象。")
        missing_fields = required_fields - set(item)
        if missing_fields:
            raise ValueError(
                f"Eastmoney {report_name} 缺少字段：{sorted(missing_fields)}"
            )

        try:
            report_timestamp = pd.Timestamp(item["REPORT_DATE"])
        except (TypeError, ValueError) as error:
            raise ValueError("Eastmoney REPORT_DATE 非法。") from error
        if pd.isna(report_timestamp):
            raise ValueError("Eastmoney REPORT_DATE 不得为空。")
        source_report_date = report_timestamp.date()
        if (
            source_report_date < request_start_date
            or source_report_date > request_end_date
        ):
            raise ValueError("Eastmoney REPORT_DATE 越出请求范围。")
        if source_report_date.day != 1:
            raise ValueError("Eastmoney REPORT_DATE 必须使用报告月 1 日编码。")

        report_frequencies = {
            series.frequency
            for series in configured_series
        }
        if len(report_frequencies) != 1:
            raise ValueError(f"{report_name} 的共享频率不唯一。")
        report_frequency = next(iter(report_frequencies))
        if report_frequency == "quarter_end" and source_report_date.month not in {
            3,
            6,
            9,
            12,
        }:
            raise ValueError("Eastmoney GDP REPORT_DATE 不是季度末月份。")
        if report_frequency not in {"month_end", "quarter_end"}:
            raise ValueError(f"{report_name} 使用了未知共享频率。")

        # Eastmoney 用报告月 1 日编码，项目主键统一使用该月最后一个自然日。
        canonical_report_date = (
            pd.Timestamp(source_report_date) + pd.offsets.MonthEnd(0)
        ).date()
        if canonical_report_date in values_by_date:
            raise ValueError("Eastmoney 跨页 REPORT_DATE 重复。")

        parsed_values: dict[str, float | None] = {}
        for series in configured_series:
            raw_value = item[series.source_column]
            if raw_value is None or pd.isna(raw_value):
                parsed_values[series.source_column] = None
                continue
            if isinstance(raw_value, (bool, np.bool_)):
                raise ValueError(
                    f"Eastmoney {series.source_column} 存在布尔值。"
                )
            try:
                numeric_value = float(raw_value)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"Eastmoney {series.source_column} 不是数值。"
                ) from error
            if not math.isfinite(numeric_value):
                raise ValueError(
                    f"Eastmoney {series.source_column} 存在 NaN/Inf。"
                )
            transformed_value = numeric_value + series.source_value_offset
            if not math.isfinite(transformed_value):
                raise ValueError(
                    f"Eastmoney {series.source_column} 数值变换后不是有限数。"
                )
            parsed_values[series.source_column] = transformed_value
        values_by_date[canonical_report_date] = parsed_values

    fact_rows = []
    outcome_counts: dict[tuple[str, date], int] = {}
    for pending_row in pending_df.to_dict("records"):
        series_code = pending_row["series_code"]
        report_date = pending_row["report_date"]
        key = (series_code, report_date)
        series = SERIES_BY_CODE.get(series_code)
        if series is None or series.source_api != report_name:
            raise ValueError(f"待办系列未命中当前报告映射：{series_code}")

        source_values = values_by_date.get(report_date)
        source_value = (
            None
            if source_values is None
            else source_values[series.source_column]
        )
        if source_value is None:
            outcome_counts[key] = 0
            continue

        outcome_counts[key] = 1
        fact_rows.append({
            "series_code": series_code,
            "report_date": report_date,
            "available_date": pending_row["expected_available_date"],
            "value": source_value,
            "source": f"Eastmoney_{report_name}",
            "updated_at": updated_at,
            "year": report_date.year,
            "month": report_date.month,
        })

    fact_df = pd.DataFrame(fact_rows, columns=MACRO_RELEASE_SCHEMA.names)
    if fact_df.empty:
        fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)
    else:
        fact_df = validate_macro_release_frame(fact_df, "Eastmoney 转换")

    expected_keys = set(
        pending_df[["series_code", "report_date"]].itertuples(
            index=False,
            name=None,
        )
    )
    if set(outcome_counts) != expected_keys:
        raise ValueError("Eastmoney 转换结果没有逐个覆盖精确待办格点。")
    return fact_df, outcome_counts


# ## 完整事实叶合并、metadata 升级与事务提交
# 
# 每个触达格点先从旧年月叶删除，再追加本次非空事实；同月未触达系列和日期原样保留。
# 旧 metadata 物理兼容时先完成整根升级，避免在一个 Dataset 中混合不同契约。
# 事务显式记录“旧内容已移走”和“新内容已安装”，两次 move 任一步失败都不会误删旧数据。

# In[ ]:


def full_fact_partition(
    existing_fact_df: pd.DataFrame,
    incoming_fact_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    partition_key: tuple[int, int],
) -> pd.DataFrame:
    year, month = partition_key
    partition_df = existing_fact_df.loc[
        existing_fact_df["year"].eq(year)
        & existing_fact_df["month"].eq(month)
    ].copy()

    for series_code, report_date in touched_grids:
        if (report_date.year, report_date.month) != partition_key:
            raise ValueError("触达宏观发布格点越出指定事实叶分区。")

    if not partition_df.empty:
        existing_keys = pd.MultiIndex.from_frame(partition_df[PRIMARY_KEY])
        touched_index = pd.MultiIndex.from_tuples(
            sorted(touched_grids),
            names=PRIMARY_KEY,
        )
        partition_df = partition_df.loc[
            ~existing_keys.isin(touched_index)
        ].copy()

    merged_df = pd.concat(
        [partition_df, incoming_fact_df],
        ignore_index=True,
    )
    if merged_df.empty:
        return empty_pandas(MACRO_RELEASE_SCHEMA)
    return validate_macro_release_frame(merged_df, "合并后的完整事实分区")


def write_fact_staging(
    frame: pd.DataFrame,
    staging_path: pathlib.Path,
) -> None:
    staging_path.mkdir(parents=True, exist_ok=False)
    file_schema = pa.schema(
        [
            field
            for field in MACRO_RELEASE_SCHEMA
            if field.name not in PARTITION_COLUMNS
        ],
        metadata=MACRO_RELEASE_SCHEMA.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        staging_path / "schema.parquet",
    )

    table = pandas_to_arrow(
        frame.loc[:, MACRO_RELEASE_SCHEMA.names],
        MACRO_RELEASE_SCHEMA,
    )
    if len(table):
        ds.write_dataset(
            table,
            staging_path,
            format="parquet",
            partitioning=FACT_PARTITIONING,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )


def upgrade_fact_metadata(
    existing_fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    complete_df = validate_macro_release_frame(
        existing_fact_df,
        "待升级完整宏观发布事实",
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
        write_fact_staging(complete_df, staging_path)
        staged_dataset = open_exact_dataset(
            staging_path,
            FACT_PARTITIONING,
            MACRO_RELEASE_SCHEMA,
            PARTITION_COLUMNS,
            "宏观发布 metadata 升级 staging",
        )
        staged_df = validate_macro_release_frame(
            arrow_to_pandas(
                staged_dataset.to_table(columns=MACRO_RELEASE_SCHEMA.names),
                MACRO_RELEASE_SCHEMA,
            ),
            "metadata 升级 staging",
        )
        if table_digest(
            staged_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("宏观发布 metadata 升级 staging 逐值复读失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    old_target_moved = False
    new_target_installed = False
    cleanup_recovery_paths = True
    try:
        if target_path.exists():
            shutil.move(str(target_path), str(backup_path))
            old_target_moved = True
        shutil.move(str(staging_path), str(target_path))
        new_target_installed = True

        committed_dataset = open_exact_dataset(
            target_path,
            FACT_PARTITIONING,
            MACRO_RELEASE_SCHEMA,
            PARTITION_COLUMNS,
            "升级后的正式宏观发布事实",
        )
        committed_df = validate_macro_release_frame(
            arrow_to_pandas(
                committed_dataset.to_table(columns=MACRO_RELEASE_SCHEMA.names),
                MACRO_RELEASE_SCHEMA,
            ),
            "升级后的正式",
        )
        if table_digest(
            committed_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("升级后的正式宏观发布事实逐值复读失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if new_target_installed and target_path.exists():
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


def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[int, int],
) -> pd.DataFrame:
    complete_df = validate_macro_release_frame(
        frame,
        "待提交完整宏观发布事实分区",
    )
    if not complete_df.empty:
        actual_keys = set(
            complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交 宏观发布 内容越出指定事实叶分区。")

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
        write_fact_staging(complete_df, staging_path)
        staged_dataset = open_exact_dataset(
            staging_path,
            FACT_PARTITIONING,
            MACRO_RELEASE_SCHEMA,
            PARTITION_COLUMNS,
            "宏观发布事实 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=MACRO_RELEASE_SCHEMA.names,
            filter=partition_expression(PARTITION_COLUMNS, partition_key),
        )
        staged_df = validate_macro_release_frame(
            arrow_to_pandas(staged_table, MACRO_RELEASE_SCHEMA),
            "staging 完整宏观发布分区",
        )
        if table_digest(
            staged_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("宏观发布 staging 完整分区逐值复读失败。")
    except Exception:
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
    old_partition_moved = False
    new_partition_installed = False
    cleanup_recovery_paths = True

    try:
        target_path.mkdir(parents=True, exist_ok=True)
        if not target_marker_path.exists():
            shutil.move(str(staging_marker_path), str(target_marker_path))
            marker_created = True

        if target_had_partition:
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(destination_path), str(saved_path))
            old_partition_moved = True
        if not complete_df.empty:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))
            new_partition_installed = True

        committed_dataset = open_exact_dataset(
            target_path,
            FACT_PARTITIONING,
            MACRO_RELEASE_SCHEMA,
            PARTITION_COLUMNS,
            "正式宏观发布事实",
        )
        committed_table = committed_dataset.to_table(
            columns=MACRO_RELEASE_SCHEMA.names,
            filter=partition_expression(PARTITION_COLUMNS, partition_key),
        )
        committed_df = validate_macro_release_frame(
            arrow_to_pandas(committed_table, MACRO_RELEASE_SCHEMA),
            "正式路径复读的完整宏观发布分区",
        )
        if table_digest(
            committed_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            MACRO_RELEASE_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("正式宏观发布完整分区逐值复读失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if new_partition_installed and destination_path.exists():
                failed_path = quarantine_path / relative_path
                failed_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(failed_path))
            if old_partition_moved and saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
            if marker_created and target_marker_path.exists():
                target_marker_path.unlink()
            if (
                target_path.is_dir()
                and next(target_path.rglob("*.parquet"), None) is None
            ):
                shutil.rmtree(target_path)
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                f"{TABLE_NAME} 分区提交失败且自动回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


# ## 日历完成、失败状态与完整叶提交
# 
# 事实正式复读计数为 1 才写 `success + passed`；完整分页没有精确系列值且正式复读为 0，
# 才写 `empty_confirmed + warning`。连接与分页漂移可重试，权限、结构、日期和数值错误永久失败；
# 失败格点保持未完成。日历按完整 `macro_release/year/month` 叶分区替换并可回滚。

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[str, date], int],
    reason_by_grid: dict[tuple[str, date], str],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    if set(reason_by_grid) != set(grid_counts):
        raise ValueError("日历完成原因必须逐个覆盖正式复读格点。")

    updated_df = calendar_df.copy()
    for index, row in updated_df.iterrows():
        key = (row["series_code"], row["report_date"])
        actual_count = grid_counts.get(key)
        if actual_count is None:
            continue
        if actual_count not in {0, 1}:
            raise ValueError(f"宏观发布 格点 {key} 正式复读计数不是 0 或 1。")

        updated_df.at[index, "is_fetch_completed"] = True
        updated_df.at[index, "fetch_result_status"] = (
            "success" if actual_count == 1 else "empty_confirmed"
        )
        updated_df.at[index, "is_data_missing"] = actual_count == 0
        updated_df.at[index, "actual_record_count"] = actual_count
        updated_df.at[index, "quality_status"] = (
            "passed" if actual_count == 1 else "warning"
        )
        updated_df.at[index, "quality_reason"] = reason_by_grid[key]
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = completed_at
        updated_df.at[index, "quality_checked_at"] = completed_at
        updated_df.at[index, "updated_at"] = completed_at

    return validate_macro_calendar_table(
        pandas_to_arrow(
            updated_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        "更新后的",
    )


def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    failed_grids: set[tuple[str, date]],
    failure_status: str,
    failure_reason: str,
    fetch_run_id: str,
    checked_at: datetime,
) -> pd.DataFrame:
    if failure_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("宏观发布 失败状态只能是 retryable_error 或 permanent_error。")

    updated_df = calendar_df.copy()
    for index, row in updated_df.iterrows():
        key = (row["series_code"], row["report_date"])
        if key not in failed_grids:
            continue

        updated_df.at[index, "is_fetch_completed"] = False
        updated_df.at[index, "fetch_result_status"] = failure_status
        updated_df.at[index, "is_data_missing"] = False
        updated_df.at[index, "actual_record_count"] = 0
        updated_df.at[index, "quality_status"] = "failed"
        updated_df.at[index, "quality_reason"] = failure_reason
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = None
        updated_df.at[index, "quality_checked_at"] = checked_at
        updated_df.at[index, "updated_at"] = checked_at

    return validate_macro_calendar_table(
        pandas_to_arrow(
            updated_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        "失败回写后的",
    )


def commit_calendar_partition(
    calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[str, int, int],
) -> pd.DataFrame:
    complete_calendar_df = validate_macro_calendar_table(
        pandas_to_arrow(
            calendar_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
            MACRO_RELEASE_CALENDAR_SCHEMA,
        ),
        "待提交完整",
    )

    partition_mask = pd.Series(True, index=complete_calendar_df.index)
    for column, value in zip(
        CALENDAR_PARTITION_COLUMNS,
        partition_key,
        strict=True,
    ):
        partition_mask &= complete_calendar_df[column].eq(value)
    partition_df = complete_calendar_df.loc[partition_mask].copy()
    if partition_df.empty:
        raise ValueError("待提交宏观发布日历完整叶分区不得为空。")

    partition_table = pandas_to_arrow(
        partition_df.loc[:, MACRO_RELEASE_CALENDAR_SCHEMA.names],
        MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / CALENDAR_TABLE_NAME
    run_id = uuid.uuid4().hex
    staging_path = silver_root / f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
    quarantine_path = silver_root / f".{CALENDAR_TABLE_NAME}.failed-{run_id}"

    for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"日历路径越出 silver 根目录：{managed_path}")

    # 上游必须已经由 c01 正式提交，c02 不创建或迁移日历根。
    open_exact_dataset(
        target_path,
        CALENDAR_PARTITIONING,
        MACRO_RELEASE_CALENDAR_SCHEMA,
        CALENDAR_PARTITION_COLUMNS,
        "待回写的正式宏观发布日历",
    )

    try:
        staging_path.mkdir(parents=True, exist_ok=False)
        file_schema = pa.schema(
            [
                field
                for field in MACRO_RELEASE_CALENDAR_SCHEMA
                if field.name not in CALENDAR_PARTITION_COLUMNS
            ],
            metadata=MACRO_RELEASE_CALENDAR_SCHEMA.metadata,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=file_schema),
            staging_path / "schema.parquet",
        )
        ds.write_dataset(
            partition_table,
            staging_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )

        staged_dataset = open_exact_dataset(
            staging_path,
            CALENDAR_PARTITIONING,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "宏观发布日历 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                partition_key,
            ),
        )
        staged_df = validate_macro_calendar_table(
            staged_table,
            "staging 完整日历分区",
        )
        if table_digest(
            staged_df,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        ) != table_digest(
            partition_df,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        ):
            raise ValueError("宏观发布日历 staging 完整叶逐值复读失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    relative_path = pathlib.Path(*[
        f"{column}={value}"
        for column, value in zip(
            CALENDAR_PARTITION_COLUMNS,
            partition_key,
            strict=True,
        )
    ])
    source_path = staging_path / relative_path
    destination_path = target_path / relative_path
    saved_path = backup_path / relative_path
    old_partition_moved = False
    new_partition_installed = False
    cleanup_recovery_paths = True

    try:
        if not destination_path.is_dir():
            raise FileNotFoundError(
                f"正式宏观发布日历缺少待回写完整叶：{relative_path}"
            )
        saved_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(destination_path), str(saved_path))
        old_partition_moved = True

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(destination_path))
        new_partition_installed = True

        committed_dataset = open_exact_dataset(
            target_path,
            CALENDAR_PARTITIONING,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "回写后的正式宏观发布日历",
        )
        committed_table = committed_dataset.to_table(
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
            filter=partition_expression(
                CALENDAR_PARTITION_COLUMNS,
                partition_key,
            ),
        )
        committed_df = validate_macro_calendar_table(
            committed_table,
            "正式路径复读的完整日历分区",
        )
        if table_digest(
            committed_df,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        ) != table_digest(
            partition_df,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PRIMARY_KEY,
        ):
            raise ValueError("正式宏观发布日历完整叶逐值复读失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if new_partition_installed and destination_path.exists():
                failed_path = quarantine_path / relative_path
                failed_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(failed_path))
            if old_partition_moved and saved_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(saved_path), str(destination_path))
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                f"{CALENDAR_TABLE_NAME} 分区提交失败且自动回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_df


# ## CLI：自动求差、状态修复、严格分页与最终对账
# 
# 默认使用 `.env` 的正式湖。显式日期参数必须成对出现，并在任何湖仓读取或 API 初始化之前
# 拦截正式湖写入。无 API 状态修复优先提交并复读；重新规划后仍有事实缺口，才创建 Eastmoney 会话。

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

    requested_start_date = start_date.date() if start_date else None
    requested_end_date = end_date.date() if end_date else None
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    fact_path = silver_root / TABLE_NAME

    calendar_df = read_macro_calendar(calendar_path)
    existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)

    if not fact_metadata_is_exact:
        if has_explicit_dates:
            raise click.UsageError(
                "现有宏观发布事实 metadata 已过期；"
                "请先移除日期参数执行一次完整自动升级。"
            )
        click.echo(
            f"metadata_upgrade_required: table={TABLE_NAME}; "
            f"rows={len(existing_fact_df)}"
        )
        if write:
            existing_fact_df = upgrade_fact_metadata(
                existing_fact_df,
                resolved_lake_root,
            )
            click.echo(
                f"metadata_upgraded: table={TABLE_NAME}; "
                f"rows={len(existing_fact_df)}"
            )

    pending_df, repair_df, complete_count = plan_macro_release_grids(
        calendar_df,
        existing_fact_df,
        requested_start_date,
        requested_end_date,
    )
    click.echo(
        f"plan: table={TABLE_NAME}; complete={complete_count}; "
        f"state_repair={len(repair_df)}; api_pending={len(pending_df)}; "
        f"write={write}; lake_root={resolved_lake_root}"
    )

    # 正式事实已经存在的格点只修复日历，不创建 Eastmoney 会话。
    if not repair_df.empty:
        repair_counts = {
            (row.series_code, row.report_date): 1
            for row in repair_df.itertuples(index=False)
        }
        repair_reasons = {key: STATE_REPAIR_REASON for key in repair_counts}
        if write:
            repair_run_id = uuid.uuid4().hex
            repair_time = datetime.now(timezone.utc)
            calendar_df = apply_calendar_completion(
                calendar_df,
                repair_counts,
                repair_reasons,
                repair_run_id,
                repair_time,
            )
            for year, month in sorted(
                set(
                    repair_df[["year", "month"]].itertuples(
                        index=False,
                        name=None,
                    )
                )
            ):
                commit_calendar_partition(
                    calendar_df,
                    resolved_lake_root,
                    (DATASET_NAME, int(year), int(month)),
                )
            calendar_df = read_macro_calendar(calendar_path)
            pending_df, repair_df, complete_count = plan_macro_release_grids(
                calendar_df,
                existing_fact_df,
                requested_start_date,
                requested_end_date,
            )
            click.echo(
                f"state_repaired: rows={len(repair_counts)}; "
                f"remaining_api_pending={len(pending_df)}"
            )
        else:
            click.echo(
                f"dry_run_state_repair: rows={len(repair_counts)}; no_write"
            )

    if pending_df.empty:
        click.echo("complete: no_api_pending; Eastmoney session not created")
        return

    pending_with_report_df = pending_df.copy()
    pending_with_report_df["source_api"] = pending_with_report_df[
        "series_code"
    ].map(lambda series_code: SERIES_BY_CODE[series_code].source_api)

    session = create_eastmoney_session()
    failed_window_count = 0
    try:
        for (
            year,
            month,
            report_name,
        ), partition_pending_df in pending_with_report_df.groupby(
            ["year", "month", "source_api"],
            sort=True,
            observed=True,
        ):
            partition_key = (int(year), int(month))
            first_pending_date = min(partition_pending_df["report_date"])
            last_pending_date = max(partition_pending_df["report_date"])

            # Eastmoney 把月度/季度报告期编码为报告月 1 日；API 过滤必须使用来源日期。
            request_start_date = date(
                first_pending_date.year,
                first_pending_date.month,
                1,
            )
            request_end_date = date(
                last_pending_date.year,
                last_pending_date.month,
                1,
            )
            touched_grids = set(
                partition_pending_df[
                    ["series_code", "report_date"]
                ].itertuples(index=False, name=None)
            )
            fetch_run_id = uuid.uuid4().hex
            request_time = datetime.now(timezone.utc)

            try:
                response_rows = query_eastmoney_report_range(
                    session,
                    str(report_name),
                    request_start_date,
                    request_end_date,
                )
                incoming_fact_df, expected_counts = (
                    normalize_macro_release_response(
                        response_rows,
                        str(report_name),
                        partition_pending_df,
                        request_start_date,
                        request_end_date,
                        request_time,
                    )
                )
            except MacroReleaseRequestError as error:
                failure_status = error.status
                failure_reason = error.reason
            except Exception as error:
                failure_status = "permanent_error"
                failure_reason = (
                    f"Eastmoney {report_name} 响应质检失败：{error}"
                )
            else:
                failure_status = None
                failure_reason = None

            if failure_status is not None:
                failed_window_count += 1
                click.echo(
                    f"api_failure: {year}-{int(month):02d}; "
                    f"report={report_name}; status={failure_status}; "
                    f"grids={len(touched_grids)}; reason={failure_reason}"
                )
                if write:
                    calendar_df = apply_calendar_failure(
                        calendar_df,
                        touched_grids,
                        failure_status,
                        failure_reason,
                        fetch_run_id,
                        datetime.now(timezone.utc),
                    )
                    commit_calendar_partition(
                        calendar_df,
                        resolved_lake_root,
                        (DATASET_NAME, int(year), int(month)),
                    )
                continue

            complete_partition_df = full_fact_partition(
                existing_fact_df,
                incoming_fact_df,
                touched_grids,
                partition_key,
            )
            click.echo(
                f"api_success: {year}-{int(month):02d}; "
                f"report={report_name}; requested_grids={len(touched_grids)}; "
                f"nonempty_grids={sum(expected_counts.values())}; "
                f"complete_partition_rows={len(complete_partition_df)}"
            )

            if not write:
                continue

            committed_partition_df = commit_complete_fact_partition(
                complete_partition_df,
                resolved_lake_root,
                partition_key,
            )
            committed_counts = fact_grid_count_map(committed_partition_df)
            committed_touched_counts = {
                key: committed_counts.get(key, 0)
                for key in touched_grids
            }
            if committed_touched_counts != expected_counts:
                raise ValueError(
                    "正式宏观发布事实复读计数与完整 API 转换结果不一致。"
                )

            completion_reasons = {
                key: API_SUCCESS_REASON if count == 1 else API_EMPTY_REASON
                for key, count in committed_touched_counts.items()
            }
            calendar_df = apply_calendar_completion(
                calendar_df,
                committed_touched_counts,
                completion_reasons,
                fetch_run_id,
                datetime.now(timezone.utc),
            )
            commit_calendar_partition(
                calendar_df,
                resolved_lake_root,
                (DATASET_NAME, int(year), int(month)),
            )

            outside_partition_df = existing_fact_df.loc[
                ~(
                    existing_fact_df["year"].eq(int(year))
                    & existing_fact_df["month"].eq(int(month))
                )
            ]
            existing_fact_df = pd.concat(
                [outside_partition_df, committed_partition_df],
                ignore_index=True,
            )
            existing_fact_df = validate_macro_release_frame(
                existing_fact_df,
                "本次运行累计正式事实",
            )
    finally:
        session.close()

    if failed_window_count:
        raise click.ClickException(
            f"{failed_window_count} 个 Eastmoney 宏观报告窗口失败；"
            "成功分区已提交，失败格点保持未完成。"
        )

    if not write:
        click.echo("dry_run_complete: API 响应已转换和质检；未写事实或日历")
        return

    # 最终必须从两个正式路径重读并再次求差，防止日历领先于事实。
    final_calendar_df = read_macro_calendar(calendar_path)
    final_fact_df, final_metadata_is_exact = read_optional_fact(fact_path)
    if not final_metadata_is_exact:
        raise TypeError("最终正式宏观发布事实 metadata 仍未升级完成。")
    final_pending_df, final_repair_df, final_complete_count = (
        plan_macro_release_grids(
            final_calendar_df,
            final_fact_df,
            requested_start_date,
            requested_end_date,
        )
    )
    if not final_pending_df.empty or not final_repair_df.empty:
        raise RuntimeError(
            "最终正式路径对账仍存在宏观发布 API 待办或日历状态修复格点。"
        )
    click.echo(
        f"complete: formal_reconciled={final_complete_count}; "
        f"fact_rows={len(final_fact_df)}"
    )


# ## 脚本入口
# 
# Notebook 中按单元格阅读和调用函数；导出的 `.py` 直接运行时进入 Click CLI。

# In[ ]:


if __name__ == "__main__":
    main()

