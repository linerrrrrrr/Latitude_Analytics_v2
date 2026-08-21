#!/usr/bin/env python
# coding: utf-8

# # c02_interest_rate
# 
# 目标表：`fact_interest_rate_daily`（SHIBOR 期限利率日表）。
# 
# 本入口读取 `dim_macro_release_calendar` 中 `interest_rate` 的 required 格点，以
# “上游有效格点 − 正式事实与日历状态共同证明完整的格点”自动求差，然后按年月窗口调用
# Tushare Pro `pro.shibor`。来源宽表只转换精确待办的系列—日期格点，不覆盖范围内已经完整的事实。
# 
# 事实完整但日历状态陈旧时，只从正式事实复读并修复日历，不认证或重拉 Tushare。

# ## 自动更新与写入边界
# 
# - 空事实表只是“下游完整格点为空”，与少量内部缺口使用同一流程。
# - 每个月最多发起一次 `pro.shibor` 范围请求；API 单日宽行拆成最多 8 个独立事实格点。
# - 成功响应中某个精确期限值缺失，才把该格点记为 `empty_confirmed`；结构、重复、越界或非法数值不是确认空。
# - `--write` 是唯一写入语义；显式日期只允许只读或非正式测试湖写入。
# - 完整年月事实叶先经 staging 逐值复读，再替换正式叶；事实正式复读成功后才回写日历。
# - Notebook 是权威源，同名 `.py` 只能由项目统一 PythonExporter 入口生成。

# In[ ]:


from __future__ import annotations

import hashlib
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

from config.data_contracts import (
    INTEREST_RATE_DAILY_SCHEMA,
    MACRO_RELEASE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.macro_release_entities import MACRO_RELEASE_SERIES
from config.settings import settings


# ## Schema 契约交互浏览
# 
# 按依赖顺序展示上游宏观发布日历和当前 SHIBOR 事实表。浏览界面不读取湖仓、
# 不调用 API，也不定义第二份表名、字段、主键或分区语义。

# In[ ]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
        INTEREST_RATE_DAILY_SCHEMA,
    ])


# ## 表名、主键、分区、来源列与质量边界
# 
# 表名、主键和 Hive 分区只从对应权威 Schema metadata 解码一次。8 个期限及 Tushare 原列
# 只来自共享宏观配置；Notebook 不再复制一份手工映射。

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
TABLE_NAME = INTEREST_RATE_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # SHIBOR 期限利率日表。
PRIMARY_KEY = INTEREST_RATE_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # SHIBOR 期限系列—观测日唯一标识。
PARTITION_COLUMNS = INTEREST_RATE_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 观测年—月完整事实叶分区。

DATASET_NAME = "interest_rate"  # 上游日历中的 SHIBOR 数据集类型。
SOURCE_NAME = "Tushare_shibor"  # 事实来源固定值。
SHIBOR_FIELDS = ["date"]  # Tushare 范围请求显式字段，以下追加 8 个期限列。

INTEREST_RATE_SERIES = tuple(
    series
    for series in MACRO_RELEASE_SERIES
    if series.dataset_name == DATASET_NAME
)
SOURCE_COLUMN_TO_SERIES = {
    series.source_column: series
    for series in INTEREST_RATE_SERIES
}
SERIES_CODE_TO_SOURCE_COLUMN = {
    series.series_code: series.source_column
    for series in INTEREST_RATE_SERIES
}
SHIBOR_FIELDS.extend(SOURCE_COLUMN_TO_SERIES)

if len(INTEREST_RATE_SERIES) != 8:
    raise ValueError("共享宏观配置必须恰好提供 8 个 SHIBOR 系列。")
if any(series.source_api != "pro.shibor" for series in INTEREST_RATE_SERIES):
    raise ValueError("SHIBOR 系列来源必须统一为 Tushare pro.shibor。")

# 单位是百分比年利率；使用宽松硬边界拦截单位错位和明显损坏，不把短期经济判断写入代码。
RATE_MIN_PERCENT = -100.0
RATE_MAX_PERCENT = 100.0

API_SUCCESS_REASON = (
    "Tushare pro.shibor 月度窗口请求完整；正式事实复读 1 行，"
    "利率为有限数且位于 [-100, 100] 百分比范围。"
)
API_EMPTY_REASON = (
    "Tushare pro.shibor 月度窗口请求完整；对应精确系列—日期未返回有效值，"
    "正式事实复读 0 行。"
)
STATE_REPAIR_REASON = (
    "正式 SHIBOR 事实已经完整复读 1 行；未调用 Tushare，"
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
        INTEREST_RATE_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化 Dataset 读取
# 
# 当前日历必须精确匹配权威 Schema/metadata。SHIBOR 事实只在无日期自动模式下允许读取
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


# ## 上游日历与 SHIBOR 事实质量校验
# 
# 消费者只检查当前计算直接依赖的上游字段、主键和状态边界，不复制 c01 的完整可用日规则。
# 事实生产者则对当前事实的系列映射、有限值、宽松利率范围、分区与审计时间承担完整验证责任。

# In[ ]:


def validate_interest_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    checked = validate_arrow_table(table, MACRO_RELEASE_CALENDAR_SCHEMA)
    frame = arrow_to_pandas(checked, MACRO_RELEASE_CALENDAR_SCHEMA)

    if frame.duplicated(CALENDAR_PRIMARY_KEY).any():
        raise ValueError(f"{context}宏观发布日历主键不唯一。")
    if not frame.empty and not frame["dataset_name"].eq(DATASET_NAME).all():
        raise ValueError(f"{context}只允许包含 interest_rate 日历行。")

    expected_series_codes = set(SERIES_CODE_TO_SOURCE_COLUMN)
    now_utc = datetime.now(timezone.utc)

    for row in checked.to_pylist():
        if row["series_code"] not in expected_series_codes:
            raise ValueError(f"{context}SHIBOR 系列未命中共享配置。")
        if row["actual_record_count"] not in {0, 1}:
            raise ValueError(f"{context}日历正式事实计数只允许 0 或 1。")
        if (
            row["report_date"].year != row["year"]
            or row["report_date"].month != row["month"]
        ):
            raise ValueError(f"{context}日历 year/month 与观测日不一致。")
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


def validate_interest_rate_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    ordered_input_df = frame.loc[:, INTEREST_RATE_DAILY_SCHEMA.names]
    table = pandas_to_arrow(ordered_input_df, INTEREST_RATE_DAILY_SCHEMA)
    checked = validate_arrow_table(table, INTEREST_RATE_DAILY_SCHEMA)
    checked_df = arrow_to_pandas(checked, INTEREST_RATE_DAILY_SCHEMA)

    if checked_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}SHIBOR 事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    expected_series_codes = set(SERIES_CODE_TO_SOURCE_COLUMN)
    for row in checked.to_pylist():
        if row["series_code"] not in expected_series_codes:
            raise ValueError(f"{context}SHIBOR 系列未命中共享配置。")
        if row["source"] != SOURCE_NAME:
            raise ValueError(f"{context}SHIBOR 事实来源必须为 {SOURCE_NAME}。")
        if not np.isfinite(row["rate"]):
            raise ValueError(f"{context}SHIBOR 利率必须为有限数。")
        if not RATE_MIN_PERCENT <= row["rate"] <= RATE_MAX_PERCENT:
            raise ValueError(
                f"{context}SHIBOR 利率越出 [-100, 100] 百分比硬边界。"
            )
        if (
            row["observation_date"].year != row["year"]
            or row["observation_date"].month != row["month"]
        ):
            raise ValueError(f"{context}事实 year/month 与观测日不一致。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}事实 updated_at 不得晚于当前 UTC 时间。")

    return checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


def read_interest_calendar(table_path: pathlib.Path) -> pd.DataFrame:
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
    return validate_interest_calendar_table(table, "正式上游")


def read_optional_fact(
    table_path: pathlib.Path,
) -> tuple[pd.DataFrame, bool]:
    if (
        not table_path.is_dir()
        or next(table_path.rglob("*.parquet"), None) is None
    ):
        return empty_pandas(INTEREST_RATE_DAILY_SCHEMA), True

    dataset, metadata_is_exact = open_compatible_dataset(
        table_path,
        FACT_PARTITIONING,
        INTEREST_RATE_DAILY_SCHEMA,
        PARTITION_COLUMNS,
        "现有正式 SHIBOR 事实",
    )
    source_table = dataset.to_table(
        columns=INTEREST_RATE_DAILY_SCHEMA.names
    )

    # 物理兼容旧表只在内存中重新附着当前 metadata，随后必须整根升级。
    current_table = pa.Table.from_arrays(
        [source_table[name].combine_chunks() for name in INTEREST_RATE_DAILY_SCHEMA.names],
        schema=INTEREST_RATE_DAILY_SCHEMA,
    )
    current_df = validate_interest_rate_frame(
        arrow_to_pandas(current_table, INTEREST_RATE_DAILY_SCHEMA),
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
# 完整 `empty_confirmed` 证据的 required 格点进入 API 待办。事实不得越出当前 required 上游水位；
# 遇到这种异常直接停止，不静默删除历史事实。

# In[ ]:


def fact_grid_count_map(
    fact_df: pd.DataFrame,
) -> dict[tuple[str, date], int]:
    if fact_df.empty:
        return {}

    counts = fact_df.groupby(PRIMARY_KEY, observed=True).size()
    return {
        (str(series_code), observation_date): int(count)
        for (series_code, observation_date), count in counts.items()
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
    raise ValueError("单个 SHIBOR 格点正式事实计数只允许 0 或 1。")


def plan_interest_rate_grids(
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

    required_keys = set(
        required_df[["series_code", "report_date"]].itertuples(
            index=False,
            name=None,
        )
    )
    all_required_keys = set(
        calendar_df.loc[
            calendar_df["is_fetch_required"],
            ["series_code", "report_date"],
        ].itertuples(index=False, name=None)
    )
    fact_keys = set(
        fact_df[PRIMARY_KEY].itertuples(index=False, name=None)
    )
    outside_watermark = fact_keys - all_required_keys
    if outside_watermark:
        sample = sorted(outside_watermark)[:10]
        raise ValueError(
            "正式 SHIBOR 事实存在越出当前 required 上游水位的格点；"
            f"不得静默删除或忽略：{sample}"
        )

    fact_counts = fact_grid_count_map(fact_df)
    pending_rows = []
    repair_rows = []
    complete_count = 0

    for _, row in required_df.sort_values(
        ["year", "month", "report_date", "series_code"]
    ).iterrows():
        key = (row["series_code"], row["report_date"])
        if key not in required_keys:
            continue
        fact_count = fact_counts.get(key, 0)
        if fact_count not in {0, 1}:
            raise ValueError(f"SHIBOR 格点 {key} 的正式事实多于 1 行。")

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


# ## Tushare 月度请求与精确格点转换
# 
# 官方接口单次最多 2000 行；一个自然月最多 31 个日期，因此按事实叶分区请求不会触顶。
# 返回表必须包含 `date` 和共享配置中的 8 个原列，日期唯一且全部位于请求范围。
# 范围内非待办格点只参与来源结构校验，不触达或覆盖正式事实。

# In[ ]:


class ShiborRequestError(RuntimeError):
    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def create_tushare_client():
    import tushare as ts

    if not settings.tushare_token:
        raise ShiborRequestError(
            "permanent_error",
            "TUSHARE_TOKEN 未配置。",
        )
    return ts.pro_api(settings.tushare_token)


def query_shibor_window(
    client,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    try:
        raw_df = client.shibor(
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            fields=",".join(SHIBOR_FIELDS),
        )
    except Exception as error:
        error_text = str(error).strip() or error.__class__.__name__
        lowered = error_text.lower()
        retryable_hints = [
            "timeout",
            "timed out",
            "connection",
            "temporar",
            "rate limit",
            "too many",
            "频率",
            "稍后",
            "网络",
            "连接",
        ]
        status = (
            "retryable_error"
            if any(hint in lowered for hint in retryable_hints)
            else "permanent_error"
        )
        raise ShiborRequestError(
            status,
            f"Tushare pro.shibor 请求失败：{error_text}",
        ) from error

    if raw_df is None:
        raise ShiborRequestError(
            "retryable_error",
            "Tushare pro.shibor 返回 None。",
        )
    if not isinstance(raw_df, pd.DataFrame):
        raise ShiborRequestError(
            "permanent_error",
            "Tushare pro.shibor 返回对象不是 Pandas DataFrame。",
        )
    return raw_df


def normalize_shibor_response(
    raw_df: pd.DataFrame,
    pending_df: pd.DataFrame,
    request_start_date: date,
    request_end_date: date,
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[tuple[str, date], int]]:
    missing_columns = set(SHIBOR_FIELDS) - set(raw_df.columns)
    if missing_columns:
        raise ValueError(
            f"Tushare pro.shibor 缺少必需列：{sorted(missing_columns)}"
        )
    if len(raw_df) > 2000:
        raise ValueError("Tushare pro.shibor 返回超过官方单次 2000 行上限。")

    selected_df = raw_df.loc[:, SHIBOR_FIELDS].copy()
    if not selected_df.empty:
        try:
            parsed_dates = pd.to_datetime(
                selected_df["date"].astype("string"),
                format="%Y%m%d",
                errors="raise",
            ).dt.date
        except Exception as error:
            raise ValueError("Tushare pro.shibor date 存在非法日期。") from error

        selected_df["observation_date"] = parsed_dates
        if selected_df["observation_date"].duplicated().any():
            raise ValueError("Tushare pro.shibor 返回重复日期。")
        if (
            selected_df["observation_date"].lt(request_start_date).any()
            or selected_df["observation_date"].gt(request_end_date).any()
        ):
            raise ValueError("Tushare pro.shibor 返回日期越出请求范围。")

        # 非空值必须能严格转换为有限数；None/pd.NA/NaN 只表示该期限没有返回事实。
        for source_column in SOURCE_COLUMN_TO_SERIES:
            source_values = selected_df[source_column]
            non_missing_mask = source_values.notna()
            if non_missing_mask.any():
                if source_values.loc[non_missing_mask].map(
                    lambda value: isinstance(value, (bool, np.bool_))
                ).any():
                    raise ValueError(
                        f"Tushare pro.shibor {source_column} 存在布尔值。"
                    )
                try:
                    numeric_values = pd.to_numeric(
                        source_values.loc[non_missing_mask],
                        errors="raise",
                    ).astype("float64")
                except Exception as error:
                    raise ValueError(
                        f"Tushare pro.shibor {source_column} 存在非数值。"
                    ) from error
                if not np.isfinite(numeric_values.to_numpy()).all():
                    raise ValueError(
                        f"Tushare pro.shibor {source_column} 存在 NaN/Inf。"
                    )
                if (
                    numeric_values.lt(RATE_MIN_PERCENT).any()
                    or numeric_values.gt(RATE_MAX_PERCENT).any()
                ):
                    raise ValueError(
                        f"Tushare pro.shibor {source_column} 越出 [-100, 100]。"
                    )
                selected_df.loc[non_missing_mask, source_column] = numeric_values

    rows_by_date = {
        row["observation_date"]: row
        for row in selected_df.to_dict("records")
    }
    fact_rows = []
    outcome_counts: dict[tuple[str, date], int] = {}

    for pending_row in pending_df.to_dict("records"):
        series_code = pending_row["series_code"]
        observation_date = pending_row["report_date"]
        key = (series_code, observation_date)
        source_column = SERIES_CODE_TO_SOURCE_COLUMN.get(series_code)
        if source_column is None:
            raise ValueError(f"待办系列未命中共享来源映射：{series_code}")

        source_row = rows_by_date.get(observation_date)
        source_value = (
            None
            if source_row is None
            else source_row[source_column]
        )
        if source_value is None or pd.isna(source_value):
            outcome_counts[key] = 0
            continue

        rate = float(source_value)
        outcome_counts[key] = 1
        fact_rows.append({
            "series_code": series_code,
            "observation_date": observation_date,
            "rate": rate,
            "source": SOURCE_NAME,
            "updated_at": updated_at,
            "year": observation_date.year,
            "month": observation_date.month,
        })

    fact_df = pd.DataFrame(
        fact_rows,
        columns=INTEREST_RATE_DAILY_SCHEMA.names,
    )
    if fact_df.empty:
        fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)
    else:
        fact_df = validate_interest_rate_frame(fact_df, "Tushare 转换")

    expected_keys = set(
        pending_df[["series_code", "report_date"]].itertuples(
            index=False,
            name=None,
        )
    )
    if set(outcome_counts) != expected_keys:
        raise ValueError("Tushare 转换结果没有逐个覆盖精确待办格点。")
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

    for series_code, observation_date in touched_grids:
        if (observation_date.year, observation_date.month) != partition_key:
            raise ValueError("触达 SHIBOR 格点越出指定事实叶分区。")

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
        return empty_pandas(INTEREST_RATE_DAILY_SCHEMA)
    return validate_interest_rate_frame(merged_df, "合并后的完整事实分区")


def write_fact_staging(
    frame: pd.DataFrame,
    staging_path: pathlib.Path,
) -> None:
    staging_path.mkdir(parents=True, exist_ok=False)
    file_schema = pa.schema(
        [
            field
            for field in INTEREST_RATE_DAILY_SCHEMA
            if field.name not in PARTITION_COLUMNS
        ],
        metadata=INTEREST_RATE_DAILY_SCHEMA.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        staging_path / "schema.parquet",
    )

    table = pandas_to_arrow(
        frame.loc[:, INTEREST_RATE_DAILY_SCHEMA.names],
        INTEREST_RATE_DAILY_SCHEMA,
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
    complete_df = validate_interest_rate_frame(
        existing_fact_df,
        "待升级完整 SHIBOR 事实",
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
            INTEREST_RATE_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "SHIBOR metadata 升级 staging",
        )
        staged_df = validate_interest_rate_frame(
            arrow_to_pandas(
                staged_dataset.to_table(columns=INTEREST_RATE_DAILY_SCHEMA.names),
                INTEREST_RATE_DAILY_SCHEMA,
            ),
            "metadata 升级 staging",
        )
        if table_digest(
            staged_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("SHIBOR metadata 升级 staging 逐值复读失败。")
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
            INTEREST_RATE_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "升级后的正式 SHIBOR 事实",
        )
        committed_df = validate_interest_rate_frame(
            arrow_to_pandas(
                committed_dataset.to_table(columns=INTEREST_RATE_DAILY_SCHEMA.names),
                INTEREST_RATE_DAILY_SCHEMA,
            ),
            "升级后的正式",
        )
        if table_digest(
            committed_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("升级后的正式 SHIBOR 事实逐值复读失败。")
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
    complete_df = validate_interest_rate_frame(
        frame,
        "待提交完整 SHIBOR 事实分区",
    )
    if not complete_df.empty:
        actual_keys = set(
            complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交 SHIBOR 内容越出指定事实叶分区。")

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
            INTEREST_RATE_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "SHIBOR 事实 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=INTEREST_RATE_DAILY_SCHEMA.names,
            filter=partition_expression(PARTITION_COLUMNS, partition_key),
        )
        staged_df = validate_interest_rate_frame(
            arrow_to_pandas(staged_table, INTEREST_RATE_DAILY_SCHEMA),
            "staging 完整 SHIBOR 分区",
        )
        if table_digest(
            staged_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("SHIBOR staging 完整分区逐值复读失败。")
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
            INTEREST_RATE_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "正式 SHIBOR 事实",
        )
        committed_table = committed_dataset.to_table(
            columns=INTEREST_RATE_DAILY_SCHEMA.names,
            filter=partition_expression(PARTITION_COLUMNS, partition_key),
        )
        committed_df = validate_interest_rate_frame(
            arrow_to_pandas(committed_table, INTEREST_RATE_DAILY_SCHEMA),
            "正式路径复读的完整 SHIBOR 分区",
        )
        if table_digest(
            committed_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ) != table_digest(
            complete_df,
            INTEREST_RATE_DAILY_SCHEMA,
            PRIMARY_KEY,
        ):
            raise ValueError("正式 SHIBOR 完整分区逐值复读失败。")
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
# 事实正式复读计数为 1 才写 `success + passed`；完整 API 响应没有精确期限值且正式复读为 0，
# 才写 `empty_confirmed + warning`。连接类错误可重试，权限、结构、日期和数值错误永久失败；
# 失败格点保持未完成。日历按完整 `interest_rate/year/month` 叶分区替换并可回滚。

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
            raise ValueError(f"SHIBOR 格点 {key} 正式复读计数不是 0 或 1。")

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

    return validate_interest_calendar_table(
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
        raise ValueError("SHIBOR 失败状态只能是 retryable_error 或 permanent_error。")

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

    return validate_interest_calendar_table(
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
    complete_calendar_df = validate_interest_calendar_table(
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
        staged_df = validate_interest_calendar_table(
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
        committed_df = validate_interest_calendar_table(
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


# ## CLI：自动求差、状态修复、API 请求与最终对账
# 
# 默认使用 `.env` 的正式湖。显式日期参数必须成对出现，并在任何湖仓读取或 API 初始化之前
# 拦截正式湖写入。无 API 状态修复优先提交并复读；重新规划后仍有事实缺口，才创建 Tushare 客户端。

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

    calendar_df = read_interest_calendar(calendar_path)
    existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)

    if not fact_metadata_is_exact:
        if has_explicit_dates:
            raise click.UsageError(
                "现有 SHIBOR 事实 metadata 已过期；"
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

    pending_df, repair_df, complete_count = plan_interest_rate_grids(
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

    # 正式事实已经存在的格点只修复日历，不创建 Tushare 客户端。
    if not repair_df.empty:
        repair_counts = {
            (row.series_code, row.report_date): 1
            for row in repair_df.itertuples(index=False)
        }
        repair_reasons = {
            key: STATE_REPAIR_REASON
            for key in repair_counts
        }
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
            calendar_df = read_interest_calendar(calendar_path)
            pending_df, repair_df, complete_count = plan_interest_rate_grids(
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
        click.echo("complete: no_api_pending; Tushare client not created")
        return

    client = create_tushare_client()
    failed_window_count = 0

    for (year, month), partition_pending_df in pending_df.groupby(
        ["year", "month"],
        sort=True,
        observed=True,
    ):
        partition_key = (int(year), int(month))
        request_start_date = min(partition_pending_df["report_date"])
        request_end_date = max(partition_pending_df["report_date"])
        touched_grids = set(
            partition_pending_df[["series_code", "report_date"]].itertuples(
                index=False,
                name=None,
            )
        )
        fetch_run_id = uuid.uuid4().hex
        request_time = datetime.now(timezone.utc)

        try:
            raw_df = query_shibor_window(
                client,
                request_start_date,
                request_end_date,
            )
            incoming_fact_df, expected_counts = normalize_shibor_response(
                raw_df,
                partition_pending_df,
                request_start_date,
                request_end_date,
                request_time,
            )
        except ShiborRequestError as error:
            failure_status = error.status
            failure_reason = error.reason
        except Exception as error:
            failure_status = "permanent_error"
            failure_reason = f"Tushare pro.shibor 响应质检失败：{error}"
        else:
            failure_status = None
            failure_reason = None

        if failure_status is not None:
            failed_window_count += 1
            click.echo(
                f"api_failure: {year}-{int(month):02d}; "
                f"status={failure_status}; grids={len(touched_grids)}; "
                f"reason={failure_reason}"
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
            f"requested_grids={len(touched_grids)}; "
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
                "正式 SHIBOR 事实复读计数与完整 API 转换结果不一致。"
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
        existing_fact_df = validate_interest_rate_frame(
            existing_fact_df,
            "本次运行累计正式事实",
        )

    if failed_window_count:
        raise click.ClickException(
            f"{failed_window_count} 个 SHIBOR 月度窗口失败；"
            "成功分区已提交，失败格点保持未完成。"
        )

    if not write:
        click.echo("dry_run_complete: API 响应已转换和质检；未写事实或日历")
        return

    # 最终必须从两个正式路径重读并再次求差，防止日历领先于事实。
    final_calendar_df = read_interest_calendar(calendar_path)
    final_fact_df, final_metadata_is_exact = read_optional_fact(fact_path)
    if not final_metadata_is_exact:
        raise TypeError("最终正式 SHIBOR 事实 metadata 仍未升级完成。")
    final_pending_df, final_repair_df, final_complete_count = (
        plan_interest_rate_grids(
            final_calendar_df,
            final_fact_df,
            requested_start_date,
            requested_end_date,
        )
    )
    if not final_pending_df.empty or not final_repair_df.empty:
        raise RuntimeError(
            "最终正式路径对账仍存在 SHIBOR API 待办或日历状态修复格点。"
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

