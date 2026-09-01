#!/usr/bin/env python
# coding: utf-8

# # c04 外部指数日表
# 
# 目标表：`fact_external_index_daily`。
# 
# 本入口只消费 `dim_external_market_calendar` 中 `dataset_name=external_index`、
# `entity_code=Eastmoney INDICATOR_ID` 且 `is_fetch_required=true` 的指标—日期格点。
# 指标 ID、项目代码、中文名、分类与有效期只来自项目级共享配置；事实表保存每个已返回指标日期的一行值。

# ## 自动更新、确认空与写入边界
# 
# 自动模式遵循：
# 
# `上游当前 required 指标—日期格点 − 事实与日历状态共同证明完整的格点 = 本次自动更新范围`
# 
# 同一指标的待办日期按上游 required 顺序切成连续段，各段分别完成分页。API 范围内非待办日期
# 只用于证明分页完整，不参与覆盖；最终只接纳精确待办日期。全部分页成功但某个待办日期没有记录时，
# 该格点记为 `empty_confirmed`，不制造占位事实行。已经退出上游 required 集合的旧事实不访问 API，
# 直接从其原完整指数分类—年月叶分区清退，保证下游事实范围不会超过上游。
# 
# `--write` 只表示是否提交；显式日期只允许只读检查，或写入与 `.env` 正式湖不同的临时/测试湖。
# 事实表经 staging 和正式路径复读成功后，才把对应外部市场日历格点标为完成。

# ## 初始化与权威 Schema

# In[ ]:


from __future__ import annotations

# 标准库负责数值检查、路径切换、分区回滚和批次审计。
import math
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, timezone

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

# 第三方库直接承担 HTTP、CLI、DataFrame 与 Arrow 数据集操作。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 项目配置只提供权威数据契约、请求实体映射和正式湖路径。
from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    EXTERNAL_INDEX_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.external_market_entities import (
    EXTERNAL_INDEX_ENTITIES,
    ExternalIndexEntity,
)
from config.settings import settings


# ## Schema 契约呈现
# 
# 本节只在交互式 Notebook 内核中呈现只读 Schema 契约。依赖顺序为外部市场日历、外部指数事实；
# 展示不会读取数据湖、请求 Eastmoney 或产生写入。

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
        EXTERNAL_INDEX_DAILY_SCHEMA,
    ])


# ## 表名、主键、Hive 分区与 Eastmoney 来源字段

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

TABLE_NAME = EXTERNAL_INDEX_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 航运与能源金属外部指数日表。
PRIMARY_KEY = EXTERNAL_INDEX_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 项目稳定指数代码—指数观测日期业务主键。
PARTITION_COLUMNS = EXTERNAL_INDEX_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 指数分类—观测年—月 Hive 叶分区顺序。

DATASET_NAME = "external_index"  # 外部市场日历中的外部指数数据集代码。
GRID_COLUMNS = [
    "source_indicator_id",  # Eastmoney 请求实体 ID。
    "observation_date",  # 指数报告/观测日期。
]

# 配置映射是日历和事实生产者共同使用的唯一来源。
INDEX_ENTITY_BY_SOURCE_ID = {
    entity.source_indicator_id: entity
    for entity in EXTERNAL_INDEX_ENTITIES
}
CONFIGURED_INDEX_IDS = set(INDEX_ENTITY_BY_SOURCE_ID)

SOURCE = "Eastmoney_RPT_INDUSTRY_INDEX"
EASTMONEY_ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_REPORT_NAME = "RPT_INDUSTRY_INDEX"
EASTMONEY_PAGE_SIZE = 500
EASTMONEY_MAX_PAGES = 10_000

# 只请求本表契约真正使用的三个来源字段。
EASTMONEY_FIELDS = [
    "INDICATOR_ID",  # Eastmoney 原始指标 ID。
    "INDICATOR_VALUE",  # 指数观测值。
    "REPORT_DATE",  # 指数报告/观测日期。
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
        EXTERNAL_INDEX_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与完整业务质检
# 
# 外部市场日历已经由 c01 生成，本入口只复核当前调度和状态回写直接依赖的边界。外部指数事实由本入口
# 完整检查 Schema/metadata、主键、共享指标映射、事实来源、观测日期、年月分区、有限指数值和审计时间。

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
    # 正式输入、staging 与提交后输出都执行同一套精确物理契约检查。
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
        if (
            row["dataset_name"] == DATASET_NAME
            and row["entity_code"] not in CONFIGURED_INDEX_IDS
        ):
            raise ValueError(f"{context}外部指数请求实体未命中共享配置。")
        if (
            row["dataset_name"] == DATASET_NAME
            and row["actual_record_count"] not in {0, 1}
        ):
            raise ValueError(f"{context}外部指数单格点事实计数只能为 0 或 1。")

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


# 事实生产者承担本表全部字段、配置映射与格点关系质检。
def validate_external_index_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    checked = validate_arrow_table(
        pandas_to_arrow(
            frame.loc[:, EXTERNAL_INDEX_DAILY_SCHEMA.names],
            EXTERNAL_INDEX_DAILY_SCHEMA,
        ),
        EXTERNAL_INDEX_DAILY_SCHEMA,
    )
    normalized = arrow_to_pandas(checked, EXTERNAL_INDEX_DAILY_SCHEMA)
    if normalized.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}外部指数事实主键不唯一。")

    now_utc = datetime.now(timezone.utc)
    for row in checked.to_pylist():
        source_indicator_id = str(row["source_indicator_id"]).strip()
        if not source_indicator_id:
            raise ValueError(f"{context}Eastmoney 原始指标 ID 不得为空。")
        entity = INDEX_ENTITY_BY_SOURCE_ID.get(source_indicator_id)
        if entity is None:
            raise ValueError(f"{context}原始指标 ID 未命中共享配置。")
        if row["index_code"] != entity.index_code:
            raise ValueError(f"{context}项目指数代码与共享配置不一致。")
        if row["index_name"] != entity.index_name_zh:
            raise ValueError(f"{context}指数名称与共享配置不一致。")
        if row["index_category"] != entity.index_category:
            raise ValueError(f"{context}指数分类与共享配置不一致。")
        if row["source"] != SOURCE:
            raise ValueError(f"{context}事实来源不一致。")
        if (
            row["observation_date"].year != row["year"]
            or row["observation_date"].month != row["month"]
        ):
            raise ValueError(f"{context}年月分区与指数观测日期不一致。")
        if row["observation_date"] > date.today():
            raise ValueError(f"{context}指数观测日期不得晚于当前可见日期。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")
        if not math.isfinite(row["index_value"]):
            raise ValueError(f"{context}指数观测值必须为有限数。")

    return normalized.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 空事实目录是合法的全量起点；已有事实则必须精确符合新契约。
def read_optional_fact(table_path: pathlib.Path) -> pd.DataFrame:
    if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
        return empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)

    dataset = open_exact_dataset(
        table_path,
        FACT_PARTITIONING,
        EXTERNAL_INDEX_DAILY_SCHEMA,
        "正式外部指数事实",
    )
    table = dataset.to_table(columns=EXTERNAL_INDEX_DAILY_SCHEMA.names)
    return validate_external_index_frame(
        arrow_to_pandas(table, EXTERNAL_INDEX_DAILY_SCHEMA),
        "正式路径读取的",
    )


# ## Eastmoney 分页查询边界与严格归一化
# 
# 每个指标的一个连续待办段使用同一日期范围分页查询。首页冻结 `pages` 和 `count`，后续页必须保持一致；
# 每页长度、累计行数、来源 ID、日期范围和跨页业务键都必须能证明完整，任何部分页都不会进入事实表。

# In[ ]:


def pending_request_ranges(
    calendar_df: pd.DataFrame,
    source_indicator_id: str,
    pending_dates: set[date],
) -> list[tuple[date, date, set[date]]]:
    # “连续”以该指标上游 required 格点的顺序为准，因此周末不会人为拆段。
    if not pending_dates:
        return []

    first_pending_date = min(pending_dates)
    last_pending_date = max(pending_dates)
    required_mask = (
        calendar_df["dataset_name"].eq(DATASET_NAME)
        & calendar_df["entity_code"].eq(source_indicator_id)
        & calendar_df["is_fetch_required"].eq(True)
        & calendar_df["observation_date"].ge(first_pending_date)
        & calendar_df["observation_date"].le(last_pending_date)
    )
    required_dates = sorted(
        calendar_df.loc[required_mask, "observation_date"].tolist()
    )

    ranges = []
    current_dates = []
    for required_date in required_dates:
        if required_date in pending_dates:
            current_dates.append(required_date)
        elif current_dates:
            ranges.append((
                current_dates[0],
                current_dates[-1],
                set(current_dates),
            ))
            current_dates = []
    if current_dates:
        ranges.append((
            current_dates[0],
            current_dates[-1],
            set(current_dates),
        ))

    covered_dates = set().union(*(item[2] for item in ranges)) if ranges else set()
    if covered_dates != pending_dates:
        raise ValueError("待办日期未被上游 required 连续段完整覆盖。")
    return ranges


def create_eastmoney_session() -> requests.Session:
    # GET 重试只覆盖连接失败、限流和服务端瞬时错误；业务结构错误由下方显式分类。
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


def query_eastmoney_indicator_range(
    session: requests.Session,
    source_indicator_id: str,
    range_start: date,
    range_end: date,
) -> list[dict[str, object]]:
    page_number = 1
    expected_pages = None
    expected_count = None
    response_rows = []

    while True:
        date_filter = (
            f'(INDICATOR_ID="{source_indicator_id}")'
            f"(REPORT_DATE>='{range_start.isoformat()}')"
            f"(REPORT_DATE<='{range_end.isoformat()}')"
        )
        params = {
            "reportName": EASTMONEY_REPORT_NAME,
            "columns": ",".join(EASTMONEY_FIELDS),
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
            error_type = (
                "retryable_error"
                if status_code in {408, 429}
                or (status_code is not None and status_code >= 500)
                else "permanent_error"
            )
            raise RuntimeError(
                f"{error_type}: Eastmoney HTTP 请求失败；"
                f"indicator={source_indicator_id}; page={page_number}; "
                f"status={status_code}。"
            ) from error
        except requests.RequestException as error:
            raise RuntimeError(
                "retryable_error: Eastmoney 请求失败；"
                f"indicator={source_indicator_id}; page={page_number}。"
            ) from error

        try:
            payload = response.json()
        except ValueError as error:
            raise RuntimeError(
                "retryable_error: Eastmoney 未返回有效 JSON；"
                f"indicator={source_indicator_id}; page={page_number}。"
            ) from error
        if not isinstance(payload, dict):
            raise RuntimeError("permanent_error: Eastmoney JSON 顶层必须为对象。")

        if payload.get("success") is not True:
            code = payload.get("code")
            message = str(payload.get("message") or "").strip()
            if str(code) == "9201" and "返回数据为空" in message:
                if page_number != 1:
                    raise RuntimeError(
                        "retryable_error: Eastmoney 后续分页意外返回空响应。"
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
            error_type = (
                "retryable_error"
                if any(
                    marker.casefold() in message.casefold()
                    for marker in retryable_markers
                )
                else "permanent_error"
            )
            raise RuntimeError(
                f"{error_type}: Eastmoney 返回失败；"
                f"indicator={source_indicator_id}; page={page_number}; "
                f"code={code}; message={message}。"
            )

        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("permanent_error: Eastmoney result 必须为对象。")
        data_rows = result.get("data")
        if not isinstance(data_rows, list):
            raise RuntimeError("permanent_error: Eastmoney result.data 必须为列表。")

        raw_pages = result.get("pages")
        raw_count = result.get("count")
        integer_metadata = []
        for metadata_name, raw_value in [
            ("pages", raw_pages),
            ("count", raw_count),
        ]:
            if isinstance(raw_value, bool):
                raise RuntimeError(
                    f"permanent_error: Eastmoney {metadata_name} 类型非法。"
                )
            if isinstance(raw_value, int):
                integer_metadata.append(raw_value)
                continue
            if isinstance(raw_value, str) and raw_value.strip().isdigit():
                integer_metadata.append(int(raw_value.strip()))
                continue
            raise RuntimeError(
                f"permanent_error: Eastmoney {metadata_name} 必须为整数。"
            )
        page_count, record_count = integer_metadata
        if page_count < 0 or record_count < 0:
            raise RuntimeError("permanent_error: Eastmoney pages/count 不得为负。")

        if record_count == 0:
            if page_number != 1 or page_count not in {0, 1} or data_rows:
                raise RuntimeError("permanent_error: Eastmoney 空响应分页元数据不一致。")
            return []

        calculated_pages = (
            record_count + EASTMONEY_PAGE_SIZE - 1
        ) // EASTMONEY_PAGE_SIZE
        if page_count != calculated_pages:
            raise RuntimeError("retryable_error: Eastmoney pages 与 count 不一致。")
        if page_count > EASTMONEY_MAX_PAGES:
            raise RuntimeError("permanent_error: Eastmoney 总页数超过安全上限。")

        if expected_pages is None:
            expected_pages = page_count
            expected_count = record_count
        elif page_count != expected_pages or record_count != expected_count:
            raise RuntimeError("retryable_error: Eastmoney 分页元数据在请求期间漂移。")

        expected_page_rows = (
            EASTMONEY_PAGE_SIZE
            if page_number < expected_pages
            else expected_count - EASTMONEY_PAGE_SIZE * (expected_pages - 1)
        )
        if len(data_rows) != expected_page_rows:
            raise RuntimeError("retryable_error: Eastmoney 当前页行数与分页元数据不一致。")
        response_rows.extend(data_rows)

        if page_number == expected_pages:
            break
        page_number += 1

    if len(response_rows) != expected_count:
        raise RuntimeError("retryable_error: Eastmoney 累计行数与 count 不一致。")
    return response_rows


def normalize_external_index_response(
    response_rows: list[dict[str, object]],
    entity: ExternalIndexEntity,
    range_start: date,
    range_end: date,
    pending_dates: set[date],
    updated_at: datetime,
) -> pd.DataFrame:
    rows = []
    seen_grids = set()

    for item in response_rows:
        if not isinstance(item, dict):
            raise ValueError("permanent_error: Eastmoney data 元素必须为对象。")
        missing_fields = set(EASTMONEY_FIELDS) - set(item)
        if missing_fields:
            raise ValueError(
                "permanent_error: Eastmoney data 缺少字段 "
                f"{sorted(missing_fields)}。"
            )

        raw_indicator_id = item["INDICATOR_ID"]
        if raw_indicator_id is None:
            raise ValueError("permanent_error: Eastmoney INDICATOR_ID 不得为空。")
        source_indicator_id = str(raw_indicator_id).strip()
        if source_indicator_id != entity.source_indicator_id:
            raise ValueError("permanent_error: Eastmoney 返回了非请求指标 ID。")

        try:
            observation_timestamp = pd.Timestamp(item["REPORT_DATE"])
        except (TypeError, ValueError) as error:
            raise ValueError("permanent_error: Eastmoney REPORT_DATE 非法。") from error
        if pd.isna(observation_timestamp):
            raise ValueError("permanent_error: Eastmoney REPORT_DATE 不得为空。")
        observation_date = observation_timestamp.date()
        if observation_date < range_start or observation_date > range_end:
            raise ValueError("permanent_error: Eastmoney 日期越出请求范围。")
        if observation_date > date.today():
            raise ValueError("permanent_error: Eastmoney 日期晚于当前可见日期。")

        grid_key = (source_indicator_id, observation_date)
        if grid_key in seen_grids:
            raise ValueError("permanent_error: Eastmoney 跨页指标—日期键重复。")
        seen_grids.add(grid_key)

        raw_value = item["INDICATOR_VALUE"]
        if raw_value is None or isinstance(raw_value, bool):
            raise ValueError("permanent_error: Eastmoney 指数值为空或类型非法。")
        try:
            index_value = float(raw_value)
        except (TypeError, ValueError) as error:
            raise ValueError("permanent_error: Eastmoney 指数值不是数值。") from error
        if not math.isfinite(index_value):
            raise ValueError("permanent_error: Eastmoney 指数值必须为有限数。")

        # 范围内非待办日期只用于分页完整性证明，不覆盖已完整正式事实。
        if observation_date not in pending_dates:
            continue
        rows.append({
            "observation_date": observation_date,
            "index_code": entity.index_code,
            "index_name": entity.index_name_zh,
            "index_category": entity.index_category,
            "index_value": index_value,
            "source_indicator_id": source_indicator_id,
            "source": SOURCE,
            "updated_at": updated_at,
            "year": observation_date.year,
            "month": observation_date.month,
        })

    frame = (
        pd.DataFrame(rows, columns=EXTERNAL_INDEX_DAILY_SCHEMA.names)
        if rows
        else empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
    )
    return validate_external_index_frame(
        frame,
        "Eastmoney 完整分页响应转换后的",
    )


# ## 完整格点与自动待办
# 
# 每个指标—日期格点最多对应一行事实。只有外部市场日历完成状态、正式事实按来源指标 ID—日期
# 复读计数和审计字段共同一致，格点才从待办中扣除；最大日期不能证明中间没有空洞。已经不在
# 上游 required 集合中的旧事实属于下游越界格点，不调用 API，直接进入完整叶分区清退计划。

# In[ ]:


def grid_count_map(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], int]:
    if frame.empty:
        return {}
    counts = frame.groupby(GRID_COLUMNS, dropna=False).size()
    count_map = {
        tuple(key): int(value)
        for key, value in counts.items()
    }
    if any(value != 1 for value in count_map.values()):
        raise ValueError("外部指数正式事实的单个指标—日期格点必须恰好一行。")
    return count_map


# 单个日期只有日历审计和正式事实计数一致时才算完整。
def calendar_grid_is_complete(
    row: dict[str, object],
    actual_fact_count: int,
) -> bool:
    if actual_fact_count not in {0, 1}:
        raise ValueError("外部指数单个日历格点的正式事实计数只能为 0 或 1。")
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

    if actual_fact_count > 0:
        return (
            row["fetch_result_status"] == "success"
            and not row["is_data_missing"]
            and row["quality_status"] in {"passed", "warning"}
        )
    return (
        row["fetch_result_status"] == "empty_confirmed"
        and row["is_data_missing"]
        and row["quality_status"] == "warning"
    )


# 显式日期仅缩小检查和测试湖写入集合；默认同时对账全部 required 格点与越界事实。
def external_index_reconciliation(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    relevant_mask = (
        calendar_df["dataset_name"].eq(DATASET_NAME)
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
    pending_rows = []
    complete_count = 0
    required_grid_keys = set()

    for row in calendar_rows:
        source_indicator_id = row["entity_code"]
        entity = INDEX_ENTITY_BY_SOURCE_ID.get(source_indicator_id)
        if entity is None:
            raise ValueError("required 外部指数格点未命中共享请求实体配置。")
        observation_date = row["observation_date"]
        grid_key = (source_indicator_id, observation_date)
        required_grid_keys.add(grid_key)
        if calendar_grid_is_complete(row, fact_counts.get(grid_key, 0)):
            complete_count += 1
            continue
        pending_rows.append({
            "source_indicator_id": source_indicator_id,
            "index_code": entity.index_code,
            "index_name": entity.index_name_zh,
            "index_category": entity.index_category,
            "observation_date": observation_date,
            "year": observation_date.year,
            "month": observation_date.month,
        })

    pending_df = pd.DataFrame(
        pending_rows,
        columns=[
            "source_indicator_id",
            "index_code",
            "index_name",
            "index_category",
            "observation_date",
            "year",
            "month",
        ],
    )
    plan_columns = list(pending_df.columns)
    if not pending_df.empty:
        pending_df = pending_df.sort_values(
            [
                "index_category",
                "year",
                "month",
                "source_indicator_id",
                "observation_date",
            ]
        ).reset_index(drop=True)

    # 正式事实若已不属于当前 required 水位，必须从其原分类—年月叶分区删除。
    fact_scope_mask = pd.Series(True, index=fact_df.index)
    if start_date is not None:
        fact_scope_mask &= fact_df["observation_date"].ge(start_date)
        fact_scope_mask &= fact_df["observation_date"].le(end_date)
    scoped_fact_df = fact_df.loc[fact_scope_mask]
    scoped_fact_keys = scoped_fact_df[GRID_COLUMNS].apply(tuple, axis=1)
    obsolete_df = scoped_fact_df.loc[
        ~scoped_fact_keys.isin(required_grid_keys),
        plan_columns,
    ].copy()
    if not obsolete_df.empty:
        obsolete_df = obsolete_df.sort_values(
            [
                "index_category",
                "year",
                "month",
                "source_indicator_id",
                "observation_date",
            ]
        ).reset_index(drop=True)

    return pending_df, obsolete_df, complete_count


# 触达指标—日期整体替换，未触达格点在完整分类—年月分区中原样保留。
def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    partition_mask = pd.Series(True, index=existing_df.index)
    for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
        partition_mask &= existing_df[column].eq(value)

    existing_partition_df = existing_df.loc[
        partition_mask,
        EXTERNAL_INDEX_DAILY_SCHEMA.names,
    ]
    existing_grid_keys = existing_partition_df[GRID_COLUMNS].apply(
        tuple,
        axis=1,
    )
    retained_df = existing_partition_df.loc[
        ~existing_grid_keys.isin(touched_grids),
        EXTERNAL_INDEX_DAILY_SCHEMA.names,
    ]
    complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
    if complete_df.empty:
        complete_df = empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
    return validate_external_index_frame(
        complete_df,
        "合并后的完整外部指数分区",
    )


# ## 完整事实叶分区 staging、正式复读与回滚
# 
# 每个指数分类—年—月事实分区都按完整叶分区提交。触达指标—日期被本次 Eastmoney 结果替换；
# 确认空格点会删除可能残留的旧事实，未触达格点原样保留。staging 和正式路径必须逐值一致。

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


# 每次只提交一个完整指数分类—年—月事实叶分区，并保留可回滚旧分区。
def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    complete_df = validate_external_index_frame(frame, "待提交完整外部指数分区")
    if not complete_df.empty:
        actual_keys = set(
            complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
        )
        if actual_keys != {partition_key}:
            raise ValueError("待提交外部指数内容越出指定 Hive 叶分区。")
    complete_table = pandas_to_arrow(
        complete_df.loc[:, EXTERNAL_INDEX_DAILY_SCHEMA.names],
        EXTERNAL_INDEX_DAILY_SCHEMA,
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
                for field in EXTERNAL_INDEX_DAILY_SCHEMA
                if field.name not in PARTITION_COLUMNS
            ],
            metadata=EXTERNAL_INDEX_DAILY_SCHEMA.metadata,
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
            EXTERNAL_INDEX_DAILY_SCHEMA,
            "外部指数 staging",
        )
        staged_table = staged_dataset.to_table(
            columns=EXTERNAL_INDEX_DAILY_SCHEMA.names,
            filter=fact_partition_expression(partition_key),
        )
        staged_df = validate_external_index_frame(
            arrow_to_pandas(staged_table, EXTERNAL_INDEX_DAILY_SCHEMA),
            "staging 完整外部指数分区",
        )
        if not pandas_to_arrow(
            staged_df,
            EXTERNAL_INDEX_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("外部指数 staging 完整分区内容检查失败。")
    except Exception:
        # staging 尚未进入正式替换；任一写入或复读异常都应立即清理。
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
            EXTERNAL_INDEX_DAILY_SCHEMA,
            "正式外部指数事实",
        )
        committed_table = committed_dataset.to_table(
            columns=EXTERNAL_INDEX_DAILY_SCHEMA.names,
            filter=fact_partition_expression(partition_key),
        )
        committed_df = validate_external_index_frame(
            arrow_to_pandas(
                committed_table,
                EXTERNAL_INDEX_DAILY_SCHEMA,
            ),
            "正式路径复读的完整外部指数分区",
        )
        if not pandas_to_arrow(
            committed_df,
            EXTERNAL_INDEX_DAILY_SCHEMA,
        ).equals(complete_table):
            raise ValueError("正式外部指数完整分区内容检查失败。")
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
# 来源 ID、分页结构或指数值错误属于永久错误。失败格点保持未完成，后续自动运行继续补缺。

# In[ ]:


# 只有本批已正式复读的指标—日期才写入完成或确认空状态。
def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_results: dict[tuple[str, date], int],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        if row["dataset_name"] != DATASET_NAME:
            continue
        grid_key = (row["entity_code"], row["observation_date"])
        actual_count = grid_results.get(grid_key)
        if actual_count is None:
            continue
        if actual_count not in {0, 1}:
            raise ValueError("外部指数完成格点的正式事实计数只能为 0 或 1。")

        has_rows = actual_count == 1
        updated_df.at[index, "is_fetch_completed"] = True
        updated_df.at[index, "fetch_result_status"] = (
            "success" if has_rows else "empty_confirmed"
        )
        updated_df.at[index, "is_data_missing"] = not has_rows
        updated_df.at[index, "actual_record_count"] = actual_count
        updated_df.at[index, "quality_status"] = (
            "passed" if has_rows else "warning"
        )
        updated_df.at[index, "quality_reason"] = (
            "Eastmoney RPT_INDUSTRY_INDEX 完整分页响应已转换，"
            f"并从正式外部指数事实复读 {actual_count} 行。"
            if has_rows
            else "Eastmoney RPT_INDUSTRY_INDEX 完整分页查询成功，"
            "该待办日期未返回记录且正式事实复读为 0 行。"
        )
        updated_df.at[index, "fetch_run_id"] = fetch_run_id
        updated_df.at[index, "fetch_completed_at"] = completed_at
        updated_df.at[index, "quality_checked_at"] = completed_at
        updated_df.at[index, "updated_at"] = completed_at

    return validate_calendar_frame(updated_df, "外部指数完成状态回写后的")


# 失败状态保留当前正式事实计数，但绝不把指标—日期标为完成。
def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    failed_grids: set[tuple[str, date]],
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_counts: dict[tuple[str, date], int],
) -> pd.DataFrame:
    if fetch_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("失败状态不在允许枚举中。")
    updated_df = calendar_df.copy()

    for index, row in updated_df.iterrows():
        grid_key = (row["entity_code"], row["observation_date"])
        if row["dataset_name"] != DATASET_NAME or grid_key not in failed_grids:
            continue
        current_fact_count = current_fact_counts.get(grid_key, 0)
        if current_fact_count not in {0, 1}:
            raise ValueError("外部指数失败格点的正式事实计数只能为 0 或 1。")
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

    return validate_calendar_frame(updated_df, "外部指数失败状态回写后的")


# 日历按 dataset—年—月完整提交，并保留同月其他请求实体。
def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    lake_root: pathlib.Path,
) -> int:
    if not touched_grids:
        return 0

    calendar_grid_keys = calendar_df[[
        "entity_code",
        "observation_date",
    ]].apply(tuple, axis=1)
    touched_mask = (
        calendar_df["dataset_name"].eq(DATASET_NAME)
        & calendar_grid_keys.isin(touched_grids)
    )
    touched_df = calendar_df.loc[touched_mask]
    if len(touched_df) != len(touched_grids):
        raise ValueError("待提交外部指数日历格点未与日历主键一一对应。")
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


# ## CLI：自动差集、Eastmoney 查询、分区进度与是否写入
# 
# 默认从正式外部市场日历寻找全部缺口。待办按事实分类—年—月叶分区顺序处理；带 `--write` 时每完成
# 一个完整叶分区就提交事实并回写日历。只有确有 API 待办时才创建 Eastmoney HTTP 会话。

# In[ ]:


# CLI 先执行正式湖显式日期门禁，再读取日历或创建 Eastmoney 会话。
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
    fact_df = read_optional_fact(fact_path)

    # 正式事实和日历状态共同参与差集；已经越出 required 水位的事实另行清退。
    pending_df, obsolete_df, complete_count = external_index_reconciliation(
        calendar_df,
        fact_df,
        requested_start,
        requested_end,
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"table={TABLE_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"pending_grid_count={len(pending_df)}; "
        f"obsolete_fact_grid_count={len(obsolete_df)}"
    )
    if pending_df.empty and obsolete_df.empty:
        click.echo("外部指数事实与外部市场日历状态已经完整一致。")
        return

    batch_id = uuid.uuid4().hex
    work_df = pd.concat([
        pending_df.assign(work_kind="fetch"),
        obsolete_df.assign(work_kind="remove"),
    ], ignore_index=True)
    partition_groups = list(
        work_df.groupby(PARTITION_COLUMNS, sort=True)
    )
    click.echo(f"reconciliation_partitions={len(partition_groups)}")

    total_rows = 0
    processed_grid_count = 0
    obsolete_grid_count = 0

    # 只有 fetch 待办才创建外部连接；纯越界事实清退不访问 Eastmoney。
    session = create_eastmoney_session() if not pending_df.empty else None
    try:
        # 按事实分类—年—月顺序推进，已完成分区可在后续重启时直接扣除。
        for group_number, (raw_partition_key, group_df) in enumerate(
            partition_groups,
            start=1,
        ):
            partition_key = (
                tuple(raw_partition_key)
                if isinstance(raw_partition_key, tuple)
                else (raw_partition_key,)
            )
            updated_at = datetime.now(timezone.utc)
            frames = []
            pending_group_df = group_df.loc[group_df["work_kind"].eq("fetch")]
            obsolete_group_df = group_df.loc[group_df["work_kind"].eq("remove")]

            click.echo(
                f"partition_start: {group_number}/{len(partition_groups)}; "
                f"key={partition_key}; fetch_grids={len(pending_group_df)}; "
                f"obsolete_grids={len(obsolete_group_df)}"
            )

            # 每个 INDICATOR_ID 独立分页；同指标的稀疏待办按 required 连续段拆分。
            for source_indicator_id, indicator_df in pending_group_df.groupby(
                "source_indicator_id",
                sort=True,
            ):
                entity = INDEX_ENTITY_BY_SOURCE_ID[source_indicator_id]
                pending_dates = set(indicator_df["observation_date"].tolist())
                request_ranges = pending_request_ranges(
                    calendar_df,
                    source_indicator_id,
                    pending_dates,
                )

                for range_start, range_end, range_pending_dates in request_ranges:
                    failed_grids = {
                        (source_indicator_id, observation_date)
                        for observation_date in range_pending_dates
                    }
                    try:
                        if session is None:
                            raise RuntimeError("缺少 Eastmoney HTTP 会话。")
                        response_rows = query_eastmoney_indicator_range(
                            session,
                            source_indicator_id,
                            range_start,
                            range_end,
                        )
                        range_df = normalize_external_index_response(
                            response_rows,
                            entity,
                            range_start,
                            range_end,
                            range_pending_dates,
                            updated_at,
                        )
                    except Exception as error:
                        message = str(error)
                        if not message.startswith((
                            "retryable_error:",
                            "permanent_error:",
                        )):
                            message = f"permanent_error: {message}"

                        # 失败只回写本请求段精确待办格点；当前事实分区绝不部分提交。
                        if write:
                            fetch_status = (
                                "retryable_error"
                                if message.startswith("retryable_error:")
                                else "permanent_error"
                            )
                            calendar_df = apply_calendar_failure(
                                calendar_df,
                                failed_grids,
                                fetch_status,
                                f"外部指数采集失败：{message}",
                                batch_id,
                                datetime.now(timezone.utc),
                                grid_count_map(fact_df),
                            )
                            commit_calendar_partitions(
                                calendar_df,
                                failed_grids,
                                resolved_lake_root,
                            )
                        raise click.ClickException(message) from error

                    click.echo(
                        "api_success: "
                        f"indicator={source_indicator_id}; "
                        f"range={range_start}/{range_end}; "
                        f"pending_grids={len(range_pending_dates)}; "
                        f"rows={len(range_df)}"
                    )
                    frames.append(range_df)

            incoming_df = (
                validate_external_index_frame(
                    pd.concat(frames, ignore_index=True),
                    "本分区 Eastmoney 响应汇总后的",
                )
                if any(not frame.empty for frame in frames)
                else empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
            )
            pending_grids = set(
                pending_group_df[["source_indicator_id", "observation_date"]]
                .itertuples(index=False, name=None)
            )
            obsolete_grids = set(
                obsolete_group_df[["source_indicator_id", "observation_date"]]
                .itertuples(index=False, name=None)
            )
            touched_grids = pending_grids | obsolete_grids
            api_counts = grid_count_map(incoming_df)
            api_grid_results = {
                grid_key: api_counts.get(grid_key, 0)
                for grid_key in pending_grids
            }

            if not write:
                total_rows += len(incoming_df)
                processed_grid_count += len(group_df)
                obsolete_grid_count += len(obsolete_grids)
                continue

            complete_df = full_fact_partition(
                fact_df,
                incoming_df,
                touched_grids,
                partition_key,
            )
            committed_partition_df = commit_complete_fact_partition(
                complete_df,
                resolved_lake_root,
                partition_key,
            )

            partition_counts = grid_count_map(committed_partition_df)
            grid_results = {
                grid_key: partition_counts.get(grid_key, 0)
                for grid_key in pending_grids
            }
            if grid_results != api_grid_results:
                raise RuntimeError(
                    "外部指数 API 精确待办计数与正式事实复读计数不一致。"
                )
            if any(
                partition_counts.get(grid_key, 0) != 0
                for grid_key in obsolete_grids
            ):
                raise RuntimeError("上游失效外部指数格点仍残留正式事实。")

            calendar_rows = 0
            if pending_grids:
                completed_at = datetime.now(timezone.utc)
                calendar_df = apply_calendar_completion(
                    calendar_df,
                    grid_results,
                    batch_id,
                    completed_at,
                )
                calendar_rows = commit_calendar_partitions(
                    calendar_df,
                    pending_grids,
                    resolved_lake_root,
                )

            # 后续分区继续使用本次已经提交的正式事实内容。
            existing_partition_keys = fact_df[PARTITION_COLUMNS].apply(
                tuple,
                axis=1,
            )
            fact_df = pd.concat([
                fact_df.loc[~existing_partition_keys.isin({partition_key})],
                committed_partition_df,
            ], ignore_index=True)

            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            obsolete_grid_count += len(obsolete_grids)
            click.echo(
                f"partition_committed: key={partition_key}; "
                f"calendar_rows={calendar_rows}; grids={len(group_df)}; "
                f"obsolete_removed={len(obsolete_grids)}"
            )
    finally:
        if session is not None:
            session.close()

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
        remaining_df, remaining_obsolete_df, _ = external_index_reconciliation(
            final_calendar_df,
            final_fact_df,
            requested_start,
            requested_end,
        )
        if not remaining_df.empty or not remaining_obsolete_df.empty:
            raise RuntimeError(
                "外部指数提交后仍存在本次范围内的未完成格点或越界事实。"
            )

    click.echo(
        f"finished: grids={processed_grid_count}; rows={total_rows}; "
        f"obsolete_grids={obsolete_grid_count}; write={str(write).lower()}"
    )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认执行正式湖自动 dry-run；测试写入必须显式使用非正式湖。
    main.main(
        args=[],
        prog_name="c04_external_index",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

