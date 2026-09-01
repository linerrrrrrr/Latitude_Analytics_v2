#!/usr/bin/env python
# coding: utf-8

# # c02 生意社国内现货基差原始页面归档
# 
# 本入口长期只归档生意社日页面的 HTTP 原始响应，不解析 HTML、不提取价格、合约或基差字段，
# 也不生产结构化现货基差事实。
# 
# 正式原文位于：
# `raw/100ppi/domestic_spot_basis/year=YYYY/month=MM/observation_date=YYYY-MM-DD/`，
# 每个日期固定保存 `response.html` 与 `response.sha256`。

# ## 自动差集、状态修复与写入边界
# 
# 自动请求范围为 `domestic_spot_basis/ALL required 日期 − 原文与日历共同证明完整的日期`。
# 原文已经完整但日历状态陈旧时，只从正式 raw 复读并无 API 修复日历。
# 
# HTTP 200 的任意响应字节（包括空正文）都按原样归档；这里只验证 HTTP 状态、原始字节、
# SHA-256 和正式路径复读，不根据正文结构或业务内容另作判断。`--write` 是唯一写入开关；
# 显式日期不得写入 `.env` 指向的正式湖。

# ## 初始化与权威外部市场日历 Schema

# In[ ]:


from __future__ import annotations

import hashlib
import pathlib
import re
import shutil
import sys
import uuid
from datetime import date, datetime, timezone

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 从任意子目录运行时，按项目唯一标记规则定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.notebook_schema_browser import display_schema_metadata
from config.settings import settings


# ## Schema 契约呈现
# 
# c02 不再拥有结构化事实 Schema；这里只呈现其直接消费和回写的外部市场日历契约。

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if 'ipykernel' in sys.modules:
    display_schema_metadata([EXTERNAL_MARKET_CALENDAR_SCHEMA])


# ## 日历键、raw 路径与请求常量

# In[ ]:


# 日历表名、主键和 Hive 分区只从权威 Schema metadata 读取一次。
CALENDAR_TABLE_NAME = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 外部市场采集日历表名。
CALENDAR_PRIMARY_KEY = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集、实体与观测日期唯一确定一个请求格点。
CALENDAR_PARTITION_COLUMNS = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 日历按数据集、年、月分区。

DATASET_NAME = "domestic_spot_basis"  # 外部市场日历中的生意社原文数据集代码。
ENTITY_CODE = "ALL"  # 每个日期请求一次完整页面，不按商品拆请求实体。

# raw 不是 silver 表，不用 Arrow Schema；路径本身是长期物理契约。
RAW_RELATIVE_ROOT = pathlib.Path("raw") / "100ppi" / "domestic_spot_basis"
RESPONSE_FILE_NAME = "response.html"
SHA256_FILE_NAME = "response.sha256"

URL_TEMPLATE = "https://www.100ppi.com/sf/day-{observation_date}.html"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.100ppi.com/sf/",
}

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

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        EXTERNAL_MARKET_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 外部市场日历读取与直接依赖校验
# 
# 日历生产者承担完整表级语义；c02 只复核当前调度和状态回写直接依赖的 Schema、主键、
# 日期分区及状态自洽关系，并落实本 raw 链路的完成门禁。

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威字段顺序重建后比较整表 metadata。
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

    expected_file_schema = pa.schema(
        [
            field
            for field in schema
            if field.name not in CALENDAR_PARTITION_COLUMNS
        ],
        metadata=schema.metadata,
    )
    for fragment in dataset.get_fragments():
        if not fragment.physical_schema.equals(
            expected_file_schema,
            check_metadata=True,
        ):
            raise TypeError(
                f"{label}存在 Schema/metadata 不一致的 fragment：{fragment.path}"
            )

    return dataset


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
            raise ValueError(f"{context}正式下游产物数量不得为负。")
        if (
            row["dataset_name"] == DATASET_NAME
            and row["entity_code"] != ENTITY_CODE
        ):
            raise ValueError(f"{context}生意社原始页面请求实体必须为 ALL。")
        if (
            row["dataset_name"] == DATASET_NAME
            and row["fetch_result_status"] == "empty_confirmed"
        ):
            raise ValueError(f"{context}生意社原始页面归档禁止 empty_confirmed。")

        if not row["is_fetch_required"]:
            if row["fetch_result_status"] != "not_required":
                raise ValueError(f"{context}无需请求格点必须为 not_required。")
            if row["is_fetch_completed"] or row["is_data_missing"]:
                raise ValueError(f"{context}无需请求格点不得标记完成或缺失。")
            if row["actual_record_count"] != 0:
                raise ValueError(f"{context}无需请求格点的产物数必须为零。")
            if row["quality_status"] != "not_applicable":
                raise ValueError(f"{context}无需请求格点必须为 not_applicable。")
        elif row["fetch_result_status"] == "not_required":
            raise ValueError(f"{context}需请求格点不得标为 not_required。")

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
                raise ValueError(f"{context}success 必须有正式产物且不得标记缺失。")
        if (
            row["fetch_result_status"] == "empty_confirmed"
            and row["actual_record_count"] != 0
        ):
            raise ValueError(f"{context}empty_confirmed 的事实计数必须为零。")
        if (
            row["is_data_missing"]
            and row["fetch_result_status"] != "empty_confirmed"
        ):
            raise ValueError(f"{context}数据缺失只能来自确认空响应。")

        # 本链路只有原始响应与摘要正式复读一致才算完成一项。
        if (
            row["dataset_name"] == DATASET_NAME
            and row["is_fetch_completed"]
            and (
                row["fetch_result_status"] != "success"
                or row["actual_record_count"] != 1
                or row["is_data_missing"]
                or row["quality_status"] != "passed"
            )
        ):
            raise ValueError(
                f"{context}生意社完成格点必须为 success + passed、产物数 1 且不缺失。"
            )

        if (
            row["quality_status"] in {"passed", "warning", "failed"}
            and row["quality_checked_at"] is None
        ):
            raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
        if row["updated_at"] > now_utc:
            raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

    return normalized.sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)


# ## raw 正式完整性、自动待办与 HTTP 请求
# 
# `response.sha256` 只证明原始字节未被改变，不属于页面业务提取。隐藏 staging、backup、
# quarantine 与 failed 目录都不计入正式完成水位。

# In[ ]:


SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def raw_leaf_path(raw_root: pathlib.Path, observation_date: date) -> pathlib.Path:
    # 日期同时决定 year/month Hive 目录和唯一正式叶目录。
    return (
        raw_root
        / f"year={observation_date.year:04d}"
        / f"month={observation_date.month:02d}"
        / f"observation_date={observation_date.isoformat()}"
    )


def inspect_raw_leaf(leaf_path: pathlib.Path) -> dict[str, object] | None:
    # 缺失、部分写入、摘要损坏或多余文件都不属于完整正式原文。
    if not leaf_path.is_dir():
        return None

    response_path = leaf_path / RESPONSE_FILE_NAME
    sha256_path = leaf_path / SHA256_FILE_NAME
    if not response_path.is_file() or not sha256_path.is_file():
        return None

    official_files = sorted(
        path.name
        for path in leaf_path.iterdir()
        if path.is_file()
    )
    if official_files != [RESPONSE_FILE_NAME, SHA256_FILE_NAME]:
        return None

    try:
        expected_digest = sha256_path.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        return None
    if SHA256_PATTERN.fullmatch(expected_digest) is None:
        return None

    response_bytes = response_path.read_bytes()
    actual_digest = hashlib.sha256(response_bytes).hexdigest()
    if actual_digest != expected_digest:
        return None

    return {
        "byte_count": len(response_bytes),
        "sha256": actual_digest,
    }


def raw_quality_reason(byte_count: int, digest: str) -> str:
    return (
        "生意社 HTTP 200 原始响应已从正式 raw 复读并核对 SHA-256；"
        f"bytes={byte_count}；sha256={digest}；未解析 HTML 或提取业务字段。"
    )


def calendar_grid_is_complete(
    row: dict[str, object],
    evidence: dict[str, object],
) -> bool:
    return bool(
        row["is_fetch_completed"]
        and row["fetch_result_status"] == "success"
        and not row["is_data_missing"]
        and row["actual_record_count"] == 1
        and row["quality_status"] == "passed"
        and row["fetch_run_id"]
        and row["fetch_completed_at"] is not None
        and row["quality_checked_at"] is not None
        and row["quality_reason"]
        == raw_quality_reason(
            int(evidence["byte_count"]),
            str(evidence["sha256"]),
        )
    )


def plan_raw_grids(
    calendar_df: pd.DataFrame,
    raw_root: pathlib.Path,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    selected_df = calendar_df.loc[
        calendar_df["dataset_name"].eq(DATASET_NAME)
        & calendar_df["entity_code"].eq(ENTITY_CODE)
        & calendar_df["is_fetch_required"]
    ].copy()
    if start_date is not None:
        selected_df = selected_df.loc[
            selected_df["observation_date"].between(start_date, end_date)
        ]

    pending_rows = []
    repair_rows = []
    complete_count = 0
    for row in selected_df.to_dict("records"):
        observation_date = row["observation_date"]
        evidence = inspect_raw_leaf(
            raw_leaf_path(raw_root, observation_date)
        )
        if evidence is None:
            pending_rows.append(row)
            continue

        row.update({
            "raw_byte_count": evidence["byte_count"],
            "raw_sha256": evidence["sha256"],
        })
        if calendar_grid_is_complete(row, evidence):
            complete_count += 1
        else:
            repair_rows.append(row)

    result_columns = [
        *EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
        "raw_byte_count",
        "raw_sha256",
    ]
    pending_df = pd.DataFrame(pending_rows)
    if pending_df.empty:
        pending_df = pd.DataFrame(columns=result_columns)
    else:
        pending_df["raw_byte_count"] = pd.NA
        pending_df["raw_sha256"] = pd.NA
        pending_df = pending_df.loc[:, result_columns]

    repair_df = pd.DataFrame(repair_rows)
    if repair_df.empty:
        repair_df = pd.DataFrame(columns=result_columns)
    else:
        repair_df = repair_df.loc[:, result_columns]

    return (
        pending_df.sort_values("observation_date").reset_index(drop=True),
        repair_df.sort_values("observation_date").reset_index(drop=True),
        complete_count,
    )


class RawRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        response_content: bytes | None = None,
    ) -> None:
        super().__init__(message)
        self.response_content = response_content


def create_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_raw_response(
    session: requests.Session,
    observation_date: date,
) -> tuple[bytes, str]:
    url = URL_TEMPLATE.format(
        observation_date=observation_date.isoformat()
    )
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as error:
        raise RawRequestError(
            f"retryable_error: 生意社连接失败；date={observation_date}。"
        ) from error

    response_content = bytes(response.content)
    if response.status_code != 200:
        error_kind = (
            "retryable_error"
            if response.status_code in {408, 429} or response.status_code >= 500
            else "permanent_error"
        )
        raise RawRequestError(
            f"{error_kind}: 生意社 HTTP {response.status_code}；"
            f"date={observation_date}。",
            response_content=response_content,
        )

    return response_content, url


def commit_raw_response(
    raw_root: pathlib.Path,
    observation_date: date,
    response_content: bytes,
) -> dict[str, object]:
    digest = hashlib.sha256(response_content).hexdigest()
    run_id = uuid.uuid4().hex
    target_path = raw_leaf_path(raw_root, observation_date)
    staging_path = raw_root / f".staging-{run_id}"
    backup_path = raw_root / f".backup-{run_id}"
    quarantine_path = raw_root / f".quarantine-{run_id}"

    for managed_path in [
        target_path,
        staging_path,
        backup_path,
        quarantine_path,
    ]:
        if not managed_path.resolve().is_relative_to(raw_root.resolve()):
            raise ValueError(f"raw 管理路径越界：{managed_path}")

    raw_root.mkdir(parents=True, exist_ok=True)
    try:
        staging_path.mkdir(parents=True, exist_ok=False)
        (staging_path / RESPONSE_FILE_NAME).write_bytes(response_content)
        (staging_path / SHA256_FILE_NAME).write_text(
            digest + "\n",
            encoding="utf-8",
            newline="\n",
        )
        staged_evidence = inspect_raw_leaf(staging_path)
        if staged_evidence is None:
            raise ValueError("生意社 raw staging 正式格式或摘要复读失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    target_had_existing = target_path.exists()
    old_target_moved = False
    new_target_installed = False
    cleanup_recovery_paths = True
    try:
        if target_had_existing:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(target_path), str(backup_path))
            old_target_moved = True

        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staging_path), str(target_path))
        new_target_installed = True

        committed_evidence = inspect_raw_leaf(target_path)
        if committed_evidence != staged_evidence:
            raise ValueError("生意社 raw 正式路径逐字节复读失败。")
    except Exception as commit_error:
        rollback_errors = []
        try:
            if new_target_installed and target_path.exists():
                shutil.move(str(target_path), str(quarantine_path))
            if old_target_moved and backup_path.exists():
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(backup_path), str(target_path))
        except Exception as rollback_error:
            rollback_errors.append(str(rollback_error))

        if rollback_errors:
            cleanup_recovery_paths = False
            raise RuntimeError(
                "生意社 raw 提交失败且自动回滚未完成；"
                f"请检查 {backup_path} 与 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        if cleanup_recovery_paths:
            shutil.rmtree(staging_path, ignore_errors=True)
            shutil.rmtree(backup_path, ignore_errors=True)
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return committed_evidence


def preserve_failed_response(
    raw_root: pathlib.Path,
    batch_id: str,
    observation_date: date,
    response_content: bytes,
) -> pathlib.Path:
    failed_leaf = (
        raw_root
        / f".failed-{batch_id}"
        / f"year={observation_date.year:04d}"
        / f"month={observation_date.month:02d}"
        / f"observation_date={observation_date.isoformat()}"
    )
    if not failed_leaf.resolve().is_relative_to(raw_root.resolve()):
        raise ValueError("raw 失败证据路径越界。")
    failed_leaf.mkdir(parents=True, exist_ok=False)
    digest = hashlib.sha256(response_content).hexdigest()
    (failed_leaf / RESPONSE_FILE_NAME).write_bytes(response_content)
    (failed_leaf / SHA256_FILE_NAME).write_text(
        digest + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return failed_leaf


# ## 日历状态回写与完整叶分区提交
# 
# 原始响应先正式提交并逐字节复读，随后才允许回写完成状态。日历按完整
# `dataset_name/year/month` 叶分区提交，分区内其他实体和日期原样保留。

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    evidence_by_date: dict[date, dict[str, object]],
    run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    updated_df = calendar_df.copy()
    for observation_date, evidence in evidence_by_date.items():
        mask = (
            updated_df["dataset_name"].eq(DATASET_NAME)
            & updated_df["entity_code"].eq(ENTITY_CODE)
            & updated_df["observation_date"].eq(observation_date)
            & updated_df["is_fetch_required"]
        )
        if int(mask.sum()) != 1:
            raise ValueError(f"待完成 raw 格点不唯一：{observation_date}")

        updated_df.loc[mask, "is_fetch_completed"] = True
        updated_df.loc[mask, "fetch_result_status"] = "success"
        updated_df.loc[mask, "is_data_missing"] = False
        updated_df.loc[mask, "actual_record_count"] = 1
        updated_df.loc[mask, "quality_status"] = "passed"
        updated_df.loc[mask, "quality_reason"] = raw_quality_reason(
            int(evidence["byte_count"]),
            str(evidence["sha256"]),
        )
        updated_df.loc[mask, "fetch_run_id"] = run_id
        updated_df.loc[mask, "fetch_completed_at"] = completed_at
        updated_df.loc[mask, "quality_checked_at"] = completed_at
        updated_df.loc[mask, "updated_at"] = completed_at

    return validate_calendar_frame(updated_df, "raw 完成状态更新后的")


def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    observation_date: date,
    fetch_status: str,
    reason: str,
    run_id: str,
    checked_at: datetime,
) -> pd.DataFrame:
    if fetch_status not in {"retryable_error", "permanent_error"}:
        raise ValueError("失败状态必须是 retryable_error 或 permanent_error。")

    updated_df = calendar_df.copy()
    mask = (
        updated_df["dataset_name"].eq(DATASET_NAME)
        & updated_df["entity_code"].eq(ENTITY_CODE)
        & updated_df["observation_date"].eq(observation_date)
        & updated_df["is_fetch_required"]
    )
    if int(mask.sum()) != 1:
        raise ValueError(f"待失败 raw 格点不唯一：{observation_date}")

    updated_df.loc[mask, "is_fetch_completed"] = False
    updated_df.loc[mask, "fetch_result_status"] = fetch_status
    updated_df.loc[mask, "is_data_missing"] = False
    updated_df.loc[mask, "actual_record_count"] = 0
    updated_df.loc[mask, "quality_status"] = "failed"
    updated_df.loc[mask, "quality_reason"] = reason
    updated_df.loc[mask, "fetch_run_id"] = run_id
    updated_df.loc[mask, "fetch_completed_at"] = None
    updated_df.loc[mask, "quality_checked_at"] = checked_at
    updated_df.loc[mask, "updated_at"] = checked_at

    return validate_calendar_frame(updated_df, "raw 失败状态更新后的")


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
        touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(
            index=False,
            name=None,
        )
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

        for managed_path in [
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ]:
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
            expression = (
                (ds.field("dataset_name") == partition_key[0])
                & (ds.field("year") == partition_key[1])
                & (ds.field("month") == partition_key[2])
            )
            staged_df = validate_calendar_frame(
                arrow_to_pandas(
                    staged_dataset.to_table(
                        columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                        filter=expression,
                    ),
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "staging 完整外部市场日历分区",
            )
            if not pandas_to_arrow(
                staged_df,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ).equals(complete_table):
                raise ValueError("外部市场日历 staging 内容检查失败。")
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        saved_path = backup_path / relative_path
        target_had_partition = destination_path.exists()
        old_partition_moved = False
        new_partition_installed = False
        cleanup_recovery_paths = True

        try:
            if target_had_partition:
                saved_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination_path), str(saved_path))
                old_partition_moved = True

            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(destination_path))
            new_partition_installed = True

            committed_dataset = open_exact_dataset(
                target_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "正式外部市场日历",
            )
            committed_df = validate_calendar_frame(
                arrow_to_pandas(
                    committed_dataset.to_table(
                        columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                        filter=expression,
                    ),
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


# ## CLI：自动 raw 补缺、无 API 状态修复与是否写入
# 
# CLI 在读取湖或创建 HTTP Session 前执行正式湖显式日期写入门禁。每个 200 响应先提交 raw，
# 正式复读成功后再提交日历；HTTP 失败只回写未完成状态，已收到的失败正文保留在 raw 隐藏证据目录。

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
    raw_root = resolved_lake_root / RAW_RELATIVE_ROOT
    calendar_path = silver_root / CALENDAR_TABLE_NAME

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

    pending_df, repair_df, complete_count = plan_raw_grids(
        calendar_df,
        raw_root,
        requested_start,
        requested_end,
    )
    mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"dataset={DATASET_NAME}; mode={mode}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )
    click.echo(
        "reconciliation_plan: "
        f"complete_grid_count={complete_count}; "
        f"state_repair_count={len(repair_df)}; "
        f"api_pending_grid_count={len(pending_df)}"
    )
    if pending_df.empty and repair_df.empty:
        click.echo("生意社原始页面与外部市场日历状态已经完整一致。")
        return

    batch_id = uuid.uuid4().hex

    # 正式 raw 完整但日历陈旧时，先无 API 修复并正式复读日历。
    if not repair_df.empty:
        repair_evidence = {
            row["observation_date"]: {
                "byte_count": int(row["raw_byte_count"]),
                "sha256": str(row["raw_sha256"]),
            }
            for row in repair_df.to_dict("records")
        }
        click.echo(
            f"state_repair_plan: grids={len(repair_evidence)}; "
            f"write={str(write).lower()}; api_requests=0"
        )
        if write:
            repaired_at = datetime.now(timezone.utc)
            calendar_df = apply_calendar_completion(
                calendar_df,
                repair_evidence,
                f"raw-state-repair-{batch_id}",
                repaired_at,
            )
            commit_calendar_partitions(
                calendar_df,
                set(repair_evidence),
                resolved_lake_root,
            )
            calendar_dataset = open_exact_dataset(
                calendar_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "raw 状态修复后的正式外部市场日历",
            )
            calendar_df = validate_calendar_frame(
                arrow_to_pandas(
                    calendar_dataset.to_table(
                        columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                    ),
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "raw 状态修复后的正式",
            )
            pending_df, repair_df, complete_count = plan_raw_grids(
                calendar_df,
                raw_root,
                requested_start,
                requested_end,
            )
            if not repair_df.empty:
                raise RuntimeError("生意社 raw 状态修复后仍有陈旧日历格点。")

    if pending_df.empty:
        click.echo(
            f"finished: complete_grids={complete_count}; "
            "api_requests=0"
        )
        return

    session = create_http_session()
    processed_grid_count = 0
    total_bytes = 0
    try:
        for observation_date in pending_df["observation_date"].tolist():
            try:
                response_content, url = fetch_raw_response(
                    session,
                    observation_date,
                )
            except RawRequestError as error:
                if write:
                    if error.response_content is not None:
                        failed_path = preserve_failed_response(
                            raw_root,
                            batch_id,
                            observation_date,
                            error.response_content,
                        )
                        click.echo(
                            f"raw_failure_preserved={failed_path}",
                            err=True,
                        )

                    message = str(error)
                    fetch_status = (
                        "retryable_error"
                        if message.startswith("retryable_error:")
                        else "permanent_error"
                    )
                    failed_at = datetime.now(timezone.utc)
                    calendar_df = apply_calendar_failure(
                        calendar_df,
                        observation_date,
                        fetch_status,
                        f"生意社原始页面请求失败：{message}",
                        batch_id,
                        failed_at,
                    )
                    commit_calendar_partitions(
                        calendar_df,
                        {observation_date},
                        resolved_lake_root,
                    )
                raise click.ClickException(str(error)) from error

            digest = hashlib.sha256(response_content).hexdigest()
            click.echo(
                f"http_success: date={observation_date}; "
                f"bytes={len(response_content)}; sha256={digest}; url={url}"
            )
            if write:
                evidence = commit_raw_response(
                    raw_root,
                    observation_date,
                    response_content,
                )
                completed_at = datetime.now(timezone.utc)
                calendar_df = apply_calendar_completion(
                    calendar_df,
                    {observation_date: evidence},
                    batch_id,
                    completed_at,
                )
                commit_calendar_partitions(
                    calendar_df,
                    {observation_date},
                    resolved_lake_root,
                )

            processed_grid_count += 1
            total_bytes += len(response_content)
    finally:
        session.close()

    if write:
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
        remaining_df, stale_df, _ = plan_raw_grids(
            final_calendar_df,
            raw_root,
            requested_start,
            requested_end,
        )
        if not remaining_df.empty or not stale_df.empty:
            raise RuntimeError("生意社 raw 提交后仍存在未完成或陈旧格点。")

    click.echo(
        f"finished: grids={processed_grid_count}; bytes={total_bytes}; "
        f"write={str(write).lower()}"
    )


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    # Notebook 默认只读计划；写入必须由操作者显式修改参数。
    main.main(
        args=[],
        prog_name="c02_domestic_spot_basis",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

