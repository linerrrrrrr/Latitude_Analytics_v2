#!/usr/bin/env python
# coding: utf-8

# # b02 生意社国内现货基差原始页面归档
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
import time
import uuid
from datetime import date, datetime, timezone

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 从任意子目录运行时，按项目唯一标记规则定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约呈现
# 
# b02 不再拥有结构化事实 Schema；这里只呈现其直接消费和回写的外部市场日历契约。
# 
# 安装与失败恢复由湖仓级 `a00_04_staged_path_transaction.py` 负责；raw 摘要与日历验收仍由本环节执行。

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([EXTERNAL_MARKET_CALENDAR_SCHEMA], lake_root=settings.futures_lake_root)


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


# ## raw 归档样例
# 只查看已有文件的状态、大小和已保存摘要；空库显示尚未归档。

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_raw_archive_demo

    display_raw_archive_demo(
        settings.futures_lake_root / RAW_RELATIVE_ROOT,
        filenames=(RESPONSE_FILE_NAME, SHA256_FILE_NAME),
        partitions=("year", "month", "observation_date"),
    )


# ## 可信上游读取与 dirty 日历叶验收
# 
# 正式日历的表级业务语义由生产者提交保证。`main()` 只物化 `domestic_spot_basis` 记录并完成一次权威 Arrow→Pandas 转换，不再重验全表业务。`open_exact_dataset()` 精确核对物理字段、类型、nullable 和表名/主键/分区身份；描述性 metadata 以当前契约为准。指定 `partition_base_dir` 时只打开当前叶，仍从 Hive 路径补回分区列。
# 
# `validate_calendar_table()` 仅接收已经通过 `pandas_to_arrow()` 的当前 dirty 完整叶，在提交前执行一次主键、状态、计数、日期与审计时间的完整业务验收并排序。主键检测只转换主键投影，业务循环直接消费 Arrow 记录；没有全表 Pandas/Arrow 往返。状态集合和排序键在逐行循环前准备。
# 
# 打开函数与业务校验函数保留起止、数量和失败日志；实际表物化由调用方报告。fragment 每 100 个、日历每 10000 行检查一次 2 秒日志间隔，不增加扫描。

# In[ ]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威字段顺序重建后比较物理结构。
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
    *,
    partition_base_dir: pathlib.Path | None = None,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_checked_fragments = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else []
        )
        if not parquet_files:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
            partition_base_dir=str(partition_base_dir) if partition_base_dir is not None else None,
        )
        log_phase = "schema"
        identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
        if (
            len(dataset.schema.names) != len(schema.names)
            or set(dataset.schema.names) != set(schema.names)
            or not reconstructed_schema(dataset, schema).equals(schema, check_metadata=False)
        ):
            raise TypeError(f"{label}物理字段、类型或 nullable 与权威契约不一致。")
        if any((dataset.schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_metadata_keys):
            raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不一致。")

        expected_file_schema = pa.schema(
            [
                field
                for field in schema
                if field.name not in CALENDAR_PARTITION_COLUMNS
            ],
            metadata=schema.metadata,
        )
        log_phase = "fragment_schema"
        for fragment in dataset.get_fragments():
            fragment_schema = fragment.physical_schema
            if not fragment_schema.equals(expected_file_schema, check_metadata=False):
                raise TypeError(f"{label}存在物理字段、类型或 nullable 不一致的 fragment：{fragment.path}")
            if any((fragment_schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_metadata_keys):
                raise TypeError(f"{label}存在表身份 metadata 不一致的 fragment：{fragment.path}")
            log_checked_fragments += 1
            if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=open_exact_dataset; phase=fragment_schema; status=running; "
                    f"label={label}; checked_fragments={log_checked_fragments}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; checked_fragments={log_checked_fragments}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=open_exact_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; checked_fragments={log_checked_fragments}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def validate_calendar_table(
    calendar_table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_checked_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "primary_key"
        calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()
        log_phase = "primary_key"
        if calendar_keys_df.duplicated().any():
            raise ValueError(f"{context}外部市场日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        completed_fetch_statuses = {"success", "empty_confirmed"}
        checked_quality_statuses = {"passed", "warning", "failed"}
        calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
        log_phase = "business_validation"
        for row in calendar_table.to_pylist():
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

            completed_status = row["fetch_result_status"] in completed_fetch_statuses
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
                row["quality_status"] in checked_quality_statuses
                and row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
            if row["updated_at"] > now_utc:
                raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")
            log_checked_rows += 1
            if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=running; "
                    f"context={context}; checked_rows={log_checked_rows}/{calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "sort"
        validated_calendar_table = calendar_table.sort_by(calendar_sort_keys)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=completed; "
            f"context={context}; checked_rows={log_checked_rows}; rows={validated_calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; checked_rows={log_checked_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## raw 完整性、自动待办与逐日期共享事务
# 
# `plan_raw_grids()` 区分已完成、仅需日历修复和需要 HTTP 采集的日期。原文与摘要完整但日历陈旧时，无需创建 HTTP Session；缺少原文或摘要不匹配才进入采集。HTTP 200 的任意原始字节均可归档，包括空正文；没有页面解析或内容质量判断。
# 
# `commit_raw_response()` 暂存一个日期的 `response.html` 和 `response.sha256` 并复读摘要，然后用一个 `StagedPathTransaction` 安装完整日期目录。正式路径的字节与摘要仍在事务内复读；只有成功退出后才返回证据，供后续日历回写使用。
# 
# 安装或正式验收失败时，共享模块按实际移动记录恢复当前日期的旧目录；首次备份失败不删除原目录。已安装的新目录保留在本批 `.quarantine-<run_id>` 内，恢复不完整时另保留 `.backup-<run_id>`；staging 清理，异常保留原因链与现场路径。隐藏的恢复目录不算正式完成。HTTP 非 200 的响应正文仍由 `preserve_failed_response()` 写入 `.failed-<batch_id>`，与事务隔离目录分开。
# 
# `create_http_session()` 现有 `Retry(total=4)` 配置保持不变；一次日期采集调用可能包含适配器重试，不能把日期数等同于实际 HTTP 尝试次数。
# 
# 待办函数自行报告选中日期数、已复读日期数以及已完成、需修复、需请求的数量，每 100 个日期检查一次 2 秒输出间隔。`inspect_raw_leaf()` 保持安静，由其调用方按逻辑块报告进度。采集函数自行报告起止、响应字节、摘要和失败；`http_success` 仅表示 HTTP 200 已接收，`persisted=false`。单日期 raw 提交在退出共享事务后才报告 `partition_committed`，此时日历尚未回写。隐藏失败正文由保存函数报告，并明确 `formal_completion=false`。
# 
# 原文两文件的完整性检查、内容 SHA-256，以及 raw staging 和正式路径复读继续保留。启动计划只扫描一次所选日期；本次成功提交是后续状态修复与批末结束的依据，不为复核重复扫描 clean raw 历史。

# ### 局部流程：raw 待办与逐日期安装
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告待办生成；计数复读 raw 原文与摘要"] --> B{"raw 与日历都完整？"}
#     B -->|是| C["跳过已完成日期"]
#     B -->|仅 raw 完整| D["生成无 HTTP 状态修复计划"]
#     B -->|raw 不完整| E["函数报告采集起止；保留现有适配器重试"]
#     E --> F{"HTTP 200？"}
#     F -->|否且写入| G["隐藏目录保存失败正文；回写未完成状态；抛错"]
#     F -->|是且写入| H["暂存两文件；复读字节与 SHA-256"]
#     H --> I["当前日期共享事务：安装目录并正式复读"]
#     I --> J["退出事务后报告 raw 落盘；返回证据"]
#     I -. 失败 .-> K["恢复当前旧目录；保留失败新数据；抛错"]
# ```

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
    log_started_at = time.perf_counter()
    log_phase = "plan"
    log_checked_dates = 0
    log_date = None
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=plan_raw_grids; phase=plan; status=started; "
        f"calendar_rows={len(calendar_df)}; start_date={start_date}; end_date={end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "select_required"
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
        log_phase = "raw_inspection"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=plan_raw_grids; phase=raw_inspection; status=started; "
            f"selected_dates={len(selected_df)}; path={raw_root}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for row in selected_df.to_dict("records"):
            if log_checked_dates and log_checked_dates % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=plan_raw_grids; phase=raw_inspection; status=running; "
                    f"checked_dates={log_checked_dates}/{len(selected_df)}; complete_grid_count={complete_count}; state_repair_count={len(repair_rows)}; api_pending_grid_count={len(pending_rows)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            observation_date = row["observation_date"]
            log_date = observation_date
            evidence = inspect_raw_leaf(
                raw_leaf_path(raw_root, observation_date)
            )
            log_checked_dates += 1
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

        log_phase = "plan_frames"
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

        planned_raw_grids = (pending_df.sort_values('observation_date').reset_index(drop=True), repair_df.sort_values('observation_date').reset_index(drop=True), complete_count)
        click.echo(
            f"reconciliation_plan: dataset={DATASET_NAME}; function=plan_raw_grids; phase=plan; status=completed; "
            f"checked_dates={log_checked_dates}; complete_grid_count={complete_count}; state_repair_count={len(repair_df)}; api_pending_grid_count={len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return planned_raw_grids
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=plan_raw_grids; phase=plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={log_date}; checked_dates={log_checked_dates}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


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
    log_started_at = time.perf_counter()
    log_phase = "fetch"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=fetch_raw_response; phase=fetch; status=started; "
        f"date={observation_date}; timeout_s={REQUEST_TIMEOUT_SECONDS}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        url = URL_TEMPLATE.format(
            observation_date=observation_date.isoformat()
        )
        try:
            log_phase = "http_request"
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as error:
            raise RawRequestError(
                f"retryable_error: 生意社连接失败；date={observation_date}。"
            ) from error

        log_phase = "http_status"
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

        log_digest = hashlib.sha256(response_content).hexdigest()
        click.echo(
            f"http_success: dataset={DATASET_NAME}; function=fetch_raw_response; phase=fetch; status=completed; "
            f"date={observation_date}; bytes={len(response_content)}; sha256={log_digest}; url={url}; http_status=200; fetch_calls=1; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return response_content, url
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=fetch_raw_response; phase=fetch; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={observation_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def commit_raw_response(
    raw_root: pathlib.Path,
    observation_date: date,
    response_content: bytes,
) -> dict[str, object]:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=commit; status=started; "
        f"date={observation_date}; bytes={len(response_content)}; scope=raw_date; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "prepare"
        digest = hashlib.sha256(response_content).hexdigest()
        run_id = uuid.uuid4().hex
        resolved_raw_root = raw_root.resolve()
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
            if not managed_path.resolve().is_relative_to(resolved_raw_root):
                raise ValueError(f"raw 管理路径越界：{managed_path}")

        raw_root.mkdir(parents=True, exist_ok=True)
        try:
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=staging_write; status=started; "
                f"date={observation_date}; bytes={len(response_content)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)
            (staging_path / RESPONSE_FILE_NAME).write_bytes(response_content)
            (staging_path / SHA256_FILE_NAME).write_text(
                digest + "\n",
                encoding="utf-8",
                newline="\n",
            )
            log_phase = "staging_readback"
            staged_evidence = inspect_raw_leaf(staging_path)
            if staged_evidence is None:
                raise ValueError("生意社 raw staging 正式格式或摘要复读失败。")
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=staging_readback; status=completed; "
                f"date={observation_date}; bytes={staged_evidence['byte_count']}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=install; status=started; "
                f"date={observation_date}; target={target_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=raw_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"dataset={DATASET_NAME}; function=commit_raw_response; "
                    f"date={observation_date}; run_id={run_id}"
                ),
            ) as transaction:
                transaction.replace(target_path=target_path, staged_path=staging_path)

                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=formal_readback; status=started; "
                    f"date={observation_date}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_evidence = inspect_raw_leaf(target_path)
                if committed_evidence != staged_evidence:
                    raise ValueError("生意社 raw 正式路径逐字节复读失败。")
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=formal_readback; status=completed; "
                    f"date={observation_date}; bytes={committed_evidence['byte_count']}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "transaction_exit"
        finally:
            # 事务进入前的异常也需清理 staging；安装后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)

        click.echo(
            f"partition_committed: dataset={DATASET_NAME}; function=commit_raw_response; phase=commit; status=completed; "
            f"date={observation_date}; run_id={run_id}; scope=raw_date; bytes={committed_evidence['byte_count']}; sha256={committed_evidence['sha256']}; persisted=true; calendar_state=not_updated; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return committed_evidence
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_raw_response; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={observation_date}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def preserve_failed_response(
    raw_root: pathlib.Path,
    batch_id: str,
    observation_date: date,
    response_content: bytes,
) -> pathlib.Path:
    log_started_at = time.perf_counter()
    log_phase = "failure_evidence"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=preserve_failed_response; phase=failure_evidence; status=started; "
        f"date={observation_date}; bytes={len(response_content)}; formal_completion=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        failed_leaf = (
            raw_root
            / f".failed-{batch_id}"
            / f"year={observation_date.year:04d}"
            / f"month={observation_date.month:02d}"
            / f"observation_date={observation_date.isoformat()}"
        )
        if not failed_leaf.resolve().is_relative_to(raw_root.resolve()):
            raise ValueError("raw 失败证据路径越界。")
        log_phase = "write_evidence"
        failed_leaf.mkdir(parents=True, exist_ok=False)
        digest = hashlib.sha256(response_content).hexdigest()
        (failed_leaf / RESPONSE_FILE_NAME).write_bytes(response_content)
        (failed_leaf / SHA256_FILE_NAME).write_text(
            digest + "\n",
            encoding="utf-8",
            newline="\n",
        )
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=preserve_failed_response; phase=failure_evidence; status=completed; "
            f"date={observation_date}; bytes={len(response_content)}; path={failed_leaf}; persisted=true; formal_completion=false; elapsed_s={time.perf_counter() - log_started_at:.3f}", err=True
        )
        return failed_leaf
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=preserve_failed_response; phase=failure_evidence; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={observation_date}; formal_completion=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 状态生成、dirty 叶一次业务验收与逐叶共享事务
# 
# raw 正式提交并复读成功后，才生成日历完成状态；HTTP 错误只生成未完成状态。两个生成函数只更新内存，不重验全部历史；多日期修复先建立 required 日期→行位置映射，再逐日期更新。完整业务验收统一归 `commit_calendar_partitions()` 的当前 dirty 完整叶负责。
# 
# 提交函数在分区循环前准备一次分组索引、列清单、排序键与正式根路径。每叶只转换并执行一次业务校验；staging 和正式复读直接打开该叶，核对物理契约及排序后的完整 Arrow 内容，不再扫描表根或重复业务验收。staging 不再写从未安装的零行标记，已有正式根级标记保持原样。
# 
# 每个日历叶仍独立进入共享事务，在事务内完成正式复读，成功退出后报告当前叶落盘。失败只恢复当前叶，此前 raw 与成功日历叶保留；失败新叶隔离，恢复不完整保留旧备份，staging 清理。全部触达叶成功后报告日历触达数、完成数及 `date_watermark=none`；错误状态已落盘不等于采集完成。
# 
# raw 日期和日历叶互相独立；再次运行可根据已提交 raw 无 API 修复日历。这里不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。

# ### 局部流程：日历状态与独立叶事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["已提交 raw 证据，或请求错误"] --> B["仅更新内存状态；尚未落盘"]
#     B --> C["按预建分组取 dirty 完整叶；一次业务验收"]
#     C --> D["写 staging；只复读当前叶物理契约和完整内容"]
#     D --> E["当前叶共享事务：备份旧叶；安装新叶"]
#     E --> F["事务内只复读当前叶；物理契约和完整内容一致"]
#     F --> G["退出事务后报告当前叶成功；处理下一叶"]
#     G -->|还有叶| C
#     G -->|全部成功| H["报告日历落盘与完成数；无独立水位"]
#     E -. 失败 .-> R["只恢复当前叶；保留失败现场；抛错"]
#     F -. 失败 .-> R
#     R --> S["此前 raw 与成功日历叶保留；再次运行可无 HTTP 修复"]
# ```

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    evidence_by_date: dict[date, dict[str, object]],
    run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_updated_grids = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=started; "
        f"planned_grids={len(evidence_by_date)}; fetch_status=success; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        updated_df = calendar_df.copy()
        required_rows = updated_df.loc[
            updated_df["dataset_name"].eq(DATASET_NAME)
            & updated_df["entity_code"].eq(ENTITY_CODE)
            & updated_df["is_fetch_required"],
            ["observation_date"],
        ]
        required_row_index_by_date = dict(zip(required_rows["observation_date"], required_rows.index, strict=True))
        log_phase = "update_state"
        for observation_date, evidence in evidence_by_date.items():
            if observation_date not in required_row_index_by_date:
                raise ValueError(f"待完成 raw 格点不存在或无需请求：{observation_date}")
            row_index = required_row_index_by_date[observation_date]

            updated_df.loc[row_index, "is_fetch_completed"] = True
            updated_df.loc[row_index, "fetch_result_status"] = "success"
            updated_df.loc[row_index, "is_data_missing"] = False
            updated_df.loc[row_index, "actual_record_count"] = 1
            updated_df.loc[row_index, "quality_status"] = "passed"
            updated_df.loc[row_index, "quality_reason"] = raw_quality_reason(
                int(evidence["byte_count"]),
                str(evidence["sha256"]),
            )
            updated_df.loc[row_index, "fetch_run_id"] = run_id
            updated_df.loc[row_index, "fetch_completed_at"] = completed_at
            updated_df.loc[row_index, "quality_checked_at"] = completed_at
            updated_df.loc[row_index, "updated_at"] = completed_at
            log_updated_grids += 1
            if log_updated_grids % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=running; "
                    f"updated_grids={log_updated_grids}/{len(evidence_by_date)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "state_ready"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=completed; "
            f"updated_grids={log_updated_grids}; fetch_status=success; rows={len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    observation_date: date,
    fetch_status: str,
    reason: str,
    run_id: str,
    checked_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=started; "
        f"date={observation_date}; fetch_status={fetch_status}; fetch_completed=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if fetch_status not in {"retryable_error", "permanent_error"}:
            raise ValueError("失败状态必须是 retryable_error 或 permanent_error。")

        log_phase = "update_state"
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

        log_phase = "state_ready"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=completed; "
            f"date={observation_date}; fetch_status={fetch_status}; fetch_completed=false; rows={len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_dates: set[date],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "commit"
    log_partition = None
    log_committed_partitions = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=started; "
        f"touched_dates={len(touched_dates)}; scope=calendar_leaves; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not touched_dates:
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=skipped; "
                f"reason=no_touched_dates; calendar_state=unchanged; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        log_phase = "select_partitions"
        touched_mask = (
            calendar_df["dataset_name"].eq(DATASET_NAME)
            & calendar_df["entity_code"].eq(ENTITY_CODE)
            & calendar_df["observation_date"].isin(touched_dates)
        )
        touched_df = calendar_df.loc[touched_mask]
        partition_keys = set(touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None))
        calendar_indices_by_partition = calendar_df.groupby(
            CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
        calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME

        for partition_key in sorted(partition_keys):
            log_partition = partition_key
            log_phase = "leaf_validation"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=leaf_validation; status=started; "
                f"partition={partition_key}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            complete_table = pandas_to_arrow(
                calendar_df.iloc[calendar_indices_by_partition[partition_key]].loc[:, calendar_columns],
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            )
            complete_table = validate_calendar_table(complete_table, "待提交的完整外部市场日历分区")

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
                log_phase = "staging_write"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging_write; status=started; "
                    f"partition={partition_key}; rows={complete_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                staging_path.mkdir(parents=True, exist_ok=False)
                ds.write_dataset(
                    complete_table,
                    staging_path,
                    format="parquet",
                    partitioning=CALENDAR_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

                log_phase = "staging_readback"
                staged_dataset = open_exact_dataset(
                    staging_path / relative_path,
                    CALENDAR_PARTITIONING,
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    "外部市场日历 staging",
                    partition_base_dir=staging_path,
                )
                staged_calendar_table = validate_arrow_table(
                    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ).sort_by(calendar_sort_keys)
                if not staged_calendar_table.equals(complete_table):
                    raise ValueError("外部市场日历 staging 内容检查失败。")
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=completed; "
                    f"partition={partition_key}; rows={staged_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            except Exception:
                shutil.rmtree(staging_path, ignore_errors=True)
                raise

            source_path = staging_path / relative_path
            destination_path = target_path / relative_path

            try:
                log_phase = "install"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=install; status=started; "
                    f"partition={partition_key}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                with StagedPathTransaction(
                    root_path=silver_root,
                    staging_dir=staging_path,
                    backup_dir=backup_path,
                    quarantine_dir=quarantine_path,
                    log_context=(
                        f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; "
                        f"partition={relative_path.as_posix()}; run_id={run_id}"
                    ),
                ) as transaction:
                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path,
                    )

                    log_phase = "formal_readback"
                    click.echo(
                        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=started; "
                        f"partition={partition_key}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    committed_dataset = open_exact_dataset(
                        destination_path,
                        CALENDAR_PARTITIONING,
                        EXTERNAL_MARKET_CALENDAR_SCHEMA,
                        "正式外部市场日历",
                        partition_base_dir=target_path,
                    )
                    committed_calendar_table = validate_arrow_table(
                        committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    ).sort_by(calendar_sort_keys)
                    if not committed_calendar_table.equals(complete_table):
                        raise ValueError("正式外部市场日历分区内容检查失败。")
                    click.echo(
                        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=completed; "
                        f"partition={partition_key}; rows={committed_calendar_table.num_rows}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    log_phase = "transaction_exit"
            finally:
                # 当前叶单独恢复；此前已成功的 raw 和日历叶不回退。
                shutil.rmtree(staging_path, ignore_errors=True)
            log_committed_partitions += 1
            click.echo(
                f"partition_committed: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit_leaf; status=completed; "
                f"partition={partition_key}; rows={committed_calendar_table.num_rows}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; scope=calendar_leaf; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
            f"committed_partitions={log_committed_partitions}; touched_grids={len(touched_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        if log_committed_partitions:
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=calendar_state; status=completed; "
                f"touched_grids={len(touched_df)}; completed_grids={int(touched_df['is_fetch_completed'].sum())}; persisted=true; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        return len(touched_df)
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; committed_partitions={log_committed_partitions}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：一次差集、局部状态更新与是否写入
# 
# 入口在读取或联网前执行正式湖显式日期写入门禁。启动时只物化生意社日历记录，一次复读全部所选 raw 日期并生成已完成、无 API 修复和 HTTP 待办。修复提交成功后信任逐叶正式复读，不再打开全表或重扫全部 raw；完成数只增加实际已提交的修复数。
# 
# 存在 HTTP 待办且启用写入时，在日期循环前把当前日历按叶分组。每个日期只生成和提交其所属完整叶，并将成功状态保留给同叶后续日期；提交顺序仍是当前日期 raw 在先、日历在后。HTTP 错误保留失败正文、写入错误状态后停止；此前成功日期保留。没有写入时仍采集，但不生成持久状态。
# 
# 每次提交函数已完成必要的正式验收，因此批末不再全表业务校验、全历史 raw 扫描或重新求差。正式历史是可信快照；本轮没有并发写入协调或进程中断后自动恢复。运行起止及日期进度由 main 报告，采集、状态生成和落盘由对应函数报告；没有独立日期水位。

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
    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_date = None
    log_boundary = "=" * 88
    click.echo(log_boundary)
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; start_date={start_date}; end_date={end_date}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
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

        log_phase = "calendar_read"
        calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            "正式外部市场日历",
        )
        log_phase = "calendar_read"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=calendar_read; status=started; "
            f"path={calendar_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        calendar_df = arrow_to_pandas(
            calendar_dataset.to_table(
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
                filter=ds.field("dataset_name") == DATASET_NAME,
            ),
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=calendar_read; status=completed; "
            f"rows={len(calendar_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        log_phase = "plan"
        pending_df, repair_df, complete_count = plan_raw_grids(
            calendar_df,
            raw_root,
            requested_start,
            requested_end,
        )
        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run_context; status=completed; "
            f"mode={mode}; lake_root={resolved_lake_root}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        if pending_df.empty and repair_df.empty:
            click.echo(
                f"finished: dataset={DATASET_NAME}; function=main; phase=run; status=completed; "
                f"outcome=up_to_date; fetch_calls=0; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(log_boundary)
            return

        batch_id = uuid.uuid4().hex

        # 正式 raw 完整但日历陈旧时，先无 API 修复并正式复读日历。
        if not repair_df.empty:
            log_phase = "repair_plan"
            repair_evidence = {
                row["observation_date"]: {
                    "byte_count": int(row["raw_byte_count"]),
                    "sha256": str(row["raw_sha256"]),
                }
                for row in repair_df.to_dict("records")
            }
            click.echo(
                f"state_repair_plan: dataset={DATASET_NAME}; function=main; phase=repair_plan; status=completed; "
                f"grids={len(repair_evidence)}; write={str(write).lower()}; fetch_calls=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if write:
                repaired_at = datetime.now(timezone.utc)
                log_phase = "apply_calendar_completion"
                calendar_df = apply_calendar_completion(
                    calendar_df,
                    repair_evidence,
                    f"raw-state-repair-{batch_id}",
                    repaired_at,
                )
                log_phase = "calendar_commit"
                commit_calendar_partitions(
                    calendar_df,
                    set(repair_evidence),
                    resolved_lake_root,
                )
                complete_count += len(repair_evidence)

        if pending_df.empty:
            click.echo(
                f"finished: dataset={DATASET_NAME}; function=main; phase=run; status=completed; "
                f"outcome={'state_repaired' if write else 'repair_planned'}; complete_grids={complete_count}; fetch_calls=0; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(log_boundary)
            return

        log_phase = "http_session"
        if write:
            calendar_leaves = {
                key: frame for key, frame in calendar_df.groupby(
                    CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
                )
            }
        session = create_http_session()
        processed_grid_count = 0
        total_bytes = 0
        try:
            for pending_row in pending_df.itertuples(index=False):
                observation_date = pending_row.observation_date
                if write:
                    partition_key = tuple(getattr(pending_row, name) for name in CALENDAR_PARTITION_COLUMNS)
                    calendar_leaf_df = calendar_leaves[partition_key]
                log_phase = "fetch"
                log_date = observation_date
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=main; phase=fetch_batch; status=running; "
                    f"date={observation_date}; processed_grids={processed_grid_count}/{len(pending_df)}; bytes={total_bytes}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                try:
                    response_content, url = fetch_raw_response(
                        session,
                        observation_date,
                    )
                except RawRequestError as error:
                    if write:
                        if error.response_content is not None:
                            log_phase = "preserve_failed_response"
                            failed_path = preserve_failed_response(
                                raw_root,
                                batch_id,
                                observation_date,
                                error.response_content,
                            )

                        message = str(error)
                        fetch_status = (
                            "retryable_error"
                            if message.startswith("retryable_error:")
                            else "permanent_error"
                        )
                        failed_at = datetime.now(timezone.utc)
                        log_phase = "apply_calendar_failure"
                        calendar_leaf_df = apply_calendar_failure(
                            calendar_leaf_df,
                            observation_date,
                            fetch_status,
                            f"生意社原始页面请求失败：{message}",
                            batch_id,
                            failed_at,
                        )
                        log_phase = "calendar_commit"
                        commit_calendar_partitions(
                            calendar_leaf_df,
                            {observation_date},
                            resolved_lake_root,
                        )
                    log_phase = "fetch"
                    raise click.ClickException(str(error)) from error

                if write:
                    log_phase = "commit_raw_response"
                    evidence = commit_raw_response(
                        raw_root,
                        observation_date,
                        response_content,
                    )
                    completed_at = datetime.now(timezone.utc)
                    log_phase = "apply_calendar_completion"
                    calendar_leaf_df = apply_calendar_completion(
                        calendar_leaf_df,
                        {observation_date: evidence},
                        batch_id,
                        completed_at,
                    )
                    log_phase = "calendar_commit"
                    commit_calendar_partitions(
                        calendar_leaf_df,
                        {observation_date},
                        resolved_lake_root,
                    )

                if write:
                    calendar_leaves[partition_key] = calendar_leaf_df

                processed_grid_count += 1
                total_bytes += len(response_content)
        finally:
            session.close()

        click.echo(
            f"finished: dataset={DATASET_NAME}; function=main; phase=run; status=completed; "
            f"outcome={'written' if write else 'readonly'}; processed_grids={processed_grid_count}; bytes={total_bytes}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(log_boundary)
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; date={log_date}; error={type(log_error).__name__}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(log_boundary)
        raise


# ## Notebook 与脚本执行入口
# 
# 与 a01/b01、b02 一样，Notebook 用显式 `notebook_args` 调用 Click，并以 `standalone_mode=False` 返回单元格，避免读入内核参数。默认 `[]` 不写湖，但存在 API 待办时仍会请求生意社；只有纯状态修复或已经完整时才不请求网络。
# 
# Notebook 分支要求交互内核且不存在 `__file__`，因此在内核中导入同名 Python 模块不会执行入口。直接运行 `.py` 时读取终端参数。最后一格仅保存手动终端命令注释，不在运行全部单元格时额外启动写入；正式写入使用 `--write`，不得用显式日期截断正式范围。

# ### 局部流程：Notebook 与脚本入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
#     B --> C["默认不写入；有待采日期仍请求 HTTP"]
#     A -->|否| D{"直接运行脚本？"}
#     D -->|是| E["读取终端参数并执行 main"]
#     D -->|否| F["模块导入不执行入口"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认不写入；有待采日期时仍会请求 HTTP，写入须显式修改参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b02_domestic_spot_basis",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()


# ### 局部流程：终端手动运行
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["在终端激活 latitude；切换到项目根目录"] --> B["人工运行对应 .py --write"]
#     B --> C["自动补 raw 或修复日历；逐日期和逐叶提交"]
# ```

# In[ ]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a03_External_Market_Data\b02_domestic_spot_basis.py --write

