#!/usr/bin/env python
# coding: utf-8

# # b04 外部指数日表
# 
# 消费外部市场日历中 `external_index/INDICATOR_ID` 的 required 指标—日期格点，按连续待办段请求 Eastmoney `RPT_INDUSTRY_INDEX`，生产 `fact_external_index_daily`；正式事实复读后回写日历，已退出 required 集合的旧事实另行清退。
# 
# | 上下游 | 本环节职责与依赖 |
# | --- | --- |
# | a01/b01 → a03/b01 | 自然日历与实体有效期形成完整外部市场日历；本入口消费 required 格点，不自行扩展日期宇宙。 |
# | `config/futures_lakehouse/external_market_entities.py` | 19 个来源指标 ID、项目代码、中文名、分类和有效期的唯一配置来源。 |
# | `config/data_contracts.py` | 外部市场日历与外部指数事实的字段、主键、分区和质量契约。 |
# | 事实输出 | 主键为 `index_code/observation_date`，按 `index_category/year/month` 完整叶提交。 |
# | 日历回写 | 按 `dataset_name/year/month` 完整叶提交；多个指数分类共用同月日历，须保留此前提交的其他指标状态。 |
# | operations 与读取 Demo | 默认人工批次第 15 阶段，在 a03/b03 后执行；数据库 Demo 按分类、年月只读展示事实。 |
# 
# 本入口与 a03/b02、b03 共用日历，但不读取它们的 raw 或事实。生产边界见湖仓 `AGENTS.md`、`README.md` 与数据库 `AGENTS.md`。

# ## 更新集合、清退与写入边界
# 
# 每次同时计算两个集合：当前 required 格点中尚未由事实和日历共同证明完整的格点进入 API 待办；正式事实中不属于当前 required 范围的格点进入无 API 清退。空事实表使用同一规则自然形成采集范围，不采用最大日期截断，也没有 `--full` 或独立日期水位文件。
# 
# | 情形 | 当前行为 |
# | --- | --- |
# | 正式事实计数与日历完成状态、审计字段一致 | 已完成，跳过。 |
# | required 格点尚不完整 | 进入采集；当前没有仅修复陈旧日历的独立无 API 分支。 |
# | 旧事实已退出 required 集合 | 不请求 API，从旧事实自身的分类—年月完整叶删除；纯清退不创建 HTTP 会话。 |
# | 完整分页未返回某个精确待办日期 | 正式计数确认 0 后写 `empty_confirmed + warning`，不造占位事实。 |
# | 不带 `--write` | 计算计划；有采集待办仍请求、归一化和验证，清退只预览，不写事实或日历。 |
# | 成对显式日期 | 只缩小本次范围；可只读，写入必须使用不同于正式湖的临时湖。 |
# 
# 连续段以同指标的上游 required 日期顺序定义；周末不人为拆段，夹在待办之间的已完成 required 日期会拆段。范围响应中的非待办日期仍参与分页和来源验收，但不得覆盖已完成事实。
# 
# 事实和日历先后独立提交。当前日历失败不会撤销此前成功事实；再次运行依现有完整性判定重新生成待办，不能描述成 b03 的无 API 修复。此前成功分区保留，业务循环不自动重跑失败分区；HTTP 适配器现有 `Retry(total=3)` 可能产生额外请求尝试。

# ## 总流程：required 格点采集与旧事实清退
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["参数门禁；读取上游日历和已有事实"] --> B["按格点共同判定完成；求采集与清退集合"]
# B --> C{"存在本批工作？"}
# C -->|否| Z["报告完整；结束"]
# C -->|是| D["准备分类年月计划和叶映射；有采集才建会话"]
# D --> E["当前叶：采集指标按 required 连续段完整分页"]
# E --> F["来源归一化；仅接纳精确待办日期"]
# F --> G{"启用 write？"}
# G -->|否| H["内存汇总；清退只预览"]
# G -->|是| I["完整叶合并；采集替换、失效清退、未触达保留"]
# I --> J["事实叶共享事务及正式复读；核对计数"]
# J --> K["有采集格点才独立提交对应日历叶"]
# K --> L["报告当前叶完成；此前成功叶保留"]
# L --> M{"还有事实叶？"}
# H --> M
# M -->|是| E
# M -->|否| N["关闭会话；完成证据来自当前叶验收"]
# N --> Z
# E -. 失败 .-> R["按既有边界回写失败或恢复；关闭会话并抛错"]
# F -. 失败 .-> R
# J -. 失败 .-> R
# K -. 失败 .-> R
# ```

# ## 初始化与共享依赖
# 
# 按项目三个标记文件定位根目录，导入两张权威 Schema、共享实体映射、项目设置以及 HTTP、DataFrame 和 Arrow 库。本单元格不创建 HTTP 会话、不请求来源、不写湖。
# 
# 湖仓级 `a00_04_staged_path_transaction.py` 负责路径安装与失败恢复；业务合并、数据校验及事务分组仍由本环节决定。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["按三个标记定位项目根"] --> B["导入 Schema、共享实体、设置与库"]
# B --> C["定义依赖；不创建会话、不请求、不写湖"]
# ```

# In[ ]:


from __future__ import annotations

# 标准库负责数值检查、路径切换、分区回滚和批次审计。
import math
import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timezone

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
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
from config.futures_lakehouse.external_market_entities import (
    EXTERNAL_INDEX_ENTITIES,
    ExternalIndexEntity,
)
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 交互内核且不存在 `__file__` 时，按上游日历、输出事实的顺序展示权威 Schema。已经传入 `lake_root`，显式选择样例会执行有界本地读取；不会请求 Eastmoney 或写入数据。普通脚本运行跳过展示。

# ### 局部流程：Schema 和本地样例
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A{"交互内核且没有 __file__？"} -->|是| B["展示日历、事实权威 Schema"]
# B --> C["用户显式选样例时有界读取本地湖"]
# A -->|否| D["跳过展示"]
# ```

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
        EXTERNAL_INDEX_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区和来源字段
# 
# 表名、主键及 Hive 分区从两张 Schema metadata 各读取一次。日历使用来源 `INDICATOR_ID` 定位请求实体，事实用稳定项目 `index_code` 作为业务身份；对应名称、分类和有效期均由共享配置提供。
# 
# `EASTMONEY_FIELDS` 只请求 `INDICATOR_ID/INDICATOR_VALUE/REPORT_DATE`。当前每页 500 行、总页数上限 10000；两个 Hive partitioning 采用权威字段类型，状态枚举服务于现有业务校验。

# ### 局部流程：表身份与来源映射
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["Schema metadata"] --> B["表名、主键、分区与 Hive 类型"]
# C["共享 19 个指数配置"] --> D["来源 ID 到项目身份映射"]
# B --> E["固定来源字段、分页上限和状态枚举"]
# D --> E
# ```

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


# ## 物理契约与表身份读取
# 
# `open_exact_dataset()` 检查 Dataset 和各 Parquet fragment 的字段、类型、nullable，以及表名、主键、分区身份 metadata。描述性 metadata 以当前 `config/data_contracts.py` 为准，不因说明差异重写历史。支持表根、当前叶和零行标记，叶读取通过 `partition_base_dir` 恢复 Hive 字段。
# 
# 启动只读取一次正式日历与事实；提交时直读当前叶，空事实复读根级零行标记，不再逐叶枚举正式表根。函数报告结构、fragment 检查进度和失败阶段，`materialized=false` 表示尚未读取数据行。

# ### 局部流程：物理契约与表身份
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["表根、当前叶或零行标记"] --> B["打开 Dataset；恢复 Hive 字段"]
# B --> C["字段、类型、nullable 及身份 metadata"]
# C --> D["逐 fragment 物理检查；报告进度"]
# D --> E["返回 Dataset；描述性差异采用当前契约"]
# ```

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
            else [table_path] if table_path.is_file() else []
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

        partition_columns = partitioning.schema.names
        expected_file_schema = pa.schema(
            [
                field
                for field in schema
                if field.name not in partition_columns
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


# ## dirty 日历叶业务验收
# 
# `validate_calendar_table()` 消费调用方已按权威 Schema 转换的 Arrow 表，检查主键、状态、年月、原因、0/1 计数、required 关系和审计时间，排序后返回 Arrow 表。只在每个待提交完整日历叶执行一次；上游正式日历和内存状态生成不再重复业务验收。
# 
# 原有循环仍报告扫描进度，每 10000 行检查一次 2 秒日志间隔。

# ### 局部流程：dirty 日历叶验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["已转换的 Arrow 完整叶"] --> B["主键、枚举、日期与状态关系"]
# B --> C["计数、完成凭证及审计时间"]
# C --> D["排序并返回 Arrow；每次提交只验收一次"]
# ```

# In[ ]:


# 日历状态生产者对待提交的 dirty 完整叶执行业务验收。
def validate_calendar_table(
    calendar_table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()
        log_phase = "primary_key"
        if calendar_keys_df.duplicated().any():
            raise ValueError(f"{context}外部市场日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        log_phase = "business_validation"
        for row in calendar_table.to_pylist():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=scan; status=running; "
                    f"context={context}; scanned_rows={log_scanned_rows}/{calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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

        log_phase = "sort"
        validated_calendar_table = calendar_table.sort_by([(name, "ascending") for name in CALENDAR_PRIMARY_KEY])
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_calendar_table)}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_calendar_table; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源结果与 dirty 事实叶业务验收
# 
# `validate_external_index_table()` 接收已转换的 Arrow 表，检查主键、来源身份、共享代码/名称/分类映射、日期、年月、有限指数值和审计时间，排序后返回同一表示。用于来源转换结果和提交前完整 dirty 叶，分别保证来源输出和旧新合并结果的业务质量。
# 
# 不在响应汇总、合并返回、staging 或正式复读时重复调用，不做 Pandas—Arrow 往返转换。当前日期在逐行循环前取得；扫描日志沿用原有迭代与节流。

# ### 局部流程：来源及 dirty 事实叶验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["已转换的 Arrow 表"] --> B["主键与共享身份映射"]
# B --> C["来源、日期、年月、有限值及审计时间"]
# C --> D["排序返回 Arrow；用于来源输出或 dirty 叶"]
# ```

# In[ ]:


# 事实生产者承担本表全部字段、配置映射与格点关系质检。
def validate_external_index_table(
    index_table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=validate_external_index_table; phase=validate; status=started; "
        f"context={context}; rows={index_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        index_keys_df = index_table.select(PRIMARY_KEY).to_pandas()
        log_phase = "primary_key"
        if index_keys_df.duplicated().any():
            raise ValueError(f"{context}外部指数事实主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        current_date = date.today()
        log_phase = "business_validation"
        for row in index_table.to_pylist():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=validate_external_index_table; phase=scan; status=running; "
                    f"context={context}; scanned_rows={log_scanned_rows}/{index_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
            if row["observation_date"] > current_date:
                raise ValueError(f"{context}指数观测日期不得晚于当前可见日期。")
            if row["updated_at"] > now_utc:
                raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")
            if not math.isfinite(row["index_value"]):
                raise ValueError(f"{context}指数观测值必须为有限数。")

        log_phase = "sort"
        validated_index_table = index_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_external_index_table; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_index_table)}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_index_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_external_index_table; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 可选正式事实读取
# 
# 目录不存在或没有 Parquet 时返回权威空表；否则检查物理契约并一次物化、按主键排序，信任正式提交已完成的业务验证。启动所需的全历史事实计数和清退集合仍保留，批末不再重读全表。
# 
# 函数区分空目录、实际物化、成功行数和异常。

# ### 局部流程：可选事实读取
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A{"有正式 Parquet？"} -->|否| B["权威空表"]
# A -->|是| C["物理契约检查；一次物化并排序"]
# C --> D["信任已提交业务证明；返回事实供启动对账"]
# B --> D
# ```

# In[ ]:


# 空事实目录是合法的全量起点；已有正式事实只检查物理契约并信任业务证明。
def read_optional_fact(table_path: pathlib.Path) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "read"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=started; "
        f"path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not table_path.is_dir() or next(table_path.rglob("*.parquet"), None) is None:
            empty_fact_df = empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=completed; "
                f"rows=0; outcome=no_fact_files; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df

        dataset = open_exact_dataset(
            table_path,
            FACT_PARTITIONING,
            EXTERNAL_INDEX_DAILY_SCHEMA,
            "正式外部指数事实",
        )
        log_phase = "materialize"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=materialize; status=started; "
            f"path={table_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        table = dataset.to_table(columns=EXTERNAL_INDEX_DAILY_SCHEMA.names)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=materialize; status=completed; "
            f"rows={table.num_rows}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "convert"
        validated_index_df = arrow_to_pandas(table, EXTERNAL_INDEX_DAILY_SCHEMA).sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=completed; "
            f"rows={len(validated_index_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_index_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 同指标的连续 required 待办段
# 
# `pending_request_ranges()` 按某指标上游 required 日期的顺序切分精确待办集合。已完成 required 日期隔开两个待办段，普通周末不拆段。返回每段首尾日期与精确待办日期集合，并确认各段并集覆盖全部待办。
# 
# 函数自行报告精确待办日期数、生成段数和失败阶段；结果只是一份内存请求计划。

# ### 局部流程：连续 required 待办段
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["某指标的精确待办日期"] --> B["读取首尾范围内的上游 required 日期序列"]
# B --> C["连续待办合段；遇已完成 required 日期拆段"]
# C --> D["各段并集必须覆盖全部待办"]
# D --> E["返回日期范围与每段精确集合"]
# ```

# In[ ]:


def pending_request_ranges(
    calendar_df: pd.DataFrame,
    source_indicator_id: str,
    pending_dates: set[date],
) -> list[tuple[date, date, set[date]]]:
    log_started_at = time.perf_counter()
    log_phase = "plan_ranges"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=pending_request_ranges; phase=plan_ranges; status=started; "
        f"indicator={source_indicator_id}; pending_dates={len(pending_dates)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # “连续”以该指标上游 required 格点的顺序为准，因此周末不会人为拆段。
        if not pending_dates:
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=pending_request_ranges; phase=plan_ranges; status=completed; "
                f"indicator={source_indicator_id}; pending_dates=0; ranges=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return []

        first_pending_date = min(pending_dates)
        last_pending_date = max(pending_dates)
        log_phase = "select_required"
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
        log_phase = "split_ranges"
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

        log_phase = "coverage"
        covered_dates = set().union(*(item[2] for item in ranges)) if ranges else set()
        if covered_dates != pending_dates:
            raise ValueError("待办日期未被上游 required 连续段完整覆盖。")
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=pending_request_ranges; phase=plan_ranges; status=completed; "
            f"indicator={source_indicator_id}; pending_dates={len(pending_dates)}; ranges={len(ranges)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return ranges
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=pending_request_ranges; phase=plan_ranges; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; indicator={source_indicator_id}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## HTTP 会话与现有重试策略
# 
# `create_eastmoney_session()` 创建带 GET 适配器的会话，沿用 `Retry(total=3)`，覆盖连接、读取及配置的限流/服务端错误。业务分页循环自身不重跑失败范围；页数不是实际 HTTP 尝试数。main 在 finally 中关闭会话。
# 
# 会话创建自行报告起止；`business_requests=0` 表示此处尚未发起请求。

# ### 局部流程：HTTP 会话
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["确有采集待办"] --> B["创建 Session；配置 GET Retry total=3"]
# B --> C["挂载适配器与请求头；返回会话"]
# C --> D["main 的 finally 关闭会话"]
# ```

# In[ ]:


def create_eastmoney_session() -> requests.Session:
    log_started_at = time.perf_counter()
    log_phase = "session"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=create_eastmoney_session; phase=session; status=started; "
        f"business_requests=0; retry_total=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
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
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=create_eastmoney_session; phase=session; status=completed; "
            f"business_requests=0; retry_total=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return session
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=create_eastmoney_session; phase=session; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## Eastmoney 完整分页查询
# 
# 一个指标、一个连续待办段共用请求日期范围。首页冻结 pages/count；逐页检查元数据、页长、累计条数与上限。仅首页合法空响应可返回空集合，后续页意外空或页数/总数漂移不能当成功。HTTP、JSON 与来源错误沿现有规则分类并抛出。
# 
# 函数返回完整原始记录列表；来源 ID、日期、跨页键和数值由紧随其后的归一化验收。任何部分页都不能推进事实提交。
# 
# 分页函数自行报告范围和每页起止、冻结页数、页行数与累计条数。全部分页验收通过后才报告一次 `api_success`；两种合法空响应也各自报告完成，标记 `normalized=false; persisted=false`。页数仅统计逻辑分页，不声称等于适配器内部的 HTTP 尝试次数。
# 
# 日期过滤和请求列字符串在分页循环前准备；页号与每页响应元数据仍在循环内处理。

# ### 局部流程：当前完整分页
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["指标与日期段；从首页开始"] --> B["GET 当前页；HTTP 与 JSON 检查"]
# B --> C{"合法首页空响应？"}
# C -->|是| Z["返回空记录列表"]
# C -->|否| D["冻结并核对 pages、count 和页长"]
# D --> E["累计完整页记录"]
# E --> F{"已到最后一页？"}
# F -->|否| G["页码递增"]
# G --> B
# F -->|是| H["核对累计条数；返回原始记录"]
# B -. 失败 .-> R["按既有分类抛错；不返回部分成功"]
# D -. 失败 .-> R
# ```
# 
# 每页报告起止与累计条数；完整分页成功不代表归一化或持久化。

# In[ ]:


def query_eastmoney_indicator_range(
    session: requests.Session,
    source_indicator_id: str,
    range_start: date,
    range_end: date,
) -> list[dict[str, object]]:
    log_started_at = time.perf_counter()
    log_phase = "fetch"
    log_request_page = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=fetch; status=started; "
        f"indicator={source_indicator_id}; range={range_start}/{range_end}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        page_number = 1
        expected_pages = None
        expected_count = None
        response_rows = []
        date_filter = f"""(INDICATOR_ID="{source_indicator_id}")(REPORT_DATE>='{range_start.isoformat()}')(REPORT_DATE<='{range_end.isoformat()}')"""
        request_columns = ",".join(EASTMONEY_FIELDS)

        while True:
            log_request_page = page_number
            params = {
                "reportName": EASTMONEY_REPORT_NAME,
                "columns": request_columns,
                "filter": date_filter,
                "pageNumber": page_number,
                "pageSize": EASTMONEY_PAGE_SIZE,
                "sortColumns": "REPORT_DATE",
                "sortTypes": "1",
                "source": "WEB",
                "client": "WEB",
            }

            try:
                log_phase = "request"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=page; status=started; "
                    f"indicator={source_indicator_id}; range={range_start}/{range_end}; page={page_number}; total_pages={expected_pages if expected_pages is not None else 'unknown'}; received_rows={len(response_rows)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
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
                log_phase = "json"
                payload = response.json()
            except ValueError as error:
                raise RuntimeError(
                    "retryable_error: Eastmoney 未返回有效 JSON；"
                    f"indicator={source_indicator_id}; page={page_number}。"
                ) from error
            log_phase = "response_metadata"
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
                    click.echo(
                        f"api_success: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=fetch; status=completed; "
                        f"indicator={source_indicator_id}; range={range_start}/{range_end}; pages_received={page_number}; rows=0; outcome=empty_response; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                click.echo(
                    f"api_success: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=fetch; status=completed; "
                    f"indicator={source_indicator_id}; range={range_start}/{range_end}; pages_received={page_number}; rows=0; outcome=empty_response; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
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
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=page; status=completed; "
                f"indicator={source_indicator_id}; range={range_start}/{range_end}; page={page_number}/{expected_pages}; page_rows={len(data_rows)}; received_rows={len(response_rows)}/{expected_count}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            if page_number == expected_pages:
                break
            page_number += 1

        log_phase = "total_count"
        if len(response_rows) != expected_count:
            raise RuntimeError("retryable_error: Eastmoney 累计行数与 count 不一致。")
        click.echo(
            f"api_success: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=fetch; status=completed; "
            f"indicator={source_indicator_id}; range={range_start}/{range_end}; pages_received={page_number}; rows={len(response_rows)}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return response_rows
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=query_eastmoney_indicator_range; phase=fetch; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; indicator={source_indicator_id}; range={range_start}/{range_end}; page={log_request_page}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 分页响应归一化与精确待办筛选
# 
# 逐条检查必需字段、请求指标 ID、日期范围、跨页指标—日期唯一性和有限数值；空值、布尔值、非法数值继续失败。先验证完整范围响应，再仅保留精确待办日期，避免覆盖已完整事实。映射项目代码/名称/分类并形成权威事实，最后完整验收返回；空响应不生成占位行。
# 
# 归一化独立报告来源条数、精确待办数、接受条数与失败位置；完成使用 `api_result`、`normalized=true; persisted=false`，不能当作事实已落盘。原有逐行循环每 10000 行检查一次 2 秒进度间隔。
# 
# 必需来源字段集合与当前可见日期在逐条循环前准备；转换后业务验收返回 Arrow，再转换一次 Pandas 供内存合并。

# ### 局部流程：来源归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["完整分页记录"] --> B["字段、请求指标、日期范围和跨页唯一性"]
# B --> C["指数值非空、非布尔且有限"]
# C --> D["只保留精确待办日期；其余不覆盖"]
# D --> E["映射项目身份；构造事实或权威空表"]
# E --> F["完整业务验收；返回内存结果"]
# ```
# 
# api_result 只表示归一化完成，persisted=false。

# In[ ]:


def normalize_external_index_response(
    response_rows: list[dict[str, object]],
    entity: ExternalIndexEntity,
    range_start: date,
    range_end: date,
    pending_dates: set[date],
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "normalize"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=normalize_external_index_response; phase=normalize; status=started; "
        f"indicator={entity.source_indicator_id}; range={range_start}/{range_end}; source_rows={len(response_rows)}; pending_grids={len(pending_dates)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        rows = []
        seen_grids = set()
        required_fields = set(EASTMONEY_FIELDS)
        current_date = date.today()

        log_phase = "source_rows"
        for item in response_rows:
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=normalize_external_index_response; phase=scan; status=running; "
                    f"indicator={entity.source_indicator_id}; scanned_rows={log_scanned_rows}/{len(response_rows)}; accepted_rows={len(rows)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            if not isinstance(item, dict):
                raise ValueError("permanent_error: Eastmoney data 元素必须为对象。")
            missing_fields = required_fields - item.keys()
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
            if observation_date > current_date:
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

        log_phase = "build_fact"
        frame = (
            pd.DataFrame(rows, columns=EXTERNAL_INDEX_DAILY_SCHEMA.names)
            if rows
            else empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
        )
        log_phase = "output_validation"
        normalized_index_table = validate_external_index_table(
            pandas_to_arrow(frame, EXTERNAL_INDEX_DAILY_SCHEMA), "Eastmoney 完整分页响应转换后的",
        )
        normalized_index_df = arrow_to_pandas(normalized_index_table, EXTERNAL_INDEX_DAILY_SCHEMA)
        click.echo(
            f"api_result: dataset={DATASET_NAME}; function=normalize_external_index_response; phase=normalize; status=completed; "
            f"indicator={entity.source_indicator_id}; range={range_start}/{range_end}; source_rows={len(response_rows)}; pending_grids={len(pending_dates)}; rows={len(normalized_index_df)}; normalized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return normalized_index_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=normalize_external_index_response; phase=normalize; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; indicator={entity.source_indicator_id}; scanned_rows={log_scanned_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 已完成、采集待办与无 API 清退
# 
# `external_index_reconciliation()` 选择本数据集当前 required 格点及可选日期范围，按来源指标—日期计算事实计数。日历完成标记、0/1 计数、成功或确认空状态、质量、批次和时间共同满足时才扣除；其余 required 格点全部进入采集待办。
# 
# 正式事实中不再属于该 required 集合的格点进入清退计划，沿用旧事实自身的 `index_category/year/month` 定位，不能按新配置重算旧分区。范围模式只清退范围内旧事实。当前不存在独立的日历状态修复集合，不能照搬境外期货 b03 的三类规划。
# 
# 对账函数报告已完成、API 待办和清退数量；利用原分类循环每 1000 个格点检查一次 2 秒进度间隔。短小的计数映射和单格点完整性判断不逐项刷日志。
# 
# 规划直接消费已契约化的日历记录，不为分类再次转成 Arrow；整批只执行一次，提交后的完成证据由各叶正式逐值复读及计数核对提供。

# ### 局部流程：采集与清退对账
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["required 日历格点与正式事实；可选范围"] --> B["逐格点核对状态和正式 0/1 计数"]
# B --> C{"共同证明完整？"}
# C -->|是| D["已完成跳过"]
# C -->|否| E["采集待办；没有独立状态修复分支"]
# A --> F["正式事实减去当前 required 集合"]
# F --> G["清退计划；保留旧事实原分区坐标"]
# D --> H["返回待采、清退和完成数"]
# E --> H
# G --> H
# ```

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
    log_started_at = time.perf_counter()
    log_phase = "plan"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=external_index_reconciliation; phase=plan; status=started; "
        f"calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        relevant_mask = (
            calendar_df["dataset_name"].eq(DATASET_NAME)
            & calendar_df["is_fetch_required"].eq(True)
        )
        if start_date is not None:
            relevant_mask &= calendar_df["observation_date"].ge(start_date)
            relevant_mask &= calendar_df["observation_date"].le(end_date)
        relevant_df = calendar_df.loc[relevant_mask].copy()

        log_phase = "calendar_records"
        calendar_rows = relevant_df.to_dict("records")
        log_phase = "fact_counts"
        fact_counts = grid_count_map(fact_df)
        pending_rows = []
        complete_count = 0
        required_grid_keys = set()

        log_phase = "classify_required"
        for row in calendar_rows:
            log_scanned_rows += 1
            if log_scanned_rows % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=external_index_reconciliation; phase=scan; status=running; "
                    f"scanned_grids={log_scanned_rows}/{len(calendar_rows)}; complete_grids={complete_count}; pending_grids={len(pending_rows)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
        log_phase = "obsolete_facts"
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

        click.echo(
            f"reconciliation_plan: dataset={DATASET_NAME}; function=external_index_reconciliation; phase=plan; status=completed; "
            f"complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; obsolete_fact_grid_count={len(obsolete_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return pending_df, obsolete_df, complete_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=external_index_reconciliation; phase=plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; scanned_grids={log_scanned_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 当前完整事实叶合并
# 
# 调用方传入当前分类—年月旧事实叶。函数保留未触达格点，删除本次采集和清退触达的旧行，再拼接已归一化来源结果；确认空与清退不造新行。结果为空时返回权威空表。
# 
# 合并只报告 `persisted=false` 的内存结果，完整业务验收交给提交函数一次执行。主循环前建立事实叶映射，当前叶内不再筛选整张历史事实。

# ### 局部流程：当前事实叶合并
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["当前旧叶和已归一化响应"] --> B["保留未触达；移除采集与清退格点旧行"]
# B --> C["拼接新行；全空则权威空表"]
# C --> D["返回内存完整叶；提交函数业务验收"]
# ```

# In[ ]:


# 触达指标—日期整体替换，未触达格点在完整分类—年月分区中原样保留。
def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=full_fact_partition; phase=generate; status=started; "
        f"partition={partition_key}; existing_rows={len(existing_df)}; incoming_rows={len(incoming_df)}; touched_grids={len(touched_grids)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:

        existing_partition_df = existing_df
        existing_grid_keys = existing_partition_df[GRID_COLUMNS].apply(
            tuple,
            axis=1,
        )
        log_phase = "retain_untouched"
        retained_df = existing_partition_df.loc[
            ~existing_grid_keys.isin(touched_grids),
            EXTERNAL_INDEX_DAILY_SCHEMA.names,
        ]
        log_phase = "merge"
        complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
        if complete_df.empty:
            complete_df = empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
        log_phase = "merge_ready"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=full_fact_partition; phase=generate; status=completed; "
            f"partition={partition_key}; retained_rows={len(retained_df)}; incoming_rows={len(incoming_df)}; complete_rows={len(complete_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return complete_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=full_fact_partition; phase=generate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## staging 事实叶过滤表达式
# 
# `fact_partition_expression()` 按权威分区列构造当前分类—年月过滤条件，只选择 staging 当前叶。正式复读直接打开目标叶或零行标记，不再通过表根过滤当前叶。

# ### 局部流程：staging 事实叶过滤
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["当前分类年月和权威分区列"] --> B["构造 Arrow 相等条件"]
# B --> C["用于 staging 当前叶；正式路径直接读叶"]
# ```

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


# ## 当前事实叶与新增标记共同提交
# 
# 完整 dirty 叶转换为 Arrow 并执行业务验收一次，确认内容没有越出指定分区。写 staging 和零行标记后只复读物理契约、排序并与期望 Arrow 表逐值比较；不再转换回 Pandas 做第二次业务校验。
# 
# 每个事实叶使用一个 `StagedPathTransaction`。已有根标记保持原样；缺失标记与当前叶共同恢复。非空结果安装新叶，空结果显式删除旧叶。事务内直读正式叶，空结果确认叶不存在并复读零行标记；新增标记单独验收零行契约。完整逐值比较成功后才转换一次 Pandas 返回，供后续计数和日历回写。
# 
# 首次备份失败保留原目标，失败按实际移动恢复；失败新叶隔离留存，新增标记回退移除，恢复不完整另保留旧备份，staging 清理。成功退出事务及当前清理后才报告 `partition_committed; calendar_state=not_updated`；此前成功叶保留。

# ### 局部流程：当前事实叶事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["dirty 完整叶转换；业务验收一次"] --> B["写 staging；物理契约和逐值复读"]
# B --> C["共享事务：新增标记及当前叶替换或删除"]
# C --> D["直读正式叶或零行标记；逐值比较"]
# D --> E["成功退出及清理；事实已提交；日历未更新"]
# C -. 失败 .-> R["按实际移动恢复；新增标记移除"]
# D -. 失败 .-> R
# R --> S["失败新叶隔离；恢复不全保留备份；抛错"]
# ```

# In[ ]:


# 每次只提交一个完整指数分类—年—月事实叶分区，并保留可回滚旧分区。
def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit; status=started; "
        f"partition={partition_key}; rows={len(frame)}; scope=fact_leaf; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "validate_leaf"
        complete_table = validate_external_index_table(
            pandas_to_arrow(frame.loc[:, EXTERNAL_INDEX_DAILY_SCHEMA.names], EXTERNAL_INDEX_DAILY_SCHEMA),
            "待提交完整外部指数分区",
        )
        fact_columns = EXTERNAL_INDEX_DAILY_SCHEMA.names
        fact_sort_keys = [(name, "ascending") for name in PRIMARY_KEY]
        if complete_table.num_rows:
            actual_keys = set(zip(*(complete_table[column].to_pylist() for column in PARTITION_COLUMNS), strict=True))
            if actual_keys != {partition_key}:
                raise ValueError("待提交外部指数内容越出指定 Hive 叶分区。")
        log_phase = "paths"

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
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=staging; status=started; "
                f"partition={partition_key}; rows={complete_table.num_rows}; scope=fact_leaf; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
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
            log_phase = "staging_readback"
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
            staged_table = validate_arrow_table(staged_table, EXTERNAL_INDEX_DAILY_SCHEMA).sort_by(fact_sort_keys)
            if not staged_table.equals(complete_table):
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
        target_marker_path = target_path / "schema.parquet"
        staging_marker_path = staging_path / "schema.parquet"
        marker_created = not target_marker_path.exists()

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=install; status=started; "
                f"partition={partition_key}; scope=fact_leaf; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"dataset={DATASET_NAME}; function=commit_complete_fact_partition; partition={partition_key}",
            ) as transaction:
                if marker_created:
                    transaction.replace(
                        target_path=target_marker_path,
                        staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if complete_table.num_rows else None,
                )

                # 正式路径复读成功是日历可以推进完成水位的前提。
                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=formal_readback; status=started; "
                    f"partition={partition_key}; scope=fact_leaf; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                if complete_table.num_rows == 0 and destination_path.exists():
                    raise ValueError("确认空的正式事实叶仍然存在。")
                committed_dataset = open_exact_dataset(
                    destination_path if complete_table.num_rows else target_marker_path,
                    FACT_PARTITIONING, EXTERNAL_INDEX_DAILY_SCHEMA, "正式外部指数事实",
                    partition_base_dir=target_path,
                )
                if marker_created and complete_table.num_rows:
                    committed_marker_dataset = open_exact_dataset(
                        target_marker_path, FACT_PARTITIONING, EXTERNAL_INDEX_DAILY_SCHEMA,
                        "新建正式事实零行标记", partition_base_dir=target_path,
                    )
                    if committed_marker_dataset.count_rows() != 0:
                        raise ValueError("新建正式事实标记必须为零行。")
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=fact_columns), EXTERNAL_INDEX_DAILY_SCHEMA,
                ).sort_by(fact_sort_keys)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式外部指数完整分区内容检查失败。")
                committed_df = arrow_to_pandas(committed_table, EXTERNAL_INDEX_DAILY_SCHEMA)
        finally:
            shutil.rmtree(staging_path, ignore_errors=True)
        click.echo(
            f"partition_committed: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit_leaf; status=completed; "
            f"partition={partition_key}; rows={len(committed_df)}; calendar_state=not_updated; scope=fact_leaf; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 当前日历叶的完成与失败状态生成
# 
# `apply_calendar_completion()` 根据正式事实 0/1 计数生成成功或确认空状态；`apply_calendar_failure()` 仅更新失败请求段的精确待办格点，保留已有事实计数，记录未完成及失败原因。两者只修改传入的当前日历叶，返回 `persisted=false` 内存结果，不做完整业务复验；验收统一由 dirty 叶提交承担。
# 
# 请求段失败不部分提交当前事实叶；此前成功叶保留。主循环维护已更新日历叶映射，因此多个分类在同月回写时保留彼此前序状态。进度计数复用原迭代，不增加扫描。

# ### 局部流程：当前日历叶状态生成
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A{"正式计数或请求失败？"} -->|计数| B["按 0 或 1 生成确认空或成功"]
# A -->|失败| C["保留当前叶事实计数；精确格点记未完成"]
# B --> D["更新当前日历叶；保留同月其他指标"]
# C --> D
# D --> E["返回内存结果；dirty 叶提交统一验收"]
# ```

# In[ ]:


# 只有本批已正式复读的指标—日期才写入完成或确认空状态。
def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_results: dict[tuple[str, date], int],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    log_updated_grids = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=started; "
        f"rows={len(calendar_df)}; planned_grids={len(grid_results)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        updated_df = calendar_df.copy()

        log_phase = "update_state"
        for index, row in updated_df.iterrows():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=scan; status=running; "
                    f"scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
            log_updated_grids += 1

        log_phase = "state_ready"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=completed; "
            f"updated_grids={log_updated_grids}; rows={len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


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
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    log_updated_grids = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=started; "
        f"rows={len(calendar_df)}; planned_grids={len(failed_grids)}; fetch_status={fetch_status}; fetch_completed=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if fetch_status not in {"retryable_error", "permanent_error"}:
            raise ValueError("失败状态不在允许枚举中。")
        updated_df = calendar_df.copy()

        log_phase = "update_state"
        for index, row in updated_df.iterrows():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=scan; status=running; "
                    f"scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
            log_updated_grids += 1

        log_phase = "state_ready"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=completed; "
            f"updated_grids={log_updated_grids}; rows={len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 日历完整叶独立提交
# 
# 精确触达格点必须与日历一一对应。循环前准备分区索引、列名、排序键和正式根路径，再取每个完整叶转换并业务验收一次。staging 直接写当前叶，不生成不会安装的临时零行标记；staging 和正式路径均直读当前叶，检查物理契约并逐值比较 Arrow 表。
# 
# 每个日历叶独立使用共享事务，正式验收在事务内。此前成功事实和日历叶保留；已有日历根标记不替换。失败新叶隔离留存，恢复不完整另保留备份，staging 清理。日历失败不会撤销事实，下一次仍可能重新请求 API。
# 
# 叶成功退出事务及当前清理后报告提交；全部触达叶成功后报告完成格点数和 `date_watermark=none`。错误状态落盘不表示采集完成。没有跨表原子可见性、强杀后的自动恢复或并发写入协调。

# ### 局部流程：日历独立叶事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["触达格点一一对应；循环前准备分组索引"] --> B["当前完整叶转换；业务验收一次"]
# B --> C["staging 直读当前叶；物理和逐值比较"]
# C --> D["共享事务内安装并直读正式叶；逐值比较"]
# D --> E{"成功退出；还有叶？"}
# E -->|是| B
# E -->|否| F["报告已完成格点；无独立日期水位"]
# D -. 失败 .-> R["恢复当前叶；此前成功叶保留；抛错"]
# ```

# In[ ]:


# 日历按 dataset—年—月完整提交，并保留同月其他请求实体。
def commit_calendar_partitions(
    calendar_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "commit"
    log_partition = None
    log_committed_partitions = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=started; "
        f"touched_grids={len(touched_grids)}; scope=calendar_leaves; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not touched_grids:
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
                f"touched_grids=0; committed_partitions=0; outcome=no_work; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        log_phase = "locate_touched"
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
        partition_keys = set(touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None))
        calendar_indices_by_partition = calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False).indices
        calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
        calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME

        for partition_key in sorted(partition_keys):
            log_partition = partition_key
            log_phase = "select_leaf"
            log_phase = "validate_leaf"
            complete_table = validate_calendar_table(
                pandas_to_arrow(calendar_df.iloc[calendar_indices_by_partition[partition_key]].loc[:, calendar_columns], EXTERNAL_MARKET_CALENDAR_SCHEMA),
                "待提交的完整外部市场日历分区",
            )
            log_phase = "paths"

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
                log_phase = "staging_write"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging; status=started; "
                    f"partition={partition_key}; rows={complete_table.num_rows}; scope=calendar_leaf; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                    staging_path / relative_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    "外部市场日历 staging", partition_base_dir=staging_path,
                )
                staged_table = validate_arrow_table(
                    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ).sort_by(calendar_sort_keys)
                if not staged_table.equals(complete_table):
                    raise ValueError("外部市场日历 staging 内容检查失败。")
            except Exception:
                # 尚未移动正式分区时失败，只需删除本次 staging。
                shutil.rmtree(staging_path, ignore_errors=True)
                raise

            source_path = staging_path / relative_path
            destination_path = target_path / relative_path

            try:
                log_phase = "install"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=install; status=started; "
                    f"partition={partition_key}; scope=calendar_leaf; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                with StagedPathTransaction(
                    root_path=target_path,
                    staging_dir=staging_path,
                    backup_dir=backup_path,
                    quarantine_dir=quarantine_path,
                    log_context=f"dataset={DATASET_NAME}; function=commit_calendar_partitions; partition={partition_key}",
                ) as transaction:
                    transaction.replace(target_path=destination_path, staged_path=source_path)

                    log_phase = "formal_readback"
                    click.echo(
                        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=started; "
                        f"partition={partition_key}; scope=calendar_leaf; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    committed_dataset = open_exact_dataset(
                        destination_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA,
                        "正式外部市场日历", partition_base_dir=target_path,
                    )
                    committed_table = validate_arrow_table(
                        committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    ).sort_by(calendar_sort_keys)
                    if not committed_table.equals(complete_table):
                        raise ValueError("正式外部市场日历分区内容检查失败。")
            finally:
                shutil.rmtree(staging_path, ignore_errors=True)
            log_committed_partitions += 1
            click.echo(
                f"partition_committed: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit_leaf; status=completed; "
                f"partition={partition_key}; rows={committed_table.num_rows}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; scope=calendar_leaf; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
            f"touched_grids={len(touched_df)}; committed_partitions={log_committed_partitions}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
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


# ## CLI：一次对账与当前叶推进
# 
# 参数门禁后，只物化本数据集的正式日历并读取一次事实。启动仍按事实计数和日历状态共同判断完整格点，并生成采集与清退集合；原有完整性标准不变。循环前按分区建立事实和日历叶映射，有采集待办才创建 HTTP 会话。
# 
# 每个事实叶只使用当前旧事实叶及对应同月日历叶，按 required 连续待办段请求并归一化。响应汇总与合并不重复业务验收，统一在来源输出和 dirty 叶提交承担；事实正式逐值复读后仍核对 API 计数和清退计数，随后生成并独立提交日历。各类别共享更新后的同月日历；不逐分区重建全表，也不在批末再次全表对账。
# 
# 读取、查询、归一化、生成、提交与日历状态各自报告，main 负责批次及分区汇总；内存结果、事实提交、日历状态明确区分。函数日志记录调用耗时，main 记录整批耗时，保留 `=` 起止分隔线和异常原因链。

# ### 局部流程：一次规划与叶内推进
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["参数门禁；启动读取并对账一次"] --> B["循环前建立事实和日历叶映射"]
# B --> C["当前叶：按 required 连续段分页及归一化"]
# C --> D{"启用 write？"}
# D -->|否| E["累计内存结果"]
# D -->|是| F["合并当前事实叶；业务校验及事务提交"]
# F --> G["正式 API 计数和清退计数核对"]
# G --> H["有采集才生成并独立提交当前日历叶"]
# H --> I["更新叶映射；同月分类继承日历状态"]
# I --> J{"还有事实叶？"}
# E --> J
# J -->|是| C
# J -->|否| K["关闭会话；报告结果；不重扫全历史"]
# C -. 失败 .-> R["按 write 回写精确失败格点；停止"]
# F -. 失败 .-> S["恢复当前事务；停止"]
# H -. 失败 .-> S
# ```

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
    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_partition = "none"
    log_indicator = "none"
    log_range = "none"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n外部指数日表 / External index daily\n"
        f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; elapsed_s=0.000"
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
        calendar_path = silver_root / CALENDAR_TABLE_NAME
        fact_path = silver_root / TABLE_NAME

        log_phase = "read_calendar"
        calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
            "正式外部市场日历",
        )
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=materialize; status=started; "
            f"frame=calendar_df; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        calendar_df = arrow_to_pandas(
            calendar_dataset.to_table(columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names, filter=ds.field("dataset_name") == DATASET_NAME),
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        )
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=materialize; status=completed; "
            f"frame=calendar_df; rows={len(calendar_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "read_fact"
        fact_df = read_optional_fact(fact_path)

        # 正式事实和日历状态共同参与差集；已经越出 required 水位的事实另行清退。
        log_phase = "plan"
        pending_df, obsolete_df, complete_count = external_index_reconciliation(
            calendar_df,
            fact_df,
            requested_start,
            requested_end,
        )
        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(f"planning_progress: table={TABLE_NAME}; mode={mode}; lake_root={resolved_lake_root}; write={str(write).lower()}; function=main; dataset={DATASET_NAME}; phase=read; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}")
        if pending_df.empty and obsolete_df.empty:
            click.echo(f"planning_progress: outcome=up_to_date; pending_grid_count=0; obsolete_fact_grid_count=0; function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}")
            return

        batch_id = uuid.uuid4().hex
        work_df = pd.concat([
            pending_df.assign(work_kind="fetch"),
            obsolete_df.assign(work_kind="remove"),
        ], ignore_index=True)
        partition_groups = list(
            work_df.groupby(PARTITION_COLUMNS, sort=True)
        )
        click.echo(f"planning_progress: reconciliation_partitions={len(partition_groups)}; function=main; dataset={DATASET_NAME}; phase=partition_plan; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}")

        fact_leaves = {tuple(key): frame for key, frame in fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
        calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
        empty_fact_df = empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
        empty_calendar_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)
        total_rows = 0
        processed_grid_count = 0
        obsolete_grid_count = 0

        # 只有 fetch 待办才创建外部连接；纯越界事实清退不访问 Eastmoney。
        log_phase = "session"
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
                log_partition = partition_key
                log_indicator = "none"
                log_range = "none"
                log_phase = "partition"
                calendar_partition_key = (DATASET_NAME, partition_key[1], partition_key[2])
                calendar_leaf_df = calendar_leaves.get(calendar_partition_key, empty_calendar_df)
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
                updated_at = datetime.now(timezone.utc)
                frames = []
                pending_group_df = group_df.loc[group_df["work_kind"].eq("fetch")]
                obsolete_group_df = group_df.loc[group_df["work_kind"].eq("remove")]

                click.echo(f"partition_start: {group_number}/{len(partition_groups)}; key={partition_key}; partition_number={group_number}; total_partitions={len(partition_groups)}; fetch_grids={len(pending_group_df)}; obsolete_grids={len(obsolete_group_df)}; processed_grids={processed_grid_count}; total_grids={len(work_df)}; function=main; dataset={DATASET_NAME}; phase=partition; status=started; elapsed_s={time.perf_counter() - log_started_at:.3f}")

                # 每个 INDICATOR_ID 独立分页；同指标的稀疏待办按 required 连续段拆分。
                for source_indicator_id, indicator_df in pending_group_df.groupby(
                    "source_indicator_id",
                    sort=True,
                ):
                    log_indicator = source_indicator_id
                    entity = INDEX_ENTITY_BY_SOURCE_ID[source_indicator_id]
                    pending_dates = set(indicator_df["observation_date"].tolist())
                    log_phase = "request_ranges"
                    request_ranges = pending_request_ranges(
                        calendar_leaf_df,
                        source_indicator_id,
                        pending_dates,
                    )

                    for range_start, range_end, range_pending_dates in request_ranges:
                        log_range = f"{range_start}/{range_end}"
                        failed_grids = {
                            (source_indicator_id, observation_date)
                            for observation_date in range_pending_dates
                        }
                        try:
                            if session is None:
                                raise RuntimeError("缺少 Eastmoney HTTP 会话。")
                            log_phase = "fetch"
                            response_rows = query_eastmoney_indicator_range(
                                session,
                                source_indicator_id,
                                range_start,
                                range_end,
                            )
                            log_phase = "normalize"
                            range_df = normalize_external_index_response(
                                response_rows,
                                entity,
                                range_start,
                                range_end,
                                range_pending_dates,
                                updated_at,
                            )
                        except Exception as error:
                            log_failed_phase = log_phase
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
                                log_phase = "failure_state"
                                calendar_leaf_df = apply_calendar_failure(
                                    calendar_leaf_df,
                                    failed_grids,
                                    fetch_status,
                                    f"外部指数采集失败：{message}",
                                    batch_id,
                                    datetime.now(timezone.utc),
                                    grid_count_map(existing_fact_leaf_df),
                                )
                                log_phase = "failure_calendar_commit"
                                commit_calendar_partitions(
                                    calendar_leaf_df,
                                    failed_grids,
                                    resolved_lake_root,
                                )
                            log_phase = log_failed_phase
                            raise click.ClickException(message) from error

                        frames.append(range_df)

                log_phase = "aggregate"
                incoming_df = (
                    pd.concat(frames, ignore_index=True)
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

                log_phase = "merge"
                complete_df = full_fact_partition(
                    existing_fact_leaf_df,
                    incoming_df,
                    touched_grids,
                    partition_key,
                )
                log_phase = "fact_commit"
                committed_partition_df = commit_complete_fact_partition(
                    complete_df,
                    resolved_lake_root,
                    partition_key,
                )

                log_phase = "fact_evidence"
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
                    log_phase = "calendar_state"
                    calendar_leaf_df = apply_calendar_completion(
                        calendar_leaf_df,
                        grid_results,
                        batch_id,
                        completed_at,
                    )
                    log_phase = "calendar_commit"
                    calendar_rows = commit_calendar_partitions(
                        calendar_leaf_df,
                        pending_grids,
                        resolved_lake_root,
                    )

                # 保留本批已提交叶；同月后续分类继承已更新日历。
                fact_leaves[partition_key] = committed_partition_df
                calendar_leaves[calendar_partition_key] = calendar_leaf_df

                total_rows += len(incoming_df)
                processed_grid_count += len(group_df)
                obsolete_grid_count += len(obsolete_grids)
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=main; phase=partition_batch; status=completed; "
                    f"partition={partition_key}; calendar_rows={calendar_rows}; grids={len(group_df)}; obsolete_removed={len(obsolete_grids)}; processed_grids={processed_grid_count}/{len(work_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        finally:
            if session is not None:
                session.close()

        click.echo(f"finished: grids={processed_grid_count}; rows={total_rows}; obsolete_grids={obsolete_grid_count}; write={str(write).lower()}; function=main; dataset={DATASET_NAME}; phase=run; status=completed; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}")
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; "
            f"indicator={log_indicator}; range={log_range}; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        raise


# ## Notebook 与脚本执行入口
# 
# 与 a01/b01、b02 一样，Notebook 用显式 `notebook_args` 和 `standalone_mode=False` 执行；默认 `[]` 不写湖，有待办时仍请求 API。交互分支同时要求没有 `__file__`，在内核中导入同名 Python 模块不会触发业务。直接执行 `.py` 时正常读取终端参数。
# 
# 最后一格仅保存手动终端命令注释，运行全部单元格不会额外启动正式写入。正式自动更新使用 `--write`，不能用显式日期截断正式湖生产范围。

# ### 局部流程：Notebook 与脚本入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；默认不写入"]
# A -->|否| C{"直接执行脚本？"}
# C -->|是| D["Click 读取终端参数并运行"]
# C -->|否| E["模块导入不运行"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认执行正式湖自动 dry-run；测试写入必须显式使用非正式湖。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b04_external_index",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()


# ### 局部流程：终端手动运行
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
# A["在终端激活 latitude；切换项目根目录"] --> B["人工执行对应 Python 脚本 --write"]
# B --> C["自动计算待办与清退；独立提交事实和日历叶"]
# ```

# In[ ]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a03_External_Market_Data\b04_external_index.py --write

