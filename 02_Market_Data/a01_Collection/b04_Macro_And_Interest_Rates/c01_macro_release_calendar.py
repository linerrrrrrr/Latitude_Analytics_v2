#!/usr/bin/env python
# coding: utf-8

# # c01 宏观发布日历
# 
# 本入口生产 `dim_macro_release_calendar`，没有直接上游维度表，不读取 b01 交易日历，也不调用 Tushare 或 Eastmoney。它按共享配置生成理论观测日/报告期、项目可用日和调度状态，为 b04/c02、c03 提供事实采集格点。
# 
# | 依赖或下游 | 与本环节的关系 |
# | --- | --- |
# | `.env` / `config.settings` | 提供统一历史起点与正式湖根目录；默认终点为北京时间当前日。 |
# | `config/futures_lakehouse/macro_release_entities.py` | 25 个系列、频率及版本化可用日规则的唯一配置来源；8 个 SHIBOR 期限、17 个宏观系列。 |
# | `config/data_contracts.py` | 日历字段、主键、分区和业务契约的唯一来源。 |
# | 现有正式日历 | 提供既有格点及 c02/c03 已回写的采集、计数、质量和审计状态。 |
# | b04/c02_interest_rate | 消费 `interest_rate` required 格点，生产 SHIBOR 事实，正式复读后回写日历。 |
# | b04/c03_macro_release | 消费 `macro_release` required 格点，生产 CPI/PPI/PMI/GDP 事实，正式复读后回写日历。 |
# | operations / 数据库读取 Demo | 默认日常阶段先执行本入口、再执行 c02/c03；Demo 只读展示正式表。 |
# 
# 本入口只提交日历，不直接读取或删除两张下游事实表。下游仍将正式事实和日历状态共同用于完整性判定；已有事实完整但日历陈旧时，下游可以无 API 修复。
# 

# ## 日期含义、自动范围与写入边界
# 
# | 日期或模式 | 当前语义 |
# | --- | --- |
# | SHIBOR `report_date` | 普通周一至周五观测日，不按交易所节假日日历过滤。 |
# | 宏观 `report_date` | CPI/PPI/PMI 使用月末，GDP 使用季末；不是来源 API 的实际发布日期。 |
# | `expected_available_date` | 项目版本化规则推定的最早可用日；已到北京时间当前日才置为 required。 |
# | 默认自动范围 | 从统一起点到北京时间当前日生成完整理论表，比较新增、内部缺口、撤销和政策变化。不是 b01 的尾部增量模式。 |
# | 成对显式日期 | 与统一起点、当前日取交集后生成；范围外旧日历原样参与完整叶合并。可只读，写入必须选择非正式湖。 |
# | 不带 `--write` | 读取本地日历、生成并校验计划；不提交，也不请求 API。 |
# | 带 `--write` | 普通更新替换或删除变化完整叶；新表、完整期望为空或旧契约版本迁移分支使用整根交换。 |
# 
# 可用日规则来自共享配置：SHIBOR 当日；CPI/PPI 次月 9 日并向后顺延周末；PMI 月末，2 月使用 3 月 4 日；GDP 季末后第 16 日。required 表示应采，并不证明来源已返回数值或事实已完成。
# 
# 当前契约版本信任既有正式状态；生成时按三个政策字段逐格点比较，政策相同才继承 `STATE_COLUMNS`（包括原 `updated_at`）。来源配置变化、可用日到达等使政策不同的行使用新初始化状态，不先把配置变化判作历史文件损坏。
# 
# 物理兼容但 `schema_version` 不同的历史进入旧契约路径：只做一次当前业务规则识别，能够通过则保留逐格点继承，不能通过则不继承旧表状态、完整重新生成。旧策略不兼容的显式日期检查仍拒绝；旧契约版本的写入迁移必须使用无日期自动模式。
# 
# 表名、主键、分区和契约版本与描述性 metadata 分开处理。纯说明文字变化以当前 Schema 为准，不形成分区差异或触发历史重写；c02/c03 对本日历使用相同读取边界。新写入文件携带完整当前 metadata。
# 
# 相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../a02_Lake/AGENTS.md)。
# 

# ## 总流程：理论格点、下游状态与正式提交
# 
# ```mermaid
# flowchart TD
# A["统一起点、北京时间当前日、共享系列配置"] --> B["读取现有日历；确认兼容及继承边界"]
# B --> C["生成完整理论格点和项目可用日"]
# C --> D["政策相同继承状态；其余按规则初始化"]
# D --> E["完整表比较：新增、内部缺口、撤销、政策变化"]
# E --> F{"需要提交变化或初始化？"}
# F -->|否| Z["已经一致；结束"]
# F -->|是| G{"启用 write？"}
# G -->|否| H["只读计划；不写湖"]
# G -->|是| I["staging 写入与复读；安装变化叶或整根"]
# I --> J["事务内正式整表验收；成功退出才报告落盘"]
# I -. 失败 .-> R["共享模块恢复本批；保留失败新数据；抛错"]
# J -. 失败 .-> R
# J -. 下游另行运行 .-> K["下游 c02/c03 消费 required 格点并生产事实"]
# K -. 正式事实复读后回写 .-> L["日历完成、计数、质量及审计状态"]
# L -. 下一次运行读取 .-> B
# ```
# 

# ## 初始化与依赖
# 
# 沿用项目标记文件搜索定位根目录，导入权威 Schema、宏观配置、项目设置和共享安装/恢复模块；仅初始化，不执行采集或写入。
# 

# ### 流程：初始化与配置
# 
# ```mermaid
# flowchart TD
# A["当前工作目录向上搜索三个项目标记"] --> B{"找到项目根？"}
# B -->|否| X["报错停止"]
# B -->|是| C["设置导入路径；加载权威 Schema、配置、设置"]
# C --> D["仅初始化依赖；不请求 API、不写湖"]
# ```
# 

# In[1]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


# 从任意子目录运行时，先按项目唯一约定定位根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Market_Data/a01_Collection"))
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
    MACRO_RELEASE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
)
from config.futures_lakehouse.macro_release_entities import (
    AVAILABILITY_RULE_VERSION,
    MACRO_RELEASE_CONFIG_VERSION,
    MACRO_RELEASE_SERIES,
    MACRO_RELEASE_SERIES_BY_KEY,
    MacroReleaseSeries,
    expected_available_date as calculate_expected_available_date,
    is_valid_report_date,
)
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 交互内核且不存在 `__file__` 时，只展示本表的权威 Schema，因为它没有直接上游维度表。调用已传入 `lake_root`，显式选择样例会执行有界本地读取；不会调用 API 或写入湖仓。字段、表名、主键和分区不在展示单元格重复定义。
# 

# ### 流程：契约和样例浏览
# 
# ```mermaid
# flowchart TD
# A{"交互内核且没有 __file__？"} -->|否| Z["跳过展示"]
# A -->|是| B["展示本表权威 Schema"]
# B --> C["显式选择样例时有界读取本地数据"]
# ```
# 

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、政策字段与可继承状态
# 
# 表名、主键和 Hive 分区从具名 Schema metadata 各读取一次。主键为数据集类型—系列—报告/观测日，完整叶按 `dataset_name/year/month` 定位。
# 
# `POLICY_COLUMNS` 是可用日、是否 required 和版本化中文原因；`STATE_COLUMNS` 是下游采集完成、结果、计数、质量、批次及时间。`requirement_reason_text()` 只生成带配置/规则版本的原因文本，不决定落盘；版本文字变化也会使政策字段比较不同。
# 

# ### 流程：表身份和状态边界
# 
# ```mermaid
# flowchart TD
# A["权威 Schema metadata"] --> B["各读取一次表名、主键、分区"]
# B --> C["定义政策字段、继承字段及状态枚举"]
# C --> D["构造 Hive partitioning"]
# E["系列、可用日、required"] --> F["原因文本包含配置与规则版本"]
# ```
# 

# In[3]:


# 表名、主键和 Hive 分区是完整理论比较与完整叶提交的稳定边界。
TABLE_NAME = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 利率观测与宏观发布日历表。
PRIMARY_KEY = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集类型—系列—报告/观测日唯一标识。
PARTITION_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 数据集类型—年—月完整叶分区。

POLICY_COLUMNS = [
    "expected_available_date",  # 项目规则推定的最早可用日。
    "is_fetch_required",  # 当前是否已经进入事实采集水位。
    "requirement_reason",  # 配置和可用日规则的版本化中文说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 事实请求、提交和正式复读是否完成。
    "fetch_result_status",  # 最近一次事实采集结果。
    "is_data_missing",  # 已确认对应事实格点为空。
    "actual_record_count",  # 对应正式事实复读行数，只允许 0 或 1。
    "quality_status",  # 格点综合质检状态。
    "quality_reason",  # 质检结论中文说明。
    "fetch_run_id",  # 最近一次事实采集批次号。
    "fetch_completed_at",  # 最近一次事实完成时间。
    "quality_checked_at",  # 最近一次事实质检时间。
    "updated_at",  # 本行政策或状态最后变化时间。
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

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        MACRO_RELEASE_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


def requirement_reason_text(
    series: MacroReleaseSeries,
    available_date: date,
    is_fetch_required: bool,
) -> str:
    # 原因文本包含配置与规则版本；任一版本变化都会显式重置下游状态。
    prefix = (
        f"宏观系列配置 v{MACRO_RELEASE_CONFIG_VERSION}；"
        f"可用日规则 {AVAILABILITY_RULE_VERSION}；"
        f"{series.series_name_zh}。"
    )
    if is_fetch_required:
        return (
            f"{prefix}项目规则可用日 {available_date.isoformat()} 已到，"
            "进入事实采集水位。"
        )
    return (
        f"{prefix}项目规则可用日 {available_date.isoformat()} 尚未到，"
        "保留理论格点但不请求。"
    )


# ## Hive 字段与完整 Schema 重建
# 
# `reconstructed_schema()` 按权威列序从 Dataset 取字段，把目录补出的 Hive 字段与文件字段组合后保留 Dataset metadata。缺少权威字段立即报错；本函数不读取记录，也不证明业务数据完整。后续读取函数判断物理兼容、表身份和契约版本。
# 

# ### 流程：重建 Schema
# 
# ```mermaid
# flowchart TD
# A["Dataset Schema；权威字段顺序"] --> B["依次取出文件和 Hive 字段"]
# B --> C["组合字段并保留 Dataset metadata"]
# B -. 缺字段 .-> X["抛出 TypeError"]
# ```
# 

# In[4]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威顺序重建后再比较整表 metadata。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少权威 Schema 字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


# ## 物理字段兼容性
# 
# 逐项比较字段名顺序、类型及 nullable，不比较 metadata；返回布尔值，不读取数据记录。
# 

# ### 流程：比较物理契约
# 
# ```mermaid
# flowchart TD
# A["实际 Schema 与期望 Schema"] --> B{"字段名及顺序一致？"}
# B -->|否| F["返回 False"]
# B -->|是| C{"类型和 nullable 逐项一致？"}
# C -->|否| F
# C -->|是| T["返回 True；不比较 metadata"]
# ```
# 

# In[5]:


def physical_schema_matches(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    # metadata 迁移不得掩盖字段、类型或 nullable 的物理破坏。
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


# ## 物理结构、表身份与契约版本读取
# 
# `open_compatible_dataset()` 确认存在 Parquet 后打开 Hive Dataset。在同一次 fragment 遍历中确认字段顺序、类型、nullable、表名、主键和分区身份，并判断是否都是当前 `schema_version`；缺失身份或版本、物理不兼容直接拒绝。
# 
# 描述性 metadata 不参与历史重写决策，以当前 Schema 解释。返回 Dataset 与契约版本是否当前的标志，不物化记录。函数报告文件数、检查进度和 `materialized=false`；实际记录读取留在调用方。
# 

# ### 流程：物理兼容读取
# 
# ```mermaid
# flowchart 
# A["发现 Parquet；打开 Hive Dataset"] --> B["检查 Dataset 物理结构和表身份"]
# B --> C["单次遍历 fragment：物理结构、身份及契约版本"]
# C --> D["返回 Dataset 和版本标志；不物化记录"]
# B -. 不兼容 .-> X["拒绝；报告失败阶段"]
# C -. 缺少身份或版本 .-> X
# ```
# 

# In[6]:


def open_compatible_dataset(
    table_path: pathlib.Path,
    label: str,
) -> tuple[ds.Dataset, bool]:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_checked_fragments = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "discovery"
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else []
        )
        if not parquet_files:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase=discovery; status=completed; "
            f"label={label}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=CALENDAR_PARTITIONING,
        )
        log_phase = "schema"
        actual_schema = reconstructed_schema(dataset, MACRO_RELEASE_CALENDAR_SCHEMA)
        if not physical_schema_matches(actual_schema, MACRO_RELEASE_CALENDAR_SCHEMA):
            raise TypeError(f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。")
        identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
        if any(
            (actual_schema.metadata or {}).get(key) != MACRO_RELEASE_CALENDAR_SCHEMA.metadata[key]
            for key in identity_metadata_keys
        ):
            raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不兼容。")
        if not (actual_schema.metadata or {}).get(b"schema_version"):
            raise TypeError(f"{label}缺少契约版本 metadata。")
        schema_is_current = actual_schema.metadata[b"schema_version"] == MACRO_RELEASE_CALENDAR_SCHEMA.metadata[b"schema_version"]

        expected_file_schema = pa.schema([
            field
            for field in MACRO_RELEASE_CALENDAR_SCHEMA
            if field.name not in PARTITION_COLUMNS
        ])
        log_phase = "fragment_schema"
        for fragment in dataset.get_fragments():
            fragment_schema = fragment.physical_schema
            if not physical_schema_matches(
                fragment_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label}存在物理结构不兼容的 fragment：{fragment.path}"
                )
            if any(
                (fragment_schema.metadata or {}).get(key) != MACRO_RELEASE_CALENDAR_SCHEMA.metadata[key]
                for key in identity_metadata_keys
            ):
                raise TypeError(f"{label}存在表身份 metadata 不兼容的 fragment：{fragment.path}")
            if not (fragment_schema.metadata or {}).get(b"schema_version"):
                raise TypeError(f"{label} fragment 缺少契约版本：{fragment.path}")
            schema_is_current = schema_is_current and fragment_schema.metadata[b"schema_version"] == MACRO_RELEASE_CALENDAR_SCHEMA.metadata[b"schema_version"]
            log_checked_fragments += 1
            if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase=fragment_schema; status=running; "
                    f"label={label}; checked_fragments={log_checked_fragments}/{len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase=dataset_open; status=completed; "
            f"label={label}; checked_fragments={log_checked_fragments}; contract_current={str(schema_is_current).lower()}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset, schema_is_current
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; label={label}; checked_fragments={log_checked_fragments}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 当前契约读取
# 
# `open_exact_dataset()` 在完整物理和表身份检查后，要求所有文件均为当前契约版本，供 staging 与正式安装复读使用。这里的精确约束是物理结构、表身份及版本，不要求历史说明文字逐字相同。新文件仍由具名 Schema 写入完整当前 metadata。
# 

# ### 流程：精确读取
# 
# ```mermaid
# flowchart RL
# A["物理兼容与表身份读取"] --> B{"全部属于当前契约版本？"}
# B -->|否| X["拒绝；交由自动模式迁移"]
# B -->|是| C["返回 Dataset；描述文字采用当前 Schema"]
# ```
# 

# In[7]:


def open_exact_dataset(
    table_path: pathlib.Path,
    label: str,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        dataset, schema_is_current = open_compatible_dataset(table_path, label)
        log_phase = "contract_version"
        if not schema_is_current:
            raise TypeError(f"{label} 契约版本与当前权威契约不一致。")
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; contract_current=true; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_exact_dataset; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; label={label}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 生成结果的单次业务验收
# 
# `validate_macro_calendar_table()` 接收已经按权威 Schema 转换的 Arrow 表，检查主键、数据集和状态枚举、年月、计数、审计时间、共享系列、理论频率、项目可用日、版本化原因及状态组合。它只做本日历业务验收，不再重复调用转换契约；返回按主键排序的 Pandas 表。
# 
# 普通路径只在生成结果上执行一次完整业务校验。范围外旧行继承正式提交证明，日期范围互斥后合并；提交、staging 和正式复读不再重复业务规则。只有旧契约版本的兼容识别会额外执行一次当前规则检查，决定是否能继承旧状态。
# 
# 时间字段列表、审计字段列表和完成状态集合移到行循环前。进度利用既有遍历计数，每 10000 行检查一次 2 秒间隔；业务失败仍报具体上下文。
# 

# ### 流程：日历业务验收
# 
# ```mermaid
# flowchart TD
# A["已完成权威转换的 Arrow 表"] --> B["主键与逐行业务规则验收"]
# B --> C["检查配置频率、可用日、原因及状态组合"]
# C --> D["按主键排序返回 Pandas；不提交"]
# B -. 失败 .-> X["记录上下文与进度；抛错"]
# C -. 失败 .-> X
# ```
# 

# In[8]:


def validate_macro_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_checked_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "convert"
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)

        log_phase = "primary_key"
        if frame.duplicated(PRIMARY_KEY).any():
            raise ValueError(f"{context}宏观发布日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        log_phase = "business_validation"
        timestamp_fields = ("fetch_completed_at", "quality_checked_at")
        audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
        completed_statuses = {"success", "empty_confirmed"}
        for row in table.to_pylist():
            if row["dataset_name"] not in {"interest_rate", "macro_release"}:
                raise ValueError(f"{context}dataset_name 不在权威枚举中。")
            if row["fetch_result_status"] not in FETCH_RESULT_STATUSES:
                raise ValueError(f"{context}fetch_result_status 不在允许枚举中。")
            if row["quality_status"] not in QUALITY_STATUSES:
                raise ValueError(f"{context}quality_status 不在允许枚举中。")
            if not str(row["requirement_reason"]).strip():
                raise ValueError(f"{context}requirement_reason 不得为空。")
            if not str(row["quality_reason"]).strip():
                raise ValueError(f"{context}quality_reason 不得为空。")
            if (
                row["report_date"].year != row["year"]
                or row["report_date"].month != row["month"]
            ):
                raise ValueError(f"{context}year/month 与报告/观测日期不一致。")
            if row["actual_record_count"] not in {0, 1}:
                raise ValueError(f"{context}正式事实复读计数只允许 0 或 1。")
            if row["expected_available_date"] < row["report_date"]:
                raise ValueError(f"{context}项目可用日不得早于报告/观测日期。")
            if row["updated_at"] > now_utc:
                raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

            for timestamp_name in timestamp_fields:
                timestamp_value = row[timestamp_name]
                if timestamp_value is not None and timestamp_value > now_utc:
                    raise ValueError(
                        f"{context}{timestamp_name} 不得晚于当前 UTC 时间。"
                    )

            completed_status = row["fetch_result_status"] in completed_statuses
            if row["is_fetch_completed"] != completed_status:
                raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
            if row["is_fetch_completed"]:
                if (
                    not row["fetch_run_id"]
                    or row["fetch_completed_at"] is None
                    or row["quality_checked_at"] is None
                ):
                    raise ValueError(f"{context}完成格点缺少批次或审计时间。")
            elif row["fetch_completed_at"] is not None:
                raise ValueError(f"{context}未完成格点不得具有完成时间。")

            if row["fetch_result_status"] == "success":
                if row["actual_record_count"] != 1 or row["is_data_missing"]:
                    raise ValueError(f"{context}success 必须正式复读 1 行且不得缺失。")
            elif row["fetch_result_status"] == "empty_confirmed":
                if row["actual_record_count"] != 0 or not row["is_data_missing"]:
                    raise ValueError(f"{context}确认空必须为 0 行且标记缺失。")
            else:
                if row["actual_record_count"] != 0 or row["is_data_missing"]:
                    raise ValueError(f"{context}未完成状态不得具有事实行或缺失结论。")

            key = (row["dataset_name"], row["series_code"])
            series = MACRO_RELEASE_SERIES_BY_KEY.get(key)
            if series is None:
                raise ValueError(f"{context}系列未命中共享宏观配置：{key}")
            if not is_valid_report_date(series, row["report_date"]):
                raise ValueError(
                    f"{context}{row['series_code']} 的报告/观测日不符合理论频率。"
                )

            expected_date = calculate_expected_available_date(
                series,
                row["report_date"],
            )
            if row["expected_available_date"] != expected_date:
                raise ValueError(
                    f"{context}{row['series_code']} 的项目可用日不符合版本化规则。"
                )
            expected_reason = requirement_reason_text(
                series,
                expected_date,
                row["is_fetch_required"],
            )
            if row["requirement_reason"] != expected_reason:
                raise ValueError(
                    f"{context}{row['series_code']} 的调度原因与当前版本不一致。"
                )

            status = row["fetch_result_status"]
            if not row["is_fetch_required"]:
                if (
                    status != "not_required"
                    or row["is_fetch_completed"]
                    or row["quality_status"] != "not_applicable"
                    or any(
                        row[name] is not None
                        for name in audit_fields
                    )
                ):
                    raise ValueError(
                        f"{context}尚未到可用日格点的状态或审计值不自洽。"
                    )
            elif status == "not_required":
                raise ValueError(f"{context}已到可用日格点不得标为 not_required。")
            elif status == "pending":
                if (
                    row["quality_status"] != "pending"
                    or row["fetch_run_id"] is not None
                    or row["quality_checked_at"] is not None
                ):
                    raise ValueError(f"{context}pending 格点状态不自洽。")
            elif status == "success":
                if row["quality_status"] not in {"passed", "warning"}:
                    raise ValueError(f"{context}success 格点必须 passed 或 warning。")
            elif status == "empty_confirmed":
                if row["quality_status"] != "warning":
                    raise ValueError(f"{context}确认空格点必须记录 warning。")
            elif (
                row["quality_status"] != "failed"
                or not row["fetch_run_id"]
                or row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}失败格点缺少 failed 结论或审计值。")
            log_checked_rows += 1
            if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=running; "
                    f"context={context}; checked_rows={log_checked_rows}/{table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "sort"
        validated_macro_calendar_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=completed; "
            f"context={context}; checked_rows={log_checked_rows}; rows={len(validated_macro_calendar_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_macro_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_macro_calendar_table; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; context={context}; checked_rows={log_checked_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 理论格点生成与状态继承
# 
# 旧行直接转为记录字典并按主键索引，不再先做一次 Pandas—Arrow 往返。普通工作日、月末和季末三个日期序列在系列循环前各生成一次，25 个系列按频率复用。
# 
# 每行计算项目可用日和 required，初始化 pending 或 not_required。旧行存在且三个政策字段逐值相同，才继承全部状态及原更新时间；其余保留新状态。共享配置变动只通过这些政策字段影响继承，不额外发明采集范围。
# 
# 生成完成后仅转换一次权威 Arrow 并执行一次完整业务验收。函数自行报告逐系列、累计生成/继承/初始化行数，均为 `persisted=false`。旧契约不能通过当前规则识别时，main 传入空继承表；这一旧契约路径仍会整体重置。
# 

# ### 流程：生成理论日历
# 
# ```mermaid
# flowchart TD
# A["旧行直接按主键索引"] --> B["循环前生成三种频率的日期序列"]
# B --> C["按系列取日期；计算可用日和初始状态"]
# C --> D{"旧行存在且政策逐值相同？"}
# D -->|是| E["继承全部状态和旧更新时间"]
# D -->|否| F["保留新状态"]
# E --> G["汇总全部行；一次权威转换和业务验收"]
# F --> G
# G --> H["返回内存结果；报告生成进度"]
# ```
# 

# In[9]:


def build_expected_calendar(
    start_date: date,
    end_date: date,
    existing_df: pd.DataFrame,
    visible_date: date,
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"
    log_processed_series = 0
    log_inherited_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=started; "
        f"start_date={start_date}; end_date={end_date}; visible_date={visible_date}; series={len(MACRO_RELEASE_SERIES)}; existing_rows={len(existing_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "existing_index"
        existing_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in existing_df.to_dict(orient="records")
        }

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=existing_index; status=completed; "
            f"existing_rows={len(existing_rows_by_key)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        expected_rows = []
        if start_date <= end_date:
            report_dates_by_frequency = {
                "business_day": pd.bdate_range(start_date, end_date).date,
                "month_end": pd.date_range(start_date, end_date, freq="ME").date,
                "quarter_end": pd.date_range(start_date, end_date, freq="QE-DEC").date,
            }
            for series in MACRO_RELEASE_SERIES:
                log_phase = "series_dates"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=series_dates; status=started; "
                    f"series={series.series_code}; series_index={log_processed_series + 1}/{len(MACRO_RELEASE_SERIES)}; frequency={series.frequency}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                report_dates = report_dates_by_frequency[series.frequency]

                log_phase = "expand"
                for report_date in report_dates:
                    available_date = calculate_expected_available_date(
                        series,
                        report_date,
                    )
                    is_fetch_required = available_date <= visible_date

                    if is_fetch_required:
                        fetch_result_status = "pending"
                        quality_status = "pending"
                        quality_reason = (
                            "已进入事实采集水位，等待对应事实生产者提交并正式复读。"
                        )
                    else:
                        fetch_result_status = "not_required"
                        quality_status = "not_applicable"
                        quality_reason = (
                            "项目规则可用日尚未到；保留理论格点但当前不请求。"
                        )

                    row = {
                        "dataset_name": series.dataset_name,
                        "series_code": series.series_code,
                        "report_date": report_date,
                        "expected_available_date": available_date,
                        "is_fetch_required": is_fetch_required,
                        "requirement_reason": requirement_reason_text(
                            series,
                            available_date,
                            is_fetch_required,
                        ),
                        "is_fetch_completed": False,
                        "fetch_result_status": fetch_result_status,
                        "is_data_missing": False,
                        "actual_record_count": 0,
                        "quality_status": quality_status,
                        "quality_reason": quality_reason,
                        "fetch_run_id": None,
                        "fetch_completed_at": None,
                        "quality_checked_at": None,
                        "updated_at": updated_at,
                        "year": report_date.year,
                        "month": report_date.month,
                    }

                    key = tuple(row[name] for name in PRIMARY_KEY)
                    existing_row = existing_rows_by_key.get(key)
                    if existing_row is not None and all(
                        existing_row[name] == row[name]
                        for name in POLICY_COLUMNS
                    ):
                        for name in STATE_COLUMNS:
                            row[name] = existing_row[name]
                        log_inherited_rows += 1

                    expected_rows.append(row)
                    if len(expected_rows) % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                        click.echo(
                            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=expand; status=running; "
                            f"series={series.series_code}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                        )
                        log_last_progress_at = time.perf_counter()
                log_processed_series += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=series_dates; status=completed; "
                    f"series={series.series_code}; processed_series={log_processed_series}/{len(MACRO_RELEASE_SERIES)}; series_rows={len(report_dates)}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        log_phase = "output_frame"
        if expected_rows:
            expected_df = pd.DataFrame(
                expected_rows,
                columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
            )
        else:
            expected_df = empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)

        log_phase = "output_validation"
        expected_calendar_table = pandas_to_arrow(expected_df, MACRO_RELEASE_CALENDAR_SCHEMA)
        expected_calendar_df = validate_macro_calendar_table(expected_calendar_table, "期望")
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=completed; "
            f"processed_series={log_processed_series}; generated_rows={len(expected_calendar_df)}; inherited_rows={log_inherited_rows}; initialized_rows={len(expected_calendar_df) - log_inherited_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return expected_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; processed_series={log_processed_series}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完整内容摘要
# 
# `table_digest()` 接收已经准备好的 Arrow 表，按主键排序并用权威 Schema 重建稳定行表示，再计算 Arrow IPC 的 SHA-256。摘要覆盖全部字段值，保留用于消除切片/缓冲区表示差异的稳定重建；删除了每次求摘要前重复的 Pandas→Arrow 转换。
# 
# 期望、旧数据和复读数据都按当前 Schema 形成同一内容表示，因此纯描述性 metadata 差异不构成内容变化。
# 

# ### 流程：完整内容摘要
# 
# ```mermaid
# flowchart TD
# A["已准备的 Arrow 表"] --> B["按主键排序；当前 Schema 重建稳定表示"]
# B --> C["完整字段值写 IPC 并计算 SHA-256"]
# C --> D["返回内容摘要；不重复 Pandas 转换"]
# ```
# 

# In[10]:


def table_digest(calendar_table: pa.Table) -> str:
    ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])
    stable_table = pa.Table.from_pylist(
        ordered_table.to_pylist(),
        schema=MACRO_RELEASE_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(
        sink,
        MACRO_RELEASE_CALENDAR_SCHEMA,
    ) as writer:
        writer.write_table(stable_table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


# ## 一次分组后比较完整叶
# 
# `changed_partition_keys()` 为期望和旧表各建立一次分区位置索引、各转换一次 Arrow 表，然后遍历分区键并集。新出现或被撤销的叶、行数不同的叶直接记为变化，其余只对当前叶取行并比较完整内容摘要。
# 
# 分区循环不再对两张整表逐列构造布尔掩码，也不重复转换整表。函数用既有循环报告当前键、序号和变化数，保留约 2 秒一次的进度节奏。
# 

# ### 流程：变化完整叶
# 
# ```mermaid
# flowchart TD
# A["两表各分组一次并各准备一次 Arrow"] --> B["遍历分区键并集；查位置索引"]
# B --> C{"缺少一侧或行数不同？"}
# C -->|是| E["记录变化键"]
# C -->|否| D["仅取当前叶并比较完整摘要"]
# D --> H{"摘要相同？"}
# H -->|否| E
# H -->|是| F["跳过该叶"]
# E --> G["遍历所有分区；报告进度并返回变化键"]
# F --> G
# ```
# 

# In[11]:


def changed_partition_keys(
    expected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
) -> list[tuple[object, ...]]:
    log_started_at = time.perf_counter()
    log_phase = "compare"
    log_partition = None
    log_seen_partitions = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=started; "
        f"expected_rows={len(expected_df)}; existing_rows={len(existing_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        expected_indices_by_partition = expected_df.groupby(
            PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        existing_indices_by_partition = existing_df.groupby(
            PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        expected_calendar_table = pa.Table.from_pandas(
            expected_df, schema=MACRO_RELEASE_CALENDAR_SCHEMA, preserve_index=False,
        )
        existing_calendar_table = pa.Table.from_pandas(
            existing_df, schema=MACRO_RELEASE_CALENDAR_SCHEMA, preserve_index=False,
        )
        partition_keys = sorted(expected_indices_by_partition.keys() | existing_indices_by_partition.keys())
        changed_keys = []

        log_phase = "compare"
        for partition_key in partition_keys:
            log_partition = partition_key
            log_seen_partitions += 1
            if log_seen_partitions == 1 or time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=running; "
                    f"partition={partition_key}; partition_index={log_seen_partitions}; changed_partitions={len(changed_keys)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            expected_indices = expected_indices_by_partition.get(partition_key)
            existing_indices = existing_indices_by_partition.get(partition_key)
            if expected_indices is None or existing_indices is None:
                changed_keys.append(partition_key)
                continue
            if len(expected_indices) != len(existing_indices):
                changed_keys.append(partition_key)
                continue
            if table_digest(expected_calendar_table.take(expected_indices)) != table_digest(existing_calendar_table.take(existing_indices)):
                changed_keys.append(partition_key)

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=completed; "
            f"compared_partitions={log_seen_partitions}; changed_partitions={len(changed_keys)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return changed_keys
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; partition={log_partition}; partition_index={log_seen_partitions}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 分组复用、单次 staging 物化与共享恢复
# 
# 完整期望已经由生成结果与不相交范围外可信旧行组成。`commit_partitions()` 排序并转换一次 Arrow，循环前准备分区位置映射、叶路径、字段列表和空表；这些对象供选取变化行、摘要、staging 检查与安装共同复用，不重新验收全表业务。
# 
# staging 写入后只物化一次完整表，按分区窄列建立索引，在内存逐叶核对行数和完整内容摘要；不逐叶调用 `to_table()`，也不逐叶扫描完整期望表。强制旧契约迁移仍核对完整 staging 摘要。
# 
# 本次全部变化叶放在同一个 `StagedPathTransaction` 内依次替换或显式删除，删除使用 `staged_path=None`；应存在的暂存叶缺失仍报错。未触达叶和既有根标记保持原样。新表、完整期望为空或旧契约迁移改为在同一事务中安装整个表根。安装后的正式整表只复读一次，检查物理契约、总行数和完整内容摘要；验收必须在事务内完成。
# 
# 共享模块按实际移动记录倒序恢复；首次备份失败保留原位旧目标，一处恢复失败仍继续恢复其余目标。恢复完整时清理 staging 和备份，已安装的失败新数据保留在隔离目录；恢复不完整时另保留旧备份，staging 仍清理。暂存或事务进入失败也会清理本批 staging。临时目录沿用同一 silver 根下的 `.<表名>.staging-<run_id>`、`.backup-<run_id>` 和 `.failed-<run_id>`；整根备份/隔离位于其中的 `_root` 子目录，逐叶证据保留分区相对路径。异常报告现场路径并保留原始原因链。
# 
# 安装与正式复读阶段仍属待完成；成功退出共享事务后才报告 `calendar_state=committed; date_watermark=none; persisted=true`，不代表下游事实已完成。返回值仍为本次实际写入的变化叶行数，不计删除行。生成、数据验收、更新范围和日志仍归环节所有，共享模块只负责同一文件系统内路径安装与恢复；不提供跨目标原子可见性、进程强杀后的自动恢复或并发写入协调。
# 

# ### 流程：共享提交和整批恢复
# 
# ```mermaid
# flowchart TD
# A["完整期望排序转 Arrow；预建分区及路径映射"] --> B["选取变化行；写 staging"]
# B --> C["staging 一次物化；内存逐叶核对完整摘要"]
# C --> T["进入一个共享事务"]
# T --> D{"新表、空表或旧契约迁移？"}
# D -->|是| E["共享模块备份旧根并安装整个表根"]
# D -->|否| F["共享模块依次替换或显式删除变化叶"]
# E --> G["事务内正式整表复读一次：物理契约、行数、完整摘要"]
# F --> G
# G --> H["成功退出后报告日历已提交；无独立水位文件"]
# E -. 失败 .-> R["倒序隔离失败新目标并恢复旧目标；逐项尝试"]
# F -. 失败 .-> R
# G -. 失败 .-> R
# R --> S{"恢复完整？"}
# S -->|是| U["清理 staging 和备份；保留失败新数据；抛错"]
# S -->|否| V["清理 staging；保留旧备份与失败新数据；抛错"]
# B -. 失败 .-> X["清理 staging；正式目标未改动；抛错"]
# C -. 失败 .-> X
# T -. 进入失败 .-> X
# ```
# 

# In[12]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
    *,
    force_full_swap: bool = False,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "commit"
    log_partition = None
    log_staged_partitions = 0
    log_installed_partitions = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=started; "
        f"expected_rows={len(expected_df)}; partitions={len(partition_keys)}; force_full_swap={str(force_full_swap).lower()}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not partition_keys and not force_full_swap:
            click.echo(
                f"committed: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=skipped; "
                f"reason=no_changed_partitions; rows=0; partitions=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        log_phase = "prepare_expected"
        expected_df = expected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
        calendar_columns = MACRO_RELEASE_CALENDAR_SCHEMA.names
        expected_calendar_table = pandas_to_arrow(expected_df, MACRO_RELEASE_CALENDAR_SCHEMA)
        empty_calendar_table = expected_calendar_table.slice(0, 0)
        expected_indices_by_partition = expected_df.groupby(
            PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        partition_relative_paths = {
            partition_key: pathlib.Path(*[
                f"{name}={value}"
                for name, value in zip(PARTITION_COLUMNS, partition_key, strict=True)
            ])
            for partition_key in partition_keys
        }
        expected_digest = table_digest(expected_calendar_table)

        log_phase = "prepare_paths"
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / TABLE_NAME
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
        backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
        quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"

        for managed_path in [
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ]:
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

        silver_root.mkdir(parents=True, exist_ok=True)
        try:
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_write; status=started; "
                f"run_id={run_id}; partitions={len(partition_keys)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)
            file_schema = pa.schema(
                [
                    field
                    for field in MACRO_RELEASE_CALENDAR_SCHEMA
                    if field.name not in PARTITION_COLUMNS
                ],
                metadata=MACRO_RELEASE_CALENDAR_SCHEMA.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=file_schema),
                staging_path / "schema.parquet",
            )

            log_phase = "select_changed_rows"
            changed_indices = sorted(
                index
                for partition_key in partition_keys
                for index in expected_indices_by_partition.get(partition_key, ())
            )
            changed_calendar_table = (
                expected_calendar_table if force_full_swap
                else expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
            )
            log_phase = "staging_write"
            if changed_calendar_table.num_rows:
                ds.write_dataset(
                    changed_calendar_table,
                    staging_path,
                    format="parquet",
                    partitioning=CALENDAR_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )

            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_write; status=completed; "
                f"run_id={run_id}; rows={changed_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=started; "
                f"run_id={run_id}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                "宏观发布日历 staging",
            )
            staged_calendar_table = staged_dataset.to_table(columns=calendar_columns)
            staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas().groupby(
                PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
            ).indices
            if staged_calendar_table.num_rows != changed_calendar_table.num_rows:
                raise ValueError("staging 触达行数与完整分区计划不一致。")
            if force_full_swap and table_digest(staged_calendar_table) != expected_digest:
                raise ValueError("metadata 迁移 staging 与完整期望表不一致。")

            for partition_key in partition_keys:
                log_partition = partition_key
                expected_indices = expected_indices_by_partition.get(partition_key)
                staged_indices = staged_indices_by_partition.get(partition_key)
                expected_partition_table = (
                    expected_calendar_table.take(expected_indices)
                    if expected_indices is not None else empty_calendar_table
                )
                staged_partition_table = (
                    staged_calendar_table.take(staged_indices)
                    if staged_indices is not None else empty_calendar_table
                )
                if staged_partition_table.num_rows != expected_partition_table.num_rows:
                    raise ValueError("staging 叶分区行数与期望不一致。")
                if (
                    expected_partition_table.num_rows > 0
                    and table_digest(staged_partition_table)
                    != table_digest(expected_partition_table)
                ):
                    raise ValueError("staging 叶分区内容与期望不一致。")
                log_staged_partitions += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=running; "
                    f"run_id={run_id}; partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={staged_partition_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=completed; "
            f"run_id={run_id}; checked_partitions={log_staged_partitions}; rows={staged_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "install"
        target_had_existing = target_path.exists()
        full_swap = (
            force_full_swap
            or expected_df.empty
            or not target_had_existing
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=started; "
            f"run_id={run_id}; full_root_swap={str(full_swap).lower()}; partitions={len(partition_keys)}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
            ) as transaction:
                if full_swap:
                    transaction.replace(target_path=target_path, staged_path=staging_path)
                    log_installed_partitions = len(partition_keys)
                    click.echo(
                        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=running; "
                        f"run_id={run_id}; full_root_swap=true; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                else:
                    for partition_key in partition_keys:
                        log_partition = partition_key
                        click.echo(
                            f"partition_start: table={TABLE_NAME}; function=commit_partitions; phase=install; status=started; "
                            f"run_id={run_id}; partition={partition_key}; partition_index={log_installed_partitions + 1}/{len(partition_keys)}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                        )
                        relative_path = partition_relative_paths[partition_key]
                        source_path = staging_path / relative_path
                        destination_path = target_path / relative_path

                        should_exist = partition_key in expected_indices_by_partition
                        if should_exist != source_path.is_dir():
                            raise FileNotFoundError(
                                "staging 叶分区存在性与期望不一致："
                                f"{relative_path}"
                            )

                        # 共享模块在安装新叶前登记旧叶的实际备份，安装失败仍可恢复。
                        transaction.replace(
                            target_path=destination_path,
                            staged_path=source_path if should_exist else None,
                        )
                        log_installed_partitions += 1
                        click.echo(
                            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=running; "
                            f"run_id={run_id}; partition={partition_key}; installed_partitions={log_installed_partitions}/{len(partition_keys)}; action={'replace' if should_exist else 'delete'}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                        )

                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=formal_readback; status=started; "
                    f"run_id={run_id}; expected_rows={len(expected_df)}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_dataset = open_exact_dataset(
                    target_path,
                    "正式宏观发布日历",
                )
                committed_calendar_table = committed_dataset.to_table(columns=calendar_columns)
                if committed_calendar_table.num_rows != len(expected_df):
                    raise ValueError("正式宏观发布日历总行数与期望不一致。")
                if table_digest(committed_calendar_table) != expected_digest:
                    raise ValueError("正式宏观发布日历内容与期望不一致。")
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=formal_readback; status=completed; "
                    f"run_id={run_id}; rows={committed_calendar_table.num_rows}; pending_cleanup=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入失败也清理 staging；进入后的安装与恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=completed; "
            f"run_id={run_id}; rows={changed_calendar_table.num_rows}; partitions={len(partition_keys)}; full_root_swap={str(full_swap).lower()}; calendar_rows={committed_calendar_table.num_rows}; calendar_state=committed; date_watermark=none; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return changed_calendar_table.num_rows
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; partition={log_partition}; installed_partitions={log_installed_partitions}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：可信状态、旧契约识别和完整计划
# 
# main 保留日期配对、顺序、正式湖写入门禁及完整理论范围。读取时确认物理结构、表身份和契约版本，再物化一次正式日历；当前版本不重验既有正式业务状态。只有旧版本才做一次当前规则识别，决定能否继承，写入迁移仍限无日期自动模式。
# 
# 显式范围将已验收的范围内生成结果与范围外可信旧行拼接，不重复全表业务校验。新增、撤销和政策重置统计直接使用行字典，不再为了计数反复转 Arrow；`policy_resets` 仍仅表示三个政策字段发生变化的交集主键数。
# 
# 没有变化且表存在则报告 up_to_date；其余只读计划或调用提交。读取、生成、比较和提交函数报告自己的工作，main 只保留自己执行的物化、范围合并、计划汇总和批次结果。每个函数耗时以该次调用为起点，失败继续传播原异常。
# 

# ### 流程：主流程和日志
# 
# ```mermaid
# flowchart TD
# A["批次开始；参数与正式湖写入门禁"] --> B["读取可信旧表；仅旧契约识别兼容策略"]
# B --> C["区分范围内外；选择可继承旧行"]
# C --> D["生成理论格点；显式范围合并范围外旧行"]
# D --> E["完整内容比较；统计变化叶和格点"]
# E --> F{"已有表且无变化或迁移？"}
# F -->|是| Z["up_to_date；结束日志"]
# F -->|否| G{"启用 write？"}
# G -->|否| H["dry_run；计划未落盘"]
# G -->|是| I["提交并正式复读；committed"]
# H --> Z
# I --> Z
# B -. 异常 .-> X["记录当前阶段；继续抛出原异常；结束分隔线"]
# D -. 异常 .-> X
# I -. 异常 .-> X
# ```
# 

# In[13]:


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
    log_phase = "parameters"
    click.echo("=" * 80)
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
            raise click.BadParameter("起始日期不得晚于结束日期。")

        policy_start_date = settings.futures_data_start_date
        visible_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        if policy_start_date > visible_date:
            raise ValueError(
                "FUTURES_DATA_START_DATE 不得晚于北京时间当前日期。"
            )

        generation_start_date = max(
            policy_start_date,
            requested_start_date or policy_start_date,
        )
        generation_end_date = min(
            visible_date,
            requested_end_date or visible_date,
        )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=parameters; status=completed; "
            f"mode={'explicit' if has_explicit_dates else 'automatic'}; lake_root={resolved_lake_root}; generation_start={generation_start_date}; generation_end={generation_end_date}; visible_date={visible_date}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "read_existing"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=read_existing; status=started; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        silver_root = resolved_lake_root / "silver"
        target_path = silver_root / TABLE_NAME
        target_dataset_exists = (
            target_path.is_dir()
            and next(target_path.rglob("*.parquet"), None) is not None
        )
        metadata_upgrade_required = False
        existing_policy_is_current = True

        if target_dataset_exists:
            existing_dataset, schema_is_current = open_compatible_dataset(
                target_path,
                "现有正式宏观发布日历",
            )
            metadata_upgrade_required = not schema_is_current
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_materialize; status=started; "
                f"path={target_path}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            existing_table = existing_dataset.to_table(
                columns=MACRO_RELEASE_CALENDAR_SCHEMA.names
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_materialize; status=completed; "
                f"rows={existing_table.num_rows}; materialized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            existing_df = arrow_to_pandas(existing_table, MACRO_RELEASE_CALENDAR_SCHEMA)
            if metadata_upgrade_required:
                try:
                    validate_macro_calendar_table(
                        existing_table,
                        "现有正式当前策略",
                    )
                except ValueError:
                    existing_policy_is_current = False
        else:
            existing_df = empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)

        if has_explicit_dates and (not existing_policy_is_current or (write and metadata_upgrade_required)):
            raise click.UsageError(
                "现有宏观发布日历需要契约版本或旧策略迁移；"
                "请先移除日期参数执行一次完整自动迁移。"
            )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=read_existing; status=completed; "
            f"rows={len(existing_df)}; metadata_upgrade_required={str(metadata_upgrade_required).lower()}; existing_policy_is_current={str(existing_policy_is_current).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "scope_existing"
        if has_explicit_dates:
            in_scope_mask = (
                existing_df["report_date"].ge(requested_start_date)
                & existing_df["report_date"].le(requested_end_date)
            )
            scoped_existing_df = existing_df.loc[in_scope_mask].copy()
            outside_scope_df = existing_df.loc[~in_scope_mask].copy()
        else:
            scoped_existing_df = existing_df
            outside_scope_df = empty_pandas(
                MACRO_RELEASE_CALENDAR_SCHEMA
            )

        # 当前版本信任正式提交状态；旧契约不能通过当前策略时才整表重置。
        inheritance_existing_df = (
            scoped_existing_df
            if existing_policy_is_current
            else empty_pandas(MACRO_RELEASE_CALENDAR_SCHEMA)
        )

        log_phase = "generate_expected"
        expected_scope_df = build_expected_calendar(
            generation_start_date,
            generation_end_date,
            inheritance_existing_df,
            visible_date,
            datetime.now(timezone.utc),
        )

        if has_explicit_dates:
            log_phase = "merge_scope"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=merge_scope; status=started; "
                f"outside_scope_rows={len(outside_scope_df)}; generated_rows={len(expected_scope_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            expected_full_df = pd.concat(
                [outside_scope_df, expected_scope_df],
                ignore_index=True,
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=merge_scope; status=completed; "
                f"rows={len(expected_full_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        else:
            expected_full_df = expected_scope_df

        log_phase = "reconcile"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=reconcile; status=started; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        existing_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in existing_df.to_dict(orient="records")
        }
        expected_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in expected_full_df.to_dict(orient="records")
        }
        existing_keys = set(existing_rows_by_key)
        expected_keys = set(expected_rows_by_key)
        new_grid_count = len(expected_keys - existing_keys)
        retired_grid_count = len(existing_keys - expected_keys)
        policy_reset_count = sum(
            any(
                existing_rows_by_key[key][name]
                != expected_rows_by_key[key][name]
                for name in POLICY_COLUMNS
            )
            for key in existing_keys & expected_keys
        )

        partition_keys = changed_partition_keys(
            expected_full_df,
            existing_df,
        )
        if metadata_upgrade_required:
            partition_keys = sorted(
                set(
                    expected_full_df[PARTITION_COLUMNS].itertuples(
                        index=False,
                        name=None,
                    )
                )
                | set(
                    existing_df[PARTITION_COLUMNS].itertuples(
                        index=False,
                        name=None,
                    )
                )
            )

        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=reconcile; status=completed; mode={mode}; "
            f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; persisted=false; "
            f"expected_rows={len(expected_full_df)}; "
            f"existing_rows={len(existing_df)}; "
            f"new_grids={new_grid_count}; "
            f"retired_grids={retired_grid_count}; "
            f"policy_resets={policy_reset_count}; "
            f"changed_partitions={len(partition_keys)}; "
            f"initial_dataset_required={str(not target_dataset_exists).lower()}; "
            f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        if (
            not partition_keys
            and not metadata_upgrade_required
            and target_dataset_exists
        ):
            click.echo(
                f"up_to_date: table={TABLE_NAME}; function=main; phase=plan; status=skipped; "
                f"reason=already_current; rows={len(expected_full_df)}; persisted=false; message=宏观发布日历已经与当前理论水位和下游状态完整一致。; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
                f"write={str(write).lower()}; changed_partitions=0; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return

        if write:
            log_phase = "commit"
            commit_partitions(expected_full_df, partition_keys, resolved_lake_root, force_full_swap=metadata_upgrade_required or not target_dataset_exists)
        else:
            click.echo(
                f"dry_run: table={TABLE_NAME}; function=main; phase=commit; status=skipped; "
                f"reason=dry_run; write=false; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"write={str(write).lower()}; changed_partitions={len(partition_keys)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase={log_phase}; status=failed; "
            f"error={type(error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
    finally:
        click.echo("=" * 80)


# ## Notebook 与脚本执行入口
# 
# 与 b01/c01 使用相同的入口结构：仅在交互内核且不存在 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取内核的 `-f` 参数。这里保留本环节原有空参数默认值，生成正式湖只读计划，不调用 API。
# 
# 终端直接运行同名 `.py` 时由 Click 读取命令行参数；只有显式 `--write` 才提交，成对日期与正式湖写入门禁仍由 main 执行。普通 Python 或 Notebook 内导入同名模块均不触发执行入口或 Schema 浏览。
# 

# ### 流程：Notebook 与脚本执行入口
# 
# ```mermaid
# flowchart TD
# A{"内核已加载且没有脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
# B --> C["默认空参数：只读计划；不调用 API"]
# A -->|否| D{"直接运行脚本？"}
# D -->|是| E["Click 读取终端参数；遵守写入门禁"]
# D -->|否| F["模块导入：不执行入口"]
# ```
# 

# In[14]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="c01_macro_release_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()

