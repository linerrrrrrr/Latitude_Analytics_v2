#!/usr/bin/env python
# coding: utf-8

# # c02 SHIBOR 期限利率
# 
# 本入口生产 `fact_interest_rate_daily`，读取 b04/c01 已提交的 `dim_macro_release_calendar`，只消费 `dataset_name=interest_rate` 的 required 系列—观测日格点。事实完整性与日历状态共同决定是否需要采集；它不自行生成交易日历或扩大上游水位。
# 
# | 上下游或依赖 | 与本环节的关系 |
# | --- | --- |
# | b04/c01 宏观发布日历 | 提供 8 个 SHIBOR 期限的理论格点、项目可用日和调度状态。 |
# | 共享宏观配置 | `config/futures_lakehouse/macro_release_entities.py` 唯一定义期限与 Tushare 原列映射。 |
# | 权威数据契约 | `config/data_contracts.py` 定义日历与事实的字段、主键、分区和质量边界。 |
# | Tushare Pro | 有实际 API 待办时才创建客户端，按年月窗口调用 `pro.shibor`，只接纳精确待办格点。 |
# | 现有正式事实 | 为完整格点和无 API 日历修复提供证据；同月未触达事实原样保留。 |
# | 日历状态回写 | 事实正式复读后，回写完整 `interest_rate/year/month` 叶，供下次运行求差及上游继承。 |
# | b04/c03 宏观事实 | 使用同一日历的 `macro_release` 叶；本入口不处理它的格点或事实。 |
# | operations / 数据库读取 Demo | 默认阶段顺序为 b04/c01 → c02 → c03；Demo 只读消费正式表。 |
# 
# 事实与日历依次提交，不是一个跨表事务。日历回写失败不会撤销已经成功的事实；后续人工再次运行时，已有事实可用于无 API 状态修复。
# 

# ## 自动范围、来源结果与写入边界
# 
# 默认范围仍是上游 `interest_rate` required 格点减去正式事实与日历状态共同完整的格点。只在启动时对账一次，事实存在但状态陈旧的格点无 API 修复；没有事实也没有完整确认空凭证的格点按年月请求。事实越出当前 required 水位仍报错，不静默删除。
# 
# 成对日期只筛选本次格点，显式日期禁止写正式湖；不带 `--write` 仍可能请求 API 并转换响应，但不提交事实或日历。每月请求一次，以待办观测日的最小、最大值为边界，来源日期唯一、范围、必需列、2000 行上限、非空数值类型、有限性及 `[-100, 100]` 百分比门禁全部保留，利率不除以 100。
# 
# 来源转换结果做一次业务验收；完整响应中缺日或缺值形成 0 行预期，当前事实完整叶正式复读为 0 后才写 `empty_confirmed + warning`。非法响应记录未完成的失败状态，继续其他月份，批末抛错，不自动重试。安装或验收失败立即停止，此前成功提交保留。
# 
# 当前契约的正式历史信任生产者业务证明，读取只确认物理结构、表名、主键、分区及契约版本；纯描述性 metadata 差异使用当前说明，不重写历史。物理/身份不兼容直接拒绝；兼容旧事实版本仍须在无日期写入模式经一次当前规则验收后整根迁移，日历旧版本由上游 c01 先迁移。
# 
# 相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../a02_Lake/AGENTS.md)。
# 

# ### 流程：SHIBOR 事实与日历完成凭证
# 
# ```mermaid
# flowchart TD
# A["读取可信日历和事实；物理、身份、版本检查"] --> B{"事实为兼容旧版本？"}
# B -->|是| C["无日期写入才整根迁移；描述变化不迁移"]
# B -->|否| D["启动一次：事实计数和日历完成状态共同对账"]
# C --> D
# D --> E["预建事实、日历叶映射；已有事实无 API 修复日历"]
# E --> F["按月请求；只转换精确待办；验收来源输出"]
# F --> G{"启用 write？"}
# G -->|否| H["只读结果；不提交"]
# G -->|是| I["合并当前事实叶；dirty 业务验收一次"]
# I --> J["共享事务内安装与正式叶逐值复读"]
# J --> K["按正式计数生成当前日历叶；独立事务提交"]
# K --> L["更新已提交叶映射和计数；批末不重扫历史"]
# F -. 来源失败 .-> R["写入时回写当前叶失败状态；继续；批末抛错"]
# J -. 失败 .-> X["恢复当前目标；保留证据；停止"]
# K -. 失败 .-> X
# E --> N["无 API 待办则结束；不创建客户端"]
# ```
# 

# ## 初始化与依赖
# 
# 沿用项目根标记搜索，导入权威契约、配置、共享安装/恢复模块和库。token 只由客户端函数按需读取，不在说明或日志中输出。
# 

# ### 流程：初始化与项目依赖
# 
# ```mermaid
# flowchart TD
# A["当前目录向上搜索项目标记"] --> B{"找到根目录？"}
# B -->|是| C["导入 Schema、配置、设置与共享事务"]
# B -->|否| X["报错"]
# C --> D["定义依赖；不调用 API、不写湖"]
# ```
# 

# In[ ]:


from __future__ import annotations

import pathlib
import shutil
import sys
import time
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
from config.futures_lakehouse.macro_release_entities import MACRO_RELEASE_SERIES
from config.settings import settings
from R02_Market_Data.a01_Collection.b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 只在交互内核且未定义 `__file__` 时，按依赖顺序展示宏观发布日历和 SHIBOR 事实的权威 Schema。已经传入 `lake_root`，显式选择样例会有界读取本地湖；不调用来源 API、不写湖，也不维护第二份契约。
# 

# ### 流程：Schema 浏览与样例
# 
# ```mermaid
# flowchart TD
# A{"交互内核且无脚本文件变量？"} -->|是| B["展示上游日历与本事实契约"]
# A -->|否| Z["跳过展示"]
# B --> C["显式选择样例时有界读取本地湖"]
# ```
# 

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from R02_Market_Data.a01_Collection.b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
        INTEREST_RATE_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、来源映射与质量状态
# 
# 日历和事实各自的表名、主键、分区从具名 Schema metadata 读取一次。事实按 `series_code/observation_date` 唯一、按 `year/month` 分区；日历按 `dataset_name/series_code/report_date` 唯一，回写完整 `interest_rate/year/month` 叶。
# 
# 从共享配置选择且确认恰好 8 个 SHIBOR 系列，建立来源列双向映射及显式请求字段。成功、确认空和无 API 修复的原因文本也是当前完成判定的一部分；状态日志不能替代这些持久字段。
# 

# ### 流程：表身份和来源映射
# 
# ```mermaid
# flowchart TD
# A["两张权威 Schema 与共享系列配置"] --> B["读取表名、主键、分区；构造 Hive 规则"]
# A --> C["选择 8 个 SHIBOR 系列；建立来源列映射"]
# C --> D["显式请求字段；质量范围与持久原因文本"]
# ```
# 

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
# 同一运行期间不变的契约表示，供各月份复用。
FACT_COLUMNS = INTEREST_RATE_DAILY_SCHEMA.names
CALENDAR_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.names
FACT_SORT_KEYS = [(name, "ascending") for name in PRIMARY_KEY]
CALENDAR_SORT_KEYS = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
FACT_FILE_SCHEMA = pa.schema(
    [field for field in INTEREST_RATE_DAILY_SCHEMA if field.name not in PARTITION_COLUMNS],
    metadata=INTEREST_RATE_DAILY_SCHEMA.metadata,
)
SHIBOR_REQUEST_FIELDS = ",".join(SHIBOR_FIELDS)


# ## Hive 字段与完整 Schema 重建
# 
# 按权威顺序从 Dataset 取字段，组合文件与 Hive 分区字段并保留 Dataset metadata；缺字段报错，不读取记录。
# 

# ### 流程：Hive 字段与完整 Schema 重建
# 
# ```mermaid
# flowchart TD
# A["Dataset 与权威列序"] --> B["依次取字段；补回 Hive 列"]
# B --> C["保留 Dataset metadata；返回 Schema"]
# B -. 缺字段 .-> X["报错"]
# ```
# 

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


# ## 物理字段兼容性
# 
# 只比较字段名及顺序、类型和 nullable，返回布尔值，不判断 metadata 或业务状态。
# 

# ### 流程：物理字段兼容性
# 
# ```mermaid
# flowchart TD
# A["实际和期望 Schema"] --> B{"名称、顺序、类型及 nullable 一致？"}
# B -->|是| T["True"]
# B -->|否| F["False"]
# ```
# 

# In[ ]:


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


# ## 一次 fragment 遍历的物理与身份读取
# 
# 支持表根、完整叶及零行标记文件；读取叶时显式指定 `partition_base_dir`，从其相对 Hive 路径补回分区列。检查 Dataset 物理字段、类型、nullable 与表身份，随后在同一次 fragment 遍历中检查物理结构、表名、主键、分区和版本。纯描述性表级或字段 metadata 差异不触发重写；旧版本用返回标志表达，物理或身份错误直接拒绝。
# 
# 只打开 Dataset，不物化记录。函数自主报告发现文件数、已检查片段数及兼容结果；不再额外遍历所有片段来重复检查 metadata。
# 

# ### 流程：一次物理与身份检查
# 
# ```mermaid
# flowchart TD
# A["指定根、叶或标记文件；叶指定分区根"] --> B["打开 Dataset；检查物理结构与表身份"]
# B --> C["一次遍历 fragment：物理、身份、版本"]
# C --> D["返回 Dataset 和当前版本标志；尚未物化"]
# B -. 不兼容 .-> X["拒绝读取"]
# C -. 不兼容 .-> X
# ```
# 

# In[ ]:


def open_compatible_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
    *,
    partition_base_dir: pathlib.Path | None = None,
) -> tuple[ds.Dataset, bool]:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_table_name = CALENDAR_TABLE_NAME if schema is MACRO_RELEASE_CALENDAR_SCHEMA else TABLE_NAME
    log_checked_fragments = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "discovery"
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else [table_path] if table_path.is_file() else []
        )
        if not parquet_files:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=discovery; status=completed; "
            f"label={label}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
            partition_base_dir=partition_base_dir.as_posix() if partition_base_dir is not None else None,
        )
        log_phase = "schema"
        actual_schema = reconstructed_schema(dataset, schema)
        if not physical_schema_matches(actual_schema, schema):
            raise TypeError(f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。")
        identity_keys = (b"table_name", b"primary_key", b"partition_columns")
        if any((actual_schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_keys):
            raise TypeError(f"{label}表名、主键或分区身份与权威契约不一致。")
        is_exact = (actual_schema.metadata or {}).get(b"schema_version") == schema.metadata[b"schema_version"]

        expected_file_schema = pa.schema([
            field
            for field in schema
            if field.name not in partition_columns
        ])
        log_phase = "fragment_contract"
        for fragment in dataset.get_fragments():
            if not physical_schema_matches(
                pa.schema(list(fragment.physical_schema)),
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label}存在物理结构不兼容的 fragment：{fragment.path}"
                )
            fragment_metadata = fragment.physical_schema.metadata or {}
            if any(fragment_metadata.get(key) != schema.metadata[key] for key in identity_keys):
                raise TypeError(f"{label}存在表身份不兼容的 fragment：{fragment.path}")
            is_exact = is_exact and fragment_metadata.get(b"schema_version") == schema.metadata[b"schema_version"]
            log_checked_fragments += 1
            if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=fragment_contract; status=running; "
                    f"label={label}; checked_fragments={log_checked_fragments}/{len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=dataset_open; status=completed; "
            f"label={label}; checked_fragments={log_checked_fragments}; metadata_exact={str(is_exact).lower()}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset, is_exact
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; label={label}; checked_fragments={log_checked_fragments}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 按当前物理、身份与版本契约读取
# 
# 复用兼容读取，要求版本标志为真；日历和事实都以当前 Schema 的说明性 metadata 为准。表根用于启动读取或旧版本整根迁移，月度提交直接打开当前叶，不遍历其他月份。
# 

# ### 流程：按当前契约打开 Dataset
# 
# ```mermaid
# flowchart TD
# A["兼容读取 Dataset 和匹配标志"] --> B{"匹配当前契约？"}
# B -->|是| C["返回 Dataset；尚未读记录"]
# B -->|否| X["拒绝读取"]
# ```
# 

# In[ ]:


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
    *,
    partition_base_dir: pathlib.Path | None = None,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_table_name = CALENDAR_TABLE_NAME if schema is MACRO_RELEASE_CALENDAR_SCHEMA else TABLE_NAME
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        dataset, is_exact = open_compatible_dataset(
            table_path,
            partitioning,
            schema,
            partition_columns,
            label,
            partition_base_dir=partition_base_dir,
        )
        log_phase = "metadata"
        if not is_exact:
            raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; label={label}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 日历 dirty 完整叶的业务验收
# 
# 输入是已经由 `pandas_to_arrow()` 完成字段和类型契约转换的 Arrow 表，直接检查主键、interest_rate 类型、系列、年月、0/1 计数、updated_at 与完成状态组合，返回排序后的同一 Arrow 表示。不再在函数里转换整张 Pandas 表。
# 
# 只在日历提交前对当前 dirty 完整叶调用一次。上游日历读取、状态生成、staging 和正式复读不重复调用；上游自身的可用日规则继续由 c01 负责。
# 

# ### 流程：日历 dirty 叶业务验收
# 
# ```mermaid
# flowchart TD
# A["已经转换的当前日历叶 Arrow 表"] --> B["主键、系列、年月、计数和完成状态检查"]
# B --> C["按主键排序；返回 Arrow 表；尚未提交"]
# ```
# 

# In[ ]:


def validate_interest_calendar_table(
    table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_interest_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        calendar_keys_df = table.select(CALENDAR_PRIMARY_KEY).to_pandas()

        log_phase = "primary_key"
        if calendar_keys_df.duplicated().any():
            raise ValueError(f"{context}宏观发布日历主键不唯一。")
        if table.num_rows and not all(value == DATASET_NAME for value in table["dataset_name"].to_pylist()):
            raise ValueError(f"{context}只允许包含 interest_rate 日历行。")

        expected_series_codes = set(SERIES_CODE_TO_SOURCE_COLUMN)
        now_utc = datetime.now(timezone.utc)

        log_phase = "business_validation"
        for row in table.to_pylist():
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
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_interest_calendar_table; phase=validate; status=running; "
                    f"processed_rows={log_processed_rows}/{table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        validated_calendar_table = table.sort_by(CALENDAR_SORT_KEYS)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_interest_calendar_table; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_calendar_table)}; checked_rows={log_processed_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_interest_calendar_table; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; context={context}; checked_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源输出与事实 dirty 完整叶验收
# 
# `validate_interest_rate_table()` 直接消费已经转换的 Arrow 表，检查主键、8 期限、来源标签、有限值、百分比范围、年月和 updated_at，返回按主键排序的 Arrow 表。
# 
# 来源转换结果验收一次；事实提交前合并得到的 dirty 完整叶验收一次。当前版本 clean 历史、合并函数、staging 与正式复读不重复做这套业务检查；旧版本事实整根迁移是单独的完整业务验收边界。
# 

# ### 流程：事实业务验收
# 
# ```mermaid
# flowchart TD
# A["已经转换的来源输出或 dirty 事实叶"] --> B["主键、系列、来源、利率、年月和时间检查"]
# B --> C["排序返回 Arrow 表；复用到 staging 和比较"]
# ```
# 

# In[ ]:


def validate_interest_rate_table(
    table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=validate_interest_rate_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        fact_keys_df = table.select(PRIMARY_KEY).to_pandas()

        log_phase = "primary_key"
        if fact_keys_df.duplicated().any():
            raise ValueError(f"{context}SHIBOR 事实主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        expected_series_codes = set(SERIES_CODE_TO_SOURCE_COLUMN)
        log_phase = "business_validation"
        for row in table.to_pylist():
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
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=validate_interest_rate_table; phase=validate; status=running; "
                    f"processed_rows={log_processed_rows}/{table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        validated_fact_table = table.sort_by(FACT_SORT_KEYS)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_interest_rate_table; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_fact_table)}; checked_rows={log_processed_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_fact_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_interest_rate_table; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; context={context}; checked_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 读取可信上游日历
# 
# 启动时打开正式日历，确认物理、身份和当前版本，物化 `interest_rate` 行并按权威 Schema 转为 Pandas。信任 c01 正式提交的主键、理论格点和状态业务证明，不重新执行整表业务校验。上游缺失或旧版本仍报错，c02 不创建日历根。
# 

# ### 流程：读取上游日历
# 
# ```mermaid
# flowchart TD
# A["正式日历物理、身份与版本检查"] --> B["物化 interest_rate 行"]
# B --> C["权威 Arrow 到 Pandas 转换；信任业务证明"]
# ```
# 

# In[ ]:


def read_interest_calendar(table_path: pathlib.Path) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "read"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_interest_calendar; phase=read; status=started; "
        f"path={table_path}; dataset_name={DATASET_NAME}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        dataset = open_exact_dataset(
            table_path,
            CALENDAR_PARTITIONING,
            MACRO_RELEASE_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "正式宏观发布日历",
        )
        log_phase = "materialize"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_interest_calendar; phase=materialize; status=started; "
            f"dataset_name={DATASET_NAME}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        table = dataset.to_table(
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
            filter=ds.field("dataset_name") == DATASET_NAME,
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_interest_calendar; phase=materialize; status=completed; "
            f"rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "convert"
        calendar_df = arrow_to_pandas(table, MACRO_RELEASE_CALENDAR_SCHEMA)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_interest_calendar; phase=read; status=completed; "
            f"rows={len(calendar_df)}; materialized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_interest_calendar; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 读取可信事实与旧版本标志
# 
# 空目录返回契约化空表；非空目录检查物理字段、表身份和版本，物化一次并直接按权威 Schema 转为 Pandas，不先重建一份 Arrow 表再验收历史业务。版本标志只由 schema_version 决定，描述性变化不触发历史改写；旧版本写入迁移仍由 main 的无日期门禁控制。
# 

# ### 流程：读取事实
# 
# ```mermaid
# flowchart TD
# A{"存在 Parquet？"} -->|否| B["返回契约空表和当前版本标志"]
# A -->|是| C["物理、身份、版本检查；物化一次"]
# C --> D["直接按权威 Schema 转 Pandas；返回版本标志"]
# ```
# 

# In[ ]:


def read_optional_fact(
    table_path: pathlib.Path,
) -> tuple[pd.DataFrame, bool]:
    log_started_at = time.perf_counter()
    log_phase = "read"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=read; status=started; "
        f"path={table_path}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if (
            not table_path.is_dir()
            or next(table_path.rglob("*.parquet"), None) is None
        ):
            empty_fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=read; status=completed; "
                f"rows=0; reason=no_parquet; metadata_exact=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df, True

        log_phase = "dataset_open"
        dataset, metadata_is_exact = open_compatible_dataset(
            table_path,
            FACT_PARTITIONING,
            INTEREST_RATE_DAILY_SCHEMA,
            PARTITION_COLUMNS,
            "现有正式 SHIBOR 事实",
        )
        log_phase = "materialize"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=materialize; status=started; "
            f"persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        source_table = dataset.to_table(
            columns=INTEREST_RATE_DAILY_SCHEMA.names
        )

        # 物理兼容旧表只在内存中重新附着当前 metadata，随后必须整根升级。
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=materialize; status=completed; "
            f"rows={source_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "current_contract"
        log_phase = "convert"
        current_df = arrow_to_pandas(source_table, INTEREST_RATE_DAILY_SCHEMA)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=read; status=completed; "
            f"rows={len(current_df)}; metadata_exact={str(metadata_is_exact).lower()}; materialized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return current_df, metadata_is_exact
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 正式事实格点计数
# 
# 按事实主键统计每个期限—观测日的行数，供求差及提交后检查使用；空事实返回空映射。
# 

# ### 流程：正式事实格点计数
# 
# ```mermaid
# flowchart TD
# A{"事实为空？"} -->|是| B["空计数映射"]
# A -->|否| C["按主键分组计数"]
# C --> D["返回格点到行数的映射"]
# ```
# 

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


# ## 单格点完成凭证
# 
# required、实际计数、批次与审计时间必须齐全。1 行事实匹配 success/passed 及认可原因，0 行匹配 empty_confirmed/warning 及确认空原因；其余计数报错。此函数不请求 API、不写状态。
# 

# ### 流程：单格点完成凭证
# 
# ```mermaid
# flowchart TD
# A["required、实际计数与审计证据"] --> B{"事实计数？"}
# B -->|1| C["核对 success、passed、非缺失及认可原因"]
# B -->|0| D["核对 empty_confirmed、warning、缺失及确认空原因"]
# B -->|其他| X["报错"]
# C --> E["返回是否完整"]
# D --> E
# ```
# 

# In[ ]:


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


# ## 一次求差与无 API 修复计划
# 
# 启动时以正式事实格点计数和日历状态共同判断完整。保留事实越出 required 水位拒绝、格点最多 1 行、确认空凭证及陈旧状态修复规则。required_df 本身已经定义本次遍历范围，删除“由它生成集合后再判断同一行属于集合”的重复判断。
# 
# 得到 API 待办、修复待办和完整格点数后复用，不在修复后和批末全表重读重算。后续只在当前叶正式验收成功后累计完成数和更新叶映射。
# 

# ### 流程：自动求差与无 API 修复计划
# 
# ```mermaid
# flowchart TD
# A["选择 required 及当前日期范围"] --> B["事实键不得越出全部 required 水位"]
# B --> C["建立事实计数；逐格点判断"]
# C --> D{"日历与事实共同完整？"}
# D -->|是| E["统计完整格点"]
# D -->|否| F{"已有 1 行事实？"}
# F -->|是| G["无 API 日历修复"]
# F -->|否| H["API 待办"]
# E --> Z["返回三类计划"]
# G --> Z
# H --> Z
# ```
# 

# In[ ]:


def plan_interest_rate_grids(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    log_started_at = time.perf_counter()
    log_phase = "plan"
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=plan_interest_rate_grids; phase=plan; status=started; "
        f"calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        required_df = calendar_df.loc[
            calendar_df["is_fetch_required"]
        ].copy()
        if start_date is not None:
            required_df = required_df.loc[
                required_df["report_date"].ge(start_date)
                & required_df["report_date"].le(end_date)
            ].copy()

        log_phase = "watermark"
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

        log_phase = "fact_counts"
        fact_counts = fact_grid_count_map(fact_df)
        pending_rows = []
        repair_rows = []
        complete_count = 0

        log_phase = "compare"
        for _, row in required_df.sort_values(
            ["year", "month", "report_date", "series_code"]
        ).iterrows():
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=plan_interest_rate_grids; phase=compare; status=running; "
                    f"processed_rows={log_processed_rows}/{len(required_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            key = (row["series_code"], row["report_date"])
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
        click.echo(
            f"reconciliation_plan: table={TABLE_NAME}; function=plan_interest_rate_grids; phase=plan; status=completed; "
            f"required_grids={len(required_df)}; complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return pending_df, repair_df, complete_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=plan_interest_rate_grids; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; processed_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 请求失败分类
# 
# 异常保存结果状态和原因，供 main 回写失败日历。retryable_error 只是可由后续人工重跑处理的分类，不实现自动重试。
# 

# ### 流程：请求失败分类
# 
# ```mermaid
# flowchart TD
# A["请求失败状态与原因"] --> B["构造 ShiborRequestError"]
# B --> C["交由 main 记录；不自动重试"]
# ```
# 

# In[ ]:


class ShiborRequestError(RuntimeError):
    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


# ## 按需创建 Tushare 客户端
# 
# 仅在仍有 API 待办时调用。读取项目设置中的 token 创建客户端；缺少 token 抛出 permanent_error。本单元格只定义函数，不在定义时认证。
# 
# 日志只表示客户端构造完成，不声称网络认证或来源请求成功；不输出 token。
# 

# ### 流程：按需创建 Tushare 客户端
# 
# ```mermaid
# flowchart TD
# A["main 确认有 API 待办"] --> B{"已配置 token？"}
# B -->|否| X["抛出 permanent_error"]
# B -->|是| C["创建 Tushare Pro 客户端"]
# ```
# 

# In[ ]:


def create_tushare_client():
    log_started_at = time.perf_counter()
    log_phase = "client"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=create_tushare_client; phase=client; status=started; "
        f"source_request_sent=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        import tushare as ts

        if not settings.tushare_token:
            raise ShiborRequestError(
                "permanent_error",
                "TUSHARE_TOKEN 未配置。",
            )
        shibor_client = ts.pro_api(settings.tushare_token)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=create_tushare_client; phase=client; status=completed; "
            f"client_ready=true; source_request_sent=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return shibor_client
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=create_tushare_client; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 单个月度窗口请求
# 
# 显式传入月内日期边界和 8 期限字段，调用一次 shibor；异常按现有提示分类并保留原因链。None 或非 DataFrame 不属于成功响应。完整列、日期和数值质量由下一步转换检查。
# 
# 函数自行报告请求日期、响应行数与失败阶段；请求成功标记 normalized=false、persisted=false，不提前声明转换或提交完成。
# 

# ### 流程：单个月度窗口请求
# 
# ```mermaid
# flowchart TD
# A["日期范围与显式字段"] --> B["一次 pro.shibor 请求"]
# B --> C{"返回 DataFrame？"}
# C -->|是| D["返回原始宽表"]
# C -->|否| X["按现有规则抛出请求异常"]
# B -. 请求异常 .-> X
# ```
# 

# In[ ]:


def query_shibor_window(
    client,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "request"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=query_shibor_window; phase=request; status=started; "
        f"start_date={start_date}; end_date={end_date}; automatic_retry=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        try:
            raw_df = client.shibor(
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                fields=SHIBOR_REQUEST_FIELDS,
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

        log_phase = "response_type"
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
        click.echo(
            f"api_result: table={TABLE_NAME}; function=query_shibor_window; phase=request; status=completed; "
            f"start_date={start_date}; end_date={end_date}; response_rows={len(raw_df)}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return raw_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=query_shibor_window; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; start_date={start_date}; end_date={end_date}; automatic_retry=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源宽表到精确待办事实
# 
# 先检查必需列、行数上限、日期唯一及范围，再校验所有来源期限的非空数值；来源门禁保持不变。逐个遍历精确待办，有效值形成事实，缺日或 None/pd.NA/NaN 形成 0 行预期。计数映射在同一个遍历中逐键生成，不再重建待办集合比较覆盖。
# 
# 非空转换结果按权威 Schema 转 Arrow 并做一次业务验收，返回事实与 0/1 预期计数。函数自行报告来源、扫描和输出进度；normalized=true 仍为 persisted=false，0 行预期只有正式事实复读后才成为确认空。
# 

# ### 流程：来源宽表到精确待办事实
# 
# ```mermaid
# flowchart TD
# A["检查列、行数、日期与来源数值"] --> B["来源按日期建索引"]
# B --> C["逐个精确待办映射期限列"]
# C --> D{"有有效值？"}
# D -->|是| E["保留百分比值；生成 1 行事实"]
# D -->|否| F["记录 0 行预期；尚未确认空"]
# E --> G["验收来源转换结果；复用逐键计数"]
# F --> G
# G --> H["返回事实及格点预期计数"]
# ```
# 

# In[ ]:


def normalize_shibor_response(
    raw_df: pd.DataFrame,
    pending_df: pd.DataFrame,
    request_start_date: date,
    request_end_date: date,
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[tuple[str, date], int]]:
    log_started_at = time.perf_counter()
    log_phase = "normalize"
    log_processed_columns = 0
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=normalize_shibor_response; phase=normalize; status=started; "
        f"source_rows={len(raw_df)}; pending_grids={len(pending_df)}; start_date={request_start_date}; end_date={request_end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "columns"
        missing_columns = set(SHIBOR_FIELDS) - set(raw_df.columns)
        if missing_columns:
            raise ValueError(
                f"Tushare pro.shibor 缺少必需列：{sorted(missing_columns)}"
            )
        if len(raw_df) > 2000:
            raise ValueError("Tushare pro.shibor 返回超过官方单次 2000 行上限。")

        log_phase = "dates"
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
            log_phase = "source_values"
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
                log_processed_columns += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=normalize_shibor_response; phase=source_values; status=running; "
                    f"column={source_column}; checked_columns={log_processed_columns}/{len(SOURCE_COLUMN_TO_SERIES)}; source_rows={len(selected_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        log_phase = "source_index"
        rows_by_date = {
            row["observation_date"]: row
            for row in selected_df.to_dict("records")
        }
        fact_rows = []
        outcome_counts: dict[tuple[str, date], int] = {}

        log_phase = "exact_grids"
        for pending_row in pending_df.to_dict("records"):
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=normalize_shibor_response; phase=exact_grids; status=running; "
                    f"processed_rows={log_processed_rows}/{len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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

        log_phase = "output_validation"
        fact_df = pd.DataFrame(
            fact_rows,
            columns=INTEREST_RATE_DAILY_SCHEMA.names,
        )
        if fact_df.empty:
            fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)
        else:
            fact_table = validate_interest_rate_table(
                pandas_to_arrow(fact_df, INTEREST_RATE_DAILY_SCHEMA), "Tushare 转换",
            )
            fact_df = arrow_to_pandas(fact_table, INTEREST_RATE_DAILY_SCHEMA)

        click.echo(
            f"api_result: table={TABLE_NAME}; function=normalize_shibor_response; phase=normalize; status=completed; "
            f"source_rows={len(raw_df)}; requested_grids={len(outcome_counts)}; fact_rows={len(fact_df)}; empty_grids={len(outcome_counts) - len(fact_df)}; normalized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return fact_df, outcome_counts
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=normalize_shibor_response; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; checked_columns={log_processed_columns}; processed_grids={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 合并当前事实完整月叶
# 
# 调用方从循环前建立的 `fact_leaves` 取当前叶；函数不再从完整历史按年月筛选。仅删除本次触达主键对应的旧行，再与来源已验收的精确待办事实合并，同月未触达数据保留。
# 
# 合并只生成候选完整叶，不再重复调用业务校验；正式写入前由提交函数对这份 dirty 完整叶验收一次。只读模式仍已验收来源输出。
# 

# ### 流程：合并当前事实叶
# 
# ```mermaid
# flowchart TD
# A["当前旧叶、精确待办事实、触达主键"] --> B["确认触达月份；剔除旧叶中触达主键"]
# B --> C["合并来源事实；保留同月其他行"]
# C --> D["返回候选完整叶；提交前业务验收"]
# ```
# 

# In[ ]:


def full_fact_partition(
    existing_fact_df: pd.DataFrame,
    incoming_fact_df: pd.DataFrame,
    touched_grids: set[tuple[str, date]],
    partition_key: tuple[int, int],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate; status=started; "
        f"partition={partition_key}; existing_rows={len(existing_fact_df)}; incoming_rows={len(incoming_fact_df)}; touched_grids={len(touched_grids)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "select_existing"
        partition_df = existing_fact_df

        log_phase = "partition_scope"
        for series_code, observation_date in touched_grids:
            if (observation_date.year, observation_date.month) != partition_key:
                raise ValueError("触达 SHIBOR 格点越出指定事实叶分区。")

        log_phase = "replace_touched"
        if not partition_df.empty:
            existing_keys = pd.MultiIndex.from_frame(partition_df[PRIMARY_KEY])
            touched_index = pd.MultiIndex.from_tuples(
                sorted(touched_grids),
                names=PRIMARY_KEY,
            )
            partition_df = partition_df.loc[
                ~existing_keys.isin(touched_index)
            ].copy()

        log_phase = "merge"
        merged_df = pd.concat(
            [partition_df, incoming_fact_df],
            ignore_index=True,
        )
        if merged_df.empty:
            empty_fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate; status=completed; "
                f"partition={partition_key}; rows=0; touched_grids={len(touched_grids)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df
        log_phase = "validate_output"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate; status=completed; "
            f"partition={partition_key}; rows={len(merged_df)}; touched_grids={len(touched_grids)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return merged_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 复用 Arrow 的事实暂存
# 
# 接收提交函数已经转换并完成质量验收的 Arrow 表，直接写零行标记和非空事实叶；不再次执行 Pandas 转 Arrow。文件 Schema、列序、排序键、请求字段字符串等稳定对象在模块初始化时准备，各月份复用。
# 

# ### 流程：事实暂存
# 
# ```mermaid
# flowchart TD
# A["已通过业务验收的 Arrow 表"] --> B["写零行标记；复用文件 Schema"]
# B --> C{"有数据？"}
# C -->|是| D["直接写当前事实叶"]
# C -->|否| E["只保留空标记"]
# D --> F["报告暂存完成；未落盘"]
# E --> F
# ```
# 

# In[ ]:


def write_fact_staging(
    table: pa.Table,
    staging_path: pathlib.Path,
) -> None:
    log_started_at = time.perf_counter()
    log_phase = "staging_write"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=write_fact_staging; phase=staging_write; status=started; "
        f"path={staging_path}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        staging_path.mkdir(parents=True, exist_ok=False)
        log_phase = "schema_marker"
        pq.write_table(
            pa.Table.from_batches([], schema=FACT_FILE_SCHEMA),
            staging_path / "schema.parquet",
        )

        log_phase = "write_parquet"
        if len(table):
            ds.write_dataset(
                table,
                staging_path,
                format="parquet",
                partitioning=FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=write_fact_staging; phase=staging_write; status=completed; "
            f"path={staging_path}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=write_fact_staging; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 兼容旧版本事实整根迁移
# 
# 仅 schema_version 过期且物理结构、表身份兼容时进入该路径，纯描述性 metadata 差异不迁移。main 仍只允许无日期写入模式提交迁移。
# 
# 完整旧事实按当前业务规则验收一次，复用同一 Arrow 表写 staging。staging 和正式整表仅检查物理契约、行数及逐值内容，不再次执行业务校验或通过 Pandas/IPC 往返生成摘要。正式验收在共享事务内，成功退出后才报告 metadata_upgraded。
# 
# 整根备份/隔离位于本批恢复目录的 `_root`；共享模块负责安装与恢复，保留失败新根，恢复不完整另保留旧备份，staging 清理。
# 

# ### 流程：旧版本事实迁移
# 
# ```mermaid
# flowchart TD
# A["兼容旧版本；完整业务验收一次"] --> B["复用 Arrow 写 staging；物理与逐值复读"]
# B --> C["共享事务安装整根"]
# C --> D["事务内正式整表物理及逐值复读"]
# D --> E["成功退出；报告迁移已提交"]
# C -. 失败 .-> R["共享恢复；保留失败新根；抛错"]
# D -. 失败 .-> R
# ```
# 

# In[ ]:


def upgrade_fact_metadata(
    existing_fact_df: pd.DataFrame,
    lake_root: pathlib.Path,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=commit; status=started; "
        f"rows={len(existing_fact_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        complete_table = validate_interest_rate_table(
            pandas_to_arrow(existing_fact_df.loc[:, FACT_COLUMNS], INTEREST_RATE_DAILY_SCHEMA), "待升级完整 SHIBOR 事实",
        )
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
            write_fact_staging(complete_table, staging_path)
            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=staging_readback; status=started; "
                f"run_id={run_id}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                FACT_PARTITIONING,
                INTEREST_RATE_DAILY_SCHEMA,
                PARTITION_COLUMNS,
                "SHIBOR metadata 升级 staging",
            )
            staged_table = validate_arrow_table(
                staged_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
            ).sort_by(FACT_SORT_KEYS)
            if not staged_table.equals(complete_table):
                raise ValueError("SHIBOR staging 完整内容逐值复读失败。")
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=staging_readback; status=completed; "
                f"run_id={run_id}; rows={staged_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        log_phase = "install"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=install; status=started; "
            f"run_id={run_id}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=upgrade_fact_metadata; run_id={run_id}",
            ) as transaction:
                transaction.replace(target_path=target_path, staged_path=staging_path)

                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=install; status=completed; "
                    f"run_id={run_id}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=formal_readback; status=started; "
                    f"run_id={run_id}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_dataset = open_exact_dataset(
                    target_path,
                    FACT_PARTITIONING,
                    INTEREST_RATE_DAILY_SCHEMA,
                    PARTITION_COLUMNS,
                    "升级后的正式 SHIBOR 事实",
                )
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
                ).sort_by(FACT_SORT_KEYS)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式 SHIBOR 完整内容逐值复读失败。")
                committed_df = arrow_to_pandas(committed_table, INTEREST_RATE_DAILY_SCHEMA)
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=formal_readback; status=completed; "
                    f"run_id={run_id}; rows={len(committed_df)}; pending_cleanup=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入失败也清理 staging；安装、验收失败恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={TABLE_NAME}; function=upgrade_fact_metadata; phase=commit; status=completed; "
            f"run_id={run_id}; rows={len(committed_df)}; persisted=true; date_watermark=none; message=metadata_upgraded; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=upgrade_fact_metadata; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 事实 dirty 完整叶与新标记共同提交
# 
# 完整候选叶只转换一次 Arrow、验收一次业务，随后复用排序后的表写 staging 并逐值比较。staging 只包含当前叶和空标记；正式复读直接打开当前叶，使用 `partition_base_dir` 补回 Hive 列，不扫描正式表根的其他月份。空结果显式删除旧叶并复读零行标记；新建标记单独确认零行，既有标记保持原样。
# 
# 当前叶与必要的新标记共用一个事务；物理契约和完整内容比较在事务内，成功退出才报告事实提交。正式复读所得 Arrow 表转为 Pandas 返回，供事实计数和叶映射复用。日历随后独立提交，失败不撤销已成功事实。失败新叶隔离保留，恢复不完整保留旧备份，staging 清理且不递归删除表根。
# 

# ### 流程：事实叶一次业务验收及共享提交
# 
# ```mermaid
# flowchart TD
# A["dirty 叶转 Arrow；业务验收一次"] --> B["复用 Arrow 写 staging；物理与逐值比较"]
# B --> C["共享事务：必要时新建标记；替换或显式删除叶"]
# C --> D["事务内直读当前叶或空标记；物理与逐值比较"]
# D --> E["成功退出；返回正式叶与提交日志；日历随后回写"]
# C -. 失败 .-> R["恢复当前叶与新标记；保留失败证据；停止"]
# D -. 失败 .-> R
# ```
# 

# In[ ]:


def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[int, int],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=commit; status=started; "
        f"rows={len(frame)}; partition={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        complete_table = validate_interest_rate_table(
            pandas_to_arrow(frame.loc[:, FACT_COLUMNS], INTEREST_RATE_DAILY_SCHEMA), "待提交完整 SHIBOR 事实分区",
        )
        if complete_table.num_rows:
            actual_keys = set(zip(*(complete_table[name].to_pylist() for name in PARTITION_COLUMNS), strict=True))
            if actual_keys != {partition_key}:
                raise ValueError("待提交 SHIBOR 内容越出指定事实叶分区。")

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
            write_fact_staging(complete_table, staging_path)
            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=staging_readback; status=started; "
                f"run_id={run_id}; partition={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                FACT_PARTITIONING,
                INTEREST_RATE_DAILY_SCHEMA,
                PARTITION_COLUMNS,
                "SHIBOR 事实 staging",
            )
            staged_table = validate_arrow_table(
                staged_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
            ).sort_by(FACT_SORT_KEYS)
            if not staged_table.equals(complete_table):
                raise ValueError("SHIBOR staging 完整内容逐值复读失败。")
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=staging_readback; status=completed; "
                f"run_id={run_id}; partition={partition_key}; rows={staged_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        except Exception:
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

        log_phase = "install"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=install; status=started; "
            f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_complete_fact_partition; run_id={run_id}; partition={partition_key}",
            ) as transaction:
                if marker_created:
                    transaction.replace(
                        target_path=target_marker_path,
                        staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if complete_table.num_rows > 0 else None,
                )

                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=install; status=completed; "
                    f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=formal_readback; status=started; "
                    f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                if complete_table.num_rows == 0 and destination_path.exists():
                    raise ValueError("确认空的正式 SHIBOR 事实叶仍然存在。")
                committed_dataset = open_exact_dataset(
                    destination_path if complete_table.num_rows else target_marker_path,
                    FACT_PARTITIONING, INTEREST_RATE_DAILY_SCHEMA, PARTITION_COLUMNS,
                    "正式 SHIBOR 事实", partition_base_dir=target_path,
                )
                if marker_created and complete_table.num_rows:
                    marker_dataset = open_exact_dataset(
                        target_marker_path, FACT_PARTITIONING, INTEREST_RATE_DAILY_SCHEMA,
                        PARTITION_COLUMNS, "新建正式 SHIBOR 零行标记", partition_base_dir=target_path,
                    )
                    if marker_dataset.count_rows() != 0:
                        raise ValueError("新建正式 SHIBOR 标记必须为零行。")
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
                ).sort_by(FACT_SORT_KEYS)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式 SHIBOR 完整内容逐值复读失败。")
                committed_df = arrow_to_pandas(committed_table, INTEREST_RATE_DAILY_SCHEMA)
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=formal_readback; status=completed; "
                    f"run_id={run_id}; partition={partition_key}; rows={len(committed_df)}; pending_cleanup=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入失败也清理 staging；安装、验收失败恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={TABLE_NAME}; function=commit_complete_fact_partition; phase=commit; status=completed; "
            f"run_id={run_id}; partition={partition_key}; rows={len(committed_df)}; persisted=true; date_watermark=none; calendar_state=not_committed_by_this_function; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_complete_fact_partition; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 生成完成状态
# 
# 只复制并更新调用方提供的当前日历完整叶。正式复读计数为 1 时记 success/passed，为 0 时记 empty_confirmed/warning；原因逐格点覆盖。 未触达行保持原样。
# 
# 生成函数报告扫描/更新数量和 persisted=false，不在返回前再转 Arrow 或完整业务校验；这些责任集中到随后日历叶提交前。
# 

# ### 流程：生成当前叶完成状态
# 
# ```mermaid
# flowchart TD
# A["当前日历叶和正式复读格点计数"] --> B["原因覆盖及 0/1 计数门禁"]
# B --> C["更新触达行成功或确认空；保留其他行"]
# C --> D["返回当前叶；尚未提交"]
# ```
# 

# In[ ]:


def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_counts: dict[tuple[str, date], int],
    reason_by_grid: dict[tuple[str, date], str],
    fetch_run_id: str,
    completed_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_updated_rows = 0
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_state; status=started; "
        f"calendar_rows={len(calendar_df)}; input_grids={len(grid_counts)}; fetch_run_id={fetch_run_id}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if set(reason_by_grid) != set(grid_counts):
            raise ValueError("日历完成原因必须逐个覆盖正式复读格点。")

        log_phase = "copy_calendar"
        updated_df = calendar_df.copy()
        log_phase = "update_rows"
        for index, row in updated_df.iterrows():
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=update_rows; status=running; "
                    f"processed_rows={log_processed_rows}/{len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
            log_updated_rows += 1

        log_phase = "validate_output"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase=generate_state; status=completed; "
            f"rows={len(updated_df)}; changed_grids={log_updated_rows}; fetch_run_id={fetch_run_id}; is_fetch_completed=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_completion; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; processed_rows={log_processed_rows}; changed_grids={log_updated_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 生成失败状态
# 
# 只复制并更新调用方提供的当前日历完整叶。只允许 retryable_error/permanent_error，清空完成凭证，计数归零、quality_status=failed；不伪装确认空。 未触达行保持原样。
# 
# 生成函数报告扫描/更新数量和 persisted=false，不在返回前再转 Arrow 或完整业务校验；这些责任集中到随后日历叶提交前。
# 

# ### 流程：生成当前叶失败状态
# 
# ```mermaid
# flowchart TD
# A["当前日历叶和失败格点"] --> B["检查失败类别；清除触达行完成凭证"]
# B --> C["返回当前叶；生成完成不代表落盘"]
# ```
# 

# In[ ]:


def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    failed_grids: set[tuple[str, date]],
    failure_status: str,
    failure_reason: str,
    fetch_run_id: str,
    checked_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_updated_rows = 0
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_state; status=started; "
        f"calendar_rows={len(calendar_df)}; input_grids={len(failed_grids)}; fetch_run_id={fetch_run_id}; result_status={failure_status}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if failure_status not in {"retryable_error", "permanent_error"}:
            raise ValueError("SHIBOR 失败状态只能是 retryable_error 或 permanent_error。")

        log_phase = "copy_calendar"
        updated_df = calendar_df.copy()
        log_phase = "update_rows"
        for index, row in updated_df.iterrows():
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=update_rows; status=running; "
                    f"processed_rows={log_processed_rows}/{len(updated_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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
            log_updated_rows += 1

        log_phase = "validate_output"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase=generate_state; status=completed; "
            f"rows={len(updated_df)}; changed_grids={log_updated_rows}; fetch_run_id={fetch_run_id}; result_status={failure_status}; is_fetch_completed=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return updated_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_calendar_failure; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; processed_rows={log_processed_rows}; changed_grids={log_updated_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 日历 dirty 完整叶独立提交
# 
# 先选取当前完整叶，再执行一次 Arrow 转换和业务验收，不对传入的全历史日历验收。main 已从循环前建立的日历叶映射传入当前叶；同月先前修复的状态由映射保存并继承。
# 
# staging 只写当前叶，不另写未使用的日历根标记。staging 与正式复读直接打开该叶，检查物理、表身份、版本及完整内容，不重复业务验收或扫描其他分区。目标叶必须已经由 c01 创建，既有根标记与其他数据集保持原样。
# 
# 正式复读在当前叶的共享事务内，成功退出才返回已提交叶并报告 calendar_state=committed；完成与否仍以每个格点字段为准。失败状态保存不是采集完成，也无独立日期水位。事务失败只恢复当前日历叶，已成功事实保留，下一次可无 API 修复。
# 

# ### 流程：当前日历叶独立提交
# 
# ```mermaid
# flowchart TD
# A["选当前叶；转 Arrow 并业务验收一次"] --> B["写 staging 当前叶；物理与逐值复读"]
# B --> C["共享事务；确认目标叶存在并替换"]
# C --> D["直读当前正式叶；物理与逐值比较"]
# D --> E["成功退出；返回已提交叶；完成按格点字段"]
# C -. 失败 .-> R["恢复当前叶；此前成功事实保留；抛错"]
# D -. 失败 .-> R
# ```
# 

# In[ ]:


def commit_calendar_partition(
    calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[str, int, int],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=commit; status=started; "
        f"rows={len(calendar_df)}; partition={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:

        log_phase = "select_partition"
        partition_mask = pd.Series(True, index=calendar_df.index)
        for column, value in zip(
            CALENDAR_PARTITION_COLUMNS,
            partition_key,
            strict=True,
        ):
            partition_mask &= calendar_df[column].eq(value)
        partition_df = calendar_df.loc[partition_mask].copy()
        if partition_df.empty:
            raise ValueError("待提交宏观发布日历完整叶分区不得为空。")

        partition_table = validate_interest_calendar_table(
            pandas_to_arrow(partition_df.loc[:, CALENDAR_COLUMNS], MACRO_RELEASE_CALENDAR_SCHEMA),
            "待提交完整日历分区",
        )
        log_phase = "paths"
        relative_path = pathlib.Path(*[
                    f"{column}={value}"
                    for column, value in zip(
                        CALENDAR_PARTITION_COLUMNS,
                        partition_key,
                        strict=True,
                    )
                ])
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
        backup_path = silver_root / f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
        quarantine_path = silver_root / f".{CALENDAR_TABLE_NAME}.failed-{run_id}"

        for managed_path in [target_path, staging_path, backup_path, quarantine_path]:
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"日历路径越出 silver 根目录：{managed_path}")

        # 上游必须已经由 b01 正式提交，b02 不创建或迁移日历根。

        try:
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=staging_write; status=started; "
                f"run_id={run_id}; partition={partition_key}; rows={len(partition_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)
            ds.write_dataset(
                partition_table,
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=staging_readback; status=started; "
                f"run_id={run_id}; partition={partition_key}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path / relative_path, CALENDAR_PARTITIONING, MACRO_RELEASE_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS, "宏观发布日历 staging", partition_base_dir=staging_path,
            )
            staged_table = validate_arrow_table(
                staged_dataset.to_table(columns=CALENDAR_COLUMNS), MACRO_RELEASE_CALENDAR_SCHEMA,
            ).sort_by(CALENDAR_SORT_KEYS)
            if not staged_table.equals(partition_table):
                raise ValueError("宏观发布日历 staging 完整叶逐值复读失败。")
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=staging_readback; status=completed; "
                f"run_id={run_id}; partition={partition_key}; rows={staged_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        log_phase = "install"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=install; status=started; "
            f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; run_id={run_id}; partition={partition_key}",
            ) as transaction:
                if not destination_path.is_dir():
                    raise FileNotFoundError(
                        f"正式宏观发布日历缺少待回写完整叶：{relative_path}"
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )

                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=install; status=completed; "
                    f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "formal_readback"
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=formal_readback; status=started; "
                    f"run_id={run_id}; partition={partition_key}; pending_acceptance=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_dataset = open_exact_dataset(
                    destination_path, CALENDAR_PARTITIONING, MACRO_RELEASE_CALENDAR_SCHEMA,
                    CALENDAR_PARTITION_COLUMNS, "回写后的正式宏观发布日历", partition_base_dir=target_path,
                )
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=CALENDAR_COLUMNS), MACRO_RELEASE_CALENDAR_SCHEMA,
                ).sort_by(CALENDAR_SORT_KEYS)
                if not committed_table.equals(partition_table):
                    raise ValueError("正式宏观发布日历完整叶逐值复读失败。")
                committed_df = arrow_to_pandas(committed_table, MACRO_RELEASE_CALENDAR_SCHEMA)
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=formal_readback; status=completed; "
                    f"run_id={run_id}; partition={partition_key}; rows={len(committed_df)}; pending_cleanup=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入失败也清理 staging；安装、验收失败恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase=commit; status=completed; "
            f"run_id={run_id}; partition={partition_key}; rows={len(committed_df)}; persisted=true; date_watermark=none; calendar_state=committed; collection_completion=per_grid; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partition; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：一次规划、分区映射与逐叶推进
# 
# 参数门禁、来源错误分类、按月串行请求及独立提交顺序保持不变。启动读取两表并对账一次，随后按分区构造事实/日历叶映射及契约空事实表；当前月份只消费所属叶，不再筛选、拼接或完整校验累计历史。
# 
# 先逐叶执行无 API 修复，把提交函数返回的正式日历叶写回映射，再消费原先互不相交的 API 待办；同月采集会继承已经修复的状态。成功或失败状态均只更新当前叶，成功提交后更新映射。批末复用已验收叶和完成格点计数，不再从两个表根重新物化并求差。只读模式不累计正式提交凭证。
# 
# main 保留批次、参数、月份、跳过及最终结果日志；来源读取、生成和提交进度由对应函数负责。来源失败仍继续后续月份，批末失败；提交或恢复异常立即传播，既有成功叶保留。
# 

# ### 流程：一次规划与分区推进
# 
# ```mermaid
# flowchart TD
# A["参数门禁；可信两表读取；必要的旧版本迁移"] --> B["启动一次对账；预建分区映射和空表"]
# B --> C["按 write 决定修复；保存已提交叶"]
# C --> D{"有 API 待办？"}
# D -->|否| E["不创建客户端；结束"]
# D -->|是| F["遍历待办月份；从映射取当前事实和日历叶"]
# F --> G["请求、转换和完整叶合并"]
# G --> H{"write？"}
# H -->|否| J["下一月；只读结束"]
# H -->|是| K["先事实提交并核对计数，再日历提交"]
# K --> L["更新已提交叶映射及完整格点数"]
# L --> M["批末复用证据；不重读全历史"]
# G -. 来源失败 .-> R["写入时回写当前叶失败状态；继续；批末抛错"]
# K -. 提交失败 .-> X["共享恢复；立即停止"]
# ```
# 

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

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=parameters; status=completed; "
            f"mode={'explicit' if has_explicit_dates else 'automatic'}; start_date={requested_start_date}; end_date={requested_end_date}; lake_root={resolved_lake_root}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "read_calendar"
        calendar_df = read_interest_calendar(calendar_path)

        log_phase = "read_fact"
        existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)


        if not fact_metadata_is_exact:
            if has_explicit_dates:
                raise click.UsageError(
                    "现有 SHIBOR 事实 metadata 已过期；"
                    "请先移除日期参数执行一次完整自动升级。"
                )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=metadata_plan; status=completed; "
                f"rows={len(existing_fact_df)}; write={str(write).lower()}; persisted=false; message=metadata_upgrade_required; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if write:
                log_phase = "metadata_upgrade"
                existing_fact_df = upgrade_fact_metadata(
                    existing_fact_df,
                    resolved_lake_root,
                )

        log_phase = "plan"
        pending_df, repair_df, complete_count = plan_interest_rate_grids(
            calendar_df, existing_fact_df, requested_start_date, requested_end_date,
        )
        fact_leaves = {tuple(key): frame for key, frame in existing_fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True)}
        calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True)}
        empty_fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)

        # 正式事实已经存在的格点只修复日历，不创建 Tushare 客户端。
        log_phase = "state_repair"
        if not repair_df.empty:
            repair_count = len(repair_df)
            if write:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=started; "
                    f"rows={repair_count}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                repair_run_id = uuid.uuid4().hex
                repair_time = datetime.now(timezone.utc)
                for key, repair_leaf_df in repair_df.groupby(["year", "month"], sort=True, observed=True):
                    calendar_partition_key = (DATASET_NAME, int(key[0]), int(key[1]))
                    repair_counts = {(row.series_code, row.report_date): 1 for row in repair_leaf_df.itertuples(index=False)}
                    repair_reasons = {key: STATE_REPAIR_REASON for key in repair_counts}
                    calendar_leaf_df = apply_calendar_completion(
                        calendar_leaves[calendar_partition_key], repair_counts, repair_reasons, repair_run_id, repair_time,
                    )
                    log_phase = "repair_calendar_commit"
                    calendar_leaves[calendar_partition_key] = commit_calendar_partition(
                        calendar_leaf_df, resolved_lake_root, calendar_partition_key,
                    )
                complete_count += repair_count
                repair_df = repair_df.iloc[0:0]
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=completed; "
                    f"rows={repair_count}; remaining_api_pending={len(pending_df)}; message=state_repaired: rows={repair_count}; "
                    f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            else:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=skipped; "
                    f"rows={repair_count}; reason=dry_run; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        if pending_df.empty:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
                f"reason=no_api_pending; write={str(write).lower()}; state_repair_pending={len(repair_df)}; message=Tushare client not created; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return

        log_phase = "create_client"
        client = create_tushare_client()

        failed_window_count = 0

        for (year, month), partition_pending_df in pending_df.groupby(
            ["year", "month"],
            sort=True,
            observed=True,
        ):
            log_phase = "prepare_window"
            partition_key = (int(year), int(month))
            calendar_partition_key = (DATASET_NAME, *partition_key)
            calendar_leaf_df = calendar_leaves[calendar_partition_key]
            existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
            click.echo(
                f"partition_start: table={TABLE_NAME}; function=main; phase=api_window; status=started; "
                f"partition={partition_key}; pending_grids={len(partition_pending_df)}; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

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
                log_phase = "request"
                raw_df = query_shibor_window(
                    client,
                    request_start_date,
                    request_end_date,
                )
                log_phase = "normalize"
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
                    f"planning_progress: table={TABLE_NAME}; function=main; phase=api_window; status=failed; "
                    f"partition={partition_key}; result_status={failure_status}; grids={len(touched_grids)}; reason={failure_reason}; persisted=false; automatic_retry=false; "
                    f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                if write:
                    log_phase = "failure_state"
                    calendar_leaf_df = apply_calendar_failure(
                        calendar_leaf_df,
                        touched_grids,
                        failure_status,
                        failure_reason,
                        fetch_run_id,
                        datetime.now(timezone.utc),
                    )
                    log_phase = "failure_calendar_commit"
                    calendar_leaves[calendar_partition_key] = commit_calendar_partition(
                        calendar_leaf_df,
                        resolved_lake_root,
                        calendar_partition_key,
                    )
                continue

            log_phase = "merge_fact"
            complete_partition_df = full_fact_partition(
                existing_fact_leaf_df,
                incoming_fact_df,
                touched_grids,
                partition_key,
            )

            if not write:
                continue

            log_phase = "fact_commit"
            committed_partition_df = commit_complete_fact_partition(
                complete_partition_df,
                resolved_lake_root,
                partition_key,
            )
            log_phase = "fact_count_check"
            committed_counts = fact_grid_count_map(committed_partition_df)
            committed_touched_counts = {
                key: committed_counts.get(key, 0)
                for key in touched_grids
            }
            if committed_touched_counts != expected_counts:
                raise ValueError(
                    "正式 SHIBOR 事实复读计数与完整 API 转换结果不一致。"
                )

            log_phase = "completion_state"
            completion_reasons = {
                key: API_SUCCESS_REASON if count == 1 else API_EMPTY_REASON
                for key, count in committed_touched_counts.items()
            }
            calendar_leaf_df = apply_calendar_completion(
                calendar_leaf_df,
                committed_touched_counts,
                completion_reasons,
                fetch_run_id,
                datetime.now(timezone.utc),
            )
            log_phase = "completion_calendar_commit"
            calendar_leaves[calendar_partition_key] = commit_calendar_partition(
                calendar_leaf_df,
                resolved_lake_root,
                calendar_partition_key,
            )


            fact_leaves[partition_key] = committed_partition_df
            complete_count += len(touched_grids)

        log_phase = "batch_result"
        if failed_window_count:
            raise click.ClickException(
                f"{failed_window_count} 个 SHIBOR 月度窗口失败；"
                "成功分区已提交，失败格点保持未完成。"
            )

        if not write:
            click.echo(
                f"dry_run: table={TABLE_NAME}; function=main; phase=run; status=completed; "
                f"write=false; persisted=false; message=API 响应已转换和质检，未写事实或日历; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return

        # 各 dirty 叶已在事务内正式验收；复用本批提交结果，不再扫描 clean 历史。
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"formal_reconciled={complete_count}; fact_rows={sum(len(frame) for frame in fact_leaves.values())}; write=true; date_watermark=none; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
    finally:
        click.echo("=" * 80)


# ## Notebook 与脚本执行入口
# 
# 与 b01/c01 使用相同的入口结构：仅在交互内核且没有 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取 PyCharm/Jupyter 内核的 `-f` 参数。默认空列表不带 `--write`，只读湖并按自动范围求差；有 API 待办时仍会请求 Tushare 和转换响应，但不提交事实或日历。
# 
# 终端直接运行同名 `.py` 时正常读取命令行参数，只有显式 `--write` 才提交；成对日期与正式湖写入门禁仍由 main 执行。普通 Python 或 Notebook 内导入同名模块均不启动业务入口或 Schema 浏览。
# 

# ### 流程：Notebook 与脚本执行入口
# 
# ```mermaid
# flowchart TD
# A{"交互内核且没有脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
# B --> C["默认空参数；自动求差；有待办请求 API；不写湖"]
# A -->|否| D{"以脚本直接运行？"}
# D -->|是| E["main 读取终端参数；write 与日期门禁保持"]
# D -->|否| F["模块导入；不启动入口"]
# ```
# 

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="c02_interest_rate",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()

