#!/usr/bin/env python
# coding: utf-8

# # c03 中国宏观发布事实
# 
# 本入口生产 `fact_macro_release`，只消费 b04/c01 已正式提交的宏观发布日历中 `dataset_name=macro_release` 的 required 系列—报告期格点。它以事实内容与日历状态共同证明完成，不自行生成理论格点或扩大上游水位。
# 
# | 上下游或依赖 | 与本环节的关系 |
# | --- | --- |
# | b04/c01 宏观发布日历 | 提供理论报告期、项目可用日及采集状态；c03 的 `available_date` 只取 `expected_available_date`。 |
# | 共享宏观配置 | `config/futures_lakehouse/macro_release_entities.py` 唯一定义 17 个系列、报告名、原列、频率及数值偏移。 |
# | 权威数据契约 | `config/data_contracts.py` 定义事实及日历的字段、主键、分区和质量边界。 |
# | Eastmoney 数据中心 | 按 CPI、PPI、PMI、GDP 报告名与年月窗口严格分页；只接纳精确待办系列。 |
# | 现有正式事实 | 共同证明格点完成，并为陈旧日历提供无 API 修复依据；同月未触达事实保留。 |
# | 日历状态回写 | 事实正式复读后独立提交完整 `macro_release/year/month` 叶，供下次求差及 c01 状态继承。 |
# | b04/c02 SHIBOR | 使用同一日历的 `interest_rate` 叶；c03 不改写其格点或事实。 |
# | operations 与读取 Demo | 默认顺序为 b04/c01 → c02 → c03；数据库 Demo 只读消费正式表。 |
# 
# 事实与日历依次独立提交。日历失败不会撤销此前成功的事实；下次人工运行可从正式事实无 API 修复状态。本环节没有独立日期水位文件，完成证据保存在事实与日历中。
# 

# ## 自动范围、来源含义与写入边界
# 
# | 模式或结果 | 当前行为 |
# | --- | --- |
# | 默认自动范围 | 上游 required 格点减去事实与日历共同完整的格点；空湖和内部缺口走同一流程。 |
# | 成对显式日期 | 筛选 required 报告期；正式湖禁止带日期写入，非正式湖可做范围检查。 |
# | 不带 `--write` | 读取本地表；有 API 待办仍会请求、转换及合并，但不提交事实、修复状态或失败状态。 |
# | 正式事实完整、日历陈旧 | 写入模式先修复日历并复读；只读模式只报告修复计划；没有 API 待办就不创建会话。 |
# | 来源有有效值 | 依据共享原列和偏移生成精确待办事实；同月其他报告或已完成系列保留。 |
# | 完整响应缺日或缺值 | 形成该格点 0 行预期；正式事实复读为 0 后才回写 `empty_confirmed + warning`。 |
# | 分页、结构或数值异常 | 不把部分结果当成有效值或确认空；写入模式回写未完成的失败日历。 |
# 
# Eastmoney 的 `REPORT_DATE` 用报告月 1 日编码，不是发布日期。CPI/PPI/PMI 归一到月末；GDP 只接纳 3、6、9、12 月并归一到季末。PPI 同比读取 `BASE_SAME`；CPI 全国/城市/农村累计及 PPI 累计按共享配置减 100，转成累计同比百分比。项目可用日始终取上游日历。
# 
# 每个报告窗口冻结首页 `pages/count`，检查后续页元数据、页长、累计行数、日期和跨页唯一性。首次明确空响应也要符合当前空响应契约。非空值不能为布尔值、非数值或非有限数；原列缺失不是确认空。
# 
# 当前 HTTP 适配器保留 `Retry(total=3)` 的有界重试。请求函数最终抛出后，main 记录失败窗口并继续其他报告窗口，批末汇总抛错，不重新执行该业务窗口；`retryable_error` 是状态分类。安装或正式验收异常直接停止，之前成功叶保留。
# 
# 启动保留一次事实计数、日历凭证和可用日对账，事实越出 required 或 available_date 不等于上游时停止。当前版本历史信任生产者证明，描述性 metadata 不重写历史；只有物理与身份兼容的旧事实版本在无日期写入时整根迁移。来源输出与每次待提交 dirty 完整叶各验收一次，staging/正式复读仅物理及逐值检查。相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../a02_Lake/AGENTS.md)。Notebook 为权威源，同名脚本由标准 PythonExporter 生成。
# 

# ### 流程：宏观事实与日历完成凭证
# 
# ```mermaid
# flowchart TD
# A["读可信日历和事实；物理、身份与版本检查"] --> B["启动一次：计数、状态和可用日共同对账"]
# B --> C["准备叶映射；write 时无 API 修复日历"]
# C --> D{"还有 API 待办？"}
# D -->|否| Z["提前结束；不创建会话"]
# D -->|是| E["按报告与年月严格分页；来源输出验收一次"]
# E --> F["合并当前完整月叶；保留其他报告"]
# F --> G{"启用 write？"}
# G -->|否| H["只读转换结果"]
# G -->|是| I["dirty 事实叶验收一次；安装并逐值复读"]
# I --> J["依据正式计数生成当前日历叶；验收并独立提交"]
# J --> K["更新叶映射和完成数；同月后续报告继承"]
# K --> L["批末复用正式提交证据；不重扫历史"]
# E -. 来源失败 .-> R["write 时保存未完成状态；继续；批末失败"]
# I -. 提交失败 .-> X["共享模块恢复当前目标；停止；此前成功叶保留"]
# J -. 提交失败 .-> X
# ```
# 

# ## 初始化与依赖
# 
# 沿用项目根标记搜索及标准库导入，随后定义函数。配置映射只有一份，来源请求由 main 的待办分支触发。
# 路径安装与失败恢复使用湖仓级 `b00_04_staged_path_transaction.py`；不新增共享层。
# 

# ### 流程：初始化与项目依赖
# 
# ```mermaid
# flowchart TD
# A["当前目录向上搜索项目标记"] --> B{"找到项目根？"}
# B -->|是| C["导入契约、共享配置与设置"]
# B -->|否| X["报错"]
# C --> D["仅初始化；不请求来源、不写湖"]
# ```
# 

# In[ ]:


from __future__ import annotations

import math
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
        sys.path.insert(0, str(candidate_root / "02_Market_Data/a01_Collection"))
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
from config.futures_lakehouse.macro_release_entities import MACRO_RELEASE_SERIES
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 仅在交互内核且没有 `__file__` 时，依次展示宏观日历与宏观事实的权威 Schema。这里已经传入 `lake_root`，显式选择数据样例会有界读取本地湖；不请求来源 API、不写湖，也不定义第二份契约。
# 

# ### 流程：Schema 浏览与本地样例
# 
# ```mermaid
# flowchart TD
# A{"交互内核且无脚本文件变量？"} -->|是| B["展示日历与事实契约"]
# A -->|否| Z["跳过"]
# B --> C["显式选择样例后有界读本地湖"]
# ```
# 

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        MACRO_RELEASE_CALENDAR_SCHEMA,
        MACRO_RELEASE_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、来源映射与状态原因
# 
# 表名、主键与 Hive 分区从两张具名 Schema 读取一次。事实按 `series_code/report_date` 唯一、按 `year/month` 分区；日历回写完整 `macro_release/year/month` 叶。
# 
# 配置先选择 17 个宏观系列，再按 4 个报告构造映射和请求字段，并检查原列及累计值偏移。成功、确认空与无 API 修复的原因文本参与完成凭证判定，日志不替代这些持久字段。
# 

# ### 流程：契约身份与共享映射
# 
# ```mermaid
# flowchart TD
# A["两张权威 Schema"] --> B["读取表名、主键和分区；构造 Hive 规则"]
# C["共享宏观系列"] --> D["选 17 系列、4 报告；建立字段映射"]
# D --> E["检查 PPI 原列和累计偏移；定义状态原因"]
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
# 同一运行期间不变的契约表示，供各月份复用。
FACT_COLUMNS = MACRO_RELEASE_SCHEMA.names
CALENDAR_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.names
FACT_SORT_KEYS = [(name, "ascending") for name in PRIMARY_KEY]
CALENDAR_SORT_KEYS = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
FACT_FILE_SCHEMA = pa.schema(
    [field for field in MACRO_RELEASE_SCHEMA if field.name not in PARTITION_COLUMNS],
    metadata=MACRO_RELEASE_SCHEMA.metadata,
)


# ## 可信历史的物理结构、表身份与版本
# 
# 日历及事实均在一次 fragment 遍历中确认物理字段、类型、nullable、表名、主键、分区和契约版本；描述性 metadata 使用当前 Schema，不触发历史重写。当前版本历史信任生产者已完成的业务验收。日历旧版本先由 c01 迁移；兼容旧事实版本仅在无日期写入模式经过一次当前业务验收后整根迁移。
# 

# ### Hive 字段与完整 Schema 重建
# 
# `reconstructed_schema`：按权威顺序从 Dataset 取字段，组合文件与 Hive 分区字段并保留 Dataset metadata；缺字段报错，不读取记录。
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


# ### 物理字段兼容性
# 
# `physical_schema_matches`：只比较字段名及顺序、类型和 nullable，返回布尔值，不判断 metadata 或业务状态。
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


# ### 打开物理兼容的 Dataset
# 
# `open_compatible_dataset`：支持表根、当前叶及零行标记文件。一次 fragment 遍历同时确认物理结构、表身份和版本；分区根以字符串传给 PyArrow 补回 Hive 列。描述性差异不影响兼容，不另扫 metadata。
# 

# ### 流程：一次物理与身份读取
# 
# ```mermaid
# flowchart TD
# A["发现当前路径文件；打开 Hive Dataset"] --> B["检查整体结构、表身份和版本"]
# B --> C["一次遍历 fragment：物理、身份、版本"]
# C --> D["返回 Dataset 与当前版本标志"]
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
        log_phase = "fragment_schema"
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
                    f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=fragment_schema; status=running; "
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


# ### 按当前契约打开 Dataset
# 
# `open_exact_dataset`：要求前一函数返回当前版本；表身份或物理结构错误仍拒绝。普通叶提交传入当前叶和 partition_base_dir，不打开整个正式表根。
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


# ## 生产者质量责任与读取边界
# 
# 来源转换结果和待提交 dirty 完整叶各承担一次业务验收。事实 value 的空值与布尔值门禁保留在 Arrow 转换前，避免布尔值被转换为浮点数；来源列、日期、频率、范围、唯一性、有限数及偏移规则均保留。
# 
# 可信历史读取不重做业务校验；staging 和正式复读只验物理契约并逐值比较。日历 dirty 叶的验证直接消费已经转换的 Arrow，不把全表来回转换。
# 

# ### 当前日历校验
# 
# `validate_macro_calendar_table`：输入为已转换的 Arrow 日历叶，只做本次输出的业务验收；窄主键用于唯一性检查，返回排序后的 Arrow 表。不重转完整 Pandas 日历。
# 

# ### 流程：日历 dirty 叶业务验收
# 
# ```mermaid
# flowchart TD
# A["已转换的 Arrow 日历叶"] --> B["主键、系列与逐行状态业务检查"]
# B --> C["返回排序后的 Arrow 表"]
# ```
# 

# In[ ]:


def validate_macro_calendar_table(
    table: pa.Table,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        calendar_keys_df = table.select(CALENDAR_PRIMARY_KEY).to_pandas()

        log_phase = "primary_key"
        if calendar_keys_df.duplicated().any():
            raise ValueError(f"{context}宏观发布日历主键不唯一。")
        if table.num_rows and not all(value == DATASET_NAME for value in table["dataset_name"].to_pylist()):
            raise ValueError(f"{context}只允许包含 macro_release 日历行。")

        expected_series_codes = set(SERIES_BY_CODE)
        now_utc = datetime.now(timezone.utc)

        log_phase = "business_validation"
        for row in table.to_pylist():
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
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=running; "
                    f"processed_rows={log_processed_rows}/{table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        validated_calendar_table = table.sort_by(CALENDAR_SORT_KEYS)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_macro_calendar_table; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_calendar_table)}; checked_rows={log_processed_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_macro_calendar_table; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; context={context}; checked_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 当前事实完整质量检查
# 
# `validate_macro_release_frame`：保留转换前的空值、布尔值和数值转换门禁，只转一次 Arrow；以窄主键检查唯一性，逐行确认系列、来源、有限性、日期和时间，返回排序后的 Arrow。来源输出及 dirty 叶分别调用一次，物理复读不调用。
# 

# ### 流程：事实一次业务验收
# 
# ```mermaid
# flowchart TD
# A["空值、布尔与数值转换门禁"] --> B["一次 Arrow 转换；检查主键"]
# B --> C["逐行检查系列、来源、有限值、日期与时间"]
# C --> D["返回排序后的已验收 Arrow 表"]
# ```
# 

# In[ ]:


def validate_macro_release_frame(
    frame: pd.DataFrame,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_processed_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=validate_macro_release_frame; phase=validate; status=started; "
        f"context={context}; rows={len(frame)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
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
            ordered_input_df["value"] = numeric_values

        table = pandas_to_arrow(ordered_input_df, MACRO_RELEASE_SCHEMA)
        fact_keys_df = table.select(PRIMARY_KEY).to_pandas()

        log_phase = "primary_key"
        if fact_keys_df.duplicated().any():
            raise ValueError(f"{context}宏观事实主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        log_phase = "business_validation"
        for row in table.to_pylist():
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
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=validate_macro_release_frame; phase=validate; status=running; "
                    f"processed_rows={log_processed_rows}/{table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        validated_fact_table = table.sort_by(FACT_SORT_KEYS)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_macro_release_frame; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_fact_table)}; checked_rows={log_processed_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_fact_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_macro_release_frame; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; context={context}; checked_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 读取正式上游日历
# 
# `read_macro_calendar`：确认物理、身份与当前版本后，只物化 macro_release 行并按权威契约转 Pandas；信任上游已完成的业务验证。
# 

# ### 流程：可信上游日历
# 
# ```mermaid
# flowchart TD
# A["物理、身份和版本确认"] --> B["只物化 macro_release 权威列"]
# B --> C["契约转 Pandas；返回可信日历"]
# ```
# 

# In[ ]:


def read_macro_calendar(table_path: pathlib.Path) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "read"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_macro_calendar; phase=read; status=started; "
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
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_macro_calendar; phase=materialize; status=started; "
            f"dataset_name={DATASET_NAME}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        table = dataset.to_table(
            columns=MACRO_RELEASE_CALENDAR_SCHEMA.names,
            filter=ds.field("dataset_name") == DATASET_NAME,
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_macro_calendar; phase=materialize; status=completed; "
            f"rows={table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "convert"
        calendar_df = arrow_to_pandas(table, MACRO_RELEASE_CALENDAR_SCHEMA)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_macro_calendar; phase=read; status=completed; "
            f"rows={len(calendar_df)}; materialized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=read_macro_calendar; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 读取现有事实与迁移标志
# 
# `read_optional_fact`：无 Parquet 返回权威空表；有文件则物理兼容读取并按权威 Schema 转 Pandas。返回版本匹配标志，纯描述差异不触发迁移；本函数不写湖、不重验完整业务。
# 

# ### 流程：可信现有事实
# 
# ```mermaid
# flowchart TD
# A{"存在 Parquet？"} -->|否| B["权威空表"]
# A -->|是| C["兼容读取并物化事实"]
# C --> D["契约转 Pandas；返回事实及版本标志"]
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
            empty_fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=read; status=completed; "
                f"rows=0; reason=no_parquet; metadata_exact=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df, True

        log_phase = "dataset_open"
        dataset, metadata_is_exact = open_compatible_dataset(
            table_path,
            FACT_PARTITIONING,
            MACRO_RELEASE_SCHEMA,
            PARTITION_COLUMNS,
            "现有正式宏观发布事实",
        )
        log_phase = "materialize"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=materialize; status=started; "
            f"persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        source_table = dataset.to_table(columns=MACRO_RELEASE_SCHEMA.names)

        # 物理兼容旧表只在内存中重新附着当前 metadata，随后必须整根升级。
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_optional_fact; phase=materialize; status=completed; "
            f"rows={source_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "convert"
        current_df = arrow_to_pandas(source_table, MACRO_RELEASE_SCHEMA)
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


# ## 一次求差与无 API 修复
# 
# 启动保留一次事实计数、日历完成凭证及 available_date 对账，并拒绝事实越出全部 required 水位。之后把事实和日历按完整叶分组，修复和采集都只使用当前叶。修复提交后直接更新叶映射，不再全表重读重算；相同月份的后续报告继承此前已提交的事实及日历状态。
# 

# ### 正式事实格点计数
# 
# `fact_grid_count_map`：按事实主键统计每个系列—报告期的行数，供求差及提交后检查使用；空事实返回空映射。
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
        (str(series_code), report_date): int(count)
        for (series_code, report_date), count in counts.items()
    }


# ### 单格点完成凭证
# 
# `calendar_grid_is_complete`：required、实际计数、批次与审计时间必须齐全。1 行事实匹配 success/passed 及认可原因，0 行匹配 empty_confirmed/warning 及确认空原因；其余计数报错。此函数不请求 API、不写状态。
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
    raise ValueError("单个宏观发布格点正式事实计数只允许 0 或 1。")


# ### 格点求差与无 API 修复计划
# 
# `plan_macro_release_grids`：全部事实键必须属于上游 required，available_date 必须等于 expected_available_date。再对当前范围结合计数、质量及审计凭证分类：完整跳过，已有 1 行但状态陈旧则修复，其余进入 API 待办。这里只返回计划。
# 
# 报告水位检查、可用日核对、格点比较及完整/修复/API 待办数量。只生成计划，persisted=false。
# 

# ### 流程：格点求差与无 API 修复计划
# 
# ```mermaid
# flowchart TD
# A["选择 required 与当前日期范围"] --> B["全部事实不得越界；可用日须等于上游"]
# B --> C["逐格点核对正式计数与日历凭证"]
# C --> D{"共同完整？"}
# D -->|是| E["统计完整"]
# D -->|否| F{"已有 1 行事实？"}
# F -->|是| G["无 API 修复"]
# F -->|否| H["API 待办"]
# ```
# 

# In[ ]:


def plan_macro_release_grids(
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
        f"planning_progress: table={TABLE_NAME}; function=plan_macro_release_grids; phase=plan; status=started; "
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

        all_required_df = calendar_df.loc[
            calendar_df["is_fetch_required"]
        ].copy()
        log_phase = "watermark"
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

        log_phase = "available_date"
        for fact_row in fact_df.itertuples(index=False):
            key = (fact_row.series_code, fact_row.report_date)
            calendar_row = required_by_key[key]
            if fact_row.available_date != calendar_row.expected_available_date:
                raise ValueError(
                    f"正式宏观事实 {key} 的 available_date 与上游日历不一致。"
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
            log_processed_rows += 1
            if log_processed_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=plan_macro_release_grids; phase=compare; status=running; "
                    f"processed_rows={log_processed_rows}/{len(required_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        columns = list(calendar_df.columns)
        pending_df = pd.DataFrame(pending_rows, columns=columns)
        repair_df = pd.DataFrame(repair_rows, columns=columns)
        click.echo(
            f"reconciliation_plan: table={TABLE_NAME}; function=plan_macro_release_grids; phase=plan; status=completed; "
            f"required_grids={len(required_df)}; complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return pending_df, repair_df, complete_count
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=plan_macro_release_grids; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; processed_rows={log_processed_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 报告窗口严格分页与精确格点转换
# 
# 先完整取得一个报告窗口，再归一来源报告期、验证值并映射精确待办。任何分页不完整都不能进入事实提交；转换得到的 0/1 只是预期计数，仍需正式事实复读后才能推进日历。现有 HTTP 重试策略与失败分类如下，业务窗口不在 main 中重试。
# 

# ### 请求错误与持久状态分类
# 
# `MacroReleaseRequestError`：保存 retryable_error 或 permanent_error 及原因。main 用于失败回写和批末汇总；该异常类本身不执行重试，也不写日历。
# 

# ### 流程：请求错误与持久状态分类
# 
# ```mermaid
# flowchart TD
# A["错误状态与原因"] --> B["构造请求异常"]
# B --> C["main 按原分支处理；不在类内重试"]
# ```
# 

# In[ ]:


class MacroReleaseRequestError(RuntimeError):
    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


# ### 按需创建 Eastmoney 会话
# 
# `create_eastmoney_session`：只有 API 待办非空才调用。保留现有 HTTPAdapter 和 Retry(total=3)：连接、读取及指定 HTTP 错误受有界重试策略控制，并尊重 Retry-After。创建会话本身不发业务请求；main 负责关闭。
# 
# 报告会话准备及现有 HTTP 重试策略；构造会话不表示请求成功。
# 

# ### 流程：按需创建 Eastmoney 会话
# 
# ```mermaid
# flowchart TD
# A["存在 API 待办"] --> B["创建 Requests Session"]
# B --> C["挂载现有 HTTP 有界重试策略与请求头"]
# C --> D["返回会话；main 最终关闭"]
# ```
# 

# In[ ]:


def create_eastmoney_session() -> requests.Session:
    log_started_at = time.perf_counter()
    log_phase = "session"

    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=create_eastmoney_session; phase=session; status=started; "
        f"source_request_sent=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
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
            f"planning_progress: table={TABLE_NAME}; function=create_eastmoney_session; phase=session; status=completed; "
            f"session_ready=true; source_request_sent=false; http_retry_total=3; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return session
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=create_eastmoney_session; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 单报告窗口的严格分页
# 
# `query_eastmoney_report_range`：从第 1 页串行请求。首次明确空响应按已有规则返回空列表；非空响应冻结 pages/count，逐页核对类型、总页数、页长和元数据，全部结束再核对累计行数。任一步失败抛分类异常，不返回部分分页。
# 
# 每页请求前报告页号及窗口；分页检查通过后报告 page、page_rows 和 received_rows。全部核对后才报告 request 完成；部分失败报告 accepted_rows，不返回部分结果。HTTP 重试策略不变。
# 

# ### 流程：单报告窗口的严格分页
# 
# ```mermaid
# flowchart TD
# A["报告字段与窗口；页号设为 1"] --> Q["请求当前页"]
# Q --> B["HTTP、JSON、success 和结构检查"]
# B --> C{"首个响应符合明确空契约？"}
# C -->|是| Z["返回空列表"]
# C -->|否| D["冻结或核对 pages/count；检查页长"]
# D --> E["累计当前页"]
# E --> F{"还有页？"}
# F -->|是| N["页号加 1"]
# N --> Q
# F -->|否| G["累计行数等于 count；返回完整响应"]
# B -. 异常 .-> X["抛分类异常；不返回部分结果"]
# D -. 不一致 .-> X
# ```
# 

# In[ ]:


def query_eastmoney_report_range(
    session: requests.Session,
    report_name: str,
    range_start: date,
    range_end: date,
) -> list[dict[str, object]]:
    log_started_at = time.perf_counter()
    log_phase = "request"
    log_page_number = 0
    log_received_rows = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request; status=started; "
        f"report={report_name}; start_date={range_start}; end_date={range_end}; http_retry_total=3; business_window_retry=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "report_fields"
        fields = FIELDS_BY_REPORT.get(report_name)
        if fields is None:
            raise MacroReleaseRequestError(
                "permanent_error",
                f"未知 Eastmoney 宏观报告：{report_name}",
            )

        date_filter = f"(REPORT_DATE>='{range_start.isoformat()}')(REPORT_DATE<='{range_end.isoformat()}')"
        request_columns = ",".join(fields)
        page_number = 1
        expected_pages = None
        expected_count = None
        response_rows: list[dict[str, object]] = []

        while True:
            log_page_number = page_number
            log_phase = "request_page"
            click.echo(
                f"request_batch: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request_page; status=started; "
                f"report={report_name}; page={page_number}; expected_pages={expected_pages}; received_rows={len(response_rows)}; start_date={range_start}; end_date={range_end}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            params = {
                "reportName": report_name,
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
                log_phase = "response_json"
                payload = response.json()
            except ValueError as error:
                raise MacroReleaseRequestError(
                    "retryable_error",
                    "Eastmoney 未返回有效 JSON；"
                    f"report={report_name}; page={page_number}。",
                ) from error
            log_phase = "response_structure"
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
                    click.echo(
                        f"api_result: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request; status=completed; "
                        f"report={report_name}; page={page_number}; response_rows=0; empty_response=true; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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

            log_phase = "pagination_metadata"
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
                click.echo(
                    f"api_result: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request; status=completed; "
                    f"report={report_name}; page={page_number}; response_rows=0; empty_response=true; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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

            log_phase = "pagination_consistency"
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
            log_received_rows = len(response_rows)
            click.echo(
                f"api_result: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request_page; status=completed; "
                f"report={report_name}; page={page_number}/{expected_pages}; page_rows={len(data_rows)}; received_rows={len(response_rows)}/{expected_count}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            if page_number == expected_pages:
                break
            page_number += 1

        log_phase = "total_count"
        if len(response_rows) != expected_count:
            raise MacroReleaseRequestError(
                "retryable_error",
                "Eastmoney 累计行数与 count 不一致。",
            )
        click.echo(
            f"api_result: table={TABLE_NAME}; function=query_eastmoney_report_range; phase=request; status=completed; "
            f"report={report_name}; pages={expected_pages}; response_rows={len(response_rows)}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return response_rows
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=query_eastmoney_report_range; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; report={report_name}; page={log_page_number}; accepted_rows={log_received_rows}; partial_result_returned=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ### 来源报告期与精确待办事实
# 
# `normalize_macro_release_response`：保留完整来源质量门禁；报告频率在来源行循环前确认。只逐个生成精确待办结果，不再从同一待办重建集合后重复核对覆盖；非空来源输出一次业务验收并返回事实及 0/1 预期。
# 

# ### 流程：来源与精确待办转换
# 
# ```mermaid
# flowchart TD
# A["确认报告字段和频率"] --> B["来源日期、唯一性和数值门禁；应用偏移"]
# B --> C["逐个精确待办：有效值写事实；缺值记 0"]
# C --> D["非空输出一次业务验收；返回事实与计数"]
# ```
# 

# In[ ]:


def normalize_macro_release_response(
    response_rows: list[dict[str, object]],
    report_name: str,
    pending_df: pd.DataFrame,
    request_start_date: date,
    request_end_date: date,
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[tuple[str, date], int]]:
    log_started_at = time.perf_counter()
    log_phase = "normalize"
    log_source_rows = 0
    log_grid_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=normalize_macro_release_response; phase=normalize; status=started; "
        f"report={report_name}; source_rows={len(response_rows)}; pending_grids={len(pending_df)}; start_date={request_start_date}; end_date={request_end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        log_phase = "report_mapping"
        configured_series = SERIES_BY_REPORT.get(report_name)
        if configured_series is None:
            raise ValueError(f"未知 Eastmoney 宏观报告：{report_name}")
        required_fields = set(FIELDS_BY_REPORT[report_name])

        report_frequencies = {series.frequency for series in configured_series}
        if len(report_frequencies) != 1:
            raise ValueError(f'{report_name} 的共享频率不唯一。')
        report_frequency = next(iter(report_frequencies))
        values_by_date: dict[date, dict[str, float | None]] = {}
        log_phase = "source_rows"
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
            log_source_rows += 1
            if log_source_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=normalize_macro_release_response; phase=source_rows; status=running; "
                    f"processed_rows={log_source_rows}/{len(response_rows)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=normalize_macro_release_response; phase=source_rows; status=completed; "
            f"report={report_name}; checked_rows={log_source_rows}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        fact_rows = []
        outcome_counts: dict[tuple[str, date], int] = {}
        log_phase = "exact_grids"
        for pending_row in pending_df.to_dict("records"):
            log_grid_rows += 1
            if log_grid_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=normalize_macro_release_response; phase=exact_grids; status=running; "
                    f"processed_rows={log_grid_rows}/{len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
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

        log_phase = "output_validation"
        fact_df = pd.DataFrame(fact_rows, columns=MACRO_RELEASE_SCHEMA.names)
        if fact_df.empty:
            fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)
        else:
            fact_table = validate_macro_release_frame(fact_df, "Eastmoney 转换")
            fact_df = arrow_to_pandas(fact_table, MACRO_RELEASE_SCHEMA)

        click.echo(
            f"api_result: table={TABLE_NAME}; function=normalize_macro_release_response; phase=normalize; status=completed; "
            f"report={report_name}; source_rows={len(response_rows)}; requested_grids={len(outcome_counts)}; fact_rows={len(fact_df)}; empty_grids={len(outcome_counts) - len(fact_df)}; normalized=true; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return fact_df, outcome_counts
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=normalize_macro_release_response; phase={log_phase}; status=failed; "
            f"error={type(log_error).__name__}; report={report_name}; checked_source_rows={log_source_rows}; visited_grids={log_grid_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## dirty 完整事实叶与共享恢复
# 
# 每个报告窗口只合并当前事实月叶，保留其他报告。来源输出和 dirty 完整叶各验收一次，复用 Arrow 写 staging 并作物理及逐值复读。普通正式复读直达当前叶或零行标记，不扫描其他月份。
# 
# 旧版本迁移使用整根事务；普通事实提交的事务覆盖当前完整叶和必要的新标记；日历随后独立提交。共享模块负责安装、显式删除和失败恢复，业务验收仍在本环节，且正式复读必须在事务内部完成。成功退出事务才报告已提交。
# 

# ### 合并完整事实月分区
# 
# `full_fact_partition`：调用方传入已分组的当前月事实叶，保留未触达键并追加本次非空事实；不筛选或拼接历史全表，也不在此重复完整业务验收。写入时由提交函数验收 dirty 完整叶。
# 

# ### 流程：当前完整事实叶合并
# 
# ```mermaid
# flowchart TD
# A["已分组的当前月事实叶"] --> B["检查触达范围；删除触达旧键"]
# B --> C["追加新事实；保留其他报告"]
# C --> D["返回完整叶；提交时一次业务验收"]
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
        for series_code, report_date in touched_grids:
            if (report_date.year, report_date.month) != partition_key:
                raise ValueError("触达宏观发布格点越出指定事实叶分区。")

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
            empty_fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=full_fact_partition; phase=generate; status=completed; "
                f"partition={partition_key}; rows=0; touched_grids={len(touched_grids)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df
        log_phase = "merge_ready"
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


# ### 暂存事实与零行契约标记
# 
# `write_fact_staging`：复用已经验收的 Arrow 表及模块级文件 Schema，写零行标记与非空叶；没有重复 Pandas 转换。staging 完成仍不等于正式提交。
# 

# ### 流程：复用 Arrow 暂存
# 
# ```mermaid
# flowchart TD
# A["创建 staging；复用文件 Schema 写零行标记"] --> B{"Arrow 表非空？"}
# B -->|是| C["直接写当前年月叶"]
# B -->|否| D["保留可读零行 staging"]
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


# ### 旧版本事实整根迁移
# 
# 无日期写入时，兼容旧版本事实先进行一次当前业务验收，暂存并作物理及逐值复读。一个 `StagedPathTransaction` 替换整个事实表根，在事务内正式整表复读并逐值比较，成功退出后报告迁移已提交。纯描述性 metadata 差异不触发此路径。
# 
# 旧根与失败新根分别保存在本批 `.backup-<run_id>/_root` 和 `.failed-<run_id>/_root`。首次备份失败保留原根；安装或验收失败恢复原根，保留已安装的失败新根；恢复不完整另保留备份并抛出异常。暂存或事务进入失败也清理 staging。
# 

# ### 流程：旧版本整根共享事务
# 
# ```mermaid
# flowchart TD
# A["旧事实一次业务验收；整根暂存与逐值复读"] --> B["共享事务备份旧根并安装新根"]
# B --> C["事务内正式整表物理及逐值复读"]
# C --> D["成功退出；清理备份与 staging；报告迁移提交"]
# B -. 失败 .-> R["恢复旧根；保留失败新根；抛错"]
# C -. 失败 .-> R
# R --> S["恢复不完整另保留旧备份；staging 清理"]
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
        complete_table = validate_macro_release_frame(
            existing_fact_df.loc[:, FACT_COLUMNS], "待升级完整 宏观发布 事实",
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
                MACRO_RELEASE_SCHEMA,
                PARTITION_COLUMNS,
                "宏观发布 metadata 升级 staging",
            )
            staged_table = validate_arrow_table(
                staged_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
            ).sort_by(FACT_SORT_KEYS)
            if not staged_table.equals(complete_table):
                raise ValueError("宏观发布 staging 完整内容逐值复读失败。")
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
                    MACRO_RELEASE_SCHEMA,
                    PARTITION_COLUMNS,
                    "升级后的正式宏观发布事实",
                )
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
                ).sort_by(FACT_SORT_KEYS)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式 宏观发布 完整内容逐值复读失败。")
                committed_df = arrow_to_pandas(committed_table, MACRO_RELEASE_SCHEMA)
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


# ### 当前完整事实叶与新增标记共同提交
# 
# dirty 完整月叶业务验收一次，复用 Arrow 暂存并逐值比较。一个共享事务覆盖当前月叶及本次必要的新建 `schema.parquet`；已有标记保持原样，新标记失败时直接移除。非空叶安装 staging 叶，空叶以 `staged_path=None` 显式删除旧叶，不能从来源路径缺失推断删除。
# 
# 正式验收在事务内直读当前叶或零行标记，新建标记另确认零行。成功退出才报告事实已提交；日历随后独立提交。本函数不写日期水位。失败时按实际移动倒序恢复当前叶和新标记，失败新叶隔离留存，恢复不完整另保留旧备份；staging 清理，不递归删除正式表根，未触达叶与其他文件保留。
# 

# ### 流程：事实叶与新增标记共享事务
# 
# ```mermaid
# flowchart TD
# A["dirty 完整叶验收一次；暂存及逐值复读"] --> B["共享事务内安装必要的新标记"]
# B --> C{"事实非空？"}
# C -->|是| D["备份旧叶；安装新叶"]
# C -->|否| E["显式删除旧叶"]
# D --> F["事务内直读当前叶或标记；物理及逐值验收"]
# E --> F
# F --> G["成功退出；报告事实提交；日历后续独立提交"]
# B -. 失败 .-> R["倒序恢复叶与新标记；保留失败新叶；抛错"]
# D -. 失败 .-> R
# E -. 失败 .-> R
# F -. 失败 .-> R
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
        complete_table = validate_macro_release_frame(
            frame.loc[:, FACT_COLUMNS], "待提交完整 宏观发布 事实分区",
        )
        if complete_table.num_rows:
            actual_keys = set(zip(*(complete_table[name].to_pylist() for name in PARTITION_COLUMNS), strict=True))
            if actual_keys != {partition_key}:
                raise ValueError("待提交 宏观发布 内容越出指定事实叶分区。")

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
                MACRO_RELEASE_SCHEMA,
                PARTITION_COLUMNS,
                "宏观发布事实 staging",
            )
            staged_table = validate_arrow_table(
                staged_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
            ).sort_by(FACT_SORT_KEYS)
            if not staged_table.equals(complete_table):
                raise ValueError("宏观发布 staging 完整内容逐值复读失败。")
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

        marker_created = False
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
                if not target_marker_path.exists():
                    transaction.replace(
                        target_path=target_marker_path,
                        staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                    marker_created = True
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if complete_table.num_rows else None,
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
                    raise ValueError("确认空的正式 宏观发布 事实叶仍然存在。")
                committed_dataset = open_exact_dataset(
                    destination_path if complete_table.num_rows else target_marker_path,
                    FACT_PARTITIONING, MACRO_RELEASE_SCHEMA, PARTITION_COLUMNS,
                    "正式宏观发布事实", partition_base_dir=target_path,
                )
                if marker_created and complete_table.num_rows:
                    marker_dataset = open_exact_dataset(
                        target_marker_path, FACT_PARTITIONING, MACRO_RELEASE_SCHEMA,
                        PARTITION_COLUMNS, "新建正式宏观发布零行标记", partition_base_dir=target_path,
                    )
                    if marker_dataset.count_rows() != 0:
                        raise ValueError("新建正式 宏观发布 标记必须为零行。")
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
                ).sort_by(FACT_SORT_KEYS)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式 宏观发布 完整内容逐值复读失败。")
                committed_df = arrow_to_pandas(committed_table, MACRO_RELEASE_SCHEMA)
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


# ## 当前日历叶的状态与提交
# 
# 状态生成只接收当前完整日历叶。正式事实 1 行对应 success/passed，完整来源且正式复读 0 行对应 empty_confirmed/warning，来源失败保持未完成。只在提交前对 dirty 日历叶执行业务验收一次，staging 和正式路径直读当前叶并逐值比较；根级标记与其他叶保持原样。
# 

# ### 生成完成状态
# 
# `apply_calendar_completion`：只复制并修改当前日历叶，保留其他报告状态；生成后不重复业务验收，提交函数承担 dirty 叶验收。内存完成状态仍 persisted=false。
# 

# ### 流程：当前叶生成完成状态
# 
# ```mermaid
# flowchart TD
# A["当前日历叶与正式 0/1 计数"] --> B["只更新触达格点和审计字段"]
# B --> C["返回内存状态；尚未提交"]
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
            log_updated_rows += 1

        log_phase = "state_ready"
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


# ### 生成失败状态
# 
# `apply_calendar_failure`：只修改当前叶的触达格点；不重验全部日历。仍保持未完成及失败原因，后续提交负责验收，保存失败状态不代表采集完成。
# 

# ### 流程：当前叶生成失败状态
# 
# ```mermaid
# flowchart TD
# A["复制当前完整日历叶"] --> B["只修改触达格点：未完成、失败状态与审计字段"]
# B --> C["返回内存叶；由提交函数验收"]
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
            raise ValueError("宏观发布 失败状态只能是 retryable_error 或 permanent_error。")

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

        log_phase = "state_ready"
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


# ### 当前日历完整叶独立提交
# 
# 上游当前 `macro_release/year/month` 叶必须已经存在；c03 不创建或迁移日历根。dirty 日历叶业务验收一次，暂存后作物理及逐值复读。一个独立共享事务安装当前日历叶，并在事务内正式复读与逐值比较。
# 
# 成功退出才报告 `calendar_state=committed; collection_completion=per_grid; persisted=true`，表示状态已经保存；失败状态保存不等于采集完成。既有日历根标记、其他月份和 `interest_rate` 叶保持原样。日历失败仅恢复当前日历叶，已成功事实保留，下次人工运行可无 API 修复。失败新叶隔离留存，恢复不完整另保留备份，staging 清理；没有独立日期水位或新增业务重试。
# 

# ### 流程：当前日历叶独立共享事务
# 
# ```mermaid
# flowchart TD
# A["确认上游叶存在；dirty 叶一次业务验收"] --> B["暂存当前叶；物理及逐值比较"]
# B --> C["独立共享事务备份旧叶、安装新叶"]
# C --> D["事务内直读正式叶；物理及逐值验收"]
# D --> E["成功退出；报告状态已保存"]
# C -. 失败 .-> R["恢复当前日历叶；此前成功事实保留；抛错"]
# D -. 失败 .-> R
# R --> S["失败新叶隔离；恢复不完整保留备份；staging 清理"]
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

        partition_table = validate_macro_calendar_table(
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
        destination_path = target_path / relative_path
        if not destination_path.is_dir():
            raise FileNotFoundError(f"正式宏观发布日历缺少待回写完整叶：{relative_path}")
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


# ## CLI：一次对账、逐报告叶处理和提交证据
# 
# 先检查日期及正式湖写入边界，读取两表、按需迁移兼容旧事实版本，并对账一次。分区映射和空事实表在循环前准备。无 API 修复先更新对应已提交日历叶；没有 API 待办则不创建会话。
# 
# 每个年月—报告窗口使用当前事实和日历叶，来源失败可回写本叶失败状态并继续，批末汇总失败；提交异常立即停止。事实先提交并核对触达计数，日历随后独立提交，两者成功后更新叶映射与完成计数。同月不同报告不会覆盖此前结果。只读模式仍请求和转换但不提交。
# 
# 成功批末复用各叶正式验收结果，不重读全历史或重验累计事实。读取、分页、生成、校验及提交日志继续由函数负责，main 保留参数、模式、失败分类和批次汇总；没有独立日期水位文件。
# 

# ### 流程：一次规划与当前叶处理
# 
# ```mermaid
# flowchart TD
# A["参数门禁；读取、按需迁移及一次规划"] --> B["建立叶映射；write 时无 API 修复日历"]
# B --> C{"有 API 待办？"}
# C -->|否| Z["结束；不创建会话"]
# C -->|是| D["逐报告窗口分页、转换与当前叶合并"]
# D --> E{"write？"}
# E -->|否| F["只读；继续窗口"]
# E -->|是| G["事实提交及计数核对；日历独立提交"]
# G --> H["更新叶映射与完成数；后续报告继承"]
# H --> I["批末复用证据；关闭会话"]
# D -. 来源失败 .-> R["write 时保存失败状态；继续；批末抛错"]
# G -. 提交失败 .-> X["共享模块恢复当前目标；立即停止；关闭会话"]
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

        log_phase = "read_calendar"
        calendar_df = read_macro_calendar(calendar_path)
        log_phase = "read_fact"
        existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)

        if not fact_metadata_is_exact:
            if has_explicit_dates:
                raise click.UsageError(
                    "现有宏观发布事实 metadata 已过期；"
                    "请先移除日期参数执行一次完整自动升级。"
                )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=metadata_plan; status=completed; "
                f"rows={len(existing_fact_df)}; persisted=false; message=metadata_upgrade_required; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if write:
                log_phase = "metadata_upgrade"
                existing_fact_df = upgrade_fact_metadata(
                    existing_fact_df,
                    resolved_lake_root,
                )

        log_phase = "plan"
        pending_df, repair_df, complete_count = plan_macro_release_grids(
            calendar_df, existing_fact_df, requested_start_date, requested_end_date,
        )
        fact_leaves = {tuple(key): frame for key, frame in existing_fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True)}
        calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True)}
        empty_fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)

        # 正式事实已经存在的格点只修复日历，不创建 Eastmoney 会话。
        if not repair_df.empty:
            repair_count = len(repair_df)
            if write:
                repair_run_id = uuid.uuid4().hex
                repair_time = datetime.now(timezone.utc)
                for key, repair_leaf_df in repair_df.groupby(["year", "month"], sort=True, observed=True):
                    calendar_partition_key = (DATASET_NAME, int(key[0]), int(key[1]))
                    repair_counts = {(row.series_code, row.report_date): 1 for row in repair_leaf_df.itertuples(index=False)}
                    repair_reasons = {key: STATE_REPAIR_REASON for key in repair_counts}
                    log_phase = "completion_state"
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
                f"reason=no_api_pending; write={str(write).lower()}; state_repair_pending={len(repair_df)}; message=Eastmoney session not created; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return

        pending_with_report_df = pending_df.copy()
        pending_with_report_df["source_api"] = pending_with_report_df[
            "series_code"
        ].map(lambda series_code: SERIES_BY_CODE[series_code].source_api)

        log_phase = "create_session"
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
                calendar_partition_key = (DATASET_NAME, *partition_key)
                calendar_leaf_df = calendar_leaves[calendar_partition_key]
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
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
                    log_phase = "request"
                    response_rows = query_eastmoney_report_range(
                        session,
                        str(report_name),
                        request_start_date,
                        request_end_date,
                    )
                    log_phase = "normalize"
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
                        f"planning_progress: table={TABLE_NAME}; function=main; phase=api_window; status=failed; "
                        f"partition={partition_key}; report={report_name}; result_status={failure_status}; grids={len(touched_grids)}; reason={failure_reason}; persisted=false; business_window_retry=false; "
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
                        log_phase = "calendar_commit"
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
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=main; phase=fact_count_check; status=started; "
                    f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                log_phase = "completion_state"
                calendar_leaf_df = apply_calendar_completion(
                    calendar_leaf_df,
                    committed_touched_counts,
                    completion_reasons,
                    fetch_run_id,
                    datetime.now(timezone.utc),
                )
                log_phase = "calendar_commit"
                calendar_leaves[calendar_partition_key] = commit_calendar_partition(
                    calendar_leaf_df,
                    resolved_lake_root,
                    calendar_partition_key,
                )

                fact_leaves[partition_key] = committed_partition_df
                complete_count += len(touched_grids)
        finally:
            session.close()

        log_phase = "batch_result"
        if failed_window_count:
            raise click.ClickException(
                f"{failed_window_count} 个 Eastmoney 宏观报告窗口失败；"
                "成功分区已提交，失败格点保持未完成。"
            )

        if not write:
            click.echo(
                f"dry_run: table={TABLE_NAME}; function=main; phase=run; status=completed; "
                f"write=false; persisted=false; message=API 响应已转换和质检，未写事实或日历; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return

        # 各 dirty 叶已经正式验收；复用本批提交结果，不再扫描 clean 历史。
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
# 与 b01/c01 同样：交互内核且没有 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取 PyCharm/Jupyter 的 `-f` 参数。默认空参数不带 `--write`，自动读取和求差；有待办仍请求 Eastmoney 并转换，但不提交事实或日历。
# 
# 终端直接执行 `.py` 正常读取命令行参数，显式 `--write` 才提交；日期与正式湖门禁仍由 main 执行。普通导入或内核中导入同名模块均不启动业务或 Schema 浏览。
# 

# ### 流程：Notebook 与脚本入口
# 
# ```mermaid
# flowchart TD
# A{"交互内核且无脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
# B --> C["默认空参数；自动求差；有待办请求 API；不写湖"]
# A -->|否| D{"脚本直接运行？"}
# D -->|是| E["main 读取命令行参数"]
# D -->|否| F["模块导入；不启动入口"]
# ```
# 

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="c03_macro_release",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()

