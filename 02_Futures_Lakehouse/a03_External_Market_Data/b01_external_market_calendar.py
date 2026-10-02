#!/usr/bin/env python
# coding: utf-8

# # b01 外部市场数据采集日历
# 
# 从上游中国自然日历和共享请求实体配置生成 `dim_external_market_calendar`，为三个下游入口提供日期—请求实体格点。本入口只读取和计算本地数据，不调用生意社、JQData 或 Eastmoney。
# 
# | 上下游 | 与本环节的关系 |
# | --- | --- |
# | a01/b01 `dim_trade_calendar` | 唯一上游维度；提供当前有效自然日、周几和中国期货交易日标记。 |
# | `config/futures_lakehouse/external_market_entities.py` | 请求实体、配置版本、指数有效期和下游指数映射的唯一来源。 |
# | a03/b02 生意社原文归档 | 消费 `domestic_spot_basis/ALL`；成功原样归档后回写 `success + passed`、产物数 1，不生产结构化基差事实。 |
# | a03/b03 境外期货 | 消费 `overseas_futures/ALL`，按日请求整表；负责事实计数、确认空和质量状态回写。 |
# | a03/b04 外部指数 | 消费 `external_index/INDICATOR_ID`；负责指数事实和状态回写，以及退出 required 范围的旧事实清退。 |
# | operations | 人工批次中先运行本日历，再运行 a03/b02—b04；识别计划和运行进度日志。 |
# 
# 本环节不读取下游 raw 或事实来重新证明完成状态。字段、主键和分区来自 `config/data_contracts.py`；运行边界见湖仓 `README.md`、`AGENTS.md` 与数据库 `AGENTS.md`。

# ## 自动更新、状态继承与写入边界
# 
# 自动模式从配置起点到上游当前有效自然日展开完整理论格点，再比较现有日历；新增、历史缺口、规则变化和上游撤销都进入差异计划。周末、非中国交易日或指数有效期外的格点仍保留，以 `is_fetch_required=false` 表达无需请求。
# 
# | 运行方式 | 当前行为 |
# | --- | --- |
# | 默认不带参数 | 完整生成与比较，只输出计划；不调用 API，不提交。 |
# | `--write` | 提交变化的 `dataset_name/year/month` 完整叶；全部验收通过才报告本次提交完成。 |
# | 成对日期 | 只重建范围内格点，范围外旧行带入完整期望；只读可用，写入必须选择非正式测试湖。 |
# | 物理兼容而 metadata 过期 | 以完整期望表 staging 并整根替换，避免新旧 metadata 混用。 |
# | 没有变化 | 直接报告无需更新；日历状态和文件保持原样。 |
# 
# 只有 `is_fetch_required` 与带配置版本的 `requirement_reason` 均不变时，才逐字段继承下游状态及原 `updated_at`；选择语义变化时使用新规则初始化状态。日历行存在、当前应采集和下游已完成是三个不同概念。
# 
# `--start-date`、`--end-date` 必须成对；没有 `--full`。正式路径来自 `settings.futures_lake_root`，不得用显式日期写正式湖。目标为空时由同一入口建表，上游为空时完整期望也可为空。本入口没有独立日期水位文件。

# ## 总流程：外部市场日历总流程
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["参数、正式写入边界；全程不调用 API"] --> B["读取上游有效自然日与已有外部日历"]
#     B --> C["自然日乘请求实体；生成完整理论格点"]
#     C --> D["选择规则不变时继承下游状态"]
#     D --> E["显式范围保留范围外旧行；比较完整叶"]
#     E --> F{"无变化且无需 metadata 迁移？"}
#     F -->|是| G["无需更新"]
#     F -->|否| H{"启用 --write？"}
#     H -->|否| I["只读计划结束"]
#     H -->|是| J["staging；普通变化叶或完整迁移表"]
#     J --> K["同一共享事务：全部变化叶或整根替换"]
#     K --> L["正式整表验收；成功后报告日历落盘"]
#     K -. 失败 .-> M["逐项恢复本批旧目标；保留失败新数据；抛错"]
#     L -. 失败 .-> M
#     L --> N["下游 b02—b04 再消费 required 格点并回写状态"]
# ```

# ## 初始化与共享依赖
# 
# 按项目规定的三个标记定位根目录，导入上游与输出权威 Schema、转换函数、共享实体配置、项目设置和共享路径事务。此单元格只加载依赖，不读取业务表、不请求网络、不写入数据。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
#     B -->|是| C["加入项目与湖仓模块路径；加载契约、配置和共享事务"]
#     B -->|否| D["抛错"]
#     C --> E["仅加载依赖；不读取业务表或写入"]
# ```

# In[ ]:


from __future__ import annotations

import hashlib
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

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.futures_lakehouse.external_market_entities import (
    DOMESTIC_SPOT_BASIS_ENTITY_CODE,
    EXTERNAL_INDEX_ENTITIES,
    EXTERNAL_MARKET_ENTITY_CONFIG_VERSION,
    OVERSEAS_FUTURES_ENTITY_CODE,
)
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 仅在交互内核且没有 `__file__` 时展示上游自然日历与当前外部市场日历。字段解释来自权威 Schema；传入 `lake_root` 后，用户显式选择样例时会进行有界本地读取。
# 
# 本格不调用来源 API、不创建数据或回写状态；普通脚本执行跳过展示。

# ### 局部流程：Schema 与本地样例
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且无 __file__？"} -->|否| B["跳过展示"]
#     A -->|是| C["展示上游与输出权威 Schema"]
#     C --> D["用户显式选择时读取有界本地样例"]
#     D --> E["不请求来源 API；不回写状态"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA,
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区与请求实体
# 
# 表名、主键和分区从权威 Schema metadata 各读取一次；输出主键是数据集—请求实体—观测日期，叶分区为 `dataset_name/year/month`。同一个指数月叶包含全部配置指数，提交必须保留叶内其他实体。
# 
# `POLICY_COLUMNS` 决定是否继承旧状态，`STATE_COLUMNS` 列出必须共同继承的完成、质量和审计字段。生意社与境外期货请求实体为 `ALL`，指数为来源 `INDICATOR_ID`；实体和有效期只从共享配置读取。

# ### 局部流程：契约和实体常量
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两张权威 Schema"] --> B["读取表名、主键和分区；建立 Hive partitioning"]
#     B --> C["共享配置提供 ALL 和指数请求实体"]
#     C --> D["区分选择字段与完整状态继承字段"]
# ```

# In[ ]:


# 表名、主键和 Hive 分区只从权威 Schema metadata 读取一次。
TABLE_NAME = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 外部市场数据采集日历维度表。
PRIMARY_KEY = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 数据集—请求实体—观测日期格点。
PARTITION_COLUMNS = EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 外部市场日历 Hive 叶分区顺序。

UPSTREAM_TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 中国期货交易日历维度表，也是本表唯一上游维度。
UPSTREAM_PRIMARY_KEY = TRADE_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一个中国自然日。
UPSTREAM_PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 上游自然日历 Hive 分区顺序。

# 数据集枚举来自字段 metadata；指数请求实体来自项目级共享配置。
DATASET_NAMES = tuple(
    EXTERNAL_MARKET_CALENDAR_SCHEMA.field("dataset_name")
    .metadata[b"enum_values_zh"]
    .decode("utf-8")
    .split("、")
)
CONFIGURED_INDEX_IDS = {
    entity.source_indicator_id
    for entity in EXTERNAL_INDEX_ENTITIES
}

POLICY_COLUMNS = [
    "is_fetch_required",  # 当前理论格点是否应由相应下游产物入口请求。
    "requirement_reason",  # 当前工作日、交易日、有效期和配置版本说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 请求、下游产物提交及正式复读是否完成。
    "fetch_result_status",  # 最近一次下游产物采集结果。
    "is_data_missing",  # 允许确认空的事实源是否判为应有而缺失。
    "actual_record_count",  # 正式下游产物复读数量；生意社 raw 成功固定为 1。
    "quality_status",  # 格点综合质量状态。
    "quality_reason",  # 格点质量结论的中文说明。
    "fetch_run_id",  # 最近一次下游产物采集批次号。
    "fetch_completed_at",  # 最近一次下游产物完成时间。
    "quality_checked_at",  # 最近一次质量检查时间。
    "updated_at",  # 本行任一业务状态最后变化时间。
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
        EXTERNAL_MARKET_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        TRADE_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## Dataset 物理兼容与精确 metadata 读取
# 
# `open_compatible_dataset()` 枚举文件后，只遍历一次 fragments，同时检查字段、类型、nullable 和表名/主键/分区身份，并累计 metadata 是否精确匹配。Schema 汇总重建、身份键和文件 Schema 都在 fragment 循环前准备。
# 
# 上游 `dim_trade_calendar` 接受物理与身份兼容的描述性 metadata 差异；其已提交业务语义由 a01/b01 保证。已有外部日历仍允许物理兼容但 metadata 过期，以维持既有整根迁移规则。`open_exact_dataset()` 用于 staging 和正式安装后的精确验收。
# 
# 两个打开函数自行报告文件数、fragment 检查及失败阶段；`materialized=false` 表示尚未读取记录。实际物化日志仍由调用处报告。

# ### 局部流程：兼容与精确契约读取
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告打开开始；枚举 Parquet 和检查契约"] --> B["一次检查汇总和 fragments 的物理结构与表身份"]
#     B --> C{"物理兼容？"}
#     C -->|否| D["拒绝读取"]
#     C -->|是| E["同一遍历累计 metadata 精确匹配结果"]
#     E --> F{"调用方要求精确？"}
#     F -->|否| G["报告打开完成、metadata 结论；尚未物化"]
#     F -->|是| H{"metadata 精确？"}
#     H -->|是| I["报告精确打开完成；尚未物化"]
#     H -->|否| D
# ```

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




def open_compatible_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> tuple[ds.Dataset, bool]:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_table_name = TABLE_NAME if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA else UPSTREAM_TABLE_NAME
    log_checked_fragments = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 读取 metadata 过期表前，先逐 fragment 证明物理结构仍可无损迁移。
        log_phase = "discovery"
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else []
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
        )
        log_phase = "schema"
        expected_names = set(schema.names)
        actual_schema = reconstructed_schema(dataset, schema)
        identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
        if (
            len(dataset.schema.names) != len(schema.names)
            or set(dataset.schema.names) != expected_names
            or not physical_schema_matches(
                actual_schema,
                schema,
            )
        ):
            raise TypeError(f"{label}物理字段、类型或 nullable 与权威契约不兼容。")

        if any(
            (dataset.schema.metadata or {}).get(key) != schema.metadata[key]
            for key in identity_metadata_keys
        ):
            raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不兼容。")
        expected_file_schema = pa.schema(
            [field for field in schema if field.name not in partition_columns],
            metadata=schema.metadata,
        )
        is_exact = actual_schema.equals(schema, check_metadata=True)
        log_phase = "fragment_schema"
        for fragment in dataset.get_fragments():
            fragment_schema = fragment.physical_schema
            if not physical_schema_matches(
                fragment_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label}存在物理结构不兼容的 Parquet fragment：{fragment.path}"
                )
            if any(
                (fragment_schema.metadata or {}).get(key) != schema.metadata[key]
                for key in identity_metadata_keys
            ):
                raise TypeError(f"{label}存在表身份 metadata 不兼容的 fragment：{fragment.path}")
            is_exact = is_exact and fragment_schema.equals(expected_file_schema, check_metadata=True)
            log_checked_fragments += 1
            if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=fragment_schema; status=running; "
                    f"label={label}; checked_fragments={log_checked_fragments}; discovered_files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "metadata_exactness"
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=dataset_open; status=completed; "
            f"label={label}; checked_fragments={log_checked_fragments}; metadata_exact={str(is_exact).lower()}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset, is_exact
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_compatible_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; checked_fragments={log_checked_fragments}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_table_name = TABLE_NAME if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA else UPSTREAM_TABLE_NAME
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # staging 与提交后输出必须逐 fragment 精确匹配当前 metadata。
        dataset, is_exact = open_compatible_dataset(
            table_path,
            partitioning,
            schema,
            (
                PARTITION_COLUMNS
                if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA
                else UPSTREAM_PARTITION_COLUMNS
            ),
            label,
        )
        log_phase = "metadata_exactness"
        if not is_exact:
            raise TypeError(f"{label} Schema/metadata 与权威契约不一致。")

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; metadata_exact=true; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise




# ## 信任上游正式业务证明
# 
# 自然日历主键、日期连续性、year、weekday 和周末关系由 a01/b01 提交时保证，本环节不再逐项复算。`main()` 直接物化选择范围、调用一次 `arrow_to_pandas()` 固定契约表示并排序，报告实际读取行数；不另设只包装转换的校验函数。

# ### 局部流程：可信上游读取
# 
# ```mermaid
# flowchart TD
#     A["物理与身份兼容的正式自然日历"] --> B["main 按范围物化；一次权威转换"]
#     B --> C["排序并报告读取行数；信任上游业务证明"]
# ```

# In[ ]:


# 上游物化与权威转换直接在 main() 执行，信任 a01/b01 的正式业务证明。


# ## 生成结果的一次完整业务验收
# 
# `build_expected_calendar()` 先执行一次 `pandas_to_arrow()`，再由 `validate_external_calendar_table()` 检查主键、请求实体、选择/完成状态、计数和时间。校验函数只消费已经契约化的 Arrow 表，不再重复转换。审计字段和状态集合在逐行循环前建立，进度仍沿原循环报告。
# 
# 生意社新完成结果必须为 `success + passed`、数量 1 且不缺失；其他来源保留确认空和 warning 语义。历史旧生意社行只作为可信已有行读取，规则原因变化后由生成逻辑重置；不再给新输出验收设置历史宽容开关。范围外旧行沿用正式证明，显式范围合并不再重跑全表业务校验。

# ### 局部流程：生成结果业务校验
# 
# ```mermaid
# flowchart TD
#     A["生成函数完成一次权威 Arrow 转换"] --> B["一次主键、枚举、实体与状态自洽检查"]
#     B --> C["一次计数与审计时间检查；沿行报告进度"]
#     C --> D["严格生意社 raw 完成规则；排序返回"]
# ```

# In[ ]:


def validate_external_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_checked_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=validate_external_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 调用方已完成权威 Arrow 转换；此处只验证外部日历业务语义。
        log_phase = "convert"
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)

        log_phase = "primary_key"
        if frame.duplicated(PRIMARY_KEY).any():
            raise ValueError(f"{context}外部市场日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        allowed_dataset_names = set(DATASET_NAMES)

        log_phase = "business_validation"
        audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
        completed_statuses = {"success", "empty_confirmed"}
        checked_quality_statuses = {"passed", "warning", "failed"}
        for row in table.to_pylist():
            if row["dataset_name"] not in allowed_dataset_names:
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
                row["observation_date"].year != row["year"]
                or row["observation_date"].month != row["month"]
            ):
                raise ValueError(f"{context}year/month 与 observation_date 不一致。")
            if row["actual_record_count"] < 0:
                raise ValueError(f"{context}实际记录数不得为负。")

            # 请求实体必须与下游原始页面归档或事实 API 的实际调用粒度一致。
            if row["dataset_name"] == "domestic_spot_basis":
                if row["entity_code"] != DOMESTIC_SPOT_BASIS_ENTITY_CODE:
                    raise ValueError(f"{context}生意社原始页面请求实体必须为 ALL。")
                if (
                    row["fetch_result_status"] == "empty_confirmed"
                ):
                    raise ValueError(f"{context}生意社原始页面归档禁止 empty_confirmed。")
            elif row["dataset_name"] == "overseas_futures":
                if row["entity_code"] != OVERSEAS_FUTURES_ENTITY_CODE:
                    raise ValueError(f"{context}境外期货请求实体必须为 ALL。")
            elif row["entity_code"] not in CONFIGURED_INDEX_IDS:
                raise ValueError(f"{context}外部指数请求实体未命中项目配置。")

            # 无需请求的理论格点仍保留，但执行、计数与审计状态必须完全自洽。
            if not row["is_fetch_required"]:
                if row["fetch_result_status"] != "not_required":
                    raise ValueError(f"{context}无需请求格点必须为 not_required。")
                if row["is_fetch_completed"] or row["is_data_missing"]:
                    raise ValueError(f"{context}无需请求格点不得标记完成或数据缺失。")
                if row["actual_record_count"] != 0:
                    raise ValueError(f"{context}无需请求格点的记录数必须为 0。")
                if row["quality_status"] != "not_applicable":
                    raise ValueError(f"{context}无需请求格点必须为 not_applicable。")
                if any(
                    row[name] is not None
                    for name in audit_fields
                ):
                    raise ValueError(f"{context}无需请求格点不得保留下游产物运行审计值。")
            elif row["fetch_result_status"] == "not_required":
                raise ValueError(f"{context}需请求格点不得标为 not_required。")

            # 只有下游产物已经正式提交并复读，或允许确认空的事实源明确确认空时才完成。
            is_completed_status = row["fetch_result_status"] in completed_statuses
            if row["is_fetch_completed"] != is_completed_status:
                raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
            if row["is_fetch_completed"]:
                if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                    raise ValueError(f"{context}完成格点缺少批次或完成时间。")
            elif row["fetch_completed_at"] is not None:
                raise ValueError(f"{context}未完成格点不得具有完成时间。")

            if row["fetch_result_status"] == "success":
                if row["actual_record_count"] <= 0 or row["is_data_missing"]:
                    raise ValueError(f"{context}success 必须有正式下游产物且不得标记缺失。")
            if row["fetch_result_status"] == "empty_confirmed" and row["actual_record_count"] != 0:
                raise ValueError(f"{context}empty_confirmed 的正式事实计数必须为 0。")
            if row["is_data_missing"] and row["fetch_result_status"] != "empty_confirmed":
                raise ValueError(f"{context}数据缺失只能来自确认空响应。")

            # 生意社只归档原始响应：正式文件字节数与 SHA-256 复读一致后固定完成一项。
            if (
                row["dataset_name"] == "domestic_spot_basis"
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
                    f"planning_progress: table={TABLE_NAME}; function=validate_external_calendar_table; phase=validate; status=running; "
                    f"context={context}; checked_rows={log_checked_rows}/{table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "sort"
        validated_external_calendar_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_external_calendar_table; phase=validate; status=completed; "
            f"context={context}; checked_rows={log_checked_rows}; rows={len(validated_external_calendar_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_external_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_external_calendar_table; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; checked_rows={log_checked_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 生成完整理论格点并继承状态
# 
# `build_expected_calendar()` 为每个自然日生成生意社 `ALL`、境外期货 `ALL` 和全部配置指数格点。生意社按中国期货交易日请求，境外期货按普通工作日请求，指数同时满足普通工作日与配置有效期；无需请求日仍保留为 `not_required`。
# 
# 现有行按主键索引。两项选择语义都未变化时继承完整 `STATE_COLUMNS`，包括成功、确认空、warning、错误与原更新时间；规则变化则初始化为当前的 pending 或 not_required。返回前完整验证生成结果；生成日历不表示下游已经采集完成。
# 
# `build_expected_calendar()` 自行报告索引、上游记录准备、格点展开与输出校验。每处理 100 个自然日检查一次 2 秒进度间隔，报告已处理日期、生成行和继承状态行数；空输入也有开始与完成。生成完成明确标记 `persisted=false`。
# 
# 已有行和上游行直接使用当前 DataFrame 的记录表示，不为建立 Python 字典往返 Arrow。配置版本前缀与各指数固定请求原因在日期循环前准备。

# ### 局部流程：理论格点与状态继承
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告生成开始；按主键索引现有行"] --> B["逐自然日读取交易日与周几"]
#     B --> C["生成生意社、境外期货及全部指数请求实体"]
#     C --> D["依据交易日、工作日和有效期设置 required 与原因"]
#     D --> E{"现有行两项选择语义不变？"}
#     E -->|是| F["逐字段继承下游状态及原 updated_at"]
#     E -->|否| G["初始化 pending 或 not_required"]
#     F --> H["报告已生成/继承数量；完整业务验收"]
#     G --> H
#     H --> I["报告生成完成；persisted=false"]
# ```

# In[ ]:


def build_expected_calendar(
    upstream_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"
    log_processed_dates = 0
    log_inherited_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=started; "
        f"upstream_dates={len(upstream_df)}; existing_rows={len(existing_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 现有正式行的业务语义由提交保证，可按权威主键索引并继承状态。
        log_phase = "existing_index"
        existing_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in existing_df.to_dict(orient="records")
        }

        expected_rows = []
        log_phase = "upstream_records"
        upstream_rows = upstream_df.to_dict(orient="records")
        config_reason_prefix = f"外部市场请求实体配置 v{EXTERNAL_MARKET_ENTITY_CONFIG_VERSION}；"
        index_required_reasons = {
            entity.source_indicator_id: (
                f"普通工作日，按 INDICATOR_ID={entity.source_indicator_id} 请求 Eastmoney 指数。"
            )
            for entity in EXTERNAL_INDEX_ENTITIES
        }

        log_phase = "expand"
        for upstream_row in upstream_rows:
            observation_date = upstream_row["calendar_date"]
            weekday = upstream_row["weekday"]
            is_weekday = weekday <= 5

            request_entities = [
                (
                    "domestic_spot_basis",
                    DOMESTIC_SPOT_BASIS_ENTITY_CODE,
                    bool(upstream_row["is_trading_day"]),
                    "中国期货交易日，按日期请求并原样归档生意社整页响应，不解析 HTML。"
                    if upstream_row["is_trading_day"]
                    else "非中国期货交易日，保留理论格点但不请求生意社页面。",
                ),
                (
                    "overseas_futures",
                    OVERSEAS_FUTURES_ENTITY_CODE,
                    is_weekday,
                    "普通工作日，按日期请求 JQData FUT_GLOBAL_DAILY 整张表。"
                    if is_weekday
                    else "周末，保留理论格点但不请求 JQData FUT_GLOBAL_DAILY。",
                ),
            ]

            for entity in EXTERNAL_INDEX_ENTITIES:
                in_active_period = (
                    (entity.active_from is None or observation_date >= entity.active_from)
                    and (entity.active_to is None or observation_date <= entity.active_to)
                )
                is_fetch_required = is_weekday and in_active_period

                if not is_weekday:
                    reason = "周末，保留理论格点但不请求 Eastmoney 指数。"
                elif not in_active_period:
                    reason = "不在该指数配置有效期内，保留理论格点但不请求。"
                else:
                    reason = index_required_reasons[entity.source_indicator_id]

                request_entities.append((
                    "external_index",
                    entity.source_indicator_id,
                    is_fetch_required,
                    reason,
                ))

            for (
                dataset_name,
                entity_code,
                is_fetch_required,
                selection_reason,
            ) in request_entities:
                requirement_reason = config_reason_prefix + selection_reason

                if is_fetch_required:
                    fetch_result_status = "pending"
                    quality_status = "pending"
                    quality_reason = "等待相应 raw 归档或事实生产者请求、提交并正式复读。"
                else:
                    fetch_result_status = "not_required"
                    quality_status = "not_applicable"
                    quality_reason = "当前规则无需请求；保留完整理论日历格点。"

                row = {
                    "dataset_name": dataset_name,
                    "entity_code": entity_code,
                    "observation_date": observation_date,
                    "is_fetch_required": is_fetch_required,
                    "requirement_reason": requirement_reason,
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
                    "year": observation_date.year,
                    "month": observation_date.month,
                }

                key = tuple(row[name] for name in PRIMARY_KEY)
                existing_row = existing_rows_by_key.get(key)

                # 选择语义未变化时，下游产物状态和原 updated_at 必须逐字段继承。
                if existing_row is not None and all(
                    existing_row[name] == row[name]
                    for name in POLICY_COLUMNS
                ):
                    for name in STATE_COLUMNS:
                        row[name] = existing_row[name]
                    log_inherited_rows += 1

                expected_rows.append(row)
            log_processed_dates += 1
            if log_processed_dates % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=running; "
                    f"processed_dates={log_processed_dates}/{len(upstream_rows)}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "output_frame"
        if expected_rows:
            candidate_df = pd.DataFrame(
                expected_rows,
                columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
            )
        else:
            candidate_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)

        log_phase = "output_validation"
        expected_calendar_table = pandas_to_arrow(candidate_df, EXTERNAL_MARKET_CALENDAR_SCHEMA)
        expected_calendar_df = validate_external_calendar_table(expected_calendar_table, "期望")
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=completed; "
            f"processed_dates={log_processed_dates}; generated_rows={len(expected_calendar_df)}; inherited_rows={log_inherited_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return expected_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; processed_dates={log_processed_dates}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完整内容摘要
# 
# `table_digest()` 直接接收已按权威 Schema 转换的 Arrow 表，按主键排序后仍从 Python 标量重建缓冲区，消除 fragment 切片布局差异，再计算完整 IPC SHA-256。摘要包括状态和更新时间；保留原摘要格式，不改成只比较行数或主键。调用方在分区循环前转换整表，逐叶只取行。

# ### 局部流程：完整内容摘要
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["已转换 Arrow 表按主键排序"] --> B["保留标量重建；统一缓冲区"]
#     B --> C["固定 Schema 的 IPC 内容"]
#     C --> D["SHA-256；包含业务状态与更新时间"]
# ```

# In[ ]:


def table_digest(calendar_table: pa.Table) -> str:
    # Arrow IPC 固定权威列顺序、类型、metadata 和主键排序后再生成摘要。
    ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])
    # 从 Python 标量按权威 Schema 重建缓冲区，消除不同 fragment 的切片布局差异。
    table = pa.Table.from_pylist(
        ordered_table.to_pylist(),
        schema=EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, EXTERNAL_MARKET_CALENDAR_SCHEMA) as writer:
        writer.write_table(table)

    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


# ## 完整叶差异计划
# 
# `changed_partition_keys()` 对期望和已有 DataFrame 各分组一次，保存每个分区的行位置，并各建立一次 Arrow 表。分区循环只按位置取行和比较完整内容摘要，不再为每个键创建全表布尔掩码。缺失一侧仍记为 None，保留新增、修订和删除语义；比较进度与返回排序不变。

# ### 局部流程：完整叶差异计划
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["各分组和转换一次；候选键并集"] --> B["逐键按索引取行；比较摘要并报告进度"]
#     B --> C["非空分区计算完整内容摘要；空侧记为 None"]
#     C --> D{"摘要相同？"}
#     D -->|是| E["不加入变更集合"]
#     D -->|否| F["加入变更集合；包括旧叶撤销"]
#     E --> G["报告比较完成；返回变化分区"]
#     F --> G
# ```

# In[ ]:


def changed_partition_keys(
    expected_df: pd.DataFrame,
    existing_df: pd.DataFrame,
) -> list[tuple[object, ...]]:
    log_started_at = time.perf_counter()
    log_phase = "compare"
    log_compared_partitions = 0
    log_partition = None
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=started; "
        f"expected_rows={len(expected_df)}; existing_rows={len(existing_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 比较完整叶分区；空期望分区也保留在结果中，用于删除上游已撤销的旧分区。
        log_phase = "partition_keys"
        expected_indices_by_partition = expected_df.groupby(
            PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        existing_indices_by_partition = existing_df.groupby(
            PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        ).indices
        partition_keys = sorted(expected_indices_by_partition.keys() | existing_indices_by_partition.keys())
        expected_calendar_table = pa.Table.from_pandas(
            expected_df, schema=EXTERNAL_MARKET_CALENDAR_SCHEMA, preserve_index=False,
        )
        existing_calendar_table = pa.Table.from_pandas(
            existing_df, schema=EXTERNAL_MARKET_CALENDAR_SCHEMA, preserve_index=False,
        )
        changed_keys = []

        log_total_partitions = len(partition_keys)
        log_phase = "compare"
        for partition_key in partition_keys:
            log_partition = partition_key
            if log_compared_partitions == 0 or time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=running; "
                    f"partition={partition_key}; compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            expected_indices = expected_indices_by_partition.get(partition_key)
            existing_indices = existing_indices_by_partition.get(partition_key)
            expected_digest = (
                table_digest(expected_calendar_table.take(expected_indices))
                if expected_indices is not None else None
            )
            existing_digest = (
                table_digest(existing_calendar_table.take(existing_indices))
                if existing_indices is not None else None
            )

            if expected_digest != existing_digest:
                changed_keys.append(partition_key)
            log_compared_partitions += 1

        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=completed; "
            f"compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return changed_keys
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; compared_partitions={log_compared_partitions}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## staging、共享安装与整批失败恢复
# 
# `commit_partitions()` 接收已验证的新期望和可信范围外旧行，排序并转换完整期望一次、计算摘要，再暂存变化叶；metadata 迁移时暂存完整期望表。staging 根级零行标记使全部待删除或空期望也能复读。staging 只物化一次，检查物理契约与总行数，再在内存按分区索引核对每叶行数和完整内容摘要；metadata 整根迁移另保留完整表摘要核对。
# 
# 全部变化叶使用同一个 `StagedPathTransaction`。普通提交替换或显式删除变化叶，未触达叶和已有根级标记保持原样；metadata 迁移或完整期望为空时整根替换。删除使用 `staged_path=None`，应存在的暂存叶缺失仍报错。在同一事务内，本函数执行一次正式整表验收，核对物理契约、总行数和完整内容摘要；成功退出事务后才允许报告本批已落盘。返回值仍是实际写入的变化叶行数，删除行不计入。
# 
# 本批临时目录位于同一 silver 根，分别为 `.a03-b01-s-<run_id前12位>`（staging）、`.a03-b01-b-...`（备份）和 `.a03-b01-f-...`（失败新数据隔离）。共享模块按实际移动记录倒序恢复；首次备份失败不会触碰仍在原位的旧目标，一处恢复失败仍继续其余目标。恢复完整时清理 staging 和备份，保留已安装新数据的隔离副本；恢复不完整时另保留旧备份，staging 仍清理。异常包含现场路径，并保留原安装或验收异常的原因链。
# 
# 暂存或事务进入失败清理本批 staging；空湖失败恢复后清理无 Parquet 的新建表目录。本环节不写独立日期水位。共享模块仅处理同一文件系统内路径替换，不负责生成或数据验收，也不提供跨目录原子可见性、进程中断后自动恢复或并发写入协调。
# 
# 提交函数自行报告输入转换、staging 写入/复读、逐叶安装及正式验收的进度。安装和正式复读完成仍为 `batch_state=pending`；成功退出共享事务后才输出 `partition_committed:` 与 `phase=calendar_state; persisted=true; date_watermark=none`。空提交报告状态未变；失败保留阶段与已安装数，恢复日志由共享模块负责。
# 
# 期望分组索引、权威列清单、空 Arrow 表和各分区相对路径在分区循环前准备并复用；安装时通过索引判断替换或显式删除。正式整表仍在全部安装后复读一次，并非逐叶重复读取。生成后、提交前、staging 和正式安装后不再重复执行同一业务行校验。

# ### 局部流程：staging、共享安装与整批恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告提交开始；一次转换、分组、摘要和路径"] --> B["写零行标记及变化叶；迁移时写完整表"]
#     B --> C["一次物化 staging；内存逐叶核对并报告数量"]
#     C --> D["进入一个共享事务"]
#     D --> E{"metadata 迁移或完整期望为空？"}
#     E -->|是| F["共享模块备份旧根；安装 staging 根"]
#     E -->|否| G["逐叶安装并报告进度；批次仍为 pending"]
#     F --> H["一次正式整表物理验收；行数与完整内容摘要一致"]
#     G --> H
#     H --> I["成功退出后报告提交与日历落盘；返回行数"]
#     F -. 失败 .-> R["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
#     G -. 失败 .-> R
#     H -. 失败 .-> R
#     R --> S{"恢复完整？"}
#     S -->|是| T["清理 staging、备份；保留失败新数据；抛错"]
#     S -->|否| U["清理 staging；保留备份与失败新数据；抛错"]
#     B -. 失败 .-> V["清理 staging；正式目标未改动"]
#     C -. 失败 .-> V
#     D -. 进入失败 .-> V
# ```

# In[ ]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
    force_full_swap: bool = False,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "commit"
    log_partition = None
    log_staged_partitions = 0
    log_installed_targets = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=started; "
        f"expected_rows={len(expected_df)}; planned_partitions={len(partition_keys)}; force_full_swap={str(force_full_swap).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not partition_keys and not force_full_swap:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=skipped; "
                f"reason=no_partitions; committed_rows=0; calendar_state=unchanged; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        # 新期望已通过生成校验；范围外旧行继承正式提交证明，此处验收写入内容。
        log_phase = "input_conversion"
        expected_df = expected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
        calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
        expected_calendar_table = pandas_to_arrow(expected_df, EXTERNAL_MARKET_CALENDAR_SCHEMA)
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
        log_phase = "expected_digest"
        expected_digest = table_digest(expected_calendar_table)

        log_phase = "paths"
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / TABLE_NAME
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".a03-b01-s-{run_id[:12]}"
        backup_path = silver_root / f".a03-b01-b-{run_id[:12]}"
        quarantine_path = silver_root / f".a03-b01-f-{run_id[:12]}"

        # 所有移动路径必须位于本次明确指定的 silver 根目录。
        for managed_path in (
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ):
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

        silver_root.mkdir(parents=True, exist_ok=True)
        try:
            log_phase = "staging_prepare"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_prepare; status=started; "
                f"run_id={run_id}; partitions={len(partition_keys)}; staging_path={staging_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)

            # 根级 0 行 Schema marker 使“全部触达分区均待删除”仍可完成 staging 复读。
            file_schema = pa.schema(
                [
                    field
                    for field in EXTERNAL_MARKET_CALENDAR_SCHEMA
                    if field.name not in PARTITION_COLUMNS
                ],
                metadata=EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata,
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

            # metadata 过期时 staging 必须包含完整 expected_full_df，禁止混合新旧叶。
            changed_calendar_table = (
                expected_calendar_table if force_full_swap
                else expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
            )
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_write; status=started; "
                f"rows={changed_calendar_table.num_rows}; partitions={len(partition_keys)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
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
                f"rows={changed_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            # staging 根路径与每个非空触达叶分区都必须能够精确复读。
            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=started; "
                f"expected_rows={changed_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "外部市场日历 staging",
            )
            staged_calendar_table = validate_arrow_table(
                staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
            )
            staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas().groupby(
                PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
            ).indices
            if staged_calendar_table.num_rows != changed_calendar_table.num_rows:
                raise ValueError("staging 触达行数与完整分区计划不一致。")
            if (
                force_full_swap
                and table_digest(staged_calendar_table) != expected_digest
            ):
                raise ValueError("metadata 升级 staging 与完整期望表逐值不一致。")
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=completed; "
                f"rows={staged_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            for partition_key in partition_keys:
                expected_indices = expected_indices_by_partition.get(partition_key)
                expected_partition_table = (
                    expected_calendar_table.take(expected_indices)
                    if expected_indices is not None else empty_calendar_table
                )
                log_partition = partition_key
                log_phase = "staging_leaf"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_leaf; status=started; "
                    f"partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; expected_rows={expected_partition_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                staged_indices = staged_indices_by_partition.get(partition_key)
                staged_partition_table = (
                    staged_calendar_table.take(staged_indices)
                    if staged_indices is not None else empty_calendar_table
                )

                if staged_partition_table.num_rows != expected_partition_table.num_rows:
                    raise ValueError("staging 叶分区行数与期望不一致。")
                if (
                    expected_partition_table.num_rows
                    and table_digest(staged_partition_table)
                    != table_digest(expected_partition_table)
                ):
                    raise ValueError("staging 叶分区内容与期望不一致。")
                log_staged_partitions += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_leaf; status=completed; "
                    f"partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={staged_partition_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        log_phase = "install"
        log_partition = None
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=started; "
            f"run_id={run_id}; partitions={len(partition_keys)}; full_root_swap={str(force_full_swap or expected_df.empty).lower()}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        target_had_existing = target_path.exists()
        full_swap = force_full_swap or expected_df.empty
        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
            ) as transaction:
                if full_swap:
                    # 空上游或 metadata 升级都以整根 swap 提交，避免部分叶混合契约。
                    transaction.replace(target_path=target_path, staged_path=staging_path)
                    log_installed_targets += 1
                    click.echo(
                        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=running; "
                        f"target={target_path}; full_root_swap=true; installed_targets={log_installed_targets}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                else:
                    for partition_key in partition_keys:
                        log_partition = partition_key
                        click.echo(
                            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=running; "
                            f"partition={partition_key}; installed_targets={log_installed_targets}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                        )
                        relative_path = partition_relative_paths[partition_key]
                        source_path = staging_path / relative_path
                        destination_path = target_path / relative_path

                        should_exist = partition_key in expected_indices_by_partition

                        if should_exist != source_path.is_dir():
                            raise FileNotFoundError(
                                f"staging 叶分区存在性与期望不一致：{relative_path}"
                            )

                        transaction.replace(
                            target_path=destination_path,
                            staged_path=source_path if should_exist else None,
                        )
                        log_installed_targets += 1
                        click.echo(
                            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install_leaf; status=completed; "
                            f"partition={partition_key}; action={'replace' if should_exist else 'delete'}; installed_targets={log_installed_targets}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                        )

                # 正式路径必须与完整期望表逐行一致，不能只验证本次新增行。
                log_phase = "formal_readback"
                log_partition = None
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=formal_readback; status=started; "
                    f"expected_rows={len(expected_df)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_dataset = open_exact_dataset(
                    target_path,
                    CALENDAR_PARTITIONING,
                    EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    "正式外部市场日历",
                )
                committed_calendar_table = validate_arrow_table(
                    committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                )
                if committed_calendar_table.num_rows != len(expected_df):
                    raise ValueError("正式外部市场日历总行数与完整期望表不一致。")
                if table_digest(committed_calendar_table) != expected_digest:
                    raise ValueError("正式外部市场日历内容与完整期望表不一致。")
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=formal_readback; status=completed; "
                    f"rows={committed_calendar_table.num_rows}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            if (
                not target_had_existing
                and target_path.is_dir()
                and next(target_path.rglob("*.parquet"), None) is None
            ):
                shutil.rmtree(target_path)
            raise

        click.echo(
            f"partition_committed: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=completed; "
            f"scope=batch; persisted=true; committed_rows={changed_calendar_table.num_rows}; committed_partitions={len(partition_keys)}; full_root_swap={str(full_swap).lower()}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=calendar_state; status=completed; "
            f"run_id={run_id}; rows={committed_calendar_table.num_rows}; persisted=true; date_watermark=none; scope=expected_external_calendar; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return changed_calendar_table.num_rows
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; installed_targets={log_installed_targets}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：完整比较与运行日志
# 
# `main()` 检查日期和正式写入边界，读取上游及现有日历，生成范围内期望并保留范围外旧行，然后比较完整叶差异。已有正式状态直接按契约读取，旧生意社语义通过规则变化重置，新期望仍必须满足当前 raw 语义；metadata 过期触发整表迁移计划。
# 
# `complete_calendar_grids` 表示现有行与期望行完整相同的格点数，不是 `is_fetch_completed=true` 的数量。只读只报告计划；提交完成日志由 `commit_partitions()` 在正式验收并成功退出共享事务后发出，并区分日历落盘与下游采集完成。
# 
# 运行边界统一使用 `=` 分隔线和 `function/phase/status/elapsed_s`。保留 `reconciliation_plan:` 前缀供 operations 识别，新增统一的完成、无需更新和失败日志；异常按原类型与异常链继续抛出。各函数报告自身起止、数量、耗时和失败阶段；main 只报告自身实际物化、范围合并、计划汇总、跳过提交及整次运行结果。`elapsed_s` 是当前函数调用累计耗时。

# ### 局部流程：运行计划与日志
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["运行开始；参数和正式写入边界"] --> B["读取上游与现有日历"]
#     B --> C["生成范围内期望；保留范围外旧行"]
#     C --> D["差异计划；metadata 迁移时覆盖全部叶"]
#     D --> E["报告计划：一致、新增修订、撤销、变化叶"]
#     E --> F{"有变化或迁移？"}
#     F -->|否| G["报告 up_to_date；结束"]
#     F -->|是| H{"启用 --write？"}
#     H -->|否| I["只读计划；运行完成"]
#     H -->|是| J["调用提交；函数自行报告整批落盘"]
#     J --> K["入口报告运行完成"]
#     B -. 失败 .-> R["报告失败阶段；原异常继续抛出"]
#     C -. 失败 .-> R
#     D -. 失败 .-> R
#     J -. 失败 .-> R
# ```

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
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n外部市场采集日历 / External market calendar\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; api_calls=0; elapsed_s=0.000"
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

        requested_start_date = start_date.date() if start_date is not None else None
        requested_end_date = end_date.date() if end_date is not None else None
        if (
            requested_start_date is not None
            and requested_start_date > requested_end_date
        ):
            raise click.BadParameter("起始日期不得晚于结束日期。")

        silver_root = resolved_lake_root / "silver"
        upstream_path = silver_root / UPSTREAM_TABLE_NAME
        target_path = silver_root / TABLE_NAME

        # 中国自然日历是本表有效水位的唯一上游维度；这里不会请求任何外部数据源。
        log_phase = "read_upstream"
        upstream_dataset, _ = open_compatible_dataset(
            upstream_path, UPSTREAM_PARTITIONING, TRADE_CALENDAR_SCHEMA,
            UPSTREAM_PARTITION_COLUMNS, "正式中国自然日历",
        )
        upstream_filter = ds.field("calendar_date") >= settings.futures_data_start_date
        if has_explicit_dates:
            upstream_filter = (
                upstream_filter
                & (ds.field("calendar_date") >= requested_start_date)
                & (ds.field("calendar_date") <= requested_end_date)
            )

        click.echo(
            f"planning_progress: table={UPSTREAM_TABLE_NAME}; function=main; phase=upstream_read; status=started; "
            f"path={upstream_path}; explicit_dates={str(has_explicit_dates).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        upstream_df = arrow_to_pandas(
            upstream_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names, filter=upstream_filter),
            TRADE_CALENDAR_SCHEMA,
        ).sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: table={UPSTREAM_TABLE_NAME}; function=main; phase=upstream_read; status=completed; "
            f"rows={len(upstream_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        # 目标不存在只是现有完整格点集合为空；旧 metadata 仅在物理完全兼容时迁移。
        log_phase = "read_existing"
        metadata_upgrade_required = False
        if target_path.is_dir() and next(target_path.rglob("*.parquet"), None):
            existing_dataset, existing_schema_is_exact = open_compatible_dataset(
                target_path,
                CALENDAR_PARTITIONING,
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
                PARTITION_COLUMNS,
                "现有正式外部市场日历",
            )
            metadata_upgrade_required = not existing_schema_is_exact
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=started; "
                f"path={target_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            existing_df = arrow_to_pandas(
                existing_dataset.to_table(columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names),
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=completed; "
                f"rows={len(existing_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        else:
            existing_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=skipped; "
                f"reason=no_existing_parquet; rows=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        # 显式范围只更新范围内格点；同一叶分区中范围外旧行原样带入完整快照。
        if has_explicit_dates:
            in_scope_mask = (
                existing_df["observation_date"].ge(requested_start_date)
                & existing_df["observation_date"].le(requested_end_date)
            )
            scoped_existing_df = existing_df.loc[in_scope_mask].copy()
            outside_scope_df = existing_df.loc[~in_scope_mask].copy()
        else:
            scoped_existing_df = existing_df
            outside_scope_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)

        log_phase = "generate_expected"
        expected_scope_df = build_expected_calendar(
            upstream_df,
            scoped_existing_df,
            datetime.now(timezone.utc),
        )

        if has_explicit_dates:
            log_phase = "merge_scope"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=merge_scope; status=started; "
                f"outside_scope_rows={len(outside_scope_df)}; expected_scope_rows={len(expected_scope_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            expected_full_df = pd.concat(
                [outside_scope_df, expected_scope_df],
                ignore_index=True,
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=merge_scope; status=completed; "
                f"expected_full_rows={len(expected_full_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        else:
            expected_full_df = expected_scope_df

        log_phase = "compare_partitions"
        partition_keys = changed_partition_keys(expected_full_df, existing_df)
        if metadata_upgrade_required:
            # metadata 升级必须触达完整表并整根替换，不能只重写内容变化的叶分区。
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

        log_phase = "plan_counts"
        existing_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in scoped_existing_df.to_dict(orient="records")
        }
        expected_rows_by_key = {
            tuple(row[name] for name in PRIMARY_KEY): row
            for row in expected_scope_df.to_dict(orient="records")
        }
        complete_grid_count = sum(
            existing_rows_by_key.get(key) == row
            for key, row in expected_rows_by_key.items()
        )
        removed_grid_count = len(
            set(existing_rows_by_key) - set(expected_rows_by_key)
        )

        run_mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=completed; mode={run_mode}; "
            f"lake_root={resolved_lake_root}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; "
            f"upstream_calendar_dates={len(upstream_df)}; "
            f"valid_request_grids={len(expected_scope_df)}; "
            f"complete_calendar_grids={complete_grid_count}; "
            f"missing_or_revised_grids={len(expected_scope_df) - complete_grid_count}; "
            f"removed_grids={removed_grid_count}; "
            f"metadata_upgrade_required={str(metadata_upgrade_required).lower()}; "
            f"changed_partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        if not partition_keys and not metadata_upgrade_required:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome=up_to_date; "
                f"pending_partitions=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )
            return

        if write:
            log_phase = "commit"
            commit_partitions(
                expected_full_df,
                partition_keys,
                resolved_lake_root,
                force_full_swap=metadata_upgrade_required,
            )
        else:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=plan; status=completed; outcome=read_only; persisted=false; dry_run_rows={len(expected_scope_df)}; "
                f"planned_partitions={len(partition_keys)}; "
                f"full_root_swap={str(metadata_upgrade_required or expected_full_df.empty).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        click.echo(
            f"finished: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome={'committed' if write else 'read_only'}; "
            f"write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        raise


# ## Notebook 与脚本执行入口
# 
# 与 a01/b01、b02 一样，Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并使用 `standalone_mode=False` 返回单元格。当前参数为 `[]`，默认读取本地正式湖并生成完整只读计划，不调用外部 API、不提交。
# 
# 只有交互内核且没有 `__file__` 时才进入 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时读取终端参数。最后一格仅列出人工终端执行命令；正式提交需运行 `--write`，不能用显式日期截断正式写入范围。

# ### 局部流程：Notebook 与脚本执行入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；不读取内核参数"]
#     B --> C["main.main；standalone_mode=False"]
#     C --> D["当前空参数：本地全量只读计划"]
#     A -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main 读取终端参数"]
#     E -->|否| G["模块导入：不执行入口"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认执行正式湖只读自动计划；测试写入必须显式改用非正式湖路径。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b01_external_market_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()


# ### 局部流程：终端手动运行
# 
# 下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["在终端激活 latitude_env_v2"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["本地完整比较；共同提交变化叶或整根迁移"]
# ```

# In[ ]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a03_External_Market_Data\b01_external_market_calendar.py --write

