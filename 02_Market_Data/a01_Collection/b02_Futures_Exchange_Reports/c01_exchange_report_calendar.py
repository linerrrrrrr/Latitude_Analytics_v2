#!/usr/bin/env python
# coding: utf-8

# # c01 期货交易所报告采集日历
# 
# 本环节读取 `dim_futures_variety_calendar`，为每个交易所—品种—交易日展开三类报告格点，生成 `dim_futures_exchange_report_calendar`。整个环节只处理本地 silver，不调用外部 API。
# 
# | 上下游 | 与本环节的关系 |
# | --- | --- |
# | b01/c02 品种日历 | 提供完整的品种交易日宇宙，决定当前有效报告格点。 |
# | 共享事实采集政策 | `config/futures_lakehouse/futures_fact_collection_policy.py` 的白名单决定当前是否应采；白名单外格点仍保留。当前三类报告没有额外覆盖排除。 |
# | b02/c02 成交持仓报告 | 消费 `position_rank`、`member_position` 两类格点，生产相应事实并回写采集、计数和质量状态。 |
# | b02/c03 仓单 | 消费 `warehouse_receipt` 格点，生产仓单事实并回写状态。 |
# | b02/c01a 特殊案例校准 | 位于本入口与 c02 之间，准备已配置案例的 raw 证据；不修改报告日历。c02 回写的校准 `success + warning` 由本入口继承。 |
# 
# `is_fetch_required` 表示当前采集义务，`is_fetch_completed` 表示持久完成凭证。政策排除只停止后续采集、清零当前缺失；已完成格点的批次、条数与质量证据继续保留，重新纳入时仍有效。
# 
# 字段、主键、分区及状态解释以 `config/data_contracts.py` 为准，下方浏览器直接展示该契约。运行语义见湖仓根目录 `README.md` 和 `02_Market_Data/a02_Lake/AGENTS.md`。

# ## 自动更新与写入边界
# 
# 默认从完整上游生成期望报告日历，与现有报告日历逐分区比较；无差异时直接结束。空湖、尾部新增、内部缺口、上游格点撤销以及白名单或规则变化均由同一条路径处理。
# 
# | 模式 | 计算范围 | 写入边界 |
# | --- | --- | --- |
# | 默认自动模式 | 当前上游全部品种交易日及其三类报告格点。 | 不带 `--write` 只读；带 `--write` 提交存在差异的完整叶分区。 |
# | 成对 `--start-date`、`--end-date` | 闭区间内的上游与现有报告格点；合并时保留范围外原有行。 | 可只读检查；写入只能指定与正式湖不同的 `--lake-root`。 |
# 
# 正式湖根目录由 `settings.futures_lake_root` 读取。显式日期必须成对且起始日不晚于结束日；本入口没有 `--full` 参数，也不单独维护日期水位文件。
# 
# 白名单缩小仍保留理论格点和已完成证据；上游品种交易日撤销则会移除相应报告格点。两种变化的含义不同。

# ## 总流程：报告日历同步
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查参数与正式写入边界"] --> B["读取品种日历和现有报告日历"]
#     B --> C["逐品种日展开三类报告；应用白名单"]
#     C --> D["继承完成和质量证据；合并范围外旧行"]
#     D --> E["比较完整叶摘要；统计日历格点差异"]
#     E --> F{"有变更分区？"}
#     F -->|否| G["无差异结束"]
#     F -->|是| H{"启用 --write？"}
#     H -->|否| I["只读计划结束"]
#     H -->|是| J["准备并复读 staging"]
#     J --> K["同一共享事务：整表置空或逐叶替换、删除"]
#     K --> L["正式整表物理契约与内容摘要核对"]
#     L -->|通过| M["清理临时路径；报告提交完成"]
#     J -->|准备失败| N["清理 staging；抛出异常"]
#     K -->|失败| O["共享模块逐项恢复；保留失败新数据与必要备份；抛错"]
#     L -->|失败| O
# ```

# ## 初始化与权威 Schema
# 
# 按项目标记文件查找根目录，加载 `latitude_env_v2` 环境中的 Pandas、PyArrow、Click，以及品种日历和报告日历两张具名 Schema、事实白名单、项目设置和共享路径事务。此格只准备依赖，不展开日历或提交数据。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前目录及其父目录"] --> B{"匹配项目标记文件？"}
#     B -->|是| C["加入项目根与湖仓支撑模块路径"]
#     B -->|否| D["抛出未找到项目根目录"]
#     C --> E["导入库、两张 Schema、白名单、设置与共享事务"]
# ```

# In[1]:


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
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界样例
# 
# 仅在交互式 Notebook 中展示：上游品种日历在前，本环节报告日历在后。共享浏览器读取权威 Schema；由于传入了 `lake_root`，还可按用户选择有界读取本地样例。浏览器不调用 API、不写入数据，也不启动本环节更新；普通脚本运行或模块导入跳过该展示。

# ### 局部流程：契约与有界样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有 __file__？"} -->|是| B["展示品种日历和报告日历 Schema"]
#     B --> C["按选择有界读取本地样例"]
#     A -->|否| D["跳过展示"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区与状态列
# 
# 两张表的表名、主键与 Hive 分区顺序在初始化时从各自 Schema metadata 读取一次，供路径、排序和校验复用。报告类型从 `dataset_name` 字段枚举读取。
# 
# | 类别 | 用途 |
# | --- | --- |
# | `POLICY_COLUMNS` | `is_fetch_required` 与中文原因；判断当前政策是否变化。 |
# | `STATE_COLUMNS` | 完成凭证、采集结果、缺失标志、条数、质量与审计时间；在生成期望行时按状态继承规则处理。 |
# | 物理契约与身份 | 字段类型、nullable、表名、主键及分区标识；描述性 metadata 以当前 Schema 为说明权威。 |
# 
# 报告日历以 `dataset_name/exchange_code/year/month` 组织完整叶；上游分区和两张表的 Hive 字段类型均使用各自权威 Schema。

# ### 局部流程：表身份与分区初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两张权威 Schema"] --> B["读取表名、主键与分区 metadata"]
#     A --> C["读取报告类型枚举"]
#     B --> D["构造两张表的 Hive partitioning"]
#     C --> E["政策列与状态列用于后续继承"]
#     D --> F["后续路径、排序、校验复用"]
#     E --> F
# ```

# In[3]:


# 三项物理契约只从权威 Schema metadata 读取一次，后续路径、排序和校验统一复用。
TABLE_NAME = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 期货交易所报告采集日历维度表。
PRIMARY_KEY = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一类报告的交易所—品种—交易日格点。
PARTITION_COLUMNS = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 正式表的 Hive 叶分区顺序。

UPSTREAM_TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 完整期货品种交易日历维度表。
UPSTREAM_PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识交易所—品种—交易日格点。
UPSTREAM_PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 上游品种日历的 Hive 叶分区顺序。

# 报告类型枚举同样读取字段 metadata，避免在业务入口维护第二份枚举定义。
DATASET_NAMES = tuple(
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field("dataset_name")
    .metadata[b"enum_values_zh"]
    .decode("utf-8")
    .split("、")
)

POLICY_COLUMNS = [
    "is_fetch_required",  # 当前格点是否必须进入事实采集。
    "requirement_reason",  # 当前选择或排除规则的中文说明。
]
STATE_COLUMNS = [
    "is_fetch_completed",  # 事实提交及正式复读形成的持久完成凭证。
    "fetch_result_status",  # 最近一次采集结果状态。
    "is_data_missing",  # 应有报告但确认空响应时的缺失状态。
    "expected_record_count",  # 当前能够明确确认的最小记录数。
    "actual_record_count",  # 相应事实表正式复读的实际记录数。
    "quality_status",  # 本格点综合质量状态。
    "quality_reason",  # 本格点质量结论的中文说明。
    "fetch_run_id",  # 最近一次事实采集批次标识。
    "fetch_completed_at",  # 事实提交和正式复读完成时间。
    "quality_checked_at",  # 最近一次质量检查时间。
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
SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]

REPORT_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
        for name in UPSTREAM_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 契约化读取与本环节业务校验
# 
# `open_exact_dataset()` 确认 Parquet 存在，打开带 Hive 分区的 Dataset，并检查重建的逻辑物理字段、类型、nullable 与表身份。描述性 metadata 以当前 Schema 为准。函数报告文件数、打开结果与失败阶段，`materialized=false` 表示业务行尚未读取。
# 
# 实际 `to_table()` 和 `arrow_to_pandas()` 留在入口：每份上游品种日历、既有报告日历各做一次契约转换。它们的主键、年月和已提交业务状态由对应生产者保证，本入口继承这些证明。
# 
# `validate_report_calendar_table()` 只接收已完成权威 Arrow 转换的本环节生成结果，执行一次主键、枚举、原因、年月、条数以及采集义务、完成、缺失和质量时间关系校验。其内部直接转为 Pandas 并遍历 Arrow 行，不再重复 cast 或调用契约 validator；固定审计字段和状态集合在行循环前准备。
# 
# 校验沿原行循环每 10000 行检查一次 2 秒进度间隔，排序完成后报告结果。staging 和正式复读只核对物理契约、行数与完整内容摘要，不再次执行报告日历业务校验。

# ### 局部流程：读取与当前校验路径
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     subgraph INPUT["可信正式输入"]
#         direction TB
#         A["打开 Dataset；检查物理字段与表身份"] --> B["报告打开完成；尚未物化"]
#         B --> C["调用方读取数据；执行一次契约转换"]
#         C --> D["品种日历与既有报告行：继承已提交业务证明"]
#     end
#     subgraph OUTPUT["本环节生成结果"]
#         direction TB
#         E["生成结果经过一次 pandas_to_arrow"] --> F["一次主键、枚举、状态与时间业务校验"]
#         F --> G["沿行循环报告进度；排序后返回"]
#     end
# ```

# In[4]:


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


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    if actual_schema.names != expected_schema.names:
        return False
    if any(
        actual_field.type != expected_field.type
        or actual_field.nullable != expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    ):
        return False
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    return all(
        actual_metadata.get(key) == expected_metadata.get(key)
        and expected_metadata.get(key) is not None
        for key in SCHEMA_IDENTITY_METADATA_KEYS
    )


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "dataset_open"
    log_table_name = UPSTREAM_TABLE_NAME if schema is FUTURES_VARIETY_CALENDAR_SCHEMA else TABLE_NAME
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 正式输入、staging 与提交后输出都检查物理契约和表身份。
        log_phase = "discovery"
        parquet_files = (
            list(table_path.rglob("*.parquet"))
            if table_path.is_dir()
            else []
        )
        if not parquet_files:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=discovery; status=completed; "
            f"label={label}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
        )
        log_phase = "schema"
        if not physically_and_identity_compatible(
            reconstructed_schema(dataset, schema),
            schema,
        ):
            raise TypeError(f"{label}物理结构或表身份与权威契约不一致。")

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; path={table_path}; files={len(parquet_files)}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; path={table_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise






def validate_report_calendar_table(
    table: pa.Table,
    context: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_checked_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=validate_report_calendar_table; phase=validate; status=started; "
        f"context={context}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 输入已由调用方完成权威 Arrow 转换，此处只承担报告日历业务校验。
        log_phase = "convert"
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)

        log_phase = "primary_key"
        if frame.duplicated(PRIMARY_KEY).any():
            raise ValueError(f"{context}报告日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        allowed_dataset_names = set(DATASET_NAMES)

        log_phase = "business_validation"
        audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
        completed_statuses = {"success", "empty_confirmed"}
        checked_quality_statuses = {"passed", "warning", "failed"}
        for row in table.to_pylist():
            # 枚举、中文说明和日期分区都是日历生产者的完整责任。
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
                row["trading_date"].year != row["year"]
                or row["trading_date"].month != row["month"]
            ):
                raise ValueError(f"{context}year/month 与 trading_date 不一致。")

            # 记录数不能为负；日历只保存格点汇总，不伪造事实明细。
            if row["expected_record_count"] < 0 or row["actual_record_count"] < 0:
                raise ValueError(f"{context}理论或实际记录数不得为负。")

            # 当前无需采集时清除当前缺失；没有历史完成凭证的格点使用未采集状态。
            if not row["is_fetch_required"]:
                if row["is_data_missing"]:
                    raise ValueError(f"{context}当前无需采集格点不得标记数据缺失。")
                if not row["is_fetch_completed"]:
                    if row["fetch_result_status"] != "not_required":
                        raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_required。")
                    if row["expected_record_count"] != 0 or row["actual_record_count"] != 0:
                        raise ValueError(f"{context}无完成凭证的免采集格点记录数必须为 0。")
                    if row["quality_status"] != "not_applicable":
                        raise ValueError(f"{context}无完成凭证的免采集格点必须为 not_applicable。")
                    if any(
                        row[name] is not None
                        for name in audit_fields
                    ):
                        raise ValueError(f"{context}无完成凭证的免采集格点不得保留运行审计值。")
            elif row["fetch_result_status"] == "not_required":
                raise ValueError(f"{context}需采集格点不得标为 not_required。")

            # 完成状态只允许成功或确认空，并且必须同时具备批次和正式复读时间。
            is_completed_status = row["fetch_result_status"] in completed_statuses
            if row["is_fetch_completed"] != is_completed_status:
                raise ValueError(f"{context}完成布尔值与采集结果状态不一致。")
            if row["is_fetch_completed"]:
                if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                    raise ValueError(f"{context}完成格点缺少批次或完成时间。")
            elif row["fetch_completed_at"] is not None:
                raise ValueError(f"{context}未完成格点不得具有完成时间。")

            # 缺失只能来自需要采集且已经确认空的响应，不能把请求错误冒充空数据。
            if row["is_data_missing"] and (
                not row["is_fetch_required"]
                or row["fetch_result_status"] != "empty_confirmed"
                or row["actual_record_count"] != 0
            ):
                raise ValueError(f"{context}缺失状态不能由当前结果复算。")

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
                    f"planning_progress: table={TABLE_NAME}; function=validate_report_calendar_table; phase=validate; status=running; "
                    f"context={context}; checked_rows={log_checked_rows}/{table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "sort"
        validated_report_calendar_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_report_calendar_table; phase=validate; status=completed; "
            f"context={context}; checked_rows={log_checked_rows}; rows={len(validated_report_calendar_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_report_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=validate_report_calendar_table; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; checked_rows={log_checked_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 生成期望报告格点与继承下游状态
# 
# `build_expected_calendar()` 将上游每个品种交易日展开为三类报告，按共享白名单设置当前采集义务与原因。新格点初始化为待采或免采状态；既有格点按主键匹配。
# 
# | 既有格点条件 | 状态处理 |
# | --- | --- |
# | 采集义务未变，或已有完成凭证 | 继承 `STATE_COLUMNS`；其中当前免采时清除当前缺失，应采且 `empty_confirmed` 时恢复缺失标志。 |
# | 采集义务改变且从未完成 | 使用新义务对应的初始状态。 |
# | 两个政策字段均未变 | 继续使用原 `updated_at`；新增或政策变化采用本批时间。 |
# 
# 输出经过报告日历业务校验后返回。上游没有行时返回契约化空表；显式日期模式的范围外原有行由 `main()` 随后合并。
# 
# `build_expected_calendar()` 自行报告索引准备、上游行展开、格点生成和输出校验。沿原品种日循环每 1000 行检查一次 2 秒进度间隔，报告已处理品种日和已生成报告行；零行输入也报告开始与完成。生成结果标记 `persisted=false`，输出校验成功后才报告生成完成。
# 
# 已契约化的输入直接使用 Pandas 记录建立索引和遍历，不再转换为 Arrow 再取 Python 行。各报告类型的固定政策原因在品种日循环前准备。新生成结果只在输出边界做一次 `pandas_to_arrow()`，随后执行一次业务 validator。

# ### 局部流程：生成与状态继承
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["直接索引可信现有行；预备固定政策原因"] --> B["遍历上游品种日和三类报告"]
#     B --> C["依据白名单设置义务及初始状态"]
#     C --> D{"既有格点且义务未变或已完成？"}
#     D -->|是| E["继承状态；按当前义务调整缺失标志"]
#     D -->|否| F["保留初始状态"]
#     E --> G{"既有两个政策字段都未变？"}
#     F --> G
#     G -->|是| H["保留原 updated_at"]
#     G -->|否| I["使用本批 updated_at"]
#     H --> J["汇集期望行并报告进度；零行保留契约"]
#     I --> J
#     J --> K["输出转换与业务校验各一次；报告完成"]
# ```

# In[5]:


def build_expected_calendar(
    upstream_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"
    log_processed_upstream = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=started; "
        f"upstream_rows={len(upstream_df)}; existing_rows={len(existing_df)}; report_types={len(DATASET_NAMES)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
        required_reason_by_dataset = {
            dataset_name: (
                "品种位于期货事实采集白名单；"
                f"{dataset_name} 当前未配置额外 API 覆盖或交易所支持排除。"
            )
            for dataset_name in DATASET_NAMES
        }
        excluded_reason_by_dataset = {
            dataset_name: (
                "品种不在期货事实采集白名单；"
                f"保留 {dataset_name} 报告日历格点但不采集事实。"
            )
            for dataset_name in DATASET_NAMES
        }

        log_phase = "expand"
        for upstream_row in upstream_rows:
            pair = (
                upstream_row["exchange_code"],
                upstream_row["underlying_code"],
            )
            is_fetch_required = pair in FUTURES_FACT_VARIETY_PAIRS

            for dataset_name in DATASET_NAMES:
                # 当前没有额外数据集覆盖排除；白名单外行仍完整保留在日历中。
                if is_fetch_required:
                    requirement_reason = required_reason_by_dataset[dataset_name]
                    fetch_result_status = "pending"
                    quality_status = "pending"
                    quality_reason = "等待相应事实采集器处理。"
                else:
                    requirement_reason = excluded_reason_by_dataset[dataset_name]
                    fetch_result_status = "not_required"
                    quality_status = "not_applicable"
                    quality_reason = (
                        "本格点仅保留完整报告日历，不进入期货事实采集。"
                    )

                row = {
                    "dataset_name": dataset_name,
                    "exchange_code": upstream_row["exchange_code"],
                    "underlying_code": upstream_row["underlying_code"],
                    "trading_date": upstream_row["trading_date"],
                    "is_fetch_required": is_fetch_required,
                    "requirement_reason": requirement_reason,
                    "is_fetch_completed": False,
                    "fetch_result_status": fetch_result_status,
                    "is_data_missing": False,
                    "expected_record_count": 0,
                    "actual_record_count": 0,
                    "quality_status": quality_status,
                    "quality_reason": quality_reason,
                    "fetch_run_id": None,
                    "fetch_completed_at": None,
                    "quality_checked_at": None,
                    "updated_at": updated_at,
                    "year": upstream_row["trading_date"].year,
                    "month": upstream_row["trading_date"].month,
                }

                key = tuple(row[name] for name in PRIMARY_KEY)
                existing_row = existing_rows_by_key.get(key)

                if existing_row is not None:
                    policy_unchanged = all(
                        existing_row[name] == row[name]
                        for name in POLICY_COLUMNS
                    )
                    preserve_execution_state = (
                        existing_row["is_fetch_required"]
                        == row["is_fetch_required"]
                        or existing_row["is_fetch_completed"]
                    )
                    if preserve_execution_state:
                        for name in STATE_COLUMNS:
                            row[name] = existing_row[name]
                        if not row["is_fetch_required"]:
                            row["is_data_missing"] = False
                        elif row["fetch_result_status"] == "empty_confirmed":
                            row["is_data_missing"] = True
                    if policy_unchanged:
                        row["updated_at"] = existing_row["updated_at"]

                expected_rows.append(row)
            log_processed_upstream += 1
            if log_processed_upstream % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=running; "
                    f"processed_upstream={log_processed_upstream}/{len(upstream_rows)}; generated_rows={len(expected_rows)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()

        log_phase = "output_frame"
        if expected_rows:
            candidate_df = pd.DataFrame(
                expected_rows,
                columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
            )
        else:
            candidate_df = empty_pandas(
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
            )

        log_phase = "output_validation"
        expected_calendar_table = pandas_to_arrow(
            candidate_df, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        )
        expected_calendar_df = validate_report_calendar_table(
            expected_calendar_table, "期望",
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=completed; "
            f"processed_upstream={log_processed_upstream}; generated_rows={len(expected_calendar_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return expected_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_expected_calendar; phase=generate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; processed_upstream={log_processed_upstream}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 一次分组与完整叶内容差异
# 
# `table_digest()` 接收已经按权威类型转换的 Arrow Table，按主键排序，从 Python 标量重建规范缓冲区后对 IPC 字节计算 SHA-256。保留标量重建，以消除 fragment 切片和 null 槽位物理布局差异；摘要内部不再往返 Pandas 或重复转换校验。
# 
# `changed_partition_keys()` 对期望与现有 DataFrame 各执行一次分组，得到分区到行位置的索引，并各恢复一次 Arrow 表示。循环仅提取当前叶的行位置并比较摘要，新增、修改和待删除旧叶仍进入变更键列表。
# 
# 政策未变的行沿用旧 `updated_at`，同一输入再次运行可得到无差异。函数报告分区数、当前键、已比较数和差异数；这些是日历差异，不是事实 API 待办数。

# ### 局部流程：内容摘要与分区差异
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两边各一次分组和 Arrow 表示恢复"] --> B["按分区索引提取当前叶；报告比较进度"]
#     B --> C["当前 Arrow 叶按主键排序"]
#     C --> D["IPC 字节计算 SHA-256；空叶记 None"]
#     D --> E{"两边摘要一致？"}
#     E -->|是| F["此叶保持原样"]
#     E -->|否| G["加入变更键；含待删除的旧叶"]
#     F --> H["报告完成与差异分区数；返回变更键"]
#     G --> H
# ```

# In[6]:


def table_digest(calendar_table: pa.Table) -> str:
    # Arrow IPC 固定权威列顺序、类型、metadata 和主键排序后再生成摘要。
    ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])
    # 从 Python 标量按权威 Schema 重建缓冲区，消除不同 fragment 的切片布局差异。
    table = pa.Table.from_pylist(
        ordered_table.to_pylist(),
        schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    )
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA) as writer:
        writer.write_table(table)

    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


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
            expected_df, schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, preserve_index=False,
        )
        existing_calendar_table = pa.Table.from_pandas(
            existing_df, schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, preserve_index=False,
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


# ## 完整叶暂存、共享安装与内容复读
# 
# `commit_partitions()` 接收已经生成并完成业务校验的期望新行，以及显式范围外可信正式旧行组成的完整期望表。调用方负责确定变更键；提交函数固定 Arrow 契约并验收写入内容，不重复执行业务 validator。
# 
# 1. 在分区循环前转换完整期望表一次、计算完整内容摘要、建立分区行位置与相对路径映射。按变更键汇集行位置，从同一 Arrow 表提取待写数据。
# 2. staging 写入零行契约标记及变更完整叶。整批数据只读取一次，做物理契约和总行数检查后建立分区索引；逐叶在内存中核对行数和完整内容摘要，不再从 Dataset 逐叶读取。
# 3. 进入一个 `StagedPathTransaction`。完整期望表为空时整根替换为零行数据集；其余情况按预计算路径安装或显式删除全部变更叶，未触达叶与已有根级契约标记保持原样。删除通过 `staged_path=None` 表达；应有的 staging 叶缺失仍报错。
# 4. 在同一事务内，正式安装后保留一次整表物理契约、总行数和完整内容摘要复读。这项检查保证新增、修改、删除和未变历史共同等于完整期望结果；它不重新执行业务校验。
# 
# 本函数报告暂存、安装与复读进度，共享模块负责失败恢复日志。安装与复读阶段仍记 `batch_state=pending`；验收并成功退出事务后才报告提交和日历状态已落盘，`date_watermark=none`。返回值仍为变更叶中实际写入的行数，删除行不计入。
# 
# 本批路径为 `silver/.b02-c01-s-<run_id前12位>/`（暂存）、`.b02-c01-b-.../`（备份）、`.b02-c01-f-.../`（隔离）。共享模块按实际移动记录倒序恢复整个批次；首次备份未成功时旧目标保持原位，一处恢复失败仍继续尝试其余目标。恢复完整时清理 staging 和 backup，保留失败新数据的隔离目录；恢复不完整时另保留旧备份，staging 仍清理。异常报告现场，并保留原安装或验收异常的原因链。
# 
# 暂存或事务进入失败只清理本次 staging；空湖失败恢复后清理无 Parquet 的新建表目录。共享模块只负责同一文件系统内的路径替换，不提供外部读者的跨目录原子可见性、进程终止后的自动恢复或并发写入协调。

# ### 局部流程：暂存、共享安装与整批恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"变更键为空？"} -->|是| B["报告无分区、状态未变；返回 0"]
#     A -->|否| C["期望转换一次；预备分区索引、摘要和路径"]
#     C --> D["写入 staging；读取一次并在内存核对各叶"]
#     D --> E["进入一个共享事务"]
#     D -. 失败 .-> X["清理 staging；抛错；正式目标不变"]
#     E -. 进入失败 .-> X
#     E --> F{"完整期望表为空？"}
#     F -->|是| G["共享模块备份旧表根；安装零行表根"]
#     F -->|否| H["共享模块逐叶替换或显式删除；批次 pending"]
#     G --> I["本函数复读正式整表契约、行数与内容摘要"]
#     H --> I
#     I -->|通过| J["成功退出事务；报告提交与日历落盘"]
#     G -. 失败 .-> K["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
#     H -. 失败 .-> K
#     I -. 失败 .-> K
#     K --> L{"恢复完整？"}
#     L -->|是| M["清理暂存及备份；保留隔离新数据；抛错"]
#     L -->|否| N["清理暂存；保留备份及隔离数据；报告现场并抛错"]
# ```

# In[7]:


def commit_partitions(
    expected_df: pd.DataFrame,
    partition_keys: list[tuple[object, ...]],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "commit"
    log_partition = None
    log_staged_partitions = 0
    log_installed_targets = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=started; "
        f"expected_rows={len(expected_df)}; planned_partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not partition_keys:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=skipped; "
                f"reason=no_partitions; committed_rows=0; committed_partitions=0; calendar_state=unchanged; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        # 期望新行已通过生成校验，范围外旧行继承正式提交证明；本函数验收安装内容。
        log_phase = "input_conversion"
        calendar_columns = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
        expected_calendar_table = pandas_to_arrow(
            expected_df.loc[:, calendar_columns], FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
        )
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
        staging_path = silver_root / f".a02-b01-s-{run_id[:12]}"
        backup_path = silver_root / f".a02-b01-b-{run_id[:12]}"
        quarantine_path = silver_root / f".a02-b01-f-{run_id[:12]}"

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
                    for field in FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
                    if field.name not in PARTITION_COLUMNS
                ],
                metadata=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.metadata,
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
            changed_calendar_table = expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
            log_phase = "staging_write"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_write; status=started; "
                f"run_id={run_id}; rows={changed_calendar_table.num_rows}; partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if changed_calendar_table.num_rows:
                ds.write_dataset(
                    changed_calendar_table,
                    staging_path,
                    format="parquet",
                    partitioning=REPORT_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_write; status=completed; "
                f"run_id={run_id}; rows={changed_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            # staging 根路径与每个非空触达叶分区都必须能够精确复读。
            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=started; "
                f"run_id={run_id}; expected_rows={changed_calendar_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                REPORT_PARTITIONING,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                "报告日历 staging",
            )
            staged_calendar_table = validate_arrow_table(
                staged_dataset.to_table(columns=calendar_columns),
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            )
            staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas(
                types_mapper=pd.ArrowDtype,
            ).groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False).indices
            if staged_calendar_table.num_rows != changed_calendar_table.num_rows:
                raise ValueError("staging 触达行数与完整分区计划不一致。")
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_readback; status=completed; "
                f"rows={staged_calendar_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

            for partition_key in partition_keys:
                expected_indices = expected_indices_by_partition.get(partition_key, ())
                staged_indices = staged_indices_by_partition.get(partition_key, ())
                log_partition = partition_key
                log_phase = "staging_leaf"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_leaf; status=started; "
                    f"partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; expected_rows={len(expected_indices)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                if len(staged_indices) != len(expected_indices):
                    raise ValueError("staging 叶分区行数与期望不一致。")
                if (
                    len(expected_indices)
                    and table_digest(staged_calendar_table.take(staged_indices))
                    != table_digest(expected_calendar_table.take(expected_indices))
                ):
                    raise ValueError("staging 叶分区内容与期望不一致。")
                log_staged_partitions += 1
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=staging_leaf; status=completed; "
                    f"partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={len(staged_indices)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        log_phase = "install"
        log_partition = None
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=started; "
            f"run_id={run_id}; partitions={len(partition_keys)}; full_swap={str(expected_df.empty).lower()}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        target_had_existing = target_path.exists()
        full_swap = expected_df.empty

        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
            ) as transaction:
                if full_swap:
                    # 上游当前有效集合为空时，全表替换为可读的 0 行契约数据集。
                    transaction.replace(target_path=target_path, staged_path=staging_path)
                    log_installed_targets += 1
                    click.echo(
                        f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=install; status=running; "
                        f"target={target_path}; full_swap=true; installed_targets={log_installed_targets}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                    REPORT_PARTITIONING,
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                    "正式报告日历",
                )
                committed_calendar_table = validate_arrow_table(
                    committed_dataset.to_table(columns=calendar_columns),
                    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                )
                if committed_calendar_table.num_rows != expected_calendar_table.num_rows:
                    raise ValueError("正式报告日历总行数与完整期望表不一致。")
                if table_digest(committed_calendar_table) != expected_digest:
                    raise ValueError("正式报告日历内容与完整期望表不一致。")
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
            f"committed: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=completed; "
            f"run_id={run_id}; committed_rows={changed_calendar_table.num_rows}; committed_partitions={len(partition_keys)}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=calendar_state; status=completed; "
            f"run_id={run_id}; rows={committed_calendar_table.num_rows}; persisted=true; date_watermark=none; scope=expected_report_calendar; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return changed_calendar_table.num_rows
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; installed_targets={log_installed_targets}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI：读取、生成、差异计划与提交
# 
# `main()` 检查参数和正式写入边界，读取上游品种日历与现有报告日历。目标表不存在时使用契约化空表；显式日期只限制上游读取及参与重建的旧行，范围外旧行合并回完整期望表，因此同月其他日期也会保留。
# 
# 生成期望表后计算变更叶和格点统计。`complete_report_grids` 表示与期望行逐值一致的日历格点数，不代表事实采集完成数；`removed_grids` 表示范围内已退出上游有效集合的旧格点数。
# 
# 日志使用 88 个 `=` 的运行边界、中文阶段说明与 `table/function/phase/status` 字段，并以 `elapsed_s` 表示当前函数调用累计秒数。各函数承担自己的起止、进度和失败日志；入口保留实际数据物化、显式范围合并、计划汇总、跳过提交与整次运行结果，不重复报告函数生成或提交起止；`reconciliation_plan:` 保留格点与分区统计，供现有 monitor 识别。提交函数在正式复读与清理成功后才输出提交及日历状态落盘；只读或无差异明确标记跳过提交。异常继续向上抛出，不输出正常完成日志。
# 
# 显式范围内新行已经通过生成校验，范围外旧行继承正式提交证明，两组日期互斥；合并不再整表复验。计划统计直接读取已有 Pandas 行记录，避免为了构造字典再转换 Arrow。

# ### 局部流程：主流程与运行分支
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["校验成对日期、先后关系和写入目标"] --> B["读取上游；目标缺失时用空表"]
#     B --> C["划分显式范围内外；生成范围内期望"]
#     C --> D["合回范围外旧行；计算变更键与统计"]
#     D --> E["输出 reconciliation_plan"]
#     E --> F{"有差异且启用 --write？"}
#     F -->|是| G["调用 commit_partitions；函数自行报告落盘"]
#     F -->|否| H["报告无差异或只读跳过提交"]
#     G --> I["入口只报告正常运行完成边界"]
#     H --> I
# ```

# In[8]:


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

    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n交易所报告日历同步开始 / Report calendar run started\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={'explicit' if has_explicit_dates else 'automatic'}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
    )
    log_phase = "inputs"
    try:

        # 上游日历是本表有效水位的唯一业务来源；本入口不请求外部 API。
        upstream_dataset = open_exact_dataset(
            upstream_path,
            UPSTREAM_PARTITIONING,
            FUTURES_VARIETY_CALENDAR_SCHEMA,
            "正式品种日历",
        )
        upstream_filter = None
        if has_explicit_dates:
            upstream_filter = (
                (ds.field("trading_date") >= requested_start_date)
                & (ds.field("trading_date") <= requested_end_date)
            )
        log_phase = "upstream_read"
        click.echo(
            f"planning_progress: table={UPSTREAM_TABLE_NAME}; function=main; phase=upstream_read; status=started; "
            f"path={upstream_path}; explicit_dates={str(has_explicit_dates).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        upstream_df = arrow_to_pandas(
            upstream_dataset.to_table(
                columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
                filter=upstream_filter,
            ),
            FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        click.echo(
            f"planning_progress: table={UPSTREAM_TABLE_NAME}; function=main; phase=upstream_read; status=completed; "
            f"rows={len(upstream_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        # 目标不存在只是现有完整格点集合为空，后续同一逻辑自然生成全量候选。
        if target_path.is_dir() and next(target_path.rglob("*.parquet"), None):
            existing_dataset = open_exact_dataset(
                target_path,
                REPORT_PARTITIONING,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                "现有正式报告日历",
            )
            log_phase = "existing_read"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=started; "
                f"path={target_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            existing_df = arrow_to_pandas(
                existing_dataset.to_table(
                    columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ),
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=completed; "
                f"rows={len(existing_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        else:
            existing_df = empty_pandas(
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
            )
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=existing_read; status=skipped; "
                f"reason=no_existing_parquet; rows=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )


        if has_explicit_dates:
            in_scope_mask = (
                existing_df["trading_date"].ge(requested_start_date)
                & existing_df["trading_date"].le(requested_end_date)
            )
            scoped_existing_df = existing_df.loc[in_scope_mask].copy()
            outside_scope_df = existing_df.loc[~in_scope_mask].copy()
        else:
            scoped_existing_df = existing_df
            outside_scope_df = empty_pandas(
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
            )

        updated_at = datetime.now(timezone.utc)
        log_phase = "generate"
        expected_scope_df = build_expected_calendar(
            upstream_df,
            scoped_existing_df,
            updated_at,
        )

        # 显式范围只替换范围内当前真值；同一月范围外旧行进入完整叶分区并原样保留。
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

        log_phase = "compare"
        partition_keys = changed_partition_keys(
            expected_full_df,
            existing_df,
        )

        log_phase = "plan_summary"
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
            f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; "
            f"mode={run_mode}; write={str(write).lower()}; "
            f"upstream_variety_grids={len(upstream_df)}; "
            f"valid_report_grids={len(expected_scope_df)}; "
            f"complete_report_grids={complete_grid_count}; "
            f"missing_or_revised_grids="
            f"{len(expected_scope_df) - complete_grid_count}; "
            f"removed_grids={removed_grid_count}; "
            f"changed_partitions={len(partition_keys)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        if not partition_keys:
            click.echo(
                "报告日历已经与上游和当前事实采集政策一致。\n"
                f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=skipped; "
                f"reason=no_changes; committed_rows=0; committed_partitions=0; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(
                f"{log_boundary}\n交易所报告日历同步完成 / Report calendar run completed\n"
                f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
                f"outcome=no_changes; write={str(write).lower()}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )
            return

        if write:
            log_phase = "commit"
            committed_rows = commit_partitions(
                expected_full_df,
                partition_keys,
                resolved_lake_root,
            )
        else:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=skipped; "
                f"reason=read_only; write=false; dry_run_rows={len(expected_scope_df)}; "
                f"planned_partitions={len(partition_keys)}; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )


        click.echo(
            f"{log_boundary}\n交易所报告日历同步完成 / Report calendar run completed\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome={'committed' if write else 'read_only'}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## Notebook 与脚本执行入口
# 
# 与 c01、c02 一样，Notebook 使用 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并用 `standalone_mode=False` 返回单元格。当前参数为 `[]`，默认读取正式湖的本地数据并生成完整自动计划，不带 `--write`，不调用 API。
# 
# 只有交互内核且没有 `__file__` 时才使用 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时读取命令行参数。最后一格仅列出终端命令注释；正式提交仍需人工运行 `--write`，不能用日期参数截断正式写入范围。

# ### 局部流程：Notebook 与脚本执行入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；不读取内核参数"]
#     B --> C["main.main；standalone_mode=False"]
#     C --> D["当前空参数：本地全量只读计划"]
#     A -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main 读取命令行参数"]
#     E -->|否| G["模块导入：不执行入口"]
# ```

# In[9]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认执行正式湖只读自动计划；需要测试写入时显式改为非正式湖参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="c01_exchange_report_calendar",
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
#     C --> D["本地全量比较；整批提交变更叶"]
# ```

# In[10]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Market_Data\a01_Collection\b02_Futures_Exchange_Reports\c01_exchange_report_calendar.py --write

