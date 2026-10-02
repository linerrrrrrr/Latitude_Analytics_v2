#!/usr/bin/env python
# coding: utf-8

# # b03 境外期货日线
# 
# 从外部市场日历选择 `overseas_futures/ALL` 的 required 日期，逐日查询 JQData `finance.FUT_GLOBAL_DAILY`，生产 `fact_overseas_futures_daily`，并在事实正式复读后回写日历完成状态。
# 
# | 上下游 | 本环节使用或产生的内容 |
# | --- | --- |
# | a01/b01 → a03/b01 | 自然日历和共享实体配置生成外部市场日历；本入口只消费其 required 日期，不自行扩展日期宇宙。 |
# | `config/futures_lakehouse/external_market_entities.py` | 境外期货请求实体 `ALL` 的唯一配置来源；一次日期查询返回当日全部来源品种。 |
# | `config/jqdata_connection.py` | 共享认证与连接边界；仅存在 API 待办时调用。 |
# | `config/data_contracts.py` | 外部市场日历与境外期货事实的字段、类型、主键和分区权威。 |
# | 事实输出与日历回写 | 事实按 `year/month` 完整叶提交；日历按 `dataset_name/year/month` 完整叶回写，保留未触达行。 |
# | operations 与读取 Demo | 默认人工批次在 a03/b01、b02 后运行本入口；数据库 Demo 按契约只读消费境外期货事实。 |
# 
# 本入口与 a03/b02 生意社原文归档、a03/b04 外部指数共用日历，但不消费它们的 raw 或事实。运行边界见湖仓 `README.md`、`AGENTS.md` 与数据库 `AGENTS.md`。

# ## 更新集合、来源质量与写入边界
# 
# 每次按「上游当前 required 日期 − 事实与日历状态共同证明完整的日期」求差，分成 API 待办与无 API 日历修复。当前不使用最大日期截断，也没有 `--full` 或独立日期水位文件。空事实表由同一差集自然形成待办。
# 
# | 情形 | 当前处理 |
# | --- | --- |
# | 正式事实与日历状态、数量、质量原因一致 | 已完成，跳过请求。 |
# | 非空事实已正式提交，日历状态或原因陈旧 | 从事实复算计数和 OHLC 结论，只修复日历，不认证 JQData。 |
# | 正式事实为零行，已有确认空审计凭证 | 依日历证据判定完成或修复；零行本身不能证明曾查询成功。 |
# | 缺少上述事实或确认空证据 | 每个待办日期查询一次；不自动重试业务查询。 |
# | 不带 `--write` | 计算计划；有 API 待办时仍查询、归一化和校验，但不提交事实或状态。 |
# | 成对显式日期 | 缩小检查范围；只读可用，写入必须指定非正式临时湖。 |
# 
# 查询条件为 `day == snapshot_date`，因此 API 事实日必须等于请求日；返回达到 5000 行上限时拒绝提交。成功空响应记为 `empty_confirmed + warning`，不生成占位事实行。
# 
# 可空数值的 `None`、`pd.NA`、Pandas `NaN` 在来源归一化时转为 Arrow null；非空非有限数、非数值文本、负成交量和负振幅继续失败。有限 OHLC 跨列异常保留来源原值，并将对应日期记为 `success + warning`。
# 
# 正式湖由 `settings.futures_lake_root` 唯一定位，禁止用显式日期写正式湖。事实提交与日历提交是先后两个边界：日历提交失败不撤销已提交事实，后续运行通过无 API 状态修复补齐。

# ## 总流程：日期证据与独立叶事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["参数门禁；读取正式日历与可选事实"] --> B["一次规划；计算计数和 OHLC 证据"]
#     B --> C["已完成跳过；陈旧状态按 write 无 API 修复"]
#     C --> D{"还有 API 待办？"}
#     D -->|否| Z["报告结果；结束"]
#     D -->|是| N["认证一次；准备月份循环"]
#     N --> E["当前月逐日查询与归一化"]
#     E --> F{"启用 write？"}
#     F -->|否| G["累计内存结果；继续下一月"]
#     F -->|是| H["合并当前完整事实叶；校验与 staging 逐值复读"]
#     H --> I["事实共享事务：当前叶与新增标记；正式逐值验收"]
#     I --> J["正式计数与已验证来源质量；生成日历状态"]
#     J --> K["日历独立叶事务；正式逐值验收"]
#     K --> G
#     G --> M{"还有月份？"}
#     M -->|是| E
#     M -->|否| Z
#     I -. 失败 .-> R["恢复当前目标；保留失败证据；停止"]
#     K -. 失败 .-> R
#     R --> S["此前成功事实保留；下次可无 API 修复日历"]
# ```

# ## 初始化与共享依赖
# 
# 按项目三个标记文件定位根目录，加载两张权威 Schema、共享实体、JQData 连接和项目设置。本格只定义依赖，不认证、不查询、不写湖。
# 
# 湖仓级 `a00_04_staged_path_transaction.py` 只负责 staging 路径安装与失败恢复，更新范围、事实合并、质量验收和事务边界仍由本环节决定。

# ### 局部流程：初始化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
#     B -->|是| C["配置导入路径；加载权威契约和共享连接"]
#     B -->|否| D["抛错停止"]
#     C --> E["不认证；不查询；不写湖"]
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
from types import ModuleType

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

# 第三方库直接承担 CLI、DataFrame 与 Arrow 数据集操作。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# 项目配置只提供权威数据契约、请求实体、JQData 连接和正式湖路径。
from config.data_contracts import (
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    OVERSEAS_FUTURES_DAILY_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.futures_lakehouse.external_market_entities import OVERSEAS_FUTURES_ENTITY_CODE
from config.jqdata_connection import authenticate_jqdata
from config.settings import settings
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界本地样例
# 
# 交互内核且没有 `__file__` 时，按上游日历、输出事实的顺序展示权威字段说明。传入 `lake_root` 后，用户显式选择样例会进行有界本地读取；不会认证 JQData、请求来源或修改数据。普通脚本运行跳过展示。

# ### 局部流程：契约和样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且无 __file__？"} -->|否| B["跳过"]
#     A -->|是| C["展示日历与事实权威 Schema"]
#     C --> D["用户选择时读取有界本地样例"]
#     D --> E["不认证 JQData；不回写"]
# ```

# In[ ]:


# 命令行导出脚本不加载 widgets，也不触发 Schema 展示。
if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        EXTERNAL_MARKET_CALENDAR_SCHEMA,
        OVERSEAS_FUTURES_DAILY_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区与来源字段
# 
# 表名、主键和 Hive 分区从两张 Schema metadata 各读取一次。事实主键为 `instrument_code/trading_date`，分区为 `year/month`；日历主键为数据集—实体—观测日期，分区为 `dataset_name/year/month`。
# 
# `JQDATA_FIELDS` 固定显式查询字段，`SOURCE` 记录来源身份；`ALL` 表示一次日期查询返回整表，不是逐品种请求名单。状态枚举用于已有业务校验，两个 Hive partitioning 使用各自权威字段类型。

# ### 局部流程：表身份和查询字段
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["两张权威 Schema"] --> B["读取表名、主键、分区；构造 Hive partitioning"]
#     B --> C["共享实体 ALL；显式来源字段；状态枚举"]
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

TABLE_NAME = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 境外期货日线事实表。
PRIMARY_KEY = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 境外期货代码—API 交易日业务主键。
PARTITION_COLUMNS = OVERSEAS_FUTURES_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 境外期货事实 Hive 年—月叶分区顺序。

DATASET_NAME = "overseas_futures"  # 外部市场日历中的境外期货数据集代码。
ENTITY_CODE = OVERSEAS_FUTURES_ENTITY_CODE  # 单次日查询返回全表，对应实体 ALL。
GRID_COLUMNS = ["snapshot_date"]  # 来源请求和完成状态的最小日期格点。

SOURCE = "JQData_finance_FUT_GLOBAL_DAILY"
JQDATA_RESULT_ROWS_LIMIT = 5000

# 显式列顺序来自当前账户已核验的 FUT_GLOBAL_DAILY ORM 字段。
JQDATA_FIELDS = [
    "id",  # 财务库原始记录 ID。
    "code",  # 境外期货品种代码。
    "name",  # 境外期货品种名称。
    "day",  # API 事实交易日。
    "open",  # 开盘价。
    "close",  # 收盘价。
    "low",  # 最低价。
    "high",  # 最高价。
    "volume",  # 成交量。
    "change_pct",  # 来源涨跌幅百分数值。
    "amplitude",  # 来源振幅百分数值。
    "pre_close",  # 前收价。
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
        OVERSEAS_FUTURES_DAILY_SCHEMA.field(name)
        for name in PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## Dataset 物理契约与表身份读取
# 
# `open_exact_dataset()` 检查 Dataset 与各 Parquet fragment 的字段、类型、nullable，以及 `table_name/primary_key/partition_columns` 身份 metadata。Hive 分区字段按权威列序补回。纯描述性 metadata 差异以当前 `config/data_contracts.py` 为准，不触发历史文件重写。
# 
# 启动时只打开一次日历和可选事实；提交复读直接定位当前完整叶，使用 `partition_base_dir` 补回 Hive 字段。空事实通过根级零行标记复读。函数报告结构读取、fragment 进度和失败阶段；`materialized=false` 表示尚未读取数据行。

# ### 局部流程：物理契约与表身份
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["表根、叶目录或零行标记"] --> B["打开 Dataset；恢复 Hive 字段"]
#     B --> C["检查物理字段、类型、nullable 和身份 metadata"]
#     C --> D["逐 fragment 检查同一物理契约"]
#     D --> E["返回 Dataset；描述性差异采用当前契约"]
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
# `validate_calendar_table()` 接收已按权威 Schema 转换的 Arrow 表，对待提交完整叶检查主键、状态枚举、年月、原因、计数、required 关系和审计时间，按主键排序返回。上游正式日历不重复业务验收；内存状态生成不再完整复核，统一由提交函数对 dirty 完整叶验收一次。

# ### 局部流程：dirty 日历叶验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["已按 Schema 转换的 dirty Arrow 叶"] --> B["主键唯一性；枚举和日期关系"]
#     B --> C["数量、required、完成状态和审计时间"]
#     C --> D["按主键排序；返回已验收 Arrow 叶"]
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
        log_phase = "primary_key"
        calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()
        if calendar_keys_df.duplicated().any():
            raise ValueError(f"{context}外部市场日历主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        completed_fetch_statuses = {"success", "empty_confirmed"}
        checked_quality_statuses = {"passed", "warning", "failed"}
        calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
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
            if row["dataset_name"] == DATASET_NAME and row["entity_code"] != ENTITY_CODE:
                raise ValueError(f"{context}境外期货请求实体必须为 ALL。")

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
                    raise ValueError(f"{context}success 必须有正式事实且不得标记缺失。")
            if row["fetch_result_status"] == "empty_confirmed":
                if row["actual_record_count"] != 0 or not row["is_data_missing"]:
                    raise ValueError(f"{context}empty_confirmed 必须为零事实并标记缺失。")
            if row["is_data_missing"] and row["fetch_result_status"] != "empty_confirmed":
                raise ValueError(f"{context}数据缺失只能来自确认空响应。")
            if (
                row["quality_status"] in checked_quality_statuses
                and row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}已形成质检结论但缺少质检时间。")
            if row["updated_at"] > now_utc:
                raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

        log_phase = "sort"
        validated_calendar_table = calendar_table.sort_by(calendar_sort_keys)
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


# ## 当前事实业务验收
# 
# 检查来源身份、业务主键、请求日与事实日、年月和审计时间。来源归一化之外的事实只接纳真实 null，残留 NaN/Inf 仍失败；成交量与振幅非负。有限 OHLC 关系异常留给独立 warning 计算，不改写价格。
# 
# 该验收用于来源归一化结果和提交前 dirty 完整叶；staging 与正式复读只核对物理契约和完整内容。转换前有限数检查保留，避免残留 NaN 在 Pandas 转 Arrow 时被误当成合法 null。

# ### 局部流程：事实验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["归一化后的事实"] --> B["拒绝残留 NaN 和 Inf；契约转换"]
#     B --> C["检查主键、来源 ID、代码、日期和年月"]
#     C --> D["可空数值有限；成交量与振幅非负"]
#     D --> E["按主键排序返回；不拒绝有限 OHLC 关系异常"]
# ```

# In[ ]:


# 事实生产者严格校验字段、日期和有限数；来源 OHLC 跨列异常另行形成 warning。
def validate_overseas_futures_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "validate"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=validate_overseas_futures_frame; phase=validate; status=started; "
        f"context={context}; rows={len(frame)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 此处验证的已是归一化事实或正式事实：只接纳真实 null，残留 IEEE NaN/Inf 必须拒绝。
        # API 返回的 Pandas NaN 缺失标记只允许在响应归一化函数中显式转为 nullable null。
        source_frame = frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names]
        finite_source_columns = [
            "open",
            "high",
            "low",
            "close",
            "previous_close",
            "volume",
            "change_pct",
            "amplitude",
        ]
        for field_name in finite_source_columns:
            for value in source_frame[field_name]:
                if value is None or value is pd.NA:
                    continue
                try:
                    is_finite = math.isfinite(float(value))
                except (TypeError, ValueError):
                    # 非数值类型交给紧随其后的权威 Arrow 类型转换给出契约错误。
                    continue
                if not is_finite:
                    raise ValueError(f"{context}{field_name} 非空时必须为有限数。")

        log_phase = "conversion"
        checked = pandas_to_arrow(source_frame, OVERSEAS_FUTURES_DAILY_SCHEMA)
        normalized = arrow_to_pandas(checked, OVERSEAS_FUTURES_DAILY_SCHEMA)
        log_phase = "primary_key"
        if normalized.duplicated(PRIMARY_KEY).any():
            raise ValueError(f"{context}境外期货事实主键不唯一。")

        now_utc = datetime.now(timezone.utc)
        price_columns = ("open", "high", "low", "close", "previous_close")
        log_phase = "business_validation"
        for row in checked.to_pylist():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=validate_overseas_futures_frame; phase=scan; status=running; "
                    f"context={context}; scanned_rows={log_scanned_rows}/{len(frame)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            if row["snapshot_date"] != row["trading_date"]:
                raise ValueError(f"{context}请求日期与 API day 不一致。")
            if not str(row["source_instrument_id"]).strip():
                raise ValueError(f"{context}JQData 原始记录 ID 不得为空。")
            if not str(row["instrument_code"]).strip():
                raise ValueError(f"{context}境外期货代码不得为空。")
            if row["instrument_name"] is not None and not str(row["instrument_name"]).strip():
                raise ValueError(f"{context}非空境外期货名称不得为空白。")
            if row["source"] != SOURCE:
                raise ValueError(f"{context}事实来源不一致。")
            if (
                row["trading_date"].year != row["year"]
                or row["trading_date"].month != row["month"]
            ):
                raise ValueError(f"{context}年月分区与 API 交易日不一致。")
            if row["updated_at"] > now_utc:
                raise ValueError(f"{context}updated_at 不得晚于当前 UTC 时间。")

            # 境外期货价格可能出现零或负值；非空值仍必须有限。
            for field_name in price_columns:
                value = row[field_name]
                if value is not None and not math.isfinite(value):
                    raise ValueError(f"{context}{field_name} 非空时必须为有限数。")

            # 有限 OHLC 的跨列关系异常来自供应商原始记录，不在这里改值或拒绝整日。
            # `ohlc_relation_warning_map()` 会按 snapshot_date 汇总异常并回写日历 warning。

            volume = row["volume"]
            if volume is not None and (
                not math.isfinite(volume)
                or volume < 0
            ):
                raise ValueError(f"{context}成交量非空时必须为有限非负数。")
            change_pct = row["change_pct"]
            if change_pct is not None and not math.isfinite(change_pct):
                raise ValueError(f"{context}涨跌幅非空时必须为有限数。")
            amplitude = row["amplitude"]
            if amplitude is not None and (
                not math.isfinite(amplitude)
                or amplitude < 0
            ):
                raise ValueError(f"{context}振幅非空时必须为有限非负数。")

        log_phase = "sort"
        validated_fact_df = normalized.sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_overseas_futures_frame; phase=validate; status=completed; "
            f"context={context}; rows={len(validated_fact_df)}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_fact_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=validate_overseas_futures_frame; phase=validate; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; scanned_rows={log_scanned_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## OHLC 来源质量旁证
# 
# `ohlc_relation_warning_map()` 按请求日期汇总 high/low/open/close 关系异常，记录代码、名称、来源 ID 和原值。无异常返回空映射；有异常不修改事实，不把日期转为采集失败。
# 
# 直接读取已契约化事实的标量记录，不为旁证再构造 Arrow 表。

# ### 局部流程：OHLC 来源旁证
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["契约化事实标量记录"] --> B["检查有限 OHLC 跨列关系"]
#     B --> C["按日期汇总异常代码与来源原值"]
#     C --> D["返回 warning 映射；不改写事实"]
# ```

# In[ ]:


# 有限 OHLC 跨列异常不改变事实值；这里仅生成按请求日期聚合的质量旁证。
def ohlc_relation_warning_map(
    frame: pd.DataFrame,
) -> dict[date, str]:
    log_started_at = time.perf_counter()
    log_phase = "quality"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=quality; status=started; "
        f"rows={len(frame)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if frame.empty:
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=quality; status=completed; "
                f"rows=0; warning_dates=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return {}

        log_phase = "conversion"
        checked_rows = frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names].to_dict("records")
        details_by_date: dict[date, list[str]] = {}
        relation_price_columns = ("open", "close")

        log_phase = "ohlc_relations"
        for row in checked_rows:
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=scan; status=running; "
                    f"scanned_rows={log_scanned_rows}/{len(frame)}; warning_dates={len(details_by_date)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            high = row["high"]
            low = row["low"]
            relation_issues = []

            if high is not None and low is not None and high < low:
                relation_issues.append(f"high={high} 低于 low={low}")
            for field_name in relation_price_columns:
                value = row[field_name]
                if value is None:
                    continue
                if high is not None and value > high:
                    relation_issues.append(
                        f"{field_name}={value} 高于 high={high}"
                    )
                if low is not None and value < low:
                    relation_issues.append(
                        f"{field_name}={value} 低于 low={low}"
                    )

            if not relation_issues:
                continue
            instrument_name = row["instrument_name"] or "无名称"
            detail = (
                f"{row['instrument_code']}（{instrument_name}，"
                f"source_id={row['source_instrument_id']}）："
                + "，".join(relation_issues)
            )
            details_by_date.setdefault(row["snapshot_date"], []).append(detail)

        quality_warning_by_date = {snapshot_date: f'JQData 来源存在 {len(details)} 行有限 OHLC 跨列关系异常；价格保持来源原值，未做修正。明细：' + '；'.join(details) for snapshot_date, details in details_by_date.items()}
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=quality; status=completed; "
            f"rows={len(frame)}; warning_dates={len(quality_warning_by_date)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        for log_warning_date, log_warning_reason in quality_warning_by_date.items():
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=source_quality; status=completed; "
                f"api_quality_warning: date={log_warning_date}; reason={log_warning_reason}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        return quality_warning_by_date
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=ohlc_relation_warning_map; phase=quality; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; scanned_rows={log_scanned_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 可选事实读取
# 
# 事实目录不存在或没有 Parquet 时返回权威空表；否则检查物理契约、读取并按主键排序，信任生产者已经完成的业务验收。启动时仍读取正式事实，计算本入口完成判断所需的逐日期计数和 OHLC 旁证；状态修复后和批末不再重新读取全表。

# ### 局部流程：可选正式事实
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"有正式 Parquet？"} -->|否| B["权威空事实表"]
#     A -->|是| C["物理契约与身份检查"]
#     C --> D["一次物化并排序；信任已提交业务证明"]
#     B --> E["返回事实供一次规划使用"]
#     D --> E
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
            empty_fact_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=completed; "
                f"rows=0; outcome=no_fact_files; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df

        dataset = open_exact_dataset(
            table_path,
            FACT_PARTITIONING,
            OVERSEAS_FUTURES_DAILY_SCHEMA,
            "正式境外期货事实",
        )
        log_phase = "materialize"
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=materialize; status=started; "
            f"path={table_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        table = dataset.to_table(columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=materialize; status=completed; "
            f"rows={table.num_rows}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "validate"
        validated_fact_df = arrow_to_pandas(table, OVERSEAS_FUTURES_DAILY_SCHEMA).sort_values(PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=completed; "
            f"rows={len(validated_fact_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_fact_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=read_optional_fact; phase=read; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## JQData 单日查询
# 
# `query_overseas_futures_grid()` 使用显式字段和 `day == snapshot_date` 查询一次，不分页、不重试。权限或表不存在等异常标为永久错误，其他查询异常及返回 `None` 标为可重试错误；这个分类不意味着本次运行自动重试。
# 
# 函数返回来源 DataFrame，字段、日期和数值验收由紧随其后的归一化完成。`api_success` 由查询函数在收到非 None 响应后报告，并明确 `normalized=false; persisted=false`；归一化独立报告 `api_result`，随后仍需合并与正式提交。

# ### 局部流程：单日查询
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["显式字段；day 等于请求日期"] --> B["一次 finance.run_query"]
#     B --> C{"正常返回且非 None？"}
#     C -->|是| D["返回来源响应；尚未归一化或提交"]
#     C -->|否| E["分类请求错误并抛出；不自动重试"]
# ```

# In[ ]:


def query_overseas_futures_grid(
    jqdata: ModuleType,
    snapshot_date: date,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "fetch"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=query_overseas_futures_grid; phase=fetch; status=started; "
        f"date={snapshot_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        table = jqdata.finance.FUT_GLOBAL_DAILY
        query_object = jqdata.query(*[
            getattr(table, field_name)
            for field_name in JQDATA_FIELDS
        ]).filter(table.day == snapshot_date)

        try:
            log_phase = "request"
            raw_df = jqdata.finance.run_query(query_object)
        except Exception as error:
            message = str(error)
            permanent_markers = ["无权限", "permission", "no table", "不存在"]
            error_type = (
                "permanent_error"
                if any(marker.lower() in message.lower() for marker in permanent_markers)
                else "retryable_error"
            )
            raise RuntimeError(
                f"{error_type}: JQData FUT_GLOBAL_DAILY 查询失败；date={snapshot_date}。"
            ) from error

        log_phase = "response"
        if raw_df is None:
            raise RuntimeError(
                f"retryable_error: JQData FUT_GLOBAL_DAILY 返回 None；date={snapshot_date}。"
            )
        click.echo(
            f"api_success: dataset={DATASET_NAME}; function=query_overseas_futures_grid; phase=fetch; status=completed; "
            f"date={snapshot_date}; response_type={type(raw_df).__name__}; rows={len(raw_df) if isinstance(raw_df, pd.DataFrame) else 'unknown'}; normalized=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return raw_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=query_overseas_futures_grid; phase=fetch; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={snapshot_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 来源响应归一化
# 
# 空 DataFrame 返回权威空事实。非空响应检查必需字段、5000 行上限、请求日期、来源 ID 和代码，再转换文本与数值。来源可空数值缺失标记归一为 null，非空非法文本和 Inf 失败，最后按事实契约验收。

# ### 局部流程：来源归一化
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["确认 DataFrame"] --> B{"响应为空？"}
#     B -->|是| C["返回权威空事实"]
#     B -->|否| D["必需字段；少于 5000 行；请求日期一致"]
#     D --> E["ID 与代码非空；名称不得为空白"]
#     E --> F["来源缺失转 null；拒绝非法文本和非有限值"]
#     F --> G["构造事实字段；完整验收返回"]
# ```

# In[ ]:


def normalize_overseas_futures_response(
    raw_df: pd.DataFrame,
    snapshot_date: date,
    updated_at: datetime,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "normalize"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=normalize_overseas_futures_response; phase=normalize; status=started; "
        f"date={snapshot_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if not isinstance(raw_df, pd.DataFrame):
            raise TypeError("schema_error: JQData 境外期货查询未返回 DataFrame。")
        if raw_df.empty:
            empty_fact_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=normalize_overseas_futures_response; phase=normalize; status=completed; "
                f"date={snapshot_date}; rows=0; outcome=empty_response; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return empty_fact_df

        log_phase = "source_columns"
        missing_columns = set(JQDATA_FIELDS) - set(raw_df.columns)
        if missing_columns:
            raise ValueError(f"schema_error: JQData 境外期货表缺列 {sorted(missing_columns)}。")
        if len(raw_df) >= JQDATA_RESULT_ROWS_LIMIT:
            raise ValueError(
                "schema_error: 单日响应达到 finance.run_query 的 5000 行上限，"
                "不能证明结果完整。"
            )

        log_phase = "date_and_identity"
        response_df = raw_df.loc[:, JQDATA_FIELDS].copy()
        response_dates = pd.to_datetime(response_df["day"], errors="coerce").dt.date
        if response_dates.isna().any() or not response_dates.eq(snapshot_date).all():
            raise ValueError("schema_error: JQData day 与待办请求日期不一致。")

        # 来源定义为不可空的 ID 和代码必须在转换前检查，避免字符串化后出现伪值。
        if response_df["id"].isna().any():
            raise ValueError("schema_error: JQData id 含空值。")
        source_ids = response_df["id"].astype("string").str.strip()
        if source_ids.isna().any() or source_ids.eq("").any():
            raise ValueError("schema_error: JQData id 含空白值。")

        if response_df["code"].isna().any():
            raise ValueError("schema_error: JQData code 含空值。")
        instrument_codes = response_df["code"].astype("string").str.strip()
        if instrument_codes.isna().any() or instrument_codes.eq("").any():
            raise ValueError("schema_error: JQData code 含空白值。")

        instrument_names = response_df["name"].astype("string").str.strip()
        if instrument_names.dropna().eq("").any():
            raise ValueError("schema_error: JQData name 的非空值不得为空白。")

        # JQData 财务库可空数值以 float64 返回，SQL NULL 会表现为 Pandas NaN。
        # None、pd.NA 和 NaN 均显式归一为 Arrow null；非空非有限数和非数值文本必须失败。
        numeric_columns = [
            "open",
            "close",
            "low",
            "high",
            "volume",
            "change_pct",
            "amplitude",
            "pre_close",
        ]
        log_phase = "numeric_conversion"
        numeric_values = {}
        for field_name in numeric_columns:
            for value in response_df[field_name]:
                if value is None or value is pd.NA or pd.isna(value):
                    continue
                try:
                    is_finite = math.isfinite(float(value))
                except (TypeError, ValueError):
                    # 非数值文本由下面的 invalid_mask 给出明确契约错误。
                    continue
                if not is_finite:
                    raise ValueError(
                        f"schema_error: JQData {field_name} 含非有限数。"
                    )
            converted = pd.to_numeric(response_df[field_name], errors="coerce")
            invalid_mask = response_df[field_name].notna() & converted.isna()
            if invalid_mask.any():
                raise ValueError(
                    f"schema_error: JQData {field_name} 含非空非数值文本。"
                )
            numeric_values[field_name] = converted.astype("Float64")

        log_phase = "build_fact"
        frame = pd.DataFrame({
            "snapshot_date": snapshot_date,
            "trading_date": response_dates,
            "source_instrument_id": source_ids,
            "instrument_code": instrument_codes,
            "instrument_name": instrument_names,
            "open": numeric_values["open"],
            "high": numeric_values["high"],
            "low": numeric_values["low"],
            "close": numeric_values["close"],
            "volume": numeric_values["volume"],
            "change_pct": numeric_values["change_pct"],
            "amplitude": numeric_values["amplitude"],
            "previous_close": numeric_values["pre_close"],
            "source": SOURCE,
            "updated_at": updated_at,
            "year": snapshot_date.year,
            "month": snapshot_date.month,
        })
        log_phase = "output_validation"
        normalized_fact_df = validate_overseas_futures_frame(frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names], 'JQData 响应转换后的')
        click.echo(
            f"api_result: dataset={DATASET_NAME}; function=normalize_overseas_futures_response; phase=normalize; status=completed; "
            f"date={snapshot_date}; rows={len(normalized_fact_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return normalized_fact_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=normalize_overseas_futures_response; phase=normalize; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; date={snapshot_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 已完成、无 API 修复与 API 待办
# 
# `plan_overseas_futures_grids()` 只选择 `overseas_futures/ALL` required 日期，应用可选范围后，从正式事实计算逐日期数量和 OHLC warning。`calendar_completion_result()` 统一首次回写、状态修复和完整性判断使用的状态与原因文本。
# 
# 日历完成标记、事实计数、质量状态与原因、批次和时间全部一致才判为完整。非空正式事实足以进入无 API 状态修复；零事实日期必须已有 `empty_confirmed`、缺失标记、零计数、完成批次与时间，才具有可修复的确认空证据。其余日期进入 API 待办。
# 
# 三类集合互相区分；不能把「有日历行」「当前 required」「已完成」混为一谈，也不能用固定品种数或最大日期替代当前格点判定。
# 
# 规划函数自行报告三类日期数与累计分类进度；每 100 日期检查一次 2 秒输出间隔。OHLC 旁证由计算函数报告日期数、原值异常原因和扫描进度，结果仍为内存证据。短小的计数映射、状态判定和字段比较函数不单独刷日志。
# 
# 规划同时返回本次已计算的事实计数与 warning 映射，供修复和失败回写复用；正式逐值验收成功后不重新做全表规划。

# ### 局部流程：日期集合规划
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["选择 ALL required 日期及可选范围"] --> B["正式事实计数和 OHLC warning"]
#     B --> C{"日历状态与事实证据一致？"}
#     C -->|是| D["已完成"]
#     C -->|否| E{"非空正式事实或已有确认空审计？"}
#     E -->|是| F["无 API 日历修复"]
#     E -->|否| G["API 待办"]
#     D --> H["返回三类结果与可复用计数和 warning 映射"]
#     F --> H
#     G --> H
# ```

# In[ ]:


def grid_count_map(frame: pd.DataFrame) -> dict[date, int]:
    if frame.empty:
        return {}
    counts = frame.groupby("snapshot_date", dropna=False).size()
    return {key: int(value) for key, value in counts.items()}


# 同一完成结果只定义一次；首次回写、状态修复和完整性判断必须使用完全相同的状态与原因。
def calendar_completion_result(
    actual_fact_count: int,
    quality_warning_reason: str | None,
) -> tuple[str, bool, str, str]:
    if actual_fact_count < 0:
        raise ValueError("正式事实计数不得为负。")
    if quality_warning_reason is not None and actual_fact_count == 0:
        raise ValueError("零事实格点不得带有 OHLC 关系 warning。")

    if actual_fact_count > 0:
        quality_status = (
            "warning" if quality_warning_reason is not None else "passed"
        )
        quality_reason = (
            f"JQData FUT_GLOBAL_DAILY 响应已转换并从正式境外期货事实复读 {actual_fact_count} 行。"
            + (
                f" {quality_warning_reason}"
                if quality_warning_reason is not None
                else ""
            )
        )
        return "success", False, quality_status, quality_reason

    return (
        "empty_confirmed",
        True,
        "warning",
        "JQData FUT_GLOBAL_DAILY 查询成功且返回空表，正式事实复读为 0 行。",
    )


# 单个日期只有日历审计与正式事实逐项一致时才算完整，陈旧 warning 或 passed 都不能被接受。
def calendar_grid_is_complete(
    row: dict[str, object],
    actual_fact_count: int,
    quality_warning_reason: str | None,
) -> bool:
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

    (
        expected_fetch_status,
        expected_is_missing,
        expected_quality_status,
        expected_quality_reason,
    ) = calendar_completion_result(
        actual_fact_count,
        quality_warning_reason,
    )
    return (
        row["fetch_result_status"] == expected_fetch_status
        and row["is_data_missing"] == expected_is_missing
        and row["quality_status"] == expected_quality_status
        and row["quality_reason"] == expected_quality_reason
    )


# 显式日期仅缩小检查集合；事实缺口和只需修复日历的格点必须分开规划。
def plan_overseas_futures_grids(
    calendar_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    start_date: date | None,
    end_date: date | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int, dict[date, int], dict[date, str]]:
    log_started_at = time.perf_counter()
    log_phase = "plan"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=plan_overseas_futures_grids; phase=plan; status=started; "
        f"calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        relevant_mask = (
            calendar_df["dataset_name"].eq(DATASET_NAME)
            & calendar_df["entity_code"].eq(ENTITY_CODE)
            & calendar_df["is_fetch_required"].eq(True)
        )
        if start_date is not None:
            relevant_mask &= calendar_df["observation_date"].ge(start_date)
            relevant_mask &= calendar_df["observation_date"].le(end_date)
        relevant_df = calendar_df.loc[relevant_mask].copy()

        log_phase = "calendar_conversion"
        calendar_rows = relevant_df.to_dict("records")
        log_phase = "fact_evidence"
        fact_counts = grid_count_map(fact_df)
        fact_warning_by_date = ohlc_relation_warning_map(fact_df)
        pending_rows = []
        state_repair_rows = []
        complete_count = 0

        log_phase = "classify_dates"
        for row in calendar_rows:
            log_scanned_rows += 1
            if log_scanned_rows % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=plan_overseas_futures_grids; phase=scan; status=running; "
                    f"scanned_dates={log_scanned_rows}/{len(relevant_df)}; complete_grid_count={complete_count}; state_repair_count={len(state_repair_rows)}; api_pending_grid_count={len(pending_rows)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            observation_date = row["observation_date"]
            actual_fact_count = fact_counts.get(observation_date, 0)
            quality_warning_reason = fact_warning_by_date.get(observation_date)
            if calendar_grid_is_complete(
                row,
                actual_fact_count,
                quality_warning_reason,
            ):
                complete_count += 1
                continue

            grid = {
                "observation_date": observation_date,
                "year": observation_date.year,
                "month": observation_date.month,
            }

            # 非空正式事实本身足以证明该日 API 全表已经提交；日历陈旧只做无 API 状态修复。
            # 0 行没有事实文件可作证，只有既有完成审计明确为 empty_confirmed 时才可修复。
            empty_result_has_evidence = (
                actual_fact_count == 0
                and row["is_fetch_completed"]
                and row["fetch_result_status"] == "empty_confirmed"
                and row["is_data_missing"]
                and row["actual_record_count"] == 0
                and bool(row["fetch_run_id"])
                and row["fetch_completed_at"] is not None
            )
            if actual_fact_count > 0 or empty_result_has_evidence:
                state_repair_rows.append(grid)
            else:
                pending_rows.append(grid)

        log_phase = "build_plan"
        pending_df = pd.DataFrame(
            pending_rows,
            columns=["observation_date", "year", "month"],
        )
        if not pending_df.empty:
            pending_df = pending_df.sort_values(
                ["year", "month", "observation_date"]
            ).reset_index(drop=True)

        state_repair_df = pd.DataFrame(
            state_repair_rows,
            columns=["observation_date", "year", "month"],
        )
        if not state_repair_df.empty:
            state_repair_df = state_repair_df.sort_values(
                ["year", "month", "observation_date"]
            ).reset_index(drop=True)
        click.echo(
            f"reconciliation_plan: dataset={DATASET_NAME}; function=plan_overseas_futures_grids; phase=plan; status=completed; "
            f"complete_grid_count={complete_count}; state_repair_count={len(state_repair_df)}; api_pending_grid_count={len(pending_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return pending_df, state_repair_df, complete_count, fact_counts, fact_warning_by_date
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=plan_overseas_futures_grids; phase=plan; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; scanned_dates={log_scanned_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 完整月份事实合并
# 
# `full_fact_partition()` 消费当前年月旧事实叶，删除触达日期的旧行，再拼接本次已归一化响应；未触达日期原样保留。确认空日期不生成占位行，结果为空时返回权威空表。合并只生成内存结果，完整业务验收交给事实提交函数执行一次。
# 
# 主循环前按年月建立事实和日历叶映射。每月只处理当前叶，不对全历史逐分区做布尔扫描，也不逐月重建整张事实表。

# ### 局部流程：当前月份合并
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["当前事实叶和本批已归一化响应"] --> B["保留未触达日期；移除触达日期旧行"]
#     B --> C["拼接本批新行；全空则用权威空表"]
#     C --> D["返回内存完整叶；提交函数统一验收"]
# ```

# In[ ]:


# 触达请求日期整体替换，未触达日期在完整月份中原样保留。
def full_fact_partition(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    touched_dates: set[date],
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=full_fact_partition; phase=generate; status=started; "
        f"partition={partition_key}; existing_rows={len(existing_df)}; incoming_rows={len(incoming_df)}; touched_dates={len(touched_dates)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        partition_mask = pd.Series(True, index=existing_df.index)
        for column, value in zip(PARTITION_COLUMNS, partition_key, strict=True):
            partition_mask &= existing_df[column].eq(value)

        existing_partition_df = existing_df.loc[
            partition_mask,
            OVERSEAS_FUTURES_DAILY_SCHEMA.names,
        ]
        log_phase = "retain_untouched"
        retained_df = existing_partition_df.loc[
            ~existing_partition_df["snapshot_date"].isin(touched_dates),
            OVERSEAS_FUTURES_DAILY_SCHEMA.names,
        ]
        log_phase = "merge"
        complete_df = pd.concat([retained_df, incoming_df], ignore_index=True)
        if complete_df.empty:
            complete_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
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


# ## staging 事实分区表达式
# 
# `fact_partition_expression()` 按权威分区顺序构造当前年月过滤条件。事实 staging 只包含一个完整叶与零行契约标记；该过滤选择当前叶数据。正式复读直接打开目标叶，不扫描正式表根。描述性 metadata 的整根迁移分支已移除。

# ### 局部流程：staging 分区表达式
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["权威分区列与年月值"] --> B["构造 Arrow 分区相等条件"]
#     B --> C["只选择 staging 当前叶；排除零行标记"]
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


# ## 完整事实叶与标记共同提交
# 
# 当前完整叶执行业务验收后写 staging 和零行标记，再按物理契约复读、按主键排序并逐值比较。每个事实叶使用一个 `StagedPathTransaction`：已有根标记保留；缺失的零行标记与当前叶纳入同一恢复范围。空结果以删除当前旧叶表达。
# 
# 正式复读及完整内容比较在事务内完成；非空时直读目标叶，空结果确认叶不存在并复读零行标记。共享模块只负责安装和恢复，业务验收仍在本函数中。首次备份失败保留原目标；安装或验收失败恢复当前旧叶、删除本批新增标记。已安装的失败新叶保存在 `.failed-<run_id>`，恢复不完整时另保留 `.backup-<run_id>`；staging 清理。
# 
# 成功退出事务后才报告事实已持久化，`calendar_state=not_updated` 表明日历尚未提交。此前成功事实叶保留。

# ### 局部流程：事实叶与标记共享事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["dirty 完整叶业务验收"] --> B["staging 写入；物理契约和逐值复读"]
#     B --> C["共享事务：需要时安装标记；替换或删除当前叶"]
#     C --> D["事务内直读正式叶或零行标记；逐值比较"]
#     D --> E["退出成功；报告事实已提交；日历尚未更新"]
#     C -. 失败 .-> R["按实际移动记录恢复；删除新增标记"]
#     D -. 失败 .-> R
#     R --> S["失败新叶隔离；恢复不全保留备份；抛错"]
# ```

# In[ ]:


# 每次只提交一个完整年—月事实叶分区，并保留可回滚旧分区。
def commit_complete_fact_partition(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[object, ...],
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "commit"

    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit; status=started; "
        f"rows={len(frame)}; partition={partition_key}; scope=fact_leaf; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        complete_df = validate_overseas_futures_frame(frame, "待提交完整境外期货分区")
        if not complete_df.empty:
            actual_keys = set(
                complete_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
            )
            if actual_keys != {partition_key}:
                raise ValueError("待提交境外期货内容越出指定 Hive 叶分区。")
        complete_table = pandas_to_arrow(complete_df, OVERSEAS_FUTURES_DAILY_SCHEMA)
        fact_columns = OVERSEAS_FUTURES_DAILY_SCHEMA.names
        fact_sort_keys = [(name, "ascending") for name in PRIMARY_KEY]

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
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=staging_write; status=started; "
                f"path={staging_path}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staging_path.mkdir(parents=True, exist_ok=False)

            file_schema = pa.schema(
                [
                    field
                    for field in OVERSEAS_FUTURES_DAILY_SCHEMA
                    if field.name not in PARTITION_COLUMNS
                ],
                metadata=OVERSEAS_FUTURES_DAILY_SCHEMA.metadata,
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
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=staging_readback; status=started; "
                f"path={staging_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            staged_dataset = open_exact_dataset(
                staging_path,
                FACT_PARTITIONING,
                OVERSEAS_FUTURES_DAILY_SCHEMA,
                "境外期货 staging",
            )
            staged_table = staged_dataset.to_table(
                columns=OVERSEAS_FUTURES_DAILY_SCHEMA.names,
                filter=fact_partition_expression(partition_key),
            )
            staged_table = validate_arrow_table(staged_table, OVERSEAS_FUTURES_DAILY_SCHEMA).sort_by(fact_sort_keys)
            if not staged_table.equals(complete_table):
                raise ValueError("境外期货 staging 完整分区内容检查失败。")
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=staging_readback; status=completed; "
                f"rows={staged_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
        except Exception:
            # staging 尚未进入正式替换；写入或复读异常只需清理本批目录。
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
                f"scope=fact_leaf; target={destination_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                        target_path=target_marker_path, staged_path=staging_marker_path,
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
                    f"path={destination_path if complete_table.num_rows else target_marker_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                if complete_table.num_rows == 0 and destination_path.exists():
                    raise ValueError("确认空的正式事实叶仍然存在。")
                committed_dataset = open_exact_dataset(
                    destination_path if complete_table.num_rows else target_marker_path,
                    FACT_PARTITIONING, OVERSEAS_FUTURES_DAILY_SCHEMA, "正式境外期货事实",
                    partition_base_dir=target_path,
                )
                if marker_created and complete_table.num_rows:
                    committed_marker_dataset = open_exact_dataset(
                        target_marker_path, FACT_PARTITIONING, OVERSEAS_FUTURES_DAILY_SCHEMA,
                        "新建正式事实零行标记", partition_base_dir=target_path,
                    )
                    if committed_marker_dataset.count_rows() != 0:
                        raise ValueError("新建正式事实标记必须为零行。")
                committed_table = validate_arrow_table(
                    committed_dataset.to_table(columns=fact_columns), OVERSEAS_FUTURES_DAILY_SCHEMA,
                ).sort_by(fact_sort_keys)
                if not committed_table.equals(complete_table):
                    raise ValueError("正式境外期货完整分区内容检查失败。")
                committed_df = arrow_to_pandas(committed_table, OVERSEAS_FUTURES_DAILY_SCHEMA)
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=formal_readback; status=completed; "
                    f"rows={len(committed_df)}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        finally:
            # 进入事务前的异常也清理 staging；正式路径恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)

        click.echo(
            f"partition_committed: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit; status=completed; "
            f"partition={partition_key}; rows={len(committed_df)}; scope=fact_leaf; persisted=true; calendar_state=not_updated; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return committed_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 日历完成与失败状态生成
# 
# `apply_calendar_completion()` 依据正式事实计数与 OHLC 结论生成成功或确认空状态，补齐批次、完成和质检时间。`apply_calendar_failure()` 保留已有正式事实计数，将失败日期设为未完成并记录原因。
# 
# 两者只生成内存状态，完整业务验收由 dirty 日历叶提交承担。常规月循环传入当前叶；批前无 API 修复复用一次规划得到的事实证据。请求或转换失败只回写失败日期，不提交当前尚未完成月份的事实；此前成功月份保留。

# ### 局部流程：日历内存状态
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"已提交事实证据或请求失败？"} -->|事实证据| B["按计数和 warning 生成成功或确认空"]
#     A -->|失败| C["保留当前事实计数；记错误且未完成"]
#     B --> D["返回内存日历；未持久化"]
#     C --> D
#     D --> E["后续 dirty 完整叶提交承担业务验收"]
# ```

# In[ ]:


# 只有本批已正式复读的日期才写入完成或确认空状态。
def apply_calendar_completion(
    calendar_df: pd.DataFrame,
    grid_results: dict[date, int],
    quality_warning_by_date: dict[date, str],
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
        unknown_warning_dates = set(quality_warning_by_date) - set(grid_results)
        if unknown_warning_dates:
            raise ValueError(
                "OHLC warning 日期必须属于本批正式复读格点："
                f"{sorted(unknown_warning_dates)}。"
            )
        updated_df = calendar_df.copy()

        log_phase = "update_state"
        for index, row in updated_df.iterrows():
            log_scanned_rows += 1
            if log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_completion; phase=generate_state; status=running; "
                    f"scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            if (
                row["dataset_name"] != DATASET_NAME
                or row["entity_code"] != ENTITY_CODE
            ):
                continue
            observation_date = row["observation_date"]
            actual_count = grid_results.get(observation_date)
            if actual_count is None:
                continue

            warning_reason = quality_warning_by_date.get(observation_date)
            (
                fetch_result_status,
                is_data_missing,
                quality_status,
                quality_reason,
            ) = calendar_completion_result(actual_count, warning_reason)
            updated_df.at[index, "is_fetch_completed"] = True
            updated_df.at[index, "fetch_result_status"] = fetch_result_status
            updated_df.at[index, "is_data_missing"] = is_data_missing
            updated_df.at[index, "actual_record_count"] = actual_count
            updated_df.at[index, "quality_status"] = quality_status
            updated_df.at[index, "quality_reason"] = quality_reason
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


# 失败状态保留当前正式事实计数，但绝不把日期标为完成。
def apply_calendar_failure(
    calendar_df: pd.DataFrame,
    observation_date: date,
    fetch_status: str,
    failure_reason: str,
    fetch_run_id: str,
    failed_at: datetime,
    current_fact_count: int,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "generate_state"
    log_scanned_rows = 0
    log_last_progress_at = log_started_at
    log_updated_grids = 0
    click.echo(
        f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=started; "
        f"rows={len(calendar_df)}; date={observation_date}; fetch_status={fetch_status}; fetch_completed=false; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                    f"planning_progress: dataset={DATASET_NAME}; function=apply_calendar_failure; phase=generate_state; status=running; "
                    f"scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_last_progress_at = time.perf_counter()
            if not (
                row["dataset_name"] == DATASET_NAME
                and row["entity_code"] == ENTITY_CODE
                and row["observation_date"] == observation_date
            ):
                continue
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
# 触达日期定位 `dataset_name/year/month` 完整叶；分组索引、列名、排序键和根路径在循环前准备。同叶未触达行保留，dirty 完整叶只执行业务验收一次；staging 与正式目标均检查物理契约并逐值比较，不再往返转换或重复业务验收。
# 
# 每个日历叶独立使用共享事务，正式复读在事务内进行。失败只恢复当前叶，此前成功事实和日历叶保留；失败新叶隔离保留，恢复不完整另保留旧备份，staging 清理。已有日历根标记不替换。
# 
# 日历失败不撤销已提交事实，下次运行可无 API 修复。成功日志在退出事务后发出；全部触达叶成功后报告触达数、完成数与 `date_watermark=none`。错误状态成功落盘不表示采集完成。本实现不提供跨表原子可见性、进程终止后的自动恢复或并发写入协调。

# ### 局部流程：日历独立叶事务
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["触达日期定位完整叶；循环前建立分组索引"] --> B["当前 dirty 叶业务验收一次"]
#     B --> C["staging 写入；物理契约和逐值复读"]
#     C --> D["当前叶共享事务；安装并正式逐值验收"]
#     D --> E["退出成功；报告叶完成；继续下一叶"]
#     E --> M{"还有日历叶？"}
#     M -->|是| B
#     M -->|否| F["全部触达叶成功；报告完成数；无独立水位"]
#     D -. 失败 .-> R["恢复当前旧叶；保留失败现场；停止"]
#     R --> S["此前事实和成功日历叶保留"]
# ```

# In[ ]:


# 日历按 dataset—年—月完整提交，并保留同月其他请求实体。
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
                f"reason=no_touched_dates; persisted=false; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        touched_mask = (
            calendar_df["dataset_name"].eq(DATASET_NAME)
            & calendar_df["entity_code"].eq(ENTITY_CODE)
            & calendar_df["observation_date"].isin(touched_dates)
        )
        touched_df = calendar_df.loc[touched_mask]
        partition_keys = set(touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None))
        calendar_indices_by_partition = calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False).indices
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
            complete_table = validate_calendar_table(
                pandas_to_arrow(calendar_df.iloc[calendar_indices_by_partition[partition_key]].loc[:, calendar_columns], EXTERNAL_MARKET_CALENDAR_SCHEMA),
                "待提交的完整外部市场日历分区",
            )

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
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging_write; status=started; "
                    f"path={staging_path}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=started; "
                    f"path={staging_path}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                staged_dataset = open_exact_dataset(
                    staging_path / relative_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA,
                    "外部市场日历 staging", partition_base_dir=staging_path,
                )
                staged_table = validate_arrow_table(
                    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ).sort_by(calendar_sort_keys)
                if not staged_table.equals(complete_table):
                    raise ValueError("外部市场日历 staging 内容检查失败。")
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=completed; "
                    f"rows={staged_table.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
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
                    f"scope=calendar_leaf; target={destination_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                        f"path={destination_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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
                    click.echo(
                        f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=completed; "
                        f"rows={committed_table.num_rows}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
            finally:
                # 当前叶独立恢复；此前成功事实和日历叶保留。
                shutil.rmtree(staging_path, ignore_errors=True)
            log_committed_partitions += 1
            click.echo(
                f"partition_committed: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=commit_leaf; status=completed; "
                f"partition={partition_key}; rows={committed_table.num_rows}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; scope=calendar_leaf; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
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


# ## CLI：一次规划、月份推进与运行日志
# 
# `main()` 先检查日期参数与正式写入边界，读取本数据集日历和可选事实，一次计算已完成、无 API 修复和 API 待办集合，并复用计数及 OHLC 证据。纯状态修复不认证 JQData；有待办才连接，按年月逐日查询。
# 
# 每月汇总已归一化响应、合并完整事实叶，先提交事实并取得正式计数。完整内容已逐值一致，因此沿用本批来源 OHLC 结论生成日历，再独立提交日历叶；只读模式仅汇总内存结果。修复后与批末不再全表求差或业务复验，完成证据来自各提交函数的正式逐值复读。
# 
# 日志使用 `function/phase/status/elapsed_s`，批次起止有 `=` 分隔线。读取、查询、归一化、质量、合并、生成与提交由所属函数报告；main 负责参数、批次和月份进度。内存结果标记 `persisted=false`，月份成功只有在事实和日历均提交后报告；失败保留阶段、月份、日期与原因链。

# ### 局部流程：CLI 与月份推进
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["参数门禁；读取；一次规划"] --> B["按 write 决定是否修复陈旧日历"]
#     B --> C{"有 API 待办？"}
#     C -->|否| Z["最终结果"]
#     C -->|是| D["认证；循环前准备叶映射"]
#     D --> E["当前月逐日查询、归一化、质量旁证"]
#     E --> F{"启用 write？"}
#     F -->|否| G["累计内存结果"]
#     F -->|是| H["合并并提交事实叶；正式逐值复读"]
#     H --> I["正式计数与来源质量；生成并提交日历叶"]
#     I --> G
#     G --> J{"还有月份？"}
#     J -->|是| E
#     J -->|否| Z
#     E -. 失败 .-> R["write 时只回写失败日期；停止"]
#     H -. 失败 .-> S["恢复当前事务；停止"]
#     I -. 失败 .-> S
# ```

# In[ ]:


# CLI 先执行正式湖显式日期门禁，再读取日历或认证 JQData。
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
    log_date = "none"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n境外期货日线 / Overseas futures daily\n"
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
        ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=materialize; status=completed; "
            f"frame=calendar_df; rows={len(calendar_df)}; materialized=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        # 正式历史业务已由提交证明；描述性 metadata 以当前契约为准。
        log_phase = "read_fact"
        fact_df = read_optional_fact(fact_path)

        mode = "explicit" if has_explicit_dates else "automatic"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; mode={mode}; lake_root={resolved_lake_root}; calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; write={str(write).lower()}; "
            f"function=main; dataset={DATASET_NAME}; phase=read; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        # 正式事实和日历状态共同参与差集；已有完整事实只修复状态，不重复调用 API。
        log_phase = "plan"
        pending_df, state_repair_df, _, fact_counts, fact_warning_by_date = plan_overseas_futures_grids(
            calendar_df,
            fact_df,
            requested_start,
            requested_end,
        )
        if pending_df.empty and state_repair_df.empty:
            click.echo(
                f"planning_progress: outcome=up_to_date; api_pending_grid_count=0; persisted=false; "
                f"function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )
            return

        batch_id = uuid.uuid4().hex

        # 正式事实已完整但日历质量状态陈旧时，直接从事实复算并提交日历，不认证 JQData。
        if not state_repair_df.empty:
            repair_dates = set(state_repair_df["observation_date"].tolist())
            click.echo(
                f"state_repair_plan: grids={len(repair_dates)}; write={str(write).lower()}; api_requests=0; "
                f"function=main; dataset={DATASET_NAME}; phase=repair_plan; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if write:
                log_phase = "repair_generate"
                repair_grid_results = {
                    observation_date: fact_counts.get(observation_date, 0)
                    for observation_date in repair_dates
                }
                repair_quality_warning_by_date = {
                    observation_date: fact_warning_by_date[observation_date]
                    for observation_date in repair_dates
                    if observation_date in fact_warning_by_date
                }
                repaired_at = datetime.now(timezone.utc)
                calendar_df = apply_calendar_completion(
                    calendar_df,
                    repair_grid_results,
                    repair_quality_warning_by_date,
                    f"state-repair-{batch_id}",
                    repaired_at,
                )
                log_phase = "repair_commit"
                repaired_calendar_rows = commit_calendar_partitions(
                    calendar_df,
                    repair_dates,
                    resolved_lake_root,
                )

                # 成功提交已经完成各 dirty 叶的正式复读，无需重新全表求差。
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=main; phase=repair_batch; status=completed; "
                    f"state_repaired: grids={len(repair_dates)}; calendar_rows={repaired_calendar_rows}; api_requests=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            elif pending_df.empty:
                click.echo(
                    f"planning_progress: state_repair_preview_only: api_requests=0; persisted=false; "
                    f"function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
                )
                return

        if pending_df.empty:
            click.echo(
                f"planning_progress: outcome=state_repaired; api_requests=0; persisted=true; date_watermark=none; "
                f"function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )
            return

        # 只有真正缺少正式事实的日期才认证；纯状态修复永远不会消耗供应商连接。
        log_phase = "authenticate"
        jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
        partition_groups = list(
            pending_df.groupby(PARTITION_COLUMNS, sort=True)
        )
        click.echo(
            f"planning_progress: pending_partitions={len(partition_groups)}; pending_grids={len(pending_df)}; "
            f"function=main; dataset={DATASET_NAME}; phase=partition_plan; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        if write:
            fact_leaves = {key: frame for key, frame in fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
            calendar_leaves = {key: frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
            empty_fact_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)

        total_rows = 0
        processed_grid_count = 0

        # 按事实年—月顺序推进，已完成月份可在后续重启时直接扣除。
        for group_number, (raw_partition_key, group_df) in enumerate(
            partition_groups,
            start=1,
        ):
            partition_key = tuple(raw_partition_key)
            log_partition = partition_key
            log_date = "none"
            if write:
                partition_values = dict(zip(PARTITION_COLUMNS, partition_key, strict=True))
                partition_values["dataset_name"] = DATASET_NAME
                calendar_partition_key = tuple(partition_values[column] for column in CALENDAR_PARTITION_COLUMNS)
                calendar_leaf_df = calendar_leaves[calendar_partition_key]
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
            updated_at = datetime.now(timezone.utc)
            frames = []
            api_quality_warning_by_date: dict[date, str] = {}

            click.echo(
                f"partition_start: {group_number}/{len(partition_groups)}; partition_number={group_number}; total_partitions={len(partition_groups)}; key={partition_key}; grids={len(group_df)}; processed_grids={processed_grid_count}; total_grids={len(pending_df)}; "
                f"function=main; dataset={DATASET_NAME}; phase=partition; status=started; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            for observation_date in group_df["observation_date"].tolist():
                log_date = observation_date
                try:
                    log_phase = "fetch"
                    click.echo(
                        f"planning_progress: dataset={DATASET_NAME}; function=main; phase=fetch_batch; status=running; "
                        f"date={observation_date}; partition={partition_key}; completed_month_grids={processed_grid_count}/{len(pending_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    raw_df = query_overseas_futures_grid(jqdata, observation_date)
                    log_phase = "normalize"
                    grid_df = normalize_overseas_futures_response(
                        raw_df,
                        observation_date,
                        updated_at,
                    )
                    log_phase = "source_quality"
                    grid_quality_warning = ohlc_relation_warning_map(grid_df)
                    api_quality_warning_by_date.update(grid_quality_warning)
                except Exception as error:
                    log_failed_phase = log_phase
                    message = str(error)
                    if not message.startswith(("retryable_error:", "permanent_error:")):
                        message = f"permanent_error: {message}"

                    # 请求或转换失败时只回写失败格点，不提交当前未完成月份事实。
                    if write:
                        fetch_status = (
                            "retryable_error"
                            if message.startswith("retryable_error:")
                            else "permanent_error"
                        )
                        current_fact_count = fact_counts.get(
                            observation_date,
                            0,
                        )
                        log_phase = "failure_state"
                        calendar_leaf_df = apply_calendar_failure(
                            calendar_leaf_df,
                            observation_date,
                            fetch_status,
                            f"境外期货采集失败：{message}",
                            batch_id,
                            datetime.now(timezone.utc),
                            current_fact_count,
                        )
                        log_phase = "failure_calendar_commit"
                        commit_calendar_partitions(
                            calendar_leaf_df,
                            {observation_date},
                            resolved_lake_root,
                        )
                    log_phase = log_failed_phase
                    raise click.ClickException(message) from error

                frames.append(grid_df)

            log_phase = "aggregate"
            incoming_df = (
                pd.concat(frames, ignore_index=True)
                if any(not frame.empty for frame in frames)
                else empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)
            )
            if not write:
                total_rows += len(incoming_df)
                processed_grid_count += len(group_df)
                continue

            touched_dates = set(group_df["observation_date"].tolist())
            log_phase = "merge"
            complete_df = full_fact_partition(
                existing_fact_leaf_df,
                incoming_df,
                touched_dates,
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
                observation_date: partition_counts.get(observation_date, 0)
                for observation_date in touched_dates
            }
            # 正式完整内容已逐值复读一致，可复用本批来源 OHLC 结论。
            committed_quality_warning_by_date = api_quality_warning_by_date
            completed_at = datetime.now(timezone.utc)
            log_phase = "calendar_state"
            calendar_leaf_df = apply_calendar_completion(
                calendar_leaf_df,
                grid_results,
                committed_quality_warning_by_date,
                batch_id,
                completed_at,
            )
            log_phase = "calendar_commit"
            calendar_rows = commit_calendar_partitions(
                calendar_leaf_df,
                touched_dates,
                resolved_lake_root,
            )

            total_rows += len(incoming_df)
            processed_grid_count += len(group_df)
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=main; phase=partition_batch; status=completed; "
                f"partition={partition_key}; processed_grids={processed_grid_count}/{len(pending_df)}; calendar_rows={calendar_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        click.echo(
            f"finished: grids={processed_grid_count}; rows={total_rows}; write={str(write).lower()}; outcome={'written' if write else 'readonly'}; date_watermark=none; "
            f"function=main; dataset={DATASET_NAME}; phase=run; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: dataset={DATASET_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; key={log_partition}; date={log_date}; error={type(log_error).__name__}; "
            f"write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        raise


# ## Notebook 与脚本执行入口
# 
# 与 a01/b01、b02 一样，用显式 `notebook_args` 和 `standalone_mode=False` 执行 Notebook；默认 `[]` 不写湖，但有 API 待办时仍认证并查询 JQData。交互分支同时要求没有 `__file__`，在内核中导入同名 Python 模块不会触发业务；直接运行 `.py` 时读取终端参数。
# 
# 最后一格仅保存手动终端命令注释，运行全部单元格不会额外启动正式写入。正式写入使用 `--write`，不得用显式日期截断自动范围。

# ### 局部流程：Notebook 与脚本入口
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；默认不写入"]
#     A -->|否| C{"直接执行脚本？"}
#     C -->|是| D["Click 读取终端参数并运行"]
#     C -->|否| E["模块导入不运行"]
# ```

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook 默认不写入；有待办仍查询 API；显式日期写入必须使用非正式湖。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b03_overseas_futures",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()


# ### 局部流程：终端手动运行
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["在终端激活 latitude_env_v2；切换项目根目录"] --> B["人工执行对应 Python 脚本 --write"]
#     B --> C["自动计算待办；独立提交事实和日历叶"]
# ```

# In[ ]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a03_External_Market_Data\b03_overseas_futures.py --write

