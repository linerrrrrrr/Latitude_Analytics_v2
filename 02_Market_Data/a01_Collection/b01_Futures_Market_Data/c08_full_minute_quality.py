#!/usr/bin/env python
# coding: utf-8

# # c08 分钟主键缺失审计
# 
# 本环节只读取本地 silver，以理论分钟主键减去已提交的分钟事实主键，生成 `fact_futures_missing_bar`，并回写 `dim_futures_bar_calendar` 的缺失计数和检查时间。不调用外部 API。
# 
# | 上下游 | 数据职责与本环节的关系 |
# | --- | --- |
# | c03 → c04 | 合约 Session 结构经 c04 形成行情日历；c08 直接读取 c04 的 Session 起止、交易日和理论条数，不另读合约日历。 |
# | c06 | 在完整日历上维护当前采集要求和完成状态，生产 `fact_futures_minute`；c08 信任正式提交的事实主键、Session 归属、范围和行情质量。 |
# | c07 | 将日线与分钟聚合比较结果写入行情日历；c08 原样保留已有旁证，不调用 c07，也不读取日线事实。 |
# | c08 当前输出 | 缺失明细整表快照，以及触达日历完整叶中的五个计数、标志和时间字段。 |
# | 后续读取 | 数据库读取示例及研究消费者按权威 Schema 读取缺失明细；c04 在结构不变时继承这些日历状态。 |
# 
# 计算范围为全部 `bar_frequency=1m AND is_fetch_required=true` 的 Session，运行前要求这些 Session 全部已完成 c06。计算式为：
# 
# `required 且已完成的理论分钟主键 − 正式分钟事实的 contract_code、bar_at 主键 = 缺失分钟主键`
# 
# 理论时间采用 `(session_start_at, session_end_at]` 的分钟结束时刻语义。分钟事实仅投影 `contract_code`、`bar_at`；事实中的 OHLC 等行情值由 c06 负责，缺失审计保留已有质量结论。
# 
# 运行语义见湖仓根目录 `README.md` 与 `02_Market_Data/a02_Lake/AGENTS.md`；字段、主键、分区及状态含义以 `config/data_contracts.py` 为准，下方浏览器直接展示该契约。

# ## 人工全量范围、就绪前提与写入边界
# 
# `--confirm-full-quality` 是必需的人工全量确认；本入口不接受日期、月份、合约等范围裁剪。所有 required 的 1m Session 必须已完成 c06，否则在创建 staging 之前失败，不跳过未完成行继续生成缺失结论。
# 
# | 方式 | 计算与暂存 | 正式写入 |
# | --- | --- | --- |
# | `--confirm-full-quality` | 从指定湖全范围读取，按分区计算并在系统临时目录写入、复读 staging。 | 不提交缺失表或日历；退出时清理临时目录。 |
# | `--confirm-full-quality --write` | 执行同样的全量审计，在该湖 silver 下准备本批 staging。 | 缺失明细整表根与全部触达日历叶共同提交、共同回滚。 |
# 
# 正式湖来自 `settings.futures_lake_root`；`--lake-root` 可用于独立临时湖。c08 是人工入口，不进入 operations 总控台的日常快捷选择；可在总控台单独勾选并显式启用 `--confirm-full-quality`，提交时另选 `--write`，随后由本次确认的后台批次执行。
# 
# 日历只更新 `actual_bar_count`、`is_data_missing`、`missing_bar_count`、`missing_checked_at`、`updated_at`。调度状态、拉取要求、完成凭证、质量状态/原因/检查时间以及全部 c07 旁证均保留。缺失计数减少也不会自动把历史 `warning` 改成 `passed`。
# 
# 即使缺失为零，也生成带零行 `schema.parquet` 的缺失表，用于替换旧缺失快照；未选中行在触达日历叶内保留原值。没有 required Session 时仍读取上游契约并生成零行缺失表，不写日历叶。c08 不推进独立日期水位。

# ## 总流程：理论分钟主键求差与两表提交
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["人工启动；必须 confirm-full-quality"] --> B["打开行情日历、分钟事实；发现 1m 日历叶"]
#     B --> C{"全部 required Session 已完成 c06？"}
#     C -->|否| X["抛错；尚未创建 staging"]
#     C -->|是| D["创建暂存目录；写缺失表零行契约标记"]
#     D --> E["按交易所月、品种月读取日历与分钟两列主键"]
#     E --> F["展开理论分钟并求差；回写五个日历字段"]
#     F --> G["逐叶写入、复读 staging；验收摘要与总量"]
#     G --> H{"write？"}
#     H -->|否| I["汇总只读结果；清理临时目录后结束"]
#     H -->|是| J["一个共享事务内安装缺失表根及全部触达日历叶"]
#     J --> K["正式复读两表；检查契约、总量、分区及摘要"]
#     K --> L["清理后报告整批提交；结束"]
#     J -. 失败 .-> R["共享模块倒序恢复日历叶和缺失表；逐项尝试"]
#     K -. 失败 .-> R
#     R --> S["保留失败新数据；恢复不完整保留备份；抛错"]
# ```

# ### 局部流程：运行环境与权威依赖
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["导入标准库"] --> B["从当前目录向上查找项目标记"]
#     B --> C{"找到项目根？"}
#     C -->|否| X["抛错停止"]
#     C -->|是| D["加入项目根与湖仓模块路径"]
#     D --> E["导入 DataFrame 库、三张 Schema、转换函数、settings 及共享事务"]
# ```

# In[1]:


from __future__ import annotations

import hashlib
import pathlib
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

# Notebook 可从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
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
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
    polars_to_arrow,
    validate_arrow_table,
)
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界样例
# 
# 仅在交互式 Notebook 中展示行情日历、分钟事实、缺失明细三张直接相关表。共享浏览器从 `config/data_contracts.py` 读取权威 Schema，可按用户选择有界读取本地样例；此格不启动全量审计或提交，脚本运行和普通模块导入跳过展示。

# ### 局部流程：Schema 与有界样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"交互式 Notebook？"} -->|否| B["跳过展示"]
#     A -->|是| C["调用共享 Schema 浏览器"]
#     C --> D["展示日历、分钟事实和缺失明细契约"]
#     D --> E["可选有界本地样例；不启动全量审计"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    # 只展示本入口的直接上游和当前产出，不再引入 b07 的日线或合约旁证。
    display_schema_metadata([
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
        FUTURES_MISSING_BAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、算法键与分区
# 
# 稳定表名、主键和 Hive 分区从三张具名 Schema 的 metadata 初始化；`MINUTE_KEY_COLUMNS` 只定义求差需要的两列投影，`SESSION_MATCH_COLUMNS` 表达合约日内的 Session 身份，不另定义表契约。
# 
# | 数据 | 分区与本环节用途 |
# | --- | --- |
# | 行情日历 | `bar_frequency/exchange_code/year/month`；外层按 1m 交易所月读取完整叶，修改后保留完整叶提交。 |
# | 分钟事实 | `exchange_code/underlying_code/year/month`；按候选品种及归属交易年月读取分钟主键。 |
# | 缺失明细 | `bar_frequency/exchange_code/underlying_code/year/month`；只生成 1m，归属交易日与 Session 属性继承理论格点。 |
# 
# Hive partitioning 使用对应权威字段的类型。跨自然日的夜盘仍按理论 Session 的归属交易日与年月组织，不能按 `bar_at` 的自然日另分区。

# ### 局部流程：表身份、算法键与分区
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从权威 Schema metadata 读取表名、主键与分区"] --> B["定义分钟两列投影和 Session 身份列"]
#     B --> C["用权威字段类型构造三张表的 Hive partitioning"]
#     C --> D["供按交易所月、品种月读取和写入使用"]
# ```

# In[3]:


MISSING_TABLE_NAME = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 国内期货缺失 bar 明细事实表。
MISSING_PRIMARY_KEY = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 缺失分钟明细的正式业务主键。
MISSING_PARTITION_COLUMNS = FUTURES_MISSING_BAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 缺失明细的 Hive 叶分区。

CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # 行情拉取与质检日历。
CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")  # 唯一标识一个 1d 或 1m 日历格点。
CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 行情日历的 Hive 叶分区。

MINUTE_TABLE_NAME = FUTURES_MINUTE_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")  # b06 正式提交的一分钟行情事实表。
MINUTE_PARTITION_COLUMNS = FUTURES_MINUTE_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")  # 分钟事实的品种月 Hive 叶分区。

# 集合差只需要事实表级主键本身；交易日和 Session 属性从理论格点继承。
MINUTE_KEY_COLUMNS = [
    "contract_code",  # 固定月份合约代码。
    "bar_at",  # 实际一分钟 bar 的 Asia/Shanghai 结束时刻。
]
SESSION_MATCH_COLUMNS = [
    "contract_code",  # 固定月份合约代码。
    "trading_date",  # Session 归属的期货交易日。
    "session_number",  # 同一合约日内的 Session 顺序号。
]

MISSING_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_MISSING_BAR_SCHEMA.field(name)
        for name in MISSING_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in CALENDAR_PARTITION_COLUMNS
    ]),
    flavor="hive",
)
MINUTE_PARTITIONING = ds.partitioning(
    pa.schema([
        FUTURES_MINUTE_SCHEMA.field(name)
        for name in MINUTE_PARTITION_COLUMNS
    ]),
    flavor="hive",
)


# ## 数据集读取、分区发现与确定性内容摘要
# 
# `open_exact_dataset()` 确认存在 Parquet 后打开 Dataset，检查重建的逻辑 Schema 和全部 fragment 的物理字段、类型、nullable 与表身份。描述性 metadata 差异不阻塞历史读取。返回 Dataset 后，调用方再按分区物化所需列。
# 
# `parquet_file_schema()` 从权威 Schema 排除 Hive 分区列，得到文件字段；`partition_expression()` 将分区等值条件以 AND 连接。`discover_partition_keys()` 根据目录发现叶键，跳过根级 `schema.parquet`，检查目录层级和列顺序，并将年、月解释为整数。
# 
# `table_digest()` 接收调用方已经按权威 Schema 转换的 Arrow 表，按主键排序，对全部逻辑内容生成 SHA-256；函数不再次转换或校验同一表。权威类型、metadata 与列顺序由转换和摘要编码共同固定。摘要单独记录 null 有效位，并归一 null 槽位、NaN payload 和正负零的物理表示，避免逻辑值相同却产生不同摘要。
# 
# 输入检查不重新承担 c06 行情质量验证；c08 自己的 staging 和正式输出继续保留逐 fragment 契约及完整内容摘要验收。上游、staging 汇总和正式复读各在表根打开后检查一次全部 fragment；逐叶完整内容摘要用于证明复读结果与已验收输出一致。
# 
# 读取函数自行报告发现、打开、契约检查和失败阶段；`materialized=false` 表示仅打开和检查文件，尚未读取业务行。fragment 检查沿现有遍历在首个及每 250 个文件报告数量；分区发现也沿原目录遍历报告已访问文件和已发现叶数。`table_digest()` 报告排序、规范化与摘要完成，列规范化在首列、每 16 列和末列报告进度。实际业务行的读取日志仍紧贴生成/提交函数中的 `to_table()`，不新增读取或日志包装函数。

# ### 局部流程：数据集读取、分区发现与摘要
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["读取函数报告开始；发现 Parquet 并打开"] --> B["检查逻辑契约和 fragment；沿原遍历报告进度"]
#     B --> C["报告完成；materialized=false；调用方物化"]
#     D["discover_partition_keys：遍历 Parquet 路径"] --> E["跳过根标记；解析叶键；报告发现数量"]
#     F["table_digest：接收已转换的 Arrow 表"] --> G["按主键排序；归一 null、NaN 和正负零表示"]
#     G --> H["报告列进度与摘要完成；异常报告阶段并抛错"]
#     B -. 不兼容 .-> X["抛错"]
#     E -. 非法路径 .-> X
# ```

# In[4]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    # Hive 分区列由目录补回；按权威列顺序重建完整 Dataset Schema。
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]


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


def parquet_file_schema(
    schema: pa.Schema,
    partition_columns: list[str],
) -> pa.Schema:
    # 单个 Parquet 文件不重复保存 Hive 目录字段。
    partition_set = set(partition_columns)
    return pa.schema(
        [field for field in schema if field.name not in partition_set],
        metadata=schema.metadata,
    )


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "discovery"
    log_table_name = (
        CALENDAR_TABLE_NAME if schema is FUTURES_BAR_CALENDAR_SCHEMA
        else MINUTE_TABLE_NAME if schema is FUTURES_MINUTE_SCHEMA
        else MISSING_TABLE_NAME if schema is FUTURES_MISSING_BAR_SCHEMA
        else "unknown"
    )
    log_checked_fragments = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
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
        log_phase = "logical_schema"
        if not physically_and_identity_compatible(
            reconstructed_schema(dataset, schema),
            schema,
        ):
            raise TypeError(f"{label} Dataset 物理结构或表身份与契约不一致。")

        expected_file_schema = parquet_file_schema(
            schema,
            partition_columns,
        )
        log_phase = "fragment_schema"
        for fragment in dataset.get_fragments():
            if not physically_and_identity_compatible(
                fragment.physical_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label} fragment 物理结构或表身份与契约不一致："
                    f"{fragment.path}"
                )
            log_checked_fragments += 1
            if log_checked_fragments == 1 or log_checked_fragments % 250 == 0:
                click.echo(
                    f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=fragment_schema; status=running; "
                    f"label={label}; checked_fragments={log_checked_fragments}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; path={table_path}; checked_fragments={log_checked_fragments}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=open_exact_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; checked_fragments={log_checked_fragments}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    # 直接构造 Arrow 表达式，不拼接字符串查询条件。
    expression = None
    for column, value in zip(
        partition_columns,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = (
            condition
            if expression is None
            else expression & condition
        )

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


def discover_partition_keys(
    table_path: pathlib.Path,
    partition_columns: list[str],
) -> set[tuple[object, ...]]:
    log_started_at = time.perf_counter()
    log_phase = "partition_discovery"
    log_visited_files = 0
    click.echo(
        f"planning_progress: table={table_path.name}; function=discover_partition_keys; phase=partition_discovery; status=started; "
        f"path={table_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 根级 schema.parquet 只证明零行契约，不属于业务叶分区。
        partition_keys = set()
        for parquet_path in table_path.rglob("*.parquet"):
            log_visited_files += 1
            if log_visited_files == 1 or log_visited_files % 250 == 0:
                click.echo(
                    f"planning_progress: table={table_path.name}; function=discover_partition_keys; phase=partition_discovery; status=running; "
                    f"path={table_path}; visited_files={log_visited_files}; discovered_partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
            if parquet_path.name == "schema.parquet":
                continue

            relative_parts = parquet_path.relative_to(table_path).parts
            if len(relative_parts) != len(partition_columns) + 1:
                raise ValueError(f"数据集包含非法分区文件：{parquet_path}")

            values = []
            for column, directory_name in zip(
                partition_columns,
                relative_parts[:-1],
                strict=True,
            ):
                prefix = f"{column}="
                if not directory_name.startswith(prefix):
                    raise ValueError(f"数据集包含非法分区目录：{parquet_path}")

                raw_value = directory_name[len(prefix):]
                values.append(
                    int(raw_value)
                    if column in {"year", "month"}
                    else raw_value
                )
            partition_keys.add(tuple(values))

        click.echo(
            f"planning_progress: table={table_path.name}; function=discover_partition_keys; phase=partition_discovery; status=completed; "
            f"path={table_path}; visited_files={log_visited_files}; partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return partition_keys
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=discover_partition_keys; phase=partition_discovery; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; path={table_path}; visited_files={log_visited_files}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def table_digest(
    table: pa.Table,
    schema: pa.Schema,
    sort_columns: list[str],
) -> str:
    log_started_at = time.perf_counter()
    log_phase = "sort"
    log_table_name = (
        CALENDAR_TABLE_NAME if schema is FUTURES_BAR_CALENDAR_SCHEMA
        else MINUTE_TABLE_NAME if schema is FUTURES_MINUTE_SCHEMA
        else MISSING_TABLE_NAME if schema is FUTURES_MISSING_BAR_SCHEMA
        else "unknown"
    )
    log_canonical_columns = 0
    click.echo(
        f"planning_progress: table={log_table_name}; function=table_digest; phase=digest; status=started; "
        f"rows={table.num_rows}; columns={table.num_columns}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        # 摘要覆盖权威类型、metadata、列顺序及确定性行顺序。
        click.echo(
            f"planning_progress: table={log_table_name}; function=table_digest; phase=digest_sort; status=started; "
            f"rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        sort_indices = pc.sort_indices(
            table,
            sort_keys=[(name, "ascending") for name in sort_columns],
        )
        sorted_table = table.take(sort_indices)
        click.echo(
            f"planning_progress: table={log_table_name}; function=table_digest; phase=digest_sort; status=completed; "
            f"rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "canonicalize"

        # null 槽位的数据 buffer 不是逻辑值；先记录有效位，再用类型固定值填充。
        canonical_columns = []
        validity_masks = []
        for field, column in zip(schema, sorted_table.columns, strict=True):
            array = column.combine_chunks()
            validity_masks.append(
                array.is_valid().to_numpy(zero_copy_only=False).tobytes()
            )
            if array.null_count:
                if pa.types.is_boolean(field.type):
                    fill_value = False
                elif pa.types.is_string(field.type):
                    fill_value = ""
                else:
                    fill_value = 0
                array = array.fill_null(pa.scalar(fill_value, type=field.type))

            if pa.types.is_floating(field.type):
                # NaN payload 与 -0.0 的物理位模式不属于业务值语义。
                array = pc.if_else(
                    pc.is_nan(array),
                    pa.scalar(float("nan"), type=field.type),
                    array,
                )
                array = pc.if_else(
                    pc.equal(array, pa.scalar(0.0, type=field.type)),
                    pa.scalar(0.0, type=field.type),
                    array,
                )
            canonical_columns.append(array)
            log_canonical_columns += 1
            if log_canonical_columns == 1 or log_canonical_columns % 16 == 0 or log_canonical_columns == len(schema):
                click.echo(
                    f"planning_progress: table={log_table_name}; function=table_digest; phase=canonicalize; status=running; "
                    f"columns={log_canonical_columns}/{len(schema)}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        canonical_table = pa.Table.from_arrays(
            canonical_columns,
            schema=schema,
        )
        log_phase = "serialize_and_hash"
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_table(canonical_table)

        digest = hashlib.sha256(sink.getvalue().to_pybytes())
        for validity_mask in validity_masks:
            digest.update(len(validity_mask).to_bytes(8, byteorder="little"))
            digest.update(validity_mask)
        click.echo(
            f"planning_progress: table={log_table_name}; function=table_digest; phase=digest; status=completed; "
            f"rows={table.num_rows}; columns={log_canonical_columns}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return digest.hexdigest()
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={log_table_name}; function=table_digest; phase=digest; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; columns={log_canonical_columns}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 本环节产出与回写字段的验收
# 
# `validate_missing_output()` 接收已经由 `polars_to_arrow()` 转换的缺失明细，只检查非空结果的主键唯一性与目标分区归属；零行结果直接返回。
# 
# `validate_calendar_audit_output()` 将完整日历叶转换为权威类型并检查分区归属；针对 required 的 1m 行，检查已完成状态、实际/缺失条数非负、两者相加等于理论条数、缺失标志一致，以及两个回写时间均等于本批 `detected_at`。
# 
# 这里验证本环节的输出和直接计算前提，不重新检查分钟行情数值，也不重新判定已有质量或旁证。两个业务 validator 各在生成完整输出叶时执行一次。staging 和正式复读使用权威 Arrow 转换及完整内容摘要比较，不重复主键/分区业务检查或日历计数、状态、时间关系检查。
# 
# 两个 validator 自行报告输入行数、目标叶、完成与失败阶段；空缺失表和无 required 行的日历叶也有完成日志。日历转换后的 Arrow 表直接转为 Pandas 用于业务检查，不再通过 `arrow_to_pandas()` 对同一 Arrow 表重复校验。主键与分区归属、计数关系和审计时间的原有检查仍在输出边界保留。

# ### 局部流程：自身输出的验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告缺失明细校验开始与输入行数"] --> B["接收已转换表；非空时检查主键和分区"]
#     B --> C["报告完成；返回已校验缺失表"]
#     D["报告日历叶校验开始与输入行数"] --> E["转换契约并检查分区；选择 required 1m 行"]
#     E --> F["检查完成状态、非负计数及实际加缺失等于理论"]
#     F --> G["检查缺失标志和本批两个回写时间"]
#     G --> H["报告完成；返回日历表；保留质量与旁证"]
#     B -. 不满足 .-> X["报告失败阶段；原样抛错"]
#     F -. 不满足 .-> X
#     G -. 不满足 .-> X
# ```

# In[5]:


def validate_missing_output(
    table: pa.Table,
    partition_key: tuple[object, ...],
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "primary_key"

    click.echo(
        f"planning_progress: table={MISSING_TABLE_NAME}; function=validate_missing_output; phase=validate_missing; status=started; "
        f"context={context}; partition={partition_key}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        if table.num_rows == 0:
            click.echo(
                f"planning_progress: table={MISSING_TABLE_NAME}; function=validate_missing_output; phase=validate_missing; status=completed; "
                f"context={context}; partition={partition_key}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return table

        checked_frame = pl.from_arrow(table)
        if checked_frame.select(MISSING_PRIMARY_KEY).is_duplicated().any():
            raise ValueError(f"{context}缺失明细主键不唯一。")

        log_phase = "partition_membership"
        actual_partition_keys = set(
            checked_frame.select(MISSING_PARTITION_COLUMNS).unique().rows()
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError(f"{context}缺失明细越出目标 Hive 分区。")

        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=validate_missing_output; phase=validate_missing; status=completed; "
            f"context={context}; partition={partition_key}; rows={table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=validate_missing_output; phase=validate_missing; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def validate_calendar_audit_output(
    frame: pd.DataFrame,
    partition_key: tuple[object, ...],
    detected_at: datetime,
    context: str,
) -> pa.Table:
    log_started_at = time.perf_counter()
    log_phase = "conversion"

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_audit_output; phase=validate_calendar; status=started; "
        f"context={context}; partition={partition_key}; rows={len(frame)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        checked_table = pandas_to_arrow(
            frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        checked_frame = checked_table.to_pandas(types_mapper=pd.ArrowDtype)

        log_phase = "partition_membership"
        actual_partition_keys = set(
            checked_frame[CALENDAR_PARTITION_COLUMNS].itertuples(
                index=False,
                name=None,
            )
        )
        if actual_partition_keys != {partition_key}:
            raise ValueError(f"{context}行情日历越出目标 Hive 分区。")

        log_phase = "audit_fields"
        selected = checked_frame.loc[
            checked_frame["bar_frequency"].eq("1m")
            & checked_frame["is_fetch_required"].eq(True)
        ]
        if selected.empty:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_audit_output; phase=validate_calendar; status=completed; "
                f"context={context}; partition={partition_key}; rows={checked_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return checked_table
        if not selected["is_fetch_completed"].all():
            raise ValueError(f"{context}required Session 尚未完成 b06。")

        expected = selected["expected_bar_count"].astype("int64")
        actual = selected["actual_bar_count"].astype("int64")
        missing = selected["missing_bar_count"].astype("int64")
        if (actual < 0).any() or (missing < 0).any():
            raise ValueError(f"{context}实际或缺失条数为负。")
        if not (actual + missing).eq(expected).all():
            raise ValueError(f"{context}实际与缺失条数不能还原理论条数。")
        if not selected["is_data_missing"].eq(missing.gt(0)).all():
            raise ValueError(f"{context}缺失标志与缺失条数不一致。")

        log_phase = "audit_timestamps"
        detected_timestamp = pd.Timestamp(detected_at)
        if not selected["missing_checked_at"].eq(detected_timestamp).all():
            raise ValueError(f"{context}缺失检查时间未统一更新。")
        if not selected["updated_at"].eq(detected_timestamp).all():
            raise ValueError(f"{context}更新时间未统一更新。")

        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_audit_output; phase=validate_calendar; status=completed; "
            f"context={context}; partition={partition_key}; rows={checked_table.num_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return checked_table
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_audit_output; phase=validate_calendar; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; partition={partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 按分区生成缺失明细与日历 staging
# 
# `build_full_audit_staging()` 先打开行情日历和分钟事实，发现 1m 日历叶，再用窄列完成一次全局就绪检查。所有 required Session 已完成后才创建两个 staging 目录，并写缺失表零行契约标记。
# 
# 外层按交易所月读取完整日历叶；选出 required 行后按品种分组；完成状态已由 staging 前的全局就绪检查保证，不在每个叶重复检查。事实侧严格只读取该品种月的 `contract_code`、`bar_at`。Polars 用 `datetime_ranges + explode` 展开 `(start, end]` 的理论分钟，再以 `anti join` 求差，并核对展开数量与理论条数一致。
# 
# 缺失点按原日历行索引汇总；实际条数按“理论条数 − 缺失条数”得到，表示理论范围内已存在的主键数，不是整个事实分区的总行数。日历只回写五个计数、时间字段。非空缺失结果逐品种月写入并复读 staging；零缺失不写业务叶，只保留表根契约标记。
# 
# 当前交易所月全部品种处理完毕后，校验、写入并复读更新后的完整日历叶。每个完整输出叶执行业务校验一次；staging 直接打开当前叶，转换为权威 Arrow 后比较完整内容摘要。末尾各打开一次 staging 表根，检查缺失表总行数及两个数据集的全部 fragment 契约。结果包含叶摘要和累计 Session、理论键、实际键、缺失键数量，尚未提交正式数据。
# 
# 无 required 行的日历叶跳过回写；不会把全历史分钟值或所有缺失明细合成一个内存大表。
# 
# 生成函数自行报告全局就绪、当前日历叶与品种月读取、理论展开、主键求差、两类 staging 写入/复读和整批验收。沿现有循环累计日历进度、已处理品种组、Session 数与缺失键数，不为日志另扫数据；无 required 行的叶报告跳过。`missing_audit_total:` 由本函数在所有 staging 验收完成后输出，始终记 `persisted=false`。失败报告当前阶段、日历叶、品种月及处理进度，并原样抛错。
# 
# 列清单、理论分钟范围表达式、时区转换表达式以及本批频率/检测时间表达式在分区循环前建立，按当前品种组求值。分钟两列投影后不再次选择同样两列；缺失明细不在写入前额外排序，摘要阶段统一按主键排序。上游根只在生成前打开，逐分区只物化当前日历叶和当前品种月分钟主键，不逐分区复查全表。

# ### 局部流程：分区求差、状态回写与暂存验收
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告生成开始；打开上游、发现叶并检查就绪"] --> B["创建 staging；写缺失表零行标记"]
#     B --> C["报告当前日历叶读取进度；选择 required 行"]
#     C --> D{"有选中 Session？"}
#     D -->|否| N{"还有日历叶？"}
#     D -->|是| E["报告品种月读取、理论展开与求差进度"]
#     E --> F["anti join 求缺失；按日历行汇总计数"]
#     F --> G["更新五个字段；其余日历状态保留"]
#     G --> H{"本品种月有缺失？"}
#     H -->|是| I["缺失叶业务校验一次；暂存后直接复读当前叶并比摘要"]
#     H -->|否| J["累计并报告本组及整批 Session、主键数量"]
#     I --> J
#     J --> K{"还有品种？"}
#     K -->|是| E
#     K -->|否| L["日历叶业务校验一次；暂存后直接复读当前叶并比摘要"]
#     L --> N
#     N -->|是| C
#     N -->|否| O["staging 总验收后报告生成完成；persisted=false"]
# ```

# In[6]:


def build_full_audit_staging(
    lake_root: pathlib.Path,
    missing_staging_path: pathlib.Path,
    calendar_staging_path: pathlib.Path,
    detected_at: datetime,
) -> dict[str, object]:
    log_started_at = time.perf_counter()
    log_phase = "prepare_paths"
    log_calendar_index = 0
    log_underlying_groups = 0
    log_partition = None
    log_minute_partition = None
    click.echo(
        f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=build_audit; status=started; "
        f"lake_root={lake_root}; detected_at={detected_at}; missing_staging={missing_staging_path}; calendar_staging={calendar_staging_path}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        silver_root = lake_root.resolve() / "silver"
        calendar_path = silver_root / CALENDAR_TABLE_NAME
        minute_path = silver_root / MINUTE_TABLE_NAME

        # 上游必须是各生产者已经正式提交的精确契约数据集。
        log_phase = "open_upstream"
        calendar_dataset = open_exact_dataset(
            calendar_path,
            CALENDAR_PARTITIONING,
            FUTURES_BAR_CALENDAR_SCHEMA,
            CALENDAR_PARTITION_COLUMNS,
            "行情日历",
        )
        minute_dataset = open_exact_dataset(
            minute_path,
            MINUTE_PARTITIONING,
            FUTURES_MINUTE_SCHEMA,
            MINUTE_PARTITION_COLUMNS,
            "一分钟行情事实",
        )

        log_phase = "partition_discovery"
        calendar_partition_keys = sorted(
            key
            for key in discover_partition_keys(
                calendar_path,
                CALENDAR_PARTITION_COLUMNS,
            )
            if key[0] == "1m"
        )

        # 先在任何 staging I/O 之前完成全局就绪门禁，避免把未采集 Session 误报为缺失。
        log_phase = "readiness"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=readiness; status=started; "
            f"calendar_partitions={len(calendar_partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        readiness_table = calendar_dataset.to_table(
            columns=[
                *CALENDAR_PRIMARY_KEY,
                "is_fetch_required",
                "is_fetch_completed",
            ],
            filter=(
                ds.field("bar_frequency") == "1m"
            ) & ds.field("is_fetch_required"),
        )
        readiness_df = readiness_table.to_pandas()
        if (
            not readiness_df.empty
            and not readiness_df["is_fetch_completed"].all()
        ):
            pending_examples = readiness_df.loc[
                readiness_df["is_fetch_completed"].ne(True),
                CALENDAR_PRIMARY_KEY,
            ].head(5)
            raise ValueError(
                "分钟缺失审计只能在全部 required Session 完成 b06 后运行；"
                f"示例：{pending_examples.to_dict(orient='records')}"
            )

        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=readiness; status=completed; "
            f"required_sessions={len(readiness_df)}; calendar_partitions={len(calendar_partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "prepare_staging"
        missing_staging_path.mkdir(parents=True, exist_ok=False)
        calendar_staging_path.mkdir(parents=True, exist_ok=False)

        # 即使本轮没有缺失，也写根级零行 marker，使正式表仍具有精确契约。
        missing_file_schema = parquet_file_schema(
            FUTURES_MISSING_BAR_SCHEMA,
            MISSING_PARTITION_COLUMNS,
        )
        pq.write_table(
            pa.Table.from_batches([], schema=missing_file_schema),
            missing_staging_path / "schema.parquet",
        )

        calendar_digests: dict[tuple[object, ...], str] = {}
        missing_digests: dict[tuple[object, ...], str] = {}
        total_sessions = 0
        total_expected = 0
        total_actual = 0
        total_missing = 0

        calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
        missing_columns = FUTURES_MISSING_BAR_SCHEMA.names
        missing_partition_base_dir = missing_staging_path.as_posix()
        calendar_partition_base_dir = calendar_staging_path.as_posix()
        session_input_columns = [
            "_calendar_index", "contract_code", "exchange_code", "underlying_code",
            "trading_date", "session_number", "session_start_at", "session_end_at",
            "expected_bar_count", "year", "month",
        ]
        expected_range_expression = pl.datetime_ranges(
            pl.col("session_start_at") + pl.duration(minutes=1),
            pl.col("session_end_at"),
            interval="1m",
            closed="both",
        ).alias("expected_bar_at")
        expected_time_expression = pl.col("expected_bar_at").cast(
            pl.Datetime("us", "Asia/Shanghai")
        )
        missing_audit_expressions = [
            pl.lit("1m").alias("bar_frequency"),
            pl.lit(detected_at).alias("detected_at"),
        ]

        for calendar_partition_key in calendar_partition_keys:
            log_calendar_index += 1
            log_partition = calendar_partition_key
            log_minute_partition = None
            log_phase = "calendar_read"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=calendar_read; status=started; "
                f"partition={calendar_partition_key}; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            calendar_table = calendar_dataset.to_table(
                columns=calendar_columns,
                filter=partition_expression(
                    CALENDAR_PARTITION_COLUMNS,
                    calendar_partition_key,
                ),
            )
            calendar_df = arrow_to_pandas(
                calendar_table,
                FUTURES_BAR_CALENDAR_SCHEMA,
            )

            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=calendar_read; status=completed; "
                f"partition={calendar_partition_key}; rows={len(calendar_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_phase = "select_sessions"
            selected_df = calendar_df.loc[
                calendar_df["bar_frequency"].eq("1m")
                & calendar_df["is_fetch_required"].eq(True)
            ].copy()
            if selected_df.empty:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=calendar_partition; status=skipped; "
                    f"partition={calendar_partition_key}; reason=no_required_sessions; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                continue

            _, exchange_code, year, month = calendar_partition_key

            # 日历完整叶只建立一次索引，后续按品种批量回写五个缺失字段。
            for underlying_code, selected_underlying_df in selected_df.groupby(
                "underlying_code",
                sort=True,
            ):
                minute_partition_key = (
                    exchange_code,
                    underlying_code,
                    year,
                    month,
                )
                log_minute_partition = minute_partition_key
                log_phase = "minute_read"
                click.echo(
                    f"planning_progress: table={MINUTE_TABLE_NAME}; function=build_full_audit_staging; phase=minute_read; status=started; "
                    f"partition={minute_partition_key}; sessions={len(selected_underlying_df)}; columns=contract_code,bar_at; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                minute_table = minute_dataset.to_table(
                    # 事实侧严格只读取正式主键两列；Hive 分区列来自路径过滤。
                    columns=MINUTE_KEY_COLUMNS,
                    filter=partition_expression(
                        MINUTE_PARTITION_COLUMNS,
                        minute_partition_key,
                    ),
                )
                click.echo(
                    f"planning_progress: table={MINUTE_TABLE_NAME}; function=build_full_audit_staging; phase=minute_read; status=completed; "
                    f"partition={minute_partition_key}; rows={minute_table.num_rows}; columns=contract_code,bar_at; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "expand_expected"
                actual_keys = (
                    pl.from_arrow(minute_table)
                    .rename({"bar_at": "expected_bar_at"})
                )

                session_input = (
                    selected_underlying_df.reset_index(
                        names="_calendar_index"
                    )
                    .loc[:, session_input_columns]
                )
                sessions = pl.from_pandas(session_input, include_index=False)

                # bar_at 采用分钟结束时刻语义，理论区间固定为 (start, end]。
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=expand_expected; status=started; "
                    f"partition={minute_partition_key}; sessions={len(selected_underlying_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                expected_points = (
                    sessions.with_columns(expected_range_expression)
                    .explode("expected_bar_at")
                    .with_columns(expected_time_expression)
                )
                expected_count = int(
                    selected_underlying_df["expected_bar_count"].sum()
                )
                if expected_points.height != expected_count:
                    raise ValueError(
                        f"{minute_partition_key} 理论时点展开条数与 b04 不一致。"
                    )

                # b06 保证事实键唯一且属于理论 Session；b08 只做存在性反连接。
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=expand_expected; status=completed; "
                    f"partition={minute_partition_key}; expected_keys={expected_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "key_difference"
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=key_difference; status=started; "
                    f"partition={minute_partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                missing_points = expected_points.join(
                    actual_keys,
                    on=["contract_code", "expected_bar_at"],
                    how="anti",
                )
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=key_difference; status=completed; "
                    f"partition={minute_partition_key}; expected_keys={expected_count}; missing_keys={missing_points.height}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "calendar_update"
                missing_counts = {
                    int(index): int(count)
                    for index, count in (
                        missing_points.group_by("_calendar_index")
                        .len()
                        .select(["_calendar_index", "len"])
                        .iter_rows()
                    )
                }

                calendar_indices = selected_underlying_df.index.to_list()
                expected_counts = (
                    selected_underlying_df["expected_bar_count"]
                    .to_list()
                )
                missing_values = [
                    missing_counts.get(int(index), 0)
                    for index in calendar_indices
                ]
                actual_values = [
                    expected_value - missing_value
                    for expected_value, missing_value in zip(
                        expected_counts,
                        missing_values,
                        strict=True,
                    )
                ]

                calendar_df.loc[
                    calendar_indices,
                    "actual_bar_count",
                ] = actual_values
                calendar_df.loc[
                    calendar_indices,
                    "is_data_missing",
                ] = [value > 0 for value in missing_values]
                calendar_df.loc[
                    calendar_indices,
                    "missing_bar_count",
                ] = missing_values
                calendar_df.loc[
                    calendar_indices,
                    "missing_checked_at",
                ] = detected_at
                calendar_df.loc[
                    calendar_indices,
                    "updated_at",
                ] = detected_at

                missing_partition_key = (
                    "1m",
                    exchange_code,
                    underlying_code,
                    year,
                    month,
                )
                if missing_points.height:
                    log_phase = "missing_staging_write"
                    click.echo(
                        f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=missing_staging; status=started; "
                        f"partition={missing_partition_key}; rows={missing_points.height}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    missing_output = (
                        missing_points.with_columns(missing_audit_expressions)
                        .select(missing_columns)
                    )
                    missing_arrow = polars_to_arrow(
                        missing_output,
                        FUTURES_MISSING_BAR_SCHEMA,
                    )
                    missing_arrow = validate_missing_output(
                        missing_arrow,
                        missing_partition_key,
                        "待写入 staging 的",
                    )
                    ds.write_dataset(
                        missing_arrow,
                        missing_staging_path,
                        format="parquet",
                        partitioning=MISSING_PARTITIONING,
                        existing_data_behavior="delete_matching",
                        basename_template="part-{i}.parquet",
                    )

                    log_phase = "missing_staging_readback"
                    staged_missing_leaf_path = missing_staging_path.joinpath(*[
                        f"{name}={value}"
                        for name, value in zip(
                            MISSING_PARTITION_COLUMNS, missing_partition_key, strict=True,
                        )
                    ])
                    staged_missing_dataset = ds.dataset(
                        staged_missing_leaf_path,
                        format="parquet",
                        partitioning=MISSING_PARTITIONING,
                        partition_base_dir=missing_partition_base_dir,
                    )
                    staged_missing_table = validate_arrow_table(
                        staged_missing_dataset.to_table(columns=missing_columns),
                        FUTURES_MISSING_BAR_SCHEMA,
                    )
                    expected_digest = table_digest(
                        missing_arrow,
                        FUTURES_MISSING_BAR_SCHEMA,
                        MISSING_PRIMARY_KEY,
                    )
                    actual_digest = table_digest(
                        staged_missing_table,
                        FUTURES_MISSING_BAR_SCHEMA,
                        MISSING_PRIMARY_KEY,
                    )
                    if actual_digest != expected_digest:
                        raise ValueError(
                            "staging 缺失明细内容检查失败："
                            f"partition={missing_partition_key}"
                        )
                    missing_digests[missing_partition_key] = expected_digest
                    click.echo(
                        f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=missing_staging; status=completed; "
                        f"partition={missing_partition_key}; rows={missing_arrow.num_rows}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )

                partition_missing = missing_points.height
                partition_actual = expected_count - partition_missing
                total_sessions += len(selected_underlying_df)
                total_expected += expected_count
                total_actual += partition_actual
                total_missing += partition_missing
                log_underlying_groups += 1
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=underlying_partition; status=completed; "
                    f"partition={minute_partition_key}; processed_groups={log_underlying_groups}; sessions={len(selected_underlying_df)}; expected_keys={expected_count}; actual_keys={partition_actual}; missing_keys={partition_missing}; total_sessions={total_sessions}; total_missing={total_missing}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

            # 只在全部品种求差完成后提交本交易所月的完整日历叶。
            log_phase = "calendar_staging_write"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=calendar_staging; status=started; "
                f"partition={calendar_partition_key}; rows={len(calendar_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            calendar_arrow = validate_calendar_audit_output(
                calendar_df,
                calendar_partition_key,
                detected_at,
                "待写入 staging 的",
            )
            ds.write_dataset(
                calendar_arrow,
                calendar_staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
            log_phase = "calendar_staging_readback"
            staged_calendar_leaf_path = calendar_staging_path.joinpath(*[
                f"{name}={value}"
                for name, value in zip(
                    CALENDAR_PARTITION_COLUMNS, calendar_partition_key, strict=True,
                )
            ])
            staged_calendar_dataset = ds.dataset(
                staged_calendar_leaf_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                partition_base_dir=calendar_partition_base_dir,
            )
            staged_calendar_table = validate_arrow_table(
                staged_calendar_dataset.to_table(columns=calendar_columns),
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            expected_digest = table_digest(
                calendar_arrow,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PRIMARY_KEY,
            )
            actual_digest = table_digest(
                staged_calendar_table,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PRIMARY_KEY,
            )
            if actual_digest != expected_digest:
                raise ValueError(
                    "staging 行情日历内容检查失败："
                    f"partition={calendar_partition_key}"
                )
            calendar_digests[calendar_partition_key] = expected_digest
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=build_full_audit_staging; phase=calendar_staging; status=completed; "
                f"partition={calendar_partition_key}; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}; staged_calendar_partitions={len(calendar_digests)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        log_phase = "staging_final_readback"
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=staging_final_readback; status=started; "
            f"calendar_partitions={len(calendar_digests)}; missing_partitions={len(missing_digests)}; expected_missing_rows={total_missing}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        staged_missing_dataset = open_exact_dataset(
            missing_staging_path,
            MISSING_PARTITIONING,
            FUTURES_MISSING_BAR_SCHEMA,
            MISSING_PARTITION_COLUMNS,
            "staging 缺失明细",
        )
        if staged_missing_dataset.count_rows() != total_missing:
            raise ValueError("staging 缺失明细总行数与主键求差汇总不一致。")

        if calendar_digests:
            open_exact_dataset(
                calendar_staging_path,
                CALENDAR_PARTITIONING,
                FUTURES_BAR_CALENDAR_SCHEMA,
                CALENDAR_PARTITION_COLUMNS,
                "staging 行情日历",
            )

        click.echo(
            f"missing_audit_total: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=build_audit; status=completed; "
            f"sessions={total_sessions}; expected_keys={total_expected}; actual_keys={total_actual}; missing_keys={total_missing}; calendar_partitions={len(calendar_digests)}; missing_partitions={len(missing_digests)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return {
            "calendar_digests": calendar_digests,
            "missing_digests": missing_digests,
            "total_sessions": total_sessions,
            "total_expected": total_expected,
            "total_actual": total_actual,
            "total_missing": total_missing,
        }
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=build_full_audit_staging; phase=build_audit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; calendar_partition={log_partition}; minute_partition={log_minute_partition}; calendar_index={log_calendar_index}; processed_groups={log_underlying_groups}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 缺失表根与日历叶的共享事务
# 
# `commit_full_audit()` 接收已验证的本批 staging 根、叶摘要与汇总。在 `silver/.c08-s-<run_id前12位>/` 下分别暂存缺失表和日历表，进入一个 `StagedPathTransaction`：先安装整个缺失表根，再按顺序安装全部触达日历叶。两者属于同一次共同回滚范围，缺失表零行契约标记随整根安装；日历根级标记保持原样。
# 
# 正式复读仍直接留在本函数：缺失表检查物理/身份契约、总行数、分区集合和逐叶完整内容摘要；日历检查数据集契约及触达叶完整内容摘要。各输出叶在生成时已完成业务校验，复读不重复这些业务检查。当前正式日历表根只打开一次检查全部 fragment，再逐个读取触达叶。
# 
# 共享模块按实际移动记录倒序恢复，先隔离已安装的新日历叶并恢复旧叶，最后恢复缺失表根；原本不存在的目标恢复为不存在。第一次备份失败时，未移动的旧目标保持原位。一处恢复失败仍继续尝试其余目标。恢复完整清理 staging 和 backup、保留失败新数据的隔离目录；恢复不完整保留 backup 和隔离目录，staging 仍清理。备份和隔离分别位于 `silver/.c08-b-<run_id前12位>/`、`silver/.c08-f-<run_id前12位>/`，内部按正式表名与叶路径保留对应关系。异常报告现场路径，并以原安装或验收异常作为原因。
# 
# 本函数报告安装和正式复读进度，共享模块报告失败恢复结果。单项安装或验收成功仍记 `batch_state=pending`；只有两表正式验收并成功退出事务后，才报告 `committed:` 和 `phase=audit_state; persisted=true; date_watermark=none`，随后返回缺失行数和日历分区数。c08 不写独立日期水位，也不改变完成凭证、质量结论或 c07 旁证。
# 
# 生成失败及事务进入失败只清理本次 staging，不动正式目标。共享模块使用同一文件系统内的路径替换，不提供外部读者的跨目录原子可见性、进程终止后的自动恢复或并发写入协调。

# ### 局部流程：两表共享安装与共同恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告提交开始；确认 staging 位于 silver 内"] --> B["进入一个共享事务：缺失表根＋全部触达日历叶"]
#     B -. 进入失败 .-> X["清理 staging；抛错；正式目标不变"]
#     B --> C["共享模块备份并安装缺失表根、逐个日历叶"]
#     C --> D["本函数复读缺失表契约、总量、分区集合及内容摘要"]
#     D --> E["本函数复读日历契约与触达叶内容摘要；仍记 pending"]
#     E --> F["成功退出事务并清理；报告 audit_state 已落盘"]
#     C -. 失败 .-> R["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
#     D -. 失败 .-> R
#     E -. 失败 .-> R
#     R --> S{"恢复完整？"}
#     S -->|是| T["清理 staging 和 backup；保留隔离的新数据；抛错"]
#     S -->|否| U["清理 staging；保留 backup 和隔离数据；报告现场并抛错"]
# ```

# In[7]:


def commit_full_audit(
    lake_root: pathlib.Path,
    staging_path: pathlib.Path,
    audit_result: dict[str, object],
    run_id: str,
) -> tuple[int, int]:
    log_started_at = time.perf_counter()
    log_phase = "prepare_paths"
    log_partition = None
    log_installed_calendar = 0
    log_verified_missing = 0
    log_verified_calendar = 0
    click.echo(
        f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=commit; status=started; "
        f"run_id={run_id}; lake_root={lake_root}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    try:
        silver_root = lake_root.resolve() / "silver"
        missing_target_path = silver_root / MISSING_TABLE_NAME
        calendar_target_path = silver_root / CALENDAR_TABLE_NAME
        short_run_id = run_id[:12]

        staging_path = staging_path.resolve()
        if staging_path == silver_root or not staging_path.is_relative_to(silver_root):
            raise ValueError(f"staging 路径必须位于 silver 根目录内：{staging_path}")
        missing_staging_path = staging_path / MISSING_TABLE_NAME
        calendar_staging_path = staging_path / CALENDAR_TABLE_NAME
        backup_path = silver_root / f".b08-b-{short_run_id}"
        quarantine_path = silver_root / f".b08-f-{short_run_id}"

        calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
        missing_columns = FUTURES_MISSING_BAR_SCHEMA.names
        calendar_digests = audit_result["calendar_digests"]
        missing_digests = audit_result["missing_digests"]
        total_missing = int(audit_result["total_missing"])

        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={MISSING_TABLE_NAME}; function=commit_full_audit; run_id={run_id}"
                ),
            ) as transaction:
                # 先替换缺失全表，再逐叶替换行情日历；备份一直保留到两表正式复读完成。
                log_phase = "missing_install"
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_install; status=started; "
                    f"run_id={run_id}; target={missing_target_path}; rows={total_missing}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                transaction.replace(
                    target_path=missing_target_path,
                    staged_path=missing_staging_path,
                )
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_install; status=completed; "
                    f"run_id={run_id}; rows={total_missing}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

                log_phase = "prepare_calendar_install"
                for partition_key in sorted(calendar_digests):
                    log_phase = "calendar_install"
                    log_partition = partition_key
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_install; status=started; "
                        f"run_id={run_id}; partition={partition_key}; installed_calendar_partitions={log_installed_calendar}/{len(calendar_digests)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    relative_path = pathlib.Path(*[
                        f"{name}={value}"
                        for name, value in zip(
                            CALENDAR_PARTITION_COLUMNS,
                            partition_key,
                            strict=True,
                        )
                    ])
                    source_path = calendar_staging_path / relative_path
                    destination_path = calendar_target_path / relative_path
                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path,
                    )
                    log_installed_calendar += 1
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_install; status=completed; "
                        f"run_id={run_id}; partition={partition_key}; installed_calendar_partitions={log_installed_calendar}/{len(calendar_digests)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )

                log_phase = "missing_formal_readback"
                log_partition = None
                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_formal_readback; status=started; "
                    f"run_id={run_id}; partitions={len(missing_digests)}; rows={total_missing}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_missing_dataset = open_exact_dataset(
                    missing_target_path,
                    MISSING_PARTITIONING,
                    FUTURES_MISSING_BAR_SCHEMA,
                    MISSING_PARTITION_COLUMNS,
                    "正式缺失明细",
                )
                if committed_missing_dataset.count_rows() != total_missing:
                    raise ValueError("正式缺失明细总行数与主键求差汇总不一致。")
                if discover_partition_keys(
                    missing_target_path,
                    MISSING_PARTITION_COLUMNS,
                ) != set(missing_digests):
                    raise ValueError("正式缺失明细分区集合与 staging 不一致。")

                for partition_key, expected_digest in missing_digests.items():
                    log_phase = "missing_formal_leaf"
                    log_partition = partition_key
                    click.echo(
                        f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_formal_leaf; status=started; "
                        f"run_id={run_id}; partition={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    table = committed_missing_dataset.to_table(
                        columns=missing_columns,
                        filter=partition_expression(
                            MISSING_PARTITION_COLUMNS,
                            partition_key,
                        ),
                    )
                    table = validate_arrow_table(table, FUTURES_MISSING_BAR_SCHEMA)
                    if table_digest(
                        table,
                        FUTURES_MISSING_BAR_SCHEMA,
                        MISSING_PRIMARY_KEY,
                    ) != expected_digest:
                        raise ValueError("正式缺失明细分区内容检查失败。")
                    log_verified_missing += 1
                    click.echo(
                        f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_formal_leaf; status=completed; "
                        f"run_id={run_id}; partition={partition_key}; checked_partitions={log_verified_missing}/{len(missing_digests)}; rows={table.num_rows}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )

                click.echo(
                    f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=missing_formal_readback; status=completed; "
                    f"run_id={run_id}; partitions={log_verified_missing}; rows={total_missing}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                log_phase = "calendar_formal_readback"
                log_partition = None
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_formal_readback; status=started; "
                    f"run_id={run_id}; partitions={len(calendar_digests)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                committed_calendar_dataset = open_exact_dataset(
                    calendar_target_path,
                    CALENDAR_PARTITIONING,
                    FUTURES_BAR_CALENDAR_SCHEMA,
                    CALENDAR_PARTITION_COLUMNS,
                    "正式行情日历",
                )
                for partition_key, expected_digest in calendar_digests.items():
                    log_phase = "calendar_formal_leaf"
                    log_partition = partition_key
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_formal_leaf; status=started; "
                        f"run_id={run_id}; partition={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    table = committed_calendar_dataset.to_table(
                        columns=calendar_columns,
                        filter=partition_expression(
                            CALENDAR_PARTITION_COLUMNS,
                            partition_key,
                        ),
                    )
                    table = validate_arrow_table(table, FUTURES_BAR_CALENDAR_SCHEMA)
                    if table_digest(
                        table,
                        FUTURES_BAR_CALENDAR_SCHEMA,
                        CALENDAR_PRIMARY_KEY,
                    ) != expected_digest:
                        raise ValueError("正式行情日历分区内容检查失败。")
                    log_verified_calendar += 1
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_formal_leaf; status=completed; "
                        f"run_id={run_id}; partition={partition_key}; checked_partitions={log_verified_calendar}/{len(calendar_digests)}; rows={table.num_rows}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_full_audit; phase=calendar_formal_readback; status=completed; "
                    f"run_id={run_id}; partitions={log_verified_calendar}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=commit; status=completed; "
            f"run_id={run_id}; write=true; committed_missing_rows={total_missing}; committed_calendar_partitions={len(calendar_digests)}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=audit_state; status=completed; "
            f"run_id={run_id}; calendar_table={CALENDAR_TABLE_NAME}; scope=missing_table_and_touched_calendar_leaves; persisted=true; date_watermark=none; completion_and_quality_preserved=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return total_missing, len(calendar_digests)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={MISSING_TABLE_NAME}; function=commit_full_audit; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; partition={log_partition}; installed_calendar_partitions={log_installed_calendar}; verified_missing_partitions={log_verified_missing}; verified_calendar_partitions={log_verified_calendar}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI 调度、汇总与运行日志
# 
# `main()` 检查人工确认参数，准备湖路径、本批 ID 与统一检测时间；根据 `--write` 选择 silver 内暂存路径或系统临时目录，再调用全量生成函数取得摘要与统计，并决定是否协调提交。只读退出会清理系统临时目录；写入模式在尚未尝试提交时发生异常，由入口清理统一的本批 staging 根。事务进入失败由提交函数清理 staging，进入后的安装恢复由共享模块处理。
# 
# 日志对齐 c01、c02 的 88 个 `=` 运行边界，采用 `table/function/phase/status` 和 `elapsed_s`。`missing_audit_scope` 表达全量范围；`missing_audit_total:` 汇总 Session、理论键、实际键、缺失键与日历叶数量；`planning_progress:` 表达生成、跳过提交及运行状态，`committed:` 由提交函数在两表协调提交成功后报告持久结果。只读结果记 `persisted=false`；普通异常继续向上抛出，不输出正常结束日志。
# 
# 读取、分区发现、摘要、输出校验、生成和提交函数分别报告自身起止、进度及失败；入口只报告运行范围、跳过提交和最终结果，不重复输出生成汇总或提交起止。此代码格只定义 Click 命令；后面的独立执行单元格区分 Notebook、直接运行脚本和模块导入。

# ### 局部流程：入口调度、运行边界与清理
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查人工确认参数；准备运行路径和批次时间"] --> B["运行开始日志；报告全量范围"]
#     B --> C["调用生成函数；函数自行报告进度与汇总"]
#     C --> D["取得已验证的 staging 摘要与统计"]
#     D --> E{"write？"}
#     E -->|否| F["报告跳过提交；只读结果未落盘"]
#     E -->|是| G["调用提交函数；自行报告安装验收，共享模块负责恢复"]
#     G --> H["提交函数报告 audit_state 已落盘后返回"]
#     F --> I["执行既有 finally 清理；报告正常结束"]
#     H --> I
#     C -. 异常 .-> X["按现有边界清理或保留现场；异常向上抛出"]
#     G -. 异常 .-> X
#     X --> Y["不报告正常结束"]
# ```

# In[8]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--confirm-full-quality", is_flag=True, required=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    confirm_full_quality: bool,
    write: bool,
) -> None:
    if not confirm_full_quality:
        raise click.UsageError(
            "分钟主键缺失审计必须显式传入 --confirm-full-quality。"
        )

    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    run_id = uuid.uuid4().hex
    detected_at = datetime.now(timezone.utc)

    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_outcome = None

    temporary_directory = None
    if write:
        silver_root = resolved_lake_root / "silver"
        silver_root.mkdir(parents=True, exist_ok=True)
        short_run_id = run_id[:12]
        staging_path = silver_root / f".b08-s-{short_run_id}"
    else:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="latitude-b08-missing-"
        )
        staging_path = pathlib.Path(temporary_directory.name)

    missing_staging_path = staging_path / MISSING_TABLE_NAME
    calendar_staging_path = staging_path / CALENDAR_TABLE_NAME

    click.echo(
        f"{log_boundary}\n分钟主键缺失审计开始 / Missing audit run started\n"
        "function=main()\n"
        f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=run; status=started; "
        f"missing_audit_scope=all_required_completed_1m_sessions; run_id={run_id}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        f"minute_columns=contract_code,bar_at; elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
    )

    commit_attempted = False
    try:
        audit_result = build_full_audit_staging(
            resolved_lake_root,
            missing_staging_path,
            calendar_staging_path,
            detected_at,
        )


        if not write:
            click.echo(
                f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=commit; status=skipped; "
                "write=false; committed_missing_rows=0; committed_calendar_partitions=0; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_outcome = "read_only"
            return

        commit_attempted = True
        committed_missing_rows, committed_calendar_partitions = (
            commit_full_audit(
                resolved_lake_root,
                staging_path,
                audit_result,
                run_id,
            )
        )
        log_outcome = "committed"
    finally:
        if temporary_directory is not None:
            temporary_directory.cleanup()
        elif write and not commit_attempted:
            shutil.rmtree(staging_path, ignore_errors=True)
        if log_outcome is not None:
            click.echo(
                f"{log_boundary}\n分钟主键缺失审计结束 / Missing audit run ended\n"
                "function=main()\n"
                f"planning_progress: table={MISSING_TABLE_NAME}; function=main; phase=run; status=completed; "
                f"outcome={log_outcome}; write={str(write).lower()}; run_id={run_id}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
            )


# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，与 c01—c07 一致。当前只传 `--confirm-full-quality`，不带 `--write`；执行此格会全量读取本地 required 1m Session 和分钟事实两列主键，在系统临时目录生成、复读缺失明细与日历 staging，退出时清理，不改动正式湖。
# 
# c08 不提供日期、月份或合约范围参数，不能复制其他环节的成对日期示例。只读模式同样执行全量审计，运行时间取决于全部待审计数据量；查看说明或流程图不会启动。直接运行脚本使用命令行参数，在 Notebook 中导入同名 Python 模块不触发业务；正式提交仍需人工显式 `--confirm-full-quality --write`。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 参数是人工全量只读确认；执行此代码格会全量读取本地数据并生成临时 staging。流程图本身不执行代码。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口单元格或运行脚本"] --> B{"Notebook 交互环境？"}
#     B -->|是| C["显式 notebook_args；不读取内核参数"]
#     C --> D["main.main：standalone_mode=False"]
#     B -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main：读取命令行参数"]
#     E -->|否| G["模块导入：不触发业务"]
#     D --> H["进入人工全量审计；是否提交由 write 决定"]
#     F --> H
# ```

# In[ ]:


# 用 Jupyter / IPython 运行时，内核进程的启动命令类似: ipykernel_launcher.py -f /path/to/connection.json
# sys.argv 里会包含: ['ipykernel_launcher.py', '-f', '/path/to/connection.json']
# parser.parse_args() 会读取 sys.argv，解析器会看到 -f 选项，但命令定义里没有 -f

# main()
# └── click.Command.__call__()
#     └── Command.main(args=None)
#         ├── args 为 None，因此读取 sys.argv[1:]
#         ├── Command.make_context(...)
#         ├── Command.parse_args(...)
#         │   └── 内部 parser.parse_args(args)
#         └── Command.invoke(...)
#             └── 调用 main(...) 函数体

if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--confirm-full-quality"] # 单元格内不写 --write
    main.main(
        args=notebook_args,
        prog_name="c08_full_minute_quality",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# ### 局部流程：终端手动运行
# 
# 下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["在终端激活 latitude_env_v2"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --confirm-full-quality --write"]
#     C --> D["全量分钟主键求差；缺失表根和日历叶共同提交"]
# ```

# In[ ]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Market_Data\a01_Collection\b01_Futures_Market_Data\c08_full_minute_quality.py --confirm-full-quality --write

