#!/usr/bin/env python
# coding: utf-8

# # 期货合约 Session 日历 · c03_futures_contract_calendar
# 
# 本入口生成 `dim_futures_contract_calendar`，供后续采集与研究定位固定月份合约在各交易日的理论交易时段。每行对应一个**固定月份合约—交易日—Session**；Session 指当日有效交易时间规则中的一段连续时段。
# 
# 输入由两部分组成：[c02 品种日历](c02_futures_variety_calendar.ipynb)已提交的品种—交易日记录，以及 JQData 提供的合约上市区间、乘数、最小变动价位和历史交易时间规则。入口使用完整固定月份合约目录，事实采集白名单不参与本表筛选。输出列出规则推导的理论 Session；当天是否实际开市，需要后续结合行情证据判断。
# 
# 阅读时先确认下方总流程、更新范围与表结构，再按各环节说明查看对应函数。函数定义单元格用于注册函数，业务调用由末尾执行入口触发。Notebook 是业务源文件；同名 `.py` 由默认 PythonExporter 完整生成，供终端和采集工作台使用。

# ## 全流程：从品种日历生成并提交合约 Session
# 
# 入口先读取配置并检查已有表的物理契约，再按更新模式选择上游品种日。存在请求记录时，采集合约元数据并建立前一交易日映射；随后逐叶生成理论 Session，与相应本地范围比较。这里的“叶分区”指一个交易所—年份—月份的数据目录，“差异分区”指存在缺失、业务字段变化、多余行或本地业务质量错误的叶。
# 
# 不带 `--write` 时输出差异计划。带 `--write` 时，逐个重建差异分区并提交；自动模式还在本批所需分区全部成功后记录已处理日期。显式日期和全历史模式即使上游为空，也会比较范围内的旧行，以识别需要清理的数据。各环节的输入、规则和失败处理在后文展开。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["加载环境、配置与函数；进入执行入口"] --> B["检查参数与已有表物理契约；选择更新范围"]
#     B --> C["读取对应范围的 c02 品种日历"]
#     C --> D{"自动模式且没有新增记录？"}
#     D -->|是| E["结束：零 API 请求；不更新水位"]
#     D -->|否| F["非空请求采集合约信息与前一交易日；空请求返回空输入"]
#     F --> G["逐叶生成并校验 Session；比较本地范围；汇总差异计划"]
#     G --> H{"存在差异分区？"}
#     H -->|否| I{"自动模式且带 write？"}
#     H -->|是| J{"带 write？"}
#     J -->|否| K["输出只读计划并结束"]
#     J -->|是| L["重建差异叶；合并、暂存、安装并复读验收"]
#     L --> M{"所有所需叶均成功？"}
#     M -->|否| N["停止：恢复失败叶；保留此前成功叶"]
#     M -->|是| I
#     I -->|否| O["输出运行汇总并结束"]
#     I -->|是| P["原子替换已处理日期标记"]
#     P --> O
#     P -.->|失败| Q["停止：保留已提交叶；水位未成功更新"]
# ```

# ## 更新范围、水位与写入参数
# 
# 三种模式均支持 `--write`，包括向正式湖提交指定日期范围的差异。不带 `--write` 时仍会读取上游，并在请求记录非空时认证和访问 JQData；只读运行只形成计划。
# 
# | 模式 | 上游与本地比较范围 | 差异分区的替换范围 |
# | --- | --- | --- |
# | 默认尾部更新 | 上游中严格晚于自动水位的品种日；本地只比较这些记录对应的交易所—品种—交易日键 | 替换本批精确键对应的行，保留同叶中的其他行 |
# | 成对显式日期 | 指定自然日闭区间内的上游记录与本地行；纳入区间所覆盖月份的已有叶 | 替换该闭区间内的行，保留区间外行 |
# | `--full` | 当前全部上游记录与全部已有叶，包含上游已无对应记录的叶 | 替换完整差异叶；结果为空时删除该叶 |
# 
# 自动水位取两个可用日期的较大值：正式行的最大 `trading_date`，以及表根 `schema.parquet` 中 `automatic_tail_processed_through` 保存的已处理日期。两者均不存在时，默认模式请求全部上游记录。没有新增记录时，入口在认证前结束，并保持标记不变。
# 
# 存在新增记录且自动 `--write` 成功时，已处理日期推进至本批最大请求交易日，即使本批没有差异叶，或候选合约日全部因缺少有效规则而跳过。**已处理日期表示生成流程成功处理到的范围，不保证每个合约日都有 Session。** 只读运行、显式日期和 `--full` 均不更新这项标记。
# 
# 默认模式用于接续上游新增日期。它不检查水位以内的缺口、规则修订或孤儿行；这类历史维护需要选择显式日期或 `--full`。差异比较排除 `updated_at`，仅采集时间不同不触发提交。
# 
# `--start-date` 与 `--end-date` 必须同时提供，起点不得晚于终点，且不能与 `--full` 同用。`--lake-root` 指定读写湖根；未提供时使用 `.env` 的 `FUTURES_LAKE_ROOT`，由 `settings.futures_lake_root` 读取。所选湖根必须包含已提交的 c02 表；日期选择以该上游记录为依据。

# ## JQData 输入与规则选择
# 
# 来源函数使用以下接口构造本批合约目录和日期映射。API 计数统计业务请求，不包含认证。
# 
# | 接口 | 请求与返回检查 | 用途 |
# | --- | --- | --- |
# | `get_all_securities(["futures"], date=None)` | 一次请求完整期货证券目录；检查索引合约代码及上市、退市日期 | 保留固定月份合约，按交易所、品种和上市闭区间筛选与本批品种日相交的合约 |
# | `get_futures_info(codes, fields=["contract_multiplier", "tick_size", "trade_time"])` | 每批最多 200 个合约；检查所需字段、记录格式和请求代码的完整覆盖 | 获得合约级乘数、最小变动价位与历史交易时间规则 |
# | `get_trade_days(end_date=首个请求交易日, count=2)` | 为本批首日请求边界交易日，要求返回日期中存在更早的交易日 | 补齐首日的前一交易日；其余日期使用本批有序请求日期序列中的前一日 |
# 
# 固定月份代码由品种字母、三位或四位合约数字及交易所后缀组成；编号 `8888`、`9998`、`9999` 的连续合约被排除。合约只在 `list_date ≤ trading_date ≤ delist_date` 时参与生成。
# 
# `trade_time` 的每条规则包含生效日期、失效日期及一个或多个 Session 文本。合约日须恰好命中一条有效期闭区间；无规则或没有当日有效规则时，跳过该合约日，并汇总数量与最多 10 条样例。规则结构非法、有效期倒置或同日命中多条规则时，生成失败并抛出异常。
# 
# 请求记录为空时，来源函数返回空目录、空日期映射和零 API 计数。认证与 API 异常记录失败阶段后向调用方抛出；入口没有自动重试逻辑。

# ## 表结构、存储位置与提交单位
# 
# 完整字段、类型、中文说明及表身份定义见[数据契约](../../../config/data_contracts.py)中的 `FUTURES_CONTRACT_CALENDAR_SCHEMA`。Notebook 的 Schema 浏览环节提供字段说明和有界样例，便于核对输入与输出。
# 
# | 对象 | 定义 |
# | --- | --- |
# | 逻辑表 | `dim_futures_contract_calendar`，21 列；保留合约身份、上市区间、规则有效期、Session 时刻与派生属性 |
# | 主键 | `contract_code, trading_date, session_number` |
# | Hive 分区 | `exchange_code/year/month`；年月由归属交易日生成 |
# | 叶目录 | `<湖根>/silver/dim_futures_contract_calendar/exchange_code=…/year=…/month=…/` |
# | 时间类型 | Session 时刻使用 `Asia/Shanghai`；`updated_at` 使用 UTC |
# | 根标记 | `schema.parquet` 保存零行物理契约，并可携带自动模式已处理日期 |
# 
# `trading_date` 是 Session 的归属交易日；`session_start_at` 与 `session_end_at` 是带时区的自然时间。夜盘的自然日期可能早于归属交易日，分区仍按 `trading_date` 的年月确定。
# 
# 每次提交先按更新模式确定替换范围，再形成一个完整叶。结果为空时移除该叶；若全表没有数据文件且缺少根标记，则安装零行 `schema.parquet`，使空表仍有可读契约。
# 
# 各叶依次、独立提交。当前叶安装或验收失败时，共享路径事务恢复本次移动的旧叶及本次新建空表标记；此前成功的叶保留。自动已处理日期在所有所需叶成功后另行写入，其失败不会回滚已提交叶。存储约束见[湖仓规则](../../a02_Lake/AGENTS.md)，本入口的来源与验收职责见[采集说明](../README.md)。

# ## 初始化：定位仓库并加载依赖
# 
# 本环节从当前工作目录逐级向上查找同时包含 `.git`、`.env` 和 `config/settings.py` 的仓库根，将仓库与采集目录加入模块搜索路径，再加载 Click、Pandas、Arrow、配置、数据契约和共享路径事务。
# 
# 执行初始化单元格会定位目录并导入依赖；JQData 认证、来源请求和湖仓提交由后续函数调用触发。找不到仓库根时抛出异常，后续流程不能运行。

# ### 局部流程：初始化
# 
# 以下步骤在初始化单元格执行时完成，为配置、生成和提交函数准备依赖。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从当前目录向上查找仓库标记"] --> B{"找到仓库根？"}
#     B -->|否| C["抛出目录定位异常"]
#     B -->|是| D["加入仓库与采集目录的模块路径"]
#     D --> E["加载配置、DataFrame 依赖、Schema 与路径事务"]
# ```

# In[1]:


from __future__ import annotations

import os
import pathlib
from bisect import bisect_left
import re
import shutil
import sys
import uuid
from datetime import date, datetime, time, timedelta, timezone
from time import perf_counter


# 从任意子目录启动时，先按项目统一标记定位根目录。
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
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Schema、类型转换和校验函数均来自根级可执行契约。
from config.data_contracts import (
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    arrow_to_pandas,
    empty_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
from config.settings import settings
from R02_Market_Data.a01_Collection.b00_04_staged_path_transaction import StagedPathTransaction


# ## 配置与契约检查：表身份、物理字段和水位标记
# 
# 表名、主键与分区列从权威 Schema metadata 读取。Hive 分区列由目录路径还原，因此叶内 Parquet 文件的物理 Schema 不含这些列。`validate_compatible_dataset_schema()` 检查字段顺序、类型、可空性及 `table_name`、`primary_key`、`partition_columns` 三项身份 metadata；描述性 metadata 允许随契约说明更新。`validate_dataset_fragment_schemas()` 逐文件检查物理 Schema，防止数据集的逻辑 Schema 掩盖文件差异。
# 
# 水位读取函数只读取根标记中的 `automatic_tail_processed_through`。文件或该项 metadata 不存在时返回 `None`；值必须能解析为 ISO 日期，否则抛出异常。
# 
# 水位写入函数在表根同目录写临时零行 Parquet，复读确认 Schema 与 metadata 后，使用 `os.replace()` 替换 `schema.parquet`，最后清理临时文件。该标记写入与叶事务分别执行。配置单元格定义这些函数；实际读写由 `main()` 根据运行模式调用。

# ### 局部流程：契约检查与水位读写
# 
# 图中三条支路对应独立函数调用。契约检查用于上游和本表读取；水位读写仅由自动更新模式使用。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从权威 Schema 提取表名、主键、分区和物理字段"] --> B["注册契约检查与水位函数"]
#     B --> C["契约检查：逻辑字段及身份 metadata；逐文件物理字段"]
#     B --> D{"读取水位：根标记含已处理日期？"}
#     D -->|否| E["返回 None"]
#     D -->|是| F["解析 ISO 日期；非法值抛出异常"]
#     B --> G["写入水位：构造含日期 metadata 的零行表"]
#     G --> H["写同目录临时文件；复读核对完整 Schema"]
#     H --> I["原子替换根标记；清理临时文件"]
#     H -.->|失败| J["清理临时文件；记录失败阶段并抛出异常"]
#     I -.->|失败| J
# ```

# In[2]:


# 表名、主键和分区顺序只从权威 Schema metadata 读取一次。
TABLE_NAME = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
UPSTREAM_TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
PARTITION_COLUMNS = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
UPSTREAM_PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
CONTRACT_CALENDAR_PHYSICAL_SCHEMA = pa.schema(
    [FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name) for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names if name not in PARTITION_COLUMNS],
    metadata=FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata,
)
PHYSICAL_PRIMARY_KEY = [name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS]
PHYSICAL_METADATA_KEYS = (b"table_name", b"primary_key", b"partition_columns")
AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY = b"automatic_tail_processed_through"


def validate_compatible_dataset_schema(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    context: str,
) -> None:
    """只固定物理字段和路由 metadata；允许描述性 metadata 随代码更新。"""
    if actual_schema.names != expected_schema.names:
        raise TypeError(f"{context}字段及顺序与契约不一致。")
    for expected_field in expected_schema:
        actual_field = actual_schema.field(expected_field.name)
        if (
            actual_field.type != expected_field.type
            or actual_field.nullable != expected_field.nullable
        ):
            raise TypeError(f"{context}字段 {expected_field.name!r} 的类型或 nullable 与契约不一致。")
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    for metadata_key in PHYSICAL_METADATA_KEYS:
        if actual_metadata.get(metadata_key) != expected_metadata.get(metadata_key):
            raise TypeError(f"{context}{metadata_key.decode()} metadata 与契约不一致。")


def validate_dataset_fragment_schemas(
    dataset: ds.Dataset,
    expected_schema: pa.Schema,
    partition_columns: list[str],
    context: str,
) -> None:
    """逐个检查物理 Parquet Schema，防止 Dataset 逻辑 Schema 遮蔽后续文件漂移。"""
    expected_file_schema = pa.schema(
        [
            expected_schema.field(name)
            for name in expected_schema.names
            if name not in partition_columns
        ],
        metadata=expected_schema.metadata,
    )
    for fragment in dataset.get_fragments():
        validate_compatible_dataset_schema(
            fragment.physical_schema,
            expected_file_schema,
            f"{context}fragment {fragment.path} ",
        )


def read_automatic_tail_processed_through(target_path: pathlib.Path) -> date | None:
    marker_path = target_path / "schema.parquet"
    if not marker_path.is_file():
        return None
    encoded_watermark = (pq.read_schema(marker_path).metadata or {}).get(
        AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY
    )
    if encoded_watermark is None:
        return None
    try:
        return date.fromisoformat(encoded_watermark.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError(
            "schema.parquet 中 automatic_tail_processed_through 不是有效 ISO 日期。"
        ) from error


def write_automatic_tail_processed_through(
    lake_root: pathlib.Path,
    processed_through: date,
) -> None:
    watermark_started_at = perf_counter()
    watermark_phase = "prepare"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=watermark; status=started; "
        f"automatic_tail_processed_through={processed_through}; lake_root={lake_root}"
    )
    try:
        target_path = lake_root.resolve() / "silver" / TABLE_NAME
        target_path.mkdir(parents=True, exist_ok=True)
        marker_path = target_path / "schema.parquet"
        temporary_marker_path = target_path / f".c03m-{uuid.uuid4().hex}.parquet"
        marker_metadata = dict(FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata or {})
        marker_metadata[AUTOMATIC_TAIL_PROCESSED_THROUGH_KEY] = (
            processed_through.isoformat().encode("ascii")
        )
        marker_schema = CONTRACT_CALENDAR_PHYSICAL_SCHEMA.with_metadata(marker_metadata)
        try:
            watermark_phase = "staging_write"
            click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_staging_write; status=started")
            pq.write_table(
                pa.Table.from_batches([], schema=marker_schema),
                temporary_marker_path,
            )
            watermark_phase = "staging_readback"
            click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_staging_readback; status=started")
            if not pq.read_schema(temporary_marker_path).equals(
                marker_schema, check_metadata=True
            ):
                raise RuntimeError("b03 自动尾部水位 staging 复读失败。")
            watermark_phase = "install"
            click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_install; status=started")
            os.replace(temporary_marker_path, marker_path)
        finally:
            temporary_marker_path.unlink(missing_ok=True)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=completed; "
            f"outcome=watermark_committed; automatic_tail_processed_through={processed_through}; "
            f"elapsed_s={perf_counter() - watermark_started_at:.3f}"
        )
    except Exception as watermark_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=failed; "
            f"failed_phase={watermark_phase}; automatic_tail_processed_through={processed_through}; "
            f"error={type(watermark_error).__name__}; elapsed_s={perf_counter() - watermark_started_at:.3f}"
        )
        raise

VARIETY_DATE_COLUMNS = ["exchange_code", "underlying_code", "trading_date"]
BUSINESS_COLUMNS = [  # 判断业务内容是否变化；排除仅表示写入时刻的审计字段。
    name for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names if name != "updated_at"
]
BUSINESS_PRIMARY_KEY_POSITIONS = tuple(BUSINESS_COLUMNS.index(name) for name in PRIMARY_KEY)
INFO_FIELDS = [  # 从 JQData get_futures_info 请求的原始字段。
    "contract_multiplier",  # 合约乘数。
    "tick_size",  # 最小价格变动单位。
    "trade_time",  # 原始交易时间文本；后续拆为多个 Session。
]
INFO_BATCH_SIZE = 200
SOURCE_NAME = "JQData_get_all_securities+get_futures_info"

SESSION_PAIR = re.compile(
    r"^\s*(\d{1,2}:\d{2})\s*[-~—–至]\s*(\d{1,2}:\d{2})\s*$"
)
FIXED_CONTRACT = re.compile(
    r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})"
    r"\.(?P<exchange_code>[A-Z]+)$"
)
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})

HIVE_PARTITIONING = ds.partitioning(
    pa.schema([FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
    flavor="hive",
)
UPSTREAM_PARTITIONING = ds.partitioning(
    pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in UPSTREAM_PARTITION_COLUMNS]),
    flavor="hive",
)


# ## Schema 浏览：核对上游与输出字段
# 
# 交互式 Notebook 中，本环节通过共享浏览器依次展示品种日历与合约 Session 日历的权威 Schema，包括中文表名、字段说明、来源和质量要求。启用的样例有界读取 `settings.futures_lake_root` 中已存在的数据；样例湖根由该设置决定。
# 
# 浏览不调用业务 API，也不写入数据。字段展示帮助读者理解契约，实际数据验收由读取、生成和提交函数执行。浏览方式见[采集说明中的 Schema 展示约定](../README.md#notebook-开篇-schema-契约呈现)。

# ### 局部流程：Schema 与有界样例展示
# 
# 该单元格根据运行环境决定是否展示；直接运行导出脚本时跳过此环节。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["执行浏览单元格"] --> B{"交互式 Notebook？"}
#     B -->|是| C["读取品种日历与 Session 日历的权威 Schema"]
#     C --> D["展示中文说明与启用的有界湖内样例"]
#     B -->|否| E["跳过展示"]
# ```

# In[3]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from R02_Market_Data.a01_Collection.b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## Session 解析、业务校验与差异键
# 
# `parse_session_text()` 将单段交易时间文本解析为起止钟点。文本须符合“小时:分钟—小时:分钟”的结构，连接符可为 `-`、`~`、`—`、`–` 或“至”；钟点必须合法。返回值保留去除首尾空白后的文本，以及解析后的开始、结束钟点。
# 
# `validate_contract_calendar_frame()` 先按权威 Schema 完成类型与非空检查，再核对本表业务条件：
# 
# - 主键唯一，代码是固定月份合约，交易所后缀与品种字段匹配，来源标识符合本入口定义。
# - 交易日在合约上市闭区间和规则有效期闭区间内。
# - 时间戳的钟点与 Session 文本一致，结束严格晚于开始，时长为整数分钟，且与正值 `minute_count` 一致。
# - 夜盘标记与开始钟点一致，跨午夜标记与起止自然日期一致；年月与归属交易日一致。
# - 乘数与最小变动价位允许为空，非空值须大于零；每个合约日的 Session 序号恰好为 `1…N`，开始时刻不重复。
# 
# 校验后的结果按主键排序。Session 序号遵循来源规则的时段顺序；校验不要求序号对应时间先后，也不额外检查不同 Session 的时间区间重叠。
# 
# 完整业务校验分别用于生成结果、计划中的本地比较范围，以及提交时合并后的完整叶。`business_rows_by_key()` 消费已校验 DataFrame，将主键映射到除 `updated_at` 外的全部业务字段，并将 `pd.NA` 统一为 `None`。生成与本地映射据此比较缺失、变化和多余行。
# 
# 计划阶段仅将本地比较范围校验产生的 `TypeError` 或 `ValueError` 记为 `quality_error` 并纳入重建计划。来源错误、生成错误和物理契约错误会向外抛出；暂存与安装复读的检查范围见提交环节。

# ### 局部流程：解析、校验与比较映射
# 
# 解析、表级校验和映射是三个独立函数。生成与提交需要完整业务校验；比较映射以已校验结果为输入。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["解析 Session 文本"] --> B["匹配单段格式；验证起止钟点"]
#     B --> C["返回文本与两个钟点"]
#     D["校验合约 Session DataFrame"] --> E["按权威 Schema 转换；检查非空与主键"]
#     E --> F{"空表？"}
#     F -->|是| G["返回契约空表"]
#     F -->|否| H["核对合约身份、来源及上市与规则有效期"]
#     H --> I["核对时间、分钟数、夜盘与跨日属性、乘数及年月"]
#     I --> J["核对合约日连续序号与唯一开始时刻"]
#     J --> K["按主键排序并返回"]
#     L["构造比较映射：输入已校验表"] --> M["读取业务列；排除 updated_at；统一空值"]
#     M --> N["返回主键到业务字段元组的映射"]
# ```

# In[4]:


# 将 JQData 的单段交易时间文本严格解析为起止时刻。
def parse_session_text(session_value: object) -> tuple[str, time, time]:
    session_text = str(session_value).strip()
    match = SESSION_PAIR.fullmatch(session_text)
    if match is None:
        raise ValueError(f"无法完整解析 trade_time Session：{session_value!r}")

    def parse_clock(clock_text: str) -> time:
        hour_text, minute_text = clock_text.split(":", 1)
        return time(int(hour_text), int(minute_text))

    return session_text, parse_clock(match.group(1)), parse_clock(match.group(2))


# 校验 c03 生成的合约 Session 表及参与比较的本地行。
def validate_contract_calendar_frame(
    contract_calendar_df: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    contract_calendar_table = pandas_to_arrow(
        contract_calendar_df.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    validated_contract_calendar_df = contract_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)

    if validated_contract_calendar_df.duplicated(PRIMARY_KEY).any():
        raise ValueError(f"{context}主键不唯一。")
    if validated_contract_calendar_df.empty:
        return validated_contract_calendar_df

    parsed_codes_df = (
        validated_contract_calendar_df["contract_code"].astype("string").str.extract(FIXED_CONTRACT)
    )
    if parsed_codes_df.isna().any().any():
        raise ValueError(f"{context}包含非固定月份合约代码。")
    if parsed_codes_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES).any():
        raise ValueError(f"{context}包含连续或指数合约代码。")
    if not parsed_codes_df["exchange_code"].reset_index(drop=True).eq(
        validated_contract_calendar_df["exchange_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}exchange_code 与合约后缀不一致。")
    if not parsed_codes_df["underlying_code"].reset_index(drop=True).eq(
        validated_contract_calendar_df["underlying_code"].astype("string").reset_index(drop=True)
    ).all():
        raise ValueError(f"{context}underlying_code 与合约代码不一致。")

    if not validated_contract_calendar_df["source"].eq(SOURCE_NAME).all():
        raise ValueError(f"{context}source 与数据契约不一致。")

    # 逐行验证合约存续期、规则有效期和 Session 时间边界。
    for row in validated_contract_calendar_df.itertuples(index=False):
        if not row.list_date <= row.trading_date <= row.delist_date:
            raise ValueError(f"{context}{row.contract_code} 的交易日越出上市区间。")
        if not row.rule_effective_date <= row.trading_date <= row.rule_expiry_date:
            raise ValueError(f"{context}{row.contract_code} 的交易日越出规则区间。")
        if row.session_number <= 0:
            raise ValueError(f"{context}session_number 必须大于 0。")

        session_text, start_clock, end_clock = parse_session_text(row.session_text)
        start_at = pd.Timestamp(row.session_start_at)
        end_at = pd.Timestamp(row.session_end_at)
        if start_at.time() != start_clock or end_at.time() != end_clock:
            raise ValueError(f"{context}{session_text} 与 Session 时间戳不一致。")
        if start_at >= end_at:
            raise ValueError(f"{context}Session 起点必须严格早于终点。")

        duration_seconds = (end_at - start_at).total_seconds()
        if duration_seconds % 60 != 0:
            raise ValueError(f"{context}Session 时间差不是整分钟。")
        if row.minute_count != int(duration_seconds // 60) or row.minute_count <= 0:
            raise ValueError(f"{context}minute_count 与 Session 时间差不一致。")

        expected_night = start_clock >= time(20) or start_clock < time(6)
        if row.is_night_session != expected_night:
            raise ValueError(f"{context}is_night_session 与起始钟点不一致。")
        if row.spans_midnight != (start_at.date() != end_at.date()):
            raise ValueError(f"{context}spans_midnight 与时间戳日期不一致。")

        if not pd.isna(row.contract_multiplier) and row.contract_multiplier <= 0:
            raise ValueError(f"{context}非空 contract_multiplier 必须大于 0。")
        if not pd.isna(row.tick_size) and row.tick_size <= 0:
            raise ValueError(f"{context}非空 tick_size 必须大于 0。")
        if row.year != row.trading_date.year or row.month != row.trading_date.month:
            raise ValueError(f"{context}year/month 与 trading_date 不一致。")

    # 同一合约日的 Session 编号必须连续，起点也不能重复。
    group_columns = ["contract_code", "trading_date"]
    for group_key, group_df in validated_contract_calendar_df.groupby(group_columns, sort=False):
        session_numbers = sorted(int(number) for number in group_df["session_number"])
        expected_numbers = list(range(1, len(session_numbers) + 1))
        if session_numbers != expected_numbers:
            raise ValueError(f"{context}{group_key} 的 Session 编号不连续。")
        if group_df["session_start_at"].duplicated().any():
            raise ValueError(f"{context}{group_key} 包含重复 Session 起点。")

    return validated_contract_calendar_df.sort_values(PRIMARY_KEY).reset_index(drop=True)


# 按主键比较业务字段，采集时间 updated_at 不参与差异判断。
def business_rows_by_key(
    validated_contract_calendar_df: pd.DataFrame,
) -> dict[tuple[object, ...], tuple[object, ...]]:
    """消费已完成本表类型校验的行；只构造比较映射，不再次转换或校验。"""
    rows_by_key = {}
    for row in validated_contract_calendar_df.loc[:, BUSINESS_COLUMNS].itertuples(index=False, name=None):
        business_values = tuple(None if value is pd.NA else value for value in row)
        key = tuple(business_values[position] for position in BUSINESS_PRIMARY_KEY_POSITIONS)
        rows_by_key[key] = business_values
    return rows_by_key


# ## 上游读取与合约元数据采集
# 
# `read_trusted_variety_calendar()` 从所选湖根读取 `dim_futures_variety_calendar`，先检查逻辑及逐文件物理契约，再按水位之后或指定日期闭区间过滤，返回上游全部契约列。主键、品种日期、`active_contract_count` 等业务语义由 c02 提交负责验收，本环节信任已提交结果。
# 
# `collect_source_data()` 对非空请求先认证并读取完整证券目录，再按本批交易所—品种—日期筛选上市区间相交的固定月份合约。筛选后没有候选合约时抛出异常。合约信息分批请求；汇总结果必须恰好覆盖请求代码，乘数与最小变动价位转换为数值后，与目录一对一合并。
# 
# 日期映射使用入口传入的有序、去重请求交易日序列。该入口传入本批日期，因此通过一次边界查询补齐首日的前一交易日；其他日期取序列中的前一日。返回值包括合约目录、前一交易日映射和请求计数，供逐叶生成复用。
# 
# 日志标识认证、目录读取、候选筛选、合约信息批次和边界查询阶段；同步 API 返回并通过对应检查后才报告完成。空请求直接返回空结果。失败时记录所处阶段并抛出异常。

# ### 局部流程：上游读取与来源采集
# 
# 先根据更新范围读取上游，再以请求记录调用来源函数。空上游在显式或全历史维护中仍可用于清理旧行。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["确认 c02 表存在；检查逻辑及逐文件物理契约"] --> B["按水位或日期范围过滤；读取契约列"]
#     B --> C{"请求记录为空？"}
#     C -->|是| D["返回空合约目录、空日期映射与零 API 计数"]
#     C -->|否| E["认证；一次读取完整期货证券目录"]
#     E --> F["核对代码和上市区间；筛选本批相关固定月份合约"]
#     F --> G["每批最多 200 个合约请求信息"]
#     G --> H["检查字段、代码覆盖与数值；合并目录"]
#     H --> I["边界查询首日的前一交易日；其余使用请求序列前一日"]
#     I --> J["返回合约目录、日期映射及 API 计数"]
# ```

# In[5]:


def read_trusted_variety_calendar(
    lake_root: pathlib.Path,
    *,
    start_date_exclusive: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    if start_date_exclusive is not None and (start_date is not None or end_date is not None):
        raise ValueError("尾部起点不能与闭区间同时提供。")
    if (start_date is None) != (end_date is None):
        raise ValueError("闭区间起止日期必须同时提供。")
    upstream_variety_calendar_path = (
        lake_root.resolve() / "silver" / UPSTREAM_TABLE_NAME
    )
    if not upstream_variety_calendar_path.is_dir():
        raise FileNotFoundError(
            f"缺少上游品种日历：{upstream_variety_calendar_path}"
        )

    upstream_variety_calendar_dataset = ds.dataset(
        upstream_variety_calendar_path,
        format="parquet",
        partitioning=UPSTREAM_PARTITIONING,
    )
    if set(upstream_variety_calendar_dataset.schema.names) != set(
        FUTURES_VARIETY_CALENDAR_SCHEMA.names
    ):
        raise TypeError("上游品种日历字段集合与契约不一致。")
    upstream_variety_calendar_schema = pa.schema(
        [
            upstream_variety_calendar_dataset.schema.field(name)
            for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
        ],
        metadata=upstream_variety_calendar_dataset.schema.metadata,
    )
    validate_compatible_dataset_schema(
        upstream_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "上游品种日历 "
    )
    validate_dataset_fragment_schemas(
        upstream_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, UPSTREAM_PARTITION_COLUMNS, "上游品种日历 "
    )

    upstream_filter = None
    if start_date_exclusive is not None:
        upstream_filter = ds.field("trading_date") > start_date_exclusive
    elif start_date is not None:
        upstream_filter = (ds.field("trading_date") >= start_date) & (
            ds.field("trading_date") <= end_date
        )

    upstream_variety_calendar_table = (
        upstream_variety_calendar_dataset.to_table(
            columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            filter=upstream_filter,
        )
    )
    return upstream_variety_calendar_table.to_pandas(
        types_mapper=pd.ArrowDtype
    )


def collect_source_data(
    requested_variety_calendar_df: pd.DataFrame,
    all_variety_trading_dates: list[date],
) -> tuple[pd.DataFrame, dict[date, date], dict[str, int]]:
    collection_started_at = perf_counter()
    collection_phase = "input"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=collect; status=started; "
        f"requested_variety_dates={len(requested_variety_calendar_df)}"
    )
    try:
        empty_contract_catalog_df = pd.DataFrame(
            columns=[
                "contract_code",
                "start_date",
                "end_date",
                "underlying_code",
                "delivery_code",
                "exchange_code",
                *INFO_FIELDS,
            ]
        )
        if requested_variety_calendar_df.empty:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; "
                f"outcome=empty_request; contracts=0; api_calls=0; "
                f"elapsed_s={perf_counter() - collection_started_at:.3f}"
            )
            return empty_contract_catalog_df, {}, {
                "api_call_count": 0,
                "contract_count": 0,
                "info_batch_count": 0,
                "boundary_trade_day_call_count": 0,
            }

        collection_phase = "authenticate"
        phase_started_at = perf_counter()
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=started")
        from config.jqdata_connection import authenticate_jqdata

        jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=completed; "
            f"elapsed_s={perf_counter() - phase_started_at:.3f}"
        )
        collection_phase = "contract_catalog"
        phase_started_at = perf_counter()
        click.echo(f"request_batch: table={TABLE_NAME}; phase=contract_catalog; status=started; date=None")

        contract_catalog_df = jqdata.get_all_securities(["futures"], date=None)
        if not isinstance(contract_catalog_df, pd.DataFrame):
            raise TypeError(
                "get_all_securities 应返回 pandas.DataFrame，实际为 "
                f"{type(contract_catalog_df).__name__}。"
            )

        click.echo(
            f"api_result: table={TABLE_NAME}; phase=contract_catalog; status=completed; "
            f"rows={len(contract_catalog_df)}; elapsed_s={perf_counter() - phase_started_at:.3f}"
        )
        collection_phase = "catalog_validation"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=started")
        contract_catalog_df = (
            contract_catalog_df.rename_axis("contract_code").reset_index()
        )
        required_columns = {"contract_code", "start_date", "end_date"}
        if not required_columns <= set(contract_catalog_df.columns):
            missing_columns = sorted(
                required_columns - set(contract_catalog_df.columns)
            )
            raise ValueError(f"get_all_securities 缺少列：{missing_columns}")

        contract_catalog_df["contract_code"] = (
            contract_catalog_df["contract_code"].astype("string").str.upper()
        )
        parsed_contract_codes_df = (
            contract_catalog_df["contract_code"].str.extract(FIXED_CONTRACT)
        )
        contract_catalog_df = pd.concat(
            [contract_catalog_df, parsed_contract_codes_df],
            axis=1,
        )
        contract_catalog_df = contract_catalog_df.dropna(
            subset=["underlying_code", "delivery_code", "exchange_code"]
        )
        contract_catalog_df = contract_catalog_df.loc[
            ~contract_catalog_df["delivery_code"].isin(
                CONTINUOUS_DELIVERY_CODES
            )
        ].copy()

        if contract_catalog_df.empty:
            raise ValueError(
                "get_all_securities 未返回任何固定月份期货合约。"
            )
        if contract_catalog_df.duplicated(["contract_code"]).any():
            duplicate_contract_codes = sorted(
                contract_catalog_df.loc[
                    contract_catalog_df.duplicated(
                        ["contract_code"],
                        keep=False,
                    ),
                    "contract_code",
                ].unique()
            )
            raise ValueError(
                "get_all_securities 合约代码重复："
                f"{duplicate_contract_codes}"
            )

        contract_catalog_df["start_date"] = pd.to_datetime(
            contract_catalog_df["start_date"],
            errors="raise",
        ).dt.date
        contract_catalog_df["end_date"] = pd.to_datetime(
            contract_catalog_df["end_date"],
            errors="raise",
        ).dt.date
        if contract_catalog_df[["start_date", "end_date"]].isna().any().any():
            raise ValueError(
                "固定月份合约的 start_date/end_date 包含空值。"
            )
        if (
            contract_catalog_df["start_date"]
            > contract_catalog_df["end_date"]
        ).any():
            raise ValueError(
                "固定月份合约存在 start_date 晚于 end_date 的记录。"
            )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=completed; "
            f"fixed_contracts={len(contract_catalog_df)}"
        )
        collection_phase = "candidate_selection"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=candidate_selection; status=started")
        candidate_contract_mask = pd.Series(
            False,
            index=contract_catalog_df.index,
        )
        for (
            exchange_code,
            underlying_code,
        ), requested_group_df in requested_variety_calendar_df.groupby(
            ["exchange_code", "underlying_code"],
            sort=False,
        ):
            requested_trading_dates = sorted(
                requested_group_df["trading_date"].unique().tolist()
            )
            group_contract_mask = (
                contract_catalog_df["exchange_code"].eq(exchange_code)
                & contract_catalog_df["underlying_code"].eq(underlying_code)
            )
            for contract_row in contract_catalog_df.loc[
                group_contract_mask,
                ["start_date", "end_date"],
            ].itertuples():
                first_candidate_position = bisect_left(
                    requested_trading_dates,
                    contract_row.start_date,
                )
                if (
                    first_candidate_position < len(requested_trading_dates)
                    and requested_trading_dates[first_candidate_position]
                    <= contract_row.end_date
                ):
                    candidate_contract_mask.at[contract_row.Index] = True

        contract_catalog_df = contract_catalog_df.loc[
            candidate_contract_mask
        ].copy()
        requested_contract_codes = sorted(
            contract_catalog_df["contract_code"].tolist()
        )
        if not requested_contract_codes:
            raise ValueError(
                "完整合约目录无法覆盖任何请求的上游品种日格点。"
            )

        info_batch_total = (len(requested_contract_codes) + INFO_BATCH_SIZE - 1) // INFO_BATCH_SIZE
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=candidate_selection; status=completed; "
            f"contracts={len(requested_contract_codes)}; info_batches={info_batch_total}"
        )
        required_info_fields = set(INFO_FIELDS)
        contract_info_by_code = {}
        info_batch_count = 0
        for offset in range(
            0,
            len(requested_contract_codes),
            INFO_BATCH_SIZE,
        ):
            batch_contract_codes = requested_contract_codes[
                offset : offset + INFO_BATCH_SIZE
            ]
            collection_phase = "contract_info_request"
            phase_started_at = perf_counter()
            click.echo(
                f"request_batch: table={TABLE_NAME}; phase=contract_info; status=started; "
                f"completed={info_batch_count}; total={info_batch_total}; "
                f"batch={info_batch_count + 1}; contracts={len(batch_contract_codes)}"
            )
            batch_contract_info = jqdata.get_futures_info(
                batch_contract_codes,
                fields=INFO_FIELDS,
            )
            info_batch_count += 1
            collection_phase = "contract_info_validation"
            if not isinstance(batch_contract_info, dict):
                raise TypeError(
                    "get_futures_info 应返回 dict，实际为 "
                    f"{type(batch_contract_info).__name__}。"
                )

            for contract_code, contract_record in batch_contract_info.items():
                normalized_contract_code = str(contract_code).upper()
                if normalized_contract_code in contract_info_by_code:
                    raise ValueError(
                        "get_futures_info 重复返回 "
                        f"{normalized_contract_code}。"
                    )
                if not isinstance(contract_record, dict):
                    raise TypeError(
                        f"{normalized_contract_code} 的合约信息不是 dict。"
                    )
                missing_info_fields = sorted(
                    required_info_fields - set(contract_record)
                )
                if missing_info_fields:
                    raise ValueError(
                        f"{normalized_contract_code} 的 "
                        "get_futures_info 缺少字段："
                        f"{missing_info_fields}"
                    )
                contract_info_by_code[normalized_contract_code] = {
                    name: contract_record[name]
                    for name in INFO_FIELDS
                }
            click.echo(
                f"api_result: table={TABLE_NAME}; phase=contract_info; status=completed; "
                f"completed={info_batch_count}; total={info_batch_total}; "
                f"requested_contracts={len(batch_contract_codes)}; returned_contracts={len(batch_contract_info)}; "
                f"elapsed_s={perf_counter() - phase_started_at:.3f}"
            )

        collection_phase = "info_coverage"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=info_coverage; status=started")
        missing_contract_codes = sorted(
            set(requested_contract_codes) - set(contract_info_by_code)
        )
        unexpected_contract_codes = sorted(
            set(contract_info_by_code) - set(requested_contract_codes)
        )
        if missing_contract_codes:
            raise ValueError(
                "get_futures_info 缺少 "
                f"{len(missing_contract_codes)} 个合约："
                f"{missing_contract_codes[:10]}"
            )
        if unexpected_contract_codes:
            raise ValueError(
                "get_futures_info 返回未请求合约："
                f"{unexpected_contract_codes[:10]}"
            )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=info_coverage; status=completed; "
            f"contracts={len(contract_info_by_code)}"
        )
        collection_phase = "catalog_merge"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_merge; status=started")
        contract_info_df = pd.DataFrame(
            [
                {
                    "contract_code": contract_code,
                    **contract_record,
                }
                for contract_code, contract_record
                in contract_info_by_code.items()
            ]
        )
        contract_info_df["contract_multiplier"] = pd.to_numeric(
            contract_info_df["contract_multiplier"],
            errors="raise",
        )
        contract_info_df["tick_size"] = pd.to_numeric(
            contract_info_df["tick_size"],
            errors="raise",
        )
        contract_catalog_df = contract_catalog_df.merge(
            contract_info_df,
            on="contract_code",
            how="left",
            validate="one_to_one",
        )
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_merge; status=completed")
        collection_phase = "previous_trading_dates"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=previous_trading_dates; status=started")

        requested_trading_dates = sorted(
            requested_variety_calendar_df["trading_date"].unique().tolist()
        )
        trading_date_position = {
            trading_date: position
            for position, trading_date
            in enumerate(all_variety_trading_dates)
        }
        boundary_previous_trading_date = None
        boundary_trade_day_call_count = 0
        if any(
            trading_date_position[trading_date] == 0
            for trading_date in requested_trading_dates
        ):
            earliest_trading_date = all_variety_trading_dates[0]
            phase_started_at = perf_counter()
            click.echo(
                f"request_batch: table={TABLE_NAME}; phase=boundary_trade_day; status=started; "
                f"end_date={earliest_trading_date}; count=2"
            )
            boundary_window = sorted(
                {
                    pd.Timestamp(value).date()
                    for value in jqdata.get_trade_days(
                        end_date=earliest_trading_date,
                        count=2,
                    )
                }
            )
            earlier_trading_dates = [
                value
                for value in boundary_window
                if value < earliest_trading_date
            ]
            if not earlier_trading_dates:
                raise ValueError(
                    f"无法确定 {earliest_trading_date} 的前一交易日。"
                )
            boundary_previous_trading_date = max(
                earlier_trading_dates
            )
            boundary_trade_day_call_count = 1
            click.echo(
                f"api_result: table={TABLE_NAME}; phase=boundary_trade_day; status=completed; "
                f"previous_trading_date={boundary_previous_trading_date}; "
                f"elapsed_s={perf_counter() - phase_started_at:.3f}"
            )

        previous_trading_date_by_trading_date = {}
        for trading_date in requested_trading_dates:
            position = trading_date_position[trading_date]
            if position == 0:
                previous_trading_date_by_trading_date[trading_date] = (
                    boundary_previous_trading_date
                )
            else:
                previous_trading_date_by_trading_date[trading_date] = (
                    all_variety_trading_dates[position - 1]
                )

        source_summary = {
            "api_call_count": (
                1
                + info_batch_count
                + boundary_trade_day_call_count
            ),
            "contract_count": len(requested_contract_codes),
            "info_batch_count": info_batch_count,
            "boundary_trade_day_call_count": (
                boundary_trade_day_call_count
            ),
        }
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=previous_trading_dates; status=completed; "
            f"trading_dates={len(previous_trading_date_by_trading_date)}"
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; outcome=source_ready; "
            f"api_calls={source_summary['api_call_count']}; contracts={source_summary['contract_count']}; "
            f"info_batches={source_summary['info_batch_count']}; "
            f"boundary_trade_day_calls={source_summary['boundary_trade_day_call_count']}; "
            f"elapsed_s={perf_counter() - collection_started_at:.3f}"
        )
        return (
            contract_catalog_df,
            previous_trading_date_by_trading_date,
            source_summary,
        )
    except Exception as collection_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=collect; status=failed; "
            f"failed_phase={collection_phase}; error={type(collection_error).__name__}; "
            f"elapsed_s={perf_counter() - collection_started_at:.3f}"
        )
        raise


# ## 逐叶生成：把规则时段映射到自然时间
# 
# `build_contract_calendar_partition()` 接收一个交易所—年月的上游品种日记录、按品种分组的合约目录、前一交易日映射及本批 `updated_at`。对每个品种日，选择仍在上市闭区间内的合约，再选出当日唯一有效的 `trade_time` 规则，按来源时段顺序编号并生成 Session。
# 
# Session 的归属交易日保持为上游 `trading_date`，自然时间按以下规则建立，统一使用 `Asia/Shanghai`：
# 
# | 开始钟点 | 开始自然日期 |
# | --- | --- |
# | 不早于 20:00 | 前一交易日的自然日期 |
# | 早于 06:00 | 前一交易日之后的下一自然日 |
# | 06:00 至 20:00 之前 | 当前归属交易日 |
# 
# 结束钟点不晚于开始钟点时，结束自然日期在开始日期上加一天；否则与开始日期相同。例如，若某归属交易日为周一、其前一交易日为周五，规则时段为 `21:00-02:30`，本方法生成周五 21:00 至周六 02:30 的理论 Session，行的 `trading_date` 仍为周一。这个例子说明日期映射，不证明该夜盘实际开市。
# 
# `minute_count` 为起止时刻的整分钟差；`is_night_session` 根据开始钟点判断，`spans_midnight` 比较起止自然日期。乘数与最小变动价位是合约级属性，复制到该合约日的各 Session，来源空值保持为空。
# 
# 无有效规则的合约日计入跳过统计并继续生成，非法规则或重叠有效期抛出异常。函数校验生成表后返回 Session 行与跳过统计；本批 warning 由入口统一汇总。日志在首个、每 25 个及最后一个品种日报告已处理数、候选合约日数与生成行数。
# 
# 计划阶段逐叶生成并比较，只保存差异计划和统计。写入阶段使用同一批合约目录、日期映射和时间戳重新生成差异叶，无须再次请求 API，避免同时持有全历史 Session 表。

# ### 局部流程：一个交易所—年月的 Session 生成
# 
# 本函数负责规则选择与时间展开；无规则跳过统计随结果返回，由入口汇总。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["接收单叶上游品种日与本批来源输入"] --> B["逐品种日选择上市中的固定月份合约"]
#     B --> C["检查 trade_time 结构及当日有效规则"]
#     C --> D{"有效规则数量？"}
#     D -->|缺失或零条| E["累计跳过合约日和样例"]
#     D -->|多条| F["抛出规则有效期重叠异常"]
#     D -->|一条| G["按来源顺序编号；解析各段起止钟点"]
#     G --> H["按前一交易日与跨午夜规则确定自然日期"]
#     H --> I["生成带时区时刻、分钟数、属性及分区列"]
#     I --> J["处理后续 Session、合约与品种日；报告进度"]
#     E --> J
#     J --> K["汇总 Session 行或契约空表"]
#     K --> L["完整业务校验；返回表与跳过统计"]
# ```

# In[6]:


def build_contract_calendar_partition(
    variety_partition_df: pd.DataFrame,
    contract_catalog_by_group: dict[
        tuple[str, str],
        pd.DataFrame,
    ],
    previous_trading_date_by_trading_date: dict[date, date],
    updated_at: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    build_started_at = perf_counter()
    build_phase = "input"
    build_partition_label = "unknown"
    try:
        build_partition_label = (
            "/".join(str(variety_partition_df.iloc[0][name]) for name in PARTITION_COLUMNS)
            if not variety_partition_df.empty else "empty"
        )
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=build; status=started; "
            f"partition={build_partition_label}; completed=0; total={len(variety_partition_df)}"
        )
        build_phase = "expand"
        processed_variety_day_count = 0
        contract_session_rows = []
        candidate_contract_day_count = 0
        no_valid_rule_contract_day_count = 0
        no_valid_rule_samples = []

        for variety_day in variety_partition_df.itertuples(index=False):
            variety_key = (
                str(variety_day.exchange_code),
                str(variety_day.underlying_code),
            )
            variety_contract_catalog_df = (
                contract_catalog_by_group.get(variety_key)
            )
            if variety_contract_catalog_df is None:
                raise ValueError(
                    f"完整合约目录缺少请求的上游品种：{variety_key}"
                )

            active_contract_catalog_df = (
                variety_contract_catalog_df.loc[
                    (
                        variety_contract_catalog_df["start_date"]
                        <= variety_day.trading_date
                    )
                    & (
                        variety_contract_catalog_df["end_date"]
                        >= variety_day.trading_date
                    )
                ]
            )

            for contract in active_contract_catalog_df.itertuples(
                index=False
            ):
                candidate_contract_day_count += 1
                trade_time_rules = contract.trade_time
                if (
                    trade_time_rules is None
                    or trade_time_rules is pd.NA
                    or trade_time_rules == []
                ):
                    no_valid_rule_contract_day_count += 1
                    if len(no_valid_rule_samples) < 10:
                        no_valid_rule_samples.append(
                            (
                                contract.contract_code,
                                variety_day.trading_date,
                            )
                        )
                    continue
                if not isinstance(trade_time_rules, (list, tuple)):
                    raise ValueError(
                        f"{contract.contract_code} 的 "
                        "trade_time 不是规则列表。"
                    )

                matching_trade_time_rules = []
                for trade_time_rule in trade_time_rules:
                    if (
                        not isinstance(trade_time_rule, (list, tuple))
                        or len(trade_time_rule) < 3
                    ):
                        raise ValueError(
                            f"{contract.contract_code} 包含非法 "
                            f"trade_time 规则：{trade_time_rule!r}"
                        )
                    rule_effective_date = pd.Timestamp(
                        trade_time_rule[0]
                    ).date()
                    rule_expiry_date = pd.Timestamp(
                        trade_time_rule[1]
                    ).date()
                    if rule_effective_date > rule_expiry_date:
                        raise ValueError(
                            f"{contract.contract_code} 的规则生效日"
                            f"晚于失效日：{trade_time_rule!r}"
                        )
                    if (
                        rule_effective_date
                        <= variety_day.trading_date
                        <= rule_expiry_date
                    ):
                        matching_trade_time_rules.append(
                            (
                                rule_effective_date,
                                rule_expiry_date,
                                trade_time_rule[2:],
                            )
                        )

                if not matching_trade_time_rules:
                    no_valid_rule_contract_day_count += 1
                    if len(no_valid_rule_samples) < 10:
                        no_valid_rule_samples.append(
                            (
                                contract.contract_code,
                                variety_day.trading_date,
                            )
                        )
                    continue
                if len(matching_trade_time_rules) > 1:
                    raise ValueError(
                        f"{contract.contract_code} 在 "
                        f"{variety_day.trading_date} 命中 "
                        f"{len(matching_trade_time_rules)} 条 "
                        "trade_time 规则。"
                    )

                (
                    rule_effective_date,
                    rule_expiry_date,
                    session_values,
                ) = matching_trade_time_rules[0]
                previous_trading_date = (
                    previous_trading_date_by_trading_date[
                        variety_day.trading_date
                    ]
                )
                contract_multiplier = (
                    None
                    if pd.isna(contract.contract_multiplier)
                    else float(contract.contract_multiplier)
                )
                tick_size = (
                    None
                    if pd.isna(contract.tick_size)
                    else float(contract.tick_size)
                )

                for session_number, session_value in enumerate(
                    session_values,
                    start=1,
                ):
                    (
                        session_text,
                        start_clock,
                        end_clock,
                    ) = parse_session_text(session_value)
                    is_night_session = (
                        start_clock >= time(20)
                        or start_clock < time(6)
                    )

                    if start_clock >= time(20):
                        start_day = previous_trading_date
                    elif start_clock < time(6):
                        start_day = (
                            previous_trading_date + timedelta(days=1)
                        )
                    else:
                        start_day = variety_day.trading_date
                    end_day = (
                        start_day + timedelta(days=1)
                        if end_clock <= start_clock
                        else start_day
                    )

                    session_start_at = pd.Timestamp(
                        datetime.combine(start_day, start_clock),
                        tz="Asia/Shanghai",
                    )
                    session_end_at = pd.Timestamp(
                        datetime.combine(end_day, end_clock),
                        tz="Asia/Shanghai",
                    )
                    minute_count = int(
                        (
                            session_end_at - session_start_at
                        ).total_seconds()
                        // 60
                    )

                    contract_session_rows.append(
                        {
                            "contract_code": contract.contract_code,
                            "exchange_code": variety_day.exchange_code,
                            "underlying_code": (
                                variety_day.underlying_code
                            ),
                            "trading_date": variety_day.trading_date,
                            "list_date": contract.start_date,
                            "delist_date": contract.end_date,
                            "contract_multiplier": contract_multiplier,
                            "tick_size": tick_size,
                            "rule_effective_date": (
                                rule_effective_date
                            ),
                            "rule_expiry_date": rule_expiry_date,
                            "session_number": session_number,
                            "session_text": session_text,
                            "session_start_at": session_start_at,
                            "session_end_at": session_end_at,
                            "is_night_session": is_night_session,
                            "spans_midnight": start_day != end_day,
                            "minute_count": minute_count,
                            "source": SOURCE_NAME,
                            "updated_at": updated_at,
                            "year": variety_day.trading_date.year,
                            "month": variety_day.trading_date.month,
                        }
                    )
            processed_variety_day_count += 1
            if (
                processed_variety_day_count == 1
                or processed_variety_day_count % 25 == 0
                or processed_variety_day_count == len(variety_partition_df)
            ):
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=expand; status=running; "
                    f"partition={build_partition_label}; completed={processed_variety_day_count}; "
                    f"total={len(variety_partition_df)}; trading_date={variety_day.trading_date}; "
                    f"candidate_contract_days={candidate_contract_day_count}; rows={len(contract_session_rows)}; "
                    f"elapsed_s={perf_counter() - build_started_at:.3f}"
                )

        if contract_session_rows:
            new_contract_calendar_df = pd.DataFrame(
                contract_session_rows,
                columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
            )
        else:
            new_contract_calendar_df = empty_pandas(
                FUTURES_CONTRACT_CALENDAR_SCHEMA
            )
        build_phase = "output_validation"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=output_validation; status=started; "
            f"partition={build_partition_label}; rows={len(new_contract_calendar_df)}"
        )
        new_contract_calendar_df = (
            validate_contract_calendar_frame(
                new_contract_calendar_df,
                "当前期望分区",
            )
        )

        audit = {
            "candidate_contract_day_count": (
                candidate_contract_day_count
            ),
            "no_valid_rule_contract_day_count": (
                no_valid_rule_contract_day_count
            ),
            "no_valid_rule_samples": no_valid_rule_samples,
            "session_row_count": len(new_contract_calendar_df),
        }
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=build; status=completed; "
            f"partition={build_partition_label}; completed={processed_variety_day_count}; total={len(variety_partition_df)}; "
            f"rows={len(new_contract_calendar_df)}; candidate_contract_days={candidate_contract_day_count}; "
            f"no_valid_rule_contract_day_count={no_valid_rule_contract_day_count}; "
            f"elapsed_s={perf_counter() - build_started_at:.3f}"
        )
        return new_contract_calendar_df, audit
    except Exception as build_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=build; status=failed; "
            f"partition={build_partition_label}; failed_phase={build_phase}; "
            f"error={type(build_error).__name__}; elapsed_s={perf_counter() - build_started_at:.3f}"
        )
        raise


# ## 单叶提交：合并、暂存、验收与恢复
# 
# `commit_partition()` 的输入是一个叶的替换行及替换范围。精确品种—日期键与日期闭区间不能同时指定；所有替换行必须属于所选叶，并位于指定范围。函数只打开当前旧叶，借助 Hive 路径还原分区列，检查逻辑及逐文件物理契约，再按模式保留范围外行或替换整叶。
# 
# 合并后的完整叶先通过 Session 业务校验，再按权威 Schema 序列化。存在结果行时，将完整叶写入临时目录，并复读检查逻辑契约、Arrow 类型与非空要求、主键唯一性及总行数。结果为零行时，明确以删除旧叶为提交动作；应有数据而暂存叶缺失时会报错。
# 
# 暂存通过后，函数在 `StagedPathTransaction` 内备份旧叶并安装新叶或删除旧叶。安装后复读当前叶的物理 Schema 与表身份 metadata，并检查主键和行数；这些复读不重新执行 Session 业务校验。全表清空且根标记不存在时，在同一事务内安装零行 `schema.parquet`，并复读其契约与零行状态。
# 
# 暂存失败时清理临时目录并抛出异常。安装或正式复读失败时，路径事务按实际移动记录倒序恢复当前叶及本次新建空表标记：失败新叶保留在隔离目录，新建标记移除；恢复未完成时保留旧备份。恢复范围限于本次调用，此前成功叶仍保留。自动已处理日期由入口在本批叶提交成功后另行写入。
# 
# 日志按输入检查、旧叶读取、合并校验、暂存复读和正式安装标识阶段；`partition_committed` 在事务成功退出后报告单叶完成。失败日志提供阶段与异常，路径事务日志提供恢复结果。

# ### 局部流程：单叶提交与失败恢复
# 
# 图中事务包含当前叶及必要的新建空表标记。合并、业务校验与 Parquet 验收由本入口执行，路径安装与恢复由共享模块执行。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查叶与替换范围；读取并检查旧叶"] --> B["保留范围外行；形成完整替换叶"]
#     B --> C["完整叶业务校验；按权威 Schema 序列化"]
#     C --> D{"结果有行？"}
#     D -->|是| E["写暂存叶；复读契约、主键和行数"]
#     D -->|否| F["明确选择删除旧叶"]
#     E -.->|失败| G["清理暂存；记录失败阶段并抛出异常"]
#     E --> H["进入单叶路径事务；备份并安装或删除"]
#     F --> H
#     H --> I["必要时安装零行根标记；复读正式叶与标记"]
#     H -.->|失败| J["按实际移动记录恢复当前叶与新建标记"]
#     I -.->|失败| J
#     J --> K["保留隔离新叶；恢复未完成则保留备份；抛出异常"]
#     I --> L["成功退出事务并清理临时目录；报告单叶完成"]
# ```

# In[7]:


def commit_partition(
    replacement_contract_calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    partition_key: tuple[str, int, int],
    replacement_variety_date_keys: (
        set[tuple[str, str, date]] | None
    ) = None,
    replace_start_date: date | None = None,
    replace_end_date: date | None = None,
) -> int:
    commit_started_at = perf_counter()
    commit_phase = "replacement_scope"
    click.echo(
        f"partition_plan: table={TABLE_NAME}; phase=commit; status=started; "
        f"partition={partition_key}; rows={len(replacement_contract_calendar_df)}; lake_root={lake_root}"
    )
    try:
        if (replace_start_date is None) != (replace_end_date is None):
            raise ValueError("替换起止日期必须同时提供。")
        if (
            replace_start_date is not None
            and replace_start_date > replace_end_date
        ):
            raise ValueError("替换起始日期不得晚于结束日期。")
        if (
            replacement_variety_date_keys is not None
            and replace_start_date is not None
        ):
            raise ValueError(
                "精确品种日替换与日期区间替换不能同时提供。"
            )

        exchange_code, year, month = partition_key
        if not replacement_contract_calendar_df.empty:
            replacement_partition_keys = set(
                replacement_contract_calendar_df[
                    PARTITION_COLUMNS
                ].itertuples(index=False, name=None)
            )
            if replacement_partition_keys != {
                (exchange_code, year, month)
            }:
                raise ValueError("待提交数据越出指定 Hive 分区。")

            if replacement_variety_date_keys is not None:
                replacement_row_variety_date_keys = set(
                    replacement_contract_calendar_df[
                        VARIETY_DATE_COLUMNS
                    ].itertuples(index=False, name=None)
                )
                if not replacement_row_variety_date_keys <= (
                    replacement_variety_date_keys
                ):
                    raise ValueError(
                        "待提交数据越出精确品种日替换范围。"
                    )
            if (
                replace_start_date is not None
                and any(
                    trading_date < replace_start_date
                    or trading_date > replace_end_date
                    for trading_date
                    in replacement_contract_calendar_df[
                        "trading_date"
                    ]
                )
            ):
                raise ValueError(
                    "待提交数据越出显式替换日期范围。"
                )

        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / TABLE_NAME
        relative_path = pathlib.Path(
            f"exchange_code={exchange_code}", f"year={year}", f"month={month}"
        )
        destination_path = target_path / relative_path
        run_id = uuid.uuid4().hex
        staging_path = silver_root / f".c03s-{run_id}"
        backup_path = silver_root / f".c03b-{run_id}"
        quarantine_path = silver_root / f".c03q-{run_id}"
        silver_root.mkdir(parents=True, exist_ok=True)

        for managed_path in (
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ):
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(
                    f"数据集路径越出 silver 根目录：{managed_path}"
                )

        commit_phase = "existing_partition"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=existing_partition; status=started; "
            f"partition={partition_key}; run_id={run_id}"
        )
        existing_contract_calendar_dataset = None
        if (
            destination_path.is_dir()
            and any(destination_path.rglob("*.parquet"))
        ):
            existing_contract_calendar_dataset = ds.dataset(
                destination_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
                partition_base_dir=target_path.as_posix(),
            )
            existing_contract_calendar_schema = pa.schema(
                [
                    existing_contract_calendar_dataset.schema.field(
                        name
                    )
                    for name
                    in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                ],
                metadata=(
                    existing_contract_calendar_dataset.schema.metadata
                ),
            )
            validate_compatible_dataset_schema(
                existing_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "现有正式分区 "
            )
            validate_dataset_fragment_schemas(
                existing_contract_calendar_dataset, FUTURES_CONTRACT_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有正式分区 "
            )

        existing_contract_calendar_df = empty_pandas(
            FUTURES_CONTRACT_CALENDAR_SCHEMA
        )
        if existing_contract_calendar_dataset is not None:
            existing_contract_calendar_table = existing_contract_calendar_dataset.to_table(
                columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
            )
            existing_contract_calendar_df = arrow_to_pandas(
                existing_contract_calendar_table,
                FUTURES_CONTRACT_CALENDAR_SCHEMA,
            )

        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=existing_partition; status=completed; "
            f"partition={partition_key}; rows={len(existing_contract_calendar_df)}; run_id={run_id}"
        )
        commit_phase = "merge_validate"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=merge_validate; status=started; "
            f"partition={partition_key}; run_id={run_id}"
        )
        if replacement_variety_date_keys is not None:
            existing_variety_date_index = pd.MultiIndex.from_frame(
                existing_contract_calendar_df[
                    VARIETY_DATE_COLUMNS
                ]
            )
            retained_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    ~existing_variety_date_index.isin(
                        replacement_variety_date_keys
                    ),
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
                ]
            )
            complete_partition_df = pd.concat(
                [
                    retained_contract_calendar_df,
                    replacement_contract_calendar_df,
                ],
                ignore_index=True,
            )
        elif replace_start_date is not None:
            retained_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        < replace_start_date
                    )
                    | (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        > replace_end_date
                    ),
                    FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
                ]
            )
            complete_partition_df = pd.concat(
                [
                    retained_contract_calendar_df,
                    replacement_contract_calendar_df,
                ],
                ignore_index=True,
            )
        else:
            complete_partition_df = (
                replacement_contract_calendar_df
            )

        complete_partition_df = validate_contract_calendar_frame(
            complete_partition_df,
            "合并后完整分区",
        )
        complete_partition_table = pa.Table.from_pandas(
            complete_partition_df,
            schema=FUTURES_CONTRACT_CALENDAR_SCHEMA,
            preserve_index=False,
            safe=True,
        ).replace_schema_metadata(FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata)

        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=merge_validate; status=completed; "
            f"partition={partition_key}; rows={len(replacement_contract_calendar_df)}; "
            f"replacement_rows={len(complete_partition_table)}; run_id={run_id}"
        )
        commit_phase = "staging_write"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=staging_write; status=started; "
            f"partition={partition_key}; run_id={run_id}"
        )
        staging_path.mkdir(parents=True, exist_ok=False)
        try:
            if len(complete_partition_table):
                ds.write_dataset(
                    complete_partition_table,
                    staging_path,
                    format="parquet",
                    partitioning=HIVE_PARTITIONING,
                    existing_data_behavior="delete_matching",
                    basename_template="part-{i}.parquet",
                )
                commit_phase = "staging_readback"
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=staging_readback; status=started; "
                    f"partition={partition_key}; run_id={run_id}"
                )
                staged_contract_calendar_dataset = ds.dataset(
                    staging_path,
                    format="parquet",
                    partitioning=HIVE_PARTITIONING,
                )
                staged_contract_calendar_schema = pa.schema(
                    [
                        staged_contract_calendar_dataset.schema.field(
                            name
                        )
                        for name
                        in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    ],
                    metadata=(
                        staged_contract_calendar_dataset.schema.metadata
                    ),
                )
                validate_compatible_dataset_schema(
                    staged_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "staging "
                )
                staged_contract_calendar_table = validate_arrow_table(
                    staged_contract_calendar_dataset.to_table(
                        columns=(
                            FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                        )
                    ),
                    FUTURES_CONTRACT_CALENDAR_SCHEMA,
                )
                staged_contract_calendar_df = staged_contract_calendar_table.to_pandas()
                if staged_contract_calendar_df.duplicated(PRIMARY_KEY).any():
                    raise ValueError("staging 主键不唯一。")
                if len(staged_contract_calendar_table) != len(complete_partition_table):
                    raise ValueError("staging 分区行数检查失败。")
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=staging; status=completed; "
            f"partition={partition_key}; rows={len(complete_partition_table)}; run_id={run_id}"
        )
        commit_phase = "prepare_install"
        source_path = staging_path / relative_path
        marker_path = target_path / "schema.parquet"
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"table={TABLE_NAME}; partition={partition_key}; run_id={run_id}",
        ) as transaction:
            commit_phase = "install"
            click.echo(
                f"partition_start: table={TABLE_NAME}; phase=install; status=started; "
                f"partition={partition_key}; completed=0; total=1; run_id={run_id}"
            )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if len(complete_partition_table) else None,
            )

            commit_phase = "formal_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=formal_readback; status=started; "
                f"partition={partition_key}; run_id={run_id}"
            )
            if (
                not len(complete_partition_table)
                and not marker_path.exists()
                and not any(path.name != "schema.parquet" for path in target_path.rglob("*.parquet"))
            ):
                commit_phase = "empty_marker"
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=CONTRACT_CALENDAR_PHYSICAL_SCHEMA),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path, staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    validate_compatible_dataset_schema(
                        marker_file.schema_arrow, CONTRACT_CALENDAR_PHYSICAL_SCHEMA, "正式空表 marker ",
                    )
                    if marker_file.metadata.num_rows != 0:
                        raise ValueError("正式空表 marker 必须为零行。")
                commit_phase = "formal_readback"

            if destination_path.is_dir():
                committed_partition_dataset = ds.dataset(
                    destination_path, format="parquet"
                )
                committed_partition_schema = pa.schema(
                    [
                        committed_partition_dataset.schema.field(name)
                        for name in committed_partition_dataset.schema.names
                    ],
                    metadata=committed_partition_dataset.schema.metadata,
                )
                validate_compatible_dataset_schema(
                    committed_partition_schema, CONTRACT_CALENDAR_PHYSICAL_SCHEMA,
                    f"正式分区 {relative_path} ",
                )
                committed_primary_key_table = committed_partition_dataset.to_table(
                    columns=PHYSICAL_PRIMARY_KEY
                )
                if committed_primary_key_table.to_pandas().duplicated(
                    PHYSICAL_PRIMARY_KEY
                ).any():
                    raise ValueError("正式分区主键不唯一。")
                committed_partition_row_count = len(committed_primary_key_table)
            else:
                committed_partition_row_count = 0
            if committed_partition_row_count != len(complete_partition_table):
                raise ValueError("正式分区行数检查失败。")

        click.echo(
            f"partition_committed: table={TABLE_NAME}; phase=commit; status=completed; "
            f"partition={partition_key}; completed=1; total=1; rows={len(replacement_contract_calendar_df)}; "
            f"replacement_rows={len(complete_partition_table)}; run_id={run_id}; "
            f"elapsed_s={perf_counter() - commit_started_at:.3f}"
        )
        return len(replacement_contract_calendar_df)
    except Exception as partition_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=commit; status=failed; "
            f"partition={partition_key}; failed_phase={commit_phase}; "
            f"error={type(partition_error).__name__}; elapsed_s={perf_counter() - commit_started_at:.3f}"
        )
        raise


# ## 入口调度：构造差异计划并顺序提交
# 
# `main()` 检查日期参数与更新模式，打开已有本表数据集，检查逻辑及全部文件物理契约，再读取选定范围的上游。自动模式仅规划新增上游记录所属叶；显式日期模式还纳入相关月份已有叶；全历史模式纳入全部已有叶，允许发现上游没有对应记录的旧行。
# 
# 入口逐叶生成预期 Session，选出本地对应比较范围，并比较主键到业务字段的映射。正常比较区分缺失、字段变化与多余行。若本地范围业务校验发生 `TypeError` 或 `ValueError`，记录 `quality_error`，将预期行计入待补、本地范围行计入待清理，并将该叶纳入重建。物理契约错误或其他阶段异常不会被这一处理覆盖。
# 
# 有差异但未带 `--write` 时输出计划并结束；带 `--write` 时重建并依次提交差异叶。没有差异时不提交叶。自动写入只要本批存在新增请求日期，就在所有所需叶成功后更新已处理标记；显式日期与全历史模式不改写该标记。
# 
# 运行日志提供模式、比较数量、跳过规则 warning 和批次结果。`partition_committed` 表示一个叶成功提交；`phase=commit_batch` 的完成记录表示本批所需叶及自动模式标记操作均已成功。后续叶或标记失败时，已完成的单叶记录不能解释为整个批次成功。
# 
# 本单元格只定义 Click 命令与调度函数。Notebook 和脚本如何触发它，见后面的执行入口。

# ### 局部流程：模式选择、差异计划与批次结果
# 
# 主流程根据模式决定比较范围，逐叶规划后再决定是否写入。自动模式的空增量、无差异和有差异分别处理。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["调用 main；检查成对日期与 full 互斥"] --> B["检查已有表逻辑及逐文件物理契约"]
#     B --> C{"更新模式？"}
#     C -->|自动| D["计算水位；读取严格晚于水位的上游记录"]
#     C -->|显式日期| E["读取闭区间；纳入相关月份已有叶"]
#     C -->|full| F["读取全部上游；纳入全部已有叶"]
#     D --> G{"上游新增为空？"}
#     G -->|是| H["报告已是最新；零 API；不更新标记"]
#     G -->|否| I["准备来源输入；按模式确定比较叶"]
#     E --> I
#     F --> I
#     I --> J["逐叶生成、读取本地范围并校验；比较业务映射"]
#     J --> K["汇总差异计划、质量错误与无规则 warning"]
#     K --> L{"存在差异叶？"}
#     L -->|否| M["自动 write 更新已处理标记；报告无差异并结束"]
#     L -->|是| N{"带 write？"}
#     N -->|否| O["输出只读计划并结束"]
#     N -->|是| P["重建并顺序提交各差异叶"]
#     P --> Q["全部叶成功后：自动模式写已处理标记"]
#     Q --> R["报告批次完成"]
#     P -.->|失败| S["停止批次；当前叶恢复；此前成功叶保留"]
#     Q -.->|失败| T["停止批次；已提交叶保留"]
# ```

# In[8]:


@click.command()
@click.option(
    "--lake-root",
    type=click.Path(path_type=pathlib.Path),
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--full",
    "full_refresh",
    is_flag=True,
)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full_refresh: bool,
    write: bool,
) -> None:
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (
        lake_root or formal_lake_root
    ).resolve()

    has_explicit_dates = (
        start_date is not None or end_date is not None
    )
    if (start_date is None) != (end_date is None):
        raise click.UsageError(
            "--start-date 与 --end-date 必须同时提供。"
        )
    if full_refresh and has_explicit_dates:
        raise click.UsageError(
            "--full 与 --start-date/--end-date 不能同时提供。"
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
        raise click.BadParameter(
            "起始日期不得晚于结束日期。"
        )

    run_started_at = perf_counter()
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\n运行入口开始 / Run entry started\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=started; "
        f"mode={'explicit' if has_explicit_dates else 'full' if full_refresh else 'automatic'}; "
        f"write={str(write).lower()}; lake_root={resolved_lake_root}\n{log_boundary}"
    )

    target_path = (
        resolved_lake_root / "silver" / TABLE_NAME
    )
    existing_contract_calendar_dataset = None
    existing_partition_keys = set()
    if (
        target_path.is_dir()
        and any(target_path.rglob("*.parquet"))
    ):
        existing_contract_calendar_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=HIVE_PARTITIONING,
        )
        existing_contract_calendar_schema = pa.schema(
            [
                existing_contract_calendar_dataset.schema.field(
                    name
                )
                for name
                in FUTURES_CONTRACT_CALENDAR_SCHEMA.names
            ],
            metadata=(
                existing_contract_calendar_dataset.schema.metadata
            ),
        )
        validate_compatible_dataset_schema(
            existing_contract_calendar_schema, FUTURES_CONTRACT_CALENDAR_SCHEMA, "现有合约日历 "
        )
        validate_dataset_fragment_schemas(
            existing_contract_calendar_dataset, FUTURES_CONTRACT_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有合约日历 "
        )

        # 显式日期与全历史模式将已有叶纳入双向比较；默认模式只规划新增上游叶。
        for parquet_path in (
            target_path.rglob("*.parquet")
            if has_explicit_dates or full_refresh
            else []
        ):
            if parquet_path.name == "schema.parquet":
                continue
            relative_parts = parquet_path.relative_to(
                target_path
            ).parts
            if (
                len(relative_parts) != 4
                or not relative_parts[0].startswith(
                    "exchange_code="
                )
                or not relative_parts[1].startswith("year=")
                or not relative_parts[2].startswith("month=")
            ):
                raise ValueError(
                    "正式表包含非法分区文件："
                    f"{parquet_path}"
                )
            existing_partition_keys.add(
                (
                    relative_parts[0].split("=", 1)[1],
                    int(
                        relative_parts[1].split(
                            "=",
                            1,
                        )[1]
                    ),
                    int(
                        relative_parts[2].split(
                            "=",
                            1,
                        )[1]
                    ),
                )
            )

    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=completed; "
        f"dataset_exists={str(existing_contract_calendar_dataset is not None).lower()}; "
        f"elapsed_s={perf_counter() - run_started_at:.3f}"
    )

    # 自动水位取正式行最大交易日与根标记已处理日期的较大值。
    latest_contract_trading_date = None
    if not full_refresh and not has_explicit_dates:
        automatic_watermark_candidates = []
        if existing_contract_calendar_dataset is not None:
            existing_contract_trading_dates = existing_contract_calendar_dataset.to_table(
                columns=["trading_date"],
            ).column("trading_date").to_pylist()
            if existing_contract_trading_dates:
                automatic_watermark_candidates.append(max(existing_contract_trading_dates))
        marker_processed_through = read_automatic_tail_processed_through(target_path)
        if marker_processed_through is not None:
            automatic_watermark_candidates.append(marker_processed_through)
        latest_contract_trading_date = max(automatic_watermark_candidates, default=None)

    if has_explicit_dates:
        mode = "explicit"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root,
            start_date=requested_start_date,
            end_date=requested_end_date,
        )
    elif full_refresh:
        mode = "full"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root
        )
    else:
        mode = "automatic"
        requested_variety_calendar_df = read_trusted_variety_calendar(
            resolved_lake_root,
            start_date_exclusive=latest_contract_trading_date,
        )
        if requested_variety_calendar_df.empty:
            click.echo(
                f"up_to_date: table={TABLE_NAME}; phase=run; status=completed; mode=automatic_tail; "
                f"latest_trading_date={latest_contract_trading_date}; api_calls=0"
            )
            click.echo(
                f"{log_boundary}\n合约日历检查完成，无待更新数据 / Contract calendar is up to date\n"
                "function=main()\n"
                f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
                f"mode={mode}; write={str(write).lower()}; outcome=up_to_date; "
                f"rows=0; partitions=0; "
                f"elapsed_s={perf_counter() - run_started_at:.3f}\n{log_boundary}"
            )
            return

    # 来源函数为本批首日补齐前一交易日，其余日期使用请求序列中的前一日。
    all_variety_trading_dates = sorted(
        requested_variety_calendar_df["trading_date"].drop_duplicates().tolist()
    )
    automatic_tail_processed_through = (
        max(all_variety_trading_dates) if mode == "automatic" else None
    )

    (
        requested_contract_catalog_df,
        previous_trading_date_by_trading_date,
        _,
    ) = collect_source_data(
        requested_variety_calendar_df,
        all_variety_trading_dates,
    )

    contract_catalog_by_group = {
        (
            str(exchange_code),
            str(underlying_code),
        ): contract_group_df.reset_index(drop=True)
        for (
            exchange_code,
            underlying_code,
        ), contract_group_df
        in requested_contract_catalog_df.groupby(
            ["exchange_code", "underlying_code"],
            sort=False,
        )
    }
    requested_variety_by_partition = {
        (
            str(exchange_code),
            int(year),
            int(month),
        ): variety_partition_df.reset_index(
            drop=True
        )
        for (
            exchange_code,
            year,
            month,
        ), variety_partition_df
        in requested_variety_calendar_df.groupby(
            PARTITION_COLUMNS,
            sort=True,
        )
    }
    requested_keys_by_partition = {
        partition_key: {
            (
                str(row.exchange_code),
                str(row.underlying_code),
                row.trading_date,
            )
            for row
            in variety_partition_df.itertuples(index=False)
        }
        for partition_key, variety_partition_df
        in requested_variety_by_partition.items()
    }

    if mode == "automatic":
        partition_keys = sorted(
            requested_variety_by_partition
        )
    elif mode == "explicit":
        first_requested_month = (
            requested_start_date.year,
            requested_start_date.month,
        )
        last_requested_month = (
            requested_end_date.year,
            requested_end_date.month,
        )
        relevant_existing_partition_keys = {
            partition_key
            for partition_key in existing_partition_keys
            if (
                first_requested_month
                <= partition_key[1:]
                <= last_requested_month
            )
        }
        partition_keys = sorted(
            set(requested_variety_by_partition)
            | relevant_existing_partition_keys
        )
    else:
        partition_keys = sorted(
            set(requested_variety_by_partition)
            | existing_partition_keys
        )

    run_updated_at = datetime.now(timezone.utc)
    dirty_partition_plans = []
    expected_session_count = 0
    missing_session_count = 0
    changed_session_count = 0
    extra_session_count = 0
    no_valid_rule_contract_day_count = 0
    no_valid_rule_samples = []

    for partition_number, partition_key in enumerate(
        partition_keys,
        start=1,
    ):
        if (
            partition_number == 1
            or partition_number % 25 == 0
        ):
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=reconcile; status=running; "
                f"completed={partition_number - 1}; total={len(partition_keys)}; "
                f"partition={partition_key}; elapsed_s={perf_counter() - run_started_at:.3f}"
            )

        requested_variety_partition_df = (
            requested_variety_by_partition.get(
                partition_key
            )
        )
        if requested_variety_partition_df is None:
            expected_contract_calendar_df = empty_pandas(
                FUTURES_CONTRACT_CALENDAR_SCHEMA
            )
            partition_audit = {
                "no_valid_rule_contract_day_count": 0,
                "no_valid_rule_samples": [],
            }
        else:
            (
                expected_contract_calendar_df,
                partition_audit,
            ) = build_contract_calendar_partition(
                requested_variety_partition_df,
                contract_catalog_by_group,
                previous_trading_date_by_trading_date,
                run_updated_at,
            )

        existing_contract_calendar_df = empty_pandas(
            FUTURES_CONTRACT_CALENDAR_SCHEMA
        )
        if (
            existing_contract_calendar_dataset is not None
            and (mode == "automatic" or partition_key in existing_partition_keys)
        ):
            exchange_code, year, month = partition_key
            existing_contract_calendar_table = (
                existing_contract_calendar_dataset.to_table(
                    columns=(
                        FUTURES_CONTRACT_CALENDAR_SCHEMA.names
                    ),
                    filter=(
                        ds.field("exchange_code")
                        == exchange_code
                    )
                    & (ds.field("year") == year)
                    & (ds.field("month") == month),
                )
            )
            existing_contract_calendar_df = (
                arrow_to_pandas(
                    existing_contract_calendar_table,
                    FUTURES_CONTRACT_CALENDAR_SCHEMA,
                )
            )

        if mode == "automatic":
            replacement_keys = (
                requested_keys_by_partition[
                    partition_key
                ]
            )
            existing_variety_date_index = (
                pd.MultiIndex.from_frame(
                    existing_contract_calendar_df[
                        VARIETY_DATE_COLUMNS
                    ]
                )
            )
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    existing_variety_date_index.isin(
                        replacement_keys
                    )
                ].reset_index(drop=True)
            )
        elif mode == "explicit":
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df.loc[
                    (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        >= requested_start_date
                    )
                    & (
                        existing_contract_calendar_df[
                            "trading_date"
                        ]
                        <= requested_end_date
                    )
                ].reset_index(drop=True)
            )
        else:
            comparison_existing_contract_calendar_df = (
                existing_contract_calendar_df
            )

        expected_business_rows = business_rows_by_key(
            expected_contract_calendar_df
        )
        existing_quality_error = None
        try:
            validated_existing_contract_calendar_df = (
                validate_contract_calendar_frame(
                    comparison_existing_contract_calendar_df,
                    f"现有分区 {partition_key} ",
                )
            )
            existing_business_rows = business_rows_by_key(
                validated_existing_contract_calendar_df
            )
        except (TypeError, ValueError) as error:
            existing_quality_error = (
                f"{type(error).__name__}: {error}"
            )
            existing_business_rows = {}

        expected_primary_keys = set(
            expected_business_rows
        )
        existing_primary_keys = set(
            existing_business_rows
        )
        missing_primary_keys = (
            expected_primary_keys - existing_primary_keys
        )
        extra_primary_keys = (
            existing_primary_keys - expected_primary_keys
        )
        changed_primary_keys = {
            primary_key
            for primary_key
            in expected_primary_keys & existing_primary_keys
            if (
                expected_business_rows[primary_key]
                != existing_business_rows[primary_key]
            )
        }
        if existing_quality_error is not None:
            missing_primary_keys = expected_primary_keys
            partition_extra_count = len(
                comparison_existing_contract_calendar_df
            )
        else:
            partition_extra_count = len(
                extra_primary_keys
            )

        is_dirty = bool(
            missing_primary_keys
            or changed_primary_keys
            or partition_extra_count
            or existing_quality_error
        )
        if is_dirty:
            dirty_partition_plans.append(
                {
                    "partition_key": partition_key,
                    "expected_row_count": len(
                        expected_contract_calendar_df
                    ),
                    "missing_count": len(
                        missing_primary_keys
                    ),
                    "changed_count": len(
                        changed_primary_keys
                    ),
                    "extra_count": (
                        partition_extra_count
                    ),
                    "quality_error": (
                        existing_quality_error
                    ),
                }
            )

        expected_session_count += len(
            expected_contract_calendar_df
        )
        missing_session_count += len(
            missing_primary_keys
        )
        changed_session_count += len(
            changed_primary_keys
        )
        extra_session_count += partition_extra_count
        no_valid_rule_contract_day_count += int(
            partition_audit[
                "no_valid_rule_contract_day_count"
            ]
        )
        for sample in partition_audit[
            "no_valid_rule_samples"
        ]:
            if len(no_valid_rule_samples) < 10:
                no_valid_rule_samples.append(sample)

    plan_name = {
        "automatic": "auto_plan",
        "explicit": "explicit_plan",
        "full": "full_plan",
    }[mode]
    click.echo(
        f"{log_boundary}\n分区差异计划已生成 / Partition reconciliation plan created\n"
        "function=main()\n"
        f"reconciliation_plan: table={TABLE_NAME}; phase=reconcile; status=completed; "
        f"plan={plan_name}; mode={mode}; completed={len(partition_keys)}; total={len(partition_keys)}; "
        "requested_variety_date_count="
        f"{len(requested_variety_calendar_df)}; "
        f"expected_session_count={expected_session_count}; "
        f"missing_session_count={missing_session_count}; "
        f"changed_session_count={changed_session_count}; "
        f"extra_session_count={extra_session_count}; "
        "no_valid_rule_contract_day_count="
        f"{no_valid_rule_contract_day_count}; "
        "touched_partition_count="
        f"{len(dirty_partition_plans)}; elapsed_s={perf_counter() - run_started_at:.3f}\n{log_boundary}"
    )
    if no_valid_rule_samples:
        click.echo(
            "warning: no_valid_trade_time_samples="
            f"{no_valid_rule_samples}; table={TABLE_NAME}; phase=reconcile; status=warning"
        )
    for partition_plan in dirty_partition_plans:
        click.echo(
            f"partition_plan: table={TABLE_NAME}; phase=reconcile; status=planned; "
            "partition="
            f"{partition_plan['partition_key']}; "
            "expected_rows="
            f"{partition_plan['expected_row_count']}; "
            f"missing={partition_plan['missing_count']}; "
            f"changed={partition_plan['changed_count']}; "
            f"extra={partition_plan['extra_count']}; "
            "quality_error="
            f"{partition_plan['quality_error']}"
        )

    if not dirty_partition_plans:
        if mode == "automatic" and write:
            write_automatic_tail_processed_through(
                resolved_lake_root, automatic_tail_processed_through
            )
        click.echo(
            f"up_to_date: table={TABLE_NAME}; phase=reconcile; status=completed; "
            f"outcome=source_unchanged; mode={mode}"
        )
        click.echo(
            f"{log_boundary}\n来源比较完成，无分区差异 / Source comparison completed, no partition changes\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write={str(write).lower()}; outcome=source_unchanged; "
            f"rows=0; partitions=0; "
            f"elapsed_s={perf_counter() - run_started_at:.3f}\n{log_boundary}"
        )
        return
    if not write:
        click.echo(
            f"{log_boundary}\n差异计划只读运行完成 / Reconciliation dry run completed\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
            f"mode={mode}; write={str(write).lower()}; outcome=dry_run; "
            f"rows={expected_session_count}; partitions={len(dirty_partition_plans)}; "
            f"elapsed_s={perf_counter() - run_started_at:.3f}\n{log_boundary}"
        )
        return

    committed_row_count = 0
    committed_partition_count = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=started; "
        f"completed=0; total={len(dirty_partition_plans)}"
    )
    for partition_plan in dirty_partition_plans:
        partition_key = partition_plan["partition_key"]
        requested_variety_partition_df = (
            requested_variety_by_partition.get(
                partition_key
            )
        )
        if requested_variety_partition_df is None:
            replacement_contract_calendar_df = (
                empty_pandas(
                    FUTURES_CONTRACT_CALENDAR_SCHEMA
                )
            )
        else:
            (
                replacement_contract_calendar_df,
                _,
            ) = build_contract_calendar_partition(
                requested_variety_partition_df,
                contract_catalog_by_group,
                previous_trading_date_by_trading_date,
                run_updated_at,
            )

        if mode == "automatic":
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
                replacement_variety_date_keys=(
                    requested_keys_by_partition[
                        partition_key
                    ]
                ),
            )
        elif mode == "explicit":
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
                replace_start_date=(
                    requested_start_date
                ),
                replace_end_date=requested_end_date,
            )
        else:
            committed_row_count += commit_partition(
                replacement_contract_calendar_df,
                resolved_lake_root,
                partition_key,
            )

        committed_partition_count += 1
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "
            f"completed={committed_partition_count}; total={len(dirty_partition_plans)}; "
            f"rows={committed_row_count}; elapsed_s={perf_counter() - run_started_at:.3f}"
        )

    if mode == "automatic":
        write_automatic_tail_processed_through(
            resolved_lake_root, automatic_tail_processed_through
        )

    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=completed; mode={mode}; "
        f"rows={committed_row_count}; "
        f"partitions={len(dirty_partition_plans)}; elapsed_s={perf_counter() - run_started_at:.3f}"
    )
    click.echo(
        f"{log_boundary}\n合约日历写入运行完成 / Contract calendar write run completed\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; "
        f"mode={mode}; write={str(write).lower()}; outcome=committed; "
        f"rows={committed_row_count}; partitions={len(dirty_partition_plans)}; "
        f"elapsed_s={perf_counter() - run_started_at:.3f}\n{log_boundary}"
    )


# ## 执行入口：Notebook、脚本与模块导入
# 
# Notebook 使用 `notebook_args` 显式传入 Click 参数，避免把内核启动参数当作业务参数。当前示例选择 `2026-08-01` 至 `2026-08-15` 的闭区间，不含 `--write`；执行该单元格会读取湖内上游，并在请求非空时访问 JQData，输出差异计划。
# 
# 直接运行同名 `.py` 时，Click 读取命令行参数；不指定日期或 `--full` 则使用默认尾部模式。将脚本作为模块导入时，定义函数与命令，不触发采集入口。需要提交时，在所选模式参数中加入 `--write`；其替换范围与水位行为见开篇更新表格。

# ### 局部流程：选择执行入口
# 
# 以下分支对应后面的代码单元格。交互式调用、脚本启动和模块导入各自使用明确的触发条件。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口单元格或启动脚本"] --> B{"交互式 Notebook？"}
#     B -->|是| C["使用显式 notebook_args；调用 main.main"]
#     B -->|否| D{"脚本作为主程序运行？"}
#     D -->|是| E["调用 main；读取命令行参数"]
#     D -->|否| F["模块导入；不触发采集入口"]
#     C --> G["按所选模式执行差异规划与可选提交"]
#     E --> G
# ```

# In[9]:


# Notebook 显式传入业务参数；脚本主程序读取命令行参数；模块导入不触发入口。
if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 示例为指定日期只读计划
    main.main(
        args=notebook_args,
        prog_name="c03_futures_contract_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# ### 局部流程：终端运行前检查与命令选择
# 
# 后面的单元格全部是注释，用于复制到终端执行。从仓库根启动，先完成统一环境预检和代码级导出一致性检查，再选择一种业务命令。示例依次提供默认尾部只读、默认尾部写入、指定日期写入与全历史写入；按需要选择，不应把所有示例连续执行。
# 
# 修改 Notebook 后，先运行 `b00_02_sync_notebook_exports.py --write` 完整同步同名脚本，再以 `--check` 做完整复核。启动前的 `--check --check-level code` 只证明代码正文一致，不能代替修改后的完整同步。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["激活 latitude_env_v2；进入仓库根"] --> B["运行统一环境预检"]
#     B --> C["代码级导出检查通过"]
#     C --> D["选择尾部、日期闭区间或 full 命令"]
#     D --> E{"命令带 write？"}
#     E -->|否| F["形成只读差异计划"]
#     E -->|是| G["提交相应差异范围；自动模式更新已处理标记"]
# ```

# In[10]:


# 在终端执行：激活标准环境并进入仓库根。
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# 启动业务前，依次运行环境预检与代码级导出检查。
# python R02_Market_Data\a01_Collection\b00_01_verify_runtime.py
# python R02_Market_Data\a01_Collection\b00_02_sync_notebook_exports.py --check --check-level code
# 以下业务示例按需要选择一种。
# 默认尾部只读计划：
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c03_futures_contract_calendar.py
# 默认尾部提交：
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c03_futures_contract_calendar.py --write
# 指定闭区间提交（替换区间内行，保留区间外行）：
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c03_futures_contract_calendar.py --start-date 2026-08-01 --end-date 2026-08-15 --write
# 全历史差异提交（仅替换有差异叶）：
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c03_futures_contract_calendar.py --full --write

