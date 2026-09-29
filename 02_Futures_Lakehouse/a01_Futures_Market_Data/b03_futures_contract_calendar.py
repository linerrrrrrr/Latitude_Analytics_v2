#!/usr/bin/env python
# coding: utf-8

# # b03_futures_contract_calendar
# 
# 生成 `dim_futures_contract_calendar`：每个固定月份合约—交易日—Session 一行。从 b02 已提交的品种日历与 JQData 合约规则展开理论 Session；理论时段不等于当天已经确认实际开市。事实采集白名单不参与本表筛选。
# 
# 阅读顺序：初始化与契约 → 水位与物理检查 → Session 解析校验 → 上游与来源读取 → 逐月生成 → 单分区提交 → 三模式入口 → 执行单元格。函数定义单元格只注册函数，实际调用顺序见总流程。Notebook 是唯一业务源，同名 `.py` 由默认 PythonExporter 生成。

# ## 总流程：从品种日历到合约 Session
# 
# 下图描述运行入口的实际调用顺序。矩形为操作，菱形为分支；未展开的校验异常向调用方抛出。单元格中的函数定义不会自行启动采集。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行 Notebook 单元格或脚本入口；检查参数"] --> B["检查现有表；选择自动、显式或 full 范围"]
#     B --> C["读取所需 b02 品种日"]
#     C --> D{"自动模式且无新增？"}
#     D -->|是| E["零 API 结束"]
#     D -->|否| F["采集合约目录、规则及前一交易日映射"]
#     F --> G["逐分区生成 Session；双向比较并汇总计划"]
#     G --> H{"启用 write？"}
#     H -->|否| I["输出只读结果；不写分区或水位"]
#     H -->|是| J{"存在 dirty 分区？"}
#     J -->|是| K["逐个重新生成、合并、提交并正式验收"]
#     K --> L{"本批分区均成功？"}
#     L -->|否| M["停止后续；按失败阶段清理或由共享事务恢复当前叶；保留此前成功叶"]
#     L -->|是| N{"自动模式？"}
#     J -->|否| N
#     N -->|是| O["原子写入本批已处理日期标记"]
#     N -->|否| P["输出运行完成"]
#     O --> P
# ```

# ## 更新范围与正式湖写入边界
# 
# | 模式 | 请求与比较范围 | `--write` 的提交范围 |
# | --- | --- | --- |
# | 默认尾部更新 | b02 中晚于 b03 自动水位的新增品种日；没有新增时在认证前结束 | 仅替换有差异分区内本批新增品种—日期键，保留其他旧行 |
# | 成对显式日期 | 指定闭区间内的上游格点与本地行，双向比较缺失、修订和多余行 | 仅提交有差异分区，替换该日期区间并保留区间外行 |
# | `--full` | b02 当前全部格点与本地整表，双向比较 | 仅提交有差异的完整分区 |
# 
# 自动水位取正式行最大交易日与表根 `schema.parquet` 的 `automatic_tail_processed_through` 较大值。即使新增候选全部没有有效 `trade_time`、未生成 Session，成功的自动 `--write` 仍记录本批已处理日期；只读运行不推进水位。没有新增日期时直接结束，不改写标记。
# 
# 默认模式信任已提交历史，不按历史合约数重新求差或清退旧行。历史内部缺口和规则修订由显式日期或 `--full` 检查。差异比较排除 `updated_at`，仅时间戳不同不触发提交。
# 
# 三种模式均可在带 `--write` 时更新正式湖；b03 的显式日期写入是已确认的例外。日期必须成对，且与 `--full` 互斥。不带 `--write` 仍会在存在请求格点时访问来源并形成计划。正式根目录来自 `.env` 的 `FUTURES_LAKE_ROOT`，运行时通过 `settings.futures_lake_root` 读取。

# ## 原始 API 与转换边界
# 
# - `get_all_securities(["futures"], date=None)` 请求完整期货证券目录。代码位于 DataFrame 索引；按固定月份代码和上市区间筛选与本批上游格点重叠的合约，不请求连续合约信息。
# - `get_futures_info(codes, fields=["contract_multiplier", "tick_size", "trade_time"])` 每批最多请求 200 个合约，检查字段和请求合约覆盖。乘数与 tick 是合约级标量；`trade_time` 包含历史生效区间。
# - 前一交易日从传入的有序交易日序列取得。当前入口传入本批请求日期序列，因此本批首日通过一次 `get_trade_days(end_date=首日, count=2)` 补齐边界，其余日期使用序列中前一日。
# - 没有当日有效规则的候选合约日跳过，并累计数量与至多 10 条样例，供入口输出 warning；规则格式非法或同日命中多条规则则抛出异常。
# 
# 请求格点为空时，来源函数直接返回空目录、空日期映射及零 API 计数，不认证。来源异常向调用方抛出，不自动重试。

# ## 逻辑表、物理分区与提交边界
# 
# 主键为 `contract_code, trading_date, session_number`，Hive 分区为 `exchange_code/year/month`。Schema、表名、主键与分区定义来自 `config.data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA`。
# 
# 每次提交合并一个完整叶分区：按精确品种—日期键、日期区间或整叶确定替换范围。替换后为空时移除该叶；全表无数据文件且缺少标记时保留 0 行 `schema.parquet`。自动更新成功后另行原子写入带已处理日期的标记。
# 
# 分区逐个提交，每个分区各自使用 `StagedPathTransaction`；失败时只恢复该次实际移动的叶和新建空表标记，此前已成功分区保留。自动水位标记在本批所需分区全部成功后写入，不属于跨分区共同回滚事务。

# ## 初始化与依赖
# 
# 按项目标记定位仓库根目录，随后导入配置、DataFrame 库和权威 Schema。执行本单元格不认证、不请求来源、不写湖。

# ### 局部流程：初始化
# 
# 本单元格只准备依赖，后续函数在被调用时才执行。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从当前目录向上搜索项目标记"] --> B{"找到项目根目录？"}
#     B -->|否| C["抛出未找到项目根目录异常"]
#     B -->|是| D["加入项目根与湖仓目录导入路径"]
#     D --> E["导入配置、表格库和权威 Schema"]
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
from a00_04_staged_path_transaction import StagedPathTransaction


# ## 表配置、物理契约与自动水位标记
# 
# 从权威 Schema metadata 读取表名、主键和分区，配置物理检查与 Session 解析所需常量。去掉 Hive 分区列后的物理 Schema、物理主键列和业务比较键的位置在这里构造一次，供各分区复用。物理检查固定字段、类型、nullable 和表身份 metadata，允许描述性 metadata 随权威配置更新。
# 
# 水位读取函数只读取 `schema.parquet` 的已处理日期；标记或该键不存在时返回 `None`。水位写入函数先在同目录写临时标记，复读 Schema 后原子替换正式标记，并清理临时文件。写入函数自行报告开始、暂存写入、复读、安装和完成耗时；失败时记录阶段并继续抛出异常。函数定义本身不执行这些 I/O。

# ### 局部流程：配置、物理检查与水位
# 
# 配置在单元格执行时加载；以下三个分支分别表示检查或水位函数被调用后的行为，彼此不是顺序调用。任一检查失败均抛出异常。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["读取 metadata；定义常量与函数"] --> B["后续按需调用"]
#     B --> C["物理检查：字段、类型、nullable、身份 metadata"]
#     C --> D["逐 fragment 检查去掉 Hive 分区列后的物理 Schema"]
#     B --> E{"读取水位：标记及日期键存在？"}
#     E -->|否| F["返回 None"]
#     E -->|是| G["解码 ISO 日期并返回；非法日期抛错"]
#     B --> H["写水位：记录开始；构造含已处理日期的空表 Schema"]
#     H --> I["写同目录临时标记并复读 Schema"]
#     I --> J["原子替换 schema.parquet"]
#     J --> K["清理临时标记后记录完成；异常清理后记录失败"]
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


# ## Schema 契约浏览
# 
# 交互式 Notebook 使用共享展示模块读取权威 Schema，并按选择展示有界数据样例。直接运行导出脚本时跳过展示；展示不调用业务 API，也不替代生产校验。

# ### 局部流程：Schema 与样例浏览
# 
# 展示只读权威 Schema 和所选有界样例，不发起业务采集。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["执行展示单元格"] --> B{"交互式 Notebook 环境？"}
#     B -->|是| C["共享展示模块读取 Schema"]
#     C --> D["按选择浏览字段说明和有界数据样例"]
#     B -->|否| E["跳过展示"]
# ```

# In[3]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_VARIETY_CALENDAR_SCHEMA,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## Session 解析与本表业务校验
# 
# `parse_session_text()` 将时段文本解析为起止钟点。`validate_contract_calendar_frame()` 先通过 `pandas_to_arrow()` 完成一次权威类型与非空校验，直接转成对应的 Pandas Arrow 扩展类型后检查主键、固定月份合约身份、上市及规则有效期、Session 序号、时间戳、分钟数、夜盘属性和派生字段。
# 
# 完整业务校验保留在生成结果、计划中的本地比较范围和合并后完整叶；提交入口只检查替换范围，不再对输入单独运行一遍完整业务校验。staging 和正式安装后的复读检查物理契约、主键和行数，不再次运行这套 Session 业务规则。`business_rows_by_key()` 只消费已完成类型校验的 DataFrame，直接按业务列生成排除 `updated_at` 的比较映射，不再转回 Arrow。可空标量的 `pd.NA` 归一成 `None`，保持原来的空值比较语义。

# ### 局部流程：Session 解析、业务校验与比较映射
# 
# 三个函数各有入口。完整业务校验失败通常抛出异常；main 对本地比较范围的 TypeError/ValueError 另有纳入修订计划的处理。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["解析时段文本"] --> B["匹配时段格式；转换起止钟点"]
#     B --> C["返回原始文本和钟点"]
#     D["校验合约日历 DataFrame"] --> E["按权威 Schema 转换；检查主键"]
#     E --> F{"空表？"}
#     F -->|是| G["返回空表"]
#     F -->|否| H["检查合约身份、来源及日期有效范围"]
#     H --> I["检查 Session 时间、分钟数、夜盘及派生字段"]
#     I --> J["检查每合约日的序号连续与起点唯一"]
#     J --> K["按主键排序并返回"]
#     L["生成业务比较映射"] --> M["直接读取已校验业务列；排除 updated_at；统一空值"]
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


# 对 b03 自己生产的完整业务语义做表级校验。
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


# 只比较业务列，避免 updated_at 造成无意义的分区重写。
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


# ## 正式上游读取与 JQData 合约元数据
# 
# `read_trusted_variety_calendar()` 检查 b02 数据集及物理契约，按尾部或成对日期过滤；b02 已证明的主键、日期、`active_contract_count` 和派生语义直接信任，不在此重做业务校验。
# 
# `collect_source_data()` 对非空请求执行认证、目录筛选、分批合约信息请求和前一交易日映射，返回本批合约目录与 API 计数。函数自身报告认证、目录检查、候选筛选、合约信息批次及边界交易日查询的进度，完成时输出 API 汇总与耗时；空请求输出零 API 完成记录。异常记录失败阶段后原样抛出。

# ### 局部流程：上游读取与来源采集
# 
# 先由入口调用上游读取函数，再把请求格点传给来源采集函数。空请求不认证；每次同步 API 请求前报告开始，返回并通过相应检查后报告结果；请求执行中不虚构百分比。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["读取 b02：检查范围参数与数据集存在"] --> B["检查逻辑及 fragment 物理契约"]
#     B --> C["按尾部或日期区间过滤并返回品种日历"]
#     C --> D{"采集输入为空？"}
#     D -->|是| E["返回空目录、空映射、零 API 计数"]
#     D -->|否| F["认证；请求完整合约目录"]
#     F --> G["校验目录；保留覆盖本批格点的固定月份合约"]
#     G --> H["每批至多 200 个合约请求信息；报告已完成批数"]
#     H --> I["检查字段与返回合约覆盖；合并目录"]
#     I --> J{"请求包含传入日期序列首日？"}
#     J -->|是| K["一次 get_trade_days 补前一交易日"]
#     J -->|否| L["使用日期序列的前一日"]
#     K --> M["构建日期映射；记录完成日志并返回目录与 API 汇总"]
#     L --> M
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


# ## 逐月展开当前有效 Session
# 
# `build_contract_calendar_partition()` 一次接收一个交易所—年月的品种日格点。对每个格点筛选上市中的固定月份合约，选择当日唯一有效规则，再逐段生成北京时间的 Session。合约乘数与 tick 的空值和浮点归一化在该合约日的 Session 循环前完成一次，各段复用。
# 
# 20:00 及以后的起点归前一交易日，06:00 前的起点归前一交易日的下一自然日，其余归当前交易日；结束钟点不晚于起点时，结束日期再加一天。函数在开始、生成进度和完成时记录日志：首个、每 25 个及最后一个品种日报告已处理数、候选合约日数和已生成行数。生成后完成本表业务校验，记录耗时并返回无有效规则统计；样例 warning 仍由入口作本批汇总。
# 
# 当前入口先逐分区生成并比较，只保存差异计划；写入时再生成 dirty 分区。这样不同时保存全历史 Session。本单元格描述现有计算顺序，不改变重复生成策略。

# ### 局部流程：逐分区生成 Session
# 
# 每次只处理一个交易所—年月。无有效规则计数后跳过；格式错误或规则重叠抛出异常。样例 warning 由入口汇总输出。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["遍历本分区品种日"] --> B["找到品种目录；筛选上市中的合约"]
#     B --> C["遍历候选合约；检查 trade_time 规则"]
#     C --> D{"当日有效规则数量？"}
#     D -->|零或缺失| E["计入无有效规则统计；继续下一合约"]
#     D -->|多条| F["抛出规则重叠异常"]
#     D -->|唯一| G["逐 Session 解析起止钟点"]
#     G --> H["依据前一交易日、夜盘和跨午夜规则确定日期"]
#     H --> I["构造北京时间戳、分钟数与完整行"]
#     I --> J["继续其余 Session、合约及品种日；定期报告生成进度"]
#     E --> J
#     J --> K["汇总分区行或构造空表"]
#     K --> L["完整业务校验；记录完成日志并返回结果与审计统计"]
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


# ## 单分区暂存、提交与失败回滚
# 
# `commit_partition()` 依次执行替换范围检查、精确读取旧叶、按模式合并、完整叶业务校验，再写 staging 并复读。合并后的完整叶负责一次完整业务校验，提交输入不再单独重复检查；序列化时按同一权威 Schema 构造 Arrow 表，不再调用已完成的校验。入口的全表检查保留一次；每次提交仅打开当前叶，并用 `partition_base_dir` 还原 Hive 分区列。独立调用时也检查该叶物理契约，不检查其他叶。
# 
# staging 通过后，在 `StagedPathTransaction` 的 `with` 中安装当前叶，再由本函数检查正式路径的物理 Schema、表身份 metadata、主键与行数。合并结果为零行时显式传入 `staged_path=None` 删除该叶；应有数据但 staging 缺失会报错，不推断为删除。全表清空且没有标记时，先在 staging 写零行 `schema.parquet`，由同一事务安装并正式复读。
# 
# staging 失败由本函数清理后抛出；安装或正式验收失败由共享模块按实际移动记录倒序恢复。失败的新叶保留在隔离目录，本次新建空表标记直接移除；恢复不完整时保留旧备份。一次调用只有一个叶及其必要的新建标记，此前成功叶不参与回滚。自动水位仍在本批分区成功后另行原子写入。
# 
# 本函数报告输入、旧叶读取、合并校验、暂存复读、正式安装与完成耗时；共享模块负责恢复结果日志。本函数在事务退出及清理结束后用 `planning_progress` 报告 `failed_phase` 和异常，保留 monitor 可识别的前缀；`partition_committed` 只在事务成功退出后报告完成。

# ### 局部流程：单分区提交与失败恢复
# 
# 合并、Parquet 写入和物理验收留在 b03；共享模块仅负责路径安装与恢复。空表标记与当前叶共同回滚，自动水位由 main 在本批成功后另行推进。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查替换范围；精确读取当前旧叶"] --> B["按键、日期区间或整叶合并"]
#     B --> C["完整叶业务校验一次；按 Schema 序列化"]
#     C --> D{"完整叶有行？"}
#     D -->|是| E["写 staging；复读 Schema、主键及行数"]
#     D -->|否| F["明确删除当前叶"]
#     E -. 暂存失败 .-> X["清理 staging；记录失败并抛错"]
#     E --> G["进入单叶共享事务；备份并安装或删除"]
#     F --> G
#     G --> H["必要时暂存并安装空表标记；正式复读验收"]
#     G -. 安装失败 .-> R["共享事务倒序恢复实际移动的目标"]
#     H -. 验收失败 .-> R
#     R --> S["隔离失败新叶；移除新标记；恢复失败则保留备份"]
#     S --> U["清理 staging；记录失败并抛错"]
#     H --> T["成功退出事务；清理临时目录；记录完成并返回"]
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


# ## 命令行入口与三模式差异计划
# 
# `main()` 负责参数门禁、一次正式数据集检查、水位与上游范围选择、差异计划及顺序提交。提交循环不再重复打开全表或检查全部 fragment，只读取各自触达叶。自动模式水位同时考虑正式最大交易日和已处理日期标记；显式日期与 `--full` 双向比较相应范围。
# 
# 比较时，本地范围的业务校验若抛出 `TypeError` 或 `ValueError`，会记录为 `quality_error` 并纳入替换计划；其他异常按原调用链抛出。没有差异时不提交叶；只读模式只输出计划。自动写入即使没有 dirty 叶，也在本批存在新增请求日期时更新已处理标记。
# 
# 日志沿用 b01、b02 的 `=` 入口边界，使用 `table/phase/status`，在已有分区进度点报告 `completed/total`，正常结束报告累计 `elapsed_s`。进度行保留 monitor 可识别的行首前缀；完成日志只在对应操作成功后输出，异常不会输出运行成功。采集、生成、单分区提交和水位写入日志由对应函数负责；入口保留模式、差异计划、聚合 warning 与批次汇总，不重复报告来源或水位完成。批次提交汇总使用 `phase=commit_batch`，与单分区完成事件区分。
# 
# 当前单元格只定义 `main()`。后面的独立执行单元格与 b01、b02 一致：Notebook 使用显式只读参数，Python 脚本读取命令行参数，模块导入不触发采集。

# ### 局部流程：三模式计划、提交与入口
# 
# 当前代码单元格只定义 main；后面的独立执行单元格负责调用。比较排除 updated_at；本地比较范围的业务错误记入修订计划。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["调用 main；检查日期成对及 full 互斥"] --> B["打开现有表并检查物理契约"]
#     B --> C{"运行模式？"}
#     C -->|自动| D["取正式最大日与标记较大值；读取上游尾部"]
#     C -->|显式日期| E["读取上游指定日期；纳入同期本地分区"]
#     C -->|full| F["读取全部上游；纳入全部本地分区"]
#     D --> G{"上游尾部为空？"}
#     G -->|是| H["up_to_date；零 API 结束"]
#     G -->|否| I["采集来源；按交易所年月分组"]
#     E --> I
#     F --> I
#     I --> J["逐分区生成期望行；读取相应本地范围"]
#     J --> K["比较缺失、变化、多余行；本地质量错误纳入计划"]
#     K --> L["汇总 dirty 计划与无有效规则 warning"]
#     L --> M{"存在 dirty 计划？"}
#     M -->|否| N["自动 write 时写已处理标记；输出无差异并结束"]
#     M -->|是| O{"启用 write？"}
#     O -->|否| P["输出只读完成；结束"]
#     O -->|是| Q["逐 dirty 分区重新生成并调用 commit_partition"]
#     Q --> R["全部成功后，自动模式写已处理标记"]
#     R --> S["输出提交汇总与运行完成"]
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

        # 只有显式/full 双向维护需要发现目标孤儿分区；日常尾部不扫描全根。
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

    # 默认水位同时信任正式行与成功自动批次 marker，不再按全历史合约数求差。
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

    # 对尾部/显式范围，首日的前一交易日由 collect_source_data 的有界边界调用补齐。
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


# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数。当前单元格使用与 b01、b02 一致的显式日期只读示例（2026-08-01 至 2026-08-15）；执行会发起来源请求。脚本运行时使用命令行参数，模式与写入限制见开篇表格。在 Notebook 中导入同名 Python 模块不会触发入口。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会进入采集流程。流程图本身不执行代码。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口单元格或运行脚本"] --> B{"Notebook 交互环境？"}
#     B -->|是| C["显式 notebook_args；不读取内核参数"]
#     C --> D["main.main：standalone_mode=False"]
#     B -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main：读取命令行参数"]
#     E -->|否| G["模块导入：不触发采集"]
#     D --> H["进入运行模式分支"]
#     F --> H
# ```

# In[9]:


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
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 单元格内不写 --write
    main.main(
        args=notebook_args,
        prog_name="b03_futures_contract_calendar",
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
#     A["在终端激活 latitude"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["默认尾部更新并提交"]
# ```

# In[10]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a01_Futures_Market_Data\b03_futures_contract_calendar.py --write

