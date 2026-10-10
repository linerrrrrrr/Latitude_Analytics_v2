#!/usr/bin/env python
# coding: utf-8

# # 中国期货交易日历：生成与更新
# 
# 本 Notebook 生成 `dim_trade_calendar`（中国期货交易日历维度表），为后续期货日历提供自然日范围和交易日标记。每个自然日保留一行，休市日也在表内；`is_trading_day` 由该日期是否属于 JQData `get_trade_days()` 返回的交易日集合决定。
# 
# 日历以 `calendar_date` 为主键，按自然年 `year` 分区，保存到目标湖的 `silver/dim_trade_calendar/`。字段、类型和表级说明由 `TRADE_CALENDAR_SCHEMA` 定义，开篇的契约浏览器可查看完整定义及已有数据样例。
# 
# 阅读时先通过全流程图了解更新与提交关系，再在各环节查看局部流程和代码。执行时使用 `latitude_env_v2` 内核，依次完成初始化、契约浏览和函数定义，最后运行入口单元格。函数定义只注册函数；入口单元格会按参数访问 JQData。同名 `.py` 由默认 PythonExporter 完整生成，供终端和批量调用。

# ## 全流程：确定范围、生成日历与提交分区
# 
# 运行由 `main()` 选择更新范围，`collect()` 生成并校验日历，启用 `--write` 后由 `commit_partitions()` 提交。默认模式在没有尾部新增日期时直接结束；全历史模式在来源比较后没有差异时结束。只读运行会完成所需的来源请求和校验。
# 
# 下图覆盖准备、三种运行模式、来源采集、差异选择、分区安装及失败恢复。矩形表示操作，菱形表示条件，实线表示正常流程，虚线表示异常转向。各环节的局部图进一步展开对应操作；参数或校验失败会抛出异常并停止。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["定位项目、导入依赖；Notebook 浏览契约；注册函数"] --> B["执行入口：校验参数、目标湖与有效截止日"]
#     B --> C{"运行模式"}
#     C -->|默认尾部| D["检查已有物理契约；从最大自然日加一天开始，空湖从配置起点开始"]
#     D --> E{"存在有效尾部日期？"}
#     E -->|否| Z["报告无需更新，结束"]
#     E -->|是| F["确定尾部闭区间"]
#     C -->|全历史| G["确定配置起点至有效截止日"]
#     C -->|显式日期| H["校验指定闭区间及写入边界"]
#     F --> I["collect：认证 JQData，一次请求交易日集合"]
#     G --> I
#     H --> I
#     I --> J["逐自然日生成字段；Arrow 契约转换与业务校验"]
#     J --> J1["按配置保存已校验日历的运行预览"]
#     J1 --> K{"全历史模式？"}
#     K -->|是| L["校验已有日历；按日期比较除 updated_at 外的字段"]
#     L --> M{"存在缺失或修订日期？"}
#     M -->|否| Z
#     M -->|是| N["选出差异行"]
#     K -->|否| O["使用本次采集区间的全部行"]
#     N --> P{"启用 --write？"}
#     O --> P
#     P -->|否| Q["报告只读计划与校验完成，结束"]
#     P -->|是| R["commit_partitions：按主键合并触达年份并校验"]
#     R --> S["进入共享事务：写 staging 并复读"]
#     S --> T["逐年备份、安装与正式复读"]
#     T --> U["整组事务成功退出；清理临时目录并报告提交完成"]
#     S -.->|事务内异常| V["按实际移动倒序恢复本批目标"]
#     T -.->|事务内异常| V
#     V --> W["清理 staging；回滚不完整保留备份；抛出异常"]
# ```

# ## 更新范围与写入条件
# 
# “有效截止日”按 `Asia/Shanghai` 时区的运行时刻确定：20:00 前取前一自然日，20:00 起取当日。“配置起点”来自 `.env` 的 `FUTURES_DATA_START_DATE`，通过 `settings.futures_data_start_date`读取；正式湖根目录来自 `.env` 的 `FUTURES_LAKE_ROOT`，通过 `settings.futures_lake_root` 读取。未传 `--lake-root` 时使用正式湖。
# 
# | 模式 | 处理范围与结果 | 启用 `--write` 的条件 |
# | --- | --- | --- |
# | 默认尾部更新 | 从目标表最大自然日的下一天推进至有效截止日；空湖从配置起点开始。没有新增日期时，在认证和 API 请求前结束 | 可写正式湖，提交本次新增行 |
# | `--full` 全历史比较 | 请求配置起点至有效截止日的完整来源日历，比较已有表并选出缺失或业务值修订的日期 | 可写正式湖，只提交差异行；与显式日期互斥 |
# | `--start-date` 与 `--end-date` 成对显式日期 | 采集并校验指定自然日闭区间；起始日不得晚于结束日，结束日不得晚于有效截止日 | 仅允许写入通过 `--lake-root` 指定的非正式湖 |
# 
# 默认尾部更新信任已提交历史，只以最大自然日确定新增范围。历史内部缺口与来源修订使用 `--full` 检查。全历史比较排除 `updated_at`，业务值相同的行保留原更新时间；已有日历含配置起点之前或有效截止日之后的日期时，比较会报错。
# 
# 不带 `--write` 时仍会执行所选模式需要的采集和校验，结果留在内存中，不提交日历分区。操作台配置了预览路径时，还会保存运行预览，具体条件见采集环节。
# 
# 完整字段定义见[数据契约中的 `TRADE_CALENDAR_SCHEMA`](../../../config/data_contracts.py)；各入口的来源、更新与验收规则见[采集说明](../README.md)，silver 的类型和存储约定见[湖仓规则](../../a02_Lake/AGENTS.md)。

# ## 初始化：定位项目并导入依赖
# 
# 从当前工作目录逐级向上查找同时包含 `.git`、`.env`、`config/settings.py` 的项目根目录，将项目根加入导入路径，并通过完整包路径导入采集支撑模块，再读取 `settings`、表格库、权威 Schema 与路径事务模块。找不到项目根时停止初始化。
# 
# 本环节准备后续函数需要的依赖；JQData 认证由 `collect()` 在采集时执行。

# ### 局部流程：初始化
# 
# 输入是 Notebook 或脚本的当前工作目录；完成后，配置、类型契约和共享事务模块可供后续单元格使用。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["以当前工作目录为候选起点"] --> B{"候选目录同时包含三个项目标记？"}
#     B -->|否，仍有父目录| P["移动到上一级候选目录"]
#     P --> B
#     B -->|否，查找已结束| X["抛出未找到项目根目录异常"]
#     B -->|是| C["将项目根加入模块搜索路径"]
#     C --> D["导入 settings、Click、Pandas、Arrow 和 TRADE_CALENDAR_SCHEMA"]
#     D --> E["导入 StagedPathTransaction；初始化完成"]
# ```

# In[1]:


from __future__ import annotations

import pathlib
import sys
import uuid
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from time import perf_counter

project_markers = ['.git', '.env', 'config/settings.py']
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all(((candidate_root / marker).exists() for marker in project_markers)):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError('未找到项目根目录')

# Click 解析命令行参数，Pandas 计算表格，Arrow 和 Dataset 处理类型契约与 Hive 分区。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

# 环境配置与 Arrow Schema 均从项目权威入口读取。
from config.settings import settings
from config.data_contracts import (
    TRADE_CALENDAR_SCHEMA,    # 中国期货交易日历的权威 Arrow Schema
    pandas_to_arrow,         # 将列及顺序匹配的 Pandas DataFrame 安全转换为契约化 Arrow 表
    validate_arrow_table    # 按权威 Schema 安全转换 Arrow 表，并校验列顺序和非空约束
)
from R02_Market_Data.a01_Collection.b00_04_staged_path_transaction import StagedPathTransaction


# ## 契约浏览：查看字段与已有样例
# 
# 在 Notebook 中，`display_schema_metadata()` 展示 `TRADE_CALENDAR_SCHEMA` 的用途、粒度、字段与 metadata（契约附加说明）。可展开完整表说明和单字段详情，并通过筛选查看 `settings.futures_lake_root` 中已有年份分区的有界样例。
# 
# 样例表头附带权威字段 metadata 中的中文含义，计算和落盘字段保持英文名称。样例未命中只说明所选读取范围没有结果；读取范围与上限见[Schema 浏览器说明](../README.md#notebook-开篇-schema-契约呈现)。本环节只读契约和已有数据，导出的脚本跳过交互展示。

# ### 局部流程：契约与样例浏览
# 
# 展示使用初始化导入的权威 Schema，数据样例按交互选择读取已有分区。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"Notebook 交互环境？"} -->|是| B["展示交易日历契约、字段和 metadata"]
#     B --> C["按筛选读取一个已有年份分区的有界样例"]
#     C --> D["展示中文表头与读取结果提示"]
#     A -->|否| E["跳过交互展示，进入后续定义"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from R02_Market_Data.a01_Collection.b00_03_notebook_schema_browser import display_schema_metadata
    display_schema_metadata([TRADE_CALENDAR_SCHEMA], lake_root=settings.futures_lake_root)


# ## 表配置与物理契约检查
# 
# 表名、主键和分区字段分别从 `TRADE_CALENDAR_SCHEMA.metadata` 读取一次，供路径构造、分区读写和校验复用。`EFFECTIVE_AFTER` 定义北京时间 20:00 的日级有效截止规则。
# 
# `validate_compatible_dataset_schema()` 比较字段名称及顺序、Arrow 类型、是否允许为空，以及 `table_name`、`primary_key`、`partition_columns` 三项表身份 metadata。描述性 metadata 的差异不阻断读取，说明以代码中的权威 Schema 为准。
# 
# 逻辑 Dataset 的 Schema 包含从 Hive 目录恢复的分区字段；单个 Parquet 文件的物理 Schema 不包含这些分区字段。`validate_dataset_fragment_schemas()` 按此区别逐文件检查，避免只看 Dataset 的逻辑 Schema 而漏掉其他文件的类型或结构差异。

# ### 局部流程：物理契约检查
# 
# 本单元格读取表配置并定义检查函数；下图描述函数被调用时的行为。任一字段或表身份不匹配均抛出异常。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"检查对象"} -->|单个 Schema| B["比较字段及顺序、类型、nullable"]
#     B --> C["比较表名、主键和分区 metadata"]
#     C --> D["检查通过，返回"]
#     A -->|Dataset 中的每个文件| E["从权威 Schema 排除 Hive 分区字段，得到预期物理 Schema"]
#     E --> F["逐个 fragment 读取 Parquet 物理 Schema"]
#     F --> G["调用单个 Schema 检查"]
#     G --> H{"还有文件？"}
#     H -->|是| F
#     H -->|否| I["逐文件检查完成，返回"]
# ```

# In[3]:


# 表名、分区、主键和日级数据生效时间。
TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")    # 中国期货交易日历维度表。
PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")  # 正式表的 Hive 分区字段。
PRIMARY_KEY = TRADE_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")  # 每个自然日唯一一行。
EFFECTIVE_AFTER = time(20, 0)
PHYSICAL_METADATA_KEYS = (b'table_name', b'primary_key', b'partition_columns')


def validate_compatible_dataset_schema(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    context: str,
) -> None:
    """只固定物理字段和路由 metadata；允许描述性 metadata 随代码更新。"""
    if actual_schema.names != expected_schema.names:
        raise TypeError(f'{context}字段及顺序与契约不一致。')
    for expected_field in expected_schema:
        actual_field = actual_schema.field(expected_field.name)
        if (
            actual_field.type != expected_field.type
            or actual_field.nullable != expected_field.nullable
        ):
            raise TypeError(f'{context}字段 {expected_field.name!r} 的类型或 nullable 与契约不一致。')
    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    for metadata_key in PHYSICAL_METADATA_KEYS:
        if actual_metadata.get(metadata_key) != expected_metadata.get(metadata_key):
            raise TypeError(f'{context}{metadata_key.decode()} metadata 与契约不一致。')


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
            f'{context}fragment {fragment.path} ',
        )


# ## 业务校验：日期、派生字段与固定值
# 
# `validate_calendar_table()` 接收已经由 `pandas_to_arrow()` 或 `validate_arrow_table()` 完成字段、类型和非空校验的 Arrow 表，验证交易日历的业务规则，成功后返回原表。
# 
# 校验内容包括：表非空、主键唯一；日期键、ISO 星期序号、周末标记和年份可由 `calendar_date` 复算；来源为 `JQData_get_trade_days`，日历名称为 `CN_FUTURES_MARKET`，时区为 `Asia/Shanghai`，生效时刻为 20:00；`updated_at` 不晚于校验时的 UTC 当前时间。交易日是否落在请求范围内，由 `collect()` 在来源响应阶段检查。
# 
# `require_contiguous=True` 还要求最小日至最大日之间逐自然日连续，用于来源生成的完整区间。新增或修订行可能散布在多个年份，提交输入、合并后的触达年份及全历史已有表使用 `False`，只关闭连续性检查，其余规则照常执行。该开关不会自动补齐日期。

# ### 局部流程：交易日历业务校验
# 
# 输入已满足 Arrow 类型与非空约束；下图中的任一业务检查失败都会抛出异常。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["接收契约化 Arrow 日历表"] --> B["转为 Pandas，按日期排序"]
#     B --> C["检查表非空、主键唯一"]
#     C --> D{"require_contiguous=True？"}
#     D -->|是| E["核对首尾日期之间的完整自然日序列"]
#     D -->|否| F["复算日期键、星期、周末标记和年份"]
#     E --> F
#     F --> G["核对来源、日历名称、时区和 20:00 生效时刻"]
#     G --> H["确认 updated_at 不晚于校验时的 UTC 时间"]
#     H --> I["返回通过业务校验的原 Arrow 表"]
# ```

# In[4]:


def validate_calendar_table(
    calendar_table: pa.Table,
    *,
    require_contiguous: bool = True,
) -> pa.Table:
    """
    校验已经通过权威 Arrow 契约转换的交易日历业务规则。

    Parameters
    ----------
    calendar_table : pa.Table
        已由 pandas_to_arrow() 或 validate_arrow_table() 完成类型与非空校验的日历表。
    require_contiguous : bool, default True
        是否要求 `calendar_date` 从最小日到最大日逐自然日连续。
        离散年份的局部提交批次可设为 `False`。

    Returns
    -------
    pa.Table
        通过业务校验的原 Arrow 表；本函数不重复类型转换。
    """

    # Arrow 契约由调用方统一转换入口保证；这里只检查本表独有的业务不变量。
    calendar_df = calendar_table.to_pandas().sort_values('calendar_date').reset_index(drop=True)

    # 交易日历至少应包含一条有效日期记录。
    if calendar_df.empty:
        raise ValueError('交易日历不得为空。')

    # 主键必须唯一，避免同一日历日期出现重复记录。
    if calendar_df.duplicated(PRIMARY_KEY).any():
        raise ValueError('交易日历存在重复主键。')

    # 完整日历批次要求覆盖区间内的每一个自然日。
    if require_contiguous:
        expected_calendar_dates = pd.date_range(
            calendar_df.calendar_date.iloc[0],
            calendar_df.calendar_date.iloc[-1],
            freq='D',
        ).date

        # 实际日期序列必须与按首尾日期生成的完整自然日序列一致。
        if calendar_df.calendar_date.tolist() != expected_calendar_dates.tolist():
            raise ValueError('交易日历自然日不连续。')

    # date_key 必须严格由 calendar_date 按 YYYYMMDD 格式派生。
    if calendar_df.date_key.tolist() != calendar_df.calendar_date.map(lambda calendar_date: calendar_date.strftime('%Y%m%d')).tolist():
        raise ValueError('date_key 与 calendar_date 不一致。')

    expected_weekdays = calendar_df.calendar_date.map(lambda calendar_date: calendar_date.weekday() + 1)

    # weekday 必须与 calendar_date 对应的 ISO 星期序号一致。
    if calendar_df.weekday.tolist() != expected_weekdays.tolist():
        raise ValueError('weekday 与 calendar_date 不一致。')

    # is_weekend 只能由 weekday 是否为周六或周日确定。
    if calendar_df.is_weekend.tolist() != expected_weekdays.isin([6, 7]).tolist():
        raise ValueError('is_weekend 与 weekday 不一致。')

    # 数据来源必须统一为约定的聚宽交易日接口。
    if not calendar_df.source.eq('JQData_get_trade_days').all():
        raise ValueError('source 不是约定值 JQData_get_trade_days。')

    # 日历标识必须统一为中国期货市场日历。
    if not calendar_df.calendar_name.eq('CN_FUTURES_MARKET').all():
        raise ValueError('calendar_name 不是约定值 CN_FUTURES_MARKET。')

    # 日历时区必须统一使用中国标准时间。
    if not calendar_df.calendar_timezone.eq('Asia/Shanghai').all():
        raise ValueError('calendar_timezone 不是 Asia/Shanghai。')

    # 每条记录的生效时间必须统一为约定的 20:00:00。
    if not calendar_df.effective_after.eq(EFFECTIVE_AFTER).all():
        raise ValueError('effective_after 不是 20:00:00。')

    # year 必须严格等于 calendar_date 所属自然年。
    if calendar_df.year.tolist() != calendar_df.calendar_date.map(lambda calendar_date: calendar_date.year).tolist():
        raise ValueError('year 与 calendar_date 不一致。')

    # 数据更新时间不得晚于执行本次校验时的 UTC 当前时间。
    if (calendar_df.updated_at > pd.Timestamp.now(tz='UTC')).any():
        raise ValueError('updated_at 不得晚于当前校验时间。')

    return calendar_table


# ## 分区提交：合并年份与失败恢复
# 
# `commit_partitions()` 接收本批新增或修订行，按 `calendar_date` 以新行覆盖已有同键行，并保留触达年份内其他已提交行。替换的是合并后的整个年份分区；“完整年份分区”指该分区原有行与本批行的合并结果，不要求生成尚未纳入有效范围的日期。其他年份不参与替换，空输入直接返回 0。
# 
# 输入先通过 Arrow 契约和业务校验；存在旧行时，合并结果再通过契约与业务校验。随后写入 staging（本批临时目录）并复读，再将各年份分区依次安装到正式位置并复读。staging 与正式复读检查物理字段、表身份、主键唯一性和行数：staging 总行数须等于合并结果，每个正式年份的行数须等于对应 staging 年份。
# 
# 本次调用的所有触达年份属于同一个 `StagedPathTransaction` 事务。安装或正式验收失败时，按实际移动记录倒序移除已安装的新分区并恢复旧分区；一处恢复失败后仍尝试其余目标。staging 在退出时清理，成功提交或完整回滚后清理备份，回滚不完整则保留备份并抛出异常。逐年替换使用同一文件系统内的路径移动；事务不提供多年份同时原子可见、进程终止后的自动恢复或并发写入协调。
# 
# 日志中的 `rows` 与函数返回值均为本批输入行数；`replacement_rows` 是包含保留旧行的完整替换行数。`partition_committed` 表示单个年份复读通过，后续年份失败仍会触发整批回滚；整组成功退出事务后输出的 `committed: status=completed` 才表示本次提交完成。

# ### 局部流程：按完整年份提交
# 
# 所有触达年份先在 staging 中准备并验收，再逐年安装。下图的异常恢复范围覆盖本次事务已经实际移动的全部年份。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录提交开始与输入行数"] --> B{"输入为空？"}
#     B -->|是| Z["记录 skipped，返回 0"]
#     B -->|否| C["输入转换与业务校验；确定触达年份和临时路径"]
#     C --> D{"存在已提交数据？"}
#     D -->|是| E["检查物理契约；读取触达年份；按主键合并并校验"]
#     D -->|否| F["使用已校验输入作为替换结果"]
#     E --> G["进入共享事务：写 staging 并复读物理契约、主键和总行数"]
#     F --> G
#     G --> H["逐年确认 staging 分区及预期行数"]
#     H --> I["共享事务备份旧年份、记录实际移动、安装新年份"]
#     I --> J["正式逐文件物理检查；核对主键与行数；记录年份进度"]
#     J --> K{"还有触达年份？"}
#     K -->|是| H
#     K -->|否| L["成功退出事务；清理备份与 staging；报告完成并返回输入行数"]
#     G -.->|事务内异常| R["按实际移动倒序移除新分区、恢复旧分区；继续尝试其余目标"]
#     H -.->|事务内异常| R
#     I -.->|事务内异常| R
#     J -.->|事务内异常| R
#     R --> S["清理 staging；回滚不完整保留备份；抛出异常"]
# ```

# In[5]:


def commit_partitions(new_calendar_df: pd.DataFrame, lake_root: pathlib.Path) -> int:
    """
    提交 `new_calendar_df` 触达年份的完整中国期货交易日历结果。

    Parameters
    ----------
    new_calendar_df : pd.DataFrame
        按权威 Schema 生成的本批新增或修订行。同一主键的新行覆盖正式表旧行，
        触达年份中未出现在 `new_calendar_df` 的其他正式行原样保留。
    lake_root : pathlib.Path
        本次读写的数据湖根目录，其下应包含或允许创建 `silver` 目录。

    Returns
    -------
    int
        本批输入 `new_calendar_df` 的行数，不是合并后完整年份分区的总行数。
    """

    commit_started_at = perf_counter()
    click.echo(
        f'partition_plan: table={TABLE_NAME}; phase=commit; status=started; '
        f'rows={len(new_calendar_df)}; lake_root={lake_root}'
    )
    if new_calendar_df.empty:
        click.echo(
            f'committed: table={TABLE_NAME}; status=skipped; rows=0; partitions=0; '
            f'elapsed_s={perf_counter() - commit_started_at:.3f}'
        )
        return 0

    click.echo(f'planning_progress: table={TABLE_NAME}; phase=merge_validate; status=started')

    # 本批修订日期可分散在多个年份；关闭连续性检查，保留其余契约和业务校验。
    new_calendar_table = validate_calendar_table(
        pandas_to_arrow(new_calendar_df.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA), # 只保留 Schema 中规定的列
        require_contiguous=False,
    )

    # 建立交易日历数据集及本批 staging、backup 路径。
    silver_dir = lake_root.resolve() / 'silver'
    calendar_path = silver_dir / TABLE_NAME

    # 每次调用使用独立批次标识，隔离 staging 和备份目录。
    run_id = uuid.uuid4().hex
    staging_dir = silver_dir / f'.c01s-{run_id}'
    backup_dir = silver_dir / f'.c01b-{run_id}'

    # 定义 Hive 分区规则
    calendar_partitioning = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(field_name) for field_name in PARTITION_COLUMNS]), flavor='hive')

    touched_partitions_df = new_calendar_df[PARTITION_COLUMNS].drop_duplicates() # 本批新增数据触达的分区键组合
    touched_years = touched_partitions_df.year.astype(int).tolist()  # 分区键组合年份提取
    silver_dir.mkdir(parents=True, exist_ok=True) # 确保目标湖的 silver 目录存在


    # 读取触达年份的旧行，保证局部日期更新保留同一年内的其他日期。
    if calendar_path.is_dir() and next(calendar_path.rglob('*.parquet'), None) is not None:


        existing_calendar_dataset = ds.dataset(calendar_path, format='parquet', partitioning=calendar_partitioning) # 创建数据目录的视图 Dataset 对象
        existing_calendar_schema = pa.schema([existing_calendar_dataset.schema.field(field_name) for field_name in TRADE_CALENDAR_SCHEMA.names], metadata=existing_calendar_dataset.schema.metadata) # Dataset 推导出的逻辑 Schema

        validate_compatible_dataset_schema(
            existing_calendar_schema, TRADE_CALENDAR_SCHEMA, '现有正式数据集 '
        )
        validate_dataset_fragment_schemas(
            existing_calendar_dataset, TRADE_CALENDAR_SCHEMA, PARTITION_COLUMNS, '现有正式数据集 '
        )

        # 已提交历史的业务规则由生产者保证；定向读取本批触达年份用于合并。
        touched_existing_calendar_table = validate_arrow_table(
            existing_calendar_dataset.to_table(
                columns=TRADE_CALENDAR_SCHEMA.names,
                filter=ds.field('year').isin(touched_years),
            ),
            TRADE_CALENDAR_SCHEMA,
        )
        touched_existing_calendar_df = touched_existing_calendar_table.to_pandas()


        # 旧行在前、新行在后，按主键保留最后一行，使本批修订覆盖旧值。
        merged_calendar_df = pd.concat([touched_existing_calendar_df, new_calendar_table.to_pandas()], ignore_index=True)

        merged_calendar_df = merged_calendar_df.drop_duplicates(PRIMARY_KEY, keep='last').sort_values('calendar_date').reset_index(drop=True)

        # 重新转换为 Arrow Table，并再次执行契约和业务检查，得到触达年份的替换数据。
        replacement_calendar_table = validate_calendar_table(
            pandas_to_arrow(merged_calendar_df.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA),
            require_contiguous=False,
        )

    else:
    # 正式数据集不存在时，没有旧年份数据需要保留。
        replacement_calendar_table = new_calendar_table  # 直接把本批新数据作为待提交 Arrow Table。


    # 接下来整个逻辑块会先完整写入并复读 staging，任何检查失败都不会触碰正式分区
    # 共享事务记录实际移动；安装或正式验收失败时倒序回滚。
    click.echo(
        f'partition_plan: table={TABLE_NAME}; phase=merge_validate; status=completed; '
        f'rows={len(new_calendar_table)}; replacement_rows={len(replacement_calendar_table)}; '
        f'partitions={len(touched_partitions_df)}; years={touched_years}; run_id={run_id}'
    )
    with StagedPathTransaction(
        root_path=calendar_path, staging_dir=staging_dir, backup_dir=backup_dir,
        log_context=f'table={TABLE_NAME}; run_id={run_id}',
    ) as transaction:

        # 第一阶段：只向本批独立 staging 目录写入 Parquet。
        click.echo(f'planning_progress: table={TABLE_NAME}; phase=staging_write; status=started; run_id={run_id}')
        ds.write_dataset(
            replacement_calendar_table, staging_dir, format='parquet', partitioning=calendar_partitioning,
            existing_data_behavior='delete_matching', # 如果写入过程中遇到相同的分区目录，使用当前待写数据替换 staging 中对应内容。
            basename_template='part-{i}.parquet' # 每个 Parquet 数据文件依次命名为：part-0.parquet、part-1.parquet……
        )

        # 把刚写出的 staging 重新打开为 PyArrow Dataset
        click.echo(f'planning_progress: table={TABLE_NAME}; phase=staging_readback; status=started; run_id={run_id}')
        staged_calendar_dataset = ds.dataset(staging_dir, format='parquet', partitioning=calendar_partitioning)
        staged_calendar_schema = pa.schema([staged_calendar_dataset.schema.field(field_name) for field_name in TRADE_CALENDAR_SCHEMA.names], metadata=staged_calendar_dataset.schema.metadata)
        # 从 staging Dataset 重建完整逻辑 Schema：
        # Parquet 文件内字段 + Hive 目录恢复出的分区字段

        validate_compatible_dataset_schema(
            staged_calendar_schema, TRADE_CALENDAR_SCHEMA, 'staging '
        )
        validate_dataset_fragment_schemas(
            staged_calendar_dataset, TRADE_CALENDAR_SCHEMA, PARTITION_COLUMNS, 'staging '
        )

        # staging 只复读物理契约、主键和行数；业务规则已经在构造完整替换分区时校验。
        staged_calendar_table = validate_arrow_table(
            staged_calendar_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
            TRADE_CALENDAR_SCHEMA,
        )
        if staged_calendar_table.to_pandas().duplicated(PRIMARY_KEY).any():
            raise ValueError('staging 主键不唯一。')
        if len(staged_calendar_table) != len(replacement_calendar_table): # 确认写入和复读过程没有丢行或额外产生行
            raise ValueError('staging 行数检查失败。')
        click.echo(
            f'planning_progress: table={TABLE_NAME}; phase=staging_readback; status=completed; '
            f'rows={len(staged_calendar_table)}; run_id={run_id}'
        )

        # staging 已经通过复读检查，开始进入正式分区替换阶段
        calendar_path.mkdir(parents=True, exist_ok=True) # 确保正式数据集根目录存在


        expected_partition_schema = pa.schema(
            [
                TRADE_CALENDAR_SCHEMA.field(name)
                for name in TRADE_CALENDAR_SCHEMA.names
                if name not in PARTITION_COLUMNS
            ],
            metadata=TRADE_CALENDAR_SCHEMA.metadata,
        )
        physical_primary_key_columns = [name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS]
        partition_count = len(touched_partitions_df)

        # touched_partitions_df 的每一行是一个完整分区键组合。
        for partition_index, partition_values in enumerate(touched_partitions_df.itertuples(index=False, name=None), start=1):
            partition_started_at = perf_counter()

            # 将分区字段名和值组合成 Hive 相对目录
            partition_relative_path = pathlib.Path(*[f'{partition_column}={partition_value}' for partition_column, partition_value in zip(PARTITION_COLUMNS, partition_values, strict=True)])  # 分区列和值必须等长

            staged_partition_path = staging_dir / partition_relative_path
            formal_partition_path = calendar_path / partition_relative_path
            click.echo(
                f'partition_start: table={TABLE_NAME}; partition={partition_relative_path}; '
                f'completed={partition_index - 1}; total={partition_count}; run_id={run_id}'
            )

            if not staged_partition_path.is_dir(): # staging 中必须存在本次计划提交的分区目录
                raise FileNotFoundError(f'staging 缺少 {partition_relative_path}。')

            staged_partition_row_count = ds.dataset(
                staged_partition_path, format='parquet'
            ).count_rows()

            # 如果正式分区已经存在，先把旧分区整体移动到 backup。
            # 共享事务登记实际移动，再将已验证的 staging 分区安装到正式位置。
            transaction.replace(target_path=formal_partition_path, staged_path=staged_partition_path)
            # 正式安装后只复读刚触达的叶：物理契约、身份 metadata、主键和行数。
            committed_partition_dataset = ds.dataset(
                formal_partition_path, format='parquet'
            )
            for fragment in committed_partition_dataset.get_fragments():
                validate_compatible_dataset_schema(
                    fragment.physical_schema, expected_partition_schema,
                    f'正式分区 {partition_relative_path} fragment {fragment.path} ',
                )
            committed_primary_key_table = committed_partition_dataset.to_table(
                columns=physical_primary_key_columns
            )
            if committed_primary_key_table.to_pandas().duplicated(
                physical_primary_key_columns
            ).any():
                raise ValueError(f'正式分区 {partition_relative_path} 主键不唯一。')
            committed_partition_row_count = len(committed_primary_key_table)
            if committed_partition_row_count != staged_partition_row_count:
                raise ValueError(f'正式分区 {partition_relative_path} 行数检查失败。')
            click.echo(
                f'partition_committed: table={TABLE_NAME}; partition={partition_relative_path}; '
                f'completed={partition_index}; total={partition_count}; rows={committed_partition_row_count}; '
                f'elapsed_s={perf_counter() - partition_started_at:.3f}; run_id={run_id}'
            )


    click.echo(
        f'committed: table={TABLE_NAME}; status=completed; rows={len(new_calendar_table)}; '
        f'partitions={partition_count}; elapsed_s={perf_counter() - commit_started_at:.3f}; run_id={run_id}'
    )
    return len(new_calendar_table)# 返回本次调用传入并通过校验的新数据行数


# ## 来源采集：生成逐自然日日历
# 
# `collect(start, end)` 处理包含起止日的自然日闭区间。函数先检查日期顺序，再通过共享连接模块认证 JQData，一次调用 `get_trade_days()` 获取交易日集合，并拒绝请求范围之外的返回日期。
# 
# 以该闭区间的全部自然日生成记录，按交易日集合设置 `is_trading_day`，由日期派生日期键、星期、周末标记和年份，补充固定来源、日历名称、时区、生效时刻及本批共用的 UTC 更新时间。结果通过 Arrow 契约转换与连续区间业务校验后，以 Pandas DataFrame 返回。
# 
# 函数日志依次报告认证、请求、构建校验和采集完成；完成时给出自然日数、交易日数及耗时。API 是一次同步请求，请求期间没有逐行进度。
# 
# 设置 `LATITUDE_B01_PREVIEW_PATH` 时，函数还将已校验结果保存为指定路径的 Parquet 预览。操作台用该路径保存本批运行证据，独立 CLI 或 Notebook 未设置该变量时不产生预览文件。预览可在只读模式生成，不代表 silver 已提交；预览保存发生文件系统错误时记录 warning，采集流程仍可返回结果。

# ### 局部流程：请求交易日并生成自然日历
# 
# 输入为自然日闭区间，输出为按权威 Schema 列序组织、通过业务校验的 DataFrame。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["检查起止顺序；记录采集开始"] --> B["生成本批 UTC 更新时间；认证 JQData"]
#     B --> C["一次 get_trade_days 请求闭区间交易日集合"]
#     C --> D["确认返回交易日均位于请求范围"]
#     D --> E["生成全部自然日，按集合标记是否交易日"]
#     E --> F["补充日期派生字段、固定值与更新时间"]
#     F --> G["pandas_to_arrow：字段、类型和非空校验"]
#     G --> H["validate_calendar_table：完整区间业务校验"]
#     H --> I{"配置了预览路径？"}
#     I -->|是| J["保存运行预览；文件系统错误记录 warning"]
#     I -->|否| K["记录行数、交易日数与耗时；返回 DataFrame"]
#     J --> K
# ```

# In[6]:


def collect(start: date, end: date) -> pd.DataFrame:
    """采集指定自然日闭区间并生成完整中国期货交易日历。

    Parameters
    ----------
    start : datetime.date
        本次采集范围的起始自然日，包含该日。
    end : datetime.date
        本次采集范围的结束自然日，包含该日。

    Returns
    -------
    pd.DataFrame
        按权威 Schema 生成并通过完整业务校验的逐自然日结果。
    """

    collection_started_at = perf_counter()
    click.echo(
        f'planning_progress: table={TABLE_NAME}; phase=collect; status=started; '
        f'start_date={start}; end_date={end}'
    )
    if start > end:
        raise ValueError('起始日期不得晚于结束日期。')

    # 同一批次的所有行共享一个 UTC 更新时间。
    batch_updated_at = pd.Timestamp.now(tz='UTC')
    from config.jqdata_connection import authenticate_jqdata

    # JQData 只提供交易日集合，全部自然日仍由本工作流生成。
    click.echo(f'planning_progress: table={TABLE_NAME}; phase=authenticate; status=started')
    jqdata_client = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    click.echo(
        f'planning_progress: table={TABLE_NAME}; phase=authenticate; status=completed; '
        f'elapsed_s={perf_counter() - collection_started_at:.3f}'
    )
    request_started_at = perf_counter()
    click.echo(
        f'request_batch: table={TABLE_NAME}; phase=trade_days; status=started; '
        f'start_date={start}; end_date={end}'
    )
    trading_dates = {pd.Timestamp(calendar_date).date() for calendar_date in jqdata_client.get_trade_days(start_date=start, end_date=end)}
    if any(calendar_date < start or calendar_date > end for calendar_date in trading_dates):
        raise ValueError('JQData 返回了请求范围之外的交易日。')
    click.echo(
        f'api_result: table={TABLE_NAME}; phase=trade_days; status=completed; '
        f'trading_day_count={len(trading_dates)}; elapsed_s={perf_counter() - request_started_at:.3f}'
    )
    click.echo(f'planning_progress: table={TABLE_NAME}; phase=build_validate; status=started')

    # 逐自然日生成 11 个契约字段，休市日也必须保留一行。
    calendar_df = pd.DataFrame({'calendar_date': pd.date_range(start, end, freq='D').date})
    calendar_df['date_key'] = calendar_df.calendar_date.map(lambda calendar_date: calendar_date.strftime('%Y%m%d'))
    calendar_df['is_trading_day'] = calendar_df.calendar_date.isin(trading_dates)
    calendar_df['weekday'] = calendar_df.calendar_date.map(lambda calendar_date: calendar_date.weekday() + 1)
    calendar_df['is_weekend'] = calendar_df.weekday.isin([6, 7])
    calendar_df['source'] = 'JQData_get_trade_days'
    calendar_df['calendar_name'] = 'CN_FUTURES_MARKET'
    calendar_df['calendar_timezone'] = 'Asia/Shanghai'
    calendar_df['effective_after'] = EFFECTIVE_AFTER
    calendar_df['updated_at'] = batch_updated_at
    calendar_df['year'] = pd.to_datetime(calendar_df.calendar_date).dt.year
    calendar_df = calendar_df[TRADE_CALENDAR_SCHEMA.names]

    # 即使本次不写湖，也先验证 API 响应转换后的完整业务契约。
    validated_calendar_table = validate_calendar_table(pandas_to_arrow(calendar_df, TRADE_CALENDAR_SCHEMA))
    # 总控台只在本批 run_history 中指定此路径；结果预览不等于正式提交。
    # 独立运行 / Notebook 未设置该变量时不产生额外文件。
    import os
    preview_path_text = os.environ.get('LATITUDE_B01_PREVIEW_PATH')
    if preview_path_text:
        import pyarrow.parquet as pq
        preview_path = pathlib.Path(preview_path_text)
        temporary_preview_path = preview_path.with_suffix('.parquet.tmp')
        try:
            preview_path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_preview_path.open('wb') as preview_stream:
                pq.write_table(validated_calendar_table, preview_stream)
                preview_stream.flush()
                os.fsync(preview_stream.fileno())
            os.replace(temporary_preview_path, preview_path)
        except OSError as preview_error:
            # 展示失败不能改变采集和正式事务的结果；日志保留可核查原因。
            click.echo(f'WARNING: 日历结果预览未保存；{type(preview_error).__name__}: {preview_error}')
    click.echo(
        f'planning_progress: table={TABLE_NAME}; phase=collect; status=completed; '
        f'rows={len(calendar_df)}; trading_day_count={int(calendar_df["is_trading_day"].sum())}; '
        f'elapsed_s={perf_counter() - collection_started_at:.3f}'
    )
    return calendar_df


# ## 运行计划：选择模式与待提交行
# 
# `main()` 解析命令行参数，确定目标湖、配置起点和有效截止日，再按照开篇模式表执行。日期参数必须成对，`--full` 与显式日期互斥，配置起点不得晚于有效截止日；显式日期写入还须通过非正式湖门禁。
# 
# 默认模式读取已有表的物理契约和最大自然日，选择尾部新增范围。全历史模式先生成完整来源日历，再校验已有表，按日期比较除 `updated_at` 外的所有字段，选出缺失或修订行。显式日期模式直接使用指定闭区间的采集结果。待提交行确定后，`--write` 决定是否调用分区提交函数。

# ### 局部流程：运行模式与待提交范围
# 
# 本图展开 `main()` 的三条分支。默认模式无新增时不认证或请求 API；全历史模式须取得来源结果后才能判断有无差异。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A0["解析目标湖"] --> A["校验日期成对、模式互斥及写入门禁"]
#     A --> B["读取配置起点，确定北京时间有效截止日；校验起点"]
#     B --> M{"运行模式"}
#     M -->|默认尾部| D["检查已有物理契约，读取最大自然日"]
#     D --> E["空湖从配置起点开始；已有表从最大自然日加一天开始"]
#     E --> F{"起点不晚于有效截止日？"}
#     F -->|否| Z["报告 up_to_date；API 调用为 0；返回"]
#     F -->|是| G["collect：采集尾部日期"]
#     M -->|显式日期| H["校验闭区间顺序及有效截止限制；collect"]
#     M -->|全历史| I["collect：配置起点至有效截止日"]
#     I --> J["读取已有日历并校验；日期超出有效范围则报错"]
#     J --> K["按日期比较除 updated_at 外的所有字段"]
#     K --> L{"存在缺失或修订日期？"}
#     L -->|否| N["报告 up_to_date；返回"]
#     L -->|是| P["从来源结果筛选差异行"]
#     G --> W{"启用 --write？"}
#     H --> W
#     P --> W
#     W -->|否| Q["只读校验完成，返回调用方"]
#     W -->|是| T["commit_partitions：提交待办并返回"]
# ```

# In[7]:


# 默认只追加正式表最大自然日之后的有效尾部；--full 才执行全历史来源比较。
@click.command()
@click.option('--lake-root', type=click.Path(path_type=pathlib.Path))
@click.option('--start-date', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--end-date', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--full', 'full_refresh', is_flag=True)
@click.option('--write', is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full_refresh: bool,
    write: bool,
) -> None:
    """按尾部更新、全历史比较或显式日期模式运行交易日历入口。

    Parameters
    ----------
    lake_root : pathlib.Path or None
        可选数据湖根目录；未提供时使用 `.env` 中的正式湖配置。
    start_date : datetime.datetime or None
        可选显式起始日，必须与 `end_date` 同时提供。
    end_date : datetime.datetime or None
        可选显式结束日，必须与 `start_date` 同时提供。
    full_refresh : bool
        是否比较配置起点至有效截止日的完整来源日历。
    write : bool
        是否提交结果；显式日期不得与正式湖写入同时使用。

    Returns
    -------
    None
        运行计划和提交结果写入标准输出。
    """

    # 命令行参数未覆盖 --lake-root 时使用 .env 中唯一的正式湖；显式路径主要供临时湖测试。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    # 任一日期参数出现即进入显式日期门禁，随后检查两者是否成对。
    has_explicit_dates = start_date is not None or end_date is not None

    if (start_date is None) != (end_date is None):
        raise click.UsageError('--start-date 与 --end-date 必须同时提供。')
    if full_refresh and has_explicit_dates:
        raise click.UsageError('--full 与 --start-date/--end-date 不能同时提供。')
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            '显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；'
            '请移除日期参数使用自动补缺，或改用非正式测试湖。'
        )

    # 空湖尾部更新与全历史比较使用配置起点；有效截止日按北京时间 20:00 确定。
    configured_start_date = settings.futures_data_start_date
    now_shanghai = datetime.now(ZoneInfo('Asia/Shanghai')) # 获取当前上海时区的日期和时刻
    valid_end_date = now_shanghai.date() if now_shanghai.time() >= EFFECTIVE_AFTER else now_shanghai.date() - timedelta(days=1)
    # EFFECTIVE_AFTER 北京时间 20:00 前，当日尚未进入有效水位

    if configured_start_date > valid_end_date:
        raise ValueError(
            f'配置起点 {configured_start_date} 晚于当前有效截止日 {valid_end_date}。'
        )


    log_boundary = '=' * 88
    run_mode = 'explicit' if has_explicit_dates else ('full' if full_refresh else 'automatic_tail')
    click.echo(
        f'{log_boundary}\n'
        '运行入口开始 / Run entry started\n'
        f'planning_progress: function=main(); table={TABLE_NAME}; phase=run; status=started; mode={run_mode}; '
        f'write={str(write).lower()}; lake_root={resolved_lake_root}; '
        f'valid_start={configured_start_date}; valid_end={valid_end_date}\n'
        f'{log_boundary}\n'
    )

    # 显式日期用于定向只读检查；启用写入时，目标必须是非正式湖。
    if has_explicit_dates:

        # Click DateTime 返回 datetime
        # 交易日历使用 date，因此去掉时分秒
        requested_start_date = start_date.date()
        requested_end_date = end_date.date()

        if requested_start_date > requested_end_date:
            raise click.BadParameter('起始日期不得晚于结束日期。')
        if requested_end_date > valid_end_date:
            raise click.BadParameter(
                f'结束日期不得晚于当前有效截止日 {valid_end_date}（北京时间 20:00 生效）。',
                param_hint='--end-date',
            )

        # 调用 JQData 获取请求区间内的交易日，同时生成完整自然日日历并执行契约校验
        requested_calendar_df = collect(requested_start_date, requested_end_date)
        if write:
            commit_partitions(requested_calendar_df, resolved_lake_root)
        else:
            click.echo(
                f'{log_boundary}\n'
                '显式日期只读运行完成 / Explicit-date dry run completed\n'
                f'planning_progress: function=main(); table={TABLE_NAME}; phase=run; status=completed; write=false; validated_rows={len(requested_calendar_df)}\n'
                f'{log_boundary}'
            )
        # 显式区间处理完成，返回调用方。
        return

    # 无显式日期时，默认从最大自然日之后追加；--full 请求完整历史并比较差异。
    calendar_path = resolved_lake_root / 'silver' / TABLE_NAME
    calendar_partitioning = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(field_name) for field_name in PARTITION_COLUMNS]), flavor='hive')

    if not full_refresh:
        latest_calendar_date = None
        if calendar_path.is_dir() and next(calendar_path.rglob('*.parquet'), None) is not None:
            existing_calendar_dataset = ds.dataset(
                calendar_path,
                format='parquet',
                partitioning=calendar_partitioning,
            )
            existing_calendar_schema = pa.schema(
                [
                    existing_calendar_dataset.schema.field(field_name)
                    for field_name in TRADE_CALENDAR_SCHEMA.names
                ],
                metadata=existing_calendar_dataset.schema.metadata,
            )
            validate_compatible_dataset_schema(
                existing_calendar_schema, TRADE_CALENDAR_SCHEMA, '现有正式数据集 '
            )
            validate_dataset_fragment_schemas(
                existing_calendar_dataset, TRADE_CALENDAR_SCHEMA, PARTITION_COLUMNS, '现有正式数据集 '
            )
            existing_calendar_dates = existing_calendar_dataset.to_table(
                columns=['calendar_date'],
            ).column('calendar_date').to_pylist()
            latest_calendar_date = max(existing_calendar_dates, default=None)

        pending_start_date = (
            configured_start_date
            if latest_calendar_date is None
            else latest_calendar_date + timedelta(days=1)
        )
        if pending_start_date > valid_end_date:
            click.echo(
                f'up_to_date: table={TABLE_NAME}; mode=automatic_tail; '
                f'latest_calendar_date={latest_calendar_date}; api_calls=0'
            )
            return

        pending_calendar_df = collect(pending_start_date, valid_end_date)
        click.echo(
            f'auto_plan: table={TABLE_NAME}; mode=automatic_tail; '
            f'start_date={pending_start_date}; end_date={valid_end_date}; '
            f'rows={len(pending_calendar_df)}; '
            f'trading_day_count={int(pending_calendar_df["is_trading_day"].sum())}'
        )
        if write:
            commit_partitions(
                pending_calendar_df,
                resolved_lake_root,
            )
        return

    # 全历史来源日历用于同时识别缺失日期与已有业务值的修订。
    expected_calendar_df = collect(configured_start_date, valid_end_date)
    # 日期元素为 datetime.date；类型标注供后续日期集合和字典键复用。
    valid_calendar_dates: list[date] = expected_calendar_df['calendar_date'].tolist()

    valid_calendar_date_set: set[date] = set(valid_calendar_dates)  # 集合用于快速执行日期差集。
    existing_calendar_df = expected_calendar_df.iloc[0:0].copy()
    # 空湖以同列同类型的空表参与比较，所有来源日期自然进入待办。
    if calendar_path.is_dir() and next(calendar_path.rglob('*.parquet'), None) is not None:

        # 建立现有 Parquet Dataset 的逻辑视图
        existing_calendar_dataset = ds.dataset(calendar_path, format='parquet', partitioning=calendar_partitioning)
        existing_calendar_schema = pa.schema([existing_calendar_dataset.schema.field(field_name) for field_name in TRADE_CALENDAR_SCHEMA.names], metadata=existing_calendar_dataset.schema.metadata) # 按权威字段顺序重建现有 Dataset 的逻辑 Schema，
        # 同时保留现有 Dataset 的表级 metadata

        validate_compatible_dataset_schema(
            existing_calendar_schema, TRADE_CALENDAR_SCHEMA, '现有正式数据集 '
        )
        validate_dataset_fragment_schemas(
            existing_calendar_dataset, TRADE_CALENDAR_SCHEMA, PARTITION_COLUMNS, '现有正式数据集 '
        )

        # 将现有 Dataset 读取为 Arrow Table (.to_table)，并执行主键、派生字段、固定值和审计时间检查
        existing_calendar_table = validate_calendar_table(
            validate_arrow_table(
                existing_calendar_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
                TRADE_CALENDAR_SCHEMA,
            ),
            require_contiguous=False,
        )
        existing_calendar_df = existing_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
        existing_calendar_date_set = set(existing_calendar_df['calendar_date'].tolist()) # 转成集合，供后面快速执行集合差运算

        # 检查下游是否含有当前有效水位之外的日期
        out_of_watermark_calendar_dates = sorted(existing_calendar_date_set - valid_calendar_date_set) # 只存在于下游、但不属于当前有效范围的日期
        if out_of_watermark_calendar_dates:
            raise ValueError(
                '现有交易日历包含配置起点之前或有效截止日之后的日期：'
                f'{out_of_watermark_calendar_dates[:10]}'
            )

    click.echo(
        f'{log_boundary}\n'
        '现有正式数据检查完成 / Existing formal dataset inspection completed\n'
        f'planning_progress: function=main() -> ds.dataset() -> validate_calendar_table(); '
        f'table={TABLE_NAME}; phase=existing_dataset; status=completed; '
        f'dataset_exists={str(calendar_path.is_dir()).lower()}; existing_rows={len(existing_calendar_df)}\n'
        f'{log_boundary}'
    )

    # updated_at 只是生成审计时间；业务内容没有变化时不应仅因运行时间不同而重写该行。
    comparison_columns: list[str] = [
        field_name for field_name in TRADE_CALENDAR_SCHEMA.names if field_name != 'updated_at'
    ]

    # 每个日期映射到除 updated_at 外的完整行签名；两个字典使用相同的 date 键和 tuple 值类型。
    expected_calendar_signature_by_date: dict[date, tuple[object, ...]] = {
        calendar_row['calendar_date']: tuple(calendar_row[field_name] for field_name in comparison_columns)
        for calendar_row in expected_calendar_df.to_dict('records')
    }
    existing_calendar_signature_by_date: dict[date, tuple[object, ...]] = {
        calendar_row['calendar_date']: tuple(calendar_row[field_name] for field_name in comparison_columns)
        for calendar_row in existing_calendar_df.to_dict('records')
    }

    # 下游缺少日期时 dict.get() 返回 None；None 与期望 tuple 不相等，该日期自然进入待补集合。
    # 下游已有日期时则逐项比较行签名，从而同时发现缺失日期和历史状态修订。
    incomplete_calendar_dates = [
        calendar_date
        for calendar_date in valid_calendar_dates
        if existing_calendar_signature_by_date.get(calendar_date)
        != expected_calendar_signature_by_date[calendar_date]
    ]
    if not incomplete_calendar_dates:
        click.echo(
            f'{log_boundary}\n'
            '正式交易日历已是最新 / Formal trade calendar is up to date\n'
            f'up_to_date: function=main(); table={TABLE_NAME}; mode=full; valid_end_date={valid_end_date}; '
            f'valid_grid_count={len(valid_calendar_dates)}\n'
            f'{log_boundary}'
        )
        return

    # 只提交缺失或交易状态已修订的日期；未变化行保留原 updated_at。
    pending_calendar_df = expected_calendar_df.loc[
        expected_calendar_df['calendar_date'].isin(incomplete_calendar_dates),
        TRADE_CALENDAR_SCHEMA.names,
    ].reset_index(drop=True)

    click.echo(
        f'{log_boundary}\n'
        '全历史差异计划已生成 / Full-history reconciliation plan created\n'
        f'reconciliation_plan: function=main(); table={TABLE_NAME}; valid_grid_count={len(valid_calendar_dates)}; '
        f'complete_grid_count={len(valid_calendar_dates) - len(incomplete_calendar_dates)}; '
        f'missing_or_revised_grid_count={len(pending_calendar_df)}\n'
        f'{log_boundary}'
    )

    if write:
        commit_partitions(pending_calendar_df, resolved_lake_root)
    else:
        click.echo(
            f'{log_boundary}\n'
            '全历史差异只读运行完成 / Full-history reconciliation dry run completed\n'
            f'planning_progress: function=main(); table={TABLE_NAME}; phase=run; status=completed; write=false; validated_rows={len(pending_calendar_df)}\n'
            f'{log_boundary}'
        )


# ## 执行入口：Notebook 与脚本
# 
# 运行下面的入口单元格会采集 **2026-08-01 至 2026-08-15** 的日历并校验，参数不含 `--write`，因此不提交日历分区。该范围是可修改的显式日期示例；切换模式前应核对开篇的日期与写入条件。
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免解析内核启动时的 `-f` 参数；`standalone_mode=False` 让调用完成后返回单元格。直接运行 `.py` 时读取命令行参数，导入同名模块时只加载定义。

# ### 局部流程：执行入口分派
# 
# Notebook 入口使用单元格内的显式参数，脚本入口使用终端参数，两者调用同一个 `main` 命令。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口代码"] --> B{"Notebook 交互环境？"}
#     B -->|是| C["读取 notebook_args 中的显式日期只读参数"]
#     C --> D["main.main：传入 args，standalone_mode=False"]
#     B -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main：读取命令行参数"]
#     E -->|否| G["模块导入完成，只加载定义"]
#     D --> H["进入 main 的运行模式分支"]
#     F --> H
# ```

# In[8]:


# Notebook 显式提供 args，避免 Click 将内核的 -f 启动参数当作业务参数。
if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 显式日期只读示例
    main.main(
        args=notebook_args,
        prog_name="c01_trade_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# ## 终端运行：尾部更新与历史维护
# 
# 在项目根目录使用 `latitude_env_v2` 解释器运行。下面依次给出环境预检、默认尾部只读检查、默认尾部写入和全历史差异写入命令；按任务选择一条业务命令执行。存在采集范围时，只读命令也会访问 JQData。
# 
# ```powershell
# cd E:\Latitude_Analytics_v2
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b00_01_verify_runtime.py
# 
# # 默认尾部只读检查
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c01_trade_calendar.py
# 
# # 默认尾部更新并写入正式湖
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --write
# 
# # 全历史比较并写入缺失或修订行
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c01_trade_calendar.py --full --write
# ```
# 
# ### 局部流程：终端手动运行
# 
# 命令需要在终端手动执行；下方代码单元格中的命令均为注释，运行该单元格不会启动采集。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["切换到项目根目录，使用 latitude_env_v2 解释器"] --> B["运行统一环境预检"]
#     B --> C["选择尾部更新、全历史比较或显式日期检查参数"]
#     C --> D["手动运行 c01_trade_calendar.py"]
#     D --> E["main 生成计划；按 --write 决定是否提交"]
# ```

# In[9]:


# 以下命令需在终端手动执行；本单元格仅保存注释。
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python R02_Market_Data\a01_Collection\b00_01_verify_runtime.py
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c01_trade_calendar.py --write

