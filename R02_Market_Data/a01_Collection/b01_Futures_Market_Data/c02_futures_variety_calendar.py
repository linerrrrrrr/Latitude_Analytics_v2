#!/usr/bin/env python
# coding: utf-8

# # 期货品种交易日历：生成与更新
# 
# 本 Notebook 生成 `dim_futures_variety_calendar`（期货品种交易日历维度表），为后续合约日历和交易所报告日历提供品种与交易日范围。每个“交易所—品种—交易日”保留一行，`active_contract_count` 表示该品种在当日上市区间内的固定月份合约数量；只有数量大于 0 时才生成记录。
# 
# 交易日来自 [c01 交易日历](c01_trade_calendar.ipynb) 已提交的 `dim_trade_calendar`，合约及上市区间来自 JQData 完整期货证券目录。日历保留完整固定月份合约范围，事实采集白名单不参与筛选。
# 
# 阅读时先通过全流程图了解上游选择、来源展开、更新计划和提交关系，再查看各环节的局部流程与代码。执行时使用 `latitude_env_v2` 内核，依次完成初始化、契约浏览和函数定义，最后运行入口单元格。函数定义只注册函数；入口按参数采集，`--write` 决定是否提交。同名 `.py` 由默认 PythonExporter 完整生成，供终端和批量调用。

# ## 全流程：从交易日与合约目录到品种日历
# 
# `main()` 选择运行模式并生成替换计划；`collect()` 读取或复用上游交易日，获取完整固定月份合约目录并逐日计数；`commit_partitions()` 按日期闭区间替换完整品种结果。默认模式无新增交易日时直接结束；全历史模式须生成来源结果并完成比较，才能判断有无差异。
# 
# 下图覆盖准备、三种运行模式、来源生成、差异选择、分区提交及失败恢复。矩形表示操作，菱形表示条件，实线表示正常流程，虚线表示异常转向。各环节的局部图展开对应操作；参数或校验失败会抛出异常并停止。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["定位项目、导入依赖；Notebook 浏览契约；注册函数"] --> B["执行入口：解析目标湖，校验日期、模式和写入边界"]
#     B --> C{"运行模式"}
#     C -->|默认尾部| D["检查已有品种日历；从 c01 选出晚于 c02 最大交易日的日期"]
#     D --> E{"存在新增交易日？"}
#     E -->|否| Z["报告无需更新，结束"]
#     E -->|是| F["将已选日期传给 collect"]
#     C -->|全历史| G["collect 选择目标湖中 c01 的全部交易日"]
#     C -->|显式日期| H["collect 选择指定闭区间内的上游交易日"]
#     F --> I{"选定交易日非空？"}
#     G --> I
#     H --> I
#     I -->|否| J["生成符合契约的空表，无认证和 API 请求"]
#     I -->|是| K["认证 JQData；一次请求完整目录；筛选固定月份合约并校验上市区间"]
#     K --> L["按上市闭区间逐日选择合约；按交易所与品种计数；转换为契约化结果"]
#     J --> M{"全历史模式？"}
#     L --> M
#     M -->|是| N["校验已有品种日历；拒绝预期集合外主键；比较主键与合约数"]
#     N --> O{"存在缺失或计数不同的日期？"}
#     O -->|否| Z
#     O -->|是| P["将相邻差异日期合为区间；每个日期取完整品种结果"]
#     M -->|否| Q["以尾部范围或指定范围规划一次替换"]
#     P --> W{"启用 --write？"}
#     Q --> W
#     W -->|否| R["报告只读计划与运行完成，结束"]
#     W -->|是| S["逐区间调用 commit_partitions：保留区间外旧行，合并完整月度分区并校验"]
#     S --> T["写 staging 并复读；通过后进入本区间的共享事务"]
#     T --> U["逐叶备份并安装或显式删除；正式复读；必要时安装零行契约标记"]
#     U --> V["本区间事务成功退出，清理临时目录并报告提交完成"]
#     V --> X{"还有差异区间？"}
#     X -->|是| S
#     X -->|否| Y["报告整次运行完成"]
#     T -.->|暂存写入或复读失败| A1["清理 staging，正式分区尚未替换；抛出异常"]
#     U -.->|事务内异常| B1["倒序隔离已安装新叶、恢复旧叶；新标记回退时移除"]
#     B1 --> C1["保留隔离证据及未恢复备份；清理 staging；抛出异常"]
# ```
# 
# 一次提交的回滚范围是当前日期区间触达的全部叶分区及本次新建标记。全历史维护中的多个区间分别提交，后一区间失败不会撤销此前成功区间。

# ## 更新范围与写入条件
# 
# 正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 配置，通过 `settings.futures_lake_root` 读取；未传 `--lake-root` 时使用正式湖。c02 的日期范围来自目标湖中 c01 已提交的交易日历，不自行按运行时刻生成交易日水位。
# 
# | 模式 | 处理范围与结果 | 启用 `--write` 的条件 |
# | --- | --- | --- |
# | 默认尾部更新 | 选择晚于目标品种日历最大交易日的上游交易日；空湖选择全部上游交易日。无新增时，在认证和 API 请求前结束 | 可写正式湖，以新增交易日的首尾日期作为替换区间 |
# | `--full` 全历史比较 | 展开全部上游交易日，比较已有主键与有效合约数，将差异日期组织为替换区间 | 可写正式湖；与显式日期互斥。已有表含预期集合外主键时停止并报错 |
# | `--start-date` 与 `--end-date` 成对显式日期 | 只选择指定自然日闭区间内、已经存在于上游的交易日；起始日不得晚于结束日 | 仅允许写入通过 `--lake-root` 指定的非正式湖 |
# 
# 默认尾部更新信任已提交历史，只以最大交易日选择新增范围；历史内部缺口和合约数量修订使用 `--full` 检查。全历史比较依据业务主键与 `active_contract_count`，更新时间变化本身不触发替换。一旦某个日期出现差异，提交使用该日全部品种的完整结果。
# 
# 显式日期也依赖目标湖中的上游交易日历：范围内没有上游交易日时返回空表；指定范围不会生成上游尚未提交的日期。非正式湖验证须先具备符合契约的上游表。不带 `--write` 时仍会执行所选模式需要的来源请求、转换和比较，结果留在内存中，不提交分区。
# 
# 完整字段定义见[数据契约中的 `FUTURES_VARIETY_CALENDAR_SCHEMA`](../../../config/data_contracts.py)；各入口的来源、更新与验收规则见[采集说明](../README.md)，silver 的类型和存储约定见[湖仓规则](../../a02_Lake/AGENTS.md)。

# ## 输入与方法：交易日、固定月份合约和上市区间
# 
# 上游 `dim_trade_calendar` 提供 `calendar_date` 与 `is_trading_day`，只选择 `is_trading_day=true` 的日期。c01 负责上游主键、自然日连续性和派生字段；c02 消费其已提交业务结论，检查物理契约后读取计算所需的两列。
# 
# 选定交易日非空时，一次调用 `get_all_securities(["futures"], date=None)` 获取完整历史期货证券目录。返回对象须为 Pandas DataFrame，索引作为合约代码，`start_date`、`end_date` 作为上市区间。例如 `IC1505.CCFX` 可解析为品种 `IC`、交割代码 `1505` 和交易所 `CCFX`。目录中的名称和类型描述不进入品种日历。
# 
# 合约代码统一大写后，保留符合固定月份格式的记录，并排除交割代码 `8888`、`9998`、`9999`。剩余代码必须唯一且集合非空；上市与结束日期须可解析、非空，并满足起始日不晚于结束日。对每个选定交易日，以 `start_date <= trading_date <= end_date` 选择合约，再按交易所和品种计数。上市区间两端均包含；当日没有有效合约的品种不产生记录。

# ## 输出与存储：逻辑表和月度叶分区
# 
# 目标表位于 `<lake_root>/silver/dim_futures_variety_calendar/`，粒度为“每个交易所—品种—交易日一行”，主键为 `exchange_code + underlying_code + trading_date`。Hive 分区顺序为 `exchange_code/year/month`，最内层月份目录称为叶分区。
# 
# 下面是一个分区路径示例，不表示该分区已经存在：
# 
# ```text
# dim_futures_variety_calendar/
#   exchange_code=CCFX/
#     year=2026/
#       month=8/
#         part-0.parquet
# ```
# 
# 单个 Parquet 文件保存 `underlying_code`、`trading_date`、`active_contract_count`、`source`、`updated_at` 五个非分区字段；`exchange_code`、`year`、`month` 从目录名恢复。使用权威分区 Schema 声明 Hive partitioning 后，Arrow Dataset 将文件字段与目录字段合为完整八列逻辑表。列序、类型、主键及分区定义均以权威 Schema 为准。
# 
# 本次合并结果为空且表内没有任何 Parquet 文件时，提交函数在表根安装零行 `schema.parquet`，保存非分区字段的物理契约；正式复读确认 Schema 与零行数后才算提交成功。通过同一 Hive partitioning 打开时，空表仍具有完整逻辑结构。零行标记表示表可读取，表内没有业务记录。

# ## 初始化：定位项目并导入依赖
# 
# 导入路径、代码解析、表格计算及 Arrow/Parquet 读写依赖，再从当前工作目录逐级向上查找同时包含 `.git`、`.env`、`config/settings.py` 的项目根目录。找到后，将项目根与采集支撑模块目录加入导入路径，读取 `settings`、两张具名权威 Schema 和共享路径事务模块；找不到项目根时停止初始化。
# 
# 本环节准备后续函数所需依赖，JQData 认证由 `collect()` 在存在选定交易日时执行。

# ### 局部流程：初始化
# 
# 输入是 Notebook 或脚本的当前工作目录；完成后，上游与目标表的契约、配置和共享事务模块可供后续单元格使用。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["导入标准库与表格、Arrow、Parquet 依赖"] --> B["以当前工作目录为候选起点"]
#     B --> C{"候选目录同时包含三个项目标记？"}
#     C -->|否，仍有父目录| D["移动到上一级候选目录"]
#     D --> C
#     C -->|否，查找已结束| X["抛出未找到项目根目录异常"]
#     C -->|是| E["将项目根加入模块搜索路径"]
#     E --> F["导入 settings、交易日历与品种日历 Schema、StagedPathTransaction"]
# ```

# In[1]:


from __future__ import annotations

# Python 标准库：路径定位、合约代码解析、目录替换、导入路径和提交批次标识。
import pathlib
import re
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from time import perf_counter

# Click 解析命令行参数，Pandas 计算表格，Arrow/Parquet 处理类型契约、分区和零行标记。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Notebook 可能从项目内任意目录启动，因此从当前工作目录逐级向上寻找项目根目录。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")


from config.settings import settings
from config.data_contracts import (
    FUTURES_VARIETY_CALENDAR_SCHEMA,   # dim_futures_variety_calendar 期货品种交易日历维度表
    TRADE_CALENDAR_SCHEMA,   # dim_trade_calendar 中国期货交易日历维度表
    pandas_to_arrow,
    validate_arrow_table,
)
# 两张具名 Schema 分别约束当前产出和直接上游；
# 转换函数统一 Arrow/Pandas 边界
from R02_Market_Data.a01_Collection.b00_04_staged_path_transaction import StagedPathTransaction


# ## 契约浏览：查看上游与产出字段
# 
# Notebook 中的 `display_schema_metadata()` 按上游在前、产出在后的顺序展示 `TRADE_CALENDAR_SCHEMA` 与 `FUTURES_VARIETY_CALENDAR_SCHEMA`。可查看两张表的用途、粒度、字段和 metadata（契约附加说明），并展开完整表说明与单字段详情。
# 
# 数据样例从 `settings.futures_lake_root` 中的已有分区按筛选有界读取，表头附带权威字段 metadata 中的中文含义；计算和落盘字段保持英文名称。样例未命中只说明所选读取范围没有结果，读取范围与上限见[Schema 浏览器说明](../README.md#notebook-开篇-schema-契约呈现)。本环节只读契约和已有数据，导出脚本跳过交互展示。

# ### 局部流程：契约与样例浏览
# 
# 先查看上游交易日历，再查看本环节品种日历；所选表决定样例读取范围。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"Notebook 交互环境？"} -->|是| B["按上游、产出顺序展示两张权威 Schema"]
#     B --> C["选择表与分区，读取已有数据的有界样例"]
#     C --> D["展示中文表头与读取结果提示"]
#     A -->|否| E["跳过交互展示，进入后续定义"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from R02_Market_Data.a01_Collection.b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA, # dim_trade_calendar 中国期货交易日历维度表
        FUTURES_VARIETY_CALENDAR_SCHEMA, # dim_futures_variety_calendar 期货品种交易日历维度表
    ], lake_root=settings.futures_lake_root)


# ## 表配置与物理契约检查
# 
# 目标品种日历的表名、主键及 Hive 分区顺序，上游交易日历的表名及分区顺序，均在初始化时从各自权威 Schema 的 metadata 读取一次，供路径构造、分区读取和校验复用。
# 
# `validate_compatible_dataset_schema()` 比较字段名称及顺序、Arrow 类型、是否允许为空，以及 `table_name`、`primary_key`、`partition_columns` 三项表身份 metadata。描述性 metadata 的差异不阻断读取，说明以代码中的权威 Schema 为准。
# 
# 逻辑 Dataset 的 Schema 包含由 Hive 目录恢复的分区字段，单个 Parquet 文件的物理 Schema 则只包含非分区字段。`validate_dataset_fragment_schemas()` 据此逐文件检查，避免只看逻辑 Schema 而漏掉其他文件的类型或结构差异。上游业务结论由 c01 的提交证明负责；本环节对自身来源结果及待替换的完整分区承担业务校验。

# ### 局部流程：读取表配置
# 
# 配置从各表权威 Schema 的 metadata 读取，后续代码复用读取结果。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["FUTURES_VARIETY_CALENDAR_SCHEMA"] --> B["读取目标表名、主键、分区字段"]
#     C["TRADE_CALENDAR_SCHEMA"] --> D["读取上游表名、分区字段"]
#     B --> E["用于路径、Hive 分区读写和契约检查"]
#     D --> E
# ```

# In[3]:


# 表名、Hive 分区顺序和业务主键只在权威 Schema metadata 中定义。
# 本模块初始化时各读取一次；后续路径、分区读写和唯一性检查只复用这些结果。
TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
PHYSICAL_METADATA_KEYS = (b"table_name", b"primary_key", b"partition_columns")
TRADE_TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
TRADE_PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")


# ### 局部流程：物理契约检查
# 
# 本单元格定义检查函数；下图描述函数被调用时的行为。任一字段或表身份不匹配均抛出异常。
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

# In[4]:


def validate_compatible_dataset_schema(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
    context: str,
) -> None:
    """检查字段、类型、nullable 与表身份 metadata；描述性说明以权威 Schema 为准。"""
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


# ## 合约识别：固定月份代码与排除集合
# 
# `FIXED_CONTRACT` 要求代码完整符合“品种字母 + 3 或 4 位交割代码 + 点号 + 交易所字母后缀”的格式，并提取 `underlying_code`、`delivery_code`、`exchange_code`。`CONTINUOUS_DELIVERY_CODES` 定义须排除的 `8888`、`9998`、`9999`。
# 
# 这两个定义在 `collect()` 中用于筛选来源目录。无法匹配格式的记录被剔除，匹配成功但命中排除集合的记录也被剔除；剩余固定月份合约进入代码唯一性和上市区间校验。该筛选依据代码与上市区间，不依据期货事实采集白名单。

# ### 局部流程：固定月份合约识别
# 
# 本单元格定义正则与排除集合；下图描述 `collect()` 使用这些定义的顺序。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["来源合约代码转为字符串并统一大写"] --> B["正则提取品种、3 或 4 位交割代码和交易所"]
#     B --> C{"格式完整匹配？"}
#     C -->|否| X["剔除该记录"]
#     C -->|是| D{"交割代码命中 8888、9998、9999？"}
#     D -->|是| X
#     D -->|否| E["保留固定月份合约，进入目录校验"]
# ```

# In[5]:


# 固定月份合约代码由“品种字母 + 3/4 位交割代码 + 交易所后缀”组成。
# ^ 和 $ 要求整个代码完全匹配；三个命名捕获组可直接转成后续业务字段。
FIXED_CONTRACT = re.compile(r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})\.(?P<exchange_code>[A-Z]+)$")

# 这些交割代码代表连续或指数序列，没有独立上市区间，不能生成固定合约日历格点。
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})


# ## 分区提交：日期区间替换与失败恢复
# 
# `commit_partitions()` 接收指定自然日闭区间内的完整品种结果。对每个触达的 `exchange_code/year/month` 叶分区，先保留区间外的旧行，再加入区间内的本批结果。区间内旧行不因本批缺少同键记录而保留，因此调用方必须传入区间内全部品种结果。空输入也具有替换语义：它可能清退范围内旧行，不能直接视为无需处理。
# 
# 触达集合由本批数据与已有目录中的交易所共同确定，并覆盖请求范围跨越的每个自然年月。输入先通过 Arrow 契约转换，并检查主键唯一、日期位于请求区间、合约数大于 0、固定来源值及年月可由交易日复算；合并后的完整替换叶再检查主键、合约数、来源和年月。随后写入 staging（本批临时目录）并复读物理契约、主键唯一性与总行数。
# 
# staging 通过后，当前调用的全部叶分区进入同一个 `StagedPathTransaction` 事务。合并结果非空的叶安装新分区，结果为零行的叶显式删除；应有数据却缺少 staging 分区时停止并报错。正式复读逐文件检查物理契约，核对主键唯一性及每个叶的预期行数。整表没有 Parquet 且本次合并结果为空时，零行契约标记在同一事务中安装并正式复读。
# 
# 安装或正式验收失败时，按实际移动倒序隔离已安装的新叶并恢复旧叶；本次新建的零行标记回退时移除。一处恢复失败后仍尝试其余目标。staging 在退出时清理，成功提交或完整回滚后清理备份；回滚不完整则保留备份并抛出异常，失败新叶的隔离证据保留供核查。路径替换使用同一文件系统内的移动，事务不提供多叶同时原子可见、进程终止后的自动恢复或并发写入协调。
# 
# 日志中的 `rows` 与函数返回值均为本批输入行数，`replacement_rows` 包含范围外保留的旧行。`partition_committed` 表示单个叶复读通过，后续叶失败仍会触发本次调用的共同回滚；整组成功退出后输出的 `committed: status=completed` 才表示该日期区间提交完成。多个修订区间分别调用本函数，回滚范围不会跨越已经成功的调用。

# ### 局部流程：按日期区间替换完整月度叶
# 
# 所有触达叶先合并、校验并在 staging 中准备，再在同一事务中逐叶安装或删除。空输入保留同样的区间替换语义。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录提交开始；校验日期顺序、输入契约和业务约束"] --> B["设置并检查正式、staging、备份和隔离路径"]
#     B --> C["检查已有数据物理契约；按新旧交易所与请求月份确定触达叶"]
#     C --> D["逐叶保留区间外旧行，加入区间内完整新行"]
#     D --> E["汇集合并结果，转换并校验完整替换叶"]
#     E --> F["写 staging 并复读物理契约、主键和总行数；空结果保留空 staging"]
#     F --> G["进入共享事务，逐叶读取预期行数"]
#     G --> H{"当前叶预期行数为零？"}
#     H -->|是| I["备份旧叶，显式删除正式叶"]
#     H -->|否| J["确认 staging 存在；备份旧叶并安装新叶"]
#     I --> K["存在叶时核对物理契约和主键；核对预期行数；记录进度"]
#     J --> K
#     K --> L{"还有触达叶？"}
#     L -->|是| G
#     L -->|否| M{"合并结果为空且整表没有 Parquet？"}
#     M -->|是| N["在 staging 写零行标记；同一事务安装并正式复读 Schema 与零行数"]
#     M -->|否| O["成功退出事务；清理临时目录；报告完成并返回输入行数"]
#     N --> O
#     F -.->|暂存写入或复读失败| P["清理 staging，正式叶尚未替换；抛出异常"]
#     I -.->|事务内异常| R["按实际移动倒序隔离新叶、恢复旧叶；新标记回退时移除"]
#     J -.->|事务内异常| R
#     K -.->|事务内异常| R
#     N -.->|事务内异常| R
#     R --> S["继续尝试其余目标；保留隔离证据及未恢复备份；清理 staging；抛出异常"]
# ```

# In[6]:


def commit_partitions(
    incoming_variety_calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    start_date: date,
    end_date: date,
) -> int:
    """按日期闭区间替换完整品种结果，并保留区间外的旧行。

    Parameters
    ----------
    incoming_variety_calendar_df : pd.DataFrame
        按权威 Schema 生成的区间内全部品种结果，仅含 [start_date, end_date]
        内的交易日。空输入表示该区间应无品种行，仍可能删除旧行。
    lake_root : pathlib.Path
        本次读写的数据湖根目录，函数在其下的 silver 目录管理目标表。
    start_date : datetime.date
        允许替换的起始自然日，包含该日。
    end_date : datetime.date
        允许替换的结束自然日，包含该日。

    Returns
    -------
    int
        本批输入行数，不是包含范围外旧行的完整替换分区行数。

    Notes
    -----
    输入与合并结果分别通过契约和业务校验；staging 与正式复读核对物理契约、
    主键和行数。一次调用触达的全部叶及本次新建零行标记共同安装和回滚，
    失败新叶隔离留存，回滚不完整时保留备份并抛出异常。
    """

    commit_started_at = perf_counter()
    click.echo(
        f"partition_plan: table={TABLE_NAME}; phase=commit; status=started; "
        f"start_date={start_date}; end_date={end_date}; rows={len(incoming_variety_calendar_df)}; "
        f"lake_root={lake_root}"
    )
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=input_validation; status=started")

    # =========================================================================
    # 1. 待提交数据规范化与业务校验
    # =========================================================================
    # 输入是日期区间内的完整品种结果；主键、日期边界与业务字段均须满足契约。

    if start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    incoming_variety_calendar_table = pandas_to_arrow(
        incoming_variety_calendar_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    incoming_variety_calendar_df = incoming_variety_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
    # 使用 ArrowDtype 保留契约类型，供后续范围比较和分区合并。


    # 本批自身必须满足主键唯一和请求日期边界
    if incoming_variety_calendar_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("待提交数据的主键不唯一。")
    if any(
        trading_date < start_date or trading_date > end_date
        for trading_date in incoming_variety_calendar_df["trading_date"]
    ):
        raise ValueError("待提交数据包含请求日期范围之外的交易日。")

    # 表级固定值在触碰正式目录前完成验证
    # active_contract_count 该品种当日上市区间内的固定月份合约数
    if (incoming_variety_calendar_df["active_contract_count"] <= 0).any():
        raise ValueError("该品种当日上市区间内的固定月份合约数 active_contract_count 必须全部大于 0。")
    if not incoming_variety_calendar_df.empty and not incoming_variety_calendar_df["source"].eq(
        "JQData_get_all_securities+dim_trade_calendar"
    ).all():
        raise ValueError("source 与数据契约不一致。")

    # 分区年月必须可以从交易日无歧义复算
    if any(
        trading_date.year != year or trading_date.month != month
        for trading_date, year, month in zip(
            incoming_variety_calendar_df["trading_date"],
            incoming_variety_calendar_df["year"],
            incoming_variety_calendar_df["month"],
            strict=True,
        )
    ):
        raise ValueError("year/month 分区列与 trading_date 不一致。")
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=input_validation; status=completed")


    # =========================================================================
    # 2. 提交路径与 Hive 分区环境初始化
    # =========================================================================

    # 所有临时、备份和隔离路径都必须位于本次指定湖的 silver 根目录
    silver_root = lake_root.resolve() / "silver"
    variety_calendar_path = silver_root / TABLE_NAME

    # 每次调用使用独立批次标识，隔离临时、备份与失败新叶目录。
    run_id = uuid.uuid4().hex
    variety_calendar_staging_dir = silver_root / f".c02s-{run_id}"
    variety_calendar_backup_dir = silver_root / f".c02b-{run_id}"

    # 提交失败时，已经安装的新叶移入隔离目录，旧叶从备份恢复。
    variety_calendar_quarantine_dir = silver_root / f".c02q-{run_id}"

    silver_root.mkdir(parents=True, exist_ok=True) # 确保目标湖的 silver 目录存在


    for managed_path in (variety_calendar_path, variety_calendar_staging_dir, variety_calendar_backup_dir, variety_calendar_quarantine_dir):
        # 路径解析后必须位于本次目标湖的 silver 根目录内。
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    # 三个分区列写进 Hive 目录名，不重复保存在分区内的 Parquet 载荷中。
    # 读取时必须传入同一分区 Schema，才能恢复完整八列逻辑表。
    variety_calendar_partitioning = ds.partitioning(
        pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
        flavor="hive",
    )


    # =========================================================================
    # 3. 现有正式数据集发现与契约校验
    # =========================================================================

    # 读取旧表时先核对物理 Schema 与身份 metadata，避免转换过程掩盖契约漂移。
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=started; run_id={run_id}")
    existing_variety_calendar_dataset = None
    if variety_calendar_path.is_dir() and any(variety_calendar_path.rglob("*.parquet")):
        existing_variety_calendar_dataset = ds.dataset(
            variety_calendar_path,
            format="parquet",
            partitioning=variety_calendar_partitioning,
        )
        existing_variety_calendar_schema = pa.schema(
            [existing_variety_calendar_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
            metadata=existing_variety_calendar_dataset.schema.metadata,
        )
        validate_compatible_dataset_schema(
            existing_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "现有正式数据集 "
        )
        validate_dataset_fragment_schemas(
            existing_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有正式数据集 "
        )


    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=completed; "
        f"dataset_exists={str(existing_variety_calendar_dataset is not None).lower()}; run_id={run_id}"
    )

    # =========================================================================
    # 4. 完整替换分区构造
    # =========================================================================

    # 请求范围内可能需要删除已失效品种，因此不能只看本批出现的交易所
    # 即使本批在某交易所为零行，也要从已有目录发现该交易所并触达其对应月份
    touched_exchange_codes = set(incoming_variety_calendar_df["exchange_code"].dropna().astype(str)) # 取得新数据中的交易所
    if variety_calendar_path.is_dir():
        touched_exchange_codes.update(
            partition_path.name.split("=", 1)[1]
            for partition_path
            in variety_calendar_path.glob("exchange_code=*") # 从 Hive 目录发现旧交易所
            if partition_path.is_dir()
        )

    # 触达集合是“全部相关交易所 × 请求跨越的每个自然年月”
    touched_partition_keys = [] # 完整 Hive 叶分区清单
    partition_year = start_date.year
    partition_month = start_date.month
    while (partition_year, partition_month) <= (end_date.year, end_date.month):
        for exchange_code in sorted(touched_exchange_codes):
            touched_partition_keys.append((exchange_code, partition_year, partition_month))
        if partition_month == 12:
            partition_year += 1
            partition_month = 1
        else:
            partition_month += 1

    # 同一月分区可能同时包含请求范围内外的日期。
    # 因此先保留范围外旧行，再与范围内的本批完整结果合并。
    partition_count = len(touched_partition_keys)
    click.echo(
        f"partition_plan: table={TABLE_NAME}; phase=merge_validate; status=started; "
        f"partitions={partition_count}; run_id={run_id}"
    )
    replacement_partition_dfs = []
    for partition_index, (exchange_code, year, month) in enumerate(touched_partition_keys, start=1):
        incoming_partition_mask = (
            incoming_variety_calendar_df["exchange_code"].eq(exchange_code)
            & incoming_variety_calendar_df["year"].eq(year)
            & incoming_variety_calendar_df["month"].eq(month)
        )
        incoming_partition_df = incoming_variety_calendar_df.loc[incoming_partition_mask, FUTURES_VARIETY_CALENDAR_SCHEMA.names]
        if existing_variety_calendar_dataset is None:
            retained_partition_df = incoming_variety_calendar_df.iloc[0:0].copy()
        else:
            existing_partition_table = existing_variety_calendar_dataset.to_table(
                columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
                filter=(ds.field("exchange_code") == exchange_code)
                & (ds.field("year") == year)
                & (ds.field("month") == month)
            )
            validated_existing_partition_table = validate_arrow_table(
                existing_partition_table,
                FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            existing_partition_df = validated_existing_partition_table.to_pandas(
                types_mapper=pd.ArrowDtype
            )
            retained_partition_df = existing_partition_df.loc[
                (existing_partition_df["trading_date"] < start_date)
                | (existing_partition_df["trading_date"] > end_date),
                FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            ]
        replacement_partition_df = pd.concat(
            [retained_partition_df, incoming_partition_df],
            ignore_index=True,
        )
        if not replacement_partition_df.empty:
            replacement_partition_dfs.append(replacement_partition_df)
        if partition_index % 250 == 0 or partition_index == partition_count:
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=merge; completed={partition_index}; "
                f"total={partition_count}; exchange_code={exchange_code}; year={year}; month={month}; run_id={run_id}"
            )

    if replacement_partition_dfs:
        staged_variety_calendar_df = pd.concat(replacement_partition_dfs, ignore_index=True)
        staged_variety_calendar_df = staged_variety_calendar_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    else:
        staged_variety_calendar_df = incoming_variety_calendar_df.iloc[0:0].copy()
    staged_variety_calendar_table = pandas_to_arrow(
        staged_variety_calendar_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    if staged_variety_calendar_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("合并后的触达分区主键不唯一。")
    if (staged_variety_calendar_df["active_contract_count"] <= 0).any():
        raise ValueError("合并后的触达分区 active_contract_count 必须全部大于 0。")
    if not staged_variety_calendar_df.empty and not staged_variety_calendar_df["source"].eq(
        "JQData_get_all_securities+dim_trade_calendar"
    ).all():
        raise ValueError("合并后的触达分区 source 与契约不一致。")
    if any(
        trading_date.year != year or trading_date.month != month
        for trading_date, year, month in zip(
            staged_variety_calendar_df["trading_date"],
            staged_variety_calendar_df["year"],
            staged_variety_calendar_df["month"],
            strict=True,
        )
    ):
        raise ValueError("合并后的触达分区年月与交易日不一致。")
    click.echo(
        f"partition_plan: table={TABLE_NAME}; phase=merge_validate; status=completed; "
        f"rows={len(incoming_variety_calendar_table)}; replacement_rows={len(staged_variety_calendar_table)}; "
        f"partitions={partition_count}; run_id={run_id}"
    )


    # =========================================================================
    # 5. staging 写入与复读验收
    # =========================================================================

    # staging 完整写入并复读通过前，不触碰正式分区。
    # write_dataset 会生成 exchange_code=.../year=.../month=.../part-*.parquet。
    commit_phase = "staging_write"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase={commit_phase}; status=started; run_id={run_id}")
    variety_calendar_staging_dir.mkdir(parents=True, exist_ok=False)
    try:
        if len(staged_variety_calendar_table):
            ds.write_dataset(
                staged_variety_calendar_table,
                variety_calendar_staging_dir,
                format="parquet",
                partitioning=variety_calendar_partitioning,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
            commit_phase = "staging_readback"
            click.echo(f"planning_progress: table={TABLE_NAME}; phase={commit_phase}; status=started; run_id={run_id}")
            staged_variety_calendar_dataset = ds.dataset(
                variety_calendar_staging_dir,
                format="parquet",
                partitioning=variety_calendar_partitioning,
            )
            staged_variety_calendar_schema = pa.schema(
                [staged_variety_calendar_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
                metadata=staged_variety_calendar_dataset.schema.metadata,
            )
            validate_compatible_dataset_schema(
                staged_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "staging "
            )
            validate_dataset_fragment_schemas(
                staged_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, PARTITION_COLUMNS, "staging "
            )
            validated_staged_variety_calendar_table = validate_arrow_table(
                staged_variety_calendar_dataset.to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names),
                FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            staged_replayed_df = validated_staged_variety_calendar_table.to_pandas()
            if staged_replayed_df.duplicated(PRIMARY_KEY).any():
                raise ValueError("staging 主键不唯一。")
            if len(validated_staged_variety_calendar_table) != len(staged_variety_calendar_table):
                raise ValueError("staging 行数检查失败。")
    except Exception as staging_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase={commit_phase}; status=failed; "
            f"error={type(staging_error).__name__}; run_id={run_id}"
        )
        shutil.rmtree(variety_calendar_staging_dir, ignore_errors=True)
        raise


    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=staging; status=completed; "
        f"rows={len(staged_variety_calendar_table)}; run_id={run_id}"
    )

    # =========================================================================
    # 6. 正式分区提交
    # =========================================================================

    # 逐分区提交：先把正式旧分区移到本批备份，再把 staging 新分区移入。
    # 每次移动都登记路径，后续任何检查失败都可以按相反顺序回滚。
    variety_calendar_path.mkdir(parents=True, exist_ok=True)
    schema_marker_path = variety_calendar_path / "schema.parquet"
    expected_partition_schema = pa.schema(
        [
            FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
            for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
            if name not in PARTITION_COLUMNS
        ],
        metadata=FUTURES_VARIETY_CALENDAR_SCHEMA.metadata,
    )
    physical_primary_key = [
        name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS
    ]
    expected_partition_row_count_by_key = staged_variety_calendar_df.groupby(
        PARTITION_COLUMNS, observed=True,
    ).size().to_dict()
    with StagedPathTransaction(
        root_path=variety_calendar_path, staging_dir=variety_calendar_staging_dir,
        backup_dir=variety_calendar_backup_dir, quarantine_dir=variety_calendar_quarantine_dir,
        log_context=f"table={TABLE_NAME}; run_id={run_id}",
    ) as transaction:
        for partition_index, (exchange_code, year, month) in enumerate(touched_partition_keys, start=1):
            partition_started_at = perf_counter()
            partition_relative_path = pathlib.Path(
                f"exchange_code={exchange_code}",
                f"year={year}",
                f"month={month}",
            )
            staged_partition_path = variety_calendar_staging_dir / partition_relative_path
            formal_partition_path = variety_calendar_path / partition_relative_path
            click.echo(
                f"partition_start: table={TABLE_NAME}; partition={partition_relative_path}; "
                f"completed={partition_index - 1}; total={partition_count}; run_id={run_id}"
            )
            expected_partition_row_count = int(
                expected_partition_row_count_by_key.get((exchange_code, year, month), 0)
            )
            # 合并结果为零行才明确删除；应有数据但 staging 缺失时由事务报错。
            transaction.replace(
                target_path=formal_partition_path,
                staged_path=staged_partition_path if expected_partition_row_count else None,
            )
            if formal_partition_path.is_dir():
                committed_partition_dataset = ds.dataset(
                    formal_partition_path, format="parquet"
                )
                for fragment in committed_partition_dataset.get_fragments():
                    validate_compatible_dataset_schema(
                        fragment.physical_schema, expected_partition_schema,
                        f"正式分区 {partition_relative_path} fragment {fragment.path} ",
                    )
                committed_primary_key_table = committed_partition_dataset.to_table(
                    columns=physical_primary_key
                )
                if committed_primary_key_table.to_pandas().duplicated(
                    physical_primary_key
                ).any():
                    raise ValueError(f"正式分区 {partition_relative_path} 主键不唯一。")
                committed_partition_row_count = len(committed_primary_key_table)
            else:
                committed_partition_row_count = 0
            if committed_partition_row_count != expected_partition_row_count:
                raise ValueError(f"正式分区 {partition_relative_path} 行数检查失败。")
            click.echo(
                f"partition_committed: table={TABLE_NAME}; partition={partition_relative_path}; "
                f"completed={partition_index}; total={partition_count}; rows={committed_partition_row_count}; "
                f"elapsed_s={perf_counter() - partition_started_at:.3f}; run_id={run_id}"
            )

        # 空库或清空最后一个分区后仍要保留可读取的表契约。
        # 根目录 marker 没有 Hive 路径值，所以物理 Schema 只写五个非分区列。
        if not len(staged_variety_calendar_table) and not any(variety_calendar_path.rglob("*.parquet")):
            staged_schema_marker_path = variety_calendar_staging_dir / "schema.parquet"
            pq.write_table(pa.Table.from_batches([], schema=expected_partition_schema), staged_schema_marker_path)
            transaction.replace(
                target_path=schema_marker_path, staged_path=staged_schema_marker_path,
                quarantine_new=False,
            )
            with pq.ParquetFile(schema_marker_path) as schema_marker_file:
                validate_compatible_dataset_schema(
                    schema_marker_file.schema_arrow, expected_partition_schema, "正式空表 marker ",
                )
                if schema_marker_file.metadata.num_rows != 0:
                    raise ValueError("正式空表 marker 必须为零行。")

    click.echo(
        f"committed: table={TABLE_NAME}; status=completed; rows={len(incoming_variety_calendar_table)}; "
        f"partitions={partition_count}; elapsed_s={perf_counter() - commit_started_at:.3f}; run_id={run_id}"
    )
    return len(incoming_variety_calendar_table)


# ## 来源采集：上游选择与逐交易日计数
# 
# `collect()` 可独立读取上游，也可接收 `selected_trading_dates`。独立调用时，确认目标湖中存在 `dim_trade_calendar` 并通过物理契约检查，读取 `calendar_date`、`is_trading_day`，按可选日期闭区间筛选并排序交易日。默认尾部入口已完成这些操作，通过 `selected_trading_dates` 传入同一份有序日期，函数直接复用；该参数的范围由调用方负责。
# 
# 没有选定交易日时，函数返回具有权威 Schema 类型的空 DataFrame，不认证或请求目录。存在交易日时，认证并请求一次完整期货证券目录，按合约识别规则筛选，校验必需字段、代码唯一性与上市闭区间，再对每个交易日按交易所和品种计数。当日没有有效固定月份合约时不产生输出行，但该日期仍计入处理进度。
# 
# 非空结果按主键排序，补充固定来源、批次 UTC 更新时间及交易年月，完成 Arrow 契约转换后以 ArrowDtype 的 Pandas DataFrame 返回。存在选定日期但展开结果为空时，也先完成契约转换再返回。
# 
# 日志依次报告上游读取或复用、认证、目录请求与校验、日期展开和结果转换。每处理 250 个交易日及最后一个交易日，报告累计日期数、结果行数和耗时。异常记录失败阶段与异常类型后向调用方抛出，不自动重试；采集成功日志表示来源结果已生成，不表示分区已提交。

# ### 局部流程：上游选择、目录校验与逐日展开
# 
# 输入是目标湖与可选日期范围，或调用方已经选好的交易日列表；输出是符合品种日历类型契约的 DataFrame。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录采集开始；检查日期成对及起止顺序"] --> B{"传入 selected_trading_dates？"}
#     B -->|是| C["复用调用方已选日期；记录 upstream_reuse"]
#     B -->|否| D["确认 c01 物理契约；窄列读取、筛选并排序交易日"]
#     C --> E{"交易日非空？"}
#     D --> E
#     E -->|否| Z["返回契约化空表；记录成功；无认证和 API 请求"]
#     E -->|是| F["认证 JQData；一次 get_all_securities 请求，date=None"]
#     F --> G["确认 DataFrame 和必需字段；解析代码并筛选固定月份合约"]
#     G --> H["校验代码唯一、合约集合非空、上市区间可解析且合法"]
#     H --> I["逐交易日选择上市日不晚于当日、结束日不早于当日的合约"]
#     I --> J["按交易所与品种计数；无合约时该日无输出行"]
#     J --> K["每 250 日及最后一日报告累计进度；补充非空结果字段"]
#     K --> L{"还有交易日？"}
#     L -->|是| I
#     L -->|否| M["汇总并按主键排序，完成 Arrow 契约转换"]
#     M --> N["记录采集成功与耗时；返回 DataFrame"]
# ```
# 
# 采集过程异常时记录当前失败阶段并抛出异常，后续展开和提交停止。

# In[7]:


def collect(
    lake_root: pathlib.Path,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    selected_trading_dates: list[date] | None = None,
) -> pd.DataFrame:
    """按正式上游交易日采集并构建品种日历。

    不传 selected_trading_dates 时，自行读取目标湖的上游交易日历并按可选
    日期闭区间筛选。传入时直接复用该有序列表，其上游物理契约和日期范围
    由调用方负责。无选定交易日时返回契约化空表，不认证或请求目录。
    """
    collection_started_at = perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=collect; status=started; "
        f"start_date={start_date}; end_date={end_date}; lake_root={lake_root}"
    )
    collection_phase = "request_validation"
    try:
        if (start_date is None) != (end_date is None):
            raise ValueError("起始日期与结束日期必须同时提供。")
        if start_date is not None and start_date > end_date:
            raise ValueError("起始日期不得晚于结束日期。")

        if selected_trading_dates is None:
            # c01 负责上游日期、连续水位和派生字段；c02 确认其物理契约后消费交易日。
            collection_phase = "upstream_read"
            click.echo(f"planning_progress: table={TABLE_NAME}; phase=upstream_read; status=started")
            trade_calendar_path = lake_root.resolve() / "silver" / TRADE_TABLE_NAME
            if not trade_calendar_path.is_dir():
                raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
            trade_calendar_partitioning = ds.partitioning(
                pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in TRADE_PARTITION_COLUMNS]),
                flavor="hive",
            )
            trade_calendar_dataset = ds.dataset(
                trade_calendar_path,
                format="parquet",
                partitioning=trade_calendar_partitioning,
            )
            trade_calendar_schema = pa.schema(
                [trade_calendar_dataset.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names],
                metadata=trade_calendar_dataset.schema.metadata,
            )
            validate_compatible_dataset_schema(
                trade_calendar_schema, TRADE_CALENDAR_SCHEMA, "上游交易日历 "
            )
            validate_dataset_fragment_schemas(
                trade_calendar_dataset, TRADE_CALENDAR_SCHEMA, TRADE_PARTITION_COLUMNS, "上游交易日历 "
            )

            # 只读取计算需要的日期与交易日标记，按可选自然日闭区间筛选。
            trade_calendar_filter = None
            if start_date is not None:
                trade_calendar_filter = (ds.field("calendar_date") >= start_date) & (
                    ds.field("calendar_date") <= end_date
                )
            trade_calendar_table = trade_calendar_dataset.to_table(
                columns=["calendar_date", "is_trading_day"],
                filter=trade_calendar_filter,
            )
            trade_calendar_df = trade_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)

            # 从 c01 已提交日历中选择交易日，并按日期排序。
            valid_trading_dates = sorted(
                trade_calendar_df.loc[trade_calendar_df["is_trading_day"].eq(True), "calendar_date"].tolist()
            )
        else:
            collection_phase = "upstream_reuse"
            valid_trading_dates = selected_trading_dates
        trading_date_count = len(valid_trading_dates)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase={collection_phase}; status=completed; "
            f"trading_dates={trading_date_count}"
        )
        if not valid_trading_dates:
            collection_phase = "contract_conversion"
            collected_variety_calendar_df = pa.Table.from_batches(
                [],
                schema=FUTURES_VARIETY_CALENDAR_SCHEMA,
            ).to_pandas(types_mapper=pd.ArrowDtype)
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; "
                f"rows=0; trading_dates=0; api_calls=0; elapsed_s={perf_counter() - collection_started_at:.3f}"
            )
            return collected_variety_calendar_df

        # 只在存在上游交易日时认证并查询，纯休市范围不会消耗 JQData 调用。
        from config.jqdata_connection import authenticate_jqdata

        collection_phase = "authenticate"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=started")
        authentication_started_at = perf_counter()
        jqdata_client = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=completed; "
            f"elapsed_s={perf_counter() - authentication_started_at:.3f}"
        )

        # 原始 API 契约：索引是标准合约代码；列包含 display_name、name、
        # start_date、end_date、type。date=None 刻意请求完整历史目录。
        # 本维度只需要代码和上市区间，且不按事实采集白名单裁剪目录。
        collection_phase = "contract_catalog"
        request_started_at = perf_counter()
        click.echo(f"request_batch: table={TABLE_NAME}; phase=contract_catalog; status=started; date=None")
        futures_contract_catalog_df = jqdata_client.get_all_securities(["futures"], date=None)
        if not isinstance(futures_contract_catalog_df, pd.DataFrame):
            raise TypeError(
                f"get_all_securities 应返回 pandas.DataFrame，实际为 {type(futures_contract_catalog_df).__name__}。"
            )

        click.echo(
            f"api_result: table={TABLE_NAME}; phase=contract_catalog; status=completed; "
            f"rows={len(futures_contract_catalog_df)}; elapsed_s={perf_counter() - request_started_at:.3f}"
        )
        collection_phase = "catalog_validation"
        click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=started")

        # 把 API 索引显式转成普通列，后续才能校验重复代码并解析交易所。
        futures_contract_catalog_df = futures_contract_catalog_df.rename_axis("contract_code").reset_index()
        required_catalog_columns = {"contract_code", "start_date", "end_date"}
        if not required_catalog_columns <= set(futures_contract_catalog_df.columns):
            raise ValueError(
                f"get_all_securities 缺少列：{sorted(required_catalog_columns - set(futures_contract_catalog_df.columns))}"
            )

        # 名称与 type 是原始目录描述字段，不属于目标表粒度，因此不写入湖。
        # 代码统一大写后，用一个正则同时解析品种、交割代码与交易所后缀。
        futures_contract_catalog_df["contract_code"] = (
            futures_contract_catalog_df["contract_code"].astype("string").str.upper()
        )
        parsed_contract_codes_df = futures_contract_catalog_df["contract_code"].str.extract(FIXED_CONTRACT)
        futures_contract_catalog_df = pd.concat([futures_contract_catalog_df, parsed_contract_codes_df], axis=1)

        # 无法匹配固定月份格式的代码先剔除；能匹配但属于连续序列的数字代码再显式排除。
        futures_contract_catalog_df = futures_contract_catalog_df.dropna(
            subset=["underlying_code", "delivery_code", "exchange_code"]
        )
        futures_contract_catalog_df = futures_contract_catalog_df.loc[
            ~futures_contract_catalog_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES)
        ].copy()

        # 完成格式过滤后，剩余固定月份合约代码必须唯一且集合不得为空。
        if futures_contract_catalog_df.duplicated(["contract_code"]).any():
            duplicate_contract_codes = sorted(
                futures_contract_catalog_df.loc[
                    futures_contract_catalog_df.duplicated(["contract_code"], keep=False),
                    "contract_code",
                ].unique()
            )
            raise ValueError(f"get_all_securities 合约代码重复：{duplicate_contract_codes}")
        if futures_contract_catalog_df.empty:
            raise ValueError("get_all_securities 未返回任何固定月份期货合约。")

        # 上市/结束日期统一转成 datetime.date；区间两端在后续判断中都包含。
        futures_contract_catalog_df["start_date"] = pd.to_datetime(
            futures_contract_catalog_df["start_date"],
            errors="raise",
        ).dt.date
        futures_contract_catalog_df["end_date"] = pd.to_datetime(
            futures_contract_catalog_df["end_date"],
            errors="raise",
        ).dt.date
        if futures_contract_catalog_df[["start_date", "end_date"]].isna().any().any():
            raise ValueError("固定月份合约的 start_date/end_date 包含空值。")
        if (futures_contract_catalog_df["start_date"] > futures_contract_catalog_df["end_date"]).any():
            raise ValueError("固定月份合约存在 start_date 晚于 end_date 的记录。")
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=completed; "
            f"fixed_contracts={len(futures_contract_catalog_df)}"
        )

        # 对每个上游交易日选择 start_date <= trading_date <= end_date 的固定合约。
        # 再按交易所—品种计数；计数大于零才产生一行品种日历格点。
        collection_phase = "expand"
        batch_updated_at = datetime.now(timezone.utc)
        daily_variety_calendar_dfs = []
        expanded_row_count = 0
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=expand; status=started; "
            f"completed=0; total={trading_date_count}"
        )
        for trading_date_index, trading_date in enumerate(valid_trading_dates, start=1):
            active_contracts_df = futures_contract_catalog_df.loc[
                (futures_contract_catalog_df["start_date"] <= trading_date)
                & (futures_contract_catalog_df["end_date"] >= trading_date)
            ]
            daily_variety_calendar_df = (
                active_contracts_df.groupby(["exchange_code", "underlying_code"])
                .size()
                .rename("active_contract_count")
                .reset_index()
            )
            expanded_row_count += len(daily_variety_calendar_df)
            if trading_date_index % 250 == 0 or trading_date_index == trading_date_count:
                click.echo(
                    f"planning_progress: table={TABLE_NAME}; phase=expand; completed={trading_date_index}; "
                    f"total={trading_date_count}; trading_date={trading_date}; rows={expanded_row_count}; "
                    f"elapsed_s={perf_counter() - collection_started_at:.3f}"
                )
            if daily_variety_calendar_df.empty:
                continue

            # 这些列由当前交易日和本批运行上下文生成，不来自原始 API。
            daily_variety_calendar_df["trading_date"] = trading_date
            daily_variety_calendar_df["active_contract_count"] = daily_variety_calendar_df["active_contract_count"].astype(
                "int16"
            )
            daily_variety_calendar_df["source"] = "JQData_get_all_securities+dim_trade_calendar"
            daily_variety_calendar_df["updated_at"] = batch_updated_at
            daily_variety_calendar_df["year"] = trading_date.year
            daily_variety_calendar_df["month"] = trading_date.month
            daily_variety_calendar_dfs.append(
                daily_variety_calendar_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names]
            )

        # 最终按主键排序并完成 Arrow 契约转换，以 ArrowDtype DataFrame 返回。
        if daily_variety_calendar_dfs:
            collected_variety_calendar_df = pd.concat(daily_variety_calendar_dfs, ignore_index=True)
            collected_variety_calendar_df = collected_variety_calendar_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
        else:
            collected_variety_calendar_df = pd.DataFrame(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names)
        collection_phase = "contract_conversion"
        collected_variety_calendar_table = pandas_to_arrow(collected_variety_calendar_df, FUTURES_VARIETY_CALENDAR_SCHEMA)
        collected_variety_calendar_df = collected_variety_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; "
            f"rows={len(collected_variety_calendar_df)}; trading_dates={trading_date_count}; "
            f"api_calls=1; elapsed_s={perf_counter() - collection_started_at:.3f}"
        )
        return collected_variety_calendar_df
    except Exception as collection_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=collect; status=failed; "
            f"failed_phase={collection_phase}; error={type(collection_error).__name__}; "
            f"elapsed_s={perf_counter() - collection_started_at:.3f}"
        )
        raise


# ## 运行计划：尾部水位与历史差异区间
# 
# `main()` 解析目标湖并检查日期成对、模式互斥及正式湖写入条件。默认模式读取品种日历最大交易日，再从 c01 选择晚于该日的交易日；有新增时将同一份日期传给 `collect()`，并以首尾日期规划一次提交。显式日期模式由 `collect()` 在指定闭区间内选择上游交易日，计划覆盖整个指定区间。
# 
# 全历史模式生成完整预期品种日历，并检查已有表的物理契约、主键、正合约数、来源与年月。已有主键不属于预期集合时直接报错。其余主键按有效合约数比较，缺失或数量不同的主键归入差异日期；某日只要一个品种存在差异，就选择该日全部品种结果。
# 
# 差异区间按预期结果的有序交易日序列合并：连续出现的差异日期属于同一区间，遇到没有差异的日期则结束当前区间。这里的相邻依据该交易日序列，可跨周末或休市日。各区间分别调用 `commit_partitions()`，成功区间保留，后续失败不会撤销此前提交。
# 
# 运行完成日志同时用于只读、有写入和无变化三类正常退出。`write=false` 表示没有启用提交，`outcome=up_to_date` 表示没有待办；日期区间实际提交成功由提交函数的 `committed` 日志证明。全历史写入完成后还输出区间数和累计输入行数的汇总。

# ### 局部流程：运行模式与替换计划
# 
# 三条分支共用日期与写入门禁。全历史比较以“交易所—品种—交易日”为主键，比较有效合约数；多个区间分别提交。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["解析目标湖；校验日期成对、模式互斥与正式湖写入门禁"] --> M{"运行模式"}
#     M -->|默认尾部| D["检查 c02 物理契约及最大交易日；从 c01 选择新增交易日"]
#     D --> E{"存在新增交易日？"}
#     E -->|否| Z["报告 up_to_date 与运行完成；API 调用为 0；返回"]
#     E -->|是| F["collect 复用已选日期；以首尾日期规划一个替换区间"]
#     M -->|显式日期| G["校验起止顺序；collect 在指定闭区间内选择上游交易日"]
#     M -->|全历史| H["collect 全部上游交易日；生成预期主键与合约数"]
#     H --> I["校验已有表；已有主键超出预期集合时抛出异常"]
#     I --> J["找出缺失或数量不同的主键，归并为差异日期"]
#     J --> K{"存在差异日期？"}
#     K -->|否| N["报告 up_to_date 与运行完成；返回"]
#     K -->|是| L["按预期交易日序列合并相邻差异日期；各日取完整品种结果"]
#     F --> W{"启用 --write？"}
#     G --> W
#     L --> W
#     W -->|否| Q["报告只读计划与运行完成；返回"]
#     W -->|是| T["逐区间调用 commit_partitions；此前成功区间保留"]
#     T --> U["全历史模式汇总提交；报告运行完成"]
# ```

# In[8]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--full", "full_refresh", is_flag=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    full_refresh: bool,
    write: bool,
) -> None:
    # 未覆盖 --lake-root 时使用 .env 中唯一的正式湖；显式路径主要供临时湖测试。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
    if full_refresh and has_explicit_dates:
        raise click.UsageError("--full 与 --start-date/--end-date 不能同时提供。")
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            "显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；"
            "请移除日期参数使用自动补缺，或改用非正式测试湖。"
        )

    log_boundary = "=" * 88
    run_started_at = perf_counter()
    run_mode = "explicit" if has_explicit_dates else ("full" if full_refresh else "automatic_tail")
    click.echo(
        f"{log_boundary}\n"
        "运行入口开始 / Run entry started\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=started; mode={run_mode}; "
        f"write={str(write).lower()}; lake_root={resolved_lake_root}"
        f"\n{log_boundary}"
    )

    # 显式日期模式只做定向检查；只有非正式湖可以同时使用 --write。
    if has_explicit_dates:
        requested_start_date = start_date.date()
        requested_end_date = end_date.date()
        if requested_start_date > requested_end_date:
            raise click.BadParameter("起始日期不得晚于结束日期。")

        requested_variety_calendar_df = collect(
            resolved_lake_root,
            requested_start_date,
            requested_end_date,
        )
        if write:
            commit_partitions(
                requested_variety_calendar_df,
                resolved_lake_root,
                requested_start_date,
                requested_end_date,
            )
        click.echo(
            f"{log_boundary}\n"
            f"显式日期{'写入' if write else '只读'}运行完成 / Explicit-date {'write' if write else 'dry'} run completed\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; mode={run_mode}; "
            f"write={str(write).lower()}; rows={len(requested_variety_calendar_df)}; "
            f"elapsed_s={perf_counter() - run_started_at:.3f}"
            f"\n{log_boundary}"
        )
        return

    variety_calendar_path = resolved_lake_root / "silver" / TABLE_NAME
    variety_calendar_partitioning = ds.partitioning(
        pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
        flavor="hive",
    )

    # 默认模式按 c02 最大交易日选择 c01 新增交易日；无新增时不认证。
    if not full_refresh:
        latest_variety_trading_date = None
        if variety_calendar_path.is_dir() and any(variety_calendar_path.rglob("*.parquet")):
            existing_variety_calendar_dataset = ds.dataset(
                variety_calendar_path,
                format="parquet",
                partitioning=variety_calendar_partitioning,
            )
            existing_variety_calendar_schema = pa.schema(
                [
                    existing_variety_calendar_dataset.schema.field(name)
                    for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
                ],
                metadata=existing_variety_calendar_dataset.schema.metadata,
            )
            validate_compatible_dataset_schema(
                existing_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "现有品种日历 "
            )
            validate_dataset_fragment_schemas(
                existing_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有品种日历 "
            )
            existing_trading_dates = existing_variety_calendar_dataset.to_table(
                columns=["trading_date"],
            ).column("trading_date").to_pylist()
            latest_variety_trading_date = max(existing_trading_dates, default=None)

        trade_calendar_path = resolved_lake_root / "silver" / TRADE_TABLE_NAME
        if not trade_calendar_path.is_dir():
            raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
        trade_calendar_partitioning = ds.partitioning(
            pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in TRADE_PARTITION_COLUMNS]),
            flavor="hive",
        )
        trade_calendar_dataset = ds.dataset(
            trade_calendar_path,
            format="parquet",
            partitioning=trade_calendar_partitioning,
        )
        trade_calendar_schema = pa.schema(
            [
                trade_calendar_dataset.schema.field(name)
                for name in TRADE_CALENDAR_SCHEMA.names
            ],
            metadata=trade_calendar_dataset.schema.metadata,
        )
        validate_compatible_dataset_schema(
            trade_calendar_schema, TRADE_CALENDAR_SCHEMA, "上游交易日历 "
        )
        validate_dataset_fragment_schemas(
            trade_calendar_dataset, TRADE_CALENDAR_SCHEMA, TRADE_PARTITION_COLUMNS, "上游交易日历 "
        )
        new_trade_calendar_filter = None
        if latest_variety_trading_date is not None:
            new_trade_calendar_filter = (
                ds.field("calendar_date") > latest_variety_trading_date
            )
        new_trade_calendar_table = trade_calendar_dataset.to_table(
            columns=["calendar_date", "is_trading_day"],
            filter=new_trade_calendar_filter,
        )
        new_trade_calendar_df = new_trade_calendar_table.to_pandas(
            types_mapper=pd.ArrowDtype
        )
        new_trading_dates = sorted(
            new_trade_calendar_df.loc[
                new_trade_calendar_df["is_trading_day"].eq(True),
                "calendar_date",
            ].tolist()
        )
        if not new_trading_dates:
            click.echo(
                f"up_to_date: table={TABLE_NAME}; mode=automatic_tail; "
                f"latest_trading_date={latest_variety_trading_date}; api_calls=0"
            )
            click.echo(
                f"{log_boundary}\n"
                "品种日历检查完成，无待更新数据 / Variety calendar check completed, no pending updates\n"
                "function=main()\n"
                f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; mode={run_mode}; "
                f"write={str(write).lower()}; outcome=up_to_date; rows=0; "
                f"elapsed_s={perf_counter() - run_started_at:.3f}"
                f"\n{log_boundary}"
            )
            return

        pending_start_date = new_trading_dates[0]
        pending_end_date = new_trading_dates[-1]
        pending_variety_calendar_df = collect(
            resolved_lake_root,
            pending_start_date,
            pending_end_date,
            selected_trading_dates=new_trading_dates,
        )
        click.echo(
            f"auto_plan: table={TABLE_NAME}; mode=automatic_tail; "
            f"new_trading_date_count={len(new_trading_dates)}; "
            f"rows={len(pending_variety_calendar_df)}; api_calls=1"
        )
        if write:
            commit_partitions(
                pending_variety_calendar_df,
                resolved_lake_root,
                pending_start_date,
                pending_end_date,
            )
        click.echo(
            f"{log_boundary}\n"
            f"尾部更新{'写入' if write else '只读'}运行完成 / Tail-update {'write' if write else 'dry'} run completed\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; mode={run_mode}; "
            f"write={str(write).lower()}; rows={len(pending_variety_calendar_df)}; "
            f"elapsed_s={perf_counter() - run_started_at:.3f}"
            f"\n{log_boundary}"
        )
        return

    # --full 由 collect 读取 c01 完整交易日范围，生成预期品种日历供历史比较。
    expected_variety_calendar_df = collect(resolved_lake_root)
    expected_contract_count_by_key = dict(
        zip(
            expected_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None),
            expected_variety_calendar_df["active_contract_count"].astype(int),
            strict=True,
        )
    )

    expected_primary_key_set = set(expected_contract_count_by_key)

    # 下游只有通过精确契约和表级约束的行，才有资格计入“已经完整落盘”。
    existing_variety_calendar_df = expected_variety_calendar_df.iloc[0:0].copy()
    if variety_calendar_path.is_dir() and any(variety_calendar_path.rglob("*.parquet")):
        existing_variety_calendar_dataset = ds.dataset(
            variety_calendar_path,
            format="parquet",
            partitioning=variety_calendar_partitioning,
        )
        existing_variety_calendar_schema = pa.schema(
            [existing_variety_calendar_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
            metadata=existing_variety_calendar_dataset.schema.metadata,
        )
        validate_compatible_dataset_schema(
            existing_variety_calendar_schema, FUTURES_VARIETY_CALENDAR_SCHEMA, "现有品种日历 "
        )
        validate_dataset_fragment_schemas(
            existing_variety_calendar_dataset, FUTURES_VARIETY_CALENDAR_SCHEMA, PARTITION_COLUMNS, "现有品种日历 "
        )
        existing_variety_calendar_table = validate_arrow_table(
            existing_variety_calendar_dataset.to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names),
            FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        existing_variety_calendar_df = existing_variety_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
        if existing_variety_calendar_df.duplicated(PRIMARY_KEY).any():
            raise ValueError("现有品种日历主键不唯一。")
        if (existing_variety_calendar_df["active_contract_count"] <= 0).any():
            raise ValueError("现有品种日历 active_contract_count 包含非正数。")
        if not existing_variety_calendar_df.empty and not existing_variety_calendar_df["source"].eq(
            "JQData_get_all_securities+dim_trade_calendar"
        ).all():
            raise ValueError("现有品种日历 source 与契约不一致。")
        if any(
            trading_date.year != year or trading_date.month != month
            for trading_date, year, month in zip(
                existing_variety_calendar_df["trading_date"],
                existing_variety_calendar_df["year"],
                existing_variety_calendar_df["month"],
                strict=True,
            )
        ):
            raise ValueError("现有品种日历分区年月与交易日不一致。")

    existing_contract_count_by_key = dict(
        zip(
            existing_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None),
            existing_variety_calendar_df["active_contract_count"].astype(int),
            strict=True,
        )
    )
    existing_primary_key_set = set(existing_contract_count_by_key)
    out_of_watermark_primary_key_set = existing_primary_key_set - expected_primary_key_set
    if out_of_watermark_primary_key_set:
        raise ValueError(
            "现有品种日历包含不属于上游有效格点的主键；"
            f"自动补缺不会静默删除这些行：{sorted(out_of_watermark_primary_key_set)[:10]}"
        )

    incomplete_primary_key_set = {
        primary_key
        for primary_key, expected_contract_count in expected_contract_count_by_key.items()
        if existing_contract_count_by_key.get(primary_key) != expected_contract_count
    }
    if not incomplete_primary_key_set:
        click.echo(
            f"up_to_date: table={TABLE_NAME}; "
            f"upstream_grid_count={len(expected_primary_key_set)}"
        )
        click.echo(
            f"{log_boundary}\n"
            "品种日历检查完成，无待更新数据 / Variety calendar check completed, no pending updates\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; mode={run_mode}; "
            f"write={str(write).lower()}; outcome=up_to_date; rows=0; "
            f"elapsed_s={perf_counter() - run_started_at:.3f}"
            f"\n{log_boundary}"
        )
        return

    # 一个日期只要缺少任一品种，就以该日的完整上游结果为提交单位。
    incomplete_trading_date_set = {primary_key[2] for primary_key in incomplete_primary_key_set}
    expected_trading_dates = sorted(
        expected_variety_calendar_df["trading_date"].drop_duplicates().tolist()
    )
    update_date_ranges = []
    range_start_date = None
    range_end_date = None
    for trading_date in expected_trading_dates:
        if trading_date in incomplete_trading_date_set:
            if range_start_date is None:
                range_start_date = trading_date
            range_end_date = trading_date
        elif range_start_date is not None:
            update_date_ranges.append((range_start_date, range_end_date))
            range_start_date = None
            range_end_date = None
    if range_start_date is not None:
        update_date_ranges.append((range_start_date, range_end_date))

    replacement_row_count = int(
        expected_variety_calendar_df["trading_date"].isin(incomplete_trading_date_set).sum()
    )
    click.echo(
        f"{log_boundary}\n"
        "全历史差异计划已生成 / Full-history reconciliation plan created\n"
        "function=main()\n"
        f"reconciliation_plan: table={TABLE_NAME}; upstream_grid_count={len(expected_primary_key_set)}; "
        f"complete_grid_count={len(expected_primary_key_set) - len(incomplete_primary_key_set)}; "
        f"missing_or_incomplete_grid_count={len(incomplete_primary_key_set)}; "
        f"replacement_row_count={replacement_row_count}; range_count={len(update_date_ranges)}"
        f"\n{log_boundary}"
    )

    if write:
        committed_row_count = 0
        for range_start_date, range_end_date in update_date_ranges:
            replacement_variety_calendar_df = expected_variety_calendar_df.loc[
                (expected_variety_calendar_df["trading_date"] >= range_start_date)
                & (expected_variety_calendar_df["trading_date"] <= range_end_date),
                FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            ].reset_index(drop=True)
            committed_row_count += commit_partitions(
                replacement_variety_calendar_df,
                resolved_lake_root,
                range_start_date,
                range_end_date,
            )
        click.echo(
            f"{log_boundary}\n"
            "全历史差异提交汇总 / Full-history reconciliation commit summary\n"
            "function=main()\n"
            f"planning_progress: table={TABLE_NAME}; phase=full_reconciliation; status=completed; "
            f"rows={committed_row_count}; ranges={len(update_date_ranges)}"
            f"\n{log_boundary}"
        )
    click.echo(
        f"{log_boundary}\n"
        f"全历史差异{'写入' if write else '只读'}运行完成 / Full-history reconciliation {'write' if write else 'dry'} run completed\n"
        "function=main()\n"
        f"planning_progress: table={TABLE_NAME}; phase=run; status=completed; mode={run_mode}; "
        f"write={str(write).lower()}; rows={replacement_row_count}; ranges={len(update_date_ranges)}; "
        f"elapsed_s={perf_counter() - run_started_at:.3f}"
        f"\n{log_boundary}"
    )


# ## 执行入口：Notebook 与脚本
# 
# 运行下面的入口单元格会检查 **2026-08-01 至 2026-08-15** 内的上游交易日并生成品种日历，参数不含 `--write`，因此不提交分区。范围内有上游交易日时会请求一次完整证券目录；没有交易日时返回空结果。该范围是可修改的显式日期示例，将 `notebook_args` 改为 `[]` 可运行默认尾部只读检查。
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，避免解析内核启动时的 `-f` 参数；`standalone_mode=False` 让调用完成后返回单元格。直接运行 `.py` 时读取命令行参数，导入同名模块时只加载定义。切换更新模式或启用写入前，应核对开篇的日期与写入条件。

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

# In[9]:


# Notebook 显式提供 args，避免 Click 将内核的 -f 启动参数当作业务参数。
if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 显式日期只读示例
    main.main(
        args=notebook_args,
        prog_name="c02_futures_variety_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# ## 终端运行：尾部更新与历史维护
# 
# 在项目根目录使用 `latitude_env_v2` 解释器运行，目标湖须已有 c01 提交的上游交易日历。下面依次给出环境预检、代码一致性检查、默认尾部只读检查、默认尾部写入和全历史差异写入命令；按任务选择一条业务命令执行。有选定交易日时，只读命令也会访问 JQData。
# 
# ```powershell
# cd E:\Latitude_Analytics_v2
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b00_01_verify_runtime.py
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b00_02_sync_notebook_exports.py --check --check-level code
# 
# # 默认尾部只读检查
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py
# 
# # 默认尾部更新并写入正式湖
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --write
# 
# # 全历史比较并按差异日期区间写入完整品种结果
# & 'E:\anaconda3\envs\latitude_env_v2\python.exe' R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c02_futures_variety_calendar.py --full --write
# ```
# 
# ### 局部流程：终端手动运行
# 
# 命令需要在终端手动执行；下方代码单元格中的命令均为注释，运行该单元格不会启动采集。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["切换到项目根目录，使用 latitude_env_v2 解释器"] --> B["运行环境预检与 Notebook/Python 代码一致性检查"]
#     B --> B1["确认目标湖已有 c01 提交的交易日历"]
#     B1 --> C["选择尾部更新、全历史比较或显式日期检查参数"]
#     C --> D["手动运行 c02_futures_variety_calendar.py"]
#     D --> E["main 生成计划；按 --write 决定是否提交"]
# ```

# In[10]:


# 以下命令需在终端手动执行；本单元格仅保存注释。
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python R02_Market_Data\a01_Collection\b00_01_verify_runtime.py
# python R02_Market_Data\a01_Collection\b00_02_sync_notebook_exports.py --check --check-level code
# python R02_Market_Data\a01_Collection\b01_Futures_Market_Data\c02_futures_variety_calendar.py --write

