#!/usr/bin/env python
# coding: utf-8

# # b02_futures_variety_calendar
# 
# 生成 `dim_futures_variety_calendar`：将 b01 已提交的交易日与 JQData 完整固定月份合约目录的上市区间求交，按交易所—品种—交易日统计有效合约数。事实采集白名单不参与本维度表构建。
# 
# 阅读顺序：更新模式与数据边界 → 初始化、Schema 和代码规则 → 分区提交 → 来源采集与展开 → CLI 与执行。函数定义单元格不发起采集；写入由 `--write` 显式启用。Notebook 是业务源，同名 Python 文件由默认 PythonExporter 生成。

# ## 总流程：从上游交易日到品种日历
# 
# 本环节消费 b01 已正式提交的交易日，按固定月份合约的上市区间生成品种日历。图按运行时调用顺序组织；函数定义单元格本身不采集。矩形表示操作，菱形表示分支；未展开的校验异常向调用方抛出。
# 
# 默认模式在 main 中先选新增交易日，无新增就直接结束；空日期集不请求目录。细分分支见 collect 与 main 局部图。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["初始化与定义 → 执行入口 main"] --> B["检查参数与写入边界；确定运行模式"]
#     B --> C["collect：选择或复用 b01 已提交的交易日"]
#     C --> P{"交易日非空？"}
#     P -->|是| D["一次请求完整目录；按上市区间逐日计数"]
#     P -->|否| E["返回契约化结果；无交易日为空表"]
#     D --> E
#     E --> F["main：规划替换区间；全历史先比较主键与计数"]
#     F --> W{"需要替换且启用 --write？"}
#     W -->|否| R["只读或无需更新，结束"]
#     W -->|是| G["commit_partitions：逐区间构造完整月度叶并校验"]
#     G --> H["staging 写入复读 → 共享事务逐叶安装与正式复读"]
#     H --> I["成功清理；事务失败则回滚并保留相应证据"]
# ```

# ## 更新范围与正式湖写入边界
# 
# | 模式 | 处理范围 | 写入行为 |
# | --- | --- | --- |
# | 默认尾部更新 | 只消费 b01 中晚于 b02 最大交易日的新增交易日 | 可用 `--write` 提交新增日期；没有新增时在认证前结束 |
# | `--full` | 展开完整上游水位，比较已有主键与合约数 | 按差异日期范围提交；现有主键超出上游有效格点时硬失败 |
# | 成对显式日期 | 仅展开给定范围内的上游交易日 | 可只读检查，或写入明确指定的非正式湖；与 `--full` 互斥 |
# 
# 空湖通过同一默认入口自然首次全建。正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 唯一指定。
# 
# b01 对交易日历的主键、连续性和派生字段负责。b02 只确认上游物理 Schema/metadata 兼容，并投影 `calendar_date`、`is_trading_day` 两列；不重复验证上游业务结论。权威字段定义见 [`config/data_contracts.py`](../../config/data_contracts.py)，运行规则见[湖仓 README](../README.md)。

# ## 来源目录与转换边界
# 
# 存在上游交易日时，`collect()` 认证后只调用一次 `get_all_securities(["futures"], date=None)`，读取完整历史期货证券目录；无交易日时返回符合契约的空表，不认证、不调用 API。
# 
# 原始结果是 Pandas DataFrame，索引为标准合约代码，如 `IC1505.CCFX`。本环节消费索引、`start_date`、`end_date`；`display_name`、`name` 和 `type` 不进入品种日历。
# 
# 代码解析保留固定月份合约，排除 `8888`、`9998`、`9999` 连续或指数代码。目录代码唯一性与上市区间校验通过后，将每张合约的闭区间 `[start_date, end_date]` 与上游交易日求交并按交易所—品种计数。

# ## 落盘表的逻辑形式与物理形式
# 
# 逻辑表路径是 `<lake_root>/silver/dim_futures_variety_calendar`，粒度为“每个交易所—品种—交易日一行”，主键是 `exchange_code + underlying_code + trading_date`。
# 
# Hive 分区目录如下：
# 
# ```text
# dim_futures_variety_calendar/
#   exchange_code=CCFX/
#     year=2026/
#       month=8/
#         part-0.parquet
# ```
# 
# 单个 Parquet 文件物理保存 `underlying_code`、`trading_date`、`active_contract_count`、`source`、`updated_at` 五列；`exchange_code`、`year`、`month` 三列来自目录名。使用声明了 Hive partitioning 的 Arrow dataset 读取时，二者合并为契约规定的八列逻辑表。
# 
# 如果整张表暂时为零行，则在表根目录写入零行 `schema.parquet` 保存非分区列 Schema；marker 从正式路径复读物理契约和零行数后才算提交成功。读取时仍通过同一 Hive partitioning 恢复完整逻辑契约。

# ## 初始化与依赖
# 
# 导入路径、表格与分区读写依赖，并按项目标记定位仓库根目录。目标表与上游表均使用具名权威 Schema；本单元格不认证、不执行写入。

# ### 局部流程：初始化
# 
# 准备依赖与权威 Schema；不认证、不请求来源、不写湖。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行初始化单元格"] --> B["从当前目录逐级查找项目标记"]
#     B --> C{"找到仓库根目录？"}
#     C -->|否| X["抛出异常，停止初始化"]
#     C -->|是| D["加入项目与湖仓导入路径"]
#     D --> E["导入 settings、表格库与权威 Schema"]
#     E --> F["后续定义单元格可执行"]
# ```

# In[1]:


# 延迟解析类型注解，避免运行时立即求值尚未导入或较新的类型表达式。
from __future__ import annotations

# Python 标准库：路径定位、合约代码解析、目录替换、导入路径和提交批次标识。
import pathlib
import re
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from time import perf_counter

# 第三方库分别承担 CLI、表格计算、Arrow 契约、Dataset 分区读写和 Parquet marker 写入。
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
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
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
from a00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约浏览
# 
# Notebook 中展示上游交易日历与本环节品种日历的权威 Schema，并提供所选分区的有界只读样例。导出脚本运行时跳过交互展示。

# ### 局部流程：Schema 与样例浏览
# 
# 展示上游交易日历和本环节品种日历的契约与有界样例。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"Notebook 交互环境？"} -->|是| B["展示权威 Schema"]
#     B --> C["按交互选择读取有界样例"]
#     A -->|否| D["跳过展示"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from a00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA, # dim_trade_calendar 中国期货交易日历维度表
        FUTURES_VARIETY_CALENDAR_SCHEMA, # dim_futures_variety_calendar 期货品种交易日历维度表
    ], lake_root=settings.futures_lake_root)


# ## 表配置与物理契约检查
# 
# 目标表的表名、主键和 Hive 分区顺序，以及上游表名、分区字段，均从各自的权威 metadata 读取一次并复用。逻辑 Schema 与逐 fragment 检查固定字段、类型、nullable 和身份 metadata；描述性 metadata 允许随代码更新。

# ### 局部流程：读取表配置
# 
# 表名、主键与分区从权威 Schema metadata 读取并复用。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["品种日历 Schema"] --> B["读取目标表名、主键、分区列"]
#     C["交易日历 Schema"] --> D["读取上游表名与分区列"]
#     B --> E["后续读写复用配置"]
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
# 检查函数只在调用时运行；任何契约不匹配直接抛出异常。上游完整业务规则由 b01 保证，这里不重复证明。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A{"调用哪个检查函数？"} -->|单 Schema 检查| B["比较字段及顺序、类型、nullable"]
#     B --> C["比较表名、主键、分区 metadata"]
#     C --> D["返回；描述性 metadata 不阻断"]
#     A -->|逐文件检查| E["从权威 Schema 排除 Hive 分区列"]
#     E --> F["遍历 fragment，读取物理 Schema"]
#     F --> G["调用单 Schema 检查"]
#     G --> H{"还有 fragment？"}
#     H -->|是| F
#     H -->|否| I["全部文件检查完成，返回"]
# ```

# In[4]:


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

# FUTURES_VARIETY_CALENDAR_SCHEMA: dim_futures_variety_calendar 期货品种交易日历维度表


# ## 固定月份合约代码规则
# 
# 正则拆分品种、交割代码和交易所后缀；连续/指数代码通过固定排除集合过滤。这些规则服务于完整合约目录，事实采集白名单不参与筛选。

# ### 局部流程：固定月份合约识别规则
# 
# 本单元格只定义正则与连续代码集合；下图说明 `collect()` 使用这些规则的顺序。维度目录不按事实采集白名单裁剪。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["来源合约代码统一大写"] --> B["正则提取品种、3 或 4 位交割代码、交易所"]
#     B --> C{"成功匹配？"}
#     C -->|否| X["剔除该记录"]
#     C -->|是| D{"交割代码为 8888、9998 或 9999？"}
#     D -->|是| X
#     D -->|否| E["保留固定月份合约，进入目录校验"]
# ```

# In[5]:


# 固定月份合约代码由“品种字母 + 3/4 位交割代码 + 交易所后缀”组成。
# ^ 和 $ 要求整个代码完全匹配；三个命名捕获组可直接转成后续业务字段。
FIXED_CONTRACT = re.compile(r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})\.(?P<exchange_code>[A-Z]+)$")

# 这些交割代码代表连续或指数序列，没有独立上市区间，不能生成固定合约日历格点。
# frozenset 表明该排除集合在运行期间不可修改，并提供常数时间成员判断。
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})


# ## 分区合并、提交与失败回滚
# 
# `commit_partitions()` 按日期闭区间替换完整品种结果，保留同一叶分区中范围外的旧行。触达集合包含新数据和旧目录中的交易所，因此输入为零行也可能需要删除范围内的旧数据，不能直接作为无操作返回。
# 
# 执行顺序沿代码中的逻辑块展开：输入校验 → 路径与物理契约 → 完整叶合并与业务校验 → staging 写入和复读 → 逐叶安装、正式复读与必要的空表 marker → 清理或回滚。输入检查与合并后完整叶检查承担不同职责，均保留。staging 和正式安装只逐文件复读物理契约，并检查主键唯一性与行数；不重复完整业务校验。空表 marker 同样在正式复读通过后才完成提交，失败进入回滚。
# 
# 函数自身报告环节、触达分区数、输入行数和替换行数。每个分区正式复读通过后推进计数；最终 `rows` 和返回值均为输入行数，零行分区仍计入处理进度。`replacement_rows` 包含范围外保留的旧行。
# 
# 物理 Schema 和主键列在安装循环前计算一次，空表 marker 复用同一物理 Schema。正式叶按这份物理 Schema 逐文件检查一次，不再额外检查覆盖相同内容的叶 Dataset Schema。预期分区行数一次分组得到，循环内按键读取；不存在的分区键按 0 行处理。
# 
# 安装与失败恢复共用 `StagedPathTransaction`：环节仍按顺序合并、写入并验收 staging，在事务 `with` 中逐分区安装并正式复读；整组退出成功才完成提交。正式复读抛出异常时，共享事务恢复本次实际移动的目标。删除由合并后的预期行数为零明确决定；应有数据但 staging 缺失会报错。空表 marker 先写入 staging，再由同一事务安装、正式复读和恢复。

# ### 局部流程：按日期区间替换完整叶分区
# 
# 输入在指定闭区间内具有完整替换语义：保留区间外旧行，再放入区间内新行。空输入也可能用于清退区间内旧行，不能直接跳过。完整 dirty 叶的业务规则在写入前校验，staging 与正式复读只承担物理与主键/行数检查。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录提交开始；校验日期范围、输入契约与业务约束"] --> B["检查路径边界；准备 staging、备份与隔离路径"]
#     B --> O["发现现有数据集并确认物理契约"]
#     O --> C["按新旧交易所与请求月份确定触达叶"]
#     C --> D["逐叶：保留区间外旧行，加上区间内完整新行"]
#     D --> E["汇集完整替换叶；统一转换并校验业务规则"]
#     E --> F["写 staging 并复读；空结果保留空 staging"]
#     F --> G["循环前准备物理 Schema、主键与各叶预期行数"]
#     G --> H["共享事务：备份旧叶；按预期行数安装或显式删除"]
#     H --> I["正式逐文件物理检查；主键与行数核对；记录进度"]
#     I --> J{"还有触达叶？"}
#     J -->|是| H
#     J -->|否| K{"整表无 Parquet 且替换结果为空？"}
#     K -->|是| L["staging 写零行 marker；共享事务安装；正式复读"]
#     K -->|否| N["清理临时目录；记录成功；返回输入行数"]
#     L --> N
#     F -.->|staging 失败| Q["清理 staging 并抛出异常；尚未替换正式叶"]
#     H -.->|提交失败| R["共享事务倒序恢复：移除新 marker；隔离新叶、恢复旧叶"]
#     I -.->|提交失败| R
#     L -.->|提交失败| R
#     R --> S["保留隔离证据及未恢复备份；清理 staging；抛出异常"]
# ```
# 
# 共享事务负责安装状态记录、失败恢复与清理；逐分区正式验收和进度仍由本环节负责。

# In[6]:


def commit_partitions(
    incoming_variety_calendar_df: pd.DataFrame,
    lake_root: pathlib.Path,
    start_date: date,
    end_date: date,
) -> int:
    """提交指定日期闭区间内的完整品种交易日历结果。

    Parameters
    ----------
    incoming_variety_calendar_df : pd.DataFrame
        按权威 Schema 生成的完整替换数据，仅包含``[start_date, end_date]`` 范围内的交易日。
    lake_root : pathlib.Path
        本次读写的数据湖根目录，其下应包含 ``silver`` 目录。
    start_date : datetime.date
        本批允许替换的起始交易日，包含该日。
    end_date : datetime.date
        本批允许替换的结束交易日，包含该日。

    Returns
    -------
    int

        本批输入 ``incoming_variety_calendar_df`` 的行数，不是合并后完整 Hive 叶分区的总行数。

    Workflow
    --------
    1. 待提交数据规范化与业务校验
        将外部传入的 Pandas DataFrame 按
        ``FUTURES_VARIETY_CALENDAR_SCHEMA`` 转换为 Arrow 表，
        并检查主键唯一性、请求日期边界、有效合约数量、
        ``source`` 固定值以及 ``year/month`` 分区字段与
        ``trading_date`` 的一致性。

    2. 提交路径与 Hive 分区环境初始化
        构造正式目录、staging、backup 和 failed/quarantine 路径，
        检查所有受管理路径均位于指定 ``silver`` 根目录下，
        并按权威 Schema 创建 Hive partitioning。

    3. 现有正式数据集发现与契约校验
        如果正式湖已经存在该表，则检查逻辑及逐文件物理 Schema，
        要求字段、类型、nullable 和身份 metadata 与权威契约一致；
        描述性 metadata 允许不同，以当前权威 Schema 的说明为准。
        此阶段只确认正式数据集是否可安全参与后续分区替换。

    4. 完整替换分区构造
        根据请求日期范围和新旧数据涉及的交易所确定全部触达分区。
        对每个触达的 ``exchange_code/year/month`` 叶分区，
        保留请求范围之外的旧数据，并用本批完整结果替换请求范围内
        的数据，最终生成本次应完整写入 staging 的分区集合。

    5. staging 写入与复读验收
        将完整替换分区写入独立 staging 目录。
        写入后重新以 Arrow Dataset 读取 staging，
        逐文件检查物理 Schema 与身份 metadata，并检查主键和行数摘要。
        staging 未通过完整验收前不得触碰正式分区。

    6. 正式分区提交
        对每个触达分区，先将现有正式分区移动到 backup，
        再将 staging 中的新分区移动到正式目录。
        若最终正式表为空，则额外写入零行 ``schema.parquet``，
        并从正式文件复读非分区列 Schema、身份 metadata 和零行数。

    7. 正式叶安装确认
        只打开刚安装的叶目录逐文件核对物理 Schema、表身份 metadata，并检查主键和行数，不重新打开或扫描正式根 Dataset。

    8. 失败回滚与临时现场清理
        正式提交或提交后验收发生异常时，
        先隔离已经进入正式目录的新分区，
        再从 backup 恢复旧分区。
        如果回滚本身不完整，则保留 backup/quarantine 现场并抛出
        更高层异常；成功或安全失败后清理不再需要的临时目录。
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
    #
    # 输入：
    # - incoming_variety_calendar_df：调用方生成的本批完整替换数据。
    # - start_date / end_date：本批允许修改的交易日闭区间。
    #
    # 输出：
    # - incoming_variety_calendar_table：按权威 Schema 转换后的 Arrow 表。
    # - incoming_variety_calendar_df：使用 ArrowDtype 统一类型后的 Pandas 计算对象。
    # - 若任一契约或业务条件不满足，则直接抛出异常。
    #
    # 环节：
    # 1. 检查起止日期关系。
    # 2. 按 FUTURES_VARIETY_CALENDAR_SCHEMA 规范化全部逻辑列。
    # 3. 检查本批主键唯一性和请求日期边界。
    # 4. 检查 active_contract_count 和 source 等业务固定约束。
    # 5. 检查 year/month 与 trading_date 的分区一致性。

    if start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    # FUTURES_VARIETY_CALENDAR_SCHEMA dim_futures_variety_calendar 期货品种交易日历维度表
    incoming_variety_calendar_table = pandas_to_arrow(
        incoming_variety_calendar_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    incoming_variety_calendar_df = incoming_variety_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
    # 先按权威 Arrow Schema 选择、排序并转换全部八个逻辑列
    # 这里同时统一 Pandas 扩展类型，避免后续比较受 object dtype 影响

    # 外部传入的 Pandas DataFrame
    #         ↓ pandas_to_arrow
    #           (内置了 _validate_columns，按权威 Schema 校验并规范化)
    #   to_pandas(types_mapper=pd.ArrowDtype)
    # 生成类型统一的 Pandas 计算对象，不重复校验刚生成的 Arrow 表


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

    # 随机 UUID（版本 4）默认表示形式 str(): 'f47ac10b-58cc-4372-a567-0e02b……'
    run_id = uuid.uuid4().hex # .hex 属性用于移除连字符: f47ac10b58cc4372a5670e02b……
    variety_calendar_staging_dir = silver_root / f".c02s-{run_id}"
    variety_calendar_backup_dir = silver_root / f".c02b-{run_id}"
    # 新增数据 临时保存路径 staging: D:/lake/silver/.trade_calendar.staging-a3c91...
    # 正式数据 临时备份路径 backup: D:/lake/silver/.trade_calendar.backup-a3c91...

    # variety_calendar_quarantine_dir 本次提交失败时，用来隔离“已经进入正式目录的新数据”
    variety_calendar_quarantine_dir = silver_root / f".c02q-{run_id}"
    # 新分区从正式目录移到 quarantine
    # 旧分区从 backup 恢复到正式目录

    silver_root.mkdir(parents=True, exist_ok=True) # 确保 silver 目录存在，或在空目录创建项目


    for managed_path in (variety_calendar_path, variety_calendar_staging_dir, variety_calendar_backup_dir, variety_calendar_quarantine_dir):
        # .resolve() 把路径转换为规范化的绝对路径
        # .is_relative_to(silver_root): 判断解析后的路径是否位于 silver_root 内
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


# ## 上游读取、目录校验与交易日展开
# 
# 独立调用 `collect()` 时，函数依次完成上游窄列读取、交易日选择、认证、完整目录请求、固定合约筛选与区间校验、逐交易日计数、最终契约转换。日常入口已完成上游读取和交易日选择，使用 `selected_trading_dates` 传入同一份有序结果，函数直接复用并记录 `upstream_reuse`，不重复读盘、检查或筛选。只读运行同样检查目录响应并转换结果。
# 
# 日志放在对应操作内部，直接调用函数也能观察进度。日期展开每 250 个交易日及最后一个交易日报告累计进度；无活跃合约的日期也计入已处理日期数。采集完成日志只在最终契约转换成功后输出。
# 
# 空结果也先完成类型转换再记录成功。采集异常会记录失败阶段和异常类型，并原样向调用方抛出；不自动重试。

# ### 局部流程：上游选择、完整目录与逐日计数
# 
# 日常入口传入已选日期后直接复用；独立调用才自行打开上游。所有采集日志在本函数内；逐日进度每 250 个交易日及最后一天输出，无活跃合约的日期也计入进度。异常记录当前阶段后原样抛出，不重试。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录采集开始；检查日期参数"] --> B{"传入 selected_trading_dates？"}
#     B -->|是| C["复用日期；记录 upstream_reuse"]
#     B -->|否| D["确认 b01 物理契约；窄列读取并筛选交易日"]
#     C --> E{"交易日非空？"}
#     D --> E
#     E -->|否| Z["返回契约化空表；成功日志；无认证、无 API"]
#     E -->|是| F["认证；get_all_securities：date=None，一次完整目录"]
#     F --> G["检查响应与必需列；筛选固定月份合约"]
#     G --> H["检查代码唯一、固定合约集合非空与上市区间"]
#     H --> I["逐交易日筛选：上市日 ≤ 交易日 ≤ 退市日"]
#     I --> J["按交易所与品种计数；无活跃合约则该日无输出行"]
#     J --> K["记录日期处理进度"]
#     K --> L{"还有交易日？"}
#     L -->|是| I
#     L -->|否| M["汇总、排序、补充字段；完成 Arrow 契约转换"]
#     M --> N["记录采集成功与耗时；返回 DataFrame"]
# ```

# In[7]:


def collect(
    lake_root: pathlib.Path,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    selected_trading_dates: list[date] | None = None,
) -> pd.DataFrame:
    """按正式上游交易日采集并构建品种日历。

    selected_trading_dates 供日常入口复用已从正式上游选出的有序交易日，
    其范围由调用方按 start_date/end_date 确定；传入后不再打开或检查上游。
    独立调用不传此参数，函数自行读取和筛选上游。
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
            # b01 对正式交易日历的主键、连续水位和派生字段承担完整责任。
            # b02 信任这些业务结论，只确认上游物理 Dataset 与权威 Schema 兼容。
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

            # 这里只投影 b02 计算直接需要的两个字段；不复查上游日期唯一性、
            # 自然日连续性或 b01 的其他派生列。显式日期只用于非正式定向测试。
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

            # 上游已经由 b01 正式提交，b02 直接消费其交易日选择结果。
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

        # 最终按主键稳定排序，并再次经过 Arrow 契约转换后返回。
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


# ## 命令行入口与计划
# 
# `main()` 负责参数门禁、默认尾部水位和全历史差异计划。采集与单次提交的日志由各自函数输出；全历史模式可能执行多个日期范围，入口追加整体提交汇总，并在只读、有写入或无变化三类正常退出路径记录运行完成。`write=false` 表示只读完成，`rows` 表示本次结果或待替换行数；真正提交成功由 `commit_partitions()` 的 `committed` 日志表达。
# 
# 环节日志使用既有 worker 支持的行首前缀，状态文件和心跳仍由 operations 管理。默认无新增交易日时保持零 API；显式日期与正式湖写入限制保持不变。
# 
# 入口、计划和完成汇总沿用 b01 的日志块样式：88 个 `=` 分隔线、中英文标题、函数标识和状态字段。采集与逐分区进度保留单行格式，供持续阅读和监控刷新；既有进度前缀仍独占行首。

# ### 局部流程：模式选择与差异提交计划
# 
# 显式日期必须成对且与 `--full` 互斥；显式日期写入仅允许非正式湖。全历史比较以“交易所、品种、交易日”为主键，比较活跃合约数；旧表存在预期集合外主键时直接报错，不静默删除。多个修订区间分别提交，前一区间成功后不会因后一区间失败而整体回滚。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["校验 CLI 与目标湖边界"] --> M{"运行模式"}
#     M -->|默认| D["读取 b02 最大交易日；从 b01 选新增交易日"]
#     D --> E{"有新增交易日？"}
#     E -->|否| Z["up_to_date；无 API；返回"]
#     E -->|是| F["collect：传入同一份已选日期；规划一个尾部区间"]
#     M -->|显式日期| G["检查日期顺序；collect 指定区间"]
#     M -->|全历史| H["collect 全部上游交易日；生成预期主键与合约数"]
#     H --> I["读取并校验现有品种日历；拒绝预期集合外主键"]
#     I --> J["找缺失或计数不同的主键，归并为差异日期"]
#     J --> K{"存在差异日期？"}
#     K -->|否| N["up_to_date；返回"]
#     K -->|是| L["按预期交易日序列合并相邻差异区间"]
#     L --> P["每个差异日期取完整品种结果"]
#     F --> W{"--write？"}
#     G --> W
#     P --> W
#     W -->|否| Q["记录只读完成；返回"]
#     W -->|是| T["逐区间调用 commit_partitions；汇总完成"]
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

    # 默认模式只根据下游最大交易日读取 b01 的尾部交易日；无新增时不认证。
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

    # --full 慢路径消费 b01 已正式提交的完整水位。collect 只打开一次上游，
    # 不再在 main 与 collect 之间重复整表读取、主键和连续日期检查。
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


# ## 执行入口
# 
# 与 b01 一样，Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并用 `standalone_mode=False` 返回 Notebook。当前示例检查 `2026-08-01` 至 `2026-08-15`，未启用 `--write`；存在上游交易日时会请求一次来源目录。需要默认尾部只读检查时可将参数改为 `[]`。
# 
# 直接运行 Python 脚本时读取命令行参数；在 Notebook 中导入同名模块不会触发入口。最后一格列出 `latitude` 环境下的脚本命令，其中 `--write` 用于手动启动默认尾部更新。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 示例检查 2026-08-01 至 2026-08-15，未启用写入；存在上游交易日时会请求一次目录。流程图本身不执行代码。
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
        prog_name="b02_futures_variety_calendar",
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
# python 02_Futures_Lakehouse\a01_Futures_Market_Data\b02_futures_variety_calendar.py --write

