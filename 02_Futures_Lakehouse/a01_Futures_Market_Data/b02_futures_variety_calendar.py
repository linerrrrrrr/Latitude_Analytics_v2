#!/usr/bin/env python
# coding: utf-8

# # b02_futures_variety_calendar
# 
# 目标表：`dim_futures_variety_calendar`。将上游交易日历与 JQData 完整固定月份合约目录的上市区间求交，生成全部品种的交易日格点。事实采集白名单不参与本维度表构建。
# 
# Notebook 是唯一业务源；同名 Python 文件由默认 PythonExporter 生成。数据写入由 `--write` 显式启用。

# ## 自动更新与正式湖写入边界
# 
# 默认模式信任已经正式提交的历史，只读取 b01 中晚于 b02 最大交易日的新增交易日；没有新增交易日时在认证和目录 API 调用之前结束。有新增时只请求一次完整证券目录，并只展开这些新增日期。空湖的最大日期为空，因此同一入口自然完成首次全建。显式 `--full` 保留全历史展开和双向比较慢路径。
# 
# `dim_trade_calendar` 的完整业务语义由 b01 在提交链路中负责。b02 信任已正式提交的上游表，只确认物理 Dataset 存在且 Schema/metadata 兼容，并仅投影 `calendar_date`、`is_trading_day` 两个计算字段；不重复检查上游主键、逐日连续性、来源、日历名称、时区、生效时间、派生字段或 JQData 一致性。
# 
# 默认提交只替换本批新增交易日；`--full` 才按全历史差异范围重建。正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 唯一指定；显式日期与 `--full` 互斥，日期范围只允许检查，或者写入非正式测试湖。

# ## 原始 API 与转换边界
# 
# 本脚本只调用一次 `jqdatasdk.get_all_securities(types=["futures"], date=None)`。`date=None` 表示读取聚宽支持的完整历史期货证券目录，而不是只取某个日期仍在上市的合约。
# 
# 原始返回值是 `pandas.DataFrame`：索引是聚宽标准合约代码（例如 `IC1505.CCFX`），原始列包括 `display_name`、`name`、`start_date`、`end_date`、`type`。本表只消费索引、`start_date` 和 `end_date`；名称与类型是目录描述字段，不进入品种—交易日日历。
# 
# 合约代码先拆成品种代码、交割年月和交易所后缀，只保留固定月份合约。`8888`、`9998`、`9999` 等连续/指数代码明确排除。随后把每张固定合约的闭区间 `[start_date, end_date]` 与上游交易日求交。

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
# 如果整张表暂时为零行，则在表根目录写入零行 `schema.parquet` 保存非分区列 Schema；读取时仍通过同一 Hive partitioning 恢复完整逻辑契约。

# ## 初始化与表配置

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
        project_root = candidate_root
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


# ## Schema 契约呈现

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA, # dim_trade_calendar 中国期货交易日历维度表
        FUTURES_VARIETY_CALENDAR_SCHEMA, # dim_futures_variety_calendar 期货品种交易日历维度表
    ], lake_root=settings.futures_lake_root)


# In[ ]:


# 表名、Hive 分区顺序和业务主键只在权威 Schema metadata 中定义。
# 本模块初始化时各读取一次；后续路径、分区读写和唯一性检查只复用这些结果。
TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")
PRIMARY_KEY_SORT = [(name, "ascending") for name in PRIMARY_KEY]
PHYSICAL_METADATA_KEYS = (b"table_name", b"primary_key", b"partition_columns")


# In[3]:


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


# In[4]:


# 固定月份合约代码由“品种字母 + 3/4 位交割代码 + 交易所后缀”组成。
# ^ 和 $ 要求整个代码完全匹配；三个命名捕获组可直接转成后续业务字段。
FIXED_CONTRACT = re.compile(r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})\.(?P<exchange_code>[A-Z]+)$")

# 这些交割代码代表连续或指数序列，没有独立上市区间，不能生成固定合约日历格点。
# frozenset 表明该排除集合在运行期间不可修改，并提供常数时间成员判断。
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})


# ## 分区合并、提交与失败回滚

# In[ ]:


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
        如果正式湖已经存在该表，则读取 Dataset 的逻辑 Schema
        和 metadata，并要求其与权威 Schema 精确一致。
        此阶段只确认正式数据集是否可安全参与后续分区替换。

    4. 完整替换分区构造
        根据请求日期范围和新旧数据涉及的交易所确定全部触达分区。
        对每个触达的 ``exchange_code/year/month`` 叶分区，
        保留请求范围之外的旧数据，并用本批完整结果替换请求范围内
        的数据，最终生成本次应完整写入 staging 的分区集合。

    5. staging 写入与复读验收
        将完整替换分区写入独立 staging 目录。
        写入后重新以 Arrow Dataset 读取 staging，
        检查物理 Schema、主键和行数摘要。
        staging 未通过完整验收前不得触碰正式分区。

    6. 正式分区提交
        对每个触达分区，先将现有正式分区移动到 backup，
        再将 staging 中的新分区移动到正式目录。
        若最终正式表为空，则额外写入零行 ``schema.parquet``，
        以保留非分区列 Schema 和表级 metadata。

    7. 正式叶安装确认
        只打开刚安装的叶目录核对物理 Schema、表身份 metadata、主键和行数，不重新打开或扫描正式根 Dataset。

    8. 失败回滚与临时现场清理
        正式提交或提交后验收发生异常时，
        先隔离已经进入正式目录的新分区，
        再从 backup 恢复旧分区。
        如果回滚本身不完整，则保留 backup/quarantine 现场并抛出
        更高层异常；成功或安全失败后清理不再需要的临时目录。
    """

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

    # 读取旧表时先精确核对 Schema/metadata，避免转换过程掩盖契约漂移。
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
    replacement_partition_dfs = []
    for exchange_code, year, month in touched_partition_keys:
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


    # =========================================================================
    # 5. staging 写入与复读验收
    # =========================================================================

    # staging 完整写入并复读通过前，不触碰正式分区。
    # write_dataset 会生成 exchange_code=.../year=.../month=.../part-*.parquet。
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
            validated_staged_variety_calendar_table = validate_arrow_table(
                staged_variety_calendar_dataset.to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names),
                FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            staged_replayed_df = validated_staged_variety_calendar_table.to_pandas()
            if staged_replayed_df.duplicated(PRIMARY_KEY).any():
                raise ValueError("staging 主键不唯一。")
            if len(validated_staged_variety_calendar_table) != len(staged_variety_calendar_table):
                raise ValueError("staging 行数检查失败。")
    except Exception:
        shutil.rmtree(variety_calendar_staging_dir, ignore_errors=True)
        raise


    # =========================================================================
    # 6. 正式分区提交
    # =========================================================================

    # 逐分区提交：先把正式旧分区移到本批备份，再把 staging 新分区移入。
    # 每次移动都登记路径，后续任何检查失败都可以按相反顺序回滚。
    moved_partition_records = []
    variety_calendar_backup_dir.mkdir(parents=True, exist_ok=False)
    variety_calendar_path.mkdir(parents=True, exist_ok=True)
    commit_succeeded = False
    schema_marker_path = variety_calendar_path / "schema.parquet"
    schema_marker_created = False
    try:
        for exchange_code, year, month in touched_partition_keys:
            partition_relative_path = pathlib.Path(
                f"exchange_code={exchange_code}",
                f"year={year}",
                f"month={month}",
            )
            staged_partition_path = variety_calendar_staging_dir / partition_relative_path
            formal_partition_path = variety_calendar_path / partition_relative_path
            backup_partition_path = variety_calendar_backup_dir / partition_relative_path
            formal_partition_path.parent.mkdir(parents=True, exist_ok=True)
            backup_partition_path.parent.mkdir(parents=True, exist_ok=True)
            if formal_partition_path.exists():
                shutil.move(str(formal_partition_path), str(backup_partition_path))
            moved_partition_records.append((partition_relative_path, formal_partition_path, backup_partition_path))
            if staged_partition_path.is_dir():
                shutil.move(str(staged_partition_path), str(formal_partition_path))
            expected_partition_row_count = int(
                (
                    staged_variety_calendar_df["exchange_code"].eq(exchange_code)
                    & staged_variety_calendar_df["year"].eq(year)
                    & staged_variety_calendar_df["month"].eq(month)
                ).sum()
            )
            if formal_partition_path.is_dir():
                committed_partition_dataset = ds.dataset(
                    formal_partition_path, format="parquet"
                )
                committed_partition_schema = pa.schema(
                    [
                        committed_partition_dataset.schema.field(name)
                        for name in committed_partition_dataset.schema.names
                    ],
                    metadata=committed_partition_dataset.schema.metadata,
                )
                expected_partition_schema = pa.schema(
                    [
                        FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                        for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
                        if name not in PARTITION_COLUMNS
                    ],
                    metadata=FUTURES_VARIETY_CALENDAR_SCHEMA.metadata,
                )
                validate_compatible_dataset_schema(
                    committed_partition_schema, expected_partition_schema,
                    f"正式分区 {partition_relative_path} ",
                )
                physical_primary_key = [
                    name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS
                ]
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

        # 空库或清空最后一个分区后仍要保留可读取的表契约。
        # 根目录 marker 没有 Hive 路径值，所以物理 Schema 只写五个非分区列。
        if not len(staged_variety_calendar_table) and not any(variety_calendar_path.rglob("*.parquet")):
            schema_marker_file_schema = pa.schema(
                [
                    FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=FUTURES_VARIETY_CALENDAR_SCHEMA.metadata,
            )
            schema_marker_created = True
            pq.write_table(pa.Table.from_batches([], schema=schema_marker_file_schema), schema_marker_path)

        commit_succeeded = True


    # =========================================================================
    # 8. 失败回滚与临时现场清理
    # =========================================================================

    except Exception as commit_error:
        # 回滚本批已经移动的分区：先隔离新分区，再把旧分区移回原位。
        # 无法完整恢复时保留备份和隔离目录，避免为了清理现场继续破坏证据。
        if schema_marker_created and schema_marker_path.exists():
            schema_marker_path.unlink()
        rollback_errors = []
        for partition_relative_path, formal_partition_path, backup_partition_path in reversed(moved_partition_records):
            try:
                if formal_partition_path.exists():
                    quarantine_partition_path = variety_calendar_quarantine_dir / partition_relative_path
                    quarantine_partition_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(formal_partition_path), str(quarantine_partition_path))
                if backup_partition_path.exists():
                    formal_partition_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(backup_partition_path), str(formal_partition_path))
            except Exception as rollback_error:
                rollback_errors.append(
                    f"{partition_relative_path}: {type(rollback_error).__name__}: {rollback_error}"
                )
        if rollback_errors:
            raise RuntimeError(
                f"分区提交失败且回滚不完整；恢复副本保留在 {variety_calendar_backup_dir}；"
                f"回滚错误：{rollback_errors}"
            ) from commit_error
        if variety_calendar_quarantine_dir.exists() and any(variety_calendar_quarantine_dir.rglob("*")):
            raise RuntimeError(
                f"分区提交失败；旧分区已恢复，新分区隔离在 {variety_calendar_quarantine_dir}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(variety_calendar_staging_dir, ignore_errors=True)
        if commit_succeeded or not any(variety_calendar_backup_dir.rglob("*.parquet")):
            shutil.rmtree(variety_calendar_backup_dir, ignore_errors=True)
        if variety_calendar_quarantine_dir.exists() and not any(variety_calendar_quarantine_dir.rglob("*")):
            shutil.rmtree(variety_calendar_quarantine_dir, ignore_errors=True)

    return len(incoming_variety_calendar_table)


# ## 上游日历读取与完整合约目录展开

# In[ ]:


def collect(
    lake_root: pathlib.Path,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    if (start_date is None) != (end_date is None):
        raise ValueError("起始日期与结束日期必须同时提供。")
    if start_date is not None and start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    # b01 对正式交易日历的主键、连续水位和派生字段承担完整责任。
    # b02 信任这些业务结论，只确认上游物理 Dataset 与权威 Schema 兼容。
    trade_calendar_path = lake_root.resolve() / "silver" / "dim_trade_calendar"
    if not trade_calendar_path.is_dir():
        raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
    trade_calendar_partitioning = ds.partitioning(
        pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
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
        trade_calendar_dataset, TRADE_CALENDAR_SCHEMA, ["year"], "上游交易日历 "
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
    if not valid_trading_dates:
        return pa.Table.from_batches(
            [],
            schema=FUTURES_VARIETY_CALENDAR_SCHEMA,
        ).to_pandas(types_mapper=pd.ArrowDtype)

    # 只在存在上游交易日时认证并查询，纯休市范围不会消耗 JQData 调用。
    from config.jqdata_connection import authenticate_jqdata

    jqdata_client = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)

    # 原始 API 契约：索引是标准合约代码；列包含 display_name、name、
    # start_date、end_date、type。date=None 刻意请求完整历史目录。
    # 本维度只需要代码和上市区间，且不按事实采集白名单裁剪目录。
    futures_contract_catalog_df = jqdata_client.get_all_securities(["futures"], date=None)
    if not isinstance(futures_contract_catalog_df, pd.DataFrame):
        raise TypeError(
            f"get_all_securities 应返回 pandas.DataFrame，实际为 {type(futures_contract_catalog_df).__name__}。"
        )

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

    # 对每个上游交易日选择 start_date <= trading_date <= end_date 的固定合约。
    # 再按交易所—品种计数；计数大于零才产生一行品种日历格点。
    batch_updated_at = datetime.now(timezone.utc)
    daily_variety_calendar_dfs = []
    for trading_date in valid_trading_dates:
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
    collected_variety_calendar_table = pandas_to_arrow(collected_variety_calendar_df, FUTURES_VARIETY_CALENDAR_SCHEMA)
    return collected_variety_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)


# ## 命令行入口

# In[ ]:


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
        click.echo(f"api_success: mode=explicit; rows={len(requested_variety_calendar_df)}")
        if write:
            committed_row_count = commit_partitions(
                requested_variety_calendar_df,
                resolved_lake_root,
                requested_start_date,
                requested_end_date,
            )
            click.echo(
                f"committed: mode=explicit_non_formal; rows={committed_row_count}"
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

        trade_calendar_path = resolved_lake_root / "silver" / TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
        if not trade_calendar_path.is_dir():
            raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
        trade_calendar_partitioning = ds.partitioning(
            pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
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
            trade_calendar_dataset, TRADE_CALENDAR_SCHEMA, ["year"], "上游交易日历 "
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
            return

        pending_start_date = new_trading_dates[0]
        pending_end_date = new_trading_dates[-1]
        pending_variety_calendar_df = collect(
            resolved_lake_root,
            pending_start_date,
            pending_end_date,
        )
        click.echo(
            f"auto_plan: table={TABLE_NAME}; mode=automatic_tail; "
            f"new_trading_date_count={len(new_trading_dates)}; "
            f"rows={len(pending_variety_calendar_df)}; api_calls=1"
        )
        if write:
            committed_row_count = commit_partitions(
                pending_variety_calendar_df,
                resolved_lake_root,
                pending_start_date,
                pending_end_date,
            )
            click.echo(
                f"committed: mode=automatic_tail; rows={committed_row_count}"
            )
        return

    # --full 慢路径消费 b01 已正式提交的完整水位。collect 只打开一次上游，
    # 不再在 main 与 collect 之间重复整表读取、主键和连续日期检查。
    expected_variety_calendar_df = collect(resolved_lake_root)
    expected_primary_key_set = set(
        expected_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None)
    )
    expected_contract_count_by_key = dict(
        zip(
            expected_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None),
            expected_variety_calendar_df["active_contract_count"].astype(int),
            strict=True,
        )
    )

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

    existing_primary_key_set = set(
        existing_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None)
    )
    out_of_watermark_primary_key_set = existing_primary_key_set - expected_primary_key_set
    if out_of_watermark_primary_key_set:
        raise ValueError(
            "现有品种日历包含不属于上游有效格点的主键；"
            f"自动补缺不会静默删除这些行：{sorted(out_of_watermark_primary_key_set)[:10]}"
        )

    existing_contract_count_by_key = dict(
        zip(
            existing_variety_calendar_df[PRIMARY_KEY].itertuples(index=False, name=None),
            existing_variety_calendar_df["active_contract_count"].astype(int),
            strict=True,
        )
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
        f"full_plan: table={TABLE_NAME}; upstream_grid_count={len(expected_primary_key_set)}; "
        f"complete_grid_count={len(expected_primary_key_set) - len(incomplete_primary_key_set)}; "
        f"missing_or_incomplete_grid_count={len(incomplete_primary_key_set)}; "
        f"replacement_row_count={replacement_row_count}; range_count={len(update_date_ranges)}"
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
            f"committed: mode=full_reconciliation; rows={committed_row_count}"
        )


# In[ ]:


if __name__ == "__main__":
    main()

