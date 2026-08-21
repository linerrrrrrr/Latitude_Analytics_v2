#!/usr/bin/env python
# coding: utf-8

# # c02_futures_variety_calendar
# 
# 目标表：`dim_futures_variety_calendar`。将上游交易日历与 JQData 完整固定月份合约目录的上市区间求交，生成全部品种的交易日格点。事实采集白名单不参与本维度表构建。
# 
# Notebook 是唯一业务源；同名 Python 文件由默认 PythonExporter 生成。数据写入由 `--write` 显式启用。

# ## 自动更新与正式湖写入边界
# 
# 自动模式遵循：`上游有效品种—交易日格点 − 下游已经完整落盘的格点 = 本次待补格点`。下游为空时差集就是全量；下游已有数据时，同时识别尾部新增格点、历史缺失主键和有效合约数量不一致的未完整格点。
# 
# `dim_trade_calendar` 的完整业务语义由 c01 在提交链路中负责。c02 信任已正式提交的上游表，只检查精确 Schema/metadata、`calendar_date` 主键唯一和请求范围逐自然日覆盖；不重复校验 c01 的来源、日历名称、时区、生效时间、派生字段或 JQData 一致性。
# 
# 提交时按缺口涉及的完整交易日范围重建这些日期，避免只写一部分品种后把该日误判为完整。正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 唯一指定；显式日期只允许检查，或者写入非正式测试湖。

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

# In[2]:


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
        project_root = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


from config.settings import settings
from config.data_contracts import (
    FUTURES_VARIETY_CALENDAR_SCHEMA,  # dim_futures_variety_calendar 期货品种交易日历维度表
    TRADE_CALENDAR_SCHEMA,  # dim_trade_calendar 中国期货交易日历维度表
    arrow_to_pandas,
    pandas_to_arrow,
    validate_arrow_table,
)
# 两张具名 Schema 分别约束当前产出和直接上游；
# 转换函数统一 Arrow/Pandas 边界


# In[3]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        TRADE_CALENDAR_SCHEMA, # dim_trade_calendar 中国期货交易日历维度表
        FUTURES_VARIETY_CALENDAR_SCHEMA, # dim_futures_variety_calendar 期货品种交易日历维度表
    ])


# In[4]:


# 表名、Hive 分区顺序和业务主键只在权威 Schema metadata 中定义。
# 本模块初始化时各读取一次；后续路径、分区读写和唯一性检查只复用这些结果。
TABLE_NAME = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
PARTITION_COLUMNS = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
PRIMARY_KEY = FUTURES_VARIETY_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")

# FUTURES_VARIETY_CALENDAR_SCHEMA: dim_futures_variety_calendar 期货品种交易日历维度表


# In[5]:


# 固定月份合约代码由“品种字母 + 3/4 位交割代码 + 交易所后缀”组成。
# ^ 和 $ 要求整个代码完全匹配；三个命名捕获组可直接转成后续业务字段。
FIXED_CONTRACT = re.compile(
    r"^(?P<underlying_code>[A-Z]+)(?P<delivery_code>\d{3,4})\.(?P<exchange_code>[A-Z]+)$"
)

# 这些交割代码代表连续或指数序列，没有独立上市区间，不能生成固定合约日历格点。
# frozenset 表明该排除集合在运行期间不可修改，并提供常数时间成员判断。
CONTINUOUS_DELIVERY_CODES = frozenset({"8888", "9998", "9999"})


# ## 分区合并、提交与失败回滚

# In[ ]:


def commit_partitions(
    frame: pd.DataFrame,
    lake_root: pathlib.Path,
    start_date: date,
    end_date: date,
) -> int:

    if start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    # FUTURES_VARIETY_CALENDAR_SCHEMA dim_futures_variety_calendar 期货品种交易日历维度表
    incoming_table = pandas_to_arrow(
        frame.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    incoming_df = arrow_to_pandas(
        incoming_table,
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    # 先按权威 Arrow Schema 选择、排序并转换全部八个逻辑列
    # 这里同时统一 Pandas 扩展类型，避免后续比较受 object dtype 影响

    # 外部传入的 Pandas DataFrame
    #         ↓ pandas_to_arrow
    #           (内置了 _validate_columns，按权威 Schema 校验并规范化)
    #   arrow_to_pandas
    # 生成类型统一的 Pandas 计算对象


    # 本批自身必须满足主键唯一和请求日期边界
    if incoming_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("待提交数据的主键不唯一。")
    if any(
        trading_date < start_date or trading_date > end_date
        for trading_date in incoming_df["trading_date"]
    ):
        raise ValueError("待提交数据包含请求日期范围之外的交易日。")

    # 表级固定值在触碰正式目录前完成验证
    # active_contract_count 该品种当日上市区间内的固定月份合约数
    if (incoming_df["active_contract_count"] <= 0).any():
        raise ValueError("该品种当日上市区间内的固定月份合约数 active_contract_count 必须全部大于 0。")
    if not incoming_df.empty and not incoming_df["source"].eq(
        "JQData_get_all_securities+dim_trade_calendar"
    ).all():
        raise ValueError("source 与数据契约不一致。")

    # 分区年月必须可以从交易日无歧义复算
    if any(
        trading_date.year != year or trading_date.month != month
        for trading_date, year, month in zip(
            incoming_df["trading_date"],
            incoming_df["year"],
            incoming_df["month"],
            strict=True,
        )
    ):
        raise ValueError("year/month 分区列与 trading_date 不一致。")

    # 所有临时、备份和隔离路径都必须位于本次指定湖的 silver 根目录
    silver_root = lake_root.resolve() / "silver"
    target_path = silver_root / TABLE_NAME

    # 随机 UUID（版本 4）默认表示形式 str(): 'f47ac10b-58cc-4372-a567-0e02b……'
    run_id = uuid.uuid4().hex # .hex 属性用于移除连字符: f47ac10b58cc4372a5670e02b……
    staging_path = silver_root / f".{TABLE_NAME}.staging-{run_id}"
    backup_path = silver_root / f".{TABLE_NAME}.backup-{run_id}"
    # 新增数据 临时保存路径 staging: D:/lake/silver/.trade_calendar.staging-a3c91...
    # 正式数据 临时备份路径 backup: D:/lake/silver/.trade_calendar.backup-a3c91...

    # quarantine_path 本次提交失败时，用来隔离“已经进入正式目录的新数据”
    quarantine_path = silver_root / f".{TABLE_NAME}.failed-{run_id}"
    # 新分区从正式目录移到 quarantine
    # 旧分区从 backup 恢复到正式目录

    silver_root.mkdir(parents=True, exist_ok=True) # 确保 silver 目录存在，或在空目录创建项目


    for managed_path in (target_path, staging_path, backup_path, quarantine_path):
        # .resolve() 把路径转换为规范化的绝对路径
        # .is_relative_to(silver_root): 判断解析后的路径是否位于 silver_root 内
        if not managed_path.resolve().is_relative_to(silver_root):
            raise ValueError(f"数据集路径越出 silver 根目录：{managed_path}")

    # 三个分区列写进 Hive 目录名，不重复保存在分区内的 Parquet 载荷中。
    # 读取时必须传入同一分区 Schema，才能恢复完整八列逻辑表。
    hive_partitioning = ds.partitioning(
        pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
        flavor="hive",
    )

    # 读取旧表时先精确核对 Schema/metadata，避免转换过程掩盖契约漂移。
    target_dataset = None
    if target_path.is_dir() and any(target_path.rglob("*.parquet")):
        target_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=hive_partitioning,
        )
        target_schema = pa.schema(
            [target_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
            metadata=target_dataset.schema.metadata,
        )
        if not target_schema.equals(FUTURES_VARIETY_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError("现有正式数据集 Schema/metadata 与契约不一致。")

    # 请求范围内可能需要删除已失效品种，因此不能只看本批出现的交易所
    # 即使本批在某交易所为零行，也要从已有目录发现该交易所并触达其对应月份
    exchange_codes = set(incoming_df["exchange_code"].dropna().astype(str)) # 取得新数据中的交易所
    if target_path.is_dir():
        exchange_codes.update(
            partition_path.name.split("=", 1)[1]
            for partition_path
            in target_path.glob("exchange_code=*") # 从 Hive 目录发现旧交易所
            if partition_path.is_dir()
        )

    # 触达集合是“全部相关交易所 × 请求跨越的每个自然年月”
    touched_partitions = [] # 完整 Hive 叶分区清单
    partition_year = start_date.year
    partition_month = start_date.month
    while (partition_year, partition_month) <= (end_date.year, end_date.month):
        for exchange_code in sorted(exchange_codes):
            touched_partitions.append((exchange_code, partition_year, partition_month))
        if partition_month == 12:
            partition_year += 1
            partition_month = 1
        else:
            partition_month += 1

    # 同一月分区可能同时包含请求范围内外的日期。
    # 因此先保留范围外旧行，再与范围内的本批完整结果合并。
    complete_partition_frames = []
    for exchange_code, year, month in touched_partitions:
        partition_mask = (
            incoming_df["exchange_code"].eq(exchange_code)
            & incoming_df["year"].eq(year)
            & incoming_df["month"].eq(month)
        )
        replacement_df = incoming_df.loc[partition_mask, FUTURES_VARIETY_CALENDAR_SCHEMA.names]
        if target_dataset is None:
            retained_df = incoming_df.iloc[0:0].copy()
        else:
            existing_table = target_dataset.to_table(
                columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
                filter=(ds.field("exchange_code") == exchange_code)
                & (ds.field("year") == year)
                & (ds.field("month") == month)
            )
            existing_df = arrow_to_pandas(
                existing_table,
                FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            retained_df = existing_df.loc[
                (existing_df["trading_date"] < start_date)
                | (existing_df["trading_date"] > end_date),
                FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            ]
        complete_partition_df = pd.concat(
            [retained_df, replacement_df],
            ignore_index=True,
        )
        if not complete_partition_df.empty:
            complete_partition_frames.append(complete_partition_df)

    if complete_partition_frames:
        staged_df = pd.concat(complete_partition_frames, ignore_index=True)
        staged_df = staged_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    else:
        staged_df = incoming_df.iloc[0:0].copy()
    staged_table = pandas_to_arrow(
        staged_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )
    if staged_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("合并后的触达分区主键不唯一。")

    # staging 完整写入并复读通过前，不触碰正式分区。
    # write_dataset 会生成 exchange_code=.../year=.../month=.../part-*.parquet。
    staging_path.mkdir(parents=True, exist_ok=False)
    try:
        if len(staged_table):
            ds.write_dataset(
                staged_table,
                staging_path,
                format="parquet",
                partitioning=hive_partitioning,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )
            staged_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=hive_partitioning,
            )
            staged_schema = pa.schema(
                [staged_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
                metadata=staged_dataset.schema.metadata,
            )
            if not staged_schema.equals(FUTURES_VARIETY_CALENDAR_SCHEMA, check_metadata=True):
                raise TypeError("staging Schema/metadata 与契约不一致。")
            checked_table = validate_arrow_table(
                staged_dataset.to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names),
                FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            if len(checked_table) != len(staged_table):
                raise ValueError("staging 行数检查失败。")
            if (
                checked_table.select(PRIMARY_KEY)
                .to_pandas()
                .duplicated(PRIMARY_KEY)
                .any()
            ):
                raise ValueError("staging 主键检查失败。")
    except Exception:
        shutil.rmtree(staging_path, ignore_errors=True)
        raise

    # 逐分区提交：先把正式旧分区移到本批备份，再把 staging 新分区移入。
    # 每次移动都登记路径，后续任何检查失败都可以按相反顺序回滚。
    moved_partitions = []
    backup_path.mkdir(parents=True, exist_ok=False)
    target_path.mkdir(parents=True, exist_ok=True)
    commit_succeeded = False
    marker_path = target_path / "schema.parquet"
    marker_created = False
    try:
        for exchange_code, year, month in touched_partitions:
            relative_path = pathlib.Path(
                f"exchange_code={exchange_code}",
                f"year={year}",
                f"month={month}",
            )
            source_path = staging_path / relative_path
            destination_path = target_path / relative_path
            saved_path = backup_path / relative_path
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            saved_path.parent.mkdir(parents=True, exist_ok=True)
            if destination_path.exists():
                shutil.move(str(destination_path), str(saved_path))
            moved_partitions.append((relative_path, destination_path, saved_path))
            if source_path.is_dir():
                shutil.move(str(source_path), str(destination_path))

        # 空库或清空最后一个分区后仍要保留可读取的表契约。
        # 根目录 marker 没有 Hive 路径值，所以物理 Schema 只写五个非分区列。
        if not any(target_path.rglob("*.parquet")):
            file_schema = pa.schema(
                [
                    FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=FUTURES_VARIETY_CALENDAR_SCHEMA.metadata,
            )
            marker_created = True
            pq.write_table(pa.Table.from_batches([], schema=file_schema), marker_path)

        # 从正式根目录重新构造 dataset：文件列与目录分区列合并后必须精确等于权威 Schema。
        if target_path.is_dir() and any(target_path.rglob("*.parquet")):
            committed_dataset = ds.dataset(
                target_path,
                format="parquet",
                partitioning=hive_partitioning,
            )
            committed_schema = pa.schema(
                [committed_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
                metadata=committed_dataset.schema.metadata,
            )
            if not committed_schema.equals(FUTURES_VARIETY_CALENDAR_SCHEMA, check_metadata=True):
                raise TypeError("正式数据集 Schema/metadata 与契约不一致。")
            for exchange_code, year, month in touched_partitions:
                committed_partition_table = committed_dataset.to_table(
                    columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names,
                    filter=(ds.field("exchange_code") == exchange_code)
                    & (ds.field("year") == year)
                    & (ds.field("month") == month)
                )
                checked_partition_table = validate_arrow_table(
                    committed_partition_table,
                    FUTURES_VARIETY_CALENDAR_SCHEMA,
                )
                expected_row_count = len(
                    staged_df.loc[
                        staged_df["exchange_code"].eq(exchange_code)
                        & staged_df["year"].eq(year)
                        & staged_df["month"].eq(month)
                    ]
                )
                if len(checked_partition_table) != expected_row_count:
                    raise ValueError(
                        f"正式分区 {exchange_code}/{year}/{month} 行数检查失败。"
                    )
                if (
                    checked_partition_table.select(PRIMARY_KEY)
                    .to_pandas()
                    .duplicated(PRIMARY_KEY)
                    .any()
                ):
                    raise ValueError(
                        f"正式分区 {exchange_code}/{year}/{month} 主键检查失败。"
                    )
        elif len(staged_table):
            raise FileNotFoundError("提交后未找到正式 Parquet 分区。")
        commit_succeeded = True
    except Exception as commit_error:
        # 回滚本批已经移动的分区：先隔离新分区，再把旧分区移回原位。
        # 无法完整恢复时保留备份和隔离目录，避免为了清理现场继续破坏证据。
        if marker_created and marker_path.exists():
            marker_path.unlink()
        rollback_errors = []
        for relative_path, destination_path, saved_path in reversed(moved_partitions):
            try:
                if destination_path.exists():
                    isolated_path = quarantine_path / relative_path
                    isolated_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(destination_path), str(isolated_path))
                if saved_path.exists():
                    destination_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(saved_path), str(destination_path))
            except Exception as rollback_error:
                rollback_errors.append(
                    f"{relative_path}: {type(rollback_error).__name__}: {rollback_error}"
                )
        if rollback_errors:
            raise RuntimeError(
                f"分区提交失败且回滚不完整；恢复副本保留在 {backup_path}；"
                f"回滚错误：{rollback_errors}"
            ) from commit_error
        if quarantine_path.exists() and any(quarantine_path.rglob("*")):
            raise RuntimeError(
                f"分区提交失败；旧分区已恢复，新分区隔离在 {quarantine_path}。"
            ) from commit_error
        raise
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)
        if commit_succeeded or not any(backup_path.rglob("*.parquet")):
            shutil.rmtree(backup_path, ignore_errors=True)
        if quarantine_path.exists() and not any(quarantine_path.rglob("*")):
            shutil.rmtree(quarantine_path, ignore_errors=True)

    return len(incoming_table)


# ## 上游日历读取与完整合约目录展开

# In[ ]:


def collect(lake_root: pathlib.Path, start_date: date, end_date: date) -> pd.DataFrame:
    if start_date > end_date:
        raise ValueError("起始日期不得晚于结束日期。")

    # c01 负责交易日历的完整业务质检；c02 只验证自身直接依赖的消费边界。
    # 请求范围必须逐自然日覆盖，且上游目录和精确 Schema 要在 API 认证前确认。
    trade_calendar_path = lake_root.resolve() / "silver" / "dim_trade_calendar"
    if not trade_calendar_path.is_dir():
        raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
    trade_partitioning = ds.partitioning(
        pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
        flavor="hive",
    )
    trade_dataset = ds.dataset(
        trade_calendar_path,
        format="parquet",
        partitioning=trade_partitioning,
    )
    trade_dataset_schema = pa.schema(
        [trade_dataset.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names],
        metadata=trade_dataset.schema.metadata,
    )
    if not trade_dataset_schema.equals(TRADE_CALENDAR_SCHEMA, check_metadata=True):
        raise TypeError("上游交易日历 Schema/metadata 与契约不一致。")

    # 此处读取请求范围内的全部自然日，不能提前只过滤 is_trading_day=true。
    # 否则下游请求超过上游最大日期时，只会得到一个看似正常的交易日前缀。
    trade_table = trade_dataset.to_table(
        columns=TRADE_CALENDAR_SCHEMA.names,
        filter=(ds.field("calendar_date") >= start_date)
        & (ds.field("calendar_date") <= end_date),
    )
    trade_df = arrow_to_pandas(trade_table, TRADE_CALENDAR_SCHEMA)
    if trade_df.duplicated(["calendar_date"]).any():
        raise ValueError("上游交易日历 calendar_date 主键不唯一。")

    # 通过逐日列表完全相等，同时识别起止缺口和范围内部缺口。
    calendar_dates = sorted(trade_df["calendar_date"].tolist())
    expected_dates = pd.date_range(start_date, end_date, freq="D").date.tolist()
    if calendar_dates != expected_dates:
        raise ValueError("请求日期范围未被上游交易日历逐自然日完整覆盖。")

    # 确认自然日水位完整后，才抽取真正需要展开品种格点的交易日。
    trading_dates = sorted(
        trade_df.loc[trade_df["is_trading_day"].eq(True), "calendar_date"].tolist()
    )
    if not trading_dates:
        return arrow_to_pandas(
            pa.Table.from_batches([], schema=FUTURES_VARIETY_CALENDAR_SCHEMA),
            FUTURES_VARIETY_CALENDAR_SCHEMA,
        )

    # 只在存在上游交易日时认证并查询，纯休市范围不会消耗 JQData 调用。
    from config.jqdata_connection import authenticate_jqdata

    jqdata = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)

    # 原始 API 契约：索引是标准合约代码；列包含 display_name、name、
    # start_date、end_date、type。date=None 刻意请求完整历史目录。
    # 本维度只需要代码和上市区间，且不按事实采集白名单裁剪目录。
    securities_df = jqdata.get_all_securities(["futures"], date=None)
    if not isinstance(securities_df, pd.DataFrame):
        raise TypeError(
            f"get_all_securities 应返回 pandas.DataFrame，实际为 {type(securities_df).__name__}。"
        )

    # 把 API 索引显式转成普通列，后续才能校验重复代码并解析交易所。
    securities_df = securities_df.rename_axis("contract_code").reset_index()
    required_columns = {"contract_code", "start_date", "end_date"}
    if not required_columns <= set(securities_df.columns):
        raise ValueError(
            f"get_all_securities 缺少列：{sorted(required_columns - set(securities_df.columns))}"
        )

    # 名称与 type 是原始目录描述字段，不属于目标表粒度，因此不写入湖。
    # 代码统一大写后，用一个正则同时解析品种、交割代码与交易所后缀。
    securities_df["contract_code"] = (
        securities_df["contract_code"].astype("string").str.upper()
    )
    parsed_codes_df = securities_df["contract_code"].str.extract(FIXED_CONTRACT)
    securities_df = pd.concat([securities_df, parsed_codes_df], axis=1)

    # 无法匹配固定月份格式的代码先剔除；能匹配但属于连续序列的数字代码再显式排除。
    securities_df = securities_df.dropna(
        subset=["underlying_code", "delivery_code", "exchange_code"]
    )
    securities_df = securities_df.loc[
        ~securities_df["delivery_code"].isin(CONTINUOUS_DELIVERY_CODES)
    ].copy()

    # 完成格式过滤后，剩余固定月份合约代码必须唯一且集合不得为空。
    if securities_df.duplicated(["contract_code"]).any():
        duplicate_codes = sorted(
            securities_df.loc[
                securities_df.duplicated(["contract_code"], keep=False),
                "contract_code",
            ].unique()
        )
        raise ValueError(f"get_all_securities 合约代码重复：{duplicate_codes}")
    if securities_df.empty:
        raise ValueError("get_all_securities 未返回任何固定月份期货合约。")

    # 上市/结束日期统一转成 datetime.date；区间两端在后续判断中都包含。
    securities_df["start_date"] = pd.to_datetime(
        securities_df["start_date"],
        errors="raise",
    ).dt.date
    securities_df["end_date"] = pd.to_datetime(
        securities_df["end_date"],
        errors="raise",
    ).dt.date
    if securities_df[["start_date", "end_date"]].isna().any().any():
        raise ValueError("固定月份合约的 start_date/end_date 包含空值。")
    if (securities_df["start_date"] > securities_df["end_date"]).any():
        raise ValueError("固定月份合约存在 start_date 晚于 end_date 的记录。")

    # 对每个上游交易日选择 start_date <= trading_date <= end_date 的固定合约。
    # 再按交易所—品种计数；计数大于零才产生一行品种日历格点。
    updated_at = datetime.now(timezone.utc)
    daily_frames = []
    for trading_date in trading_dates:
        active_df = securities_df.loc[
            (securities_df["start_date"] <= trading_date)
            & (securities_df["end_date"] >= trading_date)
        ]
        daily_df = (
            active_df.groupby(["exchange_code", "underlying_code"])
            .size()
            .rename("active_contract_count")
            .reset_index()
        )
        if daily_df.empty:
            continue

        # 这些列由当前交易日和本批运行上下文生成，不来自原始 API。
        daily_df["trading_date"] = trading_date
        daily_df["active_contract_count"] = daily_df["active_contract_count"].astype(
            "int16"
        )
        daily_df["source"] = "JQData_get_all_securities+dim_trade_calendar"
        daily_df["updated_at"] = updated_at
        daily_df["year"] = trading_date.year
        daily_df["month"] = trading_date.month
        daily_frames.append(
            daily_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names]
        )

    # 最终按主键稳定排序，并再次经过 Arrow 契约转换后返回。
    if daily_frames:
        result_df = pd.concat(daily_frames, ignore_index=True)
        result_df = result_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    else:
        result_df = pd.DataFrame(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names)
    return arrow_to_pandas(
        pandas_to_arrow(result_df, FUTURES_VARIETY_CALENDAR_SCHEMA),
        FUTURES_VARIETY_CALENDAR_SCHEMA,
    )


# ## 命令行入口

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
    # 未覆盖 --lake-root 时使用 .env 中唯一的正式湖；显式路径主要供临时湖测试。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()
    has_explicit_dates = start_date is not None or end_date is not None
    if (start_date is None) != (end_date is None):
        raise click.UsageError("--start-date 与 --end-date 必须同时提供。")
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

        frame = collect(
            resolved_lake_root,
            requested_start_date,
            requested_end_date,
        )
        click.echo(f"api_success: mode=explicit; rows={len(frame)}")
        if write:
            committed_rows = commit_partitions(
                frame,
                resolved_lake_root,
                requested_start_date,
                requested_end_date,
            )
            click.echo(
                f"committed: mode=explicit_non_formal; rows={committed_rows}"
            )
        return

    # 自动模式信任 c01 已正式提交的业务语义，只验证 c02 直接依赖的上游水位边界。
    trade_calendar_path = (
        resolved_lake_root / "silver" / "dim_trade_calendar"
    )
    if not trade_calendar_path.is_dir():
        raise FileNotFoundError(f"缺少上游交易日历：{trade_calendar_path}")
    trade_partitioning = ds.partitioning(
        pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
        flavor="hive",
    )
    trade_dataset = ds.dataset(
        trade_calendar_path,
        format="parquet",
        partitioning=trade_partitioning,
    )
    trade_dataset_schema = pa.schema(
        [trade_dataset.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names],
        metadata=trade_dataset.schema.metadata,
    )
    if not trade_dataset_schema.equals(
        TRADE_CALENDAR_SCHEMA,
        check_metadata=True,
    ):
        raise TypeError("上游交易日历 Schema/metadata 与契约不一致。")

    trade_table = validate_arrow_table(
        trade_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
        TRADE_CALENDAR_SCHEMA,
    )
    trade_df = arrow_to_pandas(trade_table, TRADE_CALENDAR_SCHEMA)
    if trade_df.empty:
        raise ValueError("上游交易日历为空。")
    if trade_df.duplicated(["calendar_date"]).any():
        raise ValueError("上游交易日历 calendar_date 主键不唯一。")

    calendar_dates = sorted(trade_df["calendar_date"].tolist())
    expected_calendar_dates = pd.date_range(
        calendar_dates[0],
        calendar_dates[-1],
        freq="D",
    ).date.tolist()
    if calendar_dates != expected_calendar_dates:
        raise ValueError("上游交易日历没有逐自然日连续覆盖其水位。")

    expected_frame = collect(
        resolved_lake_root,
        calendar_dates[0],
        calendar_dates[-1],
    )
    expected_key_set = set(
        expected_frame[PRIMARY_KEY].itertuples(index=False, name=None)
    )
    expected_count_by_key = dict(
        zip(
            expected_frame[PRIMARY_KEY].itertuples(index=False, name=None),
            expected_frame["active_contract_count"].astype(int),
            strict=True,
        )
    )

    # 下游只有通过精确契约和表级约束的行，才有资格计入“已经完整落盘”。
    target_path = resolved_lake_root / "silver" / TABLE_NAME
    hive_partitioning = ds.partitioning(
        pa.schema([FUTURES_VARIETY_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]),
        flavor="hive",
    )
    existing_df = expected_frame.iloc[0:0].copy()
    if target_path.is_dir() and any(target_path.rglob("*.parquet")):
        existing_dataset = ds.dataset(
            target_path,
            format="parquet",
            partitioning=hive_partitioning,
        )
        existing_schema = pa.schema(
            [existing_dataset.schema.field(name) for name in FUTURES_VARIETY_CALENDAR_SCHEMA.names],
            metadata=existing_dataset.schema.metadata,
        )
        if not existing_schema.equals(FUTURES_VARIETY_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError("现有品种日历 Schema/metadata 与契约不一致。")
        existing_table = validate_arrow_table(
            existing_dataset.to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names),
            FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        existing_df = arrow_to_pandas(
            existing_table,
            FUTURES_VARIETY_CALENDAR_SCHEMA,
        )
        if existing_df.duplicated(PRIMARY_KEY).any():
            raise ValueError("现有品种日历主键不唯一。")
        if (existing_df["active_contract_count"] <= 0).any():
            raise ValueError("现有品种日历 active_contract_count 包含非正数。")
        if not existing_df.empty and not existing_df["source"].eq(
            "JQData_get_all_securities+dim_trade_calendar"
        ).all():
            raise ValueError("现有品种日历 source 与契约不一致。")
        if any(
            trading_date.year != year or trading_date.month != month
            for trading_date, year, month in zip(
                existing_df["trading_date"],
                existing_df["year"],
                existing_df["month"],
                strict=True,
            )
        ):
            raise ValueError("现有品种日历分区年月与交易日不一致。")

    existing_key_set = set(
        existing_df[PRIMARY_KEY].itertuples(index=False, name=None)
    )
    extra_key_set = existing_key_set - expected_key_set
    if extra_key_set:
        raise ValueError(
            "现有品种日历包含不属于上游有效格点的主键；"
            f"自动补缺不会静默删除这些行：{sorted(extra_key_set)[:10]}"
        )

    existing_count_by_key = dict(
        zip(
            existing_df[PRIMARY_KEY].itertuples(index=False, name=None),
            existing_df["active_contract_count"].astype(int),
            strict=True,
        )
    )
    incomplete_key_set = {
        key
        for key, expected_count in expected_count_by_key.items()
        if existing_count_by_key.get(key) != expected_count
    }
    if not incomplete_key_set:
        click.echo(
            f"up_to_date: table={TABLE_NAME}; "
            f"upstream_grid_count={len(expected_key_set)}"
        )
        return

    # 一个日期只要缺少任一品种，就以该日的完整上游结果为提交单位。
    incomplete_date_set = {key[2] for key in incomplete_key_set}
    expected_trading_dates = sorted(
        expected_frame["trading_date"].drop_duplicates().tolist()
    )
    update_ranges = []
    range_start = None
    range_end = None
    for trading_date in expected_trading_dates:
        if trading_date in incomplete_date_set:
            if range_start is None:
                range_start = trading_date
            range_end = trading_date
        elif range_start is not None:
            update_ranges.append((range_start, range_end))
            range_start = None
            range_end = None
    if range_start is not None:
        update_ranges.append((range_start, range_end))

    planned_row_count = int(
        expected_frame["trading_date"].isin(incomplete_date_set).sum()
    )
    click.echo(
        f"auto_plan: table={TABLE_NAME}; upstream_grid_count={len(expected_key_set)}; "
        f"complete_grid_count={len(expected_key_set) - len(incomplete_key_set)}; "
        f"missing_or_incomplete_grid_count={len(incomplete_key_set)}; "
        f"replacement_row_count={planned_row_count}; range_count={len(update_ranges)}"
    )

    if write:
        committed_row_count = 0
        for range_start, range_end in update_ranges:
            range_frame = expected_frame.loc[
                (expected_frame["trading_date"] >= range_start)
                & (expected_frame["trading_date"] <= range_end),
                FUTURES_VARIETY_CALENDAR_SCHEMA.names,
            ].reset_index(drop=True)
            committed_row_count += commit_partitions(
                range_frame,
                resolved_lake_root,
                range_start,
                range_end,
            )
        click.echo(
            f"committed: mode=automatic_gap_fill; rows={committed_row_count}"
        )


if __name__ == "__main__":
    main()

