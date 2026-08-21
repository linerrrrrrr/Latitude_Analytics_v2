#!/usr/bin/env python
# coding: utf-8

# # c01_trade_calendar
# 
# 目标表：dim_trade_calendar。自然日与 JQData 交易日集合。

# ## 自动更新与正式湖写入边界
# 
# 自动模式遵循：`当前有效自然日及其最新交易状态 − 下游已经完整落盘且状态一致的自然日 = 本次待更新格点`。每次自动运行都重新取得完整有效区间的 JQData 交易日集合，因此同一入口既能从空目录创建全量表，也能补尾部、历史内部缺口和历史交易日状态修订。
# 
# 正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 唯一指定。命令不传日期时才允许通过 `--write` 更新正式湖；一旦显式传入 `--start-date/--end-date`，只允许检查，或者写入另行指定的非正式测试湖。

# ## 初始化与表配置

# ## Schema 契约交互浏览

# In[19]:


from __future__ import annotations

# Python 标准库：路径定位、合约代码解析、目录替换、导入路径和提交批次标识。
import pathlib
import shutil
import sys
import uuid
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

# 按项目统一标记从任意工作目录定位仓库根目录。
project_markers = ['.git', '.env', 'config/settings.py']
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all(((candidate_root / marker).exists() for marker in project_markers)):
        sys.path.insert(0, str(candidate_root))
        project_root = candidate_root
        break
else:
    raise RuntimeError('未找到项目根目录')

# 第三方库分别承担 CLI、表格计算、Arrow 契约、Dataset 分区读写和 Parquet marker 写入。
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

# 环境配置与 Arrow Schema 均从项目权威入口读取。
from config.settings import settings
from config.data_contracts import (
    TRADE_CALENDAR_SCHEMA,    # 中国期货交易日历的权威 Arrow Schema
    pandas_to_arrow,    # 将列及顺序匹配的 Pandas DataFrame 安全转换为契约化 Arrow 表
    validate_arrow_table    # 按权威 Schema 安全转换 Arrow 表，并校验列顺序和非空约束
)


# In[20]:


if "ipykernel" in sys.modules:
    from config.notebook_schema_browser import display_schema_metadata
    display_schema_metadata([TRADE_CALENDAR_SCHEMA])


# In[21]:


# 表名、分区、主键和日级数据生效时间。
TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")    # 中国期货交易日历维度表。
PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")  # 正式表的 Hive 分区字段。
PRIMARY_KEY = TRADE_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")  # 每个自然日唯一一行。
EFFECTIVE_AFTER = time(20, 0)


# ## 表级业务规则校验

# In[22]:


def validate_calendar_table(
    table: pa.Table,
    *,
    require_contiguous: bool = True,
) -> pa.Table:

    # 先按权威 Schema 安全转换，再检查本表独有的业务不变量。
    checked = validate_arrow_table(table, TRADE_CALENDAR_SCHEMA)
    frame = checked.to_pandas().sort_values('calendar_date').reset_index(drop=True)

    # 自动补缺批次和 staging 可能只含多个离散年份，因此关闭连续性检查。
    if frame.empty:
        raise ValueError('交易日历不得为空。')

    if frame.duplicated(PRIMARY_KEY).any():
        raise ValueError('交易日历存在重复主键。')

    if require_contiguous:
        expected_dates = pd.date_range(
            frame.calendar_date.iloc[0],
            frame.calendar_date.iloc[-1],
            freq='D',
        ).date
        if frame.calendar_date.tolist() != expected_dates.tolist():
            raise ValueError('交易日历自然日不连续。')

    # 所有日期派生字段必须能够由 calendar_date 唯一复算。
    if frame.date_key.tolist() != frame.calendar_date.map(lambda value: value.strftime('%Y%m%d')).tolist():
        raise ValueError('date_key 与 calendar_date 不一致。')

    expected_weekday = frame.calendar_date.map(lambda value: value.weekday() + 1)
    if frame.weekday.tolist() != expected_weekday.tolist():
        raise ValueError('weekday 与 calendar_date 不一致。')

    if frame.is_weekend.tolist() != expected_weekday.isin([6, 7]).tolist():
        raise ValueError('is_weekend 与 weekday 不一致。')

    # 来源、日历标识、时区和生效时间均为表级固定约束。
    if not frame.source.eq('JQData_get_trade_days').all():
        raise ValueError('source 不是约定值 JQData_get_trade_days。')

    if not frame.calendar_name.eq('CN_FUTURES_MARKET').all():
        raise ValueError('calendar_name 不是约定值 CN_FUTURES_MARKET。')

    if not frame.calendar_timezone.eq('Asia/Shanghai').all():
        raise ValueError('calendar_timezone 不是 Asia/Shanghai。')

    if not frame.effective_after.eq(EFFECTIVE_AFTER).all():
        raise ValueError('effective_after 不是 20:00:00。')

    if frame.year.tolist() != frame.calendar_date.map(lambda value: value.year).tolist():
        raise ValueError('year 与 calendar_date 不一致。')

    if (frame.updated_at > pd.Timestamp.now(tz='UTC')).any():
        raise ValueError('updated_at 不得晚于当前校验时间。')

    return checked


# ## 分区合并、提交与失败回滚

# In[23]:


def commit_partitions(frame: pd.DataFrame, lake_root: pathlib.Path) -> int:
    # frame: 本次准备提交的新数据
    # lake_root: 数据湖根目录

    if frame.empty:
        return 0

    # 本批数据先过契约校验，并为 staging、备份目录生成独立批次标识。
    # 把新数据转换成 Arrow，并做契约校验
    new_table = validate_calendar_table(
        pandas_to_arrow(frame.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA), # 只保留 Schema 中规定的列
        require_contiguous=False,
    )

    # 建立正式数据集路径 target
    silver = lake_root.resolve() / 'silver'
    target = silver / TABLE_NAME

    # 随机 UUID（版本 4）默认表示形式 str(): 'f47ac10b-58cc-4372-a567-0e02b……'
    run_id = uuid.uuid4().hex # .hex 属性用于移除连字符: f47ac10b58cc4372a5670e02b……
    staging = silver / f'.{TABLE_NAME}.staging-{run_id}'
    backup = silver / f'.{TABLE_NAME}.backup-{run_id}'
    # 新增数据 临时保存路径 staging: D:/lake/silver/.trade_calendar.staging-a3c91...
    # 正式数据 临时备份路径 backup: D:/lake/silver/.trade_calendar.backup-a3c91...

    # 定义 Hive 分区规则
    hive = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]), flavor='hive')
    # TRADE_CALENDAR_SCHEMA 整个中国期货交易日历维度表
    # PARTITION_COLUMNS: 维度表的分区字段
    #     TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")

    touched = frame[PARTITION_COLUMNS].drop_duplicates() # 本批新增数据触达的分区键组合
    touched_years = touched.year.astype(int).tolist()  # 分区键组合年份提取
    silver.mkdir(parents=True, exist_ok=True) # 确保 silver 目录存在，或在空目录创建项目


    # 年分区只作为提交边界；局部日期更新必须保留同一年内未触达的旧行。
    # 如果正式数据已经存在，先读取旧数据
    if target.is_dir() and next(target.rglob('*.parquet'), None) is not None:
    # .is_dir() 判断该路径是否指向一个存在的目录
    # .rglob() 是 pathlib 模块的递归遍历方法，返回一个迭代器
    # next() 从迭代器中取出下一个元素

    # next(target.rglob('*.parquet'), None) 在 target 目录及其所有子目录中，匹配第一个扩展名为 .parquet 的路径，如果找到了就返回其路径，如果没找到则返回 None


        existing = ds.dataset(target, format='parquet', partitioning=hive) # 创建数据目录的视图 Dataset 对象
        existing_schema = pa.schema([existing.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names], metadata=existing.schema.metadata) # existing.schema 是 PyArrow 从整个 Dataset 推导出的逻辑 Schema

        # check_metadata=True 表示比较字段名、顺序和类型，还要比较表级及字段级 metadata
        if not existing_schema.equals(TRADE_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError('现有正式数据集 Schema/metadata 与契约不一致。')

        # 将现有正式 Dataset 全部读取成 PyArrow Table。
        # columns=... 明确只读取权威 Schema 规定的列，并保持规定顺序。
        existing_table = validate_calendar_table(
            existing.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
            require_contiguous=False,
        )

        # 从全部现有数据中，只取出本批触达年份的旧数据
        # existing_touched_df: 本批触达年份中的已有正式数据
        existing_touched_df = existing_table.filter(pa.compute.is_in(existing_table['year'], value_set=pa.array(touched_years, type=pa.int16()))).to_pandas()
        # existing_table["year"]: 取得正式表中的 year 列，类型为 Arrow ChunkedArray
        # pa.array(..., type=pa.int16()): 将 touched_years 转成与 Schema 中 year 相同的 int16
        # pa.compute.is_in(...): 判断每行 year 是否属于 touched_years，产生布尔条件
        # existing_table.filter(...): 按布尔条件筛选 Arrow Table


        # existing_touched_df: 本批触达年份中的已有正式数据
        # new_table: 本次准备提交的新数据 (路径: pd.DataFrame -> validate_calendar_table -> pa.Table -> .to_pandas())
        merged_df = pd.concat([existing_touched_df, new_table.to_pandas()], ignore_index=True)

        merged_df = merged_df.drop_duplicates(PRIMARY_KEY, keep='last').sort_values('calendar_date').reset_index(drop=True) # drop_duplicates 为 IDE 类型桩的误判 (pa.Table.to_pandas() 没有 Python 返回类型注解)

        # 重新转换为 Arrow Table，并再次执行契约和业务检查，以得到最终的 table
        table = validate_calendar_table(
            pandas_to_arrow(merged_df.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA),
            require_contiguous=False,
        )

    else:
    # 正式数据集不存在时，没有旧年份数据需要保留。
        table = new_table  # 直接把本批新数据作为待提交 Arrow Table。


    # 接下来整个逻辑块会先完整写入并复读 staging，任何检查失败都不会触碰正式分区
    # moved 记录已经开始替换的叶分区，用于提交失败时倒序回滚
    moved = []
    # 每一项保存：
    #     正式目标路径 destination,
    #     旧分区备份路径 saved,
    #     提交前是否存在旧分区 had_existing

    try:

        # 第一阶段：只向本批独立 staging 目录写入 Parquet。
        ds.write_dataset(
            table, staging, format='parquet', partitioning=hive,
            existing_data_behavior='delete_matching', # 如果写入过程中遇到相同的分区目录，使用当前待写数据替换 staging 中对应内容。
            basename_template='part-{i}.parquet' # 每个 Parquet 数据文件依次命名为：part-0.parquet、part-1.parquet……
        )

        # 把刚写出的 staging 重新打开为 PyArrow Dataset
        staged = ds.dataset(staging, format='parquet', partitioning=hive)
        staged_schema = pa.schema([staged.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names], metadata=staged.schema.metadata)
        # 从 staging Dataset 重建完整逻辑 Schema：
        # Parquet 文件内字段 + Hive 目录恢复出的分区字段

        # 验证 staging 落盘后的 Schema 和 metadata 与权威契约完全一致
        if not staged_schema.equals(TRADE_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError('staging Schema/metadata 与契约不一致。')

        # 从 staging 实际复读全部数据，再执行表级业务规则校验
        checked = validate_calendar_table(
            staged.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
            require_contiguous=False,
        )
        if len(checked) != len(table): # 确认写入和复读过程没有丢行或额外产生行
            raise ValueError('staging 行数检查失败。')

        # staging 已经通过复读检查，开始进入正式分区替换阶段
        backup.mkdir(parents=True, exist_ok=False) # 建立本批独立备份目录
        target.mkdir(parents=True, exist_ok=True) # 确保正式数据集根目录存在


        # touched 为 Series 的每一行是一个完整分区键组合
        for values in touched.itertuples(index=False, name=None):

            # 将分区字段名和值组合成 Hive 相对目录
            relative = pathlib.Path(*[f'{name}={value}' for name, value in zip(PARTITION_COLUMNS, values, strict=True)])  # strict=True 严格要求 PARTITION_COLUMNS 和 values 长度一致

            source = staging / relative  # source: staging 中已经通过复读检查的新分区
            destination = target / relative  # destination: 新分区最终进入的正式路径
            saved = backup / relative  # saved: 原正式分区在本批 backup 中的保存路径

            if not source.is_dir(): # staging 中必须存在本次计划提交的分区目录，缺少目录说明写入结果与提交计划不一致，不能继续
                raise FileNotFoundError(f'staging 缺少 {relative}。')

            # 确保正式分区和备份分区的父目录存在
            destination.parent.mkdir(parents=True, exist_ok=True)
            saved.parent.mkdir(parents=True, exist_ok=True)

            had_existing = destination.exists()
            # 记录提交前正式分区是否已经存在
            # 回滚时据此判断是否需要恢复旧分区

            # 如果正式分区已经存在，先把旧分区整体移动到 backup
            if destination.exists():
                shutil.move(str(destination), str(saved))

            moved.append((destination, saved, had_existing)) # 在移动新分区前记录回滚信息
            shutil.move(str(source), str(destination)) # 将已经验证过的 staging 分区移动到正式位置

        # 最后从正式路径复读全表，确认契约、连续性和预期行数。
        committed = ds.dataset(target, format='parquet', partitioning=hive)
        committed_schema = pa.schema([committed.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names], metadata=committed.schema.metadata) # 重建正式 Dataset 的完整逻辑 Schema

        if not committed_schema.equals(TRADE_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError('正式数据集 Schema/metadata 与契约不一致。')

        # 从正式路径复读全表
        committed_table = validate_calendar_table(committed.to_table(columns=TRADE_CALENDAR_SCHEMA.names))
        expected_row_count = len(existing_table) - len(existing_touched_df) + len(table) if 'existing_table' in locals() else len(table)
        if len(committed_table) != expected_row_count:
            raise ValueError('正式数据集行数检查失败。')


    # try 内任何步骤失败都会进入回滚，commit_error 保存最初导致提交失败的异常
    except Exception as commit_error:

        rollback_errors = []  # 只回滚本批已经移动的分区；回滚异常时保留备份供人工恢复。
        for destination, saved, had_existing in reversed(moved):
        # 必须按提交的相反顺序回滚，后移动的分区先恢复，避免嵌套路径或中间状态互相干扰

            try:
                if destination.exists(): # 如果正式位置存在本批移入的新分区，先删除它
                    shutil.rmtree(destination)

                if had_existing and saved.exists(): # 如果提交前存在旧正式分区，并且其备份仍然存在，
                # 将旧分区从 backup 恢复到原正式位置
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(saved), str(destination))

            except Exception as rollback_error:
                rollback_errors.append(f'{destination}: {rollback_error}')

        if rollback_errors:
            raise RuntimeError('提交失败且回滚不完整；备份保留在 ' + str(backup) + '；' + '; '.join(rollback_errors)) from commit_error

        shutil.rmtree(backup, ignore_errors=True) # 回滚完整成功后不再需要 backup
        raise # 重新抛出最初的提交异常，而不是将错误吞掉

    else: # try 整体成功时执行
        shutil.rmtree(backup, ignore_errors=True)
        # 正式数据已经复读并通过检查，旧分区备份可以删除

    finally: # 无论提交成功、提交失败还是回滚失败，都会执行 finally
        shutil.rmtree(staging, ignore_errors=True)
        # staging 只是临时写入目录，最后统一清理

    return len(new_table)# 返回本次调用传入并通过校验的新数据行数


# ## JQData 交易日采集与标准化

# In[24]:


def collect(start: date, end: date) -> pd.DataFrame:
    # start: 本次采集范围的起始自然日。
    # end: 本次采集范围的结束自然日。
    # 返回值: 经过完整契约校验的 Pandas DataFrame。
    # 起止日期均包含在采集范围内。

    if start > end:
        raise ValueError('起始日期不得晚于结束日期。')

    # 同一批次的所有行共享一个 UTC 更新时间。
    batch_updated_at = pd.Timestamp.now(tz='UTC')
    from config.jqdata_connection import authenticate_jqdata

    # JQData 只提供交易日集合，全部自然日仍由本工作流生成。
    jq = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    trading = {pd.Timestamp(value).date() for value in jq.get_trade_days(start_date=start, end_date=end)}
    if any(value < start or value > end for value in trading):
        raise ValueError('JQData 返回了请求范围之外的交易日。')

    # 逐自然日生成 11 个契约字段，休市日也必须保留一行。
    frame = pd.DataFrame({'calendar_date': pd.date_range(start, end, freq='D').date})
    frame['date_key'] = frame.calendar_date.map(lambda value: value.strftime('%Y%m%d'))
    frame['is_trading_day'] = frame.calendar_date.isin(trading)
    frame['weekday'] = frame.calendar_date.map(lambda value: value.weekday() + 1)
    frame['is_weekend'] = frame.weekday.isin([6, 7])
    frame['source'] = 'JQData_get_trade_days'
    frame['calendar_name'] = 'CN_FUTURES_MARKET'
    frame['calendar_timezone'] = 'Asia/Shanghai'
    frame['effective_after'] = EFFECTIVE_AFTER
    frame['updated_at'] = batch_updated_at
    frame['year'] = pd.to_datetime(frame.calendar_date).dt.year
    frame = frame[TRADE_CALENDAR_SCHEMA.names]

    # 即使本次不写湖，也先验证 API 响应转换后的完整业务契约。
    validate_calendar_table(pandas_to_arrow(frame, TRADE_CALENDAR_SCHEMA))
    return frame


# ## 命令行入口与更新水位

# In[25]:


# 不带日期时按有效格点差集自动建表或补缺；显式日期只用于检查或非正式测试湖。
@click.command()
@click.option('--lake-root', type=click.Path(path_type=pathlib.Path))
@click.option('--start-date', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--end-date', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--write', is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    write: bool,
) -> None:

    # 命令行参数未覆盖 --lake-root 时使用 .env 中唯一的正式湖；显式路径主要供临时湖测试。
    formal_lake_root = settings.futures_lake_root
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    # 只要起始日期或结束日期任意一个存在，就先标记为“显式日期模式” has_explicit_dates
    has_explicit_dates = start_date is not None or end_date is not None

    if (start_date is None) != (end_date is None):
        raise click.UsageError('--start-date 与 --end-date 必须同时提供。')
    if has_explicit_dates and write and resolved_lake_root == formal_lake_root:
        raise click.UsageError(
            '显式指定日期时禁止写入 FUTURES_LAKE_ROOT 指向的正式湖；'
            '请移除日期参数使用自动补缺，或改用非正式测试湖。'
        )

    # 正式起点来自 .env，通过 settings.futures_data_start_date 传入
    # 自动更新模式会从这个日期开始建立完整自然日历
    configured_start_date = settings.futures_data_start_date
    now_shanghai = datetime.now(ZoneInfo('Asia/Shanghai')) # 获取当前上海时区的日期和时刻
    valid_end_date = now_shanghai.date() if now_shanghai.time() >= EFFECTIVE_AFTER else now_shanghai.date() - timedelta(days=1)
    # EFFECTIVE_AFTER 北京时间 20:00 前，当日尚未进入有效水位

    if configured_start_date > valid_end_date:
        raise ValueError(
            f'配置起点 {configured_start_date} 晚于当前有效截止日 {valid_end_date}。'
        )


    log_boundary = '=' * 88
    run_mode = 'explicit' if has_explicit_dates else 'automatic'
    click.echo(
        f'{log_boundary}\n'
        '运行入口开始 / Run entry started\n'
        f'function=main(); table={TABLE_NAME}; mode={run_mode}; '
        f'write={str(write).lower()}; lake_root={resolved_lake_root}; '
        f'valid_start={configured_start_date}; valid_end={valid_end_date}\n'
        f'{log_boundary}\n'
    )

    # 显式日期模式保留给定向检查和非正式湖验证，不参与正式自动更新
    # 同时传入 --start-date 和 --end-date 进入显式日期，可进行：
    # 1. 定向采集和只读检查；
    # 2. 非正式测试湖验证。
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
        click.echo(
            f'{log_boundary}\n'
            '开始采集交易日历 / Trade-calendar collection started\n'
            f'function=main() -> collect(); start_date={requested_start_date}; '
            f'end_date={requested_end_date}\n'
            f'{log_boundary}'
        )
        frame = collect(requested_start_date, requested_end_date)
        click.echo(
            f'{log_boundary}\n'
            '交易日历采集完成 / Trade-calendar collection completed\n'
            f'function=collect() -> main(); rows={len(frame)}; '
            f'trading_day_count={int(frame["is_trading_day"].sum())}\n'
            f'{log_boundary}'
        )
        if write:
            click.echo(
                f'{log_boundary}\n'
                '开始提交交易日历分区 / Partition commit started\n'
                f'function=main() -> commit_partitions(); rows={len(frame)}; '
                f'lake_root={resolved_lake_root}\n'
                f'{log_boundary}'
            )
            committed_rows = commit_partitions(frame, resolved_lake_root)
            click.echo(
                f'{log_boundary}\n'
                '交易日历分区提交完成 / Partition commit completed\n'
                f'function=commit_partitions() -> main(); mode=explicit_non_formal; '
                f'committed_rows={committed_rows}\n'
                f'{log_boundary}'
            )
        else:
            click.echo(
                f'{log_boundary}\n'
                '显式日期只读运行完成 / Explicit-date dry run completed\n'
                f'function=main(); write=false; validated_rows={len(frame)}\n'
                f'{log_boundary}'
            )
        # 前面的写入禁令已经确保：显式日期不能写入正式湖，
        # 所以这里的写入目标 resolved_lake_root 一定应当是非正式湖

        # 显式日期模式已经完成，不再继续执行下面的自动差集流程
        return

    # 自动模式重新取得全部有效自然日的当前交易状态，再减去主键和值都完整一致的下游格点。
    target = resolved_lake_root / 'silver' / TABLE_NAME
    hive = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in PARTITION_COLUMNS]), flavor='hive')
    # PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")

    # 每次自动运行都重新获取完整有效区间的当前交易日状态
    # 以实现不止发现尾部新增日期，同时发现历史交易状态被上游修订的日期
    click.echo(
        f'{log_boundary}\n'
        '开始采集完整有效交易日历 / Full valid-calendar collection started\n'
        f'function=main() -> collect(); start_date={configured_start_date}; '
        f'end_date={valid_end_date}\n'
        f'{log_boundary}'
    )
    expected_frame = collect(configured_start_date, valid_end_date)
    click.echo(
        f'{log_boundary}\n'
        '完整有效交易日历采集完成 / Full valid-calendar collection completed\n'
        f'function=collect() -> main(); rows={len(expected_frame)}; '
        f'trading_day_count={int(expected_frame["is_trading_day"].sum())}\n'
        f'{log_boundary}'
    )
    # Pandas 类型系统不知道单列元素来自 Arrow date32；显式标注为 datetime.date，供后续集合和字典键复用。
    valid_dates: list[date] = expected_frame['calendar_date'].tolist()

    valid_date_set: set[date] = set(valid_dates)  # 集合用于快速执行日期差集。
    existing_frame = expected_frame.iloc[0:0].copy()
    # 先创建一个与 expected_frame 具有相同列和类型的空 DataFrame
    # 如果目标数据集不存在，existing_frame 就保持为空
    # 后面的统一比较逻辑会自然认为所有日期都不完整(即空湖中全量建立 dim_trade_calendar 数据集)

    # next(target.rglob('*.parquet'), None) 在 target 目录及其所有子目录中，匹配第一个扩展名为 .parquet 的路径，如果找到了就返回其路径，如果没找到则返回 None
    if target.is_dir() and next(target.rglob('*.parquet'), None) is not None:
    # .rglob() 是 pathlib 模块的递归遍历方法，返回一个迭代器
    # next() 从迭代器中取出下一个元素

        # 建立现有 Parquet Dataset 的逻辑视图
        existing = ds.dataset(target, format='parquet', partitioning=hive)
        existing_schema = pa.schema([existing.schema.field(name) for name in TRADE_CALENDAR_SCHEMA.names], metadata=existing.schema.metadata) # 按权威字段顺序重建现有 Dataset 的逻辑 Schema，
        # 同时保留现有 Dataset 的表级 metadata

        # 正式数据集的字段、类型、顺序和 metadata
        if not existing_schema.equals(TRADE_CALENDAR_SCHEMA, check_metadata=True):
            raise TypeError('现有正式数据集 Schema/metadata 与契约不一致。')

        # 将现有 Dataset 读取为 Arrow Table (.to_table)，并执行主键、派生字段、固定值和审计时间检查
        existing_table = validate_calendar_table(
            existing.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
            require_contiguous=False,
        )
        existing_frame = existing_table.to_pandas(types_mapper=pd.ArrowDtype)
        existing_date_set = set(existing_frame['calendar_date'].tolist()) # 转成集合，供后面快速执行集合差运算

        # 检查下游是否含有当前有效水位之外的日期
        out_of_watermark_dates = sorted(existing_date_set - valid_date_set) # 只存在于下游、但不属于当前有效范围的日期
        if out_of_watermark_dates:
            raise ValueError(
                '现有交易日历包含配置起点之前或有效截止日之后的日期：'
                f'{out_of_watermark_dates[:10]}'
            )

    click.echo(
        f'{log_boundary}\n'
        '现有正式数据检查完成 / Existing formal dataset inspection completed\n'
        f'function=main() -> ds.dataset() -> validate_calendar_table(); '
        f'dataset_exists={str(target.is_dir()).lower()}; existing_rows={len(existing_frame)}\n'
        f'{log_boundary}'
    )

    # updated_at 只是生成审计时间；业务内容没有变化时不应仅因运行时间不同而重写该行。
    comparison_columns: list[str] = [
        name for name in TRADE_CALENDAR_SCHEMA.names if name != 'updated_at'
    ]

    # 每个日期映射到除 updated_at 外的完整行签名；两个字典使用相同的 date 键和 tuple 值类型。
    expected_signature_by_date: dict[date, tuple[object, ...]] = {
        row['calendar_date']: tuple(row[name] for name in comparison_columns)
        for row in expected_frame.to_dict('records')
    }
    existing_signature_by_date: dict[date, tuple[object, ...]] = {
        row['calendar_date']: tuple(row[name] for name in comparison_columns)
        for row in existing_frame.to_dict('records')
    }

    # 下游缺少日期时 dict.get() 返回 None；None 与期望 tuple 不相等，该日期自然进入待补集合。
    # 下游已有日期时则逐项比较行签名，从而同时发现缺失日期和历史状态修订。
    incomplete_dates = [
        calendar_date
        for calendar_date in valid_dates
        if existing_signature_by_date.get(calendar_date)
        != expected_signature_by_date[calendar_date]
    ]
    if not incomplete_dates:
        click.echo(
            f'{log_boundary}\n'
            '正式交易日历已是最新 / Formal trade calendar is up to date\n'
            f'function=main(); table={TABLE_NAME}; valid_end_date={valid_end_date}; '
            f'valid_grid_count={len(valid_dates)}\n'
            f'{log_boundary}'
        )
        return

    # 只提交缺失或交易状态已修订的日期；未变化行保留原 updated_at。
    frame = expected_frame.loc[
        expected_frame['calendar_date'].isin(incomplete_dates),
        TRADE_CALENDAR_SCHEMA.names,
    ].reset_index(drop=True)

    # 待提交日期可能分散在多个年份，不一定组成一段连续自然日，因此关闭连续性检查
    # 主键、派生字段、固定值等其他规则仍然执行
    validate_calendar_table(
        pandas_to_arrow(frame, TRADE_CALENDAR_SCHEMA),
        require_contiguous=False,
    )
    click.echo(
        f'{log_boundary}\n'
        '自动差集计划已生成 / Automatic reconciliation plan created\n'
        f'function=main(); table={TABLE_NAME}; valid_grid_count={len(valid_dates)}; '
        f'complete_grid_count={len(valid_dates) - len(incomplete_dates)}; '
        f'missing_or_revised_grid_count={len(frame)}\n'
        f'{log_boundary}'
    )

    if write:
        click.echo(
            f'{log_boundary}\n'
            '开始提交自动差集分区 / Automatic reconciliation commit started\n'
            f'function=main() -> commit_partitions(); rows={len(frame)}; '
            f'lake_root={resolved_lake_root}\n'
            f'{log_boundary}'
        )
        committed_rows = commit_partitions(frame, resolved_lake_root)
        click.echo(
            f'{log_boundary}\n'
            '自动差集分区提交完成 / Automatic reconciliation commit completed\n'
            f'function=commit_partitions() -> main(); mode=automatic_reconciliation; '
            f'committed_rows={committed_rows}\n'
            f'{log_boundary}'
        )
    else:
        click.echo(
            f'{log_boundary}\n'
            '自动差集只读运行完成 / Automatic reconciliation dry run completed\n'
            f'function=main(); write=false; validated_rows={len(frame)}\n'
            f'{log_boundary}'
        )


# In[26]:


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

if "ipykernel" in sys.modules:

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = [
        "--start-date",
        "2026-08-01",
        "--end-date",
        "2026-08-15",
    ]

    main.main(
        args=notebook_args,
        prog_name="c01_trade_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# In[26]:




