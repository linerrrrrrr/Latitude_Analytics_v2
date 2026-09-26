#!/usr/bin/env python
# coding: utf-8

# # b01_trade_calendar
# 
# 目标表：dim_trade_calendar。自然日与 JQData 交易日集合。

# ## 自动更新与正式湖写入边界
# 
# 默认模式信任已经正式提交的历史，只从正式表最大自然日的下一天采集到当前有效日；没有尾部新增时在认证和 API 调用之前结束。空湖的最大日期为空，因此同一默认入口自然完成首次全建。显式 `--full` 保留全历史来源比较，用于人工发现历史缺口和上游修订。
# 
# 正式湖根目录由 `.env` 的 `FUTURES_LAKE_ROOT` 唯一指定。默认尾部模式和显式 `--full` 都可通过 `--write` 更新正式湖；显式日期与 `--full` 互斥，日期范围只允许检查，或者写入另行指定的非正式测试湖。

# ## 初始化与表配置

# ## Schema 契约呈现

# In[1]:


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
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
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


# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_schema_metadata
    display_schema_metadata([TRADE_CALENDAR_SCHEMA], lake_root=settings.futures_lake_root)


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


# ## 表级业务规则校验

# In[4]:


def validate_calendar_table(
    calendar_table: pa.Table,
    *,
    require_contiguous: bool = True,
) -> pa.Table:
    """
    按权威 Schema 和交易日历业务规则校验 Arrow 表
    上游为 JQData get_trade_days() 返回区间或全量内交易日 calendar_table

    Parameters
    ----------
    calendar_table : pa.Table
        待校验的中国期货交易日历逻辑表。
    require_contiguous : bool, default True
        是否要求 `calendar_date` 从最小日到最大日逐自然日连续。
        离散年份的局部提交批次可设为 `False`。

    Returns
    -------
    pa.Table
        已安全转换为 `TRADE_CALENDAR_SCHEMA` 并通过业务校验的表。
    """

    # 先按权威 Schema 安全转换，再检查本表独有的业务不变量。
    validated_calendar_table = validate_arrow_table(calendar_table, TRADE_CALENDAR_SCHEMA)
    calendar_df = validated_calendar_table.to_pandas().sort_values('calendar_date').reset_index(drop=True)

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

    return validated_calendar_table


# ## 分区合并、提交与失败回滚

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

    if new_calendar_df.empty:
        return 0

    # 本批数据先过契约校验，并为 staging、备份目录生成独立批次标识。
    # 把新数据转换成 Arrow，并做契约校验
    new_calendar_table = validate_calendar_table(
        pandas_to_arrow(new_calendar_df.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA), # 只保留 Schema 中规定的列
        require_contiguous=False,
    )

    # 建立交易日历数据集及本批 staging、backup 路径。
    silver_dir = lake_root.resolve() / 'silver'
    calendar_path = silver_dir / TABLE_NAME

    # 随机 UUID（版本 4）默认表示形式 str(): 'f47ac10b-58cc-4372-a567-0e02b……'
    run_id = uuid.uuid4().hex # .hex 属性用于移除连字符: f47ac10b58cc4372a5670e02b……
    staging_dir = silver_dir / f'.c01s-{run_id}'
    backup_dir = silver_dir / f'.c01b-{run_id}'
    # 新增数据临时保存路径 staging_dir: D:/lake/silver/.trade_calendar.staging-a3c91...
    # 正式数据临时备份路径 backup_dir: D:/lake/silver/.trade_calendar.backup-a3c91...

    # 定义 Hive 分区规则
    calendar_partitioning = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(field_name) for field_name in PARTITION_COLUMNS]), flavor='hive')
    # TRADE_CALENDAR_SCHEMA 整个中国期货交易日历维度表
    # PARTITION_COLUMNS: 维度表的分区字段
    #     TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")

    touched_partitions_df = new_calendar_df[PARTITION_COLUMNS].drop_duplicates() # 本批新增数据触达的分区键组合
    touched_years = touched_partitions_df.year.astype(int).tolist()  # 分区键组合年份提取
    silver_dir.mkdir(parents=True, exist_ok=True) # 确保 silver 目录存在，或在空目录创建项目


    # 年分区只作为提交边界；局部日期更新必须保留同一年内未触达的旧行。
    # 如果正式数据已经存在，先读取旧数据
    if calendar_path.is_dir() and next(calendar_path.rglob('*.parquet'), None) is not None:
    # .is_dir() 判断该路径是否指向一个存在的目录
    # .rglob() 是 pathlib 模块的递归遍历方法，返回一个迭代器
    # next() 从迭代器中取出下一个元素

    # next(calendar_path.rglob('*.parquet'), None) 在交易日历目录及其所有子目录中匹配第一个 Parquet 文件。


        existing_calendar_dataset = ds.dataset(calendar_path, format='parquet', partitioning=calendar_partitioning) # 创建数据目录的视图 Dataset 对象
        existing_calendar_schema = pa.schema([existing_calendar_dataset.schema.field(field_name) for field_name in TRADE_CALENDAR_SCHEMA.names], metadata=existing_calendar_dataset.schema.metadata) # Dataset 推导出的逻辑 Schema

        validate_compatible_dataset_schema(
            existing_calendar_schema, TRADE_CALENDAR_SCHEMA, '现有正式数据集 '
        )
        validate_dataset_fragment_schemas(
            existing_calendar_dataset, TRADE_CALENDAR_SCHEMA, PARTITION_COLUMNS, '现有正式数据集 '
        )

        # clean 历史由正式提交证明；这里只定向读取本批触达年份用于合并。
        touched_existing_calendar_table = validate_arrow_table(
            existing_calendar_dataset.to_table(
                columns=TRADE_CALENDAR_SCHEMA.names,
                filter=ds.field('year').isin(touched_years),
            ),
            TRADE_CALENDAR_SCHEMA,
        )
        touched_existing_calendar_df = touched_existing_calendar_table.to_pandas()


        # touched_existing_calendar_df: 本批触达年份中的已有正式数据
        # new_calendar_table: 本次准备提交的新数据 (路径: pd.DataFrame -> validate_calendar_table -> pa.Table -> .to_pandas())
        merged_calendar_df = pd.concat([touched_existing_calendar_df, new_calendar_table.to_pandas()], ignore_index=True)

        merged_calendar_df = merged_calendar_df.drop_duplicates(PRIMARY_KEY, keep='last').sort_values('calendar_date').reset_index(drop=True) # drop_duplicates 为 IDE 类型桩的误判 (pa.Table.to_pandas() 没有 Python 返回类型注解)

        # 重新转换为 Arrow Table，并再次执行契约和业务检查，得到触达年份的替换数据。
        replacement_calendar_table = validate_calendar_table(
            pandas_to_arrow(merged_calendar_df.loc[:, TRADE_CALENDAR_SCHEMA.names], TRADE_CALENDAR_SCHEMA),
            require_contiguous=False,
        )

    else:
    # 正式数据集不存在时，没有旧年份数据需要保留。
        replacement_calendar_table = new_calendar_table  # 直接把本批新数据作为待提交 Arrow Table。


    # 接下来整个逻辑块会先完整写入并复读 staging，任何检查失败都不会触碰正式分区
    # moved_partitions 记录已经开始替换的叶分区，用于提交失败时倒序回滚。
    moved_partitions = []
    # 每一项保存：
    #     正式目标路径 formal_partition_path,
    #     旧分区备份路径 backup_partition_path,
    #     提交前是否存在旧分区 has_existing_partition

    try:

        # 第一阶段：只向本批独立 staging 目录写入 Parquet。
        ds.write_dataset(
            replacement_calendar_table, staging_dir, format='parquet', partitioning=calendar_partitioning,
            existing_data_behavior='delete_matching', # 如果写入过程中遇到相同的分区目录，使用当前待写数据替换 staging 中对应内容。
            basename_template='part-{i}.parquet' # 每个 Parquet 数据文件依次命名为：part-0.parquet、part-1.parquet……
        )

        # 把刚写出的 staging 重新打开为 PyArrow Dataset
        staged_calendar_dataset = ds.dataset(staging_dir, format='parquet', partitioning=calendar_partitioning)
        staged_calendar_schema = pa.schema([staged_calendar_dataset.schema.field(field_name) for field_name in TRADE_CALENDAR_SCHEMA.names], metadata=staged_calendar_dataset.schema.metadata)
        # 从 staging Dataset 重建完整逻辑 Schema：
        # Parquet 文件内字段 + Hive 目录恢复出的分区字段

        validate_compatible_dataset_schema(
            staged_calendar_schema, TRADE_CALENDAR_SCHEMA, 'staging '
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

        # staging 已经通过复读检查，开始进入正式分区替换阶段
        backup_dir.mkdir(parents=True, exist_ok=False) # 建立本批独立备份目录
        calendar_path.mkdir(parents=True, exist_ok=True) # 确保正式数据集根目录存在


        # touched_partitions_df 的每一行是一个完整分区键组合。
        for partition_values in touched_partitions_df.itertuples(index=False, name=None):

            # 将分区字段名和值组合成 Hive 相对目录
            partition_relative_path = pathlib.Path(*[f'{partition_column}={partition_value}' for partition_column, partition_value in zip(PARTITION_COLUMNS, partition_values, strict=True)])  # 分区列和值必须等长

            staged_partition_path = staging_dir / partition_relative_path
            formal_partition_path = calendar_path / partition_relative_path
            backup_partition_path = backup_dir / partition_relative_path

            if not staged_partition_path.is_dir(): # staging 中必须存在本次计划提交的分区目录
                raise FileNotFoundError(f'staging 缺少 {partition_relative_path}。')

            # 确保正式分区和备份分区的父目录存在
            formal_partition_path.parent.mkdir(parents=True, exist_ok=True)
            backup_partition_path.parent.mkdir(parents=True, exist_ok=True)

            has_existing_partition = formal_partition_path.exists()
            # 记录提交前正式分区是否已经存在
            # 回滚时据此判断是否需要恢复旧分区

            # 如果正式分区已经存在，先把旧分区整体移动到 backup
            if formal_partition_path.exists():
                shutil.move(str(formal_partition_path), str(backup_partition_path))

            staged_partition_row_count = ds.dataset(
                staged_partition_path, format='parquet'
            ).count_rows()
            moved_partitions.append((formal_partition_path, backup_partition_path, has_existing_partition)) # 在移动新分区前记录回滚信息
            shutil.move(str(staged_partition_path), str(formal_partition_path)) # 将已经验证过的 staging 分区移动到正式位置
            # 正式安装后只复读刚触达的叶：物理契约、身份 metadata、主键和行数。
            committed_partition_dataset = ds.dataset(
                formal_partition_path, format='parquet'
            )
            committed_partition_schema = pa.schema(
                [committed_partition_dataset.schema.field(name) for name in committed_partition_dataset.schema.names],
                metadata=committed_partition_dataset.schema.metadata,
            )
            expected_partition_schema = pa.schema(
                [
                    TRADE_CALENDAR_SCHEMA.field(name)
                    for name in TRADE_CALENDAR_SCHEMA.names
                    if name not in PARTITION_COLUMNS
                ],
                metadata=TRADE_CALENDAR_SCHEMA.metadata,
            )
            validate_compatible_dataset_schema(
                committed_partition_schema, expected_partition_schema,
                f'正式分区 {partition_relative_path} ',
            )
            committed_primary_key_table = committed_partition_dataset.to_table(
                columns=[name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS]
            )
            if committed_primary_key_table.to_pandas().duplicated(
                [name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS]
            ).any():
                raise ValueError(f'正式分区 {partition_relative_path} 主键不唯一。')
            committed_partition_row_count = len(committed_primary_key_table)
            if committed_partition_row_count != staged_partition_row_count:
                raise ValueError(f'正式分区 {partition_relative_path} 行数检查失败。')


    # try 内任何步骤失败都会进入回滚，commit_error 保存最初导致提交失败的异常
    except Exception as commit_error:

        rollback_errors = []  # 只回滚本批已经移动的分区；回滚异常时保留备份供人工恢复。
        for formal_partition_path, backup_partition_path, has_existing_partition in reversed(moved_partitions):
        # 必须按提交的相反顺序回滚，后移动的分区先恢复，避免嵌套路径或中间状态互相干扰

            try:
                if formal_partition_path.exists(): # 如果正式位置存在本批移入的新分区，先删除它
                    shutil.rmtree(formal_partition_path)

                if has_existing_partition and backup_partition_path.exists(): # 如果提交前存在旧正式分区，并且其备份仍然存在，
                # 将旧分区从 backup 恢复到原正式位置
                    formal_partition_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(backup_partition_path), str(formal_partition_path))

            except Exception as rollback_error:
                rollback_errors.append(f'{formal_partition_path}: {rollback_error}')

        if rollback_errors:
            raise RuntimeError('提交失败且回滚不完整；备份保留在 ' + str(backup_dir) + '；' + '; '.join(rollback_errors)) from commit_error

        shutil.rmtree(backup_dir, ignore_errors=True) # 回滚完整成功后不再需要 backup
        raise # 重新抛出最初的提交异常，而不是将错误吞掉

    else: # try 整体成功时执行
        shutil.rmtree(backup_dir, ignore_errors=True)
        # 正式数据已经复读并通过检查，旧分区备份可以删除

    finally: # 无论提交成功、提交失败还是回滚失败，都会执行 finally
        shutil.rmtree(staging_dir, ignore_errors=True)
        # staging 只是临时写入目录，最后统一清理

    return len(new_calendar_table)# 返回本次调用传入并通过校验的新数据行数


# ## JQData 交易日采集与标准化

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

    if start > end:
        raise ValueError('起始日期不得晚于结束日期。')

    # 同一批次的所有行共享一个 UTC 更新时间。
    batch_updated_at = pd.Timestamp.now(tz='UTC')
    from config.jqdata_connection import authenticate_jqdata

    # JQData 只提供交易日集合，全部自然日仍由本工作流生成。
    jqdata_client = authenticate_jqdata(settings.jqdata_id, settings.jqdata_secret)
    trading_dates = {pd.Timestamp(calendar_date).date() for calendar_date in jqdata_client.get_trade_days(start_date=start, end_date=end)}
    if any(calendar_date < start or calendar_date > end for calendar_date in trading_dates):
        raise ValueError('JQData 返回了请求范围之外的交易日。')

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
    validate_calendar_table(pandas_to_arrow(calendar_df, TRADE_CALENDAR_SCHEMA))
    return calendar_df


# ## 命令行入口与更新水位

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
    """按显式检查或自动求差模式运行交易日历入口。

    Parameters
    ----------
    lake_root : pathlib.Path or None
        可选数据湖根目录；未提供时使用 `.env` 中的正式湖配置。
    start_date : datetime.datetime or None
        可选显式起始日，必须与 `end_date` 同时提供。
    end_date : datetime.datetime or None
        可选显式结束日，必须与 `start_date` 同时提供。
    full_refresh : bool
        是否显式执行全历史来源比较慢路径。
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

    # 只要起始日期或结束日期任意一个存在，就先标记为“显式日期模式” has_explicit_dates
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
    run_mode = 'explicit' if has_explicit_dates else ('full' if full_refresh else 'automatic_tail')
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
        requested_calendar_df = collect(requested_start_date, requested_end_date)
        click.echo(
            f'{log_boundary}\n'
            '交易日历采集完成 / Trade-calendar collection completed\n'
            f'function=collect() -> main(); rows={len(requested_calendar_df)}; '
            f'trading_day_count={int(requested_calendar_df["is_trading_day"].sum())}\n'
            f'{log_boundary}'
        )
        if write:
            click.echo(
                f'{log_boundary}\n'
                '开始提交交易日历分区 / Partition commit started\n'
                f'function=main() -> commit_partitions(); rows={len(requested_calendar_df)}; '
                f'lake_root={resolved_lake_root}\n'
                f'{log_boundary}'
            )
            committed_row_count = commit_partitions(requested_calendar_df, resolved_lake_root)
            click.echo(
                f'{log_boundary}\n'
                '交易日历分区提交完成 / Partition commit completed\n'
                f'function=commit_partitions() -> main(); mode=explicit_non_formal; '
                f'committed_rows={committed_row_count}\n'
                f'{log_boundary}'
            )
        else:
            click.echo(
                f'{log_boundary}\n'
                '显式日期只读运行完成 / Explicit-date dry run completed\n'
                f'function=main(); write=false; validated_rows={len(requested_calendar_df)}\n'
                f'{log_boundary}'
            )
        # 前面的写入禁令已经确保：显式日期不能写入正式湖，
        # 所以这里的写入目标 resolved_lake_root 一定应当是非正式湖

        # 显式日期模式已经完成，不再继续执行下面的自动差集流程
        return

    # 自动模式只信任正式表已经提交的历史，从最大自然日的下一天开始追加。
    # --full 才继续执行下面保留的全历史来源比较慢路径。
    calendar_path = resolved_lake_root / 'silver' / TABLE_NAME
    calendar_partitioning = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(field_name) for field_name in PARTITION_COLUMNS]), flavor='hive')
    # PARTITION_COLUMNS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")

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
            committed_row_count = commit_partitions(
                pending_calendar_df,
                resolved_lake_root,
            )
            click.echo(
                f'committed: table={TABLE_NAME}; mode=automatic_tail; '
                f'rows={committed_row_count}'
            )
        return

    # --full 显式重新获取完整有效区间的当前交易日状态
    # 以实现不止发现尾部新增日期，同时发现历史交易状态被上游修订的日期
    click.echo(
        f'{log_boundary}\n'
        '开始采集完整有效交易日历 / Full valid-calendar collection started\n'
        f'function=main() -> collect(); start_date={configured_start_date}; '
        f'end_date={valid_end_date}\n'
        f'{log_boundary}'
    )
    expected_calendar_df = collect(configured_start_date, valid_end_date)
    click.echo(
        f'{log_boundary}\n'
        '完整有效交易日历采集完成 / Full valid-calendar collection completed\n'
        f'function=collect() -> main(); rows={len(expected_calendar_df)}; '
        f'trading_day_count={int(expected_calendar_df["is_trading_day"].sum())}\n'
        f'{log_boundary}'
    )
    # Pandas 类型系统不知道单列元素来自 Arrow date32；显式标注为 datetime.date，供后续集合和字典键复用。
    valid_calendar_dates: list[date] = expected_calendar_df['calendar_date'].tolist()

    valid_calendar_date_set: set[date] = set(valid_calendar_dates)  # 集合用于快速执行日期差集。
    existing_calendar_df = expected_calendar_df.iloc[0:0].copy()
    # 先创建一个与 expected_calendar_df 具有相同列和类型的空 DataFrame
    # 如果目标数据集不存在，existing_calendar_df 就保持为空
    # 后面的统一比较逻辑会自然认为所有日期都不完整(即空湖中全量建立 dim_trade_calendar 数据集)

    # next(calendar_path.rglob('*.parquet'), None) 在交易日历目录及其所有子目录中匹配第一个 Parquet 文件。
    if calendar_path.is_dir() and next(calendar_path.rglob('*.parquet'), None) is not None:
    # .rglob() 是 pathlib 模块的递归遍历方法，返回一个迭代器
    # next() 从迭代器中取出下一个元素

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
            existing_calendar_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names),
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
        f'function=main() -> ds.dataset() -> validate_calendar_table(); '
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
            f'function=main(); table={TABLE_NAME}; valid_end_date={valid_end_date}; '
            f'valid_grid_count={len(valid_calendar_dates)}\n'
            f'{log_boundary}'
        )
        return

    # 只提交缺失或交易状态已修订的日期；未变化行保留原 updated_at。
    pending_calendar_df = expected_calendar_df.loc[
        expected_calendar_df['calendar_date'].isin(incomplete_calendar_dates),
        TRADE_CALENDAR_SCHEMA.names,
    ].reset_index(drop=True)

    # 待提交日期可能分散在多个年份，不一定组成一段连续自然日，因此关闭连续性检查
    # 主键、派生字段、固定值等其他规则仍然执行
    validate_calendar_table(
        pandas_to_arrow(pending_calendar_df, TRADE_CALENDAR_SCHEMA),
        require_contiguous=False,
    )
    click.echo(
        f'{log_boundary}\n'
        '全历史差异计划已生成 / Full-history reconciliation plan created\n'
        f'function=main(); table={TABLE_NAME}; valid_grid_count={len(valid_calendar_dates)}; '
        f'complete_grid_count={len(valid_calendar_dates) - len(incomplete_calendar_dates)}; '
        f'missing_or_revised_grid_count={len(pending_calendar_df)}\n'
        f'{log_boundary}'
    )

    if write:
        click.echo(
            f'{log_boundary}\n'
            '开始提交全历史差异分区 / Full-history reconciliation commit started\n'
            f'function=main() -> commit_partitions(); rows={len(pending_calendar_df)}; '
            f'lake_root={resolved_lake_root}\n'
            f'{log_boundary}'
        )
        committed_row_count = commit_partitions(pending_calendar_df, resolved_lake_root)
        click.echo(
            f'{log_boundary}\n'
            '全历史差异分区提交完成 / Full-history reconciliation commit completed\n'
            f'function=commit_partitions() -> main(); mode=full_reconciliation; '
            f'committed_rows={committed_row_count}\n'
            f'{log_boundary}'
        )
    else:
        click.echo(
            f'{log_boundary}\n'
            '全历史差异只读运行完成 / Full-history reconciliation dry run completed\n'
            f'function=main(); write=false; validated_rows={len(pending_calendar_df)}\n'
            f'{log_boundary}'
        )


# In[8]:


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
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",]
    main.main(
        args=notebook_args,
        prog_name="b01_trade_calendar",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# In[26]:


# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a01_Futures_Market_Data\b01_trade_calendar.py --write

