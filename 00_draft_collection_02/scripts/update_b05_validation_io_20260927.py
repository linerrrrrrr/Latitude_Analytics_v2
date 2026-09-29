"""b05 第 7—9 项：收缩已证明的重复检查，预分组并复用循环不变量。"""

import ast
import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b05_futures_daily.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


# 主流程只拼接已校验且主键范围互斥的批次；保留原来的全局主键排序。
replace('71d11aec', '''    collected_df = (
        validate_daily_frame(
            pd.concat(collected_frames, ignore_index=True),
            "本次全部 JQData 结果",
            validate_predecessor_relationships=False,
        )
        if collected_frames
        else empty_pandas(FUTURES_DAILY_SCHEMA)
    )''', '''    # 各批已校验；请求按交易所、年和合约拆分，批次输出主键互斥。
    collected_df = (
        pd.concat(collected_frames, ignore_index=True)
        .sort_values(PRIMARY_KEY)
        .reset_index(drop=True)
        if collected_frames
        else empty_pandas(FUTURES_DAILY_SCHEMA)
    )''')

# 来源响应归一化已证明唯一；外连接不引入重复键，最终输出校验仍保留。
replace('d180c167', '            validate="one_to_one",\n', '', count=3)

# 输入只用于读与转换，函数后续只操作 Arrow 转回的独立 checked_df。
replace('79ac3c03', 'source_df = frame.loc[:, FUTURES_DAILY_SCHEMA.names].copy()',
        'source_df = frame.loc[:, FUTURES_DAILY_SCHEMA.names]')

# rename_axis("time").reset_index() 成功后必有 time；原第二个缺列分支不可达。
replace('107d2b6d', '''    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename(
            columns={normalized_df.columns[0]: "time"}
        )
''', '')

# 本次 dirty 集合只分组一次；每个分区通过行位置读取自己的切片。
replace('71d11aec', '''    dirty_calendar_keys = set(
        dirty_policy_df[["exchange_code", "year", "month"]].itertuples(
            index=False,
            name=None,
        )
    )
    dirty_calendar_keys |= set(
        collected_df[["exchange_code", "year", "month"]].itertuples(
            index=False,
            name=None,
        )
    )''', '''    base_partition_columns = [
        name for name in CALENDAR_PARTITION_COLUMNS if name != "bar_frequency"
    ]
    policy_row_indices_by_partition = dirty_policy_df.groupby(
        base_partition_columns, sort=False
    ).indices
    fact_row_indices_by_partition = collected_df.groupby(
        base_partition_columns, sort=False
    ).indices
    dirty_calendar_keys = (
        set(policy_row_indices_by_partition) | set(fact_row_indices_by_partition)
    )''')
replace('71d11aec', '''        policy_mask = (
            dirty_policy_df["exchange_code"].eq(exchange_code)
            & dirty_policy_df["year"].eq(year)
            & dirty_policy_df["month"].eq(month)
        )
        fact_mask = (
            collected_df["exchange_code"].eq(exchange_code)
            & collected_df["year"].eq(year)
            & collected_df["month"].eq(month)
        )
        leaf_policy_df = dirty_policy_df.loc[policy_mask].reset_index(
            drop=True
        )
        leaf_fact_df = collected_df.loc[fact_mask].reset_index(drop=True)''', '''        leaf_policy_df = dirty_policy_df.iloc[
            policy_row_indices_by_partition.get(base_key, [])
        ].reset_index(drop=True)
        leaf_fact_df = collected_df.iloc[
            fact_row_indices_by_partition.get(base_key, [])
        ].reset_index(drop=True)''')

# 同一个规格在标记检查、staging、安装及正式复读阶段共用文件 Schema。
replace('b43616a1', '''                "complete_table": complete_table,
                "relative_path": relative_path,''', '''                "complete_table": complete_table,
                "relative_path": relative_path,
                "file_schema": parquet_file_schema(schema, partition_columns),''')
replace('b43616a1', '''            expected_marker_schema = parquet_file_schema(
                spec["schema"],
                spec["partition_columns"],
            )''', '''            expected_marker_schema = spec["file_schema"]''')
replace('b43616a1', '''                expected_file_schema = parquet_file_schema(
                    spec["schema"],
                    spec["partition_columns"],
                )''', '''                expected_file_schema = spec["file_schema"]''', count=2)
replace('b43616a1', '''                        pa.Table.from_batches([], schema=parquet_file_schema(
                            spec["schema"],
                            spec["partition_columns"],
                        )),''', '''                        pa.Table.from_batches([], schema=spec["file_schema"]),''')

# 准备多个完整事实叶时，Schema 列投影不随分区改变。
replace('b5abb5b2', '''        specs = []
        for partition_values, incoming_df''', '''        specs = []
        fact_columns = FUTURES_DAILY_SCHEMA.names
        for partition_values, incoming_df''')
replace('b5abb5b2', '''                FUTURES_DAILY_SCHEMA.names,
            ]''', '''                fact_columns,
            ]''')
replace('b5abb5b2', 'incoming_df.loc[:, FUTURES_DAILY_SCHEMA.names]', 'incoming_df.loc[:, fact_columns]')

# 回看长度对所有请求组及其中的合约不变。
replace('bc59b121', '''        batches = []

        # 先按交易所''', '''        batches = []
        lookback_delta = timedelta(days=LOOKBACK_CALENDAR_DAYS)

        # 先按交易所''')
replace('bc59b121', '- timedelta(days=LOOKBACK_CALENDAR_DAYS)', '- lookback_delta', count=3)

# 文档与图只更新本次实际边界，不改变 Schema 或生产运行语义。
replace('d5230ef4',
        '当前采集批与本次采集汇总均调用它，并关闭前序行关系检查；写入前在合并后的 dirty 完整事实叶上执行完整业务校验。',
        '每个采集批调用它，并关闭前序行关系检查；请求组按交易所、年和合约拆分，批次主键范围互斥，入口只拼接并保持全局主键排序，不重复完整校验或 Pandas/Arrow 往返。写入前仍在合并后的 dirty 完整事实叶上执行完整业务校验。')
cells['fbc92c70'].source += '\n\n三类归一化响应分别检查主键唯一性，外连接保持这一性质，待办键由 b04 及请求拆分保证唯一，因此三次 merge 不再重复执行 `validate="one_to_one"`。来源归一化仍检查包含回看期的全部响应；输出校验仍覆盖新生成的事实，不能用最终待办子集校验替代来源门禁。'
cells['46ad3856'].source += '\n\n价格响应缺少 time 列时，仍通过 `rename_axis("time").reset_index()` 恢复；成功后必有 time 列，删除其后不可达的再次改名分支。'
cells['720c2940'].source += '\n\n规格准备阶段为每个叶构造一次文件 Schema，后续零行标记检查、staging 复读、标记创建和正式复读均复用它；不改变这些不同 I/O 阶段各自的验收。准备阶段的 Pandas→Arrow 转换仍是实际写 Parquet 所需，不移除。'
cells['d7734812'].source += '\n\n事实 Schema 的列投影在分区循环前读取一次；读取、合并和完整业务校验仍逐 dirty 叶执行。'
cells['57f0d8d8'].source += '\n\n回看 `timedelta` 在分组与合约循环前构造一次，所有批次复用同一长度。'
replace('c2d11e3b', '采集后汇总校验，再根据 `--write`', '采集后拼接已校验结果，再根据 `--write`')
cells['c2d11e3b'].source += '\n\n进入提交循环前，政策与事实各建立一次按交易所—年月的行位置索引，循环中直接取当前分组，避免每个分区重新比较整批数据的三列。这里优化的是本次内存结果；正式湖没有逐分区全表扫描：上游根 Dataset 启动时检查并做一次窄列规划，提交链路只读取触达的完整叶。缺少零行标记时保留原有的 Parquet 存在性探测，不据此扫描全历史事实重建完成状态。'
replace('b05-flow-overview', '汇总已采集事实并校验', '拼接已校验事实；保持主键排序')
replace('b05-flow-main', '汇总校验；报告 api_result', '拼接已校验批次；报告 api_result')
replace('b05-flow-main', '调用生成与提交函数；报告整批累计量', '按预分组索引取叶；生成、提交并累计')
replace('b05-flow-commit', '分区、重复目标与零行标记门禁', '检查目标；预备文件 Schema 并核对标记')

assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        old_comments, new_comments = comments(old.source), comments(new.source)
        retained = iter(new_comments)
        assert all(any(current == original for current in retained) for original in old_comments), old.id
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        ast.parse(new.source)
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b05: removed aggregate revalidation, redundant merge checks/copy/fallback; pre-grouped current batch; reused file schemas, columns and lookback delta.')
