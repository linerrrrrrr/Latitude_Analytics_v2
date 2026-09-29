"""b06 第 7—9 项：收缩已证明的重复操作，复用分组与循环不变量。"""

import ast
import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b06_futures_minute.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


replace('5158a600', 'import time\nimport time\n', 'import time\n')
replace('5158a600',
        'from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETY_PAIRS',
        'from config.futures_lakehouse.futures_fact_collection_policy import FUTURES_FACT_VARIETIES_BY_EXCHANGE')
cells['3c31bb50'].source += '''

# 共享政策在本次模块加载期间固定；每个交易所的 Arrow 集合只构造一次。
SELECTED_UNDERLYINGS_BY_EXCHANGE = {
    exchange_code: pa.array(
        sorted(underlying_codes),
        type=FUTURES_BAR_CALENDAR_SCHEMA.field("underlying_code").type,
    )
    for exchange_code, underlying_codes in FUTURES_FACT_VARIETIES_BY_EXCHANGE.items()
}
'''

# main 已按规划 Schema 转换；函数只计算掩码，不再次转换相同表。
replace('c06-arrow-policy-plan', 'log_phase = "planning_schema"', 'log_phase = "whitelist"')
replace('c06-arrow-policy-plan', '''        planned_table = planning_table.cast(
            MINUTE_PLANNING_SCHEMA, safe=True
        )
        log_phase = "whitelist"
        selected_underlyings = sorted(
            underlying_code
            for policy_exchange, underlying_code in FUTURES_FACT_VARIETY_PAIRS
            if policy_exchange == exchange_code
        )
        if selected_underlyings:''', '''        selected_underlyings = SELECTED_UNDERLYINGS_BY_EXCHANGE.get(exchange_code)
        if selected_underlyings is not None:''')
replace('c06-arrow-policy-plan', '''                value_set=pa.array(
                    selected_underlyings,
                    type=planned_table["underlying_code"].type,
                ),''', '''                value_set=selected_underlyings,''')
cells['c06-arrow-policy-plan'].source = cells['c06-arrow-policy-plan'].source.replace('planned_table', 'planning_table')

# 空表已提前返回；Session 关联的 many_to_one 已覆盖紧邻的重复键检查。
replace('8bfa11f6', 'if sessions_df is not None and not checked_df.empty:', 'if sessions_df is not None:')
replace('8bfa11f6', '''        if session_boundaries_df.duplicated(SESSION_KEY).any():
            raise ValueError(f"{context}待办 Session 主键不唯一。")
''', '')

# 此处只从可信正式事实中选出待替换键，不重新证明历史主键或排序。
replace('ed1ba9df', '        validate="many_to_one",\n', '')
replace('ed1ba9df', '''    if selected_df.duplicated(PRIMARY_KEY).any():
        raise ValueError("Session 选取结果主键不唯一。")
    return selected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)''', '''    return selected_df''')

# 每个物理文件已经通过字段类型门禁；保留统一 Arrow→Pandas 契约转换。
replace('126fe7bc', 'logical_arrays.append(file_table[field.name].cast(field.type, safe=True))',
        'logical_arrays.append(file_table[field.name])')

# rename_axis/reset_index 成功后必有 time，第二个分支不可达。
replace('41d66b26', '''    if "time" not in normalized_df.columns:
        normalized_df = normalized_df.rename(
            columns={normalized_df.columns[0]: "time"}
        )
''', '')

replace('b31dc6c2', '''        batches = request_batches(sessions_df)
''', '''        fact_columns = FUTURES_MINUTE_SCHEMA.names
        inherited_columns = [
            "exchange_code",
            "underlying_code",
            "trading_date",
            "session_number",
            "year",
            "month",
        ]
        batches = request_batches(sessions_df)
''')
replace('b31dc6c2', '''                pd.DatetimeIndex(pd.to_datetime(batch_df["session_start_at"])),
                pd.DatetimeIndex(pd.to_datetime(batch_df["session_end_at"])),''', '''                pd.DatetimeIndex(batch_df["session_start_at"]),
                pd.DatetimeIndex(batch_df["session_end_at"]),''')
replace('b31dc6c2', 'pd.to_datetime(normalized_df["bar_at"])', 'normalized_df["bar_at"]')
replace('b31dc6c2', 'selected_df = normalized_df.reset_index(drop=True).copy()',
        'selected_df = normalized_df.reset_index(drop=True)')
replace('b31dc6c2', '''            for column in [
                "exchange_code",
                "underlying_code",
                "trading_date",
                "session_number",
                "year",
                "month",
            ]:''', '''            for column in inherited_columns:''')
replace('b31dc6c2', 'selected_df.loc[:, FUTURES_MINUTE_SCHEMA.names]', 'selected_df.loc[:, fact_columns]')

# collect_partition 已返回主键排序且不再修改的本次结果；提交不需要重排它。
replace('3ee121e7', 'return collected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)',
        'return collected_df')

# 完成摘要消费采集时的异常计数。事实提交函数返回同一采集结果，没有第二份独立证据。
replace('c06-unified-calendar-commit', '''        invalid_df = committed_requested_df.loc[
            invalid_ohlc_mask(committed_requested_df)
        ]
        invalid_counts = (
            invalid_df.groupby(SESSION_KEY, dropna=False).size()
            if not invalid_df.empty
            else pd.Series(dtype="int64")
        )
        log_phase = "verify_quality_counts"
        reported_invalid_counts = {
            key: int(value)
            for key, value in invalid_session_counts.items()
            if int(value) > 0
        }
        if invalid_counts.to_dict() != reported_invalid_counts:
            raise ValueError(
                "已提交事实中的 OHLC 异常计数与本次响应审计不一致。"
            )
''', '''        invalid_counts = pd.Series(invalid_session_counts, dtype="int64")
''')
replace('c06-unified-calendar-commit', '''        session_summary_df = sessions_df.loc[
            :, SESSION_KEY
        ].drop_duplicates().copy()''', '''        summary_partition_columns = [
            name for name in CALENDAR_PARTITION_COLUMNS if name != "bar_frequency"
        ]
        summary_columns = [*SESSION_KEY, "_actual_count", "_invalid_count"]
        session_summary_df = sessions_df.loc[
            :, [*SESSION_KEY, *summary_partition_columns]
        ].copy()''')
replace('c06-unified-calendar-commit', '''        for partition_values, partition_sessions_df in sessions_df.groupby(
            ["exchange_code", "year", "month"], sort=True
        ):''', '''        for partition_values, partition_summary_df in session_summary_df.groupby(
            summary_partition_columns, sort=True
        ):''')
replace('c06-unified-calendar-commit', '''            partition_keys_df = partition_sessions_df.loc[
                :, SESSION_KEY
            ].drop_duplicates()
            updates_by_partition[partition_key] = partition_keys_df.merge(
                session_summary_df,
                on=SESSION_KEY,
                how="left",
                validate="one_to_one",
            )''', '''            updates_by_partition[partition_key] = partition_summary_df.loc[
                :, summary_columns
            ].reset_index(drop=True)''')
# 回写保留 join 的一对一检查，不在它前面单独检查同一个完成摘要键。
replace('c06-unified-calendar-commit', '''                if completion_updates_df.duplicated(SESSION_KEY).any():
                    raise ValueError(
                        f"完成摘要 Session 主键不唯一：{partition_key}"
                    )
''', '')
replace('c06-unified-calendar-commit', '''        for partition_key in partition_keys:
''', '''        calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
        for partition_key in partition_keys:
''')
replace('c06-unified-calendar-commit', ''':, FUTURES_BAR_CALENDAR_SCHEMA.names
            ]''', ''':, calendar_columns
            ]''')

replace('86251818', '''    for partition_number, calendar_partition_key in enumerate(
''', '''    requested_date_filter = None
    if requested_start_date is not None:
        requested_date_filter = (
            ds.field("trading_date") >= requested_start_date
        ) & (ds.field("trading_date") <= requested_end_date)
    for partition_number, calendar_partition_key in enumerate(
''')
replace('86251818', '''        if requested_start_date is not None:
            calendar_filter &= (
                ds.field("trading_date") >= requested_start_date
            ) & (ds.field("trading_date") <= requested_end_date)''', '''        if requested_date_filter is not None:
            calendar_filter &= requested_date_filter''')

cells['3af2af5f'].source += '\n\n空分钟结果在函数前段已返回；提供 Session 时保留关联的 `many_to_one` 约束，移除关联前对同一键的重复检查。待替换旧分钟来自可信正式叶，待办 Session 来自 b04 唯一格点的筛选，因此 `rows_for_sessions()` 只选取所需键，不再重复证明上游主键或排序；合并后的 dirty 完整叶仍执行完整业务校验。'
cells['f21ba82b'].source += '\n\n叶文件物理类型通过检查后，补回分区字段时直接复用非分区列，不再逐列做同类型 cast；最终仍使用统一 `arrow_to_pandas()` 完成契约转换。'
replace('667a1d22', '`minute_policy_plan()` 按交易所取共享白名单，',
        '`minute_policy_plan()` 按交易所取模块初始化时从共享配置生成的 Arrow 白名单集合，')
cells['667a1d22'].source += '\n\n入口读取窄表后按 `MINUTE_PLANNING_SCHEMA` 转换一次，规划函数消费这一已转换结果；不再对同一表重复 cast，也不在每个日历分区重建同一交易所的白名单数组。'
cells['702de1a0'].source += '\n\n时间索引恢复成功后必有 time 列，移除其后的不可达补救分支。Session 起止与归一化后的 bar_at 已是时间类型，构造 DatetimeIndex 时不再先做重复解析；无损单位对齐保留。固定列清单在请求循环前准备，reset_index 后不再追加复制。来源非有限数检查、输出分钟/Session 校验均保留。'
cells['481fb127'].source += '\n\n采集函数已经按分钟主键排序；事实提交成功后直接返回该采集结果，不再次排序和重置索引。该返回值没有从正式湖重读行情值，完成摘要使用它及同一次采集的异常计数。'
replace('c6ef32c3',
        '`build_calendar_completion_updates()` 在事实叶成功后，按 Session 生成实际条数与 OHLC 异常数，核对采集阶段记录的异常计数，并按日历叶组织摘要。',
        '`build_calendar_completion_updates()` 在事实叶成功后，按 Session 统计本次分钟条数，复用采集阶段已经记录的 OHLC 异常计数，并按日历叶组织摘要。')
cells['c6ef32c3'].source += '\n\n待办 Session 的唯一性由 b04 和规划筛选保证，生成摘要不再去重。完整摘要携带分区列并一次 groupby，循环直接取当前分组，移除每个分区与整批摘要重新 merge 的操作。日历回写仍保留一对一关联、覆盖检查和计数边界；移除紧邻关联之前的重复键检查，固定日历列清单在分区循环前准备。'
cells['42b891dc'].source += '\n\n正式湖没有逐分区重开表根或重复全表业务质检：根 Dataset 物理门禁各执行一次，日历窄列按当前分区过滤读取，提交只读取 dirty 完整叶。默认规划仍需遍历全历史理论格点以应用当前白名单；显式日期过滤表达式在规划循环前构造一次。'
replace('b06-flow-session-rows', '检查选取主键并排序返回', '返回待替换旧分钟')
replace('b06-flow-policy', '记录规划开始；窄表与交易所白名单', '已转换窄表；复用交易所白名单数组')
replace('b06-flow-calendar-updates', '统计实际及异常条数；核对响应审计', '统计实际条数；复用采集异常计数')
replace('b06-flow-calendar-updates', '生成摘要；记录 persisted=false；入口汇集', '摘要一次分组；persisted=false；入口汇集')

assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        retained = iter(comments(new.source))
        assert all(any(current == original for current in retained) for original in comments(old.source)), old.id
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        ast.parse(new.source)
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b06: redundant checks/conversions removed; summaries grouped once; loop invariants reused.')
