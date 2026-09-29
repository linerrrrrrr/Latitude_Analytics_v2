"""b07 第 7—9 项：可信输入、复用转换、分组与循环不变量。"""
import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b07_suspected_session_reconciliation.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


def block(indent, value):
    return textwrap.indent(textwrap.dedent(value).strip() + '\n', ' ' * indent)


replace('b8d9e4f3', 'from typing import Callable\n', '')

# 文件 Schema 从权威定义派生一次；调用方直接传入，不增加转换包装层。
file_schemas = '\n\n'.join(
    f'''{name}_FILE_SCHEMA = pa.schema(
    [field for field in {schema} if field.name not in {name}_PARTITION_COLUMNS],
    metadata={schema}.metadata,
)'''
    for name, schema in [('CALENDAR', 'FUTURES_BAR_CALENDAR_SCHEMA'), ('CONTRACT', 'FUTURES_CONTRACT_CALENDAR_SCHEMA'), ('DAILY', 'FUTURES_DAILY_SCHEMA'), ('MINUTE', 'FUTURES_MINUTE_SCHEMA')]
)
replace('e0a86c30', '# 持久证据标识保持稳定；目录改名不改变规则版本。', file_schemas + '\n\n# 持久证据标识保持稳定；目录改名不改变规则版本。')
replace('595a964d', '''    schema: pa.Schema,
    partition_columns: list[str],
    label: str,
) -> None:''', '''    expected_file_schema: pa.Schema,
    table_name: str,
    label: str,
) -> None:''')
replace('595a964d', block(4, '''
log_table_name = (
    CALENDAR_TABLE_NAME if schema is FUTURES_BAR_CALENDAR_SCHEMA
    else CONTRACT_TABLE_NAME if schema is FUTURES_CONTRACT_CALENDAR_SCHEMA
    else DAILY_TABLE_NAME if schema is FUTURES_DAILY_SCHEMA
    else MINUTE_TABLE_NAME if schema is FUTURES_MINUTE_SCHEMA
    else "unknown"
)
'''), '')
cells['595a964d'].source = cells['595a964d'].source.replace('{log_table_name}', '{table_name}')
replace('595a964d', block(8, '''
expected_file_schema = pa.schema(
    [
        field
        for field in schema
        if field.name not in partition_columns
    ],
    metadata=schema.metadata,
)
'''), '')
for cell_id in ('595a964d',):
    replace(cell_id, 'log_phase = "file_schema"', 'log_phase = "fragment_schema"')

for cell_id in ('57c9061e', '96342530'):
    for name, schema in [('CALENDAR', 'FUTURES_BAR_CALENDAR_SCHEMA'), ('CONTRACT', 'FUTURES_CONTRACT_CALENDAR_SCHEMA'), ('DAILY', 'FUTURES_DAILY_SCHEMA'), ('MINUTE', 'FUTURES_MINUTE_SCHEMA')]:
        source = cells[cell_id].source
        tree = ast.parse(source)
        updates = []
        lines = source.splitlines(keepends=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'validate_read_fragments' and isinstance(node.args[2], ast.Name) and node.args[2].id == schema:
                for argument, text in [(node.args[2], name + '_FILE_SCHEMA'), (node.args[3], name + '_TABLE_NAME')]:
                    updates.append((argument.lineno - 1, argument.col_offset, argument.end_col_offset, text))
        for line, start, end, text in sorted(updates, reverse=True):
            lines[line] = lines[line][:start] + text + lines[line][end:]
        cells[cell_id].source = ''.join(lines)

# 已刚刚通过同一 Schema 的 pandas_to_arrow，无需 arrow_to_pandas 再验一次。
replace('7d33cf49', 'checked_df = arrow_to_pandas(table, FUTURES_BAR_CALENDAR_SCHEMA)', 'checked_df = table.to_pandas(types_mapper=pd.ArrowDtype)')
replace('ea42f696', '''reconciled_df = arrow_to_pandas(
                        reconciled_table, FUTURES_BAR_CALENDAR_SCHEMA
                    ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)''', '''reconciled_df = reconciled_table.to_pandas(
                        types_mapper=pd.ArrowDtype,
                    ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)''')

for variable, values in [('text_fields', ['schedule_signal_reason', 'evidence_source', 'selection_reason', 'quality_reason']), ('comparison_columns', ['ohlc_matches_daily', 'volume_matches_daily', 'money_matches_daily', 'open_interest_matches_daily'])]:
    source = cells['7d33cf49'].source
    node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == variable for t in n.targets))
    lines = source.splitlines(keepends=True)
    old = ''.join(lines[node.lineno - 1:node.end_lineno])
    replace('7d33cf49', old, '')
    replace('7d33cf49', '        for row in table.to_pylist():\n', block(8, f'{variable} = {values!r}') + '        for row in table.to_pylist():\n')

# 删除四次对可信正式输入的完整主键复核，输出和安装复读的主键门禁保留。
for frame, primary_key, message, indent in [
    ('candidate_df', 'CALENDAR_PRIMARY_KEY', '疑似休市候选主键不唯一。', 4),
    ('contract_df', 'CONTRACT_PRIMARY_KEY', '合约 Session 日历分区主键不唯一。', 8),
    ('daily_df', 'DAILY_PRIMARY_KEY', '定向读取的日线事实主键不唯一。', 8),
    ('minute_df', 'MINUTE_PRIMARY_KEY', '定向读取的分钟事实主键不唯一。', 8),
]:
    replace('57c9061e', block(indent, f'if {frame}.duplicated({primary_key}).any():\n    raise ValueError("{message}")'), '')

# 一次候选分组替代每个分区重新筛选全候选表。
replace('57c9061e', block(4, '''
calendar_partition_keys = sorted(
    set(
        candidate_df[
            CALENDAR_PARTITION_COLUMNS
        ].itertuples(index=False, name=None)
    )
)
'''), block(4, '''
candidate_indices_by_partition = candidate_df.groupby(
    CALENDAR_PARTITION_COLUMNS, sort=False, dropna=False, observed=True,
).indices
calendar_partition_keys = sorted(candidate_indices_by_partition)
contract_columns = FUTURES_CONTRACT_CALENDAR_SCHEMA.names
daily_columns = FUTURES_DAILY_SCHEMA.names
minute_columns = FUTURES_MINUTE_SCHEMA.names
'''))
replace('57c9061e', block(8, '''
partition_candidate_mask = pd.Series(
    True,
    index=candidate_df.index,
)
for column, value in zip(
    CALENDAR_PARTITION_COLUMNS,
    calendar_partition_key,
    strict=True,
):
    partition_candidate_mask &= candidate_df[column].eq(value)
partition_candidate_df = candidate_df.loc[
    partition_candidate_mask
]
'''), block(8, '''
partition_candidate_df = candidate_df.iloc[
    candidate_indices_by_partition[calendar_partition_key]
]
'''))
replace('57c9061e', '    candidate_table = calendar_dataset.to_table(\n', '    calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names\n    candidate_table = calendar_dataset.to_table(\n')
replace('57c9061e', 'columns=FUTURES_BAR_CALENDAR_SCHEMA.names', 'columns=calendar_columns', count=2)
for name, schema in [('contract', 'FUTURES_CONTRACT_CALENDAR_SCHEMA'), ('daily', 'FUTURES_DAILY_SCHEMA'), ('minute', 'FUTURES_MINUTE_SCHEMA')]:
    replace('57c9061e', f'columns={schema}.names', f'columns={name}_columns')

# 当前叶内只分组一次；每个候选通过位置索引取得同合约日，不复制只读切片。
groups = block(20, '''
calendar_indices_by_day = updated_df.groupby(
    ["bar_frequency", "contract_code", "trading_date"],
    sort=False, dropna=False, observed=True,
).indices
contract_day_columns = ["contract_code", "trading_date"]
contract_indices_by_day = contract_df.groupby(
    contract_day_columns, sort=False, dropna=False, observed=True,
).indices
daily_indices_by_day = daily_df.groupby(
    contract_day_columns, sort=False, dropna=False, observed=True,
).indices
minute_indices_by_day = minute_df.groupby(
    contract_day_columns, sort=False, dropna=False, observed=True,
).indices
''')
evidence_source = block(24, '''
# 显式规则版本只标识本次校对算法，不再承担输入指纹或自动重跑水位。
evidence_source = (
    "c07:daily_vs_other_sessions:"
    f"rule={EVIDENCE_RULE_VERSION}"
)
''')
replace('ea42f696', evidence_source, '')
replace('ea42f696', '                    for index in candidate_indices:\n', groups + textwrap.indent(textwrap.dedent(evidence_source), ' ' * 20) + '                    for index in candidate_indices:\n')
replace('ea42f696', block(24, '''
same_day_calendar_df = updated_df.loc[
    updated_df["bar_frequency"].eq("1m")
    & updated_df["contract_code"].eq(contract_code)
    & updated_df["trading_date"].eq(trading_date)
].copy()
'''), block(24, '''
contract_day_key = (contract_code, trading_date)
same_day_calendar_df = updated_df.iloc[
    calendar_indices_by_day.get(("1m", *contract_day_key), [])
]
'''))
for name in ('contract', 'daily', 'minute'):
    replace('ea42f696', block(24, f'''
same_day_{name}_df = {name}_df.loc[
    {name}_df["contract_code"].eq(contract_code)
    & {name}_df["trading_date"].eq(trading_date)
].copy()
'''), block(24, f'''
same_day_{name}_df = {name}_df.iloc[
    {name}_indices_by_day.get(contract_day_key, [])
]
'''))

# 这些状态/数值已由上游正式提交证明；保留 b07 自身需要的完成/零行/完整条件。
replace('ea42f696', block(24, '''
if not row["is_data_missing"]:
    issues.append("当前疑似 Session 未记录缺失状态")
if row["missing_bar_count"] != row["expected_bar_count"]:
    issues.append("当前疑似 Session 的缺失条数无法由理论条数复算")
'''), '')
replace('ea42f696', block(24, '''
tick_sizes = pd.to_numeric(
    same_day_contract_df["tick_size"],
    errors="coerce",
).dropna().astype(float).unique()
'''), block(24, '''
tick_sizes = same_day_contract_df["tick_size"].dropna().unique()
'''))
replace('ea42f696', block(24, '''
# 每个保留 Session 必须由日历状态和正式分钟计数共同证明完整。
minute_counts = (
    retained_minute_df.groupby("session_number", dropna=False)
    .size()
    .to_dict()
)
'''), block(24, '# 每个保留 Session 使用上游正式提交的完成状态和条数证明完整。'))
replace('ea42f696', block(28, 'fact_count = int(minute_counts.get(retained_number, 0))'), '')
replace('ea42f696', block(32, '''
or retained_row["missing_bar_count"] != 0
or retained_row["is_data_missing"]
or fact_count != retained_row["expected_bar_count"]
'''), '')
replace('ea42f696', block(36, '''
if not np.isfinite(normalized_value):
    raise ValueError(
        f"日线直接依赖字段 {name} 包含非有限数。"
    )
'''), '')

# 已校验 Arrow 叶直接拼接，不再次制造合并 Pandas 表并重复转换。
replace('96342530', block(8, 'checked_frames: dict[tuple[object, ...], pd.DataFrame] = {}'), '')
replace('96342530', block(12, 'checked_frames[partition_key] = checked_df'), '')
replace('96342530', block(8, '''
combined_df = pd.concat(
    list(checked_frames.values()),
    ignore_index=True,
)
combined_table = pandas_to_arrow(combined_df, FUTURES_BAR_CALENDAR_SCHEMA)
'''), block(8, 'combined_table = pa.concat_tables(list(expected_tables.values()))'))
replace('96342530', block(12, '''
expected_file_schema = pa.schema(
    [
        field
        for field in FUTURES_BAR_CALENDAR_SCHEMA
        if field.name not in CALENDAR_PARTITION_COLUMNS
    ],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)
'''), '')
replace('96342530', '                    expected_file_schema,\n', '                    CALENDAR_FILE_SCHEMA,\n')

# 更新实际变动的说明与图，不修改规范本身或其他生产入口。
replace('782170ec', '当前入口还保留候选与定向输入的主键检查；这里描述现状，本轮不调整检查或转换次数。', '候选与输入表的主键唯一性由各自生产者保证，入口不再重复检查；自身 dirty 输出叶及 staging/正式叶的主键门禁仍保留。')
cells['782170ec'].source += '\n\n四张文件 Schema 从权威 Schema 和分区列在模块加载时派生，`validate_read_fragments()` 直接接收文件 Schema 与表名。候选先按日历分区建立位置索引，分区循环直接取本组；当前没有逐分区扫描全湖事实或重新证明全历史的检查，物化仍限定在当前日历叶、合约月及候选品种月。'
replace('d9a63ecb', '直接使用的非空非有限证据值会抛错。', '正式日线的有限数门禁由 b05 保证；本环节保留聚合结果的非有限检查，因为求和可能产生新的溢出。')
cells['d9a63ecb'].source += '\n\n四份输入在候选循环前各按合约日建立位置索引，避免每个候选重复筛选当前叶全表；旁证来源字符串也在循环前构造。tick 已由读取契约固定为数值，仅保留缺值、唯一性、有限且为正的比较前提。当前疑似 Session 仍要求已完成且实际 0 行；其他保留 Session 仍要求已完成且实际条数等于理论条数，直接信任上游提交的计数，不从事实重数，也不再次复算同义的缺失标志。'
cells['2a85dca5'].source += '\n\nPandas 输入先经 `pandas_to_arrow()` 完成契约校验，随后直接以 `pd.ArrowDtype` 转回 Pandas；不再立刻调用 `arrow_to_pandas()` 重做同一类型门禁。逐行固定说明字段和四项比较列在行循环前定义，全部业务校验保持原样。'
cells['e492cfe6'].source += '\n\n提交保留逐叶完整业务校验与契约转换；全部叶的已校验 Arrow 表直接 `pa.concat_tables()` 拼接，省去合并 Pandas 副本及其再次转 Arrow。生成结果的输出契约转换仍保留，只移除紧接其后的重复类型校验。'
diagram_changes = {
    'b07-flow-constants': [('构造四张表的 Hive partitioning', '构造四张表的 Hive partitioning 与文件 Schema')],
    'b07-flow-read': [('调用方构造候选条件或 partition_expression 等值条件', '候选一次分组；调用方构造当前分区条件')],
    'b07-flow-reconcile': [('记录生成开始；复制完整叶并选出候选', '记录开始；选出候选并建立合约日位置索引'), ('逐候选取同合约日数据；清空旧旁证', '按索引取同合约日数据；清空旧旁证'), ('检查其余 Session 状态、完整条数及 OHLC 关系', '信任提交的完成状态与条数；检查旁证 OHLC'), ('非空非有限证据值', '聚合结果非有限')],
    'b07-flow-commit': [('合并写 staging；检查 Schema、主键唯一性和行数', '拼接已校验 Arrow 叶写 staging；复读契约和主键行数')],
}
for cell_id, pairs in diagram_changes.items():
    for old, new in pairs:
        replace(cell_id, old, new)

assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
old_comments, new_comments = [], []
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        compile(new.source, str(PATH), 'exec')
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        old_comments.extend(comments(old.source))
        new_comments.extend(comments(new.source))
old_comments = [s.replace('每个保留 Session 必须由日历状态和正式分钟计数共同证明完整。', '每个保留 Session 使用上游正式提交的完成状态和条数证明完整。') for s in old_comments]
assert sorted(old_comments) == sorted(new_comments)
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)

# 唯一现有外部调用方为草稿门禁测试，同步内部函数参数。
test_path = ROOT / '00_draft_collection_02/tests/test_b01_fragment_schema_guards.py'
test_source = test_path.read_text(encoding='utf-8')
tree = ast.parse(test_source)
lines = test_source.splitlines(keepends=True)
updates = []
for node in ast.walk(tree):
    if isinstance(node, ast.Call) and ast.unparse(node.func) == 'C07.validate_read_fragments':
        for argument, value in [(node.args[2], 'C07.CALENDAR_FILE_SCHEMA'), (node.args[3], 'C07.CALENDAR_TABLE_NAME')]:
            updates.append((argument.lineno - 1, argument.col_offset, argument.end_col_offset, value))
assert len(updates) == 4
for line, start, end, value in sorted(updates, reverse=True):
    lines[line] = lines[line][:start] + value + lines[line][end:]
test_path.write_text(''.join(lines), encoding='utf-8', newline='\n')
print('b07 validation/IO contraction applied; notebook state preserved; one obsolete code comment updated.')
