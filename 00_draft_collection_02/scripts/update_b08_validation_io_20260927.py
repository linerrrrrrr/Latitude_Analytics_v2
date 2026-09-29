"""b08 第 7—9 项：收缩重复验收，限定 staging 叶读取，外移循环不变量。"""
import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b08_full_minute_quality.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


def block(indent, source):
    return textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * indent)


# 调用方已经完成权威类型转换；摘要只负责逻辑内容的确定性编码。
replace('c08-dataset-helpers', '    log_phase = "contract"\n', '    log_phase = "sort"\n')
replace('c08-dataset-helpers', '        typed_table = validate_arrow_table(table, schema)\n', '')
replace('c08-dataset-helpers', '        log_phase = "sort"\n', '')
replace('c08-dataset-helpers', '            typed_table,\n', '            table,\n')
replace('c08-dataset-helpers', '        sorted_table = typed_table.take(sort_indices).combine_chunks()\n', '        sorted_table = table.take(sort_indices)\n')

# 缺失输出已经经过 polars_to_arrow；只在此执行一次业务键/分区检查。
source = cells['c08-output-validation'].source
split = source.index('\ndef validate_calendar_audit_output(')
missing_source, calendar_source = source[:split], source[split:]
old = block(8, '''
checked_table = validate_arrow_table(
    table,
    FUTURES_MISSING_BAR_SCHEMA,
)
''')
assert missing_source.count(old) == 1
missing_source = missing_source.replace(old, '').replace('checked_table', 'table')
missing_source = missing_source.replace('log_phase = "conversion"', 'log_phase = "primary_key"')
missing_source = missing_source.replace('        log_phase = "primary_key"\n', '')
cells['c08-output-validation'].source = missing_source + calendar_source
replace('c08-output-validation', block(8, '''
checked_frame = arrow_to_pandas(
    checked_table,
    FUTURES_BAR_CALENDAR_SCHEMA,
)
'''), block(8, 'checked_frame = checked_table.to_pandas(types_mapper=pd.ArrowDtype)'))

# 全局就绪检查早于 staging；后面的同一叶检查没有增加前提。
replace('c08-build', block(12, '''
if not selected_df["is_fetch_completed"].all():
    pending_examples = selected_df.loc[
        selected_df["is_fetch_completed"].ne(True),
        CALENDAR_PRIMARY_KEY,
    ].head(5)
    raise ValueError(
        "分钟缺失审计只能在全部 required Session 完成 b06 后运行；"
        f"示例：{pending_examples.to_dict(orient='records')}"
    )
'''), '')

# schema 列表和同一批使用的表达式只建立一次，表达式仍在各组求值。
replace('c08-build', '        for calendar_partition_key in calendar_partition_keys:\n', block(8, '''
calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
missing_columns = FUTURES_MISSING_BAR_SCHEMA.names
missing_partition_base_dir = missing_staging_path.as_posix()
calendar_partition_base_dir = calendar_staging_path.as_posix()
session_input_columns = [
    "_calendar_index", "contract_code", "exchange_code", "underlying_code",
    "trading_date", "session_number", "session_start_at", "session_end_at",
    "expected_bar_count", "year", "month",
]
expected_range_expression = pl.datetime_ranges(
    pl.col("session_start_at") + pl.duration(minutes=1),
    pl.col("session_end_at"),
    interval="1m",
    closed="both",
).alias("expected_bar_at")
expected_time_expression = pl.col("expected_bar_at").cast(
    pl.Datetime("us", "Asia/Shanghai")
)
missing_audit_expressions = [
    pl.lit("1m").alias("bar_frequency"),
    pl.lit(detected_at).alias("detected_at"),
]
''') + '\n        for calendar_partition_key in calendar_partition_keys:\n')
replace('c08-build', 'columns=FUTURES_BAR_CALENDAR_SCHEMA.names', 'columns=calendar_columns', 2)
replace('c08-build', 'columns=FUTURES_MISSING_BAR_SCHEMA.names', 'columns=missing_columns')
replace('c08-build', '.select(FUTURES_MISSING_BAR_SCHEMA.names)', '.select(missing_columns)')
replace('c08-build', '                    .select(["contract_code", "expected_bar_at"])\n', '')
replace('c08-build', block(20, '''
.loc[:, [
    "_calendar_index",
    "contract_code",
    "exchange_code",
    "underlying_code",
    "trading_date",
    "session_number",
    "session_start_at",
    "session_end_at",
    "expected_bar_count",
    "year",
    "month",
]]
'''), block(20, '.loc[:, session_input_columns]'))
replace('c08-build', block(20, '''
sessions.with_columns(
    pl.datetime_ranges(
        pl.col("session_start_at")
        + pl.duration(minutes=1),
        pl.col("session_end_at"),
        interval="1m",
        closed="both",
    ).alias("expected_bar_at")
)
.explode("expected_bar_at")
.with_columns(
    pl.col("expected_bar_at").cast(
        pl.Datetime("us", "Asia/Shanghai")
    )
)
'''), block(20, '''
sessions.with_columns(expected_range_expression)
.explode("expected_bar_at")
.with_columns(expected_time_expression)
'''))
replace('c08-build', block(24, '''
missing_points.with_columns([
    pl.lit("1m").alias("bar_frequency"),
    pl.lit(detected_at).alias("detected_at"),
])
'''), block(24, 'missing_points.with_columns(missing_audit_expressions)'))
replace('c08-build', '                        .sort(MISSING_PRIMARY_KEY)\n', '')
replace('c08-build', '                    .astype("int64")\n', '')

# 每次只打开刚写出的完整叶，Hive 分区值仍从 staging 根下的路径解析。
for name, indent, table_name, columns, key in [
    ('missing', 20, 'FUTURES_MISSING_BAR_SCHEMA', 'MISSING_PARTITION_COLUMNS', 'missing_partition_key'),
    ('calendar', 12, 'FUTURES_BAR_CALENDAR_SCHEMA', 'CALENDAR_PARTITION_COLUMNS', 'calendar_partition_key'),
]:
    upper = name.upper()
    replace('c08-build', block(indent, f'''
staged_{name}_dataset = ds.dataset(
    {name}_staging_path,
    format="parquet",
    partitioning={upper}_PARTITIONING,
)
staged_{name}_table = staged_{name}_dataset.to_table(
    columns={name}_columns,
    filter=partition_expression(
        {columns},
        {key},
    ),
)
'''), block(indent, f'''
staged_{name}_leaf_path = {name}_staging_path.joinpath(*[
    f"{{name}}={{value}}"
    for name, value in zip(
        {columns}, {key}, strict=True,
    )
])
staged_{name}_dataset = ds.dataset(
    staged_{name}_leaf_path,
    format="parquet",
    partitioning={upper}_PARTITIONING,
    partition_base_dir={name}_partition_base_dir,
)
staged_{name}_table = validate_arrow_table(
    staged_{name}_dataset.to_table(columns={name}_columns),
    {table_name},
)
'''))

replace('c08-build', block(20, '''
staged_missing_table = validate_missing_output(
    staged_missing_table,
    missing_partition_key,
    "staging 复读的",
)
'''), '')
replace('c08-build', block(12, '''
staged_calendar_table = validate_calendar_audit_output(
    arrow_to_pandas(
        staged_calendar_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    ),
    calendar_partition_key,
    detected_at,
    "staging 复读的",
)
'''), '')

# 正式路径保留一次物理契约及完整内容摘要，不重新执行缺失叶业务质检。
replace('c08-commit', block(16, '''
table = validate_missing_output(
    table,
    partition_key,
    "正式复读的",
)
'''), block(16, 'table = validate_arrow_table(table, FUTURES_MISSING_BAR_SCHEMA)'))
replace('c08-commit', '                if table_digest(\n                    table,\n                    FUTURES_BAR_CALENDAR_SCHEMA,', '                table = validate_arrow_table(table, FUTURES_BAR_CALENDAR_SCHEMA)\n                if table_digest(\n                    table,\n                    FUTURES_BAR_CALENDAR_SCHEMA,')
replace('c08-commit', '        calendar_digests = audit_result["calendar_digests"]\n', '        calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names\n        missing_columns = FUTURES_MISSING_BAR_SCHEMA.names\n        calendar_digests = audit_result["calendar_digests"]\n')
replace('c08-commit', 'columns=FUTURES_MISSING_BAR_SCHEMA.names', 'columns=missing_columns')
replace('c08-commit', 'columns=FUTURES_BAR_CALENDAR_SCHEMA.names', 'columns=calendar_columns')

replace('c08-dataset-heading', '`table_digest()` 先固定权威类型、metadata 与列顺序，再按主键排序，对全部逻辑内容生成 SHA-256。', '`table_digest()` 接收调用方已经按权威 Schema 转换的 Arrow 表，按主键排序，对全部逻辑内容生成 SHA-256；函数不再次转换或校验同一表。权威类型、metadata 与列顺序由转换和摘要编码共同固定。')
replace('c08-dataset-heading', '此处说明当前实现，不将后续计划中的重复检查收缩提前视为已完成。', '上游、staging 汇总和正式复读各在表根打开后检查一次全部 fragment；逐叶完整内容摘要用于证明复读结果与已验收输出一致。')
replace('c08-dataset-heading', '`table_digest()` 报告契约转换、排序、规范化与摘要完成', '`table_digest()` 报告排序、规范化与摘要完成')
replace('b08-flow-read', 'table_digest：固定权威类型和 metadata', 'table_digest：接收已转换的 Arrow 表')
replace('c08-output-heading', '`validate_missing_output()` 将缺失明细固定为权威 Arrow 契约，检查非空结果的主键唯一性与目标分区归属；零行结果保留契约后返回。', '`validate_missing_output()` 接收已经由 `polars_to_arrow()` 转换的缺失明细，只检查非空结果的主键唯一性与目标分区归属；零行结果直接返回。')
replace('c08-output-heading', '当前生成和 staging 复读仍调用对应输出 validator；正式缺失叶再次校验，正式日历叶通过契约与完整内容摘要验收。', '两个业务 validator 各在生成完整输出叶时执行一次。staging 和正式复读使用权威 Arrow 转换及完整内容摘要比较，不重复主键/分区业务检查或日历计数、状态、时间关系检查。')
replace('c08-output-heading', '类型转换、主键与分区归属、计数关系和审计时间的原有检查均保留。', '日历转换后的 Arrow 表直接转为 Pandas 用于业务检查，不再通过 `arrow_to_pandas()` 对同一 Arrow 表重复校验。主键与分区归属、计数关系和审计时间的原有检查仍在输出边界保留。')
replace('b08-flow-validate', '固定契约；非空时检查主键和分区', '接收已转换表；非空时检查主键和分区')
replace('c08-build-heading', '选出 required 行，叶内再次检查完成状态，再按品种分组。', '选出 required 行后按品种分组；完成状态已由 staging 前的全局就绪检查保证，不在每个叶重复检查。')
replace('c08-build-heading', '各叶使用内容摘要比对；末尾再检查缺失表总行数及两个 staging 数据集契约。', '每个完整输出叶执行业务校验一次；staging 直接打开当前叶，转换为权威 Arrow 后比较完整内容摘要。末尾各打开一次 staging 表根，检查缺失表总行数及两个数据集的全部 fragment 契约。')
cells['c08-build-heading'].source += '\n\n列清单、理论分钟范围表达式、时区转换表达式以及本批频率/检测时间表达式在分区循环前建立，按当前品种组求值。分钟两列投影后不再次选择同样两列；缺失明细不在写入前额外排序，摘要阶段统一按主键排序。上游根只在生成前打开，逐分区只物化当前日历叶和当前品种月分钟主键，不逐分区复查全表。'
replace('b08-flow-build', '校验、暂存缺失叶；复读并比较内容摘要', '缺失叶业务校验一次；暂存后直接复读当前叶并比摘要')
replace('b08-flow-build', '校验、暂存完整日历叶；复读并比较内容摘要', '日历叶业务校验一次；暂存后直接复读当前叶并比摘要')
replace('c08-commit-heading', '逐叶主键与内容摘要', '逐叶完整内容摘要')

# 修改只限 source，保持所有原注释和 Notebook 运行现场。
def comments(source):
    return [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline)
            if token.type == tokenize.COMMENT]

for old, new in zip(before.cells, notebook.cells, strict=True):
    assert {key: value for key, value in old.items() if key != 'source'} == {
        key: value for key, value in new.items() if key != 'source'
    }, old.id
    if old.cell_type == 'code':
        assert comments(old.source) == comments(new.source), old.id
        ast.parse(new.source)
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print('updated:', PATH)
